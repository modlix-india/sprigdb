//! `POST /v1/databases/{db}/retrieval/{index}:search {query, limit?,
//! filter?}`: a retrieval index's hybrid search over REST, for any
//! signed-in user or API key.
//!
//! The call runs `SELECT ... FROM RETRIEVE(db.index, query, limit,
//! filter)` in the caller's own executor session, so it is the SQL
//! search exactly: the caller's privileges, column grants and row-level
//! security decide which rows' chunks come back. The reply:
//! `{database, index, hits: [{key, chunk_no, title, text, score,
//! bm25_rank, vector_rank}], warnings: [{level, code, message}]}`, best
//! first; `key` is the source row's primary key as an object (`{"id": 7}`;
//! binary columns as hexadecimal). `warnings` is what `SHOW WARNINGS`
//! answers after the statement (the gateway's own, read in the same
//! session), empty when there are none: 39827 when the search skipped its
//! keyword list.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response, StatusCode};
use serde_json::{Value, json};
use sprig_common::names::quote;
use sprig_common::{SprigCode, SprigError};
use sprig_kv::sql::SqlRunner as _;
use sprig_sql::ai_fn::string_literal;
use sprig_sql::retrieval::{MAX_LIMIT, MAX_QUERY};
use tonic::body::Body;

use crate::http::{ConnInfo, Core};
use crate::redis::SqlSession;

/// The largest request body.
const MAX_BODY: usize = 64 * 1024;

/// Hits without `limit`.
pub const DEFAULT_LIMIT: u32 = 8;

/// A search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchBody {
    /// What to look for.
    pub query: String,
    /// Hits wanted (1 to 100).
    pub limit: u32,
    /// A filter over the index's `FILTERS` columns, as in SQL.
    pub filter: Option<String>,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// `(database, index)` of a search path, percent-decoded.
#[must_use]
pub fn route(method: &str, path: &str) -> Option<(String, String)> {
    if method != "POST" {
        return None;
    }
    let rest = path.strip_prefix("/v1/databases/")?;
    let (db, rest) = rest.split_once('/')?;
    let index = rest.strip_prefix("retrieval/")?.strip_suffix(":search")?;
    if db.is_empty() || index.is_empty() || index.contains('/') {
        return None;
    }
    Some((
        sprig_api::rest::percent_decode(db),
        sprig_api::rest::percent_decode(index),
    ))
}

/// The request's fields, checked.
///
/// # Errors
/// A missing query, a field of the wrong type, a limit out of range.
pub fn parse(body: &[u8]) -> Result<SearchBody, SprigError> {
    let v: Value = if body.iter().all(u8::is_ascii_whitespace) {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_slice(body).map_err(|e| invalid(format!("the body isn't JSON: {e}")))?
    };
    let s = |k: &str| -> Result<Option<String>, SprigError> {
        match v.get(k) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(_) => Err(invalid(format!("{k} must be a string"))),
        }
    };
    let query = s("query")?.unwrap_or_default();
    if query.trim().is_empty() {
        return Err(invalid("query is required"));
    }
    if query.chars().count() > MAX_QUERY {
        return Err(invalid(format!(
            "query is longer than {MAX_QUERY} characters"
        )));
    }
    let limit = match v.get("limit") {
        None | Some(Value::Null) => DEFAULT_LIMIT,
        Some(n) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| invalid(format!("limit must be a number from 1 to {MAX_LIMIT}")))?,
    };
    Ok(SearchBody {
        query,
        limit,
        filter: s("filter")?.filter(|f| !f.trim().is_empty()),
    })
}

/// The statement a request runs.
#[must_use]
pub fn statement(db: &str, index: &str, r: &SearchBody) -> String {
    format!(
        "{} SELECT `key`, `chunk_no`, `title`, `chunk`, `score`, `bm25_rank`, `vector_rank` \
         FROM RETRIEVE({}.{}, {}, {}{}) AS r ORDER BY `rank`",
        crate::exec::RETRIEVE_KEY_JSON_HINT,
        quote(db),
        quote(index),
        string_literal(&r.query, false),
        r.limit,
        r.filter
            .as_deref()
            .map_or_else(String::new, |f| format!(", {}", string_literal(f, false)))
    )
}

fn json_response(status: u16, v: &Value) -> Response<Body> {
    let mut r = Response::new(Body::new(Full::new(Bytes::from(
        serde_json::to_vec(v).unwrap_or_default(),
    ))));
    *r.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let h = r.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn hdr(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

/// The hits of one row of the statement's answer.
fn hit(row: &[Option<Vec<u8>>]) -> Value {
    let t = |i: usize| {
        row.get(i)
            .and_then(Option::as_ref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    let n = |i: usize| t(i).and_then(|s| s.parse::<u64>().ok());
    json!({
        "key": t(0).and_then(|k| serde_json::from_str::<Value>(&k).ok()).unwrap_or(Value::Null),
        "chunk_no": n(1),
        "title": t(2),
        "text": t(3),
        "score": t(4).and_then(|s| s.parse::<f64>().ok()),
        "bm25_rank": n(5),
        "vector_rank": n(6),
    })
}

/// One row of `SHOW WARNINGS` (`Level`, `Code`, `Message`).
fn warning(row: &[Option<Vec<u8>>]) -> Value {
    let t = |i: usize| {
        row.get(i)
            .and_then(Option::as_ref)
            .map(|b| String::from_utf8_lossy(b).into_owned())
    };
    json!({
        "level": t(0),
        "code": t(1).and_then(|s| s.parse::<u64>().ok()),
        "message": t(2),
    })
}

/// Answers a search.
pub async fn search(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
    db: &str,
    index: &str,
) -> Response<Body> {
    match search_inner(core, info, req, db, index).await {
        Ok(r) => r,
        Err(e) => {
            let mut r = crate::http::rest::error_response(&e);
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
    }
}

async fn search_inner(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
    db: &str,
    index: &str,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    let tenant = hdr(&req, crate::http::rest::TENANT_HEADER);
    let host = hdr(&req, "host");
    let body = match Limited::new(req.into_body(), MAX_BODY).collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) if e.is::<http_body_util::LengthLimitError>() => {
            return Err(SprigError::new(
                SprigCode::RequestTooLarge,
                "the request body is too large",
            ));
        }
        Err(e) => return Err(invalid(format!("can't read the request body: {e}"))),
    };
    let caller = core.caller(info, auth, tenant, host).await?;
    let request = parse(&body)?;
    let mut sql = SqlSession::open(
        &core.engine.sessions,
        &core.engine.executor,
        &caller.principal,
        caller.listener,
        caller.generation,
        caller.peer,
        caller.tls.clone(),
    )?;
    let rows = sql
        .query(&statement(db, index, &request))
        .await
        .map_err(|e| SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message))?;
    let hits: Vec<Value> = rows.iter().map(|r| hit(r)).collect();
    // The search's warnings, answered by the gateway from the session.
    let warnings: Vec<Value> = sql
        .query("SHOW WARNINGS")
        .await
        .map_err(|e| SprigError::standard(e.errno.unwrap_or(1105), *b"HY000", &e.message))?
        .iter()
        .map(|r| warning(r))
        .collect();
    Ok(json_response(
        200,
        &json!({"database": db, "index": index, "hits": hits, "warnings": warnings}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_and_bodies() {
        assert_eq!(
            route("POST", "/v1/databases/shop/retrieval/kb:search"),
            Some(("shop".into(), "kb".into()))
        );
        assert_eq!(route("GET", "/v1/databases/shop/retrieval/kb:search"), None);
        assert_eq!(route("POST", "/v1/databases/shop/retrieval/kb"), None);
        assert_eq!(
            route("POST", "/v1/databases/shop/vectorIndexes/kb:search"),
            None
        );
        let r = parse(br#"{"query":"reset password","limit":3,"filter":"lang = 'en'"}"#).unwrap();
        assert_eq!(r.limit, 3);
        assert_eq!(r.filter.as_deref(), Some("lang = 'en'"));
        assert_eq!(parse(br#"{"query":"x"}"#).unwrap().limit, DEFAULT_LIMIT);
        assert!(parse(b"{}").is_err());
        assert!(parse(br#"{"query":"x","limit":0}"#).is_err());
        assert!(parse(br#"{"query":"x","limit":101}"#).is_err());
        assert!(parse(br#"{"query":1}"#).is_err());
        let sql = statement("shop", "kb", &r);
        assert!(sql.starts_with(crate::exec::RETRIEVE_KEY_JSON_HINT));
        assert!(
            sql.contains("FROM RETRIEVE(`shop`.`kb`, 'reset password', 3, 'lang = ''en''') AS r")
        );
    }

    #[test]
    fn rows_become_hits() {
        let row = vec![
            Some(br#"{"id":7}"#.to_vec()),
            Some(b"0".to_vec()),
            None,
            Some(b"chunk".to_vec()),
            Some(b"0.0325".to_vec()),
            Some(b"1".to_vec()),
            None,
        ];
        assert_eq!(
            hit(&row),
            json!({"key": {"id": 7}, "chunk_no": 0, "title": null, "text": "chunk",
                   "score": 0.0325, "bm25_rank": 1, "vector_rank": null})
        );
        let w = vec![
            Some(b"Warning".to_vec()),
            Some(b"39827".to_vec()),
            Some(b"RETRIEVE(shop.kb): no keyword list".to_vec()),
        ];
        assert_eq!(
            warning(&w),
            json!({"level": "Warning", "code": 39827,
                   "message": "RETRIEVE(shop.kb): no keyword list"})
        );
    }
}
