//! `POST /v1/docs:search {query, audience?, limit?, version?}` and
//! `GET /v1/docs/pages/{page}?version=`: any signed-in user, Studio
//! session or API key.
//!
//! The search's reply: `{version, mode, hits: [{chunk_id, page, title,
//! heading_path, audience, text, score, bm25_rank, vector_rank}]}`; `mode`
//! is `hybrid`, or `keyword` when the query wasn't embedded.
//!
//! A page's reply: `{version, page, title, audience, ordered, markdown,
//! chunks: [{chunk_id, heading_path, text, seq}]}` ([`super::page`]).

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response, StatusCode};
use serde_json::Value;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::search::SearchRequest;
use crate::http::{ConnInfo, Core};

/// The path.
pub const PATH: &str = "/v1/docs:search";
/// The pages' path: the page's path under `docs/` follows.
pub const PAGES: &str = "/v1/docs/pages";

/// The largest request body.
const MAX_BODY: usize = 64 * 1024;

fn hdr(req: &Request<Incoming>, name: &str) -> Option<String> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn json(status: u16, v: &Value) -> Response<Body> {
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

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// The request's fields, checked for type.
///
/// # Errors
/// A field of the wrong type.
pub fn parse(body: &[u8]) -> Result<SearchRequest, SprigError> {
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
    let limit = match v.get("limit") {
        None | Some(Value::Null) => None,
        Some(n) => Some(
            n.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| invalid("limit must be a positive integer"))?,
        ),
    };
    Ok(SearchRequest {
        query: s("query")?.unwrap_or_default(),
        audience: s("audience")?,
        limit,
        version: s("version")?,
    })
}

/// Answers `POST /v1/docs:search`.
pub async fn search(core: &Arc<Core>, info: &ConnInfo, req: Request<Incoming>) -> Response<Body> {
    match search_inner(core, info, req).await {
        Ok(r) => r,
        Err(e) => {
            let mut r = crate::http::rest::error_response(&e);
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
    }
}

/// Answers `GET /v1/docs/pages/{page}`.
pub async fn page(core: &Arc<Core>, info: &ConnInfo, req: Request<Incoming>) -> Response<Body> {
    match page_inner(core, info, req).await {
        Ok(r) => r,
        Err(e) => {
            let mut r = crate::http::rest::error_response(&e);
            r.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            r
        }
    }
}

/// The page path of a `GET` path under [`PAGES`], and the `version` query
/// parameter.
///
/// # Errors
/// A path that isn't under [`PAGES`].
pub fn page_request(
    path: &str,
    query: Option<&str>,
) -> Result<(String, Option<String>), SprigError> {
    let page = path
        .strip_prefix(PAGES)
        .filter(|r| r.is_empty() || r.starts_with('/'))
        .ok_or_else(|| invalid(format!("no route GET {path}")))?;
    let version = query
        .unwrap_or("")
        .split('&')
        .find_map(|kv| kv.strip_prefix("version="))
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    Ok((page.trim_start_matches('/').to_owned(), version))
}

async fn page_inner(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    let tenant = hdr(&req, crate::http::rest::TENANT_HEADER);
    let host = hdr(&req, "host");
    let (page, version) = page_request(req.uri().path(), req.uri().query())?;
    // Signed in first: nothing about the docs is answered anonymously.
    let caller = core.caller(info, auth, tenant, host).await?;
    let svc =
        super::service().ok_or_else(|| super::unavailable("this gateway has no tenant logins"))?;
    let reply = svc
        .page(caller.principal.tenant, &page, version.as_deref())
        .await?;
    Ok(json(
        200,
        &serde_json::to_value(&reply).map_err(|e| SprigError::internal(e.to_string()))?,
    ))
}

async fn search_inner(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
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
    // Signed in first: nothing about the docs is answered anonymously.
    let caller = core.caller(info, auth, tenant, host).await?;
    let request = parse(&body)?;
    let svc =
        super::service().ok_or_else(|| super::unavailable("this gateway has no tenant logins"))?;
    let reply = svc.search(caller.principal.tenant, &request).await?;
    Ok(json(
        200,
        &serde_json::to_value(&reply).map_err(|e| SprigError::internal(e.to_string()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_parse_with_types_checked() {
        let r = parse(br#"{"query":"lag","audience":"dba","limit":3,"version":"0.9.0"}"#).unwrap();
        assert_eq!(r.query, "lag");
        assert_eq!(r.audience.as_deref(), Some("dba"));
        assert_eq!(r.limit, Some(3));
        assert_eq!(r.version.as_deref(), Some("0.9.0"));
        assert_eq!(parse(b" ").unwrap(), SearchRequest::default());
        assert!(parse(br#"{"query":1}"#).is_err());
        assert!(parse(br#"{"query":"x","limit":-1}"#).is_err());
        assert!(parse(br#"{"query":"x","limit":"3"}"#).is_err());
        assert!(parse(b"not json").is_err());
    }

    #[test]
    fn page_requests_name_the_page_and_version() {
        assert_eq!(
            page_request("/v1/docs/pages/tools/studio.md", Some("version=0.9.0")).unwrap(),
            ("tools/studio.md".to_owned(), Some("0.9.0".to_owned()))
        );
        assert_eq!(
            page_request("/v1/docs/pages", None).unwrap(),
            (String::new(), None)
        );
        assert!(page_request("/v1/docs/pagesx", None).is_err());
    }
}
