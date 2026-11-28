//! Automatic indexing for the caller's own tenant, over the controller's
//! `GET /admin/v1/tenants/{t}/indexes` (the tenant's index records, the
//! loop's events of the last 30 days and the operators' pending requests)
//! and `POST /admin/v1/tenants/{t}/indexes/{index}:{action}`.
//!
//! | Route | Does |
//! |---|---|
//! | `GET /v1/indexing/indexes?db=&table=&state=&all=` | The indexes (the ended ones, `dropped`, `rejected` and `failed`, only with `all=true` or their `state`), the events and pending requests, all limited to `db` and `table` when given |
//! | `GET /v1/indexing/recommendations?db=&table=` | The indexes in state `recommended`; each names its request in the approvals inbox (`approval`), where it is approved or dismissed |
//! | `GET /v1/indexing/indexes/{index}` | One index (its name, `table.name` or `db.table.name`, any case) with its events and pending requests |
//! | `GET /v1/indexing/decisions/{id}` | One entry of the decision log |
//! | `POST /v1/indexing/indexes/{index}:{action}` | `drop`, `pin` (`freeze`), `unpin` (`unfreeze`) or `restore` `{reason}`: 202 with the request, which the node running the tenant's loop applies within a minute |
//!
//! Reads need only a signed-in user (the views show names and plans, not
//! data); actions need a privilege to create indexes in the index's
//! database ([`super::grants::Privileges::may_index`]). Requests are
//! recorded as `studio:<tenant>.<user>`.

use std::sync::Arc;

use bytes::Bytes;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use serde_json::{Value, json};
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::{ConnInfo, Core};
use super::{
    grants, hdr, invalid, json_response, parse_json, passthrough, read_body, rows_of, run_sql, text,
};

/// A query value with `%XX` escapes and `+` for spaces decoded.
fn decode(v: &str) -> String {
    let b = v.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    let hex = |c: u8| (c as char).to_digit(16);
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                if let (Some(h), Some(l)) = (hex(b[i + 1]), hex(b[i + 2])) {
                    out.push(u8::try_from(h * 16 + l).unwrap_or(b'?'));
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            b'+' => out.push(b' '),
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// States an index ends in.
const ENDED: &[&str] = &["dropped", "rejected", "failed"];

/// What a listing keeps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Filter {
    pub(crate) db: String,
    pub(crate) table: String,
    pub(crate) state: String,
    pub(crate) all: bool,
}

impl Filter {
    fn of(query: &str) -> Self {
        let mut f = Self::default();
        for kv in query.split('&') {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            let v = decode(v);
            match k {
                "db" => f.db = v,
                "table" => f.table = v,
                "state" => f.state = v,
                "all" => f.all = v == "true",
                _ => {}
            }
        }
        f
    }

    /// Whether `db.table` is in the filter's scope (any case).
    fn covers(&self, db: &str, table: &str) -> bool {
        (self.db.is_empty() || self.db.eq_ignore_ascii_case(db))
            && (self.table.is_empty() || self.table.eq_ignore_ascii_case(table))
    }
}

fn spec_of(rec: &Value) -> (&str, &str) {
    (
        rec["spec"]["db"].as_str().unwrap_or(""),
        rec["spec"]["table"].as_str().unwrap_or(""),
    )
}

fn state_of(rec: &Value) -> &str {
    rec["state"].as_str().unwrap_or("")
}

fn name_of(v: &Value) -> &str {
    v["index"]
        .as_str()
        .or_else(|| v["name"].as_str())
        .unwrap_or("")
}

/// The controller's listing `v` limited to `f`: indexes, the events of
/// tables in scope and the pending requests of the indexes kept.
pub(crate) fn filter_list(v: &Value, f: &Filter) -> Value {
    let empty = Vec::new();
    let indexes: Vec<Value> = v["indexes"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter(|r| {
            let (db, table) = spec_of(r);
            let state = state_of(r);
            f.covers(db, table)
                && if f.state.is_empty() {
                    f.all || !ENDED.contains(&state)
                } else {
                    f.state.eq_ignore_ascii_case(state)
                }
        })
        .cloned()
        .collect();
    let kept: Vec<String> = indexes
        .iter()
        .map(|r| name_of(r).to_ascii_lowercase())
        .collect();
    let events: Vec<Value> = v["events"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter(|e| {
            let t = e["table"].as_str().unwrap_or("");
            let (db, table) = t.split_once('.').unwrap_or(("", t));
            f.covers(db, table)
        })
        .cloned()
        .collect();
    let requests: Vec<Value> = v["requests"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter(|r| kept.contains(&name_of(r).to_ascii_lowercase()))
        .cloned()
        .collect();
    json!({"tenant": v["tenant"], "indexes": indexes, "events": events, "requests": requests})
}

/// The record of index `index` in listing `v`: by its name, `table.name`
/// or `db.table.name`, any case.
pub(crate) fn find_index<'a>(v: &'a Value, index: &str) -> Option<&'a Value> {
    let (qual, short) = match index.rsplit_once('.') {
        Some((q, n)) => (Some(q.to_ascii_lowercase()), n),
        None => (None, index),
    };
    v["indexes"].as_array()?.iter().find(|r| {
        let (db, table) = spec_of(r);
        name_of(r).eq_ignore_ascii_case(short)
            && qual.as_deref().is_none_or(|q| {
                q == table.to_ascii_lowercase() || q == format!("{db}.{table}").to_ascii_lowercase()
            })
    })
}

/// One index with its events and pending requests.
pub(crate) fn detail(v: &Value, index: &str) -> Option<Value> {
    let rec = find_index(v, index)?;
    let name = name_of(rec);
    let mine = |x: &&Value| name_of(x).eq_ignore_ascii_case(name);
    let empty = Vec::new();
    Some(json!({
        "index": rec,
        "events": v["events"].as_array().unwrap_or(&empty).iter().filter(mine).cloned().collect::<Vec<_>>(),
        "requests": v["requests"].as_array().unwrap_or(&empty).iter().filter(mine).cloned().collect::<Vec<_>>(),
    }))
}

/// The controller's action for Studio's `verb`.
///
/// # Errors
/// A verb that isn't one; approving or dismissing a recommended index is
/// done in the approvals inbox.
pub(crate) fn action_of(verb: &str) -> Result<&'static str, SprigError> {
    match verb.to_ascii_lowercase().as_str() {
        "drop" => Ok("drop"),
        "pin" | "freeze" => Ok("pin"),
        "unpin" | "unfreeze" => Ok("unpin"),
        "restore" => Ok("restore"),
        "approve" | "apply" | "reject" | "dismiss" => Err(invalid(format!(
            "{verb}: a recommended index is approved or dismissed in the approvals inbox \
             (its request is the index's `approval`)"
        ))),
        _ => Err(invalid(format!(
            "unknown index action {verb} (drop, pin, unpin, restore)"
        ))),
    }
}

fn no_index(index: &str) -> SprigError {
    SprigError::new(
        SprigCode::AutoIndexNotFound,
        format!("no automatic index {index}"),
    )
}

/// The controller's listing for `base` (`/admin/v1/tenants/{t}`), or its
/// answer when it isn't one.
async fn listing(
    core: &Arc<Core>,
    base: &str,
) -> Result<Result<Value, Response<Body>>, SprigError> {
    let r = core
        .admin
        .forward_internal(&Method::GET, &format!("{base}/indexes"), Bytes::new())
        .await?;
    if r.status != 200 {
        return Ok(Err(passthrough(r.status, r.body)));
    }
    Ok(Ok(serde_json::from_slice(&r.body).unwrap_or(Value::Null)))
}

/// Answers `/v1/indexing/...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let auth = hdr(&req, "authorization");
    let host = hdr(&req, "host");
    let method = req.method().clone();
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let body = read_body(req).await?;
    let caller = core.caller(info, auth, None, host).await?;
    let Some(tenant) = caller.principal.tenant_name.clone() else {
        return Err(invalid("automatic indexing needs a tenant login"));
    };
    let rest = path.trim_start_matches("/v1/indexing/");
    let seg: Vec<&str> = rest.split('/').collect();
    let safe = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-' | '$'))
    };
    let base = format!("/admin/v1/tenants/{tenant}");
    match (method.as_str(), seg.as_slice()) {
        ("GET", ["indexes"]) => match listing(core, &base).await? {
            Ok(v) => Ok(json_response(200, &filter_list(&v, &Filter::of(&query)))),
            Err(r) => Ok(r),
        },
        ("GET", ["recommendations"]) => match listing(core, &base).await? {
            Ok(v) => {
                let mut f = Filter::of(&query);
                f.state = "recommended".into();
                Ok(json_response(200, &filter_list(&v, &f)))
            }
            Err(r) => Ok(r),
        },
        ("GET", ["indexes", i]) if safe(i) => match listing(core, &base).await? {
            Ok(v) => detail(&v, i)
                .map(|d| json_response(200, &d))
                .ok_or_else(|| no_index(i)),
            Err(r) => Ok(r),
        },
        ("GET", ["decisions", id]) if safe(id) => {
            let r = core
                .admin
                .forward_internal(
                    &Method::GET,
                    &format!("{base}/decisions/{id}"),
                    Bytes::new(),
                )
                .await?;
            Ok(passthrough(r.status, r.body))
        }
        ("POST", ["indexes", i]) if safe(i) => {
            let Some((index, verb)) = i.rsplit_once(':') else {
                return Err(invalid("POST /v1/indexing/indexes/{index}:{action}"));
            };
            let action = action_of(verb)?;
            // Which database the index is in, then the caller's
            // privileges there.
            let v = match listing(core, &base).await? {
                Ok(v) => v,
                Err(r) => return Ok(r),
            };
            let rec = find_index(&v, index).ok_or_else(|| no_index(index))?;
            let name = name_of(rec).to_owned();
            let db = spec_of(rec).0.to_owned();
            let grants = run_sql(core, &caller, "-", "SHOW GRANTS".to_owned()).await?;
            let lines: Vec<String> = rows_of(&grants)
                .into_iter()
                .filter_map(|r| r.into_iter().next().flatten())
                .collect();
            if !grants::Privileges::of(&lines).may_index(&db) {
                return Err(SprigError::standard(
                    1142,
                    *b"42000",
                    format!(
                        "INDEX command denied to user '{}' for database '{db}'",
                        caller.principal.user
                    ),
                ));
            }
            let req_body = if body.is_empty() {
                json!({})
            } else {
                parse_json(&body)?
            };
            let fwd = json!({
                "reason": text(&req_body, "reason"),
                "actor": format!("studio:{tenant}.{}", caller.principal.user),
            });
            let r = core
                .admin
                .forward_internal(
                    &Method::POST,
                    &format!("{base}/indexes/{name}:{action}"),
                    Bytes::from(fwd.to_string()),
                )
                .await?;
            Ok(passthrough(r.status, r.body))
        }
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listing() -> Value {
        let rec = |name: &str, db: &str, table: &str, state: &str| {
            json!({"name": name, "spec": {"db": db, "table": table, "cols": [{"column": "k", "desc": false}]},
                   "origin": "auto", "state": state, "benefit_ms_per_hour": 12.5})
        };
        json!({
            "tenant": "acme",
            "indexes": [
                rec("sprig_auto_aaaa0001", "Shop", "Orders", "active"),
                rec("sprig_auto_aaaa0002", "shop", "items", "recommended"),
                rec("sprig_auto_aaaa0003", "shop", "orders", "dropped"),
                rec("sprig_auto_bbbb0001", "crm", "orders", "observing"),
            ],
            "events": [
                {"at_us": 1, "kind": "built", "index": "sprig_auto_aaaa0001", "def": "secondary(k)", "table": "shop.orders", "fingerprints": [], "reason": "top_total_time"},
                {"at_us": 2, "kind": "candidate", "index": "sprig_auto_bbbb0001", "def": "secondary(k)", "table": "crm.orders", "fingerprints": [], "reason": "slow_p95"},
            ],
            "requests": [
                {"index": "sprig_auto_aaaa0001", "action": "pin", "actor": "op:x", "reason": "", "at_us": 3},
                {"index": "sprig_auto_aaaa0003", "action": "restore", "actor": "op:x", "reason": "", "at_us": 4},
            ],
        })
    }

    fn names(v: &Value) -> Vec<&str> {
        v["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["name"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn listings_are_limited_to_their_scope() {
        let v = listing();
        // By default the ended ones are left out.
        let all = filter_list(&v, &Filter::default());
        assert_eq!(
            names(&all),
            [
                "sprig_auto_aaaa0001",
                "sprig_auto_aaaa0002",
                "sprig_auto_bbbb0001"
            ]
        );
        assert_eq!(all["events"].as_array().unwrap().len(), 2);
        // A dropped index's request goes with it.
        assert_eq!(all["requests"].as_array().unwrap().len(), 1);
        let shop = filter_list(&v, &Filter::of("db=SH%4FP&all=true"));
        assert_eq!(
            names(&shop),
            [
                "sprig_auto_aaaa0001",
                "sprig_auto_aaaa0002",
                "sprig_auto_aaaa0003"
            ]
        );
        assert_eq!(shop["events"].as_array().unwrap().len(), 1);
        assert_eq!(shop["requests"].as_array().unwrap().len(), 2);
        let orders = filter_list(&v, &Filter::of("db=shop&table=orders"));
        assert_eq!(names(&orders), ["sprig_auto_aaaa0001"]);
        let recommended = filter_list(&v, &Filter::of("state=recommended"));
        assert_eq!(names(&recommended), ["sprig_auto_aaaa0002"]);
        let dropped = filter_list(&v, &Filter::of("state=dropped"));
        assert_eq!(names(&dropped), ["sprig_auto_aaaa0003"]);
    }

    #[test]
    fn query_values_are_decoded() {
        assert_eq!(decode("a%20b+c%2"), "a b c%2");
        assert_eq!(decode("%zz"), "%zz");
    }

    #[test]
    fn an_index_is_found_by_name_or_qualified() {
        let v = listing();
        for ok in [
            "sprig_auto_aaaa0001",
            "SPRIG_AUTO_AAAA0001",
            "orders.sprig_auto_aaaa0001",
            "shop.orders.sprig_auto_aaaa0001",
        ] {
            assert_eq!(
                find_index(&v, ok).map(|r| r["name"].clone()),
                Some(json!("sprig_auto_aaaa0001")),
                "{ok}"
            );
        }
        assert!(find_index(&v, "items.sprig_auto_aaaa0001").is_none());
        assert!(find_index(&v, "sprig_auto_ffff0000").is_none());
        let d = detail(&v, "sprig_auto_aaaa0001").unwrap();
        assert_eq!(d["events"].as_array().unwrap().len(), 1);
        assert_eq!(d["requests"][0]["action"], "pin");
    }

    #[test]
    fn actions_are_the_controllers() {
        assert_eq!(action_of("freeze").unwrap(), "pin");
        assert_eq!(action_of("Unfreeze").unwrap(), "unpin");
        assert_eq!(action_of("drop").unwrap(), "drop");
        assert_eq!(action_of("restore").unwrap(), "restore");
        let e = action_of("approve").unwrap_err();
        assert!(e.message.contains("approvals inbox"), "{}", e.message);
        assert!(action_of("explode").is_err());
    }
}
