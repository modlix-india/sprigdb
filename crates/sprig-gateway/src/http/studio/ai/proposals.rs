//! `POST /v1/ai/proposals/{id}:accept` and `:dismiss`: what a user does
//! with a tenant assistant's proposal.
//!
//! | Kind | `:accept` does |
//! |---|---|
//! | `sql` (and the policy and logic proposals, which are statements) | runs the statement as the caller, in the database it names, and answers with its results. A statement that isn't a plain read runs only with `confirm: true`, which the client sends after the user confirmed |
//! | `generate_rows` | runs the generated `INSERT` as the caller, with `confirm: true` |
//! | `recommendation` | files an `ai_dba_advice` item in the approvals inbox, at the tenant's level; advice only |
//! | `glossary` | accepts the proposed term (tenant administrators) |
//! | `tile` | not yet: dashboards come later |
//!
//! `:dismiss` records that the user turned it down. Either way the
//! outcome goes to `ai_feedback` (`accepted`, `failed` or `dismissed`), so
//! the assistants' prompts learn from it, and to
//! `sprig_ai_proposals_total{kind,outcome}`. A proposal is found only in
//! the caller's own conversations; one already accepted can't be accepted
//! again. Nothing here has rights of its own: every statement runs with
//! the caller's grants and row-level security, as if they had typed it.

use std::sync::Arc;

use hyper::Method;
use serde_json::{Value, json};
use sprig_ai::feedback::{Found, Outcome};
use sprig_common::audit::AuditStream;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::super::sql::Caller;
use super::super::{json_response, run_sql};
use super::{Core, invalid, views};
use crate::http::studio::ai::host::result_set;
use sprig_api::proto::v1::ExecuteResponse;

/// Rows of one result the reply carries at most.
pub const MAX_ROWS: usize = 1000;

fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v.get(k).and_then(Value::as_str).unwrap_or("")
}

/// One statement's result as JSON: its columns and rows (at most
/// [`MAX_ROWS`]), or what it changed.
#[must_use]
pub fn results_json(r: &ExecuteResponse) -> Vec<Value> {
    r.results
        .iter()
        .map(|x| {
            let one = ExecuteResponse {
                results: vec![x.clone()],
            };
            let mut rs = result_set(&one);
            let cut = rs.rows.len() > MAX_ROWS;
            rs.rows.truncate(MAX_ROWS);
            json!({
                "columns": rs.columns,
                "rows": rs.rows,
                "truncated": rs.truncated || cut,
                "affected_rows": x.affected_rows,
                "last_insert_id": x.last_insert_id,
                "warnings": x.warnings,
                "info": x.info,
            })
        })
        .collect()
}

/// The statements of a proposed text, split as a client's would be.
fn split(sql: &str) -> Vec<String> {
    crate::exec::split::split(&bytes::Bytes::copy_from_slice(sql.as_bytes()))
        .into_iter()
        .map(|b| {
            String::from_utf8_lossy(&b)
                .trim()
                .trim_end_matches(';')
                .trim()
                .to_owned()
        })
        .filter(|s| !s.is_empty())
        .collect()
}

/// Whether every statement is a plain read, which runs without `confirm`.
fn all_reads(stmts: &[String]) -> bool {
    stmts
        .iter()
        .all(|q| sprig_ai::sql_guard::read_only(q).is_ok())
}

fn not_confirmed() -> SprigError {
    invalid(
        "this proposal changes data or schema: show it to the user, and send confirm: true once they confirmed",
    )
}

/// Runs `stmts` in `db` as the caller, in order, stopping at the first
/// failure: the results so far, and the failure.
async fn run_all(
    core: &Arc<Core>,
    caller: &Caller,
    db: &str,
    stmts: &[String],
) -> (Vec<Value>, Option<SprigError>) {
    let mut out = Vec::new();
    for q in stmts {
        match run_sql(core, caller, db, q.clone()).await {
            Ok(r) => out.push(json!({"sql": q, "results": results_json(&r)})),
            Err(e) => return (out, Some(e)),
        }
    }
    (out, None)
}

/// Files a recommendation proposal as an `ai_dba_advice` item.
async fn file_advice(
    core: &Arc<Core>,
    caller: &Caller,
    f: &Found,
    id: &str,
) -> Result<Value, SprigError> {
    let Some(tenant) = caller.principal.tenant else {
        return Err(invalid("the approvals inbox needs a tenant login"));
    };
    let b = f.body();
    let subject = s(b, "subject");
    let text = s(b, "body");
    let req = json!({
        "op": "check",
        "kind": sprig_approvals::Kind::AiDbaAdvice.as_str(),
        "tenant": tenant.0,
        "db": s(b, "db"),
        "target": format!("assistant/{id}"),
        "action": format!("{text}\n\nAdvice from the {} assistant, accepted by {}: nothing is changed by itself.", f.role, caller.principal.user),
        "machine": {"proposal": id},
        "evidence": {
            "assistant": f.role.as_str(),
            "kind": s(b, "kind"),
            "subject": subject,
            "proposal": id,
            "accepted_by": caller.principal.user,
        },
        "summary": if subject.is_empty() { text.chars().take(200).collect::<String>() } else { subject.to_owned() },
        "score": 1.0,
    });
    let reply = core
        .admin
        .forward_internal(
            &Method::POST,
            "/admin/v1/recommendations:gate",
            bytes::Bytes::from(req.to_string()),
        )
        .await?;
    let v: Value = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
    if reply.status >= 300 {
        return Err(SprigError::new(
            SprigCode::AdminRequestInvalid,
            v["message"]
                .as_str()
                .or_else(|| v["error"]["message"].as_str())
                .unwrap_or("the approvals inbox refused the item")
                .to_owned(),
        ));
    }
    let item = v["why"]
        .as_str()
        .and_then(|w| w.strip_prefix("filed as "))
        .map(str::to_owned);
    Ok(json!({"filed": "approvals", "item": item, "detail": v["why"]}))
}

/// `POST /v1/ai/proposals/{id}:accept|dismiss`.
///
/// # Errors
/// 39792 for an unknown proposal or verb, a proposal already accepted, a
/// change without `confirm`, or a kind that can't be accepted here; 1227
/// for a glossary term without `SPRIG_TENANT_ADMIN`.
#[allow(clippy::too_many_lines)] // one arm per kind
pub async fn route(
    core: &Arc<Core>,
    caller: &Caller,
    id: &str,
    verb: &str,
    b: &Value,
) -> Result<hyper::Response<Body>, SprigError> {
    let note = b.get("note").and_then(Value::as_str);
    if note.is_some_and(|n| n.chars().count() > sprig_ai::feedback::MAX_NOTE) {
        return Err(invalid(format!(
            "note is at most {} characters",
            sprig_ai::feedback::MAX_NOTE
        )));
    }
    if id.is_empty() || id.len() > 64 {
        return Err(invalid("POST /v1/ai/proposals/{id}:accept|dismiss"));
    }
    let v = views(core, caller).await?;
    let Some((st, user)) = &v.tenant else {
        return Err(invalid(
            "proposals are accepted from a tenant login's own conversations",
        ));
    };
    let f = st
        .proposal(*user, id)
        .await?
        .ok_or_else(|| invalid(format!("proposal {id} isn't in your conversations")))?;
    let kind = super::proposal_kind(f.kind());
    let record = |outcome: Outcome| {
        let st = Arc::clone(st);
        let user = *user;
        async move {
            st.outcome(user, id, outcome, note).await?;
            sprig_ai::metrics::proposal(kind, outcome.as_str());
            if let Some(t) = caller.principal.tenant {
                sprig_common::audit::record(
                    AuditStream::Tenant(t),
                    "ai.proposal",
                    json!({
                        "user": caller.principal.user, "proposal": id, "kind": kind,
                        "outcome": outcome.as_str(),
                    }),
                );
            }
            Ok::<(), SprigError>(())
        }
    };
    match verb {
        "dismiss" => {
            record(Outcome::Dismissed).await?;
            return Ok(json_response(
                200,
                &json!({"proposal": id, "kind": kind, "outcome": "dismissed"}),
            ));
        }
        "accept" => {}
        _ => return Err(invalid("the proposals' verbs are accept and dismiss")),
    }
    if f.decided == Some(Outcome::Accepted) {
        return Err(invalid(format!(
            "proposal {id} was already accepted; run the statement yourself to run it again"
        )));
    }
    let confirm = b.get("confirm").and_then(Value::as_bool) == Some(true);
    let body = f.body().clone();
    let (outcome, reply) = match kind {
        "sql" | "generate_rows" => {
            let (db, text) = if kind == "sql" {
                (s(&body, "db").to_owned(), s(&body, "sql").to_owned())
            } else {
                let generated = f.proposal.get("generated").cloned().unwrap_or(Value::Null);
                let sql = s(&generated, "sql").to_owned();
                if sql.is_empty() {
                    return Err(invalid(
                        "this test-data proposal holds no generated rows to insert",
                    ));
                }
                (s(&body, "db").to_owned(), sql)
            };
            let stmts = split(&text);
            if stmts.is_empty() {
                return Err(invalid("the proposal holds no statement"));
            }
            if !confirm && (kind != "sql" || !all_reads(&stmts)) {
                return Err(not_confirmed());
            }
            let db = if db.is_empty() { "-".to_owned() } else { db };
            let (ran, err) = run_all(core, caller, &db, &stmts).await;
            match err {
                None => (
                    Outcome::Accepted,
                    json!({"proposal": id, "kind": kind, "outcome": "accepted", "database": db, "ran": ran}),
                ),
                Some(e) => {
                    let (_, err_body) = sprig_api::status::to_rest(&e);
                    (
                        Outcome::Failed,
                        json!({"proposal": id, "kind": kind, "outcome": "failed", "database": db,
                               "ran": ran, "error": err_body}),
                    )
                }
            }
        }
        "recommendation" => {
            let filed = file_advice(core, caller, &f, id).await?;
            (
                Outcome::Accepted,
                json!({"proposal": id, "kind": kind, "outcome": "accepted", "approvals": filed}),
            )
        }
        "glossary" => {
            let st2 = super::stores(core, caller).await?;
            if !super::tenant_admin(core, caller).await? {
                return Err(super::not_admin());
            }
            let term = s(&body, "term");
            if !sprig_ai::glossary::GlossaryStore::accept(&*st2.glossary, term, st2.user).await? {
                return Err(invalid(format!(
                    "there is no proposed glossary entry for {term}"
                )));
            }
            (
                Outcome::Accepted,
                json!({"proposal": id, "kind": kind, "outcome": "accepted", "term": term}),
            )
        }
        _ => {
            return Err(invalid(format!(
                "{kind} proposals can't be accepted here yet"
            )));
        }
    };
    record(outcome).await?;
    Ok(json_response(200, &reply))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_run_without_confirm_and_the_rest_need_it() {
        assert!(all_reads(&split("SELECT 1; SELECT id FROM t")));
        assert!(!all_reads(&split("SELECT 1; DELETE FROM t")));
        assert!(!all_reads(&split("CREATE INDEX i ON t (a)")));
        assert_eq!(
            split("CREATE INDEX i ON t (a);\n ALTER TABLE t SPRIG SET (x = 1);").len(),
            2
        );
    }
}
