//! Recent activity for Studio's home pages, read from the tenant's catalog
//! and never shown beyond what the caller could already read.
//!
//! | Route | Who | Does |
//! |---|---|---|
//! | `GET /v1/activity/schema?limit=N` | any user | The latest schema changes (newest first, 20 by default, at most 200) of the tables, views and routines the caller can see: database, object, kind, version, the statement's verb, when and by whom. Never the statement's text |
//! | `GET /v1/activity/grants?limit=N` | `CREATE USER` or `SPRIG_TENANT_ADMIN` | The latest privilege grants (newest first): grantee, level, target, privileges, when and by whom |
//!
//! Also here: each account's login state for `GET /v1/users`
//! ([`login_states`]): the last successful login, the last failure, the
//! failures in a row and the end of a temporary lock.
//!
//! - **What a caller sees.** A schema change is listed only when its object
//!   is in what `information_schema.TABLES` or `.ROUTINES` shows the caller
//!   now (so a dropped object's changes aren't listed). Grants are listed
//!   only to users who may already read every account's grants with
//!   `SHOW GRANTS FOR`.
//! - **Where.** `schema_versions`, `objects`, `databases`, `privileges`,
//!   `roles`, `users` and `login_state` in `t<tenant>_sprig`; every call
//!   reads them, nothing is cached.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use mysql_async::prelude::Queryable as _;
use serde_json::{Value, json};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_common::names::quote;
use sprig_common::{SprigCode, SprigError};
use tonic::body::Body;

use super::super::sql::Caller;
use super::super::{ConnInfo, Core};
use super::grants::Privileges;
use super::{hdr, invalid, json_response, rows_of, run_sql};

/// Rows a call returns by default.
const DEFAULT_LIMIT: u64 = 20;
/// The most a call returns.
const MAX_LIMIT: u64 = 200;

/// Whether `path` is one of these routes.
#[must_use]
pub fn is_activity_path(path: &str) -> bool {
    path.starts_with("/v1/activity/")
}

/// `limit` from the query string, clamped to 1..=200 (20 when absent).
#[must_use]
pub fn limit_of(query: &str) -> u64 {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("limit=")?.parse::<u64>().ok())
        .unwrap_or(DEFAULT_LIMIT)
        .clamp(1, MAX_LIMIT)
}

/// The statement's verb as a change shows it: its first two words,
/// upper-cased (`ALTER TABLE`, `CREATE VIEW`), never more of the text.
#[must_use]
pub fn verb_of(ddl: &str) -> String {
    const OBJECTS: &[&str] = &[
        "TABLE",
        "VIEW",
        "INDEX",
        "FUNCTION",
        "PROCEDURE",
        "TRIGGER",
        "EVENT",
        "DATABASE",
        "SCHEMA",
        "SEQUENCE",
        "COLLECTION",
        "GRAPH",
        "TYPE",
        "VALIDATION",
        "SCHEDULE",
    ];
    let mut words = ddl
        .split(|c: char| c.is_whitespace() || c == '(')
        .filter(|w| !w.is_empty());
    let first = words.next().unwrap_or("").to_ascii_uppercase();
    if !first.chars().all(|c| c.is_ascii_alphabetic()) {
        return String::new();
    }
    let second = match first.as_str() {
        "CREATE" | "ALTER" | "DROP" | "RENAME" | "TRUNCATE" => words
            .take(8)
            .map(str::to_ascii_uppercase)
            .find(|w| OBJECTS.contains(&w.as_str())),
        _ => None,
    };
    match second {
        Some(s) => format!("{first} {s}"),
        None => first,
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

/// A connection to the caller's tenant catalog and its schema.
async fn catalog_conn(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<(mysql_async::Conn, String), SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("recent activity needs a tenant login"));
    };
    let ep = sprig_catalog::CatalogLocator::writer(&*svc.locator, tid).await?;
    let conn = ep.pool.get_conn_bounded().await.map_err(unavailable)?;
    Ok((conn, ep.store.schema().to_owned()))
}

const MS: &str = "CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {}) DIV 1000 AS SIGNED)";

fn ms_of(col: &str) -> String {
    MS.replace("{}", col)
}

/// One login state, for `GET /v1/users`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoginState {
    /// The last successful login, ms since the epoch.
    pub last_login_ms: Option<i64>,
    /// The last failed login, ms since the epoch.
    pub last_failure_ms: Option<i64>,
    /// Failures in a row since the last success.
    pub failures: u32,
    /// The end of a temporary lock after failed logins, ms since the epoch.
    pub locked_until_ms: Option<i64>,
}

type StateRow = (u32, u32, Option<i64>, Option<i64>, Option<i64>);

/// Every account's login state in the caller's tenant, by user id. Empty
/// when the catalog can't answer: the users list still shows.
pub(super) async fn login_states(core: &Arc<Core>, caller: &Caller) -> HashMap<u32, LoginState> {
    let Ok((mut conn, schema)) = catalog_conn(core, caller).await else {
        return HashMap::new();
    };
    let sql = format!(
        "SELECT user_id, consecutive_failures, {}, {}, {} FROM {}.login_state",
        ms_of("last_success_at"),
        ms_of("last_failure_at"),
        ms_of("locked_until"),
        quote(&schema)
    );
    let rows: Vec<StateRow> = match conn.query(sql).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "users: login state wasn't read");
            return HashMap::new();
        }
    };
    rows.into_iter()
        .map(|(id, failures, ok, bad, until)| {
            (
                id,
                LoginState {
                    last_login_ms: ok,
                    last_failure_ms: bad,
                    failures,
                    locked_until_ms: until,
                },
            )
        })
        .collect()
}

/// `(database, name)` pairs, lower-cased, of everything
/// `information_schema` shows the caller.
async fn visible_objects(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<HashSet<(String, String)>, SprigError> {
    let mut out = HashSet::new();
    for sql in [
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES",
        "SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES",
    ] {
        let r = match run_sql(core, caller, "-", sql.to_owned()).await {
            Ok(r) => r,
            // Routines may be refused where tables aren't; tables may not.
            Err(e) if sql.contains("ROUTINES") => {
                tracing::debug!(error = %e, "activity: routines weren't read");
                continue;
            }
            Err(e) => return Err(e),
        };
        for row in rows_of(&r) {
            if let (Some(Some(db)), Some(Some(name))) = (row.first(), row.get(1)) {
                out.insert((db.to_lowercase(), name.to_lowercase()));
            }
        }
    }
    Ok(out)
}

type ChangeRow = (String, String, String, u64, String, i64, Option<String>);

async fn schema_changes(
    core: &Arc<Core>,
    caller: &Caller,
    limit: u64,
) -> Result<Value, SprigError> {
    let visible = visible_objects(core, caller).await?;
    let (mut conn, schema) = catalog_conn(core, caller).await?;
    let s = quote(&schema);
    // Read more than asked: rows the caller can't see are skipped.
    let sql = format!(
        "SELECT d.name, o.name, o.kind, v.version, LEFT(v.ddl, 200), {}, u.name \
         FROM {s}.schema_versions v \
         JOIN {s}.objects o ON o.object_id = v.object_id \
         JOIN {s}.`databases` d ON d.db_id = o.db_id \
         LEFT JOIN {s}.users u ON u.user_id = v.applied_by \
         WHERE o.state <> 'dropped' AND d.state <> 'dropped' \
         ORDER BY v.applied_at DESC LIMIT ?",
        ms_of("v.applied_at")
    );
    let rows: Vec<ChangeRow> = conn
        .exec(sql, (limit.saturating_mul(5).min(2000),))
        .await
        .map_err(unavailable)?;
    let changes: Vec<Value> = rows
        .into_iter()
        .filter(|(db, name, ..)| visible.contains(&(db.to_lowercase(), name.to_lowercase())))
        .take(usize::try_from(limit).unwrap_or(usize::MAX))
        .map(|(db, name, kind, version, ddl, at, by)| {
            json!({
                "database": db, "object": name, "kind": kind, "version": version,
                "statement": verb_of(&ddl), "at_ms": at, "by": by,
            })
        })
        .collect();
    Ok(json!({ "changes": changes }))
}

type GrantRow = (
    String,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    String,
    u64,
    bool,
    i64,
    Option<String>,
);

async fn recent_grants(core: &Arc<Core>, caller: &Caller, limit: u64) -> Result<Value, SprigError> {
    let grants = run_sql(core, caller, "-", "SHOW GRANTS".to_owned()).await?;
    let lines: Vec<String> = rows_of(&grants)
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect();
    let privs = Privileges::of(&lines);
    if !(privs.create_user || privs.tenant_admin) {
        return Err(SprigError::standard(
            1227,
            *b"42000",
            "Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation (recent grants)",
        ));
    }
    let (mut conn, schema) = catalog_conn(core, caller).await?;
    let s = quote(&schema);
    // Grants made in this tenant to its own users and roles (grants from
    // ancestors to other tenants' grantees aren't this tenant's to show).
    let sql = format!(
        "SELECT p.grantee_kind, COALESCE(u.name, r.name), p.level, d.name, o.name, \
         p.column_name, p.actions, p.restriction, {}, g.name \
         FROM {s}.privileges p \
         LEFT JOIN {s}.users u ON p.grantee_kind = 'user' AND u.user_id = p.grantee_id \
         LEFT JOIN {s}.roles r ON p.grantee_kind = 'role' AND r.role_id = p.grantee_id \
         LEFT JOIN {s}.`databases` d ON d.db_id = p.db_id AND p.db_id <> 0 \
         LEFT JOIN {s}.objects o ON o.object_id = p.object_id AND p.object_id <> 0 \
         LEFT JOIN {s}.users g ON g.user_id = p.granted_by AND p.granted_by_tenant_id = 0 \
         WHERE p.grantee_tenant_id = 0 \
         ORDER BY p.granted_at DESC LIMIT ?",
        ms_of("p.granted_at")
    );
    let rows: Vec<GrantRow> = conn.exec(sql, (limit,)).await.map_err(unavailable)?;
    let out: Vec<Value> = rows
        .into_iter()
        .map(
            |(kind, grantee, level, db, obj, column, actions, restriction, at, by)| {
                let privileges: Vec<&str> = sprig_auth::authz::ActionSet(actions)
                    .iter()
                    .map(sprig_auth::authz::Action::command)
                    .collect();
                json!({
                    "grantee_kind": kind, "grantee": grantee, "level": level,
                    "database": db, "object": obj,
                    "column": (!column.is_empty()).then_some(column),
                    "privileges": privileges, "restriction": restriction,
                    "at_ms": at, "by": by,
                })
            },
        )
        .collect();
    Ok(json!({ "grants": out }))
}

/// Answers `/v1/activity/...`.
pub(super) async fn route(
    core: &Arc<Core>,
    info: &ConnInfo,
    req: Request<Incoming>,
) -> Result<Response<Body>, SprigError> {
    let method = req.method().as_str().to_owned();
    let path = req.uri().path().to_owned();
    let limit = limit_of(req.uri().query().unwrap_or(""));
    let caller = core
        .caller(info, hdr(&req, "authorization"), None, hdr(&req, "host"))
        .await?;
    match (method.as_str(), path.as_str()) {
        ("GET", "/v1/activity/schema") => Ok(json_response(
            200,
            &schema_changes(core, &caller, limit).await?,
        )),
        ("GET", "/v1/activity/grants") => Ok(json_response(
            200,
            &recent_grants(core, &caller, limit).await?,
        )),
        _ => Err(invalid(format!("no route {method} {path}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verbs_never_carry_more_of_the_text() {
        assert_eq!(
            verb_of("ALTER TABLE `orders` ADD COLUMN x INT"),
            "ALTER TABLE"
        );
        assert_eq!(
            verb_of("create or replace view v as select 1"),
            "CREATE VIEW"
        );
        assert_eq!(
            verb_of("CREATE TABLE IF NOT EXISTS t (a INT)"),
            "CREATE TABLE"
        );
        assert_eq!(verb_of("  drop   index ix on t"), "DROP INDEX");
        assert_eq!(verb_of("CREATE `weird`"), "CREATE");
        assert_eq!(
            verb_of("CREATE DEFINER=`a`@`%` FUNCTION f() RETURNS INT"),
            "CREATE FUNCTION"
        );
        assert_eq!(verb_of(""), "");
        assert_eq!(verb_of("ANALYZE TABLE t"), "ANALYZE");
    }

    #[test]
    fn limits_are_clamped() {
        assert_eq!(limit_of(""), 20);
        assert_eq!(limit_of("limit=5"), 5);
        assert_eq!(limit_of("x=1&limit=9999"), 200);
        assert_eq!(limit_of("limit=0"), 1);
        assert_eq!(limit_of("limit=abc"), 20);
        assert!(is_activity_path("/v1/activity/schema"));
        assert!(!is_activity_path("/v1/activityx"));
    }
}
