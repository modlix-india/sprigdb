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
//! - **Where.** The tenant's catalog records in the storage engine: the
//!   core rows (databases, objects and their schema versions) and the
//!   auth module's users, roles, privileges and login states. Every call
//!   reads them in one read-only record transaction; nothing is cached.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use std::time::{SystemTime, UNIX_EPOCH};

use hyper::Request;
use hyper::Response;
use hyper::body::Incoming;
use serde_json::{Value, json};
use sprig_auth::rec::{LOGIN_STATE, PRIVILEGES, ROLES};
use sprig_auth::store::LoginStateRow;
use sprig_catalog::store::rec::CoreRecords;
use sprig_catalog::{DbState, ObjectState, Pk};
use sprig_common::SprigError;
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

/// A transaction on the caller's tenant's catalog records (read only:
/// dropped, so rolled back).
async fn records(
    core: &Arc<Core>,
    caller: &Caller,
) -> Result<Box<dyn sprig_catalog::RecordTx>, SprigError> {
    let (Some(svc), Some(tid)) = (&core.studio.tenants, caller.principal.tenant) else {
        return Err(invalid("recent activity needs a tenant login"));
    };
    svc.records.begin(tid).await
}

/// Milliseconds since the epoch.
fn ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
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

/// Every account's login state in the caller's tenant, by user id. Empty
/// when the catalog can't answer: the users list still shows.
pub(super) async fn login_states(core: &Arc<Core>, caller: &Caller) -> HashMap<u32, LoginState> {
    let Ok(mut tx) = records(core, caller).await else {
        return HashMap::new();
    };
    let rows: Vec<LoginStateRow> = match LOGIN_STATE.rows(&mut *tx, &Pk::new()).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "users: login state wasn't read");
            return HashMap::new();
        }
    };
    rows.into_iter()
        .map(|r| {
            (
                r.user_id.0,
                LoginState {
                    last_login_ms: r.last_success_at.map(ms),
                    last_failure_ms: r.last_failure_at.map(ms),
                    failures: r.consecutive_failures,
                    locked_until_ms: r.locked_until.map(ms),
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

/// One schema change as listed: only what the page shows (the statement's
/// verb, never its text).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Change {
    at: SystemTime,
    /// Breaks ties of `at` in arrival order.
    seq: u64,
    db: String,
    object: String,
    kind: &'static str,
    version: u64,
    statement: String,
    by: Option<u32>,
}

impl Ord for Change {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(o.at, o.seq))
    }
}

impl PartialOrd for Change {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

/// The newest `cap` changes seen, in a min-heap: memory is `cap` entries
/// whatever the catalog holds.
struct Newest {
    cap: usize,
    seq: u64,
    heap: BinaryHeap<Reverse<Change>>,
}

impl Newest {
    fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            seq: 0,
            heap: BinaryHeap::new(),
        }
    }

    /// Whether a change made at `at` would be kept. False once `cap` newer
    /// ones are held: an object's older versions can then be skipped.
    fn takes(&self, at: SystemTime) -> bool {
        self.heap.len() < self.cap || self.heap.peek().is_some_and(|Reverse(m)| at > m.at)
    }

    fn push(&mut self, mut c: Change) {
        c.seq = self.seq;
        self.seq += 1;
        self.heap.push(Reverse(c));
        if self.heap.len() > self.cap {
            self.heap.pop();
        }
    }

    /// Newest first.
    fn into_sorted(self) -> Vec<Change> {
        let mut v: Vec<Change> = self.heap.into_iter().map(|Reverse(c)| c).collect();
        v.sort_by(|a, b| b.cmp(a));
        v
    }
}

async fn schema_changes(
    core: &Arc<Core>,
    caller: &Caller,
    limit: u64,
) -> Result<Value, SprigError> {
    let visible = visible_objects(core, caller).await?;
    let mut tx = records(core, caller).await?;
    let dbs: HashMap<u32, String> = CoreRecords::databases_since(&mut *tx, None)
        .await?
        .into_iter()
        .filter(|d| d.state != DbState::Dropped)
        .map(|d| (d.db_id.0, d.name))
        .collect();
    let cap = usize::try_from(limit).unwrap_or(usize::MAX);
    let mut newest = Newest::new(cap);
    for o in CoreRecords::objects_since(&mut *tx, None).await? {
        if o.state == ObjectState::Dropped {
            continue;
        }
        let Some(db) = dbs.get(&o.db_id.0) else {
            continue;
        };
        if !visible.contains(&(db.to_lowercase(), o.name.to_lowercase())) {
            continue;
        }
        // Each version by key, newest first; an object's versions are in
        // time order, so stop at the first one too old to be kept (or a
        // purged one). At most `limit` per object.
        let mut v = o.schema_version;
        let mut read = 0;
        while v > 0 && read < cap {
            let Some(row) = CoreRecords::schema_version(&mut *tx, o.object_id, v).await? else {
                break;
            };
            if !newest.takes(row.applied_at) {
                break;
            }
            newest.push(Change {
                at: row.applied_at,
                seq: 0,
                db: db.clone(),
                object: o.name.clone(),
                kind: o.kind.as_str(),
                version: row.version,
                statement: verb_of(&row.ddl),
                by: row.applied_by.map(|u| u.0),
            });
            v -= 1;
            read += 1;
        }
    }
    let users = user_names(&mut *tx).await?;
    drop(tx);
    let changes: Vec<Value> = newest
        .into_sorted()
        .into_iter()
        .map(|c| {
            json!({
                "database": c.db, "object": c.object, "kind": c.kind, "version": c.version,
                "statement": c.statement, "at_ms": ms(c.at),
                "by": c.by.and_then(|u| users.get(&u).cloned()),
            })
        })
        .collect();
    Ok(json!({ "changes": changes }))
}

/// Every user's name, by id, tombstones included (a change keeps its
/// author's name after the account is dropped).
async fn user_names(
    tx: &mut dyn sprig_catalog::RecordTx,
) -> Result<HashMap<u32, String>, SprigError> {
    Ok(sprig_auth::store::AuthStore
        .users_since(tx, None)
        .await?
        .into_iter()
        .map(|u| (u.user_id.0, u.name))
        .collect())
}

/// A `privileges` record, the fields the listing shows (the auth module
/// owns the row; unknown fields are ignored).
#[derive(Debug, serde::Deserialize)]
struct GrantRec {
    grantee_tenant_id: u64,
    grantee_kind: String,
    grantee_id: u32,
    level: String,
    db_id: u32,
    object_id: u64,
    #[serde(default)]
    column_name: String,
    actions: u64,
    restriction: u8,
    granted_by: Option<u32>,
    granted_by_tenant_id: u64,
    /// Microseconds since the epoch.
    granted_at: i64,
}

/// A `roles` record's id and name.
#[derive(Debug, serde::Deserialize)]
struct RoleName {
    role_id: u32,
    name: String,
}

/// Each object's name by id.
fn object_names(objects: Vec<sprig_catalog::ObjectRow>) -> HashMap<u64, String> {
    objects
        .into_iter()
        .map(|o| (o.object_id.0, o.name))
        .collect()
}

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
    let mut tx = records(core, caller).await?;
    // Grants made in this tenant to its own users and roles (grants from
    // ancestors to other tenants' grantees aren't this tenant's to show).
    let mut rows: Vec<GrantRec> = PRIVILEGES
        .rows::<GrantRec>(&mut *tx, &Pk::new())
        .await?
        .into_iter()
        .filter(|p| p.grantee_tenant_id == 0)
        .collect();
    rows.sort_by(|a, b| b.granted_at.cmp(&a.granted_at));
    rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    let users = user_names(&mut *tx).await?;
    let roles: HashMap<u32, String> = ROLES
        .rows::<RoleName>(&mut *tx, &Pk::new())
        .await?
        .into_iter()
        .map(|r| (r.role_id, r.name))
        .collect();
    let dbs: HashMap<u32, String> = CoreRecords::databases_since(&mut *tx, None)
        .await?
        .into_iter()
        .map(|d| (d.db_id.0, d.name))
        .collect();
    let mut ids: Vec<sprig_common::ids::ObjectId> = rows
        .iter()
        .filter(|p| p.object_id != 0)
        .map(|p| sprig_common::ids::ObjectId(p.object_id))
        .collect();
    ids.dedup();
    let objects = object_names(CoreRecords::objects_by_ids(&mut *tx, &ids).await?);
    drop(tx);
    let out: Vec<Value> = rows
        .into_iter()
        .map(|p| {
            let privileges: Vec<&str> = sprig_auth::authz::ActionSet(p.actions)
                .iter()
                .map(sprig_auth::authz::Action::command)
                .collect();
            let grantee = match p.grantee_kind.as_str() {
                "user" => users.get(&p.grantee_id).cloned(),
                "role" => roles.get(&p.grantee_id).cloned(),
                _ => None,
            };
            let by = p
                .granted_by
                .filter(|_| p.granted_by_tenant_id == 0)
                .and_then(|u| users.get(&u).cloned());
            json!({
                "grantee_kind": p.grantee_kind, "grantee": grantee, "level": p.level,
                "database": (p.db_id != 0).then(|| dbs.get(&p.db_id).cloned()).flatten(),
                "object": (p.object_id != 0).then(|| objects.get(&p.object_id).cloned()).flatten(),
                "column": (!p.column_name.is_empty()).then_some(p.column_name),
                "privileges": privileges, "restriction": p.restriction,
                "at_ms": p.granted_at / 1000, "by": by,
            })
        })
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
    fn only_the_newest_changes_are_kept() {
        let at = |s: u64| UNIX_EPOCH + std::time::Duration::from_secs(s);
        let change = |s: u64| Change {
            at: at(s),
            seq: 0,
            db: "d".into(),
            object: format!("t{s}"),
            kind: "table",
            version: 1,
            statement: "CREATE TABLE".into(),
            by: None,
        };
        let mut n = Newest::new(3);
        for s in [5, 1, 9, 3, 7, 8] {
            if n.takes(at(s)) {
                n.push(change(s));
            }
        }
        assert_eq!(n.heap.len(), 3);
        assert!(!n.takes(at(6)));
        assert!(n.takes(at(10)));
        let kept: Vec<String> = n.into_sorted().into_iter().map(|c| c.object).collect();
        assert_eq!(kept, ["t9", "t8", "t7"]);
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
