//! MongoDB collections stored on SprigDB's own storage engine.
//!
//! A database's collections live on the engine when the tenant's logical
//! database of that name is placed there (`storage.placement = 'engine'`),
//! or when the node puts whole tenants there:
//! `SPRIGDB_ENGINE_MONGO_TENANTS` (`all`, or tenant ids separated by
//! commas). Their commands then run on the node's engine
//! (`sprig_doc::docstore`) and never touch MariaDB; every other database
//! runs as before. If the engine can't open, those databases' commands are
//! refused, never answered from MariaDB.
//!
//! Cursors share the connection's cursor table: an engine cursor is a
//! [`CursorPlan`] over the store's cursor. Multi-document transactions on
//! engine databases are the store's own, by `lsid`.

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bson::{Bson, Document, doc};
use sprig_auth::authz::Action;
use sprig_doc::docstore::DocStore;
use sprig_doc::docstore::cmd::Reply;
use sprig_doc::engine::{CursorPlan, Ns};
use sprig_proto_mongo::errors::{MongoError, code};

use super::commands::owner;
use super::conn::Conn;
use super::cursors::CursorTable;

/// The variable naming whole tenants on the engine.
pub const TENANTS_VAR: &str = "SPRIGDB_ENGINE_MONGO_TENANTS";

/// Which tenants are wholly on the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tenants {
    /// Every tenant.
    All,
    /// These tenants.
    Only(BTreeSet<u64>),
}

impl Tenants {
    /// Parses `all` or a comma-separated id list (`None`: malformed).
    #[must_use]
    pub fn parse(v: &str) -> Option<Self> {
        let v = v.trim();
        if v.eq_ignore_ascii_case("all") {
            return Some(Self::All);
        }
        v.split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|x| x.trim().parse().ok())
            .collect::<Option<BTreeSet<u64>>>()
            .map(Self::Only)
    }

    fn has(&self, t: u64) -> bool {
        match self {
            Self::All => true,
            Self::Only(s) => s.contains(&t),
        }
    }
}

fn tenants() -> &'static Tenants {
    static T: OnceLock<Tenants> = OnceLock::new();
    T.get_or_init(|| {
        std::env::var(TENANTS_VAR)
            .ok()
            .and_then(|v| Tenants::parse(&v))
            .unwrap_or_else(|| Tenants::Only(BTreeSet::new()))
    })
}

/// The node's document store, opened on first use with its TTL sweeper
/// and its idle-transaction reaper.
fn store() -> Option<Arc<DocStore>> {
    static S: OnceLock<Option<Arc<DocStore>>> = OnceLock::new();
    S.get_or_init(|| match crate::node_engine::engine() {
        Ok(e) => {
            let s = Arc::new(DocStore::new(Arc::new(e)));
            drop(s.spawn_ttl(Duration::from_secs(60)));
            let r = Arc::clone(&s);
            drop(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tick.tick().await;
                    let _ = r.reap_transactions(Duration::from_secs(60));
                }
            }));
            tracing::info!("mongodb collections on the storage engine");
            Some(s)
        }
        Err(e) => {
            tracing::error!(error = %e, "mongodb collections on the storage engine not started");
            None
        }
    })
    .clone()
}

fn unavailable() -> MongoError {
    MongoError::new(
        code::INTERNAL_ERROR,
        "this database is stored on the SprigDB engine, which failed to start on this node; see the node's log",
    )
}

/// Commands the store answers.
const DATA: [&str; 21] = [
    "find",
    "insert",
    "update",
    "delete",
    "findAndModify",
    "findandmodify",
    "aggregate",
    "count",
    "distinct",
    "create",
    "drop",
    "dropDatabase",
    "listCollections",
    "listIndexes",
    "createIndexes",
    "dropIndexes",
    "deleteIndexes",
    "collMod",
    "renameCollection",
    "explain",
    "dbStats",
];

/// The privilege a command needs.
fn action_of(name: &str) -> Action {
    match name {
        "insert" => Action::Insert,
        "update" | "findAndModify" | "findandmodify" | "collMod" => Action::Update,
        "delete" => Action::Delete,
        "create" | "renameCollection" => Action::Create,
        "drop" | "dropDatabase" | "dropIndexes" | "deleteIndexes" => Action::Drop,
        "createIndexes" => Action::Index,
        _ => Action::Select,
    }
}

/// Whether `db` of the connection's tenant is on the engine.
fn on_engine(c: &mut Conn, db: &str) -> Result<bool, MongoError> {
    let tenant = owner(c).tenant;
    if tenant == 0 || db == "admin" || db == "local" || db == "config" {
        return Ok(false);
    }
    if tenants().has(tenant) {
        return Ok(true);
    }
    let sql = c.sql()?;
    Ok(sql.engine_database(db))
}

/// Answers the command when it belongs to the engine store: `None` sends
/// it down the SQL path.
pub(super) async fn route(
    c: &mut Conn,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<Result<Document, MongoError>> {
    let who = owner(c);
    match name {
        "getMore" => return get_more(c, body).await,
        // The cursor table answers it, whatever the database.
        "killCursors" => return None,
        "endSessions" | "killSessions" => {
            end_sessions(c, name, body).await;
            return None;
        }
        // Namespaces of engine databases can't be written through the SQL
        // path's bulkWrite.
        "bulkWrite" => return bulk_write_check(c, body),
        "commitTransaction" | "abortTransaction" => {
            let lsid = match body.get_document("lsid").ok()?.get("id")? {
                Bson::Binary(b) => b.bytes.clone(),
                _ => return None,
            };
            let s = store()?;
            let n = body.get("txnNumber").and_then(sprig_doc::filter::as_i64)?;
            if !s.has_txn_number(who.tenant, &lsid, n).await {
                return None;
            }
            return Some(run(c, &s, name, body, db).await);
        }
        "listDatabases" => {
            if who.tenant == 0 || !tenants().has(who.tenant) {
                return None;
            }
            let Some(s) = store() else {
                return Some(Err(unavailable()));
            };
            return Some(run(c, &s, name, body, db).await);
        }
        _ => {}
    }
    // `renameCollection` is an admin command naming its databases.
    let target_db = if name == "renameCollection" {
        body.get_str("renameCollection")
            .ok()
            .and_then(|n| n.split_once('.'))
            .map_or(db, |(d, _)| d)
    } else {
        db
    };
    match on_engine(c, target_db) {
        Ok(false) => return None,
        Ok(true) => {}
        Err(e) => return Some(Err(e)),
    }
    // A rename stays on the engine (moving a collection to a database on
    // the old path would hide it).
    if name == "renameCollection"
        && let Some((to_db, _)) = body.get_str("to").ok().and_then(|n| n.split_once('.'))
    {
        match on_engine(c, to_db) {
            Ok(true) => {}
            Ok(false) => {
                return Some(Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    format!(
                        "cannot rename a collection from a database stored on the SprigDB engine to '{to_db}', which is not"
                    ),
                )));
            }
            Err(e) => return Some(Err(e)),
        }
    }
    let Some(s) = store() else {
        return Some(Err(unavailable()));
    };
    if !DATA.contains(&name) && body.get_bool("autocommit") != Ok(false) {
        return Some(Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            format!("{name} is not supported on databases stored on the SprigDB engine yet"),
        )));
    }
    if !tenants().has(who.tenant) {
        match denied(c, name, body) {
            Ok(false) => {}
            Ok(true) => {
                return Some(Err(MongoError::new(
                    code::UNAUTHORIZED,
                    format!("not authorized on {target_db} to execute command {name}"),
                )));
            }
            Err(e) => return Some(Err(e)),
        }
    }
    Some(run(c, &s, name, body, target_db).await)
}

/// `bulkWrite` naming a namespace of an engine database is refused: the SQL
/// path's bulkWrite would write it to MariaDB.
fn bulk_write_check(c: &mut Conn, body: &Document) -> Option<Result<Document, MongoError>> {
    let dbs: Vec<String> = body
        .get_array("nsInfo")
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_document()?.get_str("ns").ok())
                .filter_map(|ns| ns.split_once('.').map(|(d, _)| d.to_owned()))
                .collect()
        })
        .unwrap_or_default();
    for d in dbs {
        match on_engine(c, &d) {
            Ok(false) => {}
            Ok(true) => {
                return Some(Err(MongoError::new(
                    code::SPRIG_DOC_FEATURE_UNSUPPORTED,
                    "bulkWrite is not supported on databases stored on the SprigDB engine yet; use insert, update and delete",
                )));
            }
            Err(e) => return Some(Err(e)),
        }
    }
    None
}

/// Whether the session lacks a privilege the command needs: tenant-wide
/// grants, as far as the executor can tell; an answer it can't give allows
/// reads only.
fn denied(c: &mut Conn, name: &str, body: &Document) -> Result<bool, MongoError> {
    let mut needed = vec![action_of(name)];
    let writes_out = body.get_array("pipeline").is_ok_and(|p| {
        p.iter().any(|s| {
            s.as_document()
                .is_some_and(|d| d.contains_key("$out") || d.contains_key("$merge"))
        })
    });
    if name == "aggregate" && writes_out {
        needed.extend([Action::Insert, Action::Update, Action::Delete]);
    }
    if name == "renameCollection" && body.get_bool("dropTarget") == Ok(true) {
        needed.push(Action::Drop);
    }
    let sql = c.sql()?;
    Ok(needed.iter().any(|a| match sql.holds_tenant_privilege(*a) {
        Some(held) => !held,
        None => *a != Action::Select,
    }))
}

/// Ends the sessions an `endSessions` or `killSessions` names: their
/// open store transactions roll back.
pub(super) async fn end_sessions(c: &mut Conn, name: &str, body: &Document) {
    let tenant = owner(c).tenant;
    if let (Some(s), Ok(a)) = (store(), body.get_array(name)) {
        for x in a {
            if let Some(Bson::Binary(b)) = x.as_document().and_then(|d| d.get("id")) {
                s.end_session(tenant, &b.bytes).await;
            }
        }
    }
}

async fn run(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let who = owner(c);
    if name == "dbStats" {
        let n = s.list(who.tenant, Some(db))?.len();
        return Ok(doc! {
            "db": db, "collections": i32::try_from(n).unwrap_or(i32::MAX), "views": 0,
            "objects": 0i64, "avgObjSize": 0.0, "dataSize": 0.0, "storageSize": 0.0,
            "indexes": 0i64, "indexSize": 0.0, "totalSize": 0.0, "scaleFactor": 1, "ok": 1.0,
        });
    }
    let reply = s.command(who.tenant, &who.user, db, name, body).await?;
    match reply {
        Reply::Doc(d) => Ok(d),
        Reply::Cursor { ns, first, rest } => {
            let Some(rest) = rest else {
                return Ok(Reply::Cursor {
                    ns,
                    first,
                    rest: None,
                }
                .into_doc(0));
            };
            let lsid = match body.get_document("lsid").ok().and_then(|l| l.get("id")) {
                Some(Bson::Binary(b)) => Some(b.bytes.clone()),
                _ => None,
            };
            let bs = body
                .get_document("cursor")
                .ok()
                .and_then(|cd| cd.get("batchSize"))
                .or_else(|| body.get("batchSize"))
                .and_then(sprig_doc::filter::as_i64)
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| *n > 0);
            let (pdb, pcoll) = ns.split_once('.').unwrap_or((db, ""));
            let plan = CursorPlan::from_store(
                Ns {
                    db: pdb.to_owned(),
                    coll: pcoll.to_owned(),
                },
                rest,
            );
            let id = c.shared.cursors.open(
                plan,
                ns.clone(),
                who,
                lsid,
                bs,
                body.get_bool("noCursorTimeout").unwrap_or(false),
                c.shared.settings().max_cursors,
            )?;
            Ok(doc! {"cursor": {"firstBatch": first, "id": id, "ns": ns}, "ok": 1.0})
        }
    }
}

/// `getMore` on an engine cursor (`None`: not one).
async fn get_more(c: &mut Conn, body: &Document) -> Option<Result<Document, MongoError>> {
    let id = sprig_doc::filter::as_i64(body.get("getMore")?)?;
    let who = owner(c);
    let cur = c.shared.cursors.get(id, &who).ok()?;
    let mut slot = cur.lock().await;
    let cursor = slot.as_mut()?;
    cursor.plan.store_mut()?;
    let st = store()?;
    let lsid = match body.get_document("lsid").ok().and_then(|l| l.get("id")) {
        Some(Bson::Binary(b)) => Some(b.bytes.clone()),
        _ => None,
    };
    if cursor.lsid.is_some() && lsid.is_some() && cursor.lsid != lsid {
        return Some(Err(MongoError::new(
            code::UNAUTHORIZED,
            "Cannot run getMore on cursor which was created in a different session",
        )));
    }
    if body.contains_key("maxTimeMS") {
        return Some(Err(MongoError::new(
            code::BAD_VALUE,
            "cannot set maxTimeMS on getMore command for a non-awaitData cursor",
        )));
    }
    CursorTable::touch(cursor);
    let want = body
        .get("batchSize")
        .and_then(sprig_doc::filter::as_i64)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .or(cursor.batch_size)
        .unwrap_or(1_000_000);
    let ns = cursor.ns.clone();
    let sc = cursor.plan.store_mut()?;
    let res = sc.next_batch(&st, want).await;
    let done = sc.exhausted();
    drop(slot);
    let batch = match res {
        Ok(b) => b,
        Err(e) => {
            c.shared.cursors.remove(id);
            return Some(Err(e));
        }
    };
    let rid = if done {
        c.shared.cursors.remove(id);
        0
    } else {
        id
    };
    Some(Ok(
        doc! {"cursor": {"nextBatch": batch, "id": rid, "ns": ns}, "ok": 1.0},
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_lists_parse() {
        assert_eq!(Tenants::parse("all"), Some(Tenants::All));
        let t = Tenants::parse("1, 2").unwrap();
        assert!(t.has(2) && !t.has(3));
        assert_eq!(Tenants::parse("x"), None);
        assert!(action_of("insert") == Action::Insert);
    }
}
