//! MongoDB collections stored on SprigDB's own storage engine.
//!
//! Every tenant's collections live on the engine: their commands run on
//! the node's engine (`sprig_doc::docstore`) and never touch MariaDB. If
//! the engine can't open, the commands are refused, never answered from
//! MariaDB.
//!
//! Cursors share the connection's cursor table: an engine cursor is a
//! [`CursorPlan`] over the store's cursor. Multi-document transactions on
//! engine databases are the store's own, by `lsid`.

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

/// The node's document store, opened on first use with its TTL sweeper
/// and its idle-transaction reaper.
pub(crate) fn store() -> Option<Arc<DocStore>> {
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

/// A kept snapshot: a read-only store transaction whose reads all see
/// one point in time, its owner, and when it was last read.
struct Snapshot {
    txn: tokio::sync::Mutex<sprig_doc::docstore::DocTxn>,
    user: String,
    used: std::sync::Mutex<std::time::Instant>,
}

type Snapshots = std::sync::Mutex<std::collections::HashMap<(u64, u64), Arc<Snapshot>>>;

/// The node's kept snapshots by tenant and time (`atClusterTime`).
fn snapshots() -> &'static Snapshots {
    static S: OnceLock<Snapshots> = OnceLock::new();
    S.get_or_init(Snapshots::default)
}

fn snapshot_key(t: bson::Timestamp) -> u64 {
    (u64::from(t.time) << 32) | u64::from(t.increment)
}

/// A read with `readConcern: {level: "snapshot"}` outside a transaction:
/// the session's first one opens a read-only store transaction named by
/// the cluster time, which the reply carries as `atClusterTime`; later
/// reads that send it back read in that transaction, so writes committed
/// since are invisible. Idle snapshots close after five minutes
/// (MongoDB's history window); a tenant keeps at most 64.
async fn snapshot_read(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let who = owner(c);
    let idle = super::snapshot::IDLE;
    let (at, snap) = {
        let mut m = snapshots()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        m.retain(|_, v| v.used.lock().map_or(true, |u| u.elapsed() < idle));
        if let Some(at) = super::snapshot::at_of(body) {
            let Some(snap) = m
                .get(&(who.tenant, snapshot_key(at)))
                .filter(|x| x.user == who.user)
                .cloned()
            else {
                return Err(MongoError::new(
                    super::snapshot::SNAPSHOT_TOO_OLD,
                    "Read timestamp is older than the oldest available timestamp on this node",
                ));
            };
            (at, snap)
        } else {
            let at = super::conn::cluster_time();
            let mine: Vec<(u64, u64)> = m.keys().filter(|k| k.0 == who.tenant).copied().collect();
            if mine.len() >= super::snapshot::PER_TENANT
                && let Some(oldest) = mine.iter().min_by_key(|k| k.1)
            {
                m.remove(oldest);
            }
            let snap = Arc::new(Snapshot {
                txn: tokio::sync::Mutex::new(s.begin(true)),
                user: who.user.clone(),
                used: std::sync::Mutex::new(std::time::Instant::now()),
            });
            m.insert((who.tenant, snapshot_key(at)), Arc::clone(&snap));
            (at, snap)
        }
    };
    if let Ok(mut u) = snap.used.lock() {
        *u = std::time::Instant::now();
    }
    let guard = c.sql().ok().and_then(|q| q.row_guard());
    let mut t = snap.txn.lock().await;
    let reply = sprig_doc::docstore::guard::scope(
        guard,
        s.snapshot_read(who.tenant, db, name, body, &mut t),
    )
    .await?;
    drop(t);
    let mut d = reply_doc(c, reply, body, db)?;
    super::snapshot::stamp(&mut d, at);
    Ok(d)
}

/// Commands the store answers.
const DATA: [&str; 22] = [
    "collStats",
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

/// Whether `db` of the connection's tenant is on the engine (every tenant
/// database is; the `Result` is kept for the callers' error paths).
#[allow(clippy::unnecessary_wraps)]
pub(super) fn on_engine(c: &mut Conn, db: &str) -> Result<bool, MongoError> {
    let tenant = owner(c).tenant;
    // Every tenant database is on the engine.
    Ok(!(tenant == 0 || db == "admin" || db == "local" || db == "config"))
}

/// Answers the command when it belongs to the engine store: `None` sends
/// it down the SQL path.
#[allow(clippy::too_many_lines)] // one step per routing rule, in order
pub(super) async fn route(
    c: &mut Conn,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<Result<Document, MongoError>> {
    let who = owner(c);
    if let Some(r) = snapshot_route(c, name, body, db).await {
        return Some(r);
    }
    match name {
        "getMore" => return get_more(c, body).await,
        // The cursor table answers it, whatever the database.
        "killCursors" => return None,
        "endSessions" | "killSessions" => {
            end_sessions(c, name, body).await;
            return None;
        }
        // The client-level bulkWrite runs on the store when its
        // namespaces are engine databases.
        "bulkWrite" => return bulk_write(c, body, db).await,
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
            if who.tenant == 0 {
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
    // Change streams open on the command path, which reads engine
    // collections' changes from the change log.
    if name == "aggregate"
        && body
            .get_array("pipeline")
            .ok()
            .and_then(|p| p.first())
            .and_then(Bson::as_document)
            .is_some_and(|st| st.contains_key("$changeStream"))
    {
        return None;
    }
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
    // Row-level security's commands: SQL's policy statements on the
    // collection's catalog object.
    if super::rls::handles(name) {
        return Some(policy_command(c, &s, name, body, db).await);
    }
    if !DATA.contains(&name) && body.get_bool("autocommit") != Ok(false) {
        return Some(Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            format!("{name} is not supported on databases stored on the SprigDB engine yet"),
        )));
    }
    if let Some(e) = linearizable_write(name, body) {
        return Some(Err(e));
    }
    match denied(c, name, body, target_db) {
        Ok(false) => {}
        Ok(true) => {
            return Some(Err(MongoError::new(
                code::UNAUTHORIZED,
                format!("not authorized on {target_db} to execute command {name}"),
            )));
        }
        Err(e) => return Some(Err(e)),
    }
    if let Some(e) = guarded_refusal(c, &s, name, body, target_db) {
        return Some(Err(e));
    }
    Some(run(c, &s, name, body, target_db).await)
}

/// `sprigCreatePolicy`, `sprigDropPolicy` and `sprigRowSecurity` on a
/// collection of the store: its catalog object made first if the catalog
/// doesn't have it yet, then the SQL policy statements on it
/// (`super::rls::engine_command`), with SQL's checks (`ALTER` on the
/// collection, 1304, 1305, 39364).
async fn policy_command(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
    let who = owner(c);
    let coll = body.get_str(name).map_err(|_| {
        MongoError::new(
            code::FAILED_TO_PARSE,
            format!("{name} needs a collection name"),
        )
    })?;
    let object = match s.coll(who.tenant, &sprig_doc::docstore::Name::new(db, coll))? {
        None => {
            return Err(MongoError::new(
                code::NAMESPACE_NOT_FOUND,
                format!("ns does not exist: {db}.{coll}"),
            ));
        }
        Some(m) if m.meta.view().is_some() => {
            return Err(MongoError::new(
                166,
                format!("Namespace {db}.{coll} is a view, not a collection"),
            ));
        }
        Some(m) => m.space.object,
    };
    let Some(svc) = c.shared.d.tenants.clone() else {
        return Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "row-level security needs the tenant catalog",
        ));
    };
    let internal =
        |e: sprig_common::SprigError| MongoError::new(code::INTERNAL_ERROR, e.to_string());
    super::collcat::ensure(&svc, s, who.tenant)
        .await
        .map_err(internal)?;
    // The name must be the collection's own object: a table or view of
    // the same name would take the policy instead.
    let tenant = sprig_common::ids::TenantId(who.tenant);
    let cat = svc
        .cache
        .get(tenant, sprig_catalog::Freshness::Latest)
        .await
        .map_err(internal)?;
    let lctn = super::collcat::lctn(&svc, &cat);
    let own = cat
        .database(&sprig_catalog::ddl::database_key(db, lctn))
        .filter(|d| d.state == DbState::Active)
        .and_then(|d| {
            cat.object(
                d.db_id,
                Namespace::Relation,
                &sprig_catalog::ddl::object_key(ObjectKind::Collection, coll, lctn),
            )
        })
        .is_some_and(|o| {
            o.state == ObjectState::Active
                && o.kind == ObjectKind::Collection
                && o.object_id.0 == u64::from(object)
        });
    if !own {
        return Err(MongoError::new(
            sprig_doc::docstore::guard::RLS_UNSUPPORTED,
            format!(
                "ER_SPRIG_RLS_UNSUPPORTED: {db}.{coll} can't take row-level security policies: \
                 another relation of the database (a table or a view) has its name"
            ),
        ));
    }
    super::rls::engine_command(c.sql()?, name, body, db).await
}

/// Whether the row guard binds collection `db.coll` (a view: the
/// collection it reads).
fn guard_binds(
    s: &DocStore,
    tenant: u64,
    g: &sprig_doc::docstore::guard::RowGuard,
    db: &str,
    coll: &str,
) -> bool {
    let mut name = coll.to_owned();
    for _ in 0..16 {
        let Ok(Some(m)) = s.coll(tenant, &sprig_doc::docstore::Name::new(db, &name)) else {
            return false;
        };
        match m.meta.view() {
            Some((on, _)) => name = on,
            None => return g.binds(m.space.object),
        }
    }
    false
}

/// 39366 for a command row-level security can't filter, on a collection
/// whose policies bind the session, as SQL refuses its own forms to bound
/// users: `explain` and statistics (they count hidden documents), index
/// and collection DDL other than `drop`, and `$out`/`$merge` into such a
/// collection.
fn guarded_refusal(
    c: &mut Conn,
    s: &DocStore,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<MongoError> {
    let g = c.sql().ok()?.row_guard()?;
    let tenant = owner(c).tenant;
    let binds = |d: &str, coll: &str| guard_binds(s, tenant, &g, d, coll);
    let refuse = |what: &str, coll: &str| {
        Some(MongoError::new(
            sprig_doc::docstore::guard::RLS_UNSUPPORTED,
            format!(
                "ER_SPRIG_RLS_UNSUPPORTED: {what} isn't supported on {db}.{coll}, whose \
                 row-level security policies bind this user"
            ),
        ))
    };
    let named = |k: &str| body.get_str(k).ok().map(str::to_owned);
    match name {
        "explain" => {
            let inner = body.get_document("explain").ok()?;
            let (_, v) = inner.iter().next()?;
            let coll = v.as_str()?;
            if binds(db, coll) {
                return refuse("explain", coll);
            }
        }
        "collStats" | "createIndexes" | "dropIndexes" | "deleteIndexes" | "collMod" => {
            let coll = named(name)?;
            if binds(db, &coll) {
                return refuse(name, &coll);
            }
        }
        "renameCollection" => {
            let (d, coll) = named(name)?
                .split_once('.')
                .map(|(d, c)| (d.to_owned(), c.to_owned()))?;
            if binds(&d, &coll) {
                return refuse(name, &coll);
            }
        }
        "dbStats" => {
            let all = s.list(tenant, Some(db)).ok()?;
            if let Some(m) = all.iter().find(|m| g.binds(m.space.object)) {
                return refuse(name, &m.meta.name);
            }
        }
        "aggregate" => {
            let coll = named("aggregate");
            for st in body
                .get_array("pipeline")
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                let Some(st) = st.as_document() else { continue };
                if let Some(coll) = &coll
                    && (st.contains_key("$collStats") || st.contains_key("$indexStats"))
                    && binds(db, coll)
                {
                    return refuse("$collStats and $indexStats", coll);
                }
                let mut out = Vec::new();
                stage_needs(st, db, &mut out);
                for (a, d, coll) in out {
                    if let (Action::Insert, Some(coll)) = (a, coll)
                        && binds(&d, &coll)
                    {
                        return refuse("$out and $merge", &coll);
                    }
                }
            }
        }
        _ => {}
    }
    None
}

/// 72 for an aggregation that writes (`$out`, `$merge`) with a
/// `linearizable` read concern, as MongoDB refuses it.
fn linearizable_write(name: &str, body: &Document) -> Option<MongoError> {
    if name != "aggregate"
        || body
            .get_document("readConcern")
            .ok()
            .and_then(|r| r.get_str("level").ok())
            != Some("linearizable")
    {
        return None;
    }
    let stage = body.get_array("pipeline").ok()?.iter().find_map(|s| {
        let d = s.as_document()?;
        ["$out", "$merge"].into_iter().find(|k| d.contains_key(*k))
    })?;
    Some(MongoError::new(
        code::INVALID_OPTIONS,
        format!(
            "Command aggregate does not support {{ readConcern: {{ level: \"linearizable\" }} }} :: caused by :: {stage} cannot be used with a 'linearizable' read concern level"
        ),
    ))
}

/// A command with `readConcern: {level: "snapshot"}` outside a
/// transaction: reads of engine databases run at a kept snapshot; other
/// commands are refused with 72 (`None`: not such a command).
async fn snapshot_route(
    c: &mut Conn,
    name: &str,
    body: &Document,
    db: &str,
) -> Option<Result<Document, MongoError>> {
    let who = owner(c);
    // Snapshot reads outside transactions: only reads may ask for them.
    if who.tenant != 0
        && !matches!(
            name,
            "getMore"
                | "killCursors"
                | "endSessions"
                | "killSessions"
                | "commitTransaction"
                | "abortTransaction"
        )
        && body.get_bool("autocommit") != Ok(false)
        && body
            .get_document("readConcern")
            .ok()
            .and_then(|r| r.get_str("level").ok())
            == Some("snapshot")
    {
        if !super::snapshot::reads(name, body) {
            return Some(Err(MongoError::new(
                code::INVALID_OPTIONS,
                format!(
                    "readConcern level snapshot is not supported for {name} outside multi-document transactions"
                ),
            )));
        }
        if matches!(on_engine(c, db), Ok(true)) {
            let Some(s) = store() else {
                return Some(Err(unavailable()));
            };
            match denied(c, name, body, db) {
                Ok(false) => {}
                Ok(true) => {
                    return Some(Err(MongoError::new(
                        code::UNAUTHORIZED,
                        format!("not authorized on {db} to execute command {name}"),
                    )));
                }
                Err(e) => return Some(Err(e)),
            }
            return Some(snapshot_read(c, &s, name, body, db).await);
        }
    }
    None
}

/// `bulkWrite` on engine databases runs on the store (`None`: none of its
/// namespaces is on the engine). A command mixing engine databases with
/// others is refused.
async fn bulk_write(
    c: &mut Conn,
    body: &Document,
    db: &str,
) -> Option<Result<Document, MongoError>> {
    let dbs: std::collections::BTreeSet<String> = body
        .get_array("nsInfo")
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_document()?.get_str("ns").ok())
                .map(|ns| ns.split_once('.').map_or(ns, |(d, _)| d).to_owned())
                .collect()
        })
        .unwrap_or_default();
    let (mut on, mut off) = (0, 0);
    for d in &dbs {
        match on_engine(c, d) {
            Ok(true) => on += 1,
            Ok(false) => off += 1,
            Err(e) => return Some(Err(e)),
        }
    }
    if on == 0 {
        return None;
    }
    if off > 0 {
        return Some(Err(MongoError::new(
            code::SPRIG_DOC_FEATURE_UNSUPPORTED,
            "bulkWrite can't write databases stored on the SprigDB engine and other databases in one command",
        )));
    }
    let Some(s) = store() else {
        return Some(Err(unavailable()));
    };
    match denied(c, "bulkWrite", body, db) {
        Ok(false) => {}
        Ok(true) => {
            return Some(Err(MongoError::new(
                code::UNAUTHORIZED,
                "not authorized to execute command bulkWrite",
            )));
        }
        Err(e) => return Some(Err(e)),
    }
    Some(run(c, &s, "bulkWrite", body, db).await)
}

/// The collections an aggregation stage reads (`$lookup`, `$graphLookup`,
/// `$unionWith`) or writes (`$out`, `$merge`), as needs.
fn stage_needs(st: &Document, db: &str, out: &mut Vec<(Action, String, Option<String>)>) {
    for (k, v) in st {
        // Collections a stage reads or writes.
        let (named, writes) = match (k.as_str(), v) {
            ("$lookup" | "$graphLookup", Bson::Document(d)) => (
                d.get_str("from")
                    .ok()
                    .map(|f| (db.to_owned(), f.to_owned())),
                false,
            ),
            ("$unionWith", Bson::String(f)) => (Some((db.to_owned(), f.clone())), false),
            ("$unionWith", Bson::Document(d)) => (
                d.get_str("coll")
                    .ok()
                    .map(|f| (db.to_owned(), f.to_owned())),
                false,
            ),
            ("$out" | "$merge", Bson::String(f)) => (Some((db.to_owned(), f.clone())), true),
            ("$out" | "$merge", Bson::Document(d)) => {
                let into = d.get("into").or_else(|| d.get("coll"));
                let target = match into {
                    Some(Bson::String(f)) => {
                        Some((d.get_str("db").unwrap_or(db).to_owned(), f.clone()))
                    }
                    Some(Bson::Document(i)) => i.get_str("coll").ok().map(|f| {
                        (
                            i.get_str("db")
                                .or_else(|_| d.get_str("db"))
                                .unwrap_or(db)
                                .to_owned(),
                            f.to_owned(),
                        )
                    }),
                    _ => None,
                };
                (target, true)
            }
            _ => (None, false),
        };
        let Some((d, c)) = named else { continue };
        if writes {
            for a in [Action::Insert, Action::Update, Action::Delete] {
                out.push((a, d.clone(), Some(c.clone())));
            }
        } else {
            out.push((Action::Select, d, Some(c)));
        }
    }
}

/// Whether the session lacks a privilege the command needs: tenant-wide
/// grants, as far as the executor can tell; an answer it can't give allows
/// reads only.
/// What a command needs: `(action, database, collection)` triples, the
/// collection `None` for database-wide commands.
fn needs(name: &str, body: &Document, db: &str) -> Vec<(Action, String, Option<String>)> {
    let coll = body.get_str(name).ok().map(str::to_owned);
    let mut out = Vec::new();
    match name {
        "bulkWrite" => {
            // Each op's kind on its own namespace.
            let ns: Vec<(String, Option<String>)> = body
                .get_array("nsInfo")
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                .filter_map(|x| x.as_document()?.get_str("ns").ok())
                .map(|ns| match ns.split_once('.') {
                    Some((d, c)) => (d.to_owned(), Some(c.to_owned())),
                    None => (ns.to_owned(), None),
                })
                .collect();
            for op in body.get_array("ops").map(Vec::as_slice).unwrap_or_default() {
                let Some(op) = op.as_document() else { continue };
                let Some((kind, i)) = op.iter().next() else {
                    continue;
                };
                let target = sprig_doc::filter::as_i64(i)
                    .and_then(|i| usize::try_from(i).ok())
                    .and_then(|i| ns.get(i))
                    .cloned()
                    .unwrap_or_else(|| (db.to_owned(), None));
                out.push((action_of(kind), target.0.clone(), target.1.clone()));
                if kind == "update" && op.get_bool("upsert") == Ok(true) {
                    out.push((Action::Insert, target.0, target.1));
                }
            }
        }
        "renameCollection" => {
            // Both namespaces are full names.
            let split = |k: &str| {
                body.get_str(k)
                    .ok()
                    .and_then(|n| n.split_once('.'))
                    .map(|(d, c)| (d.to_owned(), c.to_owned()))
            };
            if let Some((d, c)) = split("renameCollection") {
                out.push((Action::Select, d.clone(), Some(c.clone())));
                out.push((Action::Drop, d, Some(c)));
            }
            if let Some((d, c)) = split("to") {
                out.push((Action::Create, d.clone(), Some(c.clone())));
                out.push((Action::Insert, d.clone(), Some(c.clone())));
                if body.get_bool("dropTarget") == Ok(true) {
                    out.push((Action::Drop, d, Some(c)));
                }
            }
        }
        // What the explained command needs.
        "explain" => {
            if let Ok(inner) = body.get_document("explain")
                && let Some(n) = inner.keys().next()
                && n != "explain"
            {
                return needs(n, inner, db);
            }
            out.push((Action::Select, db.to_owned(), None));
        }
        _ => out.push((action_of(name), db.to_owned(), coll)),
    }
    if name == "aggregate" {
        for st in body
            .get_array("pipeline")
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            if let Some(st) = st.as_document() {
                stage_needs(st, db, &mut out);
            }
        }
    }
    out
}

/// Whether the session lacks a privilege the command needs: each on its
/// database, or on its collection by name (MongoDB's per-database and
/// per-collection grants). An executor that can't tell allows reads only.
fn denied(c: &mut Conn, name: &str, body: &Document, db: &str) -> Result<bool, MongoError> {
    let needed = needs(name, body, db);
    let sql = c.sql()?;
    Ok(needed.iter().any(
        |(a, d, coll)| match sql.holds_privilege_on(*a, d, coll.as_deref()) {
            Some(held) => !held,
            None => *a != Action::Select,
        },
    ))
}

/// Aborts the open store transaction of session `lsid` (its slot stays,
/// so a later commit answers `NoSuchTransaction`).
pub(super) async fn abort_session(c: &mut Conn, lsid: &[u8]) {
    let who = owner(c);
    if who.tenant != 0
        && let Some(s) = store()
    {
        s.abort_session_txn(who.tenant, lsid).await;
    }
}

/// `killAllSessions`: the caller's open store transactions roll back.
pub(super) async fn kill_all(c: &mut Conn) {
    let who = owner(c);
    if who.tenant != 0
        && let Some(s) = store()
    {
        let _ = s.kill_sessions_of(who.tenant, &who.user).await;
    }
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

/// Commands a write concern applies to.
const CONCERNED: [&str; 15] = [
    "insert",
    "update",
    "delete",
    "findAndModify",
    "findandmodify",
    "bulkWrite",
    "commitTransaction",
    "abortTransaction",
    "create",
    "createIndexes",
    "drop",
    "dropDatabase",
    "dropIndexes",
    "collMod",
    "renameCollection",
];

/// The nodes that hold the data: the replicas listed for Raft, or this
/// one.
fn data_bearing_nodes() -> i64 {
    let set = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
    match (
        set(crate::node_engine::RAFT_ID_VAR),
        set(crate::node_engine::RAFT_PEERS_VAR),
    ) {
        (Some(_), Some(p)) => {
            i64::try_from(p.split(',').filter(|x| !x.trim().is_empty()).count()).unwrap_or(1)
        }
        _ => 1,
    }
}

/// The write concern error of a write whose `writeConcern` no deployment
/// of this size can satisfy, as MongoDB reports it after applying the
/// write: 100 for more acknowledgements than there are nodes, 79 for an
/// unknown tag.
fn unsatisfiable(name: &str, body: &Document) -> Option<Document> {
    if !CONCERNED.contains(&name) {
        return None;
    }
    let w = body.get_document("writeConcern").ok()?.get("w")?;
    match w {
        Bson::String(t) if t != "majority" => Some(doc! {
            "code": 79,
            "codeName": "UnknownReplWriteConcern",
            "errmsg": format!("No write concern mode named '{t}' found in replica set configuration"),
        }),
        other => {
            let n = sprig_doc::filter::as_i64(other)?;
            (n > data_bearing_nodes()).then(|| {
                doc! {
                    "code": 100,
                    "codeName": "UnsatisfiableWriteConcern",
                    "errmsg": "Not enough data-bearing nodes",
                }
            })
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
    let r = run_store(c, s, name, body, db).await.map(|mut d| {
        if let Some(w) = unsatisfiable(name, body) {
            d.insert("writeConcernError", w);
        }
        d
    });
    // Inside a transaction every reply carries a recovery token, as a
    // `mongos` returns its own (drivers send it back with the commit).
    if body.get_bool("autocommit") == Ok(false)
        && !matches!(name, "commitTransaction" | "abortTransaction")
    {
        let who = owner(c);
        let token = super::txn::token(
            who.tenant,
            &c.shared.d.gateway_id,
            &std::iter::once(db.to_owned()).collect(),
        );
        return r.map(|mut d| {
            d.insert("recoveryToken", token);
            d
        });
    }
    r
}

async fn run_store(
    c: &mut Conn,
    s: &Arc<DocStore>,
    name: &str,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let who = owner(c);
    // The session's row-level security policies on collections.
    let guard = c.sql().ok().and_then(|q| q.row_guard());
    // A name that is an SQL table, or a `$lookup` into one.
    let tables = sprig_doc::docstore::guard::scope(
        guard.clone(),
        super::tables::command(c, s, name, body, db),
    )
    .await;
    let reply = match tables {
        Some(r) => r,
        None => {
            sprig_doc::docstore::guard::scope(
                guard,
                s.command(who.tenant, &who.user, db, name, body),
            )
            .await
        }
    };
    // The catalog follows collections this command made, dropped or
    // renamed.
    if let Some(svc) = c.shared.d.tenants.clone() {
        super::collcat::touch(&svc, s, who.tenant);
    }
    reply_doc(c, reply?, body, db)
}

/// The store's reply as the command's: a cursor that isn't exhausted goes
/// into the connection's cursor table.
fn reply_doc(
    c: &mut Conn,
    reply: Reply,
    body: &Document,
    db: &str,
) -> Result<Document, MongoError> {
    let who = owner(c);
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
    let guard = c.sql().ok().and_then(|q| q.row_guard());
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
    let res = sprig_doc::docstore::guard::scope(guard, sc.next_batch(&st, want)).await;
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
    fn actions() {
        assert!(action_of("insert") == Action::Insert);
    }

    #[test]
    fn needs_name_each_namespace() {
        let n = needs("find", &doc! {"find": "c"}, "d");
        assert_eq!(n, [(Action::Select, "d".to_owned(), Some("c".to_owned()))]);
        let n = needs(
            "aggregate",
            &doc! {"aggregate": "c", "pipeline": [{"$lookup": {"from": "f", "as": "x"}}, {"$out": "o"}]},
            "d",
        );
        assert!(n.contains(&(Action::Select, "d".to_owned(), Some("f".to_owned()))));
        assert!(n.contains(&(Action::Insert, "d".to_owned(), Some("o".to_owned()))));
        let n = needs(
            "bulkWrite",
            &doc! {"bulkWrite": 1, "ops": [{"insert": 1, "document": {}}, {"update": 0, "filter": {}, "updateMods": {}, "upsert": true}],
            "nsInfo": [{"ns": "a.x"}, {"ns": "b.y"}]},
            "admin",
        );
        assert_eq!(
            n,
            [
                (Action::Insert, "b".to_owned(), Some("y".to_owned())),
                (Action::Update, "a".to_owned(), Some("x".to_owned())),
                (Action::Insert, "a".to_owned(), Some("x".to_owned())),
            ]
        );
        let n = needs(
            "renameCollection",
            &doc! {"renameCollection": "a.x", "to": "b.y"},
            "admin",
        );
        assert!(n.contains(&(Action::Create, "b".to_owned(), Some("y".to_owned()))));
        assert!(n.contains(&(Action::Drop, "a".to_owned(), Some("x".to_owned()))));
    }

    #[test]
    fn write_concerns_one_node_cannot_satisfy() {
        let w = |v: Bson| doc! {"insert": "c", "writeConcern": {"w": v}};
        let e = unsatisfiable("commitTransaction", &w(Bson::Int32(10))).unwrap();
        assert_eq!(e.get_i32("code"), Ok(100));
        assert!(unsatisfiable("insert", &w(Bson::Int32(1))).is_none());
        assert!(unsatisfiable("insert", &w(Bson::String("majority".into()))).is_none());
        let e = unsatisfiable("insert", &w(Bson::String("dc1".into()))).unwrap();
        assert_eq!(e.get_i32("code"), Ok(79));
        assert!(unsatisfiable("find", &w(Bson::Int32(10))).is_none());
    }
}
