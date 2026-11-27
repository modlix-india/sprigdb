//! MongoDB collections as catalog objects.
//!
//! ```text
//! the document store's collection records (the truth)
//!   db, name, n                    → catalog object, kind collection,
//!                                    namespace relation, in database db,
//!                                    object id = the collection's engine
//!                                    object (0x4D00_0000 + n)
//!   a database the catalog lacks   → created as SQL's CREATE DATABASE
//!                                    makes it (the logical database)
//! sync(tenant): one versioned catalog change brings the catalog to the
//!   store: new collections added (a tombstone of the same id revived),
//!   renamed ones renamed (through temporary names, so swaps work), dropped
//!   ones dropped; before it, the authorization engine moves the grants of
//!   renamed and dropped ones to their names (auth.grants_on_drop) and
//!   deletes dropped ones' policies, as RENAME TABLE and DROP TABLE do
//! when: after every store command that changed the tenant's collections
//!   on this node (the store's generation moved; in the background,
//!   debounced), at the tenant's first command on the node (older
//!   collections get their objects so: the upgrade path), and before a
//!   GRANT or policy statement names a collection the catalog doesn't have
//! ```
//!
//! The object is what grants (`GRANT ... ON db.coll`) and row-level
//! security policies attach to; SQL reads of a collection already number
//! its table with the same id. A collection whose name another relation of
//! the database holds (a table, a view) gets no object: it keeps the
//! database's grants and grants held by its name, and can't take policies.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use sprig_auth::authz::authorizer::Renamed;
use sprig_catalog::ddl::{DdlTx, database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_doc::docstore::{DocStore, Namespace as StoreNs};

use crate::tenant::TenantServices;

/// What the catalog must change to match the store.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct Plan {
    /// Databases holding collections that the catalog lacks.
    pub dbs: Vec<String>,
    /// New objects.
    pub adds: Vec<ObjectRow>,
    /// (the row as it becomes, the row now) of renamed collections.
    pub renames: Vec<(ObjectRow, ObjectRow)>,
    /// Objects whose collection is gone.
    pub drops: Vec<ObjectRow>,
    /// (database, name) of collections whose name another relation holds.
    pub conflicts: Vec<(String, String)>,
}

impl Plan {
    /// Nothing to change in the objects.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.adds.is_empty() && self.renames.is_empty() && self.drops.is_empty()
    }
}

/// The changes that bring `cat`'s collection objects to the store's
/// namespaces `store` (views have no object).
#[must_use]
pub(crate) fn plan(cat: &TenantCatalog, lctn: bool, store: &[StoreNs]) -> Plan {
    let mut out = Plan::default();
    let colls: Vec<&StoreNs> = store.iter().filter(|s| !s.view).collect();
    let ids: HashSet<u64> = colls.iter().map(|s| u64::from(s.object)).collect();
    let live: HashMap<u64, &ObjectRow> = cat
        .objects
        .iter()
        .filter(|o| o.kind == ObjectKind::Collection && o.state != ObjectState::Dropped)
        .map(|o| (o.object_id.0, &**o))
        .collect();
    for o in live.values() {
        if !ids.contains(&o.object_id.0) {
            out.drops.push((*o).clone());
        }
    }
    // Names that objects leave in this change: dropped ones, and collections
    // that move (renames go through temporary names first).
    let mut leaving: HashSet<u64> = out.drops.iter().map(|o| o.object_id.0).collect();
    for s in &colls {
        if let Some(o) = live.get(&u64::from(s.object)) {
            let d = cat
                .database(&database_key(&s.db, lctn))
                .filter(|d| d.state == DbState::Active);
            if d.is_none_or(|d| d.db_id != o.db_id || o.name != s.name) {
                leaving.insert(o.object_id.0);
            }
        }
    }
    let mut taken: HashSet<(DbId, String)> = HashSet::new();
    for s in colls {
        let Some(d) = cat
            .database(&database_key(&s.db, lctn))
            .filter(|d| d.state == DbState::Active)
        else {
            if !out.dbs.contains(&s.db) {
                out.dbs.push(s.db.clone());
            }
            continue;
        };
        let id = u64::from(s.object);
        let key = object_key(ObjectKind::Collection, &s.name, lctn);
        let current = live.get(&id);
        if current.is_some_and(|o| o.db_id == d.db_id && o.name == s.name) {
            continue;
        }
        let holder = cat
            .object(d.db_id, Namespace::Relation, &key)
            .filter(|h| h.state != ObjectState::Dropped);
        let free = holder.is_none_or(|h| h.object_id.0 == id || leaving.contains(&h.object_id.0));
        if !free || !taken.insert((d.db_id, key.clone())) {
            out.conflicts.push((s.db.clone(), s.name.clone()));
            continue;
        }
        let row = ObjectRow {
            object_id: ObjectId(id),
            db_id: d.db_id,
            namespace: Namespace::Relation,
            name: s.name.clone(),
            name_key: key,
            kind: ObjectKind::Collection,
            parent_id: None,
            state: ObjectState::Active,
            schema_version: 1,
            flags: ObjectFlags::empty(),
            pending_job_id: None,
            created_at: SystemTime::now(),
            changed_version: 0,
        };
        match current {
            Some(o) => out.renames.push((row, (*o).clone())),
            None => out.adds.push(row),
        }
    }
    out
}

/// Whether the tenant's relation names compare without case.
pub(crate) fn lctn(svc: &TenantServices, cat: &TenantCatalog) -> bool {
    svc.settings.get(
        &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1
}

/// Writes `p` as one versioned catalog change; its version.
async fn apply(svc: &TenantServices, tenant: TenantId, p: &Plan) -> Result<u64> {
    let mut ch = svc.catalog.change(tenant, VersionKind::Catalog).await?;
    let version = ch.version();
    let r = async {
        let tx = ch.tx();
        for o in &p.drops {
            tx.set_object_state(o.object_id, ObjectState::Dropped, version)
                .await?;
        }
        // Renames free their names first, so two collections may swap.
        for (_, old) in &p.renames {
            let tmp = format!("#sprig-moving-{}", old.object_id.0);
            let row = ObjectRow {
                name: tmp.clone(),
                name_key: tmp,
                changed_version: version,
                ..old.clone()
            };
            DdlTx::update_object(tx, &row).await?;
        }
        for (new, _) in &p.renames {
            let row = ObjectRow {
                changed_version: version,
                ..new.clone()
            };
            DdlTx::update_object(tx, &row).await?;
        }
        for new in &p.adds {
            let row = ObjectRow {
                changed_version: version,
                ..new.clone()
            };
            // A dropped collection's tombstone (snapshots leave them out)
            // comes back when the store has the collection again.
            if DdlTx::object(tx, row.object_id).await?.is_some() {
                DdlTx::update_object(tx, &row).await?;
            } else {
                DdlTx::insert_object(tx, &row).await?;
            }
        }
        Ok::<(), SprigError>(())
    }
    .await;
    if let Err(e) = r {
        let _ = ch.rollback().await;
        return Err(e);
    }
    ch.commit().await
}

/// Brings `tenant`'s collection objects to the store's collections (see
/// the module docs). Returns the catalog version that has them, when it
/// changed anything.
///
/// # Errors
/// The catalog's; the store's read.
pub(crate) async fn sync(
    svc: &Arc<TenantServices>,
    store: &DocStore,
    tenant: TenantId,
) -> Result<Option<u64>> {
    let read = || {
        store
            .namespaces(tenant.0)
            .map_err(|e| SprigError::internal(format!("collections not read: {}", e.message)))
    };
    let mut cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let mut p = plan(&cat, lctn(svc, &cat), &read()?);
    if !p.dbs.is_empty() {
        for db in &p.dbs {
            match crate::inherited::ensure_database(svc, tenant, db).await {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(tenant = %tenant, %db, error = %e, "the logical database of a MongoDB database not created");
                }
            }
        }
        cat = svc.cache.get(tenant, Freshness::Latest).await?;
        p = plan(&cat, lctn(svc, &cat), &read()?);
    }
    let mut last = None;
    for attempt in 0..3 {
        if attempt > 0 {
            // Another node's change came first: plan again on its result.
            cat = svc.cache.get(tenant, Freshness::Latest).await?;
            p = plan(&cat, lctn(svc, &cat), &read()?);
        }
        for (db, name) in &p.conflicts {
            tracing::debug!(tenant = %tenant, %db, collection = %name, "a collection whose name another relation holds has no catalog object");
        }
        if p.is_empty() {
            return Ok(None);
        }
        let key_of = |db: DbId| {
            cat.databases
                .get(db)
                .map(|d| d.name_key.clone())
                .unwrap_or_default()
        };
        let renamed: Vec<Renamed> = p
            .renames
            .iter()
            .map(|(_, o)| Renamed {
                db: o.db_id,
                db_key: key_of(o.db_id),
                object: o.object_id,
                name_key: o.name_key.clone(),
            })
            .collect();
        let dropped: Vec<(DbId, String, ObjectId, String)> = p
            .drops
            .iter()
            .map(|o| (o.db_id, key_of(o.db_id), o.object_id, o.name_key.clone()))
            .collect();
        if let Err(e) = crate::tenant::authz::collections_moved(svc, &cat, &renamed, &dropped).await
        {
            tracing::warn!(tenant = %tenant, error = %e, "grants of renamed or dropped collections not moved to their names");
        }
        match apply(svc, tenant, &p).await {
            Ok(v) => {
                tracing::debug!(
                    tenant = %tenant,
                    added = p.adds.len(),
                    renamed = p.renames.len(),
                    dropped = p.drops.len(),
                    version = v,
                    "collection objects brought to the store's collections"
                );
                let _ = svc
                    .cache
                    .get(tenant, Freshness::Cached { min_version: v })
                    .await;
                return Ok(Some(v));
            }
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| SprigError::internal("collection objects not written")))
}

/// One tenant's sync state on this node.
#[derive(Default)]
struct Slot {
    /// Held while a sync runs.
    running: tokio::sync::Mutex<()>,
    /// The store generation the catalog was last brought to, plus one
    /// (0: never on this node).
    synced: AtomicU64,
    /// A background sync is scheduled or running.
    scheduled: AtomicBool,
    /// When the last sync failed (a failed one isn't retried at once).
    failed: Mutex<Option<Instant>>,
}

fn slot(tenant: u64) -> Arc<Slot> {
    static SLOTS: OnceLock<Mutex<HashMap<u64, Arc<Slot>>>> = OnceLock::new();
    Arc::clone(
        SLOTS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(tenant)
            .or_default(),
    )
}

/// How long a failed sync waits before the next try.
const RETRY_AFTER: Duration = Duration::from_secs(30);
/// How long a background sync waits for more of a burst of DDL.
const DEBOUNCE: Duration = Duration::from_millis(50);

/// Runs one sync of `tenant` under its slot, noting the generation it
/// reached.
async fn run_once(
    svc: &Arc<TenantServices>,
    store: &DocStore,
    tenant: u64,
    s: &Slot,
) -> Result<Option<u64>> {
    let _g = s.running.lock().await;
    let generation = store.generation(tenant) + 1;
    let r = sync(svc, store, TenantId(tenant)).await;
    match &r {
        Ok(_) => {
            s.synced.store(generation, Ordering::SeqCst);
            *s.failed.lock().unwrap_or_else(PoisonError::into_inner) = None;
        }
        Err(e) => {
            tracing::warn!(tenant, error = %e, "collection objects not brought to the store's collections");
            *s.failed.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        }
    }
    r
}

/// After a command of `tenant` on the store: when its collections changed
/// on this node since the catalog last followed them (or never did here),
/// a background sync brings the catalog to them.
pub(crate) fn touch(svc: &Arc<TenantServices>, store: &Arc<DocStore>, tenant: u64) {
    if tenant == 0 {
        return;
    }
    let s = slot(tenant);
    if s.synced.load(Ordering::SeqCst) == store.generation(tenant) + 1 {
        return;
    }
    if s.failed
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_some_and(|t| t.elapsed() < RETRY_AFTER)
    {
        return;
    }
    if s.scheduled.swap(true, Ordering::SeqCst) {
        return;
    }
    let (svc, store) = (Arc::clone(svc), Arc::clone(store));
    drop(tokio::spawn(async move {
        loop {
            tokio::time::sleep(DEBOUNCE).await;
            if run_once(&svc, &store, tenant, &s).await.is_err() {
                s.scheduled.store(false, Ordering::SeqCst);
                return;
            }
            if s.synced.load(Ordering::SeqCst) == store.generation(tenant) + 1 {
                s.scheduled.store(false, Ordering::SeqCst);
                // A change that came between the check and the release
                // found the flag set: take it here.
                if s.synced.load(Ordering::SeqCst) == store.generation(tenant) + 1
                    || s.scheduled.swap(true, Ordering::SeqCst)
                {
                    return;
                }
            }
        }
    }));
}

/// Brings `tenant`'s collection objects up to date now (a `GRANT` or a
/// policy statement is about to name one). Returns the catalog version
/// that has them, when it changed anything.
///
/// # Errors
/// The catalog's.
pub(crate) async fn ensure(
    svc: &Arc<TenantServices>,
    store: &DocStore,
    tenant: u64,
) -> Result<Option<u64>> {
    let s = slot(tenant);
    run_once(svc, store, tenant, &s).await
}

/// Whether `tenant`'s store holds collection `db.name` (not a view).
#[must_use]
pub(crate) fn has_collection(store: &DocStore, tenant: u64, db: &str, name: &str) -> bool {
    store
        .coll(tenant, &sprig_doc::docstore::Name::new(db, name))
        .ok()
        .flatten()
        .is_some_and(|c| c.meta.view().is_none())
}

/// When `db.name` names no relation in `cat` but a collection of the
/// session's tenant: the catalog brought to the store's collections first
/// (a `GRANT` or a policy statement is about to name it), and the newest
/// snapshot. `None`: nothing to do.
///
/// # Errors
/// The catalog's.
pub(crate) async fn ensure_named(
    ts: &crate::tenant::session::TenantSession,
    cat: &TenantCatalog,
    db: &str,
    name: &str,
) -> Result<Option<Arc<TenantCatalog>>> {
    let lctn = ts.lower_case_table_names();
    let known = cat
        .database(&database_key(db, lctn))
        .filter(|d| d.state == DbState::Active)
        .and_then(|d| {
            cat.object(
                d.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, name, lctn),
            )
        })
        .is_some();
    if known {
        return Ok(None);
    }
    let Some(store) = super::engine::store() else {
        return Ok(None);
    };
    if !has_collection(&store, ts.tenant.0, db, name) {
        return Ok(None);
    }
    let svc = Arc::clone(ts.services());
    ensure(&svc, &store, ts.tenant.0).await?;
    Ok(Some(svc.cache.get(ts.tenant, Freshness::Latest).await?))
}

#[cfg(test)]
mod tests;
