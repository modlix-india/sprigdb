//! Collections in the tenant catalog: the `doc` catalog module and the
//! document engine's [`DocCatalog`] over it.
//!
//! - **Records.** The module's rows are the engine's catalog records
//!   (`sprig_doc::catalog_records`: collections, their indexes and fields).
//!   A collection's id is the `objects.object_id` of its table (an
//!   ordinary `table` row, created by the engine's `CREATE TABLE` through
//!   the user's session).
//! - **Changes.** Registering or removing a collection, its indexes, and a
//!   new array path are each one versioned catalog change (the rows
//!   written through the change's records), so every gateway's cache
//!   reloads them.
//! - **Reads.** Commands read collections from the gateway's cached
//!   snapshot ([`DocState`]): no metadata read per command. The snapshot
//!   is the newest the directory watch has delivered, and never older
//!   than this gateway's own last change.
//! - **Old data.** A database written before the catalog tables existed
//!   keeps its collections in `__sprig_doc_meta`. The first command on
//!   that database in each gateway process imports its rows into the
//!   catalog and drops the table.
//!
//! The publication wait of `check_flags` (every gateway acknowledging a
//! new array path before the write commits) is in [`super::publish`].

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use bson::Document;
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::ddl::{DdlTx, LOWER_CASE_TABLE_NAMES, database_key, object_key};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module};
use sprig_catalog::records::RecordTx;
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_catalog::version::{ChangeOpts, VersionKind};
use sprig_catalog::{CatalogExtension, CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::catalog::{DocCatalog, TableCatalog};
pub use sprig_doc::catalog_records::{
    CNAMES, COLLS, CollRow, DocTables, FIELDS, FieldRow, INDEXES, IndexRow, MODULE,
};
use sprig_doc::engine::{Ns, SqlRunner};
use sprig_doc::meta::{CollMeta, META_TABLE};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::tenant::TenantServices;

/// The module's MariaDB migrations: none, its rows are engine records.
pub static DOC_MIGRATIONS: &[Migration] = &[];

/// The `doc` module, for [`Migrator::new`].
pub const DOC_MODULE: Module = Module {
    name: MODULE,
    migrations: DOC_MIGRATIONS,
};

fn map_err(e: mysql_async::Error) -> SprigError {
    match &e {
        mysql_async::Error::Server(s) => {
            let mut state = *b"HY000";
            if s.state.len() == 5 {
                state.copy_from_slice(s.state.as_bytes());
            }
            SprigError::backend(
                s.code,
                state,
                format!(
                    "collection catalog statement failed ({}): {}",
                    s.code, s.message
                ),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable"),
    }
    .with_source(e)
}

fn mongo(e: &SprigError) -> MongoError {
    let c = match e.kind {
        sprig_common::ErrorKind::Sprig(c) => i32::from(c.number()),
        _ => code::INTERNAL_ERROR,
    };
    MongoError::new(c, e.message.clone())
}

/// One tenant's collections in a snapshot: by database id, then name.
#[derive(Debug, Default)]
pub struct DocState {
    by_db: HashMap<u32, BTreeMap<String, Arc<CollMeta>>>,
}

impl ExtState for DocState {
    fn bytes(&self) -> usize {
        self.by_db
            .values()
            .flat_map(BTreeMap::values)
            .map(|m| 256 + m.array_paths.len() * 32 + m.indexes.len() * 128)
            .sum()
    }
}

impl DocState {
    /// Collection `name` of database `db`.
    #[must_use]
    pub fn get(&self, db: u32, name: &str) -> Option<&Arc<CollMeta>> {
        self.by_db.get(&db)?.get(name)
    }

    /// The collections of `db`, by name.
    pub fn of(&self, db: u32) -> impl Iterator<Item = &Arc<CollMeta>> {
        self.by_db.get(&db).into_iter().flat_map(BTreeMap::values)
    }
}

/// Makes sure catalog `schema` can take the module's records (its core
/// migrations, which create the record log).
///
/// # Errors
/// The migrator's.
pub async fn migrate(q: &mut mysql_async::Conn, schema: &str) -> Result<()> {
    Migrator::new(&[DOC_MODULE])?
        .run(q, schema, &MigrateOpts::existing())
        .await?;
    Ok(())
}

/// An object whose table still serves its collection.
fn live_table(state: ObjectState) -> bool {
    matches!(
        state,
        ObjectState::Active | ObjectState::Altering | ObjectState::Creating
    )
}

/// The module in the catalog cache: [`DocState`] in every snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct DocExtension;

#[async_trait]
impl CatalogExtension for DocExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        DOC_MIGRATIONS
    }

    fn versioned(&self) -> Option<VersionKind> {
        Some(VersionKind::Catalog)
    }

    async fn load(
        &self,
        tx: &mut SnapshotTx<'_>,
        _since: Option<u64>,
        _prev: Option<&Arc<dyn ExtState>>,
    ) -> Result<Arc<dyn ExtState>> {
        if !tx.catalog_records() {
            return Ok(Arc::new(DocState::default()));
        }
        let mut by_id = DocTables::load(tx.records()?).await?;
        // A collection whose table was dropped (also by SQL) is left out:
        // its table's object, from the snapshot's own core rows.
        let ids: Vec<ObjectId> = by_id.keys().copied().map(ObjectId).collect();
        let live: HashSet<u64> = tx
            .objects_by_ids(&ids)
            .await?
            .into_iter()
            .filter(|o| !matches!(o.state, ObjectState::Dropping | ObjectState::Dropped))
            .map(|o| o.object_id.0)
            .collect();
        by_id.retain(|cid, _| live.contains(cid));
        let mut st = DocState::default();
        for (_, (db, m)) in by_id {
            st.by_db
                .entry(db)
                .or_default()
                .insert(m.name.clone(), Arc::new(m));
        }
        Ok(Arc::new(st))
    }
}

/// The document engine's metadata in the tenant catalog.
pub struct CatalogDocs {
    svc: Arc<TenantServices>,
    /// Tenants whose module tables exist.
    migrated: Mutex<HashSet<u64>>,
    /// The newest catalog version this gateway committed, per tenant.
    seen: Mutex<HashMap<u64, u64>>,
    /// Databases checked for an old `__sprig_doc_meta`, by tenant.
    legacy: Mutex<HashMap<u64, HashSet<String>>>,
    /// The publication wait; `None` without a DC tier.
    publisher: Option<Arc<super::publish::Publisher>>,
}

impl std::fmt::Debug for CatalogDocs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogDocs").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One MongoDB shape's totals in the workload store: every
/// gateway's flushed counts, kept for the store's retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredShape {
    /// The fingerprint's text: `<command> <db>.<collection> <shape JSON>`.
    pub text: String,
    /// Runs.
    pub runs: u64,
    /// Total time, microseconds.
    pub total_us: u64,
    /// Longest run, microseconds.
    pub max_us: u64,
    /// Documents returned.
    pub returned: u64,
    /// Documents changed (writes).
    pub affected: u64,
    /// First and last seen, ms since the epoch.
    pub first_ms: i64,
    /// Last seen.
    pub last_ms: i64,
}

impl CatalogDocs {
    /// The tenant's MongoDB shapes in the workload store, with
    /// their totals over every row the store keeps; a tenant whose
    /// workload tables don't exist yet has none.
    ///
    /// # Errors
    /// The catalog's.
    pub async fn stored_shapes(&self, tenant: TenantId) -> Result<Vec<StoredShape>> {
        let mut rec = self.svc.records.begin(tenant).await?;
        let rows = sprig_workload::store::WorkloadTables::totals(
            &mut *rec,
            sprig_workload::fingerprint::FpKind::Mongo,
        )
        .await?;
        let ms = |t: std::time::SystemTime| {
            t.duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        };
        Ok(rows
            .into_iter()
            .filter(|(f, _)| !f.flags.split(',').any(|x| x == "truncated" || x == "other"))
            .map(|(f, s)| StoredShape {
                text: f.text.to_string(),
                runs: s.exec_count,
                total_us: s.total_us,
                max_us: s.max_us,
                returned: s.rows_returned,
                affected: s.rows_affected,
                first_ms: ms(f.first_seen),
                last_ms: ms(f.last_seen),
            })
            .collect())
    }

    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(HashSet::new()),
            seen: Mutex::new(HashMap::new()),
            legacy: Mutex::new(HashMap::new()),
            publisher: None,
        }
    }

    /// With the publication wait of `check_flags`.
    #[must_use]
    pub fn with_publisher(mut self, p: Arc<super::publish::Publisher>) -> Self {
        self.publisher = Some(p);
        self
    }

    async fn prepare(&self, tenant: TenantId) -> Result<()> {
        if lock(&self.migrated).contains(&tenant.0) {
            return Ok(());
        }
        let ep = self.svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
        migrate(&mut conn, ep.store.schema()).await?;
        lock(&self.migrated).insert(tenant.0);
        Ok(())
    }

    /// The snapshot: the newest the directory watch delivered, and never
    /// older than this gateway's own last change.
    async fn snapshot(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        let min = lock(&self.seen).get(&tenant.0).copied().unwrap_or(0);
        let cat = self.svc.cache.get(tenant, Freshness::Latest).await?;
        if cat.version >= min {
            return Ok(cat);
        }
        self.svc
            .cache
            .get(tenant, Freshness::Cached { min_version: min })
            .await
    }

    fn saw(&self, tenant: TenantId, v: u64) {
        let mut s = lock(&self.seen);
        let e = s.entry(tenant.0).or_insert(0);
        *e = (*e).max(v);
    }

    fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// Imports `db`'s old `__sprig_doc_meta` rows once per process.
    async fn legacy(
        &self,
        sql: &mut dyn SqlRunner,
        tenant: TenantId,
        db: &str,
        cat: &TenantCatalog,
        db_id: sprig_common::ids::DbId,
    ) -> Result<bool> {
        if self.legacy_checked(tenant, db) {
            return Ok(false);
        }
        // Only a database the round-1 engine wrote has the table (an
        // object of the catalog): nothing to read for any other, and
        // users with grants on single collections never probe it.
        let lctn = self.lctn(cat);
        if cat
            .object(
                db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, META_TABLE, lctn),
            )
            .is_none()
        {
            self.legacy_done(tenant, db);
            return Ok(false);
        }
        let rows = match TableCatalog::rows(sql, db).await {
            Ok(r) => r,
            // This user can't read it: a user who can imports it.
            Err(e) if e.code == code::UNAUTHORIZED => return Ok(false),
            Err(e) => return Err(SprigError::new(SprigCode::CatalogUnavailable, e.message)),
        };
        let mut imported = false;
        if !rows.is_empty() {
            for m in &rows {
                let ns = Ns {
                    db: db.to_owned(),
                    coll: m.name.clone(),
                };
                match self.register_in(tenant, &ns, m).await {
                    Ok(_) => imported = true,
                    // The table is gone: nothing to import.
                    Err(e) if e.kind == sprig_common::ErrorKind::Sprig(SprigCode::Internal) => {
                        tracing::warn!(%tenant, db, coll = m.name, error = %e, "old collection metadata names no table; skipped");
                    }
                    Err(e) => return Err(e),
                }
            }
            let drop = format!(
                "DROP TABLE IF EXISTS {}.{}",
                sprig_doc::sql::ident(db),
                sprig_doc::sql::ident(META_TABLE)
            );
            if let Err(e) = sql.exec(&drop).await {
                tracing::warn!(%tenant, db, error = %e.message, "dropping __sprig_doc_meta after its import failed");
            }
            tracing::info!(%tenant, db, collections = rows.len(), "collection metadata moved to the catalog");
        }
        self.legacy_done(tenant, db);
        Ok(imported)
    }

    /// `db` of `tenant` was checked for old metadata in this process.
    fn legacy_checked(&self, tenant: TenantId, db: &str) -> bool {
        lock(&self.legacy)
            .get(&tenant.0)
            .is_some_and(|s| s.contains(db))
    }

    fn legacy_done(&self, tenant: TenantId, db: &str) {
        lock(&self.legacy)
            .entry(tenant.0)
            .or_default()
            .insert(db.to_owned());
    }

    /// Registers `m` as `ns` in one change; the existing live collection
    /// if there is one.
    async fn register_in(
        &self,
        tenant: TenantId,
        ns: &Ns,
        m: &CollMeta,
    ) -> Result<Option<CollMeta>> {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        // The version row is locked first and a version is
        // allocated only when something is written, so finding the
        // collection already registered leaves no gap.
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<Option<CollMeta>> = async {
            let tx = ch.tx();
            let db = DdlTx::database_by_name(tx, &database_key(&ns.db, lctn))
                .await?
                .ok_or_else(|| {
                    SprigError::new(SprigCode::Internal, format!("no database {}", ns.db))
                })?;
            let obj = DdlTx::object_by_name(
                tx,
                db.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, &m.table, lctn),
            )
            .await?
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::Internal,
                    format!("collection {ns} has no table {}", m.table),
                )
            })?;
            let rec = tx.records().await?;
            if let Some((cid, old)) = DocTables::find(rec, db.db_id.0, &ns.coll).await? {
                if cid == obj.object_id.0 && live_table(obj.state) {
                    return Ok(Some(old));
                }
                // Left by a table dropped outside the engine.
                DocTables::delete(rec, cid).await?;
            }
            DocTables::insert(rec, obj.object_id.0, db.db_id.0, m).await?;
            Ok(None)
        }
        .await;
        match r {
            Ok(Some(x)) => {
                let _ = ch.release().await;
                Ok(Some(x))
            }
            Ok(None) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(None)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    /// Runs `f` on the locked collection `ns` in one change; the version
    /// committed, when `f` changed something.
    async fn update<F>(&self, tenant: TenantId, ns: &Ns, f: F) -> Result<Option<u64>>
    where
        F: for<'a> FnOnce(
                &'a mut dyn RecordTx,
                u64,
                CollMeta,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bool>> + Send + 'a>,
            > + Send,
    {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        // Allocate only when `f` changed something.
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<bool> = async {
            let tx = ch.tx();
            let Some(db) = DdlTx::database_by_name(tx, &database_key(&ns.db, lctn)).await? else {
                return Ok(false);
            };
            let rec = tx.records().await?;
            let Some((cid, m)) = DocTables::find(rec, db.db_id.0, &ns.coll).await? else {
                return Ok(false);
            };
            f(rec, cid, m).await
        }
        .await;
        match r {
            Ok(true) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(Some(v))
            }
            Ok(false) => {
                let _ = ch.release().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    /// Deletes the rows of collections whose table is gone (a dropped
    /// database, or a table dropped by SQL), in one change.
    async fn purge_orphans(&self, tenant: TenantId) -> Result<()> {
        self.prepare(tenant).await?;
        // After nearly every `dropDatabase` there is nothing to purge; the
        // version row is locked, and a version allocated only when rows go
        // (before, each call left a gap that held every `Freshness::Latest` load
        // of the tenant for `catalog.version_wait_ms`).
        let mut ch = self.svc.catalog.lock(tenant, VersionKind::Catalog).await?;
        // The snapshot at the locked version (no change can commit while the
        // version row is locked) names every object that isn't dropping or
        // dropped; the few collections whose table it lacks are confirmed
        // in the change itself (a table being dropped keeps its collection
        // until the drop finishes).
        let at = ch.previous();
        let r: Result<u64> = async {
            let cat = self
                .svc
                .cache
                .get(tenant, Freshness::Cached { min_version: at })
                .await?;
            let missing: Vec<ObjectId> = DocTables::ids(ch.tx().records().await?)
                .await?
                .into_iter()
                .map(ObjectId)
                .filter(|id| cat.objects.get(*id).is_none())
                .collect();
            if missing.is_empty() {
                return Ok(0);
            }
            let found: HashMap<u64, ObjectState> = ch
                .tx()
                .objects_by_ids(&missing)
                .await?
                .into_iter()
                .map(|o| (o.object_id.0, o.state))
                .collect();
            let rec = ch.tx().records().await?;
            let mut n = 0;
            for id in missing {
                if matches!(found.get(&id.0), None | Some(ObjectState::Dropped)) {
                    DocTables::delete(rec, id.0).await?;
                    n += 1;
                }
            }
            Ok(n)
        }
        .await;
        match r {
            Ok(0) => {
                let _ = ch.release().await;
                Ok(())
            }
            Ok(_) => {
                let v = ch.allocate(ChangeOpts::default()).await?.commit().await?;
                self.saw(tenant, v);
                Ok(())
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }

    async fn state(
        &self,
        sql: &mut dyn SqlRunner,
        tenant: TenantId,
        db: &str,
    ) -> Result<Option<(u32, Arc<TenantCatalog>)>> {
        let mut cat = self.snapshot(tenant).await?;
        let lctn = self.lctn(&cat);
        let Some(row) = cat.database(&database_key(db, lctn)).cloned() else {
            return Ok(None);
        };
        if self.legacy(sql, tenant, db, &cat, row.db_id).await? {
            cat = self.snapshot(tenant).await?;
        }
        Ok(Some((row.db_id.0, cat)))
    }
}

fn tenant_of(sql: &dyn SqlRunner) -> Option<TenantId> {
    Some(sql.tenant()).filter(|t| *t != 0).map(TenantId)
}

#[async_trait]
impl DocCatalog for CatalogDocs {
    async fn get(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> std::result::Result<Option<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.get(sql, ns).await;
        };
        let Some((db, cat)) = self
            .state(sql, tenant, &ns.db)
            .await
            .map_err(|e| mongo(&e))?
        else {
            return Ok(None);
        };
        Ok(cat
            .ext
            .get::<DocState>(MODULE)
            .and_then(|s| s.get(db, &ns.coll))
            .map(|m| (**m).clone()))
    }

    async fn collation(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> std::result::Result<Option<Document>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.collation(sql, ns).await;
        };
        let Some((db, cat)) = self
            .state(sql, tenant, &ns.db)
            .await
            .map_err(|e| mongo(&e))?
        else {
            return Ok(None);
        };
        Ok(cat
            .ext
            .get::<DocState>(MODULE)
            .and_then(|s| s.get(db, &ns.coll))
            .and_then(|m| m.options.get_document("collation").ok().cloned()))
    }

    async fn list(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<Vec<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.list(sql, db).await;
        };
        let Some((id, cat)) = self.state(sql, tenant, db).await.map_err(|e| mongo(&e))? else {
            return Ok(Vec::new());
        };
        Ok(cat
            .ext
            .get::<DocState>(MODULE)
            .map(|s| s.of(id).map(|m| (**m).clone()).collect())
            .unwrap_or_default())
    }

    async fn dropped_db(
        &self,
        sql: &mut dyn SqlRunner,
        _db: &str,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return Ok(());
        };
        self.purge_orphans(tenant).await.map_err(|e| mongo(&e))
    }

    async fn prepare_db(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<(), MongoError> {
        if let Some(tenant) = tenant_of(sql) {
            // Once a database was checked, the one-time work is done: no
            // catalog snapshot per command.
            if self.legacy_checked(tenant, db) {
                return Ok(());
            }
            self.state(sql, tenant, db).await.map_err(|e| mongo(&e))?;
        }
        Ok(())
    }

    async fn ensure_db(
        &self,
        sql: &mut dyn SqlRunner,
        db: &str,
    ) -> std::result::Result<(), MongoError> {
        if tenant_of(sql).is_none() {
            return TableCatalog.ensure_db(sql, db).await;
        }
        Ok(())
    }

    async fn register(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<Option<CollMeta>, MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.register(sql, ns, m).await;
        };
        self.register_in(tenant, ns, m).await.map_err(|e| mongo(&e))
    }

    async fn unregister(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.unregister(sql, ns).await;
        };
        self.update(tenant, ns, |rec, cid, _| {
            Box::pin(async move {
                DocTables::delete(rec, cid).await?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn save_indexes(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.save_indexes(sql, ns, m).await;
        };
        let m = m.clone();
        self.update(tenant, ns, move |rec, cid, _| {
            Box::pin(async move {
                DocTables::write_indexes(rec, cid, &m).await?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn add_array_paths(
        &self,
        sql: &mut dyn SqlRunner,
        ns: &Ns,
        paths: &BTreeSet<String>,
        full: bool,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.add_array_paths(sql, ns, paths, full).await;
        };
        // 1. Register what is new, marked pending, in one change.
        let req = paths.clone();
        let added = self
            .update(tenant, ns, move |rec, cid, old| {
                Box::pin(async move {
                    let fresh: BTreeSet<String> = if old.arrays_full {
                        BTreeSet::new()
                    } else {
                        req.difference(&old.array_paths).cloned().collect()
                    };
                    let becomes_full = full && !old.arrays_full;
                    if fresh.is_empty() && !becomes_full {
                        return Ok(false);
                    }
                    let mut all = old.array_paths;
                    all.extend(fresh.iter().cloned());
                    let mut pending = old.pending_paths;
                    if becomes_full {
                        pending.insert("*".to_owned());
                    } else {
                        pending.extend(fresh);
                    }
                    let full_now = becomes_full || old.arrays_full;
                    DocTables::update_row(rec, cid, move |r| {
                        r.array_paths = if full_now {
                            Vec::new()
                        } else {
                            all.into_iter().collect()
                        };
                        r.array_paths_full = full_now;
                        r.array_paths_pending = Some(pending.into_iter().collect());
                    })
                    .await?;
                    metrics::counter!("sprig_doc_path_publications_total", "kind" => "array")
                        .increment(1);
                    Ok(true)
                })
            })
            .await
            .map_err(|e| mongo(&e))?;
        // 2. Every gateway loads a version holding them before the write
        // commits (check_flags). Paths an earlier write registered without
        // a confirmation are confirmed now.
        if let Some(p) = &self.publisher {
            let cat = self.snapshot(tenant).await.map_err(|e| mongo(&e))?;
            let version = added.unwrap_or(cat.version).max(cat.version);
            let timeout = std::time::Duration::from_millis(self.svc.settings.get(
                &super::publish::PATH_PUBLISH_TIMEOUT_MS,
                &cat.setting_ctx(&self.svc.dc),
                Some(&cat.settings),
            ));
            p.publish(tenant, version, timeout).await?;
        }
        // 3. Confirmed: no longer pending.
        let done = paths.clone();
        self.update(tenant, ns, move |rec, cid, old| {
            Box::pin(async move {
                let mut pending = old.pending_paths;
                let before = pending.len();
                pending.retain(|p| !done.contains(p) && !(p == "*" && (full || old.arrays_full)));
                if pending.len() == before {
                    return Ok(false);
                }
                DocTables::update_row(rec, cid, move |r| {
                    r.array_paths_pending = if pending.is_empty() {
                        None
                    } else {
                        Some(pending.into_iter().collect())
                    };
                })
                .await?;
                Ok(true)
            })
        })
        .await
        .map(|_| ())
        .map_err(|e| mongo(&e))
    }

    async fn rename(
        &self,
        sql: &mut dyn SqlRunner,
        from: &Ns,
        to: &Ns,
        m: &CollMeta,
    ) -> std::result::Result<(), MongoError> {
        let Some(tenant) = tenant_of(sql) else {
            return TableCatalog.rename(sql, from, to, m).await;
        };
        self.unregister(sql, from).await?;
        let mut n = m.clone();
        n.name.clone_from(&to.coll);
        match self
            .register_in(tenant, to, &n)
            .await
            .map_err(|e| mongo(&e))?
        {
            None => Ok(()),
            Some(_) => Err(MongoError::new(
                code::NAMESPACE_EXISTS,
                "target namespace exists",
            )),
        }
    }
}

#[async_trait]
impl super::changes::TableNames for CatalogDocs {
    async fn collection(&self, tenant: TenantId, db: &str, table: &str) -> Option<String> {
        let cat = self.snapshot(tenant).await.ok()?;
        let lctn = self.lctn(&cat);
        let row = cat.database(&database_key(db, lctn))?;
        cat.ext
            .get::<DocState>(MODULE)?
            .of(row.db_id.0)
            .find(|m| m.table == table)
            .map(|m| m.name.clone())
    }

    async fn uuid(&self, tenant: TenantId, db: &str, table: &str) -> Option<[u8; 16]> {
        let cat = self.snapshot(tenant).await.ok()?;
        let lctn = self.lctn(&cat);
        let row = cat.database(&database_key(db, lctn))?;
        cat.ext
            .get::<DocState>(MODULE)?
            .of(row.db_id.0)
            .find(|m| m.table == table)
            .map(|m| m.uuid)
    }

    async fn images(&self, tenant: TenantId, db: &str, table: &str) -> bool {
        let Ok(cat) = self.snapshot(tenant).await else {
            return false;
        };
        let lctn = self.lctn(&cat);
        let Some(row) = cat.database(&database_key(db, lctn)) else {
            return false;
        };
        cat.ext
            .get::<DocState>(MODULE)
            .and_then(|s| s.of(row.db_id.0).find(|m| m.table == table).cloned())
            .is_some_and(|m| sprig_doc::meta::keeps_images(&m.options))
    }
}
