//! Search indexes in the tenant catalog: the `search` catalog module.
//!
//! - **Records.** The module's rows are the engine's catalog records:
//!
//!   ```text
//!   m/search/indexes/<index u64>        IndexRow (the definition and its columns)
//!   m/search/analyzers/<analyzer u32>   AnalyzerRow
//!   m/search/anames/<name>              analyzer id
//!   ```
//! - **Objects.** A search index is an object of kind `search_index`
//!   whose parent is the indexed table or collection, so its
//!   id is an `objects.object_id`.
//! - **Definitions.** An index row holds the normalized definition
//!   ([`IndexDef`] as JSON, with what was given); its fields are read
//!   from it.
//! - **Changes.** Creating or dropping an index or analyzer is one
//!   versioned catalog change (its records written through the change),
//!   so every gateway's snapshot reloads it. Indexes whose object was
//!   dropped are left out of the snapshot.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::SystemTime;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_catalog::cache::{ExtState, SnapshotTx};
use sprig_catalog::ddl::{DdlTx, LOWER_CASE_TABLE_NAMES, database_key, object_key};
use sprig_catalog::migrate::{MigrateOpts, Migration, Migrator, Module};
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_catalog::store::{Namespace, ObjectFlags, ObjectKind, ObjectRow, ObjectState};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogExtension, CatalogLocator as _, Freshness, TenantCatalog};
use sprig_common::ids::{DbId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_search::analyze::CustomAnalyzer;
use sprig_search::def::{Flavor, IndexDef};

use crate::tenant::TenantServices;

/// The module name.
pub const MODULE: &str = "search";

/// Indexes by id.
pub const INDEXES: RowTable = RowTable::new(MODULE, "indexes");
/// Custom analyzers by id.
pub const ANALYZERS: RowTable = RowTable::new(MODULE, "analyzers");
/// Analyzer ids by name.
pub const ANAMES: RowTable = RowTable::new(MODULE, "anames");

/// The module's MariaDB migrations: none, its rows are engine records.
pub static SEARCH_MIGRATIONS: &[Migration] = &[];

/// The `search` module, for [`Migrator::new`].
pub const SEARCH_MODULE: Module = Module {
    name: MODULE,
    migrations: SEARCH_MIGRATIONS,
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
                    "search catalog statement failed ({}): {}",
                    s.code, s.message
                ),
            )
        }
        _ => SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable"),
    }
    .with_source(e)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A search index: what `search_indexes` held (the per-field mirror,
/// `search_fields`, is the definition's `fields`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRow {
    /// Its id (its object's id).
    pub index_id: u64,
    /// Its database.
    pub db_id: u32,
    /// Its name.
    pub name: String,
    /// The indexed table or collection.
    pub object_id: u64,
    /// `table`, `collection` or `kv`.
    pub model: String,
    /// `sql`, `mongo_text`, `mongo_search` or `fulltext_compat`.
    pub flavor: String,
    /// The default analyzer.
    pub default_analyzer: String,
    /// Dynamic mapping.
    pub dynamic: bool,
    /// The normalized definition.
    pub definition: serde_json::Value,
    /// The definition's generation.
    pub generation: u32,
    /// `user`, `migrated` or `auto`.
    pub origin: String,
    /// `building`, `ready`, `rebuilding`, `failed` or `dropping`.
    pub state: String,
    /// Ms since the epoch.
    pub created_ms: u64,
    /// Ms since the epoch.
    pub updated_ms: u64,
}

/// A custom analyzer: what `search_analyzers` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyzerRow {
    /// Its name.
    pub name: String,
    /// The tokenizer.
    pub tokenizer: String,
    /// The tokenizer's options.
    pub tokenizer_opts: serde_json::Value,
    /// Token filters.
    pub filters: serde_json::Value,
    /// Synonyms.
    pub synonyms: serde_json::Value,
    /// Its version.
    pub version: u32,
    /// Ms since the epoch.
    pub created_ms: u64,
}

/// One tenant's search indexes and analyzers in a snapshot.
#[derive(Debug, Default)]
pub struct SearchState {
    /// Indexes by database id.
    pub by_db: HashMap<u32, Vec<Arc<IndexDef>>>,
    /// Custom analyzers.
    pub analyzers: Vec<CustomAnalyzer>,
}

impl ExtState for SearchState {
    fn bytes(&self) -> usize {
        self.by_db
            .values()
            .flatten()
            .map(|d| 512 + d.fields.len() * 128)
            .sum::<usize>()
            + self.analyzers.len() * 256
    }
}

impl SearchState {
    /// The indexes of database `db`.
    #[must_use]
    pub fn of_db(&self, db: u32) -> &[Arc<IndexDef>] {
        self.by_db.get(&db).map_or(&[], Vec::as_slice)
    }

    /// The indexes on `object` (by name) of database `db`.
    #[must_use]
    pub fn on_object(&self, db: u32, object: &str, lctn: bool) -> Vec<Arc<IndexDef>> {
        self.of_db(db)
            .iter()
            .filter(|d| {
                if lctn {
                    d.object.eq_ignore_ascii_case(object)
                } else {
                    d.object == object
                }
            })
            .cloned()
            .collect()
    }

    /// The index with id `index` (any database).
    #[must_use]
    pub fn by_id(&self, index: u64) -> Option<Arc<IndexDef>> {
        self.by_db
            .values()
            .flatten()
            .find(|d| d.index_id == index)
            .cloned()
    }

    /// Index `name` of database `db`.
    #[must_use]
    pub fn by_name(&self, db: u32, name: &str) -> Option<Arc<IndexDef>> {
        self.of_db(db)
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(name))
            .cloned()
    }
}

/// The module's rows, read and written as catalog records.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchTables;

impl SearchTables {
    /// Makes sure catalog `schema` can take the module's records (its core
    /// migrations, which create the record log).
    ///
    /// # Errors
    /// The migrator's.
    pub async fn migrate(q: &mut mysql_async::Conn, schema: &str) -> Result<()> {
        Migrator::new(&[SEARCH_MODULE])?
            .run(q, schema, &MigrateOpts::existing())
            .await?;
        Ok(())
    }

    /// Every index row, by id.
    ///
    /// # Errors
    /// The record store's.
    pub async fn indexes(rec: &mut dyn RecordTx) -> Result<Vec<IndexRow>> {
        INDEXES.rows(rec, &Pk::new()).await
    }

    /// Every index (but those being dropped) and every analyzer;
    /// `live` says which indexed objects are still there.
    ///
    /// # Errors
    /// The record store's.
    pub async fn load(rec: &mut dyn RecordTx, live: impl Fn(u64) -> bool) -> Result<SearchState> {
        let mut st = SearchState::default();
        for r in Self::indexes(rec).await? {
            if r.state == "dropping" || !live(r.object_id) {
                continue;
            }
            match serde_json::from_value::<IndexDef>(r.definition) {
                Ok(mut d) => {
                    d.index_id = r.index_id;
                    st.by_db.entry(r.db_id).or_default().push(Arc::new(d));
                }
                Err(e) => {
                    tracing::warn!(index = r.index_id, error = %e, "unreadable search index definition; skipped");
                }
            }
        }
        for a in ANALYZERS.rows::<AnalyzerRow>(rec, &Pk::new()).await? {
            st.analyzers.push(CustomAnalyzer {
                name: a.name,
                tokenizer: a.tokenizer,
                tokenizer_opts: a.tokenizer_opts,
                filters: serde_json::from_value(a.filters).unwrap_or_default(),
                synonyms: serde_json::from_value(a.synonyms).unwrap_or_default(),
                version: a.version,
            });
        }
        Ok(st)
    }

    /// Writes index `d` of database `db` (a new one, or a new generation).
    ///
    /// # Errors
    /// The record store's.
    pub async fn put_index(
        rec: &mut dyn RecordTx,
        db: u32,
        d: &IndexDef,
        origin: &str,
    ) -> Result<()> {
        let pk = Pk::new().u64(d.index_id);
        let now = now_ms();
        let created_ms = INDEXES
            .get::<IndexRow>(rec, &pk)
            .await?
            .map_or(now, |r| r.created_ms);
        let row = IndexRow {
            index_id: d.index_id,
            db_id: db,
            name: d.name.clone(),
            object_id: d.object_id,
            model: d.model.as_str().to_owned(),
            flavor: d.flavor.as_str().to_owned(),
            default_analyzer: d.default_analyzer.clone(),
            dynamic: d.dynamic,
            definition: serde_json::to_value(d).map_err(|e| SprigError::internal(e.to_string()))?,
            generation: d.generation,
            origin: origin.to_owned(),
            state: "ready".to_owned(),
            created_ms,
            updated_ms: now,
        };
        INDEXES.put(rec, &pk, &row).await
    }

    /// Deletes index `id`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn delete_index(rec: &mut dyn RecordTx, id: u64) -> Result<()> {
        INDEXES.delete(rec, &Pk::new().u64(id)).await
    }

    /// Adds custom analyzer `a`: whether it was added (`false`: the name
    /// is taken).
    ///
    /// # Errors
    /// The record store's.
    pub async fn add_analyzer(rec: &mut dyn RecordTx, a: &CustomAnalyzer) -> Result<bool> {
        let npk = Pk::new().bytes(a.name.as_bytes());
        if ANAMES.get::<u32>(rec, &npk).await?.is_some() {
            return Ok(false);
        }
        let next = ANALYZERS
            .scan::<serde_json::Value>(rec, &Pk::new())
            .await?
            .iter()
            .filter_map(|(pk, _)| <[u8; 4]>::try_from(pk.0.as_slice()).ok())
            .map(u32::from_be_bytes)
            .max()
            .unwrap_or(0)
            + 1;
        let row = AnalyzerRow {
            name: a.name.clone(),
            tokenizer: a.tokenizer.clone(),
            tokenizer_opts: a.tokenizer_opts.clone(),
            filters: serde_json::to_value(&a.filters).unwrap_or_default(),
            synonyms: serde_json::to_value(&a.synonyms).unwrap_or_default(),
            version: a.version,
            created_ms: now_ms(),
        };
        ANALYZERS.put(rec, &Pk::new().u32(next), &row).await?;
        ANAMES.put(rec, &npk, &next).await?;
        Ok(true)
    }

    /// Removes custom analyzer `name`: whether it existed.
    ///
    /// # Errors
    /// The record store's.
    pub async fn remove_analyzer(rec: &mut dyn RecordTx, name: &str) -> Result<bool> {
        let npk = Pk::new().bytes(name.as_bytes());
        let Some(id) = ANAMES.get::<u32>(rec, &npk).await? else {
            return Ok(false);
        };
        ANAMES.delete(rec, &npk).await?;
        ANALYZERS.delete(rec, &Pk::new().u32(id)).await?;
        Ok(true)
    }

    /// The name of an index that uses analyzer `name` (as a field's
    /// analyzer or query analyzer, or as its default).
    ///
    /// # Errors
    /// The record store's.
    pub async fn analyzer_user(rec: &mut dyn RecordTx, name: &str) -> Result<Option<String>> {
        for r in Self::indexes(rec).await? {
            if r.default_analyzer == name {
                return Ok(Some(r.name));
            }
            if let Ok(d) = serde_json::from_value::<IndexDef>(r.definition)
                && d.fields.iter().any(|f| {
                    f.analyzer.as_deref() == Some(name) || f.query_analyzer.as_deref() == Some(name)
                })
            {
                return Ok(Some(r.name));
            }
        }
        Ok(None)
    }
}

/// Objects whose indexes are still served (not dropping or dropped).
fn serving(state: ObjectState) -> bool {
    !matches!(state, ObjectState::Dropping | ObjectState::Dropped)
}

/// The catalog extension (loads [`SearchState`] into every snapshot).
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchExtension;

#[async_trait]
impl CatalogExtension for SearchExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        SEARCH_MIGRATIONS
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
            return Ok(Arc::new(SearchState::default()));
        }
        let objects: Vec<ObjectId> = SearchTables::indexes(tx.records()?)
            .await?
            .iter()
            .map(|r| ObjectId(r.object_id))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let live: HashSet<u64> = tx
            .objects_by_ids(&objects)
            .await?
            .into_iter()
            .filter(|o| serving(o.state))
            .map(|o| o.object_id.0)
            .collect();
        Ok(Arc::new(
            SearchTables::load(tx.records()?, |o| live.contains(&o)).await?,
        ))
    }
}

/// Reads and changes the module through the tenant services.
pub struct SearchCatalog {
    svc: Arc<TenantServices>,
    migrated: Mutex<std::collections::HashSet<u64>>,
}

impl std::fmt::Debug for SearchCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchCatalog").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An empty state for snapshots loaded before the module existed.
static EMPTY: std::sync::LazyLock<SearchState> = std::sync::LazyLock::new(SearchState::default);

/// The module's state in a snapshot (empty when the tenant has none).
#[must_use]
pub fn state(cat: &TenantCatalog) -> &SearchState {
    cat.ext.get::<SearchState>(MODULE).unwrap_or(&EMPTY)
}

impl SearchCatalog {
    /// Over the tenant services.
    #[must_use]
    pub fn new(svc: Arc<TenantServices>) -> Self {
        Self {
            svc,
            migrated: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// The tenant services.
    #[must_use]
    pub fn services(&self) -> &Arc<TenantServices> {
        &self.svc
    }

    async fn prepare(&self, tenant: TenantId) -> Result<()> {
        if lock(&self.migrated).contains(&tenant.0) {
            return Ok(());
        }
        let ep = self.svc.locator.writer(tenant).await?;
        // A tenant whose catalog is engine records only needs no MariaDB
        // connection: the module has no MariaDB tables.
        if !Migrator::new(&[SEARCH_MODULE])?
            .skips(ep.store.schema())
            .await?
        {
            let mut conn = ep.pool.get_conn().await.map_err(map_err)?;
            SearchTables::migrate(&mut conn, ep.store.schema()).await?;
        }
        lock(&self.migrated).insert(tenant.0);
        Ok(())
    }

    /// Whether the tenant's names are case-insensitive.
    #[must_use]
    pub fn lctn(&self, cat: &TenantCatalog) -> bool {
        self.svc.settings.get(
            &LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        ) == 1
    }

    /// The newest snapshot.
    ///
    /// # Errors
    /// The cache's.
    pub async fn snapshot(&self, tenant: TenantId, min_version: u64) -> Result<Arc<TenantCatalog>> {
        let cat = self.svc.cache.get(tenant, Freshness::Latest).await?;
        if cat.version >= min_version {
            return Ok(cat);
        }
        self.svc
            .cache
            .get(tenant, Freshness::Cached { min_version })
            .await
    }

    /// Creates index `d` on the object `object_id` of database `db_id`;
    /// fills in its id. Returns the committed version, or `None` when an
    /// index by that name exists (and `if_not_exists`).
    ///
    /// # Errors
    /// 1061 (`ER_DUP_KEYNAME`) for an existing name; 39830 over
    /// `max_per_object`; catalog errors.
    pub async fn create(
        &self,
        tenant: TenantId,
        db_id: DbId,
        d: &mut IndexDef,
        if_not_exists: bool,
        max_per_object: usize,
        origin: &str,
    ) -> Result<Option<u64>> {
        self.prepare(tenant).await?;
        let cat = self.snapshot(tenant, 0).await?;
        let lctn = self.lctn(&cat);
        let object_id = self.svc.catalog.backend().next_object_id(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r: Result<bool> = async {
            let tx = ch.tx();
            // Indexes whose object was dropped go first (their names can be
            // used again).
            let mine: Vec<IndexRow> = SearchTables::indexes(tx.records().await?)
                .await?
                .into_iter()
                .filter(|r| r.db_id == db_id.0)
                .collect();
            let parents: Vec<ObjectId> = mine
                .iter()
                .map(|r| ObjectId(r.object_id))
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            let live: HashSet<u64> = tx
                .objects_by_ids(&parents)
                .await?
                .into_iter()
                .filter(|o| serving(o.state))
                .map(|o| o.object_id.0)
                .collect();
            let mut existing: Vec<(u64, String, u64)> = Vec::new();
            for r in mine {
                if live.contains(&r.object_id) {
                    existing.push((r.index_id, r.name, r.object_id));
                } else {
                    SearchTables::delete_index(tx.records().await?, r.index_id).await?;
                    // A tombstone, as for any dropped object: caches that
                    // load incrementally see it, and purge removes it.
                    tx.set_object_state(ObjectId(r.index_id), ObjectState::Dropped, version)
                        .await?;
                }
            }
            // SQL names are unique per database; MongoDB's per collection.
            let per_object = matches!(d.flavor, Flavor::MongoText | Flavor::MongoSearch);
            if existing.iter().any(|(_, n, o)| {
                n.eq_ignore_ascii_case(&d.name) && (!per_object || *o == d.object_id)
            }) {
                if if_not_exists {
                    return Ok(false);
                }
                return Err(SprigError::standard(
                    1061,
                    *b"42000",
                    format!("Duplicate key name '{}'", d.name),
                ));
            }
            if existing.iter().filter(|(_, _, o)| *o == d.object_id).count() >= max_per_object {
                return Err(SprigError::new(
                    SprigCode::SearchIndexLimit,
                    format!(
                        "'{}' already has {max_per_object} search indexes (search.max_indexes_per_object)",
                        d.object
                    ),
                ));
            }
            d.index_id = object_id.0;
            let obj = ObjectRow {
                object_id,
                db_id,
                namespace: Namespace::SearchIndex,
                name: d.name.clone(),
                name_key: if matches!(d.flavor, Flavor::MongoText | Flavor::MongoSearch) {
                    object_key(
                        ObjectKind::SearchIndex,
                        &format!("{}${}", d.object_id, d.name),
                        lctn,
                    )
                } else {
                    object_key(ObjectKind::SearchIndex, &d.name, lctn)
                },
                kind: ObjectKind::SearchIndex,
                parent_id: Some(ObjectId(d.object_id)),
                state: ObjectState::Active,
                schema_version: 1,
                flags: ObjectFlags::default(),
                pending_job_id: None,
                created_at: SystemTime::now(),
                changed_version: version,
            };
            DdlTx::insert_object(tx, &obj).await?;
            SearchTables::put_index(tx.records().await?, db_id.0, d, origin).await?;
            Ok(true)
        }
        .await;
        match r {
            Ok(true) => Ok(Some(ch.commit().await?)),
            Ok(false) => {
                let _ = ch.rollback().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Replaces index `d`'s definition (a new generation, or a renamed
    /// object). Returns the committed version.
    ///
    /// # Errors
    /// Catalog errors.
    pub async fn replace(&self, tenant: TenantId, db_id: DbId, d: &IndexDef) -> Result<u64> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let r: Result<()> = async {
            let rec = ch.tx().records().await?;
            SearchTables::put_index(rec, db_id.0, d, "user").await
        }
        .await;
        match r {
            Ok(()) => ch.commit().await,
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Drops index `id`. Returns the committed version.
    ///
    /// # Errors
    /// Catalog errors.
    pub async fn drop_index(&self, tenant: TenantId, id: u64) -> Result<u64> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let version = ch.version();
        let r: Result<()> = async {
            let tx = ch.tx();
            SearchTables::delete_index(tx.records().await?, id).await?;
            // A tombstone, not a delete: the index's object row was visible,
            // and a cache loading incrementally only sees rows that changed.
            tx.set_object_state(ObjectId(id), ObjectState::Dropped, version)
                .await?;
            Ok(())
        }
        .await;
        match r {
            Ok(()) => ch.commit().await,
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Creates or drops a custom analyzer. Returns the committed version,
    /// or `None` when nothing changed (`IF [NOT] EXISTS`).
    ///
    /// # Errors
    /// 39828 / 1061 for a missing or duplicate name; catalog errors.
    pub async fn analyzer(
        &self,
        tenant: TenantId,
        create: Option<&CustomAnalyzer>,
        drop_name: Option<&str>,
        lenient: bool,
    ) -> Result<Option<u64>> {
        self.prepare(tenant).await?;
        let mut ch = self
            .svc
            .catalog
            .change(tenant, VersionKind::Catalog)
            .await?;
        let r: Result<bool> = async {
            let rec = ch.tx().records().await?;
            if let Some(a) = create {
                if !SearchTables::add_analyzer(rec, a).await? {
                    if lenient {
                        return Ok(false);
                    }
                    return Err(SprigError::standard(
                        1061,
                        *b"42000",
                        format!("search analyzer '{}' already exists", a.name),
                    ));
                }
                return Ok(true);
            }
            if let Some(n) = drop_name {
                if let Some(i) = SearchTables::analyzer_user(rec, n).await? {
                    return Err(SprigError::new(
                        SprigCode::SearchFieldType,
                        format!("search analyzer '{n}' is used by search index '{i}'"),
                    ));
                }
                if !SearchTables::remove_analyzer(rec, n).await? {
                    if lenient {
                        return Ok(false);
                    }
                    return Err(SprigError::new(
                        SprigCode::SearchAnalyzerUnknown,
                        format!("search analyzer '{n}' doesn't exist"),
                    ));
                }
                return Ok(true);
            }
            Ok(false)
        }
        .await;
        match r {
            Ok(true) => Ok(Some(ch.commit().await?)),
            Ok(false) => {
                let _ = ch.rollback().await;
                Ok(None)
            }
            Err(e) => {
                let _ = ch.rollback().await;
                Err(e)
            }
        }
    }

    /// Database `db`'s row id, by name.
    #[must_use]
    pub fn db_id(&self, cat: &TenantCatalog, db: &str) -> Option<DbId> {
        cat.database(&database_key(db, self.lctn(cat)))
            .map(|r| r.db_id)
    }

    /// The table `name` of database `db_id`: its object id.
    #[must_use]
    pub fn table_id(&self, cat: &TenantCatalog, db_id: DbId, name: &str) -> Option<u64> {
        cat.object(
            db_id,
            Namespace::Relation,
            &object_key(ObjectKind::Table, name, self.lctn(cat)),
        )
        .map(|o| o.object_id.0)
    }
}

#[cfg(test)]
mod tests {
    use sprig_catalog::records::RecordStore;
    use sprig_catalog::records::mem::MemRecords;
    use sprig_search::def::{FieldDef, FieldKind, Model, Source};

    use super::*;

    const T: TenantId = TenantId(57_406_000_003);

    fn def(id: u64, name: &str, object_id: u64, analyzer: Option<&str>) -> IndexDef {
        let mut d = IndexDef::new(name, "db", "t", Model::Table, Flavor::Sql);
        d.index_id = id;
        d.object_id = object_id;
        let mut f = FieldDef::new("body", Source::Column, FieldKind::Text);
        f.analyzer = analyzer.map(str::to_owned);
        d.push(f).unwrap();
        d
    }

    fn analyzer(name: &str) -> CustomAnalyzer {
        CustomAnalyzer {
            name: name.into(),
            tokenizer: "standard".into(),
            tokenizer_opts: serde_json::json!({"max": 5}),
            filters: vec!["lowercase".into()],
            synonyms: vec![vec!["car".into(), "auto".into()]],
            version: 1,
        }
    }

    #[tokio::test]
    async fn indexes_and_analyzers_round_trip() {
        let store = MemRecords::new();
        let mut tx = store.begin(T).await.unwrap();
        SearchTables::put_index(&mut *tx, 1, &def(10, "a", 100, Some("mine")), "user")
            .await
            .unwrap();
        SearchTables::put_index(&mut *tx, 2, &def(11, "b", 101, None), "auto")
            .await
            .unwrap();
        assert!(
            SearchTables::add_analyzer(&mut *tx, &analyzer("mine"))
                .await
                .unwrap()
        );
        assert!(
            !SearchTables::add_analyzer(&mut *tx, &analyzer("mine"))
                .await
                .unwrap()
        );
        assert!(
            SearchTables::add_analyzer(&mut *tx, &analyzer("other"))
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();

        let mut tx = store.begin(T).await.unwrap();
        // Indexes on dropped objects are left out.
        let st = SearchTables::load(&mut *tx, |o| o != 101).await.unwrap();
        assert_eq!(st.of_db(1).len(), 1);
        assert!(st.of_db(2).is_empty());
        assert_eq!(*st.of_db(1)[0], def(10, "a", 100, Some("mine")));
        assert_eq!(st.analyzers.len(), 2);
        assert_eq!(st.analyzers[0], analyzer("mine"));
        // An analyzer in use can't go; one that isn't can, once.
        assert_eq!(
            SearchTables::analyzer_user(&mut *tx, "mine").await.unwrap(),
            Some("a".into())
        );
        assert_eq!(
            SearchTables::analyzer_user(&mut *tx, "other")
                .await
                .unwrap(),
            None
        );
        assert!(
            SearchTables::remove_analyzer(&mut *tx, "other")
                .await
                .unwrap()
        );
        assert!(
            !SearchTables::remove_analyzer(&mut *tx, "other")
                .await
                .unwrap()
        );
        // A new generation keeps the creation time; a drop removes it.
        let mut d = def(10, "a", 100, None);
        d.generation = 2;
        let before = INDEXES
            .get::<IndexRow>(&mut *tx, &Pk::new().u64(10))
            .await
            .unwrap()
            .unwrap();
        SearchTables::put_index(&mut *tx, 1, &d, "user")
            .await
            .unwrap();
        let after = INDEXES
            .get::<IndexRow>(&mut *tx, &Pk::new().u64(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((after.created_ms, after.generation), (before.created_ms, 2));
        SearchTables::delete_index(&mut *tx, 11).await.unwrap();
        tx.commit().await.unwrap();
        let mut tx = store.begin(T).await.unwrap();
        let st = SearchTables::load(&mut *tx, |_| true).await.unwrap();
        assert_eq!(st.by_db.values().map(Vec::len).sum::<usize>(), 1);
        assert_eq!(st.by_id(10).unwrap().generation, 2);
        assert_eq!(st.analyzers.len(), 1);
        // Analyzer ids are `MAX + 1`, as the table's were.
        assert!(
            SearchTables::add_analyzer(&mut *tx, &analyzer("third"))
                .await
                .unwrap()
        );
        let ids: Vec<Pk> = ANALYZERS
            .scan::<AnalyzerRow>(&mut *tx, &Pk::new())
            .await
            .unwrap()
            .into_iter()
            .map(|(pk, _)| pk)
            .collect();
        assert_eq!(ids, vec![Pk::new().u32(1), Pk::new().u32(2)]);
    }
}
