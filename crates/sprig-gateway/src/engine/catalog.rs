//! The engine's databases and its tables' definitions, as a catalog
//! module loaded into every tenant snapshot.
//!
//! **Every database is an engine database.** The engine replaced MariaDB
//! as the storage of every table; the tenant's databases are its live
//! databases in the catalog.
//!
//! **Definitions.** The engine produces every table's definition itself
//! (`define`), in MariaDB 11.8's `SHOW CREATE TABLE` form, and the
//! catalog's DDL path stores it as the text of the table's schema version.
//! The engine reads each table's definition from that text, here, once per
//! catalog load. No table exists on MariaDB.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sprig_catalog::store::{DbState, ObjectKind, ObjectState};
use sprig_catalog::{
    CatalogExtension, ExtState, Migration, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::Result;
use sprig_common::ids::{DbId, ObjectId};

use super::table::{self, Parsed};

/// The module's name.
pub(crate) const MODULE: &str = "engine_e0";

/// One tenant's engine databases and tables at one catalog version.
#[derive(Debug, Default, Clone)]
pub(crate) struct EngineCatalog {
    /// The tenant's live databases (every one is the engine's).
    pub dbs: HashSet<DbId>,
    /// Tables of those databases by object, with their definitions.
    pub tables: HashMap<ObjectId, Arc<Parsed>>,
    /// Each table's definition text, as the catalog stores it.
    pub texts: HashMap<ObjectId, Arc<str>>,
    /// The same by (database, name as the catalog stores it).
    pub by_name: HashMap<(DbId, String), ObjectId>,
    /// The same by (database, lower-case name), for tenants whose names
    /// compare in lower case (`lower_case_table_names`).
    pub by_lower: HashMap<(DbId, String), ObjectId>,
    /// The foreign keys by the lower-case name of the table they reference:
    /// each child table and the key's position in its definition.
    pub refs: HashMap<String, Vec<(ObjectId, usize)>>,
    /// The views of those databases by (database, name as stored); one the
    /// engine can't read is kept as its error.
    pub views: HashMap<(DbId, String), Arc<std::result::Result<super::view::ViewDef, String>>>,
    /// The same by (database, lower-case name).
    pub views_lower: HashMap<(DbId, String), (DbId, String)>,
    /// Each database's default character set and collation (empty when
    /// it has none recorded).
    pub charsets: HashMap<DbId, (String, String)>,
    /// The sequences (MariaDB's) by object.
    pub seq_by_object: HashMap<ObjectId, Arc<super::sequence::SeqEntry>>,
    /// The same by (database, name as stored).
    pub seq_names: HashMap<(DbId, String), ObjectId>,
    /// The same by (database, lower-case name).
    pub seq_lower: HashMap<(DbId, String), ObjectId>,
    /// The session's temporary tables, in a session's own copy of this
    /// state (`super::temp`); empty in the catalog's.
    pub temps: HashSet<ObjectId>,
}

impl ExtState for EngineCatalog {
    fn bytes(&self) -> usize {
        64 + 512 * self.tables.len() + self.texts.values().map(|t| t.len()).sum::<usize>()
    }
}

impl EngineCatalog {
    /// The table `name` of database `db`. Names compare in lower case when
    /// `lower_case` (the tenant's `lower_case_table_names`), and exactly as
    /// written otherwise, as on the tenant's MariaDB databases.
    pub(crate) fn table(&self, db: DbId, name: &str, lower_case: bool) -> Option<&Arc<Parsed>> {
        let o = if lower_case {
            self.by_lower.get(&(db, name.to_lowercase()))?
        } else {
            self.by_name.get(&(db, name.to_owned()))?
        };
        self.tables.get(o)
    }

    /// The sequence `name` of database `db` (names compare as for tables).
    pub(crate) fn sequence(
        &self,
        db: DbId,
        name: &str,
        lower_case: bool,
    ) -> Option<&Arc<super::sequence::SeqEntry>> {
        let o = if lower_case {
            self.seq_lower.get(&(db, name.to_lowercase()))?
        } else {
            self.seq_names.get(&(db, name.to_owned()))?
        };
        self.seq_by_object.get(o)
    }

    /// The view `name` of database `db` (names compare as for tables).
    pub(crate) fn view(
        &self,
        db: DbId,
        name: &str,
        lower_case: bool,
    ) -> Option<&Arc<std::result::Result<super::view::ViewDef, String>>> {
        if lower_case {
            let k = self.views_lower.get(&(db, name.to_lowercase()))?;
            self.views.get(k)
        } else {
            self.views.get(&(db, name.to_owned()))
        }
    }
}

/// The module state of `cat`, when the tenant has databases.
pub(crate) fn of(cat: &TenantCatalog) -> Option<&EngineCatalog> {
    cat.ext
        .get::<EngineCatalog>(MODULE)
        .filter(|e| !e.dbs.is_empty())
}

/// The catalog module.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct EngineExtension;

#[async_trait::async_trait]
impl CatalogExtension for EngineExtension {
    fn module(&self) -> &'static str {
        MODULE
    }

    fn migrations(&self) -> &'static [Migration] {
        &[]
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
        // The snapshot's core rows (engine catalog records).
        let mut state = EngineCatalog::default();
        let mut db_names: HashMap<DbId, String> = HashMap::new();
        for d in tx.databases().await? {
            if d.state != DbState::Dropped {
                state.dbs.insert(d.db_id);
                db_names.insert(d.db_id, d.name.clone());
                state.charsets.insert(
                    d.db_id,
                    (d.default_charset.clone(), d.default_collation.clone()),
                );
            }
        }
        if state.dbs.is_empty() {
            return Ok(Arc::new(state));
        }
        let mut rows: Vec<(ObjectId, DbId, String, u64, Option<String>)> = Vec::new();
        let mut views: Vec<(ObjectId, DbId, String, Option<String>)> = Vec::new();
        for o in tx.objects().await? {
            if o.kind == ObjectKind::Sequence
                && o.state == ObjectState::Active
                && state.dbs.contains(&o.db_id)
            {
                let text = tx
                    .schema_version(o.object_id, o.schema_version)
                    .await?
                    .and_then(|v| v.create_sql)
                    .unwrap_or_default();
                let def = super::sequence::load(&text);
                state
                    .seq_lower
                    .insert((o.db_id, o.name.to_lowercase()), o.object_id);
                state.seq_names.insert((o.db_id, o.name), o.object_id);
                state.seq_by_object.insert(
                    o.object_id,
                    Arc::new(super::sequence::SeqEntry {
                        object: o.object_id,
                        version: o.schema_version,
                        def,
                        text,
                    }),
                );
                continue;
            }
            if o.kind == ObjectKind::View
                && o.state == ObjectState::Active
                && state.dbs.contains(&o.db_id)
            {
                let create_sql = tx
                    .schema_version(o.object_id, o.schema_version)
                    .await?
                    .and_then(|v| v.create_sql);
                views.push((o.object_id, o.db_id, o.name, create_sql));
                continue;
            }
            if o.kind != ObjectKind::Table
                || o.state != ObjectState::Active
                || !state.dbs.contains(&o.db_id)
            {
                continue;
            }
            let create_sql = tx
                .schema_version(o.object_id, o.schema_version)
                .await?
                .and_then(|v| v.create_sql);
            rows.push((o.object_id, o.db_id, o.name, o.schema_version, create_sql));
        }
        for (object, db, name, version, create_sql) in rows {
            if let Some(sql) = &create_sql {
                state.texts.insert(object, Arc::from(sql.as_str()));
            }
            let parsed = match create_sql {
                Some(sql) => table::parse(object, version, db, &sql),
                None => Err("table definition not served by the engine: no definition text".into()),
            };
            state.by_lower.insert((db, name.to_lowercase()), object);
            state.by_name.insert((db, name), object);
            if let Ok(def) = &parsed {
                for (i, f) in def.fks.iter().enumerate() {
                    state
                        .refs
                        .entry(f.ref_table.to_lowercase())
                        .or_default()
                        .push((object, i));
                }
            }
            state.tables.insert(object, Arc::new(parsed));
        }
        for (object, db, name, create_sql) in views {
            let db_name = db_names.get(&db).cloned().unwrap_or_default();
            let v = match create_sql {
                Some(sql) => super::view::load(object, db, &db_name, &sql).map_err(|e| e.message),
                None => Err("view definition not served by the engine: no definition text".into()),
            };
            state
                .views_lower
                .insert((db, name.to_lowercase()), (db, name.clone()));
            state.views.insert((db, name), Arc::new(v));
        }
        Ok(Arc::new(state))
    }
}
