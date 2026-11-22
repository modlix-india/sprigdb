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

use sprig_catalog::{
    CatalogExtension, ExtState, Migration, SnapshotTx, TenantCatalog, VersionKind,
};
use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};

use super::table::{self, Parsed};

/// The module's name.
pub(crate) const MODULE: &str = "engine_e0";

/// One tenant's engine databases and tables at one catalog version.
#[derive(Debug, Default)]
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

#[allow(clippy::needless_pass_by_value)] // used as `map_err(db_err)`
fn db_err(e: mysql_async::Error) -> SprigError {
    SprigError::internal(format!("engine catalog module: {e}"))
}

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
        use mysql_async::prelude::Queryable;
        let schema = tx.schema().replace('`', "``");
        let conn = tx.conn();
        let live: Vec<u64> = conn
            .query(format!(
                "SELECT db_id FROM `{schema}`.databases WHERE state <> 'dropped'"
            ))
            .await
            .map_err(db_err)?;
        let mut state = EngineCatalog::default();
        for id in live {
            if let Ok(id) = u32::try_from(id) {
                state.dbs.insert(DbId(id));
            }
        }
        if state.dbs.is_empty() {
            return Ok(Arc::new(state));
        }
        let ids: Vec<String> = state.dbs.iter().map(|d| d.0.to_string()).collect();
        let rows: Vec<(u64, u32, String, u64, Option<String>)> = conn
            .query(format!(
                "SELECT o.object_id, o.db_id, o.name, o.schema_version, v.create_sql \
                 FROM `{schema}`.objects o \
                 LEFT JOIN `{schema}`.schema_versions v \
                   ON v.object_id = o.object_id AND v.version = o.schema_version \
                 WHERE o.kind = 'table' AND o.state = 'active' AND o.db_id IN ({})",
                ids.join(",")
            ))
            .await
            .map_err(db_err)?;
        for (object, db, name, version, create_sql) in rows {
            let (object, db) = (ObjectId(object), DbId(db));
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
        Ok(Arc::new(state))
    }
}
