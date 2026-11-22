//! Automatic document indexes in the catalog.
//!
//! An automatic index on a collection table (`sprig_advisor::autoindex::doc`)
//! is built like any other: field columns, then the MariaDB index, on
//! every shard. The document engine plans with an index only once it is a
//! registered index (a catalog record, `m/doc/indexes`), so [`DocRegistry::reconcile`] keeps those rows in
//! step with `auto_indexes`, at the start of each `auto_index` pass:
//!
//! ```text
//! wanted     := document indexes in observing or active (built everywhere)
//! registered := index records with origin 'auto'
//! wanted − registered → an index record (origin auto, auto_index_id,
//!                        mariadb_name sprig_auto_<hex>, the next index id),
//!                        field records for its paths
//! registered − wanted → its index record deleted (retiring, dropping,
//!                        failed or gone): the engine stops planning with it
//!                        before the index is dropped
//! all of it in one versioned catalog change, so every gateway reloads it
//! ```
//!
//! The field columns stay when an index is unregistered (retiring drops
//! only the index); the engine reads them only for paths
//! some registered index holds.

use std::collections::{BTreeMap, BTreeSet};

use sprig_advisor::autoindex::doc;
use sprig_advisor::autoindex::model::IndexKind;
use sprig_advisor::autoindex::store::{AutoIndexRow, IndexState};
use sprig_catalog::{MariaBackend, VersionKind, Versioner};
use sprig_common::Result;
use sprig_common::ids::TenantId;
use sprig_doc::catalog_records::DocTables;

/// The paths of a document index row, `None` for any other row.
#[must_use]
pub fn paths(row: &AutoIndexRow) -> Option<Vec<(String, i32)>> {
    let def = row.definition.on(row.object, IndexKind::FieldColumn);
    doc::is_doc(&def).then(|| doc::paths_of(&def))
}

/// A document index to register: its row and its paths.
type Wanted<'a> = (&'a AutoIndexRow, Vec<(String, i32)>);

/// Keeps `doc_indexes` in step with automatic document indexes.
#[derive(Clone)]
pub struct DocRegistry {
    versioner: Versioner<MariaBackend>,
}

impl std::fmt::Debug for DocRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DocRegistry")
    }
}

impl DocRegistry {
    /// A registry changing tenant catalogs through `versioner`.
    #[must_use]
    pub fn new(versioner: Versioner<MariaBackend>) -> Self {
        Self { versioner }
    }

    /// Registers and unregisters (module docs); returns how many rows it
    /// changed. `rows` are the tenant's live `auto_indexes` rows.
    ///
    /// # Errors
    /// The catalog's.
    #[allow(clippy::too_many_lines)]
    pub async fn reconcile(&self, tenant: TenantId, rows: &[AutoIndexRow]) -> Result<u32> {
        let wanted: BTreeMap<u64, Wanted<'_>> = rows
            .iter()
            .filter(|r| matches!(r.state, IndexState::Observing | IndexState::Active))
            .filter_map(|r| Some((r.index_id.0, (r, paths(r)?))))
            .collect();
        let mut ch = self.versioner.lock(tenant, VersionKind::Catalog).await?;
        let r: Result<u32> = async {
            let rec = ch.tx().records().await?;
            let registered = DocTables::auto_indexes(rec).await?;
            let have: BTreeSet<u64> = registered.iter().filter_map(|(_, _, a)| *a).collect();
            let mut changed = 0u32;
            for (cid, iid, auto) in &registered {
                if auto.is_some_and(|a| wanted.contains_key(&a)) {
                    continue;
                }
                DocTables::delete_index(rec, *cid, *iid).await?;
                tracing::info!(%tenant, collection = cid, index = iid,
                    "auto_index: document index unregistered");
                changed += 1;
            }
            for (id, (row, ps)) in &wanted {
                if have.contains(id) {
                    continue;
                }
                let cid = row.object.0;
                // Not a collection (any more): nothing to register.
                if let Some(next) = DocTables::register_auto(rec, cid, &row.name, *id, ps).await? {
                    tracing::info!(%tenant, collection = cid, index = next, name = %row.name,
                        "auto_index: document index registered");
                    changed += 1;
                }
            }
            Ok(changed)
        }
        .await;
        match r {
            Ok(0) => {
                let _ = ch.release().await;
                Ok(0)
            }
            Ok(n) => {
                let v = ch
                    .allocate(sprig_catalog::ChangeOpts::default())
                    .await?
                    .commit()
                    .await?;
                tracing::info!(%tenant, version = v, changed = n, "auto_index: document indexes published");
                Ok(n)
            }
            Err(e) => {
                let _ = ch.release().await;
                Err(e)
            }
        }
    }
}
