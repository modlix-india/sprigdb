//! Where the cache loads snapshots from: [`CatalogSource`], and
//! [`MariaSource`] over the tenants' catalog clusters.
//!
//! A load reads, in one consistent read-only transaction, the catalog
//! format (39073 if a module is newer than this binary reads), the rows
//! changed after the previous snapshot's version (every row for a first
//! load, or when the catalog was purged past it) and each versioned
//! module's state ([`CatalogExtension::load`]).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};

use super::ext::{CatalogExtension, SnapshotTx, module_of};
use super::snapshot::{Loaded, TenantCatalog};
use crate::migrate::{FORMAT_KEY, FormatState, Migrator, check_readable};
use crate::pool_wait::Bounded as _;
use crate::records::{RecordStore, RecordTx};
use crate::sql::{map_err, timed};
use crate::store::CatalogLocator;
use crate::store::rec::CoreRecords;
use crate::version::VersionKind;

/// Which node of the catalog's cluster a load reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Node {
    /// A healthy local replica, else the writer (the locator decides).
    Replica,
    /// The writer: when a replica lags past `catalog.version_wait_ms`, and
    /// after name misses.
    Writer,
}

impl Node {
    /// The metric label (`source` of `sprig_catalog_loads_total`).
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Node::Replica => "replica",
            Node::Writer => "writer",
        }
    }
}

/// Reads catalog snapshots for the cache.
#[async_trait::async_trait]
pub trait CatalogSource: Send + Sync + 'static {
    /// Reads the rows changed after `prev.version` (every row without
    /// `prev`) and the versioned modules' states in one consistent
    /// snapshot from `node`.
    ///
    /// # Errors
    /// 39070 if the catalog is unreachable; 39072 if the tenant has no
    /// catalog; 39073 if the catalog's format is too new.
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded>;

    /// The committed catalog and auth versions on `node`, read without a
    /// snapshot: one small query, so a loader waiting for a version polls
    /// this instead of loading in full again and again. `None` when the
    /// source can't tell cheaply (the loader then just loads).
    ///
    /// # Errors
    /// As [`load`](Self::load); the loader falls back to a full load.
    async fn committed(&self, _tenant: TenantId, _node: Node) -> Result<Option<(u64, u64)>> {
        Ok(None)
    }
}

/// Loads from the catalogs' MariaDB clusters through a [`CatalogLocator`].
pub struct MariaSource {
    locator: Arc<dyn CatalogLocator>,
    extensions: Vec<Arc<dyn CatalogExtension>>,
    supports: BTreeMap<String, u32>,
    timeout: Duration,
    records: Option<Arc<dyn RecordStore>>,
}

impl std::fmt::Debug for MariaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MariaSource")
            .field(
                "extensions",
                &self
                    .extensions
                    .iter()
                    .map(|e| e.module())
                    .collect::<Vec<_>>(),
            )
            .field("supports", &self.supports)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl MariaSource {
    /// Default deadline of one load.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

    /// A source over `locator`, loading the core rows and every versioned
    /// module of `extensions`.
    ///
    /// # Errors
    /// `Internal` if a module's migrations are malformed or a name repeats.
    pub fn new(
        locator: Arc<dyn CatalogLocator>,
        extensions: Vec<Arc<dyn CatalogExtension>>,
    ) -> Result<Self> {
        let modules: Vec<_> = extensions.iter().map(|e| module_of(&**e)).collect();
        let supports = Migrator::new(&modules)?.supports();
        Ok(Self {
            locator,
            extensions,
            supports,
            timeout: Self::DEFAULT_TIMEOUT,
            records: None,
        })
    }

    /// The same source reading the catalog from the engine's catalog
    /// records (`records`): the core rows, the versions and the modules'
    /// rows; MariaDB then serves only the catalog format and the MariaDB
    /// data paths' own tables.
    #[must_use]
    pub fn with_records(mut self, records: Arc<dyn RecordStore>) -> Self {
        self.records = Some(records);
        self
    }

    /// The same source with another deadline per load.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

impl MariaSource {
    async fn load_once(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded> {
        // On records the MariaDB part is read on the writer: the engine has
        // no replicas, and a lagging MariaDB replica would give a snapshot
        // stamped with the engine's version but without that version's
        // MariaDB rows.
        let ep = match node {
            Node::Replica if self.records.is_none() => self.locator.reader(tenant).await?,
            Node::Replica | Node::Writer => self.locator.writer(tenant).await?,
        };
        timed(self.timeout, "cache load", async {
            // On records, the core rows, the versions and every module's
            // rows come from one engine read view; the MariaDB snapshot
            // serves the catalog format and the MariaDB data paths' own
            // tables.
            //
            // The engine's read view opens at its first read, and it must
            // open before the MariaDB snapshot: a change commits its MariaDB
            // part first and the engine second, so a MariaDB snapshot taken
            // after the engine view holds at least that version's rows (a
            // newer change's rows at worst, which the next load of that
            // version reads again).
            let mut rec = match &self.records {
                Some(r) => {
                    let mut t = r.begin(tenant).await?;
                    CoreRecords::versions(&mut *t).await?;
                    Some(t)
                }
                None => None,
            };
            // The catalog format: the records' copy when the migrator
            // keeps one (`migrate::mirror_formats`), then no MariaDB
            // snapshot is opened; the MariaDB row otherwise.
            let rec_format = match rec.as_mut() {
                Some(r) => CoreRecords::internal(&mut **r, FORMAT_KEY).await?,
                None => None,
            };
            let mut tx = match rec_format {
                Some(_) => None,
                None => Some(ep.store.begin_snapshot(&ep.pool).await?),
            };
            let raw = match (rec_format, tx.as_mut()) {
                (Some(v), _) => Some(v),
                (None, Some(t)) => ep.store.internal(t, FORMAT_KEY).await?,
                (None, None) => None,
            };
            let format: FormatState = match raw {
                Some(v) => serde_json::from_value(v)
                    .map_err(|e| SprigError::internal(format!("bad {FORMAT_KEY}: {e}")))?,
                None => FormatState::new(),
            };
            check_readable(&format, &self.supports)?;
            let since = prev.map(|p| p.version);
            let snapshot = match (rec.as_mut(), tx.as_mut()) {
                (Some(r), _) => CoreRecords::snapshot(&mut **r, since).await?,
                (None, Some(t)) => ep.store.snapshot_in(t, since).await?,
                (None, None) => {
                    return Err(SprigError::internal("a catalog load without a source"));
                }
            };
            let full = snapshot.since.is_none();
            let mut ext = Vec::new();
            {
                let on_records = rec.is_some();
                let mut stx = SnapshotTx::new(
                    tx.as_mut(),
                    &ep.store,
                    snapshot.versions,
                    rec.as_mut()
                        .map(|r| &mut **r as &mut (dyn RecordTx + 'static)),
                )
                .with_rec_log(on_records);
                for e in &self.extensions {
                    let Some(kind) = e.versioned() else {
                        continue;
                    };
                    let prev_state = prev.filter(|_| !full).and_then(|p| p.ext.raw(e.module()));
                    let since = match (prev_state, prev) {
                        (Some(_), Some(p)) => Some(match kind {
                            VersionKind::Catalog => p.version,
                            VersionKind::Auth => p.auth_version,
                        }),
                        _ => None,
                    };
                    ext.push((e.module(), e.load(&mut stx, since, prev_state).await?));
                }
            }
            if let Some(t) = tx {
                t.commit().await.map_err(map_err)?;
            }
            if let Some(r) = rec {
                let _ = r.rollback().await;
            }
            Ok(Loaded { snapshot, ext })
        })
        .await
    }
}

#[async_trait::async_trait]
impl CatalogSource for MariaSource {
    async fn load(
        &self,
        tenant: TenantId,
        prev: Option<&TenantCatalog>,
        node: Node,
    ) -> Result<Loaded> {
        self.load_once(tenant, prev, node).await
    }

    async fn committed(&self, tenant: TenantId, node: Node) -> Result<Option<(u64, u64)>> {
        if let Some(r) = &self.records {
            return timed(self.timeout, "catalog versions", async {
                let mut rtx = r.begin(tenant).await?;
                let v = CoreRecords::versions(&mut *rtx).await;
                let _ = rtx.rollback().await;
                let v = v?;
                Ok(Some((v.catalog, v.auth)))
            })
            .await;
        }
        let ep = match node {
            Node::Replica => self.locator.reader(tenant).await?,
            Node::Writer => self.locator.writer(tenant).await?,
        };
        timed(self.timeout, "catalog versions", async {
            let mut conn = ep.pool.get_conn_bounded().await.map_err(map_err)?;
            let v = ep.store.versions(&mut conn).await?;
            Ok(Some((v.catalog, v.auth)))
        })
        .await
    }
}
