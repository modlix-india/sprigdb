//! How the controller reaches tenant catalogs.
//!
//! A tenant's catalog is shard 0 of table group 0: the shard map
//! `shardmaps/<tenant>/0` in the tenant's home DC tier names its cluster,
//! the cluster record names the nodes and the `sprig_ctl` credential
//! reference, and the topology driver knows the writer. [`ShardMapLocator`]
//! follows that chain for [`sprig_catalog::MariaBackend`]; [`ClusterPools`]
//! is the cluster half, so tests can point clusters at their own servers.
//!
//! **Remote homes.** A tenant's map lives in its home DC's tier. A child
//! homed here reads its ancestors' catalogs (the `tree_settings` copy, the
//! inherited definitions), and an ancestor may be homed in another DC. When
//! this DC's tier has no `home` map of a tenant, [`ShardMapLocator`] reads
//! the directory entry's `home_dc` and that DC's map (single-etcd mode:
//! the one etcd; two tiers: the controllers' relay), as the gateway's
//! locator does. [`ShardMapLocator::cluster_of`] stays this DC's own map:
//! bootstrap, placement and deletion act only on catalogs homed here.
//!
//! Limits, for now: catalog loads read the writer (never a local replica).
//!
//! **Connection budgets** (the fix of a catalog-connection deadlock). Each [`RegistryPools`] is
//! one budget: at most `max` connections per cluster. The controller's
//! short catalog calls (settings, the reconciler, decisions, tenant
//! deletion, snapshots) share one budget of [`POOL_MAX`]; the automatic
//! indexing loop and its Admin API, which hold a catalog connection for a
//! whole pass or request, have their own (`autoindex_loop::SESSIONS`).
//! The rules that keep them from deadlocking:
//!
//! - a task holds at most one connection of a budget, and never waits for
//!   a second one of the same budget while it holds the first;
//! - a holder of a session connection may take a short one, never the
//!   reverse, so waits between budgets can't form a cycle;
//! - every wait for a connection ends after [`ACQUIRE_TIMEOUT`]
//!   ([`acquire`], [`bounded`]) with 39070, so a starved caller fails and
//!   retries instead of hanging; the pools themselves carry the same limit
//!   ([`RegistryPools::wait_limit`], [`sprig_catalog::pool_wait`]), so an
//!   acquire through `get_conn_bounded` anywhere is bounded too.
//!
//! `sprig_controller_catalog_pool_connections{pool, cluster, state}`
//! (`in_use`, `idle`, `waiting`) shows each budget.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use std::time::Duration;

use mysql_async::{Conn, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::{MapRole, RangeState, ShardMap};
use sprig_catalog::{CatalogEndpoint, CatalogLocator, Store};
use sprig_cluster::driver::NodeConnector;
use sprig_cluster::record::{AdminState, ClusterRecord, NodeSpec};
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Record, Tier, TierView};
use sprig_provision::credentials::CredentialConnector;

use crate::monitor::ClusterMonitor;

/// Connections a controller keeps per cluster for its short catalog calls.
pub const POOL_MAX: usize = 4;

/// The longest a controller task waits for a catalog connection.
pub const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(20);

fn starved(what: &str, after: Duration) -> SprigError {
    metrics::counter!("sprig_controller_catalog_acquire_timeouts_total").increment(1);
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!(
            "{what}: no catalog connection within {} s (the cluster's connections are all in use)",
            after.as_secs()
        ),
    )
}

/// A connection from `pool`, waiting at most [`ACQUIRE_TIMEOUT`].
///
/// # Errors
/// 39070 when none frees up in time or the connection fails.
pub async fn acquire(pool: &Pool, what: &str) -> Result<Conn> {
    match tokio::time::timeout(ACQUIRE_TIMEOUT, pool.get_conn()).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) => Err(SprigError::new(
            SprigCode::CatalogUnavailable,
            format!("{what}: catalog unreachable"),
        )
        .with_source(e)),
        Err(_) => Err(starved(what, ACQUIRE_TIMEOUT)),
    }
}

/// `f` (one short catalog call that takes its own connection, such as a
/// snapshot), cut off after [`ACQUIRE_TIMEOUT`] plus `work`.
///
/// # Errors
/// `f`'s; 39070 when it runs out of time.
pub async fn bounded<T>(
    what: &str,
    work: Duration,
    f: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let limit = ACQUIRE_TIMEOUT + work;
    tokio::time::timeout(limit, f)
        .await
        .unwrap_or_else(|_| Err(starved(what, limit)))
}

/// The catalog schema of a tenant: `t<tenant>_sprig`, or a test's own name.
pub type CatalogNaming = Arc<dyn Fn(TenantId) -> String + Send + Sync>;

/// `t<tenant>_sprig`.
#[must_use]
pub fn default_naming() -> CatalogNaming {
    Arc::new(|t| SchemaName::catalog(t).to_string())
}

/// Clusters the controller can place catalogs on and connect to.
#[async_trait::async_trait]
pub trait ClusterPools: Send + Sync + 'static {
    /// Clusters of this DC a new catalog may go to: registered and
    /// `active`, by cluster number.
    ///
    /// # Errors
    /// The store's errors.
    async fn placeable(&self) -> Result<Vec<ClusterId>>;

    /// Clusters of this DC that may hold a tenant's objects, for a deleted
    /// tenant's cleanup: every registered cluster but
    /// `rejected` and `retired` ones. The default is [`Self::placeable`].
    ///
    /// # Errors
    /// The store's errors.
    async fn sweepable(&self) -> Result<Vec<ClusterId>> {
        self.placeable().await
    }

    /// A pool of connections to `cluster`'s writer as `sprig_ctl`.
    ///
    /// # Errors
    /// 39104 for an unknown cluster; 39100 without a writer; 39998 for a
    /// credential that doesn't resolve.
    async fn writer(&self, cluster: &ClusterId) -> Result<Pool>;
}

fn pool_for(opts: mysql_async::Opts, max: usize) -> Pool {
    let constraints = PoolConstraints::new(0, max.max(1)).unwrap_or_default();
    // Keep idle connections for a minute: with mysql_async's default TTL of
    // 0, a pool with a minimum of 0 closes every connection returned to it.
    Pool::new(
        OptsBuilder::from_opts(opts).pool_opts(
            PoolOpts::new()
                .with_constraints(constraints)
                .with_inactive_connection_ttl(std::time::Duration::from_secs(60)),
        ),
    )
}

/// [`ClusterPools`] over the DC's cluster registry: credentials from the
/// record's secret references, the writer from the `registry` loop's
/// driver when it has one, else the record's first node (the writer at
/// registration for primary-replica clusters; the heaviest writer of a
/// Galera writer set).
pub struct RegistryPools {
    registry: ClusterRegistry,
    monitor: Option<Arc<ClusterMonitor>>,
    pools: Mutex<HashMap<ClusterId, (String, Pool)>>,
    /// The budget's name in metrics.
    name: &'static str,
    /// Connections per cluster.
    max: usize,
    /// The longest a task waits for one ([`Self::wait_limit`]).
    wait: Duration,
}

/// One budget's connections to one cluster, from the pool's own counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolStats {
    /// Handed out to tasks.
    pub in_use: usize,
    /// Open and idle in the pool.
    pub idle: usize,
    /// Tasks waiting for one.
    pub waiting: usize,
}

/// `pool`'s counters, also published as
/// `sprig_controller_catalog_pool_connections`.
#[must_use]
pub fn pool_stats(budget: &str, cluster: &ClusterId, pool: &Pool) -> PoolStats {
    use std::sync::atomic::Ordering::Relaxed;
    let m = pool.metrics();
    let s = PoolStats {
        in_use: m.connections_in_use.load(Relaxed),
        idle: m.connections_in_pool.load(Relaxed),
        waiting: m.active_wait_requests.load(Relaxed),
    };
    #[allow(clippy::cast_precision_loss)] // a handful of connections
    for (state, n) in [
        ("in_use", s.in_use),
        ("idle", s.idle),
        ("waiting", s.waiting),
    ] {
        metrics::gauge!("sprig_controller_catalog_pool_connections",
            "pool" => budget.to_owned(), "cluster" => cluster.to_string(), "state" => state)
        .set(n as f64);
    }
    s
}

impl std::fmt::Debug for RegistryPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryPools")
            .field("dc", self.registry.dc())
            .finish_non_exhaustive()
    }
}

impl RegistryPools {
    /// Pools over `registry`, asking `monitor` for writers.
    #[must_use]
    pub fn new(registry: ClusterRegistry, monitor: Option<Arc<ClusterMonitor>>) -> Self {
        Self {
            registry,
            monitor,
            pools: Mutex::new(HashMap::new()),
            name: "catalog",
            max: POOL_MAX,
            wait: ACQUIRE_TIMEOUT,
        }
    }

    /// The same pools as a budget of `max` connections per cluster, named
    /// `name` in metrics.
    #[must_use]
    pub fn budget(mut self, name: &'static str, max: usize) -> Self {
        self.name = name;
        self.max = max.max(1);
        self
    }

    /// The same pools with every wait for a connection ending after
    /// `wait` ([`sprig_catalog::pool_wait`]); the default is
    /// [`ACQUIRE_TIMEOUT`]. A budget whose holders legitimately keep a
    /// connection for long (a logic run) takes a longer one.
    #[must_use]
    pub fn wait_limit(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Each cluster's connections now (and the metric updated).
    #[must_use]
    pub fn stats(&self) -> Vec<(ClusterId, PoolStats)> {
        let pools: Vec<(ClusterId, Pool)> = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(c, (_, p))| (c.clone(), p.clone()))
            .collect();
        pools
            .into_iter()
            .map(|(c, p)| {
                let s = pool_stats(self.name, &c, &p);
                (c, s)
            })
            .collect()
    }

    async fn writer_spec(&self, rec: &ClusterRecord) -> Result<NodeSpec> {
        // Another DC's cluster (an ancestor's catalog homed there): its
        // writer is published in that DC's tier.
        let store = match sprig_cluster::registry::dc_of(&rec.cluster_id) {
            Some(d) if Tier::Dc(d.clone()) != *self.registry.dc_store().tier() => {
                TierView::over(self.registry.dc_store(), Tier::Dc(d))
            }
            _ => Arc::clone(self.registry.dc_store()),
        };
        let published = read_published(&*store, &rec.cluster_id).await?;
        let driver = match &self.monitor {
            Some(m) => m.writer_node(&rec.cluster_id).await,
            None => None,
        };
        writer_of(rec, &published, driver)
    }
}

/// A cluster's writer as the failover loop publishes it, read as the
/// gateway reads it (`sprig_gateway::backend::cluster`):
/// `leases/writer/<cluster>`, else the `cluster-failover` record's writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Published {
    /// The writer node and its `writer_epoch`.
    Node(sprig_common::ids::NodeId, u64),
    /// A promotion is handing the writer over, or the lease is older than
    /// the last failover: nobody may write yet.
    InTransit(String),
    /// Nothing published (no failover yet, no agents).
    Nothing,
}

/// [`Published`] from the lease and the failover record.
#[must_use]
pub fn published(
    lease: Option<&sprig_cluster::failover::writer::WriterLease>,
    recorded: Option<&crate::failover::cluster::ClusterFailoverRecord>,
) -> Published {
    use sprig_common::ids::NodeId;
    let recorded_epoch = recorded.map_or(0, |r| r.writer_epoch);
    match (lease, recorded) {
        (Some(l), _) if l.claimed_by_controller() => {
            Published::InTransit(format!("a failover is handing the writer to {}", l.node_id))
        }
        (Some(l), _) if l.writer_epoch < recorded_epoch => Published::InTransit(format!(
            "the writer lease (epoch {}) is older than the last failover (epoch {recorded_epoch})",
            l.writer_epoch
        )),
        (Some(l), _) => NodeId::new(&l.node_id).map_or_else(
            |_| Published::InTransit(format!("the lease names node {:?}", l.node_id)),
            |n| Published::Node(n, l.writer_epoch),
        ),
        (None, Some(r)) => match &r.writer {
            Some(w) => NodeId::new(w).map_or_else(
                |_| Published::InTransit(format!("the failover record names {w:?}")),
                |n| Published::Node(n, r.writer_epoch),
            ),
            None => Published::Nothing,
        },
        (None, None) => Published::Nothing,
    }
}

/// Reads `cluster`'s [`Published`] writer from the DC tier.
///
/// # Errors
/// The store's errors; `Internal` for a bad record.
pub async fn read_published(dc: &dyn MetaStore, cluster: &ClusterId) -> Result<Published> {
    use sprig_cluster::failover::writer::{WriterLease, writer_lease_key};
    let lease = match dc
        .get(
            &writer_lease_key(dc.tier(), cluster)?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => Some(Record::<WriterLease>::decode(&kv.value)?.body),
        None => None,
    };
    let recorded = match dc
        .get(
            &crate::failover::cluster::record_key(dc.tier(), cluster)?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => {
            Some(Record::<crate::failover::cluster::ClusterFailoverRecord>::decode(&kv.value)?.body)
        }
        None => None,
    };
    Ok(published(lease.as_ref(), recorded.as_ref()))
}

/// The node controllers write to on `rec`'s cluster: the published writer
/// first (after a failover the first registered node may be a read-only
/// replica, and the topology driver only learns of the change on its next
/// pass, or never if it can't see a writer); else the driver's; else
/// Galera's heaviest writer; else the first node. While a failover hands
/// the writer over, none (39100, retried by the caller's loop).
///
/// # Errors
/// 39100 while in transit, or when no node fits.
pub fn writer_of(
    rec: &ClusterRecord,
    published: &Published,
    driver: Option<sprig_common::ids::NodeId>,
) -> Result<NodeSpec> {
    let unavailable = |why: String| {
        SprigError::new(
            SprigCode::ClusterUnavailable,
            format!("cluster {}: {why}", rec.cluster_id),
        )
    };
    let chosen = match published {
        Published::InTransit(why) => return Err(unavailable(why.clone())),
        Published::Node(n, _) if rec.nodes.iter().any(|s| &s.node == n) => Some(n.clone()),
        Published::Node(..) | Published::Nothing => driver.or_else(|| {
            rec.galera.as_ref().and_then(|g| {
                g.writers
                    .iter()
                    .max_by(|a, b| a.weight.total_cmp(&b.weight))
                    .map(|w| w.node.clone())
            })
        }),
    };
    let spec = match chosen {
        Some(n) => rec.nodes.iter().find(|s| s.node == n),
        None => rec.nodes.first(),
    };
    spec.cloned()
        .ok_or_else(|| unavailable("no writer".to_owned()))
}

#[async_trait::async_trait]
impl ClusterPools for RegistryPools {
    async fn placeable(&self) -> Result<Vec<ClusterId>> {
        let mut list: Vec<(u32, ClusterId)> = self
            .registry
            .list()
            .await?
            .into_iter()
            .filter(|c| c.body().admin_state == AdminState::Active)
            .map(|c| (c.body().cluster_number, c.body().cluster_id.clone()))
            .collect();
        list.sort();
        Ok(list.into_iter().map(|(_, id)| id).collect())
    }

    async fn sweepable(&self) -> Result<Vec<ClusterId>> {
        let mut list: Vec<(u32, ClusterId)> = self
            .registry
            .list()
            .await?
            .into_iter()
            .filter(|c| {
                !matches!(
                    c.body().admin_state,
                    AdminState::Rejected | AdminState::Retired
                )
            })
            .map(|c| (c.body().cluster_number, c.body().cluster_id.clone()))
            .collect();
        list.sort();
        Ok(list.into_iter().map(|(_, id)| id).collect())
    }

    async fn writer(&self, cluster: &ClusterId) -> Result<Pool> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let spec = self.writer_spec(rec).await?;
        let fingerprint = format!("{}|{}|{}", spec.node, spec.addr, rec.secrets.ctl);
        let hit = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .filter(|(f, _)| *f == fingerprint)
            .map(|(_, p)| p.clone());
        if let Some(p) = hit {
            let _ = pool_stats(self.name, cluster, &p);
            return Ok(p);
        }
        let ep = CredentialConnector::ctl(&rec.secrets).endpoint(&spec)?;
        let pool = pool_for(ep.opts, self.max);
        sprig_catalog::pool_wait::limit(&pool, self.name, cluster.as_str(), self.max, self.wait);
        let old = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (fingerprint, pool.clone()));
        if let Some((_, old)) = old {
            tokio::spawn(async move {
                let _ = old.disconnect().await;
            });
        }
        Ok(pool)
    }
}

/// Fixed pools per cluster, for tests and single-server tools.
#[derive(Debug, Clone, Default)]
pub struct StaticPools {
    pools: Vec<(ClusterId, Pool)>,
}

impl StaticPools {
    /// No clusters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `cluster`, reached at `url` (`mysql://user:pass@host:port`).
    ///
    /// # Errors
    /// `Internal` for a bad URL.
    pub fn with_url(mut self, cluster: ClusterId, url: &str) -> Result<Self> {
        let opts = mysql_async::Opts::from_url(url)
            .map_err(|e| SprigError::internal(format!("bad MariaDB URL: {e}")))?;
        let pool = pool_for(opts, POOL_MAX);
        sprig_catalog::pool_wait::limit(
            &pool,
            "static",
            cluster.as_str(),
            POOL_MAX,
            ACQUIRE_TIMEOUT,
        );
        self.pools.push((cluster, pool));
        Ok(self)
    }

    /// Adds `cluster` with a budget of `max` connections.
    ///
    /// # Errors
    /// `Internal` for a bad URL.
    pub fn with_url_max(mut self, cluster: ClusterId, url: &str, max: usize) -> Result<Self> {
        let opts = mysql_async::Opts::from_url(url)
            .map_err(|e| SprigError::internal(format!("bad MariaDB URL: {e}")))?;
        let pool = pool_for(opts, max);
        sprig_catalog::pool_wait::limit(&pool, "static", cluster.as_str(), max, ACQUIRE_TIMEOUT);
        self.pools.push((cluster, pool));
        Ok(self)
    }
}

#[async_trait::async_trait]
impl ClusterPools for StaticPools {
    async fn placeable(&self) -> Result<Vec<ClusterId>> {
        Ok(self.pools.iter().map(|(c, _)| c.clone()).collect())
    }

    async fn writer(&self, cluster: &ClusterId) -> Result<Pool> {
        self.pools
            .iter()
            .find(|(c, _)| c == cluster)
            .map(|(_, p)| p.clone())
            .ok_or_else(|| {
                SprigError::new(SprigCode::ClusterNotFound, format!("no cluster {cluster}"))
            })
    }
}

/// Finds catalogs through their shard maps in this DC's tier, and, with
/// the directory ([`ShardMapLocator::with_directory`]), those of tenants
/// homed in other DCs through their home DC's tier.
#[derive(Clone)]
pub struct ShardMapLocator {
    dc: Arc<dyn MetaStore>,
    pools: Arc<dyn ClusterPools>,
    naming: CatalogNaming,
    global: Option<Arc<dyn MetaStore>>,
}

impl std::fmt::Debug for ShardMapLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardMapLocator")
            .field("dc", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl ShardMapLocator {
    /// A locator reading shard maps from `dc` and connecting with `pools`.
    #[must_use]
    pub fn new(
        dc: Arc<dyn MetaStore>,
        pools: Arc<dyn ClusterPools>,
        naming: CatalogNaming,
    ) -> Self {
        Self {
            dc,
            pools,
            naming,
            global: None,
        }
    }

    /// The same locator following tenants homed in other DCs: their
    /// directory entries in `global` name the home, whose map is read from
    /// that DC's tier through this DC's store (one etcd, or the relay).
    #[must_use]
    pub fn with_directory(mut self, global: Arc<dyn MetaStore>) -> Self {
        self.global = Some(global);
        self
    }

    /// The catalog schema of `tenant`.
    #[must_use]
    pub fn schema(&self, tenant: TenantId) -> String {
        (self.naming)(tenant)
    }

    /// The cluster holding `tenant`'s catalog, if it has a shard map here.
    ///
    /// # Errors
    /// The store's errors; `Internal` for a bad record.
    pub async fn cluster_of(&self, tenant: TenantId) -> Result<Option<ClusterId>> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        Ok(Record::<ShardMap>::decode(&kv.value)?
            .body
            .catalog_cluster()
            .cloned())
    }

    /// The pools.
    #[must_use]
    pub fn pools(&self) -> &Arc<dyn ClusterPools> {
        &self.pools
    }

    /// `tenant`'s catalog map in this DC's tier, if any.
    async fn local_map(&self, tenant: TenantId) -> Result<Option<ShardMap>> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let Some(kv) = self.dc.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        Ok(Some(Record::<ShardMap>::decode(&kv.value)?.body))
    }

    /// `tenant`'s catalog map in its home DC, when that is another DC
    /// (`None` without the directory, for a tenant homed here or without
    /// an entry, or when the home has no map).
    async fn home_map(&self, tenant: TenantId) -> Result<Option<ShardMap>> {
        let Some(global) = &self.global else {
            return Ok(None);
        };
        let Some(entry) = sprig_shard::home::read_home(&**global, tenant).await? else {
            return Ok(None);
        };
        if Tier::Dc(entry.home_dc.clone()) == *self.dc.tier() {
            return Ok(None);
        }
        let store = TierView::over(&self.dc, Tier::Dc(entry.home_dc));
        let key = ShardMap::catalog_key(store.tier(), tenant)?;
        let Some(kv) = store.get(&key, Consistency::Linearizable).await? else {
            return Ok(None);
        };
        Ok(Some(Record::<ShardMap>::decode(&kv.value)?.body))
    }

    /// The map that names `tenant`'s catalog wherever it is homed: this
    /// DC's `home` map; else the home DC's map (a tenant homed in another
    /// DC: an ancestor of a child homed here); else whatever map this DC
    /// has (a standby's, or a forwarding record, when the directory or the
    /// home can't be read).
    ///
    /// # Errors
    /// The store's errors; `Internal` for a bad record.
    pub async fn catalog_map(&self, tenant: TenantId) -> Result<Option<ShardMap>> {
        let local = self.local_map(tenant).await?;
        if local.as_ref().is_some_and(|m| m.role == MapRole::Home) || self.global.is_none() {
            return Ok(local);
        }
        match self.home_map(tenant).await {
            Ok(Some(m)) => {
                tracing::debug!(%tenant, "catalog homed in another DC: following its home's map");
                Ok(Some(m))
            }
            Ok(None) => Ok(local),
            Err(e) if local.is_some() => {
                tracing::debug!(%tenant, error = %e, "the home DC's map can't be read; using this DC's");
                Ok(local)
            }
            Err(e) => Err(e),
        }
    }

    /// The cluster of `tenant`'s catalog wherever it is homed
    /// ([`Self::catalog_map`]).
    ///
    /// # Errors
    /// As [`Self::catalog_map`].
    pub async fn catalog_cluster(&self, tenant: TenantId) -> Result<Option<ClusterId>> {
        Ok(self
            .catalog_map(tenant)
            .await?
            .and_then(|m| m.catalog_cluster().cloned()))
    }
}

#[async_trait::async_trait]
impl CatalogLocator for ShardMapLocator {
    async fn held_by(&self, tenant: TenantId) -> Result<Option<String>> {
        let Some(map) = self.catalog_map(tenant).await? else {
            return Ok(None);
        };
        Ok(map
            .ranges
            .iter()
            .find(|r| r.state == RangeState::Held)
            .map(|r| r.job_id.clone().unwrap_or_else(|| "unknown".to_owned())))
    }

    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        let cluster = self.catalog_cluster(tenant).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::TenantNotReady,
                format!(
                    "tenant {tenant} has no catalog map here or in its home DC (not \
                     bootstrapped yet, or deleted)"
                ),
            )
        })?;
        Ok(CatalogEndpoint {
            pool: self.pools.writer(&cluster).await?,
            store: Store::new(self.schema(tenant)),
        })
    }
}

/// Fails with 39070 (retryable) while a mover's cutover holds `tenant`'s
/// catalog ([`migrate_catalog`]).
///
/// # Errors
/// 39070 while held; the store's errors.
pub async fn not_held(locator: &dyn CatalogLocator, tenant: TenantId) -> Result<()> {
    let Some(job) = locator.held_by(tenant).await? else {
        return Ok(());
    };
    tracing::info!(tenant = %tenant, job = %job,
        "catalog migrations wait: a mover cutover holds the catalog");
    Err(SprigError::new(
        SprigCode::CatalogUnavailable,
        format!(
            "tenant {tenant}'s catalog is held by mover job {job} for a cutover; catalog \
             migrations wait until it ends"
        ),
    ))
}

/// Runs `migrator` on `tenant`'s catalog `schema` through `conn`, unless a
/// mover's cutover holds the catalog: the cutover
/// may rename the catalog's tables away, and a migration then would create
/// empty ones under their names. The migrator itself also refuses a
/// catalog holding fenced tables; this check keeps the controller from
/// trying during the whole hold.
///
/// # Errors
/// 39070 (retryable) while the catalog is held; the migrator's errors.
pub async fn migrate_catalog(
    locator: &dyn CatalogLocator,
    tenant: TenantId,
    conn: &mut Conn,
    schema: &str,
    migrator: &sprig_catalog::Migrator,
) -> Result<()> {
    not_held(locator, tenant).await?;
    migrator
        .run(
            conn,
            schema,
            // Never made here: only bootstrap creates a catalog, marked.
            &sprig_catalog::migrate::MigrateOpts::existing(),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
