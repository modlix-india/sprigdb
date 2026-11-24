//! Where a tenant's catalog lives, as the gateway finds it: the shard map
//! `shardmaps/<tenant>/0` in this DC's tier names the cluster; the cluster
//! record names its nodes and the `sprig_ctl` credential reference; the
//! gateway's own writer tracking ([`Backends::writer`]) picks the node.
//!
//! The same lookup gives a tenant's default data placement: a database's
//! shards start on its catalog's cluster, so [`GatewayLocator::cluster`]
//! is also the cluster of the session's backend target.
//!
//! **Remote homes.** A tenant's map lives in its home DC's tier. When this
//! DC has no home map for the tenant (it was never homed here, or it moved
//! away, leaving a `quarantined` forwarding map), the locator reads the
//! directory entry's `home_dc` and the home DC's map from that DC's tier of
//! the one etcd (single-etcd mode; otherwise the relay's `RemoteMaps`), and
//! follows that map from then on: holds and moves reach this gateway
//! whatever DC the tenant is homed in. A followed map that turns
//! `quarantined` (the tenant moved on) makes the locator read the directory
//! again.
//!
//! Catalog reads and writes, and database-level DDL (`CREATE DATABASE`
//! needs an account that may create `t<id>_d...` schemas), run as the
//! cluster's provisioning account `sprig_ctl`, through a small
//! `mysql_async` pool per cluster.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use mysql_async::{OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_catalog::placement::{MapRole, ShardMap};
use sprig_catalog::{CatalogEndpoint, CatalogLocator, Store};
use sprig_cluster::driver::NodeConnector as _;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{ClusterId, DcId, ShardId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Consistency, MetaStore, Mirror, Record, Tier, TierView};
use sprig_provision::credentials::CredentialConnector;
use sprig_shard::Placement;
use tokio::sync::watch;

/// The cluster a tenant's shard map names when its DC has no MariaDB
/// cluster (the controller places it so): nothing of it reaches MariaDB.
pub const ENGINE_ONLY_CLUSTER: &str = "engine-only";

use crate::backend::Backends;

/// Connections per cluster in each of the three `sprig_ctl` pools (see
/// [`Budget`]).
const POOL_MAX: usize = 4;
/// The test hook that shrinks the catalog pools (see [`pool_max`]).
const CATALOG_POOL_MAX_ENV: &str = "SPRIG_TEST_CATALOG_POOL_MAX";
/// The test hook that shrinks the other two `sprig_ctl` pools, the short
/// one and the holders' one (see [`pool_max`]).
const CTL_POOL_MAX_ENV: &str = "SPRIG_TEST_CTL_POOL_MAX";
/// How long an idle `sprig_ctl` connection stays open.
const IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(60);
/// The longest a catalog connection lives (give or take
/// [`CATALOG_CONN_JITTER`]): its server-side prepared statements and buffers
/// go with it, so whatever a connection accumulates on the catalog's
/// server is bounded in time.
const CATALOG_CONN_TTL: std::time::Duration = std::time::Duration::from_secs(15 * 60);
/// Spreads the catalog connections' renewals.
const CATALOG_CONN_JITTER: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a cluster's record, as read, serves the pools' lookups (the
/// backends' cluster views keep theirs as long); the writer is checked on
/// every lookup.
const RECORD_TTL: std::time::Duration = std::time::Duration::from_secs(2);

/// The gateway's three `sprig_ctl` pools per cluster, each its own budget
/// of [`POOL_MAX`] connections.
///
/// - [`Budget::Hold`] ([`GatewayLocator::ctl_hold_pool`]): a connection
///   that keeps a lock (a transaction's row locks, `GET_LOCK`) while its
///   task does other work with more connections: the cross-tenant outbox
///   forward, a broadcast's fan-out, the broadcast retry pass.
/// - [`Budget::Catalog`] ([`GatewayLocator::catalog_pool`]): tenant
///   catalog reads and changes.
/// - [`Budget::Ctl`] ([`GatewayLocator::ctl_pool`]): everything else
///   (shard DDL, outbox and log rows, logic routines), one step at a time.
///
/// They are taken in that order and never the other way round: a task
/// holding a hold connection may take catalog and short ones, a task
/// holding a catalog connection may take a short one, a task holding a
/// short one takes no other (a deploy over shards on several clusters
/// takes one per cluster, all at once in cluster order), and no task
/// waits for a second connection of a pool while it holds one. So waits
/// can't form a cycle, and a path that holds one while it asks the same
/// pool for another is a bug: it waits for itself once the rest are busy
/// (with the test hooks at 1, at once).
///
/// **Waits end.** Every acquire from these pools is bounded
/// ([`sprig_catalog::pool_wait`]): past [`Budget::wait`] it fails, naming
/// the pool and the cluster, and counts in `sprig_pool_wait_timeouts_total`.
/// A path that broke the order above then fails instead of hanging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Budget {
    Hold,
    Catalog,
    Ctl,
}

impl Budget {
    /// The pool's name in errors and metrics.
    fn name(self) -> &'static str {
        match self {
            Budget::Hold => "sprig_ctl_hold",
            Budget::Catalog => "catalog",
            Budget::Ctl => "sprig_ctl",
        }
    }

    /// The longest a task waits for one of the pool's connections.
    ///
    /// - Catalog: 5 s, the catalog's own `begin` timeout (39070), which
    ///   already bounded the versioned changes' waits.
    /// - Short `sprig_ctl`: 10 s, the DDL executor's wait for the same
    ///   pool's connections. Its holders are one statement or one step;
    ///   the longest is a DDL statement itself.
    /// - Holders: 30 s. A holder keeps its connection across one batch of
    ///   deliveries (up to 500 rows or members, plus the broadcast pacing,
    ///   at most 1 s a batch), so a waiter may legitimately wait for a few.
    fn wait(self) -> std::time::Duration {
        std::time::Duration::from_secs(match self {
            Budget::Hold => 30,
            Budget::Catalog => 5,
            Budget::Ctl => 10,
        })
    }
}

/// Connections per cluster in a pool of `budget`: [`POOL_MAX`], or fewer
/// (1 to 4) with the test hooks `SPRIG_TEST_CATALOG_POOL_MAX` (catalog
/// pools) and `SPRIG_TEST_CTL_POOL_MAX` (the short and hold pools).
///
/// A task holds at most one connection of a pool at a time, and gives it
/// back before anything that takes another from the same pool (a catalog
/// change, a snapshot load, a migration, a nested statement). A path that
/// held one while asking for more needed several at once and waited
/// (catalog: failed with 39070, "catalog begin timed out") whenever the
/// others were busy. With a hook at 1, such a path waits for itself, so a
/// live suite run against a gateway started with it finds them.
fn pool_max(budget: Budget) -> usize {
    static CATALOG: OnceLock<usize> = OnceLock::new();
    static CTL: OnceLock<usize> = OnceLock::new();
    let (cell, env, what) = match budget {
        Budget::Catalog => (&CATALOG, CATALOG_POOL_MAX_ENV, "catalog"),
        Budget::Ctl | Budget::Hold => (&CTL, CTL_POOL_MAX_ENV, "sprig_ctl"),
    };
    *cell.get_or_init(|| {
        let n = std::env::var(env)
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .map_or(POOL_MAX, |n| n.clamp(1, POOL_MAX));
        if n < POOL_MAX {
            tracing::warn!(
                connections = n,
                "{env} is set: {what} pools are smaller than usual (a test hook)"
            );
        }
        n
    })
}

/// Finds tenants' catalogs and data clusters (see the module docs).
pub struct GatewayLocator {
    dc: Arc<dyn MetaStore>,
    registry: ClusterRegistry,
    backends: Arc<Backends>,
    /// The `sprig_ctl` pools per cluster, by [`Budget`]; the catalog ones
    /// keep their connections' prepared statements.
    pools: Mutex<HashMap<(Budget, ClusterId), (String, Pool)>>,
    /// Cluster records as last read for the pools, and when
    /// ([`RECORD_TTL`]).
    records:
        Mutex<HashMap<ClusterId, (std::time::Instant, sprig_cluster::registry::StoredCluster)>>,
    clusters: Mutex<HashMap<TenantId, ClusterId>>,
    ctl_users: Mutex<HashMap<ClusterId, String>>,
    /// Every tenant map in this DC tier (`shardmaps/`), fed by a watch,
    /// started at first use: holds and moves.
    maps: OnceLock<Mirror>,
    /// The same for other DCs' tiers, for tenants homed there.
    remote_maps: Mutex<HashMap<DcId, Mirror>>,
    /// The global tier, for directory entries; `None`: this DC only.
    global: Option<Arc<dyn MetaStore>>,
    /// Where each tenant's map is followed, and traffic counts.
    routing: Arc<crate::dc::DcRouting>,
    /// When a tenant's home was last read again after its map turned
    /// `quarantined` (at most once a second).
    refreshed: Mutex<HashMap<TenantId, std::time::Instant>>,
    /// Decoded maps by key and etcd revision: every statement reads its
    /// tenant's map three times (holds, roles, the maps token), and
    /// decoding the record each time was a tenth of a MongoDB point read's
    /// gateway CPU.
    decoded: Mutex<HashMap<String, (u64, Arc<ShardMap>)>>,
}

impl std::fmt::Debug for GatewayLocator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayLocator")
            .field("dc", self.dc.tier())
            .finish_non_exhaustive()
    }
}

impl GatewayLocator {
    /// A locator reading shard maps from `dc` (this DC's tier) and cluster
    /// records from `registry`, asking `backends` for writers.
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, registry: ClusterRegistry, backends: Arc<Backends>) -> Self {
        let my_dc = registry.dc().clone();
        Self {
            remote_maps: Mutex::new(HashMap::new()),
            global: None,
            routing: crate::dc::DcRouting::new(my_dc),
            refreshed: Mutex::new(HashMap::new()),
            decoded: Mutex::new(HashMap::new()),
            dc,
            registry,
            backends,
            pools: Mutex::new(HashMap::new()),
            records: Mutex::new(HashMap::new()),
            clusters: Mutex::new(HashMap::new()),
            ctl_users: Mutex::new(HashMap::new()),
            maps: OnceLock::new(),
        }
    }

    /// The same locator, reading tenants' home DCs from `global`'s
    /// directory, with `routing` noting each home.
    #[must_use]
    pub fn with_homes(
        mut self,
        global: Arc<dyn MetaStore>,
        routing: Arc<crate::dc::DcRouting>,
    ) -> Self {
        self.global = Some(global);
        self.routing = routing;
        self
    }

    /// The routing state (traffic counts, homes).
    #[must_use]
    pub fn routing(&self) -> &Arc<crate::dc::DcRouting> {
        &self.routing
    }

    /// This gateway's DC.
    fn my_dc(&self) -> &DcId {
        self.routing.dc()
    }

    /// The map mirror of DC `dc`.
    fn maps_of(&self, dc: &DcId) -> Mirror {
        if dc == self.my_dc() {
            return self.maps().clone();
        }
        self.remote_maps
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(dc.clone())
            .or_insert_with(|| {
                let tier = Tier::Dc(dc.clone());
                let prefix = tier.key("shardmaps/").unwrap_or_else(|_| tier.root());
                Mirror::spawn(TierView::over(&self.dc, tier), prefix)
            })
            .clone()
    }

    /// `tenant`'s map in DC `dc`, from the mirror.
    fn mirrored_map(&self, dc: &DcId, tenant: TenantId) -> Option<ShardMap> {
        self.mirrored_map_arc(dc, tenant).map(|m| (*m).clone())
    }

    fn mirrored_map_arc(&self, dc: &DcId, tenant: TenantId) -> Option<Arc<ShardMap>> {
        let key = ShardMap::catalog_key(&Tier::Dc(dc.clone()), tenant).ok()?;
        let kv = self.maps_of(dc).get(&key)?;
        self.decode_map(key.as_str(), kv.mod_revision, &kv.value)
    }

    /// The map record `value` at `key`, decoded once per revision.
    fn decode_map(&self, key: &str, rev: u64, value: &[u8]) -> Option<Arc<ShardMap>> {
        if let Some((r, m)) = self
            .decoded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            && *r == rev
        {
            return Some(Arc::clone(m));
        }
        let m = Arc::new(Record::<ShardMap>::decode(value).ok()?.body);
        self.decoded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key.to_owned(), (rev, Arc::clone(&m)));
        Some(m)
    }

    /// The role of the map this gateway follows for `tenant` (a
    /// `quarantined` map is a fenced copy); `None` until it has loaded.
    #[must_use]
    pub fn followed_role(&self, tenant: TenantId) -> Option<MapRole> {
        self.mirrored_map_arc(&self.home_of(tenant), tenant)
            .map(|m| m.role)
    }

    /// A token of every map of `tenant` in the DC it is followed in (the
    /// sum of their etcd revisions, so any hold, release, cutover or split
    /// changes it), with that DC, and whether one of them holds a range
    /// now. A statement that failed with 1146 or 1049 while the token moved
    /// met a mover's fence, not a missing table.
    #[must_use]
    pub fn maps_token(&self, tenant: TenantId) -> (DcId, u64, bool) {
        let dc = self.home_of(tenant);
        let Ok(prefix) = Tier::Dc(dc.clone()).key(&format!("shardmaps/{tenant}/")) else {
            return (dc, 0, false);
        };
        let (sum, held) = self.maps_of(&dc).with(|entries| {
            let mut sum = 0u64;
            let mut held = false;
            for (k, kv) in entries.range(prefix.clone()..) {
                if !k.starts_with(&prefix) {
                    break;
                }
                sum = sum.wrapping_add(kv.mod_revision);
                held |= self
                    .decode_map(k.as_str(), kv.mod_revision, &kv.value)
                    .is_some_and(|m| {
                        m.ranges
                            .iter()
                            .any(|r| r.state == sprig_catalog::placement::RangeState::Held)
                    });
            }
            (sum, held)
        });
        (dc, sum, held)
    }

    /// [`Self::maps_token`] read from etcd itself rather than from the
    /// watched copy: one linearizable listing of `tenant`'s maps in the DC
    /// they are followed in. A mover writes its hold before it fences, so a
    /// listing made after a statement met the fence shows the hold (or the
    /// cutover after it), however far the watch lags.
    ///
    /// # Errors
    /// The store's.
    pub async fn maps_token_fresh(&self, tenant: TenantId) -> Result<(DcId, u64, bool)> {
        let dc = self.home_of(tenant);
        let prefix = Tier::Dc(dc.clone()).key(&format!("shardmaps/{tenant}/"))?;
        let (mut sum, mut held) = (0u64, false);
        let mut after = None;
        loop {
            let page = self
                .dc
                .list(
                    &prefix,
                    sprig_meta::ListOpts {
                        after: after.take(),
                        ..sprig_meta::ListOpts::default()
                    },
                )
                .await?;
            for kv in &page.kvs {
                sum = sum.wrapping_add(kv.mod_revision);
                held |= self
                    .decode_map(kv.key.as_str(), kv.mod_revision, &kv.value)
                    .is_some_and(|m| {
                        m.ranges
                            .iter()
                            .any(|r| r.state == sprig_catalog::placement::RangeState::Held)
                    });
            }
            match page.kvs.last() {
                Some(kv) if page.more => after = Some(kv.key.clone()),
                _ => break,
            }
        }
        Ok((dc, sum, held))
    }

    /// Whether the watched copy of the maps of the DC `tenant` is followed
    /// in has loaded (before that, [`Self::maps_token`] sees no map).
    #[must_use]
    pub fn maps_loaded(&self, tenant: TenantId) -> bool {
        self.maps_of(&self.home_of(tenant)).is_ready()
    }

    /// The DC whose map this gateway follows for `tenant`: its home as last
    /// read, else this DC.
    #[must_use]
    pub fn home_of(&self, tenant: TenantId) -> DcId {
        self.routing.home(tenant)
    }

    /// Reads `tenant`'s home DC from the directory again (a followed map
    /// turned `quarantined`), at most once a second per tenant.
    async fn refresh_home(&self, tenant: TenantId) {
        let Some(global) = &self.global else {
            return;
        };
        {
            let mut r = self
                .refreshed
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let now = std::time::Instant::now();
            if r.get(&tenant)
                .is_some_and(|t| now.duration_since(*t) < std::time::Duration::from_secs(1))
            {
                return;
            }
            r.insert(tenant, now);
        }
        match sprig_shard::home::read_home(&**global, tenant).await {
            Ok(Some(e)) => {
                if e.home_dc != self.home_of(tenant) {
                    tracing::info!(%tenant, home = %e.home_dc, "tenant homed in another DC now; following its map there");
                }
                // Start the mirror early, so the next statement finds it.
                let _ = self.maps_of(&e.home_dc);
                self.routing.set_home(tenant, e.home_dc);
            }
            Ok(None) => {}
            Err(e) => tracing::debug!(%tenant, error = %e, "can't read the tenant's home DC"),
        }
    }

    /// Where `tenant`'s map points and whether a mover cutover holds it,
    /// following its home DC: reads the directory again when the followed
    /// map turned `quarantined`. `None` until a map copy has loaded.
    pub async fn placement_followed(&self, tenant: TenantId) -> Option<Placement> {
        let dc = self.home_of(tenant);
        let map = self.mirrored_map_arc(&dc, tenant);
        if map.as_ref().is_none_or(|m| m.role != MapRole::Home) && self.global.is_some() {
            self.refresh_home(tenant).await;
            let now = self.home_of(tenant);
            if now != dc {
                // The new home's mirror may still be loading: keep following
                // the forwarding map meanwhile.
                if let Some(m) = self.mirrored_map(&now, tenant) {
                    return Placement::of(&m)
                        .ok()
                        .inspect(|p| self.note_fence(tenant, p));
                }
            }
        }
        Placement::of(&*map?)
            .ok()
            .inspect(|p| self.note_fence(tenant, p))
    }

    /// Hands a placement's read fence (a move's last write on the
    /// tenant's new cluster) to the backends, before any statement routes
    /// by the placement: reads of the tenant there then skip replicas
    /// that haven't applied the copy.
    fn note_fence(&self, tenant: TenantId, p: &Placement) {
        if let Some(f) = &p.read_fence {
            self.backends.raise_read_fence_text(&p.cluster, tenant, f);
        }
    }

    /// The DC `tenant` is followed in and its map mirror's generation:
    /// while both stay the same, so does everything the mirror says about
    /// the tenant's maps.
    #[must_use]
    pub fn maps_generation(&self, tenant: TenantId) -> (DcId, u64) {
        let dc = self.home_of(tenant);
        let g = self.maps_of(&dc).generation();
        (dc, g)
    }

    /// Changes whenever a map in the DC `tenant` is followed in changes.
    #[must_use]
    pub fn placement_changes_for(&self, tenant: TenantId) -> watch::Receiver<u64> {
        self.maps_of(&self.home_of(tenant)).changes()
    }

    fn maps(&self) -> &Mirror {
        self.maps.get_or_init(|| {
            let prefix = self
                .dc
                .tier()
                .key("shardmaps/")
                .unwrap_or_else(|_| self.dc.tier().root());
            Mirror::spawn(self.dc.clone(), prefix)
        })
    }

    /// Where `tenant`'s map points and whether a mover cutover holds it,
    /// from the watched copy (no I/O); `None` until the copy has loaded or
    /// for a tenant without a map here.
    #[must_use]
    pub fn placement(&self, tenant: TenantId) -> Option<Placement> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant).ok()?;
        let kv = self.maps().get(&key)?;
        let map = Record::<ShardMap>::decode(&kv.value).ok()?.body;
        Placement::of(&map)
            .ok()
            .inspect(|p| self.note_fence(tenant, p))
    }

    /// Changes whenever any tenant map changes.
    #[must_use]
    pub fn placement_changes(&self) -> watch::Receiver<u64> {
        self.maps().changes()
    }

    /// The cluster holding `tenant`'s catalog (and, by default, its data),
    /// read from the shard map and remembered for [`Self::cached_cluster`].
    ///
    /// # Errors
    /// 39072 if the tenant has no catalog in this DC; the store's errors.
    pub async fn cluster(&self, tenant: TenantId) -> Result<ClusterId> {
        let key = ShardMap::catalog_key(self.dc.tier(), tenant)?;
        let local = match self.dc.get(&key, Consistency::Serializable).await? {
            Some(kv) => Some(Record::<ShardMap>::decode(&kv.value)?.body),
            None => None,
        };
        let map = match local {
            Some(m) if m.role == MapRole::Home || self.global.is_none() => {
                self.routing.set_home(tenant, self.my_dc().clone());
                m
            }
            // Not homed here: the directory names the home.
            local => match self.remote_map(tenant).await {
                Ok(m) => m,
                // The directory or the home's tier can't be read: a
                // forwarding map still points at the cluster.
                Err(e) => match local {
                    Some(m) => m,
                    None => return Err(e),
                },
            },
        };
        let cluster = map
            .catalog_cluster()
            .cloned()
            .ok_or_else(|| not_here(tenant))?;
        if let Ok(p) = Placement::of(&map) {
            self.note_fence(tenant, &p);
        }
        self.clusters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant, cluster.clone());
        Ok(cluster)
    }

    /// `tenant`'s map in its home DC, from the directory entry's
    /// `home_dc`; notes the home.
    async fn remote_map(&self, tenant: TenantId) -> Result<ShardMap> {
        let Some(global) = &self.global else {
            return Err(not_here(tenant));
        };
        let entry = sprig_shard::home::read_home(&**global, tenant)
            .await?
            .ok_or_else(|| not_here(tenant))?;
        let home = entry.home_dc;
        let store = TierView::over(&self.dc, Tier::Dc(home.clone()));
        let key = ShardMap::catalog_key(store.tier(), tenant)?;
        let kv = store
            .get(&key, Consistency::Serializable)
            .await?
            .ok_or_else(|| not_here(tenant))?;
        let map = Record::<ShardMap>::decode(&kv.value)?.body;
        let _ = self.maps_of(&home);
        self.routing.set_home(tenant, home);
        Ok(map)
    }

    /// The cluster last read for `tenant`, without I/O. Logins call it:
    /// the catalog load that precedes every login read the shard map.
    #[must_use]
    pub fn cached_cluster(&self, tenant: TenantId) -> Option<ClusterId> {
        self.clusters
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&tenant)
            .cloned()
    }

    /// This DC's tier (shard maps, cluster records).
    #[must_use]
    pub fn dc_store(&self) -> Arc<dyn MetaStore> {
        Arc::clone(&self.dc)
    }

    /// Whether `cluster`'s published capability profile says XA works
    /// (its `derived.xa`); `false` without a profile.
    ///
    /// # Errors
    /// The registry's errors.
    pub async fn cluster_xa(&self, cluster: &ClusterId) -> Result<bool> {
        Ok(self
            .registry
            .get(cluster)
            .await?
            .and_then(|r| r.body().profile.as_ref().map(|p| p.derived.xa))
            .unwrap_or(false))
    }

    /// Connection options of `sprig_ctl` on `cluster`'s writer (for the
    /// resolver's admin statements).
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn ctl_opts(&self, cluster: &ClusterId) -> Result<mysql_async::Opts> {
        // A tenant placed with no MariaDB cluster in the DC: options that
        // never connect (nothing of such a tenant may reach MariaDB).
        if cluster.as_str() == ENGINE_ONLY_CLUSTER {
            return Ok(OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(9)
                .into());
        }
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let writer = match self.backends.writer(cluster, ShardId(0)).await {
            Ok(n) => rec.nodes.iter().find(|s| s.node == n.node).cloned(),
            Err(_) => None,
        }
        .or_else(|| rec.nodes.first().cloned())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {cluster} has no writer"),
            )
        })?;
        Ok(CredentialConnector::ctl(&rec.secrets)
            .endpoint(&writer)?
            .opts)
    }

    /// Whether `cluster` is a Galera cluster (its registry record's
    /// topology).
    ///
    /// # Errors
    /// 39104 for an unknown cluster; the registry's.
    pub async fn is_galera(&self, cluster: &ClusterId) -> Result<bool> {
        if cluster.as_str() == ENGINE_ONLY_CLUSTER {
            return Ok(false);
        }
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        Ok(stored.body().topology == sprig_cluster::record::TopologyKind::Galera)
    }

    /// Connection options of `sprig_ctl` on `cluster`'s replicas: its
    /// nodes other than the writer, for a primary with replicas (the
    /// queues' `queue.confirm_mode = replica`). Empty for one node, Galera
    /// (a commit is on every node already) and external clusters.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn ctl_replica_opts(&self, cluster: &ClusterId) -> Result<Vec<mysql_async::Opts>> {
        if cluster.as_str() == ENGINE_ONLY_CLUSTER {
            return Ok(Vec::new());
        }
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        if rec.topology != sprig_cluster::record::TopologyKind::PrimaryReplica {
            return Ok(Vec::new());
        }
        let writer = self
            .backends
            .writer(cluster, ShardId(0))
            .await
            .ok()
            .map(|n| n.node);
        let writer = writer.or_else(|| rec.nodes.first().map(|n| n.node.clone()));
        let mut out = Vec::new();
        for n in rec
            .nodes
            .iter()
            .filter(|n| Some(&n.node) != writer.as_ref())
        {
            out.push(CredentialConnector::ctl(&rec.secrets).endpoint(n)?.opts);
        }
        Ok(out)
    }

    /// A pool of `sprig_ctl` connections to `cluster`'s writer, for work
    /// done one step at a time: a task holding one of these takes no other
    /// connection (see [`Budget`]).
    ///
    /// # Errors
    /// 39104 for an unknown cluster; 39100 without a writer; 39998 for a
    /// credential reference that doesn't resolve.
    pub async fn ctl_pool(&self, cluster: &ClusterId) -> Result<Pool> {
        self.pool_of(cluster, Budget::Ctl).await
    }

    /// Sprig 2PC's relays on `cluster`: `None` when it has none (or isn't
    /// Galera), else the hosts its relays reach its nodes from (empty while
    /// no relay has published one).
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn relay_hosts(&self, cluster: &ClusterId) -> Result<Option<Vec<String>>> {
        let v = self.backends.view(cluster).await?;
        Ok(
            (v.relay != sprig_cluster::failover::relay::RelayState::None)
                .then(|| v.relay_hosts.clone()),
        )
    }

    /// The pool of `sprig_ctl` connections to `cluster`'s writer for a lock
    /// held across other work: a transaction whose row locks keep another
    /// gateway off the same rows (the cross-tenant outbox forward, a
    /// broadcast's fan-out) or a `GET_LOCK`, while the task delivers with
    /// catalog and [`Self::ctl_pool`] connections. A holder takes one of
    /// these first and never a second (see [`Budget`]); the deliveries
    /// can't wait for it, because they come from the other pools.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn ctl_hold_pool(&self, cluster: &ClusterId) -> Result<Pool> {
        self.pool_of(cluster, Budget::Hold).await
    }

    /// A `sprig_ctl` connection to `cluster`'s writer outside the pools:
    /// one a session holds across its transaction (record hierarchy
    /// locks). A pooled one could be held by every
    /// pool slot while each holder waits for another.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`]; 39101 when the writer doesn't answer.
    pub async fn ctl_conn(&self, cluster: &ClusterId) -> Result<mysql_async::Conn> {
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        let rec = stored.body();
        let writer = match self.backends.writer(cluster, ShardId(0)).await {
            Ok(n) => rec.nodes.iter().find(|s| s.node == n.node).cloned(),
            Err(_) => None,
        }
        .or_else(|| rec.nodes.first().cloned())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {cluster} has no writer"),
            )
        })?;
        let ep = CredentialConnector::ctl(&rec.secrets).endpoint(&writer)?;
        mysql_async::Conn::new(ep.opts).await.map_err(|e| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("cluster {cluster} unreachable"),
            )
            .with_source(e)
        })
    }

    /// Which `sprig_ctl` pool [`Self::ctl_pool`] hands out for `cluster`
    /// now (writer node, address and credential); it changes when the pool
    /// is replaced and the old one disconnected. `None` before the first.
    #[must_use]
    pub fn ctl_pool_fingerprint(&self, cluster: &ClusterId) -> Option<String> {
        self.pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(Budget::Ctl, cluster.clone()))
            .map(|(f, _)| f.clone())
    }

    /// [`Self::ctl_pool`] for work on `tenant`'s rows on `cluster`. For a
    /// tenant on Sprig 2PC's relay path it is a pool with the same options
    /// through the cluster's relay writer
    /// (`sprig_cluster::failover::relay_pools`), so the relay fence ends
    /// these connections with the tenant's own when a held branch's holder
    /// is lost, and refuses them until the branch is replayed.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`]; a retryable 39030 while the tenant is on the
    /// relay path and the cluster has no ready relay writer.
    pub async fn tenant_ctl_pool(&self, tenant: TenantId, cluster: &ClusterId) -> Result<Pool> {
        let direct = self.ctl_pool(cluster).await?;
        self.through_relay(tenant, cluster, direct, "ctl").await
    }

    /// [`Self::ctl_hold_pool`] for work on `tenant`'s rows, routed as
    /// [`Self::tenant_ctl_pool`].
    ///
    /// # Errors
    /// As [`Self::tenant_ctl_pool`].
    pub async fn tenant_hold_pool(&self, tenant: TenantId, cluster: &ClusterId) -> Result<Pool> {
        let direct = self.ctl_hold_pool(cluster).await?;
        // A pool apart from the other routed work: a holder waits for
        // work that takes connections from those.
        self.through_relay(tenant, cluster, direct, "hold").await
    }

    /// The catalog pool of `tenant`'s home cluster ([`Self::catalog_pool`]),
    /// routed as [`Self::tenant_ctl_pool`]: for the few catalog rows a
    /// tenant's own transactions also write (`stream_offsets`).
    ///
    /// # Errors
    /// As [`Self::tenant_ctl_pool`].
    pub async fn tenant_catalog_pool(&self, tenant: TenantId) -> Result<Pool> {
        let cluster = self.cluster(tenant).await?;
        let direct = self.catalog_pool(&cluster).await?;
        self.through_relay(tenant, &cluster, direct, "catalog")
            .await
    }

    /// [`Self::ctl_conn`] for a lock on `tenant`'s rows, routed as
    /// [`Self::tenant_ctl_pool`].
    ///
    /// # Errors
    /// As [`Self::ctl_conn`] and [`Self::tenant_ctl_pool`].
    pub async fn tenant_ctl_conn(
        &self,
        tenant: TenantId,
        cluster: &ClusterId,
    ) -> Result<mysql_async::Conn> {
        let probe = self.ctl_pool(cluster).await?;
        let base = self.ctl_opts(cluster).await?;
        let Some(r) = sprig_cluster::failover::relay_pools::tenant_route_with(
            &self.dc,
            tenant,
            cluster,
            &probe,
            &base,
            sprig_txn::fence::wire::preamble,
        )
        .await?
        else {
            return self.ctl_conn(cluster).await;
        };
        mysql_async::Conn::new(r.opts).await.map_err(|e| {
            let mut err = SprigError::new(
                SprigCode::BackendUnavailable,
                format!("cluster {cluster} unreachable through its relay writer"),
            )
            .with_source(e);
            err.retryable = true;
            err
        })
    }

    async fn through_relay(
        &self,
        tenant: TenantId,
        cluster: &ClusterId,
        direct: Pool,
        kind: &'static str,
    ) -> Result<Pool> {
        sprig_cluster::failover::relay_pools::tenant_pool_of(
            &self.dc,
            tenant,
            cluster,
            direct,
            kind,
            sprig_txn::fence::wire::preamble,
        )
        .await
    }

    /// The pool of `sprig_ctl` connections to `cluster`'s writer that the
    /// tenant catalogs there use ([`CatalogLocator::writer`]). Its
    /// connections are not reset when they go back to the pool, so their
    /// server-side prepared statements stay cached: with the reset (the
    /// driver's default) every catalog statement was prepared again, about
    /// 80 of a DDL statement's 160 round trips. Catalog work leaves no
    /// session state behind: its statements name their schema, lock waits
    /// are `SET STATEMENT`, transactions roll back when dropped, and a
    /// migration run resets its own connection.
    ///
    /// # Errors
    /// As [`Self::ctl_pool`].
    pub async fn catalog_pool(&self, cluster: &ClusterId) -> Result<Pool> {
        self.pool_of(cluster, Budget::Catalog).await
    }

    /// `cluster`'s record for [`Self::pool_of`]: as read within
    /// [`RECORD_TTL`], else read again (every pool lookup read it from the
    /// store, two of them per Bolt statement).
    async fn pool_record(
        &self,
        cluster: &ClusterId,
    ) -> Result<sprig_cluster::registry::StoredCluster> {
        if let Some((at, r)) = self
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            && at.elapsed() < RECORD_TTL
        {
            return Ok(r.clone());
        }
        let stored = self.registry.get(cluster).await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterNotFound,
                format!("no cluster {cluster} in dc {}", self.registry.dc()),
            )
        })?;
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(cluster.clone(), (std::time::Instant::now(), stored.clone()));
        Ok(stored)
    }

    async fn pool_of(&self, cluster: &ClusterId, budget: Budget) -> Result<Pool> {
        let catalog = budget == Budget::Catalog;
        let key = (budget, cluster.clone());
        if cluster.as_str() == ENGINE_ONLY_CLUSTER {
            // A tenant placed with no MariaDB cluster: a pool that never
            // connects (a use fails with a connection error).
            let mut pools = self.pools.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some((_, p)) = pools.get(&key) {
                return Ok(p.clone());
            }
            let opts = OptsBuilder::default()
                .ip_or_hostname("127.0.0.1")
                .tcp_port(9)
                .pool_opts(
                    PoolOpts::default()
                        .with_constraints(PoolConstraints::new(0, 1).unwrap_or_default())
                        .with_inactive_connection_ttl(IDLE_TTL),
                );
            let pool = Pool::new(opts);
            pools.insert(key, (ENGINE_ONLY_CLUSTER.to_owned(), pool.clone()));
            return Ok(pool);
        }
        let stored = self.pool_record(cluster).await?;
        let rec = stored.body();
        let writer = match self.backends.writer(cluster, ShardId(0)).await {
            Ok(n) => rec.nodes.iter().find(|s| s.node == n.node).cloned(),
            Err(e) => {
                tracing::debug!(%cluster, error = %e, "no tracked writer; using the record's first node");
                None
            }
        }
        .or_else(|| rec.nodes.first().cloned())
        .ok_or_else(|| {
            SprigError::new(
                SprigCode::ClusterUnavailable,
                format!("cluster {cluster} has no writer"),
            )
        })?;
        let fingerprint = format!("{}|{}|{}", writer.node, writer.addr, rec.secrets.ctl);
        if let Some((f, p)) = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            && *f == fingerprint
        {
            return Ok(p.clone());
        }
        let ep = CredentialConnector::ctl(&rec.secrets).endpoint(&writer)?;
        if let Some(u) = ep.opts.user() {
            self.ctl_users
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(cluster.clone(), u.to_owned());
        }
        let constraints = PoolConstraints::new(0, pool_max(budget)).unwrap_or_default();
        let mut opts = OptsBuilder::from_opts(ep.opts);
        if catalog {
            // Catalog statements name the tenant's schema, so a few
            // tenants' worth are kept per connection.
            opts = opts.stmt_cache_size(128);
        }
        let pool = Pool::new(
            opts
                // Idle connections are kept (up to POOL_MAX) for a minute.
                // With mysql_async's default TTL of 0, every connection
                // returned to a pool with a minimum of 0 is closed, so each
                // catalog read or login-state write opened a new connection
                // (thousands of TIME_WAIT sockets per minute under HTTP
                // load, one per request).
                .pool_opts(
                    PoolOpts::new()
                        .with_constraints(constraints)
                        .with_inactive_connection_ttl(IDLE_TTL)
                        .with_reset_connection(!catalog)
                        .with_abs_conn_ttl(catalog.then_some(CATALOG_CONN_TTL))
                        .with_abs_conn_ttl_jitter(catalog.then_some(CATALOG_CONN_JITTER)),
                ),
        );
        sprig_catalog::pool_wait::limit(
            &pool,
            budget.name(),
            cluster.as_str(),
            pool_max(budget),
            budget.wait(),
        );
        let old = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, (fingerprint, pool.clone()));
        if let Some((_, old)) = old {
            tokio::spawn(async move {
                let _ = old.disconnect().await;
            });
        }
        Ok(pool)
    }

    /// The provisioning account's user name on `cluster`, once a pool to it
    /// was made (output mapping hides it).
    #[must_use]
    pub fn ctl_user(&self, cluster: &ClusterId) -> Option<String> {
        self.ctl_users
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(cluster)
            .cloned()
    }

    /// Retires the pooled backend connections of `account` (its grants
    /// changed).
    pub fn retire_account(&self, account: &str) {
        self.backends.pools().retire_account(account);
    }

    /// Closes the pooled backend connections of every account of
    /// `tenant` (`t<tenant>_...`; `t3_` never matches `t30_`): the tenant
    /// was deleted.
    pub fn retire_tenant(&self, tenant: TenantId) {
        let prefix = format!("t{tenant}_");
        self.backends
            .pools()
            .retire_where(|a| a.starts_with(&prefix));
    }

    /// Closes every pool (shutdown).
    pub async fn close(&self) {
        let pools: Vec<Pool> = self
            .pools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain()
            .map(|(_, (_, p))| p)
            .collect();
        for p in pools {
            let _ = p.disconnect().await;
        }
    }
}

fn not_here(tenant: TenantId) -> SprigError {
    SprigError::new(
        SprigCode::TenantNotReady,
        format!(
            "tenant {tenant} has no catalog in this data center (not bootstrapped, or homed in \
             another DC, which needs the relay)"
        ),
    )
}

#[async_trait::async_trait]
impl CatalogLocator for GatewayLocator {
    async fn writer(&self, tenant: TenantId) -> Result<CatalogEndpoint> {
        let cluster = self.cluster(tenant).await?;
        Ok(CatalogEndpoint {
            pool: self.catalog_pool(&cluster).await?,
            store: Store::new(SchemaName::catalog(tenant).to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use sprig_cluster::TopologyKind;
    use sprig_cluster::record::{Capacity, NodeSpec, ProvisionerKind, SecretRefs};
    use sprig_cluster::registry::Registration;
    use sprig_common::ids::NodeId;
    use sprig_meta::MemStore;

    use super::*;
    use crate::backend::cluster::{Published, PublishedWriter};
    use crate::backend::{
        BackendSettings, ClusterSource, ClusterView, FixedSettings, NodeRef, StaticClusters,
    };

    fn view(id: &ClusterId, writer: &str) -> ClusterView {
        let nodes = ["n1", "n2"]
            .iter()
            .map(|n| NodeRef {
                node: NodeId::new(*n).unwrap(),
                addr: format!("{n}.db:3306"),
            })
            .collect();
        let mut v = ClusterView::new(id.clone(), TopologyKind::PrimaryReplica, nodes);
        v.published = Published {
            writer: PublishedWriter::Node {
                node: NodeId::new(writer).unwrap(),
                epoch: 1,
            },
            read_at: Instant::now(),
        };
        v
    }

    /// The pools' lookups read the cluster record once within its time,
    /// and still hand out a new pool at once when the writer changes.
    #[tokio::test]
    async fn pools_follow_the_writer_without_reading_the_record_each_time() {
        let creds = std::env::temp_dir().join(format!("gs-locator-{}.toml", std::process::id()));
        std::fs::write(
            &creds,
            "[sprig_ctl]\nuser = \"sprig_ctl\"\npassword = \"pw\"\n",
        )
        .unwrap();
        let dc_id = DcId::new("dc-a").unwrap();
        let global: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Global));
        let dc: Arc<dyn MetaStore> = Arc::new(MemStore::new(Tier::Dc(dc_id.clone())));
        let registry = ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dc)).unwrap();
        let stored = registry
            .register(Registration {
                name: "c1".into(),
                topology: TopologyKind::PrimaryReplica,
                provisioner: ProvisionerKind::External,
                nodes: ["n1", "n2"]
                    .iter()
                    .map(|n| NodeSpec {
                        node: NodeId::new(*n).unwrap(),
                        addr: format!("{n}.db:3306"),
                        zone: None,
                    })
                    .collect(),
                secrets: SecretRefs {
                    ctl: format!("file:{}#sprig_ctl", creds.display()),
                    repl: format!("file:{}#sprig_ctl", creds.display()),
                },
                capacity: Capacity::default(),
                declared: None,
            })
            .await
            .unwrap();
        let id = stored.body().cluster_id.clone();
        let clusters = StaticClusters::new();
        clusters.insert(view(&id, "n1"));
        let backends = Backends::new(
            dc_id,
            Arc::clone(&clusters) as Arc<dyn ClusterSource>,
            FixedSettings::new(BackendSettings::default()),
        );
        let l = GatewayLocator::new(Arc::clone(&dc), registry, backends);
        l.ctl_pool(&id).await.unwrap();
        let first = l.ctl_pool_fingerprint(&id).unwrap();
        assert!(first.starts_with("n1|"), "{first}");
        // The record gone from the store: lookups within its time still
        // answer from the record as read.
        dc.txn(sprig_meta::Txn {
            when: Vec::new(),
            then: vec![sprig_meta::Op::Delete(
                dc.tier().key(&format!("clusters/{id}")).unwrap(),
            )],
            otherwise: Vec::new(),
        })
        .await
        .unwrap();
        l.ctl_pool(&id).await.unwrap();
        assert_eq!(l.ctl_pool_fingerprint(&id).as_deref(), Some(first.as_str()));
        // A new writer: a new pool at once, the old one replaced.
        clusters.insert(view(&id, "n2"));
        l.ctl_pool(&id).await.unwrap();
        let second = l.ctl_pool_fingerprint(&id).unwrap();
        assert!(second.starts_with("n2|"), "{second}");
        let _ = std::fs::remove_file(&creds);
    }
}
