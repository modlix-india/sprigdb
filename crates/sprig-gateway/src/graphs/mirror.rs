//! Asynchronous mirrors in the gateway: mirror modes and freshness.
//!
//! - **Mode changes.** A grouped graph's `graphs.mirror_mode` follows
//!   `graph.mirror` (resolved at the graph's database scope), moved by the
//!   gateway that runs a statement on it, at most every
//!   [`CHECK_EVERY`] per graph:
//!
//!   ```text
//!   want async: register the tenant's `graph_mirror` subscription (the
//!               controller then keeps one applier job for the tenant)
//!     sync     -> starting          (writers still write mirrors; the
//!                                    applier starts serving the graph)
//!     starting -> async             once the applier wrote a position row
//!                                   for every shard of the map after the
//!                                   change to `starting` (it serves the
//!                                   graph, so it sees every later write)
//!   want sync (or off):
//!     starting -> sync
//!     async    -> sync, building    (writers write mirrors again; readers
//!                                    scatter until the applier has applied
//!                                    everything up to the clusters'
//!                                    positions taken after the change)
//!     building -> ready             then; the positions are deleted
//!   ```
//!
//! - **Freshness** ([`CatalogGate`]). A read statement on an `async` graph
//!   passes a gate holding the session's causal target to the store; the
//!   store asks it once, at its first incoming hop. The gate reads the
//!   binlog position of each of the graph's clusters now, and polls the
//!   positions until they reach both that and the session's target, up to
//!   `graph.mirror_max_wait_ms`; a missing or stale row (the applier isn't
//!   serving the graph) answers `false` at once. So mirrors never hide an
//!   edge committed before the statement.
//! - **Adaptive wait** ([`Lagging`]). When two gates of a graph in a row
//!   waited the whole budget and still found the applier behind, the
//!   graph's gates stop waiting for a while: each reads the positions
//!   once and scatters at once unless they are already fresh. Then one
//!   gate (the probe) waits the budget again; if the applier still lags,
//!   the next pause is twice as long (1 s up to 16 s), and a fresh gate
//!   ends the pauses. Without this, a lagging applier (writes in the mix,
//!   a slow etcd) made almost every read pay the whole budget before
//!   scattering anyway.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, Freshness, TenantCatalog};
use sprig_common::ids::{ClusterId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_graph::catalog::{GraphCatalog, backend_error};
use sprig_graph::maria::GraphShards;
use sprig_graph::mirror::{self, MirrorGate};
use sprig_meta::{Tier, TierView};

use super::{GatewayGraphs, GraphRef};

/// How often one gateway looks at a graph's mode.
pub(crate) const CHECK_EVERY: Duration = Duration::from_secs(2);

use sprig_graph::mirror::SUB_NAME;

/// How long a drain (`async` back to `sync`) waits for the applier.
const DRAIN_WAIT: Duration = Duration::from_secs(120);

/// Per-process state of mode changes.
#[derive(Debug, Default)]
pub(crate) struct MirrorState {
    checked: Mutex<HashMap<(u64, u64), Instant>>,
    subscribed: Mutex<HashSet<u64>>,
    draining: Mutex<HashSet<(u64, u64)>>,
}

/// The gate of one read statement: the session's causal target per
/// cluster, the catalog's positions, a bounded wait.
pub(crate) struct CatalogGate {
    pub(crate) graphs: Arc<GatewayGraphs>,
    pub(crate) tenant: TenantId,
    pub(crate) graph: u64,
    pub(crate) targets: BTreeMap<String, String>,
    pub(crate) wait: Duration,
}

/// Per (tenant, graph): the positions its last fresh gate read, and when.
type FreshSeen = HashMap<(u64, u64), (Vec<mirror::PosRow>, Instant)>;

/// Gates in a row that waited the whole budget for a lagging applier
/// before a graph's gates pause their waits.
const LAG_STREAK: u32 = 2;

/// The first pause of waits; it doubles after every probe that lagged.
const SKIP_FIRST: Duration = Duration::from_secs(1);

/// The longest pause of waits.
const SKIP_MAX: Duration = Duration::from_secs(16);

/// Per (tenant, graph): whether its applier lags behind the gates'
/// budget (see the module docs).
#[derive(Debug, Default)]
pub(crate) struct Lagging {
    /// Gates in a row that waited the whole budget and found it behind.
    streak: u32,
    /// Gates don't wait until then.
    skip_until: Option<Instant>,
    /// The next pause.
    skip_for: Duration,
    /// A gate is waiting to see whether the applier caught up.
    probing: bool,
}

/// What a gate whose first read wasn't fresh does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan {
    /// Wait up to the budget.
    Wait,
    /// Wait up to the budget, as the probe of a pause that ended.
    Probe,
    /// Don't wait: scatter.
    Skip,
}

impl Lagging {
    /// The plan of a gate that would wait, at `now`.
    pub(crate) fn plan(&mut self, now: Instant) -> Plan {
        match self.skip_until {
            None => Plan::Wait,
            Some(t) if now < t || self.probing => Plan::Skip,
            Some(_) => {
                self.probing = true;
                Plan::Probe
            }
        }
    }

    /// A gate found the mirrors fresh: waits resume.
    pub(crate) fn fresh(&mut self) {
        *self = Self::default();
    }

    /// A gate (`probe` or not) waited the whole budget and the applier
    /// was still behind.
    pub(crate) fn lagged(&mut self, now: Instant, probe: bool) {
        if probe {
            self.probing = false;
        }
        self.streak = self.streak.saturating_add(1);
        if self.streak < LAG_STREAK {
            return;
        }
        self.skip_for = if self.skip_for.is_zero() {
            SKIP_FIRST
        } else if probe {
            self.skip_for.saturating_mul(2).min(SKIP_MAX)
        } else {
            self.skip_for
        };
        let until = now + self.skip_for;
        self.skip_until = Some(self.skip_until.map_or(until, |t| t.max(until)));
    }

    /// A probe ended without an answer (an error, no connection).
    pub(crate) fn probe_ended(&mut self) {
        self.probing = false;
    }
}

/// [`Lagging`] of every graph this process gates.
fn lagging() -> &'static Mutex<HashMap<(u64, u64), Lagging>> {
    static C: std::sync::OnceLock<Mutex<HashMap<(u64, u64), Lagging>>> = std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What a gate found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    /// Fresh: the statement uses the mirrors.
    Fresh,
    /// Behind after the wait, or not served at all.
    Lag,
    /// Behind, and the graph's waits are paused.
    Skipped,
    /// No gate connection in time.
    Busy,
}

/// Clears a probe's mark if it ends without an answer.
struct ProbeGuard {
    key: (u64, u64),
    armed: bool,
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if self.armed
            && let Some(l) = lagging()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(&self.key)
        {
            l.probe_ended();
        }
    }
}

/// The positions each graph's last fresh gate read, and when.
fn fresh_seen() -> &'static Mutex<FreshSeen> {
    static C: std::sync::OnceLock<Mutex<FreshSeen>> = std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

#[async_trait]
impl MirrorGate for CatalogGate {
    fn last_fresh(&self, graph: u64) -> Option<Vec<mirror::PosRow>> {
        let m = fresh_seen().lock().unwrap_or_else(PoisonError::into_inner);
        let (rows, at) = m.get(&(self.tenant.0, graph))?;
        let aged = u64::try_from(at.elapsed().as_millis()).unwrap_or(u64::MAX);
        Some(
            rows.iter()
                .map(|r| mirror::PosRow {
                    age_ms: r.age_ms.saturating_add(aged),
                    ..r.clone()
                })
                .collect(),
        )
    }

    fn targets(&self) -> BTreeMap<String, String> {
        self.targets.clone()
    }

    async fn fresh(&self, layout: &GraphShards) -> bool {
        let started = Instant::now();
        let r = self.poll(layout, started).await;
        let outcome = match r {
            Ok(Found::Fresh) => "ok",
            Ok(Found::Lag) => "lag",
            Ok(Found::Skipped) => "skipped",
            Ok(Found::Busy) => "busy",
            Err(_) => "error",
        };
        metrics::counter!("sprig_graph_mirror_waits_total", "outcome" => outcome).increment(1);
        metrics::histogram!("sprig_graph_mirror_wait_seconds")
            .record(started.elapsed().as_secs_f64());
        matches!(r, Ok(Found::Fresh))
    }
}

impl CatalogGate {
    /// Whether the mirrors are fresh; [`Found::Busy`] when no gate
    /// connection came within [`GATE_CONN_WAIT`] (the statement scatters,
    /// as on a lag).
    async fn poll(&self, layout: &GraphShards, started: Instant) -> Result<Found> {
        let none = BTreeMap::new();
        // The catalog's pool and its first positions, at the same time as
        // the target below.
        let catalog_key = format!("catalog:{}", self.tenant);
        let first = async {
            let ep = self.graphs.svc.locator.writer(self.tenant).await?;
            let Some(pool) = gate_pool(&catalog_key, &ep.pool).await? else {
                return Ok(None);
            };
            let schema = ep.store.schema().to_owned();
            let Some(mut c) = gate_conn(&pool).await? else {
                return Ok(None);
            };
            let rows = mirror::read_pos(&mut c, &schema, self.graph).await?;
            Ok::<_, SprigError>(Some((pool, schema, rows)))
        };
        // The target: every transaction committed on the shards' clusters
        // before the statement (their binlog positions now), and the
        // session's causal target, so the mirrors never hide a committed
        // edge. The first positions are read at the same time (one round
        // trip, not two): positions only grow, so ones read before the
        // target that reach it are fresh, and ones that don't are read
        // again.
        let target = async {
            let mut targets = self.targets.clone();
            let mut asked: HashSet<&str> = HashSet::new();
            for s in &layout.shards {
                if !asked.insert(s.cluster.as_str()) {
                    continue;
                }
                let cluster = ClusterId::new(&s.cluster)
                    .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", s.cluster)))?;
                let ctl = self.graphs.pool(&cluster).await?;
                let key = format!("cluster:{}", s.cluster);
                let Some(pool) = gate_pool(&key, &ctl).await? else {
                    return Ok(None);
                };
                let Some(mut pc) = gate_conn(&pool).await? else {
                    return Ok(None);
                };
                let now: Option<String> = pc
                    .query_first("SELECT @@gtid_binlog_pos")
                    .await
                    .map_err(backend_error)?;
                // Both positions: `reached` keeps each domain's highest.
                let t = targets.entry(s.cluster.clone()).or_default();
                if !t.is_empty() {
                    t.push(',');
                }
                t.push_str(&now.unwrap_or_default());
            }
            Ok::<_, SprigError>(Some(targets))
        };
        let (targets, first) = futures::join!(target, first);
        let (Some(targets), Some((pool, schema, rows))) = (targets?, first?) else {
            return Ok(Found::Busy);
        };
        self.wait_fresh(&pool, &schema, layout, &targets, &none, started, rows)
            .await
    }

    /// Reads the positions again until they reach `targets` (see
    /// [`Self::poll`]), a gate connection for each read: none is held
    /// while the gate sleeps.
    #[allow(clippy::too_many_arguments)] // the poll's state
    async fn wait_fresh(
        &self,
        pool: &mysql_async::Pool,
        schema: &str,
        layout: &GraphShards,
        targets: &BTreeMap<String, String>,
        none: &BTreeMap<String, String>,
        started: Instant,
        first: Vec<mirror::PosRow>,
    ) -> Result<Found> {
        let key = (self.tenant.0, self.graph);
        let mut rows = first;
        let mut probe = ProbeGuard { key, armed: false };
        let mut planned = false;
        // The first read also tells whether the applier serves the graph
        // at all (else scatter without waiting).
        loop {
            if mirror::fresh(&rows, layout, targets) {
                fresh_seen()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(key, (rows, Instant::now()));
                if let Some(l) = lagging()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get_mut(&key)
                {
                    l.fresh();
                }
                probe.armed = false;
                return Ok(Found::Fresh);
            }
            // Not serving the graph (or behind a map change): no wait.
            if !mirror::fresh(&rows, layout, none) {
                return Ok(Found::Lag);
            }
            if !planned {
                planned = true;
                let plan = lagging()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(key)
                    .or_default()
                    .plan(Instant::now());
                match plan {
                    Plan::Skip => return Ok(Found::Skipped),
                    Plan::Probe => probe.armed = true,
                    Plan::Wait => {}
                }
            }
            if started.elapsed() >= self.wait {
                lagging()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(key)
                    .or_default()
                    .lagged(Instant::now(), probe.armed);
                probe.armed = false;
                return Ok(Found::Lag);
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
            let Some(mut c) = gate_conn(pool).await? else {
                return Ok(Found::Busy);
            };
            rows = mirror::read_pos(&mut c, schema, self.graph).await?;
        }
    }
}

/// Connections of mirror gates, by what they reach: a small pool of their
/// own per target, whose connections go back without a reset (the gate's
/// reads are plain SELECTs). Gates once kept idle connections taken from
/// the gateway's `sprig_ctl` and catalog pools (4 connections each) in a
/// cache of their own: under 32 concurrent Bolt sessions the cache held
/// all 4, and every gate that missed it waited on the pool forever (the
/// SF1 benchmark's `ConnectionReadTimeoutException`).
fn gate_pools() -> &'static Mutex<HashMap<String, mysql_async::Pool>> {
    static C: std::sync::OnceLock<Mutex<HashMap<String, mysql_async::Pool>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Connections one gate pool opens at most.
const GATE_POOL_MAX: usize = 16;

/// How long a gate waits for a connection before it gives up (the
/// statement scatters its incoming hops instead).
const GATE_CONN_WAIT: Duration = Duration::from_millis(250);

/// The gate pool of what `key` names, made once with `base`'s options;
/// `None` when `base` gave no connection within [`GATE_CONN_WAIT`].
async fn gate_pool(key: &str, base: &mysql_async::Pool) -> Result<Option<mysql_async::Pool>> {
    if let Some(p) = gate_pools()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(key)
    {
        return Ok(Some(p.clone()));
    }
    let Ok(c) = tokio::time::timeout(GATE_CONN_WAIT, base.get_conn()).await else {
        return Ok(None);
    };
    let opts = c.map_err(backend_error)?.opts().clone();
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::new()
                .with_constraints(
                    mysql_async::PoolConstraints::new(0, GATE_POOL_MAX).unwrap_or_default(),
                )
                .with_inactive_connection_ttl(Duration::from_secs(60))
                .with_abs_conn_ttl(Some(Duration::from_secs(15 * 60)))
                .with_reset_connection(false),
        ),
    );
    Ok(Some(
        gate_pools()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key.to_owned())
            .or_insert(pool)
            .clone(),
    ))
}

/// A connection of a gate pool; `None` after [`GATE_CONN_WAIT`].
async fn gate_conn(pool: &mysql_async::Pool) -> Result<Option<mysql_async::Conn>> {
    match tokio::time::timeout(GATE_CONN_WAIT, pool.get_conn()).await {
        Ok(c) => c.map(Some).map_err(backend_error),
        Err(_) => Ok(None),
    }
}

impl GatewayGraphs {
    /// Moves `g`'s mirror mode toward `graph.mirror` (see the
    /// module docs). `true` when the catalog changed, so the caller
    /// resolves the graph again. Failures are logged, never the
    /// statement's.
    pub(crate) async fn follow_mirror_setting(
        self: &Arc<Self>,
        g: &GraphRef,
        cat: &TenantCatalog,
    ) -> bool {
        match self.follow_inner(g, cat).await {
            Ok(changed) => changed,
            Err(e) => {
                tracing::debug!(error = %e, graph = g.graph_id, "graph.mirror not followed");
                false
            }
        }
    }

    async fn follow_inner(self: &Arc<Self>, g: &GraphRef, cat: &TenantCatalog) -> Result<bool> {
        if g.def.kind != "native" || g.def.group_id.is_none() {
            return Ok(false);
        }
        let key = (g.tenant.0, g.graph_id);
        {
            let mut m = self
                .mirrors
                .checked
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if m.get(&key).is_some_and(|t| t.elapsed() < CHECK_EVERY) {
                return Ok(false);
            }
            m.insert(key, Instant::now());
        }
        let ctx = cat.setting_ctx(&self.svc.dc).with_db(g.db_id);
        let want = self
            .svc
            .settings
            .get(&sprig_graph::settings::MIRROR, &ctx, Some(&cat.settings));
        let want_async = want == "async";
        if want_async {
            self.subscribe_mirrors(g.tenant).await?;
        }
        let ready = g.def.mirrors;
        let next = match (want_async, g.def.mirror_mode.as_str()) {
            (true, "sync") => Some(("starting", ready)),
            (true, "starting") => {
                if self.applier_serves(g).await? {
                    Some(("async", true))
                } else {
                    None
                }
            }
            (false, "starting") => Some(("sync", ready)),
            (false, "async") => Some(("sync", false)),
            _ => None,
        };
        let changed = if let Some((mode, ready)) = next {
            self.set_mirror(g, mode, ready).await?;
            tracing::info!(graph = g.graph_id, tenant = %g.tenant, mode, ready, "graph mirror mode changed");
            true
        } else {
            false
        };
        // A drain: back to `sync`, readers scatter until the applier has
        // applied what was written under `async`.
        let drain = matches!(next, Some(("sync", false)))
            || (!want_async && g.def.mirror_mode == "sync" && !ready);
        if drain {
            self.spawn_drain(g.clone());
        }
        Ok(changed)
    }

    /// Writes `mirror_mode` and `mirror_state` in a versioned change.
    async fn set_mirror(&self, g: &GraphRef, mode: &str, ready: bool) -> Result<()> {
        let mut ch = self
            .svc
            .catalog
            .change(g.tenant, VersionKind::Catalog)
            .await?;
        let r = {
            let (store, tx) = ch.tx().parts();
            GraphCatalog::new(store.schema())
                .set_mirror(
                    tx,
                    g.graph_id,
                    mode,
                    if ready { "ready" } else { "building" },
                )
                .await
        };
        if let Err(e) = r {
            let _ = ch.rollback().await;
            return Err(e);
        }
        ch.commit().await?;
        Ok(())
    }

    /// Whether the applier wrote a live position for every shard of `g`'s
    /// map since the graph's mode last changed.
    async fn applier_serves(&self, g: &GraphRef) -> Result<bool> {
        let ep = self.svc.locator.writer(g.tenant).await?;
        let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
        let schema = ep.store.schema().to_owned();
        let rows = mirror::read_pos_since_change(&mut c, &schema, g.graph_id).await?;
        Ok(mirror::fresh(&rows, &g.shards, &BTreeMap::new()))
    }

    /// Registers the tenant's `graph_mirror` subscription once per
    /// process (the controller keeps an applier job for each).
    async fn subscribe_mirrors(&self, tenant: TenantId) -> Result<()> {
        if self
            .mirrors
            .subscribed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&tenant.0)
        {
            return Ok(());
        }
        let home = self.svc.locator.home_of(tenant);
        let dc = self.svc.locator.dc_store();
        let store = if home == self.svc.dc {
            dc
        } else {
            TierView::over(&dc, Tier::Dc(home))
        };
        let subs = sprig_feed::subs::Subscriptions::new(store);
        let sub = sprig_feed::subs::Subscription {
            tenant,
            name: SUB_NAME.into(),
            kind: SUB_NAME.into(),
            ordering: "merged".into(),
            owner_ref: None,
            state: "active".into(),
            created_at_ms: u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis()),
            )
            .unwrap_or(0),
        };
        match subs.register(&sub).await {
            Ok(()) => {}
            Err(e) if e.code() == Some(SprigCode::FeedSubscriptionExists) => {}
            Err(e) => return Err(e),
        }
        self.mirrors
            .subscribed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tenant.0);
        Ok(())
    }

    /// Waits (in the background) until the applier has applied everything
    /// up to the shards' clusters' positions now, then marks the mirrors
    /// `ready` and deletes the positions.
    fn spawn_drain(self: &Arc<Self>, g: GraphRef) {
        let key = (g.tenant.0, g.graph_id);
        if !self
            .mirrors
            .draining
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key)
        {
            return;
        }
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let r = me.drain(&g).await;
            match &r {
                Ok(true) => tracing::info!(graph = g.graph_id, "graph mirrors drained: ready"),
                Ok(false) => tracing::warn!(
                    graph = g.graph_id,
                    "graph mirrors still building: the applier didn't catch up"
                ),
                Err(e) => {
                    tracing::warn!(graph = g.graph_id, error = %e, "graph mirror drain failed");
                }
            }
            me.mirrors
                .draining
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        });
    }

    async fn drain(&self, g: &GraphRef) -> Result<bool> {
        // Positions now, taken after the change to `sync` committed.
        let mut targets: BTreeMap<String, String> = BTreeMap::new();
        for s in &g.shards.shards {
            if targets.contains_key(&s.cluster) {
                continue;
            }
            let cluster = ClusterId::new(&s.cluster)
                .map_err(|e| SprigError::internal(format!("cluster id {}: {e}", s.cluster)))?;
            let pool = self.pool(&cluster).await?;
            let mut c = pool.get_conn().await.map_err(backend_error)?;
            let p: Option<String> = c
                .query_first("SELECT @@gtid_binlog_pos")
                .await
                .map_err(backend_error)?;
            targets.insert(s.cluster.clone(), p.unwrap_or_default());
        }
        let started = Instant::now();
        loop {
            let cat = self.catalog(g.tenant, Freshness::Latest).await?;
            let Some(now) = self.resolve(&cat, &g.db, &g.name).await? else {
                return Ok(true); // dropped
            };
            if now.def.mirror_mode != "sync" || now.def.mirrors {
                return Ok(true); // someone else finished, or async again
            }
            let ep = self.svc.locator.writer(g.tenant).await?;
            let schema = ep.store.schema().to_owned();
            // Not held across the mode change (a catalog change of its own
            // from the same pool) or the wait.
            let rows = {
                let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
                mirror::read_pos(&mut c, &schema, g.graph_id).await?
            };
            // The map may have changed since: compare against the targets
            // of the clusters it uses now (a new cluster's position was
            // read by the applier after the change, so any row will do).
            let t: BTreeMap<String, String> = now
                .shards
                .shards
                .iter()
                .filter_map(|s| {
                    targets
                        .get(&s.cluster)
                        .map(|p| (s.cluster.clone(), p.clone()))
                })
                .collect();
            if mirror::fresh(&rows, &now.shards, &t) {
                self.set_mirror(&now, "sync", true).await?;
                let mut c = ep.pool.get_conn().await.map_err(backend_error)?;
                mirror::clear_pos(&mut c, &schema, g.graph_id).await?;
                return Ok(true);
            }
            if started.elapsed() > DRAIN_WAIT {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waits_pause_after_two_lags_and_a_probe_decides() {
        let t0 = Instant::now();
        let mut l = Lagging::default();
        assert_eq!(l.plan(t0), Plan::Wait);
        l.lagged(t0, false);
        // One lag isn't a streak yet.
        assert_eq!(l.plan(t0), Plan::Wait);
        l.lagged(t0, false);
        // Paused for a second: gates don't wait.
        assert_eq!(l.plan(t0 + Duration::from_millis(500)), Plan::Skip);
        // The pause ended: one probe waits, the others still skip.
        let t1 = t0 + SKIP_FIRST;
        assert_eq!(l.plan(t1), Plan::Probe);
        assert_eq!(l.plan(t1), Plan::Skip);
        // The probe lagged too: the next pause is twice as long.
        l.lagged(t1, true);
        assert_eq!(l.plan(t1 + Duration::from_millis(1_900)), Plan::Skip);
        let t2 = t1 + SKIP_FIRST * 2;
        assert_eq!(l.plan(t2), Plan::Probe);
        // A probe without an answer lets the next gate probe.
        l.probe_ended();
        assert_eq!(l.plan(t2), Plan::Probe);
        // Fresh: waits resume.
        l.fresh();
        assert_eq!(l.plan(t2), Plan::Wait);
    }

    #[test]
    fn pauses_stop_growing_at_the_maximum() {
        let mut t = Instant::now();
        let mut l = Lagging::default();
        l.lagged(t, false);
        l.lagged(t, false);
        for _ in 0..10 {
            t += SKIP_MAX;
            assert_eq!(l.plan(t), Plan::Probe);
            l.lagged(t, true);
        }
        assert_eq!(l.skip_for, SKIP_MAX);
        assert_eq!(
            l.plan(t + SKIP_MAX.saturating_sub(Duration::from_millis(1))),
            Plan::Skip
        );
    }
}
