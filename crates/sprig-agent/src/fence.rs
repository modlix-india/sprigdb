//! The agent's self-fence of its MariaDB node: DC permits, self-fencing
//! and the writer lease of a primary with replicas.
//!
//! ```text
//! every 200 ms:
//!   serve = a valid DC permit
//!           and this agent holds leases/writer/<cluster> (a session it keeps)
//!           and the node doesn't replicate (a standby stays read-only
//!           until its promotion stops the replication)
//!   serve      -> read_only = OFF
//!   not serve  -> read_only = ON, then every tenant session killed
//!                 (once per change; read_only is checked every round)
//! watchdog, a plain OS thread:
//!   the runtime's heartbeat stalls failover.watchdog_ms -> fence directly
//!   (its own connection, its own runtime); twice that -> fence, abort
//!   the whole process was paused -> fence on resume
//! ```
//!
//! The agent registers `agents/<id>` (`cluster`, `node`, `addr`,
//! `mysqld`) under a lease, so the DC leader lists the node as
//! self-fencing in `dc-observe/<dc>`. MariaDB itself boots with
//! `read_only = ON`, so a node is writable only while its agent says so.
//!
//! **For the controller's `cluster_failover` loop**: a node that refuses
//! connections for `MYSQLD_DOWN_AFTER` is `down` in the registration, and
//! its agent hands the writer lease back ("the agent fences, then releases
//! the lease"); a node that only times out is slow and keeps it. A lease
//! the controller claimed for this node (`controller:<id>`) is taken over.
//!
//! **Semi-sync.** Before it clears `read_only`, the agent switches the
//! primary side of semi-sync off when no replica is attached, and back on
//! once one attaches: otherwise a returning primary whose standby was
//! promoted, or a promoted replica whose peers haven't reattached yet,
//! makes its first commit wait out `rpl_semi_sync_master_timeout`.
//!
//! **Restart hand-over.** A writer agent told its restart is coming
//! (SIGUSR1, which `sprig-vm.sh upgrade` sends before it replaces the
//! agent) doesn't fence its node when it stops. It records, under its
//! writer lease, until when it may be left writable (`leases/handover/
//! <cluster>`: the shorter of its permit's and its lease's time left, less
//! a margin) and exits without handing the lease back. Its successor (the
//! same agent id) finds its own lease and that record, and keeps the node
//! writable until then instead of fencing at once; it takes the lease over
//! with its first permit. Writes go on through the restart. If it isn't
//! back in time, it fences as usual.
//!
//! **Not yet.** The writer lease goes to whichever agent's node doesn't
//! replicate, not to a writer the cluster record names: these records
//! don't carry one yet. Two nodes that both stopped
//! replicating can't both be writable: the lease key is created only when
//! missing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use sprig_cluster::failover::primary_replica::{
    fence_node, semisync_guard, semisync_master_enabled, semisync_resume,
};
pub use sprig_cluster::failover::writer::{AgentRecord, MysqldState, WriterLease};
use sprig_cluster::failover::writer::{MYSQLD_DOWN_AFTER, writer_lease_key};
use sprig_cluster::record::{ClusterRecord, NodeSpec};
use sprig_cluster::sql::{NodeEndpoint, ProbeSession};
use sprig_common::config::AgentConfig;
use sprig_common::ids::{ClusterId, DcId, NodeId};
use sprig_common::time::{Clock, SystemClock};
use sprig_common::{CancellationToken, Result, SprigCode, SprigError};
use sprig_meta::permit::http::PermitKeeper;
use sprig_meta::permit::{PermitHolder, Watchdog};
use sprig_meta::session::SessionOpts;
use sprig_meta::{Cmp, Consistency, MetaStore, Op, Record, Session, Txn};

/// The loop's period.
pub const PERIOD: Duration = Duration::from_millis(200);
/// The registration and writer-lease TTL (`failover.writer_lease_seconds`'s
/// default).
pub const LEASE_TTL: Duration = Duration::from_secs(10);

/// How long a restart announced with SIGUSR1 stays announced.
pub const HANDOVER_ARMED_FOR: Duration = Duration::from_secs(60);
/// Taken off the time a stopping writer's permit and lease have left.
const HANDOVER_MARGIN: Duration = Duration::from_millis(500);
/// A shorter window isn't worth handing over: the agent fences instead.
const HANDOVER_MIN: Duration = Duration::from_secs(1);

fn invalid(msg: String) -> SprigError {
    SprigError::new(SprigCode::ConfigInvalid, msg)
}

/// What the agent fences: its node and how to reach it.
#[derive(Debug, Clone)]
pub struct Target {
    /// The cluster.
    pub cluster: ClusterId,
    /// The node.
    pub node: NodeId,
    /// The node's address.
    pub addr: String,
    /// The fencing account's endpoint.
    pub endpoint: NodeEndpoint,
}

/// Finds the node and its credentials from `[agent]` and the cluster record.
///
/// # Errors
/// 39998 for a missing cluster or node, or credentials that can't be
/// resolved; the store's errors.
pub async fn target(dc: &dyn MetaStore, cfg: &AgentConfig) -> Result<Target> {
    let cluster = cfg
        .cluster
        .as_deref()
        .ok_or_else(|| invalid("[agent] cluster is required to fence a node".into()))?;
    let cluster = ClusterId::new(cluster).map_err(|e| invalid(format!("[agent] cluster: {e}")))?;
    let key = dc.tier().key(&format!("clusters/{cluster}"))?;
    let kv = dc
        .get(&key, Consistency::Linearizable)
        .await?
        .ok_or_else(|| {
            invalid(format!(
                "[agent] cluster {cluster} isn't registered in this DC"
            ))
        })?;
    let rec = Record::<ClusterRecord>::decode(&kv.value)?.body;
    let node: &NodeSpec = match (&cfg.node, &cfg.mysql_addr) {
        (Some(n), _) => rec.nodes.iter().find(|s| s.node.as_str() == n.as_str()),
        (None, Some(a)) => rec.nodes.iter().find(|s| s.addr == *a),
        (None, None) if rec.nodes.len() == 1 => rec.nodes.first(),
        (None, None) => None,
    }
    .ok_or_else(|| invalid(format!("[agent] names no node of cluster {cluster}")))?;
    let addr = cfg.mysql_addr.clone().unwrap_or_else(|| node.addr.clone());
    let reference = cfg
        .credentials
        .clone()
        .unwrap_or_else(|| rec.secrets.ctl.clone());
    let cred = sprig_provision::credentials::resolve(&reference, "sprig_ctl")?;
    let endpoint = cred.endpoint(&NodeSpec {
        node: node.node.clone(),
        addr: addr.clone(),
        zone: None,
    })?;
    Ok(Target {
        cluster,
        node: node.node.clone(),
        addr,
        endpoint,
    })
}

/// Fences the node from any thread: its own runtime, its own connection,
/// at most 3 s. What the watchdog calls.
pub fn fence_blocking(ep: &NodeEndpoint) {
    let ep = ep.clone();
    let r = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        match rt {
            Ok(rt) => rt.block_on(async {
                tokio::time::timeout(Duration::from_secs(3), fence_node(&ep))
                    .await
                    .map_err(|_| "timed out".to_owned())
                    .and_then(|r| r.map_err(|e| e.to_string()))
            }),
            Err(e) => Err(e.to_string()),
        }
    })
    .join();
    match r {
        Ok(Ok(killed)) => tracing::warn!(killed, "watchdog: node fenced directly"),
        Ok(Err(e)) => tracing::error!(error = %e, "watchdog: direct fence failed"),
        Err(_) => tracing::error!("watchdog: direct fence panicked"),
    }
}

/// A lease-bound key the agent keeps: `agents/<id>` or the writer lease.
struct Held {
    key: sprig_meta::Key,
    session: Option<Session>,
}

impl Held {
    fn valid(&self, clock: &dyn Clock) -> bool {
        self.session
            .as_ref()
            .is_some_and(|s| !s.is_lost() && clock.now() < s.valid_until())
    }

    fn remaining(&self, clock: &dyn Clock) -> Duration {
        self.session.as_ref().map_or(Duration::ZERO, |s| {
            s.valid_until().saturating_duration_since(clock.now())
        })
    }

    /// Keeps the key: a new session after a lost one; the value put while
    /// the key is missing (or ours already, or, for the writer lease,
    /// claimed by the controller for `node`). `value` gets the lease found
    /// there, if any. Returns whether held.
    async fn keep(
        &mut self,
        store: &Arc<dyn MetaStore>,
        clock: &Arc<dyn Clock>,
        value: &(dyn Fn(Option<&WriterLease>) -> Option<Vec<u8>> + Send + Sync),
        exclusive: Option<(&str, &str)>,
    ) -> bool {
        if self.valid(&**clock) {
            return true;
        }
        if let Some(s) = self.session.take() {
            let _ = s.close().await;
        }
        let Ok(s) = Session::open(
            Arc::clone(store),
            Arc::clone(clock),
            SessionOpts::new(LEASE_TTL),
        )
        .await
        else {
            return false;
        };
        let mut when = Vec::new();
        let mut found: Option<WriterLease> = None;
        if let Some((me, node)) = exclusive {
            // Ours already (a restarted agent whose old lease lives on), or
            // claimed by the controller for this node (a promotion)?
            if let Ok(Some(kv)) = store.get(&self.key, Consistency::Linearizable).await {
                let held = Record::<WriterLease>::decode(&kv.value)
                    .ok()
                    .map(|r| r.body);
                if !held.as_ref().is_some_and(|l| l.may_be_held_by(me, node)) {
                    let _ = s.close().await;
                    return false;
                }
                if let Some(l) = held.as_ref().filter(|l| l.claimed_by_controller()) {
                    tracing::info!(
                        writer_epoch = l.writer_epoch,
                        "taking over the writer lease the controller claimed for this node"
                    );
                }
                found = held;
                when.push(Cmp::ModRev(self.key.clone(), kv.mod_revision));
            } else {
                when.push(Cmp::Missing(self.key.clone()));
            }
        }
        let Some(value) = value(found.as_ref()) else {
            let _ = s.close().await;
            return false;
        };
        let r = store
            .txn(Txn {
                when,
                then: vec![Op::Put {
                    key: self.key.clone(),
                    value,
                    lease: Some(s.lease()),
                }],
                otherwise: Vec::new(),
            })
            .await;
        if matches!(r, Ok(ref t) if t.succeeded) {
            self.session = Some(s);
            true
        } else {
            let _ = s.close().await;
            false
        }
    }

    /// Writes a new value under the session it holds (the registration
    /// when `mysqld`'s state changes).
    async fn put(&self, store: &Arc<dyn MetaStore>, value: Vec<u8>) -> bool {
        let Some(s) = self.session.as_ref() else {
            return false;
        };
        store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Put {
                    key: self.key.clone(),
                    value,
                    lease: Some(s.lease()),
                }],
                otherwise: Vec::new(),
            })
            .await
            .is_ok_and(|t| t.succeeded)
    }

    /// Hands the key back (revokes its lease).
    async fn release(&mut self) -> bool {
        match self.session.take() {
            Some(s) => {
                let _ = s.close().await;
                true
            }
            None => false,
        }
    }
}

/// The part of the controller's `cluster-failover/<cluster>` record the
/// agent reads: the last failovers' former writers.
#[derive(Debug, Default, Serialize, Deserialize)]
struct FailoverView {
    #[serde(default)]
    history: Vec<FormerView>,
    /// The writer the controller published, and its epoch: a lease this
    /// agent writes afresh carries that epoch.
    #[serde(default)]
    writer: Option<String>,
    #[serde(default)]
    writer_epoch: u64,
}

impl sprig_meta::Versioned for FailoverView {
    const V: u32 = 1;
}

#[derive(Debug, Serialize, Deserialize)]
struct FormerView {
    #[serde(default)]
    from: String,
    #[serde(default)]
    former: Option<String>,
}

/// Whether the controller's record keeps `node` out: the former writer of
/// its last failover that hasn't been rejoined or rebuilt (`fenced`,
/// `diverged`, `rebuilding`). Such a node must never take the writer
/// lease, even if the lease is free (a returning
/// former writer is checked, then rejoined or rebuilt, never served).
fn kept_out(v: &FailoverView, node: &str) -> bool {
    v.history
        .iter()
        .rev()
        .find(|e| e.from == node)
        .is_some_and(|e| {
            matches!(
                e.former.as_deref(),
                Some("fenced" | "diverged" | "rebuilding")
            )
        })
}

/// The writer epoch a lease this agent writes afresh carries: the
/// record's epoch when the record names `node` the writer, else 0.
fn lease_epoch(v: &FailoverView, node: &str) -> u64 {
    if v.writer.as_deref() == Some(node) {
        v.writer_epoch
    } else {
        0
    }
}

/// `leases/handover/<cluster>`: a stopping writer agent's note to its
/// successor, bound to its writer lease (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Handover {
    agent: String,
    node: String,
    /// Until when (Unix milliseconds) the node may stay writable.
    until_ms: u64,
}

impl sprig_meta::Versioned for Handover {
    const V: u32 = 1;
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Announces a restart: a stop within [`HANDOVER_ARMED_FOR`] hands a
/// writer's node over instead of fencing it (what SIGUSR1 does).
pub fn arm_handover(flag: &AtomicU64) {
    let until =
        unix_ms().saturating_add(u64::try_from(HANDOVER_ARMED_FOR.as_millis()).unwrap_or(u64::MAX));
    flag.store(until, Ordering::SeqCst);
    tracing::info!(
        secs = HANDOVER_ARMED_FOR.as_secs(),
        "restart announced: a stop in the next seconds leaves a writer's node writable for its successor"
    );
}

/// How long a stopping writer may leave its node writable: its permit's
/// and its lease's time left, less a margin; `None` when that is too short.
fn handover_window(permit_left: Duration, lease_left: Duration) -> Option<Duration> {
    let w = permit_left.min(lease_left).saturating_sub(HANDOVER_MARGIN);
    (w >= HANDOVER_MIN).then_some(w)
}

/// How long a starting agent keeps its node writable without a permit:
/// only when the writer lease is still its own (same agent and node, not
/// claimed by the controller) and its predecessor's hand-over names it,
/// until the hand-over's end, at most one lease TTL.
fn handover_grace(
    lease: Option<&WriterLease>,
    h: Option<&Handover>,
    agent: &str,
    node: &str,
    now_ms: u64,
) -> Option<Duration> {
    let lease = lease?;
    let h = h?;
    if lease.agent != agent || lease.node_id != node || h.agent != agent || h.node != node {
        return None;
    }
    let left = h.until_ms.checked_sub(now_ms).filter(|l| *l > 0)?;
    Some(Duration::from_millis(left).min(LEASE_TTL))
}

/// The agent's fence (see the module docs).
pub struct AgentFence {
    agent_id: String,
    dc: DcId,
    store: Arc<dyn MetaStore>,
    clock: Arc<dyn Clock>,
    target: Target,
    keeper: Arc<PermitKeeper>,
    /// Set while `node_rebuild` works on the node: never take the lease.
    hold: Arc<AtomicBool>,
    /// Until when (Unix ms) a restart is announced ([`arm_handover`]).
    handover: Arc<AtomicU64>,
    /// The internal service's URL, published in the registration.
    internal_url: Option<String>,
    rebuild: Option<Arc<crate::rebuild::RebuildService>>,
}

impl std::fmt::Debug for AgentFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentFence")
            .field("agent_id", &self.agent_id)
            .field("cluster", &self.target.cluster)
            .field("node", &self.target.node)
            .finish_non_exhaustive()
    }
}

impl AgentFence {
    /// An agent fence for `target`, holding permits from `store`'s DC.
    #[must_use]
    pub fn new(agent_id: String, dc: DcId, store: Arc<dyn MetaStore>, target: Target) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let holder = Arc::new(PermitHolder::new(
            agent_id.clone(),
            "agent",
            Arc::clone(&clock),
        ));
        let keeper = Arc::new(PermitKeeper::new(holder));
        // An agent fences from the start: without a permit, no writes.
        keeper.arm();
        Self {
            agent_id,
            dc,
            store,
            clock,
            target,
            keeper,
            hold: Arc::new(AtomicBool::new(false)),
            handover: Arc::new(AtomicU64::new(0)),
            internal_url: None,
            rebuild: None,
        }
    }

    /// The hold flag `node_rebuild` sets (see [`crate::rebuild`]).
    #[must_use]
    pub fn hold(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.hold)
    }

    /// Uses `flag` for restart announcements ([`arm_handover`]): the
    /// role's SIGUSR1 listener sets it.
    #[must_use]
    pub fn with_handover_flag(mut self, flag: Arc<AtomicU64>) -> Self {
        self.handover = flag;
        self
    }

    fn handover_armed(&self) -> bool {
        self.handover.load(Ordering::SeqCst) > unix_ms()
    }

    /// Stopping as the writer with a restart announced: records the
    /// hand-over under the writer lease, if the lease is still this
    /// agent's and the window is long enough. Returns whether it did.
    async fn hand_over(&self, writer: &Held, handover_key: &sprig_meta::Key) -> bool {
        let Some(s) = writer.session.as_ref() else {
            return false;
        };
        let Some(window) = handover_window(
            self.keeper.holder().remaining(),
            writer.remaining(&*self.clock),
        ) else {
            tracing::warn!(
                "restart announced, but the permit or the writer lease is about to end: fencing as usual"
            );
            return false;
        };
        let bounded = Duration::from_millis(800);
        let Ok(Ok(Some(kv))) = tokio::time::timeout(
            bounded,
            self.store.get(&writer.key, Consistency::Linearizable),
        )
        .await
        else {
            return false;
        };
        let ours = Record::<WriterLease>::decode(&kv.value).is_ok_and(|r| {
            r.body.agent == self.agent_id && r.body.node_id == self.target.node.as_str()
        });
        if !ours {
            return false;
        }
        let until_ms =
            unix_ms().saturating_add(u64::try_from(window.as_millis()).unwrap_or(u64::MAX));
        let Ok(value) = Record::new(Handover {
            agent: self.agent_id.clone(),
            node: self.target.node.to_string(),
            until_ms,
        })
        .encode() else {
            return false;
        };
        let r = tokio::time::timeout(
            bounded,
            self.store.txn(Txn {
                when: vec![Cmp::ModRev(writer.key.clone(), kv.mod_revision)],
                then: vec![Op::Put {
                    key: handover_key.clone(),
                    value,
                    lease: Some(s.lease()),
                }],
                otherwise: Vec::new(),
            }),
        )
        .await;
        let done = matches!(r, Ok(Ok(ref t)) if t.succeeded);
        if done {
            tracing::info!(
                cluster = %self.target.cluster,
                window_ms = window.as_millis(),
                "restart hand-over: the node stays writable and the writer lease is kept for the next agent"
            );
        }
        done
    }

    /// A starting agent: how long its predecessor's hand-over lets the
    /// node stay writable (see [`handover_grace`]).
    async fn read_handover(
        &self,
        writer_key: &sprig_meta::Key,
        handover_key: &sprig_meta::Key,
    ) -> Option<Duration> {
        let bounded = Duration::from_millis(800);
        let get = |k: &sprig_meta::Key| {
            let k = k.clone();
            let store = Arc::clone(&self.store);
            async move {
                match tokio::time::timeout(bounded, store.get(&k, Consistency::Linearizable)).await
                {
                    Ok(Ok(Some(kv))) => Some(kv.value),
                    _ => None,
                }
            }
        };
        let lease = get(writer_key)
            .await
            .and_then(|v| Record::<WriterLease>::decode(&v).ok())
            .map(|r| r.body);
        let h = get(handover_key)
            .await
            .and_then(|v| Record::<Handover>::decode(&v).ok())
            .map(|r| r.body);
        handover_grace(
            lease.as_ref(),
            h.as_ref(),
            &self.agent_id,
            self.target.node.as_str(),
            unix_ms(),
        )
    }

    /// Publishes the internal service at `url` and lets the rebuild
    /// service know when the rebuilt node replicates.
    #[must_use]
    pub fn with_rebuild(
        mut self,
        url: String,
        service: Arc<crate::rebuild::RebuildService>,
    ) -> Self {
        self.internal_url = Some(url);
        self.rebuild = Some(service);
        self
    }

    /// Runs until `stop`: the permit, the registration, the writer lease,
    /// the fence and its watchdog (`stall` = `failover.watchdog_ms`).
    ///
    /// # Errors
    /// Internal never in practice.
    #[allow(clippy::too_many_lines)] // one loop, every step in order
    pub async fn run(self, stall: Duration, stop: CancellationToken) -> Result<()> {
        let label = self.target.cluster.to_string();
        // The permit loop and its watchdog.
        let keeper_task = tokio::spawn(Arc::clone(&self.keeper).run(
            Arc::clone(&self.store),
            Duration::from_secs(1),
            stop.clone(),
        ));
        let beat = self.keeper.heartbeat();
        let (ep1, ep2) = (self.target.endpoint.clone(), self.target.endpoint.clone());
        let holder = Arc::clone(self.keeper.holder());
        let watchdog = Watchdog::spawn(
            Arc::new(move || beat.load(Ordering::SeqCst)),
            stall,
            Box::new(move || {
                holder.force_fence();
                fence_blocking(&ep1);
            }),
            Box::new(move || {
                fence_blocking(&ep2);
                tracing::error!(
                    "the agent's runtime is stuck: aborting (the stall watchdog fired)"
                );
                std::process::abort();
            }),
        );
        // At shutdown the permit loop's heartbeat stops, which isn't a
        // stuck runtime: the watchdog goes first.
        let sd = stop.clone();
        tokio::spawn(async move {
            sd.cancelled().await;
            std::thread::spawn(move || drop(watchdog));
        });
        let mut registration = Held {
            key: self
                .store
                .tier()
                .key(&format!("agents/{}", self.agent_id))?,
            session: None,
        };
        let mut writer = Held {
            key: writer_lease_key(self.store.tier(), &self.target.cluster)?,
            session: None,
        };
        let reg_value = |mysqld: MysqldState| {
            Record::new(AgentRecord {
                cluster: self.target.cluster.to_string(),
                node: self.target.node.to_string(),
                addr: self.target.addr.clone(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                mysqld,
                internal_url: self.internal_url.clone(),
            })
            .encode()
            .ok()
        };
        let (agent_id, node_id) = (self.agent_id.clone(), self.target.node.to_string());
        // The writer epoch carries over from a lease the controller claimed
        // for this node, or, when the lease is written afresh (a restarted
        // agent whose old lease ended with its session), from the failover
        // record that names this node the writer: a lease at epoch 0 after
        // a failover is one gateways refuse.
        let published_epoch = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let epoch_for_lease = Arc::clone(&published_epoch);
        let lease_value = move |found: Option<&WriterLease>| {
            Record::new(WriterLease {
                node_id: node_id.clone(),
                agent: agent_id.clone(),
                writer_epoch: found
                    .map_or(0, |l| l.writer_epoch)
                    .max(epoch_for_lease.load(Ordering::SeqCst)),
            })
            .encode()
            .ok()
        };
        let mut record_read = false;
        let mut conn: Option<ProbeSession> = None;
        // `None`: not known yet. MariaDB boots read-only.
        let mut serving: Option<bool> = None;
        // Since when connections are refused (not timed out).
        let mut refused_since: Option<std::time::Instant> = None;
        let mut mysqld = MysqldState::Unknown;
        let mut published = MysqldState::Unknown;
        // The agent switched semi-sync's primary side off (no replica).
        let mut semisync_suspended = false;
        let mut round: u64 = 0;
        // The controller keeps this node out (see `kept_out`).
        let mut kept = false;
        let failover_key = self
            .store
            .tier()
            .key(&format!("cluster-failover/{}", self.target.cluster))?;
        let handover_key = self
            .store
            .tier()
            .key(&format!("leases/handover/{}", self.target.cluster))?;
        // A predecessor's restart hand-over: until when the node stays
        // writable while this agent gets its permit and lease (checked
        // once, on the first round that reads the node).
        let mut grace: Option<std::time::Instant> = None;
        let mut grace_checked = false;
        tracing::info!(agent = %self.agent_id, dc = %self.dc, cluster = %self.target.cluster, node = %self.target.node, addr = %self.target.addr, "agent fencing its node");
        let bounded = Duration::from_millis(800);
        while !stop.is_cancelled() {
            round += 1;
            if conn.is_none() {
                match ProbeSession::connect(&self.target.endpoint, Duration::from_secs(2)).await {
                    Ok(c) => conn = Some(c),
                    // Refused (or reset): mysqld isn't running. A timeout
                    // is a slow or paused node; an error MariaDB sent
                    // means it runs.
                    Err(e) if !e.timeout && !e.is_server_error() => {
                        refused_since.get_or_insert_with(std::time::Instant::now);
                    }
                    Err(_) => refused_since = None,
                }
            }
            if conn.is_some() {
                refused_since = None;
                mysqld = MysqldState::Up;
            } else if refused_since.is_some_and(|t| t.elapsed() >= MYSQLD_DOWN_AFTER) {
                if mysqld != MysqldState::Down {
                    tracing::warn!(cluster = %label, "mysqld refuses connections: down");
                }
                mysqld = MysqldState::Down;
            }
            // A node that replicates is a replica or a standby: never the
            // writer.
            let replicating = match conn.as_mut() {
                Some(c) => match c.rows("SHOW SLAVE STATUS").await {
                    Ok(rows) => Some(!rows.is_empty()),
                    Err(_) => None,
                },
                None => None,
            };
            if replicating.is_none() {
                conn = None;
            }
            if replicating == Some(true)
                && let Some(r) = &self.rebuild
            {
                r.observe_replicating();
            }
            // A node the controller keeps out (a former writer not yet
            // rejoined, a diverged one, one being rebuilt) and a node under
            // rebuild never take the writer lease (r2close).
            if replicating == Some(false)
                && !writer.valid(&*self.clock)
                && (round.is_multiple_of(5) || !record_read)
            {
                kept = match tokio::time::timeout(
                    bounded,
                    self.store.get(&failover_key, Consistency::Linearizable),
                )
                .await
                {
                    Ok(Ok(Some(kv))) => {
                        record_read = true;
                        match Record::<FailoverView>::decode(&kv.value) {
                            Ok(r) => {
                                published_epoch.store(
                                    lease_epoch(&r.body, self.target.node.as_str()),
                                    Ordering::SeqCst,
                                );
                                kept_out(&r.body, self.target.node.as_str())
                            }
                            Err(_) => false,
                        }
                    }
                    Ok(Ok(None)) => {
                        record_read = true;
                        false
                    }
                    // Unknown: keep the last answer.
                    _ => kept,
                };
            }
            // Until the record has been read, a fresh lease could carry a
            // stale epoch: wait for it.
            let held_back = kept || !record_read || self.hold.load(Ordering::SeqCst);
            // The agent fences, then releases the lease: a down mysqld can't
            // be the writer, and the controller's cluster failover waits for
            // the lease to go.
            if mysqld == MysqldState::Down && writer.release().await {
                tracing::warn!(cluster = %label, "mysqld down: writer lease handed back");
            }
            // The permit first: without one the node fences now, before
            // any etcd call (they may hang while the DC is cut off).
            let permitted = self.keeper.holder().writable();
            if permitted {
                let v = reg_value(mysqld);
                let _ = tokio::time::timeout(
                    bounded,
                    registration.keep(&self.store, &self.clock, &move |_| v.clone(), None),
                )
                .await;
                if published != mysqld
                    && let Some(v) = reg_value(mysqld)
                    && tokio::time::timeout(bounded, registration.put(&self.store, v))
                        .await
                        .unwrap_or(false)
                {
                    published = mysqld;
                }
            }
            let lease_held = if permitted && replicating == Some(false) && !held_back {
                tokio::time::timeout(
                    bounded,
                    writer.keep(
                        &self.store,
                        &self.clock,
                        &lease_value,
                        Some((&self.agent_id, self.target.node.as_str())),
                    ),
                )
                .await
                .unwrap_or(false)
            } else {
                if replicating == Some(true) && writer.release().await {
                    tracing::info!(cluster = %label, "writer lease handed back: this node replicates");
                }
                writer.valid(&*self.clock) && replicating == Some(false)
            };
            let permit = self.keeper.holder().writable();
            let serve = permit && lease_held && replicating == Some(false);
            let read_only = match conn.as_mut() {
                Some(c) => c
                    .scalar("SELECT @@global.read_only")
                    .await
                    .ok()
                    .flatten()
                    .map(|v| v != "0" && !v.eq_ignore_ascii_case("off")),
                None => None,
            };
            if !grace_checked && read_only.is_some() && replicating.is_some() {
                grace_checked = true;
                if read_only == Some(false)
                    && replicating == Some(false)
                    && !serve
                    && let Some(left) = self.read_handover(&writer.key, &handover_key).await
                {
                    grace = Some(self.clock.now() + left);
                    tracing::info!(cluster = %label, left_ms = left.as_millis(), "restart hand-over: the node stays writable while the permit and the writer lease come back");
                }
            }
            let in_grace = !serve && grace.is_some_and(|g| self.clock.now() < g);
            if serve && grace.take().is_some() {
                tracing::info!(cluster = %label, "restart hand-over done: permit and writer lease back, the node never fenced");
                let _ = tokio::time::timeout(
                    bounded,
                    self.store.txn(Txn {
                        when: Vec::new(),
                        then: vec![Op::Delete(handover_key.clone())],
                        otherwise: Vec::new(),
                    }),
                )
                .await;
            }
            if in_grace {
                // The predecessor's permit and lease still cover the node.
            } else if serve {
                if read_only != Some(false)
                    && let Some(c) = conn.as_mut()
                {
                    // No semi-sync wait without a replica to acknowledge.
                    match semisync_guard(c).await {
                        Ok(true) => {
                            semisync_suspended = true;
                            tracing::warn!(cluster = %label, "no replica attached: semi-sync's primary side off until one attaches");
                        }
                        Ok(false) => {}
                        Err(e) => tracing::debug!(error = %e, "semi-sync guard"),
                    }
                    match c.exec("SET GLOBAL read_only = OFF").await {
                        Ok(()) => {
                            tracing::info!(cluster = %label, "writer: read_only OFF (permit, writer lease, not replicating)");
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "can't clear read_only");
                            conn = None;
                        }
                    }
                } else if round.is_multiple_of(5)
                    && let Some(c) = conn.as_mut()
                    && (semisync_suspended || matches!(semisync_master_enabled(c).await, Ok(false)))
                    && matches!(semisync_resume(c).await, Ok(true))
                {
                    // Also a writer whose mysqld restarted (it boots with
                    // the primary side off): with a
                    // replica attached, acknowledged commits wait for it
                    // again.
                    semisync_suspended = false;
                    tracing::info!(cluster = %label, "a replica attached: semi-sync's primary side on");
                }
            } else if serving != Some(false) || read_only == Some(false) {
                if grace.take().is_some() {
                    tracing::warn!(cluster = %label, "restart hand-over ended before the permit and the writer lease were back: fencing");
                }
                match fence_node(&self.target.endpoint).await {
                    Ok(killed) => {
                        tracing::warn!(cluster = %label, permit, lease_held, ?replicating, killed, "fenced: read_only ON, tenant sessions killed");
                    }
                    Err(e) => tracing::warn!(error = %e, "fence failed; trying again"),
                }
            }
            if !in_grace {
                serving = Some(serve);
            }
            metrics::gauge!("sprig_agent_fenced", "cluster" => label.clone())
                .set(if serve || in_grace { 0.0 } else { 1.0 });
            metrics::gauge!("sprig_agent_writer_lease_remaining_seconds", "cluster" => label.clone())
                .set(writer.remaining(&*self.clock).as_secs_f64());
            tokio::select! {
                () = stop.cancelled() => {}
                () = tokio::time::sleep(PERIOD) => {}
            }
        }
        // Stopping: the node fences itself, the leases are handed back;
        // unless a restart was announced and this node is the writer: then
        // the node stays writable and the writer lease is left to run, for
        // the next agent to take over (the module docs).
        let handed_over = serving == Some(true)
            && self.handover_armed()
            && self.hand_over(&writer, &handover_key).await;
        if !handed_over {
            let _ = fence_node(&self.target.endpoint).await;
        }
        if let Some(s) = registration.session {
            let _ = s.close().await;
        }
        if let Some(s) = writer.session {
            if handed_over {
                // Dropped, not closed: the lease runs out by itself unless
                // the next agent takes the key over first.
                drop(s);
            } else {
                let _ = s.close().await;
            }
        }
        if let Some(c) = conn {
            c.close().await;
        }
        let _ = keeper_task.await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(json: &str) -> FailoverView {
        Record::<FailoverView>::decode(json.as_bytes())
            .expect("record")
            .body
    }

    /// A restarted writer's agent writes its lease with the epoch of the
    /// last failover that made it the writer (a fresh lease at epoch 0
    /// after two failovers would be refused by every gateway).
    #[test]
    fn a_fresh_lease_carries_the_published_writer_epoch() {
        let v = view(
            r#"{"v":1,"state":"healthy","writer":"db1","writer_epoch":2,
                "history":[{"kind":"promote","from":"db2","to":"db1","writer_epoch":2,"former":"rejoined"}]}"#,
        );
        assert_eq!(lease_epoch(&v, "db1"), 2);
        assert_eq!(lease_epoch(&v, "db2"), 0, "not the published writer");
        assert!(!kept_out(&v, "db2"), "rejoined: not kept out");
        let fresh = view(r#"{"v":1,"state":"healthy"}"#);
        assert_eq!(lease_epoch(&fresh, "db1"), 0, "no failover yet");
    }

    fn lease(agent: &str, node: &str) -> WriterLease {
        WriterLease {
            node_id: node.into(),
            agent: agent.into(),
            writer_epoch: 3,
        }
    }

    fn note(agent: &str, node: &str, until_ms: u64) -> Handover {
        Handover {
            agent: agent.into(),
            node: node.into(),
            until_ms,
        }
    }

    /// A restarted writer agent keeps its node writable only while its own
    /// lease and its predecessor's note say so, and never past one lease TTL.
    #[test]
    fn a_restart_hand_over_covers_only_the_same_agent_and_node() {
        let now = 1_000_000;
        let (l, h) = (lease("agt-1", "db1"), note("agt-1", "db1", now + 3_000));
        assert_eq!(
            handover_grace(Some(&l), Some(&h), "agt-1", "db1", now),
            Some(Duration::from_secs(3))
        );
        assert_eq!(
            handover_grace(None, Some(&h), "agt-1", "db1", now),
            None,
            "no lease"
        );
        assert_eq!(
            handover_grace(Some(&l), None, "agt-1", "db1", now),
            None,
            "no note"
        );
        assert_eq!(
            handover_grace(Some(&l), Some(&h), "agt-1", "db1", now + 3_000),
            None,
            "the note has run out"
        );
        let claimed = lease("controller:ctl-1", "db1");
        assert_eq!(
            handover_grace(Some(&claimed), Some(&h), "agt-1", "db1", now),
            None,
            "a failover claimed the lease"
        );
        let other = lease("agt-2", "db2");
        assert_eq!(
            handover_grace(Some(&other), Some(&h), "agt-1", "db1", now),
            None
        );
        let foreign = note("agt-2", "db1", now + 3_000);
        assert_eq!(
            handover_grace(Some(&l), Some(&foreign), "agt-1", "db1", now),
            None
        );
        let long = note("agt-1", "db1", now + 60_000);
        assert_eq!(
            handover_grace(Some(&l), Some(&long), "agt-1", "db1", now),
            Some(LEASE_TTL),
            "never past one lease TTL"
        );
    }

    /// A stopping writer hands over only what its permit and lease still
    /// cover, less the margin.
    #[test]
    fn the_hand_over_window_is_the_shorter_of_permit_and_lease() {
        let s = Duration::from_secs;
        assert_eq!(
            handover_window(s(4), s(9)),
            Some(Duration::from_millis(3_500))
        );
        assert_eq!(
            handover_window(s(9), s(2)),
            Some(Duration::from_millis(1_500))
        );
        assert_eq!(handover_window(Duration::from_millis(1_200), s(9)), None);
        assert_eq!(handover_window(Duration::ZERO, s(9)), None);
    }

    #[test]
    fn an_announced_restart_expires() {
        let flag = AtomicU64::new(0);
        assert!(flag.load(Ordering::SeqCst) <= unix_ms());
        arm_handover(&flag);
        let until = flag.load(Ordering::SeqCst);
        assert!(until > unix_ms() + 50_000 && until <= unix_ms() + 60_000);
    }
}
