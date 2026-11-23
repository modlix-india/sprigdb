//! Replicated units in the simulator: three engine nodes, each running the
//! real engine (in-memory store, a fresh one per incarnation) committing
//! its tenant units through the real `sprig-raft` driver, with the node's
//! own simulated Raft disk ([`MemLogStore`]: a crash loses unsynced writes),
//! clock and network endpoint. Each tenant unit has the three nodes as
//! voters.
//!
//! Clients run bank transfers between accounts of each tenant (each
//! transfer one transaction on the unit's leader) and insert rows with
//! unique keys, while the fault plan kills nodes (they restart and rebuild
//! their store from the Raft log), partitions them, cuts links one way,
//! slows and drops messages, pauses nodes and skews clocks. Checked:
//!
//! - **consistent cuts within a unit:** at random moments a reader takes
//!   one read view on a replica (the leader, or a follower) and sums the
//!   tenant's accounts: the sum never changes;
//! - **every acknowledged commit survives** the faults (at most a minority
//!   is down at a time): after healing, every acknowledged row is on every
//!   replica;
//! - **replicas converge:** each unit's replicas end with equal checksums,
//!   and the account totals hold on every one.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_common::time::Clock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::{SimDisk, SyncMode};
use sprig_engine::txn::hlc::CommonClock;
use sprig_engine::txn::{LockRequest, Locked, TxnOptions};
use sprig_engine::{Engine, EngineClock, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineNode, EngineNodeOptions, MemLogStore, NodeId, RaftConfig, Transport};

use super::raft::{RaftCluster, RaftSimConfig, TrueClock, name};
use super::{FaultPlan, SimNet, SimResult, SimRng, World};
use crate::faults::Node;

/// The simulated cluster's shape and workload.
#[derive(Debug, Clone)]
pub struct ReplSimConfig {
    /// Nodes (all voters of every unit), ids 1..N.
    pub nodes: u64,
    /// Tenants, each its own unit.
    pub tenants: u64,
    /// Accounts per tenant.
    pub accounts: u64,
    /// Client tasks.
    pub clients: u64,
    /// How long clients run.
    pub workload: Duration,
    /// Faults in the plan.
    pub faults: usize,
    /// Raft timings.
    pub raft: RaftConfig,
}

impl Default for ReplSimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            tenants: 2,
            accounts: 6,
            clients: 4,
            workload: Duration::from_secs(20),
            faults: 6,
            raft: RaftConfig {
                static_voters: vec![1, 2, 3],
                ..RaftConfig::default()
            },
        }
    }
}

/// What a run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplRunStats {
    /// Transfers acknowledged.
    pub transfers: u64,
    /// Rows acknowledged (each checked on every replica at the end).
    pub rows: u64,
    /// Commits refused or with an unknown outcome.
    pub failed: u64,
    /// Read views summed during the run (each a consistent cut).
    pub cuts: u64,
    /// Of those, on a replica that wasn't leading the unit.
    pub follower_cuts: u64,
}

const START: i64 = 1000;

fn key(tenant: u64, object: u32, id: u64) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn account(tenant: u64, i: u64) -> Vec<u8> {
    key(tenant, 1, i)
}

fn row(tenant: u64, id: u64) -> Vec<u8> {
    key(tenant, 2, id)
}

fn tenant_of(i: u64) -> u64 {
    // Away from tenant ids other suites use in one process.
    7_000 + i
}

struct SimTransport {
    net: SimNet,
    from: Node,
}

impl Transport for SimTransport {
    fn send(&self, to: NodeId, frame: Vec<u8>) {
        self.net.send(&self.from, &name(to), frame);
    }
}

struct Member {
    id: NodeId,
    disk: Arc<MemLogStore>,
    clock: Arc<TrueClock>,
    dir: PathBuf,
    current: Mutex<Option<(Engine, Arc<EngineNode>)>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Closes the node when its process stops (a crash drops the future).
struct CloseOnDrop(Arc<EngineNode>, Arc<Member>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
        *lock(&self.1.current) = None;
    }
}

/// A simulated cluster of replicated engine nodes.
pub struct ReplCluster {
    world: World,
    cfg: ReplSimConfig,
    members: Vec<Arc<Member>>,
    stats: Mutex<ReplRunStats>,
    /// Acknowledged rows: (tenant, id).
    acked: Mutex<Vec<(u64, u64)>>,
}

impl std::fmt::Debug for ReplCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplCluster").finish_non_exhaustive()
    }
}

impl ReplCluster {
    /// A fault plan for this cluster (the Raft simulator's: kills with
    /// restarts, partitions, one-way cuts, slow lossy links, pauses, clock
    /// skew).
    #[must_use]
    pub fn plan(rng: &SimRng, cfg: &ReplSimConfig) -> FaultPlan {
        RaftCluster::plan(
            rng,
            &RaftSimConfig {
                nodes: cfg.nodes,
                voters: cfg.nodes,
                workload: cfg.workload,
                faults: cfg.faults,
                ..RaftSimConfig::default()
            },
        )
    }

    /// Starts every node under the world's supervisor.
    #[must_use]
    pub fn start(world: &World, cfg: ReplSimConfig) -> Arc<Self> {
        let origin = tokio::time::Instant::now();
        let base = std::env::temp_dir().join(format!(
            "sprig-sim-repl-{}-{}",
            std::process::id(),
            world.seed()
        ));
        let _ = std::fs::remove_dir_all(&base);
        let members = (1..=cfg.nodes)
            .map(|id| {
                Arc::new(Member {
                    id,
                    disk: MemLogStore::new(),
                    clock: Arc::new(TrueClock::new(world.clock(&name(id)), origin)),
                    dir: base.join(format!("n{id}")),
                    current: Mutex::new(None),
                })
            })
            .collect();
        let c = Arc::new(Self {
            world: world.clone(),
            cfg,
            members,
            stats: Mutex::new(ReplRunStats::default()),
            acked: Mutex::new(Vec::new()),
        });
        for m in &c.members {
            c.spawn_member(m);
        }
        c
    }

    fn spawn_member(&self, m: &Arc<Member>) {
        let m = Arc::clone(m);
        let raft = self.cfg.raft.clone();
        self.world.spawn_node(&name(m.id), move |ctx| {
            let m = Arc::clone(&m);
            let raft = raft.clone();
            async move {
                if ctx.incarnation > 0 {
                    let loss = m
                        .disk
                        .crash(ctx.world.seed() ^ (m.id << 32) ^ ctx.incarnation);
                    ctx.world.record(format!(
                        "repl n{} disk crash: {} unsynced batches lost, {} kept",
                        m.id, loss.unsynced, loss.kept
                    ));
                }
                // The store is in memory: each incarnation starts empty and
                // rebuilds from the Raft log (never trimmed here).
                let mut c = EngineConfig::new(m.dir.join(format!("i{}", ctx.incarnation)));
                c.store = StoreKind::Mem;
                c.checkpoint_interval = Duration::ZERO;
                c.log.sync = SyncMode::Off;
                c.log.manual = true;
                c.log.sim = Some(SimDisk::new());
                c.budget.engine_mb = 32;
                c.replicated = true;
                c.clock = Some(EngineClock(Arc::new(CommonClock(
                    ctx.world.clock(&name(m.id)),
                ))));
                let engine = match Engine::open(c) {
                    Ok(e) => e,
                    Err(e) => {
                        ctx.world
                            .fail(format!("n{} engine failed to open: {e}", m.id));
                        return;
                    }
                };
                let en = match EngineNode::open(
                    &engine,
                    EngineNodeOptions {
                        id: m.id,
                        config: raft,
                        store: Arc::clone(&m.disk) as Arc<dyn sprig_raft::LogStore>,
                        transport: Arc::new(SimTransport {
                            net: ctx.world.net().clone(),
                            from: ctx.node.clone(),
                        }),
                        clock: Arc::clone(&m.clock) as Arc<dyn Clock>,
                        seed: ctx.world.seed() ^ ctx.incarnation.wrapping_mul(0x9e37),
                        observer: None,
                        inline_roles: true,
                    },
                ) {
                    Ok(n) => Arc::new(n),
                    Err(e) => {
                        ctx.world
                            .fail(format!("n{} raft failed to open: {e}", m.id));
                        return;
                    }
                };
                *lock(&m.current) = Some((engine, Arc::clone(&en)));
                let node = en.node().clone();
                let _close = CloseOnDrop(en, Arc::clone(&m));
                let mut ep = ctx.world.net().endpoint(&ctx.node);
                let rx = node.clone();
                let recv = async move {
                    while let Some(env) = ep.recv().await {
                        rx.deliver(env.payload);
                    }
                };
                tokio::select! {
                    biased;
                    () = node.run() => {}
                    () = recv => {}
                }
            }
        });
    }

    fn engine(&self, id: NodeId) -> Option<Engine> {
        let m = self
            .members
            .get(usize::try_from(id).ok()?.checked_sub(1)?)?;
        if self.world.net().state(&name(id)) != super::NodeState::Running {
            return None;
        }
        lock(&m.current).as_ref().map(|(e, _)| e.clone())
    }

    /// A running node that leads `unit` for its engine, highest epoch
    /// (term) first.
    fn leader(&self, unit: UnitId) -> Option<Engine> {
        (1..=self.cfg.nodes)
            .filter_map(|id| self.engine(id))
            .filter(|e| e.leader_epoch(unit) > 0)
            .max_by_key(|e| e.leader_epoch(unit))
    }

    /// Writes the starting balances once (retrying across leaders).
    async fn init(&self, tenant: u64) {
        let unit = Engine::unit_of_tenant(tenant);
        loop {
            if let Some(e) = self.leader(unit) {
                let done = async {
                    let mut t = e.begin(TxnOptions::default()).ok()?;
                    if let Ok(Locked::Row(Some(_))) = t
                        .lock_get(&key(tenant, 3, 0).into(), LockRequest::for_update())
                        .await
                    {
                        t.rollback();
                        return Some(());
                    }
                    for i in 0..self.cfg.accounts {
                        t.put(account(tenant, i), START.to_be_bytes().to_vec())
                            .ok()?;
                    }
                    t.put(key(tenant, 3, 0), b"1".to_vec()).ok()?;
                    t.commit().ok()?.wait_async().await.ok()
                };
                if done.await.is_some() {
                    self.world
                        .record(format!("repl tenant {tenant} initialised"));
                    return;
                }
            } else {
                // A unit nobody wrote yet starts on first use.
                for id in 1..=self.cfg.nodes {
                    if let Some(e) = self.engine(id) {
                        let Ok(mut t) = e.begin(TxnOptions::default()) else {
                            continue;
                        };
                        let _ = t.put(key(tenant, 3, 1), b"x".to_vec());
                        let _ = t.commit();
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Runs the clients and the readers for `cfg.workload`.
    pub async fn run_workload(self: &Arc<Self>) {
        for i in 0..self.cfg.tenants {
            self.init(tenant_of(i)).await;
        }
        let deadline = tokio::time::Instant::now() + self.cfg.workload;
        let mut tasks = Vec::new();
        for cid in 0..self.cfg.clients {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn(&format!("repl client {cid}"), async move {
                me.client(cid, deadline).await;
            }));
        }
        let me = Arc::clone(self);
        tasks.push(
            self.world
                .spawn("repl reader", async move { me.reader(deadline).await }),
        );
        for t in tasks {
            let _ = t.await;
        }
    }

    async fn client(&self, cid: u64, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        let mut n = 0u64;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_millis(5), Duration::from_millis(60)))
                .await;
            let tenant = tenant_of(rng.below(self.cfg.tenants));
            let unit = Engine::unit_of_tenant(tenant);
            let Some(e) = self.leader(unit) else { continue };
            let from = rng.below(self.cfg.accounts);
            let to = (from + 1 + rng.below(self.cfg.accounts - 1)) % self.cfg.accounts;
            let amount = 1 + i64::try_from(rng.below(9)).unwrap_or(0);
            n += 1;
            let id = cid * 1_000_000 + n;
            let res = async {
                let mut t = e.begin(TxnOptions::default())?;
                let mut bal = [0i64; 2];
                for (j, a) in [from, to].into_iter().enumerate() {
                    bal[j] = match t
                        .lock_get(&account(tenant, a).into(), LockRequest::for_update())
                        .await?
                    {
                        Locked::Row(Some(v)) => {
                            i64::from_be_bytes(v.as_ref().try_into().map_err(|_| {
                                sprig_common::SprigError::internal("a corrupt balance")
                            })?)
                        }
                        _ => {
                            return Err(sprig_common::SprigError::internal("a missing account"));
                        }
                    };
                }
                t.put(
                    account(tenant, from),
                    (bal[0] - amount).to_be_bytes().to_vec(),
                )?;
                t.put(
                    account(tenant, to),
                    (bal[1] + amount).to_be_bytes().to_vec(),
                )?;
                t.put(row(tenant, id), id.to_be_bytes().to_vec())?;
                t.commit()?.wait_async().await
            }
            .await;
            match res {
                Ok(()) => {
                    lock(&self.stats).transfers += 1;
                    lock(&self.stats).rows += 1;
                    lock(&self.acked).push((tenant, id));
                }
                Err(e) => {
                    if e.message.contains("a missing account") || e.message.contains("corrupt") {
                        self.world.fail(format!("repl client {cid}: {e}"));
                    }
                    lock(&self.stats).failed += 1;
                }
            }
        }
    }

    /// Sums a tenant's accounts in one read view on replica `e`.
    fn sum(&self, e: &Engine, tenant: u64) -> Option<i64> {
        // On a follower the view takes the unit's applied timestamp.
        let mut t = e.begin(TxnOptions::default()).ok()?;
        let mut s = 0i64;
        for i in 0..self.cfg.accounts {
            let v = t.get_snapshot(&account(tenant, i)).ok()??;
            s += i64::from_be_bytes(v.as_ref().try_into().ok()?);
        }
        t.rollback();
        Some(s)
    }

    async fn reader(&self, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        let want = START * i64::try_from(self.cfg.accounts).unwrap_or(0);
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_millis(20), Duration::from_millis(200)))
                .await;
            let tenant = tenant_of(rng.below(self.cfg.tenants));
            let id = 1 + rng.below(self.cfg.nodes);
            let Some(e) = self.engine(id) else { continue };
            // A replica that hasn't applied the starting balances yet has
            // no accounts: nothing to sum.
            if let Some(s) = self.sum(&e, tenant) {
                let mut st = lock(&self.stats);
                st.cuts += 1;
                if e.leader_epoch(Engine::unit_of_tenant(tenant)) == 0 {
                    st.follower_cuts += 1;
                }
                if s != want {
                    self.world.fail(format!(
                        "repl: a read view on n{id} sums tenant {tenant}'s accounts to {s}, not {want}"
                    ));
                }
            }
        }
    }

    /// Heals everything, lets the replicas converge, and checks the end
    /// state.
    ///
    /// # Errors
    /// An invariant broke.
    pub async fn check(self: &Arc<Self>) -> Result<ReplRunStats, super::SimError> {
        self.world.heal_all().await;
        let want = START * i64::try_from(self.cfg.accounts).unwrap_or(0);
        for i in 0..self.cfg.tenants {
            let tenant = tenant_of(i);
            let unit = Engine::unit_of_tenant(tenant);
            // A commit on the current leader brings every replica to the
            // same applied index.
            self.init(tenant).await;
            let mut sums = BTreeMap::new();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                sums.clear();
                for id in 1..=self.cfg.nodes {
                    if let Some(e) = self.engine(id)
                        && let Ok(c) = e.unit_checksum(unit)
                    {
                        sums.insert(id, c);
                    }
                }
                let first = sums.values().next().copied();
                if sums.len() as u64 == self.cfg.nodes
                    && sums.values().all(|c| Some(c) == first.as_ref())
                {
                    break;
                }
                if tokio::time::Instant::now() > deadline {
                    return Err(format!(
                        "repl: tenant {tenant}'s replicas didn't converge: {sums:?}"
                    )
                    .into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            for id in 1..=self.cfg.nodes {
                let Some(e) = self.engine(id) else { continue };
                let s = self.sum(&e, tenant);
                if s != Some(want) {
                    return Err(format!(
                        "repl: n{id} ends with tenant {tenant}'s accounts summing to {s:?}, not {want}"
                    )
                    .into());
                }
                let mut t = e
                    .begin(TxnOptions::default())
                    .map_err(|x| format!("repl: begin on n{id}: {x}"))?;
                for (tn, rid) in lock(&self.acked).iter().filter(|(tn, _)| *tn == tenant) {
                    match t.get_snapshot(&row(*tn, *rid)) {
                        Ok(Some(_)) => {}
                        other => {
                            return Err(format!(
                                "repl: acknowledged row {rid} of tenant {tn} is missing on n{id}: {other:?}"
                            )
                            .into());
                        }
                    }
                }
                t.rollback();
            }
        }
        let fails = self.world.failures();
        if let Some(f) = fails.first() {
            return Err(f.clone().into());
        }
        Ok(*lock(&self.stats))
    }
}

/// One run: the workload under the plan, then the checks.
///
/// # Errors
/// An invariant broke.
pub async fn body(world: World, plan: FaultPlan, cfg: ReplSimConfig) -> SimResult {
    let c = ReplCluster::start(&world, cfg);
    let w = Arc::clone(&c);
    tokio::join!(world.run_plan(&plan), w.run_workload());
    let s = c.check().await?;
    world.record(format!("stats {s:?}"));
    Ok(())
}
