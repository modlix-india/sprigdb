//! The many-node Raft simulator: N nodes in one process, each running the
//! real `sprig-raft` driver on the simulation's paused-time runtime, with
//! its own simulated disk ([`MemLogStore`]: a crash loses unsynced writes
//! and tears the tail), its own clock (rate skew) and its own endpoint on
//! the simulated network (partitions, one-way cuts, delays, drops).
//!
//! [`RaftCluster::start`] supervises the nodes (a killed node restarts from
//! its disk after a crash); [`RaftCluster::run_workload`] runs clients that
//! write unique values to registers and read them back, while membership
//! changes, leader transfers and slow disks happen in the background;
//! [`RaftCluster::check`] heals, lets the cluster settle and checks the
//! invariants:
//!
//! - at most one leader per group and term (checked as roles change);
//! - no two leases of different terms overlap, in true time;
//! - every register's history (reads under leases and through `ReadIndex`,
//!   writes) is linearizable;
//! - every acknowledged write is in the final state, every replica of a
//!   group reaches the same state, and no write applies twice.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::time::Clock;
use sprig_engine::log::Durability;
use sprig_raft::toy::{self, KvMachine};
use sprig_raft::{
    ConfChangeSingle, ConfChangeType, ConfChangeV2, GroupId, MemLogStore, Node as RaftNode, NodeId,
    NodeOptions, Observer, RaftConfig, RaftError, Transport,
};

use super::linearize::{self, Op, RegOp};
use super::{
    FaultKind, FaultPlan, PlannedFault, SimClock, SimError, SimNet, SimResult, SimRng, World,
};
use crate::faults::Node;

/// The simulated cluster's shape and workload.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)] // independent switches
pub struct RaftSimConfig {
    /// Nodes, named `n1`..`nN` with Raft ids 1..N.
    pub nodes: u64,
    /// Each group starts with voters 1..=`voters`.
    pub voters: u64,
    /// Groups.
    pub groups: u64,
    /// Registers per group.
    pub keys: u64,
    /// Client tasks.
    pub clients: usize,
    /// How long clients run.
    pub workload: Duration,
    /// Faults in the plan.
    pub faults: usize,
    /// Membership changes in the background.
    pub membership: bool,
    /// Leader transfers in the background.
    pub transfers: bool,
    /// Now and then, cut a group's current leader off from every other
    /// node for a while.
    pub isolate_leaders: bool,
    /// Disks that are slow at times.
    pub slow_disks: bool,
    /// The largest clock rate error injected, in parts per million.
    pub max_rate_ppm: i64,
    /// The durability class of writes.
    pub durability: Durability,
    /// Driver settings.
    pub raft: RaftConfig,
}

impl Default for RaftSimConfig {
    fn default() -> Self {
        Self {
            nodes: 5,
            voters: 3,
            groups: 3,
            keys: 3,
            clients: 4,
            workload: Duration::from_secs(30),
            faults: 10,
            membership: true,
            transfers: true,
            isolate_leaders: true,
            slow_disks: true,
            max_rate_ppm: 20_000,
            durability: Durability::Strict,
            raft: RaftConfig {
                // Small, so snapshots to lagging replicas happen often.
                compact_after: 40,
                ..RaftConfig::default()
            },
        }
    }
}

/// A node clock that remembers the true time of every reading it gave, so
/// lease times the driver took on it can be checked in true time.
pub struct TrueClock {
    clock: Arc<SimClock>,
    origin: tokio::time::Instant,
    readings: Mutex<Vec<(Instant, Duration)>>,
}

impl std::fmt::Debug for TrueClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrueClock").finish_non_exhaustive()
    }
}

impl TrueClock {
    fn new(clock: Arc<SimClock>, origin: tokio::time::Instant) -> Self {
        Self {
            clock,
            origin,
            readings: Mutex::new(Vec::new()),
        }
    }

    /// The true time (since the run's origin) at which this clock read
    /// `at`, for a reading it gave out.
    #[must_use]
    pub fn true_time(&self, at: Instant) -> Option<Duration> {
        let r = self.readings.lock().unwrap_or_else(PoisonError::into_inner);
        let i = r.partition_point(|(n, _)| *n < at);
        r.get(i).filter(|(n, _)| *n == at).map(|(_, t)| *t)
    }
}

impl Clock for TrueClock {
    fn now(&self) -> Instant {
        let n = self.clock.now();
        let t = tokio::time::Instant::now() - self.origin;
        let mut r = self.readings.lock().unwrap_or_else(PoisonError::into_inner);
        if r.last().is_none_or(|(last, _)| *last < n) {
            r.push((n, t));
        }
        n
    }

    fn wall(&self) -> std::time::SystemTime {
        self.clock.wall()
    }
}

/// A lease that answered a read.
#[derive(Debug, Clone, Copy)]
struct LeaseUse {
    term: u64,
    node: NodeId,
    start: Duration,
    at: Duration,
}

#[derive(Default)]
struct Records {
    /// (group, term) -> the node that led it.
    leaders: BTreeMap<(GroupId, u64), NodeId>,
    leases: BTreeMap<GroupId, Vec<LeaseUse>>,
    /// (group, term) -> when its leader stopped using the lease (true time).
    lease_ends: BTreeMap<(GroupId, u64), Duration>,
    unmapped_leases: u64,
}

struct Checker {
    world: World,
    records: Mutex<Records>,
}

struct NodeObserver {
    id: NodeId,
    checker: Arc<Checker>,
    clock: Arc<TrueClock>,
}

impl Observer for NodeObserver {
    fn on_role(&self, group: GroupId, term: u64, leader: bool) {
        self.checker.world.record(format!(
            "raft g{group} n{} term {term} leader {leader}",
            self.id
        ));
        if !leader {
            return;
        }
        let mut r = self
            .checker
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let prev = *r.leaders.entry((group, term)).or_insert(self.id);
        if prev != self.id {
            self.checker.world.fail(format!(
                "two leaders in group {group} term {term}: n{prev} and n{}",
                self.id
            ));
        }
    }

    fn on_lease_end(&self, group: GroupId, term: u64, at: Instant) {
        let mut r = self
            .checker
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match self.clock.true_time(at) {
            Some(t) => {
                let e = r.lease_ends.entry((group, term)).or_insert(t);
                *e = (*e).min(t);
            }
            None => r.unmapped_leases += 1,
        }
    }

    fn on_lease_read(&self, group: GroupId, term: u64, start: Instant, now: Instant) {
        let mut r = self
            .checker
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match (self.clock.true_time(start), self.clock.true_time(now)) {
            (Some(start), Some(at)) => r.leases.entry(group).or_default().push(LeaseUse {
                term,
                node: self.id,
                start,
                at,
            }),
            _ => r.unmapped_leases += 1,
        }
    }
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

/// The simulation's name of Raft node `id`.
#[must_use]
pub fn name(id: NodeId) -> Node {
    Node::new(format!("n{id}"))
}

/// Closes the node when its process stops (a crash drops the future).
struct CloseOnDrop(RaftNode);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

struct Member {
    id: NodeId,
    disk: Arc<MemLogStore>,
    clock: Arc<TrueClock>,
    current: Mutex<Option<RaftNode>>,
    slow_until: Mutex<Duration>,
}

/// One register operation as the clients saw it.
#[derive(Default)]
struct History {
    ops: BTreeMap<(GroupId, u64), Vec<Op>>,
    acked: Vec<(GroupId, u64, u64)>,
    unknown: u64,
    failed: u64,
}

/// What a run did, for reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunStats {
    /// Writes acknowledged.
    pub acked_writes: u64,
    /// Writes whose outcome stayed unknown.
    pub unknown_writes: u64,
    /// Writes refused or lost (never committed).
    pub failed_writes: u64,
    /// Reads answered.
    pub reads: u64,
    /// Reads answered under a lease.
    pub lease_reads: u64,
    /// Elections won (distinct group terms with a leader).
    pub leaders: u64,
    /// Membership changes applied.
    pub membership_changes: u64,
}

/// A simulated cluster.
pub struct RaftCluster {
    world: World,
    cfg: RaftSimConfig,
    origin: tokio::time::Instant,
    members: Vec<Arc<Member>>,
    checker: Arc<Checker>,
    history: Mutex<History>,
    changes: Mutex<u64>,
}

impl std::fmt::Debug for RaftCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftCluster")
            .field("nodes", &self.cfg.nodes)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl RaftCluster {
    /// Starts every node under the world's supervisor; the first
    /// `cfg.voters` nodes create every group.
    #[must_use]
    pub fn start(world: &World, cfg: RaftSimConfig) -> Arc<Self> {
        let origin = tokio::time::Instant::now();
        let checker = Arc::new(Checker {
            world: world.clone(),
            records: Mutex::new(Records::default()),
        });
        let members: Vec<Arc<Member>> = (1..=cfg.nodes)
            .map(|id| {
                Arc::new(Member {
                    id,
                    disk: MemLogStore::new(),
                    clock: Arc::new(TrueClock::new(world.clock(&name(id)), origin)),
                    current: Mutex::new(None),
                    slow_until: Mutex::new(Duration::ZERO),
                })
            })
            .collect();
        let cluster = Arc::new(Self {
            world: world.clone(),
            cfg,
            origin,
            members,
            checker,
            history: Mutex::new(History::default()),
            changes: Mutex::new(0),
        });
        for m in &cluster.members {
            cluster.spawn_member(m);
        }
        cluster
    }

    fn spawn_member(&self, m: &Arc<Member>) {
        let m = Arc::clone(m);
        let cfg = self.cfg.clone();
        let checker = Arc::clone(&self.checker);
        let groups: Vec<GroupId> = (1..=self.cfg.groups).collect();
        let voters: Vec<NodeId> = (1..=self.cfg.voters).collect();
        let origin = self.origin;
        if self.cfg.slow_disks {
            let rng = self.world.rng().fork();
            let mm = Arc::clone(&m);
            m.disk.set_sync_delay(move || {
                let now = tokio::time::Instant::now() - origin;
                if now < *lock(&mm.slow_until) {
                    rng.duration(Duration::from_millis(20), Duration::from_millis(300))
                } else {
                    rng.duration(Duration::ZERO, Duration::from_millis(2))
                }
            });
        }
        self.world.spawn_node(&name(m.id), move |ctx| {
            let m = Arc::clone(&m);
            let cfg = cfg.clone();
            let checker = Arc::clone(&checker);
            let groups = groups.clone();
            let voters = voters.clone();
            async move {
                if ctx.incarnation > 0 {
                    let loss = m
                        .disk
                        .crash(ctx.world.seed() ^ (m.id << 32) ^ ctx.incarnation);
                    ctx.world.record(format!(
                        "raft n{} disk crash: {} unsynced batches, {} kept",
                        m.id, loss.unsynced, loss.kept
                    ));
                }
                let observer: Arc<dyn Observer> = Arc::new(NodeObserver {
                    id: m.id,
                    checker,
                    clock: Arc::clone(&m.clock),
                });
                let node = match RaftNode::open(NodeOptions {
                    id: m.id,
                    config: cfg.raft.clone(),
                    store: Arc::clone(&m.disk) as Arc<dyn sprig_raft::LogStore>,
                    transport: Arc::new(SimTransport {
                        net: ctx.world.net().clone(),
                        from: ctx.node.clone(),
                    }),
                    clock: Arc::clone(&m.clock) as Arc<dyn Clock>,
                    machines: Arc::new(KvMachine::factory()),
                    observer: Some(observer),
                    seed: ctx.world.seed() ^ ctx.incarnation.wrapping_mul(0x9e37),
                }) {
                    Ok(n) => n,
                    Err(e) => {
                        ctx.world.fail(format!("n{} failed to open: {e}", m.id));
                        return;
                    }
                };
                if ctx.incarnation == 0 && voters.contains(&m.id) {
                    for g in &groups {
                        if let Err(e) = node.create_group(*g, &voters) {
                            ctx.world.fail(format!("n{} create group {g}: {e}", m.id));
                        }
                    }
                }
                *lock(&m.current) = Some(node.clone());
                let _close = CloseOnDrop(node.clone());
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

    /// Node `id`'s current incarnation, unless it is down.
    #[must_use]
    pub fn node(&self, id: NodeId) -> Option<RaftNode> {
        let m = self
            .members
            .get(usize::try_from(id).ok()?.checked_sub(1)?)?;
        let n = lock(&m.current).clone()?;
        (!n.is_closed()).then_some(n)
    }

    fn now(&self) -> u64 {
        u64::try_from((tokio::time::Instant::now() - self.origin).as_nanos()).unwrap_or(u64::MAX)
    }

    /// The node that leads `group` with the highest term, if any.
    fn leader_of(&self, group: GroupId) -> Option<(NodeId, RaftNode)> {
        let mut best: Option<(u64, NodeId, RaftNode)> = None;
        for id in 1..=self.cfg.nodes {
            let Some(n) = self.node(id) else { continue };
            if self.world.net().state(&name(id)) != super::NodeState::Running {
                continue;
            }
            if let Some(s) = n.status(group)
                && s.is_leader
                && best.as_ref().is_none_or(|(t, _, _)| s.term > *t)
            {
                best = Some((s.term, id, n));
            }
        }
        best.map(|(_, id, n)| (id, n))
    }

    /// A fault plan for this cluster: partitions isolating one node or
    /// splitting the nodes, one-way cuts, slow lossy links, kills (each
    /// restarts after 0.1 to 2 s), pauses and clock rate skew.
    #[must_use]
    pub fn plan(rng: &SimRng, cfg: &RaftSimConfig) -> FaultPlan {
        let nodes: Vec<Node> = (1..=cfg.nodes).map(name).collect();
        let horizon = u64::try_from(cfg.workload.as_millis())
            .unwrap_or(u64::MAX)
            .saturating_sub(1000)
            .max(1);
        let mut faults = Vec::with_capacity(cfg.faults);
        for _ in 0..cfg.faults {
            let at_ms = rng.below(horizon);
            let for_ms = rng.range(300..6000);
            let node = rng.pick(&nodes).clone();
            let others: Vec<Node> = nodes.iter().filter(|n| **n != node).cloned().collect();
            let fault = match rng.below(9) {
                0 => FaultKind::Partition {
                    a: vec![node],
                    b: others,
                    for_ms,
                },
                1 => {
                    let mut all = nodes.clone();
                    rng.shuffle(&mut all);
                    let b = all.split_off(1 + rng.index(all.len() - 1));
                    FaultKind::Partition { a: all, b, for_ms }
                }
                2 => FaultKind::OneWay {
                    from: vec![node],
                    to: others,
                    for_ms,
                },
                3 => FaultKind::OneWay {
                    from: others,
                    to: vec![node],
                    for_ms,
                },
                4 => FaultKind::Link {
                    a: node,
                    b: rng.pick(&others).clone(),
                    latency_ms: rng.range(5..300),
                    jitter_ms: rng.range(0..100),
                    loss: if rng.chance(0.5) { 0.3 } else { 0.0 },
                    for_ms,
                },
                5 | 6 => FaultKind::Kill { node },
                7 => FaultKind::Pause {
                    node,
                    for_ms: rng.range(200..4000),
                },
                _ => {
                    let span = u64::try_from(cfg.max_rate_ppm.max(1)).unwrap_or(1) * 2;
                    FaultKind::Skew {
                        node,
                        offset_ms: 0,
                        rate_ppm: i64::try_from(rng.below(span + 1)).unwrap_or(0)
                            - cfg.max_rate_ppm,
                        for_ms,
                    }
                }
            };
            faults.push(PlannedFault { at_ms, fault });
        }
        faults.sort_by_key(|f| f.at_ms);
        FaultPlan { faults }
    }

    /// Runs the clients and the background changes for `cfg.workload`.
    pub async fn run_workload(self: &Arc<Self>) {
        let deadline = tokio::time::Instant::now() + self.cfg.workload;
        let mut tasks = Vec::new();
        for c in 0..self.cfg.clients {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn(&format!("client {c}"), async move {
                me.client(c as u64, deadline).await;
            }));
        }
        if self.cfg.membership {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn("membership", async move {
                me.membership(deadline).await;
            }));
        }
        if self.cfg.transfers {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn("transfers", async move {
                me.transfers(deadline).await;
            }));
        }
        if self.cfg.isolate_leaders {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn("leader isolation", async move {
                me.isolate_leaders(deadline).await;
            }));
        }
        if self.cfg.slow_disks {
            let me = Arc::clone(self);
            tasks.push(self.world.spawn("slow disks", async move {
                me.slow_disks(deadline).await;
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
    }

    #[allow(clippy::many_single_char_names)]
    async fn client(&self, cid: u64, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        let mut guess: BTreeMap<GroupId, NodeId> = BTreeMap::new();
        let mut next_value = (cid + 1) << 40;
        let timeout = Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let g = 1 + rng.below(self.cfg.groups);
            let key = rng.below(self.cfg.keys);
            let target = guess
                .get(&g)
                .copied()
                .unwrap_or_else(|| 1 + rng.below(self.cfg.nodes));
            let Some(node) = self.node(target) else {
                guess.remove(&g);
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            // A frozen server doesn't answer until it thaws.
            self.world.net().wait_running(&name(target)).await;
            let call = self.now();
            if rng.chance(0.5) {
                next_value += 1;
                let v = next_value;
                match node.propose(g, toy::put(key, v), self.cfg.durability) {
                    Err(RaftError::NotLeader { leader }) => {
                        match leader {
                            Some(l) => guess.insert(g, l),
                            None => guess.remove(&g),
                        };
                        lock(&self.history).failed += 1;
                    }
                    Err(_) => {
                        guess.remove(&g);
                        lock(&self.history).failed += 1;
                    }
                    Ok(slot) => {
                        let r = tokio::time::timeout(timeout, slot.wait_async()).await;
                        let mut h = lock(&self.history);
                        match r {
                            Ok(Ok(_)) => {
                                let ret = self.now();
                                h.ops.entry((g, key)).or_default().push(Op {
                                    call,
                                    ret: Some(ret),
                                    op: RegOp::Write(v),
                                });
                                h.acked.push((g, key, v));
                            }
                            // Refused or replaced: never committed.
                            Ok(Err(e)) if e.is_retryable() => {
                                h.failed += 1;
                                drop(h);
                                guess.remove(&g);
                            }
                            _ => {
                                h.ops.entry((g, key)).or_default().push(Op {
                                    call,
                                    ret: None,
                                    op: RegOp::Write(v),
                                });
                                h.unknown += 1;
                                drop(h);
                                guess.remove(&g);
                            }
                        }
                    }
                }
            } else {
                match node.read(g, toy::get(key)) {
                    Ok(slot) => {
                        if let Ok(Ok(answer)) =
                            tokio::time::timeout(timeout, slot.wait_async()).await
                        {
                            let ret = self.now();
                            lock(&self.history)
                                .ops
                                .entry((g, key))
                                .or_default()
                                .push(Op {
                                    call,
                                    ret: Some(ret),
                                    op: RegOp::Read(toy::value(&answer)),
                                });
                        } else {
                            guess.remove(&g);
                        }
                    }
                    Err(RaftError::NotLeader { leader: Some(l) }) => {
                        guess.insert(g, l);
                    }
                    Err(_) => {
                        guess.remove(&g);
                    }
                }
            }
            tokio::time::sleep(rng.duration(Duration::ZERO, Duration::from_millis(40))).await;
        }
    }

    async fn membership(&self, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_secs(2), Duration::from_secs(5))).await;
            let g = 1 + rng.below(self.cfg.groups);
            let Some((lid, leader)) = self.leader_of(g) else {
                continue;
            };
            let Some(st) = leader.status(g) else { continue };
            if !st.voters_outgoing.is_empty() {
                continue;
            }
            let mut cc = ConfChangeV2::default();
            let single = |t: ConfChangeType, id: NodeId| {
                let mut s = ConfChangeSingle::default();
                s.set_change_type(t);
                s.node_id = id;
                s
            };
            if let Some(l) = st.learners.first().copied() {
                // Promote the learner and retire a voter (perhaps the
                // leader itself): a joint configuration.
                let out = *rng.pick(&st.voters);
                cc.mut_changes().push(single(ConfChangeType::AddNode, l));
                cc.mut_changes()
                    .push(single(ConfChangeType::RemoveNode, out));
                self.world.record(format!(
                    "raft g{g} change: promote n{l}, remove n{out} (leader n{lid})"
                ));
            } else {
                let free: Vec<NodeId> = (1..=self.cfg.nodes)
                    .filter(|n| !st.voters.contains(n))
                    .collect();
                if free.is_empty() {
                    continue;
                }
                let add = *rng.pick(&free);
                cc.mut_changes()
                    .push(single(ConfChangeType::AddLearnerNode, add));
                self.world.record(format!(
                    "raft g{g} change: add learner n{add} (leader n{lid})"
                ));
            }
            if let Ok(slot) = leader.change_membership(g, cc)
                && let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_secs(3), slot.wait_async()).await
            {
                *lock(&self.changes) += 1;
            }
        }
    }

    async fn transfers(&self, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_secs(2), Duration::from_secs(6))).await;
            let g = 1 + rng.below(self.cfg.groups);
            let Some((lid, leader)) = self.leader_of(g) else {
                continue;
            };
            let Some(st) = leader.status(g) else { continue };
            let others: Vec<NodeId> = st.voters.iter().copied().filter(|v| *v != lid).collect();
            if others.is_empty() {
                continue;
            }
            let to = *rng.pick(&others);
            self.world
                .record(format!("raft g{g} transfer n{lid} -> n{to}"));
            let _ = leader.transfer_leader(g, to);
        }
    }

    async fn isolate_leaders(&self, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_secs(3), Duration::from_secs(8))).await;
            let g = 1 + rng.below(self.cfg.groups);
            let Some((lid, _)) = self.leader_of(g) else {
                continue;
            };
            let others: Vec<Node> = (1..=self.cfg.nodes)
                .filter(|n| *n != lid)
                .map(name)
                .collect();
            let net = self.world.net();
            let id = net.partition(&[name(lid)], &others);
            self.world
                .record(format!("raft g{g} leader n{lid} isolated"));
            tokio::time::sleep(rng.duration(Duration::from_millis(500), Duration::from_secs(4)))
                .await;
            net.heal(id);
        }
    }

    async fn slow_disks(&self, deadline: tokio::time::Instant) {
        let rng = self.world.rng().fork();
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(rng.duration(Duration::from_secs(1), Duration::from_secs(3))).await;
            let m = rng.pick(&self.members);
            let until = (tokio::time::Instant::now() - self.origin)
                + rng.duration(Duration::from_millis(500), Duration::from_secs(4));
            *lock(&m.slow_until) = until;
            self.world.record(format!("raft n{} slow disk", m.id));
        }
    }

    /// Heals every fault, lets the cluster settle, reads every register
    /// once more, and checks the invariants.
    ///
    /// # Errors
    /// An invariant failed.
    pub async fn check(self: &Arc<Self>) -> Result<RunStats, SimError> {
        self.world.heal_all().await;
        for m in &self.members {
            *lock(&m.slow_until) = Duration::ZERO;
        }
        for id in 1..=self.cfg.nodes {
            self.world.clock(&name(id)).reset_skew();
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
        // Final reads through each group's leader, in the history too.
        for g in 1..=self.cfg.groups {
            for key in 0..self.cfg.keys {
                self.final_read(g, key).await?;
            }
        }
        for g in 1..=self.cfg.groups {
            self.converge(g).await?;
        }
        self.check_state()?;
        self.check_linearizable()?;
        self.check_leases()?;
        let h = lock(&self.history);
        let r = lock(&self.checker.records);
        let reads = h
            .ops
            .values()
            .flatten()
            .filter(|o| matches!(o.op, RegOp::Read(_)))
            .count() as u64;
        Ok(RunStats {
            acked_writes: h.acked.len() as u64,
            unknown_writes: h.unknown,
            failed_writes: h.failed,
            reads,
            lease_reads: r.leases.values().map(|v| v.len() as u64).sum(),
            leaders: r.leaders.len() as u64,
            membership_changes: *lock(&self.changes),
        })
    }

    async fn final_read(&self, g: GroupId, key: u64) -> SimResult {
        for _ in 0..200 {
            if let Some((_, leader)) = self.leader_of(g) {
                let call = self.now();
                if let Ok(slot) = leader.read(g, toy::get(key))
                    && let Ok(Ok(a)) =
                        tokio::time::timeout(Duration::from_secs(3), slot.wait_async()).await
                {
                    let ret = self.now();
                    lock(&self.history)
                        .ops
                        .entry((g, key))
                        .or_default()
                        .push(Op {
                            call,
                            ret: Some(ret),
                            op: RegOp::Read(toy::value(&a)),
                        });
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(format!("group {g} has no leader that answers reads after healing").into())
    }

    /// Waits until every replica in the leader's configuration holds the
    /// leader's state at its commit index.
    async fn converge(&self, g: GroupId) -> SimResult {
        let mut last = String::new();
        for _ in 0..300 {
            if let Some((lid, leader)) = self.leader_of(g)
                && let Some(st) = leader.status(g)
            {
                let members: BTreeSet<NodeId> =
                    st.voters.iter().chain(&st.learners).copied().collect();
                // (applied index, registers): the dump's own header counts
                // data entries only and differs after a snapshot.
                let state = |n: &RaftNode| {
                    n.query_local(g, &toy::dump())
                        .and_then(|(a, d)| KvMachine::decode(&d).map(|m| (a, m.data().clone())))
                };
                let want = state(&leader);
                let mut ok = want.as_ref().is_some_and(|(a, _)| *a == st.committed);
                for id in &members {
                    let got = self.node(*id).and_then(|n| state(&n));
                    if got != want {
                        ok = false;
                        last = format!(
                            "n{id} at {:?}, leader n{lid} at {:?}",
                            got.as_ref().map(|g| g.0),
                            want.as_ref().map(|w| w.0)
                        );
                    }
                }
                if ok {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!("group {g}'s replicas did not converge: {last}").into())
    }

    fn final_state(&self, g: GroupId) -> Option<KvMachine> {
        let (_, leader) = self.leader_of(g)?;
        let (_, d) = leader.query_local(g, &toy::dump())?;
        KvMachine::decode(&d)
    }

    fn check_state(&self) -> SimResult {
        let h = lock(&self.history);
        let mut states = BTreeMap::new();
        for g in 1..=self.cfg.groups {
            let s = self
                .final_state(g)
                .ok_or_else(|| format!("group {g} has no state to check"))?;
            for (k, vs) in s.data() {
                let distinct: BTreeSet<&u64> = vs.iter().collect();
                if distinct.len() != vs.len() {
                    return Err(format!("group {g} key {k}: a write applied twice: {vs:?}").into());
                }
            }
            states.insert(g, s);
        }
        for (g, k, v) in &h.acked {
            let present = states[g].data().get(k).is_some_and(|vs| vs.contains(v));
            if !present {
                return Err(format!("acknowledged write {v} to group {g} key {k} was lost").into());
            }
        }
        Ok(())
    }

    fn check_linearizable(&self) -> SimResult {
        let h = lock(&self.history);
        for ((g, k), ops) in &h.ops {
            linearize::check_register(ops, linearize::MAX_STEPS)
                .map_err(|e| format!("group {g} key {k}: {e}"))?;
        }
        Ok(())
    }

    fn check_leases(&self) -> SimResult {
        let r = lock(&self.checker.records);
        if r.unmapped_leases > 0 {
            return Err(format!("{} lease reads had no true time", r.unmapped_leases).into());
        }
        // A lease is believed until its start plus the usable lease on a
        // clock running up to `max_rate_ppm` slow.
        let usable = self.cfg.raft.usable_lease();
        let ppm = u32::try_from(self.cfg.max_rate_ppm.clamp(0, 500_000)).unwrap_or(0);
        let slow = 1.0 - f64::from(ppm) / 1e6;
        let believed = Duration::from_secs_f64(usable.as_secs_f64() / slow);
        for (g, uses) in &r.leases {
            // Per term: the earliest start and the latest believed end.
            let mut terms: BTreeMap<u64, (Duration, Duration, NodeId)> = BTreeMap::new();
            for u in uses {
                let e = terms
                    .entry(u.term)
                    .or_insert((u.start, u.start + believed, u.node));
                e.0 = e.0.min(u.start);
                e.1 = e.1.max(u.start + believed);
                if u.at > u.start + believed {
                    return Err(format!(
                        "group {g}: n{} used a lease of term {} at {:?}, past its end {:?}",
                        u.node,
                        u.term,
                        u.at,
                        u.start + believed
                    )
                    .into());
                }
                if e.2 != u.node {
                    return Err(
                        format!("group {g}: two nodes held leases in term {}", u.term).into(),
                    );
                }
            }
            // A lease ends early when its leader starts a transfer or
            // steps down; no read may use it after that.
            for (t, e) in &mut terms {
                if let Some(end) = r.lease_ends.get(&(*g, *t)) {
                    if let Some(u) = uses.iter().find(|u| u.term == *t && u.at > *end) {
                        return Err(format!(
                            "group {g}: n{} used its term {t} lease at {:?}, after it ended at {end:?}",
                            u.node, u.at
                        )
                        .into());
                    }
                    e.1 = e.1.min(*end);
                }
            }
            let terms: Vec<(u64, (Duration, Duration, NodeId))> = terms.into_iter().collect();
            for w in terms.windows(2) {
                let (t1, (_, end1, n1)) = w[0];
                let (t2, (start2, _, n2)) = w[1];
                if end1 > start2 {
                    return Err(format!(
                        "group {g}: leases overlap: n{n1} term {t1} believed until {end1:?}, \
                         n{n2} term {t2} from {start2:?}"
                    )
                    .into());
                }
            }
        }
        Ok(())
    }
}
