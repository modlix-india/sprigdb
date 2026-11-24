//! An engine whose tenant units commit through Raft: one group per unit,
//! replicated on this node's Raft node.
//!
//! [`EngineNode::open`] builds the Raft node over an engine opened with
//! `EngineConfig::replicated` and attaches it as the engine's replicated
//! log. Every replica applies every committed entry to its own store
//! through the engine's state machine (with the engine's apply observers,
//! so stream segments are written on every replica too).
//!
//! **Leadership.** A unit is led here, for the engine, from the moment the
//! replica that won an election has applied an entry of its own term (so
//! everything its predecessors committed is in the store) until it stops
//! leading. Then the engine is told [`Engine::lead_at`] with the Raft term
//! as the leader epoch, or [`Engine::step_down`]: caches of the unit's
//! state rebuild under the new epoch, and leader-only state (locks, queue
//! owners, id blocks, sweepers) lives only where the unit is led.
//!
//! The Raft node holds the engine weakly: the engine holds the node (as its
//! replicated log), and dropping the engine closes it.

use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::time::Clock;
use sprig_engine::{Engine, UnitId, WeakEngine};

use crate::config::RaftConfig;
use crate::consensus::{RaftConsensus, UnitMachine, UnitSnapshots};
use crate::engine_snap::EngineSnapshots;
use crate::error::RaftError;
use crate::logstore::LogStore;
use crate::node::{Node, NodeOptions, Observer};
use crate::transport::Transport;
use crate::{GroupId, NodeId};

/// What an [`EngineNode`] is built from.
pub struct EngineNodeOptions {
    /// This node.
    pub id: NodeId,
    /// Raft settings. With `static_voters` set, every tenant unit has
    /// those voters and starts on first use.
    pub config: RaftConfig,
    /// The node's Raft log.
    pub store: Arc<dyn LogStore>,
    /// Frames to other nodes.
    pub transport: Arc<dyn Transport>,
    /// Monotonic time.
    pub clock: Arc<dyn Clock>,
    /// Seeds election timeouts.
    pub seed: u64,
    /// Further events (test harnesses), after the node's own handling.
    pub observer: Option<Arc<dyn Observer>>,
    /// Tell the engine about leadership changes inside the driver's turn
    /// (the simulator, which must stay deterministic) instead of on a
    /// thread of their own. Reads on a replica that doesn't lead their unit
    /// then don't wait for a read barrier either (the simulation runs the
    /// driver on the reader's thread): they read what the replica holds.
    pub inline_roles: bool,
}

/// Unit snapshots of an engine held weakly.
struct WeakSnapshots(WeakEngine);

impl WeakSnapshots {
    fn engine(&self) -> Result<EngineSnapshots, RaftError> {
        self.0
            .upgrade()
            .map(EngineSnapshots::new)
            .ok_or(RaftError::Closed)
    }
}

impl UnitSnapshots for WeakSnapshots {
    fn export(&self, unit: UnitId) -> Result<Vec<u8>, RaftError> {
        self.engine()?.export(unit)
    }

    fn ingest(&self, unit: UnitId, index: u64, data: &[u8]) -> Result<(), RaftError> {
        self.engine()?.ingest(unit, index, data)
    }
}

/// A leadership change for the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Lead(UnitId, u64),
    StepDown(UnitId),
}

fn tell(engine: &WeakEngine, r: Role) {
    let Some(e) = engine.upgrade() else {
        return;
    };
    match r {
        Role::Lead(u, term) => {
            tracing::debug!(unit = u.0, term, "raft: leading the unit here");
            e.lead_at(u, term);
        }
        Role::StepDown(u) => {
            tracing::debug!(unit = u.0, "raft: no longer leading the unit here");
            e.step_down(u);
        }
    }
}

/// Watches roles and applies, and tells the engine when a unit's
/// leadership starts and stops here.
struct Roles {
    engine: WeakEngine,
    next: Option<Arc<dyn Observer>>,
    /// Groups led here: the term, and whether the engine was told.
    leading: Mutex<HashMap<GroupId, (u64, bool)>>,
    /// `None`: tell the engine inline.
    tx: Option<Mutex<mpsc::Sender<Role>>>,
}

impl Roles {
    fn send(&self, r: Role) {
        match &self.tx {
            None => tell(&self.engine, r),
            Some(tx) => {
                let _ = tx.lock().unwrap_or_else(PoisonError::into_inner).send(r);
            }
        }
    }
}

impl Observer for Roles {
    fn on_role(&self, group: GroupId, term: u64, leader: bool) {
        let told = {
            let mut l = self.leading.lock().unwrap_or_else(PoisonError::into_inner);
            let old = l.remove(&group);
            if leader {
                l.insert(group, (term, false));
            }
            old.is_some_and(|(_, told)| told)
        };
        if told {
            self.send(Role::StepDown(UnitId(group)));
        }
        if let Some(n) = &self.next {
            n.on_role(group, term, leader);
        }
    }

    fn on_lease_read(&self, group: GroupId, term: u64, start: Instant, now: Instant) {
        if let Some(n) = &self.next {
            n.on_lease_read(group, term, start, now);
        }
    }

    fn on_lease_end(&self, group: GroupId, term: u64, at: Instant) {
        if let Some(n) = &self.next {
            n.on_lease_end(group, term, at);
        }
    }

    fn on_apply(&self, group: GroupId, index: u64, term: u64) {
        let lead = {
            let mut l = self.leading.lock().unwrap_or_else(PoisonError::into_inner);
            match l.get_mut(&group) {
                Some((t, told)) if !*told && *t == term => {
                    *told = true;
                    Some(*t)
                }
                _ => None,
            }
        };
        if let Some(t) = lead {
            self.send(Role::Lead(UnitId(group), t));
        }
        if let Some(n) = &self.next {
            n.on_apply(group, index, term);
        }
    }
}

/// An engine's replicated units on a Raft node.
pub struct EngineNode {
    node: Node,
    consensus: Arc<RaftConsensus>,
}

impl std::fmt::Debug for EngineNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineNode")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl EngineNode {
    /// Opens the Raft node over `engine` (opened with
    /// `EngineConfig::replicated`) and attaches it as the engine's
    /// replicated log. Drive it with [`EngineNode::spawn`] (or run the
    /// node's loop yourself, in simulation).
    ///
    /// # Errors
    /// The log store could not be recovered, the engine isn't replicated,
    /// or it already has a replicated log.
    pub fn open(engine: &Engine, opts: EngineNodeOptions) -> Result<EngineNode, RaftError> {
        if !engine.is_replicated() {
            return Err(RaftError::Invalid(
                "the engine was opened without replication".into(),
            ));
        }
        let weak = engine.downgrade();
        let tx = if opts.inline_roles {
            None
        } else {
            let (tx, rx) = mpsc::channel::<Role>();
            let w = weak.clone();
            std::thread::Builder::new()
                .name(format!("sprig-raft-roles-{}", opts.id))
                .spawn(move || {
                    while let Ok(r) = rx.recv() {
                        tell(&w, r);
                    }
                })
                .map_err(|e| RaftError::Storage(format!("spawn the roles thread: {e}")))?;
            Some(Mutex::new(tx))
        };
        let roles = Arc::new(Roles {
            engine: weak.clone(),
            next: opts.observer,
            leading: Mutex::new(HashMap::new()),
            tx,
        });
        let sm = engine.state_machine();
        let snaps: Arc<dyn UnitSnapshots> = Arc::new(WeakSnapshots(weak));
        let voters = opts.config.static_voters.clone();
        let node = Node::open(NodeOptions {
            id: opts.id,
            config: opts.config,
            store: opts.store,
            transport: opts.transport,
            clock: opts.clock,
            machines: Arc::new(move |g: GroupId| {
                Box::new(UnitMachine::new(
                    UnitId(g),
                    Arc::clone(&sm),
                    Arc::clone(&snaps),
                )) as Box<dyn crate::sm::StateMachine>
            }),
            observer: Some(roles as Arc<dyn Observer>),
            seed: opts.seed,
        })?;
        let mut consensus = RaftConsensus::with_voters(node.clone(), voters);
        consensus.set_read_barriers(!opts.inline_roles);
        let consensus = Arc::new(consensus);
        engine
            .attach_replication(Arc::clone(&consensus) as Arc<dyn sprig_engine::ReplicatedLog>)
            .map_err(|e| RaftError::Invalid(e.message))?;
        Ok(EngineNode { node, consensus })
    }

    /// The Raft node (register it with the transport).
    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// The engine's replicated log.
    #[must_use]
    pub fn consensus(&self) -> &Arc<RaftConsensus> {
        &self.consensus
    }

    /// Runs the driver on a thread of its own until the node closes.
    ///
    /// # Errors
    /// The thread could not be started.
    pub fn spawn(&self) -> std::io::Result<std::thread::JoinHandle<()>> {
        self.node.spawn()
    }

    /// Starts `unit`'s group here with the fixed voters, if this node
    /// doesn't hold it yet, and campaigns for it.
    ///
    /// # Errors
    /// The node has no fixed voters, or it is closed.
    pub fn start_unit(&self, unit: UnitId, voters: &[NodeId]) -> Result<bool, RaftError> {
        self.consensus.start_unit(unit, voters)
    }

    /// Waits up to `timeout` until some replica of `unit` known here leads
    /// it with its term's entry applied; returns that leader's node id as
    /// this replica knows it.
    #[must_use]
    pub fn wait_leader(&self, unit: UnitId, timeout: Duration) -> Option<NodeId> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(s) = self.node.status(unit.0)
                && let Some(l) = s.leader
                && (!s.is_leader || s.applied >= s.committed)
            {
                return Some(l);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Replaces `dead`, a lost replica of `unit`, with `new` (a node with
    /// an empty log for the unit): run on the unit's leader. `new` joins as
    /// a learner and gets the unit from a snapshot of the leader's store
    /// (then the log after it); once it holds everything committed, one
    /// joint change makes it a voter and removes `dead`. Waits up to
    /// `timeout` in all. Automatic replacement is later work; this is the
    /// manual step an operator (or a test) runs.
    ///
    /// # Errors
    /// This node doesn't lead the unit, another change is pending, or the
    /// new replica didn't catch up in time.
    pub fn replace_replica(
        &self,
        unit: UnitId,
        dead: NodeId,
        new: NodeId,
        timeout: Duration,
    ) -> Result<(), RaftError> {
        use crate::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
        let deadline = Instant::now() + timeout;
        let single = |t: ConfChangeType, id: NodeId| {
            let mut c = ConfChangeSingle::default();
            c.set_change_type(t);
            c.node_id = id;
            c
        };
        let left = |what: &str| -> Result<Duration, RaftError> {
            deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(|| RaftError::Invalid(format!("replacing a replica: timed out {what}")))
        };
        let g = unit.0;
        let st = self.node.status(g).ok_or(RaftError::NoGroup(g))?;
        if !st.learners.contains(&new) && !st.voters.contains(&new) {
            let mut cc = ConfChangeV2::default();
            cc.mut_changes()
                .push(single(ConfChangeType::AddLearnerNode, new));
            let slot = self.node.change_membership(g, cc)?;
            slot.wait_timeout(left("adding the learner")?)
                .ok_or_else(|| RaftError::Invalid("replacing a replica: no answer".into()))??;
        }
        // The learner catches up from a snapshot and the log.
        loop {
            let st = self.node.status(g).ok_or(RaftError::NoGroup(g))?;
            if self.node.matched(g, new).is_some_and(|m| m >= st.committed) {
                break;
            }
            std::thread::sleep(left("waiting for the learner")?.min(Duration::from_millis(10)));
        }
        let st = self.node.status(g).ok_or(RaftError::NoGroup(g))?;
        if st.voters.contains(&dead) || !st.voters.contains(&new) {
            let mut cc = ConfChangeV2::default();
            cc.mut_changes().push(single(ConfChangeType::AddNode, new));
            if st.voters.contains(&dead) {
                cc.mut_changes()
                    .push(single(ConfChangeType::RemoveNode, dead));
            }
            let slot = self.node.change_membership(g, cc)?;
            slot.wait_timeout(left("promoting the learner")?)
                .ok_or_else(|| RaftError::Invalid("replacing a replica: no answer".into()))??;
        }
        // The joint configuration is left on its own; wait for it.
        loop {
            let st = self.node.status(g).ok_or(RaftError::NoGroup(g))?;
            if st.voters_outgoing.is_empty()
                && st.voters.contains(&new)
                && !st.voters.contains(&dead)
            {
                return Ok(());
            }
            std::thread::sleep(
                left("leaving the joint configuration")?.min(Duration::from_millis(10)),
            );
        }
    }

    /// Stops the node: pending proposals fail with an unknown outcome.
    pub fn close(&self) {
        self.node.close();
    }

    /// Moves units to their preferred leaders: every `every`, each unit
    /// held here whose `preferred` leader is this node, and that another
    /// replica leads while this one is caught up, asks for its leadership
    /// (a transfer through the current leader, at most once per
    /// `3 * every` per unit). Nothing is asked during the first `settle`
    /// (a restarted node catches up first). Units `preferred` names no
    /// node for are left wherever their elections put them. Runs on a
    /// thread of its own until the node closes.
    ///
    /// # Errors
    /// The thread could not be started.
    pub fn prefer_leaders(
        &self,
        preferred: Arc<dyn Fn(UnitId) -> Option<NodeId> + Send + Sync>,
        every: Duration,
        settle: Duration,
    ) -> std::io::Result<std::thread::JoinHandle<()>> {
        let node = self.node.clone();
        std::thread::Builder::new()
            .name(format!("sprig-raft-prefer-{}", node.id()))
            .spawn(move || {
                let started = Instant::now();
                let mut asked: HashMap<GroupId, Instant> = HashMap::new();
                while !node.is_closed() {
                    std::thread::sleep(every);
                    if started.elapsed() < settle {
                        continue;
                    }
                    let me = node.id();
                    for g in node.groups() {
                        if preferred(UnitId(g)) != Some(me) {
                            continue;
                        }
                        let Some(st) = node.status(g) else { continue };
                        let caught_up = st.applied >= st.committed;
                        if st.is_leader
                            || st.leader.is_none_or(|l| l == me)
                            || !caught_up
                            || !st.voters.contains(&me)
                            || asked.get(&g).is_some_and(|t| t.elapsed() < every * 3)
                        {
                            continue;
                        }
                        asked.insert(g, Instant::now());
                        if let Err(e) = node.request_leadership(g) {
                            tracing::debug!(group = g, error = %e, "raft: asking for a preferred unit's leadership failed");
                        } else {
                            tracing::debug!(group = g, from = ?st.leader, "raft: asking for a preferred unit's leadership");
                        }
                    }
                }
            })
    }
}
