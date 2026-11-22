//! A node: every group it hosts, and the loop that drives them.
//!
//! # One turn of the loop
//!
//! 1. Step every frame that arrived (each frame is one peer node's messages
//!    for all groups), then tick the groups that aren't quiesced and check
//!    store liveness.
//! 2. For every group with work, take raft-rs's `Ready`: new entries, hard
//!    state and snapshots go into one [`WriteBatch`]; messages are queued
//!    per destination node; committed entries are kept for step 4.
//! 3. Write the batch, sync once if any group needs it, then send one frame
//!    per destination node.
//! 4. Tell raft-rs the batch is durable, apply committed entries to the
//!    state machines, resolve proposals and reads, take snapshots.
//!
//! Callers (proposals, reads, membership changes) act on a group under its
//! lock from any thread and leave the rest to the loop.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{Duration, Instant};

use protobuf::Message as _;
use raft::prelude::{
    ConfChange, ConfChangeV2, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot, SnapshotMetadata,
};
use raft::{ProgressState, RawNode, ReadOnlyOption, SnapshotStatus, StateRole};
use sprig_common::time::Clock;
use sprig_engine::log::Durability;

use crate::config::RaftConfig;
use crate::error::RaftError;
use crate::lease::Lease;
use crate::logstore::{GroupWrite, LogStore, SnapshotWrite, WriteBatch};
use crate::slot::Slot;
use crate::sm::{StateMachine, StateMachineFactory};
use crate::storage::GroupStorage;
use crate::transport::Transport;
use crate::wire::{Frame, Item};
use crate::{GroupId, INIT_INDEX, INIT_TERM, NodeId};

/// Events a test harness can watch (all no-ops by default).
pub trait Observer: Send + Sync {
    /// The replica's role changed: `leader` at `term`, or not leader.
    fn on_role(&self, group: GroupId, term: u64, leader: bool) {
        let _ = (group, term, leader);
    }
    /// A read was answered under the lease that started at `start` (node
    /// clock), at `now`.
    fn on_lease_read(&self, group: GroupId, term: u64, start: Instant, now: Instant) {
        let _ = (group, term, start, now);
    }
    /// The replica stopped using its lease of `term` at `at` (node clock):
    /// it started a leader transfer or stopped leading.
    fn on_lease_end(&self, group: GroupId, term: u64, at: Instant) {
        let _ = (group, term, at);
    }
    /// An entry was applied.
    fn on_apply(&self, group: GroupId, index: u64, term: u64) {
        let _ = (group, index, term);
    }
}

struct NoObserver;
impl Observer for NoObserver {}

/// Learns a proposal's outcome. The driver calls exactly one method once.
pub trait Waiter: Send {
    /// The entry committed and was applied at `index`.
    fn applied(&mut self, index: u64);
    /// The entry at `index` failed.
    fn failed(&mut self, index: u64, err: RaftError);
}

struct SlotWaiter(Slot<Result<u64, RaftError>>);

impl Waiter for SlotWaiter {
    fn applied(&mut self, index: u64) {
        self.0.set(Ok(index));
    }
    fn failed(&mut self, _index: u64, err: RaftError) {
        self.0.set(Err(err));
    }
}

/// What a node is built from.
pub struct NodeOptions {
    /// This node.
    pub id: NodeId,
    /// Settings.
    pub config: RaftConfig,
    /// The node's log.
    pub store: Arc<dyn LogStore>,
    /// Frames to other nodes.
    pub transport: Arc<dyn Transport>,
    /// Monotonic time for ticks, leases and liveness.
    pub clock: Arc<dyn Clock>,
    /// Each group's state machine.
    pub machines: Arc<dyn StateMachineFactory>,
    /// Events for test harnesses.
    pub observer: Option<Arc<dyn Observer>>,
    /// Seeds election timeouts and the node's incarnation id (simulations
    /// pass the run's seed; production any random number).
    pub seed: u64,
}

/// One group as seen from this node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupStatus {
    /// The group.
    pub group: GroupId,
    /// The current term.
    pub term: u64,
    /// The leader this replica knows of.
    pub leader: Option<NodeId>,
    /// Whether this replica leads.
    pub is_leader: bool,
    /// The commit index.
    pub committed: u64,
    /// The applied index.
    pub applied: u64,
    /// The last index in the log.
    pub last_index: u64,
    /// Whether the group is quiesced here.
    pub quiesced: bool,
    /// Voters (the incoming half in a joint configuration).
    pub voters: Vec<NodeId>,
    /// The outgoing voters of a joint configuration.
    pub voters_outgoing: Vec<NodeId>,
    /// Learners.
    pub learners: Vec<NodeId>,
}

/// Counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeStats {
    /// Loop turns.
    pub turns: u64,
    /// Log store writes.
    pub writes: u64,
    /// Log store syncs.
    pub syncs: u64,
    /// Frames sent.
    pub frames_sent: u64,
    /// Frames received.
    pub frames_received: u64,
    /// Raft messages sent.
    pub messages_sent: u64,
    /// Group ticks.
    pub group_ticks: u64,
    /// Reads answered under a lease.
    pub lease_reads: u64,
    /// Reads that went through `ReadIndex`.
    pub index_reads: u64,
    /// Groups hosted.
    pub groups: u64,
    /// Groups not quiesced.
    pub active: u64,
}

#[derive(Default)]
struct Counters {
    turns: AtomicU64,
    writes: AtomicU64,
    syncs: AtomicU64,
    frames_sent: AtomicU64,
    frames_received: AtomicU64,
    messages_sent: AtomicU64,
    group_ticks: AtomicU64,
    lease_reads: AtomicU64,
    index_reads: AtomicU64,
}

fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A group's commit and apply progress, readable without its lock.
#[derive(Default)]
pub(crate) struct Progress {
    committed: AtomicU64,
    applied: AtomicU64,
    closed: AtomicBool,
    mu: Mutex<()>,
    cv: Condvar,
    notify: tokio::sync::Notify,
}

impl Progress {
    fn set(&self, committed: u64, applied: u64) {
        self.committed.fetch_max(committed, Ordering::AcqRel);
        self.applied.fetch_max(applied, Ordering::AcqRel);
        drop(lock(&self.mu));
        self.cv.notify_all();
        self.notify.notify_waiters();
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.set(0, 0);
    }

    fn wait_applied(&self, index: u64) -> Result<(), RaftError> {
        let mut g = lock(&self.mu);
        loop {
            if self.applied.load(Ordering::Acquire) >= index {
                return Ok(());
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(RaftError::Closed);
            }
            g = self.cv.wait(g).unwrap_or_else(PoisonError::into_inner);
        }
    }

    async fn wait_applied_async(&self, index: u64) -> Result<(), RaftError> {
        loop {
            let n = self.notify.notified();
            let mut n = std::pin::pin!(n);
            n.as_mut().enable();
            if self.applied.load(Ordering::Acquire) >= index {
                return Ok(());
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(RaftError::Closed);
            }
            n.await;
        }
    }
}

struct Pending {
    term: u64,
    index: u64,
    conf: bool,
    waiter: Box<dyn Waiter>,
}

struct ReadReq {
    slot: Slot<Result<Vec<u8>, RaftError>>,
    query: Vec<u8>,
}

struct Group {
    id: GroupId,
    raw: RawNode<GroupStorage>,
    sm: Box<dyn StateMachine>,
    /// Durable state machines: entries up to here were applied before the
    /// restart.
    skip_until: u64,
    applied: u64,
    applied_term: u64,
    applied_conf: ConfState,
    pending: VecDeque<Pending>,
    /// `ReadIndex` requests waiting for the leader to confirm, by context.
    reads: BTreeMap<u64, ReadReq>,
    /// Reads with a confirmed index, waiting for it to be applied.
    waiting: Vec<(u64, ReadReq)>,
    lease: Lease,
    quiesced: bool,
    idle: u32,
    role: (u64, StateRole),
    /// Ticks each follower has waited for a snapshot.
    snap_wait: BTreeMap<NodeId, u32>,
}

struct Cell {
    g: Mutex<Group>,
    progress: Arc<Progress>,
}

#[derive(Debug, Default)]
struct Peer {
    last_heard: Option<Instant>,
    alive: bool,
    next_seq: u64,
    epoch_seen: u64,
    /// Seqs of the peer's frames processed this turn.
    acks: Vec<u64>,
    last_sent: Option<Instant>,
}

#[derive(Default)]
struct Inbox {
    frames: Vec<Vec<u8>>,
    dirty: BTreeSet<GroupId>,
    writes: Vec<GroupWrite>,
    created: Vec<Slot<Result<(), RaftError>>>,
}

struct Inner {
    id: NodeId,
    cfg: RaftConfig,
    store: Arc<dyn LogStore>,
    transport: Arc<dyn Transport>,
    clock: Arc<dyn Clock>,
    machines: Arc<dyn StateMachineFactory>,
    observer: Arc<dyn Observer>,
    logger: slog::Logger,
    epoch: u64,
    started: Instant,
    groups: RwLock<BTreeMap<GroupId, Arc<Cell>>>,
    tombstones: Mutex<BTreeMap<GroupId, u64>>,
    active: Mutex<BTreeSet<GroupId>>,
    inbox: Mutex<Inbox>,
    wake: tokio::sync::Notify,
    closed: AtomicBool,
    rng: Mutex<u64>,
    next_ctx: AtomicU64,
    peers: Mutex<BTreeMap<NodeId, Peer>>,
    counters: Counters,
}

/// A node hosting Raft groups. Clones share the node.
#[derive(Clone)]
pub struct Node {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

/// What step 2 of a turn keeps for step 4.
struct Collected {
    cell: Arc<Cell>,
    number: u64,
    committed: Vec<Entry>,
    snapshot_index: Option<u64>,
}

fn is_conf(e: &Entry) -> bool {
    matches!(
        e.get_entry_type(),
        EntryType::EntryConfChange | EntryType::EntryConfChangeV2
    )
}

/// Entries proposed with a relaxed durability class carry this context
/// byte; every other entry is synced before it is acknowledged.
const RELAXED: u8 = b'r';

fn needs_sync(e: &Entry) -> bool {
    e.context.first() != Some(&RELAXED)
}

fn in_conf(cs: &ConfState, id: NodeId) -> bool {
    cs.voters
        .iter()
        .chain(&cs.voters_outgoing)
        .chain(&cs.learners)
        .chain(&cs.learners_next)
        .any(|v| *v == id)
}

fn conf_ids(cs: &ConfState) -> impl Iterator<Item = NodeId> + '_ {
    cs.voters
        .iter()
        .chain(&cs.voters_outgoing)
        .chain(&cs.learners)
        .chain(&cs.learners_next)
        .copied()
}

fn known(id: u64) -> Option<NodeId> {
    (id != raft::INVALID_ID).then_some(id)
}

fn leader_msg(t: MessageType) -> bool {
    matches!(
        t,
        MessageType::MsgAppend | MessageType::MsgHeartbeat | MessageType::MsgSnapshot
    )
}

impl Node {
    /// Opens the node: recovers every group from the log store. Call
    /// [`Node::run`] (or [`Node::spawn`]) to drive it.
    ///
    /// # Errors
    /// The log store could not be read, or a group could not be rebuilt.
    pub fn open(opts: NodeOptions) -> Result<Node, RaftError> {
        let now = opts.clock.now();
        let mut seed = opts.seed ^ opts.id.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        if seed == 0 {
            seed = 0x2545_f491_4f6c_dd1d;
        }
        let inner = Arc::new(Inner {
            id: opts.id,
            logger: crate::logger::logger(opts.id),
            cfg: opts.config,
            store: opts.store,
            transport: opts.transport,
            clock: opts.clock,
            machines: opts.machines,
            observer: opts.observer.unwrap_or_else(|| Arc::new(NoObserver)),
            epoch: splitmix(seed) | 1,
            started: now,
            groups: RwLock::new(BTreeMap::new()),
            tombstones: Mutex::new(BTreeMap::new()),
            active: Mutex::new(BTreeSet::new()),
            inbox: Mutex::new(Inbox::default()),
            wake: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
            rng: Mutex::new(splitmix(seed ^ 0x5eed)),
            next_ctx: AtomicU64::new(1),
            peers: Mutex::new(BTreeMap::new()),
            counters: Counters::default(),
        });
        let node = Node { inner };
        for rg in node.inner.store.recover()? {
            if let Some(t) = rg.tombstone {
                lock(&node.inner.tombstones).insert(rg.group, t);
                continue;
            }
            let mut sm = node.inner.machines.create(rg.group);
            let meta = rg.snapshot.get_metadata().clone();
            let skip_until = if sm.durable() {
                sm.applied_index()
            } else {
                if meta.index > 0 {
                    sm.restore(meta.index, &rg.snapshot.data)?;
                }
                0
            };
            let storage = GroupStorage::recovered(rg.hard_state, &rg.snapshot, rg.entries);
            node.insert_group(
                rg.group,
                storage,
                sm,
                skip_until,
                meta.index,
                meta.term,
                meta.get_conf_state().clone(),
            )?;
        }
        // Recovered groups may have committed entries to apply again.
        let ids: Vec<GroupId> = node.read_groups().keys().copied().collect();
        lock(&node.inner.inbox).dirty.extend(ids);
        Ok(node)
    }

    fn read_groups(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<GroupId, Arc<Cell>>> {
        self.inner
            .groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write_groups(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<GroupId, Arc<Cell>>> {
        self.inner
            .groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn raft_config(&self, applied: u64) -> raft::Config {
        let c = &self.inner.cfg;
        raft::Config {
            id: self.inner.id,
            election_tick: c.election_ticks,
            heartbeat_tick: c.heartbeat_ticks,
            applied,
            max_size_per_msg: c.max_size_per_msg,
            max_inflight_msgs: c.max_inflight_msgs,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ReadOnlyOption::Safe,
            ..raft::Config::default()
        }
    }

    fn rand(&self) -> u64 {
        let mut r = lock(&self.inner.rng);
        *r = splitmix(*r);
        *r
    }

    /// raft-rs draws election timeouts from the thread's random generator;
    /// the driver redraws them from its seeded one so simulations replay.
    fn reseed_timeout(&self, raw: &mut RawNode<GroupStorage>) {
        let min = self.inner.cfg.election_ticks.max(1);
        let extra = usize::try_from(self.rand() % min as u64).unwrap_or(0);
        raw.raft.set_randomized_election_timeout(min + extra);
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_group(
        &self,
        id: GroupId,
        storage: GroupStorage,
        sm: Box<dyn StateMachine>,
        skip_until: u64,
        applied: u64,
        applied_term: u64,
        applied_conf: ConfState,
    ) -> Result<Arc<Cell>, RaftError> {
        let mut raw = RawNode::new(&self.raft_config(applied), storage, &self.inner.logger)?;
        self.reseed_timeout(&mut raw);
        let role = (raw.raft.term, raw.raft.state);
        self.note_peers(&applied_conf);
        let cell = Arc::new(Cell {
            g: Mutex::new(Group {
                id,
                raw,
                sm,
                skip_until,
                applied,
                applied_term,
                applied_conf,
                pending: VecDeque::new(),
                reads: BTreeMap::new(),
                waiting: Vec::new(),
                lease: Lease::default(),
                quiesced: false,
                idle: 0,
                role,
                snap_wait: BTreeMap::new(),
            }),
            progress: Arc::new(Progress::default()),
        });
        cell.progress.set(applied, applied);
        self.write_groups().insert(id, Arc::clone(&cell));
        lock(&self.inner.active).insert(id);
        Ok(cell)
    }

    fn note_peers(&self, cs: &ConfState) {
        let mut peers = lock(&self.inner.peers);
        for id in conf_ids(cs) {
            if id != self.inner.id {
                peers.entry(id).or_insert_with(|| Peer {
                    alive: true,
                    ..Peer::default()
                });
            }
        }
    }

    fn cell(&self, group: GroupId) -> Option<Arc<Cell>> {
        self.read_groups().get(&group).cloned()
    }

    fn mark_dirty(&self, group: GroupId) {
        lock(&self.inner.inbox).dirty.insert(group);
        self.inner.wake.notify_one();
    }

    fn wake_group(&self, g: &mut Group) {
        if g.quiesced {
            g.quiesced = false;
            g.idle = 0;
            lock(&self.inner.active).insert(g.id);
        }
    }

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    /// Hands the node a frame another node sent (transports call this).
    pub fn deliver(&self, frame: Vec<u8>) {
        if self.inner.closed.load(Ordering::Acquire) {
            return;
        }
        lock(&self.inner.inbox).frames.push(frame);
        self.inner.wake.notify_one();
    }

    /// Creates `group` here with `voters` as its first members. Every
    /// initial voter calls this with the same voters; replicas added later
    /// are created by the leader's first message instead. The slot is set
    /// once the group is durable.
    ///
    /// # Errors
    /// The node is closed, `voters` is empty, or the state machine refused
    /// the empty state.
    pub fn create_group(
        &self,
        group: GroupId,
        voters: &[NodeId],
    ) -> Result<Slot<Result<(), RaftError>>, RaftError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RaftError::Closed);
        }
        if voters.is_empty() {
            return Err(RaftError::Invalid("a group needs voters".into()));
        }
        if self.cell(group).is_some() {
            return Ok(Slot::ready(Ok(())));
        }
        lock(&self.inner.tombstones).remove(&group);
        let mut cs = ConfState::default();
        cs.voters = voters.to_vec();
        let mut sm = self.inner.machines.create(group);
        sm.restore(INIT_INDEX, &[])?;
        let mut snap = Snapshot::default();
        snap.data = sm.snapshot()?.into();
        let mut meta = SnapshotMetadata::default();
        meta.index = INIT_INDEX;
        meta.term = INIT_TERM;
        meta.set_conf_state(cs.clone());
        snap.set_metadata(meta);
        let mut hs = HardState::default();
        hs.term = INIT_TERM;
        hs.commit = INIT_INDEX;
        let storage = GroupStorage::recovered(hs.clone(), &snap, Vec::new());
        self.insert_group(group, storage, sm, 0, INIT_INDEX, INIT_TERM, cs)?;
        let slot = Slot::new();
        {
            let mut ib = lock(&self.inner.inbox);
            ib.writes.push(GroupWrite {
                group,
                snapshot: Some(SnapshotWrite::Install(snap)),
                hard_state: Some(hs),
                ..GroupWrite::default()
            });
            ib.created.push(slot.clone());
            ib.dirty.insert(group);
        }
        self.inner.wake.notify_one();
        Ok(slot)
    }

    fn leading<'a>(&self, cell: &'a Cell) -> Result<MutexGuard<'a, Group>, RaftError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RaftError::Closed);
        }
        let g = lock(&cell.g);
        if g.raw.raft.state != StateRole::Leader {
            return Err(RaftError::NotLeader {
                leader: known(g.raw.raft.leader_id),
            });
        }
        Ok(g)
    }

    /// Proposes `data` to `group`, calling `waiter(term)` for the object
    /// that learns the outcome. Returns the entry's term and index, known
    /// at once.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`] (nothing written), [`RaftError::Busy`]
    /// during a leader transfer, [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn propose_with(
        &self,
        group: GroupId,
        data: Vec<u8>,
        durability: Durability,
        waiter: impl FnOnce(u64) -> Box<dyn Waiter>,
    ) -> Result<(u64, u64), RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = self.leading(&cell)?;
        if g.raw.raft.lead_transferee.is_some() {
            return Err(RaftError::Busy("a leader transfer is running"));
        }
        let index = g.raw.raft.raft_log.last_index() + 1;
        let term = g.raw.raft.term;
        let ctx = if durability.needs_sync() {
            Vec::new()
        } else {
            vec![RELAXED]
        };
        g.raw.propose(ctx, data)?;
        if g.raw.raft.raft_log.last_index() != index {
            return Err(RaftError::NotLeader { leader: None });
        }
        g.pending.push_back(Pending {
            term,
            index,
            conf: false,
            waiter: waiter(term),
        });
        self.wake_group(&mut g);
        drop(g);
        self.mark_dirty(group);
        Ok((term, index))
    }

    /// Proposes `data`; the slot gets the entry's index once it is applied
    /// here, or why it failed.
    ///
    /// # Errors
    /// As for [`Node::propose_with`].
    pub fn propose(
        &self,
        group: GroupId,
        data: Vec<u8>,
        durability: Durability,
    ) -> Result<Slot<Result<u64, RaftError>>, RaftError> {
        let slot = Slot::new();
        let s = slot.clone();
        self.propose_with(group, data, durability, move |_| Box::new(SlotWaiter(s)))?;
        Ok(slot)
    }

    /// A linearizable read: under the leader's lease when it holds,
    /// otherwise through `ReadIndex`. The slot gets the state machine's answer
    /// to `query` once the read's index is applied.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`], [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn read(
        &self,
        group: GroupId,
        query: Vec<u8>,
    ) -> Result<Slot<Result<Vec<u8>, RaftError>>, RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = self.leading(&cell)?;
        let slot = Slot::new();
        let now = self.inner.clock.now();
        if !g.quiesced && g.raw.raft.commit_to_current_term() && g.lease.term() == g.raw.raft.term {
            let usable = self.inner.cfg.usable_lease();
            if let Some((start, _)) =
                g.lease
                    .valid_until(&g.applied_conf, self.inner.id, now, usable)
            {
                let index = g.raw.raft.raft_log.committed;
                self.inner
                    .observer
                    .on_lease_read(group, g.raw.raft.term, start, now);
                bump(&self.inner.counters.lease_reads, 1);
                let req = ReadReq {
                    slot: slot.clone(),
                    query,
                };
                if g.applied >= index {
                    req.slot.set(Ok(g.sm.query(&req.query)));
                } else {
                    g.waiting.push((index, req));
                }
                return Ok(slot);
            }
        }
        bump(&self.inner.counters.index_reads, 1);
        let ctx = self.inner.next_ctx.fetch_add(1, Ordering::Relaxed);
        g.raw.read_index(ctx.to_le_bytes().to_vec());
        g.reads.insert(
            ctx,
            ReadReq {
                slot: slot.clone(),
                query,
            },
        );
        self.wake_group(&mut g);
        drop(g);
        self.mark_dirty(group);
        Ok(slot)
    }

    /// Answers `query` from this replica's state without any check (stale
    /// reads, diagnostics), with the applied index it reflects.
    #[must_use]
    pub fn query_local(&self, group: GroupId, query: &[u8]) -> Option<(u64, Vec<u8>)> {
        let cell = self.cell(group)?;
        let g = lock(&cell.g);
        Some((g.applied, g.sm.query(query)))
    }

    /// Proposes a membership change (adding or removing voters and
    /// learners; several changes at once go through a joint configuration).
    /// The slot is set once the change is applied here.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`], [`RaftError::Busy`] while another change is
    /// pending, [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn change_membership(
        &self,
        group: GroupId,
        cc: ConfChangeV2,
    ) -> Result<Slot<Result<u64, RaftError>>, RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = self.leading(&cell)?;
        if g.raw.raft.has_pending_conf() || !g.applied_conf.voters_outgoing.is_empty() {
            return Err(RaftError::Busy("a membership change is pending"));
        }
        if g.raw.raft.lead_transferee.is_some() {
            return Err(RaftError::Busy("a leader transfer is running"));
        }
        let index = g.raw.raft.raft_log.last_index() + 1;
        let term = g.raw.raft.term;
        g.raw.propose_conf_change(Vec::new(), cc)?;
        if g.raw.raft.raft_log.last_index() != index {
            return Err(RaftError::NotLeader { leader: None });
        }
        let slot = Slot::new();
        g.pending.push_back(Pending {
            term,
            index,
            conf: true,
            waiter: Box::new(SlotWaiter(slot.clone())),
        });
        self.wake_group(&mut g);
        drop(g);
        self.mark_dirty(group);
        Ok(slot)
    }

    /// Asks the leader to hand leadership to `to`. The lease stops for the
    /// rest of this term.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`], [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn transfer_leader(&self, group: GroupId, to: NodeId) -> Result<(), RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = self.leading(&cell)?;
        g.lease.disable();
        let term = g.raw.raft.term;
        self.inner
            .observer
            .on_lease_end(group, term, self.inner.clock.now());
        g.raw.transfer_leader(to);
        self.wake_group(&mut g);
        drop(g);
        self.mark_dirty(group);
        Ok(())
    }

    /// Starts an election for `group` on this replica now.
    ///
    /// # Errors
    /// [`RaftError::NoGroup`], or raft-rs refused.
    pub fn campaign(&self, group: GroupId) -> Result<(), RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = lock(&cell.g);
        self.wake_group(&mut g);
        g.raw.campaign()?;
        drop(g);
        self.mark_dirty(group);
        Ok(())
    }

    /// The group's state here.
    #[must_use]
    pub fn status(&self, group: GroupId) -> Option<GroupStatus> {
        let cell = self.cell(group)?;
        let g = lock(&cell.g);
        let r = &g.raw.raft;
        Some(GroupStatus {
            group,
            term: r.term,
            leader: known(r.leader_id),
            is_leader: r.state == StateRole::Leader,
            committed: r.raft_log.committed,
            applied: g.applied,
            last_index: r.raft_log.last_index(),
            quiesced: g.quiesced,
            voters: g.applied_conf.voters.clone(),
            voters_outgoing: g.applied_conf.voters_outgoing.clone(),
            learners: g.applied_conf.learners.clone(),
        })
    }

    /// The groups hosted here.
    #[must_use]
    pub fn groups(&self) -> Vec<GroupId> {
        self.read_groups().keys().copied().collect()
    }

    /// The commit index of `group` known here.
    #[must_use]
    pub fn committed(&self, group: GroupId) -> u64 {
        self.cell(group)
            .map_or(0, |c| c.progress.committed.load(Ordering::Acquire))
    }

    /// The applied index of `group` here.
    #[must_use]
    pub fn applied(&self, group: GroupId) -> u64 {
        self.cell(group)
            .map_or(0, |c| c.progress.applied.load(Ordering::Acquire))
    }

    /// Blocks until `group` has applied `index` here.
    ///
    /// # Errors
    /// [`RaftError::NoGroup`], or the node closed first.
    pub fn wait_applied(&self, group: GroupId, index: u64) -> Result<(), RaftError> {
        let p = self
            .cell(group)
            .map(|c| Arc::clone(&c.progress))
            .ok_or(RaftError::NoGroup(group))?;
        p.wait_applied(index)
    }

    /// The async form of [`Node::wait_applied`].
    ///
    /// # Errors
    /// As for [`Node::wait_applied`].
    pub async fn wait_applied_async(&self, group: GroupId, index: u64) -> Result<(), RaftError> {
        let p = self
            .cell(group)
            .map(|c| Arc::clone(&c.progress))
            .ok_or(RaftError::NoGroup(group))?;
        p.wait_applied_async(index).await
    }

    /// For a durable state machine whose store has made everything up to
    /// `up_to` durable: trims the group's log up to there (only when that
    /// is still its applied index, so the recorded configuration is right).
    /// Returns whether it trimmed.
    #[must_use]
    pub fn compact_durable(&self, group: GroupId, up_to: u64) -> bool {
        let Some(cell) = self.cell(group) else {
            return false;
        };
        let mut g = lock(&cell.g);
        if !g.sm.durable() || g.applied != up_to || up_to <= g.raw.store().snapshot_index() {
            return false;
        }
        let mut snap = Snapshot::default();
        let mut meta = SnapshotMetadata::default();
        meta.index = g.applied;
        meta.term = g.applied_term;
        meta.set_conf_state(g.applied_conf.clone());
        snap.set_metadata(meta);
        g.raw.mut_store().trim(&snap);
        drop(g);
        lock(&self.inner.inbox).writes.push(GroupWrite {
            group,
            snapshot: Some(SnapshotWrite::Checkpoint(snap)),
            ..GroupWrite::default()
        });
        self.inner.wake.notify_one();
        true
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> NodeStats {
        let c = &self.inner.counters;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        NodeStats {
            turns: ld(&c.turns),
            writes: ld(&c.writes),
            syncs: ld(&c.syncs),
            frames_sent: ld(&c.frames_sent),
            frames_received: ld(&c.frames_received),
            messages_sent: ld(&c.messages_sent),
            group_ticks: ld(&c.group_ticks),
            lease_reads: ld(&c.lease_reads),
            index_reads: ld(&c.index_reads),
            groups: self.read_groups().len() as u64,
            active: lock(&self.inner.active).len() as u64,
        }
    }

    /// Bytes of log entries and cached snapshots held in memory.
    #[must_use]
    pub fn log_mem_bytes(&self) -> usize {
        self.read_groups()
            .values()
            .map(|c| lock(&c.g).raw.store().mem_bytes())
            .sum()
    }

    /// Whether the node was closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// Stops the node: the loop exits, pending proposals fail with
    /// [`RaftError::Unknown`] and reads with [`RaftError::Closed`]. A
    /// simulated crash calls this too.
    pub fn close(&self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.wake.notify_one();
        let cells: Vec<Arc<Cell>> = self.read_groups().values().cloned().collect();
        for c in cells {
            let mut g = lock(&c.g);
            fail_pending(&mut g, 0, &RaftError::Unknown);
            fail_reads(&mut g, &RaftError::Closed);
            for (_, r) in g.waiting.drain(..) {
                r.slot.set(Err(RaftError::Closed));
            }
            drop(g);
            c.progress.close();
        }
        let mut ib = lock(&self.inner.inbox);
        for s in ib.created.drain(..) {
            s.set(Err(RaftError::Closed));
        }
    }

    /// Runs the driver on a thread of its own (with its own current-thread
    /// runtime) until [`Node::close`].
    ///
    /// # Errors
    /// The thread could not be started.
    pub fn spawn(&self) -> std::io::Result<std::thread::JoinHandle<()>> {
        let node = self.clone();
        std::thread::Builder::new()
            .name(format!("sprig-raft-{}", self.inner.id))
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build()
                {
                    Ok(rt) => rt.block_on(node.run()),
                    Err(e) => {
                        tracing::error!(error = %e, "raft driver: no runtime");
                        node.close();
                    }
                }
            })
    }

    fn has_work(&self) -> bool {
        let ib = lock(&self.inner.inbox);
        !ib.frames.is_empty() || !ib.dirty.is_empty() || !ib.writes.is_empty()
    }

    /// The driver loop. Returns after [`Node::close`].
    pub async fn run(&self) {
        let cfg = self.inner.cfg.clone();
        let start = self.inner.clock.now();
        let mut next_tick = start + cfg.tick;
        let mut next_live = start;
        let max_ticks = u32::try_from(cfg.election_ticks * 2 + 1).unwrap_or(u32::MAX);
        loop {
            if self.is_closed() {
                return;
            }
            let now = self.inner.clock.now();
            let due = next_tick.min(next_live);
            if due > now && !self.has_work() {
                tokio::select! {
                    biased;
                    () = self.inner.wake.notified() => {}
                    () = tokio::time::sleep(due - now) => {}
                }
            }
            if self.is_closed() {
                return;
            }
            let now = self.inner.clock.now();
            let mut ticks = 0u32;
            while next_tick <= now {
                ticks += 1;
                next_tick += cfg.tick;
            }
            let ticks = ticks.min(max_ticks);
            let live = next_live <= now;
            if live {
                next_live = now + cfg.liveness_interval;
            }
            if let Err(e) = self.turn(now, ticks, live).await {
                tracing::error!(node = self.inner.id, error = %e, "raft driver stopped: log store failed");
                self.close();
                return;
            }
        }
    }

    async fn turn(&self, now: Instant, ticks: u32, live: bool) -> Result<(), RaftError> {
        bump(&self.inner.counters.turns, 1);
        let (frames, mut dirty, mut writes, mut created) = {
            let mut ib = lock(&self.inner.inbox);
            (
                std::mem::take(&mut ib.frames),
                std::mem::take(&mut ib.dirty),
                std::mem::take(&mut ib.writes),
                std::mem::take(&mut ib.created),
            )
        };
        let mut out: BTreeMap<NodeId, Vec<Item>> = BTreeMap::new();
        let mut ctrl: BTreeMap<NodeId, Vec<Item>> = BTreeMap::new();
        bump(&self.inner.counters.frames_received, frames.len() as u64);
        for f in &frames {
            self.on_frame(f, now, &mut dirty, &mut out);
        }
        if live {
            self.check_liveness(now, &mut dirty);
        }
        if ticks > 0 {
            self.tick_groups(ticks, &mut dirty, &mut ctrl);
        }
        for _ in 0..8 {
            if dirty.is_empty() && writes.is_empty() {
                break;
            }
            let mut batch = WriteBatch {
                groups: std::mem::take(&mut writes),
            };
            let mut need_sync = !batch.is_empty();
            let mut done = Vec::new();
            for gid in std::mem::take(&mut dirty) {
                if let Some(cell) = self.cell(gid)
                    && let Some(c) = self.collect(cell, &mut batch, &mut out, &mut need_sync, now)
                {
                    done.push(c);
                }
            }
            if !batch.is_empty() {
                if need_sync {
                    // A slow disk (simulation): a crash meanwhile loses
                    // the batch, and nothing waiting on it was answered.
                    let d = self.inner.store.sync_delay();
                    if !d.is_zero() {
                        tokio::time::sleep(d).await;
                    }
                    if self.is_closed() {
                        return Ok(());
                    }
                    bump(&self.inner.counters.syncs, 1);
                }
                self.inner.store.write(&batch, need_sync)?;
                bump(&self.inner.counters.writes, 1);
            }
            for s in created.drain(..) {
                s.set(Ok(()));
            }
            self.flush(&mut out, &mut ctrl, now);
            for c in done {
                self.finish(c, &mut writes, &mut dirty)?;
            }
        }
        if !dirty.is_empty() || !writes.is_empty() {
            let mut ib = lock(&self.inner.inbox);
            ib.dirty.extend(dirty);
            ib.writes.extend(writes);
        }
        if live {
            self.ping_peers(now, &mut out);
        }
        self.flush(&mut out, &mut ctrl, now);
        for p in lock(&self.inner.peers).values_mut() {
            p.acks.clear();
        }
        Ok(())
    }

    fn on_frame(
        &self,
        bytes: &[u8],
        now: Instant,
        dirty: &mut BTreeSet<GroupId>,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
    ) {
        let Some(frame) = Frame::decode(bytes) else {
            tracing::debug!(node = self.inner.id, "raft: malformed frame dropped");
            return;
        };
        if frame.to != self.inner.id {
            return;
        }
        {
            let mut peers = lock(&self.inner.peers);
            let peer = peers.entry(frame.from).or_default();
            peer.last_heard = Some(now);
            peer.alive = true;
            if peer.epoch_seen != frame.epoch {
                peer.epoch_seen = frame.epoch;
                peer.acks.clear();
            }
            peer.acks.push(frame.seq);
        }
        // A restarted node votes for no one until a leader's lease it may
        // have supported before the crash has run out.
        let grace =
            now.saturating_duration_since(self.inner.started) < self.inner.cfg.min_election();
        let mut responded = Vec::new();
        for it in frame.items {
            match it {
                Item::Raft(gid, sender_applied, m) => {
                    let t = m.get_msg_type();
                    if grace
                        && matches!(
                            t,
                            MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
                        )
                    {
                        continue;
                    }
                    let Some(cell) = self.cell_or_create(gid, &m, sender_applied) else {
                        continue;
                    };
                    let mut g = lock(&cell.g);
                    let response = matches!(
                        t,
                        MessageType::MsgHeartbeatResponse | MessageType::MsgAppendResponse
                    );
                    let credit = response
                        && m.term == g.raw.raft.term
                        && g.raw.raft.state == StateRole::Leader;
                    let mut m = m;
                    self.repair_stale(&mut g, gid, &mut m, out);
                    let _ = g.raw.step(m);
                    self.check_role(&mut g);
                    // A trailing response must not wake a group its leader
                    // just quiesced; anything else does.
                    if !response || g.raw.has_ready() {
                        self.wake_group(&mut g);
                    }
                    if credit
                        && g.raw.raft.state == StateRole::Leader
                        && g.lease.term() == g.raw.raft.term
                    {
                        responded.push(gid);
                    }
                    dirty.insert(gid);
                }
                Item::Quiesce { group, term, index } => {
                    if let Some(cell) = self.cell(group) {
                        let mut g = lock(&cell.g);
                        let r = &g.raw.raft;
                        if !g.quiesced
                            && r.state == StateRole::Follower
                            && r.term == term
                            && r.leader_id == frame.from
                            && r.raft_log.last_index() == index
                            && r.raft_log.committed == index
                        {
                            g.quiesced = true;
                            g.idle = 0;
                            lock(&self.inner.active).remove(&group);
                        }
                    }
                }
            }
        }
        if frame.ack_epoch == self.inner.epoch && !frame.acks.is_empty() {
            for gid in responded {
                if let Some(cell) = self.cell(gid) {
                    lock(&cell.g).lease.on_ack(frame.from, &frame.acks);
                }
            }
        }
    }

    /// Handles a group's leader holding progress for an old incarnation of
    /// this replica (removed and re-added), from either side.
    fn repair_stale(
        &self,
        g: &mut Group,
        gid: GroupId,
        m: &mut Message,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
    ) {
        // A heartbeat's commit is the leader's view of what this
        // replica holds. A replica removed and re-added can get
        // one meant for its old incarnation (a reply of the old
        // one arrived late), past its log: raft-rs would panic.
        // Not advancing the commit then is always safe; and the
        // leader is told this replica's real last index, as a
        // rejection it would otherwise never ask for.
        let last = g.raw.raft.raft_log.last_index();
        if m.get_msg_type() == MessageType::MsgHeartbeat && m.commit > last {
            let mut r = Message::default();
            r.set_msg_type(MessageType::MsgAppendResponse);
            r.to = m.from;
            r.from = self.inner.id;
            r.term = m.term;
            r.index = m.commit;
            r.reject = true;
            r.reject_hint = last;
            out.entry(m.from)
                .or_default()
                .push(Item::Raft(gid, g.applied, r));
            m.commit = g.raw.raft.raft_log.committed;
        }
        // The leader's side: a follower whose log ends below what
        // the leader thought it matched lost its log (a new
        // incarnation). Lowering `matched` is always safe; the
        // leader then probes and sends a snapshot.
        if m.get_msg_type() == MessageType::MsgAppendResponse
            && m.reject
            && g.raw.raft.state == StateRole::Leader
            && m.term == g.raw.raft.term
            && let Some(pr) = g.raw.raft.mut_prs().get_mut(m.from)
            && m.reject_hint < pr.matched
        {
            pr.matched = m.reject_hint;
            pr.become_probe();
        }
    }

    /// The group, created when a message from its leader arrives for a
    /// replica being added here.
    fn cell_or_create(&self, gid: GroupId, m: &Message, sender_applied: u64) -> Option<Arc<Cell>> {
        if let Some(c) = self.cell(gid) {
            return Some(c);
        }
        if !self.inner.cfg.auto_create || !leader_msg(m.get_msg_type()) {
            return None;
        }
        if let Some(t) = lock(&self.inner.tombstones).get(&gid).copied() {
            // Only a leader that applied this replica's removal (and so,
            // since it still sends here, re-added it later) may bring it
            // back; one that hasn't applied the removal yet must not.
            if sender_applied <= t {
                return None;
            }
        }
        lock(&self.inner.tombstones).remove(&gid);
        let sm = self.inner.machines.create(gid);
        match self.insert_group(
            gid,
            GroupStorage::default(),
            sm,
            0,
            0,
            0,
            ConfState::default(),
        ) {
            Ok(c) => Some(c),
            Err(e) => {
                tracing::warn!(group = gid, error = %e, "raft: could not create a replica");
                None
            }
        }
    }

    /// Notices role and term changes: leases, election timeouts, events.
    fn check_role(&self, g: &mut Group) {
        let role = (g.raw.raft.term, g.raw.raft.state);
        if role == g.role {
            return;
        }
        let was_leader = g.role.1 == StateRole::Leader;
        if was_leader {
            self.inner
                .observer
                .on_lease_end(g.id, g.role.0, self.inner.clock.now());
        }
        g.role = role;
        self.reseed_timeout(&mut g.raw);
        let leader = role.1 == StateRole::Leader;
        if leader {
            g.lease.reset(role.0);
        } else {
            g.lease.reset(0);
            fail_reads(
                g,
                &RaftError::NotLeader {
                    leader: known(g.raw.raft.leader_id),
                },
            );
        }
        if leader || was_leader {
            self.inner.observer.on_role(g.id, role.0, leader);
        }
        self.wake_group(g);
    }

    fn alive_peers(&self) -> BTreeSet<NodeId> {
        let mut s: BTreeSet<NodeId> = lock(&self.inner.peers)
            .iter()
            .filter(|(_, p)| p.alive)
            .map(|(id, _)| *id)
            .collect();
        s.insert(self.inner.id);
        s
    }

    fn tick_groups(
        &self,
        ticks: u32,
        dirty: &mut BTreeSet<GroupId>,
        ctrl: &mut BTreeMap<NodeId, Vec<Item>>,
    ) {
        let alive = self.alive_peers();
        let active: Vec<GroupId> = lock(&self.inner.active).iter().copied().collect();
        bump(
            &self.inner.counters.group_ticks,
            active.len() as u64 * u64::from(ticks),
        );
        for gid in active {
            let Some(cell) = self.cell(gid) else {
                lock(&self.inner.active).remove(&gid);
                continue;
            };
            let mut g = lock(&cell.g);
            if g.quiesced {
                lock(&self.inner.active).remove(&gid);
                continue;
            }
            for _ in 0..ticks {
                g.raw.tick();
            }
            self.check_role(&mut g);
            if g.raw.raft.state == StateRole::Leader {
                self.retry_snapshots(&mut g, ticks);
                if self.can_quiesce(&g, &alive) {
                    g.idle += ticks;
                    if g.idle >= self.inner.cfg.quiesce_ticks {
                        let r = &g.raw.raft;
                        let (term, index) = (r.term, r.raft_log.last_index());
                        for (id, _) in r.prs().iter() {
                            if *id != self.inner.id {
                                ctrl.entry(*id).or_default().push(Item::Quiesce {
                                    group: gid,
                                    term,
                                    index,
                                });
                            }
                        }
                        g.quiesced = true;
                        g.idle = 0;
                        lock(&self.inner.active).remove(&gid);
                    }
                } else {
                    g.idle = 0;
                }
            }
            if g.raw.has_ready() {
                dirty.insert(gid);
            }
        }
    }

    fn can_quiesce(&self, g: &Group, alive: &BTreeSet<NodeId>) -> bool {
        let r = &g.raw.raft;
        let last = r.raft_log.last_index();
        g.pending.is_empty()
            && g.reads.is_empty()
            && g.waiting.is_empty()
            && r.lead_transferee.is_none()
            && r.raft_log.committed == last
            && g.applied == last
            && !r.has_pending_conf()
            && g.applied_conf.voters_outgoing.is_empty()
            && r.prs()
                .iter()
                .all(|(id, pr)| *id == self.inner.id || (pr.matched == last && alive.contains(id)))
    }

    /// A follower that waited too long for a snapshot gets it again (the
    /// first one was lost or its install was cut short).
    fn retry_snapshots(&self, g: &mut Group, ticks: u32) {
        let waiting: Vec<NodeId> = g
            .raw
            .raft
            .prs()
            .iter()
            .filter(|(_, pr)| pr.state == ProgressState::Snapshot)
            .map(|(id, _)| *id)
            .collect();
        g.snap_wait.retain(|id, _| waiting.contains(id));
        for id in waiting {
            let w = g.snap_wait.entry(id).or_insert(0);
            *w += ticks;
            if *w >= self.inner.cfg.snapshot_retry_ticks {
                g.snap_wait.remove(&id);
                g.raw.report_snapshot(id, SnapshotStatus::Failure);
            }
        }
    }

    fn check_liveness(&self, now: Instant, dirty: &mut BTreeSet<GroupId>) {
        let timeout = self.inner.cfg.liveness_timeout;
        let mut died = Vec::new();
        {
            let mut peers = lock(&self.inner.peers);
            for (id, p) in peers.iter_mut() {
                let alive = match p.last_heard {
                    Some(t) => now.saturating_duration_since(t) < timeout,
                    None => now.saturating_duration_since(self.inner.started) < timeout,
                };
                if p.alive && !alive {
                    died.push(*id);
                }
                p.alive = alive;
            }
        }
        if died.is_empty() {
            return;
        }
        let cells: Vec<Arc<Cell>> = self.read_groups().values().cloned().collect();
        for c in cells {
            let mut g = lock(&c.g);
            let r = &g.raw.raft;
            let leader = r.state == StateRole::Leader;
            let touched = died
                .iter()
                .any(|d| r.leader_id == *d || (leader && r.prs().get(*d).is_some()));
            if !touched {
                continue;
            }
            if leader {
                for d in &died {
                    if g.raw.raft.prs().get(*d).is_some() {
                        g.raw.report_unreachable(*d);
                    }
                }
            }
            self.wake_group(&mut g);
            dirty.insert(g.id);
        }
    }

    fn ping_peers(&self, now: Instant, out: &mut BTreeMap<NodeId, Vec<Item>>) {
        let every = self.inner.cfg.liveness_interval;
        for (id, p) in lock(&self.inner.peers).iter() {
            let due = p
                .last_sent
                .is_none_or(|t| now.saturating_duration_since(t) >= every);
            if due {
                out.entry(*id).or_default();
            }
        }
    }

    /// Step 2 for one group.
    fn collect(
        &self,
        cell: Arc<Cell>,
        batch: &mut WriteBatch,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
        need_sync: &mut bool,
        _now: Instant,
    ) -> Option<Collected> {
        let mut g = lock(&cell.g);
        let gid = g.id;
        if !g.raw.has_ready() {
            drop(g);
            // Reads may be waiting on an index applied meanwhile.
            return None;
        }
        let mut rd = g.raw.ready();
        let mut snapshot_index = None;
        if !rd.snapshot().is_empty() {
            let snap = rd.snapshot().clone();
            let meta = snap.get_metadata().clone();
            if let Err(e) = g.sm.restore(meta.index, &snap.data) {
                tracing::error!(group = gid, error = %e, "raft: snapshot restore failed");
            }
            g.raw.mut_store().install(&snap);
            g.applied = meta.index;
            g.applied_term = meta.term;
            g.applied_conf = meta.get_conf_state().clone();
            g.skip_until = 0;
            self.note_peers(&g.applied_conf);
            // The log was replaced: whatever this replica proposed may or
            // may not survive in the leader's log.
            fail_pending(&mut g, 0, &RaftError::Unknown);
            batch.group(gid).snapshot = Some(SnapshotWrite::Install(snap));
            *need_sync = true;
            snapshot_index = Some(meta.index);
        }
        let entries = rd.take_entries();
        if let Some(first) = entries.first().map(|e| e.index) {
            g.raw.mut_store().append(&entries);
            // Proposals another leader's entries replaced never commit.
            let mut lost = None;
            for p in &g.pending {
                if p.index >= first && g.raw.store().term_at(p.index) != Some(p.term) {
                    lost = Some(p.index);
                    break;
                }
            }
            if let Some(i) = lost {
                fail_pending(&mut g, i, &RaftError::Dropped);
            }
            if entries.iter().any(needs_sync) {
                *need_sync = true;
            }
            batch.group(gid).entries.extend(entries);
        }
        if let Some(hs) = rd.hs() {
            let old = g.raw.store().hard_state();
            if hs.term != old.term || hs.vote != old.vote {
                *need_sync = true;
            }
            let hs = hs.clone();
            g.raw.mut_store().set_hard_state(hs.clone());
            batch.group(gid).hard_state = Some(hs);
        }
        for m in rd
            .take_messages()
            .into_iter()
            .chain(rd.take_persisted_messages())
        {
            out.entry(m.to)
                .or_default()
                .push(Item::Raft(gid, g.applied, m));
        }
        let committed = rd.take_committed_entries();
        for rs in rd.take_read_states() {
            let mut b = [0u8; 8];
            let n = rs.request_ctx.len().min(8);
            b[..n].copy_from_slice(&rs.request_ctx[..n]);
            if let Some(req) = g.reads.remove(&u64::from_le_bytes(b)) {
                g.waiting.push((rs.index, req));
            }
        }
        let number = rd.number();
        g.raw.advance_append_async(rd);
        self.check_role(&mut g);
        drop(g);
        Some(Collected {
            cell,
            number,
            committed,
            snapshot_index,
        })
    }

    /// Step 4 for one group.
    fn finish(
        &self,
        c: Collected,
        writes: &mut Vec<GroupWrite>,
        dirty: &mut BTreeSet<GroupId>,
    ) -> Result<(), RaftError> {
        let mut g = lock(&c.cell.g);
        let gid = g.id;
        g.raw.on_persist_ready(c.number);
        if let Some(i) = c.snapshot_index
            && g.raw.raft.raft_log.applied < i
        {
            g.raw.advance_apply_to(i);
        }
        let mut run: Vec<Entry> = Vec::new();
        let mut removed = None;
        for e in c.committed {
            if e.index <= g.applied {
                continue;
            }
            if is_conf(&e) {
                self.apply_run(&mut g, &mut run)?;
                let cs = if e.get_entry_type() == EntryType::EntryConfChange {
                    ConfChange::parse_from_bytes(&e.data)
                        .map_err(|x| RaftError::Raft(x.to_string()))
                        .and_then(|cc| g.raw.apply_conf_change(&cc).map_err(RaftError::from))
                } else {
                    ConfChangeV2::parse_from_bytes(&e.data)
                        .map_err(|x| RaftError::Raft(x.to_string()))
                        .and_then(|cc| g.raw.apply_conf_change(&cc).map_err(RaftError::from))
                };
                match cs {
                    Ok(cs) => {
                        self.note_peers(&cs);
                        // Judged on the last change of the batch: a removal
                        // and a re-add can apply together.
                        removed = (!cs.voters.is_empty() && !in_conf(&cs, self.inner.id))
                            .then_some(e.index);
                        g.applied_conf = cs;
                    }
                    Err(x) => {
                        tracing::warn!(group = gid, error = %x, "raft: membership change not applied");
                    }
                }
                g.applied = e.index;
                g.applied_term = e.term;
                self.inner.observer.on_apply(gid, e.index, e.term);
                resolve(&mut g, e.index, e.term, true);
            } else {
                run.push(e);
            }
        }
        self.apply_run(&mut g, &mut run)?;
        let applied = g.applied;
        if g.raw.raft.raft_log.applied < applied {
            g.raw.advance_apply_to(applied);
        }
        let mut i = 0;
        while i < g.waiting.len() {
            if g.waiting[i].0 <= applied {
                let (_, r) = g.waiting.swap_remove(i);
                r.slot.set(Ok(g.sm.query(&r.query)));
            } else {
                i += 1;
            }
        }
        c.cell.progress.set(g.raw.raft.raft_log.committed, applied);
        if let Some(at) = removed {
            fail_pending(&mut g, 0, &RaftError::Unknown);
            fail_reads(&mut g, &RaftError::NoGroup(gid));
            for (_, r) in g.waiting.drain(..) {
                r.slot.set(Err(RaftError::NoGroup(gid)));
            }
            drop(g);
            self.write_groups().remove(&gid);
            lock(&self.inner.active).remove(&gid);
            lock(&self.inner.tombstones).insert(gid, at);
            writes.push(GroupWrite {
                group: gid,
                remove: Some(at),
                ..GroupWrite::default()
            });
            c.cell.progress.close();
            return Ok(());
        }
        // Volatile state machines: keep a snapshot in the log and trim it.
        if !g.sm.durable()
            && g.applied >= g.raw.store().snapshot_index() + self.inner.cfg.compact_after
        {
            let snap = make_snapshot(&g)?;
            g.raw.mut_store().checkpoint(snap.clone());
            writes.push(GroupWrite {
                group: gid,
                snapshot: Some(SnapshotWrite::Checkpoint(snap)),
                ..GroupWrite::default()
            });
        }
        if g.raw.store().take_wanted() {
            let snap = make_snapshot(&g)?;
            g.raw.mut_store().set_cached(snap);
        }
        if g.raw.has_ready() {
            dirty.insert(gid);
        }
        Ok(())
    }

    fn apply_run(&self, g: &mut Group, run: &mut Vec<Entry>) -> Result<(), RaftError> {
        if run.is_empty() {
            return Ok(());
        }
        let skip = g.skip_until;
        let data: Vec<Entry> = run
            .iter()
            .filter(|e| !e.data.is_empty() && e.index > skip)
            .cloned()
            .collect();
        if !data.is_empty() {
            g.sm.apply(&data)?;
        }
        for e in run.drain(..) {
            g.applied = e.index;
            g.applied_term = e.term;
            self.inner.observer.on_apply(g.id, e.index, e.term);
            resolve(g, e.index, e.term, false);
        }
        Ok(())
    }

    /// Step 3's sends: one frame per destination node.
    fn flush(
        &self,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
        ctrl: &mut BTreeMap<NodeId, Vec<Item>>,
        now: Instant,
    ) {
        if out.is_empty() && ctrl.is_empty() {
            return;
        }
        let mut all = std::mem::take(out);
        // Quiesce notices go after the messages of the same turn, so a
        // follower sees the last heartbeat (and its commit index) first.
        for (to, items) in std::mem::take(ctrl) {
            all.entry(to).or_default().extend(items);
        }
        for (to, items) in all {
            if to == self.inner.id || to == raft::INVALID_ID {
                continue;
            }
            let (seq, ack_epoch, acks) = {
                let mut peers = lock(&self.inner.peers);
                let p = peers.entry(to).or_insert_with(|| Peer {
                    alive: true,
                    ..Peer::default()
                });
                p.next_seq += 1;
                p.last_sent = Some(now);
                (p.next_seq, p.epoch_seen, p.acks.clone())
            };
            let mut led: Vec<GroupId> = items
                .iter()
                .filter_map(|it| match it {
                    Item::Raft(g, _, m) if leader_msg(m.get_msg_type()) => Some(*g),
                    _ => None,
                })
                .collect();
            led.dedup();
            for gid in led {
                if let Some(cell) = self.cell(gid) {
                    let mut g = lock(&cell.g);
                    if g.raw.raft.state == StateRole::Leader && g.lease.term() == g.raw.raft.term {
                        g.lease.on_send(to, seq, now);
                    }
                }
            }
            let n = items
                .iter()
                .filter(|it| matches!(it, Item::Raft(..)))
                .count();
            let frame = Frame {
                from: self.inner.id,
                to,
                epoch: self.inner.epoch,
                seq,
                ack_epoch,
                acks,
                items,
            };
            bump(&self.inner.counters.frames_sent, 1);
            bump(&self.inner.counters.messages_sent, n as u64);
            self.inner.transport.send(to, frame.encode());
        }
    }
}

fn make_snapshot(g: &Group) -> Result<Snapshot, RaftError> {
    let mut snap = Snapshot::default();
    snap.data = g.sm.snapshot()?.into();
    let mut meta = SnapshotMetadata::default();
    meta.index = g.applied;
    meta.term = g.applied_term;
    meta.set_conf_state(g.applied_conf.clone());
    snap.set_metadata(meta);
    Ok(snap)
}

/// Resolves the proposals at or below `index` once the entry there (with
/// `term`) is applied.
fn resolve(g: &mut Group, index: u64, term: u64, conf: bool) {
    while g.pending.front().is_some_and(|p| p.index <= index) {
        let Some(mut p) = g.pending.pop_front() else {
            break;
        };
        if p.index < index || p.term != term {
            p.waiter.failed(p.index, RaftError::Dropped);
        } else if p.conf != conf {
            // raft-rs turns a membership change it can't take (another is
            // pending) into an empty entry.
            p.waiter
                .failed(p.index, RaftError::Busy("a membership change is pending"));
        } else {
            p.waiter.applied(index);
        }
    }
}

/// Fails every pending proposal from `index` on (0: all).
fn fail_pending(g: &mut Group, index: u64, err: &RaftError) {
    let keep = g.pending.iter().take_while(|p| p.index < index).count();
    for mut p in g.pending.drain(keep..) {
        p.waiter.failed(p.index, err.clone());
    }
}

fn fail_reads(g: &mut Group, err: &RaftError) {
    for (_, r) in std::mem::take(&mut g.reads) {
        r.slot.set(Err(err.clone()));
    }
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// How long a turn may wait at most (for tests that drive turns by hand).
#[must_use]
pub fn max_turn_wait(cfg: &RaftConfig) -> Duration {
    cfg.tick.min(cfg.liveness_interval)
}

#[cfg(test)]
mod tests;
