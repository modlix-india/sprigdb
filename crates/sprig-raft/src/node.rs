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
    /// Time the log writer spent in synced writes and syncs, in
    /// microseconds (the log writer thread only).
    pub sync_micros: u64,
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
    /// Snapshots this node received but could not restore: each was
    /// refused (nothing of it kept), and its group waits for the leader to
    /// send one again.
    pub snapshot_restore_failures: u64,
    /// Snapshots built into files off the driver thread.
    pub snapshots_built: u64,
    /// Snapshot bytes sent in chunks.
    pub snapshot_bytes_sent: u64,
    /// Snapshot bytes received in chunks.
    pub snapshot_bytes_received: u64,
}

#[derive(Default)]
struct Counters {
    turns: AtomicU64,
    writes: AtomicU64,
    syncs: AtomicU64,
    sync_micros: AtomicU64,
    frames_sent: AtomicU64,
    frames_received: AtomicU64,
    messages_sent: AtomicU64,
    group_ticks: AtomicU64,
    lease_reads: AtomicU64,
    index_reads: AtomicU64,
    snapshot_restore_failures: AtomicU64,
    snapshots_built: AtomicU64,
    snapshot_bytes_sent: AtomicU64,
    snapshot_bytes_received: AtomicU64,
}

fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

/// Microseconds since `t`.
fn micros(t: Instant) -> u64 {
    u64::try_from(t.elapsed().as_micros()).unwrap_or(u64::MAX)
}

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
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

/// Where a read's answer arrives ([`Node::read`], [`Node::read_barrier`],
/// [`Node::leader_barrier`]).
pub type ReadSlot = Slot<Result<Vec<u8>, RaftError>>;

struct ReadReq {
    slot: ReadSlot,
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
    /// The cell's [`Cell::lease_until`].
    lease_until: Arc<AtomicU64>,
    quiesced: bool,
    idle: u32,
    role: (u64, StateRole),
    /// Ticks each follower has waited for a snapshot.
    snap_wait: BTreeMap<NodeId, u32>,
}

impl Group {
    /// The lease is given up (a leader transfer) or gone (the role or the
    /// membership changed): nobody may read under what was published.
    fn unpublish_lease(&self) {
        self.lease_until.store(0, Ordering::Release);
    }
}

struct Cell {
    g: Mutex<Group>,
    progress: Arc<Progress>,
    /// Until when this replica may answer reads as the group's leader
    /// without asking anyone ([`Node::leader_barrier`]), as nanoseconds on
    /// the node's clock after the node started; 0: not known to. Written
    /// under `g`: set when the lease is found to hold with an entry of the
    /// leader's own term applied, cleared when the lease is given up.
    /// Readers check it without taking `g`.
    lease_until: Arc<AtomicU64>,
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

/// One log write handed to the log writer thread.
struct LogJob {
    seq: u64,
    batch: WriteBatch,
    sync: bool,
}

/// A log write the writer thread hasn't finished, and what waits on it:
/// the readies to mark persisted, the messages that may leave only once
/// it is durable, and groups whose creation it makes durable.
struct InFlight {
    seq: u64,
    done: Vec<Collected>,
    out: BTreeMap<NodeId, Vec<Item>>,
    created: Vec<Slot<Result<(), RaftError>>>,
}

#[derive(Default)]
struct WriterState {
    /// The last job written (and synced, if any job in its group asked).
    persisted: u64,
    failed: Option<RaftError>,
}

/// A write group of the pipelined log writer on its way to persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncWait {
    /// Written; waits for a sync that begins after now.
    Sync,
    /// A sync (by number) that began after it was written is running.
    Syncing(u64),
    /// Durable, or it needed no sync.
    Done,
}

/// The pipelined log writer's write groups, in write order, and its syncs.
#[derive(Default)]
struct SyncQueue {
    /// (the group's last job, its state), oldest first.
    groups: VecDeque<(u64, SyncWait)>,
    /// Syncs begun.
    begun: u64,
    /// No more groups come: the sync workers finish what waits and exit.
    stop: bool,
    /// A sync failed: nothing more is synced.
    failed: bool,
}

/// Log writes on a thread of their own ([`RaftConfig::log_writer_thread`]):
/// the loop hands each pass's batch over and goes on stepping groups while
/// the write and sync run; jobs queued meanwhile go out together, with one
/// sync. With a store that overlaps syncs ([`LogStore::sync_pipeline`]) the
/// thread only writes, and sync workers beside it sync what was written.
#[derive(Default)]
struct Writer {
    tx: Mutex<Option<std::sync::mpsc::Sender<LogJob>>>,
    /// Joined by the driver thread when it stops, so nothing is written
    /// after it returned.
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// The last job handed over.
    next: AtomicU64,
    state: Mutex<WriterState>,
    cv: Condvar,
    in_flight: Mutex<VecDeque<InFlight>>,
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
    writer: Writer,
    /// Snapshot files and transfers ([`crate::snapfile`]).
    snaps: crate::snapfile::Snaps,
    /// [`RaftConfig::snapshot_bytes_per_second`], as changed since.
    snapshot_rate: AtomicU64,
    next_snapshot: AtomicU64,
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
    /// The group was removed from this node while applying.
    removed: bool,
}

fn is_conf(e: &Entry) -> bool {
    matches!(
        e.get_entry_type(),
        EntryType::EntryConfChange | EntryType::EntryConfChangeV2
    )
}

/// Snapshots built off the driver at once, at most, per node.
const MAX_SNAPSHOT_BUILDS: usize = 2;

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
        let rate = opts.config.snapshot_bytes_per_second;
        let snaps = if opts.config.snapshot_off_driver {
            crate::snapfile::Snaps::open(opts.config.snapshot_dir.clone().unwrap_or_else(|| {
                // One folder per node in this process (tests run many nodes
                // with the same ids side by side).
                static NEXT: AtomicU64 = AtomicU64::new(0);
                std::env::temp_dir().join(format!(
                    "sprig-raft-snaps-{}-{}-{}",
                    std::process::id(),
                    opts.id,
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ))
            }))
        } else {
            crate::snapfile::Snaps::default()
        };
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
            writer: Writer::default(),
            snaps,
            snapshot_rate: AtomicU64::new(rate),
            next_snapshot: AtomicU64::new(1),
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
                let applied = sm.applied_index();
                // A snapshot installed just before a crash: its record is
                // durable, but the state machine may not have made the
                // restore durable. The record still holds the data (a
                // checkpoint's record has none, and comes only after the
                // state machine made everything up to it durable). A
                // snapshot restored from a file was made durable before its
                // record was written; the store's index can be below the
                // snapshot's only by entries without data.
                if applied < meta.index && crate::snapfile::parse_token(&rg.snapshot.data).is_some()
                {
                    meta.index
                } else if applied < meta.index
                    && !rg.snapshot.data.is_empty()
                    && (meta.index, meta.term) != (INIT_INDEX, INIT_TERM)
                {
                    // (The group's creation record holds the empty state
                    // the group started from: a store that lost its
                    // unflushed applies since replays them from the log,
                    // and restoring that record would only hand the
                    // state's other parts, the stream log's, an empty
                    // unit in place of what they kept.)
                    sm.restore(meta.index, &rg.snapshot.data)?;
                    meta.index
                } else {
                    applied
                }
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
        let lease_until = Arc::new(AtomicU64::new(0));
        let cell = Arc::new(Cell {
            lease_until: Arc::clone(&lease_until),
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
                lease_until,
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
        self.propose_in_term(group, None, data, durability, waiter)
    }

    /// [`Node::propose_with`], accepted only while this replica leads
    /// `group` in `term` (when given): a caller that decided `data` on what
    /// it read as the leader of that term must not have it appended in a
    /// later one, after entries it has not seen.
    ///
    /// # Errors
    /// As for [`Node::propose_with`]; [`RaftError::NotLeader`] also when
    /// the replica leads in another term.
    pub fn propose_in_term(
        &self,
        group: GroupId,
        term: Option<u64>,
        data: Vec<u8>,
        durability: Durability,
        waiter: impl FnOnce(u64) -> Box<dyn Waiter>,
    ) -> Result<(u64, u64), RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let mut g = self.leading(&cell)?;
        if term.is_some_and(|t| t != g.raw.raft.term) {
            return Err(RaftError::NotLeader {
                leader: Some(self.inner.id),
            });
        }
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
        if let Some((start, _)) = self.lease_at(&g, now) {
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

    /// The leader's lease at `now`, if it holds: when it started and when
    /// it ends. It holds on a leader that isn't quiesced, has committed an
    /// entry of its own term, and whose frames a quorum accepted recently
    /// enough ([`Lease::valid_until`]).
    fn lease_at(&self, g: &Group, now: Instant) -> Option<(Instant, Instant)> {
        if g.quiesced
            || g.raw.raft.state != StateRole::Leader
            || !g.raw.raft.commit_to_current_term()
            || g.lease.term() != g.raw.raft.term
        {
            return None;
        }
        g.lease.valid_until(
            &g.applied_conf,
            self.inner.id,
            now,
            self.inner.cfg.usable_lease(),
        )
    }

    /// `t` on the scale of [`Cell::lease_until`] (never 0).
    fn lease_stamp(&self, t: Instant) -> u64 {
        u64::try_from(t.saturating_duration_since(self.inner.started).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1)
    }

    /// Whether this replica is known to hold `group`'s leader lease right
    /// now, without taking the group's lock: a lease published by an
    /// earlier [`Node::leader_barrier`] that hasn't run out or been given
    /// up.
    #[must_use]
    pub fn lease_holds(&self, group: GroupId) -> bool {
        let Some(cell) = self.cell(group) else {
            return false;
        };
        let until = cell.lease_until.load(Ordering::Acquire);
        until != 0 && self.lease_stamp(self.inner.clock.now()) < until
    }

    /// Before this replica answers a read from its own state as `group`'s
    /// leader: `None` when its lease holds (no other replica can have
    /// committed as leader, and this one has applied an entry of its own
    /// term, so its state holds every acknowledged write); otherwise a slot
    /// that is set once a quorum has confirmed this leader (`ReadIndex`)
    /// and the confirmed index is applied here, or fails when this replica
    /// stops leading. A leader cut off from its followers gets no answer:
    /// the caller gives up after a wait and reports a retryable error.
    ///
    /// A lease found to hold is published until its end, so the reads that
    /// follow check it without the group's lock ([`Node::lease_holds`]),
    /// and the group counts as busy (it doesn't quiesce under reads).
    ///
    /// # Errors
    /// [`RaftError::NotLeader`], [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn leader_barrier(&self, group: GroupId) -> Result<Option<ReadSlot>, RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        let now = self.inner.clock.now();
        let until = cell.lease_until.load(Ordering::Acquire);
        if until != 0 && self.lease_stamp(now) < until {
            bump(&self.inner.counters.lease_reads, 1);
            return Ok(None);
        }
        {
            let mut g = self.leading(&cell)?;
            if let Some((start, end)) = self.lease_at(&g, now)
                && g.applied >= g.raw.raft.raft_log.committed
            {
                g.lease_until
                    .store(self.lease_stamp(end), Ordering::Release);
                g.idle = 0;
                self.inner
                    .observer
                    .on_lease_read(group, g.raw.raft.term, start, now);
                bump(&self.inner.counters.lease_reads, 1);
                return Ok(None);
            }
        }
        self.read(group, Vec::new()).map(Some)
    }

    /// A read barrier on any replica: the slot is set once this replica
    /// has applied everything the group's leader had committed when the
    /// barrier was asked (a `ReadIndex` the leader confirms with a quorum,
    /// or the leader's own lease). After it, a read of this replica's state
    /// sees every write acknowledged before the barrier started.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`] with no leader known here (retry),
    /// [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn read_barrier(
        &self,
        group: GroupId,
    ) -> Result<Slot<Result<Vec<u8>, RaftError>>, RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RaftError::Closed);
        }
        let mut g = lock(&cell.g);
        if g.raw.raft.state == StateRole::Leader {
            drop(g);
            return self.read(group, Vec::new());
        }
        if g.raw.raft.leader_id == raft::INVALID_ID {
            // raft-rs drops a follower's ReadIndex with no leader to send
            // it to: say so, so the caller retries. A quiesced group ticks
            // again meanwhile, so it learns (or elects) a leader.
            self.wake_group(&mut g);
            drop(g);
            self.mark_dirty(group);
            return Err(RaftError::NotLeader { leader: None });
        }
        bump(&self.inner.counters.index_reads, 1);
        let slot = Slot::new();
        let ctx = self.inner.next_ctx.fetch_add(1, Ordering::Relaxed);
        g.raw.read_index(ctx.to_le_bytes().to_vec());
        g.reads.insert(
            ctx,
            ReadReq {
                slot: slot.clone(),
                query: Vec::new(),
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
        g.unpublish_lease();
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

    /// Asks `group`'s leader to hand its leadership to this replica (a
    /// transfer, sent through the leader, which waits until this replica's
    /// log has caught up). Returns at once; nothing to do if this replica
    /// already leads.
    ///
    /// # Errors
    /// [`RaftError::NotLeader`] with no leader known here,
    /// [`RaftError::NoGroup`], [`RaftError::Closed`].
    pub fn request_leadership(&self, group: GroupId) -> Result<(), RaftError> {
        let cell = self.cell(group).ok_or(RaftError::NoGroup(group))?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RaftError::Closed);
        }
        let mut g = lock(&cell.g);
        if g.raw.raft.state == StateRole::Leader {
            return Ok(());
        }
        if g.raw.raft.leader_id == raft::INVALID_ID {
            return Err(RaftError::NotLeader { leader: None });
        }
        // A follower's transfer request goes to the leader, which starts
        // the transfer to this replica (see `on_frame`: its lease ends).
        g.raw.transfer_leader(self.inner.id);
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

    /// The entries `from..to` of `group` in this node's log store (the
    /// caller asks for committed indexes only). Doesn't take the group's
    /// lock: callers may hold locks the apply path waits for.
    ///
    /// # Errors
    /// The log store could not be read.
    pub fn log_entries(&self, group: GroupId, from: u64, to: u64) -> Result<Vec<Entry>, RaftError> {
        self.inner.store.entries(group, from, to)
    }

    /// The lowest index of `group` in this node's log store. Doesn't take
    /// the group's lock.
    #[must_use]
    pub fn log_first_index(&self, group: GroupId) -> Option<u64> {
        self.inner.store.first_index(group)
    }

    /// On the group's leader: the highest index `peer`'s log is known to
    /// match (`None`: not the leader, or not a member).
    #[must_use]
    pub fn matched(&self, group: GroupId, peer: NodeId) -> Option<u64> {
        let cell = self.cell(group)?;
        let g = lock(&cell.g);
        if g.raw.raft.state != StateRole::Leader {
            return None;
        }
        g.raw.raft.prs().get(peer).map(|p| p.matched)
    }

    /// The voters this node starts `group` with if it doesn't hold it yet
    /// ([`RaftConfig::start_voters`]): `None` when this node isn't one of
    /// them, or the unit's placement isn't known here.
    #[must_use]
    pub fn start_voters(&self, group: GroupId) -> Option<Vec<NodeId>> {
        self.inner.cfg.start_voters(self.inner.id, group)
    }

    /// Whether units are placed per unit here ([`RaftConfig::placement`]).
    #[must_use]
    pub fn placed(&self) -> bool {
        self.inner.cfg.placement.is_some()
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

    /// Removes this node's replica of `group` on its own, as a membership
    /// change that removed it would: its pending proposals and reads fail,
    /// its log is deleted and a tombstone at its applied index kept, so no
    /// message (nor the placement lookup) brings it back; only
    /// [`Node::create_group`] does. For a unit whose owner is gone (a
    /// deleted tenant), which every member drops this way. Returns whether
    /// this node held it.
    ///
    /// # Errors
    /// [`RaftError::Closed`].
    pub fn drop_group(&self, group: GroupId) -> Result<bool, RaftError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(RaftError::Closed);
        }
        let Some(cell) = self.cell(group) else {
            return Ok(false);
        };
        let mut g = lock(&cell.g);
        let at = g.applied;
        let (term, role) = g.role;
        self.inner.snaps.forget(group);
        fail_pending(&mut g, 0, &RaftError::NoGroup(group));
        fail_reads(&mut g, &RaftError::NoGroup(group));
        for (_, r) in g.waiting.drain(..) {
            r.slot.set(Err(RaftError::NoGroup(group)));
        }
        self.write_groups().remove(&group);
        lock(&self.inner.active).remove(&group);
        lock(&self.inner.tombstones).insert(group, at);
        lock(&self.inner.inbox).writes.push(GroupWrite {
            group,
            remove: Some(at),
            ..GroupWrite::default()
        });
        drop(g);
        cell.progress.close();
        self.drop_state_when_durable(group, Arc::clone(&cell));
        if role == StateRole::Leader {
            let now = self.inner.clock.now();
            self.inner.observer.on_lease_end(group, term, now);
            self.inner.observer.on_role(group, term, false);
        }
        self.inner.wake.notify_one();
        Ok(true)
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
            sync_micros: ld(&c.sync_micros),
            frames_sent: ld(&c.frames_sent),
            frames_received: ld(&c.frames_received),
            messages_sent: ld(&c.messages_sent),
            group_ticks: ld(&c.group_ticks),
            lease_reads: ld(&c.lease_reads),
            index_reads: ld(&c.index_reads),
            groups: self.read_groups().len() as u64,
            active: lock(&self.inner.active).len() as u64,
            snapshot_restore_failures: ld(&c.snapshot_restore_failures),
            snapshots_built: ld(&c.snapshots_built),
            snapshot_bytes_sent: ld(&c.snapshot_bytes_sent),
            snapshot_bytes_received: ld(&c.snapshot_bytes_received),
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
        drop(ib);
        // The snapshot sender stops at its next chunk.
        self.inner.snaps.cv.notify_all();
        if self.inner.cfg.snapshot_dir.is_none() && self.inner.cfg.snapshot_off_driver {
            let _ = std::fs::remove_dir_all(&self.inner.snaps.dir);
        }
        // The writer thread finishes what it was handed, then exits.
        drop(lock(&self.inner.writer.tx).take());
        for f in lock(&self.inner.writer.in_flight).drain(..) {
            for s in f.created {
                s.set(Err(RaftError::Closed));
            }
        }
        self.inner.writer.cv.notify_all();
    }

    /// Runs the driver on a thread of its own (with its own current-thread
    /// runtime) until [`Node::close`].
    ///
    /// # Errors
    /// The thread could not be started.
    pub fn spawn(&self) -> std::io::Result<std::thread::JoinHandle<()>> {
        if self.inner.cfg.log_writer_thread {
            self.start_writer()?;
        }
        let node = self.clone();
        std::thread::Builder::new()
            .name(format!("sprig-raft-{}", self.inner.id))
            .spawn(move || {
                let maint = node.start_maintenance();
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
                // The node is closed: the writer thread finishes what it
                // was handed and exits.
                let writer = lock(&node.inner.writer.thread).take();
                if let Some(t) = writer {
                    let _ = t.join();
                }
                if let Some(t) = maint {
                    let _ = t.join();
                }
            })
    }

    /// The log store's housekeeping ([`LogStore::maintain`]: files the log
    /// no longer needs are deleted) every ten seconds, on a thread of its
    /// own, until the node closes. Simulations drive [`Node::run`] without
    /// it.
    fn start_maintenance(&self) -> Option<std::thread::JoinHandle<()>> {
        let node = self.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("sprig-raft-maint-{}", self.inner.id))
            .spawn(move || {
                let mut last = node.stats();
                loop {
                    for _ in 0..100 {
                        if node.is_closed() {
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    node.inner.store.maintain();
                    let now = node.stats();
                    if now.writes != last.writes {
                        let syncs = now.syncs - last.syncs;
                        tracing::debug!(
                            node = node.inner.id,
                            writes = now.writes - last.writes,
                            syncs,
                            sync_avg_us = (now.sync_micros - last.sync_micros) / syncs.max(1),
                            turns = now.turns - last.turns,
                            "raft log in the last 10 s"
                        );
                    }
                    last = now;
                }
            });
        match spawned {
            Ok(t) => Some(t),
            Err(e) => {
                tracing::warn!(node = self.inner.id, error = %e, "raft log housekeeping not started: trimmed log files stay on disk");
                None
            }
        }
    }

    fn has_work(&self) -> bool {
        let work = {
            let ib = lock(&self.inner.inbox);
            !ib.frames.is_empty() || !ib.dirty.is_empty() || !ib.writes.is_empty()
        };
        work || self.persisted_waiting()
    }

    /// Starts the log writer thread ([`RaftConfig::log_writer_thread`]).
    fn start_writer(&self) -> std::io::Result<()> {
        let (tx, rx) = std::sync::mpsc::channel::<LogJob>();
        let node = self.clone();
        let t = std::thread::Builder::new()
            .name(format!("sprig-raft-log-{}", self.inner.id))
            .spawn(move || node.write_loop(&rx))?;
        *lock(&self.inner.writer.tx) = Some(tx);
        *lock(&self.inner.writer.thread) = Some(t);
        Ok(())
    }

    /// The log writer thread: writes jobs in order; jobs that queued up
    /// while the last write ran go out together, synced once.
    fn write_loop(&self, rx: &std::sync::mpsc::Receiver<LogJob>) {
        match self.inner.store.sync_pipeline() {
            0 => self.write_loop_serial(rx),
            depth => self.write_loop_pipelined(rx, depth),
        }
    }

    /// [`Node::write_loop`] with each write synced before the next begins.
    fn write_loop_serial(&self, rx: &std::sync::mpsc::Receiver<LogJob>) {
        while let Ok(first) = rx.recv() {
            let mut jobs = vec![first];
            while let Ok(j) = rx.try_recv() {
                jobs.push(j);
            }
            let sync = jobs.iter().any(|j| j.sync);
            let n = jobs.len();
            let batches: Vec<&WriteBatch> = jobs.iter().map(|j| &j.batch).collect();
            let began = Instant::now();
            let res = match self.inner.store.write_all(&batches, sync) {
                Ok(()) => {
                    bump(&self.inner.counters.writes, n as u64);
                    Ok(())
                }
                Err((done, e)) => {
                    bump(&self.inner.counters.writes, done as u64);
                    Err(e)
                }
            };
            if sync && res.is_ok() {
                bump(&self.inner.counters.syncs, 1);
                bump(&self.inner.counters.sync_micros, micros(began));
            }
            let failed = res.is_err();
            {
                let mut st = lock(&self.inner.writer.state);
                match res {
                    Ok(()) => st.persisted = jobs[n - 1].seq,
                    Err(e) => st.failed = Some(e),
                }
            }
            self.inner.writer.cv.notify_all();
            self.inner.wake.notify_one();
            if failed {
                return;
            }
        }
    }

    /// [`Node::write_loop`] with syncs overlapped: this thread only writes
    /// ([`LogStore::write_unsynced`]), and up to `depth` sync workers each
    /// sync everything written before they begin
    /// ([`LogStore::sync_written`], which returns only once everything
    /// written before the call is durable). A write group counts as
    /// persisted once a sync call that began after it was written has
    /// returned and every group before it counts too, so the persisted
    /// mark moves in order over a durable prefix. While syncs run, the
    /// next writes go to the file.
    fn write_loop_pipelined(&self, rx: &std::sync::mpsc::Receiver<LogJob>, depth: usize) {
        let q = (Mutex::new(SyncQueue::default()), Condvar::new());
        std::thread::scope(|s| {
            let mut workers = 0;
            for i in 0..depth {
                let q = &q;
                let spawned = std::thread::Builder::new()
                    .name(format!("sprig-raft-sync-{}-{i}", self.inner.id))
                    .spawn_scoped(s, move || self.sync_worker(q));
                workers += usize::from(spawned.is_ok());
            }
            if workers == 0 {
                tracing::warn!(
                    node = self.inner.id,
                    "raft log: no sync worker started; each write is synced before the next"
                );
                self.write_loop_serial(rx);
                return;
            }
            while let Ok(first) = rx.recv() {
                if lock(&q.0).failed {
                    break;
                }
                let mut jobs = vec![first];
                while let Ok(j) = rx.try_recv() {
                    jobs.push(j);
                }
                let sync = jobs.iter().any(|j| j.sync);
                let n = jobs.len();
                let last = jobs[n - 1].seq;
                let batches: Vec<&WriteBatch> = jobs.iter().map(|j| &j.batch).collect();
                if let Err((done, e)) = self.inner.store.write_unsynced(&batches, sync) {
                    bump(&self.inner.counters.writes, done as u64);
                    self.writer_failed(e);
                    break;
                }
                bump(&self.inner.counters.writes, n as u64);
                let mut g = lock(&q.0);
                let wait = if sync { SyncWait::Sync } else { SyncWait::Done };
                g.groups.push_back((last, wait));
                self.publish_persisted(&mut g);
                drop(g);
                q.1.notify_all();
            }
            lock(&q.0).stop = true;
            q.1.notify_all();
        });
    }

    /// One sync worker of [`Node::write_loop_pipelined`].
    fn sync_worker(&self, q: &(Mutex<SyncQueue>, Condvar)) {
        let mut g = lock(&q.0);
        loop {
            if g.failed {
                return;
            }
            if g.groups.iter().any(|(_, w)| *w == SyncWait::Sync) {
                g.begun += 1;
                let id = g.begun;
                for (_, w) in &mut g.groups {
                    if *w == SyncWait::Sync {
                        *w = SyncWait::Syncing(id);
                    }
                }
                drop(g);
                let began = Instant::now();
                let res = self.inner.store.sync_written();
                g = lock(&q.0);
                match res {
                    Ok(()) => {
                        bump(&self.inner.counters.syncs, 1);
                        bump(&self.inner.counters.sync_micros, micros(began));
                        for (_, w) in &mut g.groups {
                            if *w == SyncWait::Syncing(id) {
                                *w = SyncWait::Done;
                            }
                        }
                        self.publish_persisted(&mut g);
                    }
                    Err(e) => {
                        g.failed = true;
                        self.writer_failed(e);
                    }
                }
                q.1.notify_all();
                continue;
            }
            if g.stop {
                return;
            }
            g = q.1.wait(g).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Moves the persisted mark over the done groups at the front.
    fn publish_persisted(&self, g: &mut SyncQueue) {
        let mut upto = None;
        while let Some(&(seq, SyncWait::Done)) = g.groups.front() {
            g.groups.pop_front();
            upto = Some(seq);
        }
        if let Some(seq) = upto {
            {
                let mut st = lock(&self.inner.writer.state);
                st.persisted = st.persisted.max(seq);
            }
            self.inner.writer.cv.notify_all();
            self.inner.wake.notify_one();
        }
    }

    /// A log write or sync failed: the loop stops the node.
    fn writer_failed(&self, e: RaftError) {
        lock(&self.inner.writer.state).failed = Some(e);
        self.inner.writer.cv.notify_all();
        self.inner.wake.notify_one();
    }

    /// Whether a write handed to the writer thread is done and waits for
    /// the loop.
    fn persisted_waiting(&self) -> bool {
        let st = lock(&self.inner.writer.state);
        st.failed.is_some()
            || lock(&self.inner.writer.in_flight)
                .front()
                .is_some_and(|f| f.seq <= st.persisted)
    }

    /// Blocks until the writer thread wrote job `seq` (or failed, or the
    /// node closed).
    fn wait_persisted(&self, seq: u64) {
        let mut st = lock(&self.inner.writer.state);
        while st.persisted < seq && st.failed.is_none() && !self.is_closed() {
            st = self
                .inner
                .writer
                .cv
                .wait_timeout(st, Duration::from_millis(100))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Step 4 for the writes the writer thread finished: groups created,
    /// messages that waited for durability sent, readies marked persisted.
    fn take_persisted(
        &self,
        now: Instant,
        writes: &mut Vec<GroupWrite>,
        dirty: &mut BTreeSet<GroupId>,
    ) -> Result<(), RaftError> {
        let persisted = {
            let st = lock(&self.inner.writer.state);
            if let Some(e) = &st.failed {
                return Err(e.clone());
            }
            st.persisted
        };
        loop {
            let f = {
                let mut q = lock(&self.inner.writer.in_flight);
                if q.front().is_some_and(|f| f.seq <= persisted) {
                    q.pop_front()
                } else {
                    None
                }
            };
            let Some(mut f) = f else { break };
            for s in f.created.drain(..) {
                s.set(Ok(()));
            }
            self.flush(&mut f.out, &mut BTreeMap::new(), now);
            for c in f.done {
                self.finish(c, writes, dirty)?;
            }
        }
        Ok(())
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
        let Inbox {
            frames,
            mut dirty,
            mut writes,
            mut created,
        } = std::mem::take(&mut *lock(&self.inner.inbox));
        let mut out: BTreeMap<NodeId, Vec<Item>> = BTreeMap::new();
        let mut early: BTreeMap<NodeId, Vec<Item>> = BTreeMap::new();
        let mut ctrl: BTreeMap<NodeId, Vec<Item>> = BTreeMap::new();
        let threaded = lock(&self.inner.writer.tx).is_some();
        if threaded {
            self.take_persisted(now, &mut writes, &mut dirty)?;
        }
        bump(&self.inner.counters.frames_received, frames.len() as u64);
        for f in &frames {
            self.on_frame(f, now, &mut dirty, &mut out);
        }
        // Groups the frames created: their first state goes in this turn's
        // batch, ahead of anything the frames made them write.
        {
            let mut ib = lock(&self.inner.inbox);
            writes.append(&mut ib.writes);
            created.append(&mut ib.created);
            dirty.append(&mut ib.dirty);
        }
        if live {
            self.check_liveness(now, &mut dirty);
        }
        if ticks > 0 {
            self.tick_groups(ticks, &mut dirty, &mut ctrl);
        }
        if threaded {
            // What the frames answered doesn't wait for a log write.
            self.flush(&mut out, &mut BTreeMap::new(), now);
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
                    && let Some(c) =
                        self.collect(cell, &mut batch, &mut early, &mut out, &mut need_sync)
                {
                    done.push(c);
                }
            }
            // The leader's appends go out while its own sync runs.
            self.flush(&mut early, &mut BTreeMap::new(), now);
            // Committed entries are applied (and their proposers answered)
            // before this pass's write: a ready's committed entries are
            // already durable here (raft-rs hands out only those at or
            // below the persisted index), so they need not wait a sync for
            // entries that came after them. A snapshot's group keeps the
            // old order: everything after its write.
            for c in &mut done {
                if c.snapshot_index.is_none() {
                    self.apply_committed(c, &mut writes)?;
                }
            }
            if threaded {
                self.hand_over(
                    now,
                    batch,
                    need_sync,
                    done,
                    &mut out,
                    &mut created,
                    &mut writes,
                    &mut dirty,
                )?;
                self.flush(&mut BTreeMap::new(), &mut ctrl, now);
                // The next pass starts when the write is done (or new
                // work arrives): the loop doesn't wait for the sync.
                break;
            }
            if !self.write_on_loop(&batch, need_sync).await? {
                return Ok(());
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

    /// Step 3 on the loop itself: writes the batch, synced if asked.
    /// False when the node closed during a simulated slow sync.
    async fn write_on_loop(&self, batch: &WriteBatch, need_sync: bool) -> Result<bool, RaftError> {
        if batch.is_empty() {
            return Ok(true);
        }
        if need_sync {
            // A slow disk (simulation): a crash meanwhile loses the batch,
            // and nothing waiting on it was answered.
            let d = self.inner.store.sync_delay();
            if !d.is_zero() {
                tokio::time::sleep(d).await;
            }
            if self.is_closed() {
                return Ok(false);
            }
            bump(&self.inner.counters.syncs, 1);
        }
        self.inner.store.write(batch, need_sync)?;
        bump(&self.inner.counters.writes, 1);
        Ok(true)
    }

    /// Hands a pass's write to the writer thread. Readies whose writes are
    /// all done (nothing to write, nothing in flight) finish at once. A
    /// snapshot's group applies only after its write, as on the loop.
    #[allow(clippy::too_many_arguments)]
    fn hand_over(
        &self,
        now: Instant,
        batch: WriteBatch,
        need_sync: bool,
        done: Vec<Collected>,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
        created: &mut Vec<Slot<Result<(), RaftError>>>,
        writes: &mut Vec<GroupWrite>,
        dirty: &mut BTreeSet<GroupId>,
    ) -> Result<(), RaftError> {
        let w = &self.inner.writer;
        let snapshot = done.iter().any(|c| c.snapshot_index.is_some());
        let seq = if batch.is_empty() {
            w.next.load(Ordering::Acquire)
        } else {
            let seq = w.next.fetch_add(1, Ordering::AcqRel) + 1;
            let sent = lock(&w.tx).as_ref().is_some_and(|tx| {
                tx.send(LogJob {
                    seq,
                    batch,
                    sync: need_sync,
                })
                .is_ok()
            });
            if !sent {
                return Err(RaftError::Closed);
            }
            seq
        };
        lock(&w.in_flight).push_back(InFlight {
            seq,
            done,
            out: std::mem::take(out),
            created: std::mem::take(created),
        });
        if snapshot {
            self.wait_persisted(seq);
        }
        self.take_persisted(now, writes, dirty)
    }

    #[allow(clippy::too_many_lines)] // one pass over a frame's items, kind by kind
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
            // Frames of the snapshot sender (seq 0) carry nothing a lease
            // waits for.
            if frame.seq != 0 {
                peer.acks.push(frame.seq);
            }
        }
        // A restarted node votes for no one until a leader's lease it may
        // have supported before the crash has run out.
        let grace =
            now.saturating_duration_since(self.inner.started) < self.inner.cfg.min_election();
        let mut responded = Vec::new();
        for it in frame.items {
            match it {
                Item::SnapChunk {
                    group,
                    id,
                    offset,
                    total,
                    data,
                } => {
                    let n = self
                        .inner
                        .snaps
                        .receive(frame.from, group, id, offset, total, &data);
                    bump(&self.inner.counters.snapshot_bytes_received, n as u64);
                }
                Item::Raft(gid, sender_applied, m) => {
                    // A frame speaks for its sender only.
                    if m.from != frame.from {
                        continue;
                    }
                    let t = m.get_msg_type();
                    // A snapshot whose file didn't arrive whole is dropped:
                    // the leader sends it again after its retry window.
                    if t == MessageType::MsgSnapshot
                        && let Some((id, len)) =
                            crate::snapfile::parse_token(&m.get_snapshot().data)
                        && !self.inner.snaps.has(gid, id, len)
                    {
                        tracing::debug!(
                            group = gid,
                            id,
                            "raft: a snapshot arrived without its whole file; dropped"
                        );
                        continue;
                    }
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
                    if t == MessageType::MsgTransferLeader
                        && g.raw.raft.state == StateRole::Leader
                        && m.from != self.inner.id
                    {
                        // A follower asks for the leadership: as in
                        // `transfer_leader`, the lease ends first, so no
                        // lease read here overlaps the next leader's writes.
                        g.lease.disable();
                        g.unpublish_lease();
                        let term = g.raw.raft.term;
                        self.inner.observer.on_lease_end(gid, term, now);
                    }
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
                        {
                            // The leader quiesces only with every entry through
                            // `index` committed and matched on every follower, so
                            // a follower whose log ends there in the leader's term
                            // holds the leader's entries: it may commit them now.
                            // Otherwise it would learn of the last commits only at
                            // the group's next activity, and apply them late.
                            if g.raw.raft.raft_log.committed < index {
                                g.raw.raft.raft_log.commit_to(index);
                                dirty.insert(group);
                            }
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
        if let Some(voters) = self.inner.cfg.start_voters(self.inner.id, gid)
            && !lock(&self.inner.tombstones).contains_key(&gid)
        {
            // A fixed membership (the cluster's, or the unit's placement):
            // every member starts the group the same way, whoever hears of
            // it first.
            return match self.create_group(gid, &voters) {
                Ok(_) => self.cell(gid),
                Err(e) => {
                    tracing::warn!(group = gid, error = %e, "raft: could not create a group");
                    None
                }
            };
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
        if was_leader && role.1 != StateRole::Leader {
            self.release_snapshot_file(g);
        }
        if was_leader {
            self.inner
                .observer
                .on_lease_end(g.id, g.role.0, self.inner.clock.now());
        }
        g.role = role;
        g.unpublish_lease();
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
        // Every follower holds what the snapshot holds: its file can go.
        let needed = g.raw.store().cached_data().map(|(_, at)| at);
        let covered =
            needed.is_none_or(|at| g.raw.raft.prs().iter().all(|(_, pr)| pr.matched >= at));
        if waiting.is_empty() && covered {
            self.release_snapshot_file(g);
        }
        // A large snapshot takes longer to send and install: allow the time
        // to restore it at 20 MiB/s on top (a tick per 2 MiB at 100 ms
        // ticks), so it isn't declared lost and sent again while it is
        // still being restored.
        let per_tick = (self.inner.cfg.tick.as_secs_f64() * f64::from(20u32 << 20)).max(1.0);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let size_ticks =
            (g.raw.store().cached_len() as f64 / per_tick).min(f64::from(u32::MAX)) as u32;
        let limit = self
            .inner
            .cfg
            .snapshot_retry_ticks
            .saturating_add(size_ticks);
        for id in waiting {
            // Its chunks are still on their way: not lost.
            if self.inner.snaps.sending(id, g.id) {
                g.snap_wait.remove(&id);
                continue;
            }
            let w = g.snap_wait.entry(id).or_insert(0);
            *w += ticks;
            if *w >= limit {
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
        early: &mut BTreeMap<NodeId, Vec<Item>>,
        out: &mut BTreeMap<NodeId, Vec<Item>>,
        need_sync: &mut bool,
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
            let restored = match crate::snapfile::parse_token(&snap.data) {
                Some((id, len)) => self.restore_file(&mut g, meta.index, id, len),
                None => g.sm.restore(meta.index, &snap.data),
            };
            if let Err(e) = restored {
                bump(&self.inner.counters.snapshot_restore_failures, 1);
                tracing::error!(
                    group = gid,
                    index = meta.index,
                    error = %e,
                    "raft: snapshot restore failed; the replica keeps its log and waits for the snapshot again"
                );
                drop(rd);
                self.refuse_snapshot(&mut g);
                return None;
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
        // A leader's appends and heartbeats may go before its own write is
        // durable (its entries count towards a quorum only once persisted
        // here); everything else (votes, responses) waits for the sync.
        for m in rd.take_messages() {
            early
                .entry(m.to)
                .or_default()
                .push(Item::Raft(gid, g.applied, m));
        }
        for m in rd.take_persisted_messages() {
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
            removed: false,
        })
    }

    /// A snapshot this replica could not restore: nothing of the ready that
    /// carried it is kept (its entries, hard state and messages are
    /// dropped, as a crash before its write would). The group's Raft state
    /// starts over from its log as stored, which the snapshot never
    /// replaced, so the replica still needs one: its leader sends it again
    /// once the leader's retry window passes. The state machine may hold
    /// part of the failed restore; the next restore replaces the whole
    /// unit, and until then this replica is behind its leader, so no read
    /// is served from it.
    fn refuse_snapshot(&self, g: &mut Group) {
        if lock(&self.inner.writer.tx).is_some() {
            // Writes of this group's earlier readies finish first: the new
            // Raft state takes every entry its log holds as written.
            self.wait_persisted(self.inner.writer.next.load(Ordering::Acquire));
        }
        let storage = g.raw.store().clone();
        match RawNode::new(&self.raft_config(g.applied), storage, &self.inner.logger) {
            Ok(mut raw) => {
                self.reseed_timeout(&mut raw);
                g.raw = raw;
            }
            Err(e) => {
                tracing::error!(group = g.id, error = %e, "raft: the group's state could not be rebuilt after a refused snapshot");
                return;
            }
        }
        fail_pending(g, 0, &RaftError::Unknown);
        fail_reads(
            g,
            &RaftError::NotLeader {
                leader: known(g.raw.raft.leader_id),
            },
        );
        g.lease.reset(0);
        g.unpublish_lease();
        g.role = (g.raw.raft.term, g.raw.raft.state);
        g.snap_wait.clear();
        self.wake_group(g);
    }

    /// Once the log store holds this replica's removal from `gid`, the
    /// group's state machine drops what it kept of the group
    /// ([`crate::StateMachine::removed`]). Not before: a crash in between
    /// would bring the group back from its log over a state already
    /// dropped. Not when a new replica of the group started here meanwhile
    /// (it was added again).
    fn drop_state_when_durable(&self, gid: GroupId, cell: Arc<Cell>) {
        let slot = Slot::new();
        lock(&self.inner.inbox).created.push(slot.clone());
        self.mark_dirty(gid);
        let node = self.clone();
        let spawned = std::thread::Builder::new()
            .name(format!("sprig-raft-drop-{gid}"))
            .spawn(move || match slot.wait_timeout(Duration::from_secs(120)) {
                Some(Ok(())) => {
                    // Under the groups' lock: no new replica starts here
                    // while the old one's state goes.
                    let groups = node.read_groups();
                    if groups.contains_key(&gid) {
                        return;
                    }
                    lock(&cell.g).sm.removed();
                    drop(groups);
                }
                other => {
                    tracing::warn!(group = gid, outcome = ?other.map(Result::err), "raft: a departed replica's state was kept (its removal wasn't confirmed durable)");
                }
            });
        if let Err(e) = spawned {
            tracing::warn!(group = gid, error = %e, "raft: a departed replica's state was kept");
        }
    }

    /// Restores `g` from the complete snapshot file `id` (`len` bytes); the
    /// file goes afterwards, whatever the outcome.
    fn restore_file(&self, g: &mut Group, index: u64, id: u64, len: u64) -> Result<(), RaftError> {
        let Some(inc) = self.inner.snaps.take(g.id, id, len) else {
            return Err(RaftError::Storage(
                "the snapshot's file is not here (any more)".into(),
            ));
        };
        let f = std::fs::File::open(&inc.path)
            .map_err(|e| RaftError::Storage(format!("open a snapshot file: {e}")))?;
        let mut r = std::io::BufReader::with_capacity(crate::snapfile::CHUNK, f);
        let started = Instant::now();
        let out = g.sm.restore_from(index, &mut r);
        tracing::info!(
            group = g.id,
            index,
            bytes = len,
            ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            ok = out.is_ok(),
            "raft: snapshot restored from its file"
        );
        out
    }

    /// Starts building `gid`'s snapshot off the driver thread, unless one
    /// is being built (or too many are: then it is asked for again later).
    fn start_build(
        &self,
        gid: GroupId,
        g: &Group,
        cell: &Arc<Cell>,
        ex: Arc<dyn crate::sm::SnapshotExporter>,
    ) {
        {
            let mut b = lock(&self.inner.snaps.building);
            if b.contains(&gid) {
                return;
            }
            if b.len() >= MAX_SNAPSHOT_BUILDS {
                g.raw.store().want();
                return;
            }
            b.insert(gid);
        }
        let node = self.clone();
        let cell = Arc::clone(cell);
        let id = self.inner.next_snapshot.fetch_add(1, Ordering::Relaxed);
        let spawned = std::thread::Builder::new()
            .name(format!("sprig-raft-snap-{gid}"))
            .spawn(move || node.build_snapshot(gid, &cell, ex.as_ref(), id));
        if let Err(e) = spawned {
            lock(&self.inner.snaps.building).remove(&gid);
            g.raw.store().want();
            tracing::warn!(group = gid, error = %e, "raft: a snapshot build thread didn't start");
        }
    }

    /// The builder thread: the state's point is fixed under the group's
    /// lock, the state is written to a file, and the group offers it.
    fn build_snapshot(
        &self,
        gid: GroupId,
        cell: &Arc<Cell>,
        ex: &dyn crate::sm::SnapshotExporter,
        id: u64,
    ) {
        let started = Instant::now();
        let path = self.inner.snaps.out_path(gid, id);
        let built = write_snapshot_file(cell, ex, &path);
        lock(&self.inner.snaps.building).remove(&gid);
        let (meta, len) = match built {
            Ok(x) => x,
            Err(e) => {
                crate::snapfile::remove(&path);
                tracing::warn!(group = gid, error = %e, "raft: a snapshot build failed; it is asked for again");
                if let Some(c) = self.cell(gid) {
                    lock(&c.g).raw.store().want();
                    self.mark_dirty(gid);
                }
                return;
            }
        };
        let file = Arc::new(crate::snapfile::SnapFile { path, len });
        let same = self.cell(gid).is_some_and(|c| Arc::ptr_eq(&c, cell));
        if !same || self.is_closed() {
            return;
        }
        bump(&self.inner.counters.snapshots_built, 1);
        tracing::info!(
            group = gid,
            index = meta.index,
            bytes = len,
            ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "raft: snapshot built off the driver"
        );
        let mut snap = Snapshot::default();
        snap.data = crate::snapfile::token(id, len).into();
        snap.set_metadata(meta);
        let mut g = lock(&cell.g);
        lock(&self.inner.snaps.outgoing).insert(gid, (id, file));
        g.raw.mut_store().set_cached(snap);
        self.wake_group(&mut g);
        drop(g);
        self.mark_dirty(gid);
    }

    /// Queues the file of snapshot message `m` (a token) for the sender
    /// thread, which sends the file's chunks and then `m` to `to`.
    fn queue_transfer(&self, to: NodeId, gid: GroupId, applied: u64, m: Message) {
        let Some((id, _)) = crate::snapfile::parse_token(&m.get_snapshot().data) else {
            return;
        };
        let file = lock(&self.inner.snaps.outgoing)
            .get(&gid)
            .filter(|(i, _)| *i == id)
            .map(|(_, f)| Arc::clone(f));
        let Some(file) = file else {
            tracing::debug!(
                group = gid,
                id,
                to,
                "raft: a snapshot's file is gone; the leader asks again"
            );
            return;
        };
        {
            let mut q = lock(&self.inner.snaps.queue);
            // A newer message for the same follower replaces one waiting.
            q.0.retain(|t| !(t.to == to && t.group == gid));
            q.0.push_back(crate::snapfile::Transfer {
                to,
                group: gid,
                id,
                applied,
                msg: m,
                file,
            });
            q.1.insert((to, gid));
        }
        self.inner.snaps.cv.notify_all();
        let mut s = lock(&self.inner.snaps.sender);
        if s.is_none() {
            let node = self.clone();
            match std::thread::Builder::new()
                .name(format!("sprig-raft-snap-send-{}", self.inner.id))
                .spawn(move || node.send_loop())
            {
                Ok(h) => *s = Some(h),
                Err(e) => tracing::warn!(error = %e, "raft: the snapshot sender didn't start"),
            }
        }
    }

    /// Lets go of the snapshot file `g` offers once no follower waits for
    /// it and no transfer of it is queued: a large unit's file doesn't stay
    /// on disk after its copies caught up (a later need builds a new one).
    fn release_snapshot_file(&self, g: &mut Group) {
        let Some(id) = lock(&self.inner.snaps.outgoing).get(&g.id).map(|(i, _)| *i) else {
            return;
        };
        if lock(&self.inner.snaps.queue)
            .1
            .iter()
            .any(|(_, gr)| *gr == g.id)
        {
            return;
        }
        lock(&self.inner.snaps.outgoing).remove(&g.id);
        if g.raw
            .store()
            .cached_data()
            .and_then(|(d, _)| crate::snapfile::parse_token(d))
            .is_some_and(|(i, _)| i == id)
        {
            g.raw.mut_store().clear_cached();
        }
    }

    /// Changes how many snapshot bytes a second this node sends at most,
    /// over all its transfers (0: no limit).
    pub fn set_snapshot_rate(&self, bytes_per_second: u64) {
        self.inner
            .snapshot_rate
            .store(bytes_per_second, Ordering::Relaxed);
    }

    /// The snapshot sender thread: one transfer at a time, paced.
    fn send_loop(&self) {
        let mut pace = crate::snapfile::Pace::new();
        loop {
            let t = {
                let mut q = lock(&self.inner.snaps.queue);
                loop {
                    if self.is_closed() {
                        return;
                    }
                    if let Some(t) = q.0.pop_front() {
                        break t;
                    }
                    q = self
                        .inner
                        .snaps
                        .cv
                        .wait_timeout(q, Duration::from_millis(200))
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            };
            let started = Instant::now();
            let sent = self.send_transfer(&t, &mut pace);
            tracing::info!(
                group = t.group,
                to = t.to,
                bytes = t.file.len,
                ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                complete = sent,
                "raft: snapshot sent in chunks"
            );
            let mut q = lock(&self.inner.snaps.queue);
            if !q.0.iter().any(|x| x.to == t.to && x.group == t.group) {
                q.1.remove(&(t.to, t.group));
            }
        }
    }

    /// Sends `t`'s file in chunks, then its message. False when it stopped
    /// early (the node closed, the group left, a newer transfer to the same
    /// follower waits, or the file can't be read).
    fn send_transfer(
        &self,
        t: &crate::snapfile::Transfer,
        pace: &mut crate::snapfile::Pace,
    ) -> bool {
        use std::io::Read as _;
        let Ok(mut f) = std::fs::File::open(&t.file.path) else {
            return false;
        };
        let len = t.file.len;
        let mut buf = vec![0u8; crate::snapfile::CHUNK];
        let mut off = 0u64;
        loop {
            if self.is_closed() || self.cell(t.group).is_none() {
                return false;
            }
            if lock(&self.inner.snaps.queue)
                .0
                .iter()
                .any(|x| x.to == t.to && x.group == t.group)
            {
                return false;
            }
            let n = usize::try_from((len - off).min(crate::snapfile::CHUNK as u64)).unwrap_or(0);
            if f.read_exact(&mut buf[..n]).is_err() {
                return false;
            }
            pace.wait(n, self.inner.snapshot_rate.load(Ordering::Relaxed));
            self.send_items(
                t.to,
                vec![Item::SnapChunk {
                    group: t.group,
                    id: t.id,
                    offset: off,
                    total: len,
                    data: buf[..n].to_vec(),
                }],
            );
            bump(&self.inner.counters.snapshot_bytes_sent, n as u64);
            off += n as u64;
            if off >= len {
                break;
            }
        }
        self.send_items(t.to, vec![Item::Raft(t.group, t.applied, t.msg.clone())]);
        true
    }

    /// Sends `items` to `to` in a frame of their own (sequence 0: nothing
    /// in it renews a lease).
    fn send_items(&self, to: NodeId, items: Vec<Item>) {
        let ack_epoch = lock(&self.inner.peers).get(&to).map_or(0, |p| p.epoch_seen);
        let frame = Frame {
            from: self.inner.id,
            to,
            epoch: self.inner.epoch,
            seq: 0,
            ack_epoch,
            acks: Vec::new(),
            items,
        };
        bump(&self.inner.counters.frames_sent, 1);
        self.inner.transport.send(to, frame.encode());
    }

    /// Step 4 for one group: after the write, the ready is persisted; a
    /// snapshot's group applies its committed entries only now.
    fn finish(
        &self,
        mut c: Collected,
        writes: &mut Vec<GroupWrite>,
        dirty: &mut BTreeSet<GroupId>,
    ) -> Result<(), RaftError> {
        if c.snapshot_index.is_some() {
            {
                let mut g = lock(&c.cell.g);
                g.raw.on_persist_ready(c.number);
                if let Some(i) = c.snapshot_index
                    && g.raw.raft.raft_log.applied < i
                {
                    g.raw.advance_apply_to(i);
                }
            }
            self.apply_committed(&mut c, writes)?;
        } else if !c.removed {
            lock(&c.cell.g).raw.on_persist_ready(c.number);
        }
        if c.removed {
            return Ok(());
        }
        let g = lock(&c.cell.g);
        if g.raw.has_ready() {
            dirty.insert(g.id);
        }
        Ok(())
    }

    /// Applies a ready's committed entries (they are durable here), answers
    /// reads waiting on them and handles this replica's removal.
    fn apply_committed(
        &self,
        c: &mut Collected,
        writes: &mut Vec<GroupWrite>,
    ) -> Result<(), RaftError> {
        let mut g = lock(&c.cell.g);
        let gid = g.id;
        let mut run: Vec<Entry> = Vec::new();
        let mut removed = None;
        for e in std::mem::take(&mut c.committed) {
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
                        // The next lease is judged on the new members.
                        g.unpublish_lease();
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
            self.drop_state_when_durable(gid, Arc::clone(&c.cell));
            self.inner.snaps.forget(gid);
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
            c.removed = true;
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
            if let Some(ex) =
                g.sm.exporter()
                    .filter(|_| self.inner.cfg.snapshot_off_driver)
            {
                self.start_build(gid, &g, &c.cell, ex);
            } else {
                let snap = make_snapshot(&g)?;
                g.raw.mut_store().set_cached(snap);
            }
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
        let mut applied = Vec::with_capacity(run.len());
        for e in run.drain(..) {
            g.applied = e.index;
            g.applied_term = e.term;
            self.inner.observer.on_apply(g.id, e.index, e.term);
            applied.push((e.index, e.term));
        }
        resolve_run(g, &applied);
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
            // A snapshot that is a file goes to the sender thread: its
            // chunks first, then the message.
            let items: Vec<Item> = items
                .into_iter()
                .filter_map(|it| match it {
                    Item::Raft(g, applied, m)
                        if m.get_msg_type() == MessageType::MsgSnapshot
                            && crate::snapfile::parse_token(&m.get_snapshot().data).is_some() =>
                    {
                        self.queue_transfer(to, g, applied, m);
                        None
                    }
                    other => Some(other),
                })
                .collect();
            if items.is_empty() {
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

/// Writes `cell`'s state to `path` through `ex`, from a point fixed under
/// the group's lock: the snapshot's metadata (applied index, term,
/// membership) and the file's length.
fn write_snapshot_file(
    cell: &Arc<Cell>,
    ex: &dyn crate::sm::SnapshotExporter,
    path: &std::path::Path,
) -> Result<(SnapshotMetadata, u64), RaftError> {
    let io = |e: std::io::Error| RaftError::Storage(format!("a snapshot file: {e}"));
    let f = std::fs::File::create(path).map_err(io)?;
    let mut w = std::io::BufWriter::with_capacity(crate::snapfile::CHUNK, f);
    let mut guard = Some(lock(&cell.g));
    let mut meta = None;
    let r = ex.export(
        &mut || {
            if let Some(g) = guard.take() {
                let mut m = SnapshotMetadata::default();
                m.index = g.applied;
                m.term = g.applied_term;
                m.set_conf_state(g.applied_conf.clone());
                meta = Some(m);
            }
        },
        &mut w,
    );
    drop(guard);
    r?;
    std::io::Write::flush(&mut w).map_err(io)?;
    let len = w.get_ref().metadata().map_err(io)?.len();
    let meta = meta.ok_or_else(|| RaftError::Storage("the export never fixed its point".into()))?;
    Ok((meta, len))
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

/// Resolves the proposals of a run of applied (non-membership) entries,
/// given as (index, term) in log order, as [`resolve`] would entry by entry,
/// but newest first: proposals that share one progress (a replicated
/// unit's, per term) then learn the run's last index first, and the others'
/// reports change nothing (and wake no one).
fn resolve_run(g: &mut Group, run: &[(u64, u64)]) {
    let Some(&(last, _)) = run.last() else {
        return;
    };
    let mut done = Vec::new();
    while g.pending.front().is_some_and(|p| p.index <= last) {
        let Some(p) = g.pending.pop_front() else {
            break;
        };
        done.push(p);
    }
    for mut p in done.into_iter().rev() {
        match run.binary_search_by_key(&p.index, |e| e.0) {
            Ok(i) if run[i].1 == p.term && !p.conf => p.waiter.applied(p.index),
            // raft-rs turns a membership change it can't take (another is
            // pending) into an empty entry.
            Ok(i) if run[i].1 == p.term => p
                .waiter
                .failed(p.index, RaftError::Busy("a membership change is pending")),
            _ => p.waiter.failed(p.index, RaftError::Dropped),
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
