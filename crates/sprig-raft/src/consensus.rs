//! The engine's [`Consensus`] trait over the driver: a unit is a group.
//!
//! Each unit's entries apply to the engine's [`EngineMachine`] (shared by
//! every unit) through a [`UnitMachine`]. A proposal's ticket counts as
//! durable once the entry is committed and applied on this replica, and
//! fails when the entry is lost to a leadership change: retryable
//! (`ShardMoved`) when another leader's entry replaced it, or with an
//! unknown outcome (`CommitOutcomeUnknown`) when a snapshot or a stop hid
//! it. A proposal on a replica that doesn't lead fails at once, retryable.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use raft::prelude::Entry;
use sprig_common::Result;
use sprig_engine::log::{
    Consensus, Durability, LogEntry, ProposalProgress, StateMachine as EngineMachine, Ticket,
    UnitId,
};
use sprig_engine::reads::Served;
use sprig_engine::repl::ReplicatedLog;

use crate::error::RaftError;
use crate::node::{Node, Waiter};
use crate::sm::StateMachine;

/// How long a read on a replica that doesn't lead its unit waits for the
/// leader to confirm what it had committed (an election or two).
const READ_BARRIER_WAIT: Duration = Duration::from_secs(5);

/// How long one read-barrier request waits before it is sent again.
const READ_BARRIER_TRY: Duration = Duration::from_millis(250);

/// After a read barrier of a unit gave up (no leader reachable), how long
/// further reads of it fail at once instead of each waiting again.
const READ_BARRIER_BACKOFF: Duration = Duration::from_secs(1);

/// Runs `f`, which blocks, off the async runtime's scheduling when called
/// on a multi-thread runtime's worker: the runtime moves its other tasks to
/// another thread meanwhile. Without this, reads that each wait for a
/// leader nobody can reach hold every worker thread, and the whole gateway
/// stalls (its permit loop and listeners included).
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// Read barriers per unit, shared by every reader of the unit on this
/// replica: at most one is in flight per unit, and a read that arrives
/// while one runs waits for the next one (a barrier only covers commits
/// acknowledged before it started). Each completed barrier marks the unit
/// fresh as of its start, which bounded reads use without asking again.
struct Barriers {
    node: Node,
    units: Mutex<HashMap<UnitId, UnitBarrier>>,
    changed: Condvar,
    /// Each unit's `fresh_at`, as nanoseconds after `base`, readable by
    /// every bounded read without taking `units`.
    fresh: std::sync::RwLock<HashMap<UnitId, Arc<std::sync::atomic::AtomicU64>>>,
    base: Instant,
}

#[derive(Default)]
struct UnitBarrier {
    /// A barrier is running (started at this instant).
    running: Option<Instant>,
    /// The start of the latest barrier that completed: this replica held
    /// every commit acknowledged before then.
    fresh_at: Option<Instant>,
    /// The latest barrier that failed: its start and what went wrong.
    failed: Option<(Instant, String)>,
}

fn lock_units(b: &Barriers) -> MutexGuard<'_, HashMap<UnitId, UnitBarrier>> {
    b.units.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Barriers {
    /// Returns once a barrier of `unit` that started at or after `arrived`
    /// has completed: running one itself when none is in flight, else
    /// waiting for the one in flight (and, if that one started earlier, for
    /// the next).
    fn since(&self, unit: UnitId, arrived: Instant) -> Result<()> {
        let mut g = lock_units(self);
        loop {
            let b = g.entry(unit).or_default();
            if b.fresh_at.is_some_and(|f| f >= arrived) {
                return Ok(());
            }
            if let Some((at, msg)) = &b.failed
                && *at >= arrived
            {
                return Err(sprig_common::SprigError::new(
                    sprig_common::SprigCode::ShardMoved,
                    msg.clone(),
                ));
            }
            if b.running.is_some() {
                g = self
                    .changed
                    .wait_timeout(g, READ_BARRIER_TRY)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
                continue;
            }
            let started = Instant::now();
            b.running = Some(started);
            drop(g);
            let r = self.run(unit);
            let mut g = lock_units(self);
            let b = g.entry(unit).or_default();
            b.running = None;
            match &r {
                Ok(()) => {
                    b.fresh_at = Some(b.fresh_at.map_or(started, |f| f.max(started)));
                    self.publish_fresh(unit, started);
                }
                Err(e) => b.failed = Some((started, e.to_string())),
            }
            drop(g);
            self.changed.notify_all();
            return r;
        }
    }

    /// The start of the latest barrier of `unit` that completed.
    fn fresh_at(&self, unit: UnitId) -> Option<Instant> {
        let n = self
            .fresh
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)?
            .load(std::sync::atomic::Ordering::Acquire);
        (n > 0).then(|| self.base + Duration::from_nanos(n))
    }

    fn publish_fresh(&self, unit: UnitId, at: Instant) {
        let n = u64::try_from(at.saturating_duration_since(self.base).as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let cell = {
            let r = self.fresh.read().unwrap_or_else(PoisonError::into_inner);
            r.get(&unit).cloned()
        };
        let cell = cell.unwrap_or_else(|| {
            Arc::clone(
                self.fresh
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(unit)
                    .or_default(),
            )
        });
        cell.fetch_max(n, std::sync::atomic::Ordering::AcqRel);
    }

    /// One read barrier: asks the leader, again every short while, until it
    /// answers or the wait runs out.
    fn run(&self, unit: UnitId) -> Result<()> {
        let deadline = Instant::now() + READ_BARRIER_WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let failed = match self.node.read_barrier(unit.0) {
                // Units placed per unit: a member that doesn't hold its
                // replica yet starts it and asks the leader as any replica
                // does; a node the unit isn't placed on holds no copy, so
                // it must not answer as if the unit were empty.
                Err(RaftError::NoGroup(_)) if self.node.placed() => {
                    match self.node.start_voters(unit.0) {
                        Some(voters) => {
                            self.node.create_group(unit.0, &voters)?;
                            continue;
                        }
                        None => {
                            return Err(sprig_common::SprigError::new(
                                sprig_common::SprigCode::ShardMoved,
                                "this node holds no copy of the unit; its replicas are on \
                                 other nodes; retry there",
                            ));
                        }
                    }
                }
                // No replica of the unit here: nothing was replicated to
                // this node yet (a unit nobody wrote reads as empty).
                Err(RaftError::NoGroup(_)) => return Ok(()),
                Err(e) if e.is_retryable() => e,
                Err(e) => return Err(e.into()),
                // One attempt waits a short while only: a request sent to a
                // leader that just died is never answered, and asking again
                // reaches whichever leader this replica knows by then.
                Ok(slot) => match slot.wait_timeout(left.min(READ_BARRIER_TRY)) {
                    Some(Ok(_)) => return Ok(()),
                    Some(Err(e)) if e.is_retryable() => e,
                    Some(Err(e)) => return Err(e.into()),
                    None => RaftError::Dropped,
                },
            };
            if Instant::now() >= deadline {
                tracing::debug!(unit = unit.0, error = %failed, "raft: read barrier gave up");
                return Err(sprig_common::SprigError::new(
                    sprig_common::SprigCode::ShardMoved,
                    "this replica could not confirm it holds the unit's latest commits (no \
                     leader reachable); retry",
                ));
            }
            std::thread::sleep(left.min(Duration::from_millis(10)));
        }
    }

    /// Starts a barrier of `unit` in the background unless one is in
    /// flight: bounded reads keep the unit fresh without waiting for it.
    fn refresh(self: &Arc<Self>, unit: UnitId) {
        {
            let g = lock_units(self);
            if g.get(&unit).is_some_and(|b| b.running.is_some()) {
                return;
            }
        }
        let me = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("raft-read-refresh".into())
            .spawn(move || {
                if let Err(e) = me.since(unit, Instant::now()) {
                    tracing::debug!(unit = unit.0, error = %e, "raft: background read barrier failed");
                }
            });
        if let Err(e) = spawned {
            tracing::debug!(error = %e, "raft: could not start a background read barrier");
        }
    }
}

/// A unit's snapshots: the engine's export of a unit's key range at its
/// applied index, and the ingest of one into an empty unit.
pub trait UnitSnapshots: Send + Sync {
    /// The unit's state at its applied index.
    ///
    /// # Errors
    /// The state could not be read.
    fn export(&self, unit: UnitId) -> std::result::Result<Vec<u8>, RaftError>;

    /// Replaces the unit's state with `data`, taken at `index` (empty data:
    /// an empty unit).
    ///
    /// # Errors
    /// The data is malformed or could not be written.
    fn ingest(&self, unit: UnitId, index: u64, data: &[u8]) -> std::result::Result<(), RaftError>;

    /// Answers a read query against the unit (the driver's linearizable
    /// reads). None by default.
    fn query(&self, unit: UnitId, q: &[u8]) -> Vec<u8> {
        let _ = (unit, q);
        Vec::new()
    }
}

/// One unit's view of the engine's state machine. Durable: the engine
/// keeps its own applied index, so entries it already holds are not applied
/// again after a restart.
pub struct UnitMachine {
    unit: UnitId,
    sm: Arc<dyn EngineMachine>,
    snaps: Arc<dyn UnitSnapshots>,
}

impl UnitMachine {
    /// The machine of `unit`.
    #[must_use]
    pub fn new(unit: UnitId, sm: Arc<dyn EngineMachine>, snaps: Arc<dyn UnitSnapshots>) -> Self {
        Self { unit, sm, snaps }
    }
}

impl StateMachine for UnitMachine {
    fn apply(&mut self, entries: &[Entry]) -> std::result::Result<(), RaftError> {
        let es: Vec<LogEntry> = entries
            .iter()
            .map(|e| LogEntry::new(e.term, e.index, e.data.to_vec()))
            .collect();
        self.sm
            .apply(self.unit, &es)
            .map_err(|e| RaftError::Storage(e.to_string()))
    }

    fn query(&self, q: &[u8]) -> Vec<u8> {
        self.snaps.query(self.unit, q)
    }

    fn snapshot(&self) -> std::result::Result<Vec<u8>, RaftError> {
        self.snaps.export(self.unit)
    }

    fn restore(&mut self, index: u64, data: &[u8]) -> std::result::Result<(), RaftError> {
        self.snaps.ingest(self.unit, index, data)
    }

    fn durable(&self) -> bool {
        true
    }

    fn applied_index(&self) -> u64 {
        self.sm.applied_index(self.unit).unwrap_or(0)
    }
}

struct EpochWaiter(ProposalProgress);

impl Waiter for EpochWaiter {
    fn applied(&mut self, index: u64) {
        self.0.set_done(index);
    }

    fn failed(&mut self, index: u64, err: RaftError) {
        self.0.fail_from(index, !err.is_retryable());
    }
}

/// [`Consensus`] over a [`Node`]. Build the node with a factory of
/// [`UnitMachine`]s.
pub struct RaftConsensus {
    node: Node,
    /// The fixed voters of every unit (empty: units are created
    /// explicitly).
    voters: Vec<u64>,
    /// Per unit: the term of this replica's latest proposals and their
    /// shared progress.
    epochs: Mutex<HashMap<UnitId, (u64, ProposalProgress)>>,
    /// Whether reads on a replica that doesn't lead their unit wait for a
    /// read barrier (off: they read what the replica holds).
    barriers: bool,
    /// Units whose last read barrier gave up, and when.
    barrier_failed: Mutex<HashMap<UnitId, Instant>>,
    /// Read barriers per unit (coalesced) and each unit's freshness.
    reads: Arc<Barriers>,
    /// Units this node leads (or believed it led) whose last check with a
    /// majority got no answer in time, and when it gave up.
    leader_unconfirmed: Mutex<HashMap<UnitId, Instant>>,
}

fn lock_map(m: &Mutex<HashMap<UnitId, Instant>>) -> MutexGuard<'_, HashMap<UnitId, Instant>> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl std::fmt::Debug for RaftConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftConsensus")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl RaftConsensus {
    /// Consensus over `node`.
    #[must_use]
    pub fn new(node: Node) -> Self {
        Self::with_voters(node, Vec::new())
    }

    /// Consensus over `node` on a fixed membership: a proposal to a unit
    /// this node doesn't hold yet starts its group with `voters`.
    #[must_use]
    pub fn with_voters(node: Node, voters: Vec<u64>) -> Self {
        let reads = Arc::new(Barriers {
            node: node.clone(),
            units: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            fresh: std::sync::RwLock::new(HashMap::new()),
            // Before any barrier can start, so every start is after it.
            base: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        });
        Self {
            node,
            voters,
            epochs: Mutex::new(HashMap::new()),
            barriers: true,
            barrier_failed: Mutex::new(HashMap::new()),
            reads,
            leader_unconfirmed: Mutex::new(HashMap::new()),
        }
    }

    /// When the latest completed read barrier of `unit` on this replica
    /// started (this replica held every commit acknowledged before then).
    #[must_use]
    pub fn fresh_at(&self, unit: UnitId) -> Option<Instant> {
        self.reads.fresh_at(unit)
    }

    /// Whether a read barrier of `unit` gave up in the last second.
    fn barrier_backing_off(&self, unit: UnitId) -> bool {
        self.barrier_failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .is_some_and(|at| at.elapsed() < READ_BARRIER_BACKOFF)
    }

    /// Notes a barrier's outcome for the back-off.
    fn note_barrier(&self, unit: UnitId, r: &Result<()>) {
        let mut failed = self
            .barrier_failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if r.as_ref()
            .is_err_and(|e| e.code() == Some(sprig_common::SprigCode::ShardMoved))
        {
            failed.insert(unit, Instant::now());
        } else {
            failed.remove(&unit);
        }
    }

    /// The slow side of [`ReplicatedLog::leader_barrier`]: the lease isn't
    /// known to hold, so the driver checks it under the group's lock and,
    /// failing that, asks a majority (`ReadIndex`), again every short while
    /// until it answers or the wait runs out. Stops at once when this
    /// replica no longer leads.
    fn confirm_leading(&self, unit: UnitId) -> Result<()> {
        let deadline = Instant::now() + READ_BARRIER_WAIT;
        loop {
            let slot = match self.node.leader_barrier(unit.0) {
                Ok(None) => return Ok(()),
                Ok(Some(slot)) => slot,
                Err(RaftError::NoGroup(_)) => {
                    return Err(sprig_common::SprigError::new(
                        sprig_common::SprigCode::ShardMoved,
                        "this node no longer holds the unit; retry",
                    ));
                }
                Err(e) => return Err(e.into()),
            };
            let left = deadline.saturating_duration_since(Instant::now());
            match slot.wait_timeout(left.min(READ_BARRIER_TRY)) {
                Some(Ok(_)) => return Ok(()),
                // A request raft-rs dropped (a leader that hasn't committed
                // in its term yet) is asked again.
                Some(Err(RaftError::Dropped)) | None => {}
                Some(Err(e)) => return Err(e.into()),
            }
            if Instant::now() >= deadline {
                tracing::debug!(
                    unit = unit.0,
                    "raft: a leader's read could not confirm its leadership"
                );
                lock_map(&self.leader_unconfirmed).insert(unit, Instant::now());
                return Err(unconfirmed_leader());
            }
        }
    }

    /// Turns read barriers on (the default) or off (a simulation that
    /// steps the driver on the reader's thread: reads see what the replica
    /// holds).
    pub fn set_read_barriers(&mut self, on: bool) {
        self.barriers = on;
    }

    /// The node.
    #[must_use]
    pub fn node(&self) -> &Node {
        &self.node
    }

    fn epoch(&self, unit: UnitId, term: u64) -> ProposalProgress {
        let mut e = self.epochs.lock().unwrap_or_else(PoisonError::into_inner);
        match e.get(&unit) {
            Some((t, p)) if *t == term => p.clone(),
            _ => {
                let p = ProposalProgress::new();
                e.insert(unit, (term, p.clone()));
                p
            }
        }
    }

    /// Runs `flush_store` (which must make everything applied so far
    /// durable in the engine), then trims every unit's log up to what was
    /// applied before the flush. Returns the number of units trimmed.
    ///
    /// # Errors
    /// The flush failed.
    pub fn checkpoint(&self, flush_store: &dyn Fn() -> Result<()>) -> Result<usize> {
        self.checkpoint_capped(flush_store, &|_| None)
    }

    /// [`RaftConsensus::checkpoint`], trimming no unit above `cap(unit)`.
    ///
    /// # Errors
    /// The flush failed.
    pub fn checkpoint_capped(
        &self,
        flush_store: &dyn Fn() -> Result<()>,
        cap: &dyn Fn(UnitId) -> Option<u64>,
    ) -> Result<usize> {
        let floors: Vec<(u64, u64)> = self
            .node
            .groups()
            .into_iter()
            .map(|g| (g, self.node.applied(g)))
            .collect();
        flush_store()?;
        let mut n = 0;
        for (g, applied) in floors {
            // The driver trims only at the applied index (where it knows
            // the configuration), so a capped unit waits for a later
            // checkpoint.
            if cap(UnitId(g)).is_some_and(|c| c < applied) {
                continue;
            }
            if self.node.compact_durable(g, applied) {
                n += 1;
            }
        }
        Ok(n)
    }

    /// On a fixed membership ([`crate::RaftConfig::static_voters`]): starts
    /// `unit`'s group here if this node doesn't hold it yet, and campaigns
    /// for it. Returns whether it started it.
    ///
    /// # Errors
    /// The node is closed, or `voters` is empty.
    pub fn start_unit(&self, unit: UnitId, voters: &[u64]) -> std::result::Result<bool, RaftError> {
        if self.node.status(unit.0).is_some() {
            return Ok(false);
        }
        self.node.create_group(unit.0, voters)?;
        // The proposer's node asks first: its clients are here.
        let _ = self.node.campaign(unit.0);
        Ok(true)
    }
}

impl Consensus for RaftConsensus {
    fn propose(&self, unit: UnitId, payload: Vec<u8>, durability: Durability) -> Result<Ticket> {
        let mut progress = None;
        let proposed = self.node.propose_with(unit.0, payload, durability, |term| {
            let p = self.epoch(unit, term);
            progress = Some(p.clone());
            Box::new(EpochWaiter(p))
        });
        let (_, index) = match proposed {
            Err(RaftError::NoGroup(_)) => {
                // A unit nobody wrote yet: on a fixed membership (the
                // cluster's, or the unit's placement) a member starts its
                // group; the caller retries once it has a leader. A node the
                // unit isn't placed on answers "not the leader": its
                // replicas are elsewhere.
                if let Some(voters) = self.node.start_voters(unit.0) {
                    self.start_unit(unit, &voters)?;
                    return Err(RaftError::NotLeader { leader: None }.into());
                }
                if self.node.placed() {
                    return Err(RaftError::NotLeader { leader: None }.into());
                }
                let voters = self.voters.clone();
                if !voters.is_empty() {
                    self.start_unit(unit, &voters)?;
                    return Err(RaftError::NotLeader { leader: None }.into());
                }
                return Err(RaftError::NoGroup(unit.0).into());
            }
            r => r?,
        };
        let p =
            progress.ok_or_else(|| sprig_common::SprigError::internal("no proposal progress"))?;
        Ok(p.ticket(unit, index, durability))
    }

    fn durable_index(&self, unit: UnitId) -> u64 {
        self.node.committed(unit.0)
    }

    fn applied_index(&self, unit: UnitId) -> u64 {
        self.node.applied(unit.0)
    }

    fn wait_applied(&self, unit: UnitId, index: u64) -> Result<()> {
        self.node.wait_applied(unit.0, index).map_err(Into::into)
    }
}

impl ReplicatedLog for RaftConsensus {
    fn checkpoint(
        &self,
        flush_store: &dyn Fn() -> Result<()>,
        cap: &dyn Fn(UnitId) -> Option<u64>,
    ) -> Result<u64> {
        self.checkpoint_capped(flush_store, cap).map(|n| n as u64)
    }

    fn entries(&self, unit: UnitId, from: u64, to: u64) -> Result<Vec<LogEntry>> {
        Ok(self
            .node
            .log_entries(unit.0, from, to)?
            .into_iter()
            .filter(|e| {
                !e.data.is_empty() && e.get_entry_type() == raft::prelude::EntryType::EntryNormal
            })
            .map(|e| LogEntry::new(e.term, e.index, e.data.to_vec()))
            .collect())
    }

    fn first_index(&self, unit: UnitId) -> Option<u64> {
        self.node.log_first_index(unit.0)
    }

    fn applied(&self) -> Vec<(UnitId, u64)> {
        self.node
            .groups()
            .into_iter()
            .map(|g| (UnitId(g), self.node.applied(g)))
            .collect()
    }

    fn close(&self) {
        self.node.close();
    }

    fn read_barrier(&self, unit: UnitId) -> Result<()> {
        if !self.barriers {
            return Ok(());
        }
        if self.barrier_backing_off(unit) {
            return Err(backing_off());
        }
        let arrived = Instant::now();
        let r = blocking(|| self.reads.since(unit, arrived));
        self.note_barrier(unit, &r);
        r
    }

    fn leader_barrier(&self, unit: UnitId) -> Result<()> {
        if !self.barriers || self.node.lease_holds(unit.0) {
            return Ok(());
        }
        // A leader that reached no majority a moment ago: fail at once
        // rather than make every read wait again.
        if lock_map(&self.leader_unconfirmed)
            .get(&unit)
            .is_some_and(|at| at.elapsed() < READ_BARRIER_BACKOFF)
        {
            return Err(unconfirmed_leader());
        }
        blocking(|| self.confirm_leading(unit))
    }

    fn bounded_barrier(
        &self,
        unit: UnitId,
        max_lag: Duration,
        wait: Duration,
        holds_token: &dyn Fn() -> bool,
    ) -> Result<Served> {
        if !self.barriers {
            return Ok(Served::Local);
        }
        let arrived = Instant::now();
        // Servable: a barrier that started after this read arrived has
        // completed (the read is exact), or this replica holds the token
        // and its latest barrier is recent enough.
        let servable = |now: Instant| match self.reads.fresh_at(unit) {
            Some(f) if f >= arrived => true,
            Some(f) => now.saturating_duration_since(f) <= max_lag && holds_token(),
            None => false,
        };
        if servable(arrived) {
            // Keep the unit fresh ahead of the bound, in the background.
            if self
                .reads
                .fresh_at(unit)
                .is_some_and(|f| arrived.saturating_duration_since(f) > max_lag / 2)
            {
                self.reads.refresh(unit);
            }
            return Ok(Served::Local);
        }
        if self.barrier_backing_off(unit) {
            return Err(backing_off());
        }
        let r = blocking(|| {
            self.reads.refresh(unit);
            let deadline = arrived + wait;
            loop {
                let now = Instant::now();
                if servable(now) {
                    return Ok(Served::Waited);
                }
                if now >= deadline {
                    break;
                }
                // Woken by a barrier's end; the token is checked again at
                // least every millisecond (its commit may be applying).
                let g = lock_units(&self.reads);
                let left = deadline.saturating_duration_since(now);
                let _ = self
                    .reads
                    .changed
                    .wait_timeout(g, left.min(Duration::from_millis(1)))
                    .unwrap_or_else(PoisonError::into_inner);
            }
            self.reads.since(unit, arrived).map(|()| Served::Barrier)
        });
        self.note_barrier(unit, &r.as_ref().map(|_| ()).map_err(clone_err));
        r
    }

    fn request_lead(&self, unit: UnitId) {
        match self.node.request_leadership(unit.0) {
            Ok(()) => {}
            // A unit nobody wrote yet: start it here (it campaigns).
            Err(RaftError::NoGroup(_)) => self.ensure_unit(unit),
            // No leader known: stand for election.
            Err(RaftError::NotLeader { leader: None }) => {
                let _ = self.node.campaign(unit.0);
            }
            Err(e) => {
                tracing::debug!(unit = unit.0, error = %e, "raft: could not ask for a unit's leadership");
            }
        }
    }

    fn ensure_unit(&self, unit: UnitId) {
        let voters = match self.node.start_voters(unit.0) {
            Some(v) => v,
            // Placed per unit, not here (or not known yet): nothing to start.
            None if self.node.placed() => return,
            None => self.voters.clone(),
        };
        if !voters.is_empty()
            && let Err(e) = self.start_unit(unit, &voters)
        {
            tracing::debug!(unit = unit.0, error = %e, "raft: could not start a unit");
        }
    }
}

fn unconfirmed_leader() -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::ShardMoved,
        "this node could not confirm that it still leads the unit (it reaches no majority of the \
         unit's replicas); retry",
    )
}

fn backing_off() -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::ShardMoved,
        "this replica could not confirm it holds the unit's latest commits (no leader reachable \
         lately); retry",
    )
}

fn clone_err(e: &sprig_common::SprigError) -> sprig_common::SprigError {
    match e.code() {
        Some(c) => sprig_common::SprigError::new(c, e.to_string()),
        None => sprig_common::SprigError::internal(e.to_string()),
    }
}
