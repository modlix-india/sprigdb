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
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use raft::prelude::Entry;
use sprig_common::Result;
use sprig_engine::log::{
    Consensus, Durability, LogEntry, ProposalProgress, StateMachine as EngineMachine, Ticket,
    UnitId,
};
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
        Self {
            node,
            voters,
            epochs: Mutex::new(HashMap::new()),
            barriers: true,
            barrier_failed: Mutex::new(HashMap::new()),
        }
    }

    /// One read barrier: asks the leader, again every short while, until it
    /// answers or the wait runs out.
    fn wait_barrier(&self, unit: UnitId) -> Result<()> {
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
        if self
            .barrier_failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&unit)
            .is_some_and(|at| at.elapsed() < READ_BARRIER_BACKOFF)
        {
            return Err(sprig_common::SprigError::new(
                sprig_common::SprigCode::ShardMoved,
                "this replica could not confirm it holds the unit's latest commits (no \
                 leader reachable lately); retry",
            ));
        }
        let r = blocking(|| self.wait_barrier(unit));
        let mut failed = self
            .barrier_failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if r.as_ref()
            .is_err_and(|e| e.code() == Some(sprig_common::SprigCode::ShardMoved))
        {
            failed.insert(unit, Instant::now());
        } else {
            failed.remove(&unit);
        }
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
