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

use raft::prelude::Entry;
use sprig_common::Result;
use sprig_engine::log::{
    Consensus, Durability, LogEntry, ProposalProgress, StateMachine as EngineMachine, Ticket,
    UnitId,
};

use crate::error::RaftError;
use crate::node::{Node, Waiter};
use crate::sm::StateMachine;

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
        self.0.set_durable(index);
        self.0.set_applied(index);
    }

    fn failed(&mut self, index: u64, err: RaftError) {
        self.0.fail_from(index, !err.is_retryable());
    }
}

/// [`Consensus`] over a [`Node`]. Build the node with a factory of
/// [`UnitMachine`]s.
pub struct RaftConsensus {
    node: Node,
    /// Per unit: the term of this replica's latest proposals and their
    /// shared progress.
    epochs: Mutex<HashMap<UnitId, (u64, ProposalProgress)>>,
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
        Self {
            node,
            epochs: Mutex::new(HashMap::new()),
        }
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
        let floors: Vec<(u64, u64)> = self
            .node
            .groups()
            .into_iter()
            .map(|g| (g, self.node.applied(g)))
            .collect();
        flush_store()?;
        let mut n = 0;
        for (g, applied) in floors {
            if self.node.compact_durable(g, applied) {
                n += 1;
            }
        }
        Ok(n)
    }
}

impl Consensus for RaftConsensus {
    fn propose(&self, unit: UnitId, payload: Vec<u8>, durability: Durability) -> Result<Ticket> {
        let mut progress = None;
        let (_, index) = self
            .node
            .propose_with(unit.0, payload, durability, |term| {
                let p = self.epoch(unit, term);
                progress = Some(p.clone());
                Box::new(EpochWaiter(p))
            })?;
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
