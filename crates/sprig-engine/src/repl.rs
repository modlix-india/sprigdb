//! Replicated units: tenant units committed through a replicated log.
//!
//! An engine opened with [`crate::EngineConfig::replicated`] commits every
//! tenant unit through a [`ReplicatedLog`] (sprig-raft's, one Raft group per
//! unit) that the node attaches after opening the engine
//! ([`crate::Engine::attach_replication`]). The node's own unit stays on
//! the local log: what it holds (engine bookkeeping, the layout) belongs to
//! this node only. Until the replicated log is attached, a tenant unit's
//! proposals fail as retryable.
//!
//! The routing is [`Router`], the engine's [`Consensus`]: it sends each
//! unit's calls to the log that holds it.

use std::sync::{Arc, OnceLock};

use sprig_common::{Result, SprigCode, SprigError};

use crate::engine::NODE_UNIT;
use crate::log::{Consensus, Durability, LocalLog, LogEntry, Ticket, UnitId};

/// A commit log that replicates the units it holds. Its tickets fail,
/// retryable, when an entry is lost to a leadership change, and its
/// proposals fail at once on a replica that doesn't lead the unit.
pub trait ReplicatedLog: Consensus {
    /// Notes each unit's applied index, runs `flush_store` (which makes
    /// everything applied so far durable in the store), then trims each
    /// unit's log up to what was applied before the flush, but never above
    /// `cap(unit)` (an observer's floor). Returns the number of units
    /// trimmed.
    ///
    /// # Errors
    /// The flush failed.
    fn checkpoint(
        &self,
        flush_store: &dyn Fn() -> Result<()>,
        cap: &dyn Fn(UnitId) -> Option<u64>,
    ) -> Result<u64>;

    /// The committed entries `from..to` of `unit` the log still holds
    /// (fewer, or none, when it was trimmed). Never blocks on the log's
    /// apply path: observers call it while the store's apply waits for them.
    ///
    /// # Errors
    /// The log could not be read.
    fn entries(&self, unit: UnitId, from: u64, to: u64) -> Result<Vec<LogEntry>>;

    /// The lowest index of `unit` the log still holds.
    fn first_index(&self, unit: UnitId) -> Option<u64>;

    /// Every unit held here, with its applied index.
    fn applied(&self) -> Vec<(UnitId, u64)>;

    /// Stops the log: pending proposals fail with an unknown outcome.
    fn close(&self);

    /// Before a read of `unit` on a replica that doesn't lead it: returns
    /// once this replica has applied everything the unit's leader had
    /// committed when asked, so the read sees every write acknowledged
    /// before it started. Nothing by default.
    ///
    /// # Errors
    /// No leader confirmed in time (retryable).
    fn read_barrier(&self, unit: UnitId) -> Result<()> {
        let _ = unit;
        Ok(())
    }

    /// Before a read of `unit` on the replica that leads it: returns once
    /// it is known that no other replica can have committed to the unit as
    /// its leader, so this replica's own state holds every acknowledged
    /// write: its leader lease holds, or a majority of the unit's replicas
    /// has just confirmed it. A leader cut off from the others must not
    /// answer from its own state: a newer leader may have committed since.
    /// Nothing by default.
    ///
    /// # Errors
    /// This replica no longer leads the unit, or could not confirm in time
    /// that it does (retryable).
    fn leader_barrier(&self, unit: UnitId) -> Result<()> {
        let _ = unit;
        Ok(())
    }

    /// Before a bounded read of `unit` on a replica that doesn't lead it
    /// ([`crate::reads::ReadMode::Bounded`]): returns once the read may be
    /// served here, that is when `holds_token()` and this replica is known
    /// to be at most `max_lag` behind the leader, or once a read barrier
    /// that started after the call completed. Waits up to `wait` for the
    /// first before it runs the barrier. A read barrier by default.
    ///
    /// # Errors
    /// No leader confirmed in time (retryable).
    fn bounded_barrier(
        &self,
        unit: UnitId,
        max_lag: std::time::Duration,
        wait: std::time::Duration,
        holds_token: &dyn Fn() -> bool,
    ) -> Result<crate::reads::Served> {
        let _ = (max_lag, wait, holds_token);
        self.read_barrier(unit)
            .map(|()| crate::reads::Served::Barrier)
    }

    /// Asks `unit`'s current leader to hand the leadership to this node
    /// (a commit across units needs every unit led where it runs). Returns
    /// at once; nothing by default.
    fn request_lead(&self, unit: UnitId) {
        let _ = unit;
    }

    /// A write found `unit` not led here: a log that creates units on
    /// first use starts it here if it doesn't hold it yet (the caller
    /// retries once a leader is elected). Nothing by default.
    fn ensure_unit(&self, unit: UnitId) {
        let _ = unit;
    }
}

/// The engine's commit path: tenant units to the replicated log (when the
/// engine is replicated), everything else to the local log.
pub(crate) struct Router {
    pub(crate) local: Arc<LocalLog>,
    replicated: bool,
    /// Which units this node leads: a replicated unit takes proposals only
    /// where the engine leads it (a replica whose Raft group just won an
    /// election hasn't applied what its predecessors committed yet, so
    /// reads made for the write would be stale).
    leaders: Arc<crate::leader::Leaders>,
    repl: OnceLock<Arc<dyn ReplicatedLog>>,
}

enum Route<'a> {
    Local(&'a LocalLog),
    Repl(&'a Arc<dyn ReplicatedLog>),
    NotYet,
}

fn not_started() -> SprigError {
    SprigError::new(
        SprigCode::ShardMoved,
        "this node's replication hasn't started yet; retry",
    )
}

impl Router {
    pub(crate) fn new(
        local: Arc<LocalLog>,
        replicated: bool,
        leaders: Arc<crate::leader::Leaders>,
    ) -> Self {
        Self {
            local,
            replicated,
            leaders,
            repl: OnceLock::new(),
        }
    }

    pub(crate) fn is_replicated(&self) -> bool {
        self.replicated
    }

    /// Whether `unit` commits through the replicated log.
    pub(crate) fn is_replicated_unit(&self, unit: UnitId) -> bool {
        self.replicated && unit != NODE_UNIT && unit.0 != u64::MAX
    }

    pub(crate) fn attach(&self, r: Arc<dyn ReplicatedLog>) -> Result<()> {
        if !self.replicated {
            return Err(SprigError::internal(
                "engine: replication attached to an engine opened without it",
            ));
        }
        self.repl
            .set(r)
            .map_err(|_| SprigError::internal("engine: replication is already attached"))
    }

    pub(crate) fn replicated_log(&self) -> Option<&Arc<dyn ReplicatedLog>> {
        self.repl.get()
    }

    /// A proposal's route: refused (retryable) for a replicated unit this
    /// node doesn't lead, after asking the replicated log to start the unit
    /// if it doesn't hold it yet.
    fn route_write(&self, unit: UnitId) -> Result<Route<'_>> {
        let r = self.route(unit);
        if let Route::Repl(log) = &r
            && self.leaders.epoch(unit) == 0
        {
            log.ensure_unit(unit);
            return Err(SprigError::new(
                SprigCode::ShardMoved,
                "this node doesn't lead the data's unit; retry",
            ));
        }
        Ok(r)
    }

    fn route(&self, unit: UnitId) -> Route<'_> {
        if !self.is_replicated_unit(unit) {
            return Route::Local(&self.local);
        }
        match self.repl.get() {
            Some(r) => Route::Repl(r),
            None => Route::NotYet,
        }
    }

    /// The committed entries `from..to` of `unit`, from whichever log holds
    /// it.
    pub(crate) fn entries(&self, unit: UnitId, from: u64, to: u64) -> Result<Vec<LogEntry>> {
        match self.route(unit) {
            Route::Local(l) => l.entries(unit, from, to),
            Route::Repl(r) => r.entries(unit, from, to),
            Route::NotYet => Ok(Vec::new()),
        }
    }

    pub(crate) fn first_index(&self, unit: UnitId) -> Option<u64> {
        match self.route(unit) {
            Route::Local(l) => l.first_index(unit),
            Route::Repl(r) => r.first_index(unit),
            Route::NotYet => None,
        }
    }

    /// Every unit either log knows, with its applied index.
    pub(crate) fn applied(&self) -> Vec<(UnitId, u64)> {
        let mut u = self.local.applied();
        if let Some(r) = self.repl.get() {
            u.retain(|(id, _)| !self.is_replicated_unit(*id));
            u.extend(r.applied());
        }
        u
    }

    pub(crate) fn close_replicated(&self) {
        if let Some(r) = self.repl.get() {
            r.close();
        }
    }
}

impl Consensus for Router {
    fn propose(&self, unit: UnitId, payload: Vec<u8>, durability: Durability) -> Result<Ticket> {
        match self.route_write(unit)? {
            Route::Local(l) => l.propose(unit, payload, durability),
            Route::Repl(r) => r.propose(unit, payload, durability),
            Route::NotYet => Err(not_started()),
        }
    }

    fn propose_many(
        &self,
        parts: Vec<(UnitId, Vec<u8>)>,
        durability: Durability,
    ) -> Result<Vec<Ticket>> {
        if parts.iter().all(|(u, _)| !self.is_replicated_unit(*u)) {
            return self.local.propose_many(parts, durability);
        }
        if parts.len() > 1 {
            return Err(SprigError::internal(
                "commit log: a write across replicated units needs the commit protocol across \
                 units",
            ));
        }
        parts
            .into_iter()
            .map(|(u, p)| self.propose(u, p, durability))
            .collect()
    }

    fn durable_in_order(&self) -> bool {
        // Each replicated unit has its own quorum: nothing orders entries
        // across units.
        !self.replicated && self.local.durable_in_order()
    }

    fn durable_index(&self, unit: UnitId) -> u64 {
        match self.route(unit) {
            Route::Local(l) => l.durable_index(unit),
            Route::Repl(r) => r.durable_index(unit),
            Route::NotYet => 0,
        }
    }

    fn applied_index(&self, unit: UnitId) -> u64 {
        match self.route(unit) {
            Route::Local(l) => l.applied_index(unit),
            Route::Repl(r) => r.applied_index(unit),
            Route::NotYet => 0,
        }
    }

    fn proposed_index(&self, unit: UnitId) -> u64 {
        match self.route(unit) {
            Route::Local(l) => l.proposed_index(unit),
            Route::Repl(r) => r.proposed_index(unit),
            Route::NotYet => 0,
        }
    }

    fn wait_applied(&self, unit: UnitId, index: u64) -> Result<()> {
        match self.route(unit) {
            Route::Local(l) => l.wait_applied(unit, index),
            Route::Repl(r) => r.wait_applied(unit, index),
            Route::NotYet => Err(not_started()),
        }
    }
}
