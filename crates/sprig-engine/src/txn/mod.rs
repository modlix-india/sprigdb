//! Transactions: HLC timestamps, MVCC versions in keys, read views, write
//! sets, record locks with `InnoDB`'s errors, and early lock release.
//!
//! **Reads.** A plain read sees the transaction's own writes, else the
//! newest version at or below its read view `rts`. Under REPEATABLE READ the
//! view opens at the first plain read (or at `BEGIN` with a consistent
//! snapshot, or for SERIALIZABLE with snapshot isolation); under READ
//! COMMITTED each statement gets a fresh one. A view's timestamp is the
//! commit timestamp of the last commit queued on the unit, so every version
//! it may see is already in the pending overlay or the store.
//!
//! **Locking reads and DML** lock the row, then read the latest committed
//! version (`InnoDB`'s current read). With MariaDB's snapshot isolation a
//! locking read of a row whose latest version is newer than the open view
//! fails with 1020 and rolls the transaction back.
//!
//! **Commit.** The write set becomes one log entry with one commit
//! timestamp. Under the unit's commit latch the entry is proposed (which
//! gives it its log index) and its versions enter the pending overlay; then
//! the locks are released (early lock release). The commit returns a
//! [`CommitTicket`]; the client is answered only once it is durable.
//!
//! **Commit dependency and the reply gate.** A read that returns a pending
//! version keeps that version's log ticket. A writer's own entry comes later
//! in the same log, so its ticket covers the dependency; a reader's ticket
//! (and [`Txn::reply_gate`]) waits for it. So no client sees data a crash
//! could lose, and no isolation level or answer changes: only lock hold
//! times shrink.
//!
//! **GC.** Versions below the oldest read view are dropped by
//! [`gc::GcFilter`] (the compaction filter's rule) or [`gc::gc_scan`].

pub mod durable;
pub mod errors;
pub mod gc;
pub mod hlc;
pub mod mvcc;
pub mod overlay;
mod shared;
pub mod testing;
mod transaction;
pub mod write_set;

use std::time::Duration;

use sprig_common::Result;

pub use durable::DurableTicket;
pub use hlc::Ts;
pub use shared::{TxnShared, TxnStats};
pub use transaction::{AbortReason, CommitTicket, LockRequest, Locked, Savepoint, Txn, TxnState};

use crate::Engine;
pub use crate::locks::{LockMode, Wait};
use crate::log::Durability;
use crate::store::SortedStore;

/// Isolation levels (READ UNCOMMITTED runs as READ COMMITTED).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Isolation {
    /// A fresh view per statement; record locks only.
    ReadCommitted,
    /// One view per transaction, opened at the first plain read.
    #[default]
    RepeatableRead,
    /// REPEATABLE READ; the SQL layer turns plain reads inside transactions
    /// into shared locking reads.
    Serializable,
}

/// How a read locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadMode {
    /// A plain read at the read view.
    #[default]
    Snapshot,
    /// `LOCK IN SHARE MODE` / `FOR SHARE`: a current read with a shared lock.
    ForShare,
    /// `FOR UPDATE`: a current read with an exclusive lock.
    ForUpdate,
    /// `FOR UPDATE NOWAIT`.
    NoWait,
}

/// Scan direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Dir {
    /// Ascending keys.
    #[default]
    Forward,
    /// Descending keys.
    Reverse,
}

/// Which server's behaviour the session follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Persona {
    /// MariaDB 11.8: 1020 with snapshot isolation; `NOWAIT` gives 1205.
    #[default]
    MariaDb,
    /// MySQL 8.4: no 1020; `NOWAIT` gives 3572.
    MySql,
}

/// Options for [`Engine::begin`], from the session.
// One flag per session variable it mirrors.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnOptions {
    /// The isolation level.
    pub isolation: Isolation,
    /// `START TRANSACTION READ ONLY` (enforced by the SQL layer).
    pub read_only: bool,
    /// The commit's durability class (`None`: the engine's default).
    pub durability: Option<Durability>,
    /// The persona.
    pub persona: Persona,
    /// `innodb_snapshot_isolation` (MariaDB only).
    pub snapshot_isolation: bool,
    /// `innodb_lock_wait_timeout`; `None` waits forever.
    pub lock_wait_timeout: Option<Duration>,
    /// `innodb_rollback_on_timeout`.
    pub rollback_on_timeout: bool,
    /// `innodb_deadlock_detect`.
    pub deadlock_detect: bool,
    /// `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
    pub consistent_snapshot: bool,
}

impl TxnOptions {
    /// MariaDB 11.8's defaults: REPEATABLE READ, snapshot isolation on, a
    /// 50 s lock wait.
    #[must_use]
    pub fn mariadb() -> Self {
        Self {
            isolation: Isolation::RepeatableRead,
            read_only: false,
            durability: None,
            persona: Persona::MariaDb,
            snapshot_isolation: true,
            lock_wait_timeout: Some(Duration::from_secs(50)),
            rollback_on_timeout: false,
            deadlock_detect: true,
            consistent_snapshot: false,
        }
    }

    /// MySQL 8.4's defaults.
    #[must_use]
    pub fn mysql() -> Self {
        Self {
            persona: Persona::MySql,
            snapshot_isolation: false,
            ..Self::mariadb()
        }
    }

    /// The same options at another level.
    #[must_use]
    pub fn with_isolation(mut self, isolation: Isolation) -> Self {
        self.isolation = isolation;
        self
    }

    /// Sets the wait from `innodb_lock_wait_timeout` seconds (100,000,000 or
    /// more waits forever).
    #[must_use]
    pub fn with_lock_wait_seconds(mut self, secs: u64) -> Self {
        self.lock_wait_timeout = (secs < 100_000_000).then(|| Duration::from_secs(secs));
        self
    }
}

impl Default for TxnOptions {
    fn default() -> Self {
        Self::mariadb()
    }
}

/// Per-unit settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitConfig {
    /// Release locks when the commit is queued (true), or when it is
    /// durable (false: `InnoDB`'s hold-through-sync; benchmarks compare).
    pub early_lock_release: bool,
    /// Versions younger than this are never collected
    /// (`engine.mvcc.min_retention_s`).
    pub min_retention_ms: u64,
    /// Lock memory per transaction before 1206 (`engine.txn.lock_mem_mb`).
    pub max_lock_bytes_per_txn: usize,
    /// Write-set bytes per transaction (`engine.txn.max_bytes`).
    pub max_txn_bytes: usize,
}

impl Default for UnitConfig {
    fn default() -> Self {
        Self {
            early_lock_release: true,
            min_retention_ms: 60_000,
            max_lock_bytes_per_txn: 64 << 20,
            max_txn_bytes: 2 << 30,
        }
    }
}

/// Where a transaction's commits go: the engine's log and store, or the
/// in-memory test path.
pub trait CommitPath: Send + Sync {
    /// The store reads come from.
    fn store(&self) -> &dyn SortedStore;
    /// The unit's transaction state.
    fn shared(&self) -> &TxnShared;
    /// Queues a log payload; the ticket carries its index, assigned now.
    ///
    /// # Errors
    /// The log is closed.
    fn propose(&self, payload: Vec<u8>, durability: Option<Durability>) -> Result<DurableTicket>;
    /// The highest index applied to the store.
    fn applied_index(&self) -> u64;
    /// Tells the store side the oldest timestamp readers may still need.
    fn set_gc_watermark(&self, _ts: Ts) {}
    /// The highest commit timestamp recovered from the log, if this path
    /// leaves raising the clock to the transaction layer.
    fn recovered_max_ts(&self) -> Option<Ts> {
        None
    }
}

impl CommitPath for Engine {
    fn store(&self) -> &dyn SortedStore {
        Engine::store(self).as_ref()
    }

    fn shared(&self) -> &TxnShared {
        self.txn_shared()
    }

    fn propose(&self, payload: Vec<u8>, durability: Option<Durability>) -> Result<DurableTicket> {
        let d = durability.unwrap_or(self.config().default_durability);
        self.consensus()
            .propose(self.unit(), payload, d)
            .map(DurableTicket::Log)
    }

    fn applied_index(&self) -> u64 {
        self.consensus().applied_index(self.unit())
    }

    fn set_gc_watermark(&self, ts: Ts) {
        Engine::set_gc_watermark(self, ts);
    }
}
