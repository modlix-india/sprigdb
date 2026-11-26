//! What the Redis commands need from the engine: one unit's keys, locked
//! per batch, read with the unit's pending writes, and one commit whose
//! durability the replies wait for.
//!
//! The engine's transactions implement it, and
//! [`crate::mem::MemUnit`] implements it in memory for the model tests.

use std::future::Future;

use bytes::Bytes;

/// An engine failure. The batch it ended changed nothing that any client
/// was told about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    /// What went wrong.
    pub message: String,
    /// The same batch may succeed if sent again (a lost leadership, a
    /// deadlock victim, a full write buffer).
    pub retryable: bool,
}

impl StoreError {
    /// A failure that sending again won't fix.
    #[must_use]
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    /// A failure that sending again may fix.
    #[must_use]
    pub fn retry(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

/// A store result.
pub type StoreResult<T> = Result<T, StoreError>;

/// One unit of the engine (E0 has one per node).
pub trait Unit: Send + Sync + 'static {
    /// A batch of commands' reads and writes.
    type Batch: Batch;

    /// Starts a batch holding `keys` (sorted, without repeats) exclusively
    /// until its commit is queued, waiting for other batches that hold any
    /// of them. Every key a batch writes is one of `keys`, or lives under
    /// one of them (a collection's elements, its expiry entry), so batches
    /// that share no key never conflict and batches that do run one after
    /// another, in the order they got their keys. A batch may also read
    /// keys it doesn't hold: it sees their latest committed or queued
    /// version.
    fn begin(&self, keys: Vec<Bytes>) -> impl Future<Output = StoreResult<Self::Batch>> + Send;

    /// The leader epoch under which this node leads the unit holding
    /// `key` (0: it doesn't). Memory caches of stored state (uid blocks)
    /// built under another epoch are dropped.
    fn leader_epoch(&self, key: &[u8]) -> u64 {
        let _ = key;
        1
    }

    /// The unit's clock, in ms since the epoch: it decides expiry.
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }
}

/// What a reply waits for: the batch's commit, and every version it
/// read, durable.
pub trait Ticket: Clone + Send + Sync + 'static {
    /// Waits until a reply may go out.
    ///
    /// # Errors
    /// The log failed first: the outcome is unknown, and must not be
    /// reported as success.
    fn durable(&self) -> impl Future<Output = StoreResult<()>> + Send;
}

/// One batch: reads see the latest version of a key, the batch's own
/// writes first, then writes other batches have committed or queued.
pub trait Batch: Send {
    /// Its commit's ticket.
    type Ticket: Ticket;

    /// The latest value of `key`.
    ///
    /// # Errors
    /// The engine's.
    fn get(&mut self, key: &[u8]) -> StoreResult<Option<Bytes>>;

    /// Up to `limit` entries with `start <= key < end`, in key order.
    ///
    /// # Errors
    /// The engine's.
    fn scan(&mut self, start: &[u8], end: &[u8], limit: usize) -> StoreResult<Vec<(Bytes, Bytes)>>;

    /// Up to `limit` entries with `start <= key < end`, last key first.
    ///
    /// # Errors
    /// The engine's.
    fn scan_rev(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>>;

    /// Like [`Batch::scan`], but every entry at its latest version, even
    /// one written after the batch's first read (for deleting what is
    /// there now).
    ///
    /// # Errors
    /// The engine's.
    fn scan_latest(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        self.scan(start, end, limit)
    }

    /// Writes `key`.
    ///
    /// # Errors
    /// The engine's (a write set over its budget).
    fn put(&mut self, key: Bytes, value: Bytes) -> StoreResult<()>;

    /// Deletes `key`.
    ///
    /// # Errors
    /// The engine's.
    fn delete(&mut self, key: Bytes) -> StoreResult<()>;

    /// Takes `keys` too (sorted, none held yet), waiting for other batches
    /// as [`Unit::begin`] does: a script's batch takes each key when one of
    /// its commands first names it.
    ///
    /// # Errors
    /// The engine's; a deadlock or a lock wait that ran out is retryable.
    fn hold(&mut self, keys: Vec<Bytes>) -> impl Future<Output = StoreResult<()>> + Send;

    /// From now on, every read sees each key's latest version (a script's
    /// batch, which holds every key its commands name before they run).
    fn read_latest(&mut self) {}

    /// Adds an opaque note for `tenant`'s unit to the commit (keyspace
    /// notifications' event notes): it goes with the batch's writes or not
    /// at all.
    ///
    /// # Errors
    /// The engine's.
    fn note(&mut self, tenant: u64, note: Vec<u8>) -> StoreResult<()> {
        let _ = (tenant, note);
        Ok(())
    }

    /// Queues the batch's writes as one commit and releases its keys. The
    /// ticket says when the writes, and every version the batch read, are
    /// durable: only then may a reply go out.
    ///
    /// # Errors
    /// The engine's; the batch then changed nothing.
    fn commit(self) -> StoreResult<Self::Ticket>;
}
