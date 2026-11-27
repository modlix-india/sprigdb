//! The sorted store: an ordered byte keyspace in column families.
//!
//! [`RocksStore`] is the production implementation; [`MemStore`] is the
//! in-memory fake with the same semantics, used by the deterministic
//! simulator and by model tests that run one suite against both.
//!
//! The store knows nothing of versions or transactions. Versioned keys are
//! plain keys to it (`logical | !ts`); [`SortedStore::get_version`] is the
//! one helper that understands the suffix, because a point read at a
//! timestamp is the hottest path in the engine.
//!
//! Reads are synchronous and may be asked to stay in memory
//! ([`ReadTier::CacheOnly`]): a read that would touch the disk then returns
//! [`Read::WouldBlock`], and the caller retries it off the async worker.

mod mem;
mod rocks;

use std::sync::Arc;

use bytes::Bytes;
use sprig_common::{Result, SprigError};

pub use mem::MemStore;
pub use rocks::{RocksOptions, RocksStore};

use crate::keys::{self, KeyRange, Ts, VERSION_LEN};

/// A shared [`CompactionGc`] rule.
pub type GcRule = Arc<dyn CompactionGc>;

/// A column family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Cf {
    /// Rows, documents, Redis data, index entries and unit bookkeeping. The
    /// commit log is its write-ahead log; RocksDB's WAL is off for it.
    Data,
    /// Node-local, unreplicated state, written with RocksDB's WAL on.
    Local,
}

impl Cf {
    /// Every column family the engine opens.
    pub const ALL: [Cf; 2] = [Cf::Data, Cf::Local];

    /// RocksDB's name for it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Cf::Data => "data",
            Cf::Local => "local",
        }
    }

    /// The byte that names it in the log payload.
    #[must_use]
    pub fn code(self) -> u8 {
        match self {
            Cf::Data => 0,
            Cf::Local => 1,
        }
    }

    /// The column family of a payload byte.
    #[must_use]
    pub fn from_code(c: u8) -> Option<Cf> {
        match c {
            0 => Some(Cf::Data),
            1 => Some(Cf::Local),
            _ => None,
        }
    }
}

/// The outcome of a read that may refuse to block on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub enum Read<T> {
    /// The answer.
    Ready(T),
    /// The data isn't in memory and the caller asked not to wait for disk.
    WouldBlock,
}

impl<T> Read<T> {
    /// The answer, if there is one.
    pub fn ready(self) -> Option<T> {
        match self {
            Read::Ready(t) => Some(t),
            Read::WouldBlock => None,
        }
    }

    /// Maps the answer.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Read<U> {
        match self {
            Read::Ready(t) => Read::Ready(f(t)),
            Read::WouldBlock => Read::WouldBlock,
        }
    }

    /// The answer of a read made with [`ReadTier::All`], which never
    /// returns `WouldBlock`.
    ///
    /// # Errors
    /// The read did return `WouldBlock`.
    pub fn expect_ready(self) -> Result<T> {
        self.ready()
            .ok_or_else(|| SprigError::internal("a blocking read returned WouldBlock"))
    }
}

/// Where a read may look.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadTier {
    /// Memtables, the block cache, and the disk.
    #[default]
    All,
    /// Memtables and the block cache only: anything else is `WouldBlock`.
    CacheOnly,
}

/// How a write uses RocksDB's own write-ahead log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    /// No WAL: the commit log is the WAL (every `Cf::Data` write).
    Off,
    /// WAL, not synced.
    On,
    /// WAL, synced before returning.
    Sync,
}

/// One operation of a [`StoreBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    /// Sets `key` to `value`.
    Put {
        /// Column family.
        cf: Cf,
        /// Key.
        key: Vec<u8>,
        /// Value.
        value: Vec<u8>,
    },
    /// Removes `key`.
    Delete {
        /// Column family.
        cf: Cf,
        /// Key.
        key: Vec<u8>,
    },
    /// Removes every key in `[start, end)`.
    DeleteRange {
        /// Column family.
        cf: Cf,
        /// Inclusive start.
        start: Vec<u8>,
        /// Exclusive end.
        end: Vec<u8>,
    },
}

/// Writes applied atomically.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreBatch {
    ops: Vec<BatchOp>,
    bytes: usize,
}

impl StoreBatch {
    /// An empty batch.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a put.
    pub fn put(&mut self, cf: Cf, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        let (key, value) = (key.into(), value.into());
        self.bytes += key.len() + value.len();
        self.ops.push(BatchOp::Put { cf, key, value });
    }

    /// Adds a delete.
    pub fn delete(&mut self, cf: Cf, key: impl Into<Vec<u8>>) {
        let key = key.into();
        self.bytes += key.len();
        self.ops.push(BatchOp::Delete { cf, key });
    }

    /// Adds a range delete of `[start, end)`. An empty or inverted range
    /// deletes nothing.
    pub fn delete_range(&mut self, cf: Cf, start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) {
        let (start, end) = (start.into(), end.into());
        self.bytes += start.len() + end.len();
        self.ops.push(BatchOp::DeleteRange { cf, start, end });
    }

    /// The operations, in order.
    #[must_use]
    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    /// Whether the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Key and value bytes in the batch.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether any operation touches `cf`.
    #[must_use]
    pub fn touches(&self, cf: Cf) -> bool {
        self.ops.iter().any(|o| match o {
            BatchOp::Put { cf: c, .. }
            | BatchOp::Delete { cf: c, .. }
            | BatchOp::DeleteRange { cf: c, .. } => *c == cf,
        })
    }
}

/// Iterator settings.
#[derive(Debug, Clone, Default)]
pub struct IterOpts {
    /// Inclusive lower bound; the iterator never goes below it.
    pub lower: Option<Vec<u8>>,
    /// Exclusive upper bound.
    pub upper: Option<Vec<u8>>,
    /// Stop as soon as a key leaves the prefix (the logical key, for
    /// versioned keys) of the key first sought. Uses the prefix Bloom filter.
    pub prefix_same_as_start: bool,
    /// Memory only, or disk too.
    pub tier: ReadTier,
}

impl IterOpts {
    /// Bounds from a [`KeyRange`].
    #[must_use]
    pub fn range(r: &KeyRange) -> Self {
        Self {
            lower: Some(r.start.clone()),
            upper: r.end.clone(),
            ..Self::default()
        }
    }
}

/// A cursor over a consistent view of one column family.
///
/// After any positioning call, check [`StoreIter::valid`]; when it is
/// false, [`StoreIter::status`] tells the end of the range from an error or
/// a cache-only read that needed the disk.
pub trait StoreIter {
    /// Positions at the first key `>= key`.
    fn seek(&mut self, key: &[u8]);
    /// Positions at the last key `<= key`.
    fn seek_for_prev(&mut self, key: &[u8]);
    /// Positions at the first key in bounds.
    fn seek_to_first(&mut self);
    /// Positions at the last key in bounds.
    fn seek_to_last(&mut self);
    /// Whether the iterator is on a key.
    fn valid(&self) -> bool;
    /// The current key. Only call while `valid()`.
    fn key(&self) -> &[u8];
    /// The current value. Only call while `valid()`.
    fn value(&self) -> &[u8];
    /// Moves to the next key.
    fn next(&mut self);
    /// Moves to the previous key.
    fn prev(&mut self);
    /// `Ready(())` at a normal end, `WouldBlock` if a cache-only iterator
    /// needed the disk, or the error.
    ///
    /// # Errors
    /// The store failed.
    fn status(&self) -> Result<Read<()>>;
}

/// Store-level counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Bytes in memtables.
    pub memtable_bytes: u64,
    /// Bytes the block cache holds (memtable reservations included when
    /// they are charged to the cache).
    pub block_cache_bytes: u64,
    /// Bytes of index and filter blocks pinned outside the cache.
    pub table_readers_bytes: u64,
    /// Bytes of live SST files.
    pub sst_bytes: u64,
    /// Approximate number of keys.
    pub keys: u64,
}

/// Garbage collection of old versions while the store compacts `Cf::Data`.
///
/// The store calls [`CompactionGc::start`] once per compaction and feeds the
/// returned decider every entry of that compaction in ascending key order;
/// the decider returns `true` to drop the entry. The rule itself (which
/// versions no reader can still see) belongs to the transaction layer; the
/// store only runs it. Entries a compaction doesn't visit are kept, so a
/// rule that keeps the newest version at or below its watermark stays
/// correct when a compaction sees only part of a key's versions.
pub trait CompactionGc: Send + Sync {
    /// A decider for one compaction. `bottommost` is true when the
    /// compaction covers every file of the column family, so nothing older
    /// sits below what it sees (delete markers at the floor may go).
    fn start(&self, bottommost: bool) -> GcDecider;
}

/// A per-compaction GC decider: `true` drops the entry.
pub type GcDecider = Box<dyn FnMut(&[u8], &[u8]) -> bool + Send>;

/// [`SortedStore::get_versions`]' answer: per key, its newest version's
/// timestamp and stored value, if any.
pub type Versions = Vec<Option<(Ts, Bytes)>>;

/// The ordered keyspace. RocksDB in production, [`MemStore`] in tests and
/// the simulator. Both pass the same model tests.
pub trait SortedStore: Send + Sync {
    /// The value of exactly `key`.
    ///
    /// # Errors
    /// The store failed.
    fn get(&self, cf: Cf, key: &[u8], tier: ReadTier) -> Result<Read<Option<Bytes>>>;

    /// Like [`SortedStore::get`], copying the value into `out` (cleared
    /// first) so a hot path can reuse one buffer. Returns whether the key
    /// exists.
    ///
    /// # Errors
    /// The store failed.
    fn get_into(
        &self,
        cf: Cf,
        key: &[u8],
        tier: ReadTier,
        out: &mut Vec<u8>,
    ) -> Result<Read<bool>> {
        out.clear();
        Ok(self.get(cf, key, tier)?.map(|v| match v {
            Some(v) => {
                out.extend_from_slice(&v);
                true
            }
            None => false,
        }))
    }

    /// The values of several keys, in the order given. Sorted keys share
    /// block reads.
    ///
    /// # Errors
    /// The store failed.
    fn multi_get(
        &self,
        cf: Cf,
        keys: &[&[u8]],
        tier: ReadTier,
    ) -> Result<Read<Vec<Option<Bytes>>>> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            match self.get(cf, k, tier)? {
                Read::Ready(v) => out.push(v),
                Read::WouldBlock => return Ok(Read::WouldBlock),
            }
        }
        Ok(Read::Ready(out))
    }

    /// The newest version of `logical` at or below `at`: its timestamp,
    /// with the stored value (header byte included) copied into `out`.
    /// `None` if no version is that old. Delete markers are returned like
    /// any value; the caller reads the header.
    ///
    /// # Errors
    /// The store failed.
    fn get_version(
        &self,
        cf: Cf,
        logical: &[u8],
        at: Ts,
        tier: ReadTier,
        out: &mut Vec<u8>,
    ) -> Result<Read<Option<Ts>>> {
        let seek = keys::versioned(logical, at);
        let mut it = self.iter(
            cf,
            IterOpts {
                prefix_same_as_start: true,
                tier,
                ..IterOpts::default()
            },
        );
        it.seek(&seek);
        if !it.valid() {
            return Ok(it.status()?.map(|()| None));
        }
        let k = it.key();
        if k.len() != logical.len() + VERSION_LEN || !k.starts_with(logical) {
            return Ok(Read::Ready(None));
        }
        let Some((_, ts)) = keys::split_version(k) else {
            return Ok(Read::Ready(None));
        };
        out.clear();
        out.extend_from_slice(it.value());
        Ok(Read::Ready(Some(ts)))
    }

    /// [`SortedStore::get_version`] for several logical keys, in the order
    /// given: each the newest version at or below `at` (timestamp and
    /// stored value, header byte included). Keys given in ascending order
    /// let a store share one cursor across them.
    ///
    /// # Errors
    /// The store failed.
    fn get_versions(
        &self,
        cf: Cf,
        logicals: &[&[u8]],
        at: Ts,
        tier: ReadTier,
    ) -> Result<Read<Versions>> {
        let mut out = Vec::with_capacity(logicals.len());
        let mut buf = Vec::new();
        for l in logicals {
            match self.get_version(cf, l, at, tier, &mut buf)? {
                Read::Ready(ts) => out.push(ts.map(|ts| (ts, Bytes::copy_from_slice(&buf)))),
                Read::WouldBlock => return Ok(Read::WouldBlock),
            }
        }
        Ok(Read::Ready(out))
    }

    /// Lets go of whatever the store keeps between reads to make them
    /// cheaper (cursors that pin memtables and files). The engine's
    /// checkpointer calls it on every tick.
    fn release_idle_reads(&self) {}

    /// A cursor over `cf`, consistent as of its creation.
    // A cursor that seeks both ways, as RocksDB's raw iterator does, not a
    // Rust `Iterator`; the name matches the store it mirrors.
    #[allow(clippy::iter_not_returning_iterator)]
    fn iter(&self, cf: Cf, opts: IterOpts) -> Box<dyn StoreIter + '_>;

    /// Applies a batch atomically.
    ///
    /// # Errors
    /// The store failed. A failed write leaves the store unchanged.
    fn write(&self, batch: &StoreBatch, wal: WalMode) -> Result<()>;

    /// Flushes every column family's memtables to disk, atomically across
    /// column families, and waits.
    ///
    /// # Errors
    /// The store failed.
    fn flush_all(&self) -> Result<()>;

    /// Compacts every column family over its whole range and waits, so the
    /// [`CompactionGc`] rule (if the store has one) visits every entry of
    /// `Cf::Data`. Tests and manual maintenance call it; normal compactions
    /// run in the background.
    ///
    /// # Errors
    /// The store failed.
    fn compact_all(&self) -> Result<()>;

    /// Approximate bytes stored in `range`.
    fn approx_size(&self, cf: Cf, range: &KeyRange) -> u64;

    /// Counters.
    fn stats(&self) -> StoreStats;
}

#[cfg(test)]
mod tests;
