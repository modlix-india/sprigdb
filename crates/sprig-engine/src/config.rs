//! The engine's settings.
//!
//! These are plain structs with defaults for a single node. They become
//! `engine.*` settings in sprig-settings once the node binary exists; until
//! then the caller fills them in.

use std::path::PathBuf;
use std::time::Duration;

use crate::budget::BudgetConfig;
use crate::log::{Durability, LogConfig};

/// Which sorted store backs the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// RocksDB under `<data_dir>/rocksdb`.
    Rocks,
    /// The in-memory fake: nothing survives a restart except what the log
    /// replays. Used by tests and the simulator.
    Mem,
}

/// Everything [`crate::Engine::open`] needs.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// The store's directory: `rocksdb/` and `raft/` live under it.
    pub data_dir: PathBuf,
    /// RocksDB or the in-memory fake.
    pub store: StoreKind,
    /// Memory budgets.
    pub budget: BudgetConfig,
    /// The commit log.
    pub log: LogConfig,
    /// The durability class of commits that don't name one (per database
    /// later; a session may only raise it).
    pub default_durability: Durability,
    /// RocksDB tuning that is not a budget.
    pub rocks: RocksTuning,
    /// How often the checkpointer flushes the store and trims the log.
    /// Zero turns the background checkpointer off (tests call
    /// [`crate::Engine::checkpoint`] themselves).
    pub checkpoint_interval: Duration,
    /// Trim the log once this many entries were applied since the last
    /// checkpoint, whatever the interval.
    pub checkpoint_entries: u64,
}

impl EngineConfig {
    /// Defaults for a node whose data lives in `data_dir`.
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            store: StoreKind::Rocks,
            budget: BudgetConfig::default(),
            log: LogConfig::default(),
            default_durability: Durability::Local,
            rocks: RocksTuning::default(),
            checkpoint_interval: Duration::from_secs(30),
            checkpoint_entries: 500_000,
        }
    }
}

/// RocksDB settings that are not memory budgets. The defaults are the
/// single-node profile: small blocks and prefix Bloom filters for point reads.
#[derive(Debug, Clone)]
pub struct RocksTuning {
    /// Background flush and compaction threads.
    pub background_jobs: i32,
    /// Data block size in bytes. Small blocks favour point reads.
    pub block_size: usize,
    /// Bloom filter bits per key (prefix filters on the logical key).
    pub bloom_bits_per_key: f64,
    /// Target SST file size in bytes.
    pub target_file_size: u64,
    /// One memtable's size in bytes; the write buffer manager caps the sum.
    pub write_buffer_size: usize,
    /// Collect RocksDB statistics (costs a few percent of CPU).
    pub statistics: bool,
    /// Skip the flush on close. Only crash simulations set this: with the
    /// WAL off, unflushed memtables are then lost exactly as in a crash.
    pub avoid_flush_on_close: bool,
}

impl Default for RocksTuning {
    fn default() -> Self {
        Self {
            background_jobs: 4,
            block_size: 8 * 1024,
            bloom_bits_per_key: 10.0,
            target_file_size: 64 << 20,
            write_buffer_size: 64 << 20,
            statistics: false,
            avoid_flush_on_close: false,
        }
    }
}
