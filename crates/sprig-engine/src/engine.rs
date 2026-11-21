//! The engine handle: the store, the commit log, recovery, budgets and the
//! checkpointer, for one node with one unit.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use sprig_common::{Result, SprigError};

use crate::budget::Budgets;
use crate::config::{EngineConfig, StoreKind};
use crate::keys;
use crate::log::{self, Consensus, LocalLog, LogEntry, LogStats, StateMachine, UnitId};
use crate::store::{
    BatchOp, Cf, CompactionGc, GcDecider, GcRule, MemStore, ReadTier, RocksOptions, RocksStore,
    SortedStore, StoreBatch, StoreStats, WalMode,
};
use crate::txn::{self, Txn, TxnOptions};

/// The node's single unit.
const UNIT: UnitId = UnitId(1);

/// The field of a unit's bookkeeping that holds its applied index.
const APPLIED: &[u8] = b"applied";

/// Applies committed log entries to the store: the payloads of a whole
/// group, plus the unit's applied index, in one batch with the WAL off.
struct StoreMachine {
    store: Arc<dyn SortedStore>,
    /// The highest version timestamp ever applied, kept in the unit's
    /// bookkeeping (`max_ts`) so a restarted clock starts above it even when
    /// the wall clock stepped back.
    max_ts: AtomicU64,
}

/// The field of a unit's bookkeeping that holds the highest version
/// timestamp applied.
const MAX_TS: &[u8] = b"max_ts";

fn read_u64(store: &dyn SortedStore, key: &[u8]) -> Result<u64> {
    match store.get(Cf::Data, key, ReadTier::All)?.expect_ready()? {
        None => Ok(0),
        Some(b) => Ok(u64::from_be_bytes(b.as_ref().try_into().map_err(|_| {
            SprigError::internal("storage: unit bookkeeping is corrupt")
        })?)),
    }
}

/// The version timestamp of a stored data key, if it is a versioned key
/// (engine bookkeeping under the reserved tenant is not).
fn version_of(key: &[u8]) -> Option<keys::Ts> {
    if key.len() < keys::PREFIX_LEN + keys::VERSION_LEN
        || key[..8] == keys::META_TENANT.to_be_bytes()
    {
        return None;
    }
    keys::split_version(key).map(|(_, ts)| ts)
}

impl StateMachine for StoreMachine {
    fn applied_index(&self, unit: UnitId) -> Result<u64> {
        read_u64(self.store.as_ref(), &keys::unit_meta_key(unit.0, APPLIED))
    }

    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()> {
        let Some(last) = entries.last() else {
            return Ok(());
        };
        let mut batch = StoreBatch::new();
        for e in entries {
            log::payload::decode_into(&e.data, &mut batch)?;
        }
        let batch_max = batch
            .ops()
            .iter()
            .filter_map(|o| match o {
                BatchOp::Put {
                    cf: Cf::Data, key, ..
                } => version_of(key),
                _ => None,
            })
            .max();
        batch.put(
            Cf::Data,
            keys::unit_meta_key(unit.0, APPLIED),
            last.index.to_be_bytes(),
        );
        if let Some(m) = batch_max {
            let top = self.max_ts.fetch_max(m.0, Ordering::AcqRel).max(m.0);
            batch.put(
                Cf::Data,
                keys::unit_meta_key(unit.0, MAX_TS),
                top.to_be_bytes(),
            );
        }
        self.store.write(&batch, WalMode::Off)
    }
}

/// The transaction layer's rule for dropping old versions in compactions,
/// installed with [`Engine::set_version_gc`].
pub trait VersionGc: Send + Sync {
    /// A decider for one compaction: it sees entries of `data` in ascending
    /// key order and returns `true` to drop one. No reader is older than
    /// `watermark`; `bottommost` means nothing older sits below what the
    /// compaction sees.
    fn start(&self, watermark: keys::Ts, bottommost: bool) -> GcDecider;
}

/// The store's GC rule: the installed [`VersionGc`] at the current
/// watermark, or keep everything.
#[derive(Default)]
struct GcSlot {
    watermark: AtomicU64,
    rule: RwLock<Option<Arc<dyn VersionGc>>>,
}

impl CompactionGc for GcSlot {
    fn start(&self, bottommost: bool) -> GcDecider {
        let rule = self
            .rule
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        match rule {
            Some(r) => r.start(keys::Ts(self.watermark.load(Ordering::Acquire)), bottommost),
            None => Box::new(|_, _| false),
        }
    }
}

/// The transaction layer's GC rule. It asks the transaction layer for the
/// watermark when a compaction starts (its oldest read view, the last
/// queued commit and the retention floor), so the watermark is fresh even
/// when commits have stopped; the value the transaction layer pushes is the
/// floor if the layer is gone.
struct TxnGc(std::sync::Weak<txn::TxnShared>);

impl VersionGc for TxnGc {
    fn start(&self, pushed: keys::Ts, bottommost: bool) -> GcDecider {
        let watermark = self.0.upgrade().map_or(pushed, |t| {
            let now_ms = t.now().physical_ms();
            t.gc_watermark(now_ms).max(pushed)
        });
        let mut filter = txn::gc::GcFilter::new(watermark, bottommost);
        Box::new(move |k, v| filter.decide(k, v) == txn::gc::GcDecision::Drop)
    }
}

struct Stop {
    stopped: Mutex<bool>,
    cv: Condvar,
}

struct Inner {
    cfg: EngineConfig,
    store: Arc<dyn SortedStore>,
    rocks: Option<Arc<RocksStore>>,
    log: Arc<LocalLog>,
    budgets: Budgets,
    unit: UnitId,
    /// The version GC rule compactions of `data` run, and its watermark.
    gc: Arc<GcSlot>,
    txn: Arc<txn::TxnShared>,
    checkpoints: AtomicU64,
    stop: Arc<Stop>,
    checkpointer: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = true;
        self.stop.cv.notify_all();
        if let Some(h) = self
            .checkpointer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            // The checkpointer may hold the last reference itself; it then
            // exits on its own after this drop.
            if h.thread().id() != std::thread::current().id() {
                let _ = h.join();
            }
        }
        // Drain and sync the log before the store closes behind it.
        self.log.close();
    }
}

/// The storage engine of one node. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("data_dir", &self.inner.cfg.data_dir)
            .field("store", &self.inner.cfg.store)
            .finish_non_exhaustive()
    }
}

/// What [`Engine::stats`] reports.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    /// The store's counters.
    pub store: StoreStats,
    /// The log's counters.
    pub log: LogStats,
    /// Bytes of log files.
    pub log_disk_bytes: u64,
    /// The unit's applied index.
    pub applied_index: u64,
    /// The unit's synced index.
    pub durable_index: u64,
    /// Checkpoints taken.
    pub checkpoints: u64,
    /// Bytes held by the transaction pool.
    pub txn_bytes: u64,
    /// Bytes held by the query pool.
    pub query_bytes: u64,
}

impl Engine {
    /// Opens the engine in `cfg.data_dir`: opens the store, replays the log
    /// into it (recovery), and starts the log writer and the checkpointer.
    ///
    /// # Errors
    /// The store or the log failed to open, or replay failed.
    pub fn open(cfg: EngineConfig) -> Result<Engine> {
        let budgets = Budgets::new(&cfg.budget);
        let gc = Arc::new(GcSlot::default());
        let rule: GcRule = Arc::clone(&gc) as GcRule;
        let (store, rocks): (Arc<dyn SortedStore>, Option<Arc<RocksStore>>) = match cfg.store {
            StoreKind::Rocks => {
                let r = Arc::new(RocksStore::open(
                    &cfg.data_dir.join("rocksdb"),
                    &RocksOptions {
                        block_cache_bytes: usize::try_from(budgets.block_cache_bytes)
                            .unwrap_or(usize::MAX),
                        memtable_bytes: usize::try_from(budgets.memtable_bytes)
                            .unwrap_or(usize::MAX),
                        tuning: cfg.rocks.clone(),
                        gc: Some(rule),
                    },
                )?);
                (r.clone(), Some(r))
            }
            StoreKind::Mem => (Arc::new(MemStore::with_gc(Some(rule))), None),
        };
        let max_ts = read_u64(store.as_ref(), &keys::unit_meta_key(UNIT.0, MAX_TS))?;
        let sm = Arc::new(StoreMachine {
            store: Arc::clone(&store),
            max_ts: AtomicU64::new(max_ts),
        });
        let log = Arc::new(LocalLog::open(
            cfg.data_dir.join("raft"),
            cfg.log.clone(),
            Arc::clone(&sm) as Arc<dyn StateMachine>,
        )?);
        let stop = Arc::new(Stop {
            stopped: Mutex::new(false),
            cv: Condvar::new(),
        });
        // After replay: the clock starts above every version in the store.
        let txn_shared = Arc::new(txn::TxnShared::new());
        txn_shared.observe(keys::Ts(sm.max_ts.load(Ordering::Acquire)));
        // Compactions drop versions by the transaction layer's rule, at the
        // watermark it computes when each compaction starts.
        let txn_gc = Arc::new(TxnGc(Arc::downgrade(&txn_shared)));
        let engine = Engine {
            inner: Arc::new(Inner {
                txn: txn_shared,
                store,
                rocks,
                log,
                budgets,
                unit: UNIT,
                gc,
                checkpoints: AtomicU64::new(0),
                stop,
                checkpointer: Mutex::new(None),
                cfg,
            }),
        };
        engine.set_version_gc(txn_gc);
        if !engine.inner.cfg.checkpoint_interval.is_zero()
            && engine.inner.cfg.store != StoreKind::Mem
        {
            let h = spawn_checkpointer(&engine)?;
            *engine
                .inner
                .checkpointer
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(h);
        }
        Ok(engine)
    }

    /// Starts a transaction.
    ///
    /// # Errors
    /// The transaction layer refused (for example, its memory budget is
    /// spent).
    pub fn begin(&self, opts: TxnOptions) -> Result<Txn> {
        Txn::begin(self.clone(), opts)
    }

    /// The sorted store.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn SortedStore> {
        &self.inner.store
    }

    /// The RocksDB store, when the engine runs on RocksDB.
    #[must_use]
    pub fn rocks(&self) -> Option<&Arc<RocksStore>> {
        self.inner.rocks.as_ref()
    }

    /// The commit path.
    #[must_use]
    pub fn consensus(&self) -> &dyn Consensus {
        self.inner.log.as_ref()
    }

    /// The commit log itself.
    #[must_use]
    pub fn log(&self) -> &LocalLog {
        &self.inner.log
    }

    /// The single unit the node runs.
    #[must_use]
    pub fn unit(&self) -> UnitId {
        self.inner.unit
    }

    /// The memory budgets.
    #[must_use]
    pub fn budgets(&self) -> &Budgets {
        &self.inner.budgets
    }

    /// The settings the engine was opened with.
    #[must_use]
    pub fn config(&self) -> &EngineConfig {
        &self.inner.cfg
    }

    /// The transaction layer's shared state.
    #[must_use]
    pub fn txn_shared(&self) -> &txn::TxnShared {
        &self.inner.txn
    }

    /// Sets the oldest timestamp any reader may still need.
    pub fn set_gc_watermark(&self, ts: keys::Ts) {
        self.inner.gc.watermark.fetch_max(ts.0, Ordering::AcqRel);
    }

    /// The GC watermark.
    #[must_use]
    pub fn gc_watermark(&self) -> keys::Ts {
        keys::Ts(self.inner.gc.watermark.load(Ordering::Acquire))
    }

    /// Installs the transaction layer's version GC rule. Until one is
    /// installed, compactions keep every version.
    pub fn set_version_gc(&self, rule: Arc<dyn VersionGc>) {
        *self
            .inner
            .gc
            .rule
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(rule);
    }

    /// Flushes the store and trims the log below what it holds. Returns the
    /// number of log entries trimmed. On the in-memory store it does nothing:
    /// that store keeps nothing across a restart, so the log is its only copy
    /// and must not be trimmed.
    ///
    /// # Errors
    /// The log sync, the flush or the purge failed.
    pub fn checkpoint(&self) -> Result<u64> {
        if self.inner.cfg.store == StoreKind::Mem {
            return Ok(0);
        }
        let store = Arc::clone(&self.inner.store);
        let n = self.inner.log.checkpoint(&move || store.flush_all())?;
        self.inner.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> EngineStats {
        let i = &self.inner;
        EngineStats {
            store: i.store.stats(),
            log: i.log.stats(),
            log_disk_bytes: i.log.disk_bytes(),
            applied_index: i.log.applied_index(i.unit),
            durable_index: i.log.durable_index(i.unit),
            checkpoints: i.checkpoints.load(Ordering::Relaxed),
            txn_bytes: i.budgets.txn.used(),
            query_bytes: i.budgets.query.used(),
        }
    }

    /// The data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.inner.cfg.data_dir
    }
}

fn spawn_checkpointer(engine: &Engine) -> Result<JoinHandle<()>> {
    let weak = Arc::downgrade(&engine.inner);
    let stop = Arc::clone(&engine.inner.stop);
    let interval = engine.inner.cfg.checkpoint_interval;
    let every = engine.inner.cfg.checkpoint_entries;
    let every_bytes = engine.inner.cfg.checkpoint_log_bytes;
    std::thread::Builder::new()
        .name("sprig-checkpoint".into())
        .spawn(move || {
            let tick = interval.min(Duration::from_secs(1));
            let mut last_applied = 0u64;
            let mut last_bytes = 0u64;
            let mut last_time = Instant::now();
            loop {
                {
                    let g = stop.stopped.lock().unwrap_or_else(PoisonError::into_inner);
                    let (g, _) = stop
                        .cv
                        .wait_timeout(g, tick)
                        .unwrap_or_else(PoisonError::into_inner);
                    if *g {
                        return;
                    }
                }
                let Some(inner) = weak.upgrade() else { return };
                inner.store.release_idle_reads();
                let engine = Engine { inner };
                let applied = engine.consensus().applied_index(engine.unit());
                let bytes = engine.inner.log.stats().bytes;
                let due = applied.saturating_sub(last_applied) >= every
                    || (every_bytes > 0 && bytes.saturating_sub(last_bytes) >= every_bytes)
                    || (applied > last_applied && last_time.elapsed() >= interval);
                if due {
                    match engine.checkpoint() {
                        Ok(_) => {
                            last_applied = applied;
                            last_bytes = bytes;
                            last_time = Instant::now();
                        }
                        Err(e) => tracing::warn!(error = %e, "engine checkpoint failed"),
                    }
                }
            }
        })
        .map_err(|e| SprigError::internal("engine: spawn checkpointer").with_source(e))
}

#[cfg(test)]
mod tests;
