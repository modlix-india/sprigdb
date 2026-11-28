//! The engine handle: the store, the commit log, recovery, budgets and the
//! checkpointer, for one node with many units.
//!
//! **Units.** Each tenant is its own unit: its own log (a raft-engine
//! group, one Raft group once replication is built), applied index and
//! commit latch. Tenant `t` is unit `t`. The node's own unit
//! ([`Engine::unit`]) carries what no tenant owns. Every key belongs to
//! exactly one unit ([`Engine::unit_of_key`]), and only that unit's log
//! writes it, so units replay independently. One clock, one lock table
//! and one set of read views serve every unit of the node.

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

/// The node's own unit: engine bookkeeping that no tenant owns, and what
/// is still kept per node rather than per tenant.
pub const NODE_UNIT: UnitId = UnitId(keys::META_TENANT - 1);

/// Before units, a node had one unit, numbered 1, holding every tenant.
const LEGACY_UNIT: UnitId = UnitId(1);

/// The node bookkeeping field that records the data layout.
const LAYOUT: &[u8] = b"layout";
/// Units per tenant (layout 2).
const LAYOUT_UNITS: u64 = 2;
/// Queue messages in their tenants' ranges (layout 3, the current one).
const LAYOUT_QUEUES: u64 = 3;

/// How often the cross-unit resolver thread looks for unfinished commits.
const CROSS_RESOLVE_EVERY: std::time::Duration = std::time::Duration::from_millis(10);

/// How long a read on a unit's new leader waits for the commits across
/// units its predecessors left unfinished there.
const CROSS_READ_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// The field of a unit's bookkeeping that holds its applied index.
const APPLIED: &[u8] = b"applied";

/// Applies committed log entries to the store: the payloads of a whole
/// group, plus the unit's applied index, in one batch with the WAL off.
struct StoreMachine {
    store: Arc<dyn SortedStore>,
    /// Apply observers ([`crate::observe`]). Held shared across a whole
    /// apply, so registering one sees either all of an apply or none.
    observers: RwLock<Vec<Arc<dyn crate::observe::ApplyObserver>>>,
    /// Each unit's last index applied to the store (set inside the apply).
    last_applied: RwLock<std::collections::HashMap<UnitId, u64>>,
    /// The highest version timestamp ever applied, kept in the unit's
    /// bookkeeping (`max_ts`) so a restarted clock starts above it even when
    /// the wall clock stepped back.
    max_ts: AtomicU64,
    /// Per unit, the highest version timestamp applied since the engine
    /// opened (what a read view on a replica that doesn't lead the unit
    /// takes: see [`Engine::follower_read`]).
    unit_ts: RwLock<std::collections::HashMap<UnitId, u64>>,
    /// Per unit, the prepare records of commits across units this replica
    /// has applied and not yet seen finished (the finishing entry deletes
    /// the record). While a unit has one, a commit that is already
    /// acknowledged may still be missing from the unit here, at a timestamp
    /// below versions it already holds: a replica that doesn't lead the
    /// unit doesn't open a read view on it until the record is gone.
    prepared: Mutex<std::collections::HashMap<UnitId, std::collections::BTreeSet<Vec<u8>>>>,
    /// How many units have one (read without the latch).
    prepared_units: std::sync::atomic::AtomicUsize,
}

/// What an applied batch did to prepare records: per record, its unit, its
/// key and whether it was written (else deleted); and the ranges deleted.
#[derive(Default)]
struct PrepareOps {
    records: Vec<(UnitId, Vec<u8>, bool)>,
    ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Every prepare record in the store, by unit (`only`: that unit's).
fn scan_prepared(
    store: &dyn SortedStore,
    only: Option<UnitId>,
) -> Result<std::collections::HashMap<UnitId, std::collections::BTreeSet<Vec<u8>>>> {
    let mut out: std::collections::HashMap<UnitId, std::collections::BTreeSet<Vec<u8>>> =
        std::collections::HashMap::new();
    let all = keys::unit_meta_key(0, b"");
    let units = &all[..all.len() - 8];
    let prefix = match only {
        Some(u) => keys::unit_meta_key(u.0, crate::txn::CROSS_PREPARE),
        None => units.to_vec(),
    };
    let mut it = store.iter(
        Cf::Data,
        crate::store::IterOpts::range(&keys::KeyRange::prefix(&prefix)),
    );
    it.seek_to_first();
    while it.valid() {
        let rest = &it.key()[units.len()..];
        if rest.len() >= 8 && rest[8..].starts_with(crate::txn::CROSS_PREPARE) {
            let unit = UnitId(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8])));
            out.entry(unit).or_default().insert(it.key().to_vec());
        }
        it.next();
    }
    it.status()?.expect_ready()?;
    Ok(out)
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
/// (engine bookkeeping and queue messages are not).
fn version_of(key: &[u8]) -> Option<keys::Ts> {
    if key.len() < keys::PREFIX_LEN + keys::VERSION_LEN || keys::is_unversioned(key) {
        return None;
    }
    keys::split_version(key).map(|(_, ts)| ts)
}

impl StoreMachine {
    /// The prepare records `unit` holds here now (see `prepared`).
    fn prepared_of(&self, unit: UnitId) -> Vec<Vec<u8>> {
        if self.prepared_units.load(Ordering::Acquire) == 0 {
            return Vec::new();
        }
        self.prepared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether `unit` still holds any of `records`.
    fn any_prepared(&self, unit: UnitId, records: &[Vec<u8>]) -> bool {
        self.prepared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .is_some_and(|s| records.iter().any(|r| s.contains(r)))
    }

    /// Folds an applied batch's prepare records in.
    fn note_prepares(&self, ops: PrepareOps) {
        let mut p = self.prepared.lock().unwrap_or_else(PoisonError::into_inner);
        for (start, end) in &ops.ranges {
            for set in p.values_mut() {
                set.retain(|k| k.as_slice() < start.as_slice() || k.as_slice() >= end.as_slice());
            }
        }
        for (unit, key, put) in ops.records {
            if put {
                p.entry(unit).or_default().insert(key);
            } else if let Some(set) = p.get_mut(&unit) {
                set.remove(&key);
            }
        }
        p.retain(|_, set| !set.is_empty());
        self.prepared_units.store(p.len(), Ordering::Release);
    }

    /// Reads `unit`'s prepare records back from the store (after its
    /// snapshot was ingested).
    fn reload_prepared(&self, unit: UnitId) {
        let found = scan_prepared(self.store.as_ref(), Some(unit)).unwrap_or_default();
        let mut p = self.prepared.lock().unwrap_or_else(PoisonError::into_inner);
        p.remove(&unit);
        p.extend(found);
        self.prepared_units.store(p.len(), Ordering::Release);
    }

    /// `unit`'s last applied index.
    fn last_applied(&self, unit: UnitId) -> u64 {
        if let Some(i) = self
            .last_applied
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
        {
            return *i;
        }
        read_u64(self.store.as_ref(), &keys::unit_meta_key(unit.0, APPLIED)).unwrap_or(0)
    }
}

impl StateMachine for StoreMachine {
    fn applied_index(&self, unit: UnitId) -> Result<u64> {
        read_u64(self.store.as_ref(), &keys::unit_meta_key(unit.0, APPLIED))
    }

    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()> {
        self.apply_group(&[(unit, entries)])
    }

    /// Every unit's part of a group in one store batch: a checkpoint's
    /// flush sees all of a write across units or none of it, so the store
    /// can't keep one unit's half of a relaxed commit whose log tail a
    /// crash then loses.
    fn apply_group(&self, parts: &[(UnitId, &[LogEntry])]) -> Result<()> {
        let observers = self
            .observers
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let mut batch = StoreBatch::new();
        let mut lasts = Vec::with_capacity(parts.len());
        let mut maxes = Vec::with_capacity(parts.len());
        let mut prepares = PrepareOps::default();
        for (unit, entries) in parts {
            let Some(last) = entries.last() else {
                continue;
            };
            let from = batch.ops().len();
            for e in *entries {
                log::payload::decode_into(&e.data, &mut batch)?;
            }
            let tp = keys::unit_meta_key(unit.0, crate::txn::CROSS_PREPARE);
            for o in &batch.ops()[from..] {
                match o {
                    BatchOp::Put {
                        cf: Cf::Data, key, ..
                    } if key.starts_with(&tp) => prepares.records.push((*unit, key.clone(), true)),
                    BatchOp::Delete { cf: Cf::Data, key } if key.starts_with(&tp) => {
                        prepares.records.push((*unit, key.clone(), false));
                    }
                    BatchOp::DeleteRange {
                        cf: Cf::Data,
                        start,
                        end,
                    } => prepares.ranges.push((start.clone(), end.clone())),
                    _ => {}
                }
            }
            let part_max = batch.ops()[from..]
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
            if let Some(m) = part_max {
                maxes.push((*unit, m.0));
                let top = self.max_ts.fetch_max(m.0, Ordering::AcqRel).max(m.0);
                batch.put(
                    Cf::Data,
                    keys::unit_meta_key(unit.0, MAX_TS),
                    top.to_be_bytes(),
                );
            }
            lasts.push((*unit, last.index));
        }
        if lasts.is_empty() {
            return Ok(());
        }
        self.store.write(&batch, WalMode::Off)?;
        if !prepares.records.is_empty()
            || (!prepares.ranges.is_empty() && self.prepared_units.load(Ordering::Acquire) > 0)
        {
            self.note_prepares(prepares);
        }
        // Published only once the batch is in the store: a read on a copy
        // that finds a unit's timestamp at or above its token reads the
        // store without waiting for anything else.
        if !maxes.is_empty() {
            let mut ut = self.unit_ts.write().unwrap_or_else(PoisonError::into_inner);
            for (u, m) in maxes {
                ut.entry(u).and_modify(|t| *t = (*t).max(m)).or_insert(m);
            }
        }
        {
            let mut la = self
                .last_applied
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for (u, i) in &lasts {
                la.insert(*u, *i);
            }
        }
        for (unit, entries) in parts {
            if entries.is_empty() {
                continue;
            }
            for o in observers.iter() {
                o.applied(*unit, entries);
            }
        }
        Ok(())
    }
}

/// The `local` key holding an observer's log floor.
fn floor_key(unit: UnitId, name: &str) -> Vec<u8> {
    let mut f = b"floor/".to_vec();
    f.extend_from_slice(name.as_bytes());
    keys::unit_meta_key(unit.0, &f)
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
    sm: Arc<StoreMachine>,
    /// Observers' log floors, by unit and name ([`crate::observe`]).
    floors: Mutex<std::collections::BTreeMap<(UnitId, String), u64>>,
    store: Arc<dyn SortedStore>,
    rocks: Option<Arc<RocksStore>>,
    log: Arc<LocalLog>,
    /// The commit path: the local log, or the replicated one for tenant
    /// units of a replicated engine.
    router: crate::repl::Router,
    budgets: Budgets,
    unit: UnitId,
    /// The version GC rule compactions of `data` run, and its watermark.
    gc: Arc<GcSlot>,
    txn: Arc<txn::TxnShared>,
    checkpoints: AtomicU64,
    /// Which units this node leads, under which epochs.
    leaders: Arc<crate::leader::Leaders>,
    /// What unit snapshots carry beside the unit's keys.
    snapshot_parts: RwLock<Vec<Arc<dyn crate::snapshot::UnitSnapshotPart>>>,
    /// Parts of an ingested snapshot whose part wasn't registered yet (a
    /// replica re-restoring a snapshot while it opens, before the stream
    /// log starts): name, unit, index and bytes, handed over on
    /// registration.
    pending_parts: Mutex<Vec<crate::snapshot::PendingPart>>,
    stop: Arc<Stop>,
    checkpointer: Mutex<Option<JoinHandle<()>>>,
    /// Whether the cross-unit resolver thread runs
    /// ([`Engine::start_cross_resolver`]).
    cross_resolver: std::sync::atomic::AtomicBool,
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
        // Drain and sync the logs before the store closes behind it.
        self.router.close_replicated();
        self.log.close();
    }
}

/// The storage engine of one node. Cheap to clone.
#[derive(Clone)]
pub struct Engine {
    /// Held by every handle callers have (not by the checkpointer's own):
    /// when the last one goes, the checkpointer is stopped and joined
    /// first, so the engine closes on the caller's thread and its files are
    /// free when the drop returns. Declared before `inner` so it drops
    /// first.
    user: Option<Arc<Users>>,
    inner: Arc<Inner>,
}

/// See [`Engine::user`].
struct Users {
    stop: Arc<Stop>,
    inner: std::sync::Weak<Inner>,
}

impl Drop for Users {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = true;
        self.stop.cv.notify_all();
        let h = self.inner.upgrade().and_then(|i| {
            i.checkpointer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        });
        if let Some(h) = h
            && h.thread().id() != std::thread::current().id()
        {
            let _ = h.join();
        }
    }
}

/// A handle that doesn't keep the engine open ([`Engine::downgrade`]):
/// what a replicated log the engine holds keeps of it.
#[derive(Clone)]
pub struct WeakEngine(std::sync::Weak<Inner>);

impl std::fmt::Debug for WeakEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WeakEngine")
    }
}

impl WeakEngine {
    /// The engine, while it is open.
    #[must_use]
    pub fn upgrade(&self) -> Option<Engine> {
        self.0.upgrade().map(|inner| Engine { user: None, inner })
    }
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
    /// Entries applied, summed over every unit (each unit's applied
    /// index).
    pub applied_index: u64,
    /// Entries synced, summed over every unit.
    pub durable_index: u64,
    /// Units the commit log knows.
    pub units: u64,
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
        let max_ts = max_ts_of_units(store.as_ref())?;
        let prepared = scan_prepared(store.as_ref(), None)?;
        let sm = Arc::new(StoreMachine {
            store: Arc::clone(&store),
            observers: RwLock::new(Vec::new()),
            last_applied: RwLock::new(std::collections::HashMap::new()),
            max_ts: AtomicU64::new(max_ts),
            unit_ts: RwLock::new(std::collections::HashMap::new()),
            prepared_units: std::sync::atomic::AtomicUsize::new(prepared.len()),
            prepared: Mutex::new(prepared),
        });
        let leaders = Arc::new(crate::leader::Leaders::new(1, cfg.replicated));
        let log = Arc::new(LocalLog::open(
            cfg.data_dir.join("raft"),
            cfg.log.clone(),
            Arc::clone(&sm) as Arc<dyn StateMachine>,
        )?);
        // Replay is done and nothing else writes yet.
        if cfg.store != StoreKind::Mem {
            migrate_legacy(store.as_ref())?;
        }
        let floors = load_all_floors(store.as_ref())?;
        let stop = Arc::new(Stop {
            stopped: Mutex::new(false),
            cv: Condvar::new(),
        });
        // After replay: the clock starts above every version in the store.
        let txn_shared = Arc::new(match &cfg.clock {
            Some(c) => txn::TxnShared::with_config(cfg.txn, Arc::clone(&c.0)),
            None => txn::TxnShared::with_config(cfg.txn, Arc::new(txn::hlc::SystemClock)),
        });
        txn_shared.observe(keys::Ts(sm.max_ts.load(Ordering::Acquire)));
        // Compactions drop versions by the transaction layer's rule, at the
        // watermark it computes when each compaction starts.
        let txn_gc = Arc::new(TxnGc(Arc::downgrade(&txn_shared)));
        let mut engine = Engine {
            user: None,
            inner: Arc::new(Inner {
                sm: Arc::clone(&sm),
                floors: Mutex::new(floors),
                txn: txn_shared,
                store,
                rocks,
                router: crate::repl::Router::new(
                    Arc::clone(&log),
                    cfg.replicated,
                    Arc::clone(&leaders),
                ),
                log,
                budgets,
                unit: NODE_UNIT,
                gc,
                checkpoints: AtomicU64::new(0),
                leaders,
                snapshot_parts: RwLock::new(Vec::new()),
                pending_parts: Mutex::new(Vec::new()),
                stop,
                checkpointer: Mutex::new(None),
                cross_resolver: std::sync::atomic::AtomicBool::new(false),
                cfg,
            }),
        };
        engine.user = Some(Arc::new(Users {
            stop: Arc::clone(&engine.inner.stop),
            inner: Arc::downgrade(&engine.inner),
        }));
        engine.set_version_gc(txn_gc);
        // Prepared transactions come back, and commits across units a crash
        // left unfinished are resolved, before anything reads.
        engine.inner.txn.recover_once(&engine);
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

    /// Moves unfinished commits across units on (participants whose
    /// commit point is durable are resolved, stale pending ones aborted,
    /// finished records deleted). Commits do this as they go; a stepped log
    /// (the simulator) or an idle node calls it. Never blocks on the log.
    pub fn resolve_cross_unit(&self) {
        self.inner.txn.drive_cross(self);
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
        &self.inner.router
    }

    /// A handle that doesn't keep the engine open.
    #[must_use]
    pub fn downgrade(&self) -> WeakEngine {
        WeakEngine(Arc::downgrade(&self.inner))
    }

    /// Whether tenant units commit through a replicated log
    /// ([`EngineConfig::replicated`]).
    #[must_use]
    pub fn is_replicated(&self) -> bool {
        self.inner.router.is_replicated()
    }

    /// Whether `unit` commits through the replicated log.
    #[must_use]
    pub fn is_replicated_unit(&self, unit: UnitId) -> bool {
        self.inner.router.is_replicated_unit(unit)
    }

    /// Before a read of `unit`: on a replica that doesn't lead the
    /// (replicated) unit, waits until this replica has applied everything
    /// the unit's leader had committed when asked (a read barrier), and
    /// returns the timestamp a read view must take to see it all: the
    /// highest version applied to the unit (within a unit, commit
    /// timestamps follow log order, so every commit at or below it is
    /// applied). `None` where the unit is led here or not replicated:
    /// reads use the node's own view. A replica that leads the unit first
    /// makes sure it still may answer from its own state: its leader lease
    /// holds, or a majority confirms it (a leader cut off from the others
    /// must not serve what a newer leader may have replaced).
    ///
    /// # Errors
    /// No leader confirmed in time, this node could not confirm that it
    /// still leads the unit, or replication hasn't started (retryable).
    pub fn follower_read(&self, unit: UnitId) -> Result<Option<keys::Ts>> {
        if !self.is_replicated_unit(unit) {
            return Ok(None);
        }
        if self.leader_epoch(unit) > 0 {
            if let Some(r) = self.replicated_log() {
                r.leader_barrier(unit)?;
            }
            self.wait_cross_finished(unit)?;
            return Ok(None);
        }
        let Some(r) = self.replicated_log() else {
            return Err(SprigError::new(
                sprig_common::SprigCode::ShardMoved,
                "this node's replication hasn't started yet; retry",
            ));
        };
        // A reader session's statement says how fresh its reads must be
        // (see `crate::reads`); anything else reads exactly.
        let policy = crate::reads::current();
        let bounded = policy.as_ref().and_then(|p| match p.mode {
            crate::reads::ReadMode::Bounded { max_lag } => Some((max_lag, p.wait, p.token())),
            crate::reads::ReadMode::Exact => None,
        });
        let served = if let Some((max_lag, wait, token)) = bounded {
            r.bounded_barrier(unit, max_lag, wait, &|| {
                token == 0 || self.unit_readable_ts(unit) >= token
            })?
        } else {
            r.read_barrier(unit)?;
            crate::reads::Served::Barrier
        };
        crate::reads::stats().count(served);
        let applied_ts = || {
            self.inner
                .sm
                .unit_ts
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&unit)
                .copied()
                // Nothing applied since the engine opened: the node's highest
                // (at or above the unit's own).
                .unwrap_or_else(|| self.inner.sm.max_ts.load(Ordering::Acquire))
        };
        // The timestamp first, then the prepare records: a commit across
        // units at or below the timestamp was prepared here before the
        // version that carries the timestamp was applied, so it is either
        // finished here or its prepare record is among those found now.
        // The view waits for exactly those: such a commit may be
        // acknowledged already, and its versions would appear below the
        // view once it is finished. Records prepared later belong to
        // commits above the timestamp.
        let ts = applied_ts();
        let prepared = self.inner.sm.prepared_of(unit);
        if !prepared.is_empty() {
            let deadline = std::time::Instant::now() + CROSS_READ_WAIT;
            while self.inner.sm.any_prepared(unit, &prepared) {
                if !self.inner.cross_resolver.load(Ordering::Acquire)
                    || std::time::Instant::now() >= deadline
                {
                    return Err(SprigError::new(
                        sprig_common::SprigCode::ShardMoved,
                        "a commit across units is prepared on this data and not finished on this \
                         replica yet; retry",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        let ts = keys::Ts(ts);
        // The node's clock and views move past what it now holds.
        self.txn_shared().observe(ts);
        // The token moves to what this replica is known to hold of the
        // unit (the view's timestamp may be the node's highest when the
        // unit applied nothing since the engine opened, which says nothing
        // about the unit's own commits).
        if let Some(p) = &policy {
            p.raise(self.unit_readable_ts(unit));
        }
        Ok(Some(ts))
    }

    /// On the unit's leader: waits until every commit across units that an
    /// earlier leader left prepared on `unit` is finished here (its
    /// versions readable). Until then such a commit may already be
    /// acknowledged while this node's readers can't see its writes to the
    /// unit. Only where the resolver thread runs.
    fn wait_cross_finished(&self, unit: UnitId) -> Result<()> {
        let shared = self.txn_shared();
        if !shared.cross_unfinished_on(unit.0) || !self.inner.cross_resolver.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let deadline = std::time::Instant::now() + CROSS_READ_WAIT;
        loop {
            self.resolve_cross_unit();
            if !shared.cross_unfinished_on(unit.0) {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(SprigError::new(
                    sprig_common::SprigCode::ShardMoved,
                    "a commit across units an earlier leader left unfinished on this data has no \
                     outcome here yet; retry",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// The highest version timestamp applied to `unit` since the engine
    /// opened, published once it is in the store (0: nothing applied
    /// yet). Within a unit commit timestamps follow log order, so this
    /// replica holds every commit of `unit` at or below it.
    #[must_use]
    pub fn unit_readable_ts(&self, unit: UnitId) -> u64 {
        self.inner
            .sm
            .unit_ts
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&unit)
            .copied()
            .unwrap_or(0)
    }

    /// A proposal to `unit` under leader epoch `epoch` (see
    /// `CommitPath::propose_led`).
    pub(crate) fn propose_under(
        &self,
        unit: UnitId,
        epoch: u64,
        payload: Vec<u8>,
        durability: crate::log::Durability,
    ) -> Result<crate::log::Ticket> {
        self.inner
            .router
            .propose_under(unit, epoch, payload, durability)
    }

    /// Starts the thread that finishes commits across units left
    /// unfinished by a leadership change (the units' next leaders read
    /// them back; this drives them every few milliseconds while any is
    /// open). A node whose log runs on its own threads calls it once; a
    /// simulation that steps everything itself calls
    /// [`Engine::resolve_cross_unit`] instead.
    ///
    /// # Errors
    /// The thread could not be started.
    pub fn start_cross_resolver(&self) -> std::io::Result<()> {
        if self.inner.cross_resolver.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let weak = self.downgrade();
        std::thread::Builder::new()
            .name("sprig-cross-resolve".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(CROSS_RESOLVE_EVERY);
                    let Some(e) = weak.upgrade() else { return };
                    if e.txn_shared().cross_unit_open() > 0 {
                        e.resolve_cross_unit();
                    }
                }
            })
            .map(drop)
    }

    /// Attaches the replicated log tenant units commit through. Once only,
    /// on an engine opened with [`EngineConfig::replicated`].
    ///
    /// # Errors
    /// The engine isn't replicated, or a log is already attached.
    pub fn attach_replication(&self, r: Arc<dyn crate::repl::ReplicatedLog>) -> Result<()> {
        self.inner.router.attach(r)
    }

    /// The replicated log, once attached.
    #[must_use]
    pub fn replicated_log(&self) -> Option<&Arc<dyn crate::repl::ReplicatedLog>> {
        self.inner.router.replicated_log()
    }

    /// What applies committed entries to this engine's store: a replicated
    /// log applies every unit's entries through it, on every replica.
    #[must_use]
    pub fn state_machine(&self) -> Arc<dyn StateMachine> {
        Arc::clone(&self.inner.sm) as Arc<dyn StateMachine>
    }

    /// The highest version timestamp the store has applied (what a new
    /// leader's clock must start above).
    #[must_use]
    pub fn applied_max_ts(&self) -> keys::Ts {
        keys::Ts(self.inner.sm.max_ts.load(Ordering::Acquire))
    }

    /// The commit log itself.
    #[must_use]
    pub fn log(&self) -> &LocalLog {
        &self.inner.log
    }

    /// The node's own unit ([`NODE_UNIT`]): engine bookkeeping no tenant
    /// owns, stream appends, and the queue messages until they move into
    /// their tenants' ranges.
    #[must_use]
    pub fn unit(&self) -> UnitId {
        self.inner.unit
    }

    /// The unit of `tenant`: tenant `t` is unit `t`, and the reserved
    /// tenant's keys are the node unit's.
    #[must_use]
    pub fn unit_of_tenant(tenant: u64) -> UnitId {
        if tenant == keys::META_TENANT {
            NODE_UNIT
        } else {
            UnitId(tenant)
        }
    }

    /// The unit `key` (a logical or stored key) belongs to: its tenant's.
    /// A unit's own bookkeeping key belongs to that unit; other keys of
    /// the reserved tenant, and keys too short to name a tenant, to the
    /// node unit.
    #[must_use]
    pub fn unit_of_key(&self, key: &[u8]) -> UnitId {
        unit_of_key(key)
    }

    /// The unit whose log entries carry transactions' notes (stream
    /// appends): the node unit, for now.
    #[must_use]
    pub fn notes_unit(&self) -> UnitId {
        NODE_UNIT
    }

    /// Every unit the commit log knows on this node, with its applied
    /// index.
    #[must_use]
    pub fn units(&self) -> Vec<(UnitId, u64)> {
        let mut u = self.inner.router.applied();
        u.sort_unstable();
        u
    }

    pub(crate) fn leaders(&self) -> &crate::leader::Leaders {
        &self.inner.leaders
    }

    /// The parts unit snapshots carry.
    pub(crate) fn snapshot_parts(
        &self,
    ) -> &RwLock<Vec<Arc<dyn crate::snapshot::UnitSnapshotPart>>> {
        &self.inner.snapshot_parts
    }

    pub(crate) fn pending_parts(&self) -> &Mutex<Vec<crate::snapshot::PendingPart>> {
        &self.inner.pending_parts
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
        let store = &self.inner.store;
        // Per unit, the lowest floor any observer holds on it.
        let mut caps: std::collections::HashMap<UnitId, u64> = std::collections::HashMap::new();
        for ((u, _), f) in self
            .inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let c = caps.entry(*u).or_insert(*f);
            *c = (*c).min(*f);
        }
        let flush = || store.flush_all();
        let mut n = self
            .inner
            .log
            .checkpoint_capped(&flush, &|u| caps.get(&u).copied())?;
        if let Some(r) = self.inner.router.replicated_log() {
            n += r.checkpoint(&flush, &|u| caps.get(&u).copied())?;
        }
        self.inner.checkpoints.fetch_add(1, Ordering::Relaxed);
        Ok(n)
    }

    /// An observer's log floor on the node unit ([`crate::observe`]), if
    /// it ever set one.
    #[must_use]
    pub fn log_floor(&self, name: &str) -> Option<u64> {
        self.log_floor_of(NODE_UNIT, name)
    }

    /// An observer's log floor on `unit`, if it ever set one.
    #[must_use]
    pub fn log_floor_of(&self, unit: UnitId, name: &str) -> Option<u64> {
        self.inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(unit, name.to_owned()))
            .copied()
    }

    /// Every unit observer `name` holds a floor on, with the floor.
    #[must_use]
    pub fn log_floors(&self, name: &str) -> Vec<(UnitId, u64)> {
        self.inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|((_, n), _)| n == name)
            .map(|((u, _), f)| (*u, *f))
            .collect()
    }

    /// Records that observer `name` holds every entry of the node unit up
    /// to `index` durably in its own files: the checkpoint keeps the log
    /// above it. Synced before it returns.
    ///
    /// # Errors
    /// The store failed.
    pub fn set_log_floor(&self, name: &str, index: u64) -> Result<()> {
        self.set_log_floors(name, &[(NODE_UNIT, index)])
    }

    /// [`Engine::set_log_floor`] on several units in one synced write: the
    /// checkpoint keeps each unit's log above its floor. A unit's floor is
    /// set before the observer depends on any of its entries.
    ///
    /// # Errors
    /// The store failed.
    pub fn set_log_floors(&self, name: &str, floors_at: &[(UnitId, u64)]) -> Result<()> {
        if floors_at.is_empty() {
            return Ok(());
        }
        let mut floors = self
            .inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut b = StoreBatch::new();
        for (unit, index) in floors_at {
            b.put(Cf::Local, floor_key(*unit, name), index.to_be_bytes());
        }
        self.inner.store.write(&b, WalMode::Sync)?;
        for (unit, index) in floors_at {
            floors.insert((*unit, name.to_owned()), *index);
        }
        Ok(())
    }

    /// Forgets every observer's floor on `unit`: this node's replica of the
    /// unit left, so nothing here needs its log kept.
    ///
    /// # Errors
    /// The store failed.
    pub fn forget_log_floors_of(&self, unit: UnitId) -> Result<()> {
        let mut floors = self
            .inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let gone: Vec<(UnitId, String)> =
            floors.keys().filter(|(u, _)| *u == unit).cloned().collect();
        if gone.is_empty() {
            return Ok(());
        }
        let mut b = StoreBatch::new();
        for (u, n) in &gone {
            b.delete(Cf::Local, floor_key(*u, n));
        }
        self.inner.store.write(&b, WalMode::Sync)?;
        for k in gone {
            floors.remove(&k);
        }
        Ok(())
    }

    /// Forgets observer `name`'s floors on every unit: the log may be
    /// trimmed past them.
    ///
    /// # Errors
    /// The store failed.
    pub fn clear_log_floor(&self, name: &str) -> Result<()> {
        let mut floors = self
            .inner
            .floors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut b = StoreBatch::new();
        let gone: Vec<(UnitId, String)> =
            floors.keys().filter(|(_, n)| n == name).cloned().collect();
        for (u, n) in &gone {
            b.delete(Cf::Local, floor_key(*u, n));
        }
        self.inner.store.write(&b, WalMode::Sync)?;
        for k in gone {
            floors.remove(&k);
        }
        Ok(())
    }

    /// The lowest log index the engine's commit log still holds (`None`:
    /// it holds none). An observer whose floor is below it can't catch up
    /// from the log: a binary that didn't keep observer floors trimmed it.
    #[must_use]
    pub fn log_first_index(&self) -> Option<u64> {
        self.inner.router.first_index(self.inner.unit)
    }

    /// [`Engine::log_first_index`] of `unit`.
    #[must_use]
    pub fn log_first_index_of(&self, unit: UnitId) -> Option<u64> {
        self.inner.router.first_index(unit)
    }

    /// The highest index of `unit` the store has applied.
    #[must_use]
    pub fn last_applied_of(&self, unit: UnitId) -> u64 {
        self.inner.sm.last_applied(unit)
    }

    /// The highest index of the notes unit the store has applied (what
    /// [`Engine::observe`] hands an observer up to).
    #[must_use]
    pub fn last_applied(&self) -> u64 {
        self.inner.sm.last_applied(self.notes_unit())
    }

    /// Registers an apply observer. Entries of the notes unit above `from`
    /// that the store has already applied are handed to it first (from the
    /// log), then it sees every new entry of every unit; no entry is missed
    /// or seen twice. Returns the last index of the notes unit it was given.
    ///
    /// # Errors
    /// The log no longer holds an entry it needs (the observer's floor was
    /// not kept), or reading the log failed.
    pub fn observe(&self, o: Arc<dyn crate::observe::ApplyObserver>, from: u64) -> Result<u64> {
        let sm = &self.inner.sm;
        let unit = self.notes_unit();
        let mut obs = sm.observers.write().unwrap_or_else(PoisonError::into_inner);
        let top = sm.last_applied(unit);
        let next = from + 1;
        if next <= top && self.inner.router.first_index(unit).is_none_or(|f| f > next) {
            return Err(SprigError::internal(format!(
                "engine: the log no longer holds entry {next}, which an observer needs"
            )));
        }
        self.catch_up(o.as_ref(), unit, from, top)?;
        obs.push(o);
        Ok(top)
    }

    /// Hands `o` the entries of `unit` above `from` up to `top` (applied
    /// already) from the log. A replicated unit's log has indexes without a
    /// payload (a new leader's first entry, membership changes), so its
    /// entries may skip indexes.
    fn catch_up(
        &self,
        o: &dyn crate::observe::ApplyObserver,
        unit: UnitId,
        from: u64,
        top: u64,
    ) -> Result<()> {
        let gone = |next: u64| {
            SprigError::internal(format!(
                "engine: the log of unit {} no longer holds entry {next}, which an observer \
                 needs",
                unit.0
            ))
        };
        let replicated = self.is_replicated_unit(unit);
        let mut next = from + 1;
        if replicated && next <= top && self.inner.router.first_index(unit).is_none_or(|f| f > next)
        {
            return Err(gone(next));
        }
        while next <= top {
            let end = (next + 512).min(top + 1);
            let got = self.inner.router.entries(unit, next, end)?;
            if replicated {
                if !got.is_empty() {
                    o.applied(unit, &got);
                }
                next = end;
                continue;
            }
            if got.first().map(|e| e.index) != Some(next) {
                return Err(gone(next));
            }
            next += got.len() as u64;
            o.applied(unit, &got);
        }
        Ok(())
    }

    /// [`Engine::observe`] over several units: for each `(unit, from)`,
    /// the unit's entries above `from` the store has already applied are
    /// handed to the observer first, then it sees every new entry of every
    /// unit. Returns the last index given for each listed unit.
    ///
    /// # Errors
    /// The log no longer holds an entry it needs, or reading it failed.
    pub fn observe_units(
        &self,
        o: Arc<dyn crate::observe::ApplyObserver>,
        from: &[(UnitId, u64)],
    ) -> Result<Vec<(UnitId, u64)>> {
        let sm = &self.inner.sm;
        let mut obs = sm.observers.write().unwrap_or_else(PoisonError::into_inner);
        let mut tops = Vec::with_capacity(from.len());
        for &(unit, from) in from {
            let top = sm.last_applied(unit);
            self.catch_up(o.as_ref(), unit, from, top)?;
            tops.push((unit, top.max(from)));
        }
        obs.push(o);
        Ok(tops)
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> EngineStats {
        let i = &self.inner;
        let units = i.router.applied();
        EngineStats {
            store: i.store.stats(),
            log: i.log.stats(),
            log_disk_bytes: i.log.disk_bytes(),
            applied_index: units.iter().map(|(_, a)| a).sum(),
            durable_index: units.iter().map(|(u, _)| i.router.durable_index(*u)).sum(),
            units: units.len() as u64,
            checkpoints: i.checkpoints.load(Ordering::Relaxed),
            txn_bytes: i.budgets.txn.used(),
            query_bytes: i.budgets.query.used(),
        }
    }

    /// After a unit snapshot was ingested: the unit's applied index and the
    /// node's highest timestamp take its values, and its prepared XA
    /// transactions come back with their locks.
    pub(crate) fn note_ingested(&self, unit: UnitId, index: u64, max_ts: u64) {
        self.inner
            .sm
            .last_applied
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(unit, index);
        self.inner.sm.max_ts.fetch_max(max_ts, Ordering::AcqRel);
        self.inner
            .sm
            .unit_ts
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(unit)
            .and_modify(|t| *t = (*t).max(max_ts))
            .or_insert(max_ts);
        self.inner.sm.reload_prepared(unit);
        self.inner.txn.load_prepared_of(self, Some(unit.0));
    }

    /// The data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.inner.cfg.data_dir
    }
}

/// Every observer floor stored, by unit and name.
fn load_all_floors(
    store: &dyn SortedStore,
) -> Result<std::collections::BTreeMap<(UnitId, String), u64>> {
    let prefix = keys::unit_meta_key(0, b"");
    let prefix = &prefix[..prefix.len() - 8];
    let mut out = std::collections::BTreeMap::new();
    let mut it = store.iter(
        Cf::Local,
        crate::store::IterOpts::range(&keys::KeyRange::prefix(prefix)),
    );
    it.seek(prefix);
    while it.valid() {
        let rest = &it.key()[prefix.len()..];
        if rest.len() > 8 && rest[8..].starts_with(b"floor/") {
            let unit = UnitId(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8])));
            let name = String::from_utf8_lossy(&rest[8 + 6..]).into_owned();
            let v: [u8; 8] = it
                .value()
                .try_into()
                .map_err(|_| SprigError::internal("storage: an observer floor is corrupt"))?;
            out.insert((unit, name), u64::from_be_bytes(v));
        }
        it.next();
    }
    it.status()?.expect_ready()?;
    Ok(out)
}

/// Every observer floor stored for `unit`.
fn load_floors(
    store: &dyn SortedStore,
    unit: UnitId,
) -> Result<std::collections::BTreeMap<String, u64>> {
    let prefix = floor_key(unit, "");
    let mut out = std::collections::BTreeMap::new();
    let mut it = store.iter(
        Cf::Local,
        crate::store::IterOpts::range(&keys::KeyRange::prefix(&prefix)),
    );
    it.seek(&prefix);
    while it.valid() {
        let name = String::from_utf8_lossy(&it.key()[prefix.len()..]).into_owned();
        let v: [u8; 8] = it
            .value()
            .try_into()
            .map_err(|_| SprigError::internal("storage: an observer floor is corrupt"))?;
        out.insert(name, u64::from_be_bytes(v));
        it.next();
    }
    it.status()?.expect_ready()?;
    Ok(out)
}

/// The unit of a key: see [`Engine::unit_of_key`].
#[must_use]
pub fn unit_of_key(key: &[u8]) -> UnitId {
    let Some(head) = key.get(..8) else {
        return NODE_UNIT;
    };
    let tenant = u64::from_be_bytes(head.try_into().unwrap_or([0; 8]));
    if tenant != keys::META_TENANT {
        return UnitId(tenant);
    }
    let meta = keys::unit_meta_key(0, b"");
    let head = &meta[..meta.len() - 8];
    match (key.strip_prefix(head), key.len() >= meta.len()) {
        (Some(rest), true) => UnitId(u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8]))),
        _ => NODE_UNIT,
    }
}

/// One unit bookkeeping entry: `(unit, field, value)`.
type Bookkeeping = (u64, Vec<u8>, Vec<u8>);

/// Every unit's bookkeeping.
fn unit_bookkeeping(store: &dyn SortedStore) -> Result<Vec<Bookkeeping>> {
    let meta = keys::unit_meta_key(0, b"");
    let head = &meta[..meta.len() - 8];
    let mut out = Vec::new();
    let mut it = store.iter(
        Cf::Data,
        crate::store::IterOpts::range(&keys::KeyRange::prefix(head)),
    );
    it.seek(head);
    while it.valid() {
        let rest = &it.key()[head.len()..];
        if rest.len() >= 8 {
            let unit = u64::from_be_bytes(rest[..8].try_into().unwrap_or([0; 8]));
            out.push((unit, rest[8..].to_vec(), it.value().to_vec()));
        }
        it.next();
    }
    it.status()?.expect_ready()?;
    Ok(out)
}

/// The highest version timestamp any unit has applied.
fn max_ts_of_units(store: &dyn SortedStore) -> Result<u64> {
    let mut top = 0;
    for (_, field, v) in unit_bookkeeping(store)? {
        if field == MAX_TS {
            let b: [u8; 8] = v
                .as_slice()
                .try_into()
                .map_err(|_| SprigError::internal("storage: unit bookkeeping is corrupt"))?;
            top = top.max(u64::from_be_bytes(b));
        }
    }
    Ok(top)
}

/// Moves a data directory written when the node had one unit (unit 1,
/// holding every tenant) to one unit per tenant, once, right after replay:
/// - the store is flushed, so the old unit's entries are never replayed
///   again (they hold every tenant's keys, and units replay
///   independently);
/// - the node unit continues the old unit's index sequence and takes its
///   observer floors, because what the old unit's entries carried for
///   observers (stream appends) now comes on the node unit's log, and
///   stream files record their position as an index of that log;
/// - tenant 1's unit keeps its own applied index and continues after it.
///
/// An apply observer's files must already hold everything the old unit's
/// log carried for it (a clean shutdown flushes them).
///
/// Then (layout 3) queue messages kept under the reserved tenant move into
/// their tenants' ranges ([`move_queue_keys`]).
fn migrate_legacy(store: &dyn SortedStore) -> Result<()> {
    let layout_key = keys::unit_meta_key(NODE_UNIT.0, LAYOUT);
    let layout = read_u64(store, &layout_key)?;
    if layout >= LAYOUT_QUEUES {
        return Ok(());
    }
    if layout < LAYOUT_UNITS {
        migrate_units(store)?;
    }
    let moved = move_queue_keys(store)?;
    if moved > 0 {
        tracing::info!(
            keys = moved,
            "engine: moved queue messages into their tenants' ranges"
        );
    }
    let mut b = StoreBatch::new();
    b.put(Cf::Data, layout_key, LAYOUT_QUEUES.to_be_bytes());
    // Synced through the store's own log: durable without a flush.
    store.write(&b, WalMode::Sync)?;
    if moved > 0 {
        store.flush_all()?;
    }
    Ok(())
}

/// Before layout 3, queue messages sat under the reserved tenant:
/// `META | 0 | queues | kind | index | tenant | db | rest`. They move to
/// `tenant | 0 | queues | kind | index | db | rest`, flushed before the
/// old keys go, so a crash in between leaves both and the move runs again.
/// Returns the number of keys moved.
fn move_queue_keys(store: &dyn SortedStore) -> Result<u64> {
    let old_head = keys::KeyPrefix {
        tenant: keys::META_TENANT,
        shard: 0,
        object: keys::QUEUES_OBJECT,
        kind: 0,
        index: 0,
    }
    .to_vec();
    let head = &old_head[..16];
    let mut moved = 0u64;
    loop {
        let mut puts = StoreBatch::new();
        let mut dels = StoreBatch::new();
        let mut n = 0;
        {
            let mut it = store.iter(
                Cf::Data,
                crate::store::IterOpts::range(&keys::KeyRange::prefix(head)),
            );
            it.seek(head);
            while it.valid() && n < 10_000 {
                let k = it.key();
                if k.len() >= keys::PREFIX_LEN + 8 {
                    let mut nk = Vec::with_capacity(k.len() - 8);
                    nk.extend_from_slice(&k[keys::PREFIX_LEN..keys::PREFIX_LEN + 8]);
                    nk.extend_from_slice(&k[8..keys::PREFIX_LEN]);
                    nk.extend_from_slice(&k[keys::PREFIX_LEN + 8..]);
                    puts.put(Cf::Data, nk, it.value().to_vec());
                }
                dels.delete(Cf::Data, k.to_vec());
                n += 1;
                it.next();
            }
            it.status()?.expect_ready()?;
        }
        if n == 0 {
            return Ok(moved);
        }
        store.write(&puts, WalMode::Sync)?;
        store.flush_all()?;
        store.write(&dels, WalMode::Sync)?;
        moved += n;
    }
}

/// Layout 1 to 2: see [`migrate_legacy`].
fn migrate_units(store: &dyn SortedStore) -> Result<()> {
    let layout_key = keys::unit_meta_key(NODE_UNIT.0, LAYOUT);
    let legacy_applied = read_u64(store, &keys::unit_meta_key(LEGACY_UNIT.0, APPLIED))?;
    let mut b = StoreBatch::new();
    if legacy_applied > 0 && read_u64(store, &keys::unit_meta_key(NODE_UNIT.0, APPLIED))? == 0 {
        store.flush_all()?;
        b.put(
            Cf::Data,
            keys::unit_meta_key(NODE_UNIT.0, APPLIED),
            legacy_applied.to_be_bytes(),
        );
        for (name, index) in load_floors(store, LEGACY_UNIT)? {
            let mut lb = StoreBatch::new();
            lb.put(Cf::Local, floor_key(NODE_UNIT, &name), index.to_be_bytes());
            lb.delete(Cf::Local, floor_key(LEGACY_UNIT, &name));
            store.write(&lb, WalMode::Sync)?;
        }
        tracing::info!(
            index = legacy_applied,
            "engine: moved a one-unit data directory to one unit per tenant"
        );
    }
    b.put(Cf::Data, layout_key, LAYOUT_UNITS.to_be_bytes());
    store.write(&b, WalMode::Sync)?;
    // Only a moved directory needs the flush (its old unit's entries must
    // never replay); a new one would just get a one-key table file that
    // every read then probes until a compaction.
    if legacy_applied > 0 {
        store.flush_all()?;
    }
    Ok(())
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
                let engine = Engine { user: None, inner };
                // Entries written on every unit, not one unit's index.
                let stats = engine.inner.log.stats();
                // Replicated units' entries count by their applied indexes.
                let applied = stats.entries
                    + engine
                        .inner
                        .router
                        .replicated_log()
                        .map_or(0, |r| r.applied().iter().map(|(_, a)| *a).sum::<u64>());
                let bytes = stats.bytes;
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
