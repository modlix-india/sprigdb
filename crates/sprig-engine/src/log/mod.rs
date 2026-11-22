//! The commit log: durability, group commit, apply and replay.
//!
//! Every write the engine makes durable is a proposal to a unit's log
//! ([`Consensus::propose`]). The proposal gets its index at once, in queue
//! order, so a caller can release its locks as soon as `propose` returns
//! (early lock release) and wait for durability before it replies
//! ([`Ticket::wait`]).
//!
//! [`LocalLog`] is the single-voter implementation: raft-engine as the
//! write-ahead log, with one writer thread that
//! 1. takes every queued proposal (up to `group_max_bytes`), waiting up to
//!    `group_max_wait` for more only when the previous group was much
//!    larger than the queue (0 µs when idle);
//! 2. appends them in one raft-engine write, synced if any proposal's
//!    durability class needs it (one sync covers the whole group, and
//!    everything before it);
//! 3. hands them to the [`StateMachine`], which applies the whole group to
//!    the store in one batch with RocksDB's WAL off.
//!
//! The next group gathers in the queue while a sync runs. Relaxed classes
//! are synced by a timer. A failed write or sync is fatal: the process
//! aborts and recovery replays the log (fsync errors are never retried).
//!
//! On open, every unit's entries above the state machine's applied index are
//! replayed into it before the log accepts proposals.

mod entry;
mod fs;
pub mod payload;

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use raft_engine::{LogBatch, ReadableSize, RecoveryMode};
use sprig_common::{Result, SprigError};

pub use entry::{EntryExt, LogEntry};
pub use fs::{LogFs, SimDisk, SyncMode};
pub use payload::Payload;

/// A unit: an unsharded tenant or one shard, with its own log (one Raft
/// group once replication is built).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct UnitId(pub u64);

/// When a write is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Durability {
    /// A majority synced. With one voter: a local sync.
    Strict,
    /// A majority holds it in memory, synced within
    /// `LogConfig::replicated_sync`. With one voter: the OS write.
    Replicated,
    /// A local sync (`fdatasync`, `F_FULLFSYNC` on macOS).
    Local,
    /// The OS write; synced within `LogConfig::relaxed_sync` (Redis
    /// `everysec`).
    LocalRelaxed,
}

impl Durability {
    /// Whether the acknowledgement waits for a sync.
    #[must_use]
    pub fn needs_sync(self) -> bool {
        matches!(self, Durability::Strict | Durability::Local)
    }
}

/// Log settings.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// The longest a group waits for more proposals under load.
    pub group_max_wait: Duration,
    /// The most payload bytes in one group.
    pub group_max_bytes: usize,
    /// `LocalRelaxed` entries are synced within this.
    pub relaxed_sync: Duration,
    /// `Replicated` entries are synced within this.
    pub replicated_sync: Duration,
    /// How syncs reach the disk.
    pub sync: SyncMode,
    /// raft-engine's file size. A checkpoint deletes whole files only, and
    /// never the one being written, so up to this much trimmed log stays on
    /// disk.
    pub target_file_size: u64,
    /// LZ4-compress a commit group's log batch once it is at least this
    /// many bytes. Zero turns log compression off.
    pub compress_min_bytes: u64,
    /// Crash simulation for the log's files, in tests.
    pub sim: Option<Arc<SimDisk>>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            group_max_wait: Duration::from_micros(500),
            group_max_bytes: 4 << 20,
            relaxed_sync: Duration::from_millis(1000),
            replicated_sync: Duration::from_millis(100),
            sync: SyncMode::Full,
            target_file_size: 16 << 20,
            compress_min_bytes: 4 << 10,
            sim: None,
        }
    }
}

/// What applies committed entries: the engine's store, in production.
pub trait StateMachine: Send + Sync {
    /// The highest index already applied for `unit` (0 if none). Replay
    /// starts after it.
    ///
    /// # Errors
    /// The state could not be read.
    fn applied_index(&self, unit: UnitId) -> Result<u64>;

    /// Applies `entries` (contiguous, ascending) for `unit`, atomically
    /// with recording the last one's index.
    ///
    /// # Errors
    /// The state could not be written. The log treats this as fatal.
    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()>;
}

/// Commits through a log. A single node has [`LocalLog`]; replication will add a Raft
/// implementation with the same contract.
pub trait Consensus: Send + Sync {
    /// Queues `payload` for `unit`. The returned ticket carries the entry's
    /// index, assigned now: entries of one unit become durable and are
    /// applied in index order.
    ///
    /// # Errors
    /// The log is closed.
    fn propose(&self, unit: UnitId, payload: Vec<u8>, durability: Durability) -> Result<Ticket>;

    /// The highest index of `unit` that is synced.
    fn durable_index(&self, unit: UnitId) -> u64;

    /// The highest index of `unit` applied to the state machine.
    fn applied_index(&self, unit: UnitId) -> u64;

    /// Blocks until `unit` has applied `index`.
    ///
    /// # Errors
    /// The log closed first.
    fn wait_applied(&self, unit: UnitId, index: u64) -> Result<()>;
}

/// Progress of one unit, shared by the writer and every waiter.
#[derive(Debug, Default)]
struct UnitState {
    written: AtomicU64,
    synced: AtomicU64,
    applied: AtomicU64,
    closed: AtomicBool,
    mu: Mutex<()>,
    cv: Condvar,
    notify: tokio::sync::Notify,
}

impl UnitState {
    fn wake(&self) {
        drop(self.mu.lock().unwrap_or_else(PoisonError::into_inner));
        self.cv.notify_all();
        self.notify.notify_waiters();
    }

    fn wait_until(&self, done: impl Fn(&Self) -> bool) -> Result<()> {
        if done(self) {
            return Ok(());
        }
        let mut g = self.mu.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if done(self) {
                return Ok(());
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(closed());
            }
            g = self.cv.wait(g).unwrap_or_else(PoisonError::into_inner);
        }
    }

    async fn wait_until_async(&self, done: impl Fn(&Self) -> bool) -> Result<()> {
        loop {
            let n = self.notify.notified();
            let mut n = std::pin::pin!(n);
            n.as_mut().enable();
            if done(self) {
                return Ok(());
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(closed());
            }
            n.await;
        }
    }
}

fn closed() -> SprigError {
    SprigError::internal("the commit log is closed")
}

/// A queued proposal's handle: its index, and a way to wait until it meets
/// its durability class.
#[derive(Debug, Clone)]
pub struct Ticket {
    unit: Arc<UnitState>,
    unit_id: UnitId,
    index: u64,
    sync: bool,
}

impl Ticket {
    /// The unit.
    #[must_use]
    pub fn unit(&self) -> UnitId {
        self.unit_id
    }

    /// The entry's index in its unit's log.
    #[must_use]
    pub fn index(&self) -> u64 {
        self.index
    }

    fn durable(&self, u: &UnitState) -> bool {
        let w = if self.sync { &u.synced } else { &u.written };
        w.load(Ordering::Acquire) >= self.index
    }

    /// Whether the entry already meets its durability class.
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.durable(&self.unit)
    }

    /// Blocks until the entry meets its durability class. Replies whose
    /// content depends on this entry wait here.
    ///
    /// # Errors
    /// The log closed first.
    pub fn wait(&self) -> Result<()> {
        self.unit.wait_until(|u| self.durable(u))
    }

    /// The async form of [`Ticket::wait`].
    ///
    /// # Errors
    /// The log closed first.
    pub async fn wait_async(&self) -> Result<()> {
        self.unit.wait_until_async(|u| self.durable(u)).await
    }

    /// Blocks until the entry is applied to the store.
    ///
    /// # Errors
    /// The log closed first.
    pub fn wait_applied(&self) -> Result<()> {
        let i = self.index;
        self.unit
            .wait_until(|u| u.applied.load(Ordering::Acquire) >= i)
    }

    /// The async form of [`Ticket::wait_applied`].
    ///
    /// # Errors
    /// The log closed first.
    pub async fn wait_applied_async(&self) -> Result<()> {
        let i = self.index;
        self.unit
            .wait_until_async(|u| u.applied.load(Ordering::Acquire) >= i)
            .await
    }
}

struct Proposal {
    unit: UnitId,
    index: u64,
    payload: Vec<u8>,
    durability: Durability,
}

#[derive(Default)]
struct Queue {
    items: VecDeque<Proposal>,
    bytes: usize,
    next: HashMap<UnitId, u64>,
    closed: bool,
    /// Stop without writing what is queued and without a final sync: a
    /// simulated crash.
    halted: bool,
}

/// Counters for benchmarks and metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogStats {
    /// Write groups.
    pub groups: u64,
    /// Entries written.
    pub entries: u64,
    /// Syncs issued.
    pub syncs: u64,
    /// Total time in raft-engine writes (syncs included), µs.
    pub write_us: u64,
    /// Total time applying groups to the state machine, µs.
    pub apply_us: u64,
    /// Payload bytes written.
    pub bytes: u64,
    /// Entries replayed at open.
    pub replayed: u64,
}

#[derive(Default)]
struct Counters {
    groups: AtomicU64,
    entries: AtomicU64,
    syncs: AtomicU64,
    write_us: AtomicU64,
    apply_us: AtomicU64,
    bytes: AtomicU64,
    replayed: AtomicU64,
}

struct Shared {
    engine: raft_engine::Engine<LogFs>,
    sm: Arc<dyn StateMachine>,
    cfg: LogConfig,
    queue: Mutex<Queue>,
    work: Condvar,
    units: RwLock<HashMap<UnitId, Arc<UnitState>>>,
    counters: Counters,
}

/// raft-engine with one voter, behind [`Consensus`].
pub struct LocalLog {
    sh: Arc<Shared>,
    writer: Mutex<Option<JoinHandle<()>>>,
    dir: PathBuf,
}

impl std::fmt::Debug for LocalLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalLog")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

/// A log failure the process can't survive: the write may or may not be on
/// disk, so continuing could acknowledge lost data.
fn fatal(what: &str, e: &dyn std::fmt::Display) -> ! {
    tracing::error!(error = %e, "commit log {what} failed; aborting");
    eprintln!("sprig-engine: commit log {what} failed: {e}; aborting");
    std::process::abort()
}

fn log_err(what: &str, e: raft_engine::Error) -> SprigError {
    SprigError::internal(format!("commit log: {what} failed")).with_source(e)
}

impl LocalLog {
    /// Opens the log in `dir`, replays every unit's entries above
    /// `sm.applied_index(unit)` into `sm`, and starts the writer.
    ///
    /// # Errors
    /// raft-engine failed to open or read, or the log was trimmed past what
    /// the state machine applied.
    pub fn open(
        dir: impl Into<PathBuf>,
        cfg: LogConfig,
        sm: Arc<dyn StateMachine>,
    ) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| {
            SprigError::internal(format!("commit log: create {}", dir.display())).with_source(e)
        })?;
        let rcfg = raft_engine::Config {
            dir: dir.to_string_lossy().into_owned(),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            batch_compression_threshold: ReadableSize(cfg.compress_min_bytes),
            target_file_size: ReadableSize(cfg.target_file_size),
            // raft-engine deletes trimmed files only while its files add up
            // to more than this, and it refuses a value below the file
            // size. At the file size, every checkpoint that leaves more
            // than the file being written deletes the trimmed ones.
            purge_threshold: ReadableSize(cfg.target_file_size.max(1 << 20)),
            enable_log_recycle: false,
            ..raft_engine::Config::default()
        };
        let fs = Arc::new(LogFs::new(cfg.sync, cfg.sim.clone()));
        let engine =
            raft_engine::Engine::open_with_file_system(rcfg, fs).map_err(|e| log_err("open", e))?;
        let sh = Arc::new(Shared {
            engine,
            sm,
            cfg,
            queue: Mutex::new(Queue::default()),
            work: Condvar::new(),
            units: RwLock::new(HashMap::new()),
            counters: Counters::default(),
        });
        // raft-engine caches the last log block it read in a per-thread
        // slot keyed only by file number, offset and length, not by engine:
        // replaying on the caller's thread could hand this log a block that
        // another (or an earlier) engine read there. A fresh thread starts
        // with an empty slot.
        on_fresh_thread(|| {
            for u in sh.engine.raft_groups() {
                replay(&sh, UnitId(u))?;
            }
            Ok(())
        })?;
        let w = Arc::clone(&sh);
        let writer = std::thread::Builder::new()
            .name("sprig-log-writer".into())
            .spawn(move || run(&w))
            .map_err(|e| SprigError::internal("commit log: spawn writer").with_source(e))?;
        Ok(Self {
            sh,
            writer: Mutex::new(Some(writer)),
            dir,
        })
    }

    fn unit(&self, id: UnitId) -> Arc<UnitState> {
        unit_state(&self.sh, id)
    }

    /// Counters.
    #[must_use]
    pub fn stats(&self) -> LogStats {
        let c = &self.sh.counters;
        LogStats {
            groups: c.groups.load(Ordering::Relaxed),
            entries: c.entries.load(Ordering::Relaxed),
            syncs: c.syncs.load(Ordering::Relaxed),
            write_us: c.write_us.load(Ordering::Relaxed),
            apply_us: c.apply_us.load(Ordering::Relaxed),
            bytes: c.bytes.load(Ordering::Relaxed),
            replayed: c.replayed.load(Ordering::Relaxed),
        }
    }

    /// Every unit the log knows, with its applied index.
    #[must_use]
    pub fn applied(&self) -> Vec<(UnitId, u64)> {
        self.sh
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|(id, u)| (*id, u.applied.load(Ordering::Acquire)))
            .collect()
    }

    /// Syncs the log, runs `flush_store` (which must persist everything
    /// applied so far), then trims every unit's log below what was applied
    /// before the flush. Returns the number of entries trimmed.
    ///
    /// # Errors
    /// The sync, the flush or the purge failed.
    pub fn checkpoint(&self, flush_store: &dyn Fn() -> Result<()>) -> Result<u64> {
        self.checkpoint_capped(flush_store, &|_| None)
    }

    /// [`LocalLog::checkpoint`], keeping every entry of a unit above
    /// `cap(unit)` when it returns an index (what an apply observer has not
    /// made durable in its own files yet).
    ///
    /// # Errors
    /// As for [`LocalLog::checkpoint`].
    pub fn checkpoint_capped(
        &self,
        flush_store: &dyn Fn() -> Result<()>,
        cap: &dyn Fn(UnitId) -> Option<u64>,
    ) -> Result<u64> {
        let floors = self.applied();
        self.sh.engine.sync().map_err(|e| log_err("sync", e))?;
        flush_store()?;
        let mut trimmed = 0;
        for (u, applied) in floors {
            let keep_above = cap(u).map_or(applied, |c| c.min(applied));
            trimmed += self.sh.engine.compact_to(u.0, keep_above + 1);
        }
        // A purge can read entries back to rewrite them, through the same
        // per-thread cache as replay: on a fresh thread, for the reason
        // given in `open`.
        on_fresh_thread(|| {
            self.sh
                .engine
                .purge_expired_files()
                .map(drop)
                .map_err(|e| log_err("purge", e))
        })?;
        Ok(trimmed)
    }

    /// The entry at `index` of `unit`, if the log still holds it. For
    /// diagnostics and tests (what a crash left in the log).
    ///
    /// # Errors
    /// raft-engine failed to read.
    pub fn entry(&self, unit: UnitId, index: u64) -> Result<Option<LogEntry>> {
        // On a fresh thread, for the reason given in `open`.
        on_fresh_thread(|| {
            let mut buf = Vec::new();
            match self.sh.engine.fetch_entries_to::<EntryExt>(
                unit.0,
                index,
                index + 1,
                None,
                &mut buf,
            ) {
                Ok(_) => Ok(buf.pop()),
                Err(raft_engine::Error::EntryNotFound | raft_engine::Error::EntryCompacted) => {
                    Ok(None)
                }
                Err(e) => Err(log_err("read", e)),
            }
        })
    }

    /// The lowest index of `unit` the log still holds (`None`: it holds
    /// none, everything was trimmed or nothing was written).
    #[must_use]
    pub fn first_index(&self, unit: UnitId) -> Option<u64> {
        self.sh.engine.first_index(unit.0)
    }

    /// The entries `from..to` of `unit` the log still holds, in order
    /// (fewer when the log ends first). For apply observers catching up.
    ///
    /// # Errors
    /// raft-engine failed to read, or the range starts below what the log
    /// still holds.
    pub fn entries(&self, unit: UnitId, from: u64, to: u64) -> Result<Vec<LogEntry>> {
        if from >= to {
            return Ok(Vec::new());
        }
        // On a fresh thread, for the reason given in `open`.
        on_fresh_thread(|| {
            let last = self.sh.engine.last_index(unit.0).unwrap_or(0);
            let to = to.min(last + 1);
            let mut buf = Vec::new();
            if from >= to {
                return Ok(buf);
            }
            self.sh
                .engine
                .fetch_entries_to::<EntryExt>(unit.0, from, to, None, &mut buf)
                .map_err(|e| log_err("read", e))?;
            Ok(buf)
        })
    }

    /// Bytes of log files on disk.
    #[must_use]
    pub fn disk_bytes(&self) -> u64 {
        self.sh.engine.get_used_size() as u64
    }

    /// Stops the writer at once, dropping queued proposals and skipping the
    /// closing sync: what a crash leaves. For crash simulations. Under a
    /// [`SimDisk`], the power is cut once the writer has stopped, so the sync
    /// raft-engine makes when the log is dropped keeps nothing.
    pub fn halt(&self) {
        {
            let mut q = self.sh.queue.lock().unwrap_or_else(PoisonError::into_inner);
            q.closed = true;
            q.halted = true;
            q.items.clear();
            q.bytes = 0;
        }
        self.close();
        if let Some(sim) = &self.sh.cfg.sim {
            sim.power_cut();
        }
    }

    /// Stops the writer after it drains and syncs the queue. Later
    /// proposals fail; waiters on unwritten entries get an error.
    pub fn close(&self) {
        {
            let mut q = self.sh.queue.lock().unwrap_or_else(PoisonError::into_inner);
            q.closed = true;
        }
        self.sh.work.notify_all();
        let h = self
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(h) = h {
            let _ = h.join();
        }
    }
}

impl Drop for LocalLog {
    fn drop(&mut self) {
        self.close();
    }
}

/// Runs `f` on a new thread and waits for it, so raft-engine's per-thread
/// block cache starts empty.
fn on_fresh_thread<T: Send>(f: impl FnOnce() -> Result<T> + Send) -> Result<T> {
    std::thread::scope(|s| {
        std::thread::Builder::new()
            .name("sprig-log-read".into())
            .spawn_scoped(s, f)
            .map_err(|e| SprigError::internal("commit log: spawn reader").with_source(e))?
            .join()
            .unwrap_or_else(|p| std::panic::resume_unwind(p))
    })
}

fn unit_state(sh: &Shared, id: UnitId) -> Arc<UnitState> {
    if let Some(u) = sh
        .units
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&id)
    {
        return Arc::clone(u);
    }
    let mut units = sh.units.write().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(units.entry(id).or_default())
}

/// Replays `unit`'s entries above the state machine's applied index, and
/// sets the unit's next index.
fn replay(sh: &Shared, unit: UnitId) -> Result<()> {
    let applied = sh.sm.applied_index(unit)?;
    let first = sh.engine.first_index(unit.0);
    let last = sh.engine.last_index(unit.0).unwrap_or(0);
    if let Some(first) = first
        && applied + 1 < first
    {
        return Err(SprigError::internal(format!(
            "commit log: unit {} was trimmed to {first} but only {applied} is applied",
            unit.0
        )));
    }
    let mut i = (applied + 1).max(first.unwrap_or(1));
    let mut buf = Vec::new();
    while i <= last {
        let end = (i + 1024).min(last + 1);
        buf.clear();
        sh.engine
            .fetch_entries_to::<EntryExt>(unit.0, i, end, None, &mut buf)
            .map_err(|e| log_err("replay read", e))?;
        if buf.is_empty() {
            break;
        }
        sh.sm.apply(unit, &buf)?;
        sh.counters
            .replayed
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        i += buf.len() as u64;
    }
    // The store can be ahead of the log when relaxed entries were applied
    // and flushed but their log tail was lost: drop the stale tail so new
    // entries continue after the store.
    if applied > last {
        sh.engine.compact_to(unit.0, applied + 1);
    }
    let top = applied.max(last);
    let u = unit_state(sh, unit);
    u.written.store(top, Ordering::Release);
    u.synced.store(top, Ordering::Release);
    u.applied.store(top, Ordering::Release);
    sh.queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .next
        .insert(unit, top + 1);
    Ok(())
}

impl Consensus for LocalLog {
    fn propose(&self, unit: UnitId, payload: Vec<u8>, durability: Durability) -> Result<Ticket> {
        let state = self.unit(unit);
        let mut q = self.sh.queue.lock().unwrap_or_else(PoisonError::into_inner);
        if q.closed {
            return Err(closed());
        }
        let next = match q.next.entry(unit) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => {
                // A unit this log has never seen: start after whatever the
                // state machine already holds.
                let applied = self.sh.sm.applied_index(unit)?;
                let start = applied.max(self.sh.engine.last_index(unit.0).unwrap_or(0)) + 1;
                for w in [&state.written, &state.synced, &state.applied] {
                    w.store(start - 1, Ordering::Release);
                }
                v.insert(start)
            }
        };
        let index = *next;
        *next += 1;
        q.bytes += payload.len();
        q.items.push_back(Proposal {
            unit,
            index,
            payload,
            durability,
        });
        drop(q);
        self.sh.work.notify_one();
        Ok(Ticket {
            unit: state,
            unit_id: unit,
            index,
            sync: durability.needs_sync(),
        })
    }

    fn durable_index(&self, unit: UnitId) -> u64 {
        self.unit(unit).synced.load(Ordering::Acquire)
    }

    fn applied_index(&self, unit: UnitId) -> u64 {
        self.unit(unit).applied.load(Ordering::Acquire)
    }

    fn wait_applied(&self, unit: UnitId, index: u64) -> Result<()> {
        self.unit(unit)
            .wait_until(|u| u.applied.load(Ordering::Acquire) >= index)
    }
}

fn lock_queue(sh: &Shared) -> MutexGuard<'_, Queue> {
    sh.queue.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The writer thread.
fn run(sh: &Shared) {
    let mut last_group = 0usize;
    let mut relaxed_due: Option<Instant> = None;
    let mut group: Vec<Proposal> = Vec::new();
    loop {
        let mut q = lock_queue(sh);
        if q.halted {
            break;
        }
        while q.items.is_empty() && !q.closed {
            match relaxed_due {
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        break;
                    }
                    q = sh
                        .work
                        .wait_timeout(q, d - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
                None => q = sh.work.wait(q).unwrap_or_else(PoisonError::into_inner),
            }
        }
        if q.items.is_empty() {
            drop(q);
            if relaxed_due.take().is_some() && !lock_queue(sh).halted {
                sync_all(sh);
            }
            if lock_queue(sh).closed {
                break;
            }
            continue;
        }
        // Under load, wait briefly for the group to fill.
        if !sh.cfg.group_max_wait.is_zero() && q.items.len() * 2 < last_group {
            let deadline = Instant::now() + sh.cfg.group_max_wait;
            while q.items.len() * 2 < last_group && q.bytes < sh.cfg.group_max_bytes && !q.closed {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                q = sh
                    .work
                    .wait_timeout(q, deadline - now)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
        }
        let mut bytes = 0;
        while let Some(p) = q.items.front() {
            if !group.is_empty() && bytes + p.payload.len() > sh.cfg.group_max_bytes {
                break;
            }
            bytes += p.payload.len();
            if let Some(p) = q.items.pop_front() {
                group.push(p);
            }
        }
        q.bytes -= bytes;
        drop(q);
        last_group = group.len();
        write_group(sh, &mut group, &mut relaxed_due);
    }
    for u in sh
        .units
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .values()
    {
        u.closed.store(true, Ordering::Release);
        u.wake();
    }
}

fn sync_all(sh: &Shared) {
    let t = Instant::now();
    if let Err(e) = sh.engine.sync() {
        fatal("sync", &e);
    }
    sh.counters.syncs.fetch_add(1, Ordering::Relaxed);
    add_us(&sh.counters.write_us, t);
    for u in sh
        .units
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .values()
    {
        u.synced
            .store(u.written.load(Ordering::Acquire), Ordering::Release);
        u.wake();
    }
}

fn add_us(c: &AtomicU64, since: Instant) {
    c.fetch_add(
        u64::try_from(since.elapsed().as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
}

fn write_group(sh: &Shared, group: &mut Vec<Proposal>, relaxed_due: &mut Option<Instant>) {
    let mut by_unit: BTreeMap<UnitId, Vec<LogEntry>> = BTreeMap::new();
    let mut need_sync = false;
    let mut relax: Option<Duration> = None;
    let mut bytes = 0u64;
    let n = group.len() as u64;
    for p in group.drain(..) {
        need_sync |= p.durability.needs_sync();
        let window = match p.durability {
            Durability::Replicated => Some(sh.cfg.replicated_sync),
            Durability::LocalRelaxed => Some(sh.cfg.relaxed_sync),
            _ => None,
        };
        if let Some(w) = window {
            relax = Some(relax.map_or(w, |r| r.min(w)));
        }
        bytes += p.payload.len() as u64;
        by_unit
            .entry(p.unit)
            .or_default()
            .push(LogEntry::new(1, p.index, p.payload));
    }
    let mut lb = LogBatch::with_capacity(by_unit.len());
    for (u, entries) in &by_unit {
        if let Err(e) = lb.add_entries::<EntryExt>(u.0, entries) {
            fatal("encode", &e);
        }
    }
    let t = Instant::now();
    if let Err(e) = sh.engine.write(&mut lb, need_sync) {
        fatal("write", &e);
    }
    add_us(&sh.counters.write_us, t);
    let c = &sh.counters;
    c.groups.fetch_add(1, Ordering::Relaxed);
    c.entries.fetch_add(n, Ordering::Relaxed);
    c.bytes.fetch_add(bytes, Ordering::Relaxed);
    if need_sync {
        c.syncs.fetch_add(1, Ordering::Relaxed);
        *relaxed_due = None;
    } else if let Some(w) = relax {
        let due = Instant::now() + w;
        *relaxed_due = Some(relaxed_due.map_or(due, |d| d.min(due)));
    }
    let units: Vec<(Arc<UnitState>, u64)> = by_unit
        .iter()
        .map(|(id, es)| (unit_state(sh, *id), es.last().map_or(0, |e| e.index)))
        .collect();
    if need_sync {
        // One sync covers everything written so far, on every unit.
        for u in sh
            .units
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
        {
            u.synced
                .store(u.written.load(Ordering::Acquire), Ordering::Release);
        }
    }
    for (u, last) in &units {
        u.written.store(*last, Ordering::Release);
        if need_sync {
            u.synced.store(*last, Ordering::Release);
        }
        u.wake();
    }
    let t = Instant::now();
    for (id, entries) in &by_unit {
        if let Err(e) = sh.sm.apply(*id, entries) {
            fatal("apply", &e);
        }
    }
    add_us(&c.apply_us, t);
    for (u, last) in &units {
        u.applied.store(*last, Ordering::Release);
        u.wake();
    }
}

#[cfg(test)]
mod tests;
