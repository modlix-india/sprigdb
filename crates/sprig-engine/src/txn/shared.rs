//! One unit's transaction state: the clock, the lock table, the pending
//! overlay, read views, and the commit path with early lock release.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use sprig_common::Result;

use super::durable::DurableTicket;
use super::hlc::{Clock, Hlc, SystemClock, Ts};
use super::mvcc::encode_value_into;
use super::overlay::Overlay;
use super::{CommitPath, UnitConfig};
use crate::keys;
use crate::locks::{LockConfig, LockTable, TxnId};
use crate::log::{Durability, Payload};
use crate::store::Cf;

const VIEW_SHARDS: usize = 16;
/// How often (in commits) the GC watermark is pushed to the store side.
const WATERMARK_EVERY: u64 = 1024;

/// Counters for status output and benchmarks.
#[derive(Debug, Default)]
pub struct TxnStats {
    /// Transactions that committed writes.
    pub commits: AtomicU64,
    /// Transactions rolled back (explicitly or by an error).
    pub rollbacks: AtomicU64,
    /// Reads that returned a pending version (each gates a reply).
    pub pending_reads: AtomicU64,
}

type HeldQueue = (Mutex<VecDeque<(DurableTicket, TxnId)>>, Condvar);

/// Per-unit transaction state, shared by every transaction of the unit.
pub struct TxnShared {
    pub(crate) hlc: Hlc,
    pub(crate) locks: Arc<LockTable>,
    pub(crate) overlay: Overlay,
    commit_latch: Mutex<()>,
    /// Commits whose versions are still in the overlay: `(index, keys)`.
    unapplied: Mutex<VecDeque<(u64, Vec<Bytes>)>>,
    /// The commit timestamp of the last queued commit. New read views take
    /// it, so every version at or below a view is already readable.
    visible_ts: AtomicU64,
    views: [Mutex<BTreeMap<Ts, usize>>; VIEW_SHARDS],
    next_txn: AtomicU64,
    open_txns: AtomicUsize,
    early_lock_release: AtomicBool,
    min_retention_ms: AtomicU64,
    pub(crate) max_txn_bytes: AtomicUsize,
    /// With early lock release off: commits whose locks wait for durability.
    held: Arc<HeldQueue>,
    releaser: OnceLock<()>,
    recovered: OnceLock<()>,
    /// Status counters.
    pub stats: TxnStats,
}

impl std::fmt::Debug for TxnShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnShared")
            .field("visible_ts", &Ts(self.visible_ts.load(Ordering::Relaxed)))
            .field("pending_versions", &self.overlay.len())
            .finish_non_exhaustive()
    }
}

impl Default for TxnShared {
    fn default() -> Self {
        Self::new()
    }
}

fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl TxnShared {
    /// State with the default settings and the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(UnitConfig::default(), Arc::new(SystemClock))
    }

    /// State with `config` over `clock`.
    #[must_use]
    pub fn with_config(config: UnitConfig, clock: Arc<dyn Clock>) -> Self {
        let now_ms = clock.now_ms();
        let hlc = Hlc::new(clock);
        // A millisecond ahead of the wall clock, as a second line of defence
        // behind recovery's `observe` of the store's highest commit
        // timestamp: commits made in the same millisecond before a restart
        // stay below new read views.
        hlc.observe(Ts::from_parts(now_ms + 1, 0));
        let start = hlc.now();
        Self {
            hlc,
            locks: Arc::new(LockTable::new(LockConfig {
                max_lock_bytes_per_txn: config.max_lock_bytes_per_txn,
            })),
            overlay: Overlay::default(),
            commit_latch: Mutex::new(()),
            unapplied: Mutex::new(VecDeque::new()),
            visible_ts: AtomicU64::new(start.0),
            views: std::array::from_fn(|_| Mutex::new(BTreeMap::new())),
            next_txn: AtomicU64::new(1),
            open_txns: AtomicUsize::new(0),
            early_lock_release: AtomicBool::new(config.early_lock_release),
            min_retention_ms: AtomicU64::new(config.min_retention_ms),
            max_txn_bytes: AtomicUsize::new(config.max_txn_bytes),
            held: Arc::default(),
            releaser: OnceLock::new(),
            recovered: OnceLock::new(),
            stats: TxnStats::default(),
        }
    }

    /// Turns early lock release on or off (benchmarks compare the two).
    pub fn set_early_lock_release(&self, on: bool) {
        self.early_lock_release.store(on, Ordering::Relaxed);
    }

    /// Whether early lock release is on.
    #[must_use]
    pub fn early_lock_release(&self) -> bool {
        self.early_lock_release.load(Ordering::Relaxed)
    }

    /// Folds in a timestamp found in recovered data, so new commits are
    /// above it.
    pub fn observe(&self, ts: Ts) {
        self.hlc.observe(ts);
        let now = self.hlc.now();
        self.visible_ts.fetch_max(now.0, Ordering::AcqRel);
    }

    /// The lock table.
    #[must_use]
    pub fn locks(&self) -> &Arc<LockTable> {
        &self.locks
    }

    /// Versions queued in the log and not yet applied (or not yet pruned).
    #[must_use]
    pub fn pending_versions(&self) -> usize {
        self.overlay.len()
    }

    /// Transactions not yet committed or rolled back.
    #[must_use]
    pub fn open_transactions(&self) -> usize {
        self.open_txns.load(Ordering::Relaxed)
    }

    /// A new timestamp from the unit's clock.
    pub fn now(&self) -> Ts {
        self.hlc.now()
    }

    /// The timestamp a read view opened now would take.
    #[must_use]
    pub fn visible_ts(&self) -> Ts {
        Ts(self.visible_ts.load(Ordering::Acquire))
    }

    /// Once per unit, before the first transaction: raises the clock and
    /// the read-view timestamp above the highest commit timestamp the
    /// commit path recovered, if it reports one (the engine already does
    /// this when it opens).
    pub(crate) fn recover_once(&self, path: &dyn CommitPath) {
        self.recovered.get_or_init(|| {
            if let Some(ts) = path.recovered_max_ts() {
                self.observe(ts);
            }
        });
    }

    pub(crate) fn next_txn_id(&self) -> TxnId {
        self.open_txns.fetch_add(1, Ordering::Relaxed);
        self.next_txn.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn txn_closed(&self) {
        self.open_txns.fetch_sub(1, Ordering::Relaxed);
    }

    // ----- read views -------------------------------------------------------

    pub(crate) fn open_view(&self, txn: TxnId) -> Ts {
        let mut shard = lock_or_recover(&self.views[shard_of(txn)]);
        // Read under the shard latch: the watermark reads `visible_ts`
        // before the shards, so it can never pass a view being opened.
        let rts = Ts(self.visible_ts.load(Ordering::Acquire));
        *shard.entry(rts).or_insert(0) += 1;
        rts
    }

    pub(crate) fn close_view(&self, txn: TxnId, rts: Ts) {
        let mut shard = lock_or_recover(&self.views[shard_of(txn)]);
        if let Some(n) = shard.get_mut(&rts) {
            *n -= 1;
            if *n == 0 {
                shard.remove(&rts);
            }
        }
    }

    /// The timestamp at or below which only the newest version of each key
    /// is still needed: the oldest open read view, the last queued commit,
    /// and `now_ms - min_retention_ms`, whichever is oldest.
    #[must_use]
    pub fn gc_watermark(&self, now_ms: u64) -> Ts {
        let mut wm = Ts(self.visible_ts.load(Ordering::Acquire));
        for s in &self.views {
            if let Some((&oldest, _)) = lock_or_recover(s).iter().next() {
                wm = wm.min(oldest);
            }
        }
        let retention = self.min_retention_ms.load(Ordering::Relaxed);
        wm.min(Ts::from_parts(now_ms.saturating_sub(retention), 0))
    }

    // ----- commit -------------------------------------------------------------

    /// Queues one transaction's writes: takes `cts`, proposes the versions to
    /// the log, publishes them in the overlay, then releases the locks
    /// (early lock release) or hands them to the releaser (released at
    /// durability).
    pub(crate) fn commit_writes(
        &self,
        path: &dyn CommitPath,
        txn: TxnId,
        writes: Vec<(Bytes, Option<Bytes>)>,
        durability: Option<Durability>,
    ) -> Result<(Ts, DurableTicket)> {
        let size: usize = writes
            .iter()
            .map(|(k, v)| k.len() + 24 + v.as_ref().map_or(0, Bytes::len))
            .sum();
        let mut val = Vec::new();
        let (cts, ticket) = {
            let _latch = lock_or_recover(&self.commit_latch);
            let cts = self.hlc.now();
            let mut p = Payload::with_capacity(size);
            for (k, v) in &writes {
                val.clear();
                encode_value_into(&mut val, v.as_deref());
                p.put(Cf::Data, &keys::versioned(k, cts), &val);
            }
            let ticket = path.propose(p.into_bytes(), durability)?;
            self.overlay
                .insert(cts, &ticket, writes.iter().map(|(k, v)| (k, v)));
            lock_or_recover(&self.unapplied)
                .push_back((ticket.index(), writes.into_iter().map(|(k, _)| k).collect()));
            self.visible_ts.store(cts.0, Ordering::Release);
            (cts, ticket)
        };
        if self.early_lock_release() {
            self.locks.release_all(txn);
        } else {
            self.hold_until_durable(ticket.clone(), txn);
        }
        let n = self.stats.commits.fetch_add(1, Ordering::Relaxed) + 1;
        self.prune(path);
        if n.is_multiple_of(WATERMARK_EVERY) {
            path.set_gc_watermark(self.gc_watermark(SystemClock.now_ms()));
        }
        Ok((cts, ticket))
    }

    /// Drops overlay versions the store has applied.
    pub(crate) fn prune(&self, path: &dyn CommitPath) {
        if self.overlay.is_empty() {
            return;
        }
        let applied = path.applied_index();
        let Ok(mut q) = self.unapplied.try_lock() else {
            return;
        };
        while q.front().is_some_and(|(i, _)| *i <= applied) {
            if let Some((i, keys)) = q.pop_front() {
                self.overlay.remove_applied(i, keys.iter());
            }
        }
    }

    fn hold_until_durable(&self, ticket: DurableTicket, txn: TxnId) {
        self.releaser.get_or_init(|| {
            let held = Arc::downgrade(&self.held);
            let locks = Arc::downgrade(&self.locks);
            let _ = std::thread::Builder::new()
                .name("sprig-txn-release".into())
                .spawn(move || release_loop(&held, &locks));
        });
        let (q, cv) = &*self.held;
        lock_or_recover(q).push_back((ticket, txn));
        cv.notify_one();
    }
}

/// Releases held locks in commit order as each commit becomes durable (or
/// its log fails). Ends when the unit is gone.
fn release_loop(held: &Weak<HeldQueue>, locks: &Weak<LockTable>) {
    loop {
        let Some(h) = held.upgrade() else { return };
        let next = {
            let (q, cv) = &*h;
            let mut g = lock_or_recover(q);
            if g.is_empty() {
                g = cv
                    .wait_timeout(g, Duration::from_millis(100))
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
            }
            g.pop_front()
        };
        drop(h);
        if let Some((ticket, txn)) = next {
            let _ = ticket.wait();
            if let Some(l) = locks.upgrade() {
                l.release_all(txn);
            }
        }
    }
}

fn shard_of(txn: TxnId) -> usize {
    usize::try_from(txn % VIEW_SHARDS as u64).unwrap_or(0)
}
