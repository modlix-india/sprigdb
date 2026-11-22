//! One unit's transaction state: the clock, the lock table, the pending
//! overlay, read views, and the commit path with early lock release.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, Weak};
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

/// A transaction after `XA PREPARE`: it belongs to the unit, not to a
/// session, until it is committed or rolled back by its XID.
#[derive(Debug, Clone)]
pub(crate) struct PreparedTxn {
    /// The id its locks are held under.
    pub(crate) id: TxnId,
    /// Its buffered writes, in key order.
    pub(crate) writes: Vec<(Bytes, Option<Bytes>)>,
    /// The log entry of its prepare record.
    pub(crate) ticket: DurableTicket,
    /// The durability class its session asked for.
    pub(crate) durability: Option<Durability>,
    /// The unit holding its prepare record.
    pub(crate) home: u64,
    /// A session is committing or rolling it back right now. It stays
    /// listed meanwhile, so its XID stays taken.
    pub(crate) finishing: bool,
}

/// Commit timestamps taken and not yet published, and the newest one
/// published.
#[derive(Debug, Default)]
struct Inflight {
    open: BTreeMap<Ts, u32>,
    done_max: Ts,
}

/// One part of a commit: the writes, plain writes and notes for one unit.
#[derive(Default)]
struct UnitPart<'a> {
    writes: Vec<(Bytes, Option<Bytes>)>,
    extras: Vec<&'a super::transaction::Extra>,
    delete: Option<&'a [u8]>,
}

/// The node's transaction state, shared by every transaction: one clock,
/// one lock table and one set of read views over all units, and each
/// unit's commit latch.
pub struct TxnShared {
    pub(crate) hlc: Hlc,
    pub(crate) locks: Arc<LockTable>,
    pub(crate) overlay: Overlay,
    /// One commit latch per unit: a unit's commits take their timestamps
    /// and log indexes in the same order. A commit across units holds every
    /// unit's latch, taken in unit order.
    latches: RwLock<HashMap<u64, Arc<Mutex<()>>>>,
    /// Commits that took a timestamp and are not yet in the overlay.
    inflight: Mutex<Inflight>,
    /// Signalled when `visible_ts` moves.
    published: Condvar,
    /// Counts commits published to the overlay (a locking scan rereads
    /// when it moved).
    commit_seq: AtomicU64,
    /// Commits whose versions are still in the overlay: `(unit, index,
    /// keys)`.
    unapplied: Mutex<VecDeque<(u64, u64, Vec<Bytes>)>>,
    /// Every commit at or below this timestamp is in the overlay or the
    /// store. New read views take it, on every unit of the node.
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
    /// Prepared (XA) transactions, by XID.
    prepared: Mutex<HashMap<Vec<u8>, PreparedTxn>>,
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
            latches: RwLock::new(HashMap::new()),
            inflight: Mutex::new(Inflight::default()),
            published: Condvar::new(),
            commit_seq: AtomicU64::new(0),
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
            prepared: Mutex::new(HashMap::new()),
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
            self.load_prepared(path);
        });
    }

    /// An id for a transaction that no session opened (a prepared one read
    /// back after a restart).
    pub(crate) fn fresh_txn_id(&self) -> TxnId {
        self.next_txn.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn note_rollback(&self) {
        self.stats.rollbacks.fetch_add(1, Ordering::Relaxed);
    }

    // ----- prepared transactions ---------------------------------------------

    /// Whether a prepared transaction has `xid`.
    pub(crate) fn xa_known(&self, xid: &[u8]) -> bool {
        lock_or_recover(&self.prepared).contains_key(xid)
    }

    /// Queues `prepared`'s record with `propose` and lists it under `xid`,
    /// in one step, so two sessions can't both prepare one XID.
    pub(crate) fn xa_register(
        &self,
        xid: &[u8],
        mut prepared: PreparedTxn,
        propose: impl FnOnce() -> Result<DurableTicket>,
    ) -> Result<DurableTicket> {
        let mut list = lock_or_recover(&self.prepared);
        if list.contains_key(xid) {
            return Err(super::xa::xaer_dupid());
        }
        let ticket = propose()?;
        prepared.ticket = ticket.clone();
        list.insert(xid.to_vec(), prepared);
        Ok(ticket)
    }

    /// Claims `xid`'s prepared transaction for one session to finish.
    /// `None`: no such transaction, or another session is finishing it. It
    /// stays listed until [`TxnShared::xa_finished`].
    pub(crate) fn xa_claim(&self, xid: &[u8]) -> Option<PreparedTxn> {
        let mut list = lock_or_recover(&self.prepared);
        let p = list.get_mut(xid).filter(|p| !p.finishing)?;
        p.finishing = true;
        Some(p.clone())
    }

    /// Ends a claim: the transaction is finished (`done`) and leaves the
    /// list, or the log refused and it stays prepared.
    pub(crate) fn xa_finished(&self, xid: &[u8], done: bool) {
        let mut list = lock_or_recover(&self.prepared);
        if done {
            list.remove(xid);
        } else if let Some(p) = list.get_mut(xid) {
            p.finishing = false;
        }
    }

    /// Lists a prepared transaction read back after a restart.
    pub(crate) fn xa_restore(&self, xid: &[u8], prepared: PreparedTxn) {
        lock_or_recover(&self.prepared).insert(xid.to_vec(), prepared);
    }

    /// The XIDs of the prepared transactions, sorted.
    pub(crate) fn xa_list(&self) -> Vec<Vec<u8>> {
        let mut xids: Vec<Vec<u8>> = lock_or_recover(&self.prepared).keys().cloned().collect();
        xids.sort_unstable();
        xids
    }

    /// Prepared transactions waiting to be finished.
    #[must_use]
    pub fn prepared_transactions(&self) -> usize {
        lock_or_recover(&self.prepared).len()
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
    /// durability). `also_delete` is a reserved key the same entry removes
    /// (a prepared transaction's record).
    pub(crate) fn commit_writes(
        &self,
        path: &dyn CommitPath,
        txn: TxnId,
        writes: Vec<(Bytes, Option<Bytes>)>,
        durability: Option<Durability>,
        also_delete: Option<&[u8]>,
    ) -> Result<(Ts, DurableTicket)> {
        self.commit_writes_noted(path, txn, writes, durability, also_delete, &[])
    }

    /// [`TxnShared::commit_writes`], with log notes in the same entry.
    ///
    /// Each write goes to the unit its key belongs to, notes to the unit
    /// that carries them. Writes to one unit are one log entry; writes to
    /// several are one entry per unit, proposed as one atomic write, all at
    /// one commit timestamp.
    pub(crate) fn commit_writes_noted(
        &self,
        path: &dyn CommitPath,
        txn: TxnId,
        writes: Vec<(Bytes, Option<Bytes>)>,
        durability: Option<Durability>,
        also_delete: Option<&[u8]>,
        notes: &[super::transaction::Extra],
    ) -> Result<(Ts, DurableTicket)> {
        use super::transaction::Extra;
        let notes_unit = path.notes_unit();
        let mut parts: BTreeMap<u64, UnitPart<'_>> = BTreeMap::new();
        for (k, v) in writes {
            parts
                .entry(path.unit_of(&k))
                .or_default()
                .writes
                .push((k, v));
        }
        if let Some(key) = also_delete {
            parts.entry(path.unit_of(key)).or_default().delete = Some(key);
        }
        for n in notes {
            let u = match n {
                Extra::Note(_) => notes_unit,
                Extra::Put(_, k, _) | Extra::Delete(_, k) => path.unit_of(k),
            };
            parts.entry(u).or_default().extras.push(n);
        }
        if parts.is_empty() {
            parts.entry(notes_unit).or_default();
        }
        let latches: Vec<Arc<Mutex<()>>> = parts.keys().map(|u| self.latch(*u)).collect();
        let (cts, tickets) = {
            let _held: Vec<MutexGuard<'_, ()>> =
                latches.iter().map(|l| lock_or_recover(l)).collect();
            let cts = self.take_commit_ts();
            let mut val = Vec::new();
            let mut payloads = Vec::with_capacity(parts.len());
            for (u, part) in &parts {
                let size: usize = part
                    .writes
                    .iter()
                    .map(|(k, v)| k.len() + 24 + v.as_ref().map_or(0, Bytes::len))
                    .sum();
                let mut p = Payload::with_capacity(size);
                for (k, v) in &part.writes {
                    val.clear();
                    encode_value_into(&mut val, v.as_deref());
                    p.put(Cf::Data, &keys::versioned(k, cts), &val);
                }
                if let Some(key) = part.delete {
                    p.delete(Cf::Data, key);
                }
                for n in &part.extras {
                    match n {
                        Extra::Note(b) => p.note(b),
                        Extra::Put(cf, k, v) => p.put(*cf, k, v),
                        Extra::Delete(cf, k) => p.delete(*cf, k),
                    }
                }
                payloads.push((*u, p.into_bytes()));
            }
            let tickets = match path.propose_many(payloads, durability) {
                Ok(t) => t,
                Err(e) => {
                    self.publish_commit_ts(cts);
                    return Err(e);
                }
            };
            for (part, ticket) in parts.values().zip(&tickets) {
                if part.writes.is_empty() {
                    continue;
                }
                self.overlay
                    .insert(cts, ticket, part.writes.iter().map(|(k, v)| (k, v)));
                lock_or_recover(&self.unapplied).push_back((
                    ticket.unit_key().unwrap_or(u64::MAX),
                    ticket.index(),
                    part.writes.iter().map(|(k, _)| k.clone()).collect(),
                ));
            }
            self.commit_seq.fetch_add(1, Ordering::AcqRel);
            self.publish_commit_ts(cts);
            (cts, tickets)
        };
        // The unit carrying the notes comes first: its entry is the one
        // observers report outcomes under.
        let mut ordered: Vec<DurableTicket> = Vec::with_capacity(tickets.len());
        let mut rest = Vec::with_capacity(tickets.len());
        for (u, t) in parts.keys().zip(tickets) {
            if *u == notes_unit && !notes.is_empty() {
                ordered.push(t);
            } else {
                rest.push(t);
            }
        }
        ordered.extend(rest);
        let ticket = DurableTicket::all(ordered);
        // The session's next read view must see this commit.
        self.wait_visible(cts);
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

    /// `unit`'s commit latch.
    fn latch(&self, unit: u64) -> Arc<Mutex<()>> {
        if let Some(l) = self
            .latches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&unit)
        {
            return Arc::clone(l);
        }
        let mut w = self
            .latches
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(w.entry(unit).or_default())
    }

    /// Takes a commit timestamp and records it as in flight, so read views
    /// stay below it until it is published.
    fn take_commit_ts(&self) -> Ts {
        let mut g = lock_or_recover(&self.inflight);
        let cts = self.hlc.now();
        *g.open.entry(cts).or_insert(0) += 1;
        cts
    }

    /// `cts`'s commit is in the overlay (or failed): read views may move up
    /// to just below the oldest commit still in flight.
    fn publish_commit_ts(&self, cts: Ts) {
        let mut g = lock_or_recover(&self.inflight);
        if let Some(n) = g.open.get_mut(&cts) {
            *n -= 1;
            if *n == 0 {
                g.open.remove(&cts);
            }
        }
        g.done_max = g.done_max.max(cts);
        let visible = match g.open.keys().next() {
            Some(oldest) => Ts(oldest.0.saturating_sub(1)).min(g.done_max),
            None => g.done_max,
        };
        self.visible_ts.fetch_max(visible.0, Ordering::AcqRel);
        drop(g);
        self.published.notify_all();
    }

    /// Waits until read views see `cts`: until every commit that took an
    /// older timestamp (on another unit) is published too. A commit in
    /// flight holds every latch it needs, so this never waits on the
    /// caller.
    fn wait_visible(&self, cts: Ts) {
        if self.visible_ts.load(Ordering::Acquire) >= cts.0 {
            return;
        }
        let mut g = lock_or_recover(&self.inflight);
        while self.visible_ts.load(Ordering::Acquire) < cts.0 {
            g = self
                .published
                .wait(g)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Changes whenever a commit enters the overlay.
    pub(crate) fn commit_seq(&self) -> u64 {
        self.commit_seq.load(Ordering::Acquire)
    }

    /// Drops overlay versions the store has applied.
    pub(crate) fn prune(&self, path: &dyn CommitPath) {
        if self.overlay.is_empty() {
            return;
        }
        let Ok(mut q) = self.unapplied.try_lock() else {
            return;
        };
        while let Some(&(unit, i, _)) = q.front() {
            if i > path.applied_index_of(unit) {
                break;
            }
            if let Some((unit, i, keys)) = q.pop_front() {
                self.overlay.remove_applied(unit, i, keys.iter());
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
