//! The per-unit lock table.
//!
//! Locks live in memory on the unit leader and are never replicated. There
//! are two kinds, both keyed by logical keys (no version suffix):
//!
//! - **Record locks**, shared (S) and exclusive (X), on one key each, in an
//!   ordered map. A key need not exist to be locked.
//! - **Gap locks** ([`gaps`]): stretches of an index a transaction has
//!   closed to other transactions' inserts. A next-key lock is a record lock
//!   plus the gap below it; a locking range read holds the range it scanned.
//!
//! The rules follow `InnoDB` as clients observe it:
//! - S is compatible with S; X conflicts with everything. A transaction never
//!   conflicts with itself, and a granted X covers a later S request.
//! - Record waits are FIFO: a request waits if it conflicts with a lock
//!   another transaction holds, or with an earlier request still waiting.
//!   Releasing grants waiters in queue order while they stay compatible.
//! - Gap locks are always granted at once and never conflict with each
//!   other. They block only inserts: an insert waits (its *insert
//!   intention*) while another transaction's gap covers its key. Nobody
//!   waits for a waiting insert.
//! - A range read must not miss a row another transaction has inserted but
//!   not yet committed. Such a row is invisible in the store, but its
//!   inserter holds the key's X lock, so [`LockTable::gap_acquire`] reports
//!   the first key in the range that another transaction holds exclusively,
//!   and the reader waits for it like for any locked row.
//! - Every new wait runs a cycle check over the waits-for graph. The victim is
//!   the transaction in the cycle with the smallest weight (write-set entries
//!   plus locks held); on a tie the requester is the victim.
//! - Lock memory is charged per transaction; past the cap the request fails
//!   with 1206.
//!
//! One mutex guards the table. Every critical section is a few map lookups;
//! sharding the table is a later step if profiles show contention.

mod gaps;
mod report;
mod wait;

use std::collections::btree_map::Entry as MapEntry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;

use gaps::GapSet;
pub use gaps::Pos;
pub use report::{DeadlockMember, DeadlockReport};
pub use wait::{LockWait, WaitOutcome};
use wait::{Slot, SlotState};

/// A transaction's id inside one unit. Ids increase with age: a higher id is
/// a younger transaction.
pub type TxnId = u64;

/// A record lock's mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockMode {
    /// `FOR SHARE` / `LOCK IN SHARE MODE`, duplicate and FK checks.
    Shared,
    /// `FOR UPDATE`, `UPDATE`, `DELETE`, writes.
    Exclusive,
}

impl LockMode {
    /// Whether two transactions may hold `self` and `other` on one key.
    #[must_use]
    pub fn compatible(self, other: LockMode) -> bool {
        self == LockMode::Shared && other == LockMode::Shared
    }

    /// Whether holding `self` already satisfies a request for `req`.
    #[must_use]
    pub fn covers(self, req: LockMode) -> bool {
        self == LockMode::Exclusive || req == LockMode::Shared
    }
}

/// How long a locking request may wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Wait {
    /// Up to the transaction's `innodb_lock_wait_timeout`.
    #[default]
    Block,
    /// `NOWAIT`: fail at once (1205 for MariaDB, 3572 for MySQL).
    NoWait,
    /// `SKIP LOCKED`: skip the row silently.
    SkipLocked,
    /// `WAIT n`.
    Seconds(u32),
}

/// Why a wait was ended from outside the lock table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Interrupt {
    /// `KILL QUERY` (1317).
    Kill,
    /// The statement's time limit passed (1969 for MariaDB, 3024 for MySQL).
    StatementTimeout,
}

/// Lock table settings.
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Per-transaction lock memory, in bytes, before 1206.
    pub max_lock_bytes_per_txn: usize,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            max_lock_bytes_per_txn: 512 << 20,
        }
    }
}

/// Approximate memory charged per record lock beyond its key bytes.
const LOCK_OVERHEAD_BYTES: usize = 64;

/// The answer to a lock request.
#[derive(Debug)]
pub enum Acquire {
    /// Held now. `fresh` is false when the transaction already held a lock
    /// that covers the request.
    Granted {
        /// Whether this request created or upgraded the lock.
        fresh: bool,
        /// Whether it upgraded a shared lock the transaction already held.
        upgrade: bool,
    },
    /// Queued; wait on the handle.
    Wait(LockWait),
    /// Must wait, but the caller asked not to (`NOWAIT`, `SKIP LOCKED`). Not
    /// queued.
    WouldWait,
    /// Queuing would close a cycle and the requester is the victim. Not
    /// queued; the caller rolls back the whole transaction (1213).
    Deadlock,
    /// The transaction's lock memory is exhausted (1206). Not queued.
    TableFull,
}

/// The answer to a gap lock request. A gap lock never waits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapAcquire {
    /// The whole interval is held.
    Granted,
    /// Another transaction holds this key exclusively inside the interval
    /// (it may be inserting it). The interval is held only up to that key,
    /// which is left out; the caller locks the key (waiting for its holder)
    /// and carries on from there.
    StoppedAt(Bytes),
    /// The transaction's lock memory is exhausted (1206).
    TableFull,
}

/// Counters for status output (`Innodb_row_lock_*`, `Innodb_deadlocks`).
#[derive(Debug, Default)]
pub struct LockStats {
    /// Requests that had to wait.
    pub waits: AtomicU64,
    /// Waits that timed out.
    pub timeouts: AtomicU64,
    /// Deadlocks found.
    pub deadlocks: AtomicU64,
    /// Total time spent waiting, in microseconds.
    pub wait_micros: AtomicU64,
    /// Inserts that waited for another transaction's gap lock.
    pub insert_waits: AtomicU64,
}

struct Waiter {
    txn: TxnId,
    mode: LockMode,
    slot: Arc<Slot>,
}

#[derive(Default)]
struct KeyLocks {
    granted: Vec<(TxnId, LockMode)>,
    waiters: VecDeque<Waiter>,
}

/// What a waiting transaction waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WaitOn {
    /// A record lock on this key.
    Record(Bytes),
    /// Other transactions' gap locks over this key (an insert intention).
    Insert(Bytes),
}

/// An insert waiting for gap locks over its key to go.
struct InsertWaiter {
    txn: TxnId,
    key: Bytes,
    slot: Arc<Slot>,
}

struct TxnLocks {
    keys: Vec<Bytes>,
    bytes: usize,
    waiting_on: Option<WaitOn>,
    slot: Option<Arc<Slot>>,
    writes: Arc<AtomicUsize>,
    /// The indexes this transaction holds gap locks in.
    gap_buckets: Vec<Bytes>,
}

impl TxnLocks {
    fn new(writes: &Arc<AtomicUsize>) -> Self {
        Self {
            keys: Vec::new(),
            bytes: 0,
            waiting_on: None,
            slot: None,
            writes: writes.clone(),
            gap_buckets: Vec::new(),
        }
    }
}

#[derive(Default)]
struct State {
    keys: BTreeMap<Bytes, KeyLocks>,
    txns: HashMap<TxnId, TxnLocks>,
    /// Gap locks: index → transaction → its merged intervals. The empty
    /// index holds intervals that span more than one index.
    gaps: HashMap<Bytes, HashMap<TxnId, GapSet>>,
    inserters: Vec<InsertWaiter>,
    last_deadlock: Option<DeadlockReport>,
    /// Transactions that held or waited for locks of a unit whose
    /// leadership left this node: their next lock request, wait or commit
    /// fails with 1213.
    doomed: HashSet<TxnId>,
}

/// The per-unit lock table.
pub struct LockTable {
    state: Mutex<State>,
    config: LockConfig,
    /// How many (index, transaction) pairs hold gap locks. Zero lets an
    /// insert skip its gap check without taking the latch.
    gap_sets: AtomicUsize,
    /// `State::doomed`'s size: zero lets every check skip the latch.
    doomed: AtomicUsize,
    /// Status counters.
    pub stats: LockStats,
}

impl std::fmt::Debug for LockTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockTable").finish_non_exhaustive()
    }
}

impl Default for LockTable {
    fn default() -> Self {
        Self::new(LockConfig::default())
    }
}

impl LockTable {
    /// An empty table.
    #[must_use]
    pub fn new(config: LockConfig) -> Self {
        Self {
            state: Mutex::new(State::default()),
            config,
            gap_sets: AtomicUsize::new(0),
            doomed: AtomicUsize::new(0),
            stats: LockStats::default(),
        }
    }

    /// The unit whose keys fall in `range` stopped being led here: every
    /// transaction holding or waiting for a record or gap lock in it is
    /// doomed (its wait ends now; its next lock request or commit fails
    /// with 1213, and its rollback releases its locks). Returns them.
    pub fn doom_range(&self, range: &crate::keys::KeyRange) -> Vec<TxnId> {
        let mut guard = self.lock();
        let st = &mut *guard;
        let mut hit: HashSet<TxnId> = HashSet::new();
        let upper = match &range.end {
            Some(e) => Bound::Excluded(Bytes::copy_from_slice(e)),
            None => Bound::Unbounded,
        };
        for (_, kl) in st
            .keys
            .range((Bound::Included(Bytes::copy_from_slice(&range.start)), upper))
        {
            hit.extend(kl.granted.iter().map(|(t, _)| *t));
            hit.extend(kl.waiters.iter().map(|w| w.txn));
        }
        for (index, holders) in &st.gaps {
            if range.contains(index) {
                hit.extend(holders.keys().copied());
            }
        }
        for w in &st.inserters {
            if range.contains(&w.key) {
                hit.insert(w.txn);
            }
        }
        let mut out: Vec<TxnId> = hit.into_iter().collect();
        out.sort_unstable();
        for t in &out {
            if st.doomed.insert(*t) {
                self.doomed.fetch_add(1, Ordering::AcqRel);
            }
            if let Some(slot) = Self::withdraw(st, *t) {
                slot.set(SlotState::Interrupted(Interrupt::Kill));
            }
        }
        out
    }

    /// Whether `txn` was doomed by [`LockTable::doom_range`].
    #[must_use]
    pub fn is_doomed(&self, txn: TxnId) -> bool {
        self.doomed.load(Ordering::Acquire) > 0 && self.lock().doomed.contains(&txn)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while holding this latch aborts the process (the engine's
        // rule for engine latches), so poisoning can't be observed in a
        // running node; tests recover the guard.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    // ----- record locks -----------------------------------------------------

    /// Requests `mode` on `key` for `txn`. `writes` is the transaction's
    /// write-set size, read for deadlock weights. With `enqueue` false a
    /// request that would wait returns [`Acquire::WouldWait`]. With
    /// `detect_deadlocks` false a wait is never refused, and only the
    /// timeout ends a cycle.
    pub fn acquire(
        self: &Arc<Self>,
        txn: TxnId,
        writes: &Arc<AtomicUsize>,
        key: &Bytes,
        mode: LockMode,
        enqueue: bool,
        detect_deadlocks: bool,
    ) -> Acquire {
        let mut guard = self.lock();
        self.acquire_in(
            &mut guard,
            txn,
            writes,
            key,
            mode,
            enqueue,
            detect_deadlocks,
        )
    }

    /// The lock an insert's duplicate check takes on `key`. If no other
    /// transaction holds or waits for the key, the insert takes X at once
    /// (nobody is there to conflict with). Otherwise someone else is working
    /// on the key, and the request is for `dup_mode`: S for a plain insert,
    /// so several inserters of one key can all learn whether it exists; X
    /// for `REPLACE` and `ON DUPLICATE KEY UPDATE`. Returns the mode asked
    /// for with the answer.
    #[allow(clippy::too_many_arguments)]
    pub fn acquire_insert_probe(
        self: &Arc<Self>,
        txn: TxnId,
        writes: &Arc<AtomicUsize>,
        key: &Bytes,
        dup_mode: LockMode,
        enqueue: bool,
        detect_deadlocks: bool,
    ) -> (Acquire, LockMode) {
        let mut guard = self.lock();
        let contended = guard.keys.get(key).is_some_and(|e| {
            e.granted.iter().any(|(t, _)| *t != txn) || e.waiters.iter().any(|w| w.txn != txn)
        });
        let mode = if contended {
            dup_mode
        } else {
            LockMode::Exclusive
        };
        let got = self.acquire_in(
            &mut guard,
            txn,
            writes,
            key,
            mode,
            enqueue,
            detect_deadlocks,
        );
        (got, mode)
    }

    #[allow(clippy::too_many_arguments)]
    fn acquire_in(
        self: &Arc<Self>,
        st: &mut State,
        txn: TxnId,
        writes: &Arc<AtomicUsize>,
        key: &Bytes,
        mode: LockMode,
        enqueue: bool,
        detect_deadlocks: bool,
    ) -> Acquire {
        let entry = st.keys.entry(key.clone()).or_default();
        let held = entry
            .granted
            .iter()
            .find(|(t, _)| *t == txn)
            .map(|(_, m)| *m);
        if held.is_some_and(|m| m.covers(mode)) {
            return Acquire::Granted {
                fresh: false,
                upgrade: false,
            };
        }
        let blocked = entry
            .granted
            .iter()
            .any(|(t, m)| *t != txn && !m.compatible(mode))
            || entry
                .waiters
                .iter()
                .any(|w| w.txn != txn && !w.mode.compatible(mode));
        let tl = st.txns.entry(txn).or_insert_with(|| TxnLocks::new(writes));
        if held.is_none()
            && tl.bytes + key.len() + LOCK_OVERHEAD_BYTES > self.config.max_lock_bytes_per_txn
        {
            if entry.granted.is_empty() && entry.waiters.is_empty() {
                st.keys.remove(key);
            }
            return Acquire::TableFull;
        }
        if !blocked {
            if let Some(g) = entry.granted.iter_mut().find(|(t, _)| *t == txn) {
                g.1 = mode;
            } else {
                entry.granted.push((txn, mode));
                tl.keys.push(key.clone());
                tl.bytes += key.len() + LOCK_OVERHEAD_BYTES;
            }
            return Acquire::Granted {
                fresh: true,
                upgrade: held.is_some(),
            };
        }
        if !enqueue {
            if entry.granted.is_empty() && entry.waiters.is_empty() {
                st.keys.remove(key);
            }
            return Acquire::WouldWait;
        }
        let slot = Arc::new(Slot::default());
        entry.waiters.push_back(Waiter {
            txn,
            mode,
            slot: slot.clone(),
        });
        tl.waiting_on = Some(WaitOn::Record(key.clone()));
        tl.slot = Some(slot.clone());
        self.stats.waits.fetch_add(1, Ordering::Relaxed);
        if detect_deadlocks && self.resolve_deadlocks(st, txn) {
            // The requester is the victim: withdraw its request.
            Self::withdraw(st, txn);
            self.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            return Acquire::Deadlock;
        }
        Acquire::Wait(LockWait::new(
            self.clone(),
            txn,
            key.clone(),
            false,
            held.is_some(),
            slot,
        ))
    }

    /// Gives `txn` the X lock on `key` outright: no queue, no memory cap.
    /// Only for rebuilding a prepared transaction's locks after a restart,
    /// before anything else runs; a lock it must hold can't be refused.
    pub fn restore_exclusive(&self, txn: TxnId, writes: &Arc<AtomicUsize>, key: &Bytes) {
        let mut guard = self.lock();
        let st = &mut *guard;
        let entry = st.keys.entry(key.clone()).or_default();
        let tl = st.txns.entry(txn).or_insert_with(|| TxnLocks::new(writes));
        if let Some(g) = entry.granted.iter_mut().find(|(t, _)| *t == txn) {
            g.1 = LockMode::Exclusive;
        } else {
            entry.granted.push((txn, LockMode::Exclusive));
            tl.keys.push(key.clone());
            tl.bytes += key.len() + LOCK_OVERHEAD_BYTES;
        }
    }

    // ----- gap locks ----------------------------------------------------------

    /// Whether any transaction holds a gap lock. Read without the latch.
    #[must_use]
    pub fn has_gaps(&self) -> bool {
        self.gap_sets.load(Ordering::Acquire) != 0
    }

    /// Takes a gap lock on the places in `[lo, hi)` for `txn`. It is granted
    /// at once: gap locks never wait and never conflict with each other.
    ///
    /// With `stop_at_foreign_x`, the interval stops short of the first key
    /// in it (the last, when `reverse`) that another transaction holds
    /// exclusively, unless that transaction is itself waiting to insert the
    /// key: the holder may be inserting a row this reader can't see yet.
    /// Then only the part before that key is locked (after it, when
    /// `reverse`) and the key is returned in [`GapAcquire::StoppedAt`].
    pub fn gap_acquire(
        &self,
        txn: TxnId,
        writes: &Arc<AtomicUsize>,
        lo: &Pos,
        hi: &Pos,
        reverse: bool,
        stop_at_foreign_x: bool,
    ) -> GapAcquire {
        if lo >= hi {
            return GapAcquire::Granted;
        }
        let mut guard = self.lock();
        let st = &mut *guard;
        let (mut lo, mut hi) = (lo.clone(), hi.clone());
        let mut stopped = None;
        if stop_at_foreign_x && !st.keys.is_empty() {
            let from: Bound<&[u8]> = match &lo {
                Pos::Key(k, false) => Bound::Included(&k[..]),
                Pos::Key(k, true) => Bound::Excluded(&k[..]),
                Pos::End => Bound::Unbounded,
            };
            let to: Bound<&[u8]> = match &hi {
                Pos::Key(k, false) => Bound::Excluded(&k[..]),
                Pos::Key(k, true) => Bound::Included(&k[..]),
                Pos::End => Bound::Unbounded,
            };
            let txns = &st.txns;
            let foreign = |(k, e): &(&Bytes, &KeyLocks)| {
                e.granted.iter().any(|(t, m)| {
                    *t != txn
                        && *m == LockMode::Exclusive
                        && !matches!(
                            txns.get(t).and_then(|tl| tl.waiting_on.as_ref()),
                            Some(WaitOn::Insert(w)) if w == *k
                        )
                })
            };
            let mut range = st.keys.range::<[u8], _>((from, to));
            let hit = if reverse {
                range.rfind(foreign)
            } else {
                range.find(foreign)
            };
            if let Some((k, _)) = hit {
                stopped = Some(k.clone());
            }
        }
        if let Some(k) = &stopped {
            if reverse {
                lo = Pos::after(k.clone());
            } else {
                hi = Pos::at(k.clone());
            }
        }
        if lo < hi {
            let tl = st.txns.entry(txn).or_insert_with(|| TxnLocks::new(writes));
            let bucket = gaps::bucket_of(&lo, &hi);
            let already = st.gaps.get(&bucket).and_then(|m| m.get(&txn));
            if already.is_none_or(|gs| !gs.covers_range(&lo, &hi))
                && tl.bytes + gaps::cost_of(&lo, &hi) > self.config.max_lock_bytes_per_txn
            {
                return GapAcquire::TableFull;
            }
            let sets = st.gaps.entry(bucket.clone()).or_default();
            let gs = match sets.entry(txn) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    tl.gap_buckets.push(bucket);
                    self.gap_sets.fetch_add(1, Ordering::AcqRel);
                    e.insert(GapSet::default())
                }
            };
            let (before, after) = gs.add(lo, hi);
            tl.bytes = (tl.bytes + after).saturating_sub(before);
        }
        match stopped {
            Some(k) => GapAcquire::StoppedAt(k),
            None => GapAcquire::Granted,
        }
    }

    /// The other transactions whose gap locks cover `key`.
    fn gap_holders(st: &State, txn: TxnId, key: &Bytes, out: &mut Vec<TxnId>) {
        if st.gaps.is_empty() {
            return;
        }
        let p = Pos::at(key.clone());
        let own = gaps::bucket_of_key(key).and_then(|b| st.gaps.get(b));
        let wide = st.gaps.get(&b""[..]);
        for sets in [own, wide].into_iter().flatten() {
            for (t, gs) in sets {
                if *t != txn && gs.covers(&p) && !out.contains(t) {
                    out.push(*t);
                }
            }
        }
    }

    /// An insert's check before it writes `key`, whose X lock `txn` already
    /// holds: granted when no other transaction's gap lock covers the key.
    /// Otherwise the insert waits (or, with `enqueue` false, is refused with
    /// [`Acquire::WouldWait`]). A wait that ends with
    /// [`WaitOutcome::Granted`] means the gaps that blocked it are gone; the
    /// caller asks again, because a new one may have appeared.
    pub fn insert_intent(
        self: &Arc<Self>,
        txn: TxnId,
        writes: &Arc<AtomicUsize>,
        key: &Bytes,
        enqueue: bool,
        detect_deadlocks: bool,
    ) -> Acquire {
        if !self.has_gaps() {
            return Acquire::Granted {
                fresh: false,
                upgrade: false,
            };
        }
        let mut guard = self.lock();
        let st = &mut *guard;
        let mut holders = Vec::new();
        Self::gap_holders(st, txn, key, &mut holders);
        if holders.is_empty() {
            return Acquire::Granted {
                fresh: false,
                upgrade: false,
            };
        }
        if !enqueue {
            return Acquire::WouldWait;
        }
        let slot = Arc::new(Slot::default());
        st.inserters.push(InsertWaiter {
            txn,
            key: key.clone(),
            slot: slot.clone(),
        });
        let tl = st.txns.entry(txn).or_insert_with(|| TxnLocks::new(writes));
        tl.waiting_on = Some(WaitOn::Insert(key.clone()));
        tl.slot = Some(slot.clone());
        self.stats.waits.fetch_add(1, Ordering::Relaxed);
        self.stats.insert_waits.fetch_add(1, Ordering::Relaxed);
        if detect_deadlocks && self.resolve_deadlocks(st, txn) {
            Self::withdraw(st, txn);
            self.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            return Acquire::Deadlock;
        }
        Acquire::Wait(LockWait::new(
            self.clone(),
            txn,
            key.clone(),
            true,
            false,
            slot,
        ))
    }

    /// Wakes every waiting insert whose key no gap lock covers any more.
    fn recheck_inserters(st: &mut State) {
        let mut i = 0;
        let mut holders = Vec::new();
        while i < st.inserters.len() {
            holders.clear();
            Self::gap_holders(st, st.inserters[i].txn, &st.inserters[i].key, &mut holders);
            if !holders.is_empty() {
                i += 1;
                continue;
            }
            let w = st.inserters.swap_remove(i);
            if let Some(tl) = st.txns.get_mut(&w.txn) {
                tl.waiting_on = None;
                tl.slot = None;
            }
            w.slot.set(SlotState::Granted);
        }
    }

    /// Drops `txn`'s gap locks. Returns whether it held any.
    fn drop_gaps(&self, st: &mut State, txn: TxnId) -> bool {
        let Some(tl) = st.txns.get_mut(&txn) else {
            return false;
        };
        let buckets = std::mem::take(&mut tl.gap_buckets);
        let mut freed = 0;
        for b in &buckets {
            if let Some(sets) = st.gaps.get_mut(b) {
                if let Some(gs) = sets.remove(&txn) {
                    freed += gs.bytes();
                    self.gap_sets.fetch_sub(1, Ordering::AcqRel);
                }
                if sets.is_empty() {
                    st.gaps.remove(b);
                }
            }
        }
        if let Some(tl) = st.txns.get_mut(&txn) {
            tl.bytes = tl.bytes.saturating_sub(freed);
        }
        !buckets.is_empty()
    }

    // ----- deadlocks ------------------------------------------------------------

    /// Breaks every cycle through `requester`. Returns true when the
    /// requester itself is the victim; other victims are woken with
    /// [`WaitOutcome::Deadlock`].
    fn resolve_deadlocks(&self, st: &mut State, requester: TxnId) -> bool {
        while let Some(cycle) = Self::find_cycle(st, requester) {
            let weight = |t: TxnId| {
                st.txns
                    .get(&t)
                    .map_or(0, |tl| tl.keys.len() + tl.writes.load(Ordering::Relaxed))
            };
            // Smallest weight. On a tie the requester is the victim; among
            // other tied members, the youngest.
            let min = cycle.iter().map(|&t| weight(t)).min().unwrap_or(0);
            let victim = if weight(requester) == min {
                requester
            } else {
                cycle
                    .iter()
                    .copied()
                    .filter(|&t| weight(t) == min)
                    .max()
                    .unwrap_or(requester)
            };
            st.last_deadlock = Some(Self::describe(st, &cycle, victim));
            if victim == requester {
                return true;
            }
            self.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            if let Some(slot) = Self::withdraw(st, victim) {
                slot.set(SlotState::Deadlock);
            }
        }
        false
    }

    /// What the deadlock report says about a cycle.
    fn describe(st: &State, cycle: &[TxnId], victim: TxnId) -> DeadlockReport {
        let mut buf = Vec::new();
        let members = cycle
            .iter()
            .map(|&t| {
                let tl = st.txns.get(&t);
                let (waits_for_key, insert) = match tl.and_then(|tl| tl.waiting_on.as_ref()) {
                    Some(WaitOn::Record(k)) => (k.clone(), false),
                    Some(WaitOn::Insert(k)) => (k.clone(), true),
                    None => (Bytes::new(), false),
                };
                let mode = if insert {
                    LockMode::Exclusive
                } else {
                    st.keys
                        .get(&waits_for_key)
                        .and_then(|e| e.waiters.iter().find(|w| w.txn == t).map(|w| w.mode))
                        .unwrap_or(LockMode::Exclusive)
                };
                Self::blockers(st, t, &mut buf);
                DeadlockMember {
                    txn: t,
                    waits_for_key,
                    mode,
                    insert_intention: insert,
                    blocked_by: buf.clone(),
                    locks_held: tl.map_or(0, |tl| tl.keys.len()),
                    undo_entries: tl.map_or(0, |tl| tl.writes.load(Ordering::Relaxed)),
                }
            })
            .collect();
        DeadlockReport {
            at: std::time::SystemTime::now(),
            members,
            victim,
        }
    }

    /// The most recent deadlock this table found, for the `LATEST DETECTED
    /// DEADLOCK` section of the engine status.
    #[must_use]
    pub fn latest_deadlock(&self) -> Option<DeadlockReport> {
        self.lock().last_deadlock.clone()
    }

    /// The transactions `t`'s pending request waits for.
    fn blockers(st: &State, t: TxnId, out: &mut Vec<TxnId>) {
        out.clear();
        let key = match st.txns.get(&t).and_then(|tl| tl.waiting_on.as_ref()) {
            None => return,
            Some(WaitOn::Insert(key)) => {
                Self::gap_holders(st, t, key, out);
                return;
            }
            Some(WaitOn::Record(key)) => key,
        };
        let Some(entry) = st.keys.get(key) else {
            return;
        };
        let Some(pos) = entry.waiters.iter().position(|w| w.txn == t) else {
            return;
        };
        let mode = entry.waiters[pos].mode;
        for (h, m) in &entry.granted {
            if *h != t && !m.compatible(mode) {
                out.push(*h);
            }
        }
        for w in entry.waiters.iter().take(pos) {
            if w.txn != t && !w.mode.compatible(mode) {
                out.push(w.txn);
            }
        }
    }

    /// A cycle in the waits-for graph through `start`, as the list of its
    /// members (including `start`).
    fn find_cycle(st: &State, start: TxnId) -> Option<Vec<TxnId>> {
        let mut visited = HashSet::new();
        let mut path: Vec<(TxnId, Vec<TxnId>, usize)> = Vec::new();
        let mut buf = Vec::new();
        Self::blockers(st, start, &mut buf);
        path.push((start, buf.clone(), 0));
        visited.insert(start);
        while let Some((_, next, idx)) = path.last_mut() {
            if *idx >= next.len() {
                path.pop();
                continue;
            }
            let b = next[*idx];
            *idx += 1;
            if b == start {
                return Some(path.iter().map(|(t, _, _)| *t).collect());
            }
            if visited.insert(b) {
                Self::blockers(st, b, &mut buf);
                path.push((b, buf.clone(), 0));
            }
        }
        None
    }

    // ----- waits ----------------------------------------------------------------

    /// Withdraws whatever request `txn` has queued (record or insert),
    /// granting whoever was behind it. Returns the request's slot.
    fn withdraw(st: &mut State, txn: TxnId) -> Option<Arc<Slot>> {
        let target = st.txns.get_mut(&txn).and_then(|tl| {
            tl.slot = None;
            tl.waiting_on.take()
        })?;
        match target {
            WaitOn::Record(key) => {
                let mut slot = None;
                if let Some(entry) = st.keys.get_mut(&key)
                    && let Some(pos) = entry.waiters.iter().position(|w| w.txn == txn)
                {
                    slot = entry.waiters.remove(pos).map(|w| w.slot);
                }
                Self::regrant(st, &key);
                slot
            }
            WaitOn::Insert(_) => st
                .inserters
                .iter()
                .position(|w| w.txn == txn)
                .map(|pos| st.inserters.swap_remove(pos).slot),
        }
    }

    fn drop_if_empty(st: &mut State, key: &Bytes) {
        if let MapEntry::Occupied(e) = st.keys.entry(key.clone())
            && e.get().granted.is_empty()
            && e.get().waiters.is_empty()
        {
            e.remove();
        }
    }

    /// Grants queued requests on `key` in FIFO order while they are
    /// compatible with what is granted and with earlier waiters.
    fn regrant(st: &mut State, key: &Bytes) {
        let Some(entry) = st.keys.get_mut(key) else {
            return;
        };
        let mut i = 0;
        while i < entry.waiters.len() {
            let w = &entry.waiters[i];
            let ok = entry
                .granted
                .iter()
                .all(|(t, m)| *t == w.txn || m.compatible(w.mode))
                && entry
                    .waiters
                    .iter()
                    .take(i)
                    .all(|e| e.txn == w.txn || e.mode.compatible(w.mode));
            if !ok {
                i += 1;
                continue;
            }
            let Some(w) = entry.waiters.remove(i) else {
                break;
            };
            if let Some(g) = entry.granted.iter_mut().find(|(t, _)| *t == w.txn) {
                g.1 = w.mode;
            } else {
                entry.granted.push((w.txn, w.mode));
                if let Some(tl) = st.txns.get_mut(&w.txn) {
                    tl.keys.push(key.clone());
                    tl.bytes += key.len() + LOCK_OVERHEAD_BYTES;
                }
            }
            if let Some(tl) = st.txns.get_mut(&w.txn) {
                tl.waiting_on = None;
                tl.slot = None;
            }
            w.slot.set(SlotState::Granted);
        }
        Self::drop_if_empty(st, key);
    }

    /// Ends a wait that timed out or was abandoned. Returns the outcome
    /// that won the race if the wait had already ended, otherwise withdraws
    /// the request.
    pub(crate) fn cancel_wait(&self, txn: TxnId, slot: &Arc<Slot>) -> WaitOutcome {
        let mut guard = self.lock();
        match slot.get() {
            SlotState::Granted => return WaitOutcome::Granted,
            SlotState::Deadlock => return WaitOutcome::Deadlock,
            SlotState::Interrupted(why) => return WaitOutcome::Interrupted(why),
            SlotState::Waiting => {}
        }
        let st = &mut *guard;
        // Only this request: the transaction can't have queued another
        // while this one's handle is alive.
        if st
            .txns
            .get(&txn)
            .and_then(|tl| tl.slot.as_ref())
            .is_some_and(|s| Arc::ptr_eq(s, slot))
        {
            Self::withdraw(st, txn);
        }
        self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
        WaitOutcome::TimedOut
    }

    /// Ends `txn`'s current wait, if it has one, with
    /// [`WaitOutcome::Interrupted`] (`KILL QUERY`, a statement time limit).
    /// Returns whether a wait was ended.
    pub fn interrupt(&self, txn: TxnId, why: Interrupt) -> bool {
        let mut guard = self.lock();
        let st = &mut *guard;
        match Self::withdraw(st, txn) {
            Some(slot) => {
                slot.set(SlotState::Interrupted(why));
                true
            }
            None => false,
        }
    }

    // ----- release ----------------------------------------------------------------

    /// Releases one record lock (READ COMMITTED's unlock of a row that
    /// failed the `WHERE`).
    pub fn unlock(&self, txn: TxnId, key: &Bytes) {
        let mut guard = self.lock();
        let st = &mut *guard;
        if let Some(entry) = st.keys.get_mut(key) {
            entry.granted.retain(|(t, _)| *t != txn);
        }
        // The row being unlocked is nearly always the last one locked.
        if let Some(tl) = st.txns.get_mut(&txn)
            && let Some(pos) = tl.keys.iter().rposition(|k| k == key)
        {
            tl.keys.swap_remove(pos);
            tl.bytes = tl.bytes.saturating_sub(key.len() + LOCK_OVERHEAD_BYTES);
        }
        Self::regrant(st, key);
    }

    /// Turns `txn`'s X lock on `key` into an S lock, granting waiters that
    /// are compatible with it. An insert that finds its key already present
    /// keeps only a shared lock on it.
    pub fn downgrade(&self, txn: TxnId, key: &Bytes) {
        let mut guard = self.lock();
        let st = &mut *guard;
        if let Some(entry) = st.keys.get_mut(key)
            && let Some(g) = entry.granted.iter_mut().find(|(t, _)| *t == txn)
        {
            g.1 = LockMode::Shared;
        }
        Self::regrant(st, key);
    }

    /// Releases every lock `txn` holds and withdraws any request it has
    /// queued (commit, rollback, deadlock victim).
    pub fn release_all(&self, txn: TxnId) {
        let mut guard = self.lock();
        let st = &mut *guard;
        if self.doomed.load(Ordering::Acquire) > 0 && st.doomed.remove(&txn) {
            self.doomed.fetch_sub(1, Ordering::AcqRel);
        }
        if !st.txns.contains_key(&txn) {
            return;
        }
        Self::withdraw(st, txn);
        let had_gaps = self.drop_gaps(st, txn);
        let Some(tl) = st.txns.remove(&txn) else {
            return;
        };
        for key in tl.keys {
            if let Some(entry) = st.keys.get_mut(&key) {
                entry.granted.retain(|(t, _)| *t != txn);
            }
            Self::regrant(st, &key);
        }
        if had_gaps {
            Self::recheck_inserters(st);
        }
    }

    /// Releases `txn`'s gap locks and shared record locks, keeping its
    /// exclusive record locks: what `XA PREPARE` does, so a prepared
    /// transaction never blocks an insert or a reader's shared lock.
    pub fn release_shared_and_gaps(&self, txn: TxnId) {
        let mut guard = self.lock();
        let st = &mut *guard;
        let had_gaps = self.drop_gaps(st, txn);
        let mut shared = Vec::new();
        if let Some(tl) = st.txns.get_mut(&txn) {
            let keys = &st.keys;
            let mut freed = 0;
            tl.keys.retain(|k| {
                let s = keys.get(k).is_some_and(|e| {
                    e.granted
                        .iter()
                        .any(|(t, m)| *t == txn && *m == LockMode::Shared)
                });
                if s {
                    freed += k.len() + LOCK_OVERHEAD_BYTES;
                    shared.push(k.clone());
                }
                !s
            });
            tl.bytes = tl.bytes.saturating_sub(freed);
        }
        for key in shared {
            if let Some(entry) = st.keys.get_mut(&key) {
                entry.granted.retain(|(t, _)| *t != txn);
            }
            Self::regrant(st, &key);
        }
        if had_gaps {
            Self::recheck_inserters(st);
        }
    }

    // ----- inspection ---------------------------------------------------------------

    /// Number of record locks `txn` holds.
    #[must_use]
    pub fn held_by(&self, txn: TxnId) -> usize {
        self.lock().txns.get(&txn).map_or(0, |tl| tl.keys.len())
    }

    /// The exclusive record locks `txn` holds, in key order.
    #[must_use]
    pub fn exclusive_keys(&self, txn: TxnId) -> Vec<Bytes> {
        let st = self.lock();
        let mut out: Vec<Bytes> = st
            .txns
            .get(&txn)
            .map(|tl| {
                tl.keys
                    .iter()
                    .filter(|k| {
                        st.keys.get(*k).is_some_and(|e| {
                            e.granted
                                .iter()
                                .any(|(t, m)| *t == txn && *m == LockMode::Exclusive)
                        })
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        out.sort_unstable();
        out
    }

    /// Lock memory charged to `txn`, in bytes.
    #[must_use]
    pub fn bytes_held_by(&self, txn: TxnId) -> usize {
        self.lock().txns.get(&txn).map_or(0, |tl| tl.bytes)
    }

    /// The mode `txn` holds on `key`, if any.
    #[must_use]
    pub fn mode_held(&self, txn: TxnId, key: &[u8]) -> Option<LockMode> {
        self.lock()
            .keys
            .get(key)
            .and_then(|e| e.granted.iter().find(|(t, _)| *t == txn).map(|(_, m)| *m))
    }

    /// Whether a gap lock of `txn` covers `key`: another transaction's
    /// insert of `key` would wait for `txn`.
    #[must_use]
    pub fn gap_covers(&self, txn: TxnId, key: &Bytes) -> bool {
        let st = self.lock();
        let p = Pos::at(key.clone());
        let own = gaps::bucket_of_key(key).and_then(|b| st.gaps.get(b));
        let wide = st.gaps.get(&b""[..]);
        [own, wide]
            .into_iter()
            .flatten()
            .any(|sets| sets.get(&txn).is_some_and(|gs| gs.covers(&p)))
    }

    /// The merged gap intervals `txn` holds, over every index.
    #[must_use]
    pub fn gaps_held_by(&self, txn: TxnId) -> Vec<(Pos, Pos)> {
        let st = self.lock();
        let mut out: Vec<(Pos, Pos)> = st
            .gaps
            .values()
            .filter_map(|sets| sets.get(&txn))
            .flat_map(|gs| gs.intervals().map(|(l, h)| (l.clone(), h.clone())))
            .collect();
        out.sort_unstable();
        out
    }

    /// Keys with any record lock or waiter (for leak checks in tests).
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.lock().keys.len()
    }

    /// Gap intervals held by all transactions, plus waiting inserts (for
    /// leak checks in tests).
    #[must_use]
    pub fn gap_count(&self) -> usize {
        let st = self.lock();
        st.gaps
            .values()
            .flat_map(|sets| sets.values())
            .map(GapSet::len)
            .sum::<usize>()
            + st.inserters.len()
    }

    /// The current waits-for edges, `(waiter, holder)`. Distribution's
    /// cross-unit detector consumes these.
    #[must_use]
    pub fn waits_for(&self) -> Vec<(TxnId, TxnId)> {
        let st = self.lock();
        let mut out = Vec::new();
        let mut buf = Vec::new();
        for (&t, tl) in &st.txns {
            if tl.waiting_on.is_some() {
                Self::blockers(&st, t, &mut buf);
                out.extend(buf.iter().map(|&h| (t, h)));
            }
        }
        out
    }

    /// Records time spent waiting.
    pub(crate) fn note_wait(&self, waited: Duration) {
        self.stats.wait_micros.fetch_add(
            u64::try_from(waited.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

#[cfg(test)]
mod tests;
