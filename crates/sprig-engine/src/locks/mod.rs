//! The per-unit record lock table.
//!
//! Locks live in memory on the unit leader and are never replicated. E0 has
//! record locks only, shared (S) and exclusive (X), keyed by the logical key
//! (no version suffix). Gap, next-key and insert-intention locks come later;
//! the table's shape leaves room for them beside the record map.
//!
//! The rules follow `InnoDB` as clients observe it:
//! - S is compatible with S; X conflicts with everything. A transaction never
//!   conflicts with itself, and a granted X covers a later S request.
//! - Waits are FIFO: a request waits if it conflicts with a lock another
//!   transaction holds, or with an earlier request still waiting. Releasing
//!   grants waiters in queue order while they stay compatible.
//! - Every new wait runs a cycle check over the waits-for graph. The victim is
//!   the transaction in the cycle with the smallest weight (write-set entries
//!   plus locks held); on a tie the requester is the victim.
//! - Lock memory is charged per transaction; past the cap the request fails
//!   with 1206.
//!
//! One mutex guards the table. Every critical section is a few hash lookups,
//! and E0 runs one unit per node; sharding the map is a later step if
//! profiles show contention.

mod wait;

use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;

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

/// Lock table settings.
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Per-transaction lock memory, in bytes, before 1206.
    pub max_lock_bytes_per_txn: usize,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            max_lock_bytes_per_txn: 64 << 20,
        }
    }
}

/// Approximate memory charged per lock beyond its key bytes.
const LOCK_OVERHEAD_BYTES: usize = 64;

/// The answer to a lock request.
#[derive(Debug)]
pub enum Acquire {
    /// Held now. `fresh` is false when the transaction already held a lock
    /// that covers the request.
    Granted {
        /// Whether this request created or upgraded the lock.
        fresh: bool,
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

struct TxnLocks {
    keys: Vec<Bytes>,
    bytes: usize,
    waiting_on: Option<Bytes>,
    slot: Option<Arc<Slot>>,
    writes: Arc<AtomicUsize>,
}

#[derive(Default)]
struct State {
    keys: HashMap<Bytes, KeyLocks>,
    txns: HashMap<TxnId, TxnLocks>,
}

/// The per-unit lock table.
pub struct LockTable {
    state: Mutex<State>,
    config: LockConfig,
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
            stats: LockStats::default(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while holding this latch aborts the process (the engine's
        // rule for engine latches), so poisoning can't be observed in a
        // running node; tests recover the guard.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

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
        let st = &mut *guard;
        let entry = st.keys.entry(key.clone()).or_default();
        let held = entry
            .granted
            .iter()
            .find(|(t, _)| *t == txn)
            .map(|(_, m)| *m);
        if held.is_some_and(|m| m.covers(mode)) {
            return Acquire::Granted { fresh: false };
        }
        let blocked = entry
            .granted
            .iter()
            .any(|(t, m)| *t != txn && !m.compatible(mode))
            || entry
                .waiters
                .iter()
                .any(|w| w.txn != txn && !w.mode.compatible(mode));
        let tl = st.txns.entry(txn).or_insert_with(|| TxnLocks {
            keys: Vec::new(),
            bytes: 0,
            waiting_on: None,
            slot: None,
            writes: writes.clone(),
        });
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
            return Acquire::Granted { fresh: true };
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
        tl.waiting_on = Some(key.clone());
        tl.slot = Some(slot.clone());
        self.stats.waits.fetch_add(1, Ordering::Relaxed);
        if detect_deadlocks && self.resolve_deadlocks(st, txn) {
            // The requester is the victim: withdraw its request.
            Self::remove_waiter(st, txn, key);
            self.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            return Acquire::Deadlock;
        }
        Acquire::Wait(LockWait::new(self.clone(), txn, key.clone(), slot))
    }

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
            if victim == requester {
                return true;
            }
            self.stats.deadlocks.fetch_add(1, Ordering::Relaxed);
            let key = st.txns.get(&victim).and_then(|tl| tl.waiting_on.clone());
            if let Some(key) = key {
                if let Some(slot) = Self::remove_waiter(st, victim, &key) {
                    slot.set(SlotState::Deadlock);
                }
                Self::regrant(st, &key);
            }
        }
        false
    }

    /// The transactions `t`'s pending request waits for.
    fn blockers(st: &State, t: TxnId, out: &mut Vec<TxnId>) {
        out.clear();
        let Some(key) = st.txns.get(&t).and_then(|tl| tl.waiting_on.as_ref()) else {
            return;
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

    /// Removes `txn`'s waiting request on `key`, returning its slot.
    fn remove_waiter(st: &mut State, txn: TxnId, key: &Bytes) -> Option<Arc<Slot>> {
        let mut slot = None;
        if let Some(entry) = st.keys.get_mut(key)
            && let Some(pos) = entry.waiters.iter().position(|w| w.txn == txn)
        {
            slot = entry.waiters.remove(pos).map(|w| w.slot);
        }
        if let Some(tl) = st.txns.get_mut(&txn)
            && tl.waiting_on.as_ref() == Some(key)
        {
            tl.waiting_on = None;
            tl.slot = None;
        }
        Self::drop_if_empty(st, key);
        slot
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

    /// Ends a wait that timed out or was abandoned. Returns `Granted` if the
    /// grant won the race, otherwise withdraws the request.
    pub(crate) fn cancel_wait(&self, txn: TxnId, key: &Bytes, slot: &Arc<Slot>) -> WaitOutcome {
        let mut guard = self.lock();
        match slot.get() {
            SlotState::Granted => return WaitOutcome::Granted,
            SlotState::Deadlock => return WaitOutcome::Deadlock,
            SlotState::Waiting => {}
        }
        let st = &mut *guard;
        Self::remove_waiter(st, txn, key);
        Self::regrant(st, key);
        self.stats.timeouts.fetch_add(1, Ordering::Relaxed);
        WaitOutcome::TimedOut
    }

    /// Releases one lock (READ COMMITTED's unlock of a row that failed the
    /// `WHERE`).
    pub fn unlock(&self, txn: TxnId, key: &Bytes) {
        let mut guard = self.lock();
        let st = &mut *guard;
        if let Some(entry) = st.keys.get_mut(key) {
            entry.granted.retain(|(t, _)| *t != txn);
        }
        if let Some(tl) = st.txns.get_mut(&txn)
            && let Some(pos) = tl.keys.iter().position(|k| k == key)
        {
            tl.keys.swap_remove(pos);
            tl.bytes = tl.bytes.saturating_sub(key.len() + LOCK_OVERHEAD_BYTES);
        }
        Self::regrant(st, key);
    }

    /// Releases every lock `txn` holds and withdraws any request it has
    /// queued (commit, rollback, deadlock victim).
    pub fn release_all(&self, txn: TxnId) {
        let mut guard = self.lock();
        let st = &mut *guard;
        let Some(tl) = st.txns.remove(&txn) else {
            return;
        };
        if let Some(key) = tl.waiting_on {
            if let Some(entry) = st.keys.get_mut(&key) {
                entry.waiters.retain(|w| w.txn != txn);
            }
            Self::regrant(st, &key);
        }
        for key in tl.keys {
            if let Some(entry) = st.keys.get_mut(&key) {
                entry.granted.retain(|(t, _)| *t != txn);
            }
            Self::regrant(st, &key);
        }
    }

    /// Number of locks `txn` holds.
    #[must_use]
    pub fn held_by(&self, txn: TxnId) -> usize {
        self.lock().txns.get(&txn).map_or(0, |tl| tl.keys.len())
    }

    /// The mode `txn` holds on `key`, if any.
    #[must_use]
    pub fn mode_held(&self, txn: TxnId, key: &[u8]) -> Option<LockMode> {
        self.lock()
            .keys
            .get(key)
            .and_then(|e| e.granted.iter().find(|(t, _)| *t == txn).map(|(_, m)| *m))
    }

    /// Keys with any lock or waiter (for leak checks in tests).
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.lock().keys.len()
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
