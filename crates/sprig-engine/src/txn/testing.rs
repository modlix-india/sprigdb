//! An in-memory commit path for transaction tests: a `MemStore` and a
//! group-commit log whose syncs can be paused, slowed or crashed. Tests use
//! it to hold commits in the "queued, not durable" state on purpose.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use sprig_common::{Result, SprigError};

use super::durable::{DurableTicket, DurableWatch};
use super::hlc::{Clock, SystemClock, Ts};
use super::{CommitPath, TxnShared, UnitConfig};
use crate::keys;
use crate::log::{Durability, payload};
use crate::store::{BatchOp, MemStore, SortedStore, StoreBatch, WalMode};

#[derive(Default)]
struct LogState {
    entries: Vec<(u64, Vec<u8>)>,
    next: u64,
    durable: u64,
    paused: bool,
    crashed: bool,
    stopped: bool,
    groups: u64,
}

struct Inner {
    state: Mutex<LogState>,
    cv: Condvar,
    store: Arc<MemStore>,
    watch: Arc<DurableWatch>,
    applied: AtomicU64,
    delay: Duration,
}

fn guard(m: &Mutex<LogState>) -> MutexGuard<'_, LogState> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A store and log in memory, with a transaction layer on top.
pub struct MemPath {
    inner: Arc<Inner>,
    shared: TxnShared,
    recovered_max_ts: Option<Ts>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MemPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemPath").finish_non_exhaustive()
    }
}

impl MemPath {
    /// A fresh path whose log takes `sync_delay` per group.
    ///
    /// # Panics
    /// The sync thread can't be started.
    #[must_use]
    pub fn start(config: UnitConfig, sync_delay: Duration) -> Arc<Self> {
        Self::start_with(config, sync_delay, Arc::new(SystemClock), Vec::new())
            .expect("an empty log replays")
    }

    /// A path restarted from `entries` (a crashed path's durable log),
    /// with `clock` as its wall clock.
    ///
    /// # Errors
    /// A malformed entry.
    ///
    /// # Panics
    /// The sync thread can't be started.
    pub fn start_with(
        config: UnitConfig,
        sync_delay: Duration,
        clock: Arc<dyn Clock>,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> Result<Arc<Self>> {
        let store = Self::replay(&entries)?;
        let mut max_ts = None;
        for (_, p) in &entries {
            let mut b = StoreBatch::new();
            payload::decode_into(p, &mut b)?;
            for op in b.ops() {
                if let BatchOp::Put { key, .. } = op
                    && !key.starts_with(&keys::META_TENANT.to_be_bytes())
                    && let Some((_, ts)) = keys::split_version(key)
                {
                    max_ts = max_ts.max(Some(ts));
                }
            }
        }
        let last = entries.last().map_or(0, |e| e.0);
        let watch = Arc::new(DurableWatch::default());
        watch.advance(last);
        let inner = Arc::new(Inner {
            state: Mutex::new(LogState {
                next: last + 1,
                durable: last,
                entries,
                ..LogState::default()
            }),
            cv: Condvar::new(),
            store: Arc::new(store),
            watch,
            applied: AtomicU64::new(last),
            delay: sync_delay,
        });
        let i2 = inner.clone();
        let h = std::thread::Builder::new()
            .name("mempath-sync".into())
            .spawn(move || sync_loop(&i2))
            .expect("start the sync thread");
        Ok(Arc::new(Self {
            inner,
            shared: TxnShared::with_config(config, clock),
            recovered_max_ts: max_ts,
            thread: Mutex::new(Some(h)),
        }))
    }

    /// The store.
    #[must_use]
    pub fn mem_store(&self) -> &Arc<MemStore> {
        &self.inner.store
    }

    /// Holds syncing: commits queue up and stay pending.
    pub fn pause(&self) {
        guard(&self.inner.state).paused = true;
    }

    /// Resumes syncing.
    pub fn resume(&self) {
        guard(&self.inner.state).paused = false;
        self.inner.cv.notify_all();
    }

    /// Simulates a crash: the unsynced tail is lost and every waiter fails.
    pub fn crash(&self) {
        {
            let mut g = guard(&self.inner.state);
            g.crashed = true;
            let d = g.durable;
            g.entries.retain(|e| e.0 <= d);
        }
        self.inner.cv.notify_all();
        self.inner.watch.fail();
    }

    /// The durable payloads, in order: what a crash leaves.
    #[must_use]
    pub fn durable_entries(&self) -> Vec<(u64, Vec<u8>)> {
        let g = guard(&self.inner.state);
        g.entries
            .iter()
            .filter(|e| e.0 <= g.durable)
            .cloned()
            .collect()
    }

    /// Sync groups so far.
    #[must_use]
    pub fn groups(&self) -> u64 {
        guard(&self.inner.state).groups
    }

    /// Rebuilds a store from log payloads (recovery after a crash).
    ///
    /// # Errors
    /// A malformed payload.
    pub fn replay(entries: &[(u64, Vec<u8>)]) -> Result<MemStore> {
        let store = MemStore::new();
        for (_, p) in entries {
            let mut b = StoreBatch::new();
            payload::decode_into(p, &mut b)?;
            store.write(&b, WalMode::Off)?;
        }
        Ok(store)
    }
}

impl Drop for MemPath {
    fn drop(&mut self) {
        guard(&self.inner.state).stopped = true;
        self.inner.cv.notify_all();
        let h = self
            .thread
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(h) = h {
            let _ = h.join();
        }
    }
}

fn sync_loop(inner: &Inner) {
    loop {
        let (upto, group) = {
            let mut g = guard(&inner.state);
            loop {
                if g.stopped || g.crashed {
                    return;
                }
                let last = g.entries.last().map_or(0, |e| e.0);
                if last > g.durable && !g.paused {
                    break;
                }
                g = inner
                    .cv
                    .wait(g)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            let from = g.durable;
            let group: Vec<Vec<u8>> = g
                .entries
                .iter()
                .filter(|e| e.0 > from)
                .map(|e| e.1.clone())
                .collect();
            (g.entries.last().map_or(0, |e| e.0), group)
        };
        if !inner.delay.is_zero() {
            std::thread::sleep(inner.delay);
        }
        {
            let mut g = guard(&inner.state);
            if g.crashed {
                return;
            }
            g.durable = upto;
            g.groups += 1;
        }
        let mut b = StoreBatch::new();
        for p in &group {
            if payload::decode_into(p, &mut b).is_err() {
                inner.watch.fail();
                return;
            }
        }
        if inner.store.write(&b, WalMode::Off).is_err() {
            inner.watch.fail();
            return;
        }
        inner.applied.store(upto, Ordering::Release);
        inner.watch.advance(upto);
    }
}

impl CommitPath for MemPath {
    fn store(&self) -> &dyn SortedStore {
        self.inner.store.as_ref()
    }

    fn shared(&self) -> &TxnShared {
        &self.shared
    }

    fn propose(&self, payload: Vec<u8>, _durability: Option<Durability>) -> Result<DurableTicket> {
        let index = {
            let mut g = guard(&self.inner.state);
            if g.crashed || g.stopped {
                return Err(SprigError::internal("commit log is closed"));
            }
            let index = g.next;
            g.next += 1;
            g.entries.push((index, payload));
            index
        };
        self.inner.cv.notify_all();
        Ok(DurableTicket::Watch(self.inner.watch.clone(), index))
    }

    fn applied_index(&self) -> u64 {
        self.inner.applied.load(Ordering::Acquire)
    }

    fn recovered_max_ts(&self) -> Option<Ts> {
        self.recovered_max_ts
    }
}
