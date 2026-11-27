//! An in-memory [`Unit`] for the model tests and the in-process
//! differential test: a sorted map, per-key latches, no durability.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use tokio::sync::OwnedMutexGuard;

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::store::{Batch, StoreError, StoreResult, Ticket, Unit};

/// [`MemUnit`]'s commits are durable at once.
#[derive(Debug, Clone, Copy)]
pub struct MemTicket;

impl Ticket for MemTicket {
    async fn durable(&self) -> StoreResult<()> {
        Ok(())
    }
}

type Latch = Arc<tokio::sync::Mutex<()>>;

/// Committed notes: (tenant, note), in commit order.
type Notes = Arc<Mutex<Vec<(u64, Vec<u8>)>>>;

/// The in-memory unit.
#[derive(Debug, Default)]
pub struct MemUnit {
    data: Arc<Mutex<BTreeMap<Bytes, Bytes>>>,
    latches: Arc<Mutex<HashMap<Bytes, Latch>>>,
    /// A fixed clock (tests of expiry), else the system's.
    clock: Mutex<Option<i64>>,
    /// The notes of committed batches, in commit order.
    notes: Notes,
    /// Commits still to refuse as retryable, as an engine refuses a
    /// transaction whose unit's leadership changed (tests of retries).
    fail_commits: Arc<AtomicUsize>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl MemUnit {
    /// An empty unit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fixes the clock at `ms` (`None`: the system's again).
    pub fn set_clock(&self, ms: Option<i64>) {
        *lock(&self.clock) = ms;
    }

    /// Makes the next `n` commits fail as retryable, changing nothing.
    pub fn fail_next_commits(&self, n: usize) {
        self.fail_commits.store(n, Ordering::Release);
    }

    /// Takes the notes committed so far: (tenant, note).
    #[must_use]
    pub fn take_notes(&self) -> Vec<(u64, Vec<u8>)> {
        std::mem::take(&mut *lock(&self.notes))
    }

    /// Every entry, in key order.
    #[must_use]
    pub fn dump(&self) -> Vec<(Bytes, Bytes)> {
        lock(&self.data)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// A batch of [`MemUnit`].
#[derive(Debug)]
pub struct MemBatch {
    data: Arc<Mutex<BTreeMap<Bytes, Bytes>>>,
    latches: Arc<Mutex<HashMap<Bytes, Latch>>>,
    held: Vec<(Bytes, OwnedMutexGuard<()>)>,
    writes: BTreeMap<Bytes, Option<Bytes>>,
    notes: Vec<(u64, Vec<u8>)>,
    committed_notes: Notes,
    fail_commits: Arc<AtomicUsize>,
}

impl Unit for MemUnit {
    type Batch = MemBatch;

    async fn begin(&self, keys: Vec<Bytes>) -> StoreResult<MemBatch> {
        let mut held = Vec::with_capacity(keys.len());
        for k in keys {
            let latch = Arc::clone(lock(&self.latches).entry(k.clone()).or_default());
            held.push((k, latch.lock_owned().await));
        }
        Ok(MemBatch {
            data: Arc::clone(&self.data),
            latches: Arc::clone(&self.latches),
            held,
            writes: BTreeMap::new(),
            notes: Vec::new(),
            committed_notes: Arc::clone(&self.notes),
            fail_commits: Arc::clone(&self.fail_commits),
        })
    }

    fn now_ms(&self) -> i64 {
        lock(&self.clock).unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        })
    }
}

impl Drop for MemBatch {
    fn drop(&mut self) {
        let keys: Vec<Bytes> = self.held.drain(..).map(|(k, _)| k).collect();
        let mut l = lock(&self.latches);
        for k in keys {
            if l.get(&k).is_some_and(|a| Arc::strong_count(a) == 1) {
                l.remove(&k);
            }
        }
    }
}

impl Batch for MemBatch {
    type Ticket = MemTicket;

    async fn hold(&mut self, keys: Vec<Bytes>) -> StoreResult<()> {
        for k in keys {
            let latch = Arc::clone(lock(&self.latches).entry(k.clone()).or_default());
            // No deadlock detection here: a wait that lasts gives up, and
            // the script runs again.
            let g = tokio::time::timeout(std::time::Duration::from_secs(2), latch.lock_owned())
                .await
                .map_err(|_| crate::store::StoreError::retry("lock wait timed out"))?;
            self.held.push((k, g));
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        Ok(lock(&self.data).get(key).cloned())
    }

    fn scan(&mut self, start: &[u8], end: &[u8], limit: usize) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let range = (
            Bound::Included(Bytes::copy_from_slice(start)),
            Bound::Excluded(Bytes::copy_from_slice(end)),
        );
        let mut merged: BTreeMap<Bytes, Option<Bytes>> = lock(&self.data)
            .range(range.clone())
            .take(limit + self.writes.len())
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        for (k, v) in self.writes.range(range) {
            merged.insert(k.clone(), v.clone());
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .take(limit)
            .collect())
    }

    fn scan_rev(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        let range = (
            Bound::Included(Bytes::copy_from_slice(start)),
            Bound::Excluded(Bytes::copy_from_slice(end)),
        );
        let mut merged: BTreeMap<Bytes, Option<Bytes>> = lock(&self.data)
            .range(range.clone())
            .rev()
            .take(limit + self.writes.len())
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        for (k, v) in self.writes.range(range) {
            merged.insert(k.clone(), v.clone());
        }
        Ok(merged
            .into_iter()
            .rev()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .take(limit)
            .collect())
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> StoreResult<()> {
        self.writes.insert(key, Some(value));
        Ok(())
    }

    fn delete(&mut self, key: Bytes) -> StoreResult<()> {
        self.writes.insert(key, None);
        Ok(())
    }

    fn note(&mut self, tenant: u64, note: Vec<u8>) -> StoreResult<()> {
        self.notes.push((tenant, note));
        Ok(())
    }

    fn commit(mut self) -> StoreResult<MemTicket> {
        if self
            .fail_commits
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(StoreError::retry(
                "the data's leader changed (a test's refused commit)",
            ));
        }
        lock(&self.committed_notes).append(&mut self.notes);
        let writes = std::mem::take(&mut self.writes);
        let mut d = lock(&self.data);
        for (k, v) in writes {
            match v {
                Some(v) => d.insert(k, v),
                None => d.remove(&k),
            };
        }
        Ok(MemTicket)
    }
}
