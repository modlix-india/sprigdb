//! The in-memory sorted store.
//!
//! Same semantics as [`super::RocksStore`]: atomic batches, range deletes,
//! iterators that see the store as of their creation, prefix-bounded
//! iteration on the logical key. Each key keeps a short history of
//! `(sequence, value)` so open iterators keep their view; history no open
//! iterator can see is dropped on the next write to the key.
//!
//! Nothing here is durable. Durability comes from the commit log, which
//! [`crate::Engine::open`] replays into a fresh `MemStore`.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Mutex, PoisonError, RwLock};

use bytes::Bytes;
use sprig_common::Result;

use super::{
    BatchOp, Cf, GcRule, IterOpts, Read, ReadTier, SortedStore, StoreBatch, StoreIter, StoreStats,
    WalMode,
};
use crate::keys::{KeyRange, VERSION_LEN, strip_version};

type History = Vec<(u64, Option<Bytes>)>;

#[derive(Default)]
struct Inner {
    cfs: [BTreeMap<Vec<u8>, History>; 2],
    seq: u64,
    bytes: u64,
}

/// The in-memory store.
#[derive(Default)]
pub struct MemStore {
    inner: RwLock<Inner>,
    /// Sequence numbers of open iterators, with counts.
    open: Mutex<BTreeMap<u64, usize>>,
    /// The version GC rule [`SortedStore::compact_all`] runs on `data`.
    gc: Option<GcRule>,
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore").finish_non_exhaustive()
    }
}

fn idx(cf: Cf) -> usize {
    match cf {
        Cf::Data => 0,
        Cf::Local => 1,
    }
}

fn visible(h: &History, seq: u64) -> Option<&Bytes> {
    h.iter()
        .rev()
        .find(|(s, _)| *s <= seq)
        .and_then(|(_, v)| v.as_ref())
}

impl MemStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store whose compactions run `gc` on `data`, as
    /// [`super::RocksStore`]'s do.
    #[must_use]
    pub fn with_gc(gc: Option<GcRule>) -> Self {
        Self {
            gc,
            ..Self::default()
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn oldest_open(&self) -> Option<u64> {
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .next()
            .copied()
    }

    fn register(&self) -> u64 {
        // Hold the read lock while registering, so no write can prune the
        // history this iterator needs between reading `seq` and registering.
        let g = self.read();
        let seq = g.seq;
        *self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(seq)
            .or_default() += 1;
        drop(g);
        seq
    }

    fn unregister(&self, seq: u64) {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = open.get_mut(&seq) {
            *n -= 1;
            if *n == 0 {
                open.remove(&seq);
            }
        }
    }
}

/// Appends a version to a key's history and drops what no reader can see.
fn record(
    map: &mut BTreeMap<Vec<u8>, History>,
    key: &[u8],
    seq: u64,
    v: Option<Bytes>,
    floor: Option<u64>,
) {
    let h = map.entry(key.to_vec()).or_default();
    h.push((seq, v));
    // Keep every entry an open iterator (seq >= floor) might read: the
    // newest entry at or below the floor, and everything after it.
    let floor = floor.unwrap_or(seq);
    if let Some(keep_from) = h.iter().rposition(|(s, _)| *s <= floor) {
        h.drain(..keep_from);
    }
    if h.len() == 1 && h[0].1.is_none() {
        map.remove(key);
    }
}

impl SortedStore for MemStore {
    fn get(&self, cf: Cf, key: &[u8], _tier: ReadTier) -> Result<Read<Option<Bytes>>> {
        let g = self.read();
        let seq = g.seq;
        Ok(Read::Ready(
            g.cfs[idx(cf)]
                .get(key)
                .and_then(|h| visible(h, seq))
                .cloned(),
        ))
    }

    fn iter(&self, cf: Cf, opts: IterOpts) -> Box<dyn StoreIter + '_> {
        let seq = self.register();
        Box::new(MemIter {
            store: self,
            cf,
            seq,
            opts,
            prefix: None,
            cur: None,
        })
    }

    fn write(&self, batch: &StoreBatch, _wal: WalMode) -> Result<()> {
        let mut g = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        let floor = self.oldest_open();
        g.seq += 1;
        let seq = g.seq;
        let mut added = 0u64;
        for op in batch.ops() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    added += (key.len() + value.len()) as u64;
                    record(
                        &mut g.cfs[idx(*cf)],
                        key,
                        seq,
                        Some(Bytes::copy_from_slice(value)),
                        floor,
                    );
                }
                BatchOp::Delete { cf, key } => {
                    if g.cfs[idx(*cf)].contains_key(key.as_slice()) {
                        record(&mut g.cfs[idx(*cf)], key, seq, None, floor);
                    }
                }
                BatchOp::DeleteRange { cf, start, end } => {
                    if start >= end {
                        continue;
                    }
                    let map = &mut g.cfs[idx(*cf)];
                    let doomed: Vec<Vec<u8>> = map
                        .range::<[u8], _>((
                            Bound::Included(start.as_slice()),
                            Bound::Excluded(end.as_slice()),
                        ))
                        .filter(|(_, h)| visible(h, seq).is_some())
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in doomed {
                        record(map, &k, seq, None, floor);
                    }
                }
            }
        }
        g.bytes += added;
        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        Ok(())
    }

    /// One pass of the GC rule over every current entry of `data`, as one
    /// bottommost compaction (the in-memory store has a single level).
    fn compact_all(&self) -> Result<()> {
        let Some(gc) = &self.gc else {
            return Ok(());
        };
        let mut decide = gc.start(true);
        let mut doomed = StoreBatch::new();
        {
            let g = self.read();
            let seq = g.seq;
            for (k, h) in &g.cfs[idx(Cf::Data)] {
                if let Some(v) = visible(h, seq)
                    && decide(k, v)
                {
                    doomed.delete(Cf::Data, k.clone());
                }
            }
        }
        if doomed.is_empty() {
            return Ok(());
        }
        self.write(&doomed, WalMode::Off)
    }

    fn approx_size(&self, cf: Cf, range: &KeyRange) -> u64 {
        let g = self.read();
        let seq = g.seq;
        let upper = range
            .end
            .as_deref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        g.cfs[idx(cf)]
            .range::<[u8], _>((Bound::Included(range.start.as_slice()), upper))
            .filter_map(|(k, h)| visible(h, seq).map(|v| (k.len() + v.len()) as u64))
            .sum()
    }

    fn stats(&self) -> StoreStats {
        let g = self.read();
        let keys = g.cfs.iter().map(|m| m.len() as u64).sum();
        StoreStats {
            memtable_bytes: g.bytes,
            keys,
            ..StoreStats::default()
        }
    }
}

struct MemIter<'a> {
    store: &'a MemStore,
    cf: Cf,
    seq: u64,
    opts: IterOpts,
    /// The logical prefix fixed by the last seek, in prefix mode.
    prefix: Option<Vec<u8>>,
    cur: Option<(Vec<u8>, Bytes)>,
}

impl Drop for MemIter<'_> {
    fn drop(&mut self) {
        self.store.unregister(self.seq);
    }
}

impl MemIter<'_> {
    fn in_bounds(&self, k: &[u8]) -> bool {
        self.opts.lower.as_deref().is_none_or(|l| k >= l)
            && self.opts.upper.as_deref().is_none_or(|u| k < u)
            && self
                .prefix
                .as_deref()
                .is_none_or(|p| k.len() >= VERSION_LEN && strip_version(k) == p)
    }

    fn find(&mut self, from: Bound<&[u8]>, forward: bool) {
        let g = self.store.read();
        let map = &g.cfs[idx(self.cf)];
        let found = if forward {
            map.range::<[u8], _>((from, Bound::Unbounded))
                .find_map(|(k, h)| visible(h, self.seq).map(|v| (k, v)))
                .map(|(k, v)| (k.clone(), v.clone()))
        } else {
            map.range::<[u8], _>((Bound::Unbounded, from))
                .rev()
                .find_map(|(k, h)| visible(h, self.seq).map(|v| (k, v)))
                .map(|(k, v)| (k.clone(), v.clone()))
        };
        drop(g);
        self.cur = found.filter(|(k, _)| self.in_bounds(k));
    }

    fn set_prefix(&mut self, key: &[u8]) {
        self.prefix = (self.opts.prefix_same_as_start && key.len() >= VERSION_LEN)
            .then(|| strip_version(key).to_vec());
    }
}

impl StoreIter for MemIter<'_> {
    fn seek(&mut self, key: &[u8]) {
        self.set_prefix(key);
        let lower = self.opts.lower.clone();
        let start = match lower {
            Some(l) if l.as_slice() > key => l,
            _ => key.to_vec(),
        };
        self.find(Bound::Included(&start), true);
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        self.set_prefix(key);
        let upper = self.opts.upper.clone();
        match upper {
            Some(u) if u.as_slice() <= key => self.find(Bound::Excluded(&u), false),
            _ => self.find(Bound::Included(key), false),
        }
    }

    fn seek_to_first(&mut self) {
        self.prefix = None;
        let lower = self.opts.lower.clone().unwrap_or_default();
        self.find(Bound::Included(&lower), true);
    }

    fn seek_to_last(&mut self) {
        self.prefix = None;
        match self.opts.upper.clone() {
            Some(u) => self.find(Bound::Excluded(&u), false),
            None => self.find(Bound::Unbounded, false),
        }
    }

    fn valid(&self) -> bool {
        self.cur.is_some()
    }

    fn key(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |(k, _)| k.as_slice())
    }

    fn value(&self) -> &[u8] {
        self.cur.as_ref().map_or(&[], |(_, v)| v.as_ref())
    }

    fn next(&mut self) {
        if let Some((k, _)) = self.cur.take() {
            self.find(Bound::Excluded(&k), true);
        }
    }

    fn prev(&mut self) {
        if let Some((k, _)) = self.cur.take() {
            self.find(Bound::Excluded(&k), false);
        }
    }

    fn status(&self) -> Result<Read<()>> {
        Ok(Read::Ready(()))
    }
}
