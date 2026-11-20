//! The pending overlay: versions whose commit entry is queued in the log but
//! not yet applied to the store.
//!
//! Early lock release makes these versions reachable before they are
//! durable. Whoever reads one records its log index, and must not reply
//! before that index is durable (the reply gate).

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;

use super::durable::DurableTicket;
use super::hlc::Ts;
use crate::keys::KeyRange;

/// A version proposed but not yet applied.
#[derive(Debug, Clone)]
pub struct PendingVersion {
    /// Its commit timestamp.
    pub ts: Ts,
    /// The value; `None` for a delete.
    pub value: Option<Bytes>,
    /// Its commit entry: a reply that depends on this version waits on it.
    pub ticket: DurableTicket,
}

impl PendingVersion {
    /// The log index of its commit entry.
    #[must_use]
    pub fn index(&self) -> u64 {
        self.ticket.index()
    }
}

/// The per-unit overlay. Versions of a key are kept oldest first.
#[derive(Debug, Default)]
pub struct Overlay {
    map: RwLock<BTreeMap<Bytes, Vec<PendingVersion>>>,
    versions: AtomicUsize,
}

impl Overlay {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<Bytes, Vec<PendingVersion>>> {
        self.map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<Bytes, Vec<PendingVersion>>> {
        self.map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether no version is pending (the common case for reads).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.versions.load(Ordering::Acquire) == 0
    }

    /// Number of pending versions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.versions.load(Ordering::Acquire)
    }

    /// Adds one commit's versions.
    pub fn insert<'a>(
        &self,
        ts: Ts,
        ticket: &DurableTicket,
        writes: impl IntoIterator<Item = (&'a Bytes, &'a Option<Bytes>)>,
    ) {
        let mut m = self.write();
        let mut n = 0;
        for (k, v) in writes {
            m.entry(k.clone()).or_default().push(PendingVersion {
                ts,
                value: v.clone(),
                ticket: ticket.clone(),
            });
            n += 1;
        }
        self.versions.fetch_add(n, Ordering::AcqRel);
    }

    /// The newest pending version of `key` with `ts <= at`.
    #[must_use]
    pub fn read_at(&self, key: &[u8], at: Ts) -> Option<PendingVersion> {
        if self.is_empty() {
            return None;
        }
        self.read()
            .get(key)
            .and_then(|vs| vs.iter().rev().find(|v| v.ts <= at).cloned())
    }

    /// Pending versions with `ts <= at` over a key range (newest per key).
    #[must_use]
    pub fn range_at(&self, range: &KeyRange, at: Ts) -> Vec<(Bytes, PendingVersion)> {
        if self.is_empty() {
            return Vec::new();
        }
        let lo: Bound<&[u8]> = Bound::Included(range.start.as_slice());
        let hi: Bound<&[u8]> = range
            .end
            .as_deref()
            .map_or(Bound::Unbounded, Bound::Excluded);
        self.read()
            .range::<[u8], _>((lo, hi))
            .filter_map(|(k, vs)| {
                vs.iter()
                    .rev()
                    .find(|v| v.ts <= at)
                    .map(|v| (k.clone(), v.clone()))
            })
            .collect()
    }

    /// Drops the versions of one applied commit.
    pub fn remove_applied<'a>(&self, index: u64, keys: impl IntoIterator<Item = &'a Bytes>) {
        let mut m = self.write();
        let mut n = 0;
        for k in keys {
            if let Some(vs) = m.get_mut(k) {
                let before = vs.len();
                vs.retain(|v| v.index() != index);
                n += before - vs.len();
                if vs.is_empty() {
                    m.remove(k);
                }
            }
        }
        self.versions.fetch_sub(n, Ordering::AcqRel);
    }
}
