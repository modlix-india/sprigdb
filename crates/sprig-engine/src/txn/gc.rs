//! Garbage collection of old versions below the oldest read view.
//!
//! Every version above the unit's watermark is kept, plus the newest one at
//! or below it (what a reader at the watermark sees). Older versions go. A
//! tombstone at the floor goes too, but only where nothing older can sit
//! below it (the bottommost level of an LSM, or a full scan of the store).
//!
//! [`GcFilter`] is the per-entry decision a `RocksDB` compaction filter calls,
//! fed in key order. [`gc_scan`] runs the same rule over a store directly
//! (`MemStore`, tests, and manual GC).

use sprig_common::Result;

use super::hlc::Ts;
use super::mvcc::is_tombstone;
use crate::keys::{KeyRange, split_version};
use crate::store::{Cf, IterOpts, SortedStore, StoreBatch, WalMode};

/// Keep or drop one stored entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcDecision {
    /// Keep it.
    Keep,
    /// Drop it.
    Drop,
}

/// The per-entry GC rule, fed stored keys in ascending order.
#[derive(Debug)]
pub struct GcFilter {
    watermark: Ts,
    bottommost: bool,
    current: Vec<u8>,
    floor_seen: bool,
}

impl GcFilter {
    /// A filter for one compaction (or scan) at `watermark`.
    #[must_use]
    pub fn new(watermark: Ts, bottommost: bool) -> Self {
        Self {
            watermark,
            bottommost,
            current: Vec::new(),
            floor_seen: false,
        }
    }

    /// The decision for one entry. Unversioned keys (bookkeeping under the
    /// reserved tenant, queue messages) are kept.
    pub fn decide(&mut self, stored_key: &[u8], value: &[u8]) -> GcDecision {
        if crate::keys::is_unversioned(stored_key) {
            return GcDecision::Keep;
        }
        let Some((logical, ts)) = split_version(stored_key) else {
            return GcDecision::Keep;
        };
        if logical != self.current.as_slice() {
            self.current.clear();
            self.current.extend_from_slice(logical);
            self.floor_seen = false;
        }
        if ts > self.watermark {
            return GcDecision::Keep;
        }
        if self.floor_seen {
            return GcDecision::Drop;
        }
        self.floor_seen = true;
        if self.bottommost && is_tombstone(value) {
            GcDecision::Drop
        } else {
            GcDecision::Keep
        }
    }
}

/// Applies the rule to every version in `range` of `store`, deleting what
/// it drops (WAL off: a delete lost in a crash only brings back a version
/// nobody reads). Returns the number of entries removed.
///
/// # Errors
/// Store failures.
pub fn gc_scan(store: &dyn SortedStore, range: &KeyRange, watermark: Ts) -> Result<usize> {
    let mut filter = GcFilter::new(watermark, true);
    let mut batch = StoreBatch::new();
    let mut removed = 0;
    {
        let mut it = store.iter(Cf::Data, IterOpts::range(range));
        it.seek_to_first();
        while it.valid() {
            if filter.decide(it.key(), it.value()) == GcDecision::Drop {
                batch.delete(Cf::Data, it.key().to_vec());
                removed += 1;
            }
            it.next();
        }
        it.status()?.expect_ready()?;
    }
    if !batch.is_empty() {
        store.write(&batch, WalMode::Off)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::versioned;
    use crate::store::MemStore;
    use crate::txn::mvcc::{encode_value_into, read_at};

    fn val(v: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        encode_value_into(&mut out, v);
        out
    }

    #[test]
    fn unversioned_keys_are_never_collected() {
        let mut queue = crate::keys::KeyPrefix {
            tenant: 7,
            shard: 0,
            object: crate::keys::QUEUES_OBJECT,
            kind: b'E',
            index: 0,
        }
        .to_vec();
        queue.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]);
        let meta = crate::keys::unit_meta_key(7, b"applied");
        let mut f = GcFilter::new(Ts::MAX, true);
        for k in [&queue, &meta] {
            assert_eq!(f.decide(k, b"x"), GcDecision::Keep);
            assert_eq!(f.decide(k, b"x"), GcDecision::Keep);
        }
    }

    #[test]
    fn keeps_the_floor_version_and_everything_above() {
        let s = MemStore::new();
        let mut b = StoreBatch::new();
        for ts in [10u64, 20, 30, 40] {
            b.put(
                Cf::Data,
                versioned(b"a\0", Ts(ts)),
                val(Some(&[u8::try_from(ts).unwrap()])),
            );
        }
        b.put(Cf::Data, versioned(b"d\0", Ts(5)), val(Some(b"x")));
        b.put(Cf::Data, versioned(b"d\0", Ts(15)), val(None));
        b.put(Cf::Data, versioned(b"e\0", Ts(50)), val(None));
        s.write(&b, WalMode::Off).unwrap();
        let removed = gc_scan(&s, &KeyRange::all(), Ts(25)).unwrap();
        // a@10 goes; d@5 and the d@15 marker go; e@50 is above the mark.
        assert_eq!(removed, 3);
        for at in [25, 30, 45] {
            assert_eq!(
                read_at(&s, b"a\0", Ts(at)).unwrap().unwrap().ts.0,
                if at < 30 {
                    20
                } else if at < 40 {
                    30
                } else {
                    40
                }
            );
        }
        assert_eq!(read_at(&s, b"d\0", Ts(25)).unwrap(), None);
        assert!(read_at(&s, b"e\0", Ts(60)).unwrap().is_some());
    }

    #[test]
    fn non_bottommost_keeps_a_floor_tombstone() {
        let mut f = GcFilter::new(Ts(100), false);
        assert_eq!(
            f.decide(&versioned(b"k\0", Ts(50)), &val(None)),
            GcDecision::Keep
        );
        assert_eq!(
            f.decide(&versioned(b"k\0", Ts(40)), &val(Some(b"v"))),
            GcDecision::Drop
        );
    }
}
