//! Garbage collection against live transactions, on the in-memory path and
//! on the engine (`MemStore` and RocksDB): the unit's watermark never passes
//! an open read view, so collecting at it keeps every answer that view gives;
//! once the view closes, old versions and dead keys go.

#![allow(clippy::unwrap_used, clippy::missing_panics_doc)]

mod txn_common;

use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::StoreKind;
use sprig_engine::keys::KeyRange;
use sprig_engine::log::SyncMode;
use sprig_engine::store::{Cf, IterOpts, SortedStore};
use sprig_engine::txn::gc::gc_scan;
use sprig_engine::txn::{CommitPath, TxnOptions};
use txn_common::{Bed, Dir, bed, engine_bed, k, v};

/// Far enough ahead that the minimum retention never holds anything back.
fn later_ms() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    u64::try_from(now.as_millis()).unwrap() + 3_600_000
}

fn commit(b: &Bed, key: &Bytes, value: Option<&str>) {
    let mut t = b.begin(TxnOptions::mariadb());
    match value {
        Some(s) => t.put(key.clone(), v(s)).unwrap(),
        None => t.delete(key.clone()).unwrap(),
    }
    t.commit().unwrap().wait().unwrap();
}

/// Waits until the store has applied everything queued so far, so a scan
/// sees every version.
fn settle(b: &Bed) {
    let path: &dyn CommitPath = &*b.path;
    let deadline = Instant::now() + Duration::from_secs(10);
    while path.shared().pending_versions() > 0 {
        let mut t = b.begin(TxnOptions::mariadb());
        // A read prunes overlay versions the store has applied.
        let _ = t.get_snapshot(&k("settle")).unwrap();
        t.rollback();
        assert!(Instant::now() < deadline, "the store never applied the log");
        std::thread::yield_now();
    }
}

/// Stored versions of one logical key.
fn versions(store: &dyn SortedStore, key: &Bytes) -> usize {
    let mut it = store.iter(Cf::Data, IterOpts::range(&KeyRange::prefix(key)));
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        n += 1;
        it.next();
    }
    n
}

fn gc_keeps_what_open_views_read(b: &Bed, what: &str) {
    let (a, gone) = (k("a"), k("gone"));
    commit(b, &a, Some("v1"));
    commit(b, &a, Some("v2"));
    commit(b, &gone, Some("x"));
    let mut reader = b.begin(TxnOptions::mariadb());
    assert_eq!(reader.get_snapshot(&a).unwrap(), Some(v("v2")));
    let view = reader.read_view().unwrap();
    commit(b, &a, Some("v3"));
    commit(b, &a, Some("v4"));
    commit(b, &gone, None);
    settle(b);
    let store = b.path.store();
    assert_eq!(versions(store, &a), 4, "{what}");

    // The watermark stops at the open view: only v1 goes.
    let wm = b.shared().gc_watermark(later_ms());
    assert_eq!(wm, view, "{what}: the watermark is the oldest open view");
    let removed = gc_scan(store, &KeyRange::all(), wm).unwrap();
    assert_eq!(versions(store, &a), 3, "{what}");
    assert_eq!(versions(store, &gone), 2, "{what}: x is the view's version");
    assert!(removed >= 1, "{what}");
    assert_eq!(reader.get_snapshot(&a).unwrap(), Some(v("v2")), "{what}");
    assert_eq!(reader.get_snapshot(&gone).unwrap(), Some(v("x")), "{what}");
    let mut fresh = b.begin(TxnOptions::mariadb());
    assert_eq!(fresh.get_snapshot(&a).unwrap(), Some(v("v4")), "{what}");
    assert_eq!(fresh.get_snapshot(&gone).unwrap(), None, "{what}");
    fresh.rollback();

    // With the view closed, only the newest version of each key is left,
    // and the deleted key disappears with its tombstone.
    reader.rollback();
    let wm = b.shared().gc_watermark(later_ms());
    assert!(wm > view, "{what}");
    gc_scan(store, &KeyRange::all(), wm).unwrap();
    assert_eq!(versions(store, &a), 1, "{what}");
    assert_eq!(versions(store, &gone), 0, "{what}");
    let mut fresh = b.begin(TxnOptions::mariadb());
    assert_eq!(fresh.get_snapshot(&a).unwrap(), Some(v("v4")), "{what}");
    assert_eq!(fresh.get_snapshot(&gone).unwrap(), None, "{what}");
    fresh.rollback();
    // Collecting again at the same mark changes nothing.
    assert_eq!(gc_scan(store, &KeyRange::all(), wm).unwrap(), 0, "{what}");
}

#[test]
fn gc_on_the_in_memory_path() {
    gc_keeps_what_open_views_read(&bed(), "in-memory path");
}

#[test]
fn gc_on_the_engine() {
    for store in [StoreKind::Mem, StoreKind::Rocks] {
        let dir = Dir::new("gc");
        let (_e, b) = engine_bed(&dir, store, SyncMode::Off);
        gc_keeps_what_open_views_read(&b, &format!("engine on {store:?}"));
    }
}

/// The minimum retention holds versions back even with no reader.
#[test]
fn the_minimum_retention_holds_recent_versions() {
    let b = bed();
    let a = k("a");
    commit(&b, &a, Some("v1"));
    commit(&b, &a, Some("v2"));
    settle(&b);
    let now = later_ms() - 3_600_000;
    let wm = b.shared().gc_watermark(now);
    assert!(wm.physical_ms() <= now - 60_000);
    gc_scan(b.path.store(), &KeyRange::all(), wm).unwrap();
    assert_eq!(versions(b.path.store(), &a), 2);
}
