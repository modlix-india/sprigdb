//! Model tests: the same operation sequences against `MemStore`, `RocksStore`
//! and a `BTreeMap` oracle. This keeps the fake faithful to RocksDB.

use std::collections::BTreeMap;
use std::ops::Bound;

use proptest::prelude::*;

use super::*;
use crate::keys::{self, Ts};
use crate::testutil::TempDir;

#[derive(Debug, Clone)]
enum Op {
    Put(u8, u8, Vec<u8>),
    Delete(u8, u8),
    DeleteRange(u8, u8, u8),
    Batch(Vec<(u8, u8, Option<Vec<u8>>)>),
    Flush,
    Compact,
}

/// Keys from a small alphabet, so operations collide. Key `n` is the
/// logical key `[n / 4]` at version `n % 4`.
fn key(n: u8) -> Vec<u8> {
    keys::versioned(&[b'k', n / 4], Ts(u64::from(n % 4) + 1))
}

fn cf_of(c: u8) -> Cf {
    if c.is_multiple_of(2) {
        Cf::Data
    } else {
        Cf::Local
    }
}

fn op() -> impl Strategy<Value = Op> {
    let k = 0u8..24;
    let v = proptest::collection::vec(any::<u8>(), 0..6);
    prop_oneof![
        5 => (0u8..2, k.clone(), v.clone()).prop_map(|(c, k, v)| Op::Put(c, k, v)),
        2 => (0u8..2, k.clone()).prop_map(|(c, k)| Op::Delete(c, k)),
        1 => (0u8..2, k.clone(), k.clone()).prop_map(|(c, a, b)| Op::DeleteRange(c, a.min(b), a.max(b))),
        2 => proptest::collection::vec((0u8..2, k, proptest::option::of(v)), 1..5).prop_map(Op::Batch),
        1 => Just(Op::Flush),
        1 => Just(Op::Compact),
    ]
}

type Model = [BTreeMap<Vec<u8>, Vec<u8>>; 2];

fn apply_model(m: &mut Model, b: &StoreBatch) {
    for o in b.ops() {
        match o {
            BatchOp::Put { cf, key, value } => {
                m[cf.code() as usize].insert(key.clone(), value.clone());
            }
            BatchOp::Delete { cf, key } => {
                m[cf.code() as usize].remove(key);
            }
            BatchOp::DeleteRange { cf, start, end } => {
                m[cf.code() as usize].retain(|k, _| !(k >= start && k < end));
            }
        }
    }
}

fn batch_of(op: &Op) -> Option<StoreBatch> {
    let mut b = StoreBatch::new();
    match op {
        Op::Put(c, k, v) => b.put(cf_of(*c), key(*k), v.clone()),
        Op::Delete(c, k) => b.delete(cf_of(*c), key(*k)),
        Op::DeleteRange(c, a, z) => b.delete_range(cf_of(*c), key(*a), key(*z)),
        Op::Batch(items) => {
            for (c, k, v) in items {
                match v {
                    Some(v) => b.put(cf_of(*c), key(*k), v.clone()),
                    None => b.delete(cf_of(*c), key(*k)),
                }
            }
        }
        Op::Flush | Op::Compact => return None,
    }
    Some(b)
}

fn scan(s: &dyn SortedStore, cf: Cf, opts: IterOpts, reverse: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = s.iter(cf, opts);
    let mut out = Vec::new();
    if reverse {
        it.seek_to_last();
    } else {
        it.seek_to_first();
    }
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        if reverse {
            it.prev();
        } else {
            it.next();
        }
    }
    assert_eq!(it.status().unwrap(), Read::Ready(()));
    out
}

fn check(s: &dyn SortedStore, m: &Model) {
    for cf in Cf::ALL {
        let mm = &m[cf.code() as usize];
        // Point reads, single and batched.
        let ks: Vec<Vec<u8>> = (0..24).map(key).collect();
        for k in &ks {
            let got = s.get(cf, k, ReadTier::All).unwrap().expect_ready().unwrap();
            assert_eq!(got.as_deref(), mm.get(k).map(Vec::as_slice), "get {k:?}");
        }
        let refs: Vec<&[u8]> = ks.iter().map(Vec::as_slice).collect();
        let multi = s
            .multi_get(cf, &refs, ReadTier::All)
            .unwrap()
            .expect_ready()
            .unwrap();
        for (k, v) in ks.iter().zip(multi) {
            assert_eq!(v.as_deref(), mm.get(k).map(Vec::as_slice));
        }
        // Full scans both ways.
        let want: Vec<_> = mm.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(scan(s, cf, IterOpts::default(), false), want);
        let mut rev = want.clone();
        rev.reverse();
        assert_eq!(scan(s, cf, IterOpts::default(), true), rev);
        // Bounded scan.
        let (lo, hi) = (key(5), key(17));
        let want_b: Vec<_> = mm
            .range::<[u8], _>((
                Bound::Included(lo.as_slice()),
                Bound::Excluded(hi.as_slice()),
            ))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let opts = IterOpts::range(&KeyRange::new(lo.clone(), hi.clone()));
        assert_eq!(scan(s, cf, opts.clone(), false), want_b);
        let mut rb = want_b;
        rb.reverse();
        assert_eq!(scan(s, cf, opts, true), rb);
        // Versioned reads at every timestamp.
        for l in 0..6u8 {
            let logical = [b'k', l];
            for at in 0..6u64 {
                let mut buf = Vec::new();
                let got = s
                    .get_version(cf, &logical, Ts(at), ReadTier::All, &mut buf)
                    .unwrap()
                    .expect_ready()
                    .unwrap();
                let want = (1..=at.min(4)).rev().find_map(|v| {
                    mm.get(&keys::versioned(&logical, Ts(v)))
                        .map(|val| (Ts(v), val.clone()))
                });
                assert_eq!(
                    got.map(|t| (t, buf.clone())),
                    want,
                    "version of {l} at {at}"
                );
            }
        }
    }
}

fn run_ops(s: &dyn SortedStore, ops: &[Op]) {
    let mut m: Model = Default::default();
    for o in ops {
        match batch_of(o) {
            Some(b) => {
                let wal = if b.touches(Cf::Local) {
                    WalMode::On
                } else {
                    WalMode::Off
                };
                s.write(&b, wal).unwrap();
                apply_model(&mut m, &b);
            }
            None if matches!(o, Op::Compact) => s.compact_all().unwrap(),
            None => s.flush_all().unwrap(),
        }
    }
    check(s, &m);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn mem_store_matches_model(ops in proptest::collection::vec(op(), 0..40)) {
        run_ops(&MemStore::new(), &ops);
    }

    #[test]
    fn rocks_store_matches_model(ops in proptest::collection::vec(op(), 0..40)) {
        let dir = TempDir::new("model");
        let s = RocksStore::open(dir.path(), &RocksOptions { block_cache_bytes: 4 << 20, memtable_bytes: 4 << 20, ..RocksOptions::default() }).unwrap();
        run_ops(&s, &ops);
    }
}

/// An iterator keeps the view it was created with, on both stores.
fn iterator_is_a_snapshot(s: &dyn SortedStore) {
    let mut b = StoreBatch::new();
    b.put(Cf::Data, b"a1".to_vec(), b"1".to_vec());
    b.put(Cf::Data, b"a2".to_vec(), b"2".to_vec());
    s.write(&b, WalMode::Off).unwrap();
    let mut it = s.iter(Cf::Data, IterOpts::default());
    let mut b = StoreBatch::new();
    b.delete(Cf::Data, b"a1".to_vec());
    b.put(Cf::Data, b"a2".to_vec(), b"changed".to_vec());
    b.put(Cf::Data, b"a3".to_vec(), b"3".to_vec());
    s.write(&b, WalMode::Off).unwrap();
    it.seek_to_first();
    let mut seen = Vec::new();
    while it.valid() {
        seen.push((it.key().to_vec(), it.value().to_vec()));
        it.next();
    }
    assert_eq!(
        seen,
        vec![
            (b"a1".to_vec(), b"1".to_vec()),
            (b"a2".to_vec(), b"2".to_vec())
        ]
    );
    drop(it);
    assert_eq!(scan(s, Cf::Data, IterOpts::default(), false).len(), 2);
}

#[test]
fn snapshots_on_both_stores() {
    iterator_is_a_snapshot(&MemStore::new());
    let dir = TempDir::new("snap");
    iterator_is_a_snapshot(&RocksStore::open(dir.path(), &RocksOptions::default()).unwrap());
}

/// The prefix mode stops at the end of the logical key, on both stores.
fn prefix_mode(s: &dyn SortedStore) {
    let mut b = StoreBatch::new();
    for (l, v) in [(b"x1", 3u64), (b"x1", 1), (b"x2", 9)] {
        b.put(
            Cf::Data,
            keys::versioned(l, Ts(v)),
            v.to_be_bytes().to_vec(),
        );
    }
    s.write(&b, WalMode::Off).unwrap();
    let mut it = s.iter(
        Cf::Data,
        IterOpts {
            prefix_same_as_start: true,
            ..IterOpts::default()
        },
    );
    it.seek(&keys::versioned(b"x1", Ts::MAX));
    let mut n = 0;
    while it.valid() {
        assert!(it.key().starts_with(b"x1"));
        n += 1;
        it.next();
    }
    assert_eq!(n, 2);
    let mut buf = Vec::new();
    assert_eq!(
        s.get_version(Cf::Data, b"x1", Ts(2), ReadTier::All, &mut buf)
            .unwrap(),
        Read::Ready(Some(Ts(1)))
    );
    assert_eq!(
        s.get_version(Cf::Data, b"x1", Ts(0), ReadTier::All, &mut buf)
            .unwrap(),
        Read::Ready(None)
    );
    assert_eq!(
        s.get_version(Cf::Data, b"x0", Ts::MAX, ReadTier::All, &mut buf)
            .unwrap(),
        Read::Ready(None)
    );
}

#[test]
fn prefix_mode_on_both_stores() {
    prefix_mode(&MemStore::new());
    let dir = TempDir::new("prefix");
    let s = RocksStore::open(dir.path(), &RocksOptions::default()).unwrap();
    prefix_mode(&s);
    // And from SST files, where the prefix Bloom filter is consulted.
    s.flush_all().unwrap();
    prefix_mode(&s);
}

/// A cache-only read of data that is only on disk says `WouldBlock`.
#[test]
fn rocks_cache_only_reads_would_block() {
    let dir = TempDir::new("tier");
    let opts = RocksOptions::default();
    {
        let s = RocksStore::open(dir.path(), &opts).unwrap();
        let mut b = StoreBatch::new();
        for i in 0..2000u32 {
            b.put(
                Cf::Data,
                keys::versioned(&i.to_be_bytes(), Ts(1)),
                vec![7u8; 100],
            );
        }
        s.write(&b, WalMode::Off).unwrap();
        s.flush_all().unwrap();
    }
    let s = RocksStore::open(dir.path(), &opts).unwrap();
    let k = keys::versioned(&5u32.to_be_bytes(), Ts(1));
    assert_eq!(
        s.get(Cf::Data, &k, ReadTier::CacheOnly).unwrap(),
        Read::WouldBlock
    );
    assert!(
        s.get(Cf::Data, &k, ReadTier::All)
            .unwrap()
            .expect_ready()
            .unwrap()
            .is_some()
    );
    // Now cached.
    assert!(matches!(
        s.get(Cf::Data, &k, ReadTier::CacheOnly).unwrap(),
        Read::Ready(Some(_))
    ));
    let mut buf = Vec::new();
    let far = 1999u32.to_be_bytes();
    assert_eq!(
        s.get_version(Cf::Data, &far, Ts::MAX, ReadTier::CacheOnly, &mut buf)
            .unwrap(),
        Read::WouldBlock
    );
}

/// Versioned point reads keep their cursors between calls. A kept cursor
/// must see every later write, on every thread, across flushes and
/// compactions, and the store must close (and open again) with cursors
/// still kept.
#[test]
fn rocks_kept_point_cursors_see_later_writes() {
    let dir = TempDir::new("kept");
    let opts = RocksOptions::default();
    let read = |s: &RocksStore, logical: &[u8], at: Ts| {
        let mut buf = Vec::new();
        s.get_version(Cf::Data, logical, at, ReadTier::All, &mut buf)
            .unwrap()
            .expect_ready()
            .unwrap()
            .map(|t| (t, buf))
    };
    {
        let s = RocksStore::open(dir.path(), &opts).unwrap();
        assert_eq!(read(&s, b"a", Ts::MAX), None);
        for round in 1..=40u64 {
            let mut b = StoreBatch::new();
            b.put(
                Cf::Data,
                keys::versioned(b"a", Ts(round)),
                round.to_be_bytes().to_vec(),
            );
            s.write(&b, WalMode::Off).unwrap();
            // The cursor kept by the read before this write sees it.
            assert_eq!(
                read(&s, b"a", Ts::MAX),
                Some((Ts(round), round.to_be_bytes().to_vec()))
            );
            // An older timestamp still reads the older version.
            if round > 1 {
                assert_eq!(
                    read(&s, b"a", Ts(round - 1)).map(|(t, _)| t),
                    Some(Ts(round - 1))
                );
            }
            // A key that isn't there, between two that are.
            assert_eq!(read(&s, b"b", Ts::MAX), None);
            match round % 10 {
                3 => s.flush_all().unwrap(),
                7 => s.compact_all().unwrap(),
                9 => s.release_idle_reads(),
                _ => {}
            }
        }
        // Other threads have their own cursors; all read the same state.
        std::thread::scope(|sc| {
            for _ in 0..8 {
                sc.spawn(|| {
                    for _ in 0..50 {
                        assert_eq!(read(&s, b"a", Ts::MAX).map(|(t, _)| t), Some(Ts(40)));
                        assert_eq!(read(&s, b"a", Ts(5)).map(|(t, _)| t), Some(Ts(5)));
                    }
                });
            }
        });
        // Dropped here with cursors kept on nine threads' pools.
    }
    let s = RocksStore::open(dir.path(), &opts).unwrap();
    assert_eq!(read(&s, b"a", Ts::MAX).map(|(t, _)| t), Some(Ts(40)));
}

/// With the WAL off and no flush, a close that skips the flush loses the
/// memtable, exactly like a crash. A `Local` (WAL on) write survives.
#[test]
fn rocks_wal_off_loses_unflushed_on_crash_close() {
    let dir = TempDir::new("walloss");
    let mut opts = RocksOptions::default();
    opts.tuning.avoid_flush_on_close = true;
    {
        let s = RocksStore::open(dir.path(), &opts).unwrap();
        let mut b = StoreBatch::new();
        b.put(Cf::Data, b"data-key".to_vec(), b"v".to_vec());
        s.write(&b, WalMode::Off).unwrap();
        let mut b = StoreBatch::new();
        b.put(Cf::Local, b"local-key".to_vec(), b"v".to_vec());
        s.write(&b, WalMode::Sync).unwrap();
    }
    let s = RocksStore::open(dir.path(), &opts).unwrap();
    assert_eq!(
        s.get(Cf::Data, b"data-key", ReadTier::All).unwrap(),
        Read::Ready(None)
    );
    assert!(
        s.get(Cf::Local, b"local-key", ReadTier::All)
            .unwrap()
            .expect_ready()
            .unwrap()
            .is_some()
    );
}

/// A GC rule for the test: drops every entry of `data` whose value starts
/// with `x`, and records whether each compaction was bottommost.
struct DropX(std::sync::Mutex<Vec<bool>>);

impl CompactionGc for DropX {
    fn start(&self, bottommost: bool) -> GcDecider {
        self.0.lock().unwrap().push(bottommost);
        Box::new(|_, v| v.first() == Some(&b'x'))
    }
}

/// Compactions run the GC rule on `data` only, on both stores.
fn compaction_runs_the_gc_rule(s: &dyn SortedStore, rule: &DropX) {
    let mut b = StoreBatch::new();
    for i in 0..50u8 {
        let v: &[u8] = if i % 2 == 0 { b"xdead" } else { b"live" };
        b.put(Cf::Data, vec![b'd', i], v.to_vec());
        b.put(Cf::Local, vec![b'l', i], v.to_vec());
    }
    s.write(&b, WalMode::On).unwrap();
    s.flush_all().unwrap();
    s.compact_all().unwrap();
    let data = scan(s, Cf::Data, IterOpts::default(), false);
    assert_eq!(data.len(), 25);
    assert!(data.iter().all(|(_, v)| v == b"live"));
    assert_eq!(scan(s, Cf::Local, IterOpts::default(), false).len(), 50);
    let seen = rule.0.lock().unwrap();
    assert!(
        seen.iter().any(|b| *b),
        "no bottommost compaction ran: {seen:?}"
    );
}

#[test]
fn compaction_gc_on_both_stores() {
    let rule = Arc::new(DropX(std::sync::Mutex::new(Vec::new())));
    compaction_runs_the_gc_rule(&MemStore::with_gc(Some(rule.clone() as GcRule)), &rule);
    let rule = Arc::new(DropX(std::sync::Mutex::new(Vec::new())));
    let dir = TempDir::new("gc");
    let s = RocksStore::open(
        dir.path(),
        &RocksOptions {
            gc: Some(rule.clone() as GcRule),
            ..RocksOptions::default()
        },
    )
    .unwrap();
    compaction_runs_the_gc_rule(&s, &rule);
}
