//! `Txn::scan_many`: several ranges read through one store cursor give, for
//! each range, what a snapshot `Txn::scan` of it gives: with several
//! versions per key, delete markers, keys whose newest version came after
//! the reader's view, the reader's own writes and deletes, pending commits
//! not yet applied, empty ranges, ranges out of order and a range at the
//! outer range's end; on the in-memory path and the engine on both stores
//! (memtable and flushed files).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc
)]

mod txn_common;

use bytes::Bytes;
use sprig_engine::StoreKind;
use sprig_engine::keys::{KeyRange, prefix_end};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{Dir as ScanDir, ReadMode, TxnOptions};
use txn_common::{Bed, Dir, bed, engine_bed};

/// A logical key under an engine head (family `a/`): `a/<near>/<n>`.
fn rk(s: &str) -> Vec<u8> {
    let mut v = Vec::new();
    sprig_engine::keys::KeyPrefix {
        tenant: 7,
        shard: 1,
        object: 3,
        kind: sprig_engine::keys::kind::ADJACENCY,
        index: 0,
    }
    .encode_into(&mut v);
    v.extend_from_slice(s.as_bytes());
    v
}

fn put(b: &Bed, ops: &[(String, Option<String>)]) {
    let mut t = b.begin(TxnOptions::mariadb());
    for (k, v) in ops {
        match v {
            Some(v) => t.put(rk(k), Bytes::from(v.clone())).unwrap(),
            None => t.delete(rk(k)).unwrap(),
        }
    }
    t.commit().unwrap().wait().unwrap();
}

fn run(b: &Bed, flush: &dyn Fn(), what: &str) {
    // Five versions of most keys under ten prefixes, some deleted.
    for round in 0..5 {
        let mut ops = Vec::new();
        for near in 0..10 {
            for n in 0..(near % 4) * 3 {
                let k = format!("a/{near:02}/{n:02}");
                if (near + n + round) % 7 == 0 {
                    ops.push((k, None));
                } else {
                    ops.push((k, Some(format!("v{round}-{near}-{n}"))));
                }
            }
        }
        put(b, &ops);
        if round == 2 {
            flush();
        }
    }
    let outer_p = rk("a/");
    let outer = KeyRange::new(outer_p.clone(), prefix_end(&outer_p).unwrap());
    // The reader's view opens now; later commits are invisible to it.
    let mut t = b.begin(TxnOptions::mariadb());
    let _ = t.read_ts();
    put(
        b,
        &[
            ("a/03/00".into(), Some("late".into())),
            ("a/05/01".into(), None),
            ("a/07/99".into(), Some("late".into())),
        ],
    );
    // Its own writes.
    t.put(rk("a/02/50"), Bytes::from_static(b"own")).unwrap();
    t.put(rk("a/06/01"), Bytes::from_static(b"own")).unwrap();
    t.delete(rk("a/09/02")).unwrap();
    let mut prefixes: Vec<String> = (0..10).map(|n| format!("a/{n:02}/")).collect();
    prefixes.push("a/04/".into()); // again, out of order
    prefixes.push("a/42/".into()); // empty
    prefixes.push("a/".into()); // the whole outer range
    let ranges: Vec<KeyRange> = prefixes
        .iter()
        .map(|p| {
            let k = rk(p);
            KeyRange::new(k.clone(), prefix_end(&k).unwrap())
        })
        .collect();
    let many = t.scan_many(&ranges, &outer, usize::MAX).unwrap();
    assert_eq!(many.len(), ranges.len());
    let mut rows = 0;
    for (r, got) in ranges.iter().zip(&many) {
        let want = t
            .scan(r, ScanDir::Forward, usize::MAX, ReadMode::Snapshot)
            .unwrap();
        let got: Vec<(Vec<u8>, Bytes)> = got.iter().map(|(k, v)| (k.to_vec(), v.clone())).collect();
        assert_eq!(got, want, "{what}: range {:?}", r.start);
        rows += got.len();
    }
    assert!(rows > 20, "{what}: {rows} rows");
    // A limit applies per range.
    let limited = t.scan_many(&ranges, &outer, 2).unwrap();
    for (r, got) in ranges.iter().zip(&limited) {
        let want = t.scan(r, ScanDir::Forward, 2, ReadMode::Snapshot).unwrap();
        assert_eq!(got.len(), want.len(), "{what}: limited {:?}", r.start);
    }
    t.rollback();
}

#[test]
fn scan_many_on_the_in_memory_path() {
    run(&bed(), &|| {}, "in-memory path");
}

#[test]
fn scan_many_on_the_engine() {
    for store in [StoreKind::Mem, StoreKind::Rocks] {
        let dir = Dir::new("scanmany");
        let (e, b) = engine_bed(&dir, store, SyncMode::Off);
        let flush = || e.store().flush_all().unwrap();
        run(&b, &flush, &format!("engine on {store:?}"));
    }
}
