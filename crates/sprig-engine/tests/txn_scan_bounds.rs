//! Range scans whose bounds fall between, before, after or inside stored
//! keys: a bound that extends a stored key (a paging caller's "just after
//! `k`", `k` followed by a zero byte, or a bound that cuts through `k`'s
//! version suffixes) must not bring in `k` at the start nor drop it at the
//! end. Checked against a model on both stores, both directions, snapshot,
//! current and locking reads, with several versions and delete markers.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::missing_panics_doc
)]

mod txn_common;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::StoreKind;
use sprig_engine::keys::{KeyRange, prefix_end};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{CommitPath, Dir as ScanDir, ReadMode, Ts, Txn, TxnOptions};
use txn_common::{Bed, Dir, bed, engine_bed};

/// A raw (unterminated) logical key under the usual engine head, like the
/// catalog's record keys: `m/g/3` is stored as is, so `m/g/3` followed by a
/// zero byte is a bound that extends it. The stored names are prefix-free.
fn rk(s: &str) -> Vec<u8> {
    let mut v = Vec::new();
    sprig_engine::keys::KeyPrefix {
        tenant: 7,
        shard: 0,
        object: 2,
        kind: sprig_engine::keys::kind::PRIMARY,
        index: 0,
    }
    .encode_into(&mut v);
    v.extend_from_slice(s.as_bytes());
    v
}

type Model = BTreeMap<Vec<u8>, Bytes>;

/// Commits `ops` (`None` deletes) and waits until the store has applied
/// them, so scans read the store and not the pending-commit overlay.
fn commit(b: &Bed, ops: &[(&str, Option<&str>)], model: &mut Model) {
    let mut t = b.begin(TxnOptions::mariadb());
    for (name, val) in ops {
        if let Some(s) = val {
            let v = Bytes::copy_from_slice(s.as_bytes());
            t.put(rk(name), v.clone()).unwrap();
            model.insert(rk(name), v);
        } else {
            t.delete(rk(name)).unwrap();
            model.remove(&rk(name));
        }
    }
    t.commit().unwrap().wait().unwrap();
    settle(b);
}

fn settle(b: &Bed) {
    let path: &dyn CommitPath = &*b.path;
    let deadline = Instant::now() + Duration::from_secs(10);
    while path.shared().pending_versions() > 0 {
        let mut t = b.begin(TxnOptions::mariadb());
        // A read prunes overlay versions the store has applied.
        let _ = t.get_snapshot(&rk("settle")).unwrap();
        t.rollback();
        assert!(Instant::now() < deadline, "the store never applied the log");
        std::thread::yield_now();
    }
}

/// A reader with its read view open now.
fn reader(b: &Bed) -> (Txn, Ts) {
    let mut t = b.begin(TxnOptions::mariadb());
    let ts = t.read_ts();
    (t, ts)
}

/// Bounds around, between, inside and beyond the stored keys.
fn bounds(cuts: &[Ts]) -> Vec<Vec<u8>> {
    let names = [
        "", "m/", "m/g/", "m/g/0", "m/g/1", "m/g/2", "m/g/3", "m/g/4", "m/g/5", "m/g/6", "m/h/1",
        "m/z",
    ];
    let mut out = Vec::new();
    for n in names {
        let k = rk(n);
        out.push(k.clone());
        // "Just after k" the way paging callers write it.
        let mut z = k.clone();
        z.push(0);
        out.push(z);
        // "Just after k" the way the locking scan writes it.
        let mut ff = k.clone();
        ff.extend_from_slice(&[0xFF; 9]);
        out.push(ff);
        if let Some(e) = prefix_end(&k) {
            out.push(e);
        }
        // Bounds that cut through k's version suffixes, between versions
        // newer and older than each reader.
        for ts in cuts {
            let mut c = k.clone();
            c.extend_from_slice(&(!ts.0).to_be_bytes());
            out.push(c);
        }
    }
    out.sort();
    out.dedup();
    out
}

fn expected(model: &Model, r: &KeyRange, dir: ScanDir, limit: usize) -> Vec<(Vec<u8>, Bytes)> {
    let mut rows: Vec<(Vec<u8>, Bytes)> = model
        .iter()
        .filter(|(k, _)| r.contains(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if dir == ScanDir::Reverse {
        rows.reverse();
    }
    rows.truncate(limit);
    rows
}

fn show(rows: &[(Vec<u8>, Bytes)]) -> Vec<String> {
    let head = rk("").len();
    rows.iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                String::from_utf8_lossy(&k[head..]),
                String::from_utf8_lossy(v)
            )
        })
        .collect()
}

/// Every range over `bounds` (and up to the end of the test's key head),
/// both directions, full and limited, read by `read`, against `model`. (Not
/// unbounded: the engine keeps its own bookkeeping keys after every
/// tenant's data, and a current read of everything would see them.)
fn check_all(
    what: &str,
    bounds: &[Vec<u8>],
    model: &Model,
    mut read: impl FnMut(&KeyRange, ScanDir, usize) -> Vec<(Vec<u8>, Bytes)>,
) -> usize {
    let mut checked = 0;
    let head_end = prefix_end(&rk(""));
    for (i, s) in bounds.iter().enumerate() {
        let ends = bounds[i..]
            .iter()
            .cloned()
            .map(Some)
            .chain([head_end.clone()]);
        for e in ends {
            let r = KeyRange {
                start: s.clone(),
                end: e,
            };
            for dir in [ScanDir::Forward, ScanDir::Reverse] {
                for limit in [usize::MAX, 1, 2] {
                    let got = read(&r, dir, limit);
                    let want = expected(model, &r, dir, limit);
                    assert_eq!(
                        show(&got),
                        show(&want),
                        "{what}: {dir:?} limit {limit} over {r:?}"
                    );
                    checked += 1;
                }
            }
        }
    }
    checked
}

/// Pages through `[from, to)` one row at a time, as paging callers do:
/// forward with the next start just after the last key (zero byte or
/// `0xFF` padding), reverse with the next end at the last key.
fn check_paging(
    what: &str,
    model: &Model,
    mut read: impl FnMut(&KeyRange, ScanDir) -> Vec<(Vec<u8>, Bytes)>,
) {
    let all = KeyRange::prefix(&rk("m/"));
    let want: Vec<(Vec<u8>, Bytes)> = expected(model, &all, ScanDir::Forward, usize::MAX);
    for pad in [&[0u8][..], &[0xFF; 9][..]] {
        let mut got = Vec::new();
        let mut r = all.clone();
        loop {
            let page = read(&r, ScanDir::Forward);
            assert!(page.len() <= 1);
            let Some((k, v)) = page.into_iter().next() else {
                break;
            };
            assert!(
                got.len() <= want.len(),
                "{what}: forward paging repeats rows"
            );
            let mut next = k.clone();
            next.extend_from_slice(pad);
            r.start = next;
            got.push((k, v));
        }
        assert_eq!(
            show(&got),
            show(&want),
            "{what}: forward paging, pad {pad:?}"
        );
    }
    let mut got = Vec::new();
    let mut r = all.clone();
    loop {
        let page = read(&r, ScanDir::Reverse);
        let Some((k, v)) = page.into_iter().next() else {
            break;
        };
        assert!(
            got.len() <= want.len(),
            "{what}: reverse paging repeats rows"
        );
        r.end = Some(k.clone());
        got.push((k, v));
    }
    got.reverse();
    assert_eq!(show(&got), show(&want), "{what}: reverse paging");
}

fn scan(
    t: &mut Txn,
    r: &KeyRange,
    dir: ScanDir,
    limit: usize,
    mode: ReadMode,
) -> Vec<(Vec<u8>, Bytes)> {
    t.scan(r, dir, limit, mode).unwrap()
}

fn run(b: &Bed, flush: &dyn Fn(), what: &str) {
    let mut model = Model::new();
    let (mut r0, t0) = reader(b);
    let m0 = model.clone();
    commit(
        b,
        &[
            ("m/g/1", Some("a1")),
            ("m/g/2", Some("a2")),
            ("m/g/3", Some("a3")),
            ("m/g/5", Some("a5")),
            ("m/h/1", Some("ah")),
        ],
        &mut model,
    );
    // On RocksDB the first versions go to a file, the rest stay in memory.
    flush();
    let (mut r1, t1) = reader(b);
    let m1 = model.clone();
    commit(
        b,
        &[
            ("m/g/3", Some("b3")),
            ("m/g/2", None),
            ("m/g/4", Some("b4")),
        ],
        &mut model,
    );
    let (mut r2, t2) = reader(b);
    let m2 = model.clone();
    commit(
        b,
        &[
            ("m/g/3", None),
            ("m/g/2", Some("c2")),
            ("m/g/1", Some("c1")),
        ],
        &mut model,
    );
    let (mut r3, t3) = reader(b);
    let m3 = model.clone();

    let bounds = bounds(&[t0, t1, t2, t3]);
    let mut n = 0;
    for (name, t, m) in [
        ("snapshot before any commit", &mut r0, &m0),
        ("snapshot after the first commit", &mut r1, &m1),
        ("snapshot after the second commit", &mut r2, &m2),
        ("snapshot after the third commit", &mut r3, &m3),
    ] {
        let what = format!("{what}, {name}");
        n += check_all(&what, &bounds, m, |r, dir, limit| {
            scan(t, r, dir, limit, ReadMode::Snapshot)
        });
        check_paging(&what, m, |r, dir| scan(t, r, dir, 1, ReadMode::Snapshot));
    }

    // Current reads without locks.
    let mut cur = b.begin(TxnOptions::mariadb());
    let w = format!("{what}, current read");
    n += check_all(&w, &bounds, &model, |r, dir, limit| {
        cur.scan_current(r, dir, limit)
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v))
            .collect()
    });
    check_paging(&w, &model, |r, dir| {
        cur.scan_current(r, dir, 1)
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v))
            .collect()
    });
    cur.rollback();

    // Locking reads, each in its own transaction so no lock is left behind.
    let w = format!("{what}, locking read");
    let locking = |r: &KeyRange, dir: ScanDir, limit: usize| {
        let mut t = b.begin(TxnOptions::mariadb());
        let rows = scan(&mut t, r, dir, limit, ReadMode::ForUpdate);
        t.rollback();
        rows
    };
    n += check_all(&w, &bounds, &model, locking);
    check_paging(&w, &model, |r, dir| locking(r, dir, 1));

    // Snapshot reads merged with the transaction's own writes.
    let mut own = b.begin(TxnOptions::mariadb());
    let _ = own.read_ts();
    let mut mine = model.clone();
    own.put(rk("m/g/6"), Bytes::from_static(b"own6")).unwrap();
    mine.insert(rk("m/g/6"), Bytes::from_static(b"own6"));
    own.delete(rk("m/g/4")).unwrap();
    mine.remove(&rk("m/g/4"));
    own.put(rk("m/g/3"), Bytes::from_static(b"own3")).unwrap();
    mine.insert(rk("m/g/3"), Bytes::from_static(b"own3"));
    let w = format!("{what}, own writes");
    n += check_all(&w, &bounds, &mine, |r, dir, limit| {
        scan(&mut own, r, dir, limit, ReadMode::Snapshot)
    });
    own.rollback();
    for t in [r0, r1, r2, r3] {
        t.rollback();
    }
    assert!(n > 10_000, "{what}: only {n} scans checked");
}

#[test]
fn the_reported_case() {
    // Keys m/g/1..3; a scan from just after m/g/3 must not return m/g/3,
    // and a scan up to just after m/g/3 must return it.
    let b = bed();
    let mut model = Model::new();
    commit(
        &b,
        &[
            ("m/g/1", Some("1")),
            ("m/g/2", Some("2")),
            ("m/g/3", Some("3")),
        ],
        &mut model,
    );
    let mut after = rk("m/g/3");
    after.push(0);
    let (mut t, _) = reader(&b);
    for dir in [ScanDir::Forward, ScanDir::Reverse] {
        let tail = KeyRange {
            start: after.clone(),
            end: prefix_end(&rk("m/")),
        };
        assert!(scan(&mut t, &tail, dir, 10, ReadMode::Snapshot).is_empty());
        let head = KeyRange::new(rk("m/"), after.clone());
        assert_eq!(
            show(&scan(&mut t, &head, dir, 10, ReadMode::Snapshot)).len(),
            3,
            "{dir:?}"
        );
    }
    t.rollback();
}

#[test]
fn bounds_on_the_in_memory_path() {
    run(&bed(), &|| {}, "in-memory path");
}

#[test]
fn bounds_on_the_engine() {
    for store in [StoreKind::Mem, StoreKind::Rocks] {
        let dir = Dir::new("scanbounds");
        let (e, b) = engine_bed(&dir, store, SyncMode::Off);
        let flush = || e.store().flush_all().unwrap();
        run(&b, &flush, &format!("engine on {store:?}"));
    }
}
