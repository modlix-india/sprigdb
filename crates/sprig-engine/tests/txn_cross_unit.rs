//! Commits across units: prepare records, the anchor's transaction record
//! as the commit point, and resolution afterwards; a coordinator that stops
//! part way, a lost resolution, a stale pending transaction aborted by
//! compare-and-set, recovery after a restart, and readers that never see
//! half of a transfer between two tenants. Each runs with the steps queued
//! at once (the single-node log) and with waits between them (the
//! replicated protocol).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

mod txn_common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::keys::{self, KeyPrefix, KeyRange, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::store::{Cf, IterOpts};
use sprig_engine::txn::{CrossFault, CrossStage, LockRequest, ReadMode, TxnOptions, Wait};
use sprig_engine::{Engine, StoreKind};
use txn_common::{Dir, engine_cfg, errno};

fn key(tenant: u64, s: &str) -> Bytes {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(s.as_bytes());
    Bytes::from(k)
}

fn open(dir: &Dir, store: StoreKind, wait: bool) -> Engine {
    let e = Engine::open(engine_cfg(dir, store, SyncMode::Off)).unwrap();
    e.txn_shared().set_cross_unit_wait(wait);
    e
}

fn get(e: &Engine, tenant: u64, k: &str) -> Option<Bytes> {
    let mut t = e.begin(TxnOptions::mariadb()).unwrap();
    t.get_snapshot(&key(tenant, k)).unwrap()
}

fn lockable(e: &Engine, tenant: u64, k: &str) -> bool {
    let mut t = e.begin(TxnOptions::mariadb()).unwrap();
    t.lock_blocking(
        &key(tenant, k),
        LockRequest::write().with_wait(Wait::NoWait),
    )
    .is_ok()
}

fn put_across(e: &Engine, rows: &[(u64, &str)], val: &str) -> sprig_common::Result<()> {
    let mut t = e.begin(TxnOptions::mariadb()).unwrap();
    for (tn, k) in rows {
        t.put(key(*tn, k).to_vec(), Bytes::copy_from_slice(val.as_bytes()))
            .unwrap();
    }
    t.commit()?.wait()
}

/// Prepare and transaction records left in the store.
fn records(e: &Engine) -> usize {
    let all = keys::unit_meta_key(0, b"");
    let prefix = &all[..all.len() - 8];
    let mut it = e
        .store()
        .iter(Cf::Data, IterOpts::range(&KeyRange::prefix(prefix)));
    it.seek_to_first();
    let mut n = 0;
    while it.valid() {
        let f = &it.key()[prefix.len() + 8..];
        if f.starts_with(b"tr:") || f.starts_with(b"tp:") {
            n += 1;
        }
        it.next();
    }
    n
}

/// Drives the resolver until nothing is open and the store holds no record.
fn settle(e: &Engine) {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        e.resolve_cross_unit();
        let applied = e
            .units()
            .iter()
            .all(|(u, _)| e.consensus().applied_index(*u) >= e.consensus().durable_index(*u));
        if e.txn_shared().cross_unit_open() == 0 && applied && records(e) == 0 {
            return;
        }
        assert!(
            Instant::now() < until,
            "commits across units never finished"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn modes() -> [(StoreKind, bool); 4] {
    [
        (StoreKind::Mem, false),
        (StoreKind::Mem, true),
        (StoreKind::Rocks, false),
        (StoreKind::Rocks, true),
    ]
}

#[test]
fn a_commit_across_units_is_whole_and_leaves_no_records() {
    for (store, wait) in modes() {
        let dir = Dir::new("cross-whole");
        let e = open(&dir, store, wait);
        put_across(&e, &[(3, "a"), (4, "b"), (4, "c")], "1").unwrap();
        assert_eq!(get(&e, 3, "a").as_deref(), Some(&b"1"[..]));
        assert_eq!(get(&e, 4, "b").as_deref(), Some(&b"1"[..]));
        assert!(lockable(&e, 3, "a") && lockable(&e, 4, "b"));
        settle(&e);
        if store == StoreKind::Rocks {
            drop(e);
            let e = open(&dir, store, wait);
            assert_eq!(get(&e, 4, "c").as_deref(), Some(&b"1"[..]));
            assert_eq!(get(&e, 3, "a").as_deref(), Some(&b"1"[..]));
            assert_eq!(records(&e), 0);
        }
    }
}

#[test]
fn a_coordinator_lost_before_the_commit_point_is_aborted() {
    for (store, wait) in modes() {
        let dir = Dir::new("cross-lost-prep");
        let e = open(&dir, store, wait);
        e.txn_shared()
            .set_cross_unit_faults(Some(Arc::new(|s| match s {
                CrossStage::Prepared => CrossFault::CoordinatorLost,
                _ => CrossFault::Go,
            })));
        let err = put_across(&e, &[(3, "a"), (4, "b")], "1").unwrap_err();
        assert_eq!(errno(&err), 1213);
        e.txn_shared().set_cross_unit_faults(None);
        // Its locks stay until a resolver finds it stale and aborts it.
        assert!(!lockable(&e, 4, "b"), "{store:?} wait={wait}");
        assert_eq!(get(&e, 3, "a"), None);
        assert_eq!(get(&e, 4, "b"), None);
        settle(&e);
        assert!(lockable(&e, 3, "a") && lockable(&e, 4, "b"));
        assert_eq!(get(&e, 4, "b"), None);
        put_across(&e, &[(3, "a"), (4, "b")], "2").unwrap();
        assert_eq!(get(&e, 4, "b").as_deref(), Some(&b"2"[..]));
    }
}

#[test]
fn a_coordinator_lost_after_the_commit_point_commits() {
    for (store, wait) in modes() {
        let dir = Dir::new("cross-lost-point");
        let e = open(&dir, store, wait);
        e.txn_shared()
            .set_cross_unit_faults(Some(Arc::new(|s| match s {
                CrossStage::CommitPoint => CrossFault::CoordinatorLost,
                _ => CrossFault::Go,
            })));
        let err = put_across(&e, &[(3, "a"), (3, "z"), (4, "b")], "1").unwrap_err();
        assert_eq!(errno(&err), 1213, "the outcome is unknown to the client");
        e.txn_shared().set_cross_unit_faults(None);
        // Committed: readers see the participant's write before it is
        // resolved; writers wait for its lock.
        assert_eq!(get(&e, 4, "b").as_deref(), Some(&b"1"[..]));
        assert_eq!(get(&e, 3, "a").as_deref(), Some(&b"1"[..]));
        assert!(!lockable(&e, 4, "b"));
        settle(&e);
        assert!(lockable(&e, 4, "b"));
        assert_eq!(get(&e, 4, "b").as_deref(), Some(&b"1"[..]));
        if store == StoreKind::Rocks {
            drop(e);
            let e = open(&dir, store, wait);
            assert_eq!(get(&e, 4, "b").as_deref(), Some(&b"1"[..]));
        }
    }
}

#[test]
fn a_lost_resolution_is_redone_by_the_resolver() {
    for (store, wait) in modes() {
        let dir = Dir::new("cross-drop");
        let e = open(&dir, store, wait);
        e.txn_shared()
            .set_cross_unit_faults(Some(Arc::new(|s| match s {
                CrossStage::Resolve(5) => CrossFault::Drop,
                _ => CrossFault::Go,
            })));
        put_across(&e, &[(3, "a"), (3, "y"), (4, "b"), (5, "c")], "1").unwrap();
        e.txn_shared().set_cross_unit_faults(None);
        assert_eq!(get(&e, 5, "c").as_deref(), Some(&b"1"[..]));
        // Unit 4 was resolved; the transaction keeps its locks until unit 5
        // is too.
        assert!(!lockable(&e, 5, "c"));
        assert!(e.txn_shared().cross_unit_open() > 0);
        settle(&e);
        assert!(lockable(&e, 5, "c") && lockable(&e, 4, "b"));
        assert_eq!(get(&e, 5, "c").as_deref(), Some(&b"1"[..]));
    }
}

/// Waits until the store holds a record (the entries are applied).
fn until_recorded(e: &Engine) {
    let until = Instant::now() + Duration::from_secs(10);
    while records(e) == 0 {
        assert!(Instant::now() < until, "no record reached the store");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn recovery_finishes_or_aborts_what_a_crash_left() {
    for wait in [false, true] {
        for stage in [CrossStage::CommitPoint, CrossStage::Prepared] {
            let dir = Dir::new("cross-recover");
            let e = open(&dir, StoreKind::Rocks, wait);
            e.txn_shared()
                .set_cross_unit_faults(Some(Arc::new(move |s| {
                    if s == stage {
                        CrossFault::CoordinatorLost
                    } else {
                        CrossFault::Go
                    }
                })));
            assert!(put_across(&e, &[(3, "a"), (3, "x"), (4, "b")], "1").is_err());
            until_recorded(&e);
            // The node stops with the transaction unfinished.
            drop(e);
            let e = open(&dir, StoreKind::Rocks, wait);
            let want = (stage == CrossStage::CommitPoint).then_some(&b"1"[..]);
            assert_eq!(get(&e, 4, "b").as_deref(), want, "{stage:?} wait={wait}");
            assert_eq!(get(&e, 3, "x").as_deref(), want);
            assert!(lockable(&e, 4, "b") && lockable(&e, 3, "a"));
            settle(&e);
            drop(e);
            let e = open(&dir, StoreKind::Rocks, wait);
            assert_eq!(records(&e), 0);
            assert_eq!(get(&e, 4, "b").as_deref(), want);
        }
    }
}

#[test]
fn a_stale_pending_transaction_is_aborted_by_compare_and_set() {
    let dir = Dir::new("cross-stale");
    let e = open(&dir, StoreKind::Mem, true);
    // The coordinator stops after its prepare records are durable, until
    // the resolver has run.
    let at_prepared = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let (a, r) = (Arc::clone(&at_prepared), Arc::clone(&resume));
    e.txn_shared()
        .set_cross_unit_faults(Some(Arc::new(move |s| {
            if s == CrossStage::Prepared {
                a.wait();
                r.wait();
            }
            CrossFault::Go
        })));
    let e2 = e.clone();
    let coordinator = std::thread::spawn(move || put_across(&e2, &[(3, "a"), (4, "b")], "1"));
    at_prepared.wait();
    // Alive but silent past the stale time: aborted by compare-and-set.
    e.txn_shared().set_cross_unit_stale_ms(0);
    e.resolve_cross_unit();
    resume.wait();
    let err = coordinator.join().unwrap().unwrap_err();
    assert_eq!(errno(&err), 1213);
    e.txn_shared().set_cross_unit_faults(None);
    assert_eq!(get(&e, 3, "a"), None);
    assert_eq!(get(&e, 4, "b"), None);
    settle(&e);
    assert!(lockable(&e, 3, "a") && lockable(&e, 4, "b"));
}

/// Money moves between accounts of four tenants; readers sum every
/// account in one read view and must always find the same total.
#[test]
fn readers_never_see_half_a_transfer() {
    for (store, wait) in [(StoreKind::Mem, false), (StoreKind::Mem, true)] {
        let dir = Dir::new("cross-bank");
        let e = open(&dir, store, wait);
        let accounts: Vec<(u64, String)> =
            (0..8).map(|i| (10 + i % 4, format!("acct{i}"))).collect();
        {
            let mut t = e.begin(TxnOptions::mariadb()).unwrap();
            for (tn, a) in &accounts {
                t.put(
                    key(*tn, a).to_vec(),
                    Bytes::from(100u64.to_be_bytes().to_vec()),
                )
                .unwrap();
            }
            t.commit().unwrap().wait().unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let moved = Arc::new(Mutex::new(0u64));
        let mut hs = Vec::new();
        for w in 0..4u64 {
            let (e, accounts, stop, moved) = (
                e.clone(),
                accounts.clone(),
                Arc::clone(&stop),
                Arc::clone(&moved),
            );
            hs.push(std::thread::spawn(move || {
                let mut seed = w;
                while !stop.load(Ordering::Relaxed) {
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    let a = usize::try_from(seed >> 33).unwrap() % accounts.len();
                    let b = (a + 1 + usize::try_from(seed >> 40).unwrap() % (accounts.len() - 1))
                        % accounts.len();
                    let (lo, hi) = (a.min(b), a.max(b));
                    let mut t = e.begin(TxnOptions::mariadb()).unwrap();
                    let bal = |txn: &mut sprig_engine::txn::Txn, ix: usize| {
                        let (tn, name) = &accounts[ix];
                        let v = txn
                            .get(&key(*tn, name), ReadMode::ForUpdate)
                            .ok()?
                            .expect_ready()
                            .ok()??;
                        Some(u64::from_be_bytes(v.as_ref().try_into().unwrap()))
                    };
                    let (Some(x), Some(y)) = (bal(&mut t, lo), bal(&mut t, hi)) else {
                        continue;
                    };
                    let amount = (seed >> 50) % 7;
                    let (from, to, fx, ty) = if a == lo {
                        (lo, hi, x, y)
                    } else {
                        (hi, lo, y, x)
                    };
                    if fx < amount {
                        continue;
                    }
                    for (ix, nv) in [(from, fx - amount), (to, ty + amount)] {
                        let (tn, name) = &accounts[ix];
                        t.put(
                            key(*tn, name).to_vec(),
                            Bytes::from(nv.to_be_bytes().to_vec()),
                        )
                        .unwrap();
                    }
                    if t.commit().and_then(|c| c.wait()).is_ok() {
                        *moved.lock().unwrap() += 1;
                    }
                }
            }));
        }
        let mut sums = 0;
        let until = Instant::now() + Duration::from_millis(1500);
        while Instant::now() < until {
            let mut t = e.begin(TxnOptions::mariadb()).unwrap();
            let total: u64 = accounts
                .iter()
                .map(|(tn, a)| {
                    let v = t.get_snapshot(&key(*tn, a)).unwrap().unwrap();
                    u64::from_be_bytes(v.as_ref().try_into().unwrap())
                })
                .sum();
            assert_eq!(total, 800, "a reader saw half a transfer (wait={wait})");
            sums += 1;
        }
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().unwrap();
        }
        assert!(*moved.lock().unwrap() > 50, "too few transfers");
        assert!(sums > 50);
        settle(&e);
    }
}

/// An XA transaction over two units: its commit runs the protocol across
/// units with the XA record deleted by the commit point, so a crash right
/// after the commit point leaves it committed and no longer prepared.
#[test]
fn an_xa_commit_across_units_is_not_prepared_again_after_a_crash() {
    use sprig_engine::txn::{CommitPath, Txn};
    for wait in [false, true] {
        let dir = Dir::new("cross-xa");
        let e = open(&dir, StoreKind::Rocks, wait);
        let path: Arc<dyn CommitPath> = Arc::new(e.clone());
        let mut t = e.begin(TxnOptions::mariadb()).unwrap();
        // The XA record goes to the first write's unit (3); unit 4 has more
        // writes.
        t.put(key(3, "a").to_vec(), Bytes::from_static(b"1"))
            .unwrap();
        t.put(key(4, "b").to_vec(), Bytes::from_static(b"1"))
            .unwrap();
        t.put(key(4, "c").to_vec(), Bytes::from_static(b"1"))
            .unwrap();
        t.xa_prepare(b"x1").unwrap().wait().unwrap();
        drop(t);
        e.txn_shared()
            .set_cross_unit_faults(Some(Arc::new(|s| match s {
                CrossStage::CommitPoint => CrossFault::CoordinatorLost,
                _ => CrossFault::Go,
            })));
        assert!(Txn::xa_commit(&path, b"x1").is_err());
        e.txn_shared().set_cross_unit_faults(None);
        until_recorded(&e);
        drop(path);
        drop(e);
        let e = open(&dir, StoreKind::Rocks, wait);
        let path: Arc<dyn CommitPath> = Arc::new(e.clone());
        assert!(Txn::xa_recover(&path).is_empty(), "wait={wait}");
        assert_eq!(get(&e, 4, "c").as_deref(), Some(&b"1"[..]));
        assert_eq!(get(&e, 3, "a").as_deref(), Some(&b"1"[..]));
        assert!(lockable(&e, 4, "b") && lockable(&e, 3, "a"));
        drop(path);
        settle(&e);
    }
}
