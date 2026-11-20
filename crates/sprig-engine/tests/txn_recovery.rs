//! Recovered data stays visible after a restart, even when the wall clock
//! stepped back or the restart lands in the same millisecond as the last
//! commits: the transaction layer raises its clock above the highest
//! recovered commit timestamp before the first transaction.

#![allow(clippy::unwrap_used, clippy::missing_panics_doc)]

mod txn_common;

use std::sync::Arc;
use std::time::Duration;

use sprig_engine::StoreKind;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::hlc::ManualClock;
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{LockRequest, Locked, Ts, Txn, TxnOptions, UnitConfig};
use txn_common::{Dir, engine_bed, k, v};

const T0: u64 = 1_800_000_000_000;

/// Commits rows r0..r9 on a path whose clock reads `T0`, then restarts
/// from its durable log with the clock at `restart_ms`.
fn restart_at(restart_ms: u64) {
    let p1 = MemPath::start_with(
        UnitConfig::default(),
        Duration::ZERO,
        Arc::new(ManualClock::new(T0)),
        Vec::new(),
    )
    .unwrap();
    let mut last = Ts::MIN;
    for i in 0..10 {
        let mut t = Txn::begin_on(p1.clone(), TxnOptions::mariadb());
        t.put(k(&format!("r{i}")), v(&format!("v{i}"))).unwrap();
        let c = t.commit().unwrap();
        c.wait().unwrap();
        last = c.commit_ts().unwrap();
    }
    // All ten commits share one millisecond, at or after the restart's.
    assert!(last.physical_ms() <= T0 + 1 && last.logical() >= 9);
    let entries = p1.durable_entries();
    drop(p1);

    let p2 = MemPath::start_with(
        UnitConfig::default(),
        Duration::ZERO,
        Arc::new(ManualClock::new(restart_ms)),
        entries,
    )
    .unwrap();
    let mut r = Txn::begin_on(p2.clone(), TxnOptions::mariadb());
    for i in 0..10 {
        assert_eq!(
            r.get_snapshot(&k(&format!("r{i}"))).unwrap(),
            Some(v(&format!("v{i}"))),
            "row r{i} hidden after a restart at {restart_ms}"
        );
    }
    assert!(r.read_view().unwrap() >= last);
    r.rollback();
    // New commits order after the recovered ones.
    let mut w = Txn::begin_on(p2.clone(), TxnOptions::mariadb());
    assert_eq!(
        w.lock_get_blocking(&k("r0"), LockRequest::for_update())
            .unwrap(),
        Locked::Row(Some(v("v0")))
    );
    w.put(k("r0"), v("after")).unwrap();
    let c = w.commit().unwrap();
    c.wait().unwrap();
    assert!(c.commit_ts().unwrap() > last);
    let mut r = Txn::begin_on(p2, TxnOptions::mariadb());
    assert_eq!(r.get_snapshot(&k("r0")).unwrap(), Some(v("after")));
}

#[test]
fn restart_in_the_same_millisecond() {
    restart_at(T0);
}

#[test]
fn restart_with_the_clock_stepped_back_five_seconds() {
    restart_at(T0 - 5_000);
}

#[test]
fn engine_reopen_reads_its_last_commits() {
    for store in [StoreKind::Mem, StoreKind::Rocks] {
        let dir = Dir::new("reopen");
        {
            let (_e, b) = engine_bed(&dir, store, SyncMode::Off);
            for i in 0..50 {
                let mut t = b.begin(TxnOptions::mariadb());
                t.put(k(&format!("r{i}")), v(&format!("v{i}"))).unwrap();
                t.commit().unwrap().wait().unwrap();
            }
        }
        let (_e, b) = engine_bed(&dir, store, SyncMode::Off);
        let mut r = b.begin(TxnOptions::mariadb());
        for i in 0..50 {
            assert_eq!(
                r.get_snapshot(&k(&format!("r{i}"))).unwrap(),
                Some(v(&format!("v{i}"))),
                "{store:?}: row r{i} hidden after reopen"
            );
        }
    }
}
