//! Lock table unit tests.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use bytes::Bytes;

use super::*;

fn table() -> Arc<LockTable> {
    Arc::new(LockTable::default())
}

fn w() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn key(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn granted(a: &Acquire) -> bool {
    matches!(a, Acquire::Granted { .. })
}

#[test]
fn shared_coexist_exclusive_waits_then_is_granted_fifo() {
    let t = table();
    let k = key("k");
    assert!(granted(&t.acquire(
        1,
        &w(),
        &k,
        LockMode::Shared,
        true,
        true
    )));
    assert!(granted(&t.acquire(
        2,
        &w(),
        &k,
        LockMode::Shared,
        true,
        true
    )));
    let Acquire::Wait(x3) = t.acquire(3, &w(), &k, LockMode::Exclusive, true, true) else {
        panic!("X must wait behind two S");
    };
    // A later S queues behind the waiting X.
    let Acquire::Wait(s4) = t.acquire(4, &w(), &k, LockMode::Shared, true, true) else {
        panic!("S must queue behind a waiting X");
    };
    let mut edges = t.waits_for();
    edges.sort_unstable();
    assert_eq!(edges, vec![(3, 1), (3, 2), (4, 3)]);
    t.release_all(1);
    t.release_all(2);
    assert_eq!(
        x3.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    t.release_all(3);
    assert_eq!(
        s4.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    t.release_all(4);
    assert_eq!(t.key_count(), 0);
}

#[test]
fn reentrant_and_upgrade() {
    let t = table();
    let k = key("k");
    assert!(granted(&t.acquire(
        1,
        &w(),
        &k,
        LockMode::Exclusive,
        true,
        true
    )));
    assert!(matches!(
        t.acquire(1, &w(), &k, LockMode::Shared, true, true),
        Acquire::Granted { fresh: false }
    ));
    let k2 = key("k2");
    assert!(granted(&t.acquire(
        1,
        &w(),
        &k2,
        LockMode::Shared,
        true,
        true
    )));
    assert!(matches!(
        t.acquire(1, &w(), &k2, LockMode::Exclusive, true, true),
        Acquire::Granted { fresh: true }
    ));
    assert_eq!(t.mode_held(1, &k2), Some(LockMode::Exclusive));
    assert_eq!(t.held_by(1), 2);
}

#[test]
fn two_shared_holders_upgrading_deadlock() {
    let t = table();
    let k = key("k");
    assert!(granted(&t.acquire(
        1,
        &w(),
        &k,
        LockMode::Shared,
        true,
        true
    )));
    assert!(granted(&t.acquire(
        2,
        &w(),
        &k,
        LockMode::Shared,
        true,
        true
    )));
    let Acquire::Wait(up1) = t.acquire(1, &w(), &k, LockMode::Exclusive, true, true) else {
        panic!("upgrade waits for the other S");
    };
    // 2's upgrade closes the cycle; equal weight: the requester is the victim.
    assert!(matches!(
        t.acquire(2, &w(), &k, LockMode::Exclusive, true, true),
        Acquire::Deadlock
    ));
    t.release_all(2);
    assert_eq!(
        up1.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    assert_eq!(t.mode_held(1, &k), Some(LockMode::Exclusive));
}

#[test]
fn would_wait_and_abandoned_waits_leave_nothing_behind() {
    let t = table();
    let k = key("k");
    assert!(granted(&t.acquire(
        1,
        &w(),
        &k,
        LockMode::Exclusive,
        true,
        true
    )));
    assert!(matches!(
        t.acquire(2, &w(), &k, LockMode::Shared, false, true),
        Acquire::WouldWait
    ));
    let Acquire::Wait(h) = t.acquire(3, &w(), &k, LockMode::Shared, true, true) else {
        panic!("waits");
    };
    drop(h);
    assert!(t.waits_for().is_empty());
    assert!(granted(&t.acquire(
        4,
        &w(),
        &key("other"),
        LockMode::Shared,
        false,
        true
    )));
    t.release_all(1);
    t.release_all(4);
    assert_eq!(t.key_count(), 0);
}
