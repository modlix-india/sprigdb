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
        Acquire::Granted { fresh: false, .. }
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
        Acquire::Granted { fresh: true, .. }
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

// ----- gap locks ------------------------------------------------------------

fn at(s: &str) -> Pos {
    Pos::at(key(s))
}

fn after(s: &str) -> Pos {
    Pos::after(key(s))
}

fn x(t: &Arc<LockTable>, txn: TxnId, k: &str) -> Acquire {
    t.acquire(txn, &w(), &key(k), LockMode::Exclusive, true, true)
}

#[test]
fn gap_locks_are_shared_and_block_only_inserts() {
    let t = table();
    assert!(!t.has_gaps());
    assert_eq!(
        t.gap_acquire(1, &w(), &at("b"), &at("f"), false, true),
        GapAcquire::Granted
    );
    assert_eq!(
        t.gap_acquire(2, &w(), &at("a"), &at("d"), false, true),
        GapAcquire::Granted
    );
    assert!(t.has_gaps());
    // Record locks inside the gaps are free for anyone.
    assert!(granted(&x(&t, 3, "c")));
    // An insert of `c` waits for both gap holders; of `e`, for one.
    let Acquire::Wait(c3) = t.insert_intent(3, &w(), &key("c"), true, true) else {
        panic!("the insert waits");
    };
    assert!(c3.is_insert_intention());
    let mut edges = t.waits_for();
    edges.sort_unstable();
    assert_eq!(edges, vec![(3, 1), (3, 2)]);
    assert!(granted(&x(&t, 4, "e")));
    assert!(matches!(
        t.insert_intent(4, &w(), &key("e"), false, true),
        Acquire::WouldWait
    ));
    // Outside every gap: no wait. The bounds: `f` is outside, `a` inside.
    assert!(granted(&t.insert_intent(4, &w(), &key("f"), true, true)));
    assert!(matches!(
        t.insert_intent(4, &w(), &key("a"), false, true),
        Acquire::WouldWait
    ));
    // A holder's own gap never blocks it.
    assert!(granted(&t.insert_intent(1, &w(), &key("e"), true, true)));
    t.release_all(1);
    // Still covered by 2.
    assert_eq!(t.waits_for(), vec![(3, 2)]);
    t.release_all(2);
    assert_eq!(
        c3.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    assert!(granted(&t.insert_intent(3, &w(), &key("c"), true, true)));
    t.release_all(3);
    t.release_all(4);
    assert_eq!(t.key_count(), 0);
    assert_eq!(t.gap_count(), 0);
    assert!(!t.has_gaps());
}

#[test]
fn a_gap_stops_at_a_key_another_transaction_holds_exclusively() {
    let t = table();
    assert!(granted(&x(&t, 9, "c")));
    assert!(granted(&x(&t, 9, "e")));
    assert!(granted(&t.acquire(
        8,
        &w(),
        &key("b"),
        LockMode::Shared,
        true,
        true
    )));
    // Forward: up to the first such key. Shared locks don't stop it.
    assert_eq!(
        t.gap_acquire(1, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::StoppedAt(key("c"))
    );
    assert!(t.gap_covers(1, &key("b")));
    assert!(t.gap_covers(1, &key("bz")));
    assert!(!t.gap_covers(1, &key("c")));
    assert!(!t.gap_covers(1, &key("d")));
    // Backward: down to the last such key.
    assert_eq!(
        t.gap_acquire(2, &w(), &at("a"), &at("z"), true, true),
        GapAcquire::StoppedAt(key("e"))
    );
    assert!(t.gap_covers(2, &key("f")));
    assert!(!t.gap_covers(2, &key("e")));
    assert!(!t.gap_covers(2, &key("d")));
    // A reader that skips locked rows takes the whole stretch.
    assert_eq!(
        t.gap_acquire(3, &w(), &at("a"), &at("z"), false, false),
        GapAcquire::Granted
    );
    assert!(t.gap_covers(3, &key("c")));
    // The holder's own locks never stop its own gap.
    assert_eq!(
        t.gap_acquire(9, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    // The bounds: a key at the end bound is outside, one at the start is in.
    assert_eq!(
        t.gap_acquire(4, &w(), &at("c0"), &at("e"), false, true),
        GapAcquire::Granted
    );
    assert_eq!(
        t.gap_acquire(4, &w(), &at("e"), &at("f"), false, true),
        GapAcquire::StoppedAt(key("e"))
    );
    assert_eq!(
        t.gap_acquire(5, &w(), &after("e"), &at("f"), false, true),
        GapAcquire::Granted
    );
    assert_eq!(
        t.gap_acquire(5, &w(), &after("d"), &after("e"), false, true),
        GapAcquire::StoppedAt(key("e"))
    );
    for txn in [1, 2, 3, 4, 5, 8, 9] {
        t.release_all(txn);
    }
    assert_eq!(t.key_count() + t.gap_count(), 0);
}

#[test]
fn a_gap_passes_over_a_key_whose_holder_is_itself_waiting_to_insert_it() {
    let t = table();
    assert_eq!(
        t.gap_acquire(1, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    assert!(granted(&x(&t, 2, "m")));
    let Acquire::Wait(ins) = t.insert_intent(2, &w(), &key("m"), true, true) else {
        panic!("the insert waits for 1's gap");
    };
    // 3's read doesn't wait for 2, which has inserted nothing yet; 2 now
    // waits for both readers.
    assert_eq!(
        t.gap_acquire(3, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    let mut edges = t.waits_for();
    edges.sort_unstable();
    assert_eq!(edges, vec![(2, 1), (2, 3)]);
    t.release_all(1);
    t.release_all(3);
    assert_eq!(
        ins.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    // Its wait is over: the next reader stops at the key.
    assert_eq!(
        t.gap_acquire(4, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::StoppedAt(key("m"))
    );
    t.release_all(2);
    t.release_all(4);
    assert_eq!(t.key_count() + t.gap_count(), 0);
}

#[test]
fn two_gap_holders_that_both_insert_deadlock() {
    let t = table();
    for txn in [1, 2] {
        assert_eq!(
            t.gap_acquire(txn, &w(), &at("a"), &at("z"), false, true),
            GapAcquire::Granted
        );
    }
    assert!(granted(&x(&t, 1, "k1")));
    assert!(granted(&x(&t, 2, "k2")));
    let Acquire::Wait(first) = t.insert_intent(1, &w(), &key("k1"), true, true) else {
        panic!("1 waits for 2's gap");
    };
    // 2 holds as much as 1: the requester is the victim.
    assert!(matches!(
        t.insert_intent(2, &w(), &key("k2"), true, true),
        Acquire::Deadlock
    ));
    let report = t.latest_deadlock().expect("recorded");
    assert_eq!(report.victim, 2);
    assert_eq!(report.members.len(), 2);
    assert!(report.members.iter().all(|m| m.insert_intention));
    t.release_all(2);
    assert_eq!(
        first.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    t.release_all(1);
    assert_eq!(t.key_count() + t.gap_count(), 0);
    assert_eq!(t.stats.deadlocks.load(Ordering::Relaxed), 1);
}

#[test]
fn a_heavier_insert_waiter_makes_the_other_transaction_the_victim() {
    let t = table();
    let heavy = Arc::new(AtomicUsize::new(50));
    assert_eq!(
        t.gap_acquire(1, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    assert_eq!(
        t.gap_acquire(2, &heavy, &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    assert!(granted(&x(&t, 1, "k1")));
    assert!(granted(&t.acquire(
        2,
        &heavy,
        &key("k2"),
        LockMode::Exclusive,
        true,
        true
    )));
    let Acquire::Wait(light) = t.insert_intent(1, &w(), &key("k1"), true, true) else {
        panic!("1 waits");
    };
    // 2 has written 50 rows: 1, waiting, is cheaper to roll back. 2 goes
    // on waiting until 1's rollback drops its gap lock.
    let Acquire::Wait(heavy_wait) = t.insert_intent(2, &heavy, &key("k2"), true, true) else {
        panic!("2 waits for 1's gap");
    };
    assert_eq!(
        light.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Deadlock
    );
    assert_eq!(t.latest_deadlock().map(|r| r.victim), Some(1));
    t.release_all(1);
    assert_eq!(
        heavy_wait.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    t.release_all(2);
    assert_eq!(t.key_count() + t.gap_count(), 0);
}

#[test]
fn an_insert_wait_times_out_and_can_be_abandoned_or_interrupted() {
    let t = table();
    assert_eq!(
        t.gap_acquire(1, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    assert!(granted(&x(&t, 2, "m")));
    let Acquire::Wait(h) = t.insert_intent(2, &w(), &key("m"), true, true) else {
        panic!("waits");
    };
    assert_eq!(
        h.wait_blocking(Some(Duration::from_millis(20))),
        WaitOutcome::TimedOut
    );
    assert!(t.waits_for().is_empty());
    let Acquire::Wait(h) = t.insert_intent(2, &w(), &key("m"), true, true) else {
        panic!("waits");
    };
    drop(h);
    assert!(t.waits_for().is_empty());
    assert_eq!(t.gap_count(), 1);
    // Interrupts end record waits and insert waits alike.
    let Acquire::Wait(h) = t.insert_intent(2, &w(), &key("m"), true, true) else {
        panic!("waits");
    };
    assert!(t.interrupt(2, Interrupt::Kill));
    assert_eq!(
        h.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Interrupted(Interrupt::Kill)
    );
    let Acquire::Wait(h) = x(&t, 3, "m") else {
        panic!("waits for 2's lock");
    };
    assert!(t.interrupt(3, Interrupt::StatementTimeout));
    assert_eq!(
        h.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Interrupted(Interrupt::StatementTimeout)
    );
    assert!(t.waits_for().is_empty());
    // Nothing to interrupt.
    assert!(!t.interrupt(3, Interrupt::Kill));
    assert!(!t.interrupt(77, Interrupt::Kill));
    t.release_all(1);
    t.release_all(2);
    t.release_all(3);
    assert_eq!(t.key_count() + t.gap_count(), 0);
}

#[test]
fn an_insert_probe_takes_x_when_alone_and_the_duplicate_mode_when_not() {
    let t = table();
    let k = key("k");
    let (got, mode) = t.acquire_insert_probe(1, &w(), &k, LockMode::Shared, true, true);
    assert!(granted(&got));
    assert_eq!(mode, LockMode::Exclusive);
    // Someone is there: ask for the duplicate check's mode, and wait.
    let (got, mode) = t.acquire_insert_probe(2, &w(), &k, LockMode::Shared, true, true);
    assert_eq!(mode, LockMode::Shared);
    let Acquire::Wait(s2) = got else {
        panic!("waits for 1's X");
    };
    let (got, mode) = t.acquire_insert_probe(3, &w(), &k, LockMode::Exclusive, false, true);
    assert_eq!(mode, LockMode::Exclusive);
    assert!(matches!(got, Acquire::WouldWait));
    // 1 found the key present: it keeps a shared lock, which lets 2 in.
    t.downgrade(1, &k);
    assert_eq!(t.mode_held(1, &k), Some(LockMode::Shared));
    assert_eq!(
        s2.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    assert_eq!(t.mode_held(2, &k), Some(LockMode::Shared));
    // An upgrade says so.
    t.release_all(1);
    assert!(matches!(
        t.acquire(2, &w(), &k, LockMode::Exclusive, true, true),
        Acquire::Granted {
            fresh: true,
            upgrade: true
        }
    ));
    t.release_all(2);
    assert_eq!(t.key_count(), 0);
}

#[test]
fn prepare_keeps_only_exclusive_record_locks() {
    let t = table();
    assert!(granted(&x(&t, 1, "a")));
    assert!(granted(&t.acquire(
        1,
        &w(),
        &key("b"),
        LockMode::Shared,
        true,
        true
    )));
    assert_eq!(
        t.gap_acquire(1, &w(), &at("a"), &at("z"), false, true),
        GapAcquire::Granted
    );
    let before = t.bytes_held_by(1);
    assert!(granted(&x(&t, 2, "m")));
    let Acquire::Wait(ins) = t.insert_intent(2, &w(), &key("m"), true, true) else {
        panic!("waits for 1's gap");
    };
    let Acquire::Wait(xb) = x(&t, 3, "b") else {
        panic!("waits for 1's S");
    };
    t.release_shared_and_gaps(1);
    assert_eq!(
        ins.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    assert_eq!(
        xb.wait_blocking(Some(Duration::from_secs(1))),
        WaitOutcome::Granted
    );
    assert_eq!(t.exclusive_keys(1), vec![key("a")]);
    assert_eq!(t.held_by(1), 1);
    assert!(t.gaps_held_by(1).is_empty());
    assert!(t.bytes_held_by(1) < before);
    assert!(!t.has_gaps());
    for txn in [1, 2, 3] {
        t.release_all(txn);
    }
    assert_eq!(t.key_count() + t.gap_count(), 0);
}

#[test]
fn gap_memory_is_charged_and_capped() {
    let t = Arc::new(LockTable::new(LockConfig {
        max_lock_bytes_per_txn: 400,
    }));
    // One growing interval costs one interval.
    for i in 0..50 {
        assert_eq!(
            t.gap_acquire(
                1,
                &w(),
                &at(&format!("k{i:02}")),
                &after(&format!("k{:02}", i + 1)),
                false,
                true
            ),
            GapAcquire::Granted
        );
    }
    assert_eq!(t.gaps_held_by(1).len(), 1);
    assert!(t.bytes_held_by(1) < 100);
    // Separate intervals add up to the cap.
    let mut full = false;
    for i in 0..50 {
        let k = format!("m{i:02}");
        if t.gap_acquire(2, &w(), &at(&k), &after(&k), false, true) == GapAcquire::TableFull {
            full = true;
            break;
        }
    }
    assert!(full);
    assert!(t.bytes_held_by(2) <= 400);
    t.release_all(1);
    t.release_all(2);
    assert_eq!(t.gap_count(), 0);
}

/// A key with the head every engine key has; `index` picks the index.
fn in_index(index: u16, tail: &str) -> Bytes {
    let mut v = Vec::new();
    crate::keys::KeyPrefix {
        tenant: 7,
        shard: 0,
        object: 1,
        kind: crate::keys::kind::PRIMARY,
        index,
    }
    .encode_into(&mut v);
    v.extend_from_slice(tail.as_bytes());
    Bytes::from(v)
}

#[test]
fn a_gap_in_one_index_blocks_no_other_index_and_a_wide_gap_blocks_all_it_spans() {
    let t = table();
    // Inside index 1.
    assert_eq!(
        t.gap_acquire(
            1,
            &w(),
            &Pos::at(in_index(1, "a")),
            &Pos::at(in_index(1, "z")),
            false,
            true
        ),
        GapAcquire::Granted
    );
    let blocked = |txn: TxnId, k: &Bytes| {
        assert!(granted(&t.acquire(
            txn,
            &w(),
            k,
            LockMode::Exclusive,
            true,
            true
        )));
        matches!(
            t.insert_intent(txn, &w(), k, false, true),
            Acquire::WouldWait
        )
    };
    assert!(blocked(5, &in_index(1, "m")));
    assert!(!blocked(5, &in_index(2, "m")));
    assert!(!blocked(5, &in_index(0, "m")));
    // From the middle of index 2 to the middle of index 4: indexes 2 (upper
    // part), 3 (all) and 4 (lower part).
    assert_eq!(
        t.gap_acquire(
            2,
            &w(),
            &Pos::at(in_index(2, "n")),
            &Pos::at(in_index(4, "n")),
            false,
            true
        ),
        GapAcquire::Granted
    );
    assert!(!blocked(6, &in_index(2, "b")));
    assert!(blocked(6, &in_index(2, "x")));
    assert!(blocked(6, &in_index(3, "anything")));
    assert!(blocked(6, &in_index(4, "b")));
    assert!(!blocked(6, &in_index(4, "x")));
    assert!(!blocked(6, &in_index(5, "b")));
    // Unbounded above.
    assert_eq!(
        t.gap_acquire(3, &w(), &Pos::at(in_index(9, "")), &Pos::End, false, true),
        GapAcquire::Granted
    );
    assert!(blocked(7, &in_index(9, "k")));
    assert!(blocked(7, &in_index(500, "k")));
    assert!(!blocked(7, &in_index(8, "k")));
    for txn in [1, 2, 3, 5, 6, 7] {
        t.release_all(txn);
    }
    assert_eq!(t.key_count() + t.gap_count(), 0);
    assert!(!t.has_gaps());
}

#[test]
fn a_million_row_locks_fit_the_default_lock_memory() {
    // One transaction writing a million rows of a table keyed by a BIGINT
    // (the key's head and eight bytes), each with its entries in four
    // secondary indexes (an INT and the key), stays under the default cap.
    let t = Arc::new(LockTable::new(LockConfig::default()));
    let w = w();
    for i in 0..1_000_000u64 {
        for ix in 0..5u16 {
            let mut k = in_index(ix, "").to_vec();
            if ix > 0 {
                k.extend_from_slice(&u32::try_from(i).unwrap().to_be_bytes());
            }
            k.extend_from_slice(&i.to_be_bytes());
            let k = Bytes::from(k);
            assert!(
                granted(&t.acquire(1, &w, &k, LockMode::Exclusive, false, false)),
                "row {i} index {ix} refused at {} bytes",
                t.bytes_held_by(1)
            );
        }
    }
    t.release_all(1);
}
