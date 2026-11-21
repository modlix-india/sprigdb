//! Gap, next-key and insert-intention locks, rule by rule, as clients of
//! `InnoDB` observe them at REPEATABLE READ and SERIALIZABLE: which inserts a
//! locking read blocks, which it doesn't, and what each kind of read keeps
//! locked.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

mod txn_common;

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::keys::KeyRange;
use sprig_engine::locks::LockMode;
use sprig_engine::txn::{
    AbortReason, Dir, InsertLock, InsertProbe, Isolation, LockRequest, Locked, Txn, TxnOptions,
    UnitConfig, Wait,
};
use txn_common::{Bed, bed, errno, k, mem_bed_with, v};

fn x() -> LockRequest {
    LockRequest::for_update()
}

fn s() -> LockRequest {
    LockRequest::for_share()
}

/// Commits `key = val` in its own transaction.
fn seed(unit: &Bed, key: &str, val: &str) {
    let mut t = unit.begin(TxnOptions::mariadb());
    t.lock_blocking(&k(key), LockRequest::write()).unwrap();
    t.put(k(key), v(val)).unwrap();
    t.commit().unwrap().wait().unwrap();
}

/// Rows 010, 020, 030 with values a, b, c.
fn three_rows() -> Bed {
    let b = bed();
    seed(&b, "010", "a");
    seed(&b, "020", "b");
    seed(&b, "030", "c");
    b
}

/// `[lo, hi)` over test keys.
fn range(lo: &str, hi: &str) -> KeyRange {
    KeyRange::new(k(lo).to_vec(), k(hi).to_vec())
}

/// The keys of a scan's rows, as the strings they were made from.
fn names(rows: &[(Bytes, Bytes)]) -> Vec<String> {
    rows.iter()
        .map(|(key, _)| {
            let tail = &key[sprig_engine::keys::PREFIX_LEN..key.len() - 1];
            String::from_utf8(tail.to_vec()).unwrap()
        })
        .collect()
}

/// Whether another transaction could insert `key` right now.
fn can_insert(unit: &Bed, key: &str) -> bool {
    let mut t = unit.begin(TxnOptions::mariadb());
    let r = t.lock_insert_blocking(&k(key), InsertLock::insert().with_wait(Wait::NoWait));
    match r {
        Ok(InsertProbe::Free) => true,
        Ok(InsertProbe::Exists(_)) => panic!("{key} exists"),
        Err(e) => {
            assert_eq!(errno(&e), 1205, "{e}");
            false
        }
    }
}

/// Whether another transaction could lock `key` exclusively right now.
fn can_lock(unit: &Bed, key: &str) -> bool {
    let mut t = unit.begin(TxnOptions::mariadb().with_isolation(Isolation::ReadCommitted));
    t.lock_blocking(&k(key), LockRequest::write().with_wait(Wait::NoWait))
        .is_ok()
}

/// Inserts and commits `key = val`, waiting for locks.
fn insert(unit: &Bed, key: &str, val: &str) -> Result<(), u16> {
    let mut t = unit.begin(TxnOptions::mariadb());
    match t.lock_insert_blocking(&k(key), InsertLock::insert()) {
        Ok(InsertProbe::Free) => {}
        Ok(InsertProbe::Exists(_)) => return Err(1062),
        Err(e) => return Err(errno(&e)),
    }
    t.put(k(key), v(val)).map_err(|e| errno(&e))?;
    t.commit().unwrap().wait().unwrap();
    Ok(())
}

/// Waits until `n` waits-for edges exist.
fn until_waiting(unit: &Bed, n: usize) {
    let start = Instant::now();
    while unit.locks().waits_for().len() < n {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "no waiter appeared"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

fn no_leak(unit: &Bed) {
    assert_eq!(unit.locks().key_count(), 0, "record locks left behind");
    assert_eq!(unit.locks().gap_count(), 0, "gap locks left behind");
    assert!(!unit.locks().has_gaps());
}

fn rr() -> TxnOptions {
    TxnOptions::mysql()
}

fn rc() -> TxnOptions {
    TxnOptions::mysql().with_isolation(Isolation::ReadCommitted)
}

// ----- range reads ------------------------------------------------------------

#[test]
fn a_range_read_blocks_inserts_into_the_range_and_not_outside_it() {
    for iso in [Isolation::RepeatableRead, Isolation::Serializable] {
        let b = three_rows();
        let mut t = b.begin(rr().with_isolation(iso));
        let rows = t
            .scan_lock_blocking(&range("010", "031"), Dir::Forward, usize::MAX, x())
            .unwrap();
        assert_eq!(names(&rows), ["010", "020", "030"]);
        assert_eq!(rows[1].1, v("b"));
        // Inside: between rows, and after the last row up to the range's end.
        assert!(!can_insert(&b, "015"));
        assert!(!can_insert(&b, "025"));
        assert!(!can_insert(&b, "0305"));
        // Outside: below the first bound and at or above the end bound.
        assert!(can_insert(&b, "005"));
        assert!(can_insert(&b, "031"));
        assert!(can_insert(&b, "035"));
        // Every row read is record-locked.
        for key in ["010", "020", "030"] {
            assert_eq!(
                b.locks().mode_held(t.id(), &k(key)),
                Some(LockMode::Exclusive)
            );
            assert!(!can_lock(&b, key));
        }
        // One merged interval, however many rows.
        assert_eq!(b.locks().gaps_held_by(t.id()).len(), 1);
        t.commit().unwrap().wait().unwrap();
        assert!(can_insert(&b, "015"));
        no_leak(&b);
    }
}

#[test]
fn repeating_a_locking_read_returns_the_same_rows() {
    let b = three_rows();
    let mut t = b.begin(rr());
    let r = range("010", "040");
    let first = t
        .scan_lock_blocking(&r, Dir::Forward, usize::MAX, s())
        .unwrap();
    let b2 = b.clone();
    let inserter = thread::spawn(move || insert(&b2, "015", "phantom"));
    until_waiting(&b, 1);
    // The inserter waits for this transaction's gap lock.
    assert_eq!(b.locks().waits_for().len(), 1);
    assert_eq!(b.locks().waits_for()[0].1, t.id());
    let again = t
        .scan_lock_blocking(&r, Dir::Forward, usize::MAX, s())
        .unwrap();
    assert_eq!(first, again);
    t.commit().unwrap().wait().unwrap();
    inserter.join().unwrap().unwrap();
    let mut t = b.begin(rr());
    let rows = t
        .scan_lock_blocking(&r, Dir::Forward, usize::MAX, s())
        .unwrap();
    assert_eq!(names(&rows), ["010", "015", "020", "030"]);
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_range_read_waits_for_a_row_being_inserted_into_its_range() {
    for commit in [true, false] {
        let b = three_rows();
        let mut ins = b.begin(rr());
        assert_eq!(
            ins.lock_insert_blocking(&k("015"), InsertLock::insert())
                .unwrap(),
            InsertProbe::Free
        );
        ins.put(k("015"), v("new")).unwrap();
        let b2 = b.clone();
        let reader = thread::spawn(move || {
            let mut t = b2.begin(rr());
            let rows = t
                .scan_lock_blocking(&range("010", "040"), Dir::Forward, usize::MAX, x())
                .unwrap();
            t.commit().unwrap().wait().unwrap();
            names(&rows)
        });
        until_waiting(&b, 1);
        if commit {
            ins.commit().unwrap().wait().unwrap();
            assert_eq!(reader.join().unwrap(), ["010", "015", "020", "030"]);
        } else {
            ins.rollback();
            assert_eq!(reader.join().unwrap(), ["010", "020", "030"]);
        }
        no_leak(&b);
    }
}

#[test]
fn a_range_that_starts_at_an_existing_key_locks_nothing_below_it() {
    let b = three_rows();
    let mut t = b.begin(rr());
    let rows = t
        .scan_lock_blocking(
            &KeyRange {
                start: k("020").to_vec(),
                end: Some(k("999").to_vec()),
            },
            Dir::Forward,
            usize::MAX,
            x(),
        )
        .unwrap();
    assert_eq!(names(&rows), ["020", "030"]);
    assert!(can_insert(&b, "015"));
    assert!(can_insert(&b, "019"));
    assert!(can_lock(&b, "010"));
    assert!(!can_insert(&b, "025"));
    assert!(!can_insert(&b, "500"));
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_limit_stops_the_gap_lock_at_the_last_row_returned() {
    let b = three_rows();
    let mut t = b.begin(rr());
    let r = range("000", "999");
    let rows = t.scan_lock_blocking(&r, Dir::Forward, 1, x()).unwrap();
    assert_eq!(names(&rows), ["010"]);
    assert!(!can_insert(&b, "005"));
    assert!(can_insert(&b, "015"));
    assert!(can_lock(&b, "020"));
    let rows = t.scan_lock_blocking(&r, Dir::Forward, 2, x()).unwrap();
    assert_eq!(names(&rows), ["010", "020"]);
    assert!(!can_insert(&b, "015"));
    assert!(can_insert(&b, "025"));
    assert!(can_lock(&b, "030"));
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_descending_read_locks_from_the_top_down() {
    let b = three_rows();
    let mut t = b.begin(rr());
    let r = range("000", "040");
    let rows = t.scan_lock_blocking(&r, Dir::Reverse, 1, x()).unwrap();
    assert_eq!(names(&rows), ["030"]);
    assert!(!can_insert(&b, "035"));
    assert!(can_insert(&b, "025"));
    assert!(can_insert(&b, "040"));
    let rows = t
        .scan_lock_blocking(&r, Dir::Reverse, usize::MAX, x())
        .unwrap();
    assert_eq!(names(&rows), ["030", "020", "010"]);
    assert!(!can_insert(&b, "025"));
    assert!(!can_insert(&b, "005"));
    assert!(!can_insert(&b, "000"));
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_descending_read_waits_for_a_row_being_inserted() {
    let b = three_rows();
    let mut ins = b.begin(rr());
    ins.lock_insert_blocking(&k("025"), InsertLock::insert())
        .unwrap();
    ins.put(k("025"), v("new")).unwrap();
    let b2 = b.clone();
    let reader = thread::spawn(move || {
        let mut t = b2.begin(rr());
        let rows = t
            .scan_lock_blocking(&range("000", "040"), Dir::Reverse, usize::MAX, s())
            .unwrap();
        names(&rows)
    });
    until_waiting(&b, 1);
    ins.commit().unwrap().wait().unwrap();
    assert_eq!(reader.join().unwrap(), ["030", "025", "020", "010"]);
    no_leak(&b);
}

#[test]
fn a_long_read_holds_one_gap_interval_and_a_lock_per_row() {
    let b = bed();
    {
        let mut t = b.begin(rr());
        for i in 0..600 {
            t.put(k(&format!("r{i:04}")), v("x")).unwrap();
        }
        t.commit().unwrap().wait().unwrap();
    }
    let mut t = b.begin(rr());
    let rows = t
        .scan_lock_blocking(&range("r", "s"), Dir::Forward, usize::MAX, s())
        .unwrap();
    assert_eq!(rows.len(), 600);
    assert_eq!(b.locks().held_by(t.id()), 600);
    assert_eq!(b.locks().gaps_held_by(t.id()).len(), 1);
    assert!(!can_insert(&b, "r0300x"));
    assert!(!can_insert(&b, "r9"));
    assert!(can_insert(&b, "s"));
    // The same rows the other way round, in pieces.
    let mut back = Vec::new();
    let mut hi = k("s").to_vec();
    loop {
        let part = t
            .scan_lock_blocking(
                &KeyRange::new(k("r").to_vec(), hi.clone()),
                Dir::Reverse,
                100,
                s(),
            )
            .unwrap();
        let Some((last, _)) = part.last() else { break };
        hi = last.to_vec();
        back.extend(part);
    }
    back.reverse();
    assert_eq!(rows, back);
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_read_sees_its_own_inserts_and_deletes() {
    let b = three_rows();
    let mut t = b.begin(rr());
    assert_eq!(
        t.lock_insert_blocking(&k("015"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Free
    );
    t.put(k("015"), v("mine")).unwrap();
    t.lock_blocking(&k("020"), x()).unwrap();
    t.delete(k("020")).unwrap();
    let rows = t
        .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
        .unwrap();
    assert_eq!(names(&rows), ["010", "015", "030"]);
    assert_eq!(rows[1].1, v("mine"));
    t.commit().unwrap().wait().unwrap();
    no_leak(&b);
}

#[test]
fn skip_locked_leaves_out_locked_rows_and_rows_being_inserted() {
    let b = three_rows();
    let mut other = b.begin(rr());
    other.lock_blocking(&k("020"), x()).unwrap();
    other
        .lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap();
    other.put(k("015"), v("new")).unwrap();
    let mut t = b.begin(rr());
    let started = Instant::now();
    let rows = t
        .scan_lock_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x().with_wait(Wait::SkipLocked),
        )
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(names(&rows), ["010", "030"]);
    assert_eq!(b.locks().mode_held(t.id(), &k("020")), None);
    t.rollback();
    other.rollback();
    no_leak(&b);
}

#[test]
fn nowait_fails_a_range_read_at_the_first_locked_row() {
    let b = three_rows();
    let mut other = b.begin(rr());
    other.lock_blocking(&k("020"), x()).unwrap();
    for (opts, want) in [(TxnOptions::mariadb(), 1205), (TxnOptions::mysql(), 3572)] {
        let mut t = b.begin(opts);
        let e = t
            .scan_lock_blocking(
                &range("000", "999"),
                Dir::Forward,
                usize::MAX,
                x().with_wait(Wait::NoWait),
            )
            .unwrap_err();
        assert_eq!(errno(&e), want);
        // Statement scope: the transaction is open and keeps the lock it
        // got on the first row.
        assert!(t.state().is_open);
        assert_eq!(
            b.locks().mode_held(t.id(), &k("010")),
            Some(LockMode::Exclusive)
        );
        t.rollback();
    }
    other.rollback();
    no_leak(&b);
}

// ----- one key ------------------------------------------------------------------

#[test]
fn a_row_found_by_its_key_is_locked_without_any_gap() {
    let b = three_rows();
    let mut t = b.begin(rr());
    assert_eq!(
        t.lock_get_blocking(&k("020"), x()).unwrap(),
        Locked::Row(Some(v("b")))
    );
    assert!(can_insert(&b, "015"));
    assert!(can_insert(&b, "025"));
    assert!(!can_lock(&b, "020"));
    assert!(b.locks().gaps_held_by(t.id()).is_empty());
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_missing_row_locks_the_place_it_would_be_and_readers_share_it() {
    let b = three_rows();
    let mut t1 = b.begin(rr());
    let mut t2 = b.begin(rr());
    assert_eq!(
        t1.lock_get_blocking(&k("015"), x()).unwrap(),
        Locked::Row(None)
    );
    // A second reader of the same missing row doesn't wait.
    assert_eq!(
        t2.lock_get_blocking(&k("015"), x().with_wait(Wait::NoWait))
            .unwrap(),
        Locked::Row(None)
    );
    // Neither holds a record lock on the key; both hold its gap.
    assert_eq!(b.locks().mode_held(t1.id(), &k("015")), None);
    assert!(b.locks().gap_covers(t1.id(), &k("015")));
    assert!(b.locks().gap_covers(t2.id(), &k("015")));
    // Nobody else can insert it; its neighbours are free.
    assert!(!can_insert(&b, "015"));
    assert!(can_insert(&b, "016"));
    assert!(can_insert(&b, "014"));
    // Both now insert it: the first waits for the second's gap lock, and
    // the second closes the cycle.
    let b2 = b.clone();
    let first = thread::spawn(move || {
        let r = t1.lock_insert_blocking(&k("015"), InsertLock::insert());
        if r.is_ok() {
            t1.put(k("015"), v("first")).unwrap();
            t1.commit().unwrap().wait().unwrap();
        }
        r.map_err(|e| errno(&e))
    });
    until_waiting(&b2, 1);
    let e = t2
        .lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap_err();
    assert_eq!(errno(&e), 1213);
    assert_eq!(t2.state().aborted_by, Some(AbortReason::Deadlock));
    assert_eq!(first.join().unwrap(), Ok(InsertProbe::Free));
    let report = b
        .locks()
        .latest_deadlock()
        .expect("a deadlock was recorded");
    assert_eq!(report.members.len(), 2);
    assert!(report.to_string().contains("insert intention"));
    no_leak(&b);
}

#[test]
fn a_missing_row_keeps_nothing_locked_at_read_committed() {
    let b = three_rows();
    let mut t = b.begin(rc());
    assert_eq!(
        t.lock_get_blocking(&k("015"), x()).unwrap(),
        Locked::Row(None)
    );
    assert_eq!(b.locks().held_by(t.id()), 0);
    assert!(can_insert(&b, "015"));
    // A row that is found stays locked.
    assert_eq!(
        t.lock_get_blocking(&k("020"), x()).unwrap(),
        Locked::Row(Some(v("b")))
    );
    assert!(!can_lock(&b, "020"));
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_row_this_transaction_deleted_stays_locked_when_read_again() {
    let b = three_rows();
    let mut t = b.begin(rr());
    t.lock_blocking(&k("020"), x()).unwrap();
    t.delete(k("020")).unwrap();
    assert_eq!(
        t.lock_get_blocking(&k("020"), x()).unwrap(),
        Locked::Row(None)
    );
    assert_eq!(
        b.locks().mode_held(t.id(), &k("020")),
        Some(LockMode::Exclusive)
    );
    t.rollback();
    no_leak(&b);
}

// ----- gap locks and record locks ---------------------------------------------------

#[test]
fn gap_locks_never_conflict_with_each_other_or_with_row_changes() {
    let b = three_rows();
    let r = range("000", "999");
    let mut t1 = b.begin(rr());
    let mut t2 = b.begin(rr());
    t1.lock_gap(&r).unwrap();
    t2.lock_gap(&r).unwrap();
    assert_eq!(b.locks().held_by(t1.id()), 0);
    // A third transaction changes and deletes rows inside both gaps
    // without waiting: only inserts are blocked.
    let mut t3 = b.begin(TxnOptions::mysql().with_lock_wait_seconds(0));
    t3.lock_blocking(&k("020"), x()).unwrap();
    t3.put(k("020"), v("changed")).unwrap();
    t3.lock_blocking(&k("030"), x()).unwrap();
    t3.delete(k("030")).unwrap();
    t3.commit().unwrap().wait().unwrap();
    assert!(!can_insert(&b, "015"));
    t1.rollback();
    assert!(!can_insert(&b, "015"));
    t2.rollback();
    assert!(can_insert(&b, "015"));
    no_leak(&b);
}

#[test]
fn a_gap_lock_does_nothing_at_read_committed() {
    let b = three_rows();
    let mut t = b.begin(rc());
    t.lock_gap(&range("000", "999")).unwrap();
    assert!(can_insert(&b, "015"));
    assert!(!b.locks().has_gaps());
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_write_without_the_insert_call_still_waits_for_a_gap_lock() {
    let b = three_rows();
    let mut t = b.begin(rr());
    t.scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, s())
        .unwrap();
    let b2 = b.clone();
    let writer = thread::spawn(move || {
        let mut w = b2.begin(rr());
        w.put(k("015"), v("blind")).unwrap();
        w.commit().unwrap().wait().unwrap();
    });
    until_waiting(&b, 1);
    assert!(!writer.is_finished());
    t.commit().unwrap().wait().unwrap();
    writer.join().unwrap();
    let mut r = b.begin(rr());
    assert_eq!(r.get_snapshot(&k("015")).unwrap(), Some(v("blind")));
    no_leak(&b);
}

#[test]
fn waiting_inserts_do_not_block_each_other() {
    let b = three_rows();
    let mut t = b.begin(rr());
    t.lock_gap(&range("000", "999")).unwrap();
    let (tx, rx) = mpsc::channel();
    let threads: Vec<_> = ["011", "012", "013"]
        .into_iter()
        .map(|key| {
            let b2 = b.clone();
            let tx = tx.clone();
            thread::spawn(move || {
                tx.send(insert(&b2, key, "x")).unwrap();
            })
        })
        .collect();
    until_waiting(&b, 3);
    assert!(rx.try_recv().is_err());
    t.commit().unwrap().wait().unwrap();
    for h in threads {
        h.join().unwrap();
    }
    for _ in 0..3 {
        assert_eq!(rx.recv().unwrap(), Ok(()));
    }
    no_leak(&b);
}

#[test]
fn an_insert_that_cannot_wait_fails_with_the_persona_s_error() {
    let b = three_rows();
    let mut t = b.begin(rr());
    t.lock_gap(&range("000", "999")).unwrap();
    for (opts, want) in [(TxnOptions::mariadb(), 1205), (TxnOptions::mysql(), 3572)] {
        let mut i = b.begin(opts);
        i.begin_statement();
        i.put(k("zzz"), v("earlier in the statement")).unwrap();
        let e = i
            .lock_insert_blocking(&k("015"), InsertLock::insert().with_wait(Wait::NoWait))
            .unwrap_err();
        assert_eq!(errno(&e), want);
        assert!(i.state().is_open);
        // The statement's write is undone. The insert never got as far as
        // its row, so nothing stays locked for it.
        assert_eq!(i.get_snapshot(&k("zzz")).unwrap(), None);
        assert_eq!(b.locks().mode_held(i.id(), &k("015")), None);
        i.rollback();
    }
    // WAIT 1 waits about a second, then 1205.
    let mut i = b.begin(TxnOptions::mysql());
    let started = Instant::now();
    let e = i
        .lock_insert_blocking(&k("015"), InsertLock::insert().with_wait(Wait::Seconds(1)))
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(950) && waited < Duration::from_secs(5));
    assert!(b.locks().waits_for().is_empty());
    i.rollback();
    t.rollback();
    no_leak(&b);
}

// ----- duplicate checks --------------------------------------------------------------

#[test]
fn a_duplicate_keeps_a_shared_lock_on_the_row_it_hit() {
    let b = three_rows();
    let mut t1 = b.begin(rr());
    let mut t2 = b.begin(rr());
    assert_eq!(
        t1.lock_insert_blocking(&k("020"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Exists(v("b"))
    );
    assert_eq!(
        b.locks().mode_held(t1.id(), &k("020")),
        Some(LockMode::Shared)
    );
    // A second inserter of the same key learns the same without waiting.
    assert_eq!(
        t2.lock_insert_blocking(&k("020"), InsertLock::insert().with_wait(Wait::NoWait))
            .unwrap(),
        InsertProbe::Exists(v("b"))
    );
    assert_eq!(
        b.locks().mode_held(t2.id(), &k("020")),
        Some(LockMode::Shared)
    );
    // The row can't be changed while they are open.
    assert!(!can_lock(&b, "020"));
    t1.rollback();
    assert!(!can_lock(&b, "020"));
    t2.rollback();
    assert!(can_lock(&b, "020"));
    no_leak(&b);
}

#[test]
fn replace_locks_the_existing_row_exclusively() {
    let b = three_rows();
    let mut t1 = b.begin(rr());
    assert_eq!(
        t1.lock_insert_blocking(&k("020"), InsertLock::replace())
            .unwrap(),
        InsertProbe::Exists(v("b"))
    );
    assert_eq!(
        b.locks().mode_held(t1.id(), &k("020")),
        Some(LockMode::Exclusive)
    );
    let mut t2 = b.begin(TxnOptions::mariadb());
    let e = t2
        .lock_insert_blocking(&k("020"), InsertLock::insert().with_wait(Wait::NoWait))
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    t1.put(k("020"), v("replaced")).unwrap();
    t1.commit().unwrap().wait().unwrap();
    t2.rollback();
    no_leak(&b);
}

#[test]
fn a_duplicate_of_a_row_the_transaction_holds_keeps_its_exclusive_lock() {
    let b = three_rows();
    let mut t = b.begin(rr());
    t.lock_blocking(&k("020"), x()).unwrap();
    assert_eq!(
        t.lock_insert_blocking(&k("020"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Exists(v("b"))
    );
    assert_eq!(
        b.locks().mode_held(t.id(), &k("020")),
        Some(LockMode::Exclusive)
    );
    t.rollback();
    no_leak(&b);
}

/// The three-session insert deadlock: one transaction inserts a key and
/// rolls back while two others wait to insert the same key.
#[test]
fn two_waiting_inserters_deadlock_when_the_first_rolls_back() {
    let b = three_rows();
    let mut first = b.begin(rr());
    assert_eq!(
        first
            .lock_insert_blocking(&k("015"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Free
    );
    first.put(k("015"), v("first")).unwrap();
    let waiters: Vec<_> = (0..2)
        .map(|i| {
            let b2 = b.clone();
            thread::spawn(move || insert(&b2, "015", &format!("waiter{i}")))
        })
        .collect();
    until_waiting(&b, 2);
    first.rollback();
    let mut results: Vec<_> = waiters.into_iter().map(|h| h.join().unwrap()).collect();
    results.sort();
    assert_eq!(results, [Ok(()), Err(1213)]);
    assert_eq!(
        b.locks()
            .stats
            .deadlocks
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    no_leak(&b);
}

#[test]
fn two_waiting_inserters_both_get_a_duplicate_when_the_first_commits() {
    let b = three_rows();
    let mut first = b.begin(rr());
    first
        .lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap();
    first.put(k("015"), v("first")).unwrap();
    let waiters: Vec<_> = (0..2)
        .map(|i| {
            let b2 = b.clone();
            thread::spawn(move || insert(&b2, "015", &format!("waiter{i}")))
        })
        .collect();
    until_waiting(&b, 2);
    first.commit().unwrap().wait().unwrap();
    for h in waiters {
        assert_eq!(h.join().unwrap(), Err(1062));
    }
    no_leak(&b);
}

// ----- READ COMMITTED ------------------------------------------------------------------

#[test]
fn read_committed_locks_only_the_rows_it_returns() {
    let b = three_rows();
    let mut t = b.begin(rc());
    let rows = t
        .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
        .unwrap();
    assert_eq!(names(&rows), ["010", "020", "030"]);
    assert!(can_insert(&b, "015"));
    assert!(can_insert(&b, "999"));
    assert!(!can_lock(&b, "020"));
    assert!(b.locks().gaps_held_by(t.id()).is_empty());
    // A phantom is allowed here: a committed insert shows up in the next
    // locking read.
    insert(&b, "015", "phantom").unwrap();
    let rows = t
        .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
        .unwrap();
    assert_eq!(names(&rows), ["010", "015", "020", "030"]);
    t.rollback();
    no_leak(&b);
}

#[test]
fn read_committed_unlocks_rows_that_fail_the_where_and_repeatable_read_keeps_them() {
    let keep_b = |_: &[u8], val: &[u8]| val == b"b";
    let b = three_rows();
    let mut t = b.begin(rc());
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            false,
            keep_b,
        )
        .unwrap();
    assert_eq!(names(&rows), ["020"]);
    assert!(can_lock(&b, "010"));
    assert!(!can_lock(&b, "020"));
    assert!(can_lock(&b, "030"));
    assert_eq!(b.locks().held_by(t.id()), 1);
    t.rollback();

    let mut t = b.begin(rr());
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            false,
            keep_b,
        )
        .unwrap();
    assert_eq!(names(&rows), ["020"]);
    for key in ["010", "020", "030"] {
        assert!(!can_lock(&b, key));
    }
    assert!(!can_insert(&b, "015"));
    t.rollback();
    no_leak(&b);
}

#[test]
fn the_early_unlock_spares_locks_from_earlier_statements_and_written_rows() {
    let b = three_rows();
    let mut t = b.begin(rc());
    t.begin_statement();
    t.lock_get_blocking(&k("010"), x()).unwrap();
    t.begin_statement();
    t.lock_get_blocking(&k("020"), x()).unwrap();
    t.put(k("020"), v("written")).unwrap();
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            false,
            |_: &[u8], _: &[u8]| false,
        )
        .unwrap();
    assert!(rows.is_empty());
    // 010 was locked by an earlier statement and 020 was written: both stay.
    assert!(!can_lock(&b, "010"));
    assert!(!can_lock(&b, "020"));
    assert!(can_lock(&b, "030"));
    // Outside READ COMMITTED the unlock does nothing at all.
    let mut r = b.begin(rr());
    r.lock_get_blocking(&k("030"), x()).unwrap();
    r.unlock(&k("030"));
    assert!(!can_lock(&b, "030"));
    r.rollback();
    t.rollback();
    no_leak(&b);
}

#[test]
fn a_semi_consistent_read_skips_a_locked_row_that_does_not_match() {
    let b = three_rows();
    let mut holder = b.begin(rr());
    holder.lock_blocking(&k("020"), x()).unwrap();
    holder.put(k("020"), v("uncommitted")).unwrap();

    // MySQL at READ COMMITTED: the locked row's committed version ("b")
    // fails the WHERE, so it is skipped without waiting.
    let mut t = b.begin(rc().with_lock_wait_seconds(0));
    let mut seen = Vec::new();
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            true,
            |_: &[u8], val: &[u8]| {
                seen.push(val.to_vec());
                val == b"c"
            },
        )
        .unwrap();
    assert_eq!(names(&rows), ["030"]);
    assert_eq!(seen, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(b.locks().held_by(t.id()), 1);
    t.rollback();

    // Without the flag the same read waits (here: times out at once).
    let mut t = b.begin(rc().with_lock_wait_seconds(0));
    let e = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            false,
            |_: &[u8], val: &[u8]| val == b"c",
        )
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    t.rollback();

    // MariaDB with snapshot isolation on never reads semi-consistently.
    let maria_rc = TxnOptions::mariadb()
        .with_isolation(Isolation::ReadCommitted)
        .with_lock_wait_seconds(0);
    let mut t = b.begin(maria_rc);
    let e = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            true,
            |_: &[u8], val: &[u8]| val == b"c",
        )
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    t.rollback();

    // With snapshot isolation off, MariaDB does.
    let mut opts = maria_rc;
    opts.snapshot_isolation = false;
    let mut t = b.begin(opts);
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            true,
            |_: &[u8], val: &[u8]| val == b"c",
        )
        .unwrap();
    assert_eq!(names(&rows), ["030"]);
    t.rollback();

    // REPEATABLE READ ignores the flag.
    let mut t = b.begin(rr().with_lock_wait_seconds(0));
    let e = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            true,
            |_: &[u8], val: &[u8]| val == b"c",
        )
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    t.rollback();
    holder.rollback();
    no_leak(&b);
}

#[test]
fn a_semi_consistent_read_waits_for_a_locked_row_that_matches_and_judges_it_again() {
    for (new_value, want) in [("b2", vec!["020"]), ("other", vec![])] {
        let b = three_rows();
        let mut holder = b.begin(rr());
        holder.lock_blocking(&k("020"), x()).unwrap();
        holder.put(k("020"), v(new_value)).unwrap();
        let b2 = b.clone();
        let reader = thread::spawn(move || {
            let mut t = b2.begin(rc());
            let rows = t
                .scan_lock_where_blocking(
                    &range("000", "999"),
                    Dir::Forward,
                    usize::MAX,
                    x(),
                    true,
                    |_: &[u8], val: &[u8]| val.starts_with(b"b"),
                )
                .unwrap();
            let held = b2.locks().held_by(t.id());
            t.rollback();
            (names(&rows), held)
        });
        // The committed version ("b") matches, so the reader waits.
        until_waiting(&b, 1);
        holder.commit().unwrap().wait().unwrap();
        let (rows, held) = reader.join().unwrap();
        assert_eq!(rows, want);
        // A row that no longer matches is unlocked again.
        assert_eq!(held, want.len());
        no_leak(&b);
    }
}

// ----- snapshot isolation (MariaDB's 1020) on range reads -------------------------------

fn open_view(t: &mut Txn) {
    t.get_snapshot(&k("none")).unwrap();
}

#[test]
fn a_range_read_fails_with_1020_for_a_row_changed_since_the_view() {
    type Change = fn(&Bed);
    let changes: [(&str, Change); 3] = [
        ("update", |b| seed(b, "020", "changed")),
        ("delete", |b| {
            let mut t = b.begin(TxnOptions::mariadb());
            t.lock_blocking(&k("020"), LockRequest::write()).unwrap();
            t.delete(k("020")).unwrap();
            t.commit().unwrap().wait().unwrap();
        }),
        ("insert", |b| insert(b, "015", "new").unwrap()),
    ];
    for (what, change) in changes {
        let b = three_rows();
        let mut t = b.begin(TxnOptions::mariadb());
        open_view(&mut t);
        change(&b);
        let e = t
            .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
            .unwrap_err();
        assert_eq!(errno(&e), 1020, "{what}");
        assert_eq!(t.state().aborted_by, Some(AbortReason::CheckRead), "{what}");
        no_leak(&b);

        // MySQL, and MariaDB with the check off, read the latest rows.
        let mut off = TxnOptions::mariadb();
        off.snapshot_isolation = false;
        for opts in [TxnOptions::mysql(), off] {
            let b = three_rows();
            let mut t = b.begin(opts);
            open_view(&mut t);
            change(&b);
            let rows = t
                .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
                .unwrap();
            assert_eq!(
                rows.len(),
                if what == "update" {
                    3
                } else if what == "delete" {
                    2
                } else {
                    4
                }
            );
            t.rollback();
            no_leak(&b);
        }
    }
}

#[test]
fn a_range_read_that_waited_for_a_writer_fails_with_1020_when_it_commits() {
    let b = three_rows();
    let mut writer = b.begin(TxnOptions::mariadb());
    writer.lock_blocking(&k("020"), x()).unwrap();
    writer.put(k("020"), v("changed")).unwrap();
    let b2 = b.clone();
    let reader = thread::spawn(move || {
        let mut t = b2.begin(TxnOptions::mariadb());
        open_view(&mut t);
        t.scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
            .map(|rows| rows.len())
            .map_err(|e| errno(&e))
    });
    until_waiting(&b, 1);
    writer.commit().unwrap().wait().unwrap();
    assert_eq!(reader.join().unwrap(), Err(1020));
    no_leak(&b);
}

#[test]
fn no_1020_on_a_gap_lock_or_a_duplicate_check() {
    let b = three_rows();
    let mut t = b.begin(TxnOptions::mariadb());
    open_view(&mut t);
    seed(&b, "020", "changed");
    // A gap-only lock looks at no row.
    t.lock_gap(&range("000", "999")).unwrap();
    // The duplicate check reports the row, not 1020, unless asked.
    assert_eq!(
        t.lock_insert_blocking(&k("020"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Exists(v("changed"))
    );
    let e = t
        .lock_insert_blocking(&k("020"), InsertLock::insert().with_snapshot_check())
        .unwrap_err();
    assert_eq!(errno(&e), 1020);
    no_leak(&b);
}

// ----- limits ---------------------------------------------------------------------------

#[test]
fn gap_locks_count_against_lock_memory() {
    let cfg = UnitConfig {
        max_lock_bytes_per_txn: 2000,
        ..UnitConfig::default()
    };
    let b = mem_bed_with(cfg, Duration::ZERO);
    let mut t = b.begin(rr());
    let mut failed = None;
    for i in 0..100 {
        // Separate missing rows: separate intervals.
        match t.lock_get_blocking(&k(&format!("m{i:03}")), s()) {
            Ok(_) => {}
            Err(e) => {
                failed = Some((i, errno(&e)));
                break;
            }
        }
    }
    let (at, code) = failed.expect("the cap was reached");
    assert_eq!(code, 1206);
    assert!(at > 5, "failed after {at} locks");
    assert_eq!(t.state().aborted_by, Some(AbortReason::LockTableFull));
    no_leak(&b);
}

#[test]
fn a_committed_transaction_leaves_no_gap_behind_with_locks_held_to_durability() {
    let cfg = UnitConfig {
        early_lock_release: false,
        ..UnitConfig::default()
    };
    let b = mem_bed_with(cfg, Duration::from_millis(20));
    seed(&b, "010", "a");
    let mut t = b.begin(rr());
    t.scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
        .unwrap();
    t.put(k("010"), v("a2")).unwrap();
    let ticket = t.commit().unwrap();
    // The commit is queued; its locks go when it is durable.
    ticket.wait().unwrap();
    let start = Instant::now();
    while b.locks().has_gaps() {
        assert!(start.elapsed() < Duration::from_secs(5));
        thread::sleep(Duration::from_millis(2));
    }
    assert!(can_insert(&b, "015"));
    no_leak(&b);
}

// ----- waits on the runtime ---------------------------------------------------------------

/// The async calls park the task, not the thread: on a one-thread runtime a
/// waiting insert and a waiting range read make progress when the holder,
/// another task on the same thread, commits.
#[tokio::test(flavor = "current_thread")]
async fn async_waits_park_the_task_and_resume_when_the_holder_ends() {
    let b = three_rows();
    let mut holder = b.begin(rr());
    let rows = holder
        .scan_lock(&range("000", "025"), Dir::Forward, usize::MAX, x())
        .await
        .unwrap();
    assert_eq!(names(&rows), ["010", "020"]);

    let b2 = b.clone();
    let inserter = tokio::spawn(async move {
        let mut t = b2.begin(rr());
        let probe = t
            .lock_insert(&k("015"), InsertLock::insert())
            .await
            .unwrap();
        t.put(k("015"), v("new")).unwrap();
        t.commit().unwrap().wait_async().await.unwrap();
        probe
    });
    let b3 = b.clone();
    let reader = tokio::spawn(async move {
        let mut t = b3.begin(rr());
        let rows = t
            .scan_lock_where(
                &range("000", "999"),
                Dir::Forward,
                usize::MAX,
                s(),
                false,
                |_: &[u8], val: &[u8]| val != b"c",
            )
            .await
            .unwrap();
        let got = t.lock_get(&k("030"), s()).await.unwrap();
        t.commit().unwrap().wait_async().await.unwrap();
        (names(&rows), got)
    });
    // Both are waiting for the holder; this task still runs.
    while b.locks().waits_for().len() < 2 {
        tokio::task::yield_now().await;
    }
    assert!(!inserter.is_finished() && !reader.is_finished());
    holder.commit().unwrap().wait_async().await.unwrap();
    assert_eq!(inserter.await.unwrap(), InsertProbe::Free);
    let (rows, got) = reader.await.unwrap();
    // Whichever of the two got to key 015 first: the reader either waited
    // for the insert and returns it, or closed the gap first and the insert
    // waited for the reader.
    assert!(
        rows == ["010", "020"] || rows == ["010", "015", "020"],
        "{rows:?}"
    );
    assert_eq!(got, Locked::Row(Some(v("c"))));
    no_leak(&b);
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_an_async_wait_withdraws_the_request() {
    let b = three_rows();
    let mut holder = b.begin(rr());
    holder.lock_gap(&range("000", "999")).unwrap();
    holder.lock(&k("020"), x()).await.unwrap();
    let mut t = b.begin(rr());
    // A killed statement: its future is dropped mid-wait.
    let r = tokio::time::timeout(
        Duration::from_millis(200),
        t.lock_insert(&k("015"), InsertLock::insert()),
    )
    .await;
    assert!(r.is_err(), "still waiting when dropped");
    assert!(b.locks().waits_for().is_empty());
    let r = tokio::time::timeout(
        Duration::from_millis(200),
        t.scan_lock(&range("000", "999"), Dir::Forward, usize::MAX, x()),
    )
    .await;
    assert!(r.is_err());
    assert!(b.locks().waits_for().is_empty());
    // The transaction is still usable, and its own timeouts run on the
    // runtime: WAIT 1 gives 1205.
    let e = t
        .lock_get(&k("020"), x().with_wait(Wait::Seconds(1)))
        .await
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    assert!(t.state().is_open);
    holder.rollback();
    assert_eq!(
        t.lock_insert(&k("015"), InsertLock::insert())
            .await
            .unwrap(),
        InsertProbe::Free
    );
    t.rollback();
    no_leak(&b);
}

#[test]
fn rollback_on_timeout_applies_to_an_insert_s_wait_too() {
    let b = three_rows();
    let mut holder = b.begin(rr());
    holder.lock_gap(&range("000", "999")).unwrap();
    let mut opts = TxnOptions::mariadb().with_lock_wait_seconds(0);
    opts.rollback_on_timeout = true;
    let mut t = b.begin(opts);
    t.put(k("zzz"), v("earlier")).unwrap();
    let e = t
        .lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    assert_eq!(
        t.state().aborted_by,
        Some(AbortReason::Timeout {
            rollback_on_timeout: true
        })
    );
    assert_eq!(b.locks().held_by(t.id()), 0);
    holder.rollback();
    no_leak(&b);
}

// ----- nobody waits for a waiting insert ------------------------------------------------

#[test]
fn a_waiting_insert_blocks_neither_the_gap_s_owner_nor_other_readers() {
    let b = three_rows();
    let mut owner = b.begin(rr());
    assert_eq!(
        owner.lock_get_blocking(&k("015"), x()).unwrap(),
        Locked::Row(None)
    );
    let b2 = b.clone();
    let waiter = thread::spawn(move || insert(&b2, "015", "late"));
    until_waiting(&b, 1);
    // The waiting inserter holds nothing on the key.
    assert!(can_lock(&b, "015"));
    // A third session's read of the missing row doesn't wait either.
    let mut third = b.begin(rr().with_lock_wait_seconds(0));
    assert_eq!(
        third.lock_get_blocking(&k("015"), x()).unwrap(),
        Locked::Row(None)
    );
    third.rollback();
    // The owner inserts the row itself, without a deadlock.
    assert_eq!(
        owner
            .lock_insert_blocking(&k("015"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Free
    );
    owner.put(k("015"), v("owner")).unwrap();
    owner.commit().unwrap().wait().unwrap();
    // The waiter now finds the row there.
    assert_eq!(waiter.join().unwrap(), Err(1062));
    assert_eq!(
        b.locks()
            .stats
            .deadlocks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    no_leak(&b);
}

#[test]
fn a_blind_write_gives_its_lock_back_while_it_waits_for_a_gap() {
    let b = three_rows();
    let mut owner = b.begin(rr());
    owner
        .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, s())
        .unwrap();
    let b2 = b.clone();
    let writer = thread::spawn(move || {
        let mut w = b2.begin(rr());
        w.put(k("015"), v("blind")).unwrap();
        w.commit().unwrap().wait().unwrap();
    });
    until_waiting(&b, 1);
    assert!(can_lock(&b, "015"));
    owner.rollback();
    writer.join().unwrap();
    let mut r = b.begin(rr());
    assert_eq!(r.get_snapshot(&k("015")).unwrap(), Some(v("blind")));
    no_leak(&b);
}

#[test]
fn the_insert_check_is_good_for_one_statement_only() {
    let b = three_rows();
    let mut i = b.begin(rr().with_lock_wait_seconds(0));
    i.begin_statement();
    assert_eq!(
        i.lock_insert_blocking(&k("015"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Free
    );
    // The statement ends without writing; the key's lock stays.
    i.begin_statement();
    let mut g = b.begin(rr());
    g.lock_gap(&range("000", "999")).unwrap();
    // A later statement's bare write is checked again and has to wait.
    let e = i.put(k("015"), v("x")).unwrap_err();
    assert_eq!(errno(&e), 1205);
    g.rollback();
    i.begin_statement();
    i.put(k("015"), v("x")).unwrap();
    i.commit().unwrap().wait().unwrap();
    no_leak(&b);
}

#[test]
fn a_range_read_keeps_no_lock_on_a_key_that_turned_out_to_be_no_row() {
    let b = three_rows();
    let mut ins = b.begin(rr());
    ins.lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap();
    ins.put(k("015"), v("never")).unwrap();
    let b2 = b.clone();
    let (tx, rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let reader = thread::spawn(move || {
        let mut t = b2.begin(rr());
        let rows = t
            .scan_lock_blocking(&range("000", "999"), Dir::Forward, usize::MAX, x())
            .unwrap();
        tx.send((names(&rows), t.id())).unwrap();
        done_rx.recv().unwrap();
        t.rollback();
    });
    until_waiting(&b, 1);
    ins.rollback();
    let (rows, id) = rx.recv().unwrap();
    assert_eq!(rows, ["010", "020", "030"]);
    // No record lock on the key that never became a row; its place is
    // still closed to inserts by the gap lock.
    assert_eq!(b.locks().mode_held(id, &k("015")), None);
    assert!(b.locks().gap_covers(id, &k("015")));
    assert!(!can_insert(&b, "015"));
    let mut other = b.begin(rr().with_lock_wait_seconds(0));
    assert_eq!(
        other.lock_get_blocking(&k("015"), x()).unwrap(),
        Locked::Row(None)
    );
    other.rollback();
    done_tx.send(()).unwrap();
    reader.join().unwrap();
    no_leak(&b);
}

#[test]
fn a_rejected_row_keeps_a_lock_the_same_statement_took_earlier() {
    let b = three_rows();
    let mut t = b.begin(rc());
    t.begin_statement();
    // The statement locks row 020 and keeps it, then visits it again in a
    // read whose WHERE rejects it.
    assert_eq!(
        t.lock_get_blocking(&k("020"), x()).unwrap(),
        Locked::Row(Some(v("b")))
    );
    let rows = t
        .scan_lock_where_blocking(
            &range("000", "999"),
            Dir::Forward,
            usize::MAX,
            x(),
            false,
            |_: &[u8], _: &[u8]| false,
        )
        .unwrap();
    assert!(rows.is_empty());
    assert!(!can_lock(&b, "020"));
    assert!(can_lock(&b, "010"));
    t.rollback();
    no_leak(&b);
}

/// Whether another transaction could insert `key` right now, at READ
/// COMMITTED or not (`false`: it would wait).
fn insert_now(unit: &Bed, key: &str) -> bool {
    can_insert(unit, key)
}

#[test]
fn an_insert_undone_by_its_statement_frees_the_key_on_mariadb() {
    for iso in [Isolation::RepeatableRead, Isolation::ReadCommitted] {
        let b = three_rows();
        let mut t = b.begin(TxnOptions::mariadb().with_isolation(iso));
        t.begin_statement();
        assert_eq!(
            t.lock_insert_blocking(&k("015"), InsertLock::insert())
                .unwrap(),
            InsertProbe::Free
        );
        t.put(k("015"), v("x")).unwrap();
        // The statement's second row is a duplicate: it fails, its first
        // row is undone.
        assert!(matches!(
            t.lock_insert_blocking(&k("020"), InsertLock::insert())
                .unwrap(),
            InsertProbe::Exists(_)
        ));
        assert!(!insert_now(&b, "015"));
        t.rollback_statement();
        // The undone row's key is free; the duplicate's shared lock stays.
        assert!(insert_now(&b, "015"));
        assert_eq!(b.locks().mode_held(t.id(), &k("015")), None);
        assert_eq!(
            b.locks().mode_held(t.id(), &k("020")),
            Some(LockMode::Shared)
        );
        assert!(!can_lock(&b, "020"));
        t.rollback();
        no_leak(&b);
    }
}

#[test]
fn an_insert_undone_on_mysql_keeps_the_place_closed_at_repeatable_read_only() {
    for (iso, closed) in [
        (Isolation::RepeatableRead, true),
        (Isolation::Serializable, true),
        (Isolation::ReadCommitted, false),
    ] {
        let b = three_rows();
        let mut t = b.begin(TxnOptions::mysql().with_isolation(iso));
        t.begin_statement();
        assert_eq!(
            t.lock_insert_blocking(&k("015"), InsertLock::insert())
                .unwrap(),
            InsertProbe::Free
        );
        t.put(k("015"), v("x")).unwrap();
        t.rollback_statement();
        // No record lock is left; the place stays closed to inserts where
        // the level has gap locks.
        assert_eq!(b.locks().mode_held(t.id(), &k("015")), None);
        assert_eq!(insert_now(&b, "015"), !closed, "{iso:?}");
        assert!(insert_now(&b, "016"));
        // A locking read of the missing key by another transaction doesn't
        // wait (a gap lock is no record lock).
        let mut other = b.begin(TxnOptions::mysql().with_lock_wait_seconds(0));
        assert_eq!(
            other.lock_get_blocking(&k("015"), x()).unwrap(),
            Locked::Row(None)
        );
        other.rollback();
        // The transaction itself may insert the key again.
        t.begin_statement();
        assert_eq!(
            t.lock_insert_blocking(&k("015"), InsertLock::insert())
                .unwrap(),
            InsertProbe::Free
        );
        t.put(k("015"), v("y")).unwrap();
        t.commit().unwrap().wait().unwrap();
        no_leak(&b);
    }
}

#[test]
fn rollback_to_a_savepoint_frees_inserted_keys_and_keeps_other_locks() {
    let b = three_rows();
    let mut t = b.begin(TxnOptions::mariadb());
    t.begin_statement();
    t.lock_insert_blocking(&k("005"), InsertLock::insert())
        .unwrap();
    t.put(k("005"), v("kept")).unwrap();
    let sp = t.savepoint();
    t.begin_statement();
    t.lock_insert_blocking(&k("015"), InsertLock::insert())
        .unwrap();
    t.put(k("015"), v("undone")).unwrap();
    t.begin_statement();
    // A row that exists and is changed after the savepoint stays locked.
    t.lock_blocking(&k("020"), LockRequest::write()).unwrap();
    t.put(k("020"), v("changed")).unwrap();
    t.rollback_to(sp);
    assert!(insert_now(&b, "015"));
    assert!(!insert_now(&b, "005"));
    assert!(!can_lock(&b, "020"));
    assert_eq!(t.read_current(&k("020")).unwrap(), Some(v("b")));
    assert_eq!(t.read_current(&k("005")).unwrap(), Some(v("kept")));
    t.commit().unwrap().wait().unwrap();
    no_leak(&b);
}

#[test]
fn an_undone_insert_keeps_a_lock_the_transaction_held_before() {
    let b = three_rows();
    let mut t = b.begin(TxnOptions::mariadb());
    // The transaction deleted the row earlier: the key's lock is its own,
    // from before the insert.
    t.begin_statement();
    t.lock_blocking(&k("020"), LockRequest::write()).unwrap();
    t.delete(k("020")).unwrap();
    t.begin_statement();
    assert_eq!(
        t.lock_insert_blocking(&k("020"), InsertLock::insert())
            .unwrap(),
        InsertProbe::Free
    );
    t.put(k("020"), v("again")).unwrap();
    t.rollback_statement();
    assert!(!can_lock(&b, "020"));
    assert_eq!(t.read_current(&k("020")).unwrap(), None);
    t.rollback();
    no_leak(&b);
}
