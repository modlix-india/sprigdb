//! Lock and isolation errors with their exact codes and rollback scopes
//! (`InnoDB` as clients observe it, MariaDB 11.8 and MySQL 8.4 personas).

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

use std::thread;
use std::time::{Duration, Instant};

use sprig_engine::locks::LockMode;
use sprig_engine::txn::{
    AbortReason, Interrupt, Isolation, LockRequest, Locked, TxnOptions, UnitConfig, Wait,
};
use txn_common::{Bed, bed, errno, k, mem_bed_with, sqlstate, v};

fn x() -> LockRequest {
    LockRequest::for_update()
}

/// Commits `key = val` in its own transaction.
fn seed(unit: &Bed, key: &str, val: &str) {
    let mut t = unit.begin(TxnOptions::mariadb());
    t.lock_blocking(&k(key), LockRequest::write()).unwrap();
    t.put(k(key), v(val)).unwrap();
    t.commit().unwrap().wait().unwrap();
}

/// Waits until `n` lock requests are queued.
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

#[test]
fn nowait_is_1205_on_mariadb_and_3572_on_mysql_statement_scope() {
    let b = bed();
    seed(&b, "a", "1");
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();

    for (opts, want) in [(TxnOptions::mariadb(), 1205), (TxnOptions::mysql(), 3572)] {
        let mut t = b.begin(opts);
        t.lock_blocking(&k("b"), LockRequest::write()).unwrap();
        t.put(k("b"), v("kept")).unwrap();
        t.begin_statement();
        t.lock_blocking(&k("c"), LockRequest::write()).unwrap();
        t.put(k("c"), v("undone")).unwrap();
        let e = t
            .lock_get_blocking(&k("a"), x().with_wait(Wait::NoWait))
            .unwrap_err();
        assert_eq!(errno(&e), want);
        assert_eq!(&sqlstate(&e), b"HY000");
        // Statement scope: the transaction stays open, the statement's write
        // is undone, earlier writes and every lock stay.
        assert!(t.state().is_open);
        assert_eq!(t.get_snapshot(&k("c")).unwrap(), None);
        assert_eq!(t.get_snapshot(&k("b")).unwrap(), Some(v("kept")));
        assert_eq!(
            b.locks().mode_held(t.id(), &k("c")),
            Some(LockMode::Exclusive)
        );
        t.rollback();
    }
    holder.rollback();
}

#[test]
fn skip_locked_skips_silently() {
    let b = bed();
    seed(&b, "a", "1");
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();
    let mut t = b.begin(TxnOptions::mariadb());
    assert_eq!(
        t.lock_get_blocking(&k("a"), x().with_wait(Wait::SkipLocked))
            .unwrap(),
        Locked::Skipped
    );
    assert!(t.state().is_open);
    holder.rollback();
    assert_eq!(
        t.lock_get_blocking(
            &k("a"),
            LockRequest::for_share().with_wait(Wait::SkipLocked)
        )
        .unwrap(),
        Locked::Row(Some(v("1")))
    );
}

#[test]
fn timeout_is_1205_rolls_back_the_statement_and_keeps_its_locks() {
    let b = bed();
    seed(&b, "a", "1");
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();

    let mut t = b.begin(TxnOptions::mariadb().with_lock_wait_seconds(1));
    t.begin_statement();
    t.lock_blocking(&k("s1"), LockRequest::write()).unwrap();
    t.put(k("s1"), v("first statement")).unwrap();
    t.begin_statement();
    t.lock_blocking(&k("s2"), LockRequest::write()).unwrap();
    t.put(k("s2"), v("second statement")).unwrap();
    let started = Instant::now();
    let e = t.lock_get_blocking(&k("a"), x()).unwrap_err();
    let waited = started.elapsed();
    assert_eq!(errno(&e), 1205);
    assert!(e.retryable);
    assert!(waited >= Duration::from_millis(950) && waited < Duration::from_secs(5));
    assert!(t.state().is_open);
    assert_eq!(t.state().aborted_by, None);
    assert_eq!(
        t.get_snapshot(&k("s1")).unwrap(),
        Some(v("first statement"))
    );
    assert_eq!(t.get_snapshot(&k("s2")).unwrap(), None);
    // The timed-out statement's lock on s2 is kept: another transaction
    // can't take it.
    let mut other = b.begin(TxnOptions::mariadb());
    let e = other
        .lock_get_blocking(&k("s2"), x().with_wait(Wait::NoWait))
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    // The holder never saw the waiter's request: it was withdrawn.
    assert!(b.locks().waits_for().is_empty());
    t.rollback();
    other.rollback();
    holder.rollback();
    assert_eq!(b.locks().key_count(), 0);
}

#[test]
fn rollback_on_timeout_ends_the_whole_transaction() {
    let b = bed();
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();
    let mut opts = TxnOptions::mariadb().with_lock_wait_seconds(0);
    opts.rollback_on_timeout = true;
    let mut t = b.begin(opts);
    t.lock_blocking(&k("mine"), LockRequest::write()).unwrap();
    t.put(k("mine"), v("x")).unwrap();
    let e = t.lock_get_blocking(&k("a"), x()).unwrap_err();
    assert_eq!(errno(&e), 1205);
    assert_eq!(
        t.state().aborted_by,
        Some(AbortReason::Timeout {
            rollback_on_timeout: true
        })
    );
    assert!(!t.state().is_open);
    // Its locks are gone.
    assert_eq!(b.locks().mode_held(t.id(), &k("mine")), None);
    assert!(t.get_snapshot(&k("mine")).is_err());
    holder.rollback();
}

#[test]
fn deadlock_requester_is_the_victim_on_equal_weight() {
    let b = bed();
    seed(&b, "1", "one");
    seed(&b, "2", "two");
    let unit = b.clone();
    let mut a = unit.begin(TxnOptions::mariadb());
    let mut bt = unit.begin(TxnOptions::mariadb());
    a.lock_blocking(&k("1"), x()).unwrap();
    bt.lock_blocking(&k("2"), x()).unwrap();
    let a_id = a.id();
    let h = thread::spawn(move || {
        let r = a.lock_get_blocking(&k("2"), x());
        (r, a)
    });
    until_waiting(&unit, 1);
    let e = bt.lock_get_blocking(&k("1"), x()).unwrap_err();
    assert_eq!(errno(&e), 1213);
    assert_eq!(&sqlstate(&e), b"40001");
    assert!(e.retryable);
    assert_eq!(bt.state().aborted_by, Some(AbortReason::Deadlock));
    // The victim's locks went, so the other transaction proceeds.
    let (r, a) = h.join().unwrap();
    assert_eq!(r.unwrap(), Locked::Row(Some(v("two"))));
    assert_eq!(
        unit.locks().mode_held(a_id, &k("2")),
        Some(LockMode::Exclusive)
    );
    // The report names both waits and the transaction rolled back.
    let report = unit.locks().latest_deadlock().expect("recorded");
    assert_eq!(report.victim, bt.id());
    assert_eq!(report.members.len(), 2);
    let victim = report.members.iter().find(|m| m.txn == bt.id()).unwrap();
    assert_eq!(victim.waits_for_key, k("1"));
    assert_eq!(victim.blocked_by, vec![a_id]);
    assert_eq!(victim.mode, LockMode::Exclusive);
    let other = report.members.iter().find(|m| m.txn == a_id).unwrap();
    assert_eq!(other.waits_for_key, k("2"));
    assert_eq!(other.locks_held, 1);
    let text = report.to_string();
    assert!(text.contains("*** (1) TRANSACTION:"), "{text}");
    assert!(
        text.contains("*** (2) WAITING FOR THIS LOCK TO BE GRANTED:"),
        "{text}"
    );
    assert!(
        text.contains("lock_mode X locks rec but not gap waiting"),
        "{text}"
    );
    assert!(text.contains("*** WE ROLL BACK TRANSACTION ("), "{text}");
    assert!(
        unit.locks()
            .stats
            .deadlocks
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    drop(a);
}

#[test]
fn deadlock_victim_is_the_lighter_transaction() {
    let b = bed();
    let unit = b.clone();
    let mut light = unit.begin(TxnOptions::mariadb());
    let mut heavy = unit.begin(TxnOptions::mariadb());
    light.lock_blocking(&k("1"), x()).unwrap();
    heavy.lock_blocking(&k("2"), x()).unwrap();
    for i in 0..3 {
        let key = k(&format!("w{i}"));
        heavy.lock_blocking(&key, LockRequest::write()).unwrap();
        heavy.put(key, v("x")).unwrap();
    }
    let h = thread::spawn(move || {
        let r = light.lock_get_blocking(&k("2"), x());
        (r, light)
    });
    until_waiting(&unit, 1);
    // The heavy requester closes the cycle; the light waiter is the victim.
    let r = heavy.lock_get_blocking(&k("1"), x());
    assert_eq!(r.unwrap(), Locked::Row(None));
    let (r, light) = h.join().unwrap();
    assert_eq!(errno(&r.unwrap_err()), 1213);
    assert_eq!(light.state().aborted_by, Some(AbortReason::Deadlock));
    heavy.commit().unwrap().wait().unwrap();
}

#[test]
fn deadlock_detection_off_leaves_it_to_the_timeout() {
    let b = bed();
    let unit = b.clone();
    let mut opts = TxnOptions::mariadb().with_lock_wait_seconds(1);
    opts.deadlock_detect = false;
    let mut a = unit.begin(opts);
    let mut bt = unit.begin(opts);
    a.lock_blocking(&k("1"), x()).unwrap();
    bt.lock_blocking(&k("2"), x()).unwrap();
    let h = thread::spawn(move || {
        let r = a.lock_get_blocking(&k("2"), x());
        (r, a)
    });
    until_waiting(&unit, 1);
    let e1 = bt.lock_get_blocking(&k("1"), x()).unwrap_err();
    let (r, _a) = h.join().unwrap();
    assert_eq!(errno(&e1), 1205);
    assert_eq!(errno(&r.unwrap_err()), 1205);
}

#[test]
fn three_way_cycle_is_found() {
    let b = bed();
    let unit = b.clone();
    let mut t: Vec<_> = (0..3).map(|_| unit.begin(TxnOptions::mariadb())).collect();
    for (i, tx) in t.iter_mut().enumerate() {
        tx.lock_blocking(&k(&i.to_string()), x()).unwrap();
    }
    let t2 = t.pop().unwrap();
    let t1 = t.pop().unwrap();
    let t0 = t.pop().unwrap();
    let mut hs = Vec::new();
    for (mut tx, want) in [(t0, "1"), (t1, "2")] {
        hs.push(thread::spawn(move || {
            let r = tx.lock_get_blocking(&k(want), x());
            (r.map(|_| ()), tx)
        }));
    }
    until_waiting(&unit, 2);
    let mut t2 = t2;
    let e = t2.lock_get_blocking(&k("0"), x()).unwrap_err();
    assert_eq!(errno(&e), 1213);
    // t2's release lets t1 through, then t1's commit lets t0 through.
    for h in hs.into_iter().rev() {
        let (r, tx) = h.join().unwrap();
        r.unwrap();
        tx.commit().unwrap().wait().unwrap();
    }
}

#[test]
fn check_read_1020_when_a_committed_writer_is_newer_than_the_view() {
    let b = bed();
    seed(&b, "a", "1");
    let mut t = b.begin(TxnOptions::mariadb());
    assert_eq!(t.get_snapshot(&k("a")).unwrap(), Some(v("1"))); // opens the view
    t.lock_blocking(&k("other"), x()).unwrap();
    seed(&b, "a", "2");
    let started = Instant::now();
    let e = t.lock_get_blocking(&k("a"), x()).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "must not wait"
    );
    assert_eq!(errno(&e), 1020);
    assert_eq!(&sqlstate(&e), b"HY000");
    assert!(!e.retryable);
    assert_eq!(t.state().aborted_by, Some(AbortReason::CheckRead));
    assert_eq!(b.locks().mode_held(t.id(), &k("other")), None);
}

#[test]
fn check_read_1020_after_waiting_for_an_active_writer_that_commits() {
    let b = bed();
    seed(&b, "a", "1");
    let unit = b.clone();
    let mut w = unit.begin(TxnOptions::mariadb());
    w.lock_blocking(&k("a"), x()).unwrap();
    w.put(k("a"), v("2")).unwrap();
    let mut t = unit.begin(TxnOptions::mariadb());
    t.get_snapshot(&k("a")).unwrap();
    let h = thread::spawn(move || {
        let r = t.lock_get_blocking(&k("a"), x());
        (r, t)
    });
    until_waiting(&unit, 1);
    w.commit().unwrap().wait().unwrap();
    let (r, t) = h.join().unwrap();
    assert_eq!(errno(&r.unwrap_err()), 1020);
    assert_eq!(t.state().aborted_by, Some(AbortReason::CheckRead));
}

#[test]
fn check_read_passes_when_the_active_writer_rolls_back() {
    let b = bed();
    seed(&b, "a", "1");
    let unit = b.clone();
    let mut w = unit.begin(TxnOptions::mariadb());
    w.lock_blocking(&k("a"), x()).unwrap();
    w.put(k("a"), v("2")).unwrap();
    let mut t = unit.begin(TxnOptions::mariadb());
    t.get_snapshot(&k("a")).unwrap();
    let h = thread::spawn(move || {
        let r = t.lock_get_blocking(&k("a"), x());
        (r, t)
    });
    until_waiting(&unit, 1);
    w.rollback();
    let (r, _t) = h.join().unwrap();
    assert_eq!(r.unwrap(), Locked::Row(Some(v("1"))));
}

#[test]
fn no_1020_without_a_view_for_mysql_or_at_read_committed() {
    for (opts, plain_read_first) in [
        (TxnOptions::mariadb(), false),
        (TxnOptions::mysql(), true),
        (
            TxnOptions::mariadb().with_isolation(Isolation::ReadCommitted),
            true,
        ),
        (
            {
                let mut o = TxnOptions::mariadb();
                o.snapshot_isolation = false;
                o
            },
            true,
        ),
    ] {
        let b = bed();
        seed(&b, "a", "1");
        let mut t = b.begin(opts);
        if plain_read_first {
            assert_eq!(t.get_snapshot(&k("a")).unwrap(), Some(v("1")));
        }
        seed(&b, "a", "2");
        // A current read: the latest committed version, not the snapshot.
        assert_eq!(
            t.lock_get_blocking(&k("a"), x()).unwrap(),
            Locked::Row(Some(v("2")))
        );
        if plain_read_first && opts.isolation == Isolation::RepeatableRead {
            // The snapshot is unchanged for plain reads.
            assert_eq!(t.get_snapshot(&k("a")).unwrap(), Some(v("1")));
        }
    }
}

#[test]
fn serializable_with_snapshot_isolation_opens_the_view_at_the_first_statement() {
    let b = bed();
    seed(&b, "a", "1");
    // BEGIN alone opens no view: a commit before the first statement is
    // not a conflict (MariaDB 11.8 answers the UPDATE, not 1020).
    let mut t = b.begin(TxnOptions::mariadb().with_isolation(Isolation::Serializable));
    assert!(t.read_view().is_none());
    seed(&b, "a", "2");
    t.begin_statement();
    assert!(t.read_view().is_some());
    assert_eq!(
        t.lock_get_blocking(&k("a"), x()).unwrap(),
        Locked::Row(Some(v("2")))
    );
    t.rollback();
    // The first statement opens it, whatever it is: a commit after that is
    // a conflict for a later statement.
    let mut t = b.begin(TxnOptions::mariadb().with_isolation(Isolation::Serializable));
    t.begin_statement();
    t.lock_get_blocking(&k("b"), x()).unwrap();
    seed(&b, "a", "3");
    t.begin_statement();
    assert_eq!(errno(&t.lock_get_blocking(&k("a"), x()).unwrap_err()), 1020);
    assert_eq!(t.state().aborted_by, Some(AbortReason::CheckRead));
    // MySQL has no such check, and REPEATABLE READ opens no view here.
    for opts in [
        TxnOptions::mysql().with_isolation(Isolation::Serializable),
        TxnOptions::mariadb(),
    ] {
        let mut t = b.begin(opts);
        t.begin_statement();
        assert!(t.read_view().is_none());
        t.rollback();
    }
}

#[test]
fn lock_memory_exhausted_is_1206_and_ends_the_transaction() {
    let cfg = UnitConfig {
        max_lock_bytes_per_txn: 1000,
        ..UnitConfig::default()
    };
    let b = mem_bed_with(cfg, Duration::ZERO);
    let mut t = b.begin(TxnOptions::mariadb());
    let mut err = None;
    for i in 0..100 {
        if let Err(e) = t.lock_blocking(&k(&format!("key{i}")), x()) {
            err = Some(e);
            break;
        }
    }
    let e = err.expect("1206 expected");
    assert_eq!(errno(&e), 1206);
    assert_eq!(t.state().aborted_by, Some(AbortReason::LockTableFull));
    assert_eq!(b.locks().key_count(), 0);
}

#[test]
fn savepoints_undo_writes_and_keep_locks() {
    let b = bed();
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_blocking(&k("a"), x()).unwrap();
    t.put(k("a"), v("1")).unwrap();
    let sp = t.savepoint();
    t.lock_blocking(&k("b"), x()).unwrap();
    t.put(k("b"), v("2")).unwrap();
    t.put(k("a"), v("changed")).unwrap();
    t.rollback_to(sp);
    assert_eq!(t.get_snapshot(&k("a")).unwrap(), Some(v("1")));
    assert_eq!(t.get_snapshot(&k("b")).unwrap(), None);
    assert_eq!(
        b.locks().mode_held(t.id(), &k("b")),
        Some(LockMode::Exclusive)
    );
    t.commit().unwrap().wait().unwrap();
    let mut r = b.begin(TxnOptions::mariadb());
    assert_eq!(r.get_snapshot(&k("a")).unwrap(), Some(v("1")));
    assert_eq!(r.get_snapshot(&k("b")).unwrap(), None);
}

#[test]
fn shared_locks_coexist_and_block_writers_fifo() {
    let b = bed();
    seed(&b, "a", "1");
    let unit = b.clone();
    let mut s1 = unit.begin(TxnOptions::mariadb());
    let mut s2 = unit.begin(TxnOptions::mariadb());
    s1.lock_blocking(&k("a"), LockRequest::for_share()).unwrap();
    s2.lock_blocking(&k("a"), LockRequest::for_share()).unwrap();
    let mut w = unit.begin(TxnOptions::mariadb());
    let h = thread::spawn(move || {
        let r = w.lock_get_blocking(&k("a"), x());
        (r, w)
    });
    until_waiting(&unit, 2);
    // A new shared request queues behind the waiting writer (FIFO).
    let mut s3 = unit.begin(TxnOptions::mariadb());
    let e = s3
        .lock_get_blocking(&k("a"), LockRequest::for_share().with_wait(Wait::NoWait))
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    s1.rollback();
    s2.rollback();
    let (r, _w) = h.join().unwrap();
    assert_eq!(r.unwrap(), Locked::Row(Some(v("1"))));
}

#[test]
fn repeatable_read_keeps_its_snapshot_read_committed_does_not() {
    let b = bed();
    seed(&b, "a", "1");
    let mut rr = b.begin(TxnOptions::mariadb());
    let mut rc = b.begin(TxnOptions::mariadb().with_isolation(Isolation::ReadCommitted));
    rr.begin_statement();
    rc.begin_statement();
    assert_eq!(rr.get_snapshot(&k("a")).unwrap(), Some(v("1")));
    assert_eq!(rc.get_snapshot(&k("a")).unwrap(), Some(v("1")));
    seed(&b, "a", "2");
    rr.begin_statement();
    rc.begin_statement();
    assert_eq!(rr.get_snapshot(&k("a")).unwrap(), Some(v("1")));
    assert_eq!(rc.get_snapshot(&k("a")).unwrap(), Some(v("2")));
    // The view opens at the first plain read, not at BEGIN.
    let mut late = b.begin(TxnOptions::mariadb());
    seed(&b, "a", "3");
    assert_eq!(late.get_snapshot(&k("a")).unwrap(), Some(v("3")));
}

// ----- waits ended from outside -----------------------------------------------

#[test]
fn kill_query_ends_a_lock_wait_with_1317_and_only_the_statement_is_rolled_back() {
    let b = bed();
    seed(&b, "a", "1");
    let unit = b.clone();
    let mut holder = unit.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();
    let mut t = unit.begin(TxnOptions::mariadb());
    t.begin_statement();
    t.put(k("first"), v("kept")).unwrap();
    t.begin_statement();
    t.put(k("second"), v("undone")).unwrap();
    let kill = t.interrupt_handle();
    let h = thread::spawn(move || {
        let started = Instant::now();
        let r = t.lock_get_blocking(&k("a"), x());
        (r, t, started.elapsed())
    });
    until_waiting(&unit, 1);
    kill.interrupt(Interrupt::Kill);
    let (r, mut t, waited) = h.join().unwrap();
    let e = r.unwrap_err();
    assert_eq!(errno(&e), 1317);
    assert_eq!(&sqlstate(&e), b"70100");
    assert!(waited < Duration::from_secs(5));
    assert!(unit.locks().waits_for().is_empty());
    // Statement scope: open, earlier statement kept, locks kept.
    assert!(t.state().is_open);
    assert_eq!(t.state().aborted_by, None);
    assert_eq!(t.get_snapshot(&k("first")).unwrap(), Some(v("kept")));
    assert_eq!(t.get_snapshot(&k("second")).unwrap(), None);
    assert_eq!(
        unit.locks().mode_held(t.id(), &k("second")),
        Some(LockMode::Exclusive)
    );
    // Still interrupted until the next statement starts.
    let e = t.lock_blocking(&k("other"), x()).unwrap_err();
    assert_eq!(errno(&e), 1317);
    t.begin_statement();
    t.lock_blocking(&k("other"), x()).unwrap();
    holder.rollback();
    assert_eq!(
        t.lock_get_blocking(&k("a"), x()).unwrap(),
        Locked::Row(Some(v("1")))
    );
    t.commit().unwrap().wait().unwrap();
    assert_eq!(unit.locks().key_count(), 0);
}

#[test]
fn a_statement_time_limit_is_1969_on_mariadb_and_3024_on_mysql() {
    for (opts, want, state) in [
        (TxnOptions::mariadb(), 1969, b"70100"),
        (TxnOptions::mysql(), 3024, b"HY000"),
    ] {
        let b = bed();
        seed(&b, "a", "1");
        let unit = b.clone();
        let mut holder = unit.begin(TxnOptions::mariadb());
        holder.lock_blocking(&k("a"), x()).unwrap();
        let mut t = unit.begin(opts);
        let limit = t.interrupt_handle();
        let h = thread::spawn(move || {
            let r = t.lock_get_blocking(&k("a"), x());
            (r, t)
        });
        until_waiting(&unit, 1);
        limit.interrupt(Interrupt::StatementTimeout);
        let (r, t) = h.join().unwrap();
        let e = r.unwrap_err();
        assert_eq!(errno(&e), want);
        assert_eq!(&sqlstate(&e), state);
        assert!(t.state().is_open);
    }
}

#[test]
fn an_interrupt_that_arrives_before_the_wait_fails_the_request_at_once() {
    let b = bed();
    seed(&b, "a", "1");
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();
    let mut t = b.begin(TxnOptions::mariadb());
    t.begin_statement();
    t.interrupt_handle().interrupt(Interrupt::Kill);
    let started = Instant::now();
    let e = t.lock_get_blocking(&k("a"), x()).unwrap_err();
    assert_eq!(errno(&e), 1317);
    assert!(started.elapsed() < Duration::from_secs(2));
    // Range reads and inserts check it too.
    let e = t
        .scan(
            &sprig_engine::keys::KeyRange::all(),
            sprig_engine::txn::Dir::Forward,
            10,
            sprig_engine::txn::ReadMode::ForUpdate,
        )
        .unwrap_err();
    assert_eq!(errno(&e), 1317);
    let e = t
        .lock_insert_blocking(&k("new"), sprig_engine::txn::InsertLock::insert())
        .unwrap_err();
    assert_eq!(errno(&e), 1317);
    assert!(t.state().is_open);
}

#[test]
fn wait_n_waits_that_long_and_wait_0_not_at_all() {
    let b = bed();
    seed(&b, "a", "1");
    let mut holder = b.begin(TxnOptions::mariadb());
    holder.lock_blocking(&k("a"), x()).unwrap();
    // WAIT n overrides the session's timeout for this request, on both
    // personas, and its error is the timeout's (1205), not NOWAIT's.
    for opts in [
        TxnOptions::mariadb().with_lock_wait_seconds(0),
        TxnOptions::mysql().with_lock_wait_seconds(100_000_000),
    ] {
        let mut t = b.begin(opts);
        let started = Instant::now();
        let e = t
            .lock_get_blocking(&k("a"), x().with_wait(Wait::Seconds(1)))
            .unwrap_err();
        let waited = started.elapsed();
        assert_eq!(errno(&e), 1205);
        assert!(waited >= Duration::from_millis(950) && waited < Duration::from_secs(5));
        let started = Instant::now();
        let e = t
            .lock_get_blocking(&k("a"), x().with_wait(Wait::Seconds(0)))
            .unwrap_err();
        assert_eq!(errno(&e), 1205);
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(t.state().is_open);
    }
    assert!(b.locks().waits_for().is_empty());
}

#[test]
fn a_request_that_cannot_wait_is_never_part_of_a_deadlock() {
    // A zero timeout, WAIT 0 and NOWAIT would each close a cycle if they
    // queued. They fail at once with their own error, and nobody is rolled
    // back.
    for (opts, wait, want) in [
        (
            TxnOptions::mariadb().with_lock_wait_seconds(0),
            Wait::Block,
            1205,
        ),
        (TxnOptions::mysql(), Wait::Seconds(0), 1205),
        (TxnOptions::mysql(), Wait::NoWait, 3572),
    ] {
        let b = bed();
        seed(&b, "1", "one");
        seed(&b, "2", "two");
        let unit = b.clone();
        let mut a = unit.begin(TxnOptions::mariadb());
        a.lock_blocking(&k("1"), x()).unwrap();
        let mut t = unit.begin(opts);
        t.lock_blocking(&k("2"), LockRequest::write().with_wait(Wait::Seconds(5)))
            .unwrap();
        let h = thread::spawn(move || {
            let r = a.lock_get_blocking(&k("2"), x());
            (r, a)
        });
        until_waiting(&unit, 1);
        let e = t
            .lock_get_blocking(&k("1"), x().with_wait(wait))
            .unwrap_err();
        assert_eq!(errno(&e), want);
        assert!(t.state().is_open);
        t.rollback();
        let (r, a) = h.join().unwrap();
        assert_eq!(r.unwrap(), Locked::Row(Some(v("two"))));
        assert_eq!(a.state().aborted_by, None);
        assert_eq!(
            unit.locks()
                .stats
                .deadlocks
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }
}
