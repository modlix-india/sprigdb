//! Early lock release, the commit-dependency rule and the reply gate.

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

use std::time::Duration;

use sprig_engine::txn::{LockRequest, Locked, TxnOptions, UnitConfig, Wait};
use txn_common::{bed, errno, k, mem_bed_with, v};

#[test]
fn locks_go_at_queue_time_and_readers_of_pending_data_wait_for_durability() {
    let b = bed();
    b.mem().pause();
    let mut t1 = b.begin(TxnOptions::mariadb());
    t1.lock_blocking(&k("hot"), LockRequest::for_update())
        .unwrap();
    t1.put(k("hot"), v("1")).unwrap();
    let c1 = t1.commit().unwrap();
    assert!(!c1.is_durable(), "the log is paused");

    // The lock is already free: a writer takes it at once and reads the
    // pending version.
    let mut t2 = b.begin(TxnOptions::mariadb());
    assert_eq!(
        t2.lock_get_blocking(&k("hot"), LockRequest::for_update().with_wait(Wait::NoWait))
            .unwrap(),
        Locked::Row(Some(v("1")))
    );
    // Its statement reply must wait for t1's entry.
    assert!(!t2.reply_gate().is_durable());
    t2.put(k("hot"), v("2")).unwrap();
    let c2 = t2.commit().unwrap();
    assert!(c2.durable_ticket().index() > c1.durable_ticket().index());
    assert!(c2.commit_ts() > c1.commit_ts());

    // A plain reader whose view includes the pending commits sees them, and
    // its read-only commit is gated too.
    let mut r = b.begin(TxnOptions::mariadb());
    assert_eq!(r.get_snapshot(&k("hot")).unwrap(), Some(v("2")));
    let cr = r.commit().unwrap();
    assert_eq!(cr.durable_ticket().index(), c2.durable_ticket().index());
    assert!(!cr.is_durable());

    assert_eq!(b.shared().pending_versions(), 2);
    b.mem().resume();
    c1.wait().unwrap();
    c2.wait().unwrap();
    cr.wait().unwrap();
    // Applied and out of the overlay.
    let start = std::time::Instant::now();
    while b.path.applied_index() < c2.durable_ticket().index() {
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::yield_now();
    }
    let mut r = b.begin(TxnOptions::mariadb());
    assert_eq!(r.get_snapshot(&k("hot")).unwrap(), Some(v("2")));
    // Applied versions leave the overlay.
    assert_eq!(b.shared().pending_versions(), 0);
    assert!(r.commit().unwrap().is_durable());
}

#[test]
fn a_view_older_than_the_pending_commit_does_not_see_it_and_is_not_gated() {
    let b = bed();
    let mut t0 = b.begin(TxnOptions::mariadb());
    t0.lock_blocking(&k("a"), LockRequest::write()).unwrap();
    t0.put(k("a"), v("old")).unwrap();
    t0.commit().unwrap().wait().unwrap();

    let mut reader = b.begin(TxnOptions::mariadb());
    assert_eq!(reader.get_snapshot(&k("a")).unwrap(), Some(v("old")));
    b.mem().pause();
    let mut w = b.begin(TxnOptions::mariadb());
    w.lock_blocking(&k("a"), LockRequest::write()).unwrap();
    w.put(k("a"), v("new")).unwrap();
    let cw = w.commit().unwrap();
    assert_eq!(reader.get_snapshot(&k("a")).unwrap(), Some(v("old")));
    assert!(reader.commit().unwrap().is_durable());
    b.mem().resume();
    cw.wait().unwrap();
}

#[test]
fn without_early_release_locks_are_held_until_durable() {
    let cfg = UnitConfig {
        early_lock_release: false,
        ..UnitConfig::default()
    };
    let b = mem_bed_with(cfg, Duration::ZERO);
    b.mem().pause();
    let mut t1 = b.begin(TxnOptions::mariadb());
    t1.lock_blocking(&k("hot"), LockRequest::for_update())
        .unwrap();
    t1.put(k("hot"), v("1")).unwrap();
    let c1 = t1.commit().unwrap();
    let mut t2 = b.begin(TxnOptions::mariadb());
    let e = t2
        .lock_get_blocking(&k("hot"), LockRequest::for_update().with_wait(Wait::NoWait))
        .unwrap_err();
    assert_eq!(errno(&e), 1205);
    b.mem().resume();
    c1.wait().unwrap();
    assert_eq!(
        t2.lock_get_blocking(&k("hot"), LockRequest::for_update())
            .unwrap(),
        Locked::Row(Some(v("1")))
    );
}

#[test]
fn a_crash_fails_every_reply_that_depends_on_lost_entries() {
    let b = bed();
    b.mem().pause();
    let mut t1 = b.begin(TxnOptions::mariadb());
    t1.lock_blocking(&k("a"), LockRequest::write()).unwrap();
    t1.put(k("a"), v("lost")).unwrap();
    let c1 = t1.commit().unwrap();
    let mut r = b.begin(TxnOptions::mariadb());
    assert_eq!(r.get_snapshot(&k("a")).unwrap(), Some(v("lost")));
    let gate = r.reply_gate();
    b.mem().crash();
    assert_eq!(errno(&c1.wait().unwrap_err()), 1213);
    assert!(gate.wait().is_err());
    // Nothing more can commit on the failed log.
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_blocking(&k("b"), LockRequest::write()).unwrap();
    t.put(k("b"), v("x")).unwrap();
    assert!(t.commit().is_err());
    assert!(b.mem().durable_entries().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_waits_and_tickets() {
    let b = mem_bed_with(UnitConfig::default(), Duration::from_millis(2));
    let unit = b.clone();
    {
        // The counter exists before the tasks start. On a missing row the
        // locking read would keep only a gap lock, and the tasks' inserts
        // would then wait for each other inside `put`, on the runtime's
        // threads (and deadlock, as they do in `InnoDB`).
        let mut t = unit.begin(TxnOptions::mariadb());
        t.put(k("ctr"), bytes::Bytes::copy_from_slice(&0u64.to_be_bytes()))
            .unwrap();
        t.commit().unwrap().wait_async().await.unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let unit = unit.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..50 {
                let mut t = unit.begin(TxnOptions::mariadb());
                let cur = match t
                    .lock_get(&k("ctr"), LockRequest::for_update())
                    .await
                    .unwrap()
                {
                    Locked::Row(v) => {
                        v.map_or(0, |b| u64::from_be_bytes(b[..].try_into().unwrap()))
                    }
                    Locked::Skipped => unreachable!(),
                };
                t.put(
                    k("ctr"),
                    bytes::Bytes::copy_from_slice(&(cur + 1).to_be_bytes()),
                )
                .unwrap();
                t.commit().unwrap().wait_async().await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let mut r = unit.begin(TxnOptions::mariadb());
    let got = r.get_snapshot(&k("ctr")).unwrap().unwrap();
    assert_eq!(u64::from_be_bytes(got[..].try_into().unwrap()), 800);
    // Group commit batched them: far fewer syncs than commits.
    assert!(b.mem().groups() < 800, "groups {}", b.mem().groups());
}
