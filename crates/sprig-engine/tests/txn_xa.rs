//! XA on one unit: prepare makes a transaction survive its session and a
//! crash; commit and rollback finish it by XID from any session; recovery
//! brings prepared transactions back with their exclusive locks.

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

use std::sync::Arc;
use std::time::Duration;

use sprig_engine::keys::KeyRange;
use sprig_engine::locks::LockMode;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::hlc::SystemClock;
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{
    CommitPath, Dir as ScanDir, InsertLock, InsertProbe, Isolation, LockRequest, Txn, TxnOptions,
    UnitConfig, Wait,
};
use sprig_engine::{Engine, StoreKind};
use txn_common::{Bed, Dir, bed, engine_cfg, errno, k, sqlstate, v};

fn x() -> LockRequest {
    LockRequest::for_update()
}

fn seed(unit: &Bed, key: &str, val: &str) {
    let mut t = unit.begin(TxnOptions::mariadb());
    t.put(k(key), v(val)).unwrap();
    t.commit().unwrap().wait().unwrap();
}

fn read(unit: &Bed, key: &str) -> Option<bytes::Bytes> {
    let mut t = unit.begin(TxnOptions::mariadb());
    t.get_snapshot(&k(key)).unwrap()
}

fn can_lock(unit: &Bed, key: &str) -> bool {
    let mut t = unit.begin(TxnOptions::mariadb().with_isolation(Isolation::ReadCommitted));
    t.lock_blocking(&k(key), LockRequest::write().with_wait(Wait::NoWait))
        .is_ok()
}

fn can_insert(unit: &Bed, key: &str) -> bool {
    let mut t = unit.begin(TxnOptions::mariadb());
    matches!(
        t.lock_insert_blocking(&k(key), InsertLock::insert().with_wait(Wait::NoWait)),
        Ok(InsertProbe::Free)
    )
}

#[test]
fn a_prepared_transaction_is_committed_by_xid_from_another_session() {
    let b = bed();
    seed(&b, "a", "old");
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_blocking(&k("a"), x()).unwrap();
    t.put(k("a"), v("new")).unwrap();
    t.put(k("b"), v("made")).unwrap();
    let id = t.id();
    let ticket = t.xa_prepare(b"xid-1").unwrap();
    ticket.wait().unwrap();
    assert!(!t.state().is_open, "the session's handle is finished");
    drop(t);
    // Not committed yet: readers see the old data, writers wait.
    assert_eq!(read(&b, "a"), Some(v("old")));
    assert_eq!(read(&b, "b"), None);
    assert!(!can_lock(&b, "a"));
    assert_eq!(b.locks().mode_held(id, &k("b")), Some(LockMode::Exclusive));
    assert_eq!(Txn::xa_recover(&b.path), vec![b"xid-1".to_vec()]);

    let c = Txn::xa_commit(&b.path, b"xid-1").unwrap();
    c.wait().unwrap();
    assert!(c.commit_ts().is_some());
    assert_eq!(read(&b, "a"), Some(v("new")));
    assert_eq!(read(&b, "b"), Some(v("made")));
    assert!(can_lock(&b, "a"));
    assert!(Txn::xa_recover(&b.path).is_empty());
    assert_eq!(b.locks().key_count(), 0);
}

#[test]
fn a_prepared_transaction_is_rolled_back_by_xid() {
    let b = bed();
    seed(&b, "a", "old");
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_blocking(&k("a"), x()).unwrap();
    t.put(k("a"), v("new")).unwrap();
    t.xa_prepare(b"xid-2").unwrap().wait().unwrap();
    drop(t);
    assert!(!can_lock(&b, "a"));
    Txn::xa_rollback(&b.path, b"xid-2").unwrap().wait().unwrap();
    assert_eq!(read(&b, "a"), Some(v("old")));
    assert!(can_lock(&b, "a"));
    assert!(Txn::xa_recover(&b.path).is_empty());
    assert_eq!(b.locks().key_count(), 0);
}

#[test]
fn unknown_and_duplicate_xids_have_their_errors() {
    let b = bed();
    let e = Txn::xa_commit(&b.path, b"nope").unwrap_err();
    assert_eq!(errno(&e), 1397);
    assert_eq!(&sqlstate(&e), b"XAE04");
    let e = Txn::xa_rollback(&b.path, b"nope").unwrap_err();
    assert_eq!(errno(&e), 1397);

    let mut t1 = b.begin(TxnOptions::mariadb());
    t1.put(k("a"), v("1")).unwrap();
    t1.xa_prepare(b"same").unwrap().wait().unwrap();
    assert!(Txn::xa_exists(&b.path, b"same"));
    assert!(!Txn::xa_exists(&b.path, b"other"));
    let mut t2 = b.begin(TxnOptions::mariadb());
    t2.put(k("b"), v("2")).unwrap();
    let e = t2.xa_prepare(b"same").unwrap_err();
    assert_eq!(errno(&e), 1440);
    assert_eq!(&sqlstate(&e), b"XAE08");
    // The refused transaction is still open and can go on.
    assert!(t2.state().is_open);
    t2.xa_prepare(b"other").unwrap().wait().unwrap();
    let mut xids = Txn::xa_recover(&b.path);
    xids.sort();
    assert_eq!(xids, vec![b"other".to_vec(), b"same".to_vec()]);
    Txn::xa_commit(&b.path, b"same").unwrap().wait().unwrap();
    Txn::xa_rollback(&b.path, b"other").unwrap().wait().unwrap();
    assert_eq!(read(&b, "a"), Some(v("1")));
    assert_eq!(read(&b, "b"), None);
    // A finished XID is unknown again, and can be used again.
    assert_eq!(errno(&Txn::xa_commit(&b.path, b"same").unwrap_err()), 1397);
    let mut t3 = b.begin(TxnOptions::mariadb());
    t3.put(k("c"), v("3")).unwrap();
    t3.xa_prepare(b"same").unwrap().wait().unwrap();
    Txn::xa_commit(&b.path, b"same").unwrap().wait().unwrap();
    assert_eq!(read(&b, "c"), Some(v("3")));
    assert_eq!(b.locks().key_count(), 0);
}

#[test]
fn prepare_gives_up_shared_and_gap_locks_and_keeps_exclusive_ones() {
    // MariaDB gives them up, except at SERIALIZABLE; MySQL keeps them all.
    for (base, iso, kept) in [
        (TxnOptions::mariadb(), Isolation::RepeatableRead, false),
        (TxnOptions::mariadb(), Isolation::Serializable, true),
        (TxnOptions::mysql(), Isolation::RepeatableRead, true),
        (TxnOptions::mysql(), Isolation::Serializable, true),
    ] {
        let b = bed();
        seed(&b, "010", "a");
        seed(&b, "020", "b");
        seed(&b, "030", "c");
        let mut t = b.begin(base.with_isolation(iso));
        // S on every row and a gap lock on the whole range, then X on one.
        t.scan_lock_blocking(
            &KeyRange::new(k("000").to_vec(), k("999").to_vec()),
            ScanDir::Forward,
            usize::MAX,
            LockRequest::for_share(),
        )
        .unwrap();
        t.lock_blocking(&k("020"), x()).unwrap();
        t.put(k("020"), v("b2")).unwrap();
        assert!(!can_insert(&b, "015"));
        assert!(!can_lock(&b, "010"));
        t.xa_prepare(b"p").unwrap().wait().unwrap();
        assert_eq!(can_insert(&b, "015"), !kept, "{iso:?}: gap lock");
        assert_eq!(can_lock(&b, "010"), !kept, "{iso:?}: shared lock");
        assert!(!can_lock(&b, "020"), "{iso:?}: exclusive lock");
        Txn::xa_commit(&b.path, b"p").unwrap().wait().unwrap();
        assert!(can_insert(&b, "015"));
        assert!(can_lock(&b, "020"));
        assert_eq!(read(&b, "020"), Some(v("b2")));
        assert_eq!(b.locks().key_count() + b.locks().gap_count(), 0);
    }
}

#[test]
fn a_read_only_transaction_can_be_prepared_and_finished() {
    let b = bed();
    seed(&b, "a", "1");
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_get_blocking(&k("a"), x()).unwrap();
    t.xa_prepare(b"ro").unwrap().wait().unwrap();
    assert!(!can_lock(&b, "a"));
    let c = Txn::xa_commit(&b.path, b"ro").unwrap();
    c.wait().unwrap();
    assert!(can_lock(&b, "a"));
    assert_eq!(read(&b, "a"), Some(v("1")));
}

#[test]
fn the_commit_is_not_durable_before_its_prepare() {
    let b = bed();
    b.mem().pause();
    let mut t = b.begin(TxnOptions::mariadb());
    t.put(k("a"), v("1")).unwrap();
    let p = t.xa_prepare(b"x").unwrap();
    assert!(!p.is_durable());
    let c = Txn::xa_commit(&b.path, b"x").unwrap();
    assert!(!c.is_durable());
    assert!(c.durable_ticket().index() > p.index());
    b.mem().resume();
    c.wait().unwrap();
    assert!(p.is_durable());
    assert_eq!(read(&b, "a"), Some(v("1")));
}

/// Restarts the in-memory path from what a crash left durable.
fn restart(b: &Bed) -> Bed {
    let entries = b.mem().durable_entries();
    let mem = MemPath::start_with(
        UnitConfig::default(),
        Duration::ZERO,
        Arc::new(SystemClock),
        entries,
    )
    .unwrap();
    Bed {
        path: mem.clone(),
        mem: Some(mem),
    }
}

#[test]
fn prepared_transactions_come_back_after_a_crash_with_their_locks() {
    let b = bed();
    seed(&b, "a", "old");
    seed(&b, "c", "kept");
    // One prepared and durable; one prepared whose record the crash loses;
    // one committed after prepare; one rolled back after prepare.
    let mut t = b.begin(TxnOptions::mariadb());
    t.lock_blocking(&k("a"), x()).unwrap();
    t.put(k("a"), v("new")).unwrap();
    t.put(k("b"), v("made")).unwrap();
    t.lock_blocking(&k("c"), x()).unwrap();
    t.xa_prepare(b"durable").unwrap().wait().unwrap();
    for (xid, key) in [(&b"done"[..], "d"), (&b"undone"[..], "e")] {
        let mut t = b.begin(TxnOptions::mariadb());
        t.put(k(key), v("x")).unwrap();
        t.xa_prepare(xid).unwrap().wait().unwrap();
    }
    Txn::xa_commit(&b.path, b"done").unwrap().wait().unwrap();
    Txn::xa_rollback(&b.path, b"undone")
        .unwrap()
        .wait()
        .unwrap();
    b.mem().pause();
    let mut t = b.begin(TxnOptions::mariadb());
    t.put(k("f"), v("lost")).unwrap();
    let lost = t.xa_prepare(b"lost").unwrap();
    b.mem().crash();
    assert!(lost.wait().is_err(), "its client was never answered");

    let b2 = restart(&b);
    assert_eq!(Txn::xa_recover(&b2.path), vec![b"durable".to_vec()]);
    assert_eq!(read(&b2, "a"), Some(v("old")));
    assert_eq!(read(&b2, "b"), None);
    assert_eq!(read(&b2, "d"), Some(v("x")));
    assert_eq!(read(&b2, "e"), None);
    assert_eq!(read(&b2, "f"), None);
    // Its exclusive locks are held again: written keys and locked rows.
    assert!(!can_lock(&b2, "a"));
    assert!(!can_lock(&b2, "b"));
    assert!(!can_lock(&b2, "c"));
    assert!(can_lock(&b2, "d"));

    // A second crash before it is finished changes nothing.
    b2.mem().crash();
    let b3 = restart(&b2);
    assert_eq!(Txn::xa_recover(&b3.path), vec![b"durable".to_vec()]);
    assert!(!can_lock(&b3, "a"));
    Txn::xa_commit(&b3.path, b"durable")
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read(&b3, "a"), Some(v("new")));
    assert_eq!(read(&b3, "b"), Some(v("made")));
    assert_eq!(read(&b3, "c"), Some(v("kept")));
    assert!(can_lock(&b3, "a"));
    assert_eq!(b3.locks().key_count(), 0);

    // And once committed, a restart doesn't bring it back.
    b3.mem().crash();
    let b4 = restart(&b3);
    assert!(Txn::xa_recover(&b4.path).is_empty());
    assert_eq!(read(&b4, "a"), Some(v("new")));
    assert!(can_lock(&b4, "a"));
}

#[test]
fn prepared_transactions_survive_an_engine_restart() {
    for store in [StoreKind::Rocks, StoreKind::Mem] {
        let dir = Dir::new("xa");
        {
            let e = Engine::open(engine_cfg(&dir, store, SyncMode::Off)).unwrap();
            let path: Arc<dyn CommitPath> = Arc::new(e.clone());
            let b = Bed {
                path: path.clone(),
                mem: None,
            };
            seed(&b, "a", "old");
            let mut t = b.begin(TxnOptions::mariadb());
            t.lock_blocking(&k("a"), x()).unwrap();
            t.put(k("a"), v("new")).unwrap();
            t.xa_prepare(b"across").unwrap().wait().unwrap();
            let mut t = b.begin(TxnOptions::mariadb());
            t.put(k("z"), v("gone")).unwrap();
            t.xa_prepare(b"dropped").unwrap().wait().unwrap();
            Txn::xa_rollback(&path, b"dropped").unwrap().wait().unwrap();
        }
        let e = Engine::open(engine_cfg(&dir, store, SyncMode::Off)).unwrap();
        let path: Arc<dyn CommitPath> = Arc::new(e.clone());
        let b = Bed {
            path: path.clone(),
            mem: None,
        };
        assert_eq!(
            Txn::xa_recover(&path),
            vec![b"across".to_vec()],
            "{store:?}"
        );
        assert_eq!(read(&b, "a"), Some(v("old")));
        assert_eq!(read(&b, "z"), None);
        assert!(!can_lock(&b, "a"));
        Txn::xa_commit(&path, b"across").unwrap().wait().unwrap();
        assert_eq!(read(&b, "a"), Some(v("new")));
        assert!(can_lock(&b, "a"));
        assert!(Txn::xa_recover(&path).is_empty());
    }
}

#[test]
fn a_transaction_the_log_refuses_to_finish_stays_prepared() {
    let b = bed();
    let mut t = b.begin(TxnOptions::mariadb());
    t.put(k("a"), v("1")).unwrap();
    t.xa_prepare(b"stuck").unwrap().wait().unwrap();
    b.mem().crash();
    // The log is gone: neither finish goes through, and the XID stays
    // taken and listed.
    assert!(Txn::xa_commit(&b.path, b"stuck").is_err());
    assert!(Txn::xa_rollback(&b.path, b"stuck").is_err());
    assert!(Txn::xa_exists(&b.path, b"stuck"));
    assert_eq!(Txn::xa_recover(&b.path), vec![b"stuck".to_vec()]);
    assert!(!can_lock(&b, "a"));
    let b2 = restart(&b);
    Txn::xa_commit(&b2.path, b"stuck").unwrap().wait().unwrap();
    assert_eq!(read(&b2, "a"), Some(v("1")));
}
