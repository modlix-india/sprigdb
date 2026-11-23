//! The engine committing through Raft: three engines (RocksDB stores on
//! disk), each with its Raft node, over an in-process network. A tenant
//! unit has three voters; commits on its leader reach every replica's
//! store; a killed leader is replaced and, restarted, catches up.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::time::SystemClock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};

const TENANT: u64 = 4242;
const VOTERS: [u64; 3] = [1, 2, 3];
const WAIT: Duration = Duration::from_secs(15);

fn fast() -> RaftConfig {
    RaftConfig {
        tick: Duration::from_millis(10),
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        static_voters: VOTERS.to_vec(),
        ..RaftConfig::default()
    }
}

fn key(tenant: u64, i: u64) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
}

struct Replica {
    engine: Engine,
    node: EngineNode,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Cluster {
    dir: PathBuf,
    net: Arc<MemNetwork>,
    replicas: BTreeMap<u64, Replica>,
}

fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

impl Cluster {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("sprig-repl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
        };
        for id in VOTERS {
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: u64) {
        let d = self.dir.join(format!("n{id}"));
        let mut cfg = EngineConfig::new(d.join("engine"));
        cfg.store = StoreKind::Rocks;
        cfg.checkpoint_interval = Duration::ZERO;
        cfg.log.sync = SyncMode::Off;
        cfg.replicated = true;
        let engine = Engine::open(cfg).unwrap();
        let store = EngineLogStore::open(d.join("raft"), SyncMode::Off, None).unwrap();
        let node = EngineNode::open(
            &engine,
            EngineNodeOptions {
                id,
                config: fast(),
                store,
                transport: self.net.transport(id),
                clock: Arc::new(SystemClock),
                seed: id,
                observer: None,
                inline_roles: false,
            },
        )
        .unwrap();
        self.net.register(node.node());
        let thread = Some(node.spawn().unwrap());
        self.replicas.insert(
            id,
            Replica {
                engine,
                node,
                thread,
            },
        );
    }

    fn kill(&mut self, id: u64) {
        let mut r = self.replicas.remove(&id).unwrap();
        self.net.unregister(id);
        r.node.close();
        if let Some(t) = r.thread.take() {
            t.join().unwrap();
        }
        drop(r);
    }

    /// The replica leading `unit` for the engine (its leader epoch set).
    fn leader(&self, unit: UnitId) -> u64 {
        until("a leader", || {
            self.replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(unit) > 0)
                .map(|(id, _)| *id)
        })
    }

    /// Commits `k -> v` on the unit's leader, retrying across leader
    /// changes; returns the replica that acknowledged it.
    fn put(&self, unit: UnitId, k: &[u8], v: &[u8]) -> u64 {
        until("an acknowledged commit", || {
            let l = self.leader(unit);
            let e = &self.replicas[&l].engine;
            let mut t = e.begin(TxnOptions::default()).ok()?;
            t.put(k.to_vec(), Bytes::copy_from_slice(v)).ok()?;
            let c = t.commit().ok()?;
            c.wait().ok()?;
            Some(l)
        })
    }

    fn get(&self, id: u64, k: &[u8]) -> Option<Bytes> {
        let e = &self.replicas[&id].engine;
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.get_snapshot(k).unwrap()
    }

    /// Waits until every replica's store holds the same unit.
    fn converged(&self, unit: UnitId) -> sprig_engine::snapshot::SpanChecksum {
        until("replicas to converge", || {
            let sums: Vec<_> = self
                .replicas
                .values()
                .map(|r| r.engine.unit_checksum(unit).unwrap())
                .collect();
            sums.windows(2).all(|w| w[0] == w[1]).then(|| sums[0])
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let ids: Vec<u64> = self.replicas.keys().copied().collect();
        for id in ids {
            self.kill(id);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[test]
fn three_replicas_commit_fail_over_and_catch_up() {
    let mut c = Cluster::new("failover");
    let unit = Engine::unit_of_tenant(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let first = c.leader(unit);
    // Only the leader leads the unit for its engine.
    for (id, r) in &c.replicas {
        assert_eq!(r.engine.leader_epoch(unit) > 0, *id == first);
        assert!(r.engine.is_replicated_unit(unit));
    }
    for i in 0..50u64 {
        c.put(unit, &key(TENANT, i), format!("a{i}").as_bytes());
    }
    let sum = c.converged(unit);
    assert_eq!(sum.entries, 50);
    for id in VOTERS {
        assert_eq!(c.get(id, &key(TENANT, 7)).as_deref(), Some(&b"a7"[..]));
    }
    // The leader dies; another takes the unit and commits go on.
    let old_epoch = c.replicas[&first].engine.leader_epoch(unit);
    c.kill(first);
    let second = c.leader(unit);
    assert_ne!(second, first);
    assert!(c.replicas[&second].engine.leader_epoch(unit) > old_epoch);
    for i in 50..100u64 {
        c.put(unit, &key(TENANT, i), format!("b{i}").as_bytes());
    }
    // Restarted, the old leader catches up from the log.
    c.start(first);
    let sum = c.converged(unit);
    assert_eq!(sum.entries, 100);
    assert_eq!(c.get(first, &key(TENANT, 99)).as_deref(), Some(&b"b99"[..]));
    // The node unit stays local: it commits on every node by itself.
    for r in c.replicas.values() {
        assert!(!r.engine.is_replicated_unit(sprig_engine::NODE_UNIT));
    }
}

#[test]
fn a_follower_refuses_writes_as_retryable() {
    let c = Cluster::new("follower");
    let unit = Engine::unit_of_tenant(TENANT + 1);
    c.replicas[&2].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    c.put(unit, &key(TENANT + 1, 1), b"x");
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let e = &c.replicas[&f].engine;
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(key(TENANT + 1, 2), Bytes::from_static(b"y")).unwrap();
    let Err(err) = t.commit() else {
        panic!("a follower's commit fails");
    };
    assert_eq!(
        err.code(),
        Some(sprig_common::SprigCode::ShardMoved),
        "{err}"
    );
}

fn is_deadlock(e: &sprig_common::SprigError) -> bool {
    matches!(
        e.kind,
        sprig_common::ErrorKind::Standard { errno: 1213, .. }
    )
}

/// A lock holder on a replica that loses the unit's leadership is rolled
/// back with 1213 at its next step; the new leader's transactions don't
/// wait for its locks.
#[test]
fn losing_leadership_rolls_back_lock_holders_with_1213() {
    let c = Cluster::new("locks");
    let unit = Engine::unit_of_tenant(TENANT + 2);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let k = key(TENANT + 2, 1);
    c.put(unit, &k, b"v0");
    let l = c.leader(unit);
    let e = c.replicas[&l].engine.clone();
    let mut held = e.begin(TxnOptions::default()).unwrap();
    let kb = Bytes::from(k.clone());
    held.lock_get_blocking(&kb, sprig_engine::txn::LockRequest::for_update())
        .unwrap();
    // A second transaction on the same replica waits for that lock.
    let waiter = {
        let e = e.clone();
        let kb = kb.clone();
        std::thread::spawn(move || {
            let mut t = e.begin(TxnOptions::default()).unwrap();
            t.lock_get_blocking(&kb, sprig_engine::txn::LockRequest::for_update())
                .err()
        })
    };
    std::thread::sleep(Duration::from_millis(50));
    let to = VOTERS.into_iter().find(|id| *id != l).unwrap();
    until("the leader transfer", || {
        let _ = c.replicas[&l].node.node().transfer_leader(unit.0, to);
        std::thread::sleep(Duration::from_millis(20));
        (c.replicas[&l].engine.leader_epoch(unit) == 0).then_some(())
    });
    let werr = waiter.join().unwrap().expect("the waiter is rolled back");
    assert!(is_deadlock(&werr), "{werr}");
    held.put(k.clone(), Bytes::from_static(b"lost")).unwrap();
    let Err(err) = held.commit() else {
        panic!("the holder is rolled back");
    };
    assert!(is_deadlock(&err), "{err}");
    // The new leader writes the key at once.
    let got = c.put(unit, &k, b"v1");
    assert_ne!(got, l);
    c.converged(unit);
    for id in VOTERS {
        assert_eq!(c.get(id, &k).as_deref(), Some(&b"v1"[..]));
    }
}

/// A commit a deposed leader proposed but never got committed fails as
/// retryable, and so does a reply that read its early-released write.
#[test]
fn a_deposed_leaders_pending_commit_fails_retryable() {
    let c = Cluster::new("pending");
    let unit = Engine::unit_of_tenant(TENANT + 3);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let k = key(TENANT + 3, 1);
    c.put(unit, &k, b"v0");
    let l = c.leader(unit);
    let e = c.replicas[&l].engine.clone();
    c.net.isolate(l, true);
    let mut a = e.begin(TxnOptions::default()).unwrap();
    a.put(k.clone(), Bytes::from_static(b"lost")).unwrap();
    let ta = a
        .commit()
        .expect("the isolated leader still takes the proposal");
    // Locks were released early: a reader sees the pending write, and its
    // reply waits on that commit.
    let mut b = e.begin(TxnOptions::default()).unwrap();
    assert_eq!(b.read_current(&k).unwrap().as_deref(), Some(&b"lost"[..]));
    let gate = b.reply_gate();
    // The others elect a leader and write.
    until("a new leader", || {
        c.replicas
            .iter()
            .find(|(id, r)| **id != l && r.engine.leader_epoch(unit) > 0)
            .map(|_| ())
    });
    let mut w = None;
    for (id, r) in &c.replicas {
        if *id != l && r.engine.leader_epoch(unit) > 0 {
            let mut t = r.engine.begin(TxnOptions::default()).unwrap();
            t.put(k.clone(), Bytes::from_static(b"v1")).unwrap();
            t.commit().unwrap().wait().unwrap();
            w = Some(*id);
        }
    }
    assert!(w.is_some());
    c.net.isolate(l, false);
    // Retryable: the transaction layer reports a lost commit as 1213.
    let Err(err) = ta.wait() else {
        panic!("the lost commit fails");
    };
    assert!(
        is_deadlock(&err) || err.code() == Some(sprig_common::SprigCode::ShardMoved),
        "{err}"
    );
    assert!(gate.wait().is_err(), "the reply that read it fails too");
    c.converged(unit);
    for id in VOTERS {
        assert_eq!(c.get(id, &k).as_deref(), Some(&b"v1"[..]));
    }
}

/// A lost replica is replaced by a new node: it gets the unit from a
/// snapshot of the leader's store, becomes a voter, and the unit then
/// survives losing another of the original replicas.
#[test]
fn a_lost_replica_is_replaced_from_a_snapshot() {
    let mut c = Cluster::new("replace");
    let unit = Engine::unit_of_tenant(TENANT + 4);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    for i in 0..30u64 {
        c.put(unit, &key(TENANT + 4, i), b"a");
    }
    c.converged(unit);
    // A leader change last: the newest entry is the new leader's empty
    // one, so the snapshot's index is above the store's applied index.
    let first = c.leader(unit);
    let next = VOTERS.into_iter().find(|id| *id != first).unwrap();
    until("the leader transfer", || {
        let _ = c.replicas[&first].node.node().transfer_leader(unit.0, next);
        std::thread::sleep(Duration::from_millis(20));
        (c.replicas[&next].engine.leader_epoch(unit) > 0).then_some(())
    });
    // Every log is trimmed: the new replica can only start from a snapshot.
    for r in c.replicas.values() {
        r.engine.checkpoint().unwrap();
        until("the log trimmed", || {
            r.node
                .node()
                .log_first_index(unit.0)
                .is_none_or(|f| f > 6)
                .then_some(())
        });
    }
    let dead = 3;
    c.kill(dead);
    c.start(4);
    let l = c.leader(unit);
    c.replicas[&l]
        .node
        .replace_replica(unit, dead, 4, WAIT)
        .unwrap();
    for i in 30..60u64 {
        c.put(unit, &key(TENANT + 4, i), b"b");
    }
    let sum = c.converged(unit);
    assert_eq!(sum.entries, 60);
    let st = c.replicas[&4].node.node().status(unit.0).unwrap();
    assert!(st.voters.contains(&4) && !st.voters.contains(&dead));
    // Two of three voters are left after losing one more original: the
    // unit still commits.
    let other = [1, 2].into_iter().find(|id| *id != c.leader(unit)).unwrap();
    c.kill(other);
    c.put(unit, &key(TENANT + 4, 60), b"c");
    // Committed means both remaining voters hold it; the new one applies
    // it once it hears the commit index.
    until("the new replica to apply", || {
        (c.get(4, &key(TENANT + 4, 60)).as_deref() == Some(&b"c"[..])).then_some(())
    });
}

/// A transaction prepared (XA) on a leader that then dies is listed by the
/// new leader, from the store, and commits there.
#[test]
fn a_prepared_xa_transaction_survives_a_failover() {
    use sprig_engine::txn::{CommitPath, Txn};
    let mut c = Cluster::new("xa");
    let unit = Engine::unit_of_tenant(TENANT + 5);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let k = key(TENANT + 5, 1);
    c.put(unit, &k, b"v0");
    let l = c.leader(unit);
    {
        let e = &c.replicas[&l].engine;
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.put(k.clone(), Bytes::from_static(b"xa")).unwrap();
        t.xa_prepare(b"x1").unwrap().wait().unwrap();
    }
    c.converged(unit);
    // A follower restarted now doesn't list it either (it isn't the
    // unit's leader).
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    c.kill(f);
    c.start(f);
    c.kill(l);
    let n = c.leader(unit);
    let path: Arc<dyn CommitPath> = Arc::new(c.replicas[&n].engine.clone());
    assert_eq!(Txn::xa_recover(&path), vec![b"x1".to_vec()]);
    // Followers don't list it: only the unit's leader holds its locks.
    for (id, r) in &c.replicas {
        if *id != n {
            let p: Arc<dyn CommitPath> = Arc::new(r.engine.clone());
            assert!(Txn::xa_recover(&p).is_empty(), "n{id}");
        }
    }
    Txn::xa_commit(&path, b"x1").unwrap().wait().unwrap();
    c.converged(unit);
    for id in c.replicas.keys() {
        assert_eq!(c.get(*id, &k).as_deref(), Some(&b"xa"[..]));
    }
}

/// A transaction across two tenants' units commits through the protocol
/// across units when one node leads both, and every replica of each unit
/// ends with its half.
#[test]
fn a_commit_across_two_replicated_units() {
    let c = Cluster::new("cross");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 6),
        Engine::unit_of_tenant(TENANT + 7),
    );
    for u in [ua, ub] {
        c.replicas[&1].node.start_unit(u, &VOTERS).unwrap();
    }
    // Both units led by one node.
    let l = c.leader(ua);
    until("one node leading both units", || {
        if c.replicas[&l].engine.leader_epoch(ub) > 0 {
            return Some(());
        }
        let other = c.leader(ub);
        let _ = c.replicas[&other].node.node().transfer_leader(ub.0, l);
        std::thread::sleep(Duration::from_millis(20));
        None
    });
    let (ka, kb) = (key(TENANT + 6, 1), key(TENANT + 7, 1));
    let e = &c.replicas[&l].engine;
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    t.commit().unwrap().wait().unwrap();
    e.resolve_cross_unit();
    c.converged(ua);
    c.converged(ub);
    for id in VOTERS {
        assert_eq!(c.get(id, &ka).as_deref(), Some(&b"a"[..]), "n{id}");
        until("the participant resolved", || {
            (c.get(id, &kb).as_deref() == Some(&b"b"[..])).then_some(())
        });
    }
}

#[test]
fn a_follower_read_sees_every_acknowledged_commit() {
    let c = Cluster::new("follower-read");
    let unit = Engine::unit_of_tenant(TENANT + 7);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let followers: Vec<u64> = VOTERS.into_iter().filter(|id| *id != l).collect();
    for i in 0..40u64 {
        let k = key(TENANT + 7, i);
        let v = format!("v{i}");
        c.put(unit, &k, v.as_bytes());
        // Right after the acknowledgement, on every follower: a point read,
        // a range read and a current read all see it.
        for f in &followers {
            let e = &c.replicas[f].engine;
            let mut t = e.begin(TxnOptions::default()).unwrap();
            assert_eq!(
                t.get_snapshot(&k).unwrap().as_deref(),
                Some(v.as_bytes()),
                "follower {f} misses commit {i}"
            );
            let mut t = e.begin(TxnOptions::default()).unwrap();
            let range = sprig_engine::keys::KeyRange {
                start: key(TENANT + 7, 0),
                end: Some(key(TENANT + 7, 1000)),
            };
            let rows = t
                .scan(
                    &range,
                    sprig_engine::txn::Dir::Forward,
                    1000,
                    sprig_engine::txn::ReadMode::Snapshot,
                )
                .unwrap();
            assert_eq!(
                rows.len() as u64,
                i + 1,
                "follower {f} scan after commit {i}"
            );
            let mut t = e.begin(TxnOptions::default()).unwrap();
            assert_eq!(t.read_current(&k).unwrap().as_deref(), Some(v.as_bytes()));
        }
    }
    // A follower's read view is a snapshot: a transaction keeps reading
    // what it first saw while the leader commits on; the next one sees it.
    let f = followers[0];
    let k = key(TENANT + 7, 0);
    let mut t = c.replicas[&f].engine.begin(TxnOptions::default()).unwrap();
    assert_eq!(t.get_snapshot(&k).unwrap().as_deref(), Some(&b"v0"[..]));
    c.put(unit, &k, b"changed");
    assert_eq!(t.get_snapshot(&k).unwrap().as_deref(), Some(&b"v0"[..]));
    drop(t);
    assert_eq!(c.get(f, &k).as_deref(), Some(&b"changed"[..]));
}

#[test]
fn a_commit_across_units_led_by_different_nodes() {
    let c = Cluster::new("cross-nodes");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 8),
        Engine::unit_of_tenant(TENANT + 9),
    );
    c.replicas[&1].node.start_unit(ua, &VOTERS).unwrap();
    c.replicas[&2].node.start_unit(ub, &VOTERS).unwrap();
    // The two units led by two different nodes.
    let la = c.leader(ua);
    until("the units led by different nodes", || {
        let lb = c.leader(ub);
        if lb != la {
            return Some(());
        }
        let to = VOTERS.into_iter().find(|id| *id != la).unwrap();
        let _ = c.replicas[&lb].node.node().transfer_leader(ub.0, to);
        std::thread::sleep(Duration::from_millis(20));
        None
    });
    let (ka, kb) = (key(TENANT + 8, 1), key(TENANT + 9, 1));
    let e = &c.replicas[&la].engine;
    // The first try is refused (retryable) while the other unit's
    // leadership moves here; a retry commits on both.
    let mut tries = 0;
    until("the commit across nodes", || {
        tries += 1;
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
        t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
        match t.commit() {
            Ok(c) => c.wait().ok(),
            Err(err) => {
                assert_eq!(
                    err.code(),
                    Some(sprig_common::SprigCode::ShardMoved),
                    "{err}"
                );
                std::thread::sleep(Duration::from_millis(20));
                None
            }
        }
    });
    assert!(tries > 1, "the first try is refused");
    assert!(e.leader_epoch(ua) > 0 && e.leader_epoch(ub) > 0);
    e.resolve_cross_unit();
    for id in VOTERS {
        until("both writes on every replica", || {
            (c.get(id, &ka).as_deref() == Some(&b"a"[..])
                && c.get(id, &kb).as_deref() == Some(&b"b"[..]))
            .then_some(())
        });
    }
    // A node that leads neither unit only refuses: no leadership moves.
    let other = VOTERS.into_iter().find(|id| *id != la).unwrap();
    let mut t = c.replicas[&other]
        .engine
        .begin(TxnOptions::default())
        .unwrap();
    t.put(ka.clone(), Bytes::from_static(b"x")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"y")).unwrap();
    assert!(t.commit().is_err());
    std::thread::sleep(Duration::from_millis(300));
    assert!(e.leader_epoch(ua) > 0 && e.leader_epoch(ub) > 0);
}

#[test]
fn a_follower_current_read_under_a_write_lock_sees_the_latest_commit() {
    // The pattern of a Redis command: READ COMMITTED, the key locked, then
    // a current read.
    let c = Cluster::new("follower-current");
    let unit = Engine::unit_of_tenant(TENANT + 10);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    // A key of a Redis database's object: versioned like a table's.
    let mut k = KeyPrefix {
        tenant: TENANT + 10,
        shard: 0,
        object: 0x5244_0000,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(b"frk");
    for i in 0..20u64 {
        let v = format!("r{i}");
        c.put(unit, &k, v.as_bytes());
        let e = &c.replicas[&f].engine;
        let opts = TxnOptions::mysql().with_isolation(sprig_engine::txn::Isolation::ReadCommitted);
        let mut t = e.begin(opts).unwrap();
        let kb = Bytes::from(k.clone());
        t.lock_blocking(&kb, sprig_engine::txn::LockRequest::write())
            .unwrap();
        assert_eq!(
            t.read_current(&k).unwrap().as_deref(),
            Some(v.as_bytes()),
            "round {i}"
        );
        t.rollback();
    }
}

#[test]
fn a_commit_across_a_led_unit_and_a_new_one() {
    let c = Cluster::new("cross-new");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 11),
        Engine::unit_of_tenant(TENANT + 12),
    );
    c.replicas[&1].node.start_unit(ua, &VOTERS).unwrap();
    let la = c.leader(ua);
    // `ub` was never written: no replica holds it yet. The first try
    // starts it here; a retry commits on both.
    let (ka, kb) = (key(TENANT + 11, 1), key(TENANT + 12, 1));
    let e = &c.replicas[&la].engine;
    until("the commit across a led unit and a new one", || {
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
        t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
        if let Ok(c) = t.commit() {
            c.wait().ok()
        } else {
            std::thread::sleep(Duration::from_millis(20));
            None
        }
    });
    assert!(e.leader_epoch(ub) > 0);
}

#[test]
fn an_isolated_followers_reads_fail_fast_and_leave_the_runtime_free() {
    let c = Cluster::new("isolated-reads");
    let unit = Engine::unit_of_tenant(TENANT + 9);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let k = key(TENANT + 9, 1);
    c.put(unit, &k, b"v");
    assert_eq!(c.get(f, &k).as_deref(), Some(&b"v"[..]));
    c.net.isolate(f, true);
    let e = c.replicas[&f].engine.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let t2 = Arc::clone(&ticks);
    rt.spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            t2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });
    // A read on the runtime's only worker waits for a leader it can't
    // reach; the runtime's other tasks keep running meanwhile.
    let read = {
        let (e, k) = (e.clone(), k.clone());
        move || {
            e.begin(TxnOptions::default())
                .and_then(|mut t| t.get_snapshot(&k))
        }
    };
    let started = Instant::now();
    let r = rt.block_on(rt.spawn({
        let read = read.clone();
        async move { read() }
    }));
    let waited = started.elapsed();
    let err = r.unwrap().expect_err("no leader is reachable");
    assert_eq!(
        err.code(),
        Some(sprig_common::SprigCode::ShardMoved),
        "{err}"
    );
    let n = ticks.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        n * 10 * 2 >= u64::try_from(waited.as_millis()).unwrap(),
        "the runtime stalled: {n} ticks in {waited:?}"
    );
    // The next read fails at once instead of waiting again.
    let started = Instant::now();
    let r = rt.block_on(rt.spawn({
        let read = read.clone();
        async move { read() }
    }));
    assert!(r.unwrap().is_err());
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "{:?}",
        started.elapsed()
    );
    // Back in touch, reads work again.
    c.net.isolate(f, false);
    until("reads on the follower again", || {
        rt.block_on(rt.spawn({
            let read = read.clone();
            async move { read() }
        }))
        .unwrap()
        .ok()
        .flatten()
    });
    rt.shutdown_background();
}
