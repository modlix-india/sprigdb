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
use sprig_engine::txn::{CrossFault, CrossStage, TxnOptions};
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
    /// Each node's Raft settings.
    config: fn(u64) -> RaftConfig,
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
        Self::with_config(name, |_| fast())
    }

    /// [`Cluster::new`] with Raft settings chosen per node.
    fn with_config(name: &str, config: fn(u64) -> RaftConfig) -> Self {
        let dir = std::env::temp_dir().join(format!("sprig-repl-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
            config,
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
                config: (self.config)(id),
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

    /// [`Cluster::get`] that returns a failed read (retryable while a
    /// unit's leadership is moving) instead of panicking: for reads inside
    /// [`until`].
    fn try_get(&self, id: u64, k: &[u8]) -> sprig_common::Result<Option<Bytes>> {
        let e = &self.replicas[&id].engine;
        e.begin(TxnOptions::default())?.get_snapshot(k)
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
    // The cut-off leader stepped down without a successor it knew of, so
    // its commit failed with an unknown outcome (it can't tell whether a
    // majority holds the entry); had it heard of the new leader first, the
    // commit would have failed as retryable (1213): the new leader's entry
    // replaced it.
    let Err(err) = ta.wait() else {
        panic!("the lost commit fails");
    };
    assert!(
        is_deadlock(&err)
            || err.code() == Some(sprig_common::SprigCode::ShardMoved)
            || err.code() == Some(sprig_common::SprigCode::CommitOutcomeUnknown),
        "{err}"
    );
    assert!(gate.wait().is_err(), "the reply that read it fails too");
    c.converged(unit);
    for id in VOTERS {
        assert_eq!(c.get(id, &k).as_deref(), Some(&b"v1"[..]));
    }
}

/// A commit waiting on a leader that is cut off from the other replicas
/// fails once the leader steps down (it no longer hears from a majority),
/// with an unknown outcome, instead of waiting until the cut heals: a
/// client on a long-lived connection gets an answer in about an election
/// timeout and reconnects to the new leader. A write proposed after the
/// step-down is refused at once (retryable).
#[test]
fn a_cut_off_leaders_pending_commit_fails_before_the_heal() {
    let c = Cluster::new("cutoff");
    let unit = Engine::unit_of_tenant(TENANT + 9);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let k = key(TENANT + 9, 1);
    c.put(unit, &k, b"v0");
    let l = c.leader(unit);
    let e = c.replicas[&l].engine.clone();
    c.net.isolate(l, true);
    let mut a = e.begin(TxnOptions::default()).unwrap();
    a.put(k.clone(), Bytes::from_static(b"cut")).unwrap();
    let ta = a
        .commit()
        .expect("the isolated leader still takes the proposal");
    let cut = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let r = ta.wait();
        let _ = tx.send(r);
    });
    let got = rx.recv_timeout(Duration::from_secs(5));
    let took = cut.elapsed();
    // The client's next statement there (a read) fails at once too: the
    // node can't hear a majority, so no leader can confirm what it holds.
    let read_took = got.is_ok().then(|| {
        let t0 = Instant::now();
        let r = e
            .begin(TxnOptions::default())
            .and_then(|mut t| t.get_snapshot(&k));
        assert!(r.is_err(), "a read on the cut-off node succeeded: {r:?}");
        t0.elapsed()
    });
    c.net.isolate(l, false);
    waiter.join().unwrap();
    let r = got.expect("the cut-off leader's commit was still waiting after 5 s");
    let Err(err) = r else {
        panic!("a commit no majority acknowledged succeeded");
    };
    assert_eq!(
        err.code(),
        Some(sprig_common::SprigCode::CommitOutcomeUnknown),
        "{err}"
    );
    assert!(took < Duration::from_secs(3), "answered after {took:?}");
    assert!(
        read_took.is_some_and(|t| t < Duration::from_secs(1)),
        "the next read waited {read_took:?}"
    );
    // Once healed, the replicas agree, whatever became of the write.
    c.put(unit, &key(TENANT + 9, 2), b"after");
    c.converged(unit);
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
            (c.try_get(id, &ka).ok()?.as_deref() == Some(&b"a"[..])
                && c.try_get(id, &kb).ok()?.as_deref() == Some(&b"b"[..]))
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

/// A leader cut off from the other replicas must not answer reads from its
/// own state once its lease has run out. The others elect a leader and
/// commit a new value; the old leader, which hasn't noticed anything yet
/// (it checks for a lost quorum on its own, long, election timeout), still
/// leads the unit for its engine. Its read fails as retryable; it never
/// returns the value the new leader replaced.
#[test]
fn an_isolated_leader_does_not_serve_stale_reads() {
    let c = Cluster::with_config("stale-leader", |id| RaftConfig {
        election_ticks: if id == 1 { 300 } else { 10 },
        ..fast()
    });
    let unit = Engine::unit_of_tenant(TENANT + 9);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let k = key(TENANT + 9, 1);
    c.put(unit, &k, b"v0");
    until("node 1 to lead", || {
        let l = c.leader(unit);
        if l != 1 {
            let _ = c.replicas[&l].node.node().transfer_leader(unit.0, 1);
            std::thread::sleep(Duration::from_millis(20));
        }
        (c.replicas[&1].engine.leader_epoch(unit) > 0).then_some(())
    });
    c.put(unit, &k, b"v0");
    // In touch with a majority, the leader reads from its own state.
    assert_eq!(c.get(1, &k).as_deref(), Some(&b"v0"[..]));

    c.net.isolate(1, true);
    let new = until("a new leader", || {
        c.replicas
            .iter()
            .find(|(id, r)| **id != 1 && r.engine.leader_epoch(unit) > 0)
            .map(|(id, _)| *id)
    });
    let mut t = c.replicas[&new]
        .engine
        .begin(TxnOptions::default())
        .unwrap();
    t.put(k.clone(), Bytes::from_static(b"v1")).unwrap();
    t.commit().unwrap().wait().unwrap();

    // The write is acknowledged; the old leader still believes it leads.
    let old = &c.replicas[&1].engine;
    assert!(
        old.leader_epoch(unit) > 0,
        "the test needs an old leader that hasn't noticed yet"
    );
    let mut r = old.begin(TxnOptions::default()).unwrap();
    match r.get_snapshot(&k) {
        Ok(v) => panic!(
            "the isolated leader answered a read with {v:?} after the new leader acknowledged v1"
        ),
        Err(e) => assert_eq!(
            e.code(),
            Some(sprig_common::SprigCode::ShardMoved),
            "retryable: {e}"
        ),
    }
    drop(r);

    // Back in touch, it follows the new leader and reads the new value.
    c.net.isolate(1, false);
    until("the old leader to follow", || {
        (old.leader_epoch(unit) == 0).then_some(())
    });
    until("the new value on the old leader", || {
        let mut t = old.begin(TxnOptions::default()).ok()?;
        (t.get_snapshot(&k).ok()?.as_deref() == Some(&b"v1"[..])).then_some(())
    });
}

/// Starts `ua` and `ub` and brings both under one node's leadership;
/// returns that node.
fn lead_both(c: &Cluster, ua: UnitId, ub: UnitId) -> u64 {
    for u in [ua, ub] {
        c.replicas
            .values()
            .next()
            .unwrap()
            .node
            .start_unit(u, &VOTERS)
            .unwrap();
    }
    let l = c.leader(ua);
    until("one node leading both units", || {
        if c.replicas[&l].engine.leader_epoch(ua) > 0 && c.replicas[&l].engine.leader_epoch(ub) > 0
        {
            return Some(());
        }
        for u in [ua, ub] {
            let other = c.leader(u);
            if other != l {
                let _ = c.replicas[&other].node.node().transfer_leader(u.0, l);
            }
        }
        std::thread::sleep(Duration::from_millis(20));
        None
    });
    l
}

/// Drops every participant's resolution on its way out of the coordinator,
/// as if the node had stopped right after the commit was acknowledged.
fn drop_resolutions(e: &Engine) {
    e.txn_shared()
        .set_cross_unit_faults(Some(Arc::new(|s| match s {
            CrossStage::Resolve(_) => CrossFault::Drop,
            _ => CrossFault::Go,
        })));
}

/// An acknowledged commit across two units stays whole when the node that
/// ran it dies before the participant's resolution reached that unit's
/// log: the commit point is in the anchor's log, the participant holds
/// only its prepare record, and whichever replicas lead the units next
/// must finish it. Both halves are then readable on the new leaders.
#[test]
fn an_acknowledged_commit_across_units_survives_its_coordinators_death() {
    let mut c = Cluster::new("cross-death");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 20),
        Engine::unit_of_tenant(TENANT + 21),
    );
    let (ka, kb) = (key(TENANT + 20, 1), key(TENANT + 21, 1));
    let l = lead_both(&c, ua, ub);
    let e = c.replicas[&l].engine.clone();
    drop_resolutions(&e);
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    // Acknowledged: the reply waits for the commit point only.
    t.commit().unwrap().wait().unwrap();
    drop(e);
    c.kill(l);
    for (u, k, v) in [(ua, &ka, &b"a"[..]), (ub, &kb, &b"b"[..])] {
        until("both halves on the new leaders", || {
            let n = c
                .replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(u) > 0)
                .map(|(id, _)| *id)?;
            for r in c.replicas.values() {
                r.engine.resolve_cross_unit();
            }
            let e = &c.replicas[&n].engine;
            let mut t = e.begin(TxnOptions::default()).ok()?;
            (t.get_snapshot(k).ok()?.as_deref() == Some(v)).then_some(())
        });
    }
    // And the replicas agree.
    c.converged(ua);
    c.converged(ub);
}

/// The same with no crash: the participant's leadership is handed to
/// another node (a graceful transfer) while its resolution is still
/// missing. The new leader finishes the commit from the anchor's record.
#[test]
fn a_commit_across_units_is_finished_after_the_participants_leader_moves() {
    let c = Cluster::new("cross-move");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 22),
        Engine::unit_of_tenant(TENANT + 23),
    );
    let (ka, kb) = (key(TENANT + 22, 1), key(TENANT + 23, 1));
    let l = lead_both(&c, ua, ub);
    let e = c.replicas[&l].engine.clone();
    drop_resolutions(&e);
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    t.commit().unwrap().wait().unwrap();
    // The participant (the higher unit: the anchor is the lower on a tie)
    // moves to another node; the coordinator never resolves it.
    let to = VOTERS.into_iter().find(|id| *id != l).unwrap();
    until("the participant's leader to move", || {
        let _ = c.replicas[&l].node.node().transfer_leader(ub.0, to);
        std::thread::sleep(Duration::from_millis(20));
        (c.replicas[&to].engine.leader_epoch(ub) > 0).then_some(())
    });
    until("the participant's half on its new leader", || {
        for r in c.replicas.values() {
            r.engine.resolve_cross_unit();
        }
        let mut t = c.replicas[&to].engine.begin(TxnOptions::default()).ok()?;
        (t.get_snapshot(&kb).ok()?.as_deref() == Some(&b"b"[..])).then_some(())
    });
    assert_eq!(c.get(l, &ka).as_deref(), Some(&b"a"[..]));
    c.converged(ub);
}

/// A replica that doesn't lead a unit doesn't answer a read of it while a
/// commit across units is prepared there and not finished. The commit is
/// acknowledged once its commit point is durable in the anchor, so a read
/// that starts afterwards must see it; and when the participant is
/// finished, its versions carry a timestamp below versions the replica may
/// already hold. The read waits (here: fails as retryable, the resolution
/// being held back), and reads the new value once the participant is
/// finished.
#[test]
fn a_follower_read_waits_for_a_commit_across_units_prepared_on_its_unit() {
    let c = Cluster::new("cross-follow");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 24),
        Engine::unit_of_tenant(TENANT + 25),
    );
    let (ka, kb) = (key(TENANT + 24, 1), key(TENANT + 25, 1));
    let l = lead_both(&c, ua, ub);
    let e = c.replicas[&l].engine.clone();
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"old")).unwrap();
    t.commit().unwrap().wait().unwrap();
    drop_resolutions(&e);
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    t.commit().unwrap().wait().unwrap();
    // The leader shows the acknowledged commit at once.
    assert_eq!(c.get(l, &kb).as_deref(), Some(&b"b"[..]));
    // A follower holds the prepare record and not yet the resolution.
    let f = VOTERS.into_iter().find(|id| *id != l).unwrap();
    let fe = &c.replicas[&f].engine;
    let mut r = fe.begin(TxnOptions::default()).unwrap();
    match r.get_snapshot(&kb) {
        Ok(v) => assert_eq!(
            v.as_deref(),
            Some(&b"b"[..]),
            "a follower read missed an acknowledged commit across units"
        ),
        Err(err) => assert_eq!(
            err.code(),
            Some(sprig_common::SprigCode::ShardMoved),
            "retryable: {err}"
        ),
    }
    drop(r);
    // The coordinator's node finishes the participant; the follower reads it.
    e.txn_shared().set_cross_unit_faults(None);
    until("the follower to read the commit", || {
        let mut t = fe.begin(TxnOptions::default()).ok()?;
        (t.get_snapshot(&kb).ok()?.as_deref() == Some(&b"b"[..])).then_some(())
    });
}

/// A commit across units whose coordinator dies between the prepare
/// records and the commit point is aborted by the anchor's next leader
/// (through the anchor's log), the participant's next leader then drops
/// its prepare record, and the rows it had locked take new writes.
#[test]
fn a_commit_across_units_without_a_commit_point_is_aborted_by_the_next_leaders() {
    let mut c = Cluster::new("cross-abort");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 26),
        Engine::unit_of_tenant(TENANT + 27),
    );
    let (ka, kb) = (key(TENANT + 26, 1), key(TENANT + 27, 1));
    let l = lead_both(&c, ua, ub);
    let e = c.replicas[&l].engine.clone();
    for k in [&ka, &kb] {
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.put(k.clone(), Bytes::from_static(b"old")).unwrap();
        t.commit().unwrap().wait().unwrap();
    }
    e.txn_shared()
        .set_cross_unit_faults(Some(Arc::new(|s| match s {
            CrossStage::Prepared => CrossFault::CoordinatorLost,
            _ => CrossFault::Go,
        })));
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    assert!(
        t.commit().is_err(),
        "the coordinator stops before its commit point"
    );
    drop(e);
    c.kill(l);
    // Neither half ever shows, and both rows take new writes on the new
    // leaders once the transaction is aborted there.
    for (u, k) in [(ua, &ka), (ub, &kb)] {
        until("a new write past the aborted transaction", || {
            let n = c
                .replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(u) > 0)
                .map(|(id, _)| *id)?;
            let e = &c.replicas[&n].engine;
            let mut r = e.begin(TxnOptions::default()).ok()?;
            let seen = r.get_snapshot(k).ok()?;
            assert_eq!(seen.as_deref(), Some(&b"old"[..]), "an aborted half showed");
            drop(r);
            let mut t = e
                .begin(TxnOptions {
                    lock_wait_timeout: Some(Duration::from_millis(200)),
                    ..TxnOptions::default()
                })
                .ok()?;
            let row = Bytes::from(k.clone());
            t.lock_get_blocking(&row, sprig_engine::txn::LockRequest::for_update())
                .ok()?;
            t.put(k.clone(), Bytes::from_static(b"new")).ok()?;
            t.commit().ok()?.wait().ok()
        });
    }
    c.converged(ua);
    c.converged(ub);
}

/// A resolution the coordinator proposes while it is cut off (it still
/// believes it leads the participant) is lost to the next leader's log.
/// The next leader finishes the participant from the anchor's record; the
/// coordinator, back in touch, finds its own entry lost and the prepare
/// record gone, and applies nothing a second time.
#[test]
fn a_resolution_lost_to_a_leader_change_is_applied_once() {
    let c = Cluster::new("cross-lost");
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 28),
        Engine::unit_of_tenant(TENANT + 29),
    );
    let (ka, kb) = (key(TENANT + 28, 1), key(TENANT + 29, 1));
    let l = lead_both(&c, ua, ub);
    let e = c.replicas[&l].engine.clone();
    drop_resolutions(&e);
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(ka.clone(), Bytes::from_static(b"a")).unwrap();
    t.put(kb.clone(), Bytes::from_static(b"b")).unwrap();
    t.commit().unwrap().wait().unwrap();
    // Cut off, the coordinator's resolver proposes the resolution.
    c.net.isolate(l, true);
    e.txn_shared().set_cross_unit_faults(None);
    e.resolve_cross_unit();
    let new = until("a new leader of the participant", || {
        c.replicas
            .iter()
            .find(|(id, r)| **id != l && r.engine.leader_epoch(ub) > 0)
            .map(|(id, _)| *id)
    });
    until("the participant's half on its new leader", || {
        let mut t = c.replicas[&new].engine.begin(TxnOptions::default()).ok()?;
        (t.get_snapshot(&kb).ok()?.as_deref() == Some(&b"b"[..])).then_some(())
    });
    c.net.isolate(l, false);
    until("the old coordinator to follow", || {
        (e.leader_epoch(ub) == 0).then_some(())
    });
    c.converged(ub);
    c.converged(ua);
    for id in VOTERS {
        until("both halves on every replica", || {
            (c.try_get(id, &ka).ok()?.as_deref() == Some(&b"a"[..])
                && c.try_get(id, &kb).ok()?.as_deref() == Some(&b"b"[..]))
            .then_some(())
        });
    }
    // The old coordinator keeps nothing of it: the units are led elsewhere.
    until("the old coordinator to drop its entry", || {
        e.resolve_cross_unit();
        (e.txn_shared().cross_unit_open() == 0 || e.leader_epoch(ua) > 0 || e.leader_epoch(ub) > 0)
            .then_some(())
    });
}

/// Fills `unit` (of `tenant`) with about `mb` MB through its leader, in
/// commits of 1 MB.
fn fill(c: &Cluster, unit: UnitId, tenant: u64, mb: u64) {
    let value = vec![7u8; 4096];
    for batch in 0..mb {
        until("a filling commit", || {
            let l = c.leader(unit);
            let e = &c.replicas[&l].engine;
            let mut t = e.begin(TxnOptions::default()).ok()?;
            for i in 0..256 {
                t.put(key(tenant, batch * 256 + i), Bytes::copy_from_slice(&value))
                    .ok()?;
            }
            t.commit().ok()?.wait().ok()
        });
    }
}

/// Adds node `id` as a learner of `unit` through its leader.
fn add_learner(c: &Cluster, unit: UnitId, id: u64) {
    use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
    until("the learner added", || {
        let l = c.leader(unit);
        let mut s = ConfChangeSingle::default();
        s.set_change_type(ConfChangeType::AddLearnerNode);
        s.node_id = id;
        let mut cc = ConfChangeV2::default();
        cc.mut_changes().push(s);
        let slot = c.replicas[&l]
            .node
            .node()
            .change_membership(unit.0, cc)
            .ok()?;
        slot.wait_timeout(WAIT)?.ok()
    });
}

/// Copies `unit` (about `mb` MB, its log trimmed) to a new node 4 as a
/// learner, its leader sending at most `rate` bytes a second; checks that
/// the snapshot went as a file in chunks, at the pace, while the leader's
/// driver kept committing another unit's writes promptly, and that the
/// copy equals the leader's. Returns how long the copy took.
fn copy_to_a_new_replica(name: &str, mb: u64, rate: u64) -> Duration {
    let mut c = Cluster::new(name);
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 20),
        Engine::unit_of_tenant(TENANT + 21),
    );
    let l = lead_both(&c, ua, ub);
    fill(&c, ua, TENANT + 20, mb);
    // The voters trim their logs: a new replica needs a snapshot.
    let last = c.replicas[&l].node.node().committed(ua.0);
    for id in VOTERS {
        until("the log trimmed", || {
            let r = &c.replicas[&id];
            let _ = r.engine.checkpoint();
            r.node
                .node()
                .log_first_index(ua.0)
                .is_none_or(|f| f > last.saturating_sub(2))
                .then_some(())
        });
    }
    c.start(4);
    for id in VOTERS {
        c.replicas[&id].node.node().set_snapshot_rate(rate);
    }
    let before = c.replicas[&l].node.node().stats();
    let started = Instant::now();
    add_learner(&c, ua, 4);
    // Meanwhile the leader's other unit commits as usual: the snapshot is
    // built and sent off its driver thread.
    let mut worst = Duration::ZERO;
    let mut i = 0u64;
    let want = c.replicas[&l].engine.unit_checksum(ua).unwrap();
    loop {
        if c.replicas[&4].engine.unit_checksum(ua).unwrap() == want {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "the new replica never caught up"
        );
        let t0 = Instant::now();
        c.put(ub, &key(TENANT + 21, i), b"w");
        worst = worst.max(t0.elapsed());
        i += 1;
        std::thread::sleep(Duration::from_millis(20));
    }
    let took = started.elapsed();
    let after = c.replicas[&l].node.node().stats();
    let sent = after.snapshot_bytes_sent - before.snapshot_bytes_sent;
    let got = c.replicas[&4].node.node().stats().snapshot_bytes_received;
    eprintln!(
        "{name}: {mb} MB unit copied in {took:?} ({sent} bytes sent, {got} received; {i} writes to the other unit meanwhile, slowest {worst:?})"
    );
    assert!(
        after.snapshots_built > before.snapshots_built,
        "built off the driver"
    );
    assert!(sent >= mb << 20, "sent in chunks: {sent} bytes");
    assert_eq!(got, sent, "every chunk arrived once");
    assert!(
        worst < Duration::from_secs(2),
        "the leader's other unit waited {worst:?} for a commit"
    );
    if rate > 0 {
        #[allow(clippy::cast_precision_loss)]
        let floor = Duration::from_secs_f64(sent as f64 / rate as f64 * 0.8);
        assert!(
            took >= floor,
            "paced: {took:?} for {sent} bytes at {rate} B/s"
        );
    }
    // The copy follows later writes.
    c.put(ua, &key(TENANT + 20, u64::MAX - 1), b"after");
    until("the copy follows the log", || {
        (c.try_get(4, &key(TENANT + 20, u64::MAX - 1))
            .ok()?
            .as_deref()
            == Some(&b"after"[..]))
        .then_some(())
    });
    took
}

/// A unit of 48 MB reaches a new replica as a snapshot built off the
/// leader's driver thread and sent in 1 MiB chunks at 16 MB/s.
#[test]
fn a_large_snapshot_goes_in_paced_chunks_off_the_driver() {
    copy_to_a_new_replica("chunks", 48, 16 << 20);
}

/// The same with a unit of several GB (`SPRIG_SNAPSHOT_TEST_MB`, default
/// 3072) and no pace (`SPRIG_SNAPSHOT_TEST_RATE`, bytes a second).
#[test]
#[ignore = "heavy: a unit of several GB on disk; run with --ignored"]
fn a_unit_of_several_gb_goes_in_chunks() {
    let mb = std::env::var("SPRIG_SNAPSHOT_TEST_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3072);
    let rate = std::env::var("SPRIG_SNAPSHOT_TEST_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    copy_to_a_new_replica("gb", mb, rate);
}

/// Node 4's settings with snapshots restored on its driver thread (the
/// default).
fn restore_on_driver(_id: u64) -> RaftConfig {
    fast()
}

/// Snapshots restored off the driver thread.
fn restore_off_driver(_id: u64) -> RaftConfig {
    RaftConfig {
        restore_off_driver: true,
        ..fast()
    }
}

/// While node 4 restores a large unit's snapshot (a 2 s restore), how long
/// it takes, at worst, to apply each of its leader's commits to another
/// unit it holds. Returns that, and whether the large unit's copy ended
/// equal to the leader's.
fn other_unit_lag_during_a_restore(name: &str, config: fn(u64) -> RaftConfig) -> Duration {
    let mut c = Cluster::with_config(name, config);
    let (ua, ub) = (
        Engine::unit_of_tenant(TENANT + 30),
        Engine::unit_of_tenant(TENANT + 31),
    );
    let l = lead_both(&c, ua, ub);
    fill(&c, ua, TENANT + 30, 8);
    let last = c.replicas[&l].node.node().committed(ua.0);
    for id in VOTERS {
        until("the log trimmed", || {
            let r = &c.replicas[&id];
            let _ = r.engine.checkpoint();
            r.node
                .node()
                .log_first_index(ua.0)
                .is_none_or(|f| f > last.saturating_sub(2))
                .then_some(())
        });
    }
    c.start(4);
    // Node 4 first holds the small unit and follows it.
    add_learner(&c, ub, 4);
    c.put(ub, &key(TENANT + 31, 0), b"w");
    let caught_up = |c: &Cluster| {
        c.replicas[&4].engine.last_applied_of(ub) >= c.replicas[&l].engine.last_applied_of(ub)
    };
    until("node 4 following the small unit", || {
        caught_up(&c).then_some(())
    });
    // Now its copy of the large unit comes as a snapshot, slow to restore.
    c.replicas[&4]
        .node
        .node()
        .set_restore_delay(Duration::from_secs(2));
    let want = c.replicas[&l].engine.unit_checksum(ua).unwrap();
    add_learner(&c, ua, 4);
    let started = Instant::now();
    let mut worst = Duration::ZERO;
    let mut i = 1u64;
    while c.replicas[&4].engine.unit_checksum(ua).unwrap() != want {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "node 4 never restored the large unit"
        );
        c.put(ub, &key(TENANT + 31, i), b"w");
        let t0 = Instant::now();
        until("node 4 applying the small unit's commit", || {
            caught_up(&c).then_some(())
        });
        worst = worst.max(t0.elapsed());
        i += 1;
        std::thread::sleep(Duration::from_millis(10));
    }
    eprintln!(
        "{name}: large unit restored on node 4 after {:?}; {i} commits to the other unit meanwhile, slowest applied there after {worst:?}",
        started.elapsed()
    );
    // The copy follows later writes.
    c.put(ua, &key(TENANT + 30, u64::MAX - 1), b"after");
    until("the copy follows the log", || {
        (c.try_get(4, &key(TENANT + 30, u64::MAX - 1))
            .ok()?
            .as_deref()
            == Some(&b"after"[..]))
        .then_some(())
    });
    worst
}

/// A follower restores a snapshot that came as a file on a thread of its
/// own: while a slow restore of one unit runs, the node goes on applying
/// another unit's commits. Restored on the driver (as before), the other
/// unit waits for the whole restore.
#[test]
fn a_follower_restores_a_snapshot_off_its_driver() {
    let before = other_unit_lag_during_a_restore("restore-on-driver", restore_on_driver);
    assert!(
        before >= Duration::from_millis(1500),
        "on the driver the other unit waits for the restore ({before:?})"
    );
    let after = other_unit_lag_during_a_restore("restore-off-driver", restore_off_driver);
    assert!(
        after < Duration::from_millis(700),
        "off the driver the other unit doesn't wait ({after:?})"
    );
}

/// Node 4's peers send it snapshots whole, as to a node of an older build.
fn whole_to_4(_id: u64) -> RaftConfig {
    RaftConfig {
        whole_snapshot_peers: vec![4],
        ..fast()
    }
}

/// A leader sends a node of an older build (one that can't take chunks) a
/// snapshot whole, inside its message: the copy arrives with no chunk.
#[test]
fn a_node_of_an_older_build_gets_snapshots_whole() {
    let mut c = Cluster::with_config("whole", whole_to_4);
    let unit = Engine::unit_of_tenant(TENANT + 32);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let l = c.leader(unit);
    fill(&c, unit, TENANT + 32, 4);
    let last = c.replicas[&l].node.node().committed(unit.0);
    for id in VOTERS {
        until("the log trimmed", || {
            let r = &c.replicas[&id];
            let _ = r.engine.checkpoint();
            r.node
                .node()
                .log_first_index(unit.0)
                .is_none_or(|f| f > last.saturating_sub(2))
                .then_some(())
        });
    }
    c.start(4);
    let before = c.replicas[&l].node.node().stats();
    add_learner(&c, unit, 4);
    let want = c.replicas[&l].engine.unit_checksum(unit).unwrap();
    until("node 4's copy", || {
        (c.replicas[&4].engine.unit_checksum(unit).unwrap() == want).then_some(())
    });
    let after = c.replicas[&l].node.node().stats();
    assert!(
        after.snapshot_bytes_sent - before.snapshot_bytes_sent >= 4 << 20,
        "the snapshot went"
    );
    assert_eq!(
        c.replicas[&4].node.node().stats().snapshot_bytes_received,
        0,
        "no chunk reached node 4"
    );
    // Once it can take chunks, the leader sends them.
    c.replicas[&l].node.node().set_whole_snapshot_peers([]);
    c.put(unit, &key(TENANT + 32, u64::MAX - 1), b"after");
    until("the copy follows the log", || {
        (c.try_get(4, &key(TENANT + 32, u64::MAX - 1))
            .ok()?
            .as_deref()
            == Some(&b"after"[..]))
        .then_some(())
    });
}
