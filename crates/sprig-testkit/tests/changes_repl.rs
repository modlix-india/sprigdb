//! The change log on a replicated unit: three engines in one process,
//! replicated through Raft over an in-memory network, each with its own
//! change log. Every replica writes the same change log (same entries,
//! records, commit timestamps, keys and values), across a leader change
//! and a lost replica replaced from a unit snapshot (which carries the
//! unit's change-log files).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::time::SystemClock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};
use sprig_streams::changes::{ChangeConfig, ChangeLog, Pos};

const TENANT: u64 = 5151;
const VOTERS: [u64; 3] = [1, 2, 3];
const WAIT: Duration = Duration::from_secs(20);

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

fn key(i: u64) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant: TENANT,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&i.to_be_bytes());
    k
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

struct Replica {
    engine: Engine,
    node: EngineNode,
    changes: ChangeLog,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Cluster {
    dir: PathBuf,
    net: Arc<MemNetwork>,
    replicas: BTreeMap<u64, Replica>,
    /// A stopped replica loses what its store held only in memory (the
    /// store isn't flushed at close), as after `kill -9`: the engine
    /// applies with the store's write-ahead log off, so the store comes
    /// back behind the Raft log.
    crash: bool,
}

type Record = (u64, u32, u64, Vec<u8>, Option<Vec<u8>>);

impl Cluster {
    fn new() -> Self {
        Self::with("sprig-chg-repl", false)
    }

    fn with(name: &str, crash: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
            crash,
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
        cfg.rocks.avoid_flush_on_close = self.crash;
        // A restarted replica's previous engine may still be closing (its
        // change log's flusher held it): its store's lock goes with it.
        let deadline = Instant::now() + Duration::from_secs(10);
        let engine = loop {
            match Engine::open(cfg.clone()) {
                Ok(e) => break e,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("engine of replica {id}: {e}"),
            }
        };
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
        let changes = ChangeLog::open(&engine, d.join("changes"), ChangeConfig::default()).unwrap();
        self.net.register(node.node());
        let thread = Some(node.spawn().unwrap());
        self.replicas.insert(
            id,
            Replica {
                engine,
                node,
                changes,
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

    fn leader(&self, unit: UnitId) -> u64 {
        until("a leader", || {
            self.replicas
                .iter()
                .find(|(_, r)| r.engine.leader_epoch(unit) > 0)
                .map(|(id, _)| *id)
        })
    }

    fn put(&self, unit: UnitId, i: u64, v: &[u8]) {
        until("an acknowledged commit", || {
            let l = self.leader(unit);
            let e = &self.replicas[&l].engine;
            let mut t = e.begin(TxnOptions::default()).ok()?;
            t.put(key(i), v.to_vec()).ok()?;
            t.put(key(10_000 + i), v.to_vec()).ok()?;
            let c = t.commit().ok()?;
            c.wait().ok()?;
            Some(())
        });
    }

    fn records(r: &Replica, unit: UnitId) -> Vec<Record> {
        let mut rd = r.changes.reader(unit.0, Pos::default(), None).unwrap();
        let mut out = Vec::new();
        loop {
            let p = rd.read(1 << 20).unwrap();
            out.extend(
                p.changes
                    .into_iter()
                    .map(|c| (c.index, c.seq, c.cts, c.key, c.value)),
            );
            if p.at_end {
                return out;
            }
        }
    }

    /// Waits until every replica's change log holds `n` records, then
    /// checks they are the same.
    fn same_logs(&self, unit: UnitId, n: usize) -> Vec<Record> {
        until("every replica's change log", || {
            let all: Vec<Vec<Record>> = self
                .replicas
                .values()
                .map(|r| Self::records(r, unit))
                .collect();
            (all.iter().all(|l| l.len() == n) && all.windows(2).all(|w| w[0] == w[1]))
                .then(|| all[0].clone())
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
fn every_replica_writes_the_same_change_log() {
    let mut c = Cluster::new();
    let unit = Engine::unit_of_tenant(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    for i in 0..20u64 {
        c.put(unit, i, b"a");
    }
    let first = c.same_logs(unit, 40);
    // A leader change in the middle.
    let l = c.leader(unit);
    let next = VOTERS.into_iter().find(|id| *id != l).unwrap();
    until("the leader transfer", || {
        let _ = c.replicas[&l].node.node().transfer_leader(unit.0, next);
        std::thread::sleep(Duration::from_millis(20));
        (c.replicas[&next].engine.leader_epoch(unit) > 0).then_some(())
    });
    for i in 20..40u64 {
        c.put(unit, i, b"b");
    }
    let log = c.same_logs(unit, 80);
    assert_eq!(&log[..40], &first[..]);
    for w in log.windows(2) {
        assert!((w[0].0, w[0].1) < (w[1].0, w[1].1));
    }
    // A lost replica, replaced from a snapshot after every log is trimmed:
    // the new replica gets the change log with the unit's keys.
    for r in c.replicas.values() {
        r.changes.flush().unwrap();
        r.engine.checkpoint().unwrap();
    }
    let dead = VOTERS.into_iter().find(|id| *id != c.leader(unit)).unwrap();
    c.kill(dead);
    c.start(4);
    let l = c.leader(unit);
    c.replicas[&l]
        .node
        .replace_replica(unit, dead, 4, WAIT)
        .unwrap();
    for i in 40..50u64 {
        c.put(unit, i, b"c");
    }
    let log2 = c.same_logs(unit, 100);
    assert_eq!(&log2[..80], &log[..]);
}

/// The records `n` puts write (each puts two keys).
fn records_of(n: u64) -> usize {
    usize::try_from(2 * n).unwrap()
}

/// Every committed change stays in every replica's change log across a
/// crash of a replica (follower, then leader) under load. The engine applies
/// with the store's write-ahead log off, so a replica comes back with its
/// store behind its Raft log, and its change log's files ahead of the
/// store: the records the change log cuts back at open must come again
/// from the Raft log, and a reader that had read past them goes on from
/// its place.
#[test]
fn a_replicas_crash_keeps_every_change() {
    let mut c = Cluster::with("sprig-chg-crash", true);
    let unit = Engine::unit_of_tenant(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let mut n = 0u64;
    for _ in 0..30 {
        c.put(unit, n, b"a");
        n += 1;
    }
    c.same_logs(unit, records_of(n));
    for round in 0..2 {
        let l = c.leader(unit);
        let victim = if round == 0 {
            VOTERS.into_iter().find(|id| *id != l).unwrap()
        } else {
            l
        };
        // The victim's change log has made its records durable (its
        // manifest is ahead of the store, which holds nothing durable of
        // the unit's applies since its last checkpoint); a consumer has
        // read up to there.
        c.replicas[&victim].changes.flush().unwrap();
        let read = Cluster::records(&c.replicas[&victim], unit);
        assert_eq!(read.len(), records_of(n), "round {round}");
        let last = read.last().map(|r| Pos::after_entry(r.0)).unwrap();
        let top = c.replicas[&victim].engine.last_applied_of(unit);
        c.kill(victim);
        for _ in 0..10 {
            c.put(unit, n, b"b");
            n += 1;
        }
        c.start(victim);
        for _ in 0..10 {
            c.put(unit, n, b"c");
            n += 1;
        }
        let all = c.same_logs(unit, records_of(n));
        // The store came back behind: the change log had to cut and
        // replay (otherwise this round tested nothing).
        let r = &c.replicas[&victim];
        assert!(
            r.engine.last_applied_of(unit) > top,
            "round {round}: the victim applied the later entries"
        );
        // The consumer's place still holds: everything after it, once.
        let mut rd = r.changes.reader(unit.0, last, None).unwrap();
        let mut after = Vec::new();
        loop {
            let p = rd.read(1 << 20).unwrap();
            after.extend(
                p.changes
                    .into_iter()
                    .map(|c| (c.index, c.seq, c.cts, c.key, c.value)),
            );
            if p.at_end {
                break;
            }
        }
        let want: Vec<Record> = all
            .iter()
            .filter(|x| Pos::after_entry(x.0) > last)
            .cloned()
            .collect();
        assert_eq!(
            after, want,
            "round {round}: a consumer's place after the crash"
        );
    }
}

/// A replica that gets the unit from a snapshot keeps the unit's change
/// history through a crash right after the restore. The restore counts as
/// done once the engine's store is flushed; the change log's part of the
/// snapshot must be durable by then too, or a crash before its next flush
/// leaves the replica without the history before the snapshot (readers
/// there get "history lost"), records its leader still serves.
#[test]
fn a_restored_replica_keeps_the_change_history_through_a_crash() {
    let mut c = Cluster::with("sprig-chg-snapcrash", true);
    let unit = Engine::unit_of_tenant(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    for i in 0..20u64 {
        c.put(unit, i, b"a");
    }
    c.same_logs(unit, 40);
    // Every log is trimmed: a new replica needs the snapshot.
    for r in c.replicas.values() {
        r.changes.flush().unwrap();
        r.engine.checkpoint().unwrap();
    }
    let dead = VOTERS.into_iter().find(|id| *id != c.leader(unit)).unwrap();
    c.kill(dead);
    c.start(4);
    let l = c.leader(unit);
    c.replicas[&l]
        .node
        .replace_replica(unit, dead, 4, WAIT)
        .unwrap();
    for i in 20..25u64 {
        c.put(unit, i, b"b");
    }
    let before = c.same_logs(unit, 50);
    // Node 4 crashes before its change log's next flush.
    c.kill(4);
    c.start(4);
    for i in 25..30u64 {
        c.put(unit, i, b"c");
    }
    let after = c.same_logs(unit, 60);
    assert_eq!(&after[..50], &before[..]);
}
