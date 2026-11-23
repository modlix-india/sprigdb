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
}

type Record = (u64, u32, u64, Vec<u8>, Option<Vec<u8>>);

impl Cluster {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("sprig-chg-repl-{}", std::process::id()));
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
