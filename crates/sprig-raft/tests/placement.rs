//! Units placed per unit: five engines (RocksDB stores on disk), each with
//! its Raft node, over an in-process network, where each tenant unit lives
//! on the three nodes its placement names. Members start the unit's group
//! with exactly those voters; the other nodes never hold it, refuse its
//! writes and reads as retryable, and keep not holding it across restarts.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::many_single_char_names
)]

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::SprigCode;
use sprig_common::time::SystemClock;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{
    EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, PlacementLookup, RaftConfig,
    UnitPlacement,
};

const NODES: [u64; 5] = [1, 2, 3, 4, 5];
const WAIT: Duration = Duration::from_secs(20);

/// The placements every node sees (as a directory watch would fill it).
type Placements = Arc<RwLock<HashMap<u64, Vec<u64>>>>;

fn config(placements: &Placements) -> RaftConfig {
    let p = Arc::clone(placements);
    RaftConfig {
        tick: Duration::from_millis(10),
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        // The cluster's fixed voters must not matter for placed units.
        static_voters: NODES.to_vec(),
        placement: Some(PlacementLookup::new(move |g| {
            p.read()
                .unwrap()
                .get(&g)
                .map_or(UnitPlacement::Unknown, |v| UnitPlacement::Voters(v.clone()))
        })),
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
    placements: Placements,
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
        let dir = std::env::temp_dir().join(format!("sprig-place-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            placements: Arc::new(RwLock::new(HashMap::new())),
            replicas: BTreeMap::new(),
        };
        for id in NODES {
            c.start(id);
        }
        c
    }

    fn place(&self, tenant: u64, voters: &[u64]) {
        self.placements
            .write()
            .unwrap()
            .insert(Engine::unit_of_tenant(tenant).0, voters.to_vec());
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
                config: config(&self.placements),
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

    /// Commits `k -> v` through node `via` once (no retry).
    fn put_on(&self, via: u64, k: &[u8], v: &[u8]) -> sprig_common::Result<()> {
        let e = &self.replicas[&via].engine;
        let mut t = e.begin(TxnOptions::default())?;
        t.put(k.to_vec(), Bytes::copy_from_slice(v))?;
        t.commit()?.wait()
    }

    /// Commits `k -> v`, trying the unit's members in turn as a client
    /// would until one acknowledges; returns that node.
    fn put(&self, members: &[u64], k: &[u8], v: &[u8]) -> u64 {
        until("an acknowledged commit", || {
            members
                .iter()
                .copied()
                .find(|id| self.replicas.contains_key(id) && self.put_on(*id, k, v).is_ok())
        })
    }

    fn get(&self, id: u64, k: &[u8]) -> sprig_common::Result<Option<Bytes>> {
        let e = &self.replicas[&id].engine;
        let mut t = e.begin(TxnOptions::default())?;
        t.get_snapshot(k)
    }

    fn holds(&self, id: u64, unit: UnitId) -> bool {
        self.replicas[&id].node.node().status(unit.0).is_some()
    }

    /// Waits until the listed replicas' stores hold the same unit.
    fn converged(&self, unit: UnitId, on: &[u64]) -> sprig_engine::snapshot::SpanChecksum {
        until("replicas to converge", || {
            let sums: Vec<_> = on
                .iter()
                .map(|id| self.replicas[id].engine.unit_checksum(unit).unwrap())
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

const A: u64 = 7001;
const B: u64 = 7002;
const ON_A: [u64; 3] = [1, 2, 3];
const ON_B: [u64; 3] = [3, 4, 5];

#[test]
fn units_start_only_on_the_nodes_they_are_placed_on() {
    let c = Cluster::new("start");
    c.place(A, &ON_A);
    c.place(B, &ON_B);
    let (ua, ub) = (Engine::unit_of_tenant(A), Engine::unit_of_tenant(B));
    for i in 0..30u64 {
        c.put(&ON_A, &key(A, i), format!("a{i}").as_bytes());
        c.put(&ON_B, &key(B, i), format!("b{i}").as_bytes());
    }
    assert_eq!(c.converged(ua, &ON_A).entries, 30);
    assert_eq!(c.converged(ub, &ON_B).entries, 30);
    for id in NODES {
        assert_eq!(c.holds(id, ua), ON_A.contains(&id), "node {id}, unit A");
        assert_eq!(c.holds(id, ub), ON_B.contains(&id), "node {id}, unit B");
        let st = c.replicas[&id].node.node().status(ua.0);
        if let Some(st) = st {
            assert_eq!(st.voters, ON_A.to_vec(), "node {id}'s voters of A");
        }
    }
    // Nodes outside a unit's placement keep none of its rows.
    for id in [4, 5] {
        assert_eq!(c.replicas[&id].engine.unit_checksum(ua).unwrap().entries, 0);
    }
    for id in [1, 2] {
        assert_eq!(c.replicas[&id].engine.unit_checksum(ub).unwrap().entries, 0);
    }
}

#[test]
fn a_node_outside_the_placement_refuses_writes_and_reads_as_retryable() {
    let c = Cluster::new("outside");
    c.place(A, &ON_A);
    c.put(&ON_A, &key(A, 1), b"x");
    let err = c.put_on(4, &key(A, 2), b"y").unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::ShardMoved), "{err}");
    let err = c.get(5, &key(A, 1)).unwrap_err();
    assert_eq!(err.code(), Some(SprigCode::ShardMoved), "{err}");
    // A member that doesn't lead reads the latest commit through its
    // barrier.
    let leader = c.put(&ON_A, &key(A, 3), b"z");
    let follower = ON_A.into_iter().find(|id| *id != leader).unwrap();
    assert_eq!(
        c.get(follower, &key(A, 3)).unwrap().as_deref(),
        Some(&b"z"[..])
    );
    assert!(!c.holds(4, Engine::unit_of_tenant(A)));
}

#[test]
fn a_unit_whose_placement_is_unknown_starts_nowhere_until_it_is_placed() {
    let c = Cluster::new("unknown");
    let unit = Engine::unit_of_tenant(A);
    for id in NODES {
        let err = c.put_on(id, &key(A, 1), b"x").unwrap_err();
        assert_eq!(err.code(), Some(SprigCode::ShardMoved), "{err}");
    }
    std::thread::sleep(Duration::from_millis(200));
    for id in NODES {
        assert!(!c.holds(id, unit), "node {id} started an unplaced unit");
    }
    let on = [2, 4, 5];
    c.place(A, &on);
    c.put(&on, &key(A, 1), b"x");
    assert_eq!(c.converged(unit, &on).entries, 1);
    for id in NODES {
        assert_eq!(c.holds(id, unit), on.contains(&id), "node {id}");
    }
}

#[test]
fn members_keep_a_unit_across_failover_and_restarts_and_others_never_get_it() {
    let mut c = Cluster::new("restart");
    c.place(A, &ON_A);
    let unit = Engine::unit_of_tenant(A);
    let mut leader = 0;
    for i in 0..20u64 {
        leader = c.put(&ON_A, &key(A, i), format!("a{i}").as_bytes());
    }
    // The leader dies; the other two members carry on.
    c.kill(leader);
    let alive: Vec<u64> = ON_A.into_iter().filter(|id| *id != leader).collect();
    for i in 20..40u64 {
        c.put(&alive, &key(A, i), format!("b{i}").as_bytes());
    }
    // Restarted, it catches up; a restarted outsider still holds nothing.
    c.start(leader);
    c.kill(4);
    c.start(4);
    assert_eq!(c.converged(unit, &ON_A).entries, 40);
    assert_eq!(
        c.get(leader, &key(A, 39)).unwrap().as_deref(),
        Some(&b"b39"[..])
    );
    std::thread::sleep(Duration::from_millis(200));
    for id in [4, 5] {
        assert!(!c.holds(id, unit), "node {id}");
    }
}
