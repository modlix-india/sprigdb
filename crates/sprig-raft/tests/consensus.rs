//! The engine's `Consensus` trait over three Raft nodes (real threads,
//! raft-engine logs on disk), against a toy engine state machine that
//! keeps its state across a node restart the way the engine's store does.

#![allow(
    clippy::unwrap_used,
    clippy::type_complexity,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sprig_common::time::SystemClock;
use sprig_common::{Result, SprigCode};
use sprig_engine::log::{Consensus, Durability, LogEntry, StateMachine, SyncMode, UnitId};
use sprig_raft::{
    EngineLogStore, MemNetwork, Node, NodeOptions, RaftConfig, RaftConsensus, RaftError,
    UnitMachine, UnitSnapshots,
};

/// The engine's stand-in: per unit, the payloads applied in order and the
/// applied index. It survives node restarts (it plays the durable store).
#[derive(Default)]
struct ToyEngine {
    units: Mutex<BTreeMap<u64, (u64, Vec<Vec<u8>>)>>,
    applies: Mutex<u64>,
}

impl StateMachine for ToyEngine {
    fn applied_index(&self, unit: UnitId) -> Result<u64> {
        Ok(self.units.lock().unwrap().get(&unit.0).map_or(0, |u| u.0))
    }

    fn apply(&self, unit: UnitId, entries: &[LogEntry]) -> Result<()> {
        let mut units = self.units.lock().unwrap();
        let u = units.entry(unit.0).or_default();
        for e in entries {
            // Entries come once, in order.
            assert!(
                e.index > u.0,
                "entry {} applied twice (at {})",
                e.index,
                u.0
            );
            u.0 = e.index;
            u.1.push(e.data.clone());
            *self.applies.lock().unwrap() += 1;
        }
        Ok(())
    }
}

impl UnitSnapshots for ToyEngine {
    fn export(&self, unit: UnitId) -> std::result::Result<Vec<u8>, RaftError> {
        let units = self.units.lock().unwrap();
        let (applied, items) = units.get(&unit.0).cloned().unwrap_or_default();
        let mut b = applied.to_le_bytes().to_vec();
        for it in items {
            b.extend_from_slice(&(it.len() as u32).to_le_bytes());
            b.extend_from_slice(&it);
        }
        Ok(b)
    }

    fn ingest(&self, unit: UnitId, index: u64, data: &[u8]) -> std::result::Result<(), RaftError> {
        let mut items = Vec::new();
        if data.len() >= 8 {
            let mut at = 8;
            while at < data.len() {
                let n = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
                items.push(data[at + 4..at + 4 + n].to_vec());
                at += 4 + n;
            }
        }
        self.units.lock().unwrap().insert(unit.0, (index, items));
        Ok(())
    }
}

fn fast() -> RaftConfig {
    RaftConfig {
        tick: Duration::from_millis(10),
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        ..RaftConfig::default()
    }
}

struct Member {
    consensus: Arc<RaftConsensus>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Cluster {
    dir: PathBuf,
    net: Arc<MemNetwork>,
    engines: BTreeMap<u64, Arc<ToyEngine>>,
    members: BTreeMap<u64, Member>,
}

const WAIT: Duration = Duration::from_secs(10);

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
        let dir = std::env::temp_dir().join(format!("sprig-raft-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            engines: BTreeMap::new(),
            members: BTreeMap::new(),
        };
        for id in 1..=3 {
            c.engines.insert(id, Arc::new(ToyEngine::default()));
            c.start(id);
        }
        c
    }

    fn start(&mut self, id: u64) {
        let store = EngineLogStore::open(self.dir.join(format!("n{id}")), SyncMode::Full, None)
            .expect("log store");
        let engine = Arc::clone(&self.engines[&id]);
        let node = Node::open(NodeOptions {
            id,
            config: fast(),
            store,
            transport: self.net.transport(id),
            clock: Arc::new(SystemClock),
            machines: Arc::new(move |g| {
                let sm: Arc<dyn StateMachine> = Arc::clone(&engine) as Arc<dyn StateMachine>;
                let snaps: Arc<dyn UnitSnapshots> = Arc::clone(&engine) as Arc<dyn UnitSnapshots>;
                Box::new(UnitMachine::new(UnitId(g), sm, snaps))
                    as Box<dyn sprig_raft::StateMachine>
            }),
            observer: None,
            seed: id,
        })
        .expect("open");
        self.net.register(&node);
        let thread = Some(node.spawn().expect("spawn"));
        self.members.insert(
            id,
            Member {
                consensus: Arc::new(RaftConsensus::new(node)),
                thread,
            },
        );
    }

    fn stop(&mut self, id: u64) {
        let mut m = self.members.remove(&id).expect("member");
        self.net.unregister(id);
        m.consensus.node().close();
        if let Some(t) = m.thread.take() {
            t.join().unwrap();
        }
    }

    fn c(&self, id: u64) -> &RaftConsensus {
        &self.members[&id].consensus
    }

    fn leader(&self, unit: u64) -> u64 {
        until("a leader", || {
            self.members.iter().find_map(|(id, m)| {
                m.consensus
                    .node()
                    .status(unit)
                    .filter(|s| s.is_leader && s.committed >= s.last_index)
                    .map(|_| *id)
            })
        })
    }

    fn propose(&self, unit: u64, payload: &[u8]) -> u64 {
        until("a durable proposal", || {
            let l = self.leader(unit);
            let t = self
                .c(l)
                .propose(UnitId(unit), payload.to_vec(), Durability::Strict)
                .ok()?;
            t.wait().ok()?;
            t.wait_applied().ok()?;
            Some(t.index())
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let ids: Vec<u64> = self.members.keys().copied().collect();
        for id in ids {
            self.stop(id);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn create(c: &Cluster, units: &[u64]) {
    for u in units {
        for id in 1..=3 {
            let s = c.c(id).node().create_group(*u, &[1, 2, 3]).unwrap();
            assert_eq!(s.wait_timeout(WAIT), Some(Ok(())));
        }
    }
}

#[test]
fn proposals_commit_on_every_replica() {
    let c = Cluster::new("commit");
    create(&c, &[1, 2, 3, 4]);
    let mut last = BTreeMap::new();
    for i in 0..40u64 {
        let unit = 1 + i % 4;
        let idx = c.propose(unit, format!("w{i}").as_bytes());
        last.insert(unit, idx);
    }
    for (unit, idx) in &last {
        for id in 1..=3 {
            c.c(id).wait_applied(UnitId(*unit), *idx).unwrap();
            assert!(c.c(id).applied_index(UnitId(*unit)) >= *idx);
            assert!(c.c(id).durable_index(UnitId(*unit)) >= *idx);
            let units = c.engines[&id].units.lock().unwrap();
            let items = &units[unit].1;
            assert_eq!(items.len(), 10, "unit {unit} on node {id}");
            assert_eq!(items[9], format!("w{}", 36 + unit - 1).into_bytes());
        }
    }
    // A replica that doesn't lead refuses at once, retryably.
    let l = c.leader(1);
    let f = (1..=3).find(|id| *id != l).unwrap();
    let e = c
        .c(f)
        .propose(UnitId(1), b"x".to_vec(), Durability::Strict)
        .unwrap_err();
    assert!(e.retryable, "{e}");
    assert_eq!(e.code(), Some(SprigCode::ShardMoved));
}

#[test]
fn a_ticket_lost_to_a_new_leader_fails_retryably() {
    let c = Cluster::new("lost");
    create(&c, &[1]);
    c.propose(1, b"a");
    let old = c.leader(1);
    c.net.isolate(old, true);
    let t = c
        .c(old)
        .propose(UnitId(1), b"lost".to_vec(), Durability::Strict)
        .expect("still leads for now");
    let new = until("a new leader", || {
        (1..=3).find(|id| *id != old && c.c(*id).node().status(1).is_some_and(|s| s.is_leader))
    });
    let t2 = c
        .c(new)
        .propose(UnitId(1), b"b".to_vec(), Durability::Strict)
        .unwrap();
    t2.wait().unwrap();
    c.net.isolate(old, false);
    // Retryable when the old leader heard of the new one before it noticed
    // it had lost its majority (the new leader's entry replaced "lost");
    // an unknown outcome when it stepped down on its own first (it can't
    // tell then whether a majority holds the entry). Never a success.
    let e = t.wait().unwrap_err();
    match e.code() {
        Some(SprigCode::ShardMoved) => assert!(e.retryable, "{e}"),
        Some(SprigCode::CommitOutcomeUnknown) => assert!(!e.retryable, "{e}"),
        _ => panic!("{e}"),
    }
    assert!(!t.is_durable());
    // Every replica holds a, b and never "lost".
    let idx = c.propose(1, b"c");
    for id in 1..=3 {
        c.c(id).wait_applied(UnitId(1), idx).unwrap();
        let units = c.engines[&id].units.lock().unwrap();
        assert_eq!(
            units[&1].1,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            "node {id}"
        );
    }
}

#[test]
fn a_restart_applies_nothing_twice_and_checkpoints_trim() {
    let mut c = Cluster::new("restart");
    create(&c, &[1, 2]);
    for i in 0..20u64 {
        c.propose(1 + i % 2, &i.to_le_bytes());
    }
    let victim = 3;
    let before = *c.engines[&victim].applies.lock().unwrap();
    c.stop(victim);
    c.start(victim);
    // The store kept its state: nothing re-applied (the toy asserts too).
    let idx = c.propose(1, b"after");
    c.c(victim).wait_applied(UnitId(1), idx).unwrap();
    assert_eq!(*c.engines[&victim].applies.lock().unwrap(), before + 1);
    // Checkpoints trim every replica's log; a replica that missed entries
    // then catches up by snapshot (the toy engine's export and ingest).
    c.stop(victim);
    for i in 0..10u64 {
        c.propose(2, &(100 + i).to_le_bytes());
    }
    for id in [1, 2] {
        let n = c.c(id).checkpoint(&|| Ok(())).unwrap();
        assert!(n >= 1);
    }
    let behind = c.engines[&victim].units.lock().unwrap()[&2].0;
    c.start(victim);
    let idx = c.propose(2, b"last");
    c.c(victim).wait_applied(UnitId(2), idx).unwrap();
    let units = c.engines[&victim].units.lock().unwrap();
    assert!(units[&2].0 > behind);
    assert_eq!(units[&2].1.last().unwrap(), b"last");
    assert_eq!(units[&2].1.len(), 21);
}

/// A node writes and syncs once per pass for every group with new entries,
/// whichever node leads each group: a burst over many groups costs each
/// node a few syncs, not one per group.
#[test]
fn groups_share_one_sync_per_pass() {
    let c = Cluster::new("shared-sync");
    let units: Vec<u64> = (1..=32).collect();
    create(&c, &units);
    let leaders: Vec<u64> = units.iter().map(|u| c.leader(*u)).collect();
    // Settle: every group's first entries are in.
    for u in &units {
        c.propose(*u, b"warm");
    }
    std::thread::sleep(Duration::from_millis(100));
    let before: Vec<u64> = (1..=3).map(|id| c.c(id).node().stats().syncs).collect();
    let tickets: Vec<_> = units
        .iter()
        .zip(&leaders)
        .filter_map(|(u, l)| {
            c.c(*l)
                .propose(UnitId(*u), b"burst".to_vec(), Durability::Strict)
                .ok()
        })
        .collect();
    assert!(tickets.len() >= 24, "most leaders kept their groups");
    for t in &tickets {
        t.wait().unwrap();
        t.wait_applied().unwrap();
    }
    for id in 1..=3u64 {
        let i = usize::try_from(id - 1).unwrap();
        let syncs = c.c(id).node().stats().syncs - before[i];
        assert!(
            syncs < 12,
            "node {id}: {syncs} syncs for {} entries over 32 groups",
            tickets.len()
        );
    }
}
