//! Clusters of real driver threads in one process, over `MemNetwork` and
//! simulated disks, with fast timings.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use raft::prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use sprig_common::time::SystemClock;
use sprig_engine::log::Durability;

use super::*;
use crate::logstore::MemLogStore;
use crate::toy::{self, KvMachine};
use crate::transport::MemNetwork;

pub(crate) fn fast() -> RaftConfig {
    RaftConfig {
        tick: Duration::from_millis(10),
        election_ticks: 10,
        heartbeat_ticks: 2,
        lease: Duration::from_millis(90),
        lease_margin: Duration::from_millis(10),
        quiesce_ticks: 20,
        liveness_interval: Duration::from_millis(30),
        liveness_timeout: Duration::from_millis(150),
        snapshot_retry_ticks: 30,
        ..RaftConfig::default()
    }
}

struct Member {
    node: Node,
    thread: Option<std::thread::JoinHandle<()>>,
    store: Arc<MemLogStore>,
}

struct Cluster {
    net: Arc<MemNetwork>,
    cfg: RaftConfig,
    members: BTreeMap<NodeId, Member>,
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
    fn new(ids: &[NodeId], cfg: RaftConfig) -> Self {
        Self::with_stores(ids, cfg, |_| {})
    }

    /// [`Cluster::new`] with each member's store set up by `setup`.
    fn with_stores(ids: &[NodeId], cfg: RaftConfig, setup: impl Fn(&MemLogStore)) -> Self {
        let mut c = Cluster {
            net: MemNetwork::new(),
            cfg,
            members: BTreeMap::new(),
        };
        for id in ids {
            let store = MemLogStore::new();
            setup(&store);
            c.start(*id, store);
        }
        c
    }

    fn start(&mut self, id: NodeId, store: Arc<MemLogStore>) {
        let node = Node::open(NodeOptions {
            id,
            config: self.cfg.clone(),
            store: Arc::clone(&store) as Arc<dyn LogStore>,
            transport: self.net.transport(id),
            clock: Arc::new(SystemClock),
            machines: Arc::new(KvMachine::factory()),
            observer: None,
            seed: id * 7919,
        })
        .expect("open");
        self.net.register(&node);
        let thread = Some(node.spawn().expect("spawn"));
        self.members.insert(
            id,
            Member {
                node,
                thread,
                store,
            },
        );
    }

    fn stop(&mut self, id: NodeId) -> Arc<MemLogStore> {
        let mut m = self.members.remove(&id).expect("member");
        self.net.unregister(id);
        m.node.close();
        if let Some(t) = m.thread.take() {
            t.join().expect("driver thread");
        }
        m.store
    }

    fn crash_restart(&mut self, id: NodeId, seed: u64) {
        let store = self.stop(id);
        store.crash(seed);
        self.start(id, store);
    }

    fn node(&self, id: NodeId) -> &Node {
        &self.members[&id].node
    }

    fn create(&self, group: GroupId, voters: &[NodeId]) {
        for v in voters {
            let s = self.node(*v).create_group(group, voters).expect("create");
            assert_eq!(s.wait_timeout(WAIT), Some(Ok(())));
        }
    }

    fn leader(&self, group: GroupId) -> NodeId {
        until("a leader", || {
            self.members.iter().find_map(|(id, m)| {
                m.node
                    .status(group)
                    .filter(|s| s.is_leader && s.committed >= s.last_index)
                    .map(|_| *id)
            })
        })
    }

    fn put(&self, group: GroupId, key: u64, value: u64) -> u64 {
        until("a committed write", || {
            let l = self.leader(group);
            let slot = self
                .node(l)
                .propose(group, toy::put(key, value), Durability::Strict)
                .ok()?;
            slot.wait_timeout(WAIT).and_then(Result::ok)
        })
    }

    fn get(&self, group: GroupId, key: u64) -> Option<u64> {
        until("a read", || {
            let l = self.leader(group);
            let slot = self.node(l).read(group, toy::get(key)).ok()?;
            slot.wait_timeout(WAIT).and_then(Result::ok)
        })
        .as_slice()
        .try_into()
        .ok()
        .map(u64::from_le_bytes)
    }

    /// Every member holding `group` reaches the same state.
    fn converged(&self, group: GroupId) -> KvMachine {
        until("replicas to converge", || {
            let states: Vec<(u64, Vec<u8>)> = self
                .members
                .values()
                .filter_map(|m| m.node.query_local(group, &toy::dump()))
                .collect();
            let first = states.first()?.clone();
            let same = states
                .iter()
                .all(|(a, d)| *a == first.0 && toy_data(d) == toy_data(&first.1));
            same.then(|| KvMachine::decode(&first.1).expect("dump"))
        })
    }
}

fn toy_data(d: &[u8]) -> BTreeMap<u64, Vec<u64>> {
    KvMachine::decode(d).expect("dump").data().clone()
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let ids: Vec<NodeId> = self.members.keys().copied().collect();
        for id in ids {
            self.stop(id);
        }
    }
}

#[test]
fn elect_replicate_and_read() {
    let c = Cluster::new(&[1, 2, 3], fast());
    for g in 10..14 {
        c.create(g, &[1, 2, 3]);
    }
    for g in 10..14 {
        for i in 0..20 {
            c.put(g, i % 3, g * 1000 + i);
        }
        assert_eq!(c.get(g, 0), Some(g * 1000 + 18));
        assert_eq!(c.get(g, 2), Some(g * 1000 + 17));
        assert_eq!(c.get(g, 9), None);
        let s = c.converged(g);
        assert_eq!(s.data()[&1].len(), 7);
    }
    // A busy leader answers reads under its lease.
    let l = c.leader(10);
    for _ in 0..20 {
        let _ = c.get(10, 1);
    }
    let st = c.node(l).stats();
    assert!(st.lease_reads > 0, "{st:?}");
}

/// [`fast`] with the log written on its own thread.
fn threaded() -> RaftConfig {
    RaftConfig {
        log_writer_thread: true,
        ..fast()
    }
}

#[test]
fn a_log_writer_thread_replicates_and_keeps_writes_through_crashes() {
    let mut c = Cluster::new(&[1, 2, 3], threaded());
    for g in 10..14 {
        c.create(g, &[1, 2, 3]);
    }
    for g in 10..14 {
        for i in 0..20 {
            c.put(g, i % 3, g * 1000 + i);
        }
        assert_eq!(c.get(g, 0), Some(g * 1000 + 18));
    }
    let syncs: u64 = c.members.values().map(|m| m.node.stats().syncs).sum();
    assert!(syncs > 0);
    for (n, id) in [1, 2, 3].into_iter().enumerate() {
        c.crash_restart(id, 2000 + n as u64);
    }
    for g in 10..14 {
        assert_eq!(c.get(g, 0), Some(g * 1000 + 18));
        assert_eq!(c.get(g, 2), Some(g * 1000 + 17));
        let s = c.converged(g);
        assert_eq!(s.data()[&1].len(), 7);
    }
}

/// The log writer thread with its syncs overlapped (each takes 2 ms, up to
/// four at once): writes from several clients replicate, syncs run side by
/// side, and every acknowledged write survives crashes that lose whatever
/// wasn't synced.
#[test]
fn a_pipelined_log_writer_overlaps_syncs_and_keeps_acknowledged_writes() {
    let mut c = Cluster::with_stores(&[1, 2, 3], threaded(), |s| {
        s.set_sync_pipeline(4);
        s.set_sync_delay(|| Duration::from_millis(2));
    });
    for g in 20..24 {
        c.create(g, &[1, 2, 3]);
    }
    std::thread::scope(|s| {
        for t in 0..8u64 {
            let c = &c;
            s.spawn(move || {
                for i in 0..25 {
                    c.put(20 + t % 4, t, t * 1000 + i);
                }
            });
        }
    });
    let most = c
        .members
        .values()
        .map(|m| m.store.most_syncs_at_once())
        .max()
        .unwrap_or(0);
    assert!(most > 1, "syncs never overlapped (most at once: {most})");
    for (n, id) in [1, 2, 3].into_iter().enumerate() {
        c.crash_restart(id, 3000 + n as u64);
    }
    for t in 0..8u64 {
        assert_eq!(c.get(20 + t % 4, t), Some(t * 1000 + 24), "client {t}");
    }
    for g in 20..24 {
        c.converged(g);
    }
}

#[test]
fn a_log_writer_thread_installs_snapshots_on_a_lagging_follower() {
    let cfg = RaftConfig {
        compact_after: 10,
        ..threaded()
    };
    let c = Cluster::new(&[1, 2, 3], cfg);
    c.create(3, &[1, 2, 3]);
    c.put(3, 0, 0);
    let l = c.leader(3);
    let lagging = c.members.keys().copied().find(|id| *id != l).expect("f");
    c.net.isolate(lagging, true);
    for i in 1..60 {
        c.put(3, i % 5, i);
    }
    c.net.isolate(lagging, false);
    let s = c.converged(3);
    assert_eq!(s.data()[&4].last(), Some(&59));
}

#[test]
fn an_isolated_leaders_proposal_is_dropped_definitely() {
    let c = Cluster::new(&[1, 2, 3], fast());
    c.create(1, &[1, 2, 3]);
    c.put(1, 1, 1);
    let old = c.leader(1);
    c.net.isolate(old, true);
    let slot = c
        .node(old)
        .propose(1, toy::put(1, 99), Durability::Strict)
        .expect("still leads for now");
    // The others elect a new leader and commit their own entries.
    let new = until("a new leader", || {
        c.members
            .keys()
            .copied()
            .find(|id| *id != old && c.node(*id).status(1).is_some_and(|s| s.is_leader))
    });
    let s = c
        .node(new)
        .propose(1, toy::put(1, 2), Durability::Strict)
        .expect("propose");
    assert!(matches!(s.wait_timeout(WAIT), Some(Ok(_))));
    c.net.isolate(old, false);
    assert_eq!(slot.wait_timeout(WAIT), Some(Err(RaftError::Dropped)));
    assert_eq!(c.get(1, 1), Some(2));
    let st = c.converged(1);
    assert_eq!(st.data()[&1], vec![1, 2]);
    // A replica that doesn't lead refuses at once.
    let follower = c
        .members
        .keys()
        .copied()
        .find(|id| *id != c.leader(1))
        .expect("f");
    assert!(matches!(
        c.node(follower)
            .propose(1, toy::put(1, 3), Durability::Strict),
        Err(RaftError::NotLeader { .. })
    ));
}

#[test]
fn crashes_keep_every_acknowledged_write() {
    let mut c = Cluster::new(&[1, 2, 3], fast());
    c.create(1, &[1, 2, 3]);
    c.create(2, &[1, 2, 3]);
    for i in 0..30 {
        c.put(1 + i % 2, i, i);
    }
    for (n, id) in [1, 2, 3].into_iter().enumerate() {
        c.crash_restart(id, 1000 + n as u64);
    }
    for i in 0..30 {
        assert_eq!(c.get(1 + i % 2, i), Some(i));
    }
    c.converged(1);
    c.converged(2);
}

#[test]
fn membership_changes_through_learners_and_joint_consensus() {
    let c = Cluster::new(&[1, 2, 3, 4], fast());
    c.create(7, &[1, 2, 3]);
    for i in 0..10 {
        c.put(7, i, i);
    }
    let change = |changes: &[(ConfChangeType, NodeId)]| {
        until("a membership change", || {
            let l = c.leader(7);
            let mut cc = ConfChangeV2::default();
            for (t, id) in changes {
                let mut s = ConfChangeSingle::default();
                s.set_change_type(*t);
                s.node_id = *id;
                cc.mut_changes().push(s);
            }
            let slot = c.node(l).change_membership(7, cc).ok()?;
            slot.wait_timeout(WAIT).and_then(Result::ok)
        })
    };
    change(&[(ConfChangeType::AddLearnerNode, 4)]);
    until("the learner to catch up", || {
        c.node(4)
            .query_local(7, &toy::history(9))
            .filter(|(_, v)| toy::values(v) == vec![9])
    });
    // Promote 4 and remove 1 at once: a joint configuration, left
    // automatically.
    change(&[
        (ConfChangeType::AddNode, 4),
        (ConfChangeType::RemoveNode, 1),
    ]);
    until("the joint configuration to be left", || {
        let l = c.leader(7);
        c.node(l)
            .status(7)
            .filter(|s| s.voters_outgoing.is_empty() && !s.voters.contains(&1))
    });
    c.put(7, 100, 100);
    until("node 1 to drop the group", || {
        c.node(1).status(7).is_none().then_some(())
    });
    let l = c.leader(7);
    let mut v = c.node(l).status(7).expect("status").voters;
    v.sort_unstable();
    assert_eq!(v, vec![2, 3, 4]);
    assert_eq!(c.get(7, 100), Some(100));
}

#[test]
fn a_lagging_follower_catches_up_from_a_snapshot() {
    let cfg = RaftConfig {
        compact_after: 10,
        ..fast()
    };
    let c = Cluster::new(&[1, 2, 3], cfg);
    c.create(3, &[1, 2, 3]);
    c.put(3, 0, 0);
    let l = c.leader(3);
    let lagging = c.members.keys().copied().find(|id| *id != l).expect("f");
    c.net.isolate(lagging, true);
    for i in 1..60 {
        c.put(3, i % 5, i);
    }
    c.net.isolate(lagging, false);
    let s = c.converged(3);
    assert_eq!(s.data()[&4].last(), Some(&59));
}

#[test]
fn idle_groups_quiesce_wake_and_fail_over() {
    let mut c = Cluster::new(&[1, 2, 3], fast());
    for g in 1..=20 {
        c.create(g, &[1, 2, 3]);
        c.put(g, 1, g);
    }
    until("every group to quiesce", || {
        c.members
            .values()
            .all(|m| m.node.stats().active == 0)
            .then_some(())
    });
    // A read of a quiesced group goes through `ReadIndex` (no lease
    // without heartbeats); a write wakes the group.
    let before: u64 = c.members.values().map(|m| m.node.stats().index_reads).sum();
    assert_eq!(c.get(5, 1), Some(5));
    let after: u64 = c.members.values().map(|m| m.node.stats().index_reads).sum();
    assert!(after > before);
    c.put(5, 1, 500);
    assert_eq!(c.get(5, 1), Some(500));
    until("quiescing again", || {
        c.members
            .values()
            .all(|m| m.node.stats().active == 0)
            .then_some(())
    });
    // Stop the node leading group 1: store liveness wakes its groups on
    // the others, which elect new leaders.
    let dead = c.leader(1);
    let led: Vec<GroupId> = (1..=20)
        .filter(|g| c.node(dead).status(*g).is_some_and(|s| s.is_leader))
        .collect();
    let _ = c.stop(dead);
    let t = Instant::now();
    for g in &led {
        let l = c.leader(*g);
        assert_ne!(l, dead);
        assert_eq!(c.get(*g, 1), Some(if *g == 5 { 500 } else { *g }));
    }
    assert!(t.elapsed() < WAIT);
}

/// The lease a leader publishes for its own reads ([`Node::leader_barrier`])
/// holds only while a quorum keeps accepting its frames: cut off, it runs
/// out (the leader hasn't even noticed it lost anything yet), and from then
/// on a read gets no answer until the replica stops leading. A transfer
/// gives the lease up at once.
#[test]
fn a_leader_cut_off_from_its_followers_loses_its_lease() {
    // The leader notices a lost quorum late (its own election timeout is
    // long); the lease is as short as everyone's.
    let late = RaftConfig {
        election_ticks: 200,
        ..fast()
    };
    let mut c = Cluster::new(&[2, 3], fast());
    c.cfg = late;
    c.start(1, MemLogStore::new());
    c.cfg = fast();
    c.create(1, &[1, 2, 3]);
    c.put(1, 1, 1);
    until("node 1 to lead", || {
        let l = c.leader(1);
        if l != 1 {
            let _ = c.node(l).transfer_leader(1, 1);
            std::thread::sleep(Duration::from_millis(20));
        }
        c.node(1).status(1).filter(|s| s.is_leader).map(|_| ())
    });
    c.put(1, 1, 1);
    // The lease holds and is published: reads check it without the lock.
    until("the lease", || {
        matches!(c.node(1).leader_barrier(1), Ok(None)).then_some(())
    });
    assert!(c.node(1).lease_holds(1));
    // A follower holds no lease, and its barrier is refused.
    assert!(!c.node(2).lease_holds(1));
    assert!(matches!(
        c.node(2).leader_barrier(1),
        Err(RaftError::NotLeader { .. })
    ));
    c.net.isolate(1, true);
    until("the lease to run out", || {
        (!c.node(1).lease_holds(1)).then_some(())
    });
    assert!(
        c.node(1).status(1).is_some_and(|s| s.is_leader),
        "the cut-off leader hasn't noticed yet"
    );
    // The others elect a leader and commit.
    let new = until("a new leader", || {
        [2, 3]
            .into_iter()
            .find(|id| c.node(*id).status(1).is_some_and(|s| s.is_leader))
    });
    let s = c
        .node(new)
        .propose(1, toy::put(1, 2), Durability::Strict)
        .expect("propose");
    assert!(matches!(s.wait_timeout(WAIT), Some(Ok(_))));
    // The old leader still believes it leads, but its read gets no answer:
    // no lease, and no quorum confirms it.
    assert!(c.node(1).status(1).is_some_and(|s| s.is_leader));
    let slot = c
        .node(1)
        .leader_barrier(1)
        .expect("still a leader")
        .expect("no lease: it must ask a quorum");
    assert_eq!(slot.wait_timeout(Duration::from_millis(300)), None);
    assert!(!c.node(1).lease_holds(1));
    // Back in touch it follows the new leader, and the read fails.
    c.net.isolate(1, false);
    assert!(matches!(
        slot.wait_timeout(WAIT),
        Some(Err(RaftError::NotLeader { .. }))
    ));
    assert!(matches!(
        c.node(1).leader_barrier(1),
        Err(RaftError::NotLeader { .. })
    ));
    // The new leader's lease is given up the moment a transfer starts.
    until("the new leader's lease", || {
        matches!(c.node(new).leader_barrier(1), Ok(None)).then_some(())
    });
    assert!(c.node(new).lease_holds(1));
    let to = if new == 2 { 3 } else { 2 };
    c.node(new).transfer_leader(1, to).expect("transfer");
    assert!(!c.node(new).lease_holds(1));
}

#[test]
fn leadership_moves_by_transfer() {
    let c = Cluster::new(&[1, 2, 3], fast());
    c.create(1, &[1, 2, 3]);
    c.put(1, 1, 1);
    let l = c.leader(1);
    let to = c.members.keys().copied().find(|id| *id != l).expect("f");
    c.node(l).transfer_leader(1, to).expect("transfer");
    until("the transfer", || {
        c.node(to).status(1).filter(|s| s.is_leader).map(|_| ())
    });
    c.put(1, 1, 2);
    assert_eq!(c.get(1, 1), Some(2));
}

#[test]
fn proposals_on_a_closed_node_fail_and_waiters_learn() {
    let mut c = Cluster::new(&[1, 2, 3], fast());
    c.create(1, &[1, 2, 3]);
    c.put(1, 1, 1);
    let l = c.leader(1);
    let n = c.node(l).clone();
    let _ = c.stop(l);
    assert_eq!(
        n.propose(1, toy::put(1, 2), Durability::Strict).err(),
        Some(RaftError::Closed)
    );
}
