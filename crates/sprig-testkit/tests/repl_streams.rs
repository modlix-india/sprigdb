//! Stream appends on replicated units: every replica's stream log writes
//! the same records at the same offsets (the apply observer runs on every
//! replica), through a leader failover and a replica's restart. With note
//! blobs each batch is written once per replica (the Raft log's blob file,
//! referred to by the stream's segments).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::many_single_char_names,
    // The stream defaults' type lives in a crate this one doesn't depend on.
    clippy::default_trait_access
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::time::SystemClock;
use sprig_engine::log::SyncMode;
use sprig_engine::log::blobs::NoteBlobs;
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};
use sprig_streams::seg::kafka::EngineStreams;
use sprig_streams::seg::part::{Outcome, PartKey};
use sprig_streams::seg::testing::{batch_bytes, records_of};
use sprig_streams::seg::{SegConfig, SegLog};

const VOTERS: [u64; 3] = [1, 2, 3];
const TENANT: u64 = 7_300;
const W: Duration = Duration::from_secs(10);
const GROUPS: u32 = 3;

/// The preferred leader of a partition group's unit, as the gateway picks
/// it: the voters in order, from the tenant's offset.
fn preferred(unit: u64) -> Option<u64> {
    let (tenant, g) = sprig_streams::seg::group_of_unit(unit)?;
    Some(VOTERS[usize::try_from((tenant % 3 + u64::from(g)) % 3).unwrap()])
}

struct Replica {
    streams: EngineStreams,
    engine: Engine,
    node: EngineNode,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct Cluster {
    dir: PathBuf,
    net: Arc<MemNetwork>,
    replicas: BTreeMap<u64, Replica>,
    /// Partition groups per tenant (0: the tenant's unit).
    groups: u32,
    /// Stream appends written once (note blobs).
    blobs: bool,
    /// A stopped replica loses what its store held only in memory (the
    /// store isn't flushed at close), as after `kill -9`.
    crash: bool,
}

fn key() -> PartKey {
    PartKey {
        tenant: TENANT,
        stream: 1,
        partition: 0,
    }
}

async fn until<T>(what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

impl Replica {
    fn log(&self) -> &SegLog {
        self.streams.log()
    }
}

impl Cluster {
    fn start(&mut self, id: u64) {
        let d = self.dir.join(format!("n{id}"));
        let blobs = self.blobs.then(|| {
            NoteBlobs::open(
                d.join("notes"),
                SyncMode::Off,
                sprig_streams::seg::note::is_append,
                true,
            )
            .unwrap()
        });
        let config = || {
            let mut c = EngineConfig::new(d.join("engine"));
            c.store = StoreKind::Rocks;
            c.checkpoint_interval = Duration::ZERO;
            c.log.sync = SyncMode::Off;
            c.budget.engine_mb = 64;
            c.replicated = true;
            c.rocks.avoid_flush_on_close = self.crash;
            c
        };
        // A restarted replica's previous engine may still be closing (a
        // background pass of its stream log held it): its store's lock goes
        // with it.
        let deadline = Instant::now() + Duration::from_secs(10);
        let engine = loop {
            match Engine::open(config()) {
                Ok(e) => break e,
                Err(e) if Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("engine of replica {id}: {e}"),
            }
        };
        let node = EngineNode::open(
            &engine,
            EngineNodeOptions {
                id,
                config: RaftConfig {
                    tick: Duration::from_millis(10),
                    lease: Duration::from_millis(90),
                    lease_margin: Duration::from_millis(10),
                    liveness_interval: Duration::from_millis(30),
                    liveness_timeout: Duration::from_millis(150),
                    static_voters: VOTERS.to_vec(),
                    ..RaftConfig::default()
                },
                store: {
                    let s = EngineLogStore::open(d.join("raft"), SyncMode::Off, None).unwrap();
                    if let Some(b) = &blobs {
                        s.set_note_blobs(Arc::clone(b));
                    }
                    s
                },
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
        if self.groups > 0 {
            node.prefer_leaders(
                Arc::new(|u: UnitId| preferred(u.0)),
                Duration::from_millis(50),
                Duration::ZERO,
            )
            .unwrap();
        }
        // After the replicated log is attached: the stream log catches up
        // from it.
        let streams = EngineStreams::open(
            &engine,
            d.join("streams"),
            SegConfig {
                segment_bytes: 4096,
                index_interval: 512,
                groups: self.groups,
                blobs,
                ..SegConfig::default()
            },
            Default::default(),
        )
        .unwrap();
        self.replicas.insert(
            id,
            Replica {
                streams,
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
    }

    fn leader(&self) -> Option<u64> {
        let u = Engine::unit_of_tenant(TENANT);
        self.replicas
            .iter()
            .find(|(_, r)| r.engine.leader_epoch(u) > 0)
            .map(|(id, _)| *id)
    }

    /// Appends `n` records through the leader, retrying across leader
    /// changes; returns the batch's base offset.
    async fn append(&self, n: i32) -> i64 {
        self.append_sized(n, 10).await
    }

    /// [`Cluster::append`] with values of `len` bytes.
    async fn append_sized(&self, n: i32, len: usize) -> i64 {
        loop {
            if let Some(l) = self.leader()
                && let Ok(Outcome::Appended { base, .. }) = self.replicas[&l]
                    .log()
                    .append(key(), &batch_bytes(n, 1, len), W)
                    .await
            {
                return base;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The replica whose engine leads `unit`.
    fn leader_of(&self, unit: u64) -> Option<u64> {
        self.replicas
            .iter()
            .find(|(_, r)| r.engine.leader_epoch(UnitId(unit)) > 0)
            .map(|(id, _)| *id)
    }

    /// Appends `n` records to `k` through the leader of its unit, retrying
    /// across leader changes; returns the batch's base offset.
    async fn append_to(&self, k: PartKey, n: i32) -> i64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(Instant::now() < deadline, "no append to {k:?}");
            let unit = self.replicas.values().next().unwrap().log().unit_of_part(k);
            if let Some(l) = self.leader_of(unit)
                && let Ok(Outcome::Appended { base, .. }) = self.replicas[&l]
                    .log()
                    .append(k, &batch_bytes(n, 1, 150), W)
                    .await
            {
                return base;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Bytes in replica `id`'s plain segment files and blob segment files
    /// (`.log`, `.ref`), and in its note blob files, after a flush.
    fn disk(&self, id: u64) -> (u64, u64, u64) {
        fn walk(p: &std::path::Path, ext: &str) -> u64 {
            let Ok(rd) = std::fs::read_dir(p) else {
                return 0;
            };
            rd.flatten()
                .map(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        walk(&p, ext)
                    } else if p.extension().is_some_and(|x| x == ext) {
                        e.metadata().map_or(0, |m| m.len())
                    } else {
                        0
                    }
                })
                .sum()
        }
        self.replicas[&id].log().flush().unwrap();
        let d = self.dir.join(format!("n{id}"));
        (
            walk(&d.join("streams"), "log"),
            walk(&d.join("streams"), "ref"),
            walk(&d.join("notes"), "blob"),
        )
    }

    /// Every record replica `id`'s stream log holds, by offset.
    fn records(&self, id: u64) -> Vec<i64> {
        self.records_of(id, key())
    }

    /// Every record of `k` replica `id`'s stream log holds, by offset.
    fn records_of(&self, id: u64, k: PartKey) -> Vec<i64> {
        let l = self.replicas[&id].log();
        let b = l.bounds(k);
        let mut out = Vec::new();
        let mut at = b.log_start;
        while at < b.high_watermark {
            let Ok((_, bytes)) = l.read(k, at, 1 << 20) else {
                break;
            };
            if bytes.is_empty() {
                break;
            }
            let before = out.len();
            for r in records_of(&bytes) {
                if r.0 >= at {
                    out.push(r.0);
                }
            }
            if out.len() == before {
                break;
            }
            at = out.last().copied().unwrap_or(at) + 1;
        }
        out
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_replica_writes_the_streams_segments() {
    let dir = std::env::temp_dir().join(format!("sprig-repl-streams-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Cluster {
        dir,
        net: MemNetwork::new(),
        replicas: BTreeMap::new(),
        groups: 0,
        blobs: false,
        crash: false,
    };
    for id in VOTERS {
        c.start(id);
    }
    c.replicas[&1]
        .node
        .start_unit(UnitId(TENANT), &VOTERS)
        .unwrap();
    let mut want = Vec::new();
    for _ in 0..10 {
        let base = c.append(3).await;
        want.extend(base..base + 3);
    }
    for id in VOTERS {
        until("every replica's stream log", || {
            (c.records(id) == want).then_some(())
        })
        .await;
    }
    // The leader dies; the next one appends after the same offsets.
    let l = c.leader().unwrap();
    let before = c.replicas[&l].log().bounds(key()).epoch;
    c.kill(l);
    for _ in 0..5 {
        let base = c.append(2).await;
        assert_eq!(base, *want.last().unwrap() + 1, "offsets go on in order");
        want.extend(base..base + 2);
    }
    // The new leader's epoch is the unit's new Raft term: above the old
    // leader's, whichever node took over.
    let now = c.leader().unwrap();
    let after = c.replicas[&now].log().bounds(key()).epoch;
    assert!(after > before, "leader epoch {after} after {before}");
    // Restarted, the old leader's stream log catches up.
    c.start(l);
    for id in VOTERS {
        until("every replica's stream log after the failover", || {
            (c.records(id) == want).then_some(())
        })
        .await;
    }
    // Every replica stored the same bytes (epochs stamped from the terms),
    // and reports the same epoch.
    let bytes = |id: u64| c.replicas[&id].log().read(key(), 0, 1 << 24).unwrap().1;
    for id in VOTERS {
        assert_eq!(bytes(id), bytes(VOTERS[0]), "replica {id}'s batches");
        assert_eq!(c.replicas[&id].log().bounds(key()).epoch, after);
    }
}

/// Creates topic `name` through the leader; false if there is none or the
/// commit failed (retry).
async fn create_topic(c: &Cluster, name: &str) -> bool {
    use sprig_streams::seg::records::TopicRecord;
    let Some(l) = c.leader() else { return false };
    let rec = TopicRecord {
        name: name.to_owned(),
        stream_id: 0,
        partitions: 3,
        retention_ms: None,
        retention_bytes: None,
        compact: None,
        max_message_bytes: None,
        delete_retention_ms: None,
        created_ms: 1,
    };
    matches!(
        c.replicas[&l].streams.create(TENANT, rec).await,
        Ok(Some(_))
    )
}

/// Topics (engine records of the tenant's unit) created through the leader
/// are listed on every replica, including one restarted since and one that
/// listed them before (its topic cache follows replication).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_records_reach_every_replica() {
    let dir = std::env::temp_dir().join(format!("sprig-repl-topics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Cluster {
        dir,
        net: MemNetwork::new(),
        replicas: BTreeMap::new(),
        groups: 0,
        blobs: false,
        crash: false,
    };
    for id in VOTERS {
        c.start(id);
    }
    c.replicas[&1]
        .node
        .start_unit(UnitId(TENANT), &VOTERS)
        .unwrap();
    let names = |c: &Cluster, id: u64| -> Vec<String> {
        c.replicas[&id]
            .streams
            .topics(TENANT)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect()
    };
    let mut want = Vec::new();
    for i in 0..3 {
        let name = format!("db.t{i}");
        loop {
            if create_topic(&c, &name).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        want.push(name);
        for id in VOTERS {
            until("the topic on every replica", || {
                (names(&c, id) == want).then_some(())
            })
            .await;
        }
    }
    let l = c.leader().unwrap();
    c.kill(l);
    let name = "db.after".to_owned();
    loop {
        if create_topic(&c, &name).await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    want.push(name);
    want.sort();
    c.start(l);
    for id in VOTERS {
        until("the topics after a failover and a restart", || {
            (names(&c, id) == want).then_some(())
        })
        .await;
    }
}

/// The partitions whose records differ from `want`, per replica.
fn differences(c: &Cluster, want: &BTreeMap<u32, Vec<i64>>) -> String {
    use std::fmt::Write as _;
    let mut d = String::new();
    for id in c.replicas.keys() {
        for (p, w) in want {
            let k = PartKey {
                tenant: TENANT,
                stream: 1,
                partition: *p,
            };
            let got = c.records_of(*id, k);
            if got != *w {
                let _ = write!(
                    d,
                    "replica {id} partition {p}: {} records, want {}; ",
                    got.len(),
                    w.len()
                );
            }
        }
    }
    d
}

/// With partition groups each group of partitions commits through a unit of
/// its own, led by its preferred replica (so the three groups' leaders are
/// three different nodes); every replica writes the same records at the
/// same offsets, through the loss of one group's leader, and the restarted
/// node takes its group back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_groups_spread_their_leaders_and_replicate() {
    use sprig_streams::seg::group_unit;
    let dir = std::env::temp_dir().join(format!("sprig-repl-groups-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Cluster {
        dir,
        net: MemNetwork::new(),
        replicas: BTreeMap::new(),
        groups: GROUPS,
        blobs: true,
        crash: false,
    };
    for id in VOTERS {
        c.start(id);
    }
    let part = |p: u32| PartKey {
        tenant: TENANT,
        stream: 1,
        partition: p,
    };
    for p in 0..6 {
        let want = group_unit(TENANT, p % GROUPS);
        for id in VOTERS {
            assert_eq!(c.replicas[&id].log().unit_of_part(part(p)), want);
        }
    }
    // Every group starts on node 1; each moves to its preferred replica.
    for g in 0..GROUPS {
        c.replicas[&1]
            .node
            .start_unit(UnitId(group_unit(TENANT, g)), &VOTERS)
            .unwrap();
    }
    let leaders = |c: &Cluster| -> Vec<Option<u64>> {
        (0..GROUPS)
            .map(|g| c.leader_of(group_unit(TENANT, g)))
            .collect()
    };
    let prefs: Vec<Option<u64>> = (0..GROUPS)
        .map(|g| preferred(group_unit(TENANT, g)))
        .collect();
    until("each group led by its preferred replica", || {
        (leaders(&c) == prefs).then_some(())
    })
    .await;
    let mut distinct = prefs.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(distinct.len(), 3, "three groups, three leaders: {prefs:?}");
    // The tenant's own unit isn't involved.
    assert!(c.leader().is_none(), "no append touched the tenant's unit");
    let mut want: BTreeMap<u32, Vec<i64>> = BTreeMap::new();
    for round in 0..4 {
        for p in 0..6 {
            let base = c.append_to(part(p), 2 + round).await;
            want.entry(p)
                .or_default()
                .extend(base..base + i64::from(2 + round));
        }
    }
    let same = |c: &Cluster, want: &BTreeMap<u32, Vec<i64>>| {
        c.replicas
            .keys()
            .all(|id| want.iter().all(|(p, w)| c.records_of(*id, part(*p)) == *w))
    };
    until("every replica's partitions", || {
        same(&c, &want).then_some(())
    })
    .await;
    // Group 0's leader dies: its partitions go on through a new leader.
    let l0 = prefs[0].unwrap();
    c.kill(l0);
    for p in [0, 3] {
        let base = c.append_to(part(p), 3).await;
        let w = want.get_mut(&p).unwrap();
        assert_eq!(base, *w.last().unwrap() + 1, "offsets go on in order");
        w.extend(base..base + 3);
    }
    // Restarted, it catches up and takes its group back.
    c.start(l0);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !same(&c, &want) {
        assert!(
            Instant::now() < deadline,
            "the restarted replica {l0}'s partitions differ: {}",
            differences(&c, &want)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    until("group 0 back on its preferred replica", || {
        (c.leader_of(group_unit(TENANT, 0)) == Some(l0)).then_some(())
    })
    .await;
    let base = c.append_to(part(0), 1).await;
    assert_eq!(base, *want[&0].last().unwrap() + 1);
}

/// With note blobs every replica writes each batch once: its Raft log keeps
/// the batch in a blob file, and its segments are blob segments that refer
/// to it (no plain segment bytes). Records and stored bytes match on every
/// replica through a failover, a restart (the log's references put back),
/// a snapshot install on a replica the log left behind (written as plain
/// segments), and compaction (which rewrites blob segments as plain ones).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)] // one scenario, step by step
async fn appends_are_written_once_on_every_replica() {
    let dir = std::env::temp_dir().join(format!("sprig-repl-once-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Cluster {
        dir,
        net: MemNetwork::new(),
        replicas: BTreeMap::new(),
        groups: 0,
        blobs: true,
        crash: false,
    };
    for id in VOTERS {
        c.start(id);
    }
    let unit = UnitId(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let mut want = Vec::new();
    for _ in 0..40 {
        let base = c.append_sized(5, 200).await;
        want.extend(base..base + 5);
    }
    let same = |c: &Cluster, want: &Vec<i64>| c.replicas.keys().all(|id| c.records(*id) == *want);
    until("every replica's records", || same(&c, &want).then_some(())).await;
    let payload: u64 = 40 * batch_bytes(5, 1, 200).len() as u64;
    for id in VOTERS {
        let (plain, refs, blobs) = c.disk(id);
        assert_eq!(plain, 0, "replica {id}: no batch bytes in segment files");
        assert!(
            refs > 0 && refs < payload / 4,
            "replica {id}: records only ({refs} B)"
        );
        assert!(
            blobs >= payload,
            "replica {id}: the blobs hold the batches ({blobs} B)"
        );
    }
    let bytes = |c: &Cluster, id: u64| c.replicas[&id].log().read(key(), 0, 1 << 24).unwrap().1;
    // A failover and a restart: the restarted replica reopens its blob
    // segments and catches up through the log's references.
    let l = c.leader().unwrap();
    c.kill(l);
    for _ in 0..10 {
        let base = c.append_sized(5, 200).await;
        want.extend(base..base + 5);
    }
    c.start(l);
    until("every replica after a restart", || {
        same(&c, &want).then_some(())
    })
    .await;
    for id in VOTERS {
        assert_eq!(
            bytes(&c, id),
            bytes(&c, VOTERS[0]),
            "replica {id}'s batches"
        );
    }
    // A replica the log leaves behind gets a snapshot: the stream part
    // reads the leader's blob segments and writes plain ones.
    let behind = VOTERS
        .into_iter()
        .find(|id| Some(*id) != c.leader())
        .unwrap();
    c.kill(behind);
    for _ in 0..20 {
        let base = c.append_sized(5, 200).await;
        want.extend(base..base + 5);
    }
    let last = c.replicas[&c.leader().unwrap()]
        .node
        .node()
        .committed(unit.0);
    for r in c.replicas.values() {
        r.log().flush().unwrap();
        r.engine.checkpoint().unwrap();
    }
    for id in c.replicas.keys().copied().collect::<Vec<_>>() {
        until("the log trimmed", || {
            let r = &c.replicas[&id];
            let _ = r.log().flush();
            let _ = r.engine.checkpoint();
            r.node
                .node()
                .log_first_index(unit.0)
                .is_none_or(|f| f > last.saturating_sub(5))
                .then_some(())
        })
        .await;
    }
    c.start(behind);
    until("the replica restored from a snapshot", || {
        same(&c, &want).then_some(())
    })
    .await;
    let (plain, ..) = c.disk(behind);
    assert!(
        plain > 0,
        "a snapshot's partitions are written as plain segments"
    );
    // Appends after it go to blob segments again.
    for _ in 0..5 {
        let base = c.append_sized(5, 200).await;
        want.extend(base..base + 5);
    }
    until("every replica after the snapshot", || {
        same(&c, &want).then_some(())
    })
    .await;
    for id in VOTERS {
        assert_eq!(
            bytes(&c, id),
            bytes(&c, VOTERS[0]),
            "replica {id}'s batches"
        );
    }
    // Compaction keeps each key's newest record (keys k0..k4 repeat in
    // every batch) and rewrites rolled blob segments as plain ones.
    let pol = sprig_streams::seg::Policy {
        compact: true,
        ..sprig_streams::seg::Policy::default()
    };
    let leader = c.leader().unwrap();
    let before = c.disk(leader);
    c.replicas[&leader].log().set_policy(TENANT, 1, pol);
    c.replicas[&leader].log().maintain().unwrap();
    let after = c.disk(leader);
    assert!(
        after.1 < before.1,
        "rolled blob segments rewritten: {before:?} -> {after:?}"
    );
    let kept = c.records(leader);
    assert!(kept.len() < want.len() && kept.ends_with(&want[want.len() - 5..]));
}

/// Proposes one membership change of `unit` (`what` for node `id`) through
/// its leader, retrying while there is none or another change is pending.
async fn change_member(c: &Cluster, unit: u64, what: sprig_raft::ConfChangeType, id: u64) {
    use sprig_raft::{ConfChangeSingle, ConfChangeV2};
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            Instant::now() < deadline,
            "no membership change {what:?} {id}"
        );
        if let Some(l) = c.leader_of(unit) {
            let mut s = ConfChangeSingle::default();
            s.set_change_type(what);
            s.node_id = id;
            let mut cc = ConfChangeV2::default();
            cc.mut_changes().push(s);
            if let Ok(slot) = c.replicas[&l].node.node().change_membership(unit, cc)
                && matches!(slot.wait_timeout(W), Some(Ok(_)))
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A read copy (a learner on a node outside the voters) removed and added
/// again, round after round, on the same node: each time it catches up
/// (from the log, or from a snapshot once the voters trimmed their logs),
/// its Raft driver keeps running, and its stream log holds the voters'
/// records. Before the fix the dropped copy kept the unit's keys, its
/// stream log's partitions and floor: the stream log of the node restarted
/// meanwhile didn't open (it waited for a log the node no longer had, or
/// read topics of a unit it held no copy of), and every later snapshot of
/// the unit failed to restore there ("stream log: closed").
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_read_copy_removed_and_added_again_catches_up() {
    use sprig_raft::ConfChangeType::{AddLearnerNode, RemoveNode};
    let dir = std::env::temp_dir().join(format!("sprig-repl-relearn-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Cluster {
        dir,
        net: MemNetwork::new(),
        replicas: BTreeMap::new(),
        groups: 0,
        blobs: false,
        crash: false,
    };
    for id in VOTERS {
        c.start(id);
    }
    // Node 4 holds no unit until a leader adds it.
    c.start(4);
    let unit = UnitId(TENANT);
    c.replicas[&1].node.start_unit(unit, &VOTERS).unwrap();
    let mut want = Vec::new();
    for _ in 0..10 {
        let base = c.append(4).await;
        want.extend(base..base + 4);
    }
    for round in 0..6 {
        change_member(&c, unit.0, AddLearnerNode, 4).await;
        until(&format!("round {round}: the read copy's records"), || {
            assert!(
                !c.replicas[&4].node.node().is_closed(),
                "round {round}: node 4's Raft driver stopped"
            );
            (c.records(4) == want).then_some(())
        })
        .await;
        change_member(&c, unit.0, RemoveNode, 4).await;
        // A removed copy never hears of its removal (the leader stops
        // sending to it): the node drops it when the unit's descriptor no
        // longer lists it, as the gateway's stale-replica sweep does.
        c.replicas[&4].node.node().drop_group(unit.0).unwrap();
        if round % 3 == 2 {
            // The copy's node restarts while it holds no copy.
            c.kill(4);
            c.start(4);
        }
        for _ in 0..5 {
            let base = c.append(4).await;
            want.extend(base..base + 4);
        }
        if round % 2 == 1 {
            // The voters trim their logs: the next add gets a snapshot.
            let l = c.leader().unwrap();
            let last = c.replicas[&l].node.node().committed(unit.0);
            for id in VOTERS {
                until("the log trimmed", || {
                    let r = &c.replicas[&id];
                    let _ = r.log().flush();
                    let _ = r.engine.checkpoint();
                    r.node
                        .node()
                        .log_first_index(unit.0)
                        .is_none_or(|f| f > last.saturating_sub(2))
                        .then_some(())
                })
                .await;
            }
        }
    }
    change_member(&c, unit.0, AddLearnerNode, 4).await;
    until("the read copy at the end", || {
        (c.records(4) == want).then_some(())
    })
    .await;
    assert!(!c.replicas[&4].node.node().is_closed());
    // The copy's engine holds the same unit as the voters'.
    let l = c.leader().unwrap();
    until("the copy's keys equal the leader's", || {
        let a = c.replicas[&l].engine.unit_checksum(unit).ok()?;
        let b = c.replicas[&4].engine.unit_checksum(unit).ok()?;
        (a == b).then_some(())
    })
    .await;
}

/// A tenant's first appends survive a replica's crash before the engine's
/// first checkpoint of the unit: the restarted replica's stream log holds
/// every record (it replays them from the log) and keeps taking new ones.
/// Before the fix, the store had lost the unit's applied index with its
/// unflushed memory, so the restart restored the group's creation record
/// (an export of the empty unit, with its stream part); that part, handed
/// to the stream log when it registered, wiped the partitions it had just
/// restored, and the log went on from the store's index: every record
/// before the crash was gone on that replica, and on a leader elected
/// later, lost to clients (Kafka kill loops on a fresh tenant).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fresh_units_appends_survive_a_replicas_crash() {
    for blobs in [false, true] {
        let dir =
            std::env::temp_dir().join(format!("sprig-repl-crash-{blobs}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut c = Cluster {
            dir,
            net: MemNetwork::new(),
            replicas: BTreeMap::new(),
            groups: 0,
            blobs,
            crash: true,
        };
        for id in VOTERS {
            c.start(id);
        }
        c.replicas[&1]
            .node
            .start_unit(UnitId(TENANT), &VOTERS)
            .unwrap();
        let mut want = Vec::new();
        for _ in 0..20 {
            let base = c.append(3).await;
            want.extend(base..base + 3);
        }
        for id in VOTERS {
            until("every replica's stream log", || {
                (c.records(id) == want).then_some(())
            })
            .await;
        }
        // Every replica crashes in turn and comes back; appends go on.
        for victim in VOTERS {
            c.kill(victim);
            for _ in 0..5 {
                let base = c.append(3).await;
                want.extend(base..base + 3);
            }
            c.start(victim);
            for id in VOTERS {
                until(
                    &format!("blobs {blobs}: replica {id} after replica {victim}'s crash"),
                    || (c.records(id) == want).then_some(()),
                )
                .await;
            }
        }
    }
}
