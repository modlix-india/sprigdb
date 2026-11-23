//! Stream appends on replicated units: every replica's stream log writes
//! the same records at the same offsets (the apply observer runs on every
//! replica), through a leader failover and a replica's restart.

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
use sprig_engine::{Engine, EngineConfig, StoreKind, UnitId};
use sprig_raft::{EngineLogStore, EngineNode, EngineNodeOptions, MemNetwork, RaftConfig};
use sprig_streams::seg::kafka::EngineStreams;
use sprig_streams::seg::part::{Outcome, PartKey};
use sprig_streams::seg::testing::{batch_bytes, records_of};
use sprig_streams::seg::{SegConfig, SegLog};

const VOTERS: [u64; 3] = [1, 2, 3];
const TENANT: u64 = 7_300;
const W: Duration = Duration::from_secs(10);

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
        let mut c = EngineConfig::new(d.join("engine"));
        c.store = StoreKind::Rocks;
        c.checkpoint_interval = Duration::ZERO;
        c.log.sync = SyncMode::Off;
        c.budget.engine_mb = 64;
        c.replicated = true;
        let engine = Engine::open(c).unwrap();
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
                store: EngineLogStore::open(d.join("raft"), SyncMode::Off, None).unwrap(),
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
        // After the replicated log is attached: the stream log catches up
        // from it.
        let streams = EngineStreams::open(
            &engine,
            d.join("streams"),
            SegConfig {
                segment_bytes: 4096,
                index_interval: 512,
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
        loop {
            if let Some(l) = self.leader()
                && let Ok(Outcome::Appended { base, .. }) = self.replicas[&l]
                    .log()
                    .append(key(), &batch_bytes(n, 1, 10), W)
                    .await
            {
                return base;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Every record replica `id`'s stream log holds, by offset.
    fn records(&self, id: u64) -> Vec<i64> {
        let l = self.replicas[&id].log();
        let b = l.bounds(key());
        let mut out = Vec::new();
        let mut at = b.log_start;
        while at < b.high_watermark {
            let Ok((_, bytes)) = l.read(key(), at, 1 << 20) else {
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
    c.kill(l);
    for _ in 0..5 {
        let base = c.append(2).await;
        assert_eq!(base, *want.last().unwrap() + 1, "offsets go on in order");
        want.extend(base..base + 2);
    }
    // Restarted, the old leader's stream log catches up.
    c.start(l);
    for id in VOTERS {
        until("every replica's stream log after the failover", || {
            (c.records(id) == want).then_some(())
        })
        .await;
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
