//! Kafka produce and fetch throughput on engine-stored streams, through the
//! broker over TCP on loopback with the crate's own client: the cell of the
//! market report (1 KiB records, 12 partitions, durable writes).
//!
//! ```text
//! cargo test -p sprig-streams --release --test engine_bench -- --ignored --nocapture
//! ```
//!
//! `SPRIG_BENCH_RECORDS` (per partition, default 50,000), `SPRIG_BENCH_SIZE`
//! (1024), `SPRIG_BENCH_PARTITIONS` (12), `SPRIG_BENCH_BATCH` (records per
//! batch, 16 = Kafka's 16 KiB `batch.size`), `SPRIG_BENCH_INFLIGHT` (5),
//! `SPRIG_BENCH_SYNC` (`full` or `off`).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::type_complexity,
    missing_docs
)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_engine::log::SyncMode;
use sprig_engine::{Engine, EngineConfig};
use sprig_proto_kafka::broker::data::DataService;
use sprig_proto_kafka::broker::{
    AuthFailure, Broker, BrokerConfig, BrokerNode, Cluster, ConnInfo, Identity, ProducerIds,
    ScramFinish, ScramLookup,
};
use sprig_proto_kafka::client::Client;
use sprig_proto_kafka::group::{Coordinator, GroupConfig};
use sprig_proto_kafka::mem::{MemData, MemGroupStore, MemLeaseTable, MemLeases, MemTopics};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::records::{Record, RecordBatch};
use sprig_proto_kafka::sasl::{PlainCredentials, ScramCredential, ScramMechanism};
use sprig_proto_kafka::topics::{NewTopic, Principal, TopicConfigs, TopicDefaults, Topics};
use sprig_streams::seg::SegConfig;
use sprig_streams::seg::kafka::{
    EngineStreams, Gate, Need, RoutedData, RoutedGroups, RoutedTopics,
};
use sprig_streams::seg::testing::TempDir;
use tokio::net::TcpStream;

fn principal() -> Principal {
    Principal {
        tenant_id: 41,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(7),
        handle: None,
    }
}

struct Id;
struct Fin;

#[async_trait]
impl ScramFinish for Fin {
    async fn finish(self: Box<Self>, _verified: bool) -> Result<Principal, AuthFailure> {
        Err(AuthFailure("no SCRAM".into()))
    }
}

#[async_trait]
impl Identity for Id {
    async fn plain(&self, _c: &ConnInfo, _p: PlainCredentials) -> Result<Principal, AuthFailure> {
        Ok(principal())
    }
    async fn scram(&self, _c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup {
        ScramLookup {
            credential: ScramCredential::decoy(mech, b"d", user, 4096),
            finish: Box::new(Fin),
        }
    }
}

struct One(u16);

#[async_trait]
impl Cluster for One {
    fn node_id(&self) -> i32 {
        1
    }
    fn cluster_id(&self) -> String {
        "bench".into()
    }
    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        vec![BrokerNode {
            node_id: 1,
            host: "127.0.0.1".into(),
            port: i32::from(self.0),
            rack: None,
        }]
    }
}

struct AllEngine;

#[async_trait]
impl Gate for AllEngine {
    async fn on_engine(&self, _p: &Principal, _db: &str) -> bool {
        true
    }
    fn tenant_on_engine(&self, _t: u64) -> bool {
        true
    }
    async fn allowed(&self, _p: &Principal, _db: &str, _s: &str, _n: Need) -> bool {
        true
    }
}

fn env(k: &str, d: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

fn batch(first: usize, n: usize, size: usize, producer: (i64, i16, i32)) -> Vec<u8> {
    let now = 1_763_000_000_000i64;
    RecordBatch {
        last_offset_delta: n as i32 - 1,
        base_timestamp: now,
        max_timestamp: now,
        producer_id: producer.0,
        producer_epoch: producer.1,
        base_sequence: producer.2,
        records: (0..n)
            .map(|i| {
                let mut v = vec![0u8; size];
                // Not compressible to nothing: a counter pattern.
                for (j, b) in v.iter_mut().enumerate() {
                    *b = ((first + i).wrapping_mul(31) ^ j) as u8;
                }
                Record {
                    offset_delta: i as i32,
                    key: None,
                    value: Some(v),
                    ..Record::default()
                }
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap()
}

async fn client(addr: std::net::SocketAddr) -> Client<TcpStream> {
    let s = TcpStream::connect(addr).await.unwrap();
    s.set_nodelay(true).unwrap();
    let mut c = Client::new(s, "bench");
    c.sasl_plain("acme.app", "x").await.unwrap();
    c
}

fn produce_req(topic: &str, partition: i32, records: Vec<u8>) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 30_000,
        topic_data: vec![produce_request::TopicProduceData {
            name: topic.into(),
            partition_data: vec![produce_request::PartitionProduceData {
                index: partition,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a benchmark: run in release with --ignored --nocapture"]
async fn kafka_produce_and_fetch_on_the_engine() {
    let n = env("SPRIG_BENCH_RECORDS", 50_000);
    let size = env("SPRIG_BENCH_SIZE", 1024);
    let parts = env("SPRIG_BENCH_PARTITIONS", 12) as i32;
    let per_batch = env("SPRIG_BENCH_BATCH", 16);
    let inflight = env("SPRIG_BENCH_INFLIGHT", 5);
    let sync = std::env::var("SPRIG_BENCH_SYNC").unwrap_or_else(|_| "full".into());
    let d = TempDir::new("kbench");
    let mut cfg = EngineConfig::new(d.path().join("engine"));
    cfg.budget.engine_mb = 512;
    cfg.log.sync = if sync == "off" {
        SyncMode::Off
    } else {
        SyncMode::Full
    };
    let e = Engine::open(cfg).unwrap();
    let eng = Arc::new(
        EngineStreams::open(
            &e,
            d.path().join("streams"),
            SegConfig::default(),
            TopicDefaults::default(),
        )
        .unwrap(),
    );
    eng.spawn_background();
    let gate: Arc<dyn Gate> = Arc::new(AllEngine);
    let mem = Arc::new(MemTopics::default());
    let topics = Arc::new(RoutedTopics::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        mem.clone(),
    ));
    let data = Arc::new(RoutedData::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Some(Arc::new(MemData::new(mem))),
    ));
    let groups = Arc::new(RoutedGroups::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Arc::new(MemGroupStore::default()),
    ));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let broker = Arc::new(Broker {
        identity: Arc::new(Id),
        cluster: Arc::new(One(addr.port())),
        topics: topics.clone(),
        groups: Arc::new(Coordinator::new(
            groups,
            Arc::new(MemLeases {
                table: Arc::new(MemLeaseTable::default()),
                node: 1,
            }),
            GroupConfig::default(),
        )),
        data: Some(Arc::new(DataService::new(data))),
        config: Arc::new(|| BrokerConfig {
            plain_without_tls: true,
            ..BrokerConfig::default()
        }),
        producer_ids: ProducerIds::new(1),
    });
    tokio::spawn(async move {
        loop {
            let (mut s, peer) = l.accept().await.unwrap();
            s.set_nodelay(true).unwrap();
            let b = Arc::clone(&broker);
            tokio::spawn(async move {
                let info = ConnInfo {
                    peer,
                    tls: false,
                    sni: None,
                    client_cert: false,
                    client_dn: None,
                };
                let _ = b.serve(&mut s, info, CancellationToken::new()).await;
            });
        }
    });
    let topic = "bench.events";
    topics
        .create(
            &principal(),
            NewTopic {
                name: topic.into(),
                partitions: parts,
                configs: TopicConfigs::default(),
            },
            false,
        )
        .await
        .unwrap();
    // Produce: one idempotent producer connection per partition.
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for part in 0..parts {
        tasks.push(tokio::spawn(async move {
            let mut c = client(addr).await;
            let ip: InitProducerIdResponse = c
                .call(
                    4,
                    &InitProducerIdRequest {
                        transaction_timeout_ms: -1,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let mut pending = VecDeque::new();
            let (mut sent, mut acked) = (0usize, 0usize);
            let mut lat = Vec::new();
            while acked < n {
                while sent < n && pending.len() < inflight {
                    let k = per_batch.min(n - sent);
                    let b = batch(sent, k, size, (ip.producer_id, 0, sent as i32));
                    let id = c.send(9, &produce_req(topic, part, b)).await.unwrap();
                    pending.push_back((id, k, Instant::now()));
                    sent += k;
                }
                let (id, k, at) = pending.pop_front().unwrap();
                let r: ProduceResponse = c.recv(9, id).await.unwrap();
                let p = &r.responses[0].partition_responses[0];
                assert_eq!(p.error_code, 0, "{:?}", p.error_message);
                lat.push(at.elapsed());
                acked += k;
            }
            lat.sort();
            (lat[lat.len() / 2], lat[lat.len() * 99 / 100])
        }));
    }
    let mut lats = Vec::new();
    for t in tasks {
        lats.push(t.await.unwrap());
    }
    let secs = t0.elapsed().as_secs_f64();
    let total = n * parts as usize;
    let mb = (total * size) as f64 / 1e6;
    let p50 = lats.iter().map(|l| l.0).max().unwrap();
    let p99 = lats.iter().map(|l| l.1).max().unwrap();
    println!(
        "BENCH produce sync={sync} records={total} size={size} partitions={parts} batch={per_batch} inflight={inflight}: {:.1} MB/s, {:.0} records/s, batch latency p50 {:.2} ms p99 {:.2} ms (worst partition)",
        mb / secs,
        total as f64 / secs,
        p50.as_secs_f64() * 1000.0,
        p99.as_secs_f64() * 1000.0
    );
    // Fetch everything, one consumer connection per partition.
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for part in 0..parts {
        tasks.push(tokio::spawn(async move {
            let mut c = client(addr).await;
            let mut off = 0i64;
            let mut bytes = 0usize;
            while off < n as i64 {
                let q = FetchRequest {
                    replica_id: -1,
                    max_wait_ms: 500,
                    min_bytes: 1,
                    max_bytes: 50 << 20,
                    isolation_level: 1,
                    session_epoch: -1,
                    topics: vec![fetch_request::FetchTopic {
                        topic: topic.into(),
                        partitions: vec![fetch_request::FetchPartition {
                            partition: part,
                            current_leader_epoch: -1,
                            fetch_offset: off,
                            partition_max_bytes: 1 << 20,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                let r: FetchResponse = c.call(12, &q).await.unwrap();
                let p = &r.responses[0].partitions[0];
                assert_eq!(p.error_code, 0);
                let recs = p.records.as_deref().unwrap_or(&[]);
                bytes += recs.len();
                for b in RecordBatch::decode_all(recs, 64 << 20, true).unwrap() {
                    off = off.max(b.base_offset + i64::from(b.last_offset_delta) + 1);
                }
            }
            bytes
        }));
    }
    let mut bytes = 0usize;
    for t in tasks {
        bytes += t.await.unwrap();
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "BENCH fetch records={total} partitions={parts}: {:.1} MB/s of record values ({:.1} MB/s on the wire), {:.0} records/s",
        mb / secs,
        bytes as f64 / 1e6 / secs,
        total as f64 / secs
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
}
