//! Kafka through the broker over streams stored on the engine: produce
//! with every codec and version, fetch with every version, idempotent
//! retries, long polls, list offsets and leader epochs, consumer-group
//! offsets, topic administration, a restart, and routing of other topics to
//! the front end's own store.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::unused_async,
    missing_docs
)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use sprig_common::CancellationToken;
use sprig_engine::log::SyncMode;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_proto_kafka::broker::data::DataService;
use sprig_proto_kafka::broker::{
    AuthFailure, Broker, BrokerConfig, BrokerNode, Cluster, ConnInfo, Identity, ProducerIds,
    ScramFinish, ScramLookup,
};
use sprig_proto_kafka::client::Client;
use sprig_proto_kafka::compression::Compression;
use sprig_proto_kafka::group::{CommittedOffset, Coordinator, GroupConfig, GroupStore};
use sprig_proto_kafka::mem::{MemData, MemGroupStore, MemLeaseTable, MemLeases, MemTopics};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::records::{Header, Record, RecordBatch};
use sprig_proto_kafka::sasl::{PlainCredentials, ScramCredential, ScramMechanism};
use sprig_proto_kafka::topics::{NewTopic, Principal, TopicConfigs, Topics};
use sprig_streams::seg::SegConfig;
use sprig_streams::seg::kafka::{
    EngineStreams, FIRST_STREAM_ID, Gate, Need, RoutedData, RoutedGroups, RoutedTopics,
};
use sprig_streams::seg::testing::TempDir;
use tokio::io::DuplexStream;

const TENANT: u64 = 41;

fn principal() -> Principal {
    Principal {
        tenant_id: TENANT,
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
        Err(AuthFailure("no SCRAM here".into()))
    }
}

#[async_trait]
impl Identity for Id {
    async fn plain(&self, _c: &ConnInfo, p: PlainCredentials) -> Result<Principal, AuthFailure> {
        let mut pr = principal();
        if p.user.ends_with(".reader") {
            pr.user = "reader".into();
        }
        Ok(pr)
    }
    async fn scram(&self, _c: &ConnInfo, mech: ScramMechanism, user: &str) -> ScramLookup {
        ScramLookup {
            credential: ScramCredential::decoy(mech, b"d", user, 4096),
            finish: Box::new(Fin),
        }
    }
}

struct One;

#[async_trait]
impl Cluster for One {
    fn node_id(&self) -> i32 {
        1
    }
    fn cluster_id(&self) -> String {
        "sprig-dc-a".into()
    }
    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        vec![BrokerNode {
            node_id: 1,
            host: "gw1".into(),
            port: 9092,
            rack: None,
        }]
    }
}

/// Databases named `eng*` are on the engine; user `reader` may only read.
struct TestGate;

#[async_trait]
impl Gate for TestGate {
    async fn on_engine(&self, _p: &Principal, db: &str) -> bool {
        db.starts_with("eng")
    }
    fn tenant_on_engine(&self, _tenant: u64) -> bool {
        false
    }
    async fn allowed(&self, p: &Principal, _db: &str, _stream: &str, need: Need) -> bool {
        p.user != "reader" || need == Need::Read
    }
}

fn engine(dir: &Path) -> Engine {
    let mut cfg = EngineConfig::new(dir.join("engine"));
    cfg.store = StoreKind::Rocks;
    cfg.log.sync = SyncMode::Off;
    cfg.budget.engine_mb = 64;
    Engine::open(cfg).unwrap()
}

struct Env {
    eng: Arc<EngineStreams>,
    topics: Arc<RoutedTopics>,
    groups: Arc<RoutedGroups>,
    broker: Arc<Broker>,
    _engine: Engine,
}

async fn env(dir: &Path) -> Env {
    let e = engine(dir);
    let eng = Arc::new(
        EngineStreams::open(
            &e,
            dir.join("streams"),
            SegConfig {
                segment_bytes: 4096,
                index_interval: 512,
                ..SegConfig::default()
            },
            sprig_proto_kafka::topics::TopicDefaults::default(),
        )
        .unwrap(),
    );
    eng.spawn_background();
    let gate: Arc<dyn Gate> = Arc::new(TestGate);
    let mem_topics = Arc::new(MemTopics::default());
    let mem_data = Arc::new(MemData::new(Arc::clone(&mem_topics)));
    let topics = Arc::new(RoutedTopics::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        mem_topics.clone(),
    ));
    let groups = Arc::new(RoutedGroups::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Arc::new(MemGroupStore::default()),
    ));
    let data = Arc::new(RoutedData::new(
        Arc::clone(&eng),
        Arc::clone(&gate),
        Some(mem_data),
    ));
    let coord = Arc::new(Coordinator::new(
        groups.clone(),
        Arc::new(MemLeases {
            table: Arc::new(MemLeaseTable::default()),
            node: 1,
        }),
        GroupConfig::default(),
    ));
    let broker = Arc::new(Broker {
        identity: Arc::new(Id),
        cluster: Arc::new(One),
        topics: topics.clone(),
        groups: coord,
        data: Some(Arc::new(DataService::new(data))),
        config: Arc::new(BrokerConfig::default),
        producer_ids: ProducerIds::new(1),
    });
    Env {
        eng,
        topics,
        groups,
        broker,
        _engine: e,
    }
}

async fn create(t: &RoutedTopics, name: &str, n: i32, compact: bool) {
    t.create(
        &principal(),
        NewTopic {
            name: name.into(),
            partitions: n,
            configs: TopicConfigs {
                cleanup: compact.then_some(sprig_proto_kafka::topics::Cleanup::Compact),
                ..TopicConfigs::default()
            },
        },
        false,
    )
    .await
    .unwrap();
}

async fn client_as(b: &Arc<Broker>, user: &str) -> Client<DuplexStream> {
    let (a, mut s) = tokio::io::duplex(1 << 22);
    let b = Arc::clone(b);
    tokio::spawn(async move {
        let info = ConnInfo {
            peer: "10.0.0.9:51000".parse().unwrap(),
            tls: true,
            sni: None,
            client_cert: false,
            client_dn: None,
        };
        let _ = b.serve(&mut s, info, CancellationToken::new()).await;
    });
    let mut c = Client::new(a, "sprig-test");
    c.sasl_plain(user, "x").await.unwrap();
    c
}

async fn client(b: &Arc<Broker>) -> Client<DuplexStream> {
    client_as(b, "acme.app").await
}

fn batch(values: &[&str], codec: Compression, producer: Option<(i64, i16, i32)>) -> Vec<u8> {
    let (pid, epoch, seq) = producer.unwrap_or((-1, -1, -1));
    RecordBatch {
        attributes: i16::from(codec.id()),
        last_offset_delta: i32::try_from(values.len()).unwrap() - 1,
        base_timestamp: 1_762_000_000_000,
        max_timestamp: 1_762_000_000_000,
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: seq,
        records: values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                timestamp_delta: i64::try_from(i).unwrap(),
                offset_delta: i32::try_from(i).unwrap(),
                key: Some(format!("k{i}").into_bytes()),
                value: Some(v.as_bytes().to_vec()),
                headers: vec![Header {
                    key: "h".into(),
                    value: Some(b"x".to_vec()),
                }],
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap()
}

async fn produce(
    c: &mut Client<DuplexStream>,
    v: i16,
    topic: &str,
    partition: i32,
    records: Vec<u8>,
) -> produce_response::PartitionProduceResponse {
    let q = ProduceRequest {
        acks: -1,
        timeout_ms: 5000,
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
    };
    let r: ProduceResponse = c.call(v, &q).await.unwrap();
    r.responses[0].partition_responses[0].clone()
}

fn fetch_req(topic: &str, partition: i32, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: 1 << 20,
        isolation_level: 1,
        session_epoch: -1,
        topics: vec![fetch_request::FetchTopic {
            topic: topic.into(),
            partitions: vec![fetch_request::FetchPartition {
                partition,
                current_leader_epoch: -1,
                fetch_offset: offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// (offset, value) of every record at or after `from`.
fn records_of(p: &fetch_response::PartitionData, from: i64) -> Vec<(i64, Vec<u8>)> {
    RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 24, true)
        .unwrap()
        .into_iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records.into_iter().map(move |r| {
                (
                    base + i64::from(r.offset_delta),
                    r.value.unwrap_or_default(),
                )
            })
        })
        .filter(|(o, _)| *o >= from)
        .collect()
}

/// Everything from `from` on, following fetches until the high watermark.
async fn read_all(
    c: &mut Client<DuplexStream>,
    topic: &str,
    partition: i32,
    from: i64,
) -> (Vec<(i64, Vec<u8>)>, i64) {
    let mut out = Vec::new();
    let mut at = from;
    loop {
        let r: FetchResponse = c
            .call(12, &fetch_req(topic, partition, at, 0))
            .await
            .unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "fetch {topic}/{partition} at {at}");
        let got = records_of(p, at);
        if let Some((o, _)) = got.last() {
            at = o + 1;
        }
        let done = got.is_empty() || at >= p.high_watermark;
        out.extend(got);
        if done {
            return (out, p.high_watermark);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_every_codec_and_version_then_fetch_every_version() {
    let d = TempDir::new("ek-codecs");
    let e = env(d.path()).await;
    create(&e.topics, "engdb.events", 2, false).await;
    let info = e
        .topics
        .get(&principal(), "engdb.events")
        .await
        .unwrap()
        .unwrap();
    assert!(info.stream_id >= FIRST_STREAM_ID);
    let mut c = client(&e.broker).await;
    let mut expected = Vec::new();
    let mut offset = 0i64;
    for v in 3..=9 {
        for codec in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let vals: Vec<String> = (0..3)
                .map(|i| format!("v{v}-{}-{i}", codec.name()))
                .collect();
            let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
            let r = produce(&mut c, v, "engdb.events", 1, batch(&refs, codec, None)).await;
            assert_eq!(r.error_code, 0, "v{v} {codec:?}: {:?}", r.error_message);
            assert_eq!(r.base_offset, offset);
            for (i, s) in vals.iter().enumerate() {
                expected.push((offset + i64::try_from(i).unwrap(), s.clone().into_bytes()));
            }
            offset += 3;
        }
    }
    for v in 4..=13 {
        let mut q = fetch_req("engdb.events", 1, 0, 0);
        q.topics[0].topic_id = info.topic_id;
        let r: FetchResponse = c.call(v, &q).await.unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "fetch v{v}");
        assert_eq!(p.high_watermark, offset);
        assert_eq!(p.last_stable_offset, offset);
        if v >= 13 {
            assert_eq!(r.responses[0].topic_id, info.topic_id);
        }
    }
    let (got, hw) = read_all(&mut c, "engdb.events", 1, 0).await;
    assert_eq!(hw, offset);
    assert_eq!(got, expected);
    // A fetch from the middle of a batch returns that whole batch as
    // stored; the records before the offset are the client's to skip.
    let r: FetchResponse = c
        .call(11, &fetch_req("engdb.events", 1, 4, 0))
        .await
        .unwrap();
    let b = RecordBatch::decode_all(
        r.responses[0].partitions[0].records.as_ref().unwrap(),
        1 << 20,
        true,
    )
    .unwrap();
    assert_eq!(b[0].base_offset, 3);
    assert_eq!(b[0].records[1].headers[0].key, "h");
    assert_eq!(
        b[0].base_timestamp + b[0].records[1].timestamp_delta,
        1_762_000_000_001
    );
    // The other partition is untouched; an offset past the end is out of
    // range.
    let (none, hw0) = read_all(&mut c, "engdb.events", 0, 0).await;
    assert!(none.is_empty());
    assert_eq!(hw0, 0);
    let r: FetchResponse = c
        .call(12, &fetch_req("engdb.events", 1, offset + 5, 0))
        .await
        .unwrap();
    assert_eq!(r.responses[0].partitions[0].error_code, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_retries_sequences_refusals_and_privileges() {
    let d = TempDir::new("ek-idem");
    let e = env(d.path()).await;
    create(&e.topics, "engdb.events", 1, false).await;
    create(&e.topics, "engdb.latest", 1, true).await;
    let mut c = client(&e.broker).await;
    let pid = 9_001i64;
    let a = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["a", "b"], Compression::None, Some((pid, 0, 0))),
    )
    .await;
    assert_eq!((a.error_code, a.base_offset), (0, 0));
    // The same batch again: the same offsets, nothing appended.
    let again = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["a", "b"], Compression::None, Some((pid, 0, 0))),
    )
    .await;
    assert_eq!((again.error_code, again.base_offset), (0, 0));
    let b = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["c"], Compression::Zstd, Some((pid, 0, 2))),
    )
    .await;
    assert_eq!((b.error_code, b.base_offset), (0, 2));
    // A gap in the sequence.
    let gap = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["x"], Compression::None, Some((pid, 0, 9))),
    )
    .await;
    assert_eq!(gap.error_code, 45, "{:?}", gap.error_message);
    let (got, hw) = read_all(&mut c, "engdb.events", 0, 0).await;
    assert_eq!(hw, 3);
    assert_eq!(got.len(), 3);
    // Transactional batches, unknown partitions and unknown topics.
    let unknown = produce(
        &mut c,
        9,
        "engdb.nope",
        0,
        batch(&["x"], Compression::None, None),
    )
    .await;
    assert_eq!(unknown.error_code, 3);
    let nopart = produce(
        &mut c,
        9,
        "engdb.events",
        7,
        batch(&["x"], Compression::None, None),
    )
    .await;
    assert_eq!(nopart.error_code, 3);
    // A compacted stream needs keys.
    let mut keyless =
        RecordBatch::decode_all(&batch(&["x"], Compression::None, None), 1 << 20, true).unwrap();
    keyless[0].records[0].key = None;
    let kl = produce(&mut c, 9, "engdb.latest", 0, keyless[0].to_bytes().unwrap()).await;
    assert_eq!(kl.error_code, 87, "{:?}", kl.error_message);
    // A reader can fetch but not produce.
    let mut r = client_as(&e.broker, "acme.reader").await;
    let denied = produce(
        &mut r,
        9,
        "engdb.events",
        0,
        batch(&["x"], Compression::None, None),
    )
    .await;
    assert_eq!(denied.error_code, 29);
    let (seen, _) = read_all(&mut r, "engdb.events", 0, 0).await;
    assert_eq!(seen.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_long_polls_until_records_arrive() {
    let d = TempDir::new("ek-poll");
    let e = env(d.path()).await;
    create(&e.topics, "engdb.events", 1, false).await;
    let mut c = client(&e.broker).await;
    let b = Arc::clone(&e.broker);
    let started = std::time::Instant::now();
    let waiter = tokio::spawn(async move {
        let mut c2 = client(&b).await;
        let r: FetchResponse = c2
            .call(12, &fetch_req("engdb.events", 0, 0, 5000))
            .await
            .unwrap();
        records_of(&r.responses[0].partitions[0], 0)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let p = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["late"], Compression::None, None),
    )
    .await;
    assert_eq!(p.error_code, 0);
    let got = waiter.await.unwrap();
    assert_eq!(got, vec![(0, b"late".to_vec())]);
    assert!(started.elapsed() < Duration::from_secs(4));
    // Nothing new: the wait ends at max_wait.
    let t = std::time::Instant::now();
    let r: FetchResponse = c
        .call(12, &fetch_req("engdb.events", 0, 1, 200))
        .await
        .unwrap();
    assert!(records_of(&r.responses[0].partitions[0], 1).is_empty());
    assert!(t.elapsed() >= Duration::from_millis(150));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_offsets_leader_epochs_and_group_offsets() {
    let d = TempDir::new("ek-offsets");
    let e = env(d.path()).await;
    create(&e.topics, "engdb.events", 1, false).await;
    let mut c = client(&e.broker).await;
    for i in 0..4 {
        let mut b =
            RecordBatch::decode_all(&batch(&["a", "b"], Compression::None, None), 1 << 20, true)
                .unwrap();
        b[0].base_timestamp = 1_000 * (i + 1);
        b[0].max_timestamp = 1_000 * (i + 1) + 1;
        let r = produce(&mut c, 9, "engdb.events", 0, b[0].to_bytes().unwrap()).await;
        assert_eq!(r.error_code, 0);
    }
    let lo = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![list_offsets_request::ListOffsetsTopic {
            name: "engdb.events".into(),
            partitions: vec![list_offsets_request::ListOffsetsPartition {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let off = |r: &ListOffsetsResponse| {
        let p = &r.topics[0].partitions[0];
        (p.error_code, p.offset, p.timestamp)
    };
    let latest: ListOffsetsResponse = c.call(7, &lo(-1)).await.unwrap();
    assert_eq!(off(&latest).1, 8);
    let earliest: ListOffsetsResponse = c.call(7, &lo(-2)).await.unwrap();
    assert_eq!(off(&earliest).1, 0);
    let at: ListOffsetsResponse = c.call(7, &lo(2_001)).await.unwrap();
    assert_eq!((off(&at).1, off(&at).2), (3, 2_001));
    let max: ListOffsetsResponse = c.call(7, &lo(-3)).await.unwrap();
    assert_eq!((off(&max).1, off(&max).2), (7, 4_001));
    let epoch = latest.topics[0].partitions[0].leader_epoch;
    assert!(epoch >= 1);
    // OffsetForLeaderEpoch: the current epoch ends at the high watermark.
    let q = OffsetForLeaderEpochRequest {
        replica_id: -1,
        topics: vec![offset_for_leader_epoch_request::OffsetForLeaderTopic {
            topic: "engdb.events".into(),
            partitions: vec![offset_for_leader_epoch_request::OffsetForLeaderPartition {
                partition: 0,
                current_leader_epoch: -1,
                leader_epoch: epoch,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let r: OffsetForLeaderEpochResponse = c.call(4, &q).await.unwrap();
    let p = &r.topics[0].partitions[0];
    assert_eq!((p.error_code, p.leader_epoch, p.end_offset), (0, epoch, 8));
    // Group offsets of engine streams are engine records.
    let info = e
        .topics
        .get(&principal(), "engdb.events")
        .await
        .unwrap()
        .unwrap();
    e.groups
        .commit(
            TENANT,
            "g1",
            &[CommittedOffset {
                stream_id: info.stream_id,
                partition: 0,
                offset: 5,
                leader_epoch: Some(epoch),
                metadata: Some("m".into()),
            }],
        )
        .await
        .unwrap();
    let got = e.groups.fetch(TENANT, "g1", None).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].offset, 5);
    let in_records = e.eng.records().offsets(TENANT, "g1", None).unwrap();
    assert_eq!(in_records.len(), 1);
    // OffsetFetch through the broker sees it.
    let of = OffsetFetchRequest {
        group_id: "g1".into(),
        topics: Some(vec![offset_fetch_request::OffsetFetchRequestTopic {
            name: "engdb.events".into(),
            partition_indexes: vec![0],
            ..Default::default()
        }]),
        ..Default::default()
    };
    let r: OffsetFetchResponse = c.call(5, &of).await.unwrap();
    assert_eq!(r.topics[0].partitions[0].committed_offset, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn administration_restart_and_routing() {
    let d = TempDir::new("ek-restart");
    let p = principal();
    {
        let e = env(d.path()).await;
        create(&e.topics, "engdb.events", 2, false).await;
        // A topic of another database goes to the front end's own store.
        create(&e.topics, "shop.events", 1, false).await;
        let shop = e.topics.get(&p, "shop.events").await.unwrap().unwrap();
        assert!(shop.stream_id < FIRST_STREAM_ID);
        let names: Vec<String> = e
            .topics
            .list(&p)
            .await
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(names, vec!["engdb.events", "shop.events"]);
        let mut c = client(&e.broker).await;
        for i in 0..20 {
            let v = format!("r{i}");
            let r = produce(
                &mut c,
                9,
                "engdb.events",
                i % 2,
                batch(&[&v], Compression::Lz4, None),
            )
            .await;
            assert_eq!(r.error_code, 0);
        }
        let s = produce(
            &mut c,
            9,
            "shop.events",
            0,
            batch(&["x"], Compression::None, None),
        )
        .await;
        assert_eq!((s.error_code, s.base_offset), (0, 0));
        // Grow, configure, refuse a shrink, refuse a duplicate.
        e.topics.grow(&p, "engdb.events", 3, false).await.unwrap();
        assert_eq!(
            e.topics
                .grow(&p, "engdb.events", 2, false)
                .await
                .unwrap_err()
                .code,
            37
        );
        e.topics
            .set_configs(
                &p,
                "engdb.events",
                TopicConfigs {
                    retention_ms: Some(3_600_000),
                    ..TopicConfigs::default()
                },
                false,
            )
            .await
            .unwrap();
        let dup = e
            .topics
            .create(
                &p,
                NewTopic {
                    name: "engdb.events".into(),
                    partitions: 1,
                    configs: TopicConfigs::default(),
                },
                false,
            )
            .await
            .unwrap_err();
        assert_eq!(dup.code, 36);
        create(&e.topics, "engdb.gone", 1, false).await;
        let r = produce(
            &mut c,
            9,
            "engdb.gone",
            0,
            batch(&["x"], Compression::None, None),
        )
        .await;
        assert_eq!(r.error_code, 0);
        e.topics.delete(&p, "engdb.gone").await.unwrap();
        assert!(e.topics.get(&p, "engdb.gone").await.unwrap().is_none());
        // Records not yet flushed are in the commit log only: the restart
        // replays them. The connection's task holds the broker (and the
        // engine) until it sees the client go.
        drop(c);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let e = env(d.path()).await;
    let t = e.topics.get(&p, "engdb.events").await.unwrap().unwrap();
    assert_eq!(t.partitions, 3);
    assert_eq!(t.configs.retention_ms, Some(3_600_000));
    assert!(e.topics.get(&p, "engdb.gone").await.unwrap().is_none());
    let mut c = client(&e.broker).await;
    for part in 0..2 {
        let (got, hw) = read_all(&mut c, "engdb.events", part, 0).await;
        assert_eq!(hw, 10);
        let want: Vec<(i64, Vec<u8>)> = (0..10)
            .map(|i| (i, format!("r{}", i * 2 + i64::from(part)).into_bytes()))
            .collect();
        assert_eq!(got, want);
    }
    let r = produce(
        &mut c,
        9,
        "engdb.events",
        0,
        batch(&["after"], Compression::None, None),
    )
    .await;
    assert_eq!((r.error_code, r.base_offset), (0, 10));
    let r = produce(
        &mut c,
        9,
        "engdb.gone",
        0,
        batch(&["x"], Compression::None, None),
    )
    .await;
    assert_eq!(r.error_code, 3);
    // A new stream after the restart gets a new id.
    create(&e.topics, "engdb.next", 1, false).await;
    let n = e.topics.get(&p, "engdb.next").await.unwrap().unwrap();
    assert!(n.stream_id > t.stream_id + 1);
}
