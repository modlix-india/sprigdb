//! Live test against gateways whose engines replicate (three or more):
//! a client that follows Metadata, as Kafka clients do, reaches the
//! gateway leading the tenant's unit for every write, from any bootstrap
//! gateway, and again after that gateway is killed.
//!
//! Ignored by default. Run with the variables of `live.rs` and
//! `SPRIG_KAFKA_ADDR` naming any gateway (a follower checks the most):
//!
//! ```text
//! SPRIG_KAFKA_ADDR=127.0.0.1:26324 SPRIG_KAFKA_CA=deploy/compose/gateway-certs/ca.pem \
//! SPRIG_KAFKA_USER=acme.app SPRIG_KAFKA_PASSWORD=... SPRIG_KAFKA_DB=<an existing database> \
//! cargo test -p sprig-proto-kafka --test live_repl -- --ignored --nocapture
//! ```
//!
//! With `SPRIG_KAFKA_FAILOVER=<command>`, the command is run with the
//! leading gateway's `host:port` as its argument (it should kill that
//! gateway, and may start it again); the test then waits for Metadata to
//! name another leader, produces there and checks that every acknowledged
//! record and the group's committed offset survived.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::many_single_char_names,
    clippy::single_match_else,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use rustls::pki_types::pem::PemObject as _;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_proto_kafka::client::{Client, assignment, subscription};
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::{Record, RecordBatch};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

type Conn = Client<Box<dyn Io>>;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn login(addr: &str) -> std::io::Result<Conn> {
    let tcp = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::other("connect timed out"))??;
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(env("SPRIG_KAFKA_CA").unwrap()).unwrap();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        roots.add(c.unwrap()).unwrap();
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let s = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await?;
    let mut c: Conn = Client::new(Box::new(s), "repl-live");
    c.sasl_plain(
        &env("SPRIG_KAFKA_USER").unwrap(),
        &env("SPRIG_KAFKA_PASSWORD").unwrap(),
    )
    .await
    .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    Ok(c)
}

/// What Metadata said, and a connection per broker.
#[derive(Default)]
struct View {
    brokers: BTreeMap<i32, String>,
    controller: i32,
    leaders: BTreeMap<i32, i32>,
    /// Each partition's leader epoch as last accepted.
    epochs: BTreeMap<i32, i32>,
    /// Metadata answers that named a partition with an older leader epoch
    /// than one already seen: like Kafka's clients, the view keeps the
    /// leader it had for that partition.
    stale: usize,
    conns: HashMap<i32, Conn>,
}

impl View {
    /// Asks Metadata through `via` (any broker that answers, else the
    /// bootstrap address).
    async fn refresh(&mut self, topic: Option<&str>) {
        let req = MetadataRequest {
            topics: topic.map(|t| {
                vec![metadata_request::MetadataRequestTopic {
                    name: Some(t.into()),
                    ..Default::default()
                }]
            }),
            ..Default::default()
        };
        let mut addrs: Vec<String> = self.brokers.values().cloned().collect();
        addrs.push(env("SPRIG_KAFKA_ADDR").unwrap());
        for a in addrs {
            let Ok(mut c) = login(&a).await else { continue };
            let Ok(m) = c.call::<_, MetadataResponse>(12, &req).await else {
                continue;
            };
            if env("SPRIG_KAFKA_DEBUG").is_some() {
                for t in &m.topics {
                    println!(
                        "metadata from {a}: {:?} error {} partitions {}",
                        t.name,
                        t.error_code,
                        t.partitions.len()
                    );
                }
            }
            self.brokers = m
                .brokers
                .iter()
                .map(|b| (b.node_id, format!("{}:{}", b.host, b.port)))
                .collect();
            self.controller = m.controller_id;
            // Leaders of the one topic asked for (all topics: as listed),
            // keeping a partition's leader when the answer shows an older
            // epoch than one already seen.
            let mut leaders = BTreeMap::new();
            let wanted = m
                .topics
                .iter()
                .filter(|t| topic.is_none_or(|n| t.name.as_deref() == Some(n)));
            for p in wanted.flat_map(|t| t.partitions.iter()) {
                let i = p.partition_index;
                if topic.is_some() && self.epochs.get(&i).is_some_and(|e| p.leader_epoch < *e) {
                    self.stale += 1;
                    if let Some(l) = self.leaders.get(&i) {
                        leaders.insert(i, *l);
                    }
                    continue;
                }
                if topic.is_some() {
                    self.epochs.insert(i, p.leader_epoch);
                }
                leaders.insert(i, p.leader_id);
            }
            self.leaders = leaders;
            self.conns
                .retain(|n, _| m.brokers.iter().any(|b| b.node_id == *n));
            return;
        }
        panic!("no gateway answered Metadata");
    }

    /// A connection to broker `node`, reused.
    async fn conn(&mut self, node: i32) -> Option<&mut Conn> {
        if !self.conns.contains_key(&node) {
            let addr = self.brokers.get(&node)?.clone();
            let c = login(&addr).await.ok()?;
            self.conns.insert(node, c);
        }
        self.conns.get_mut(&node)
    }
}

fn batch(tag: &str, first: usize, n: usize) -> Vec<u8> {
    RecordBatch {
        last_offset_delta: n as i32 - 1,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..n)
            .map(|i| Record {
                offset_delta: i as i32,
                value: Some(format!("{tag}-{}", first + i).into_bytes()),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap()
}

fn produce_req(topic: &str, partition: i32, records: Vec<u8>) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 10_000,
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

/// Produces one batch the way a client does: to Metadata's leader, asking
/// Metadata again on a retryable error. The error codes seen on the way.
async fn produce(
    v: &mut View,
    topic: &str,
    partition: i32,
    records: &[u8],
    patience: Duration,
) -> (i64, Vec<i16>) {
    let until = Instant::now() + patience;
    let mut seen = Vec::new();
    loop {
        assert!(
            Instant::now() < until,
            "partition {partition}: no acknowledgement in {patience:?}; errors {seen:?}"
        );
        let leader = v.leaders.get(&partition).copied().unwrap_or(-1);
        let answer = match v.conn(leader).await {
            None => None,
            Some(c) => c
                .call::<_, ProduceResponse>(9, &produce_req(topic, partition, records.to_vec()))
                .await
                .ok(),
        };
        match answer {
            Some(r) => {
                let p = &r.responses[0].partition_responses[0];
                if p.error_code == err::NONE {
                    return (p.base_offset, seen);
                }
                seen.push(p.error_code);
            }
            None => {
                v.conns.remove(&leader);
                seen.push(-2);
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        v.refresh(Some(topic)).await;
    }
}

async fn fetch_all(v: &mut View, topic: &str, partition: i32) -> Vec<(i64, String)> {
    let leader = v.leaders[&partition];
    let c = v.conn(leader).await.unwrap();
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        let r: FetchResponse = c
            .call(
                11,
                &FetchRequest {
                    replica_id: -1,
                    max_wait_ms: 0,
                    min_bytes: 1,
                    max_bytes: 50 << 20,
                    isolation_level: 1,
                    session_epoch: -1,
                    topics: vec![fetch_request::FetchTopic {
                        topic: topic.into(),
                        partitions: vec![fetch_request::FetchPartition {
                            partition,
                            current_leader_epoch: -1,
                            fetch_offset: offset,
                            partition_max_bytes: 4 << 20,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "fetch {topic}/{partition} at {offset}");
        let got: Vec<(i64, String)> =
            RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 26, true)
                .unwrap()
                .into_iter()
                .flat_map(|b| {
                    let base = b.base_offset;
                    b.records.into_iter().map(move |r| {
                        (
                            base + i64::from(r.offset_delta),
                            String::from_utf8(r.value.unwrap_or_default()).unwrap(),
                        )
                    })
                })
                .filter(|(o, _)| *o >= offset)
                .collect();
        if got.is_empty() {
            return out;
        }
        offset = got.last().unwrap().0 + 1;
        out.extend(got);
        if offset >= p.high_watermark {
            return out;
        }
    }
}

async fn coordinator(v: &mut View, group: &str) -> i32 {
    let c = v.conn(v.controller).await.unwrap();
    let r: FindCoordinatorResponse = c
        .call(
            4,
            &FindCoordinatorRequest {
                key_type: 0,
                coordinator_keys: vec![group.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.coordinators[0].error_code, 0, "{:?}", r.coordinators[0]);
    r.coordinators[0].node_id
}

/// Joins `group` alone and commits `offset` for partition 0, retrying
/// through FindCoordinator until a coordinator accepts it.
async fn commit_offset(v: &mut View, group: &str, topic: &str, offset: i64) -> Vec<i16> {
    let mut seen = Vec::new();
    let until = Instant::now() + Duration::from_secs(30);
    while Instant::now() < until {
        let node = coordinator(v, group).await;
        let Some(c) = v.conn(node).await else {
            seen.push(-2);
            v.refresh(None).await;
            continue;
        };
        let join = |member: String| JoinGroupRequest {
            group_id: group.into(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 10_000,
            member_id: member,
            protocol_type: "consumer".into(),
            protocols: vec![join_group_request::JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: subscription(&[topic]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut j: JoinGroupResponse = c.call(9, &join(String::new())).await.unwrap();
        if j.error_code == err::MEMBER_ID_REQUIRED {
            j = c.call(9, &join(j.member_id.clone())).await.unwrap();
        }
        if j.error_code != 0 {
            seen.push(j.error_code);
            tokio::time::sleep(Duration::from_millis(200)).await;
            v.refresh(None).await;
            continue;
        }
        let s: SyncGroupResponse = c
            .call(
                5,
                &SyncGroupRequest {
                    group_id: group.into(),
                    generation_id: j.generation_id,
                    member_id: j.member_id.clone(),
                    protocol_type: Some("consumer".into()),
                    protocol_name: j.protocol_name.clone(),
                    assignments: vec![sync_group_request::SyncGroupRequestAssignment {
                        member_id: j.member_id.clone(),
                        assignment: assignment(&[(topic, vec![0])]),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if s.error_code != 0 {
            seen.push(s.error_code);
            continue;
        }
        let r: OffsetCommitResponse = c
            .call(
                8,
                &OffsetCommitRequest {
                    group_id: group.into(),
                    generation_id_or_member_epoch: j.generation_id,
                    member_id: j.member_id.clone(),
                    topics: vec![offset_commit_request::OffsetCommitRequestTopic {
                        name: topic.into(),
                        partitions: vec![offset_commit_request::OffsetCommitRequestPartition {
                            partition_index: 0,
                            committed_offset: offset,
                            committed_leader_epoch: -1,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let code = r.topics[0].partitions[0].error_code;
        if code == 0 {
            let _: LeaveGroupResponse = c
                .call(
                    3,
                    &LeaveGroupRequest {
                        group_id: group.into(),
                        member_id: j.member_id.clone(),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            return seen;
        }
        seen.push(code);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("group {group}: no offset commit accepted; errors {seen:?}");
}

async fn committed(v: &mut View, group: &str) -> i64 {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let node = coordinator(v, group).await;
        if let Some(c) = v.conn(node).await
            && let Ok(r) = c
                .call::<_, OffsetFetchResponse>(
                    8,
                    &OffsetFetchRequest {
                        groups: vec![offset_fetch_request::OffsetFetchRequestGroup {
                            group_id: group.into(),
                            topics: None,
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )
                .await
            && r.groups[0].error_code == 0
            && let Some(p) = r.groups[0]
                .topics
                .first()
                .and_then(|t| t.partitions.first())
        {
            return p.committed_offset;
        }
        assert!(Instant::now() < until, "group {group}: offsets unreadable");
        tokio::time::sleep(Duration::from_millis(200)).await;
        v.refresh(None).await;
    }
}

#[tokio::test]
#[ignore = "needs three gateways whose engines replicate, with the kafka listener"]
async fn live_writes_follow_the_tenants_leader() {
    let db = env("SPRIG_KAFKA_DB").unwrap();
    let topic = format!("{db}.repl_{}", std::process::id());
    let group = format!("repl-g-{}", std::process::id());
    let parts = 3;
    let mut v = View::default();
    v.refresh(None).await;
    println!("brokers {:?}, controller {}", v.brokers, v.controller);
    assert!(v.brokers.len() >= 3, "three gateways expected");
    assert!(v.controller >= 0, "Metadata names no controller");

    // A topic change sent to a gateway that doesn't lead the tenant's unit
    // is refused with NOT_CONTROLLER; at the controller it commits.
    let create = CreateTopicsRequest {
        topics: vec![create_topics_request::CreatableTopic {
            name: topic.clone(),
            num_partitions: parts,
            replication_factor: -1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };
    let other = *v.brokers.keys().find(|b| **b != v.controller).unwrap();
    let r: CreateTopicsResponse = v.conn(other).await.unwrap().call(7, &create).await.unwrap();
    println!(
        "CreateTopics at broker {other}: {:?}",
        r.topics[0].error_code
    );
    assert_eq!(
        r.topics[0].error_code,
        err::NOT_CONTROLLER,
        "{:?}",
        r.topics[0]
    );
    let controller = v.controller;
    let r: CreateTopicsResponse = v
        .conn(controller)
        .await
        .unwrap()
        .call(7, &create)
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0, "{:?}", r.topics[0].error_message);

    v.refresh(Some(&topic)).await;
    if env("SPRIG_KAFKA_DEBUG").is_some() {
        let t0 = Instant::now();
        while v.leaders.is_empty() && t0.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            v.refresh(Some(&topic)).await;
        }
        println!("topic seen by Metadata after {:?}", t0.elapsed());
        for round in 0..3 {
            for (n, a) in v.brokers.clone() {
                let mut c = login(&a).await.unwrap();
                let m: MetadataResponse = c
                    .call(
                        12,
                        &MetadataRequest {
                            topics: Some(vec![metadata_request::MetadataRequestTopic {
                                name: Some(topic.clone()),
                                ..Default::default()
                            }]),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                println!(
                    "round {round} broker {n}: error {} partitions {}",
                    m.topics[0].error_code,
                    m.topics[0].partitions.len()
                );
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    println!("leaders {:?}", v.leaders);
    assert_eq!(v.leaders.len(), parts as usize);
    assert!(
        v.leaders.values().all(|l| v.brokers.contains_key(l)),
        "every partition led by a broker: {:?}",
        v.leaders
    );
    if env("SPRIG_KAFKA_SPREAD").is_some() {
        // Partition groups: the partitions' leaders are spread over the
        // gateways (after the first writes start the groups and they move
        // to their preferred gateways).
        for p in 0..parts {
            let _ = produce(
                &mut v,
                &topic,
                p,
                &batch("w", 0, 1),
                Duration::from_secs(10),
            )
            .await;
        }
        let t0 = Instant::now();
        let mut distinct = 0;
        while t0.elapsed() < Duration::from_secs(30) {
            v.refresh(Some(&topic)).await;
            let mut l: Vec<i32> = v.leaders.values().copied().collect();
            l.sort_unstable();
            l.dedup();
            distinct = l.len();
            if distinct == parts as usize {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        println!("spread leaders after {:?}: {:?}", t0.elapsed(), v.leaders);
        assert_eq!(
            distinct, parts as usize,
            "partition leaders spread: {:?}",
            v.leaders
        );
    }

    // A produce at a gateway that doesn't lead the partition:
    // NOT_LEADER_OR_FOLLOWER, so the client asks Metadata again.
    let l0 = v.leaders[&0];
    let other = *v.brokers.keys().find(|b| **b != l0).unwrap();
    let r: ProduceResponse = v
        .conn(other)
        .await
        .unwrap()
        .call(9, &produce_req(&topic, 0, batch("x", 0, 1)))
        .await
        .unwrap();
    assert_eq!(
        r.responses[0].partition_responses[0].error_code,
        err::NOT_LEADER_OR_FOLLOWER
    );

    // Produce through Metadata: every batch acknowledged at once.
    let mut acked: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    if env("SPRIG_KAFKA_SPREAD").is_some() {
        for p in 0..parts {
            acked.entry(p).or_default().push("w-0".into());
        }
    }
    let mut sent = 0;
    for round in 0..20 {
        for p in 0..parts {
            let (_, seen) = produce(
                &mut v,
                &topic,
                p,
                &batch("a", sent, 5),
                Duration::from_secs(10),
            )
            .await;
            assert!(seen.is_empty(), "round {round} partition {p}: {seen:?}");
            acked
                .entry(p)
                .or_default()
                .extend((sent..sent + 5).map(|i| format!("a-{i}")));
            sent += 5;
        }
    }
    for p in 0..parts {
        let got: Vec<String> = fetch_all(&mut v, &topic, p)
            .await
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(got, acked[&p], "partition {p}");
    }
    assert_eq!(coordinator(&mut v, &group).await, v.controller);
    let seen = commit_offset(&mut v, &group, &topic, 42).await;
    println!("offset commit retries before failover: {seen:?}");
    assert_eq!(committed(&mut v, &group).await, 42);
    println!("{sent} records acknowledged on {parts} partitions, read back; offset 42 committed");

    let Some(cmd) = env("SPRIG_KAFKA_FAILOVER") else {
        return;
    };
    let old = v.controller;
    let addr = v.brokers[&old].clone();
    println!("failover: running {cmd} {addr}");
    let started = Instant::now();
    let st = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("{cmd} {addr}"))
        .status()
        .unwrap();
    assert!(st.success(), "{cmd}: {st}");
    v.conns.clear();
    // Writes resume where Metadata now points.
    let mut first_ack = None;
    for round in 0..10 {
        for p in 0..parts {
            let (_, seen) = produce(
                &mut v,
                &topic,
                p,
                &batch("b", sent, 5),
                Duration::from_secs(60),
            )
            .await;
            if first_ack.is_none() {
                first_ack = Some(started.elapsed());
                println!(
                    "first acknowledgement after the kill: {:?} (errors on the way {seen:?})",
                    started.elapsed()
                );
            }
            let _ = round;
            acked
                .entry(p)
                .or_default()
                .extend((sent..sent + 5).map(|i| format!("b-{i}")));
            sent += 5;
        }
    }
    println!(
        "new leader {} (was {old}), leaders {:?}",
        v.controller, v.leaders
    );
    assert_ne!(v.controller, old, "the killed gateway can't still lead");
    for p in 0..parts {
        let got: Vec<String> = fetch_all(&mut v, &topic, p)
            .await
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        assert_eq!(got, acked[&p], "partition {p} after the failover");
    }
    assert_eq!(
        committed(&mut v, &group).await,
        42,
        "committed offset after the failover"
    );
    let seen = commit_offset(&mut v, &group, &topic, 43).await;
    println!("offset commit retries after failover: {seen:?}");
    assert_eq!(committed(&mut v, &group).await, 43);
    println!(
        "after the failover: {sent} records acknowledged in all, every one read back; RTO {:?}",
        first_ack.unwrap()
    );
}

/// Kill loop: one writer produces to six partitions round-robin through
/// Metadata (keeping a partition's leader when Metadata shows an older
/// leader epoch, as Kafka's clients do) while the tenant's leading gateway
/// is killed `SPRIG_KAFKA_KILLS` times, every `SPRIG_KAFKA_KILL_EVERY_S`
/// seconds, with `SPRIG_KAFKA_FAILOVER`. Reports the gaps between
/// acknowledgements and checks every acknowledged record is read back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a three-gateway cluster and a kill command"]
async fn live_kill_loop() {
    let db = env("SPRIG_KAFKA_DB").unwrap();
    let cmd = env("SPRIG_KAFKA_FAILOVER").expect("SPRIG_KAFKA_FAILOVER");
    let kills: usize = env("SPRIG_KAFKA_KILLS").map_or(10, |v| v.parse().unwrap());
    let every =
        Duration::from_secs(env("SPRIG_KAFKA_KILL_EVERY_S").map_or(7, |v| v.parse().unwrap()));
    let topic = format!("{db}.kill_{}", std::process::id());
    let parts = 6;
    let mut v = View::default();
    v.refresh(None).await;
    let create = CreateTopicsRequest {
        topics: vec![create_topics_request::CreatableTopic {
            name: topic.clone(),
            num_partitions: parts,
            replication_factor: -1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };
    let r: CreateTopicsResponse = v
        .conn(v.controller)
        .await
        .unwrap()
        .call(7, &create)
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0, "{:?}", r.topics[0].error_message);
    tokio::time::sleep(Duration::from_secs(1)).await;
    v.refresh(Some(&topic)).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let killer = {
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut k = View::default();
            for round in 0..kills {
                tokio::time::sleep(every).await;
                k.refresh(None).await;
                let addr = k.brokers[&k.controller].clone();
                let line = format!("{cmd} {addr}");
                let st = tokio::task::spawn_blocking(move || {
                    std::process::Command::new("sh")
                        .arg("-c")
                        .arg(line)
                        .status()
                        .unwrap()
                })
                .await
                .unwrap();
                println!("round {round}: killed {addr} ({st})");
            }
            tokio::time::sleep(every).await;
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        })
    };
    let started = Instant::now();
    let mut acks: Vec<Duration> = Vec::new();
    let mut acked: BTreeMap<i32, Vec<String>> = BTreeMap::new();
    let mut retried = 0usize;
    let mut sent = 0;
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        let p = i32::try_from(sent % 6).unwrap();
        let (_, seen) = produce(
            &mut v,
            &topic,
            p,
            &batch("k", sent, 1),
            Duration::from_secs(120),
        )
        .await;
        retried += seen.len();
        acks.push(started.elapsed());
        acked.entry(p).or_default().push(format!("k-{sent}"));
        sent += 1;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    killer.await.unwrap();
    let mut gaps: Vec<f64> = acks
        .windows(2)
        .map(|w| w[1].saturating_sub(w[0]).as_secs_f64())
        .collect();
    gaps.sort_by(|a, b| b.total_cmp(a));
    v.refresh(Some(&topic)).await;
    let mut missing = 0;
    for p in 0..parts {
        let got: std::collections::BTreeSet<String> = fetch_all(&mut v, &topic, p)
            .await
            .into_iter()
            .map(|(_, s)| s)
            .collect();
        missing += acked[&p].iter().filter(|r| !got.contains(*r)).count();
    }
    println!(
        "kill loop: {kills} kills, {sent} acknowledged, {retried} errors on the way, {} stale Metadata answers, \
         lost {missing}; gaps > 1 s: {}, > 5 s: {}, longest {:?}",
        v.stale,
        gaps.iter().filter(|g| **g > 1.0).count(),
        gaps.iter().filter(|g| **g > 5.0).count(),
        &gaps[..gaps.len().min(5)]
    );
    assert_eq!(missing, 0, "acknowledged records lost");
}
