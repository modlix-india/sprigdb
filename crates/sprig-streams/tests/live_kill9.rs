//! kill -9 of a gateway serving Kafka topics stored on the engine: a
//! producer keeps producing while a script kills the gateway with SIGKILL
//! and starts it again; every batch that was acknowledged is there
//! afterwards, at its offsets, and producing continues after them.
//!
//! ```text
//! SPRIG_LIVE_KAFKA=127.0.0.1:64092 SPRIG_LIVE_KAFKA_USER=t.app SPRIG_LIVE_KAFKA_PASSWORD=... \
//! SPRIG_LIVE_KAFKA_DB=<database> SPRIG_GATEWAY_CRASH='<script that kill -9s and restarts the gateway>' \
//!   cargo test -p sprig-streams --test live_kill9 -- --ignored --nocapture
//! ```
//!
//! Logs in with SCRAM-SHA-256 over plaintext.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sprig_proto_kafka::client::Client;
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::records::{Record, RecordBatch};
use sprig_proto_kafka::sasl::ScramMechanism;
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn login() -> Option<Client<TcpStream>> {
    let addr = env("SPRIG_LIVE_KAFKA")?;
    let s = TcpStream::connect(&addr).await.ok()?;
    let mut c = Client::new(s, "live-kill9");
    c.sasl_scram(
        ScramMechanism::Sha256,
        &env("SPRIG_LIVE_KAFKA_USER").unwrap(),
        &env("SPRIG_LIVE_KAFKA_PASSWORD").unwrap(),
    )
    .await
    .ok()?;
    Some(c)
}

async fn login_retrying() -> Client<TcpStream> {
    for _ in 0..240 {
        if let Some(c) = login().await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("no login within a minute");
}

fn batch(n: u32, i: u32) -> Vec<u8> {
    RecordBatch {
        last_offset_delta: 1,
        base_timestamp: 1_763_000_000_000,
        max_timestamp: 1_763_000_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..2)
            .map(|j| Record {
                offset_delta: j,
                key: Some(format!("k{n}").into_bytes()),
                value: Some(format!("round{n}-batch{i}-rec{j}").into_bytes()),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
    .to_bytes()
    .unwrap()
}

fn produce_req(topic: &str, records: Vec<u8>) -> ProduceRequest {
    ProduceRequest {
        acks: -1,
        timeout_ms: 5000,
        topic_data: vec![produce_request::TopicProduceData {
            name: topic.into(),
            partition_data: vec![produce_request::PartitionProduceData {
                index: 0,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running gateway and a crash script"]
async fn acknowledged_produces_survive_a_gateway_kill9() {
    let Some(crash) = env("SPRIG_GATEWAY_CRASH") else {
        println!("skipped: set SPRIG_GATEWAY_CRASH");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = env("SPRIG_LIVE_KAFKA_DB").unwrap();
    let topic = format!("{db}.kill9_{}", std::process::id());
    let mut c = login_retrying().await;
    let r: CreateTopicsResponse = c
        .call(
            7,
            &CreateTopicsRequest {
                topics: vec![create_topics_request::CreatableTopic {
                    name: topic.clone(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 10_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(r.topics[0].error_code, 0, "{:?}", r.topics[0].error_message);
    // value -> offset of every acknowledged record.
    let mut acked: BTreeMap<i64, String> = BTreeMap::new();
    for round in 0..3u32 {
        // Produce in the background of the crash: the script runs while
        // batches are in flight.
        let script = crash.clone();
        let killer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            let out = tokio::task::spawn_blocking(move || {
                std::process::Command::new("bash")
                    .arg("-c")
                    .arg(&script)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        });
        let started = Instant::now();
        let mut i = 0u32;
        let mut errors = 0u32;
        while !killer.is_finished() && started.elapsed() < Duration::from_secs(120) {
            i += 1;
            let r: Result<ProduceResponse, _> =
                c.call(9, &produce_req(&topic, batch(round, i))).await;
            if let Ok(r) = r {
                let p = &r.responses[0].partition_responses[0];
                if p.error_code == 0 {
                    for j in 0..2 {
                        acked.insert(p.base_offset + j, format!("round{round}-batch{i}-rec{j}"));
                    }
                } else {
                    errors += 1;
                }
            } else {
                // The gateway went away: log in again once it is back.
                errors += 1;
                c = login_retrying().await;
            }
        }
        killer.await.unwrap();
        c = login_retrying().await;
        println!(
            "round {round}: {i} batches sent, {} records acknowledged so far, {errors} failed sends",
            acked.len()
        );
        // Every acknowledged record is there.
        let mut off = 0i64;
        let mut seen: BTreeMap<i64, String> = BTreeMap::new();
        loop {
            let q = FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 1,
                max_bytes: 8 << 20,
                isolation_level: 1,
                session_epoch: -1,
                topics: vec![fetch_request::FetchTopic {
                    topic: topic.clone(),
                    partitions: vec![fetch_request::FetchPartition {
                        partition: 0,
                        current_leader_epoch: -1,
                        fetch_offset: off,
                        partition_max_bytes: 8 << 20,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let r: FetchResponse = c.call(12, &q).await.unwrap();
            let p = &r.responses[0].partitions[0];
            assert_eq!(p.error_code, 0);
            let mut any = false;
            for b in RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 64 << 20, true)
                .unwrap()
            {
                for rec in b.records {
                    let o = b.base_offset + i64::from(rec.offset_delta);
                    if o >= off {
                        any = true;
                        seen.insert(o, String::from_utf8(rec.value.unwrap()).unwrap());
                    }
                }
            }
            if let Some((o, _)) = seen.last_key_value() {
                off = o + 1;
            }
            if !any || off >= p.high_watermark {
                break;
            }
        }
        for (o, v) in &acked {
            assert_eq!(
                seen.get(o),
                Some(v),
                "round {round}: acknowledged offset {o} lost"
            );
        }
        let offsets: Vec<i64> = seen.keys().copied().collect();
        assert_eq!(
            offsets,
            (0..offsets.len() as i64).collect::<Vec<_>>(),
            "dense offsets"
        );
    }
    let _: Result<DeleteTopicsResponse, _> = c
        .call(
            6,
            &DeleteTopicsRequest {
                topics: vec![delete_topics_request::DeleteTopicState {
                    name: Some(topic.clone()),
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            },
        )
        .await;
    assert!(acked.len() > 100, "{} acknowledged", acked.len());
}
