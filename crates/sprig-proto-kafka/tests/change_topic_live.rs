//! Live check of a table's change topic (`<db>.<table>.changes`) on a
//! running gateway's `kafka` listener, with this crate's own client.
//!
//! Ignored by default. Run with
//!
//! ```text
//! SPRIG_KAFKA_ADDR=127.0.0.1:9092 SPRIG_KAFKA_USER=acme.app SPRIG_KAFKA_PASSWORD=... \
//! SPRIG_KAFKA_TOPIC=shop.items.changes SPRIG_KAFKA_EXPECT=533 \
//! cargo test -p sprig-proto-kafka --test change_topic_live -- --ignored --nocapture
//! ```
//!
//! The listener must take SASL PLAIN without TLS (`listener.kafka.tls =
//! off`). The test reads the topic from its start to its high watermark,
//! checks the offsets only grow and that it holds `SPRIG_KAFKA_EXPECT`
//! change events, and prints each event's value as one line.

#![allow(
    clippy::unwrap_used,
    clippy::doc_markdown,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;

use sprig_proto_kafka::client::Client;
use sprig_proto_kafka::messages::*;
use sprig_proto_kafka::protocol::err;
use sprig_proto_kafka::records::RecordBatch;
use tokio::net::TcpStream;

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("{k} is not set"))
}

fn list_offsets(topic: &str, ts: i64) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: -1,
        topics: vec![list_offsets_request::ListOffsetsTopic {
            name: topic.into(),
            partitions: vec![list_offsets_request::ListOffsetsPartition {
                partition_index: 0,
                current_leader_epoch: -1,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn fetch_req(topic: &str, offset: i64, max_wait_ms: i32) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: 8 << 20,
        isolation_level: 1,
        session_epoch: -1,
        topics: vec![fetch_request::FetchTopic {
            topic: topic.into(),
            partitions: vec![fetch_request::FetchPartition {
                partition: 0,
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

#[tokio::test]
#[ignore = "needs a running gateway with a Kafka listener and an engine table's changes"]
async fn change_topic_reads_every_change_in_order() {
    let topic = env("SPRIG_KAFKA_TOPIC");
    let expect: usize = env("SPRIG_KAFKA_EXPECT").parse().unwrap();
    let tcp = TcpStream::connect(env("SPRIG_KAFKA_ADDR")).await.unwrap();
    let mut c = Client::new(tcp, "change-topic-live");
    c.sasl_plain(&env("SPRIG_KAFKA_USER"), &env("SPRIG_KAFKA_PASSWORD"))
        .await
        .unwrap();
    let r: ListOffsetsResponse = c.call(7, &list_offsets(&topic, -2)).await.unwrap();
    let p = &r.topics[0].partitions[0];
    assert_eq!(p.error_code, err::NONE, "{r:?}");
    let start = p.offset;
    let r: ListOffsetsResponse = c.call(7, &list_offsets(&topic, -1)).await.unwrap();
    let hw = r.topics[0].partitions[0].offset;
    println!("bounds [{start}, {hw})");
    let mut at = start;
    let mut seen = 0usize;
    let mut last = -1i64;
    let mut ops: BTreeMap<String, usize> = BTreeMap::new();
    let mut fetches = 0;
    while at < hw {
        fetches += 1;
        let r: FetchResponse = c.call(12, &fetch_req(&topic, at, 500)).await.unwrap();
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, err::NONE, "fetch at {at}: {p:?}");
        let batches =
            RecordBatch::decode_all(p.records.as_deref().unwrap_or(&[]), 1 << 26, true).unwrap();
        let mut next = at;
        for b in batches {
            next = next.max(b.base_offset + i64::from(b.last_offset_delta) + 1);
            for rec in b.records {
                let o = b.base_offset + i64::from(rec.offset_delta);
                assert!(o > last, "offset {o} after {last}");
                last = o;
                let v = String::from_utf8(rec.value.unwrap_or_default()).unwrap();
                let op = v
                    .split_once("\"op\":\"")
                    .and_then(|(_, r)| r.split('"').next())
                    .unwrap_or("?");
                *ops.entry(op.to_owned()).or_default() += 1;
                println!(
                    "{o} {} {v}",
                    String::from_utf8_lossy(&rec.key.unwrap_or_default())
                );
                seen += 1;
            }
        }
        assert!(next > at || fetches < 3, "no progress at {at}");
        at = next;
    }
    println!("events {seen} ops {ops:?} fetches {fetches}");
    assert_eq!(seen, expect);
    // At the high watermark a fetch waits and comes back empty.
    let r: FetchResponse = c.call(12, &fetch_req(&topic, at, 300)).await.unwrap();
    let p = &r.responses[0].partitions[0];
    assert_eq!(p.error_code, err::NONE);
    assert!(p.records.as_deref().unwrap_or(&[]).len() < 70, "{p:?}");
}
