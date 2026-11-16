//! A gateway closes a vhost nobody uses (`queue.vhost_idle_close_seconds`)
//! and opens it again on the next use, in process:
//!
//! - closed only after its last connection went and the idle time
//!   passed: its owners' and forwarder's leases are given back, and the
//!   messages are all there when it opens again;
//! - a consumer, or an `x-expires` queue, keeps it open;
//! - a delayed message that falls due, and a message expiring at a
//!   queue's head, open it again by themselves: the delayed message
//!   reaches its queue and the expired one its dead-letter queue with no
//!   client connected.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::similar_names,
    missing_docs
)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Server, mem_catalog, server_on, uniq};
use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicGetOptions, BasicPublishOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::{BasicProperties, Channel, ExchangeKind};
use sprig_queues::QueueEngine;
use sprig_queues::engine::{FORWARDER_OBJECT, MqCatalog, VhostRef};
use sprig_queues::mem::{MemCatalog, MemLeases};
use sprig_queues::settings::QueueSettings;

const TENANT: u64 = 42;

fn args(kv: &[(&str, AMQPValue)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in kv {
        t.insert(ShortString::from(*k), v.clone());
    }
    t
}

fn s(v: &str) -> AMQPValue {
    AMQPValue::LongString(v.into())
}

/// One gateway over a fresh catalog whose vhosts close after `idle_ms`.
async fn gateway(idle_ms: u64) -> (Server, Arc<MemCatalog>, MemLeases, Arc<QueueEngine>) {
    let catalog = mem_catalog();
    catalog.set_settings(QueueSettings {
        vhost_idle_close_ms: idle_ms,
        takeover_grace_ms: 50,
        ..QueueSettings::default()
    });
    let leases = MemLeases::new();
    let srv = server_on(
        Arc::clone(&catalog),
        Arc::new(leases.for_gateway("gw-idle")),
        1,
    )
    .await;
    let e = Arc::clone(srv.engine.as_ref().unwrap());
    (srv, catalog, leases, e)
}

async fn connect(srv: &Server) -> (lapin::Connection, Channel) {
    let conn = common::lapin(srv).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    (conn, ch)
}

async fn queue(ch: &Channel, q: &str, a: FieldTable) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        a,
    )
    .await
    .unwrap();
}

async fn send(ch: &Channel, x: &str, key: &str, body: &str, p: BasicProperties) {
    let c = ch
        .basic_publish(
            x.into(),
            key.into(),
            BasicPublishOptions::default(),
            body.as_bytes(),
            p,
        )
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(c.is_ack());
}

async fn get(ch: &Channel, q: &str) -> Option<String> {
    let m = ch
        .basic_get(q.into(), BasicGetOptions::default())
        .await
        .unwrap()?;
    m.ack(BasicAckOptions::default()).await.unwrap();
    Some(String::from_utf8_lossy(&m.delivery.data).into_owned())
}

/// The vhost `/` (database `mq`) as the catalog maps it.
async fn vref(catalog: &MemCatalog) -> VhostRef {
    let p = sprig_proto_amqp::auth::Principal {
        tenant_id: TENANT,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    };
    catalog.open(&p, "/").await.unwrap()
}

/// Waits up to `limit` for the vhost to be open (`open`) or closed.
async fn wait_open(e: &QueueEngine, db_id: u32, open: bool, limit: Duration) -> Duration {
    let start = Instant::now();
    while e.is_open(TENANT, db_id) != open {
        assert!(
            start.elapsed() < limit,
            "vhost not {} after {limit:?}",
            if open { "open" } else { "closed" }
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    start.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_vhost_closes_and_opens_again() {
    let (srv, catalog, leases, e) = gateway(400).await;
    let v = vref(&catalog).await;
    let (conn, ch) = connect(&srv).await;
    let q = uniq("idle");
    queue(&ch, &q, FieldTable::default()).await;
    for i in 0..3 {
        send(&ch, "", &q, &format!("m{i}"), BasicProperties::default()).await;
    }
    assert_eq!(get(&ch, &q).await.as_deref(), Some("m0"));
    let qid = catalog.topology(&v).queues[&q].id;
    assert_eq!(leases.holder(TENANT, qid).as_deref(), Some("gw-idle"));
    // An open connection keeps it open past the idle time.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(e.is_open(TENANT, v.db_id), "open while a connection is");
    assert_eq!(e.open_vhosts(), 1);
    conn.close(200, "bye".into()).await.unwrap();
    let took = wait_open(&e, v.db_id, false, Duration::from_secs(6)).await;
    eprintln!("closed {took:?} after the last connection (idle time 400 ms)");
    assert!(took >= Duration::from_millis(300), "{took:?}");
    assert_eq!(e.open_vhosts(), 0);
    // The owner's and the forwarder's leases are back.
    let start = Instant::now();
    while leases.holder(TENANT, qid).is_some()
        || leases
            .holder(TENANT, FORWARDER_OBJECT | u64::from(v.db_id))
            .is_some()
    {
        assert!(start.elapsed() < Duration::from_secs(3), "leases kept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The next use opens it again, with every message.
    let (conn, ch) = connect(&srv).await;
    assert!(e.is_open(TENANT, v.db_id));
    assert_eq!(get(&ch, &q).await.as_deref(), Some("m1"));
    assert_eq!(get(&ch, &q).await.as_deref(), Some("m2"));
    assert_eq!(get(&ch, &q).await, None);
    conn.close(200, "bye".into()).await.unwrap();
    wait_open(&e, v.db_id, false, Duration::from_secs(6)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consumer_or_an_expiring_queue_keeps_it_open() {
    let (srv, catalog, _leases, e) = gateway(300).await;
    let v = vref(&catalog).await;
    // A consumer on another connection.
    let (conn, ch) = connect(&srv).await;
    let q = uniq("consumed");
    queue(&ch, &q, FieldTable::default()).await;
    let (cconn, cch) = connect(&srv).await;
    let mut consumer = cch
        .basic_consume(
            q.as_str().into(),
            "c".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    conn.close(200, "bye".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(e.is_open(TENANT, v.db_id), "a consumer keeps it open");
    // It still delivers.
    let (conn, ch) = connect(&srv).await;
    send(&ch, "", &q, "hello", BasicProperties::default()).await;
    let d = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&*d.data, b"hello");
    d.ack(BasicAckOptions::default()).await.unwrap();
    // An `x-expires` queue's clock needs its owner.
    let xq = uniq("expires");
    queue(
        &ch,
        &xq,
        args(&[("x-expires", AMQPValue::LongInt(600_000))]),
    )
    .await;
    send(&ch, "", &xq, "x", BasicProperties::default()).await;
    conn.close(200, "bye".into()).await.unwrap();
    cconn.close(200, "bye".into()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        e.is_open(TENANT, v.db_id),
        "an x-expires queue's owner keeps it open"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn due_and_expiring_messages_open_it_again() {
    let (srv, catalog, _leases, e) = gateway(300).await;
    let v = vref(&catalog).await;
    let (conn, ch) = connect(&srv).await;
    let x = uniq("delayed");
    let later = uniq("later");
    ch.exchange_declare(
        x.as_str().into(),
        ExchangeKind::Custom("x-delayed-message".into()),
        ExchangeDeclareOptions {
            durable: true,
            ..ExchangeDeclareOptions::default()
        },
        args(&[("x-delayed-type", s("direct"))]),
    )
    .await
    .unwrap();
    queue(&ch, &later, FieldTable::default()).await;
    ch.queue_bind(
        later.as_str().into(),
        x.as_str().into(),
        "k".into(),
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    // A queue whose message expires into a dead-letter queue.
    let ttl = uniq("ttl");
    let dlq = uniq("dead");
    queue(&ch, &dlq, FieldTable::default()).await;
    queue(
        &ch,
        &ttl,
        args(&[
            ("x-dead-letter-exchange", s("")),
            ("x-dead-letter-routing-key", s(&dlq)),
        ]),
    )
    .await;
    let start = Instant::now();
    send(
        &ch,
        &x,
        "k",
        "later",
        BasicProperties::default().with_headers(args(&[("x-delay", AMQPValue::LongInt(4_000))])),
    )
    .await;
    send(
        &ch,
        "",
        &ttl,
        "short-lived",
        BasicProperties::default().with_expiration("6000".into()),
    )
    .await;
    conn.close(200, "bye".into()).await.unwrap();
    wait_open(&e, v.db_id, false, Duration::from_secs(3)).await;
    assert!(
        start.elapsed() < Duration::from_millis(3_500),
        "closed before due"
    );
    // Due at 4 s: open again with no client.
    wait_open(&e, v.db_id, true, Duration::from_secs(5)).await;
    let woke = start.elapsed();
    eprintln!("opened again {woke:?} after the 4 s delayed publish");
    assert!(woke >= Duration::from_millis(3_900), "{woke:?}");
    // Back to closed after the release, then open at the expiry at 6 s.
    // Check the results without a connection touching those queues' owners
    // first: the delayed message is in `later`, the expired one in `dlq`.
    tokio::time::sleep(Duration::from_millis(3_500)).await;
    let (conn, ch) = connect(&srv).await;
    let mut dead = None;
    for _ in 0..50 {
        dead = get(&ch, &dlq).await;
        if dead.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        dead.as_deref(),
        Some("short-lived"),
        "dead-lettered at expiry"
    );
    assert_eq!(get(&ch, &ttl).await, None);
    assert_eq!(get(&ch, &later).await.as_deref(), Some("later"));
    conn.close(200, "bye".into()).await.unwrap();
}
