//! The feed-driven forwarder's engine side (publishing inside a
//! database transaction), in process:
//!
//! - `SqlVhost::forward_outbox` enqueues outbox rows in the order it is
//!   given (the feed's commit order), not the table's (insert order);
//! - the forward ledger keeps a replayed forward a no-op after the
//!   message was consumed (a forwarder that crashed between the enqueue
//!   and the delete);
//! - a batch of publishes with message ids (a change exchange's) reaches a
//!   queue in order, each id once, before and after it is consumed;
//! - with `queue.forwarder = feed` the polling forwarder leaves young
//!   transactional publishes to the feed and takes them only after
//!   `queue.forward_fallback_ms`; with `poll` it takes them at once.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{lapin, server_with, uniq};
use lapin::options::{BasicAckOptions, BasicGetOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use sprig_proto_amqp::auth::Principal;
use sprig_queues::QueueEngine;
use sprig_queues::settings::QueueSettings;
use sprig_queues::sql::{SqlVhost, TxnWrite, message};
use sprig_queues::store::{MsgId, OutboxRow};

fn principal() -> Arc<Principal> {
    Arc::new(Principal {
        tenant_id: 42,
        tenant: "acme".into(),
        user: "app".into(),
        user_id: Some(1),
        handle: None,
    })
}

async fn vhost(e: &Arc<QueueEngine>) -> SqlVhost {
    e.sql_vhost(principal(), "/", 7).await.unwrap()
}

/// An outbox row as a transactional publish of `body` to queue `q` makes.
async fn row(v: &SqlVhost, q: &str, body: &str) -> OutboxRow {
    let m = message("", q, body.as_bytes().to_vec(), None).unwrap();
    match v.publish_in_txn(m, true).await.unwrap() {
        TxnWrite::Outbox(r, n) => {
            assert_eq!(n, 1);
            r
        }
        other => panic!("not an outbox row: {other:?}"),
    }
}

async fn drain(ch: &lapin::Channel, q: &str) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(m) = ch
        .basic_get(q.into(), BasicGetOptions::default())
        .await
        .unwrap()
    {
        out.push(String::from_utf8_lossy(&m.delivery.data).into_owned());
        m.delivery.ack(BasicAckOptions::default()).await.unwrap();
    }
    out
}

async fn declare(ch: &lapin::Channel, q: &str) {
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forwards_in_the_order_given_and_never_twice() {
    let s = server_with(QueueSettings {
        forward_feed: true,
        forward_fallback_ms: 600_000,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("fwd-order");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    // 40 rows inserted in order 0..40 (their ids rise with it); the
    // "commit order" is a shuffle of them.
    let mut rows = Vec::new();
    for i in 0..40 {
        rows.push(row(&v, &q, &format!("m{i}")).await);
    }
    store.put_outbox(rows.clone());
    let mut order: Vec<usize> = (0..40).collect();
    // A fixed shuffle: odd ones first, backwards, then the even ones.
    order.sort_by_key(|i| (i % 2 == 0, std::cmp::Reverse(*i)));
    let ids: Vec<MsgId> = order.iter().map(|i| rows[*i].out_id).collect();
    let (fwd, gone) = v.forward_outbox(&ids[..25]).await.unwrap();
    assert_eq!((fwd, gone), (25, 0));
    let (fwd, gone) = v.forward_outbox(&ids[25..]).await.unwrap();
    assert_eq!((fwd, gone), (15, 0));
    assert_eq!(store.outbox_len(), 0, "forwarded rows are deleted");
    let got = drain(&ch, &q).await;
    let want: Vec<String> = order.iter().map(|i| format!("m{i}")).collect();
    assert_eq!(got, want, "enqueued in the order given (commit order)");

    // A forwarder that crashed after its enqueue and before its delete:
    // the row is back, its message consumed meanwhile. Forwarding it again
    // adds nothing (the forward ledger), and the row is deleted.
    store.put_outbox(vec![rows[3].clone(), rows[4].clone()]);
    let (fwd, gone) = v
        .forward_outbox(&[rows[3].out_id, rows[4].out_id, [9; 16]])
        .await
        .unwrap();
    assert_eq!((fwd, gone), (2, 1), "two still there, one unknown");
    assert_eq!(store.outbox_len(), 0);
    assert!(drain(&ch, &q).await.is_empty(), "no message twice");

    // The ledger keeps rows while their outbox row exists or they are
    // young; a purge before now + 1 s takes the 40 forwarded ones.
    let n = sprig_queues::store::MsgStore::purge_ledger(&*store, i64::MAX, i64::MAX)
        .await
        .unwrap();
    assert_eq!(n, 40);
    conn.close(0, "done".into()).await.unwrap();
}

/// A change exchange's publishes: a batch of messages, each
/// as its message id, reaches a queue in order and each id once, while the
/// message waits and after it was consumed (the forward ledger), within a
/// batch too; a refused message ends the batch; a publish without an id is
/// never deduplicated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_of_messages_with_ids_reaches_a_queue_once_each() {
    let s = server_with(QueueSettings::default()).await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("pub-once");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let m = |body: &str| message("", &q, body.as_bytes().to_vec(), None).unwrap();
    let id = |i: u8| -> MsgId { [i; 16] };
    let ok = |r: Vec<Result<u32, _>>| -> Vec<u32> { r.into_iter().map(Result::unwrap).collect() };
    // 50 in one batch, the fifth twice in it: in order, once each.
    let mut batch: Vec<_> = (0..50u8).map(|i| (m(&format!("m{i}")), id(i))).collect();
    batch.insert(10, (m("m4 again"), id(4)));
    assert_eq!(ok(v.publish_batch_once(batch).await), vec![1; 51]);
    let want: Vec<String> = (0..50).map(|i| format!("m{i}")).collect();
    assert_eq!(drain(&ch, &q).await, want);
    // The same ids again after they were consumed (a forwarder's replay
    // after a restart): the ledger keeps them out; a new id goes in.
    let again: Vec<_> = (0..51u8).map(|i| (m(&format!("r{i}")), id(i))).collect();
    assert_eq!(ok(v.publish_batch_once(again).await), vec![1; 51]);
    assert_eq!(drain(&ch, &q).await, ["r50"]);
    // A message to an exchange that doesn't exist ends the batch.
    let bad = message("no-such-exchange", "k", b"x".to_vec(), None).unwrap();
    let r = v
        .publish_batch_once(vec![(m("n1"), id(101)), (bad, id(102)), (m("n3"), id(103))])
        .await;
    assert_eq!(r.len(), 2);
    assert!(r[0].is_ok() && r[1].is_err(), "{r:?}");
    assert_eq!(drain(&ch, &q).await, ["n1"]);
    // Without an id, every publish is a new message.
    v.publish_now(m("c"), true).await.unwrap();
    v.publish_now(m("c"), true).await.unwrap();
    assert_eq!(drain(&ch, &q).await, ["c", "c"]);
    conn.close(0, "done".into()).await.unwrap();
}

/// A change exchange's ids (version 8) stay in the forward
/// ledger longer than other ids, so a forwarder that comes back late still
/// publishes its last changes once: a purge with the general cutoff past
/// takes the others only, and the change ids still keep their consumed
/// messages out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_exchange_ids_outlive_other_ledger_rows() {
    let s = server_with(QueueSettings::default()).await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("cx-ledger");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    let m = |body: &str| message("", &q, body.as_bytes().to_vec(), None).unwrap();
    // Byte 6's high nibble is the UUID version: 0x8_ a change id, 0x7_ not.
    let id = |change: bool, i: u8| -> MsgId {
        let mut b = [i; 16];
        b[6] = if change { 0x80 } else { 0x70 } | (i & 0x0f);
        b
    };
    assert!(sprig_queues::store::is_change_id(&id(true, 1)));
    assert!(!sprig_queues::store::is_change_id(&id(false, 1)));
    let batch = |tag: &str| -> Vec<_> {
        (0..6u8)
            .map(|i| (m(&format!("{tag}{i}")), id(i % 2 == 0, i)))
            .collect()
    };
    let ok = |r: Vec<Result<u32, _>>| r.into_iter().all(|x| x.is_ok());
    assert!(ok(v.publish_batch_once(batch("a")).await));
    assert_eq!(drain(&ch, &q).await.len(), 6, "consumed");
    // Past the general cutoff, not the change ids' one: the three other
    // rows go.
    let purge = |before: i64, changes: i64| {
        let store = Arc::clone(&store);
        async move {
            sprig_queues::store::MsgStore::purge_ledger(&*store, before, changes)
                .await
                .unwrap()
        }
    };
    assert_eq!(purge(i64::MAX, 0).await, 3);
    // The forwarder comes back and publishes the same six again: the
    // change ids are still no-ops; the others, forgotten, go in again.
    assert!(ok(v.publish_batch_once(batch("b")).await));
    assert_eq!(drain(&ch, &q).await, ["b1", "b3", "b5"]);
    assert_eq!(purge(i64::MAX, 0).await, 3);
    assert_eq!(
        purge(i64::MAX, i64::MAX).await,
        3,
        "the change ids go after their own cutoff"
    );
    conn.close(0, "done".into()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_poller_leaves_young_publishes_to_the_feed() {
    let s = server_with(QueueSettings {
        forward_feed: true,
        forward_fallback_ms: 1_200,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("fwd-fallback");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    let start = Instant::now();
    store.put_outbox(vec![row(&v, &q, "late").await]);
    v.committed();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(store.outbox_len(), 1, "a young publish waits for the feed");
    // The fallback: forwarded by the poller once older than 1.2 s.
    let mut got = Vec::new();
    while got.is_empty() && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        got = drain(&ch, &q).await;
    }
    assert_eq!(got, ["late"]);
    let waited = start.elapsed();
    assert!(
        waited >= Duration::from_millis(1_200),
        "fallback after the delay, not before ({waited:?})"
    );
    conn.close(0, "done".into()).await.unwrap();

    // `poll`: the old behaviour, at once.
    let s = server_with(QueueSettings {
        forward_feed: false,
        ..QueueSettings::default()
    })
    .await;
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = s.catalog.as_ref().unwrap().store_of(42, "mq").unwrap();
    let start = Instant::now();
    store.put_outbox(vec![row(&v, &q, "now").await]);
    v.committed();
    let mut got = Vec::new();
    while got.is_empty() && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        got = drain(&ch, &q).await;
    }
    assert_eq!(got, ["now"]);
    assert!(start.elapsed() < Duration::from_millis(1_000));
    conn.close(0, "done".into()).await.unwrap();
}

/// Regression (`live_forward`'s commit-order inversions): the fallback read
/// late transactional publishes in insert order while the feed was still
/// forwarding them in commit order, and the two raced into the queue. Now,
/// while the feed-driven forwarder runs, the fallback leaves a publish to
/// it until the publish is older than `FALLBACK_WHILE_FEED_RUNS_MS` (30 s);
/// when the forwarder stops, `queue.forward_fallback_ms` applies again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_fallback_leaves_late_publishes_to_a_running_feed() {
    let s = server_with(QueueSettings {
        forward_feed: true,
        forward_fallback_ms: 300,
        ..QueueSettings::default()
    })
    .await;
    let Some(cat) = s.catalog.as_ref() else {
        return; // an external server: nothing to switch
    };
    cat.set_feed_forwarder_runs(true);
    let e = Arc::clone(s.engine.as_ref().unwrap());
    let conn = lapin(&s).await;
    let ch = conn.create_channel().await.unwrap();
    let q = uniq("fwd-feed-runs");
    declare(&ch, &q).await;
    let v = vhost(&e).await;
    v.ensure_store().await.unwrap();
    let store = cat.store_of(42, "mq").unwrap();
    // One publish past the 300 ms fallback delay, one past the 30 s
    // (a run the feed gave up on): only the second is the fallback's.
    let late = row(&v, &q, "late").await;
    let mut given_up = row(&v, &q, "given-up").await;
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let wait = i64::try_from(sprig_queues::engine::FALLBACK_WHILE_FEED_RUNS_MS).unwrap();
    let mut old = sprig_queues::store::msg_id_floor(now_ms - wait - 10_000);
    old[15] = 1;
    given_up.out_id = old;
    store.put_outbox(vec![late.clone(), given_up]);
    v.committed();
    let start = Instant::now();
    let mut got = Vec::new();
    while start.elapsed() < Duration::from_millis(1_500) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        got.extend(drain(&ch, &q).await);
    }
    assert_eq!(
        got,
        ["given-up"],
        "while the feed runs, a 1.5 s old publish stays the feed's"
    );
    assert_eq!(store.outbox_len(), 1);
    // The forwarder stops: the fallback takes the row after its delay.
    cat.set_feed_forwarder_runs(false);
    let start = Instant::now();
    while got.len() < 2 && start.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        got.extend(drain(&ch, &q).await);
    }
    assert_eq!(got, ["given-up", "late"]);
    assert_eq!(store.outbox_len(), 0);
    conn.close(0, "done".into()).await.unwrap();
}
