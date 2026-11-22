//! AMQP publish and consume through the in-process queue engine, the
//! market report's `PerfTest` cell: one durable queue, one producer
//! publishing persistent 1000-byte messages with up to 100 unconfirmed,
//! one consumer acknowledging each delivery; rates and the latency from
//! publish to delivery.
//!
//! ```text
//! SPRIG_BENCH_STORE=engine|maria|memory \
//!   cargo test -p sprig-queues --release --test engine_bench -- --ignored --nocapture
//! ```
//!
//! `engine`: messages on the storage engine (commit log with full syncs);
//! `maria`: the MariaDB store over `SPRIG_BENCH_MARIA`
//! (`mysql://root:...@127.0.0.1:<port>`, schema `msbench_q`);
//! `SPRIG_BENCH_AMQP=host:port` with `SPRIG_BENCH_USER`/`SPRIG_BENCH_PASS`
//! runs the same client against another broker instead (`RabbitMQ`).
//! `SPRIG_BENCH_SECS` (default 15), `SPRIG_BENCH_SIZE` (1000).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::cast_possible_wrap,
    missing_docs
)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ConfirmSelectOptions,
    QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64)
}

async fn target() -> (String, &'static str, Option<common::Server>) {
    if let Ok(addr) = std::env::var("SPRIG_BENCH_AMQP") {
        let user = std::env::var("SPRIG_BENCH_USER").unwrap_or_else(|_| "guest".into());
        let pass = std::env::var("SPRIG_BENCH_PASS").unwrap_or_else(|_| "guest".into());
        return (format!("amqp://{user}:{pass}@{addr}/%2f"), "external", None);
    }
    let store = std::env::var("SPRIG_BENCH_STORE").unwrap_or_else(|_| "engine".into());
    let c = common::mem_catalog();
    let kind: &'static str = match store.as_str() {
        "engine" => {
            let dir = std::env::temp_dir().join(format!("sprig-qbench-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let mut cfg = sprig_engine::EngineConfig::new(&dir);
            cfg.budget.engine_mb = 256;
            if std::env::var("SPRIG_BENCH_SYNC").as_deref() == Ok("off") {
                cfg.log.sync = sprig_engine::log::SyncMode::Off;
            }
            let e = sprig_engine::Engine::open(cfg).unwrap();
            let q = Arc::new(sprig_queues::enginestore::EngineQueues::new(
                e,
                sprig_engine::Durability::Local,
            ));
            c.set_store_factory(Arc::new(move |t, d| {
                q.store(t, d) as Arc<dyn sprig_queues::store::MsgStore>
            }));
            "engine"
        }
        "maria" => {
            let url = std::env::var("SPRIG_BENCH_MARIA").unwrap();
            let opts =
                mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url(&url).unwrap())
                    .pool_opts(
                        mysql_async::PoolOpts::new()
                            .with_constraints(mysql_async::PoolConstraints::new(1, 16).unwrap())
                            .with_inactive_connection_ttl(Duration::from_secs(60)),
                    );
            let pool = mysql_async::Pool::new(opts);
            {
                use mysql_async::prelude::Queryable;
                let mut conn = pool.get_conn().await.unwrap();
                conn.query_drop("DROP DATABASE IF EXISTS msbench_q")
                    .await
                    .unwrap();
                conn.query_drop("CREATE DATABASE msbench_q").await.unwrap();
            }
            let s = Arc::new(sprig_queues::maria::MariaStore::new(pool, "msbench_q"));
            s.ensure().await.unwrap();
            c.set_store_factory(Arc::new(move |_, _| {
                Arc::clone(&s) as Arc<dyn sprig_queues::store::MsgStore>
            }));
            "maria"
        }
        _ => "memory",
    };
    let srv = common::server_on(
        c,
        Arc::new(sprig_queues::mem::MemLeases::new().for_gateway("gw-bench")),
        1,
    )
    .await;
    (
        format!("amqp://{}:{}@{}/%2f", srv.user, srv.pass, srv.addr),
        kind,
        Some(srv),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a benchmark: run in release with --ignored --nocapture"]
async fn amqp_publish_consume_latency() {
    let secs: u64 = std::env::var("SPRIG_BENCH_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);
    let size: usize = std::env::var("SPRIG_BENCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let (uri, kind, _srv) = target().await;
    let q = format!("bench-pt-{}", std::process::id());
    let pconn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let cconn = Connection::connect(&uri, ConnectionProperties::default())
        .await
        .unwrap();
    let pch = pconn.create_channel().await.unwrap();
    pch.queue_declare(
        q.as_str().into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    pch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    let cch = cconn.create_channel().await.unwrap();
    let mut consumer = cch
        .basic_consume(
            q.as_str().into(),
            "bench".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let received = Arc::new(AtomicU64::new(0));
    // The consumer: latency from the publish time in the body.
    let cons = {
        let stop = Arc::clone(&stop);
        let received = Arc::clone(&received);
        tokio::spawn(async move {
            let mut lat = Vec::with_capacity(1 << 20);
            let mut warm = true;
            let started = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let Ok(Some(d)) =
                    tokio::time::timeout(Duration::from_secs(2), consumer.next()).await
                else {
                    continue;
                };
                let d = d.unwrap();
                let sent = u64::from_be_bytes(d.data[..8].try_into().unwrap());
                if warm && started.elapsed() > Duration::from_secs(2) {
                    warm = false;
                }
                if !warm {
                    lat.push(now_us().saturating_sub(sent));
                    received.fetch_add(1, Ordering::Relaxed);
                }
                d.ack(BasicAckOptions::default()).await.unwrap();
            }
            lat
        })
    };
    // The producer: up to 100 unconfirmed.
    let mut pending = std::collections::VecDeque::new();
    let mut sent = 0u64;
    let started = Instant::now();
    let warm = Duration::from_secs(2);
    let mut counted_from = None;
    let mut sent_at_count = 0u64;
    // `SPRIG_BENCH_RATE`: messages a second (0: as fast as confirms allow).
    let rate: u64 = std::env::var("SPRIG_BENCH_RATE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut issued = 0u64;
    while started.elapsed() < warm + Duration::from_secs(secs) {
        if rate > 0 {
            let due = Duration::from_secs_f64(issued as f64 / rate as f64);
            let el = started.elapsed();
            if let Some(gap) = due.checked_sub(el)
                && pending.is_empty()
            {
                tokio::time::sleep(gap).await;
            }
        }
        while pending.len() < 100
            && (rate == 0
                || Duration::from_secs_f64(issued as f64 / rate as f64) <= started.elapsed())
        {
            issued += 1;
            let mut body = vec![b'x'; size.max(8)];
            body[..8].copy_from_slice(&now_us().to_be_bytes());
            let c = pch
                .basic_publish(
                    "".into(),
                    q.as_str().into(),
                    BasicPublishOptions::default(),
                    &body,
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .unwrap();
            pending.push_back(c);
        }
        let Some(c) = pending.pop_front() else {
            continue;
        };
        let c = c.await.unwrap();
        assert!(c.is_ack());
        sent += 1;
        if counted_from.is_none() && started.elapsed() > warm {
            counted_from = Some(Instant::now());
            sent_at_count = sent;
        }
    }
    let span = counted_from.map_or(1.0, |t| t.elapsed().as_secs_f64());
    let sent_rate = (sent - sent_at_count) as f64 / span;
    for c in pending {
        let _ = c.await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    let mut lat = cons.await.unwrap();
    lat.sort_unstable();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 - 1.0) * p) as usize)
            .copied()
            .unwrap_or(0) as f64
            / 1000.0
    };
    let recv_rate = received.load(Ordering::Relaxed) as f64 / (span + 2.0);
    println!(
        "BENCH amqp store={kind} size={size} secs={secs} rate={rate}: send {sent_rate:.0} msg/s, receive {recv_rate:.0} msg/s, latency p50 {:.2} ms p99 {:.2} ms (n={})",
        pct(0.50),
        pct(0.99),
        lat.len()
    );
    let _ = pch
        .queue_delete(q.as_str().into(), QueueDeleteOptions::default())
        .await;
}
