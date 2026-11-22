//! kill -9 of a gateway while clients declare durable queues and
//! exchanges and bind them (each one catalog change whose rows are the
//! engine's catalog records), then a check of the topology after the
//! restart.
//!
//! ```text
//! SPRIG_AMQP_ADDR=127.0.0.1:57672 SPRIG_AMQP_USER=... SPRIG_AMQP_PASSWORD=... \
//! SPRIG_GATEWAY_KILL='<kill -9 the gateway>' SPRIG_GATEWAY_START='<start it, wait until ready>' \
//! SPRIG_KILL_ROUNDS=6 cargo test -p sprig-queues --test catalog_kill9_live -- --nocapture
//! ```
//!
//! Each round a client declares `q<i>` and `x<i>` and binds `x<i>` to
//! `q<i>` with key `k<i>`, one after another, recording what the gateway
//! acknowledged; the gateway is killed 0.3 to 1.5 s in and started again.
//! Then for every acknowledged declare the object exists (a passive
//! declare succeeds), and for every acknowledged bind a message published
//! to `x<i>` with key `k<i>` reaches `q<i>`. Unacknowledged ones may or may
//! not exist; nothing else is allowed. Without `SPRIG_AMQP_ADDR` the test
//! returns at once.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lapin::options::{
    BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
    ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

async fn amqp() -> Connection {
    let addr = var("SPRIG_AMQP_ADDR").unwrap();
    let (u, p) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    for _ in 0..120 {
        if let Ok(c) = Connection::connect(
            &format!("amqp://{u}:{p}@{addr}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("can't connect to {addr}");
}

fn sh(cmd: &str) {
    let st = std::process::Command::new("bash")
        .args(["-c", cmd])
        .status()
        .unwrap();
    assert!(st.success(), "{cmd}: {st}");
}

#[derive(Default)]
struct Acked {
    queues: AtomicUsize,
    exchanges: AtomicUsize,
    binds: AtomicUsize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declares_survive_kill_9() {
    if var("SPRIG_AMQP_ADDR").is_none() {
        eprintln!("skipped: set SPRIG_AMQP_ADDR");
        return;
    }
    let kill = var("SPRIG_GATEWAY_KILL").unwrap();
    let start = var("SPRIG_GATEWAY_START").unwrap();
    let rounds: usize = var("SPRIG_KILL_ROUNDS").map_or(6, |r| r.parse().unwrap());
    let mut total = 0;
    for round in 0..rounds {
        let tag = format!("s3k{round}_{}", std::process::id());
        let acked = Arc::new(Acked::default());
        let writer = {
            let acked = Arc::clone(&acked);
            let tag = tag.clone();
            tokio::spawn(async move {
                let conn = amqp().await;
                let ch = conn.create_channel().await.unwrap();
                for i in 0..10_000 {
                    let q = format!("{tag}_q{i}");
                    let x = format!("{tag}_x{i}");
                    let ok = ch
                        .queue_declare(
                            q.as_str().into(),
                            QueueDeclareOptions {
                                durable: true,
                                ..QueueDeclareOptions::default()
                            },
                            FieldTable::default(),
                        )
                        .await;
                    if ok.is_err() {
                        return;
                    }
                    acked.queues.store(i + 1, Ordering::SeqCst);
                    if ch
                        .exchange_declare(
                            x.as_str().into(),
                            ExchangeKind::Direct,
                            ExchangeDeclareOptions {
                                durable: true,
                                ..ExchangeDeclareOptions::default()
                            },
                            FieldTable::default(),
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                    acked.exchanges.store(i + 1, Ordering::SeqCst);
                    if ch
                        .queue_bind(
                            q.as_str().into(),
                            x.as_str().into(),
                            format!("k{i}").as_str().into(),
                            QueueBindOptions::default(),
                            FieldTable::default(),
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                    acked.binds.store(i + 1, Ordering::SeqCst);
                }
            })
        };
        let wait = 300 + (round as u64 * 241) % 1200;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        sh(&kill);
        let _ = writer.await;
        sh(&start);
        let (nq, nx, nb) = (
            acked.queues.load(Ordering::SeqCst),
            acked.exchanges.load(Ordering::SeqCst),
            acked.binds.load(Ordering::SeqCst),
        );
        total += nq + nx + nb;

        let conn = amqp().await;
        let ch = conn.create_channel().await.unwrap();
        ch.confirm_select(ConfirmSelectOptions::default())
            .await
            .unwrap();
        for i in 0..nq {
            let q = format!("{tag}_q{i}");
            ch.queue_declare(
                q.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|e| panic!("round {round}: acknowledged queue {q} lost: {e}"));
        }
        for i in 0..nx {
            let x = format!("{tag}_x{i}");
            ch.exchange_declare(
                x.as_str().into(),
                ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    passive: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|e| panic!("round {round}: acknowledged exchange {x} lost: {e}"));
        }
        for i in 0..nb {
            let (q, x) = (format!("{tag}_q{i}"), format!("{tag}_x{i}"));
            let c = ch
                .basic_publish(
                    x.as_str().into(),
                    format!("k{i}").as_str().into(),
                    BasicPublishOptions::default(),
                    b"probe",
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .unwrap()
                .await
                .unwrap();
            assert!(c.is_ack(), "round {round}: publish to {x}: {c:?}");
            let mut got = None;
            for _ in 0..50 {
                got = ch
                    .basic_get(q.as_str().into(), BasicGetOptions { no_ack: true })
                    .await
                    .unwrap();
                if got.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(
                got.is_some(),
                "round {round}: acknowledged binding {x} -> {q} lost"
            );
        }
        eprintln!(
            "round {round}: killed after {wait} ms; acknowledged {nq} queues, {nx} exchanges, {nb} bindings: all present"
        );
        // Clean up this round's objects (acknowledged and maybe not).
        for i in 0..=nq {
            let _ = ch
                .queue_delete(
                    format!("{tag}_q{i}").as_str().into(),
                    QueueDeleteOptions::default(),
                )
                .await;
            let _ = ch
                .exchange_delete(
                    format!("{tag}_x{i}").as_str().into(),
                    ExchangeDeleteOptions::default(),
                )
                .await;
        }
        let _ = conn.close(0, "done".into()).await;
    }
    eprintln!("{rounds} rounds, {total} acknowledged catalog changes, 0 lost");
}
