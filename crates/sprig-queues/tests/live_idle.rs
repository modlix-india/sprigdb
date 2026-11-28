//! Many vhosts through a running gateway, for measuring what idle vhosts
//! cost (`queue.vhost_idle_close_seconds`):
//!
//! ```text
//! SPRIG_IDLE_AMQP_ADDR=127.0.0.1:29672 SPRIG_IDLE_USER=r9idle.app SPRIG_IDLE_PASSWORD=... \
//! SPRIG_IDLE_PREFIX=r9i_ SPRIG_IDLE_N=300 SPRIG_IDLE_PAR=8 SPRIG_IDLE_MODE=open \
//! cargo test -p sprig-queues --test live_idle -- --nocapture --test-threads 1
//! ```
//!
//! The databases `<prefix>0001` to `<prefix><N>` must exist. `open`
//! declares a durable queue `q` in each one's vhost and publishes one
//! message with a confirm; `read` takes it back, so the vhosts closed in
//! between must open again with their messages. Each vhost gets one
//! connection, closed after use, at most `PAR` at a time and never more
//! than 50 new connections a second, so the client leaves few sockets in
//! `TIME_WAIT`. Without `SPRIG_IDLE_AMQP_ADDR` the test returns at once.

#![allow(clippy::unwrap_used, clippy::print_stderr, missing_docs)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicPublishOptions, ConfirmSelectOptions,
    QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use tokio::sync::Mutex;

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// One vhost: `open` declares and publishes, else reads the message back.
async fn one(addr: &str, user: &str, pass: &str, vhost: &str, open: bool) -> Result<(), String> {
    let c = Connection::connect(
        &format!("amqp://{user}:{pass}@{addr}/{vhost}"),
        ConnectionProperties::default(),
    )
    .await
    .map_err(|e| format!("{vhost}: connect: {e}"))?;
    let ch = c.create_channel().await.map_err(|e| e.to_string())?;
    let r = async {
        if open {
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .map_err(|e| e.to_string())?;
            ch.queue_declare(
                "q".into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| format!("declare: {e}"))?;
            let ok = ch
                .basic_publish(
                    "".into(),
                    "q".into(),
                    BasicPublishOptions::default(),
                    vhost.as_bytes(),
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .map_err(|e| e.to_string())?
                .await
                .map_err(|e| e.to_string())?
                .is_ack();
            if !ok {
                return Err("publish nacked".to_owned());
            }
        } else {
            let m = ch
                .basic_get("q".into(), BasicGetOptions::default())
                .await
                .map_err(|e| format!("get: {e}"))?
                .ok_or("no message")?;
            if &*m.delivery.data != vhost.as_bytes() {
                return Err("another message".to_owned());
            }
            m.ack(BasicAckOptions::default())
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    .await;
    let _ = c.close(200, "done".into()).await;
    r.map_err(|e| format!("{vhost}: {e}"))
}

#[ignore = "live: needs SPRIG_IDLE_AMQP_ADDR; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_vhosts() {
    let Some(addr) = var("SPRIG_IDLE_AMQP_ADDR") else {
        eprintln!("skipped: set SPRIG_IDLE_AMQP_ADDR");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let user = var("SPRIG_IDLE_USER").unwrap();
    let pass = var("SPRIG_IDLE_PASSWORD").unwrap();
    let prefix = var("SPRIG_IDLE_PREFIX").unwrap();
    let n: usize = var("SPRIG_IDLE_N").unwrap().parse().unwrap();
    let par: usize = var("SPRIG_IDLE_PAR").map_or(8, |p| p.parse().unwrap());
    let open = var("SPRIG_IDLE_MODE").as_deref() != Some("read");
    let start = Instant::now();
    // At most 50 new connections a second.
    let pace = Arc::new(Mutex::new(Instant::now()));
    let failed: Vec<String> = stream::iter(1..=n)
        .map(|i| {
            let (addr, user, pass, pace) = (addr.clone(), user.clone(), pass.clone(), &pace);
            let vhost = format!("{prefix}{i:04}");
            async move {
                {
                    let mut next = pace.lock().await;
                    tokio::time::sleep_until((*next).into()).await;
                    *next = Instant::now() + Duration::from_millis(20);
                }
                one(&addr, &user, &pass, &vhost, open).await.err()
            }
        })
        .buffer_unordered(par)
        .filter_map(|r| async move { r })
        .collect()
        .await;
    eprintln!(
        "{} {n} vhosts in {:.1} s; {} failed",
        if open { "opened" } else { "read" },
        start.elapsed().as_secs_f64(),
        failed.len()
    );
    for f in failed.iter().take(10) {
        eprintln!("  {f}");
    }
    assert!(failed.is_empty());
}
