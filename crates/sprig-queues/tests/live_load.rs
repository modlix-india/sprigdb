//! Load and failover checks against a gateway cluster (ignored by
//! default; they need running gateways):
//!
//! ```text
//! SPRIG_AMQP_ADDRS=127.0.0.1:26315,127.0.0.1:26325,127.0.0.1:26335 \
//! SPRIG_AMQP_USER=acme.app SPRIG_AMQP_PASSWORD=... \
//! cargo test -p sprig-queues --test live_load -- --ignored --nocapture --test-threads 1
//! ```
//!
//! - `throughput`: publishers with confirms and consumers with manual
//!   acknowledgements on one durable queue (`SPRIG_AMQP_PUBLISHERS`,
//!   `SPRIG_AMQP_CONSUMERS`, `SPRIG_AMQP_SECS`, 1 KiB bodies, a confirm
//!   window of 1,000 per publisher, prefetch 1,000): rates and the
//!   consumers' latency from publish to delivery. `SPRIG_AMQP_CONSUME_AFTER`
//!   (seconds) holds the consumers back so a backlog builds first; the
//!   received rate while it drains is the delivery ceiling.
//! - `failover_loses_nothing`: one publisher at `SPRIG_AMQP_RATE` messages a
//!   second with confirms and one consumer, both reconnecting through the
//!   listed gateways, while `SPRIG_AMQP_FAILOVER` (a command that kills the
//!   gateway leading the tenant, and may start it again) runs
//!   `SPRIG_AMQP_KILLS` times every `SPRIG_AMQP_KILL_EVERY_S` seconds:
//!   every confirmed message must be delivered; reports the longest gaps
//!   between confirms and between deliveries.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::similar_names,
    missing_docs
)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn num(k: &str, d: u64) -> u64 {
    var(k).map_or(d, |v| v.parse().unwrap())
}

fn addrs() -> Vec<String> {
    var("SPRIG_AMQP_ADDRS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect()
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

/// A connection through the `i`-th gateway (round robin), retried.
async fn connect(i: usize) -> Connection {
    let (u, p) = (
        var("SPRIG_AMQP_USER").unwrap(),
        var("SPRIG_AMQP_PASSWORD").unwrap(),
    );
    let a = addrs();
    let mut k = i;
    loop {
        let addr = &a[k % a.len()];
        if let Ok(c) = Connection::connect(
            &format!("amqp://{u}:{p}@{addr}/%2f"),
            ConnectionProperties::default(),
        )
        .await
        {
            return c;
        }
        k += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn declare(ch: &Channel, q: &str) -> lapin::Result<()> {
    let mut args = FieldTable::default();
    args.insert(
        "x-queue-type".into(),
        AMQPValue::LongString("quorum".into()),
    );
    ch.queue_declare(
        q.into(),
        QueueDeclareOptions {
            durable: true,
            ..QueueDeclareOptions::default()
        },
        args,
    )
    .await
    .map(|_| ())
}

fn body(id: u64) -> Vec<u8> {
    let mut b = vec![b'x'; 1024];
    b[..8].copy_from_slice(&id.to_be_bytes());
    b[8..16].copy_from_slice(&now_us().to_be_bytes());
    b
}

fn parse(b: &[u8]) -> (u64, u64) {
    (
        u64::from_be_bytes(b[..8].try_into().unwrap()),
        u64::from_be_bytes(b[8..16].try_into().unwrap()),
    )
}

fn pct(v: &mut [u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i] as f64 / 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a running gateway"]
async fn throughput() {
    let q = format!("load_{}", now_us());
    let (np, nc, secs) = (
        num("SPRIG_AMQP_PUBLISHERS", 4),
        num("SPRIG_AMQP_CONSUMERS", 4),
        num("SPRIG_AMQP_SECS", 20),
    );
    let c = connect(0).await;
    declare(&c.create_channel().await.unwrap(), &q)
        .await
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let got = Arc::new(AtomicU64::new(0));
    let lat = Arc::new(Mutex::new(Vec::<u64>::new()));
    let mut tasks = Vec::new();
    for i in 0..nc {
        let (q, stop, got, lat) = (q.clone(), stop.clone(), got.clone(), lat.clone());
        tasks.push(tokio::spawn(async move {
            // A backlog first measures how fast delivery drains it.
            tokio::time::sleep(Duration::from_secs(num("SPRIG_AMQP_CONSUME_AFTER", 0))).await;
            let c = connect(i as usize).await;
            let ch = c.create_channel().await.unwrap();
            ch.basic_qos(1000, BasicQosOptions::default())
                .await
                .unwrap();
            let mut s = ch
                .basic_consume(
                    q.as_str().into(),
                    format!("c{i}").as_str().into(),
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();
            while !stop.load(Ordering::Relaxed) {
                let Ok(Some(Ok(d))) =
                    tokio::time::timeout(Duration::from_millis(200), s.next()).await
                else {
                    continue;
                };
                let (_, t) = parse(&d.data);
                lat.lock().unwrap().push(now_us().saturating_sub(t));
                got.fetch_add(1, Ordering::Relaxed);
                d.ack(BasicAckOptions::default()).await.unwrap();
            }
        }));
    }
    let started = Instant::now();
    for i in 0..np {
        let (q, stop, sent) = (q.clone(), stop.clone(), sent.clone());
        tasks.push(tokio::spawn(async move {
            let c = connect(i as usize).await;
            let ch = c.create_channel().await.unwrap();
            ch.confirm_select(ConfirmSelectOptions::default())
                .await
                .unwrap();
            let mut window = FuturesUnordered::new();
            let mut id = i << 40;
            while !stop.load(Ordering::Relaxed) {
                while window.len() < 1000 {
                    id += 1;
                    let pc = ch
                        .basic_publish(
                            "".into(),
                            q.as_str().into(),
                            BasicPublishOptions::default(),
                            &body(id),
                            BasicProperties::default().with_delivery_mode(2),
                        )
                        .await
                        .unwrap();
                    window.push(pc);
                }
                if let Some(r) = window.next().await {
                    assert!(r.unwrap().is_ack());
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    let mut last = (0, 0);
    for s in 1..=secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (a, b) = (sent.load(Ordering::Relaxed), got.load(Ordering::Relaxed));
        println!(
            "t={s}s sent {}/s received {}/s backlog {}",
            a - last.0,
            b - last.1,
            a.saturating_sub(b)
        );
        last = (a, b);
    }
    stop.store(true, Ordering::Relaxed);
    let el = started.elapsed().as_secs_f64();
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(10), t).await;
    }
    let mut l = lat.lock().unwrap().clone();
    println!(
        "throughput: {np} publishers x {nc} consumers, {secs} s: sent {:.0} msg/s, received {:.0} msg/s, consumer latency p50 {:.1} ms p99 {:.1} ms",
        sent.load(Ordering::Relaxed) as f64 / el,
        got.load(Ordering::Relaxed) as f64 / el,
        pct(&mut l, 0.5),
        pct(&mut l, 0.99)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a gateway cluster and a kill command"]
async fn failover_loses_nothing() {
    let q = format!("kill_{}", now_us());
    let cmd = var("SPRIG_AMQP_FAILOVER").unwrap();
    let (rate, kills, every) = (
        num("SPRIG_AMQP_RATE", 500),
        num("SPRIG_AMQP_KILLS", 10),
        num("SPRIG_AMQP_KILL_EVERY_S", 10),
    );
    let c = connect(0).await;
    declare(&c.create_channel().await.unwrap(), &q)
        .await
        .unwrap();
    drop(c);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::new(AtomicBool::new(false));
    let confirmed = Arc::new(Mutex::new(Vec::<(u64, Instant)>::new()));
    let received = Arc::new(Mutex::new(Vec::<(u64, Instant)>::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let consumer = {
        let (q, stop_c, received, errors) =
            (q.clone(), stop_c.clone(), received.clone(), errors.clone());
        tokio::spawn(async move {
            let mut k = 1;
            while !stop_c.load(Ordering::Relaxed) {
                let c = connect(k).await;
                k += 1;
                let Ok(ch) = c.create_channel().await else {
                    continue;
                };
                if ch.basic_qos(100, BasicQosOptions::default()).await.is_err() {
                    continue;
                }
                let Ok(mut s) = ch
                    .basic_consume(
                        q.as_str().into(),
                        "kc".into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                else {
                    errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                loop {
                    if stop_c.load(Ordering::Relaxed) {
                        return;
                    }
                    match tokio::time::timeout(Duration::from_millis(500), s.next()).await {
                        Err(_) => {}
                        Ok(Some(Ok(d))) => {
                            received
                                .lock()
                                .unwrap()
                                .push((parse(&d.data).0, Instant::now()));
                            if d.ack(BasicAckOptions::default()).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => break,
                    }
                }
                errors.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    let publisher = {
        let (q, stop, confirmed, errors) =
            (q.clone(), stop.clone(), confirmed.clone(), errors.clone());
        tokio::spawn(async move {
            let mut id = 0u64;
            let mut k = 0;
            let gap = Duration::from_micros(1_000_000 / rate.max(1));
            while !stop.load(Ordering::Relaxed) {
                let c = connect(k).await;
                k += 1;
                let Ok(ch) = c.create_channel().await else {
                    continue;
                };
                if ch
                    .confirm_select(ConfirmSelectOptions::default())
                    .await
                    .is_err()
                {
                    continue;
                }
                let mut window = FuturesUnordered::new();
                let mut next = Instant::now();
                let broken = loop {
                    if stop.load(Ordering::Relaxed) {
                        break false;
                    }
                    if Instant::now() >= next && window.len() < 100 {
                        id += 1;
                        next += gap;
                        let this = id;
                        match ch
                            .basic_publish(
                                "".into(),
                                q.as_str().into(),
                                BasicPublishOptions::default(),
                                &body(this),
                                BasicProperties::default().with_delivery_mode(2),
                            )
                            .await
                        {
                            Ok(pc) => window.push(async move { (this, pc.await) }),
                            Err(_) => break true,
                        }
                        continue;
                    }
                    tokio::select! {
                        Some((this, r)) = window.next(), if !window.is_empty() => match r {
                            Ok(c) if c.is_ack() => confirmed.lock().unwrap().push((this, Instant::now())),
                            Ok(_) => { errors.fetch_add(1, Ordering::Relaxed); }
                            Err(_) => break true,
                        },
                        () = tokio::time::sleep_until(next.into()) => {}
                    }
                };
                if broken {
                    errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        })
    };
    for round in 0..kills {
        tokio::time::sleep(Duration::from_secs(every)).await;
        let st = tokio::task::spawn_blocking({
            let cmd = cmd.clone();
            move || {
                std::process::Command::new("sh")
                    .arg("-c")
                    .arg(cmd)
                    .status()
                    .unwrap()
            }
        })
        .await
        .unwrap();
        println!("round {round}: {cmd}: {st}");
    }
    tokio::time::sleep(Duration::from_secs(every)).await;
    // Let the consumer catch up with everything confirmed.
    let want: BTreeSet<u64> = confirmed.lock().unwrap().iter().map(|c| c.0).collect();
    let deadline = Instant::now() + Duration::from_secs(60);
    stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(10), publisher).await;
    let want: BTreeSet<u64> = confirmed
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.0)
        .chain(want)
        .collect();
    loop {
        let got: BTreeSet<u64> = received.lock().unwrap().iter().map(|r| r.0).collect();
        let missing = want.difference(&got).count();
        if missing == 0 || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    stop_c.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(Duration::from_secs(10), consumer).await;
    let rec = received.lock().unwrap().clone();
    let got: BTreeSet<u64> = rec.iter().map(|r| r.0).collect();
    let missing: Vec<u64> = want.difference(&got).copied().take(10).collect();
    let gaps = |v: &[(u64, Instant)]| {
        let mut t: Vec<Instant> = v.iter().map(|x| x.1).collect();
        t.sort_unstable();
        let mut g: Vec<f64> = t
            .windows(2)
            .map(|w| w[1].saturating_duration_since(w[0]).as_secs_f64())
            .collect();
        g.sort_by(|a, b| b.total_cmp(a));
        g.truncate(5);
        g
    };
    println!(
        "failover: {kills} kills; {} confirmed, {} delivered ({} distinct, {} again), {} errors on the way; \
         longest confirm gaps {:?} s, delivery gaps {:?} s; confirmed but never delivered: {} {missing:?}",
        want.len(),
        rec.len(),
        got.len(),
        rec.len() - got.len(),
        errors.load(Ordering::Relaxed),
        gaps(&confirmed.lock().unwrap()),
        gaps(&rec),
        want.difference(&got).count()
    );
    assert!(missing.is_empty(), "confirmed messages lost: {missing:?}");
}
