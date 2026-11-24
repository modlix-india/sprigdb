//! Commit latency while a backup runs, against the same load without one
//! (ignored: a measurement, `cargo test --release -p sprig-backup --test
//! load -- --ignored --nocapture`). `NBACKUP_LOAD_MB` sets the backed-up
//! tenant's size (default 256).

#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{Dir, backup_cfg, key, open_engine, settle};
use sprig_backup::service::BackupService;
use sprig_engine::StoreKind;
use sprig_engine::txn::TxnOptions;

/// 8 writers committing one-row transactions to tenant 9 for `secs`;
/// returns (commits, p50 µs, p99 µs).
fn load(e: &sprig_engine::Engine, secs: u64) -> (usize, u64, u64) {
    let stop = Arc::new(AtomicBool::new(false));
    let hs: Vec<_> = (0..8u32)
        .map(|w| {
            let (e, stop) = (e.clone(), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut lat = Vec::new();
                let mut i = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    let mut x = e.begin(TxnOptions::default()).unwrap();
                    x.put(
                        key(9, w * 1_000_000 + i % 50_000),
                        Bytes::from_static(b"load-value-0123456789"),
                    )
                    .unwrap();
                    x.commit().unwrap().wait().unwrap();
                    lat.push(t.elapsed().as_micros() as u64);
                    i += 1;
                }
                lat
            })
        })
        .collect();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    let mut all: Vec<u64> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    let p = |q: f64| all[((all.len() as f64 - 1.0) * q) as usize];
    (all.len(), p(0.50), p(0.99))
}

#[test]
#[ignore = "a measurement"]
fn commit_latency_with_and_without_a_backup() {
    let mb: usize = std::env::var("NBACKUP_LOAD_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);
    let d = Dir::new("load");
    let e = open_engine(&d.0.join("engine"), StoreKind::Rocks);
    // Tenant 7: `mb` MiB of 1 KiB rows.
    let rows = mb * 1024;
    let val = "z".repeat(1000);
    for b in 0..rows / 500 {
        let mut x = e.begin(TxnOptions::default()).unwrap();
        for i in 0..500 {
            x.put(
                key(7, (b * 500 + i) as u32),
                Bytes::from(format!("{b}{val}")),
            )
            .unwrap();
        }
        x.commit().unwrap().wait().unwrap();
    }
    settle(&e, 7);
    e.checkpoint().unwrap();
    let rate: u64 = std::env::var("NBACKUP_RATE_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    let mut cfg = backup_cfg(&d);
    cfg.rate_mb_per_sec = Arc::new(move || rate);
    let svc = BackupService::open(e.clone(), cfg, Arc::new(common::TestKeys)).unwrap();
    let (n0, p50_0, p99_0) = load(&e, 10);
    let running = Arc::new(AtomicBool::new(true));
    let bk = {
        let (svc, running) = (svc.clone(), Arc::clone(&running));
        std::thread::spawn(move || {
            let mut n = 0;
            let mut took = Vec::new();
            while running.load(Ordering::Relaxed) {
                let t = Instant::now();
                svc.backup_now(7).unwrap();
                took.push(t.elapsed().as_millis());
                n += 1;
            }
            (n, took)
        })
    };
    let (n1, p50_1, p99_1) = load(&e, 10);
    running.store(false, Ordering::Relaxed);
    let (backups, took) = bk.join().unwrap();
    println!(
        "rate {rate} MiB/s; tenant {mb} MiB; without a backup: {n0} commits, p50 {p50_0} µs, p99 {p99_0} µs"
    );
    println!(
        "with backups running ({backups} taken, ms each {took:?}): {n1} commits, p50 {p50_1} µs, p99 {p99_1} µs"
    );
    println!("p99 ratio {:.2}", p99_1 as f64 / p99_0.max(1) as f64);
}
