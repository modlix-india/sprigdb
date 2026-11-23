//! What idle tenants cost: N tenants, each its own unit with one commit,
//! then the node sits idle. Prints resident memory before and after the
//! units exist, and the CPU the idle node uses.
//!
//! `cargo run --release -p sprig-engine --example idle_units -- <dir> [tenants] [idle secs]`

// A measuring tool: it panics on any failure.
#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, UnitId};

/// This process's resident set (KB) and CPU time used (seconds), from `ps`.
fn usage() -> (u64, f64) {
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid])
        .output()
        .unwrap();
    let s = String::from_utf8_lossy(&out.stdout);
    let mut it = s.split_whitespace();
    let rss: u64 = it.next().unwrap().parse().unwrap();
    // [[dd-]hh:]mm:ss.cc
    let t = it.next().unwrap();
    let mut secs = 0.0;
    for part in t.replace('-', ":").split(':') {
        secs = secs * 60.0 + part.parse::<f64>().unwrap();
    }
    (rss, secs)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(args.get(1).expect("a directory"));
    let n: u64 = args.get(2).map_or(10_000, |a| a.parse().unwrap());
    let idle: u64 = args.get(3).map_or(60, |a| a.parse().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = EngineConfig::new(&dir);
    cfg.budget.engine_mb = 512;
    let e = Engine::open(cfg).unwrap();
    // Warm up one unit, so the baseline holds the engine's fixed costs.
    let base_tenant = 1_000_000;
    let commit = |tenant: u64| {
        let mut k = KeyPrefix {
            tenant,
            shard: 0,
            object: 1,
            kind: kind::PRIMARY,
            index: 0,
        }
        .to_vec();
        k.push(1);
        let mut t = e.begin(TxnOptions::default()).unwrap();
        t.put(k, Bytes::from_static(b"row")).unwrap();
        t.commit().unwrap().wait().unwrap();
    };
    commit(base_tenant);
    std::thread::sleep(Duration::from_secs(2));
    let (rss0, _) = usage();
    let t0 = Instant::now();
    for i in 1..=n {
        commit(base_tenant + i);
    }
    let made = t0.elapsed();
    // The commits are durable; wait for the last one's apply too.
    let deadline = Instant::now() + Duration::from_secs(30);
    while e.consensus().applied_index(UnitId(base_tenant + n)) < 1 {
        assert!(
            Instant::now() < deadline,
            "the last unit's commit wasn't applied"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    e.checkpoint().unwrap();
    std::thread::sleep(Duration::from_secs(5));
    let (rss1, cpu1) = usage();
    let t1 = Instant::now();
    std::thread::sleep(Duration::from_secs(idle));
    let (rss2, cpu2) = usage();
    let wall = t1.elapsed().as_secs_f64();
    let stats = e.stats();
    println!(
        "idle_units: {n} units made in {made:.1?} ({} units in the log); \
         rss before {} MB, after {} MB (+{} MB, {:.1} KB/unit), after {idle} s idle {} MB; \
         idle CPU {:.3}% of one core",
        stats.units,
        rss0 / 1024,
        rss1 / 1024,
        rss1.saturating_sub(rss0) / 1024,
        rss1.saturating_sub(rss0) as f64 / n as f64,
        rss2 / 1024,
        (cpu2 - cpu1) / wall * 100.0
    );
    drop(e);
    let _ = std::fs::remove_dir_all(&dir);
}
