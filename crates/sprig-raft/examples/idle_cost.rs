//! Idle cost of quiesced groups: three nodes in one process (raft-engine
//! logs on disk, in-process transport, production timings), `N` groups each
//! with one write, all quiesced; then the process's CPU and memory are
//! sampled while idle.
//!
//! ```text
//! cargo run --release -p sprig-raft --example idle_cost -- <groups> <dir> [idle_secs]
//! ```
//!
//! Prints per-node figures: resident memory added by the groups, CPU as a
//! share of one core while idle, and frames sent per second.

#![allow(clippy::too_many_lines, clippy::cast_precision_loss)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_common::time::SystemClock;
use sprig_engine::log::{Durability, SyncMode};
use sprig_raft::toy::{self, KvMachine};
use sprig_raft::{EngineLogStore, MemNetwork, Node, NodeOptions, RaftConfig};

/// (resident KiB, CPU seconds) of this process, from `ps`.
fn sample() -> (u64, f64) {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    let s = String::from_utf8_lossy(&out.stdout);
    let mut it = s.split_whitespace();
    let rss = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
    // [[dd-]hh:]mm:ss.cc
    let t = it.next().unwrap_or("0:0");
    let mut secs = 0.0;
    for part in t.split(':') {
        let p = part.split('-').next_back().unwrap_or("0");
        secs = secs * 60.0 + p.parse::<f64>().unwrap_or(0.0);
    }
    (rss, secs)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let groups: u64 = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(10_000);
    let dir = std::path::PathBuf::from(args.get(2).cloned().unwrap_or_else(|| "idle-cost".into()));
    let idle_secs: u64 = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(60);
    let _ = std::fs::remove_dir_all(&dir);
    let (rss0, _) = sample();
    let net = MemNetwork::new();
    let mut nodes = Vec::new();
    let mut threads = Vec::new();
    let mut stores = Vec::new();
    for id in 1..=3u64 {
        let store = EngineLogStore::open(dir.join(format!("n{id}")), SyncMode::Full, None)
            .expect("log store");
        stores.push(Arc::clone(&store));
        let node = Node::open(NodeOptions {
            id,
            config: RaftConfig::default(),
            store,
            transport: net.transport(id),
            clock: Arc::new(SystemClock),
            machines: Arc::new(KvMachine::factory()),
            observer: None,
            seed: id,
        })
        .expect("open");
        net.register(&node);
        threads.push(node.spawn().expect("spawn"));
        nodes.push(node);
    }
    let t0 = Instant::now();
    let chunk = 2000;
    let mut g = 1;
    while g <= groups {
        let end = (g + chunk).min(groups + 1);
        let mut slots = Vec::new();
        for n in &nodes {
            for gid in g..end {
                slots.push(n.create_group(gid, &[1, 2, 3]).expect("create"));
            }
        }
        for s in slots {
            s.wait_timeout(Duration::from_secs(60))
                .expect("created")
                .expect("created");
        }
        // Spread leadership: each group campaigns on one node at once.
        for gid in g..end {
            let _ = nodes[usize::try_from(gid % 3).unwrap_or(0)].campaign(gid);
        }
        g = end;
    }
    eprintln!("created {groups} groups on 3 nodes in {:?}", t0.elapsed());
    // One write per group.
    let mut pending: Vec<u64> = (1..=groups).collect();
    while !pending.is_empty() {
        let mut slots = Vec::new();
        let mut retry = Vec::new();
        for gid in pending {
            let leader = nodes
                .iter()
                .find(|n| n.status(gid).is_some_and(|s| s.is_leader));
            match leader.map(|l| l.propose(gid, toy::put(1, gid), Durability::Strict)) {
                Some(Ok(s)) => slots.push((gid, s)),
                _ => retry.push(gid),
            }
        }
        for (gid, s) in slots {
            if !matches!(s.wait_timeout(Duration::from_secs(30)), Some(Ok(_))) {
                retry.push(gid);
            }
        }
        pending = retry;
        if !pending.is_empty() {
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    eprintln!("wrote one entry per group by {:?}", t0.elapsed());
    loop {
        let active: u64 = nodes.iter().map(|n| n.stats().active).sum();
        if active == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
        if t0.elapsed() > Duration::from_secs(600) {
            eprintln!("still {active} active groups after 10 minutes");
            break;
        }
    }
    eprintln!("all quiesced by {:?}", t0.elapsed());
    // Let allocator caches settle, then measure.
    std::thread::sleep(Duration::from_secs(5));
    let frames0: u64 = nodes.iter().map(|n| n.stats().frames_sent).sum();
    let (rss1, cpu1) = sample();
    std::thread::sleep(Duration::from_secs(idle_secs));
    let (rss2, cpu2) = sample();
    let frames1: u64 = nodes.iter().map(|n| n.stats().frames_sent).sum();
    let active: u64 = nodes.iter().map(|n| n.stats().active).sum();
    #[allow(clippy::cast_precision_loss)]
    let per_node_mb = (rss2.saturating_sub(rss0)) as f64 / 1024.0 / 3.0;
    let cpu_pct = (cpu2 - cpu1) / idle_secs as f64 * 100.0 / 3.0;
    #[allow(clippy::cast_precision_loss)]
    let fps = (frames1 - frames0) as f64 / idle_secs as f64 / 3.0;
    let log_mb: u64 = stores.iter().map(|s| s.disk_bytes()).sum::<u64>() / 3 / (1 << 20);
    println!(
        "groups_per_node={groups} idle_secs={idle_secs} rss_added_mb_per_node={per_node_mb:.1} \
         rss_mb_total={:.1} cpu_pct_of_core_per_node={cpu_pct:.3} frames_per_sec_per_node={fps:.1} \
         active_groups={active} log_disk_mb_per_node={log_mb} rss_before_idle_mb={:.1}",
        rss2 as f64 / 1024.0,
        rss1 as f64 / 1024.0,
    );
    for n in &nodes {
        n.close();
    }
    for t in threads {
        let _ = t.join();
    }
    drop(stores);
    let _ = std::fs::remove_dir_all(&dir);
}
