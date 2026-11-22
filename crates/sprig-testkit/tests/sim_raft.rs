//! The Raft driver's simulation gates: N nodes running the real
//! `sprig-raft` driver under crashes (lost unsynced writes, torn tails),
//! partitions (both ways and one way), slow and lossy links, pauses, clock
//! rate skew, slow disks, membership changes and leader transfers.
//!
//! Checked per seed: at most one leader per term, no overlapping leases,
//! linearizable register histories, every acknowledged write kept, replicas
//! equal. `SPRIG_SIM_SEEDS` sets how many seeds (default 200).

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::raft::{RaftCluster, RaftSimConfig, RunStats};

fn add(t: &mut RunStats, s: &RunStats) {
    t.acked_writes += s.acked_writes;
    t.unknown_writes += s.unknown_writes;
    t.failed_writes += s.failed_writes;
    t.reads += s.reads;
    t.lease_reads += s.lease_reads;
    t.leaders += s.leaders;
    t.membership_changes += s.membership_changes;
}

fn suite(name: &str, cfg: RaftSimConfig) {
    let totals = Arc::new(Mutex::new((0u64, RunStats::default())));
    let plan_cfg = cfg.clone();
    let t = Arc::clone(&totals);
    Sim::new(name)
        .seeds(200)
        .max_time(Duration::from_secs(900))
        .run_with_faults(
            move |rng| RaftCluster::plan(rng, &plan_cfg),
            move |world, plan| {
                let cfg = cfg.clone();
                let t = Arc::clone(&t);
                async move {
                    let c = RaftCluster::start(&world, cfg);
                    let w = Arc::clone(&c);
                    tokio::join!(world.run_plan(&plan), w.run_workload());
                    let s = c.check().await?;
                    world.record(format!("stats {s:?}"));
                    let mut t = t.lock().unwrap();
                    t.0 += 1;
                    add(&mut t.1, &s);
                    Ok(())
                }
            },
        );
    let t = totals.lock().unwrap();
    eprintln!("sim {name}: {} runs, totals {:?}", t.0, t.1);
}

#[test]
fn raft_gates() {
    suite("raft_gates", RaftSimConfig::default());
}

/// Many groups, mostly idle: quiescence and store liveness under faults.
#[test]
fn raft_many_idle_groups() {
    suite(
        "raft_idle",
        RaftSimConfig {
            groups: 24,
            keys: 1,
            clients: 2,
            membership: false,
            ..RaftSimConfig::default()
        },
    );
}

/// The gates catch a broken driver: with a lease longer than the election
/// timeout, a deposed leader keeps answering reads under its old lease, and
/// some seed must fail (overlapping leases or a stale read).
#[test]
fn raft_gates_catch_an_unsafe_lease() {
    use sprig_testkit::sim::SimRng;
    let mut cfg = RaftSimConfig::default();
    cfg.raft.lease = Duration::from_secs(6);
    cfg.raft.lease_margin = Duration::ZERO;
    let sim = Sim::new("raft_unsafe_lease").check_determinism(false);
    let mut failed = Vec::new();
    for seed in 1..=40u64 {
        let plan = RaftCluster::plan(&SimRng::new(seed), &cfg);
        let c2 = cfg.clone();
        let out = sim.run_once(seed, &plan, &move |world, plan| {
            let cfg = c2.clone();
            async move {
                let c = RaftCluster::start(&world, cfg);
                let w = Arc::clone(&c);
                tokio::join!(world.run_plan(&plan), w.run_workload());
                c.check().await.map(drop)
            }
        });
        if let Err(e) = out.result {
            failed.push((seed, e));
            if failed.len() >= 3 {
                break;
            }
        }
    }
    eprintln!("unsafe lease: {} failing seed(s): {failed:?}", failed.len());
    assert!(!failed.is_empty(), "no seed caught an unsafe lease");
}
