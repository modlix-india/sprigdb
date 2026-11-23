//! Replicated engine units under faults (the engine's invariants with
//! three replicas per unit): consistent cuts within a unit, every
//! acknowledged commit surviving minority crashes, replicas converging.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::repl::{ReplCluster, ReplRunStats, ReplSimConfig};

fn suite(name: &str, cfg: ReplSimConfig, seeds: u64) {
    let totals = Arc::new(Mutex::new((0u64, ReplRunStats::default())));
    let plan_cfg = cfg.clone();
    let t = Arc::clone(&totals);
    Sim::new(name)
        .seeds(seeds)
        .max_time(Duration::from_secs(900))
        .run_with_faults(
            move |rng| ReplCluster::plan(rng, &plan_cfg),
            move |world, plan| {
                let cfg = cfg.clone();
                let t = Arc::clone(&t);
                async move {
                    let c = ReplCluster::start(&world, cfg);
                    let w = Arc::clone(&c);
                    tokio::join!(world.run_plan(&plan), w.run_workload());
                    let s = c.check().await?;
                    world.record(format!("stats {s:?}"));
                    let mut t = t.lock().unwrap();
                    t.0 += 1;
                    t.1.transfers += s.transfers;
                    t.1.rows += s.rows;
                    t.1.failed += s.failed;
                    t.1.cuts += s.cuts;
                    t.1.follower_cuts += s.follower_cuts;
                    Ok(())
                }
            },
        );
    let t = totals.lock().unwrap();
    eprintln!("sim {name}: {} runs, totals {:?}", t.0, t.1);
}

#[test]
fn replicated_engine_gates() {
    suite("repl_gates", ReplSimConfig::default(), 50);
}
