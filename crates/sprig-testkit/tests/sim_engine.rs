//! One storage-engine node in the simulator: transactions over several
//! units (some across two), the log stepped by the run, crashes that lose
//! unsynced log writes. Checked per seed: acknowledged commits kept, no
//! commit partly there, and two runs of the first seed alike.
//! `SPRIG_SIM_SEEDS` sets how many seeds (default 200).

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::engine::{EngineSimConfig, body};

#[test]
fn engine_node_gates() {
    Sim::new("engine_node")
        .seeds(200)
        .max_time(Duration::from_secs(600))
        .check_determinism(true)
        .run(|world| body(world, EngineSimConfig::default()));
}

fn trace_of(seed: u64) -> Vec<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let w = sprig_testkit::sim::World::new(seed);
        sprig_testkit::sim::engine::run(&w, &EngineSimConfig::default())
            .await
            .unwrap();
        w.trace().lines()
    })
}

/// The determinism check, twice per seed: the same seed gives the same
/// trace (commits, durability classes, crashes and the bytes each lost).
#[test]
fn engine_node_is_deterministic() {
    for seed in 1..=30 {
        assert_eq!(trace_of(seed), trace_of(seed), "seed {seed}");
    }
}
