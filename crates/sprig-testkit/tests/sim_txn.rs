//! Commits across units under deterministic simulation: one engine node,
//! transactions over several tenants (each its own unit; many span two or
//! three), the log stepped by the run, crashes that lose unsynced writes,
//! and faults in the commit protocol: a coordinator that stops before its
//! commit point, right after it, or between participants, and
//! participants' resolutions lost on the way. The resolver runs at random
//! moments and once more when the run heals.
//!
//! Invariants, checked on every seed (in one read view, at random moments
//! and after every crash):
//!
//! 1. Atomicity: every transaction's rows are in all of its units or in
//!    none.
//! 2. The client's answer is true: an acknowledged commit is there; a
//!    commit the client was told was rolled back is not.
//! 3. Nothing stays prepared and no record is left once the resolver has
//!    run after the faults healed.
//!
//! Each runs with the protocol's steps queued at once (the single-node
//! log) and with waits between them (as on a replicated log). The first
//! seed runs twice and the traces must match. `SPRIG_SIM_SEEDS` sets how
//! many seeds (default 200).

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use sprig_testkit::sim::Sim;
use sprig_testkit::sim::engine::{EngineSimConfig, body, run};

fn cfg(wait: bool) -> EngineSimConfig {
    EngineSimConfig {
        commits: 200,
        tenants: 5,
        crashes: 3,
        max_units: 3,
        cross: 0.5,
        faults: 0.3,
        wait,
    }
}

#[test]
fn commits_across_units_queued_at_once() {
    Sim::new("txn_queued")
        .seeds(200)
        .max_time(Duration::from_secs(600))
        .check_determinism(true)
        .run(|world| body(world, cfg(false)));
}

#[test]
fn commits_across_units_with_waits() {
    Sim::new("txn_waits")
        .seeds(200)
        .max_time(Duration::from_secs(600))
        .check_determinism(true)
        .run(|world| body(world, cfg(true)));
}

fn trace_of(seed: u64, wait: bool) -> Vec<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    rt.block_on(async {
        let w = sprig_testkit::sim::World::new(seed);
        let stats = run(&w, &cfg(wait)).await.unwrap();
        assert!(stats.cross_unit > 0 && stats.faults > 0, "{stats:?}");
        w.trace().lines()
    })
}

/// The same seed gives the same trace (commits, faults, answers, crashes
/// and the bytes each lost), with and without waits.
#[test]
fn commits_across_units_are_deterministic() {
    for seed in 1..=15 {
        for wait in [false, true] {
            assert_eq!(trace_of(seed, wait), trace_of(seed, wait), "seed {seed}");
        }
    }
}
