//! One storage-engine node inside the simulator: the real engine
//! (transactions, the commit log on raft-engine, the in-memory store), with
//! no threads of its own. The log writer is stepped by the run, the clock is
//! the world's node clock, and the log's files go through `SimDisk`, so a
//! crash keeps every synced write and a seed-chosen part of the rest.
//!
//! The workload commits transactions over several tenants (each its own
//! unit; some commits span two), steps and syncs the log at random, and
//! crashes and reopens the node at random. Checked after every reopen:
//! every acknowledged commit is there, no commit is partly there (inside a
//! unit or across two), and the clock starts above every stored version.
//!
//! Commits across units go through the commit protocol (prepare records,
//! the anchor's transaction record, resolution). With faults on, a seed
//! stops some coordinators part way (before or after the commit point, or
//! between participants) and loses some participants' resolutions; the
//! resolver runs at random moments. With `wait`, the protocol waits
//! between its steps as on a replicated log. Also checked, in one read view
//! at random moments: no commit is partly visible, a commit the client was
//! told failed is not there, and once everything is resolved no record is
//! left.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::{Durability, SimDisk, SyncMode};
use sprig_engine::txn::hlc::CommonClock;
use sprig_engine::txn::{CrossFault, CrossStage, ReadMode, TxnOptions};
use sprig_engine::{Engine, EngineClock, EngineConfig, StoreKind};

use super::{SimResult, World};
use crate::faults::Node;

/// The workload's size.
#[derive(Debug, Clone)]
pub struct EngineSimConfig {
    /// Transactions per run.
    pub commits: u32,
    /// Tenants (units) the transactions spread over.
    pub tenants: u64,
    /// Crashes per run, at random points.
    pub crashes: u32,
    /// Units a commit across units spans at most (2 or more).
    pub max_units: u64,
    /// Chance that a commit spans several units.
    pub cross: f64,
    /// Chance that a commit across units meets a fault (its coordinator
    /// stops part way, or a participant's resolution is lost).
    pub faults: f64,
    /// Wait between the protocol's steps, as on a replicated log.
    pub wait: bool,
}

impl Default for EngineSimConfig {
    fn default() -> Self {
        Self {
            commits: 300,
            tenants: 4,
            crashes: 3,
            max_units: 2,
            cross: 0.3,
            faults: 0.0,
            wait: false,
        }
    }
}

/// What one run did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineRunStats {
    /// Commits acknowledged (durable before the next crash).
    pub acked: u64,
    /// Commits that spanned two units.
    pub cross_unit: u64,
    /// Crashes.
    pub crashes: u64,
    /// Commits a crash lost (never acknowledged).
    pub lost: u64,
    /// Commits across units that met a fault.
    pub faults: u64,
    /// Commits refused (rolled back) without a fault.
    pub refused: u64,
}

fn key(tenant: u64, id: u32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// One commit of the workload: rows it wrote, all with one value.
#[derive(Debug, Clone)]
struct Commit {
    rows: Vec<(u64, u32)>,
    value: u32,
    /// The client was told it failed (rolled back): it must be absent.
    refused: bool,
}

fn open(world: &World, dir: &PathBuf, sim: Option<Arc<SimDisk>>) -> Engine {
    let mut c = EngineConfig::new(dir);
    c.store = StoreKind::Mem;
    c.checkpoint_interval = std::time::Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.log.manual = true;
    c.log.sim = sim;
    c.budget.engine_mb = 32;
    let clock = world.clock(&Node::new("engine"));
    c.clock = Some(EngineClock(Arc::new(CommonClock(clock))));
    #[allow(clippy::expect_used)]
    let e = Engine::open(c).expect("engine opens in the simulator");
    e
}

fn read(t: &mut sprig_engine::txn::Txn, tenant: u64, id: u32) -> Option<Vec<u8>> {
    t.get(&key(tenant, id), ReadMode::Snapshot)
        .ok()?
        .expect_ready()
        .ok()?
        .map(|b| b.to_vec())
}

/// Runs the workload on one simulated engine node; `Err` on a broken
/// invariant. Every step goes into the world's trace, so two runs of a
/// seed can be compared.
///
/// # Errors
/// An acknowledged commit was lost, a commit is partly there, or the
/// engine failed.
// Async like every simulation body, though the stepped engine never awaits.
#[allow(clippy::unused_async, clippy::too_many_lines)]
pub async fn run(world: &World, cfg: &EngineSimConfig) -> Result<EngineRunStats, String> {
    let rng = world.rng();
    let dir = std::env::temp_dir().join(format!(
        "sprig-sim-engine-{}-{}",
        std::process::id(),
        world.seed()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let raft = dir.join("raft");
    let mut stats = EngineRunStats::default();
    let sim = SimDisk::new();
    let mut e = open(world, &dir, Some(sim.clone()));
    // Every commit made, and which were acknowledged.
    let mut commits: Vec<Commit> = Vec::new();
    // Commits not acknowledged yet: a strict one once its ticket is durable
    // (synced); a relaxed one only once a sync ran after it was written
    // (its ticket counts as durable at the OS write, which a crash loses).
    let mut pending: Vec<(usize, sprig_engine::txn::CommitTicket, bool)> = Vec::new();
    let mut acked: BTreeSet<usize> = BTreeSet::new();
    let crash_at: Vec<u32> = (0..cfg.crashes)
        .map(|_| u32::try_from(rng.below(u64::from(cfg.commits))).unwrap_or(0))
        .collect();
    for i in 0..cfg.commits {
        e.txn_shared().set_cross_unit_wait(cfg.wait);
        // A transaction over one unit, or several (the first tenant has
        // the most rows, so it is the anchor).
        let t1 = rng.below(cfg.tenants) + 1;
        let mut rows = vec![(t1, i), (t1, 100_000 + i)];
        if rng.chance(cfg.cross) {
            let extra = 1 + rng.below(cfg.max_units.max(2) - 1);
            for j in 0..extra {
                let t = rng.below(cfg.tenants) + 1;
                if t != t1 && !rows.iter().any(|(r, _)| *r == t) {
                    rows.push((t, 200_000 + 1_000_000 * u32::try_from(j).unwrap_or(0) + i));
                }
            }
        }
        let others: Vec<u64> = rows.iter().map(|(t, _)| *t).filter(|t| *t != t1).collect();
        let mut fault = None;
        if !others.is_empty() {
            stats.cross_unit += 1;
            if rng.chance(cfg.faults) {
                let stage = match rng.below(3) {
                    0 => CrossStage::Prepared,
                    1 => CrossStage::CommitPoint,
                    _ => CrossStage::Resolve(
                        others[usize::try_from(rng.below(others.len() as u64)).unwrap_or(0)],
                    ),
                };
                let what = if matches!(stage, CrossStage::Resolve(_)) && rng.chance(0.5) {
                    CrossFault::Drop
                } else {
                    CrossFault::CoordinatorLost
                };
                fault = Some((stage, what));
                stats.faults += 1;
            }
        }
        e.txn_shared()
            .set_cross_unit_faults(fault.map(|(stage, what)| {
                let f: sprig_engine::txn::CrossFaults =
                    Arc::new(move |s| if s == stage { what } else { CrossFault::Go });
                f
            }));
        let durability = if rng.chance(0.5) {
            Durability::Local
        } else {
            Durability::LocalRelaxed
        };
        let mut t = e
            .begin(TxnOptions {
                durability: Some(durability),
                ..TxnOptions::default()
            })
            .map_err(|x| x.to_string())?;
        for (tn, id) in &rows {
            t.put(key(*tn, *id), i.to_be_bytes().to_vec())
                .map_err(|x| x.to_string())?;
        }
        let done = t.commit();
        e.txn_shared().set_cross_unit_faults(None);
        world.record(format!(
            "commit {i} rows {rows:?} {durability:?} fault {fault:?} ok {}",
            done.is_ok()
        ));
        match done {
            Ok(tk) => {
                commits.push(Commit {
                    rows,
                    value: i,
                    refused: false,
                });
                pending.push((commits.len() - 1, tk, durability.needs_sync()));
            }
            // The coordinator stopped part way: the outcome is unknown.
            Err(_) if fault.is_some_and(|(_, w)| w == CrossFault::CoordinatorLost) => {
                commits.push(Commit {
                    rows,
                    value: i,
                    refused: false,
                });
            }
            Err(x) => {
                stats.refused += 1;
                world.record(format!("refused {i}: {x}"));
                commits.push(Commit {
                    rows,
                    value: i,
                    refused: true,
                });
            }
        }
        // The resolver runs now and then (a resolution lost on the way
        // waits for it).
        if rng.chance(0.3) {
            e.resolve_cross_unit();
        }
        // The log writer runs when the run says so.
        for _ in 0..rng.below(3) {
            e.log().step();
        }
        let synced = rng.chance(0.1);
        if synced {
            e.log().sync_now();
        }
        pending.retain(|(c, tk, strict)| {
            if tk.is_durable() && (*strict || synced) {
                acked.insert(*c);
                false
            } else {
                true
            }
        });
        if rng.chance(0.05) {
            check(&e, &commits, &acked)?;
        }
        if crash_at.contains(&i) {
            stats.crashes += 1;
            e.log().halt();
            drop(e);
            let lost = sim
                .crash(&raft, rng.next_u64())
                .map_err(|x| x.to_string())?;
            world.record(format!("crash after {i}: {lost} bytes lost"));
            stats.lost += pending.len() as u64;
            pending.clear();
            e = open(world, &dir, Some(sim.clone()));
            check(&e, &commits, &acked)?;
        }
    }
    // Heal: the resolver finishes everything left open.
    e.txn_shared().set_cross_unit_stale_ms(0);
    for _ in 0..8 {
        while e.log().step() {}
        e.log().sync_now();
        e.resolve_cross_unit();
    }
    for (c, tk, _) in pending.drain(..) {
        if tk.is_durable() {
            acked.insert(c);
        }
    }
    check(&e, &commits, &acked)?;
    if e.txn_shared().cross_unit_open() != 0 {
        return Err(format!(
            "{} commits across units are still open after healing",
            e.txn_shared().cross_unit_open()
        ));
    }
    stats.acked = acked.len() as u64;
    world.record(format!("done {stats:?}"));
    drop(e);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(stats)
}

/// In one read view: every acknowledged commit is there; every commit is
/// whole or absent (later commits never touch the same rows, so a row
/// holds its commit's value or nothing); a refused commit is absent.
fn check(e: &Engine, commits: &[Commit], acked: &BTreeSet<usize>) -> Result<(), String> {
    let mut t = e.begin(TxnOptions::default()).map_err(|x| x.to_string())?;
    for (n, c) in commits.iter().enumerate() {
        let got: Vec<Option<Vec<u8>>> = c
            .rows
            .iter()
            .map(|(tn, id)| read(&mut t, *tn, *id))
            .collect();
        let present = got.iter().filter(|g| g.is_some()).count();
        if present != 0 && present != got.len() {
            return Err(format!(
                "commit {n} is partly there: {:?} of {:?}",
                got, c.rows
            ));
        }
        if acked.contains(&n) && present == 0 {
            return Err(format!("acknowledged commit {n} was lost"));
        }
        if c.refused && present != 0 {
            return Err(format!("commit {n} was refused but is there"));
        }
        if let Some(Some(v)) = got.first()
            && v.as_slice() != c.value.to_be_bytes()
        {
            return Err(format!("commit {n} reads a wrong value"));
        }
    }
    Ok(())
}

/// [`run`] as a simulation body.
///
/// # Errors
/// As for [`run`].
pub async fn body(world: World, cfg: EngineSimConfig) -> SimResult {
    run(&world, &cfg).await.map(drop).map_err(Into::into)
}
