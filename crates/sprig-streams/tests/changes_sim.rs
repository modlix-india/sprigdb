//! Seeded crash runs of the change log over a stepped engine whose commit
//! log goes through `SimDisk` (a crash keeps every synced write of the log
//! and a seed-chosen part of the rest; the store is in memory, so the log is
//! all there is).
//!
//! Each run commits transactions over three tenants (some across two
//! units, some relaxed), steps and syncs the log, flushes the change log
//! and crashes at random. After every reopen, per tenant: the change log
//! holds exactly the commits the store holds (each whole, once, nothing
//! else) with strictly increasing places, and the resolved timestamp never
//! went back.
//!
//! `SPRIG_CHG_SIM_SEEDS` (default 64) seeds of `SPRIG_CHG_SIM_COMMITS`
//! (default 150) commits.

#![allow(
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::{Durability, SimDisk, SyncMode};
use sprig_engine::txn::{ReadMode, TxnOptions};
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_streams::changes::{ChangeConfig, ChangeLog, Pos};
use sprig_streams::seg::testing::TempDir;

const TENANTS: u64 = 3;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 10_000) as f64 / 10_000.0 < p
    }
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

fn open(dir: &Path, sim: &Arc<SimDisk>) -> (Engine, ChangeLog) {
    let mut c = EngineConfig::new(dir.join("engine"));
    c.store = StoreKind::Mem;
    c.checkpoint_interval = std::time::Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.log.manual = true;
    c.log.sim = Some(Arc::clone(sim));
    c.budget.engine_mb = 32;
    let e = Engine::open(c).unwrap();
    let l = ChangeLog::open(
        &e,
        dir.join("changes"),
        ChangeConfig {
            segment_bytes: 4096,
            index_interval: 512,
            ..ChangeConfig::default()
        },
    )
    .unwrap();
    (e, l)
}

/// Per tenant, the commit numbers whose rows the store holds.
fn stored(e: &Engine, commits: &[(u32, Vec<u64>)]) -> BTreeMap<u64, BTreeSet<u32>> {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    let mut out: BTreeMap<u64, BTreeSet<u32>> = BTreeMap::new();
    for (i, tenants) in commits {
        for tn in tenants {
            let v = t
                .get(&key(*tn, *i), ReadMode::Snapshot)
                .unwrap()
                .expect_ready()
                .unwrap();
            if v.is_some() {
                out.entry(*tn).or_default().insert(*i);
            }
        }
    }
    t.rollback();
    out
}

fn check(
    seed: u64,
    e: &Engine,
    l: &ChangeLog,
    commits: &[(u32, Vec<u64>)],
    resolved: &mut BTreeMap<u64, u64>,
) {
    let have = stored(e, commits);
    for tn in 1..=TENANTS {
        let unit = Engine::unit_of_tenant(tn).0;
        let mut r = l.reader(unit, Pos::default(), None).unwrap();
        let mut seen: Vec<(Pos, u32, u64)> = Vec::new();
        loop {
            let p = r.read(1000).unwrap();
            for c in p.changes {
                let id = u32::from_be_bytes(c.key[c.key.len() - 4..].try_into().unwrap());
                seen.push((c.pos(), id, c.index));
            }
            if p.at_end {
                break;
            }
        }
        for w in seen.windows(2) {
            assert!(
                w[0].0 < w[1].0,
                "seed {seed} tenant {tn}: places out of order"
            );
        }
        // Each commit's two rows of this tenant: id i and 100_000 + i, in
        // one entry.
        let mut by_commit: BTreeMap<u32, Vec<(u32, u64)>> = BTreeMap::new();
        for (_, id, index) in &seen {
            by_commit
                .entry(id % 100_000)
                .or_default()
                .push((*id, *index));
        }
        let got: BTreeSet<u32> = by_commit.keys().copied().collect();
        let want = have.get(&tn).cloned().unwrap_or_default();
        assert_eq!(
            got, want,
            "seed {seed} tenant {tn}: change log and store differ"
        );
        for (i, rows) in &by_commit {
            assert_eq!(
                rows.len(),
                2,
                "seed {seed} tenant {tn}: commit {i} not whole or repeated: {rows:?}"
            );
            assert_eq!(
                rows[0].1, rows[1].1,
                "seed {seed}: commit {i} split over entries"
            );
        }
        let rt = l.resolved(unit);
        let before = resolved.insert(tn, rt).unwrap_or(0);
        assert!(
            rt >= before,
            "seed {seed} tenant {tn}: resolved went back {before} -> {rt}"
        );
    }
}

#[test]
fn change_log_equals_committed_history_across_crashes() {
    let seeds: u64 = std::env::var("SPRIG_CHG_SIM_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let n: u32 = std::env::var("SPRIG_CHG_SIM_COMMITS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    let mut crashes = 0u64;
    for seed in 1..=seeds {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let d = TempDir::new("chg-sim");
        let sim = SimDisk::new();
        let raft = d.path().join("engine").join("raft");
        let (mut e, mut l) = open(d.path(), &sim);
        let mut commits: Vec<(u32, Vec<u64>)> = Vec::new();
        let mut resolved = BTreeMap::new();
        for i in 0..n {
            let t1 = rng.below(TENANTS) + 1;
            let mut tenants = vec![t1];
            if rng.chance(0.2) {
                let t2 = rng.below(TENANTS) + 1;
                if t2 != t1 {
                    tenants.push(t2);
                }
            }
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
                .unwrap();
            for tn in &tenants {
                t.put(key(*tn, i), i.to_be_bytes().to_vec()).unwrap();
                t.put(key(*tn, 100_000 + i), i.to_be_bytes().to_vec())
                    .unwrap();
            }
            if t.commit().is_ok() {
                commits.push((i, tenants));
            }
            for _ in 0..rng.below(3) {
                e.log().step();
            }
            if rng.chance(0.1) {
                e.log().sync_now();
            }
            if rng.chance(0.15) {
                l.flush().unwrap();
            }
            if rng.chance(0.05) {
                e.resolve_cross_unit();
            }
            if rng.chance(0.03) {
                crashes += 1;
                e.log().halt();
                drop(l);
                drop(e);
                sim.crash(&raft, rng.next()).unwrap();
                (e, l) = open(d.path(), &sim);
                for _ in 0..4 {
                    while e.log().step() {}
                    e.resolve_cross_unit();
                }
                check(seed, &e, &l, &commits, &mut resolved);
            }
        }
        for _ in 0..4 {
            while e.log().step() {}
            e.log().sync_now();
            e.resolve_cross_unit();
        }
        check(seed, &e, &l, &commits, &mut resolved);
    }
    eprintln!(
        "{seeds} seeds of {n} commits, {crashes} crashes: change log equal to the store's history"
    );
}
