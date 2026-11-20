use std::time::Duration;

use super::*;
use crate::keys::{KeyPrefix, kind, memcmp};
use crate::log::{Durability, SimDisk, SyncMode};
use crate::testutil::TempDir;
use crate::txn::{ReadMode, TxnOptions};

fn cfg(dir: &Path, store: StoreKind) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.store = store;
    c.checkpoint_interval = Duration::ZERO;
    c.log.sync = SyncMode::Off;
    c.budget.engine_mb = 64;
    c
}

fn row_key(id: i32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant: 42,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    memcmp::put_i32(&mut k, id);
    k
}

fn put(e: &Engine, id: i32, v: &str, d: Durability) -> crate::txn::CommitTicket {
    let mut t = e
        .begin(TxnOptions {
            durability: Some(d),
            ..TxnOptions::default()
        })
        .unwrap();
    t.put(row_key(id), v.as_bytes().to_vec()).unwrap();
    t.commit().unwrap()
}

fn get(e: &Engine, id: i32) -> Option<Vec<u8>> {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(&row_key(id), ReadMode::Snapshot)
        .unwrap()
        .expect_ready()
        .unwrap()
        .map(|b| b.to_vec())
}

fn round_trip(store: StoreKind) {
    let dir = TempDir::new("engine");
    {
        let e = Engine::open(cfg(dir.path(), store)).unwrap();
        for i in 0..100 {
            put(&e, i, &format!("v{i}"), Durability::Local)
                .wait()
                .unwrap();
        }
        put(&e, 7, "seven", Durability::Local).wait().unwrap();
        assert_eq!(get(&e, 7).as_deref(), Some(&b"seven"[..]));
        assert_eq!(e.stats().applied_index, 101);
    }
    // Reopen: RocksDB flushed on close (or MemStore replays the whole log).
    let e = Engine::open(cfg(dir.path(), store)).unwrap();
    assert_eq!(get(&e, 7).as_deref(), Some(&b"seven"[..]));
    assert_eq!(get(&e, 99).as_deref(), Some(&b"v99"[..]));
    assert_eq!(get(&e, 100), None);
    let t = put(&e, 100, "next", Durability::Local);
    assert_eq!(t.log_ticket().unwrap().index(), 102);
    t.wait().unwrap();
}

#[test]
fn round_trip_rocks() {
    round_trip(StoreKind::Rocks);
}

#[test]
fn round_trip_mem() {
    round_trip(StoreKind::Mem);
}

/// In-process crash: the log loses its unsynced tail (`SimDisk`) and RocksDB
/// loses its memtables (WAL off, no flush on close). Every acknowledged
/// commit is back after recovery, and no commit is partial.
fn crash_round(store: StoreKind, seed: u64, checkpoint_at: Option<i32>) {
    let dir = TempDir::new("crash");
    let sim = SimDisk::new();
    let mut c = cfg(dir.path(), store);
    c.log.sim = Some(sim.clone());
    c.rocks.avoid_flush_on_close = true;
    let mut acked = Vec::new();
    {
        let e = Engine::open(c.clone()).unwrap();
        for i in 0..(40 + (seed % 7) as i32 * 10) {
            if Some(i) == checkpoint_at {
                e.checkpoint().unwrap();
            }
            let d = if i % 4 == 3 {
                Durability::LocalRelaxed
            } else {
                Durability::Local
            };
            // Two rows per commit: both or neither may survive.
            let mut t = e
                .begin(TxnOptions {
                    durability: Some(d),
                    ..TxnOptions::default()
                })
                .unwrap();
            t.put(row_key(i), format!("a{i}").into_bytes()).unwrap();
            t.put(row_key(10_000 + i), format!("b{i}").into_bytes())
                .unwrap();
            let tk = t.commit().unwrap();
            if d.needs_sync() {
                tk.log_ticket().unwrap().wait().unwrap();
                acked.push(i);
            }
        }
        e.log().halt();
    }
    sim.crash(&dir.path().join("raft"), seed).unwrap();
    c.log.sim = None;
    let e = Engine::open(c).unwrap();
    for &i in &acked {
        assert_eq!(
            get(&e, i),
            Some(format!("a{i}").into_bytes()),
            "seed {seed}: acknowledged commit {i} lost"
        );
    }
    for i in 0..120 {
        assert_eq!(
            get(&e, i).is_some(),
            get(&e, 10_000 + i).is_some(),
            "seed {seed}: partial commit {i}"
        );
    }
}

#[test]
fn crash_simulation_rocks() {
    for seed in 1..=25 {
        crash_round(
            StoreKind::Rocks,
            seed,
            if seed % 3 == 0 { Some(20) } else { None },
        );
    }
}

#[test]
fn crash_simulation_mem() {
    for seed in 1..=25 {
        crash_round(StoreKind::Mem, seed, None);
    }
}

#[test]
fn background_checkpointer_trims_the_log() {
    let dir = TempDir::new("bgckpt");
    let mut c = cfg(dir.path(), StoreKind::Rocks);
    c.checkpoint_interval = Duration::from_millis(50);
    c.checkpoint_entries = 10;
    let e = Engine::open(c).unwrap();
    for i in 0..50 {
        put(&e, i, "x", Durability::Local).wait().unwrap();
    }
    std::thread::sleep(Duration::from_millis(1500));
    assert!(e.stats().checkpoints >= 1);
}

/// The clock restarts above every version in the store, even one stamped
/// far in the future (a wall clock that stepped back).
#[test]
fn clock_restarts_above_stored_versions() {
    let dir = TempDir::new("maxts");
    let future = crate::keys::Ts::from_parts(4_000_000_000_000, 5);
    {
        let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
        let mut p = crate::log::Payload::new();
        p.put(
            Cf::Data,
            &crate::keys::versioned(&row_key(1), future),
            &[crate::keys::value::PUT, b'x'],
        );
        e.consensus()
            .propose(e.unit(), p.into_bytes(), Durability::Local)
            .unwrap()
            .wait_applied()
            .unwrap();
    }
    let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
    assert!(e.txn_shared().now() > future);
}

/// A rule for the test: keeps only the newest version of each logical key
/// at or below the watermark, and records the watermarks it was given.
struct KeepFloor(std::sync::Mutex<Vec<keys::Ts>>);

impl VersionGc for KeepFloor {
    fn start(&self, watermark: keys::Ts, _bottommost: bool) -> GcDecider {
        self.0.lock().unwrap().push(watermark);
        let mut current: Vec<u8> = Vec::new();
        let mut floor_seen = false;
        Box::new(move |k, _| {
            let Some((logical, ts)) = keys::split_version(k) else {
                return false;
            };
            if logical != current.as_slice() {
                current = logical.to_vec();
                floor_seen = false;
            }
            if ts > watermark {
                return false;
            }
            std::mem::replace(&mut floor_seen, true)
        })
    }
}

/// Compactions run the installed rule at the engine's watermark.
fn version_gc_runs_at_the_watermark(store: StoreKind) {
    let dir = TempDir::new("vgc");
    let e = Engine::open(cfg(dir.path(), store)).unwrap();
    for v in ["a", "b", "c"] {
        put(&e, 1, v, Durability::Local).wait().unwrap();
    }
    let versions = |e: &Engine| {
        let lo = row_key(1);
        let hi = keys::prefix_end(&lo).unwrap();
        let mut it = e.store().iter(
            Cf::Data,
            crate::store::IterOpts {
                lower: Some(lo),
                upper: Some(hi),
                ..Default::default()
            },
        );
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        n
    };
    let rule = Arc::new(KeepFloor(std::sync::Mutex::new(Vec::new())));
    e.set_version_gc(rule.clone());
    let w = e.txn_shared().now();
    e.set_gc_watermark(w);
    put(&e, 1, "d", Durability::Local).wait().unwrap();
    e.checkpoint().unwrap();
    e.store().compact_all().unwrap();
    // "d" is above the watermark, "c" is the floor; "a" and "b" go.
    assert_eq!(versions(&e), 2);
    assert_eq!(get(&e, 1).as_deref(), Some(&b"d"[..]));
    assert!(rule.0.lock().unwrap().contains(&w));
}

#[test]
fn version_gc_rocks() {
    version_gc_runs_at_the_watermark(StoreKind::Rocks);
}

#[test]
fn version_gc_mem() {
    version_gc_runs_at_the_watermark(StoreKind::Mem);
}
