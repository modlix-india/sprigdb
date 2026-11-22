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
    row_key_in(42, id)
}

fn row_key_in(tenant: u64, id: i32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
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
    get_in(e, 42, id)
}

fn get_in(e: &Engine, tenant: u64, id: i32) -> Option<Vec<u8>> {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(&row_key_in(tenant, id), ReadMode::Snapshot)
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
        // `wait` returns when the commit is durable; its apply to the store
        // follows on the apply thread, so give that a moment.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while e.stats().applied_index < 101 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
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
fn crash_round(store: StoreKind, seed: u64, checkpoint_at: Option<i32>) -> u64 {
    crash_round_with(store, seed, checkpoint_at.as_slice(), 0, None, 42)
}

/// One crash round. Each value is padded with `pad` bytes; `log_file` sets
/// the log's file size (with compression off, so the padding fills files).
fn crash_round_with(
    store: StoreKind,
    seed: u64,
    checkpoints: &[i32],
    pad: usize,
    log_file: Option<u64>,
    b_tenant: u64,
) -> u64 {
    let dir = TempDir::new("crash");
    let sim = SimDisk::new();
    let mut c = cfg(dir.path(), store);
    c.log.sim = Some(sim.clone());
    c.rocks.avoid_flush_on_close = true;
    if let Some(size) = log_file {
        c.log.target_file_size = size;
        c.log.compress_min_bytes = 0;
    }
    let value = |side: char, i: i32| format!("{side}{i}{}", "p".repeat(pad)).into_bytes();
    let mut acked = Vec::new();
    {
        let e = Engine::open(c.clone()).unwrap();
        for i in 0..(40 + (seed % 7) as i32 * 10) {
            if checkpoints.contains(&i) {
                e.checkpoint().unwrap();
                if let Some(size) = log_file {
                    // What is left: the file being written, at most one
                    // before it, and the file of moved entries.
                    let on_disk = e.stats().log_disk_bytes;
                    assert!(on_disk <= 3 * size, "seed {seed}, commit {i}: {on_disk}");
                }
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
            t.put(row_key(i), value('a', i)).unwrap();
            t.put(row_key_in(b_tenant, 10_000 + i), value('b', i))
                .unwrap();
            let tk = t.commit().unwrap();
            if d.needs_sync() {
                tk.wait().unwrap();
                acked.push(i);
            }
        }
        e.log().halt();
    }
    let lost = sim.crash(&dir.path().join("raft"), seed).unwrap();
    c.log.sim = None;
    let e = Engine::open(c).unwrap();
    for &i in &acked {
        assert_eq!(
            get(&e, i),
            Some(value('a', i)),
            "seed {seed}: acknowledged commit {i} lost"
        );
    }
    for i in 0..120 {
        assert_eq!(
            get(&e, i).is_some(),
            get_in(&e, b_tenant, 10_000 + i).is_some(),
            "seed {seed}: partial commit {i}"
        );
    }
    lost
}

#[test]
fn crash_simulation_rocks() {
    let mut lost = 0;
    for seed in 1..=25 {
        lost += crash_round(
            StoreKind::Rocks,
            seed,
            if seed % 3 == 0 { Some(20) } else { None },
        );
    }
    assert!(lost > 0, "the crash simulation lost nothing");
}

/// The same with a log of small files that checkpoints delete as the
/// round goes: a crash after files were deleted (and their live entries
/// moved) loses no acknowledged commit either.
#[test]
fn crash_simulation_after_log_files_were_deleted() {
    let mut lost = 0;
    for seed in 1..=12 {
        lost += crash_round_with(
            StoreKind::Rocks,
            seed,
            &[12, 13, 25, 33, 38],
            48 << 10,
            Some(1 << 20),
            42,
        );
    }
    assert!(lost > 0, "the crash simulation lost nothing");
}

/// Each commit writes one row of tenant 42 and one of tenant 43: two
/// units, one atomic log write. After a crash both rows are there or
/// neither is, and every acknowledged pair is there.
#[test]
fn crash_simulation_across_units() {
    let mut lost = 0;
    for seed in 1..=25 {
        lost += crash_round_with(
            if seed % 2 == 0 {
                StoreKind::Rocks
            } else {
                StoreKind::Mem
            },
            seed,
            if seed % 3 == 0 { &[20] } else { &[] },
            0,
            None,
            43,
        );
    }
    assert!(lost > 0, "the crash simulation lost nothing");
}

/// Small files and checkpoints that delete them, across units.
#[test]
fn crash_simulation_across_units_after_log_files_were_deleted() {
    for seed in 1..=6 {
        crash_round_with(
            StoreKind::Rocks,
            seed,
            &[12, 13, 25, 33, 38],
            48 << 10,
            Some(1 << 20),
            43,
        );
    }
}

/// Every tenant is its own unit with its own log and applied index; a
/// transaction over two tenants commits on both at one timestamp.
#[test]
fn each_tenant_is_its_own_unit() {
    let dir = TempDir::new("units");
    let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
    assert_eq!(Engine::unit_of_tenant(42), UnitId(42));
    assert_eq!(Engine::unit_of_tenant(keys::META_TENANT), NODE_UNIT);
    assert_eq!(e.unit_of_key(&row_key_in(7, 1)), UnitId(7));
    assert_eq!(e.unit_of_key(&keys::unit_meta_key(9, b"x")), UnitId(9));
    assert_eq!(e.unit_of_key(b"short"), NODE_UNIT);
    for i in 0..5 {
        put(&e, i, "a", Durability::Local).wait().unwrap();
    }
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(row_key_in(43, 1), b"b".to_vec()).unwrap();
    t.commit().unwrap().wait().unwrap();
    let c = e.consensus();
    assert_eq!(c.durable_index(UnitId(42)), 5);
    assert_eq!(c.durable_index(UnitId(43)), 1);
    // One transaction, two units.
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(row_key_in(42, 100), b"x".to_vec()).unwrap();
    t.put(row_key_in(43, 100), b"y".to_vec()).unwrap();
    let tk = t.commit().unwrap();
    assert!(matches!(
        tk.durable_ticket(),
        crate::txn::DurableTicket::Many(p) if p.len() == 2
    ));
    tk.wait().unwrap();
    assert_eq!(c.durable_index(UnitId(42)), 6);
    assert_eq!(c.durable_index(UnitId(43)), 2);
    assert_eq!(get_in(&e, 42, 100).as_deref(), Some(&b"x"[..]));
    assert_eq!(get_in(&e, 43, 100).as_deref(), Some(&b"y"[..]));
    // Both versions carry the one commit timestamp.
    let ts = |tenant| {
        let lo = row_key_in(tenant, 100);
        let mut it = e.store().iter(
            Cf::Data,
            crate::store::IterOpts::range(&keys::KeyRange::prefix(&lo)),
        );
        it.seek(&lo);
        keys::split_version(it.key()).unwrap().1
    };
    assert_eq!(ts(42), ts(43));
    drop(e);
    let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
    assert_eq!(get_in(&e, 43, 100).as_deref(), Some(&b"y"[..]));
    let units: Vec<UnitId> = e.units().into_iter().map(|(u, _)| u).collect();
    assert!(units.contains(&UnitId(42)) && units.contains(&UnitId(43)));
    // New commits continue each unit's own sequence.
    let t = put(&e, 200, "z", Durability::Local);
    assert_eq!(t.log_ticket().unwrap().index(), 7);
}

/// A read view sees every commit below it on every unit, even when
/// commits on different units publish out of timestamp order.
#[test]
fn read_views_are_consistent_across_units() {
    let dir = TempDir::new("views");
    let e = Engine::open(cfg(dir.path(), StoreKind::Mem)).unwrap();
    std::thread::scope(|s| {
        for w in 0..4u64 {
            let e = e.clone();
            s.spawn(move || {
                for i in 0..200 {
                    // Writer w moves a counter from tenant 50+w to tenant 60+w.
                    let mut t = e.begin(TxnOptions::default()).unwrap();
                    t.put(row_key_in(50 + w, 0), i.to_string().into_bytes())
                        .unwrap();
                    t.put(row_key_in(60 + w, 0), i.to_string().into_bytes())
                        .unwrap();
                    t.commit().unwrap();
                }
            });
        }
        for _ in 0..2 {
            let e = e.clone();
            s.spawn(move || {
                for _ in 0..400 {
                    let mut t = e.begin(TxnOptions::default()).unwrap();
                    for w in 0..4u64 {
                        let a = t.get_snapshot(&row_key_in(50 + w, 0)).unwrap();
                        let b = t.get_snapshot(&row_key_in(60 + w, 0)).unwrap();
                        assert_eq!(a, b, "a read view saw half of a commit across units");
                    }
                }
            });
        }
    });
}

/// A data directory from before units (one unit, numbered 1, holding
/// every tenant) opens: the node unit continues unit 1's index sequence
/// and takes its observer floors.
#[test]
fn a_one_unit_data_directory_moves_to_units() {
    let dir = TempDir::new("legacy");
    let c = cfg(dir.path(), StoreKind::Rocks);
    {
        let e = Engine::open(c.clone()).unwrap();
        // What the old engine wrote: every commit on unit 1.
        for i in 0..9 {
            let mut p = log::Payload::with_capacity(32);
            p.put(
                Cf::Data,
                &keys::versioned(&row_key_in(5, i), keys::Ts(1 << 20)),
                b"\x01v",
            );
            e.consensus()
                .propose(LEGACY_UNIT, p.into_bytes(), Durability::Local)
                .unwrap()
                .wait_applied()
                .unwrap();
        }
        let mut b = StoreBatch::new();
        // A queue message where the old engine kept it.
        let mut old = KeyPrefix {
            tenant: keys::META_TENANT,
            shard: 0,
            object: keys::QUEUES_OBJECT,
            kind: kind::ELEMENTS,
            index: 0,
        }
        .to_vec();
        old.extend_from_slice(&5u64.to_be_bytes());
        old.extend_from_slice(&3u32.to_be_bytes());
        old.extend_from_slice(b"msg");
        b.put(Cf::Data, old, b"body".to_vec());
        b.put(
            Cf::Local,
            floor_key(LEGACY_UNIT, "streams"),
            7u64.to_be_bytes(),
        );
        b.delete(Cf::Data, keys::unit_meta_key(NODE_UNIT.0, LAYOUT));
        e.store().write(&b, WalMode::Sync).unwrap();
    }
    let e = Engine::open(c.clone()).unwrap();
    assert_eq!(e.log_floor("streams"), Some(7));
    assert_eq!(e.last_applied(), 9);
    let mut moved = KeyPrefix {
        tenant: 5,
        shard: 0,
        object: keys::QUEUES_OBJECT,
        kind: kind::ELEMENTS,
        index: 0,
    }
    .to_vec();
    moved.extend_from_slice(&3u32.to_be_bytes());
    moved.extend_from_slice(b"msg");
    let got = e
        .store()
        .get(Cf::Data, &moved, ReadTier::All)
        .unwrap()
        .expect_ready()
        .unwrap();
    assert_eq!(got.as_deref(), Some(&b"body"[..]));
    assert_eq!(e.unit_of_key(&moved), UnitId(5));
    assert!(keys::is_unversioned(&moved));
    let all = e.store().iter(
        Cf::Data,
        crate::store::IterOpts::range(&keys::KeyRange::prefix(&keys::META_TENANT.to_be_bytes())),
    );
    let mut all = all;
    all.seek_to_first();
    while all.valid() {
        assert!(
            u32::from_be_bytes(all.key()[12..16].try_into().unwrap()) != keys::QUEUES_OBJECT,
            "a queue message stayed under the reserved tenant"
        );
        all.next();
    }
    drop(all);
    let t = e
        .consensus()
        .propose(
            NODE_UNIT,
            log::Payload::with_capacity(0).into_bytes(),
            Durability::Local,
        )
        .unwrap();
    assert_eq!(t.index(), 10);
    t.wait_applied().unwrap();
    drop(e);
    // Once moved, never again.
    let e = Engine::open(c).unwrap();
    assert_eq!(e.last_applied(), 10);
    assert_eq!(e.log_floor("streams"), Some(7));
}

#[test]
fn crash_simulation_mem() {
    let mut lost = 0;
    for seed in 1..=25 {
        lost += crash_round(StoreKind::Mem, seed, None);
    }
    assert!(lost > 0, "the crash simulation lost nothing");
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

/// A steady write load checkpoints by the bytes it wrote, long before the
/// interval or the entry count, so the log on disk stays bounded; nothing
/// is lost across a restart.
#[test]
fn checkpointer_bounds_the_log_by_bytes_written() {
    let dir = TempDir::new("ckptbytes");
    let config = || {
        let mut c = cfg(dir.path(), StoreKind::Rocks);
        c.checkpoint_interval = Duration::from_secs(3600);
        c.checkpoint_entries = u64::MAX;
        c.checkpoint_log_bytes = 2 << 20;
        c.log.target_file_size = 1 << 20;
        c.log.compress_min_bytes = 0;
        c
    };
    let value = "v".repeat(16 << 10);
    {
        let e = Engine::open(config()).unwrap();
        // 16 MiB of payload, in steps the checkpointer's one-second tick
        // can follow.
        for i in 0..1000 {
            put(&e, i, &value, Durability::Local).wait().unwrap();
            if i % 125 == 124 {
                std::thread::sleep(Duration::from_millis(1100));
            }
        }
        let s = e.stats();
        assert!(s.checkpoints >= 4, "{} checkpoints", s.checkpoints);
        assert!(
            s.log_disk_bytes <= 8 << 20,
            "{} bytes of log on disk after 16 MiB of writes",
            s.log_disk_bytes
        );
    }
    let e = Engine::open(config()).unwrap();
    for i in (0..1000).step_by(37) {
        assert_eq!(get(&e, i).as_deref(), Some(value.as_bytes()));
    }
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
    // Applied, not only durable: the compaction must see "d" in the store.
    put(&e, 1, "d", Durability::Local)
        .log_ticket()
        .unwrap()
        .wait_applied()
        .unwrap();
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

/// The in-memory store never trims the log: after a restart, every commit
/// comes back from the log alone.
#[test]
fn mem_store_checkpoint_keeps_the_log() {
    let dir = TempDir::new("memckpt");
    {
        let e = Engine::open(cfg(dir.path(), StoreKind::Mem)).unwrap();
        for i in 0..20 {
            put(&e, i, "x", Durability::Local).wait().unwrap();
        }
        assert_eq!(e.checkpoint().unwrap(), 0);
    }
    let e = Engine::open(cfg(dir.path(), StoreKind::Mem)).unwrap();
    for i in 0..20 {
        assert_eq!(get(&e, i).as_deref(), Some(&b"x"[..]), "commit {i} lost");
    }
}

/// Collects the notes it is shown, with their indexes.
#[derive(Default)]
struct Notes(std::sync::Mutex<Vec<(u64, Vec<u8>)>>);

impl crate::observe::ApplyObserver for Notes {
    fn applied(&self, _: UnitId, entries: &[LogEntry]) {
        let mut g = self.0.lock().unwrap();
        for e in entries {
            for n in log::payload::notes(&e.data).unwrap() {
                g.push((e.index, n.to_vec()));
            }
        }
    }
}

fn note(e: &Engine, text: &str) -> u64 {
    let mut p = log::Payload::new();
    p.note(text.as_bytes());
    let t = e
        .consensus()
        .propose(e.unit(), p.into_bytes(), Durability::Local)
        .unwrap();
    t.wait_applied().unwrap();
    t.index()
}

#[test]
fn observers_catch_up_and_floors_keep_the_log() {
    let dir = TempDir::new("observe");
    let floor;
    {
        let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
        let first = note(&e, "a");
        put(&e, 1, "x", Durability::Local).wait().unwrap();
        let obs = Arc::new(Notes::default());
        // Registered late: it is handed what was applied after `first - 1`.
        e.observe(
            Arc::clone(&obs) as Arc<dyn crate::observe::ApplyObserver>,
            first - 1,
        )
        .unwrap();
        let second = note(&e, "b");
        let seen: Vec<_> = obs.0.lock().unwrap().clone();
        assert_eq!(seen, vec![(first, b"a".to_vec()), (second, b"b".to_vec())]);
        // The observer holds "a" durably; "b" and later only in the log.
        e.set_log_floor("notes", first).unwrap();
        floor = first;
        note(&e, "c");
        for i in 0..50 {
            put(&e, 100 + i, "y", Durability::Local).wait().unwrap();
        }
        e.checkpoint().unwrap();
        assert_eq!(e.log_floor("notes"), Some(first));
    }
    let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
    assert_eq!(e.log_floor("notes"), Some(floor));
    let obs = Arc::new(Notes::default());
    e.observe(
        Arc::clone(&obs) as Arc<dyn crate::observe::ApplyObserver>,
        floor,
    )
    .unwrap();
    let texts: Vec<Vec<u8>> = obs
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|(_, n)| n.clone())
        .collect();
    assert_eq!(texts, vec![b"b".to_vec(), b"c".to_vec()]);
    // Without a floor the checkpoint trims, and a late observer from 0 fails.
    e.clear_log_floor("notes").unwrap();
    note(&e, "d");
    e.checkpoint().unwrap();
    assert!(
        e.observe(
            Arc::new(Notes::default()) as Arc<dyn crate::observe::ApplyObserver>,
            0
        )
        .is_err()
    );
}

#[test]
fn plain_writes_commit_with_the_transaction_or_not_at_all() {
    let dir = TempDir::new("plain");
    let e = Engine::open(cfg(dir.path(), StoreKind::Rocks)).unwrap();
    let raw = |n: u8| vec![0xF0, 0x51, n];
    let read = |n: u8| {
        e.store()
            .get(Cf::Data, &raw(n), crate::store::ReadTier::All)
            .unwrap()
            .expect_ready()
            .unwrap()
            .map(|b| b.to_vec())
    };
    let wait = |t: &crate::txn::CommitTicket| {
        t.wait().unwrap();
        t.log_ticket().unwrap().wait_applied().unwrap();
    };
    // Committed with a versioned write: both land.
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(row_key(1), b"v".to_vec()).unwrap();
    t.add_plain_put(Cf::Data, raw(1), b"one".to_vec()).unwrap();
    t.add_plain_put(Cf::Data, raw(2), b"two".to_vec()).unwrap();
    assert!(t.has_plain_writes());
    // Not visible before the commit.
    assert_eq!(read(1), None);
    wait(&t.commit().unwrap());
    assert_eq!(read(1).as_deref(), Some(&b"one"[..]));
    assert_eq!(get(&e, 1).as_deref(), Some(&b"v"[..]));
    // Alone (no versioned write), a delete commits too.
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.add_plain_delete(Cf::Data, raw(2)).unwrap();
    wait(&t.commit().unwrap());
    assert_eq!(read(2), None);
    // Rolled back: nothing.
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.add_plain_put(Cf::Data, raw(3), b"x".to_vec()).unwrap();
    t.rollback();
    assert_eq!(read(3), None);
    // A statement rollback and a savepoint rollback drop theirs only.
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.begin_statement();
    t.add_plain_put(Cf::Data, raw(4), b"kept".to_vec()).unwrap();
    t.begin_statement();
    t.add_plain_put(Cf::Data, raw(5), b"undone".to_vec())
        .unwrap();
    t.rollback_statement();
    let sp = t.savepoint();
    t.add_plain_delete(Cf::Data, raw(1)).unwrap();
    t.rollback_to(sp);
    // XA PREPARE refuses a transaction with plain writes.
    assert!(t.xa_prepare(b"x1").is_err());
    wait(&t.commit().unwrap());
    assert_eq!(read(4).as_deref(), Some(&b"kept"[..]));
    assert_eq!(read(5), None);
    assert_eq!(read(1).as_deref(), Some(&b"one"[..]));
}
