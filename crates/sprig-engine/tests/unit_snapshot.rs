//! Unit snapshots: export a tenant's unit at an applied index, ingest it
//! into another node, and compare span checksums.

#![allow(clippy::unwrap_used, clippy::many_single_char_names)]

mod txn_common;

use std::sync::Arc;

use bytes::Bytes;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{CommitPath, ReadMode, Txn, TxnOptions};
use sprig_engine::{Engine, StoreKind, UnitId};
use txn_common::{Dir, engine_cfg};

fn key(tenant: u64, id: u32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant,
        shard: 0,
        object: 3,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

fn put(e: &Engine, kvs: &[(Vec<u8>, Option<&str>)]) {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    for (k, v) in kvs {
        match v {
            Some(v) => t
                .put(k.clone(), Bytes::copy_from_slice(v.as_bytes()))
                .unwrap(),
            None => t.delete(k.clone()).unwrap(),
        }
    }
    t.commit().unwrap().wait().unwrap();
}

fn get(e: &Engine, k: &[u8]) -> Option<Bytes> {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(k, ReadMode::Snapshot)
        .unwrap()
        .expect_ready()
        .unwrap()
}

fn path(e: &Engine) -> Arc<dyn CommitPath> {
    Arc::new(e.clone())
}

/// Tenant 7 with history (updates, deletes, a prepared XA branch) and a
/// neighbour, tenant 8, that must not travel with it.
fn fill(e: &Engine) {
    for i in 0..300u32 {
        put(e, &[(key(7, i), Some("a")), (key(8, i), Some("n"))]);
    }
    for i in (0..300u32).step_by(3) {
        put(e, &[(key(7, i), Some("b"))]);
    }
    for i in (0..300u32).step_by(7) {
        put(e, &[(key(7, i), None)]);
    }
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.put(key(7, 9999), Bytes::from_static(b"prepared"))
        .unwrap();
    t.xa_prepare(b"x1").unwrap().wait().unwrap();
    settle(e);
}

/// Waits until the store applied everything durable (a commit's reply
/// waits for durability; its apply follows).
fn settle(e: &Engine) {
    for u in [UnitId(7), UnitId(8)] {
        let c = e.consensus();
        while c.applied_index(u) < c.durable_index(u) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

fn round_trip(store: StoreKind) {
    let (da, db) = (Dir::new("snap-a"), Dir::new("snap-b"));
    let a = Engine::open(engine_cfg(&da, store, SyncMode::Off)).unwrap();
    fill(&a);
    let out = da.0.join("export");
    let x = a.export_unit(UnitId(7), &out, 4 << 10).unwrap();
    assert_eq!(x.index, a.consensus().applied_index(UnitId(7)));
    assert_eq!(x.checksum, a.unit_checksum(UnitId(7)).unwrap());
    if store == StoreKind::Rocks {
        assert!(x.files.len() > 1, "small files: {}", x.files.len());
    }
    let b = Engine::open(engine_cfg(&db, store, SyncMode::Off)).unwrap();
    let got = b.ingest_unit(UnitId(7), &x.files).unwrap();
    assert_eq!(got, x.index);
    assert_eq!(b.unit_checksum(UnitId(7)).unwrap(), x.checksum);
    assert!(b.unit_checksum(UnitId(8)).unwrap().entries == 0);
    for i in [0u32, 1, 3, 7, 21, 299] {
        assert_eq!(get(&a, &key(7, i)), get(&b, &key(7, i)), "row {i}");
    }
    // The prepared branch came with it, holding its lock.
    assert_eq!(Txn::xa_recover(&path(&b)), vec![b"x1".to_vec()]);
    Txn::xa_commit(&path(&b), b"x1").unwrap().wait().unwrap();
    assert_eq!(get(&b, &key(7, 9999)).as_deref(), Some(&b"prepared"[..]));
    // The unit's log continues after the snapshot.
    let mut t = b.begin(TxnOptions::default()).unwrap();
    t.put(key(7, 5000), Bytes::from_static(b"next")).unwrap();
    let tk = t.commit().unwrap();
    tk.wait().unwrap();
    assert_eq!(tk.log_ticket().unwrap().index(), x.index + 2);
    // A unit that already holds data is refused.
    assert!(b.ingest_unit(UnitId(7), &[]).is_err());
    // And it all survives a restart of the target (the memory store keeps
    // nothing across one).
    if store != StoreKind::Rocks {
        return;
    }
    drop(b);
    let b = Engine::open(engine_cfg(&db, store, SyncMode::Off)).unwrap();
    assert_eq!(get(&b, &key(7, 5000)).as_deref(), Some(&b"next"[..]));
    assert_eq!(get(&a, &key(7, 1)), get(&b, &key(7, 1)));
}

#[test]
fn a_unit_round_trips_on_rocksdb() {
    round_trip(StoreKind::Rocks);
}

#[test]
fn a_unit_round_trips_on_the_memory_store() {
    round_trip(StoreKind::Mem);
}

/// The stream form, and a damaged stream refused with nothing left behind.
#[test]
fn a_damaged_stream_is_refused_and_undone() {
    let (da, db) = (Dir::new("snap-c"), Dir::new("snap-d"));
    let a = Engine::open(engine_cfg(&da, StoreKind::Mem, SyncMode::Off)).unwrap();
    fill(&a);
    let mut buf = Vec::new();
    let x = a.export_unit_to(UnitId(7), &mut buf).unwrap();
    let b = Engine::open(engine_cfg(&db, StoreKind::Rocks, SyncMode::Off)).unwrap();
    let mut bad = buf.clone();
    let n = bad.len();
    bad[n / 2] ^= 0x55;
    assert!(b.ingest_unit_from(UnitId(7), &mut bad.as_slice()).is_err());
    assert_eq!(b.unit_checksum(UnitId(7)).unwrap().entries, 0);
    assert_eq!(
        b.ingest_unit_from(UnitId(7), &mut buf.as_slice()).unwrap(),
        x.index
    );
    assert_eq!(b.unit_checksum(UnitId(7)).unwrap(), x.checksum);
    // Another unit's stream is refused.
    let mut other = Vec::new();
    a.export_unit_to(UnitId(8), &mut other).unwrap();
    assert!(
        b.ingest_unit_from(UnitId(9), &mut other.as_slice())
            .is_err()
    );
}

/// An export taken while commits keep coming is one point in time: every
/// commit wrote two rows of the unit, and the snapshot has both or neither.
#[test]
fn an_export_under_writes_is_consistent() {
    let (da, db) = (Dir::new("snap-e"), Dir::new("snap-f"));
    let a = Engine::open(engine_cfg(&da, StoreKind::Rocks, SyncMode::Off)).unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = {
        let (a, stop) = (a.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                put(
                    &a,
                    &[(key(7, i), Some("x")), (key(7, 100_000 + i), Some("y"))],
                );
                i += 1;
            }
            i
        })
    };
    std::thread::sleep(std::time::Duration::from_millis(200));
    let x = a
        .export_unit(UnitId(7), &da.0.join("export"), 64 << 10)
        .unwrap();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let n = w.join().unwrap();
    let b = Engine::open(engine_cfg(&db, StoreKind::Rocks, SyncMode::Off)).unwrap();
    assert_eq!(b.ingest_unit(UnitId(7), &x.files).unwrap(), x.index);
    let mut present = 0;
    for i in 0..n {
        let (p, q) = (get(&b, &key(7, i)), get(&b, &key(7, 100_000 + i)));
        assert_eq!(p.is_some(), q.is_some(), "commit {i} is half there");
        present += u64::from(p.is_some());
    }
    // One commit per index here: the snapshot holds exactly its index's.
    assert_eq!(present, x.index);
}

/// A part that keeps what it was given.
struct Kept(std::sync::Mutex<Vec<(u64, Vec<u8>)>>);

impl sprig_engine::snapshot::UnitSnapshotPart for Kept {
    fn name(&self) -> &'static str {
        "kept"
    }

    fn export(
        &self,
        unit: UnitId,
        _index: u64,
        out: &mut dyn std::io::Write,
    ) -> sprig_common::Result<()> {
        out.write_all(format!("state of {}", unit.0).as_bytes())
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))
    }

    fn ingest(
        &self,
        unit: UnitId,
        _index: u64,
        input: &mut dyn std::io::Read,
    ) -> sprig_common::Result<()> {
        let mut b = Vec::new();
        input
            .read_to_end(&mut b)
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        self.0.lock().unwrap().push((unit.0, b));
        Ok(())
    }
}

/// A snapshot ingested before its part is registered (a replica restoring
/// while it opens, before the stream log starts) keeps the part's bytes and
/// hands them over when the part registers.
#[test]
fn a_part_registered_after_the_ingest_gets_its_bytes() {
    let (da, db) = (Dir::new("snap-p1"), Dir::new("snap-p2"));
    let a = Engine::open(engine_cfg(&da, StoreKind::Mem, SyncMode::Off)).unwrap();
    a.add_snapshot_part(Arc::new(Kept(std::sync::Mutex::new(Vec::new()))));
    fill(&a);
    let mut buf = Vec::new();
    a.export_unit_to(UnitId(7), &mut buf).unwrap();
    let b = Engine::open(engine_cfg(&db, StoreKind::Mem, SyncMode::Off)).unwrap();
    b.ingest_unit_from(UnitId(7), &mut buf.as_slice()).unwrap();
    let kept = Arc::new(Kept(std::sync::Mutex::new(Vec::new())));
    b.add_snapshot_part(Arc::clone(&kept) as Arc<dyn sprig_engine::snapshot::UnitSnapshotPart>);
    assert_eq!(*kept.0.lock().unwrap(), vec![(7, b"state of 7".to_vec())]);
}
