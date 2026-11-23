//! A unit snapshot round trip at size: fill one tenant's unit with
//! `<MB>` of rows, export it (SST files), ingest it into a second engine,
//! and compare the span checksums.
//!
//! `cargo run --release -p sprig-engine --example unit_snapshot -- <dir> [MB]`

// A measuring tool: it panics on any failure.
#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use std::time::Instant;

use bytes::Bytes;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig, UnitId};

fn open(dir: &std::path::Path) -> Engine {
    let mut c = EngineConfig::new(dir);
    c.budget.engine_mb = 1024;
    c.log.sync = SyncMode::Off;
    Engine::open(c).unwrap()
}

fn du(p: &std::path::Path) -> u64 {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let m = e.metadata().unwrap();
            n += if m.is_dir() { du(&e.path()) } else { m.len() };
        }
    }
    n
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(args.get(1).expect("a directory"));
    let mb: u64 = args.get(2).map_or(1024, |a| a.parse().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
    let unit = UnitId(77);
    let a = open(&dir.join("a"));
    let t0 = Instant::now();
    let value = Bytes::from(vec![0x5a; 1000]);
    let mut id = 0u64;
    for _ in 0..mb {
        let mut t = a.begin(TxnOptions::default()).unwrap();
        for _ in 0..1000 {
            let mut k = KeyPrefix {
                tenant: unit.0,
                shard: 0,
                object: 1,
                kind: kind::PRIMARY,
                index: 0,
            }
            .to_vec();
            k.extend_from_slice(&id.to_be_bytes());
            let mut v = value.to_vec();
            v[..8].copy_from_slice(&id.to_be_bytes());
            t.put(k, Bytes::from(v)).unwrap();
            id += 1;
        }
        t.commit().unwrap().wait().unwrap();
    }
    a.checkpoint().unwrap();
    let filled = t0.elapsed();
    let t1 = Instant::now();
    let x = a.export_unit(unit, &dir.join("export"), 64 << 20).unwrap();
    let exported = t1.elapsed();
    let files = du(&dir.join("export"));
    let b = open(&dir.join("b"));
    let t2 = Instant::now();
    let index = b.ingest_unit(unit, &x.files).unwrap();
    let ingested = t2.elapsed();
    let t3 = Instant::now();
    let (sa, sb) = (
        a.unit_checksum(unit).unwrap(),
        b.unit_checksum(unit).unwrap(),
    );
    let summed = t3.elapsed();
    println!(
        "unit_snapshot: {} rows, {:.2} GB of keys and values (filled in {filled:.1?}); \
         export at index {} into {} files, {:.2} GB on disk, in {exported:.1?}; \
         ingest in {ingested:.1?} (index {index}); checksums {} (source {:?}, target {:?}, {summed:.1?})",
        x.checksum.entries,
        x.checksum.bytes as f64 / 1e9,
        x.index,
        x.files.len(),
        files as f64 / 1e9,
        if sa == sb && sa == x.checksum {
            "EQUAL"
        } else {
            "DIFFER"
        },
        sa,
        sb
    );
    assert_eq!(sa, sb);
    drop((a, b));
    let _ = std::fs::remove_dir_all(&dir);
}
