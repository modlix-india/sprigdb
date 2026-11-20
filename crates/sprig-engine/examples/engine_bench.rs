//! Micro-benchmarks of the storage core: point get and put latency, and
//! group-commit throughput.
//!
//! `cargo run --release -p sprig-engine --example engine_bench -- <dir> [secs] [sync|nosync]`

// A measuring tool: it panics on any failure, and its statistics convert
// between counts and floating point freely.
#![allow(
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use sprig_engine::keys::{self, KeyPrefix, Ts, kind, memcmp};
use sprig_engine::log::{Payload, SyncMode};
use sprig_engine::store::{Cf, ReadTier};
use sprig_engine::{Durability, Engine, EngineConfig};

fn row(id: u32) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant: 1,
        shard: 0,
        object: 1,
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    memcmp::put_i32(&mut k, i32::try_from(id).unwrap_or(i32::MAX));
    k
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn commit(e: &Engine, id: u32, ts: Ts, d: Durability) -> sprig_engine::log::Ticket {
    let mut p = Payload::with_capacity(160);
    let mut v = vec![keys::value::PUT];
    v.extend_from_slice(&[b'x'; 120]);
    p.put(Cf::Data, &keys::versioned(&row(id), ts), &v);
    e.consensus().propose(e.unit(), p.into_bytes(), d).unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/tmp/sprig-engine-bench".into());
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let sync = args.get(3).is_none_or(|s| s != "nosync");
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = EngineConfig::new(&dir);
    c.budget.engine_mb = 1024;
    c.log.sync = if sync { SyncMode::Full } else { SyncMode::Off };
    let e = Engine::open(c).unwrap();
    println!(
        "dir {dir}, sync {}",
        if sync {
            "full (F_FULLFSYNC on macOS, fdatasync on Linux)"
        } else {
            "off"
        }
    );

    // Preload 100k rows (sysbench-sized values) through the log.
    let n = 100_000u32;
    let t0 = Instant::now();
    let mut last = None;
    for id in 0..n {
        last = Some(commit(&e, id, Ts(10), Durability::LocalRelaxed));
    }
    last.unwrap().wait_applied().unwrap();
    println!(
        "preload {n} rows via the log (relaxed): {:.0} rows/s",
        f64::from(n) / t0.elapsed().as_secs_f64()
    );

    // Point gets: versioned reads of random rows, all cached.
    let store = e.store();
    let mut buf = Vec::new();
    let mut lat = Vec::with_capacity(200_000);
    let mut x = 88_172_645_463_325_252u64;
    for round in 0..2 {
        lat.clear();
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = row((x % u64::from(n)) as u32);
            let t = Instant::now();
            let r = store
                .get_version(Cf::Data, &k, Ts::MAX, ReadTier::CacheOnly, &mut buf)
                .unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
            assert!(matches!(r, sprig_engine::Read::Ready(Some(_))) || round == 0);
        }
    }
    let avg = lat.iter().sum::<u64>() / lat.len() as u64;
    println!(
        "point get (memtable, versioned seek): avg {avg} ns, p50 {} ns, p99 {} ns",
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.99)
    );
    e.checkpoint().unwrap();
    for _ in 0..2 {
        lat.clear();
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = row((x % u64::from(n)) as u32);
            let t = Instant::now();
            let _ = store
                .get_version(Cf::Data, &k, Ts::MAX, ReadTier::All, &mut buf)
                .unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
    }
    let avg = lat.iter().sum::<u64>() / lat.len() as u64;
    println!(
        "point get (SST, block cache warm): avg {avg} ns, p50 {} ns, p99 {} ns",
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.99)
    );

    // Point puts, one client: propose and wait for durability.
    for d in [Durability::Local, Durability::LocalRelaxed] {
        lat.clear();
        let end = Instant::now() + Duration::from_secs(secs);
        let mut id = 0;
        while Instant::now() < end {
            let t = Instant::now();
            commit(&e, id % n, Ts(20 + u64::from(id)), d)
                .wait()
                .unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
            id += 1;
        }
        let avg = lat.iter().sum::<u64>() / lat.len() as u64;
        println!(
            "point put {d:?}, 1 client: {} commits, avg {:.1} us, p50 {:.1} us, p99 {:.1} us",
            lat.len(),
            avg as f64 / 1e3,
            pct(&mut lat, 0.5) as f64 / 1e3,
            pct(&mut lat, 0.99) as f64 / 1e3
        );
    }

    // Group commit: N clients, Local durability.
    for clients in [1u32, 8, 32, 64, 128] {
        let before = e.log().stats();
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        let hs: Vec<_> = (0..clients)
            .map(|c| {
                let (e, stop, done) = (e.clone(), Arc::clone(&stop), Arc::clone(&done));
                std::thread::spawn(move || {
                    let mut i = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        commit(
                            &e,
                            (c * 1000 + (i % 1000) as u32) % n,
                            Ts(1_000_000 + i),
                            Durability::Local,
                        )
                        .wait()
                        .unwrap();
                        i += 1;
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().unwrap();
        }
        let el = t0.elapsed().as_secs_f64();
        let after = e.log().stats();
        let groups = after.groups - before.groups;
        let entries = after.entries - before.entries;
        let syncs = after.syncs - before.syncs;
        println!(
            "group commit, {clients} clients: {:.0} commits/s, {:.0} syncs/s, {:.1} entries/group, write+sync {:.0} us/group",
            done.load(Ordering::Relaxed) as f64 / el,
            syncs as f64 / el,
            entries as f64 / groups.max(1) as f64,
            (after.write_us - before.write_us) as f64 / groups.max(1) as f64
        );
    }
    let s = e.stats();
    println!("store: {:?}", s.store);
    println!(
        "log disk {} bytes; checkpoints {}",
        s.log_disk_bytes, s.checkpoints
    );
}
