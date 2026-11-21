//! Hot-key commit throughput with early lock release on and off, on the
//! real engine (RocksDB, raft-engine with real syncs).
//!
//! Each client loops `BEGIN; SELECT … FOR UPDATE; UPDATE (+1); COMMIT` on
//! one key and waits for durability before its next transaction, as a
//! client waits for its reply.
//!
//! `cargo run --release --example txn_hotkey_bench -- <data-dir> [clients] [seconds] [rocks|mem]`

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{CommitPath, LockRequest, Locked, Txn, TxnOptions};
use sprig_engine::{Engine, EngineConfig, StoreKind};

fn run(
    dir: &std::path::Path,
    store: StoreKind,
    clients: usize,
    secs: u64,
    elr: bool,
) -> (f64, f64, u64) {
    let data = dir.join(if elr { "elr" } else { "hold" });
    let _ = std::fs::remove_dir_all(&data);
    let mut cfg = EngineConfig::new(&data);
    cfg.store = store;
    cfg.log.sync = SyncMode::Full;
    cfg.budget.engine_mb = 512;
    let engine = Engine::open(cfg).expect("open");
    engine.txn_shared().set_early_lock_release(elr);
    let path: Arc<dyn CommitPath> = Arc::new(engine.clone());
    let key = Bytes::from_static(b"hot\0");
    {
        // The row exists before the clients start, as a hot row does. (Two
        // clients that both find it missing and both insert it deadlock,
        // which is what `InnoDB` does and not what this measures.)
        let mut t = Txn::begin_on(path.clone(), TxnOptions::mariadb());
        t.put(key.clone(), Bytes::copy_from_slice(&0u64.to_be_bytes()))
            .expect("seed");
        t.commit().expect("commit").wait().expect("durable");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));
    let lat_us = Arc::new(AtomicU64::new(0));
    let syncs0 = engine.stats().log.syncs;
    let threads: Vec<_> = (0..clients)
        .map(|_| {
            let (path, key, stop, done, lat_us) = (
                path.clone(),
                key.clone(),
                stop.clone(),
                done.clone(),
                lat_us.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    let mut t = Txn::begin_on(path.clone(), TxnOptions::mariadb());
                    let cur = match t.lock_get_blocking(&key, LockRequest::for_update()) {
                        Ok(Locked::Row(Some(v))) => {
                            u64::from_be_bytes(v[..8].try_into().unwrap_or([0; 8]))
                        }
                        Ok(_) => 0,
                        Err(e) => panic!("lock: {e}"),
                    };
                    t.put(
                        key.clone(),
                        Bytes::copy_from_slice(&(cur + 1).to_be_bytes()),
                    )
                    .expect("put");
                    t.commit().expect("commit").wait().expect("durable");
                    done.fetch_add(1, Ordering::Relaxed);
                    lat_us.fetch_add(
                        u64::try_from(t0.elapsed().as_micros()).unwrap_or(0),
                        Ordering::Relaxed,
                    );
                }
            })
        })
        .collect();
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(secs));
    stop.store(true, Ordering::Relaxed);
    for t in threads {
        let _ = t.join();
    }
    let elapsed = start.elapsed().as_secs_f64();
    let n = done.load(Ordering::Relaxed);
    let syncs = engine.stats().log.syncs - syncs0;
    drop(path);
    drop(engine);
    let _ = std::fs::remove_dir_all(&data);
    #[allow(clippy::cast_precision_loss)]
    let r = (
        n as f64 / elapsed,
        lat_us.load(Ordering::Relaxed) as f64 / n.max(1) as f64,
        syncs,
    );
    r
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(args.next().unwrap_or_else(|| ".".into()));
    let clients: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(50);
    let secs: u64 = args.next().and_then(|a| a.parse().ok()).unwrap_or(5);
    let store = match args.next().as_deref() {
        Some("mem") => StoreKind::Mem,
        _ => StoreKind::Rocks,
    };
    std::fs::create_dir_all(&dir).expect("dir");
    println!("hot key on the engine ({store:?}, real syncs), {clients} clients, {secs} s each");
    let mut tps = [0.0; 2];
    for (i, elr) in [false, true].into_iter().enumerate() {
        let (t, avg, syncs) = run(&dir, store, clients, secs, elr);
        tps[i] = t;
        println!(
            "early lock release {:>3}: {t:>9.0} commits/s, mean latency {avg:>7.0} us, {syncs} syncs",
            if elr { "on" } else { "off" }
        );
    }
    println!("speed-up: {:.1}x", tps[1] / tps[0].max(1e-9));
}
