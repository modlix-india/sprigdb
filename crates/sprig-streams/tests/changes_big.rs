//! A very large transaction read by a slow consumer: the change log pages
//! it from disk, so the process's memory stays flat while the consumer
//! reads (no growth beyond a few pages).
//!
//! Ignored by default (it writes about 70 MB); run with `--ignored`.
//! `SPRIG_BIG_ROWS` sets the row count (default 1,000,000; the engine's
//! default per-transaction lock memory, 64 MiB at 87 bytes a row lock,
//! would refuse a million rows with 1206, so the test raises it).

#![allow(
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    missing_docs
)]

use std::time::{Duration, Instant};

use sprig_engine::keys::{KeyPrefix, kind};
use sprig_engine::txn::TxnOptions;
use sprig_engine::{Engine, EngineConfig};
use sprig_streams::changes::{ChangeConfig, ChangeLog};
use sprig_streams::seg::testing::TempDir;

/// The process's resident set, in KiB.
fn rss_kib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

#[test]
#[ignore = "writes about 100 MB; run explicitly"]
fn large_transaction_streams_with_flat_memory() {
    const T: u64 = 41;
    let rows: u32 = std::env::var("SPRIG_BIG_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let d = TempDir::new("chg-big");
    let mut cfg = EngineConfig::new(d.path().join("engine"));
    cfg.budget.engine_mb = 64;
    // A million row locks need more than the default 64 MiB of lock
    // memory per transaction (`engine.txn.lock_mem_mb`).
    cfg.txn.max_lock_bytes_per_txn = 256 << 20;
    let e = Engine::open(cfg).unwrap();
    let log = ChangeLog::open(&e, d.path().join("changes"), ChangeConfig::default()).unwrap();
    let unit = Engine::unit_of_tenant(T).0;
    let start = log.head(unit).0;
    let t0 = Instant::now();
    {
        let mut t = e.begin(TxnOptions::default()).unwrap();
        for i in 0..rows {
            let mut k = KeyPrefix {
                tenant: T,
                shard: 0,
                object: 9,
                kind: kind::PRIMARY,
                index: 0,
            }
            .to_vec();
            k.extend_from_slice(&i.to_be_bytes());
            t.put(k, vec![b'v'; 48]).unwrap();
        }
        let c = t.commit().unwrap();
        c.wait().unwrap();
    }
    let want = e.consensus().durable_index(sprig_engine::UnitId(unit));
    while e.last_applied_of(sprig_engine::UnitId(unit)) < want {
        std::thread::sleep(Duration::from_millis(5));
    }
    let committed = t0.elapsed();
    // Let the commit's own memory settle before measuring.
    log.flush().unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let base = rss_kib();
    let mut peak = base;
    let mut r = log.reader(unit, start, None).unwrap();
    let mut n = 0u64;
    let mut pages = 0u64;
    let mut biggest = 0usize;
    let t1 = Instant::now();
    loop {
        let p = r.read(1 << 20).unwrap();
        let bytes: usize = p
            .changes
            .iter()
            .map(|c| c.key.len() + c.value.as_ref().map_or(0, Vec::len))
            .sum();
        biggest = biggest.max(bytes);
        n += p.changes.len() as u64;
        pages += 1;
        if pages.is_multiple_of(16) {
            peak = peak.max(rss_kib());
            // A slow consumer.
            std::thread::sleep(Duration::from_millis(2));
        }
        if p.at_end {
            break;
        }
    }
    peak = peak.max(rss_kib());
    eprintln!(
        "{rows} rows: committed and applied in {committed:?}; read {n} changes in {pages} pages in {:?}; \
         biggest page {biggest} bytes; RSS {base} KiB before reading, peak {peak} KiB (+{} KiB)",
        t1.elapsed(),
        peak.saturating_sub(base)
    );
    assert_eq!(n, u64::from(rows));
    assert!(biggest <= 2 << 20, "a page held {biggest} bytes");
    assert!(
        peak.saturating_sub(base) < 64 * 1024,
        "memory grew by {} KiB while reading",
        peak.saturating_sub(base)
    );
}
