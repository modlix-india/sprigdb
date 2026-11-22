//! YCSB-style cells for the engine document store (ignored; run by hand):
//! workloads A (50% reads, 50% updates), B (95/5), C (reads only) and F
//! (read-modify-write) over `SPRIG_BENCH_RECORDS` documents of ten
//! 100-byte fields, keys chosen with a Zipf-like skew, `SPRIG_BENCH_THREADS`
//! client tasks for `SPRIG_BENCH_SECS` seconds per cell.
//!
//! Two targets: the store in this process on the engine over RocksDB,
//! called through its command layer (no wire protocol, no gateway), and,
//! with `SPRIG_TEST_MONGO_REF`, a reference MongoDB through the Rust
//! driver (wire protocol and the container's network included). The two
//! are not the same measurement; the numbers say how far the store's own
//! cost is from MongoDB's end-to-end latency on this laptop.
//!
//! `cargo test -p sprig-doc --release --test docstore_bench -- --ignored --nocapture`

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::unused_async,
    clippy::map_entry,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bson::{Document, doc};
use sprig_doc::docstore::DocStore;

fn env(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// A skewed record number: rank `n·u³`, scattered by a multiplier.
    fn key(&mut self, n: u64) -> u64 {
        let rank = (n as f64 * self.unit().powi(3)) as u64;
        rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) % n
    }
}

fn record(i: u64) -> Document {
    let mut d = doc! {"_id": format!("user{i:012}")};
    for f in 0..10 {
        d.insert(format!("field{f}"), "x".repeat(100));
    }
    d
}

#[derive(Clone, Copy)]
enum Op {
    Read,
    Update,
    Rmw,
}

fn mix(w: char, r: &mut Rng) -> Op {
    let u = r.unit();
    match w {
        'a' if u < 0.5 => Op::Update,
        'b' if u < 0.05 => Op::Update,
        'f' if u < 0.5 => Op::Rmw,
        _ => Op::Read,
    }
}

#[async_trait::async_trait]
trait Target: Send + Sync {
    async fn cmd(&self, c: Document) -> Document;
}

struct Store(Arc<DocStore>);

#[async_trait::async_trait]
impl Target for Store {
    async fn cmd(&self, mut c: Document) -> Document {
        let name = c.keys().next().unwrap().clone();
        c.insert("$db", "ycsb");
        match self.0.command(7, "u", "ycsb", &name, &c).await.unwrap() {
            sprig_doc::docstore::cmd::Reply::Doc(d) => d,
            r => r.into_doc(0),
        }
    }
}

struct Mongo(mongodb::Database);

#[async_trait::async_trait]
impl Target for Mongo {
    async fn cmd(&self, c: Document) -> Document {
        self.0.run_command(c).await.unwrap()
    }
}

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize]
}

/// This process's CPU time so far, in µs (`ps`: no unsafe code needed).
fn cpu_us() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "cputime=", "-p", &std::process::id().to_string()])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_default();
    // [[dd-]hh:]mm:ss.cc
    let mut secs = 0.0f64;
    for part in out.rsplit(':').enumerate() {
        let (i, p) = part;
        let p = p.rsplit('-').next().unwrap_or(p);
        let v: f64 = p.parse().unwrap_or(0.0);
        secs += v * [1.0, 60.0, 3600.0][i.min(2)];
    }
    (secs * 1e6) as u64
}

async fn cell(
    t: Arc<dyn Target>,
    w: char,
    n: u64,
    threads: u64,
    secs: u64,
) -> (f64, u64, u64, u64, f64) {
    let stop = Instant::now() + Duration::from_secs(secs);
    let ops = Arc::new(AtomicU64::new(0));
    let cpu0 = cpu_us();
    let t0 = Instant::now();
    let mut hs = Vec::new();
    for th in 0..threads {
        let t = Arc::clone(&t);
        let ops = Arc::clone(&ops);
        hs.push(tokio::spawn(async move {
            let mut r = Rng(0x1234_5678 + th * 7919);
            let (mut reads, mut updates) = (Vec::new(), Vec::new());
            while Instant::now() < stop {
                let id = format!("user{:012}", r.key(n));
                let s = Instant::now();
                match mix(w, &mut r) {
                    Op::Read => {
                        t.cmd(doc! {"find": "usertable", "filter": {"_id": id}, "limit": 1, "singleBatch": true}).await;
                        reads.push(s.elapsed().as_micros() as u64);
                    }
                    Op::Update => {
                        t.cmd(doc! {"update": "usertable", "updates": [{"q": {"_id": id}, "u": {"$set": {"field3": "y".repeat(100)}}}]}).await;
                        updates.push(s.elapsed().as_micros() as u64);
                    }
                    Op::Rmw => {
                        t.cmd(doc! {"find": "usertable", "filter": {"_id": &id}, "limit": 1, "singleBatch": true}).await;
                        t.cmd(doc! {"update": "usertable", "updates": [{"q": {"_id": id}, "u": {"$set": {"field5": "z".repeat(100)}}}]}).await;
                        updates.push(s.elapsed().as_micros() as u64);
                    }
                }
                ops.fetch_add(1, Ordering::Relaxed);
            }
            (reads, updates)
        }));
    }
    let (mut reads, mut updates) = (Vec::new(), Vec::new());
    for h in hs {
        let (r, u) = h.await.unwrap();
        reads.extend(r);
        updates.extend(u);
    }
    let el = t0.elapsed().as_secs_f64();
    let n_ops = ops.load(Ordering::Relaxed);
    let cpu = (cpu_us() - cpu0) as f64 / n_ops.max(1) as f64;
    (
        n_ops as f64 / el,
        pct(&mut reads, 0.5),
        pct(&mut reads, 0.99),
        pct(&mut updates, 0.99),
        cpu,
    )
}

async fn load(t: &Arc<dyn Target>, n: u64) -> f64 {
    let s = Instant::now();
    let mut i = 0;
    while i < n {
        let docs: Vec<Document> = (i..(i + 500).min(n)).map(record).collect();
        let r = t
            .cmd(doc! {"insert": "usertable", "documents": docs, "ordered": false})
            .await;
        assert!(r.get_array("writeErrors").is_err(), "{r}");
        i += 500;
    }
    n as f64 / s.elapsed().as_secs_f64()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a benchmark: run by hand"]
async fn ycsb_cells() {
    let n = env("SPRIG_BENCH_RECORDS", 100_000);
    let threads = env("SPRIG_BENCH_THREADS", 16);
    let secs = env("SPRIG_BENCH_SECS", 20);
    let mut targets: Vec<(&str, Arc<dyn Target>)> = Vec::new();
    let d = common::dir("bench");
    let store = common::rocks_store(&d);
    targets.push(("engine store (in process)", Arc::new(Store(store))));
    if let Ok(addr) = std::env::var("SPRIG_TEST_MONGO_REF") {
        let c = mongodb::Client::with_uri_str(format!(
            "mongodb://{addr}/?directConnection=true&maxPoolSize=64&w=1&journal=true"
        ))
        .await
        .unwrap();
        let db = c.database("ycsb");
        db.drop().await.unwrap();
        targets.push(("MongoDB 8.0 (driver, container)", Arc::new(Mongo(db))));
    }
    eprintln!("records {n}, threads {threads}, {secs} s per cell");
    eprintln!(
        "target | cell | ops/s | read p50 us | read p99 us | update p99 us | client-process CPU us/op"
    );
    for (name, t) in &targets {
        let lr = load(t, n).await;
        eprintln!("{name} | load | {lr:.0} | | | |");
        for w in ['a', 'b', 'c', 'f'] {
            let (ops, p50, p99, u99, cpu) = cell(Arc::clone(t), w, n, threads, secs).await;
            eprintln!("{name} | {w} | {ops:.0} | {p50} | {p99} | {u99} | {cpu:.1}");
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}
