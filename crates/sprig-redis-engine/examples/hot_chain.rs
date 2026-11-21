//! How fast one hot key's batches pass through the engine with no sync
//! cost: 50 tasks, one INCR batch each at a time.

#![allow(clippy::unwrap_used, clippy::print_stdout, clippy::cast_precision_loss)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::txn::testing::MemPath;
use sprig_engine::txn::{CommitPath, UnitConfig};
use sprig_redis_engine::{Backend, EngineUnit, Tenants};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let delay_us: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(0);
    let cmd: String = std::env::args().nth(2).unwrap_or_else(|| "INCR".into());
    let path = MemPath::start(UnitConfig::default(), Duration::from_micros(delay_us));
    let b = Backend::new(
        Arc::new(EngineUnit::new(path as Arc<dyn CommitPath>)),
        Tenants::All,
        16,
    );
    let d = b.db(1, 0).unwrap();
    let n: u32 = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let d = Arc::clone(&d);
        let cmd = cmd.clone();
        tasks.push(tokio::spawn(async move {
            let a: Vec<Bytes> = match cmd.as_str() {
                "SET" => vec!["SET".into(), "k".into(), "xxx".into()],
                "GET" => vec!["GET".into(), "k".into()],
                "HSET" => vec!["HSET".into(), "h".into(), "f".into(), "xxx".into()],
                _ => vec!["INCR".into(), "k".into()],
            };
            for _ in 0..n / 50 {
                d.run_batch(&[&a[..]]).await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let s = t0.elapsed().as_secs_f64();
    println!("{cmd} delay {delay_us} us: {:.0} ops/s", f64::from(n) / s);
}
