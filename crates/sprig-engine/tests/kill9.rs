//! The `kill -9` harness: a child process commits under load and prints
//! every commit once it is acknowledged (durable); the parent kills it with
//! SIGKILL at a random moment, recovers the engine in-process, and checks
//! that every acknowledged commit is there and no commit is partial.
//!
//! Rounds: `SPRIG_KILL9_ROUNDS` (default 5). The child is this same test
//! binary, run with `SPRIG_KILL9_CHILD=<dir>`.

#![allow(clippy::unwrap_used)]

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_engine::keys::{KeyPrefix, kind, memcmp};
use sprig_engine::log::SyncMode;
use sprig_engine::txn::{ReadMode, TxnOptions};
use sprig_engine::{Durability, Engine, EngineConfig};

const THREADS: u32 = 8;

fn cfg(dir: &Path) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.budget.engine_mb = 128;
    c.log.sync = SyncMode::Full;
    c.checkpoint_interval = Duration::from_millis(300);
    c.checkpoint_entries = 2_000;
    c
}

/// Two rows per commit: `(round, thread, i, side)`.
fn key(round: u32, thread: u32, i: u32, side: u8) -> Vec<u8> {
    let mut k = KeyPrefix {
        tenant: 9,
        shard: 0,
        object: u32::from(side),
        kind: kind::PRIMARY,
        index: 0,
    }
    .to_vec();
    for v in [round, thread, i] {
        memcmp::put_uint(&mut k, u64::from(v), 4);
    }
    k
}

#[test]
fn kill9_child() {
    let Ok(dir) = std::env::var("SPRIG_KILL9_CHILD") else {
        return;
    };
    let round: u32 = std::env::var("SPRIG_KILL9_ROUND").unwrap().parse().unwrap();
    let e = Engine::open(cfg(Path::new(&dir))).unwrap();
    let out = Arc::new(std::sync::Mutex::new(std::io::stdout()));
    let hs: Vec<_> = (0..THREADS)
        .map(|t| {
            let (e, out) = (e.clone(), Arc::clone(&out));
            std::thread::spawn(move || {
                for i in 0.. {
                    let mut tx = e
                        .begin(TxnOptions {
                            durability: Some(Durability::Local),
                            ..TxnOptions::default()
                        })
                        .unwrap();
                    tx.put(key(round, t, i, 1), format!("a{i}").into_bytes())
                        .unwrap();
                    tx.put(key(round, t, i, 2), vec![b'b'; 200]).unwrap();
                    let tk = tx.commit().unwrap();
                    tk.log_ticket().unwrap().wait().unwrap();
                    let mut o = out.lock().unwrap();
                    writeln!(o, "ACK {t} {i}").unwrap();
                    o.flush().unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
}

fn get(e: &Engine, k: &[u8]) -> bool {
    let mut t = e.begin(TxnOptions::default()).unwrap();
    t.get(k, ReadMode::Snapshot)
        .unwrap()
        .expect_ready()
        .unwrap()
        .is_some()
}

#[test]
fn kill9_loop() {
    if std::env::var("SPRIG_KILL9_CHILD").is_ok() {
        return;
    }
    let rounds: u32 = std::env::var("SPRIG_KILL9_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let dir: PathBuf =
        std::env::temp_dir().join(format!("sprig-engine-kill9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().unwrap();
    let mut seed = u64::from(std::process::id()) | 1;
    let mut total_acked = 0usize;
    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args(["kill9_child", "--exact", "--nocapture", "--test-threads=1"])
            .env("SPRIG_KILL9_CHILD", &dir)
            .env("SPRIG_KILL9_ROUND", round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            let mut acked = Vec::new();
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                let mut p = line.split_whitespace();
                if p.next() == Some("ACK") {
                    let t: u32 = p.next().unwrap().parse().unwrap();
                    let i: u32 = p.next().unwrap().parse().unwrap();
                    acked.push((t, i));
                }
            }
            acked
        });
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let wait = Duration::from_millis(400 + seed % 1200);
        let start = Instant::now();
        while start.elapsed() < wait {
            std::thread::sleep(Duration::from_millis(20));
        }
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();
        let acked = reader.join().unwrap();
        assert!(
            !acked.is_empty(),
            "round {round}: the child acknowledged nothing"
        );
        total_acked += acked.len();
        let e = Engine::open(cfg(&dir)).unwrap();
        for &(t, i) in &acked {
            assert!(
                get(&e, &key(round, t, i, 1)),
                "round {round}: acknowledged ({t}, {i}) lost"
            );
        }
        let max = acked.iter().map(|a| a.1).max().unwrap_or(0);
        for t in 0..THREADS {
            for i in 0..=max + 64 {
                assert_eq!(
                    get(&e, &key(round, t, i, 1)),
                    get(&e, &key(round, t, i, 2)),
                    "round {round}: partial commit ({t}, {i})"
                );
            }
        }
        eprintln!(
            "kill9 round {round}: {} acknowledged, all present, after {wait:?}",
            acked.len()
        );
    }
    eprintln!("kill9: {rounds} rounds, {total_acked} acknowledged commits, 0 lost");
    let _ = std::fs::remove_dir_all(&dir);
}
