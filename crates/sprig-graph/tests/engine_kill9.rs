//! `kill -9` while graph writes run on the engine: a child process runs
//! four writers on a graph stored in a real engine (RocksDB, commit log with
//! full syncs). Each writer creates a node with a relationship to a hub and
//! one back, relabels a hub now and then (which rewrites the label set in
//! every adjacency entry naming it, both directions, in one transaction)
//! and detach-deletes one of its older nodes. It prints every statement
//! once its commit is durable. The parent kills the child with SIGKILL at a
//! random moment, opens the engine again in-process, and checks:
//! - the layout's invariants: every relationship has its out-adjacency,
//!   in-adjacency, record and type-scan entries or none of them; no entry
//!   names a missing node; every entry's label set is its node's;
//! - every acknowledged node is there with both its relationships, and
//!   every acknowledged delete is gone.
//!
//! Rounds: `SPRIG_GRAPH_KILL9_ROUNDS` (default 6). The child is this test
//! binary, run with `SPRIG_GRAPH_KILL9_CHILD=<dir>`.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names
)]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sprig_engine::log::SyncMode;
use sprig_engine::{Engine, EngineConfig};
use sprig_graph::engine::{EngineGraph, check_invariants};
use sprig_graph::eval::Params;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

const WRITERS: u64 = 4;
const HUBS: u64 = 3;

fn cfg(dir: &Path) -> EngineConfig {
    let mut c = EngineConfig::new(dir);
    c.budget.engine_mb = 64;
    c.log.sync = SyncMode::Full;
    c.checkpoint_interval = Duration::from_millis(300);
    c.checkpoint_entries = 500;
    c
}

async fn run(g: &Arc<EngineGraph>, q: &str) -> sprig_common::Result<Output> {
    let mut tx = g.begin(true);
    let r = {
        let mut s = g.store(&mut tx);
        execute(&mut s, q, &Params::new(), &Budgets::default()).await
    };
    match r {
        Ok(o) => {
            tx.commit(g).await?;
            Ok(o)
        }
        Err(e) => {
            tx.rollback();
            Err(e)
        }
    }
}

#[test]
fn graph_kill9_child() {
    let Ok(dir) = std::env::var("SPRIG_GRAPH_KILL9_CHILD") else {
        return;
    };
    let round: u64 = std::env::var("SPRIG_GRAPH_KILL9_ROUND")
        .unwrap()
        .parse()
        .unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let e = Engine::open(cfg(Path::new(&dir))).unwrap();
        let g = EngineGraph::open(Arc::new(e), 5, 9, 1).unwrap();
        for h in 0..HUBS {
            run(&g, &format!("MERGE (:Hub {{h: {h}}})")).await.unwrap();
        }
        let out = Arc::new(Mutex::new(std::io::stdout()));
        let mut hs = Vec::new();
        for w in 0..WRITERS {
            let (g, out) = (Arc::clone(&g), Arc::clone(&out));
            hs.push(tokio::spawn(async move {
                let id = round * 100 + w;
                for j in 0u64.. {
                    let hub = (j + w) % HUBS;
                    let q = format!(
                        "MATCH (h:Hub {{h: {hub}}}) CREATE (n:W {{w: {id}, j: {j}}}), (n)-[:E {{j: {j}}}]->(h), (h)-[:F {{j: {j}}}]->(n)"
                    );
                    if run(&g, &q).await.is_ok() {
                        let mut o = out.lock().unwrap();
                        writeln!(o, "ACK {id} {j}").unwrap();
                        o.flush().unwrap();
                    }
                    if j % 7 == 3 {
                        let q = if j % 2 == 0 {
                            format!("MATCH (h:Hub {{h: {hub}}}) SET h:Odd")
                        } else {
                            format!("MATCH (h:Hub {{h: {hub}}}) REMOVE h:Odd")
                        };
                        let _ = run(&g, &q).await;
                    }
                    if j % 5 == 4 {
                        let old = j - 3;
                        let q = format!("MATCH (n:W {{w: {id}, j: {old}}}) DETACH DELETE n");
                        // Announced first: a kill between its commit and
                        // its `DEL` line may or may not have kept the node.
                        {
                            let mut o = out.lock().unwrap();
                            writeln!(o, "TRY {id} {old}").unwrap();
                            o.flush().unwrap();
                        }
                        if run(&g, &q).await.is_ok() {
                            let mut o = out.lock().unwrap();
                            writeln!(o, "DEL {id} {old}").unwrap();
                            o.flush().unwrap();
                        }
                    }
                }
            }));
        }
        for h in hs {
            let _ = h.await;
        }
    });
}

fn values(o: &Output) -> BTreeSet<(i64, i64)> {
    o.rows
        .iter()
        .filter_map(|r| match (&r[0], &r[1]) {
            (Value::Int(a), Value::Int(b)) => Some((*a, *b)),
            _ => None,
        })
        .collect()
}

#[test]
fn graph_kill9_loop() {
    if std::env::var("SPRIG_GRAPH_KILL9_CHILD").is_ok() {
        return;
    }
    let rounds: u64 = std::env::var("SPRIG_GRAPH_KILL9_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let dir: PathBuf = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("m-graph-kill9-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let exe = std::env::current_exe().unwrap();
    let mut seed = u64::from(std::process::id()) | 1;
    let mut acked: BTreeSet<(i64, i64)> = BTreeSet::new();
    let mut deleted: BTreeSet<(i64, i64)> = BTreeSet::new();
    let mut tried: BTreeSet<(i64, i64)> = BTreeSet::new();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for round in 0..rounds {
        let mut child = Command::new(&exe)
            .args([
                "graph_kill9_child",
                "--exact",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("SPRIG_GRAPH_KILL9_CHILD", &dir)
            .env("SPRIG_GRAPH_KILL9_ROUND", round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            let mut lines = Vec::new();
            for l in BufReader::new(stdout).lines() {
                let Ok(l) = l else { break };
                lines.push(l);
            }
            lines
        });
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        // Let the writers get going, then kill at a random moment.
        std::thread::sleep(Duration::from_millis(1500 + seed % 2500));
        child.kill().unwrap();
        let _ = child.wait();
        let lines = reader.join().unwrap();
        let (mut a, mut d) = (0, 0);
        for l in lines {
            let p: Vec<&str> = l.split(' ').collect();
            if p.len() != 3 || !matches!(p[0], "ACK" | "DEL" | "TRY") {
                continue;
            }
            let (Ok(x), Ok(y)) = (p[1].parse(), p[2].parse()) else {
                continue;
            };
            let k = (x, y);
            match p[0] {
                "ACK" => {
                    acked.insert(k);
                    a += 1;
                }
                "TRY" => {
                    tried.insert(k);
                }
                "DEL" => {
                    deleted.insert(k);
                    d += 1;
                }
                _ => {}
            }
        }
        // Recover in-process and check.
        rt.block_on(async {
            let e = Engine::open(cfg(&dir)).unwrap();
            let g = EngineGraph::open(Arc::new(e), 5, 9, 1).unwrap();
            let bad = check_invariants(&g).unwrap();
            assert!(bad.is_empty(), "round {round}: {bad:?}");
            let o = run(&g, "MATCH (n:W) RETURN n.w, n.j").await.unwrap();
            let present = values(&o);
            let o = run(
                &g,
                "MATCH (n:W)-[:E]->(h:Hub)-[:F]->(n) RETURN n.w, n.j",
            )
            .await
            .unwrap();
            let linked = values(&o);
            for k in &acked {
                if deleted.contains(k) {
                    assert!(!present.contains(k), "round {round}: deleted {k:?} is back");
                } else if present.contains(k) {
                    assert!(linked.contains(k), "round {round}: {k:?} lost a relationship");
                } else {
                    // Only a delete in flight at the kill may have taken it.
                    assert!(tried.contains(k), "round {round}: acknowledged {k:?} lost");
                }
            }
            eprintln!(
                "round {round}: {a} creates and {d} deletes acknowledged; {} nodes, all invariants hold",
                present.len()
            );
        });
        assert!(a > 0, "round {round}: the child acknowledged nothing");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
