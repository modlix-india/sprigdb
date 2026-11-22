//! The engine store against the in-memory store on random graph
//! operations: creates, merges, property and label changes, relationship
//! and node deletes (plain and detached), failing statements, an index and
//! a uniqueness constraint. After every few operations a set of reads must
//! give the same rows on both, and the engine's keys must keep their
//! invariants (every relationship has all four keys or none, label sets in
//! adjacency entries are current, one label-scan entry per node).
//!
//! The engine is a real one (RocksDB and its commit log) in a temporary
//! directory, on one and on three data shards. Each statement is one engine
//! transaction, committed when it succeeds and rolled back when it fails;
//! the in-memory store is restored to its state before a failed statement,
//! and a statement that leaves a deleted node with relationships counts as
//! failed on it, as it does at the engine's commit.
//!
//! `SPRIG_ENGINE_MODEL_OPS` sets the operations per run (default 600).

#![allow(clippy::unwrap_used, clippy::too_many_lines, clippy::many_single_char_names)]

use std::path::PathBuf;
use std::sync::Arc;

use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_graph::engine::{EngineGraph, check_invariants};
use sprig_graph::eval::Params;
use sprig_graph::mem::MemStore;
use sprig_graph::store::GraphStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn dir(tag: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("m-graph-model-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn rows(o: &Output) -> Vec<String> {
    let mut v: Vec<String> = o
        .rows
        .iter()
        .map(|r| r.iter().map(Value::display).collect::<Vec<_>>().join(" | "))
        .collect();
    v.sort();
    v
}

async fn on_engine(g: &Arc<EngineGraph>, q: &str) -> sprig_common::Result<Output> {
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

async fn on_mem(m: &mut MemStore, q: &str) -> sprig_common::Result<Output> {
    let before = m.clone();
    let r = execute(m, q, &Params::new(), &Budgets::default()).await;
    let r = match r {
        Ok(o) if !o.deleted_nodes.is_empty() => {
            if m.edges_touching(&o.deleted_nodes, 1).await?.is_empty() {
                Ok(o)
            } else {
                Err(sprig_common::SprigError::internal(
                    "deleted node kept relationships",
                ))
            }
        }
        r => r,
    };
    if r.is_err() {
        *m = before;
    }
    r
}

/// A random statement over nodes keyed `k` in `0..next`.
fn statement(r: &mut Rng, next: &mut u64) -> String {
    let n = (*next).max(1);
    let mut pick = || r.below(n);
    let (a, b) = (pick(), pick());
    let roll = r.below(100);
    let v = r.below(10);
    let labels = [":A", ":B", ":A:B", ":C", ""][usize::try_from(r.below(5)).unwrap()];
    let t = ["T", "U"][usize::try_from(r.below(2)).unwrap()];
    match roll {
        0..=17 => {
            *next += 1;
            format!("CREATE (n{labels} {{k: {}, v: {v}, s: 's{v}'}})", *next - 1)
        }
        18..=37 => format!(
            "MATCH (a {{k: {a}}}), (b {{k: {b}}}) CREATE (a)-[:{t} {{w: {v}, big: '{}'}}]->(b)",
            "x".repeat(usize::try_from(r.below(2)).unwrap() * 300)
        ),
        38..=45 => format!("MATCH (a {{k: {a}}}) SET a.v = {v}, a.f = {v}.5"),
        46..=51 => format!(
            "MATCH (a {{k: {a}}}) SET a{} REMOVE a{}",
            [":A", ":B", ":C"][usize::try_from(v % 3).unwrap()],
            [":A", ":B", ":C"][usize::try_from((v + 1) % 3).unwrap()]
        ),
        52..=55 => format!(
            "MATCH (a {{k: {a}}}) REMOVE a.s SET a += {{z: [{v}, {}]}}",
            v + 1
        ),
        // Every relationship of the lowest weight (ids differ between the
        // stores after failed statements, so no tie may decide).
        56..=63 => format!(
            "MATCH (a {{k: {a}}})-[r:{t}]->() WITH a, min(r.w) AS mw MATCH (a)-[r:{t}]->() WHERE r.w = mw DELETE r"
        ),
        64..=67 => format!("MATCH (a {{k: {a}}})-[r]-(b) WHERE b.k = {b} SET r.w = {v}"),
        68..=71 => format!("MATCH (a {{k: {a}}}) DETACH DELETE a"),
        72..=75 => format!("MATCH (a {{k: {a}}}) DELETE a"),
        76..=81 => {
            *next += 1;
            format!(
                "MERGE (m:M {{u: {}}}) ON CREATE SET m.k = {} ON MATCH SET m.hits = coalesce(m.hits, 0) + 1",
                v % 4,
                *next - 1
            )
        }
        82..=85 => {
            let start = *next;
            *next += 3;
            format!(
                "UNWIND range({start}, {}) AS i CREATE (:A:Bulk {{k: i, v: i % 7}})",
                start + 2
            )
        }
        86..=89 => {
            // Fails after writing: nothing of it may stay.
            *next += 1;
            format!(
                "CREATE (x:Doomed {{k: {}}}) WITH x RETURN 1 / 0 AS boom",
                *next - 1
            )
        }
        90..=93 => format!("MATCH (a:Uq) WHERE a.k = {a} SET a.code = {v}"),
        _ => {
            *next += 1;
            format!("CREATE (:Uq {{k: {}, code: {}}})", *next - 1, r.below(40))
        }
    }
}

const CHECKS: &[&str] = &[
    // Labels as a set: the engine gives them in label-id order, memory in
    // the order they were added (Cypher leaves the order open).
    "MATCH (n) RETURN n.k, size(labels(n)), n:A, n:B, n:C, n:M, n:Bulk, n:Uq, properties(n)",
    "MATCH (a)-[r]->(b) RETURN a.k, type(r), r.w, size(coalesce(r.big, '')), b.k",
    "MATCH (a)<-[r:T]-(b) RETURN a.k, r.w, b.k",
    "MATCH (a:A)-[:T|U]-(b:B) RETURN a.k, b.k",
    "MATCH (n:A) RETURN n.k",
    "MATCH (n:B:A) RETURN n.k",
    "MATCH (n:C) RETURN count(*)",
    "MATCH (n:A {v: 3}) RETURN n.k",
    "MATCH (n:Uq {code: 7}) RETURN n.k",
    "MATCH (a {k: 1})-[:T*1..3]->(b) RETURN DISTINCT b.k",
    "MATCH (a)-[:T]->(b)-[:U]->(c) RETURN a.k, b.k, c.k",
    "MATCH (n) WHERE NOT (n)--() RETURN n.k",
    "MATCH (m:M) RETURN m.u, m.hits",
    "MATCH ()-[r]->() RETURN type(r), count(*)",
];

async fn run_model(shards: usize, seed: u64, ops: usize) {
    let d = dir(&format!("{shards}-{seed}"));
    let mut cfg = EngineConfig::new(&d);
    cfg.store = StoreKind::Rocks;
    cfg.budget.engine_mb = 64;
    let engine = Engine::open(cfg).unwrap();
    let g = EngineGraph::open(Arc::new(engine.clone()), 5, 9, shards).unwrap();
    let mut m = MemStore::new(shards);
    let mut r = Rng(seed);
    let mut next = 0u64;
    for setup in [
        "CREATE INDEX a_v FOR (n:A) ON (n.v)",
        "CREATE CONSTRAINT uq_code FOR (n:Uq) REQUIRE n.code IS UNIQUE",
    ] {
        on_engine(&g, setup).await.unwrap();
        on_mem(&mut m, setup).await.unwrap();
    }
    let (mut ok, mut failed) = (0, 0);
    for i in 0..ops {
        let q = statement(&mut r, &mut next);
        let e = on_engine(&g, &q).await;
        let w = on_mem(&mut m, &q).await;
        match (&e, &w) {
            (Ok(a), Ok(b)) => {
                assert_eq!(rows(a), rows(b), "{q}");
                ok += 1;
            }
            (Err(_), Err(_)) => failed += 1,
            _ => panic!(
                "seed {seed} op {i}: {q}\n  engine: {:?}\n  memory: {:?}",
                e.map(|o| rows(&o)),
                w.map(|o| rows(&o))
            ),
        }
        if i % 25 == 24 || i + 1 == ops {
            for c in CHECKS {
                let a = on_engine(&g, c).await.unwrap();
                let b = on_mem(&mut m, c).await.unwrap();
                let (ra, rb) = (rows(&a), rows(&b));
                if ra != rb {
                    let only_e: Vec<&String> = ra.iter().filter(|x| !rb.contains(x)).collect();
                    let only_m: Vec<&String> = rb.iter().filter(|x| !ra.contains(x)).collect();
                    panic!(
                        "seed {seed} after op {i}: {c}\n  engine only: {only_e:?}\n  memory only: {only_m:?}\n  last: {q}"
                    );
                }
            }
            let bad = check_invariants(&g).unwrap();
            assert!(bad.is_empty(), "seed {seed} after op {i}: {bad:?}");
        }
    }
    eprintln!(
        "model: {shards} shard(s), seed {seed}: {ok} statements equal, {failed} failed on both, {} nodes",
        m.node_count()
    );
    drop(g);
    drop(engine);
    let _ = std::fs::remove_dir_all(&d);
}

fn ops() -> usize {
    std::env::var("SPRIG_ENGINE_MODEL_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_store_equals_memory_on_one_shard() {
    for seed in [0x9E37_79B9_7F4A_7C15, 0x1234_5678_9ABC_DEF1] {
        run_model(1, seed, ops()).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_store_equals_memory_on_three_shards() {
    for seed in [0xDEAD_BEEF_CAFE_F00D, 0x0F0F_1234_4321_ABCD] {
        run_model(3, seed, ops()).await;
    }
}
