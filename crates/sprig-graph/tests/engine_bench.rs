//! Per-hop and per-edge cost of graph reads on three stores, with one
//! generated graph and one set of queries (`SPRIG_GRAPH_BENCH=1`, best in
//! a release build):
//! - **engine:** the engine store over a real engine (RocksDB, commit log)
//!   in this process;
//! - **sql:** the MariaDB store (one SQL statement per store call) over
//!   `SPRIG_TEST_MARIADB_SINGLE`, in this process;
//! - **neo4j:** `SPRIG_TEST_NEO4J_ADDR` / `_PASSWORD` over Bolt.
//!
//! The graph: chains of `NEXT` relationships (out-degree 1), and stars
//! whose centre has `d` `KNOWS` relationships for d in 1, 10, 100, 1,000
//! and 10,000. Every query starts from a node by id.
//! - **Per hop:** `MATCH (a)-[:NEXT*h]->(b) WHERE id(a) = $x RETURN b.i`
//!   for h = 1 to 128; the slope of the median time over h is the cost of
//!   one hop (a seek and one relationship), and the intercept is the
//!   statement's fixed cost (a Bolt round trip for Neo4j).
//! - **Per edge:** `MATCH (s)-[:KNOWS]->(b) WHERE id(s) = $x RETURN
//!   count(b)` (links only) and `... RETURN sum(b.i)` (far nodes read) over
//!   d; the slope over d is the cost of one more relationship.
//!
//! The engine arm also times the store call itself (`links`), without the
//! executor: the seek, and the cost per relationship.
//!
//! `SPRIG_GRAPH_BENCH_REPS` sets repetitions per point (default 31).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_engine::{Engine, EngineConfig, StoreKind};
use sprig_graph::ast::Dir;
use sprig_graph::catalog::{GraphCatalog, Tables};
use sprig_graph::engine::EngineGraph;
use sprig_graph::eval::Params;
use sprig_graph::maria::{CounterIds, Dictionary, GraphShards, MariaStore, OneConn};
use sprig_graph::store::GraphStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::client::BoltClient;
use tokio::net::TcpStream;

/// The graph catalog's records for these tests (in memory: the graph
/// module's rows live in the engine's catalog records).
fn catalog_records() -> &'static sprig_catalog::records::mem::MemRecords {
    static M: std::sync::OnceLock<sprig_catalog::records::mem::MemRecords> =
        std::sync::OnceLock::new();
    M.get_or_init(sprig_catalog::records::mem::MemRecords::new)
}

/// Adds a native label or type to graph `g` in a transaction of its own.
async fn add_source(g: u64, label: bool, name: &str) -> sprig_common::Result<u32> {
    use sprig_catalog::records::RecordStore as _;
    let mut tx = catalog_records()
        .begin(sprig_common::ids::TenantId(1))
        .await?;
    let id = sprig_graph::catalog::GraphCatalog::add_source(&mut *tx, g, label, name).await?;
    tx.commit().await?;
    Ok(id)
}

const DEGREES: &[u64] = &[1, 10, 100, 1000, 10_000];
const HOPS: u64 = 128;
const HOP_POINTS: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 128];
const CHAINS: u64 = 4;
const GRAPH: u64 = 911;

/// The default budgets, with room for the longest chain.
fn budgets() -> Budgets {
    Budgets {
        max_hops: 256,
        ..Budgets::default()
    }
}

fn reps() -> usize {
    std::env::var("SPRIG_GRAPH_BENCH_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(31)
}

/// The statements that build the graph, in batches: chain nodes `L`
/// (chain c, position p), stars `S` with `d` leaves `P`.
fn load_statements() -> Vec<String> {
    let mut v = Vec::new();
    for c in 0..CHAINS {
        let mut q = String::from("CREATE ");
        for p in 0..=HOPS {
            if p > 0 {
                q.push_str("-[:NEXT]->");
            }
            let _ = write!(q, "(:L {{c: {c}, p: {p}, i: {}}})", c * 1000 + p);
        }
        v.push(q);
    }
    for &d in DEGREES {
        v.push(format!("CREATE (:S {{d: {d}, i: {d}}})"));
        let mut start = 0;
        while start < d {
            let end = (start + 2000).min(d);
            v.push(format!(
                "MATCH (s:S {{d: {d}}}) UNWIND range({start}, {}) AS k CREATE (s)-[:KNOWS]->(:P {{i: k}})",
                end - 1
            ));
            start = end;
        }
    }
    v
}

#[async_trait]
trait Arm: Send {
    fn name(&self) -> &'static str;
    async fn run(&mut self, q: &str) -> Result<Vec<Vec<String>>>;
}

fn texts(o: &Output) -> Vec<Vec<String>> {
    o.rows
        .iter()
        .map(|r| r.iter().map(Value::display).collect())
        .collect()
}

struct EngineArm {
    g: Arc<EngineGraph>,
}

#[async_trait]
impl Arm for EngineArm {
    fn name(&self) -> &'static str {
        "engine"
    }
    async fn run(&mut self, q: &str) -> Result<Vec<Vec<String>>> {
        let mut tx = self.g.begin(true);
        let r = {
            let mut s = self.g.store(&mut tx);
            execute(&mut s, q, &Params::new(), &budgets()).await
        };
        match r {
            Ok(o) => {
                tx.commit(&self.g).await?;
                Ok(texts(&o))
            }
            Err(e) => {
                tx.rollback();
                Err(e)
            }
        }
    }
}

struct Dict {
    labels: Mutex<BTreeMap<u32, String>>,
    types: Mutex<BTreeMap<u32, String>>,
}

#[async_trait]
impl Dictionary for Dict {
    async fn create(&self, label: bool, name: &str) -> Result<u32> {
        let id = add_source(GRAPH, label, name).await?;
        let m = if label { &self.labels } else { &self.types };
        m.lock().unwrap().insert(id, name.to_owned());
        Ok(id)
    }
}

struct SqlArm {
    pool: mysql_async::Pool,
    dict: Arc<Dict>,
    ids: Arc<CounterIds>,
    layout: Arc<GraphShards>,
    schemas: Vec<String>,
}

#[async_trait]
impl Arm for SqlArm {
    fn name(&self) -> &'static str {
        "sql"
    }
    async fn run(&mut self, q: &str) -> Result<Vec<Vec<String>>> {
        let be = sprig_graph::catalog::backend_error;
        let mut conn = self.pool.get_conn().await.map_err(be)?;
        conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .map_err(be)?;
        conn.query_drop("START TRANSACTION").await.map_err(be)?;
        let labels = self.dict.labels.lock().unwrap().clone();
        let types = self.dict.types.lock().unwrap().clone();
        let r = {
            let mut one = OneConn(&mut conn);
            let mut store = MariaStore::new(
                &mut one,
                Arc::clone(&self.layout),
                labels,
                types,
                self.dict.clone(),
                self.ids.clone(),
            );
            execute(&mut store, q, &Params::new(), &budgets()).await
        };
        conn.query_drop(if r.is_ok() { "COMMIT" } else { "ROLLBACK" })
            .await
            .map_err(be)?;
        r.map(|o| texts(&o))
    }
}

async fn sql_arm() -> Option<SqlArm> {
    let url = std::env::var("SPRIG_TEST_MARIADB_SINGLE").ok()?;
    let opts = mysql_async::Opts::from_url(&url).unwrap();
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::new()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    );
    let prefix = "mgraph_bench";
    let cat = format!("{prefix}_cat");
    let data = format!("{prefix}_s1");
    let schemas = vec![data.clone(), cat.clone()];
    let mut c = pool.get_conn().await.unwrap();
    for s in &schemas {
        c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {s}")).await.unwrap();
    }
    GraphCatalog::migrate(&mut c, &cat).await.unwrap();
    let layout = GraphShards::single(GRAPH, 1, &data, "c1");
    for sh in &layout.shards {
        for s in sh.tables.shard_ddl() {
            c.query_drop(s).await.unwrap();
        }
    }
    let first = Tables::of(&data, GRAPH);
    for s in first.seq_ddl() {
        c.query_drop(s).await.unwrap();
    }
    drop(c);
    Some(SqlArm {
        dict: Arc::new(Dict {
            labels: Mutex::new(BTreeMap::new()),
            types: Mutex::new(BTreeMap::new()),
        }),
        ids: Arc::new(CounterIds::new(pool.clone(), &first, 1000)),
        pool,
        layout: Arc::new(layout),
        schemas,
    })
}

struct NeoArm {
    c: BoltClient<TcpStream>,
}

fn pack_text(v: &PackValue) -> String {
    match v {
        PackValue::Null => "null".into(),
        PackValue::Int(i) => i.to_string(),
        PackValue::Float(f) => f.to_string(),
        PackValue::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

#[async_trait]
impl Arm for NeoArm {
    fn name(&self) -> &'static str {
        "neo4j"
    }
    async fn run(&mut self, q: &str) -> Result<Vec<Vec<String>>> {
        let r = self
            .c
            .query(q, vec![], vec![])
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        Ok(r.records
            .iter()
            .map(|r| r.iter().map(pack_text).collect())
            .collect())
    }
}

async fn neo_arm() -> Option<NeoArm> {
    let addr = std::env::var("SPRIG_TEST_NEO4J_ADDR").ok()?;
    let pw = std::env::var("SPRIG_TEST_NEO4J_PASSWORD").ok()?;
    let s = TcpStream::connect(&addr).await.ok()?;
    let mut c = BoltClient::handshake(s, None).await.ok()?;
    c.login("neo4j", &pw).await.ok()?;
    c.query("MATCH (n) DETACH DELETE n", vec![], vec![])
        .await
        .ok()?;
    Some(NeoArm { c })
}

fn median(mut v: Vec<Duration>) -> f64 {
    v.sort();
    v[v.len() / 2].as_secs_f64() * 1e6
}

/// Least-squares slope and intercept of `(x, y)`.
fn fit(pts: &[(f64, f64)]) -> (f64, f64) {
    let n = pts.len() as f64;
    let (sx, sy): (f64, f64) = pts.iter().fold((0.0, 0.0), |a, p| (a.0 + p.0, a.1 + p.1));
    let (mx, my) = (sx / n, sy / n);
    let num: f64 = pts.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum();
    let den: f64 = pts.iter().map(|p| (p.0 - mx).powi(2)).sum();
    let slope = num / den.max(f64::MIN_POSITIVE);
    (slope, my - slope * mx)
}

async fn time(arm: &mut dyn Arm, q: &str, n: usize) -> f64 {
    // Warm.
    arm.run(q)
        .await
        .unwrap_or_else(|e| panic!("{} {q}: {e}", arm.name()));
    let mut ts = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        arm.run(q).await.unwrap();
        ts.push(t.elapsed());
    }
    median(ts)
}

async fn id_of(arm: &mut dyn Arm, q: &str) -> i64 {
    arm.run(q).await.unwrap()[0][0].parse().unwrap()
}

/// Loads the graph and measures; one report line per measurement.
async fn measure(arm: &mut dyn Arm, out: &mut String) {
    let t = Instant::now();
    for q in load_statements() {
        arm.run(&q)
            .await
            .unwrap_or_else(|e| panic!("{} load {q}: {e}", arm.name()));
    }
    let _ = writeln!(out, "{}: loaded in {:?}", arm.name(), t.elapsed());
    let n = reps();
    // Per hop: chains from several starts, the median over starts.
    let mut pts = Vec::new();
    let mut line = format!("{} hops (µs):", arm.name());
    for &h in HOP_POINTS {
        let mut per = Vec::new();
        for c in 0..4u64 {
            let x = id_of(arm, &format!("MATCH (a:L {{c: {c}, p: 0}}) RETURN id(a)")).await;
            let q = format!("MATCH (a)-[:NEXT*{h}]->(b) WHERE id(a) = {x} RETURN b.i");
            per.push(time(arm, &q, n).await);
        }
        per.sort_by(f64::total_cmp);
        let m = per[per.len() / 2];
        let _ = write!(line, " h{h}={m:.0}");
        pts.push((h as f64, m));
    }
    let (slope, icpt) = fit(&pts);
    let _ = writeln!(out, "{line}\n  per hop {slope:.1} µs, fixed {icpt:.0} µs");
    for q in [
        "MATCH (a:L {c: 1, p: 7}) RETURN a.i",
        "MATCH (a:L) WHERE a.c = 1 AND a.p = 7 RETURN a.i",
        "MATCH (a:L) RETURN count(a)",
    ] {
        let m = time(arm, q, n).await;
        let _ = writeln!(out, "{} `{q}`: {m:.0} µs", arm.name());
    }
    for (what, ret) in [("links", "count(b)"), ("far nodes", "sum(b.i)")] {
        let mut pts = Vec::new();
        let mut line = format!("{} degree, {what} (µs):", arm.name());
        for &d in DEGREES {
            let x = id_of(arm, &format!("MATCH (s:S {{d: {d}}}) RETURN id(s)")).await;
            let q = format!("MATCH (s)-[:KNOWS]->(b) WHERE id(s) = {x} RETURN {ret}");
            let m = time(arm, &q, n.min(if d >= 10_000 { 9 } else { n })).await;
            let _ = write!(line, " d{d}={m:.0}");
            pts.push((d as f64, m));
        }
        let (slope, _) = fit(&pts);
        let _ = writeln!(out, "{line}\n  per relationship {slope:.3} µs");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_hop_costs() {
    if std::env::var("SPRIG_GRAPH_BENCH").is_err() {
        eprintln!("skipped: SPRIG_GRAPH_BENCH is not set");
        return;
    }
    let mut out = String::new();
    // The engine, in this process.
    let d = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("m-graph-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    let mut cfg = EngineConfig::new(&d);
    cfg.store = StoreKind::Rocks;
    cfg.budget.engine_mb = 512;
    let engine = Engine::open(cfg).unwrap();
    let g = EngineGraph::open(Arc::new(engine.clone()), 5, 9, 1).unwrap();
    let mut e = EngineArm { g: Arc::clone(&g) };
    measure(&mut e, &mut out).await;
    // The store call alone: one `links` per near node.
    {
        let mut tx = g.begin(true);
        let mut s = g.store(&mut tx);
        let knows = s.type_id("KNOWS", false).await.unwrap().unwrap();
        let next = s.type_id("NEXT", false).await.unwrap().unwrap();
        let mut line = String::from("engine store call `links` (µs):");
        let mut pts = Vec::new();
        for &deg in DEGREES {
            let o = execute(
                &mut s,
                &format!("MATCH (s:S {{d: {deg}}}) RETURN id(s)"),
                &Params::new(),
                &Budgets::default(),
            )
            .await
            .unwrap();
            let Value::Int(x) = o.rows[0][0] else {
                panic!()
            };
            let x = u64::try_from(x).unwrap();
            let reps = if deg >= 10_000 { 51 } else { 2001 };
            let mut ts = Vec::new();
            for _ in 0..reps {
                let t = Instant::now();
                let l = s.links(&[x], &[knows], Dir::Out).await.unwrap();
                ts.push(t.elapsed());
                assert_eq!(l.len() as u64, deg);
            }
            let m = median(ts);
            let _ = write!(line, " d{deg}={m:.2}");
            pts.push((deg as f64, m));
        }
        let (slope, icpt) = fit(&pts);
        let _ = writeln!(
            out,
            "{line}\n  seek {icpt:.2} µs, per relationship {slope:.3} µs"
        );
        // Far nodes as store calls: the 10,000 leaves of the biggest star.
        {
            let o = execute(
                &mut s,
                "MATCH (s:S {d: 10000})-[:KNOWS]->(b) RETURN id(b)",
                &Params::new(),
                &Budgets::default(),
            )
            .await
            .unwrap();
            let ids: Vec<u64> = o
                .rows
                .iter()
                .filter_map(|r| match r[0] {
                    Value::Int(i) => u64::try_from(i).ok(),
                    _ => None,
                })
                .collect();
            let keys = ["i".to_owned()];
            for (what, k) in [
                ("labels only", Some(&[][..])),
                ("one key", Some(&keys[..])),
                ("all", None),
            ] {
                let mut ts = Vec::new();
                for _ in 0..21 {
                    let t = Instant::now();
                    let n = s.nodes_part(&ids, k).await.unwrap();
                    ts.push(t.elapsed());
                    assert_eq!(n.len(), ids.len());
                }
                let per = median(ts) / ids.len() as f64;
                let _ = writeln!(
                    out,
                    "engine store call `nodes_part` ({what}): {per:.3} µs per node"
                );
            }
        }
        // A chain hop as a store call: degree 1.
        let o = execute(
            &mut s,
            "MATCH (a:L {c: 0, p: 0}) RETURN id(a)",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .unwrap();
        let Value::Int(x) = o.rows[0][0] else {
            panic!()
        };
        let mut ts = Vec::new();
        for _ in 0..5001 {
            let t = Instant::now();
            let _ = s
                .links(&[u64::try_from(x).unwrap()], &[next], Dir::Out)
                .await
                .unwrap();
            ts.push(t.elapsed());
        }
        let _ = writeln!(
            out,
            "engine store call, one hop of degree 1: {:.2} µs",
            median(ts)
        );
        drop(s);
        tx.rollback();
    }
    if let Some(mut sql) = sql_arm().await {
        measure(&mut sql, &mut out).await;
        let mut c = sql.pool.get_conn().await.unwrap();
        for s in &sql.schemas {
            c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
                .await
                .unwrap();
        }
        drop(c);
        sql.pool.disconnect().await.unwrap();
    } else {
        let _ = writeln!(out, "sql: skipped (SPRIG_TEST_MARIADB_SINGLE not set)");
    }
    if let Some(mut neo) = neo_arm().await {
        measure(&mut neo, &mut out).await;
        let _ = neo.run("MATCH (n) DETACH DELETE n").await;
    } else {
        let _ = writeln!(
            out,
            "neo4j: skipped (SPRIG_TEST_NEO4J_ADDR not set or unreachable)"
        );
    }
    eprintln!("{out}");
    if let Ok(p) = std::env::var("SPRIG_GRAPH_BENCH_OUT") {
        std::fs::write(p, &out).unwrap();
    }
    drop(g);
    drop(engine);
    let _ = std::fs::remove_dir_all(&d);
}

/// A Bolt server as an arm: the gateway (graph `db`) or Neo4j (`db` none).
struct BoltArm {
    name: &'static str,
    c: BoltClient<TcpStream>,
    db: Option<String>,
}

#[async_trait]
impl Arm for BoltArm {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn run(&mut self, q: &str) -> Result<Vec<Vec<String>>> {
        let extra = match &self.db {
            Some(d) => vec![("db", PackValue::String(d.clone()))],
            None => vec![],
        };
        let r = self
            .c
            .query(q, vec![], extra)
            .await
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        Ok(r.records
            .iter()
            .map(|r| r.iter().map(pack_text).collect())
            .collect())
    }
}

async fn bolt(addr: &str, user: &str, pw: &str) -> BoltClient<TcpStream> {
    let s = TcpStream::connect(addr).await.unwrap();
    let mut c = BoltClient::handshake(s, None).await.unwrap();
    c.login(user, pw).await.unwrap();
    c
}

/// Statement latency end to end over Bolt (`SPRIG_GRAPH_BOLT_BENCH=<label>`):
/// the gateway at `SPRIG_TEST_BOLT_ADDR` on graph `SPRIG_GRAPH_BOLT_DB`
/// (engine or MariaDB, as the gateway was started), and Neo4j when
/// `SPRIG_TEST_NEO4J_ADDR` is set. Medians of `SPRIG_GRAPH_BENCH_REPS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bolt_statement_latency() {
    let Ok(label) = std::env::var("SPRIG_GRAPH_BOLT_BENCH") else {
        eprintln!("skipped: SPRIG_GRAPH_BOLT_BENCH is not set");
        return;
    };
    let v = |k: &str| std::env::var(k).unwrap();
    let mut arms: Vec<BoltArm> = vec![BoltArm {
        name: "gateway",
        c: bolt(
            &v("SPRIG_TEST_BOLT_ADDR"),
            &v("SPRIG_TEST_BOLT_USER"),
            &v("SPRIG_TEST_BOLT_PASSWORD"),
        )
        .await,
        db: Some(v("SPRIG_GRAPH_BOLT_DB")),
    }];
    if let Ok(addr) = std::env::var("SPRIG_TEST_NEO4J_ADDR")
        && !addr.is_empty()
    {
        arms.push(BoltArm {
            name: "neo4j",
            c: bolt(&addr, "neo4j", &v("SPRIG_TEST_NEO4J_PASSWORD")).await,
            db: None,
        });
    }
    let mut out = String::new();
    let n = reps();
    for arm in &mut arms {
        arm.run("MATCH (n) DETACH DELETE n").await.unwrap();
        let t = Instant::now();
        for q in load_statements() {
            arm.run(&q)
                .await
                .unwrap_or_else(|e| panic!("{} load: {e}", arm.name));
        }
        let _ = writeln!(out, "{label} {}: loaded in {:?}", arm.name, t.elapsed());
        let chain = id_of(arm, "MATCH (a:L {c: 0, p: 0}) RETURN id(a)").await;
        let s10 = id_of(arm, "MATCH (s:S {d: 10}) RETURN id(s)").await;
        let s100 = id_of(arm, "MATCH (s:S {d: 100}) RETURN id(s)").await;
        let s1000 = id_of(arm, "MATCH (s:S {d: 1000}) RETURN id(s)").await;
        let qs: Vec<(&str, String)> = vec![
            ("RETURN 1", "RETURN 1".into()),
            (
                "node by id",
                format!("MATCH (a) WHERE id(a) = {chain} RETURN a.i"),
            ),
            (
                "1 hop, 10 rels",
                format!("MATCH (s)-[:KNOWS]->(b) WHERE id(s) = {s10} RETURN b.i"),
            ),
            (
                "1 hop, 100 rels, sum",
                format!("MATCH (s)-[:KNOWS]->(b) WHERE id(s) = {s100} RETURN sum(b.i)"),
            ),
            (
                "1 hop, 1000 rels, count",
                format!("MATCH (s)-[:KNOWS]->(b) WHERE id(s) = {s1000} RETURN count(b)"),
            ),
            (
                "12 hops",
                format!("MATCH (a)-[:NEXT*12]->(b) WHERE id(a) = {chain} RETURN b.i"),
            ),
            (
                "label + property",
                "MATCH (a:L {c: 1, p: 7}) RETURN a.i".into(),
            ),
        ];
        for (what, q) in qs {
            let m = time(arm, &q, n).await;
            let _ = writeln!(out, "{label} {}: {what}: {m:.0} µs", arm.name);
        }
        arm.run("MATCH (n) DETACH DELETE n").await.unwrap();
    }
    eprintln!("{out}");
    if let Ok(p) = std::env::var("SPRIG_GRAPH_BENCH_OUT") {
        let mut prev = std::fs::read_to_string(&p).unwrap_or_default();
        prev.push_str(&out);
        std::fs::write(p, prev).unwrap();
    }
}
