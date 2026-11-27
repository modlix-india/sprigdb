//! The pattern planner, range indexes and statistics on a generated
//! social network (the shape of `ldbc_live.rs`'s: persons in cities,
//! skewed `KNOWS`, posts and comments with creators and tags, `LIKES`),
//! stored in the storage engine on RocksDB in this process.
//!
//! - `planner_answers_equal`: every query, with several parameter sets,
//!   gives the same rows with the planner on, with it off (the fixed
//!   rules), and on the in-memory store; ordered queries row for row,
//!   others as multisets. With `SPRIG_TEST_NEO4J_ADDR` and
//!   `SPRIG_TEST_NEO4J_PASSWORD` (user `neo4j`), the same data is loaded
//!   into that reference Neo4j and its answers are compared too.
//! - `planner_speed` (ignored; run it in a release build): medians of 31
//!   runs per query, planner off against on. `SPRIG_TEST_PLAN_PERSONS` sets
//!   the scale (default 10,000 persons).

#![allow(
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines,
    clippy::similar_names
)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_engine::{Engine, EngineConfig};
use sprig_graph::engine::EngineGraph;
use sprig_graph::eval::Params;
use sprig_graph::exec::set_planner;
use sprig_graph::mem::MemStore;
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::client::BoltClient;
use tokio::net::TcpStream;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

/// A small deterministic generator (xorshift64*), so runs repeat.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Skewed towards small values: about a power law.
    fn skewed(&mut self, n: u64) -> u64 {
        let a = self.below(n);
        let b = self.below(n);
        a.min(b).min(self.below(n))
    }
}

/// A parameter value both servers take.
#[derive(Clone)]
enum V {
    I(i64),
    S(String),
    L(Vec<V>),
    M(Vec<(&'static str, V)>),
}

impl V {
    fn pack(&self) -> PackValue {
        match self {
            V::I(i) => PackValue::Int(*i),
            V::S(s) => PackValue::String(s.clone()),
            V::L(l) => PackValue::List(l.iter().map(V::pack).collect()),
            V::M(m) => PackValue::map(m.iter().map(|(k, v)| (*k, v.pack()))),
        }
    }
    fn val(&self) -> Value {
        match self {
            V::I(i) => Value::Int(*i),
            V::S(s) => Value::String(s.clone()),
            V::L(l) => Value::List(l.iter().map(V::val).collect()),
            V::M(m) => Value::Map(
                m.iter()
                    .map(|(k, v)| ((*k).to_owned(), v.val()))
                    .collect::<BTreeMap<_, _>>(),
            ),
        }
    }
}

fn pack_text(v: &PackValue) -> String {
    match v {
        PackValue::Null => "null".into(),
        PackValue::Bool(b) => b.to_string(),
        PackValue::Int(i) => i.to_string(),
        PackValue::Float(f) => format!("{f:?}"),
        PackValue::String(s) => s.clone(),
        PackValue::List(l) => format!(
            "[{}]",
            l.iter().map(pack_text).collect::<Vec<_>>().join(",")
        ),
        other => format!("{other:?}"),
    }
}

fn val_text(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::String(s) => s.clone(),
        Value::List(l) => format!("[{}]", l.iter().map(val_text).collect::<Vec<_>>().join(",")),
        other => other.display(),
    }
}

const FIRST: &[&str] = &[
    "Ana", "Bo", "Chen", "Dara", "Eli", "Fay", "Gus", "Hana", "Ivo", "Jun", "Kai", "Lea",
];
const LAST: &[&str] = &[
    "Ito", "Khan", "Lopez", "Meyer", "Novak", "Okafor", "Park", "Rossi",
];

/// The data: statements with batched rows.
struct Data {
    persons: i64,
    posts: i64,
    comments: i64,
    batches: Vec<(&'static str, Vec<V>)>,
    elements: usize,
}

fn generate(persons: i64, seed: u64) -> Data {
    let mut r = Rng(seed);
    let cities = 20;
    let tags = 40;
    let mut batches: Vec<(&'static str, Vec<V>)> = Vec::new();
    let mut elements = 0;
    let mut push = |q: &'static str, rows: Vec<V>, batches: &mut Vec<(&'static str, Vec<V>)>| {
        elements += rows.len();
        for c in rows.chunks(250) {
            batches.push((q, c.to_vec()));
        }
    };
    push(
        "UNWIND $rows AS r CREATE (:City {id: r.id, name: r.name})",
        (0..cities)
            .map(|i| V::M(vec![("id", V::I(i)), ("name", V::S(format!("city{i}")))]))
            .collect(),
        &mut batches,
    );
    push(
        "UNWIND $rows AS r CREATE (:Tag {id: r.id, name: r.name})",
        (0..tags)
            .map(|i| V::M(vec![("id", V::I(i)), ("name", V::S(format!("tag{i}")))]))
            .collect(),
        &mut batches,
    );
    let mut rows = Vec::new();
    for i in 0..persons {
        let u = u64::try_from(i).unwrap_or(0);
        rows.push(V::M(vec![
            ("id", V::I(i)),
            (
                "first",
                V::S(FIRST[(r.next() % FIRST.len() as u64) as usize].into()),
            ),
            ("last", V::S(LAST[(u % LAST.len() as u64) as usize].into())),
            (
                "born",
                V::I(1_970 + i64::try_from(r.below(35)).unwrap_or(0)),
            ),
            (
                "created",
                V::I(1_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
            ),
            ("city", V::I(i64::try_from(r.below(20)).unwrap_or(0))),
        ]));
    }
    push(
        "UNWIND $rows AS r CREATE (:Person {id: r.id, firstName: r.first, lastName: r.last, birthYear: r.born, creationDate: r.created, browserUsed: 'Firefox', locationIP: '10.0.0.' + toString(r.id % 250), cityId: r.city})",
        rows,
        &mut batches,
    );
    let pn = u64::try_from(persons).unwrap_or(1);
    // KNOWS: about 10 per person on average, skewed, no self loops, each
    // pair once.
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    for a in 0..pn {
        let k = 1 + r.skewed(30);
        for _ in 0..k {
            let b = r.below(pn);
            let (x, y) = (a.min(b), a.max(b));
            if x == y || !seen.insert((x, y)) {
                continue;
            }
            rows.push(V::M(vec![
                ("a", V::I(i64::try_from(x).unwrap_or(0))),
                ("b", V::I(i64::try_from(y).unwrap_or(0))),
                (
                    "d",
                    V::I(2_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
                ),
            ]));
        }
    }
    let knows = rows.len();
    push(
        "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (b:Person {id: r.b}) CREATE (a)-[:KNOWS {creationDate: r.d}]->(b)",
        rows,
        &mut batches,
    );
    push(
        "UNWIND $rows AS r MATCH (p:Person {id: r.p}), (c:City {id: r.c}) CREATE (p)-[:IS_LOCATED_IN]->(c)",
        (0..persons)
            .map(|i| {
                V::M(vec![
                    ("p", V::I(i)),
                    ("c", V::I(i64::try_from(r.below(20)).unwrap_or(0))),
                ])
            })
            .collect(),
        &mut batches,
    );
    let posts = persons * 3;
    let mut rows = Vec::new();
    for i in 0..posts {
        rows.push(V::M(vec![
            ("id", V::I(i)),
            ("creator", V::I(i64::try_from(r.skewed(pn)).unwrap_or(0))),
            (
                "created",
                V::I(3_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
            ),
            ("tag", V::I(i64::try_from(r.below(40)).unwrap_or(0))),
        ]));
    }
    push(
        "UNWIND $rows AS r MATCH (c:Person {id: r.creator}), (t:Tag {id: r.tag}) CREATE (m:Post {id: r.id, content: 'post ' + toString(r.id), creationDate: r.created, length: r.id % 140})-[:HAS_CREATOR]->(c), (m)-[:HAS_TAG]->(t)",
        rows,
        &mut batches,
    );
    let comments = persons * 2;
    let pp = u64::try_from(posts).unwrap_or(1);
    let mut rows = Vec::new();
    for i in 0..comments {
        rows.push(V::M(vec![
            ("id", V::I(1_000_000 + i)),
            ("creator", V::I(i64::try_from(r.below(pn)).unwrap_or(0))),
            ("post", V::I(i64::try_from(r.skewed(pp)).unwrap_or(0))),
            (
                "created",
                V::I(4_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
            ),
        ]));
    }
    push(
        "UNWIND $rows AS r MATCH (c:Person {id: r.creator}), (p:Post {id: r.post}) CREATE (m:Comment {id: r.id, content: 'comment ' + toString(r.id), creationDate: r.created})-[:HAS_CREATOR]->(c), (m)-[:REPLY_OF]->(p)",
        rows,
        &mut batches,
    );
    let mut rows = Vec::new();
    for a in 0..pn {
        for _ in 0..r.skewed(12) {
            rows.push(V::M(vec![
                ("a", V::I(i64::try_from(a).unwrap_or(0))),
                ("p", V::I(i64::try_from(r.below(pp)).unwrap_or(0))),
                (
                    "d",
                    V::I(5_000_000 + i64::try_from(r.below(1_000_000)).unwrap_or(0)),
                ),
            ]));
        }
    }
    push(
        "UNWIND $rows AS r MATCH (a:Person {id: r.a}), (p:Post {id: r.p}) CREATE (a)-[:LIKES {creationDate: r.d}]->(p)",
        rows,
        &mut batches,
    );
    eprintln!(
        "ldbc-shaped data: {persons} persons, {knows} KNOWS, {posts} posts, {comments} comments"
    );
    Data {
        persons,
        posts,
        comments,
        batches,
        elements,
    }
}

/// LDBC-like reads (from `ldbc_live.rs`).
const READS: &[(&str, &str, &[&str])] = &[
    (
        "IS1",
        "MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(p:City) RETURN n.firstName, n.lastName, n.birthYear, n.locationIP, n.browserUsed, p.id AS cityId, n.creationDate",
        &["personId"],
    ),
    (
        "IS2",
        "MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(m) WITH m ORDER BY m.creationDate DESC, m.id DESC LIMIT 10 MATCH (m)-[:REPLY_OF*0..1]->(p:Post)-[:HAS_CREATOR]->(c:Person) RETURN m.id, m.content, m.creationDate, p.id, c.id, c.firstName, c.lastName ORDER BY m.creationDate DESC, m.id DESC",
        &["personId"],
    ),
    (
        "IS3",
        "MATCH (n:Person {id: $personId})-[r:KNOWS]-(friend) RETURN friend.id, friend.firstName, friend.lastName, r.creationDate ORDER BY r.creationDate DESC, friend.id",
        &["personId"],
    ),
    (
        "IS4",
        "MATCH (m:Post {id: $messageId}) RETURN m.creationDate, m.content",
        &["messageId"],
    ),
    (
        "IS5",
        "MATCH (m:Post {id: $messageId})-[:HAS_CREATOR]->(p:Person) RETURN p.id, p.firstName, p.lastName",
        &["messageId"],
    ),
    (
        "IS6",
        "MATCH (m:Comment {id: $commentId})-[:REPLY_OF]->(p:Post)-[:HAS_CREATOR]->(mod:Person) RETURN p.id, mod.id, mod.firstName, mod.lastName",
        &["commentId"],
    ),
    (
        "IS7",
        "MATCH (m:Post {id: $messageId})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) RETURN c.id, c.content, c.creationDate, p.id, p.firstName, p.lastName, r IS NOT NULL AS knows ORDER BY c.creationDate DESC, p.id",
        &["messageId"],
    ),
    (
        "IC1",
        "MATCH (p:Person {id: $personId}), (f:Person {firstName: $firstName}) WHERE p <> f MATCH path = shortestPath((p)-[:KNOWS*1..3]-(f)) WITH f, length(path) AS d ORDER BY d, f.lastName, f.id LIMIT 20 MATCH (f)-[:IS_LOCATED_IN]->(c:City) RETURN f.id, f.lastName, d, c.name ORDER BY d, f.lastName, f.id",
        &["personId", "firstName"],
    ),
    (
        "IC2",
        "MATCH (:Person {id: $personId})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate <= $maxDate RETURN f.id, f.firstName, f.lastName, m.id, m.content, m.creationDate ORDER BY m.creationDate DESC, m.id LIMIT 20",
        &["personId", "maxDate"],
    ),
    (
        "IC9",
        "MATCH (:Person {id: $personId})-[:KNOWS*1..2]-(f:Person) WHERE f.id <> $personId WITH DISTINCT f MATCH (f)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate < $maxDate RETURN f.id, f.firstName, m.id, m.creationDate ORDER BY m.creationDate DESC, m.id LIMIT 20",
        &["personId", "maxDate"],
    ),
    (
        "IC13",
        "MATCH (a:Person {id: $personId}), (b:Person {id: $person2Id}) OPTIONAL MATCH path = shortestPath((a)-[:KNOWS*]-(b)) RETURN CASE WHEN path IS NULL THEN -1 ELSE length(path) END AS len",
        &["personId", "person2Id"],
    ),
];

/// The planner's own queries: selective nodes at the end or in the
/// middle of a pattern, comma patterns written in a poor order, ranges,
/// prefixes and top-k orders served from range indexes.
const PLANNED: &[(&str, &str, &[&str])] = &[
    (
        "P1 end",
        "MATCH (t:Tag {name: $tagName})<-[:HAS_TAG]-(m:Post)-[:HAS_CREATOR]->(p:Person {id: $personId}) RETURN m.id, t.name",
        &["tagName", "personId"],
    ),
    (
        "P2 end",
        "MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person)-[:KNOWS]->(f:Person {id: $personId}) RETURN p.id, c.name",
        &["personId"],
    ),
    (
        "P3 middle",
        "MATCH (a:Person)-[:LIKES]->(m:Post {id: $messageId})-[:HAS_TAG]->(t:Tag) RETURN a.id, t.name",
        &["messageId"],
    ),
    (
        "P4 order",
        "MATCH (p:Person)-[:IS_LOCATED_IN]->(c:City), (f:Person {id: $personId})-[:KNOWS]-(p) RETURN p.id, c.id",
        &["personId"],
    ),
    (
        "P5 range",
        "MATCH (p:Person) WHERE p.creationDate >= $lo AND p.creationDate < $hi RETURN count(p)",
        &["lo", "hi"],
    ),
    (
        "P6 top",
        "MATCH (m:Post) WHERE m.creationDate > $lo RETURN m.id, m.creationDate ORDER BY m.creationDate DESC, m.id LIMIT 20",
        &["lo"],
    ),
    (
        "P7 prefix",
        "MATCH (p:Person) WHERE p.lastName STARTS WITH $prefix AND p.birthYear > 1990 RETURN count(*)",
        &["prefix"],
    ),
    (
        "P8 top asc",
        "MATCH (m:Post) RETURN m.id ORDER BY m.creationDate, m.id LIMIT 10",
        &[],
    ),
    (
        "P9 fof",
        "MATCH (a:Person {id: $personId})-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person) WHERE c.firstName = $firstName AND c <> a RETURN count(DISTINCT c)",
        &["personId", "firstName"],
    ),
    (
        "P10 range join",
        "MATCH (p:Person)<-[:HAS_CREATOR]-(m:Post) WHERE m.creationDate >= $lo AND m.creationDate < $hi2 RETURN p.id, m.id",
        &["lo", "hi2"],
    ),
];

/// The indexes both stores (and Neo4j) get.
const INDEXES: &[(&str, &str)] = &[
    ("Person", "id"),
    ("Post", "id"),
    ("Comment", "id"),
    ("City", "id"),
    ("Tag", "id"),
    ("Person", "firstName"),
    ("Person", "lastName"),
    ("Person", "creationDate"),
    ("Post", "creationDate"),
    ("Tag", "name"),
];

fn param(name: &str, r: &mut Rng, d: &Data) -> V {
    let below =
        |r: &mut Rng, n: i64| i64::try_from(r.below(u64::try_from(n).unwrap_or(1))).unwrap_or(0);
    match name {
        "personId" | "person2Id" => V::I(below(r, d.persons)),
        "messageId" => V::I(below(r, d.posts)),
        "commentId" => V::I(1_000_000 + below(r, d.comments)),
        "firstName" => V::S(FIRST[(r.next() % FIRST.len() as u64) as usize].into()),
        "maxDate" | "hi2" => V::I(3_000_000 + below(r, 1_000_000)),
        "tagName" => V::S(format!("tag{}", below(r, 40))),
        "lo" => V::I(1_000_000 + below(r, 1_000_000)),
        // A narrow window above `lo`'s range (persons) or inside the posts'.
        "hi" => V::I(1_000_000 + below(r, 1_000_000) + 20_000),
        "prefix" => V::S(LAST[(r.next() % LAST.len() as u64) as usize][..2].into()),
        _ => V::I(0),
    }
}

/// Some parameter values are tied together: `hi` above `lo`, and the
/// posts' window `[lo, hi2)` narrow.
fn params(names: &[&str], r: &mut Rng, d: &Data) -> Vec<(&'static str, V)> {
    let mut out: Vec<(&'static str, V)> = Vec::new();
    for n in names {
        let v = param(n, r, d);
        let n: &'static str = Box::leak((*n).to_owned().into_boxed_str());
        out.push((n, v));
    }
    let lo = out
        .iter()
        .find(|(k, _)| *k == "lo")
        .and_then(|(_, v)| match v {
            V::I(i) => Some(*i),
            _ => None,
        });
    if let Some(lo) = lo {
        for (k, v) in &mut out {
            if *k == "hi" {
                *v = V::I(lo + 15_000);
            }
            if *k == "hi2" {
                *v = V::I(3_000_000 + (lo - 1_000_000) + 5_000);
            }
        }
        if names.contains(&"hi2") {
            for (k, v) in &mut out {
                if *k == "lo" {
                    *v = V::I(3_000_000 + (lo - 1_000_000));
                }
            }
        }
        if names == ["lo"] {
            // P6: posts after a late date.
            for (k, v) in &mut out {
                if *k == "lo" {
                    *v = V::I(3_900_000 + (lo % 90_000));
                }
            }
        }
    }
    out
}

fn mparams(ps: &[(&str, V)]) -> Params {
    let mut p = Params::new();
    for (k, v) in ps {
        p.insert((*k).to_owned(), v.val());
    }
    p
}

/// A statement's rows as text: in order when the statement's last
/// projection orders them, else sorted.
fn rows_of(q: &str, rows: Vec<String>) -> Vec<String> {
    let last = q.rsplit("RETURN").next().unwrap_or("");
    let mut rows = rows;
    if !last.contains("ORDER BY") {
        rows.sort();
    }
    rows
}

fn out_rows(q: &str, o: &Output) -> Vec<String> {
    rows_of(
        q,
        o.rows
            .iter()
            .map(|r| r.iter().map(val_text).collect::<Vec<_>>().join("|"))
            .collect(),
    )
}

struct Fixture {
    g: Arc<EngineGraph>,
    mem: MemStore,
    data: Data,
    _dir: PathBuf,
}

async fn on_engine(g: &Arc<EngineGraph>, q: &str, p: &Params) -> sprig_common::Result<Output> {
    let mut tx = g.begin(true);
    let r = {
        let mut s = g.store(&mut tx);
        execute(&mut s, q, p, &Budgets::default()).await
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

async fn fixture(persons: i64, with_mem: bool) -> Fixture {
    let base =
        std::env::var("CARGO_TARGET_TMPDIR").map_or_else(|_| std::env::temp_dir(), PathBuf::from);
    let dir = base.join(format!("graph-planner-{}-{persons}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("dir");
    let mut cfg = EngineConfig::new(&dir);
    cfg.budget.engine_mb = 256;
    let engine = Engine::open(cfg).expect("engine");
    let g = EngineGraph::open(Arc::new(engine), 7, 1, 1).expect("graph");
    let mut mem = MemStore::new(1);
    for (l, p) in INDEXES {
        let q = format!("CREATE INDEX FOR (n:{l}) ON (n.{p})");
        on_engine(&g, &q, &Params::new()).await.expect("index");
        if with_mem {
            execute(&mut mem, &q, &Params::new(), &Budgets::default())
                .await
                .expect("mem index");
        }
    }
    let data = generate(persons, 0x5EED_1DBC);
    let t0 = Instant::now();
    for (q, rows) in &data.batches {
        let mut p = Params::new();
        p.insert("rows".into(), V::L(rows.clone()).val());
        on_engine(&g, q, &p)
            .await
            .unwrap_or_else(|e| panic!("load {q}: {e}"));
        if with_mem {
            execute(&mut mem, q, &p, &Budgets::default())
                .await
                .unwrap_or_else(|e| panic!("mem load {q}: {e}"));
        }
    }
    eprintln!(
        "loaded {} elements in {:.1} s",
        data.elements,
        t0.elapsed().as_secs_f64()
    );
    let t0 = Instant::now();
    g.analyze().expect("analyze");
    eprintln!(
        "statistics pass: {:.0} ms",
        t0.elapsed().as_secs_f64() * 1000.0
    );
    Fixture {
        g,
        mem,
        data,
        _dir: dir,
    }
}

async fn neo4j(data: &Data) -> Option<BoltClient<TcpStream>> {
    let (Some(addr), Some(pw)) = (
        env("SPRIG_TEST_NEO4J_ADDR"),
        env("SPRIG_TEST_NEO4J_PASSWORD"),
    ) else {
        eprintln!("Neo4j differential skipped: SPRIG_TEST_NEO4J_ADDR / _PASSWORD not set");
        return None;
    };
    let s = TcpStream::connect(&addr).await.expect("connect neo4j");
    let mut c = BoltClient::handshake(s, None).await.expect("handshake");
    c.login("neo4j", &pw).await.expect("login");
    c.query(
        "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS",
        vec![],
        vec![],
    )
    .await
    .expect("clean");
    for (l, p) in INDEXES {
        c.query(
            &format!("CREATE INDEX IF NOT EXISTS FOR (n:{l}) ON (n.{p})"),
            vec![],
            vec![],
        )
        .await
        .expect("index");
    }
    c.query("CALL db.awaitIndexes(300)", vec![], vec![])
        .await
        .expect("await");
    for (q, rows) in &data.batches {
        c.query(q, vec![("rows", V::L(rows.clone()).pack())], vec![])
            .await
            .unwrap_or_else(|e| panic!("neo4j load {q}: {e}"));
    }
    Some(c)
}

fn all_queries() -> Vec<(&'static str, &'static str, &'static [&'static str])> {
    PLANNED.iter().chain(READS.iter()).copied().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn planner_answers_equal() {
    let persons: i64 = env("SPRIG_TEST_PLAN_CHECK_PERSONS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    let mut f = fixture(persons, true).await;
    let mut neo = neo4j(&f.data).await;
    let st = f.g.stats().expect("statistics");
    assert!(st.sets.values().sum::<f64>() > 0.0);
    let mut r = Rng(7);
    let mut checked = 0;
    let mut neo_checked = 0;
    let mut bad = Vec::new();
    for (name, q, names) in all_queries() {
        for _ in 0..6 {
            let ps = params(names, &mut r, &f.data);
            let mp = mparams(&ps);
            set_planner(false);
            let off = on_engine(&f.g, q, &mp)
                .await
                .unwrap_or_else(|e| panic!("{name} off: {e}"));
            set_planner(true);
            let on = on_engine(&f.g, q, &mp)
                .await
                .unwrap_or_else(|e| panic!("{name} on: {e}"));
            let mem = execute(&mut f.mem, q, &mp, &Budgets::default())
                .await
                .unwrap_or_else(|e| panic!("{name} mem: {e}"));
            let (a, b, c) = (out_rows(q, &on), out_rows(q, &off), out_rows(q, &mem));
            if a != b || a != c {
                bad.push(format!(
                    "{name} {mp:?}\n  on:  {a:?}\n  off: {b:?}\n  mem: {c:?}"
                ));
            }
            if let Some(n) = neo.as_mut() {
                let got = n
                    .query(q, ps.iter().map(|(k, v)| (*k, v.pack())).collect(), vec![])
                    .await
                    .unwrap_or_else(|e| panic!("{name} neo4j: {e}"));
                let nr = rows_of(
                    q,
                    got.records
                        .iter()
                        .map(|r| r.iter().map(pack_text).collect::<Vec<_>>().join("|"))
                        .collect(),
                );
                if nr != a {
                    bad.push(format!(
                        "{name} {mp:?} NEO4J\n  on:    {a:?}\n  neo4j: {nr:?}"
                    ));
                }
                neo_checked += 1;
            }
            checked += 1;
        }
    }
    // What the planner chose, from PROFILE.
    set_planner(true);
    let mut planned = 0;
    for (name, q, names) in PLANNED {
        let mp = mparams(&params(names, &mut r, &f.data));
        let o = on_engine(&f.g, &format!("PROFILE {q}"), &mp)
            .await
            .unwrap_or_else(|e| panic!("{name} profile: {e}"));
        let lines: Vec<&String> = o
            .plan
            .iter()
            .flatten()
            .filter(|l| l.starts_with("Planner") || l.starts_with("NodeScan"))
            .collect();
        if lines.iter().any(|l| l.starts_with("Planner")) {
            planned += 1;
        }
        eprintln!("{name}: {lines:?}");
    }
    assert!(planned >= 4, "the planner chose for {planned} queries");
    eprintln!(
        "planner answers: {checked} statements compared (on / off / in memory), {neo_checked} against Neo4j, {} different",
        bad.len()
    );
    for b in &bad {
        eprintln!("DIFFERENT {b}");
    }
    assert!(bad.is_empty());
}

fn median(v: &mut [Duration]) -> Duration {
    v.sort();
    v[v.len() / 2]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a benchmark: run in a release build"]
async fn planner_speed() {
    let persons: i64 = env("SPRIG_TEST_PLAN_PERSONS")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    let only = env("SPRIG_TEST_PLAN_ONLY");
    let f = fixture(persons, false).await;
    let st = f.g.stats().expect("statistics");
    eprintln!(
        "graph: {:.0} nodes, {:.0} relationships",
        st.sets.values().sum::<f64>(),
        st.triples.values().sum::<f64>()
    );
    let mut r = Rng(11);
    eprintln!("| query | planner off p50 | planner on p50 | ratio | rows |");
    eprintln!("|---|---|---|---|---|");
    for (name, q, names) in all_queries() {
        if only.as_ref().is_some_and(|o| !name.starts_with(o.as_str())) {
            continue;
        }
        let sets: Vec<Params> = (0..31)
            .map(|_| mparams(&params(names, &mut r, &f.data)))
            .collect();
        let mut t_off = Vec::new();
        let mut t_on = Vec::new();
        let mut rows = 0;
        // One warm-up each, then alternate.
        for (i, p) in sets.iter().enumerate() {
            for on in [false, true] {
                set_planner(on);
                let t = Instant::now();
                let o = on_engine(&f.g, q, p)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                let d = t.elapsed();
                if i > 0 || sets.len() == 1 {
                    if on {
                        t_on.push(d);
                        rows += o.rows.len();
                    } else {
                        t_off.push(d);
                    }
                }
            }
        }
        let (a, b) = (median(&mut t_off), median(&mut t_on));
        eprintln!(
            "| {name} | {:.3} ms | {:.3} ms | {:.1}x | {:.1} |",
            a.as_secs_f64() * 1000.0,
            b.as_secs_f64() * 1000.0,
            a.as_secs_f64() / b.as_secs_f64().max(1e-9),
            rows as f64 / t_on.len().max(1) as f64
        );
    }
    // Far-node reads, one by one against batched (planner on).
    set_planner(true);
    let far: &[(&str, &str, &[&str])] = &[
        (
            "F1 tag posts",
            "MATCH (t:Tag {name: $tagName})<-[:HAS_TAG]-(m:Post) RETURN sum(m.length)",
            &["tagName"],
        ),
        (
            "F2 friends",
            "MATCH (p:Person {id: $personId})-[:KNOWS]-(f:Person) RETURN count(f.firstName)",
            &["personId"],
        ),
        (
            "F3 2-hop",
            "MATCH (p:Person {id: $personId})-[:KNOWS]-(f:Person)<-[:HAS_CREATOR]-(m:Post) RETURN max(m.creationDate)",
            &["personId"],
        ),
    ];
    eprintln!("| query | one by one p50 | batched p50 | ratio | rows |");
    eprintln!("|---|---|---|---|---|");
    for (name, q, names) in far {
        let sets: Vec<Params> = (0..31)
            .map(|_| mparams(&params(names, &mut r, &f.data)))
            .collect();
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for (i, p) in sets.iter().enumerate() {
            for on in [false, true] {
                sprig_graph::engine::set_batched_reads(on);
                let t = Instant::now();
                on_engine(&f.g, q, p)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {e}"));
                let d = t.elapsed();
                if i > 0 {
                    if on { b.push(d) } else { a.push(d) }
                }
            }
        }
        let (x, y) = (median(&mut a), median(&mut b));
        eprintln!(
            "| {name} | {:.3} ms | {:.3} ms | {:.2}x | 1 |",
            x.as_secs_f64() * 1000.0,
            y.as_secs_f64() * 1000.0,
            x.as_secs_f64() / y.as_secs_f64().max(1e-9)
        );
    }
    sprig_graph::engine::set_batched_reads(true);
    let _ = BTreeMap::<u8, u8>::new();
    let _ = Value::Null;
}
