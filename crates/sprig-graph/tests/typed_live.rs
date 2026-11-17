//! Live tests of typed storage ([`sprig_graph::typed`]): the same
//! statements on typed tables and in memory give the same answers, on one
//! shard schema and on four with mirrors; label moves, values of two types
//! on one key, lists, keys added while a transaction holds a table. Skipped
//! unless `SPRIG_TEST_MARIADB_SINGLE` names a server; uses only the
//! schemas `graphstore_typed_*`, dropped at the end.

#![allow(clippy::too_many_lines)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use sprig_common::Result;
use sprig_graph::catalog::{GraphCatalog, Tables, backend_error};
use sprig_graph::eval::Params;
use sprig_graph::maria::{CounterIds, Dictionary, GraphShards, OneConn, ShardLoc};
use sprig_graph::mem::MemStore;
use sprig_graph::store::GraphStore;
use sprig_graph::typed::catalog::{DdlConns, SqlDdl, load};
use sprig_graph::typed::store::{TypedStore, check_deleted};
use sprig_graph::value::Value;
use sprig_graph::{Budgets, Output, execute};

const GRAPH: u64 = 902;

struct Dict {
    cat: String,
    pool: mysql_async::Pool,
    labels: Mutex<BTreeMap<u32, String>>,
    types: Mutex<BTreeMap<u32, String>>,
}

#[async_trait]
impl Dictionary for Dict {
    async fn create(&self, label: bool, name: &str) -> Result<u32> {
        let mut c = self.pool.get_conn().await.map_err(backend_error)?;
        let id = GraphCatalog::new(&self.cat)
            .add_source(&mut c, GRAPH, label, name)
            .await?;
        let m = if label { &self.labels } else { &self.types };
        m.lock().expect("lock").insert(id, name.to_owned());
        Ok(id)
    }
}

struct Conns(mysql_async::Pool);

#[async_trait]
impl DdlConns for Conns {
    async fn catalog(&self) -> Result<mysql_async::Conn> {
        self.0.get_conn().await.map_err(backend_error)
    }
    async fn cluster(&self, _cluster: &str) -> Result<mysql_async::Conn> {
        self.0.get_conn().await.map_err(backend_error)
    }
}

struct Live {
    pool: mysql_async::Pool,
    cat: String,
    dict: Arc<Dict>,
    ids: Arc<CounterIds>,
    layout: Arc<GraphShards>,
    ddl: Arc<SqlDdl>,
    schemas: Vec<String>,
}

impl Live {
    /// The store's counters for one read (rolled back).
    async fn stats_of(&self, text: &str) -> Result<sprig_graph::store::StoreStats> {
        let mut conn = self.pool.get_conn().await.map_err(backend_error)?;
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(backend_error)?;
        let schema = {
            let mut c = self.pool.get_conn().await.map_err(backend_error)?;
            load(&mut c, &self.cat, GRAPH).await?
        };
        let st = {
            let mut one = OneConn(&mut conn);
            let ddl: Arc<dyn sprig_graph::typed::catalog::TypedDdl> = self.ddl.clone();
            let mut store = TypedStore::new(
                &mut one,
                Arc::clone(&self.layout),
                schema,
                ddl,
                self.dict.labels.lock().expect("lock").clone(),
                self.dict.types.lock().expect("lock").clone(),
                self.dict.clone(),
                self.ids.clone(),
            );
            execute(&mut store, text, &Params::new(), &Budgets::default()).await?;
            store.stats()
        };
        conn.query_drop("ROLLBACK").await.map_err(backend_error)?;
        Ok(st)
    }

    async fn run_with(&self, text: &str, budgets: &Budgets) -> Result<Output> {
        let mut conn = self.pool.get_conn().await.map_err(backend_error)?;
        conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .map_err(backend_error)?;
        conn.query_drop("START TRANSACTION")
            .await
            .map_err(backend_error)?;
        let labels = self.dict.labels.lock().expect("lock").clone();
        let types = self.dict.types.lock().expect("lock").clone();
        let schema = {
            let mut c = self.pool.get_conn().await.map_err(backend_error)?;
            load(&mut c, &self.cat, GRAPH).await?
        };
        let result = {
            let mut one = OneConn(&mut conn);
            let ran = {
                let ddl: Arc<dyn sprig_graph::typed::catalog::TypedDdl> = self.ddl.clone();
                let mut store = TypedStore::new(
                    &mut one,
                    Arc::clone(&self.layout),
                    schema.clone(),
                    ddl,
                    labels,
                    types,
                    self.dict.clone(),
                    self.ids.clone(),
                );
                execute(&mut store, text, &Params::new(), budgets).await
            };
            match ran {
                Ok(out) => {
                    let fresh = {
                        let mut c = self.pool.get_conn().await.map_err(backend_error)?;
                        load(&mut c, &self.cat, GRAPH).await?
                    };
                    match check_deleted(&mut one, &self.layout, &fresh, &out.deleted_nodes).await {
                        Ok(()) => Ok(out),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
        };
        let end = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
        conn.query_drop(end).await.map_err(backend_error)?;
        result
    }

    async fn run(&self, text: &str) -> Output {
        self.run_with(text, &Budgets::default())
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"))
    }

    async fn cleanup(self) {
        let mut c = self.pool.get_conn().await.expect("conn");
        for s in &self.schemas {
            c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
                .await
                .expect("drop");
        }
        drop(c);
        self.pool.disconnect().await.expect("disconnect");
    }
}

fn show(o: &Output) -> Vec<String> {
    o.rows
        .iter()
        .map(|r| r.iter().map(Value::display).collect::<Vec<_>>().join("|"))
        .collect()
}

async fn setup(prefix: &str, shards: u32) -> Option<Live> {
    let url = std::env::var("SPRIG_TEST_MARIADB_SINGLE").ok()?;
    let opts = mysql_async::Opts::from_url(&url).expect("url");
    let pool = mysql_async::Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::new()
                .with_constraints(mysql_async::PoolConstraints::new(0, 16).expect("pool"))
                .with_inactive_connection_ttl(Duration::from_secs(30)),
        ),
    );
    let cat = format!("{prefix}_cat");
    let data: Vec<String> = (1..=shards).map(|i| format!("{prefix}_s{i}")).collect();
    let mut schemas = data.clone();
    schemas.push(cat.clone());
    let mut c = pool.get_conn().await.expect("conn");
    for s in &schemas {
        c.query_drop(format!("DROP DATABASE IF EXISTS {s}"))
            .await
            .expect("drop");
        c.query_drop(format!("CREATE DATABASE {s}"))
            .await
            .expect("create");
    }
    GraphCatalog::new(&cat)
        .migrate(&mut c)
        .await
        .expect("migrate");
    GraphCatalog::new(&cat)
        .insert_native(&mut c, GRAPH, 1)
        .await
        .expect("graph row");
    let mut layout = GraphShards::single(GRAPH, 1, &data[0], "c1");
    if shards > 1 {
        layout.group = 7;
        layout.mirrors = true;
        layout.shards = (0..shards)
            .map(|i| ShardLoc {
                shard: i + 1,
                lo: u64::try_from((u128::from(i) << 64) / u128::from(shards)).expect("lo"),
                epoch: 1,
                cluster: "c1".into(),
                schema: data[i as usize].clone(),
                tables: Tables::of(&data[i as usize], GRAPH),
            })
            .collect();
    }
    for sh in &layout.shards {
        for s in sprig_graph::typed::base_ddl(&sh.schema, GRAPH) {
            c.query_drop(s).await.expect("ddl");
        }
    }
    let first = Tables::of(&data[0], GRAPH);
    for s in first.seq_ddl() {
        c.query_drop(s).await.expect("seq");
    }
    drop(c);
    let ddl = Arc::new(SqlDdl {
        cat: cat.clone(),
        graph: GRAPH,
        shards: layout
            .shards
            .iter()
            .map(|s| (s.cluster.clone(), s.schema.clone()))
            .collect(),
        conns: Arc::new(Conns(pool.clone())),
    });
    Some(Live {
        dict: Arc::new(Dict {
            cat: cat.clone(),
            pool: pool.clone(),
            labels: Mutex::new(BTreeMap::new()),
            types: Mutex::new(BTreeMap::new()),
        }),
        ids: Arc::new(CounterIds::new(pool.clone(), &first, 64)),
        cat,
        pool,
        layout: Arc::new(layout),
        ddl,
        schemas,
    })
}

const CORPUS_SETUP: &str = "CREATE (a:Person {name: 'Alice', email: 'alice@example.com', age: 30}),
    (b:Person {name: 'Bob', age: 25}), (c:Person {name: 'Carol', age: 35}),
    (d:Person {name: 'Dave', age: 40}), (e:Person {name: 'Eve'}),
    (p1:Product {name: 'Lamp', price: 10.5}), (p2:Product {name: 'Desk', price: 100}),
    (p3:Product {name: 'Pen', price: 1}),
    (a)-[:KNOWS {since: 2010}]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(a),
    (a)-[:BOUGHT]->(p1), (b)-[:BOUGHT]->(p2), (c)-[:BOUGHT]->(p2), (c)-[:BOUGHT]->(p3),
    (d)-[:BOUGHT]->(p3)";

const CORPUS: &[&str] = &[
    "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS n ORDER BY n",
    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b) RETURN b.name, r.since",
    "MATCH (a {name: 'Alice'})-[:KNOWS]-(x) RETURN x.name ORDER BY x.name",
    "MATCH (n) RETURN count(*) AS c, count(n.age) AS a",
    "MATCH (a:Person {email: 'alice@example.com'})-[:KNOWS*1..2]->(f:Person)-[:BOUGHT]->(p:Product) WHERE NOT (a)-[:BOUGHT]->(p) AND f <> a RETURN p.name AS product, count(DISTINCT f) AS buyers ORDER BY buyers DESC, product LIMIT 5",
    "MATCH (a {name: 'Alice'})-[:KNOWS*1..3]->(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH p = (a {name: 'Alice'})-[:KNOWS*1..4]->(x) RETURN x.name, length(p) ORDER BY length(p)",
    "MATCH (a {name: 'Alice'})-[:KNOWS*]->(x) RETURN count(DISTINCT x) AS c",
    "MATCH (a {name: 'Alice'})<-[:KNOWS*1..2]-(x) RETURN DISTINCT x.name AS n ORDER BY n",
    "MATCH (a {name: 'Alice'}), (c {name: 'Carol'}) MATCH p = allShortestPaths((a)-[:KNOWS*]-(c)) RETURN [n IN nodes(p) | n.name] AS ns ORDER BY ns",
    "MATCH (p:Person) OPTIONAL MATCH (p)-[:BOUGHT]->(x) RETURN p.name, count(x) AS c ORDER BY p.name",
    "MATCH (p:Product) RETURN sum(p.price) AS s, min(p.name) AS mn, max(p.price) AS mx",
    "MATCH (a:Person) WHERE exists((a)-[:BOUGHT]->(:Product {name: 'Pen'})) RETURN a.name ORDER BY a.name",
    "MATCH (n:Product) WHERE n.price = 100 RETURN n.name",
    "MATCH (n:Product) WHERE n.price = '100' RETURN n.name",
    "MATCH (n:Product {price: 10.5}) RETURN n.name",
];

async fn agree(live: &Live, mem: &mut MemStore, qs: &[&str]) -> usize {
    let mut n = 0;
    for q in qs {
        let a = show(&live.run(q).await);
        let b = show(
            &execute(mem, q, &Params::new(), &Budgets::default())
                .await
                .expect(q),
        );
        assert_eq!(a, b, "{q}");
        n += 1;
    }
    n
}

async fn both(live: &Live, mem: &mut MemStore, q: &str) {
    live.run(q).await;
    execute(mem, q, &Params::new(), &Budgets::default())
        .await
        .expect(q);
}

/// The corpus on one shard schema and on four with mirrors, as memory
/// answers it; then writes that move labels, change a key's type, keep
/// lists, add keys in a transaction that holds the table.
#[tokio::test]
async fn typed_tables_agree_with_memory() {
    for shards in [1u32, 4] {
        let Some(live) = setup(&format!("graphstore_typed_{shards}"), shards).await else {
            eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
            return;
        };
        let mut mem = MemStore::new(shards as usize);
        both(&live, &mut mem, CORPUS_SETUP).await;
        // A ring of 60 with chords, so paths cross shards many times.
        let ring = "UNWIND range(0, 59) AS i CREATE (:R {i: i})";
        let link = "MATCH (a:R), (b:R) WHERE b.i = (a.i + 1) % 60 OR b.i = (a.i * 7) % 60 CREATE (a)-[:NEXT {w: a.i}]->(b)";
        // A hub with more incoming links than a joined read joins.
        let hub = "CREATE (:H {i: 1000})";
        let spokes = "MATCH (h:H {i: 1000}) UNWIND range(0, 199) AS i CREATE (:L {i: i, d: i % 17})-[:TO {w: i}]->(h)";
        for q in [ring, link, hub, spokes] {
            both(&live, &mut mem, q).await;
        }
        let extra = [
            "MATCH (a:R {i: 0})<-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
            "MATCH (a:R {i: 5})<-[:NEXT*1..3]-(x) RETURN count(DISTINCT x) AS c",
            "MATCH (a:R {i: 5})-[:NEXT*1..4]->(x) RETURN count(DISTINCT x) AS c",
            "MATCH (a:R {i: 5})<-[r:NEXT*2]-(x) RETURN x.i, [e IN r | e.w] AS ws ORDER BY x.i, ws",
            "MATCH (a:R {i: 1}), (b:R {i: 31}) MATCH p = shortestPath((a)-[:NEXT*]->(b)) RETURN length(p)",
            "MATCH (a:R {i: 3})-[:NEXT]-(x) RETURN x.i ORDER BY x.i",
            "MATCH (x:R)<-[:NEXT]-(y:R) WHERE x.i < 4 RETURN x.i, count(y) ORDER BY x.i",
            // Walks read ahead in one recursive statement: trails through the
            // ring's cycles, relationship lists, end labels and a hop after.
            "MATCH (a:R {i: 5})-[:NEXT*0..5]->(x) RETURN x.i, count(*) AS c ORDER BY x.i",
            "MATCH (a:R {i: 2})-[r:NEXT*1..3]->(x) RETURN x.i, [e IN r | e.w] AS ws ORDER BY x.i, ws",
            "MATCH (a:R) WHERE a.i < 3 MATCH (a)-[:NEXT*0..4]->(x:R), (x)-[:NEXT]->(y) RETURN a.i, x.i, y.i, count(*) AS c ORDER BY a.i, x.i, y.i",
            // Shortest paths to many ends: the last level read from the ends.
            "MATCH (a:R {i: 5}), (b:R) WHERE b.i IN [11, 23, 31, 47, 58, 6] MATCH p = shortestPath((a)-[:NEXT*1..3]-(b)) RETURN b.i, length(p) ORDER BY b.i",
            "MATCH (a:R {i: 5}), (b:R) WHERE b.i IN [11, 23, 31, 47, 58, 6] MATCH p = shortestPath((a)-[:NEXT*1..4]->(b)) RETURN b.i, length(p), [n IN nodes(p) | n.i][0] AS s ORDER BY b.i",
            // A start with more links than are joined: the same statement
            // gives them unjoined, and the far nodes are read by id.
            "MATCH (h:H {i: 1000})<-[:TO]-(l) WITH l ORDER BY l.d DESC, l.i LIMIT 5 RETURN l.i, l.d",
            "MATCH (h:H {i: 1000})<-[t:TO]-(l:L) WHERE l.i < 190 RETURN count(*) AS c, sum(t.w) AS w",
            "MATCH (l:L) WHERE l.i < 3 MATCH (l)-[:TO]->(h)<-[:TO]-(o) RETURN l.i, count(o) AS c ORDER BY l.i",
            // A later pattern's start scanned with the first pattern's read.
            "MATCH (a:R {i: 5}), (b:L {d: 3}) WHERE NOT a = b RETURN a.i, b.i ORDER BY b.i",
            "MATCH (a:R {i: 5}), (b:R) WHERE b.i IN [6, 35] MATCH p = shortestPath((a)-[:NEXT*1..3]-(b)) RETURN b.i, length(p) ORDER BY b.i",
            // A hop's next hop read along with it.
            "MATCH (a:R) WHERE a.i < 4 OPTIONAL MATCH (a)-[r:NEXT]->(b:R)-[:NEXT]->(c:R) RETURN a.i, b.i, c.i, r.w ORDER BY a.i, b.i, c.i",
            "MATCH (l:L) WHERE l.i < 4 OPTIONAL MATCH (l)-[:TO]->(h:H)-[:NEXT]->(x) RETURN l.i, h.i, x.i ORDER BY l.i",
            "MATCH (a:R {i: 7})-[:NEXT]->(b)-[:NEXT]->(c)-[:NEXT]->(d:R) RETURN b.i, c.i, d.i ORDER BY b.i, c.i, d.i",
        ];

        let mut n = agree(&live, &mut mem, CORPUS).await;
        n += agree(&live, &mut mem, &extra).await;
        // Round trips of a few reads (each shard statement batch is one).
        for q in [
            "MATCH (a:R {i: 7})-[:NEXT]->(b) RETURN b.i",
            "MATCH (a:R {i: 7})-[:NEXT]->(b)-[:NEXT]->(c) RETURN c.i",
            "MATCH (a:R {i: 0})<-[:NEXT]-(x) RETURN x.i",
            "MATCH (l:L) WHERE l.i < 3 MATCH (l)-[:TO]->(h)<-[:TO]-(o) RETURN l.i, count(o)",
        ] {
            let st = live.stats_of(q).await.expect(q);
            eprintln!(
                "typed, {shards} shard(s): {} round trips, {} rows read :: {q}",
                st.shard_queries, st.sql_rows
            );
        }
        // Label moves, a key of two types, lists, temporal values, and keys
        // added after the transaction read the table.
        for q in [
            "MATCH (p:Person {name: 'Bob'}) SET p:Admin, p.level = 3",
            "MATCH (p:Person {name: 'Carol'}) SET p.age = 'thirty-five', p.tags = ['a', 'b'], p.born = date('1990-01-02')",
            "MATCH (p:Person {name: 'Dave'}) SET p.empty = [], p.f = 1.0, p.big = 9007199254740993",
            "MATCH (p:Admin) REMOVE p:Admin",
            "MATCH (a:Person {name: 'Alice'})-[k:KNOWS]->(b) SET k.notes = ['x'], k.since = 2011",
            "CREATE (:Person:Admin {name: 'Zed', age: 1})-[:KNOWS {since: 1999}]->(:Person {name: 'Yan'})",
        ] {
            both(&live, &mut mem, q).await;
        }
        n += agree(
            &live,
            &mut mem,
            &[
                "MATCH (p:Person) RETURN p.name, p.age, p.level, p.tags, p.born, p.empty, p.f, p.big, labels(p) ORDER BY p.name",
                "MATCH (p:Admin) RETURN p.name",
                "MATCH (a)-[k:KNOWS]->(b) RETURN a.name, b.name, k.since, k.notes ORDER BY a.name, b.name",
                "MATCH (p:Person) WHERE p.age = 'thirty-five' RETURN p.name",
                "MATCH (p:Person {age: 30}) RETURN p.name",
            ],
        )
        .await;
        // DETACH DELETE, and DELETE of a node with relationships refused.
        both(
            &live,
            &mut mem,
            "MATCH (p:Person {name: 'Zed'}) DETACH DELETE p",
        )
        .await;
        assert!(
            live.run_with(
                "MATCH (p:Person {name: 'Alice'}) DELETE p",
                &Budgets::default()
            )
            .await
            .is_err()
        );
        n += agree(
            &live,
            &mut mem,
            &[
                "MATCH (n) RETURN count(n)",
                "MATCH ()-[r]->() RETURN count(r)",
            ],
        )
        .await;
        eprintln!("typed, {shards} shard(s): {n} statements agree with memory");
        live.cleanup().await;
    }
}
