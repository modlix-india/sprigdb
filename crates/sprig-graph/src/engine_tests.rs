//! Unit tests of the engine store: layout invariants after writes, the
//! dictionary and ids across reopening, uniqueness, and the deleted-node
//! check at commit.

use std::sync::Arc;
use std::time::Duration;

use sprig_engine::txn::UnitConfig;
use sprig_engine::txn::testing::MemPath;

use super::*;
use crate::eval::Params;
use crate::exec::Budgets;

fn path() -> Arc<MemPath> {
    MemPath::start(UnitConfig::default(), Duration::ZERO)
}

async fn run(g: &Arc<EngineGraph>, text: &str) -> sprig_common::Result<crate::Output> {
    let mut tx = g.begin(true);
    let r = {
        let mut s = g.store(&mut tx);
        crate::execute(&mut s, text, &Params::new(), &Budgets::default()).await
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

fn show(o: &crate::Output) -> Vec<String> {
    o.rows
        .iter()
        .map(|r| {
            r.iter()
                .map(crate::value::Value::display)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[tokio::test]
async fn writes_keep_the_layout_consistent() {
    for shards in [1, 4] {
        let p = path();
        let g = EngineGraph::open(p.clone(), 7, 3, shards).unwrap();
        run(&g, "CREATE (a:P {n: 1}), (b:P:Q {n: 2}), (c {n: 3}), (a)-[:K {w: 1}]->(b), (b)-[:K]->(c), (c)-[:L]->(a), (a)-[:K]->(a)")
            .await
            .unwrap();
        assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
        assert_eq!(counts(&g).unwrap(), (3, 4));
        // A label change rewrites the neighbours' entries.
        run(&g, "MATCH (b {n: 2}) SET b:R REMOVE b:P")
            .await
            .unwrap();
        run(&g, "MATCH (a {n: 1}) SET a:Z").await.unwrap();
        assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
        let o = run(
            &g,
            "MATCH (x)-[:K]->(y) RETURN x.n, labels(y), y.n ORDER BY x.n, y.n",
        )
        .await
        .unwrap();
        assert_eq!(
            show(&o),
            ["1|[\"P\", \"Z\"]|1", "1|[\"Q\", \"R\"]|2", "2|[]|3"]
        );
        // Deleting a node with relationships fails at commit.
        let e = run(&g, "MATCH (c {n: 3}) DELETE c").await.unwrap_err();
        assert_eq!(e.code(), Some(SprigCode::GraphNodeHasRelationships));
        run(&g, "MATCH (c {n: 3}) DETACH DELETE c").await.unwrap();
        assert_eq!(counts(&g).unwrap(), (2, 2));
        assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
        // Reopening reads the dictionary back; ids keep growing.
        let g2 = EngineGraph::open(p.clone(), 7, 3, shards).unwrap();
        let o = run(&g2, "MATCH (n:Q) RETURN n.n").await.unwrap();
        assert_eq!(show(&o), ["2"]);
        let o = run(&g2, "CREATE (n:New) RETURN id(n) > 2").await.unwrap();
        assert_eq!(show(&o), ["true"]);
        assert_eq!(check_invariants(&g2).unwrap(), Vec::<String>::new());
    }
}

#[tokio::test]
async fn unique_constraints_and_indexes() {
    let g = EngineGraph::open(path(), 7, 4, 2).unwrap();
    run(&g, "CREATE (:U {k: 1}), (:U {k: 2}), (:U {k: 'x'})")
        .await
        .unwrap();
    run(&g, "CREATE CONSTRAINT u_k FOR (u:U) REQUIRE u.k IS UNIQUE")
        .await
        .unwrap();
    let e = run(&g, "CREATE (:U {k: 1.0})").await.unwrap_err();
    assert!(
        matches!(e.kind, sprig_common::ErrorKind::Backend { errno: 1062, .. }),
        "{e}"
    );
    run(&g, "CREATE (:U {k: 3})").await.unwrap();
    run(&g, "MATCH (u:U {k: 3}) SET u.k = 4").await.unwrap();
    run(&g, "CREATE (:U {k: 3})").await.unwrap();
    let o = run(&g, "MATCH (u:U) WHERE u.k = 4 RETURN count(*)")
        .await
        .unwrap();
    assert_eq!(show(&o), ["1"]);
    run(&g, "CREATE INDEX u_x FOR (u:V) ON (u.x)")
        .await
        .unwrap();
    run(&g, "UNWIND range(1, 50) AS i CREATE (:V {x: i % 5})")
        .await
        .unwrap();
    let o = run(&g, "MATCH (v:V {x: 3}) RETURN count(*)").await.unwrap();
    assert_eq!(show(&o), ["10"]);
    run(&g, "MATCH (v:V {x: 3}) DELETE v").await.unwrap();
    let o = run(&g, "MATCH (v:V {x: 3}) RETURN count(*)").await.unwrap();
    assert_eq!(show(&o), ["0"]);
    run(&g, "UNWIND range(1, 4) AS i CREATE (:W {x: i % 2})")
        .await
        .unwrap();
    let e = run(&g, "CREATE CONSTRAINT w_x FOR (w:W) REQUIRE w.x IS UNIQUE")
        .await
        .unwrap_err();
    assert!(e.message.contains("ConstraintCreationFailed"), "{e}");
    run(&g, "DROP CONSTRAINT u_k").await.unwrap();
    run(&g, "CREATE (:U {k: 1})").await.unwrap();
    // Reopened, the definitions come back.
    let g2 = EngineGraph::open(Arc::clone(g.path()), 7, 4, 2).unwrap();
    assert_eq!(g2.defs().len(), 1);
}

#[tokio::test]
async fn rolled_back_writes_leave_nothing() {
    let g = EngineGraph::open(path(), 7, 5, 1).unwrap();
    run(&g, "CREATE (:A {i: 1})-[:R]->(:B)").await.unwrap();
    let mut tx = g.begin(false);
    {
        let mut s = g.store(&mut tx);
        crate::execute(
            &mut s,
            "MATCH (a:A) CREATE (a)-[:R]->(:C)",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .unwrap();
        let o = crate::execute(
            &mut s,
            "MATCH (:A)-[:R]->(x) RETURN count(x)",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .unwrap();
        assert_eq!(show(&o), ["2"]);
    }
    tx.rollback();
    assert_eq!(counts(&g).unwrap(), (2, 1));
    assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
}

/// Runs `q` `n` times on each of `tasks` concurrent tasks; failures that
/// a driver retries (deadlocks) are retried.
async fn hammer(g: &Arc<EngineGraph>, tasks: usize, n: usize, q: impl Fn(usize, usize) -> String) {
    let mut hs = Vec::new();
    for t in 0..tasks {
        let g = Arc::clone(g);
        let qs: Vec<String> = (0..n).map(|i| q(t, i)).collect();
        hs.push(tokio::spawn(async move {
            for q in qs {
                loop {
                    match run(&g, &q).await {
                        Ok(_) => break,
                        Err(e) if e.retryable => {}
                        // The node it matched was deleted before it got the
                        // node's lock: the statement is refused, as in Neo4j.
                        Err(e) if e.code() == Some(SprigCode::GraphNotFound) => break,
                        Err(e) => panic!("{q}: {e}"),
                    }
                }
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_relationship_updates_lose_nothing() {
    let g = EngineGraph::open(path(), 7, 6, 1).unwrap();
    run(&g, "CREATE (:A)-[:R {c: 0}]->(:B)").await.unwrap();
    hammer(&g, 4, 25, |_, _| {
        "MATCH (:A)-[r:R]->() SET r.c = r.c + 1".into()
    })
    .await;
    let o = run(&g, "MATCH ()-[r:R]->() RETURN r.c").await.unwrap();
    assert_eq!(show(&o), ["100"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detach_delete_takes_relationships_made_meanwhile() {
    let g = EngineGraph::open(path(), 7, 7, 1).unwrap();
    run(&g, "UNWIND range(1, 40) AS i CREATE (:Hub {i: i})")
        .await
        .unwrap();
    // Writers link new nodes to hubs while others detach-delete hubs.
    let g2 = Arc::clone(&g);
    let writers = tokio::spawn(async move {
        hammer(&g2, 3, 40, |t, i| {
            format!(
                "MATCH (h:Hub {{i: {}}}) CREATE (:X {{t: {t}}})-[:TO]->(h)",
                (i % 40) + 1
            )
        })
        .await;
    });
    hammer(&g, 2, 20, |t, i| {
        format!("MATCH (h:Hub {{i: {}}}) DETACH DELETE h", t * 20 + i + 1)
    })
    .await;
    writers.await.unwrap();
    assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
    let o = run(&g, "MATCH (h:Hub) RETURN count(h)").await.unwrap();
    assert_eq!(show(&o), ["0"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_new_names_and_an_index_built_under_writers() {
    let g = EngineGraph::open(path(), 7, 8, 2).unwrap();
    // New property keys in overlapping sets, from several writers at once.
    let g2 = Arc::clone(&g);
    let writers = tokio::spawn(async move {
        hammer(&g2, 4, 60, |t, i| {
            format!(
                "CREATE (:P {{k{}: 1, k{}: 2, v: {}}})",
                (t + i) % 7,
                (t + 2 * i) % 7,
                i % 5
            )
        })
        .await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    run(&g, "CREATE INDEX p_v FOR (p:P) ON (p.v)")
        .await
        .unwrap();
    writers.await.unwrap();
    // Every node is found through the index (the planner uses it: online).
    for v in 0..5 {
        let a = run(&g, &format!("MATCH (p:P {{v: {v}}}) RETURN count(*)"))
            .await
            .unwrap();
        let b = run(
            &g,
            &format!("MATCH (p:P) WHERE p.v + 0 = {v} RETURN count(*)"),
        )
        .await
        .unwrap();
        assert_eq!(show(&a), show(&b), "v = {v}");
    }
    let o = run(&g, "MATCH (p:P) RETURN count(*)").await.unwrap();
    assert_eq!(show(&o), ["240"]);
}

/// A node that takes the graph's tenant leadership again drops its id
/// blocks: the next ids come from a new block of the counter.
#[tokio::test]
async fn id_blocks_of_an_older_leader_epoch_are_dropped() {
    use sprig_engine::config::{EngineConfig, StoreKind};
    let d = std::env::temp_dir().join(format!("graph-ids-epoch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    let mut c = EngineConfig::new(&d);
    c.store = StoreKind::Mem;
    let engine = sprig_engine::Engine::open(c).unwrap();
    let g = EngineGraph::open(Arc::new(engine.clone()), 7, 3, 1).unwrap();
    assert_eq!(g.alloc(IdKind::Node, 2).await.unwrap(), vec![1, 2]);
    assert_eq!(g.alloc(IdKind::Node, 1).await.unwrap(), vec![3]);
    engine.lead(sprig_engine::Engine::unit_of_tenant(7));
    let next = g.alloc(IdKind::Node, 1).await.unwrap();
    assert!(next[0] > ID_BLOCK, "{next:?}");
    drop(g);
    drop(engine);
    let _ = std::fs::remove_dir_all(&d);
}
