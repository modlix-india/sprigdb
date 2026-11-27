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

/// The counters kept by commits equal what a fresh pass counts, after
/// creates, label changes (self-loops too), deletes, a failed statement
/// and a rolled-back transaction.
#[tokio::test]
async fn statistics_counters_follow_commits() {
    for shards in [1, 3] {
        let g = EngineGraph::open(path(), 7, 5, shards).unwrap();
        // A new graph starts with exact, empty counters.
        assert!(g.stats().is_some_and(|s| s.elements() == 0.0));
        run(&g, "UNWIND range(1, 30) AS i CREATE (a:A {i: i}), (b:B {i: i}), (a)-[:R]->(b), (b)-[:S]->(a), (a)-[:L]->(a)")
            .await
            .unwrap();
        run(&g, "MATCH (a:A) WHERE a.i % 3 = 0 SET a:C REMOVE a:A")
            .await
            .unwrap();
        run(&g, "MATCH (b:B) WHERE b.i % 5 = 0 DETACH DELETE b")
            .await
            .unwrap();
        run(&g, "MATCH (a)-[r:L]->(a) WHERE a.i % 7 = 0 DELETE r")
            .await
            .unwrap();
        assert!(run(&g, "MATCH (a:A {i: 1}) DELETE a").await.is_err());
        let mut tx = g.begin(true);
        {
            let mut s = g.store(&mut tx);
            crate::execute(
                &mut s,
                "CREATE (:A), (:Z)-[:R]->(:Z)",
                &Params::new(),
                &Budgets::default(),
            )
            .await
            .unwrap();
        }
        tx.rollback();
        let kept = g.stats().unwrap();
        let fresh = g.collect_stats().unwrap();
        let clean = |m: &std::collections::HashMap<u32, f64>| {
            let mut v: Vec<(u32, f64)> = m
                .iter()
                .filter(|(_, n)| **n > 0.0)
                .map(|(a, b)| (*a, *b))
                .collect();
            v.sort_by_key(|x| x.0);
            v
        };
        let clean3 = |m: &std::collections::HashMap<(u32, u32, u32), f64>| {
            let mut v: Vec<((u32, u32, u32), f64)> = m
                .iter()
                .filter(|(_, n)| **n > 0.0)
                .map(|(a, b)| (*a, *b))
                .collect();
            v.sort_by_key(|x| x.0);
            v
        };
        assert_eq!(clean(&kept.sets), clean(&fresh.sets), "shards {shards}");
        assert_eq!(
            clean3(&kept.triples),
            clean3(&fresh.triples),
            "shards {shards}"
        );
        let o = run(&g, "CALL sprig.graph.analyze()").await.unwrap();
        assert_eq!(
            show(&o),
            [format!(
                "{:?}|{:?}",
                54.0_f64,
                fresh.triples.values().sum::<f64>()
            )]
        );
    }
}

/// Waits for a background statistics pass to end.
async fn settle(g: &Arc<EngineGraph>) {
    while g.collecting.load(std::sync::atomic::Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

type Counts = (Vec<(u32, f64)>, Vec<((u32, u32, u32), f64)>);

fn counts_of(st: &stats::GraphStats) -> Counts {
    let mut a: Vec<(u32, f64)> = st
        .sets
        .iter()
        .filter(|(_, n)| **n > 0.0)
        .map(|(x, y)| (*x, *y))
        .collect();
    a.sort_by_key(|x| x.0);
    let mut b: Vec<((u32, u32, u32), f64)> = st
        .triples
        .iter()
        .filter(|(_, n)| **n > 0.0)
        .map(|(x, y)| (*x, *y))
        .collect();
    b.sort_by_key(|x| x.0);
    (a, b)
}

/// The counters survive reopening the graph exactly: the stored record
/// plus the deltas committed since (auto-commit statements, an explicit
/// transaction, `CALL { } IN TRANSACTIONS` batches, a rollback that
/// stores nothing); many deltas are folded into the record; a pass deletes
/// the deltas it counted.
#[tokio::test]
async fn statistics_counters_survive_reopening() {
    for shards in [1, 3] {
        let p = path();
        let g = EngineGraph::open(p.clone(), 7, 8, shards).unwrap();
        run(&g, "UNWIND range(1, 20) AS i CREATE (a:A {i: i}), (b:B {i: i}), (a)-[:R]->(b), (a)-[:L]->(a)")
            .await
            .unwrap();
        run(&g, "MATCH (a:A) WHERE a.i % 3 = 0 SET a:C REMOVE a:A")
            .await
            .unwrap();
        run(&g, "MATCH (b:B) WHERE b.i % 4 = 0 DETACH DELETE b")
            .await
            .unwrap();
        run(
            &g,
            "UNWIND range(1, 9) AS i CALL { WITH i CREATE (:T {i: i})-[:R]->(:B) } IN TRANSACTIONS OF 2 ROWS",
        )
        .await
        .unwrap();
        // An explicit transaction of two statements.
        let mut tx = g.begin(false);
        for q in ["CREATE (:X)-[:R]->(:Y)", "MATCH (x:X) SET x:Z"] {
            let mut s = g.store(&mut tx);
            crate::execute(&mut s, q, &Params::new(), &Budgets::default())
                .await
                .unwrap();
        }
        tx.commit(&g).await.unwrap();
        // Rolled back: nothing stored.
        let mut tx = g.begin(false);
        {
            let mut s = g.store(&mut tx);
            crate::execute(&mut s, "CREATE (:A)", &Params::new(), &Budgets::default())
                .await
                .unwrap();
        }
        tx.rollback();
        let fresh = g.collect_stats().unwrap();
        assert_eq!(counts_of(&g.stats().unwrap()), counts_of(&fresh));
        let stored = g.stored_deltas(usize::MAX).unwrap().len();
        assert!(stored >= 9, "{stored} deltas stored");
        settle(&g).await;
        drop(g);
        let g = EngineGraph::open(p.clone(), 7, 8, shards).unwrap();
        assert_eq!(
            counts_of(&g.stats().unwrap()),
            counts_of(&fresh),
            "shards {shards} after reopening"
        );
        // More deltas than the fold threshold: folded at the next open.
        for i in 0..=stats::FOLD_AT {
            run(&g, &format!("CREATE (:F {{i: {i}}})")).await.unwrap();
        }
        let fresh = g.collect_stats().unwrap();
        settle(&g).await;
        drop(g);
        let g = EngineGraph::open(p.clone(), 7, 8, shards).unwrap();
        assert_eq!(counts_of(&g.stats().unwrap()), counts_of(&fresh));
        assert!(g.stored_deltas(usize::MAX).unwrap().len() <= stats::FOLD_AT);
        settle(&g).await;
        drop(g);
        let g = EngineGraph::open(p.clone(), 7, 8, shards).unwrap();
        assert_eq!(counts_of(&g.stats().unwrap()), counts_of(&fresh));
        // A pass folds the deltas into the record.
        g.analyze().unwrap();
        assert!(g.stored_deltas(usize::MAX).unwrap().is_empty());
        run(&g, "CREATE (:G)").await.unwrap();
        assert_eq!(g.stored_deltas(usize::MAX).unwrap().len(), 1);
        g.analyze().unwrap();
        assert!(g.stored_deltas(usize::MAX).unwrap().len() <= stats::FOLD_AT);
        settle(&g).await;
        drop(g);
        let g = EngineGraph::open(p, 7, 8, shards).unwrap();
        assert_eq!(
            counts_of(&g.stats().unwrap()),
            counts_of(&g.collect_stats().unwrap())
        );
    }
}

/// Indexes and constraints made before ordered keys are rebuilt into them
/// online when the graph opens, with writers running: one index each in
/// `SHOW` throughout, the same answers as a scan, uniqueness kept, the
/// layout invariants whole, and ranges served after.
#[tokio::test]
async fn equality_indexes_are_rebuilt_ordered() {
    for shards in [1, 3] {
        let p = path();
        let g = EngineGraph::open(p.clone(), 7, 9, shards).unwrap();
        upgrade::set_legacy_indexes(true);
        run(&g, "CREATE INDEX idx_v FOR (n:P) ON (n.v)")
            .await
            .unwrap();
        run(&g, "CREATE CONSTRAINT u_k FOR (n:P) REQUIRE n.k IS UNIQUE")
            .await
            .unwrap();
        upgrade::set_legacy_indexes(false);
        run(
            &g,
            "UNWIND range(1, 300) AS i CREATE (:P {v: CASE WHEN i % 5 = 0 THEN toFloat(i % 17) ELSE i % 17 END, k: i})",
        )
        .await
        .unwrap();
        assert!(g.defs().iter().all(|d| !g.is_ordered(d.id)));
        assert!(g.needs_upgrade());
        let show = "SHOW INDEXES YIELD name RETURN name ORDER BY name";
        let names = show_of(&run(&g, show).await.unwrap());
        drop(g);
        // Reopened: the rebuild starts in the background; writers run
        // meanwhile.
        let g = EngineGraph::open(p.clone(), 7, 9, shards).unwrap();
        let g2 = Arc::clone(&g);
        let n_shown = names.len();
        let writers = tokio::spawn(async move {
            for i in 0..40 {
                run(
                    &g2,
                    &format!("CREATE (:P {{v: {}, k: {}}})", i % 17, 1000 + i),
                )
                .await
                .unwrap();
                run(
                    &g2,
                    &format!("MATCH (n:P {{k: {}}}) SET n.v = 99", i * 7 + 1),
                )
                .await
                .unwrap();
                assert_eq!(show_of(&run(&g2, show).await.unwrap()).len(), n_shown);
                tokio::task::yield_now().await;
            }
        });
        g.upgrade_indexes().await.unwrap();
        writers.await.unwrap();
        g.upgrade_indexes().await.unwrap();
        assert!(!g.needs_upgrade());
        assert!(
            g.defs()
                .iter()
                .all(|d| g.is_ordered(d.id) && d.state == "online")
        );
        assert_eq!(g.defs().len(), 2);
        assert_eq!(show_of(&run(&g, show).await.unwrap()), names);
        assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
        for (q, scan) in [
            (
                "MATCH (n:P) WHERE n.v = 5 RETURN count(n)",
                "MATCH (n:P) WHERE n.v + 0 = 5 RETURN count(n)",
            ),
            (
                "MATCH (n:P) WHERE n.v IN [3, 99] RETURN count(n)",
                "MATCH (n:P) WHERE n.v + 0 IN [3, 99] RETURN count(n)",
            ),
            (
                "MATCH (n:P) WHERE n.v > 10 AND n.v <= 15 RETURN count(n)",
                "MATCH (n:P) WHERE n.v + 0 > 10 AND n.v + 0 <= 15 RETURN count(n)",
            ),
            (
                "MATCH (n:P {k: 77}) RETURN n.v",
                "MATCH (n:P) WHERE n.k + 0 = 77 RETURN n.v",
            ),
        ] {
            assert_eq!(
                show_of(&run(&g, q).await.unwrap()),
                show_of(&run(&g, scan).await.unwrap()),
                "{q}"
            );
        }
        assert!(run(&g, "CREATE (:P {k: 5})").await.is_err());
        // Reopened again: nothing left to do.
        drop(g);
        let g = EngineGraph::open(p.clone(), 7, 9, shards).unwrap();
        assert!(!g.needs_upgrade());
        run(&g, "DROP INDEX idx_v").await.unwrap();
        assert_eq!(show_of(&run(&g, show).await.unwrap()).len(), n_shown - 1);
        assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
    }
}

/// `DROP INDEX` while a rebuild is half done drops both definitions; the
/// rebuild then removes what its twin had.
#[tokio::test]
async fn dropping_an_index_mid_rebuild_drops_its_twin() {
    let p = path();
    let g = EngineGraph::open(p.clone(), 7, 10, 1).unwrap();
    upgrade::set_legacy_indexes(true);
    run(&g, "CREATE INDEX idx_w FOR (n:P) ON (n.w)")
        .await
        .unwrap();
    upgrade::set_legacy_indexes(false);
    run(&g, "UNWIND range(1, 50) AS i CREATE (:P {w: i})")
        .await
        .unwrap();
    let old = g.defs()[0].clone();
    let show = "SHOW INDEXES YIELD name RETURN name";
    let n_shown = show_of(&run(&g, show).await.unwrap()).len();
    let twin = g.publish_twin(&old).await.unwrap();
    assert_eq!(g.defs().len(), 2);
    assert_eq!(show_of(&run(&g, show).await.unwrap()).len(), n_shown);
    // A write keeps the twin's entries too.
    run(&g, "CREATE (:P {w: 1000})").await.unwrap();
    run(&g, "DROP INDEX idx_w").await.unwrap();
    assert!(g.defs().is_empty());
    g.backfill(&twin).await.unwrap();
    g.upgrade_indexes().await.unwrap();
    assert!(!g.swap_twin(&old, &twin).await.unwrap());
    assert!(g.defs().is_empty());
    assert_eq!(check_invariants(&g).unwrap(), Vec::<String>::new());
    assert_eq!(show_of(&run(&g, show).await.unwrap()).len(), n_shown - 1);
}

fn show_of(o: &crate::Output) -> Vec<String> {
    show(o)
}

/// Range conditions, prefixes and top-k orders read through an index give
/// the rows a scan gives, over mixed types, nulls, ties and long strings.
#[tokio::test]
async fn range_indexes_answer_as_scans() {
    let g = EngineGraph::open(path(), 7, 6, 1).unwrap();
    run(&g, "CREATE INDEX FOR (n:P) ON (n.v)").await.unwrap();
    let long = "x".repeat(1500);
    run(
        &g,
        &format!(
            "UNWIND range(1, 60) AS i CREATE (:P {{v: CASE i % 6 WHEN 0 THEN i WHEN 1 THEN toFloat(i) / 2 WHEN 2 THEN 'k' + toString(i) WHEN 3 THEN null WHEN 4 THEN i % 5 ELSE '{long}' + toString(i % 3) END, i: i}}), (:Q {{v: i}})"
        ),
    )
    .await
    .unwrap();
    g.analyze().unwrap();
    let queries = [
        "MATCH (n:P) WHERE n.v > 10 RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE n.v >= 2 AND n.v < 20.5 RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE 5 > n.v RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE n.v STARTS WITH 'k1' RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE n.v STARTS WITH 'xxx' AND n.v < 'y' RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE n.v > 'k3' RETURN n.i ORDER BY n.i",
        "MATCH (n:P) WHERE n.v > 3 AND n.v < 'z' RETURN n.i ORDER BY n.i",
        "MATCH (n:P) RETURN n.i, n.v ORDER BY n.v LIMIT 7",
        "MATCH (n:P) RETURN n.i, n.v ORDER BY n.v, n.i LIMIT 50",
        "MATCH (n:P) RETURN n.i ORDER BY n.v DESC LIMIT 5",
        "MATCH (n:P) WHERE n.v > 0 RETURN n.i, n.v ORDER BY n.v DESC LIMIT 5",
        "MATCH (n:P) WHERE n.v < 4 RETURN n.i, n.v ORDER BY n.v LIMIT 3",
        "MATCH (n:P) WHERE n.v >= 'k' RETURN n.i ORDER BY n.v DESC, n.i SKIP 2 LIMIT 4",
    ];
    let mut mem = crate::mem::MemStore::new(1);
    crate::execute(
        &mut mem,
        "CREATE INDEX FOR (n:P) ON (n.v)",
        &Params::new(),
        &Budgets::default(),
    )
    .await
    .unwrap();
    crate::execute(
        &mut mem,
        &format!(
            "UNWIND range(1, 60) AS i CREATE (:P {{v: CASE i % 6 WHEN 0 THEN i WHEN 1 THEN toFloat(i) / 2 WHEN 2 THEN 'k' + toString(i) WHEN 3 THEN null WHEN 4 THEN i % 5 ELSE '{long}' + toString(i % 3) END, i: i}}), (:Q {{v: i}})"
        ),
        &Params::new(),
        &Budgets::default(),
    )
    .await
    .unwrap();
    for q in queries {
        let a = run(&g, q).await.unwrap();
        let b = crate::execute(&mut mem, q, &Params::new(), &Budgets::default())
            .await
            .unwrap();
        assert_eq!(show(&a), show(&b), "{q}");
    }
}

/// Batched node reads give what one-by-one reads give: the transaction's
/// own writes and deletes, committed nodes, missing ids, any order.
#[tokio::test]
async fn batched_node_reads_equal_single_reads() {
    for shards in [1, 3] {
        let g = EngineGraph::open(path(), 7, 8, shards).unwrap();
        run(&g, "UNWIND range(1, 40) AS i CREATE (:A {i: i})")
            .await
            .unwrap();
        let mut tx = g.begin(false);
        let mut s = g.store(&mut tx);
        crate::execute(
            &mut s,
            "MATCH (a:A) WHERE a.i % 4 = 0 SET a.j = 1 WITH 1 AS x MATCH (b:A) WHERE b.i % 7 = 0 DETACH DELETE b CREATE (:A {i: 99})",
            &Params::new(),
            &Budgets::default(),
        )
        .await
        .unwrap();
        let mut ids: Vec<u64> = (0..1200).rev().collect();
        ids.extend([5, 5, 3]);
        let many = s.node_values(&ids).unwrap();
        for (id, v) in ids.iter().zip(&many) {
            let (d, _) = g.home(*id);
            let key = g.space.node(d, *id);
            assert_eq!(&s.get(&key).unwrap(), v, "node {id}, shards {shards}");
        }
        assert!(many.iter().filter(|v| v.is_some()).count() > 30);
        drop(s);
        tx.rollback();
    }
}
