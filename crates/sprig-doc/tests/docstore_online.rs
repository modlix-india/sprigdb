//! The store's online index builds, client-level `bulkWrite` and
//! collection statistics.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::many_single_char_names
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use common::{Bed, check_indexes};

fn names(r: &Document) -> Vec<String> {
    r.get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()
        .iter()
        .map(|x| x.as_document().unwrap().get_str("name").unwrap().to_owned())
        .collect()
}

/// Writers keep running while an index is built; afterwards every entry
/// matches the documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn index_builds_do_not_stop_writers() {
    let b = Arc::new(Bed::mem());
    let n = 20_000;
    for chunk in (0..n).collect::<Vec<i32>>().chunks(1000) {
        let docs: Vec<Bson> = chunk
            .iter()
            .map(|i| Bson::Document(doc! {"_id": *i, "a": i % 97, "t": [i % 3, i % 5]}))
            .collect();
        let r = b.run("d", doc! {"insert": "c", "documents": docs}).await;
        assert_eq!(r.get_i32("n").unwrap(), 1000, "{r}");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let worst = Arc::new(AtomicU64::new(0));
    let mut hs = Vec::new();
    for w in 0..4i32 {
        let (b, stop, writes, worst) = (
            Arc::clone(&b),
            Arc::clone(&stop),
            Arc::clone(&writes),
            Arc::clone(&worst),
        );
        hs.push(tokio::spawn(async move {
            let mut i = 0i32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let id = (i * 7919 + w * 13) % n;
                let t0 = Instant::now();
                let r = match i % 3 {
                    0 => {
                        b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": id}, "u": {"$inc": {"a": 1}, "$push": {"t": i}}}]})
                            .await
                    }
                    1 => {
                        b.run("d", doc! {"insert": "c", "documents": [{"_id": n + w * 1_000_000 + i, "a": i % 11}]})
                            .await
                    }
                    _ => b.run("d", doc! {"delete": "c", "deletes": [{"q": {"_id": id}, "limit": 1}]}).await,
                };
                assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
                let us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
                worst.fetch_max(us, Ordering::Relaxed);
                writes.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = writes.load(Ordering::Relaxed);
    let t0 = Instant::now();
    let r = b
        .run("d", doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a_1"}, {"key": {"t": 1, "a": -1}, "name": "ta"}]})
        .await;
    let took = t0.elapsed();
    assert_eq!(r.get_i32("numIndexesAfter").unwrap(), 3, "{r}");
    let during = writes.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.await.unwrap();
    }
    eprintln!(
        "build of 2 indexes over {n} documents: {took:?}; {during} writes ran during it; worst write {} us",
        worst.load(Ordering::Relaxed)
    );
    assert!(during > 0, "writers were stopped by the build");
    check_indexes(&b, "d").await;
    let r = b.run("d", doc! {"listIndexes": "c"}).await;
    assert_eq!(names(&r), ["_id_", "a_1", "ta"]);
    let e = b
        .run("d", doc! {"explain": {"find": "c", "filter": {"a": 5}}})
        .await;
    assert!(e.to_string().contains("a_1"), "{e}");
    // The same command again: nothing to do.
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a_1"}]},
        )
        .await;
    assert_eq!(r.get_str("note").unwrap(), "all indexes already exist");
    // A bad document fails the build and leaves nothing behind.
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "c", "indexes": [{"key": {"x": 1, "y": 1}, "name": "xy"}]},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    b.run(
        "d",
        doc! {"insert": "p", "documents": [{"_id": 1, "x": [1, 2], "y": [3, 4]}]},
    )
    .await;
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "p", "indexes": [{"key": {"x": 1, "y": 1}, "name": "xy"}]},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 171, "{r}");
    let r = b.run("d", doc! {"listIndexes": "p"}).await;
    assert_eq!(names(&r), ["_id_"]);
    check_indexes(&b, "d").await;
}

/// A build left `building` (as after a crash) is hidden, kept up to date,
/// and finished by the same command.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_left_build_is_finished_by_the_same_command() {
    let b = Bed::mem();
    for i in 0..300 {
        b.run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": i, "a": i % 10}]},
        )
        .await;
    }
    b.run(
        "d",
        doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a_1"}]},
    )
    .await;
    // Mark it building again, as a crash mid-build would leave it.
    b.store
        .mark_building_for_tests(common::TENANT, "d", "c", "a_1")
        .await;
    let r = b.run("d", doc! {"listIndexes": "c"}).await;
    assert_eq!(names(&r), ["_id_"]);
    let r = b
        .run("d", doc! {"find": "c", "filter": {"a": 1}, "hint": "a_1"})
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 2, "{r}");
    b.run(
        "d",
        doc! {"insert": "c", "documents": [{"_id": 1000, "a": 1}]},
    )
    .await;
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a_1"}]},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b.run("d", doc! {"listIndexes": "c"}).await;
    assert_eq!(names(&r), ["_id_", "a_1"]);
    check_indexes(&b, "d").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_write_across_collections() {
    let b = Bed::mem();
    b.run(
        "d",
        doc! {"createIndexes": "c", "indexes": [{"key": {"u": 1}, "name": "u_1", "unique": true}]},
    )
    .await;
    let r = b
        .run(
            "admin",
            doc! {
                "bulkWrite": 1,
                "ops": [
                    {"insert": 0, "document": {"_id": 1, "u": 1}},
                    {"insert": 0, "document": {"_id": 2, "u": 1}},
                    {"update": 1, "filter": {"_id": 5}, "updateMods": {"$set": {"x": 1}}, "upsert": true},
                    {"update": 0, "filter": {"_id": 1}, "updateMods": {"$set": {"x": 1}}},
                    {"delete": 0, "filter": {"_id": 1}},
                ],
                "nsInfo": [{"ns": "d.c"}, {"ns": "e.f"}],
                "ordered": false,
            },
        )
        .await;
    let batch = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 5, "{r}");
    let e = batch[1].as_document().unwrap();
    assert_eq!(e.get_i32("code").unwrap(), 11000);
    assert_eq!(e.get_document("keyPattern").unwrap(), &doc! {"u": 1});
    assert!(!e.contains_key("codeName"));
    assert_eq!(
        batch[2]
            .as_document()
            .unwrap()
            .get_document("upserted")
            .unwrap(),
        &doc! {"_id": 5}
    );
    for (k, v) in [
        ("nErrors", 1),
        ("nInserted", 1),
        ("nMatched", 1),
        ("nModified", 1),
        ("nUpserted", 1),
        ("nDeleted", 1),
    ] {
        assert_eq!(r.get_i32(k).unwrap(), v, "{k} in {r}");
    }
    let r = b.run("e", doc! {"find": "f"}).await;
    assert_eq!(
        r.get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .len(),
        1
    );
    // Ordered stops at the first error; errorsOnly leaves out successes.
    let r = b
        .run(
            "admin",
            doc! {"bulkWrite": 1, "ops": [{"insert": 0, "document": {"_id": 9}}, {"insert": 0, "document": {"_id": 9}}, {"insert": 0, "document": {"_id": 10}}],
                 "nsInfo": [{"ns": "d.c"}], "errorsOnly": true},
        )
        .await;
    let batch = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(batch.len(), 1, "{r}");
    assert_eq!(r.get_i32("nInserted").unwrap(), 1);
    // Only on admin.
    let r = b
        .run("d", doc! {"bulkWrite": 1, "ops": [{"insert": 0, "document": {}}], "nsInfo": [{"ns": "d.c"}]})
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 13);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_statistics() {
    let b = Bed::mem();
    b.run(
        "d",
        doc! {"insert": "c", "documents": [{"_id": 1, "a": 1}, {"_id": 2, "a": 2, "b": [1, 2]}]},
    )
    .await;
    b.run(
        "d",
        doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a_1"}]},
    )
    .await;
    b.run("d", doc! {"find": "c", "filter": {"a": 1}}).await;
    let r = b
        .run("d", doc! {"aggregate": "c", "pipeline": [{"$collStats": {"storageStats": {}, "count": {}}}], "cursor": {}})
        .await;
    let d = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()[0]
        .as_document()
        .unwrap()
        .clone();
    assert_eq!(d.get_i32("count").unwrap(), 2, "{d}");
    let s = d.get_document("storageStats").unwrap();
    assert_eq!(s.get_i32("count").unwrap(), 2);
    assert_eq!(s.get_i32("size").unwrap(), 64, "{s}");
    assert_eq!(s.get_i32("nindexes").unwrap(), 2);
    assert!(
        s.get_document("indexSizes")
            .unwrap()
            .get_i32("a_1")
            .unwrap()
            > 0
    );
    let r = b
        .run("d", doc! {"aggregate": "c", "pipeline": [{"$indexStats": {}}, {"$sort": {"name": 1}}], "cursor": {}})
        .await;
    let a = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    assert_eq!(a.len(), 2, "{r}");
    let a1 = a[1].as_document().unwrap();
    assert_eq!(a1.get_str("name").unwrap(), "a_1");
    assert_eq!(
        a1.get_document("accesses").unwrap().get_i64("ops").unwrap(),
        1,
        "{a1}"
    );
    let r = b
        .run(
            "d",
            doc! {"aggregate": "nope", "pipeline": [{"$collStats": {"count": {}}}], "cursor": {}},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 26, "{r}");
    let r = b
        .run(
            "d",
            doc! {"aggregate": "c", "pipeline": [{"$collStats": {"bogus": 1}}], "cursor": {}},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 40415, "{r}");
    let r = b.run("d", doc! {"collStats": "c"}).await;
    assert_eq!(r.get_i32("count").unwrap(), 2, "{r}");
    let r = b.run("d", doc! {"dbStats": 1}).await;
    assert_eq!(r.get_i64("objects").unwrap(), 2, "{r}");
    assert_eq!(r.get_i64("indexes").unwrap(), 2, "{r}");
}

/// The plan `explain` reports: the index name, or the stage.
async fn plan_of(b: &Bed, find: Document) -> String {
    let r = b.run("d", doc! {"explain": find}).await;
    let w = r
        .get_document("queryPlanner")
        .and_then(|q| q.get_document("winningPlan"))
        .unwrap_or_else(|_| panic!("{r}"));
    match w.get_document("inputStage") {
        Ok(i) => i.get_str("indexName").unwrap().to_owned(),
        Err(_) => w.get_str("stage").unwrap().to_owned(),
    }
}

/// A collection's default collation applies to its reads, writes and new
/// indexes; a collated index serves reads under its collation (and
/// number bounds under any), and a unique one refuses collation-equal
/// strings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collated_indexes_and_default_collation() {
    let b = Bed::mem();
    let r = b
        .run(
            "d",
            doc! {"create": "ci", "collation": {"locale": "en", "strength": 2}},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b
        .run("d", doc! {"listCollections": 1, "filter": {"name": "ci"}})
        .await;
    let first = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()[0]
        .as_document()
        .unwrap()
        .clone();
    let c = first
        .get_document("options")
        .unwrap()
        .get_document("collation")
        .unwrap();
    assert_eq!(c.get_i32("strength").unwrap(), 2, "{first}");
    assert_eq!(c.get_str("version").unwrap(), "57.1");
    b.run(
        "d",
        doc! {"insert": "ci", "documents": [
            {"_id": 1, "s": "apple"}, {"_id": 2, "s": "Apple"}, {"_id": 3, "s": "APPLE"},
            {"_id": 4, "s": "banana"}, {"_id": 5, "s": 10}, {"_id": 6, "s": "\u{e1}pple"},
        ]},
    )
    .await;
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "ci", "indexes": [{"key": {"s": 1}, "name": "s_1"}]},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b.run("d", doc! {"listIndexes": "ci"}).await;
    let spec = r
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()[1]
        .as_document()
        .unwrap()
        .clone();
    assert_eq!(
        spec.get_document("collation")
            .unwrap()
            .get_str("locale")
            .unwrap(),
        "en",
        "{spec}"
    );
    let ids = |v: Vec<Document>| {
        let mut x = common::ids(&v);
        x.sort_unstable();
        x
    };
    // The default collation: strength 2 ignores case, not accents.
    let f = doc! {"find": "ci", "filter": {"s": "APPLE"}};
    assert_eq!(ids(b.find_all("d", f.clone()).await), [1, 2, 3]);
    assert_eq!(plan_of(&b, f).await, "s_1");
    // The simple collation: an exact match, not through the index.
    let f = doc! {"find": "ci", "filter": {"s": "APPLE"}, "collation": {"locale": "simple"}};
    assert_eq!(ids(b.find_all("d", f.clone()).await), [3]);
    assert_eq!(plan_of(&b, f).await, "COLLSCAN");
    // A number bound uses the collated index under any collation.
    let f = doc! {"find": "ci", "filter": {"s": 10}, "collation": {"locale": "simple"}};
    assert_eq!(ids(b.find_all("d", f.clone()).await), [5]);
    assert_eq!(plan_of(&b, f).await, "s_1");
    // Ranges and sorts follow the collation.
    let got = common::ids(
        &b.find_all(
            "d",
            doc! {"find": "ci", "filter": {"s": {"$gte": "APPLE", "$lt": "b"}}, "sort": {"s": 1, "_id": 1}},
        )
        .await,
    );
    assert_eq!(got, [1, 2, 3, 6]);
    let r = b
        .run(
            "d",
            doc! {"delete": "ci", "deletes": [{"q": {"s": "apple"}, "limit": 0}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 3, "{r}");
    // A unique collated index.
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "u", "indexes": [{"key": {"u": 1}, "name": "u_ci", "unique": true,
                "collation": {"locale": "en", "strength": 1}}]},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b
        .run(
            "d",
            doc! {"insert": "u", "documents": [{"_id": 1, "u": "Cafe"}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    let r = b
        .run(
            "d",
            doc! {"insert": "u", "documents": [{"_id": 2, "u": "caf\u{e9}"}]},
        )
        .await;
    let e = r.get_array("writeErrors").unwrap()[0]
        .as_document()
        .unwrap();
    assert_eq!(e.get_i32("code").unwrap(), 11000, "{r}");
    // Two indexes on one key with different collations are two indexes.
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "u", "indexes": [{"key": {"u": 1}, "name": "u_1"}]},
        )
        .await;
    assert_eq!(r.get_i32("numIndexesAfter").unwrap(), 3, "{r}");
    check_indexes(&b, "d").await;
}

/// A unique index builds online: writers keep running and are never
/// refused for the index being built; duplicates (already there, or
/// written during the build) fail the build unless a later change removed
/// them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_indexes_build_online() {
    let b = Arc::new(Bed::mem());
    let n = 5_000;
    for chunk in (0..n).collect::<Vec<i32>>().chunks(1000) {
        let docs: Vec<Bson> = chunk
            .iter()
            .map(|i| Bson::Document(doc! {"_id": *i, "u": *i}))
            .collect();
        b.run("d", doc! {"insert": "c", "documents": docs}).await;
    }
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let mut hs = Vec::new();
    for w in 0..2i32 {
        let (b, stop, writes) = (Arc::clone(&b), Arc::clone(&stop), Arc::clone(&writes));
        hs.push(tokio::spawn(async move {
            let mut i = 0i32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let fresh = n + (w + 1) * 1_000_000 + i;
                let r = match i % 3 {
                    0 => {
                        b.run("d", doc! {"insert": "c", "documents": [{"_id": fresh, "u": fresh}]})
                            .await
                    }
                    1 => {
                        let id = (i * 7919 + w) % n;
                        b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": id}, "u": {"$set": {"u": -fresh}}}]})
                            .await
                    }
                    _ => {
                        let id = (i * 104_729 + w) % n;
                        b.run("d", doc! {"delete": "c", "deletes": [{"q": {"_id": id}, "limit": 1}]})
                            .await
                    }
                };
                assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
                assert!(r.get_array("writeErrors").is_err(), "{r}");
                writes.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let before = writes.load(Ordering::Relaxed);
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "c", "indexes": [{"key": {"u": 1}, "name": "u_1", "unique": true}]},
        )
        .await;
    assert_eq!(r.get_i32("numIndexesAfter").unwrap(), 2, "{r}");
    let during = writes.load(Ordering::Relaxed) - before;
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.await.unwrap();
    }
    assert!(during > 0, "writers were stopped by the build");
    check_indexes(&b, "d").await;
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": -1, "u": 1_000_000_000}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": -2, "u": 1_000_000_000}]},
        )
        .await;
    assert!(r.get_array("writeErrors").is_ok(), "{r}");

    // A duplicate already there fails the build; nothing is left.
    b.run(
        "d",
        doc! {"insert": "q", "documents": [{"_id": 1, "u": 1}, {"_id": 2, "u": 1}]},
    )
    .await;
    let unique = doc! {"key": {"u": 1}, "name": "u_1", "unique": true};
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "q", "indexes": [unique.clone()]},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 11000, "{r}");
    let r = b.run("d", doc! {"listIndexes": "q"}).await;
    assert_eq!(names(&r), ["_id_"]);

    // A duplicate written during the build is accepted; the build fails.
    for coll in ["r", "s"] {
        b.run(
            "d",
            doc! {"insert": coll, "documents": [{"_id": 1, "u": 1}]},
        )
        .await;
        b.run(
            "d",
            doc! {"createIndexes": coll, "indexes": [unique.clone()]},
        )
        .await;
        b.store
            .mark_building_for_tests(common::TENANT, "d", coll, "u_1")
            .await;
        let r = b
            .run(
                "d",
                doc! {"insert": coll, "documents": [{"_id": 2, "u": 1}]},
            )
            .await;
        assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    }
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "r", "indexes": [unique.clone()]},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 11000, "{r}");
    let r = b.run("d", doc! {"listIndexes": "r"}).await;
    assert_eq!(names(&r), ["_id_"]);
    let r = b.run("d", doc! {"count": "r"}).await;
    assert_eq!(r.get_i32("n").unwrap(), 2, "{r}");
    // ... unless the first document leaves before the build ends.
    b.run(
        "d",
        doc! {"delete": "s", "deletes": [{"q": {"_id": 1}, "limit": 1}]},
    )
    .await;
    let r = b
        .run("d", doc! {"createIndexes": "s", "indexes": [unique]})
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b.run("d", doc! {"listIndexes": "s"}).await;
    assert_eq!(names(&r), ["_id_", "u_1"]);
    check_indexes(&b, "d").await;
    let r = b
        .run("d", doc! {"insert": "s", "documents": [{"_id": 3, "u": 1}]})
        .await;
    let e = r.get_array("writeErrors").unwrap()[0]
        .as_document()
        .unwrap();
    assert_eq!(e.get_i32("code").unwrap(), 11000, "{r}");
}

fn session(n: u8) -> Document {
    doc! {"id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![n; 16] })}
}

fn coll_names(r: &Document) -> Vec<String> {
    let mut v = names(r);
    v.sort();
    v
}

/// Collections and indexes a transaction creates are left out of other
/// sessions' listings until the commit; an abort drops them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_in_a_transaction_is_seen_at_commit() {
    let b = Bed::mem();
    b.run("d", doc! {"insert": "old", "documents": [{"_id": 1}]})
        .await;
    let t = |n: i64, start: bool| {
        let mut d = doc! {"lsid": session(1), "txnNumber": n, "autocommit": false};
        if start {
            d.insert("startTransaction", true);
        }
        d
    };
    let with = |mut cmd: Document, extra: Document| {
        cmd.extend(extra);
        cmd
    };
    let r = b.run("d", with(doc! {"create": "made"}, t(1, true))).await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b
        .run(
            "d",
            with(
                doc! {"insert": "implicit", "documents": [{"_id": 1}]},
                t(1, false),
            ),
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    let r = b
        .run(
            "d",
            with(
                doc! {"createIndexes": "made", "indexes": [{"key": {"x": 1}, "name": "x_1"}]},
                t(1, false),
            ),
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b.run("d", doc! {"listCollections": 1}).await;
    assert_eq!(coll_names(&r), ["old"]);
    let r = b.run("d", doc! {"listIndexes": "made"}).await;
    assert_eq!(r.get_i32("code").unwrap(), 26, "{r}");
    let r = b
        .run(
            "d",
            doc! {"commitTransaction": 1, "lsid": session(1), "txnNumber": 1i64, "autocommit": false},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    let r = b.run("d", doc! {"listCollections": 1}).await;
    assert_eq!(coll_names(&r), ["implicit", "made", "old"]);
    let r = b.run("d", doc! {"listIndexes": "made"}).await;
    assert_eq!(names(&r), ["_id_", "x_1"]);
    // An index on an empty collection, then an abort: dropped again.
    b.run("d", doc! {"create": "empty"}).await;
    b.run(
        "d",
        with(
            doc! {"createIndexes": "empty", "indexes": [{"key": {"y": 1}, "name": "y_1"}]},
            t(2, true),
        ),
    )
    .await;
    let r = b.run("d", doc! {"listIndexes": "empty"}).await;
    assert_eq!(names(&r), ["_id_"]);
    b.run(
        "d",
        doc! {"abortTransaction": 1, "lsid": session(1), "txnNumber": 2i64, "autocommit": false},
    )
    .await;
    let r = b.run("d", doc! {"listIndexes": "empty"}).await;
    assert_eq!(names(&r), ["_id_"]);
}

/// `$lookup` with `localField`/`foreignField` and a `pipeline`: the
/// pipeline runs on the documents the equality matched, so it may drop the
/// foreign field (MongoDB 8.0's answer).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_with_fields_and_a_pipeline() {
    let b = Bed::mem();
    b.run(
        "d",
        doc! {"insert": "oo", "documents": [{"_id": 10, "cid": 1}, {"_id": 11, "cid": null}]},
    )
    .await;
    b.run(
        "d",
        doc! {"insert": "cc", "documents": [{"_id": 1, "id": 1, "name": "A"}, {"_id": 2, "id": 2, "name": "B"}]},
    )
    .await;
    let got = b
        .find_all(
            "d",
            doc! {"aggregate": "oo", "cursor": {}, "pipeline": [
                {"$lookup": {"from": "cc", "localField": "cid", "foreignField": "id",
                    "pipeline": [{"$project": {"_id": 0, "name": 1}}], "as": "c"}},
                {"$sort": {"_id": 1}},
            ]},
        )
        .await;
    assert_eq!(
        got,
        [
            doc! {"_id": 10, "cid": 1, "c": [{"name": "A"}]},
            doc! {"_id": 11, "cid": null, "c": []},
        ]
    );
}
