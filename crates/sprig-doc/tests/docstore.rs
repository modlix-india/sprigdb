//! The engine document store, through its commands: CRUD, every index
//! kind it stores, cursors, transactions, retryable writes, TTL,
//! aggregation, and recovery after a crash.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::unused_async,
    clippy::map_entry,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::sync::Arc;

use bson::{Bson, Document, doc};
use common::{Bed, ids, rocks_store};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crud_and_cursors() {
    let b = Bed::mem();
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": 1, "a": 1}, {"_id": 2, "a": 2}, {"_id": 1}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 2);
    assert_eq!(
        r.get_array("writeErrors").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("code")
            .unwrap(),
        11000
    );
    // Numbers equal as values are one `_id`.
    let r = b
        .run("d", doc! {"insert": "c", "documents": [{"_id": 1.0}]})
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 0);
    for i in 3..=300 {
        b.run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": i, "a": i % 7}]},
        )
        .await;
    }
    let all = b.find_all("d", doc! {"find": "c", "batchSize": 10}).await;
    assert_eq!(all.len(), 300);
    assert_eq!(ids(&all)[..3], [1, 2, 3]);
    let desc = b
        .find_all("d", doc! {"find": "c", "sort": {"_id": -1}, "limit": 5})
        .await;
    assert_eq!(ids(&desc), vec![300, 299, 298, 297, 296]);
    let r = b
        .run("d", doc! {"update": "c", "updates": [{"q": {"a": 3}, "u": {"$inc": {"b": 1}}, "multi": true}]})
        .await;
    let n = all.iter().filter(|d| d.get_i32("a").unwrap() == 3).count();
    assert_eq!(r.get_i32("n").unwrap() as usize, n);
    assert_eq!(r.get_i32("nModified").unwrap() as usize, n);
    let r = b
        .run("d", doc! {"update": "c", "updates": [{"q": {"_id": 999}, "u": {"$set": {"x": 1}}, "upsert": true}]})
        .await;
    assert_eq!(
        r.get_array("upserted").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("_id")
            .unwrap(),
        999
    );
    let r = b.run("d", doc! {"count": "c", "query": {"a": 3}}).await;
    assert_eq!(r.get_i32("n").unwrap() as usize, n);
    let r = b
        .run(
            "d",
            doc! {"delete": "c", "deletes": [{"q": {"a": 3}, "limit": 0}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap() as usize, n);
    let r = b
        .run("d", doc! {"findAndModify": "c", "query": {"_id": 1}, "update": {"$set": {"z": 9}}, "new": true})
        .await;
    assert_eq!(r.get_document("value").unwrap().get_i32("z").unwrap(), 9);
    let r = b.run("d", doc! {"distinct": "c", "key": "a"}).await;
    assert_eq!(r.get_array("values").unwrap().len(), 6);
    let r = b.run("d", doc! {"drop": "c"}).await;
    assert_eq!(r.get_i32("nIndexesWas").unwrap(), 1);
    assert!(b.find_all("d", doc! {"find": "c"}).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexes_unique_sparse_partial_multikey() {
    let b = Bed::mem();
    b.run(
        "d",
        doc! {"insert": "c", "documents": [{"_id": 1, "e": "x"}, {"_id": 2, "e": "x"}]},
    )
    .await;
    let r = b
        .run("d", doc! {"createIndexes": "c", "indexes": [{"key": {"e": 1}, "name": "e_1", "unique": true}]})
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 11000, "{r}");
    b.run(
        "d",
        doc! {"delete": "c", "deletes": [{"q": {"_id": 2}, "limit": 1}]},
    )
    .await;
    let r = b
        .run("d", doc! {"createIndexes": "c", "indexes": [{"key": {"e": 1}, "name": "e_1", "unique": true}]})
        .await;
    assert_eq!(r.get_i32("numIndexesAfter").unwrap(), 2, "{r}");
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": 3, "e": "x"}]},
        )
        .await;
    let we = r.get_array("writeErrors").unwrap()[0]
        .as_document()
        .unwrap()
        .clone();
    assert_eq!(we.get_i32("code").unwrap(), 11000);
    assert_eq!(we.get_document("keyPattern").unwrap(), &doc! {"e": 1});
    // Missing is null: one document without `e`, not two.
    b.run("d", doc! {"insert": "c", "documents": [{"_id": 4}]})
        .await;
    let r = b
        .run("d", doc! {"insert": "c", "documents": [{"_id": 5}]})
        .await;
    assert!(r.get_array("writeErrors").is_ok());
    // Changing the value frees the old key.
    b.run(
        "d",
        doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$set": {"e": "y"}}}]},
    )
    .await;
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": 6, "e": "x"}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    // Sparse unique: many documents without the field.
    b.run("d", doc! {"createIndexes": "s", "indexes": [{"key": {"k": 1}, "name": "k", "unique": true, "sparse": true}]}).await;
    let r = b
        .run(
            "d",
            doc! {"insert": "s", "documents": [{"_id": 1}, {"_id": 2}, {"_id": 3, "k": 1}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 3);
    // Partial unique: only matching documents are unique.
    b.run("d", doc! {"createIndexes": "p", "indexes": [{"key": {"k": 1}, "name": "k", "unique": true, "partialFilterExpression": {"live": true}}]}).await;
    let r = b.run("d", doc! {"insert": "p", "documents": [{"_id": 1, "k": 1}, {"_id": 2, "k": 1}, {"_id": 3, "k": 1, "live": true}, {"_id": 4, "k": 1, "live": true}]}).await;
    assert_eq!(r.get_i32("n").unwrap(), 3, "{r}");
    // Multikey: found through any element, once each.
    b.run(
        "d",
        doc! {"createIndexes": "m", "indexes": [{"key": {"t": 1}, "name": "t"}]},
    )
    .await;
    b.run("d", doc! {"insert": "m", "documents": [{"_id": 1, "t": [1, 2, 3]}, {"_id": 2, "t": [3, 4]}, {"_id": 3, "t": 5}]}).await;
    let got = b
        .find_all("d", doc! {"find": "m", "filter": {"t": {"$gte": 3}}})
        .await;
    let mut g = ids(&got);
    g.sort_unstable();
    assert_eq!(g, vec![1, 2, 3]);
    let got = b
        .find_all(
            "d",
            doc! {"find": "m", "filter": {"t": {"$gt": 2, "$lt": 2}}},
        )
        .await;
    assert_eq!(ids(&got), vec![1], "multikey bounds are not intersected");
    let r = b
        .run(
            "d",
            doc! {"insert": "m2", "documents": [{"_id": 1, "a": [1], "b": [2]}]},
        )
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 1);
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "m2", "indexes": [{"key": {"a": 1, "b": 1}, "name": "ab"}]},
        )
        .await;
    assert_eq!(r.get_i32("code").unwrap(), 171, "{r}");
    // Compound with a descending field serves sorts both ways.
    b.run(
        "d",
        doc! {"createIndexes": "o", "indexes": [{"key": {"g": 1, "v": -1}, "name": "gv"}]},
    )
    .await;
    for i in 0..50 {
        b.run(
            "d",
            doc! {"insert": "o", "documents": [{"_id": i, "g": i % 3, "v": i}]},
        )
        .await;
    }
    let got = b
        .find_all(
            "d",
            doc! {"find": "o", "filter": {"g": 1}, "sort": {"v": -1}, "batchSize": 4},
        )
        .await;
    let want: Vec<i32> = (0..50).rev().filter(|i| i % 3 == 1).collect();
    assert_eq!(ids(&got), want);
    let got = b
        .find_all(
            "d",
            doc! {"find": "o", "filter": {"g": 1, "v": {"$lt": 20}}, "sort": {"v": 1}},
        )
        .await;
    let want: Vec<i32> = (0..20).filter(|i| i % 3 == 1).collect();
    assert_eq!(ids(&got), want);
    let ex = b
        .run("d", doc! {"explain": {"find": "o", "filter": {"g": 1}}})
        .await;
    assert_eq!(
        ex.get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap()
            .get_document("inputStage")
            .unwrap()
            .get_str("indexName")
            .unwrap(),
        "gv"
    );
    // `$or` as a union of index reads; a regex prefix as a range.
    let ex = b
        .run(
            "d",
            doc! {"explain": {"find": "o", "filter": {"$or": [{"g": 1}, {"_id": 3}]}}},
        )
        .await;
    assert_eq!(
        ex.get_document("queryPlanner")
            .unwrap()
            .get_document("winningPlan")
            .unwrap()
            .get_str("stage")
            .unwrap(),
        "OR",
        "{ex}"
    );
    let got = b.find_all("d", doc! {"find": "o", "filter": {"$or": [{"g": 1}, {"_id": 3}, {"_id": 4}]}, "batchSize": 3}).await;
    let mut ids_got = ids(&got);
    ids_got.sort_unstable();
    let mut want: Vec<i32> = (0..50).filter(|i| i % 3 == 1 || *i == 3).collect();
    want.sort_unstable();
    assert_eq!(ids_got, want);
    b.run(
        "d",
        doc! {"createIndexes": "r", "indexes": [{"key": {"s": 1}, "name": "s"}]},
    )
    .await;
    b.run("d", doc! {"insert": "r", "documents": [{"_id": 1, "s": "apple"}, {"_id": 2, "s": "apricot"}, {"_id": 3, "s": "banana"}, {"_id": 4, "s": ["x", "apex"]}]}).await;
    let got = b
        .find_all("d", doc! {"find": "r", "filter": {"s": {"$regex": "^ap"}}})
        .await;
    let mut g = ids(&got);
    g.sort_unstable();
    assert_eq!(g, vec![1, 2, 4]);
    let ex = b
        .run(
            "d",
            doc! {"explain": {"find": "r", "filter": {"s": {"$regex": "^ap"}}}},
        )
        .await;
    assert!(format!("{ex}").contains("IXSCAN"), "{ex}");
    // Hashed and wildcard.
    b.run("d", doc! {"createIndexes": "h", "indexes": [{"key": {"k": "hashed"}, "name": "kh"}, {"key": {"$**": 1}, "name": "w"}]}).await;
    b.run("d", doc! {"insert": "h", "documents": [{"_id": 1, "k": 1, "x": {"y": 2}}, {"_id": 2, "k": 1.0}, {"_id": 3, "k": 2}]}).await;
    let got = b
        .find_all("d", doc! {"find": "h", "filter": {"k": 1}, "hint": "kh"})
        .await;
    assert_eq!(ids(&got), vec![1, 2]);
    let got = b
        .find_all("d", doc! {"find": "h", "filter": {"x.y": 2}, "hint": "w"})
        .await;
    assert_eq!(ids(&got), vec![1]);
    let r = b
        .run(
            "d",
            doc! {"insert": "h", "documents": [{"_id": 4, "k": [1]}]},
        )
        .await;
    assert_eq!(
        r.get_array("writeErrors").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("code")
            .unwrap(),
        16766
    );
    // Refused kinds keep their errors.
    let r = b
        .run(
            "d",
            doc! {"createIndexes": "h", "indexes": [{"key": {"t": "text"}, "name": "t"}]},
        )
        .await;
    assert_eq!(
        r.get_i32("code").unwrap(),
        sprig_proto_mongo::errors::code::SPRIG_DOC_FEATURE_UNSUPPORTED
    );
    let r = b.run("d", doc! {"listIndexes": "h"}).await;
    assert_eq!(
        r.get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .len(),
        3
    );
    let r = b.run("d", doc! {"dropIndexes": "h", "index": "kh"}).await;
    assert_eq!(r.get_i32("nIndexesWas").unwrap(), 3);
    common::check_indexes(&b, "d").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_commit_abort_conflict() {
    let b = Bed::mem();
    b.run("d", doc! {"insert": "c", "documents": [{"_id": 1, "n": 0}]})
        .await;
    let lsid = |n: u8| doc! {"id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![n; 16] })};
    // A transaction sees its own writes; others don't until commit.
    let r = b.run("d", doc! {"insert": "c", "documents": [{"_id": 2}], "lsid": lsid(1), "txnNumber": 1i64, "autocommit": false, "startTransaction": true}).await;
    assert_eq!(r.get_i32("n").unwrap(), 1, "{r}");
    let r = b
        .run(
            "d",
            doc! {"find": "c", "lsid": lsid(1), "txnNumber": 1i64, "autocommit": false},
        )
        .await;
    assert_eq!(
        r.get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(b.find_all("d", doc! {"find": "c"}).await.len(), 1);
    let r = b
        .run(
            "admin",
            doc! {"commitTransaction": 1, "lsid": lsid(1), "txnNumber": 1i64, "autocommit": false},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    assert_eq!(b.find_all("d", doc! {"find": "c"}).await.len(), 2);
    // Abort undoes.
    b.run("d", doc! {"delete": "c", "deletes": [{"q": {}, "limit": 0}], "lsid": lsid(1), "txnNumber": 2i64, "autocommit": false, "startTransaction": true}).await;
    b.run(
        "admin",
        doc! {"abortTransaction": 1, "lsid": lsid(1), "txnNumber": 2i64, "autocommit": false},
    )
    .await;
    assert_eq!(b.find_all("d", doc! {"find": "c"}).await.len(), 2);
    // Two transactions writing one document: the second conflicts at once.
    b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}], "lsid": lsid(2), "txnNumber": 1i64, "autocommit": false, "startTransaction": true}).await;
    let r = b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}], "lsid": lsid(3), "txnNumber": 1i64, "autocommit": false, "startTransaction": true}).await;
    assert_eq!(r.get_i32("code").unwrap(), 112, "{r}");
    assert!(format!("{r}").contains("TransientTransactionError"));
    b.run(
        "admin",
        doc! {"commitTransaction": 1, "lsid": lsid(2), "txnNumber": 1i64, "autocommit": false},
    )
    .await;
    // A snapshot older than a committed change can't write that document.
    b.run("d", doc! {"find": "c", "lsid": lsid(4), "txnNumber": 1i64, "autocommit": false, "startTransaction": true}).await;
    b.run(
        "d",
        doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}]},
    )
    .await;
    let r = b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}], "lsid": lsid(4), "txnNumber": 1i64, "autocommit": false}).await;
    assert_eq!(r.get_i32("code").unwrap(), 112, "{r}");
    let d = b
        .find_all("d", doc! {"find": "c", "filter": {"_id": 1}})
        .await;
    assert_eq!(d[0].get_i32("n").unwrap(), 2);
    let r = b.run("d", doc! {"count": "c", "lsid": lsid(5), "txnNumber": 1i64, "autocommit": false, "startTransaction": true}).await;
    assert_eq!(r.get_i32("code").unwrap(), 263);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retryable_writes_apply_once() {
    let b = Bed::mem();
    let l = doc! {"id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: vec![9; 16] })};
    let cmd = doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}, "upsert": true}], "lsid": l.clone(), "txnNumber": 7i64};
    let a = b.run("d", cmd.clone()).await;
    let again = b.run("d", cmd).await;
    assert_eq!(a, again);
    let d = b.find_all("d", doc! {"find": "c"}).await;
    assert_eq!(d[0].get_i32("n").unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_sweeper_deletes_expired() {
    let b = Bed::mem();
    b.store.set_clock(Some(1_000_000));
    b.run("d", doc! {"createIndexes": "t", "indexes": [{"key": {"at": 1}, "name": "at", "expireAfterSeconds": 10}]}).await;
    let at = |ms: i64| Bson::DateTime(bson::DateTime::from_millis(ms));
    b.run("d", doc! {"insert": "t", "documents": [{"_id": 1, "at": at(900_000)}, {"_id": 2, "at": at(995_000)}, {"_id": 3, "at": "not a date"}, {"_id": 4, "at": [at(2_000_000), at(100)]}]}).await;
    let n = b.store.sweep_ttl(100).await.unwrap();
    assert_eq!(n, 2);
    let left = b.find_all("d", doc! {"find": "t"}).await;
    assert_eq!(ids(&left), vec![2, 3]);
    b.store.set_clock(Some(1_006_000));
    assert_eq!(b.store.sweep_ttl(100).await.unwrap(), 1);
    common::check_indexes(&b, "d").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregation_over_the_store() {
    let b = Bed::mem();
    b.run("d", doc! {"insert": "o", "documents": [{"_id": 1, "c": 1, "v": 5}, {"_id": 2, "c": 2, "v": 7}, {"_id": 3, "c": 1, "v": 1}]}).await;
    b.run(
        "d",
        doc! {"insert": "u", "documents": [{"_id": 1, "name": "a"}, {"_id": 2, "name": "b"}]},
    )
    .await;
    let r = b
        .find_all_cmd(
            "d",
            doc! {"aggregate": "o", "pipeline": [
                {"$match": {"v": {"$gt": 1}}},
                {"$lookup": {"from": "u", "localField": "c", "foreignField": "_id", "as": "u"}},
                {"$group": {"_id": {"$first": "$u.name"}, "total": {"$sum": "$v"}}},
                {"$sort": {"_id": 1}},
            ], "cursor": {}},
        )
        .await;
    assert_eq!(
        r,
        vec![doc! {"_id": "a", "total": 5}, doc! {"_id": "b", "total": 7}]
    );
    let r = b.find_all_cmd("d", doc! {"aggregate": "o", "pipeline": [
        {"$lookup": {"from": "u", "let": {"c": "$c"}, "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$c"]}}}], "as": "u"}},
        {"$project": {"n": {"$size": "$u"}}},
    ], "cursor": {}}).await;
    assert!(r.iter().all(|d| d.get_i32("n").unwrap() == 1), "{r:?}");
    b.find_all_cmd(
        "d",
        doc! {"aggregate": "o", "pipeline": [{"$match": {"c": 1}}, {"$out": "o2"}], "cursor": {}},
    )
    .await;
    assert_eq!(b.find_all("d", doc! {"find": "o2"}).await.len(), 2);
    b.find_all_cmd("d", doc! {"aggregate": "o", "pipeline": [{"$project": {"v": 1}}, {"$merge": {"into": "o2", "whenMatched": "merge"}}], "cursor": {}}).await;
    assert_eq!(b.find_all("d", doc! {"find": "o2"}).await.len(), 3);
    let r = b.find_all_cmd("d", doc! {"aggregate": "o", "pipeline": [{"$unionWith": "u"}, {"$count": "n"}], "cursor": {}}).await;
    assert_eq!(r, vec![doc! {"n": 5}]);
    let r = b.find_all_cmd("d", doc! {"aggregate": "o", "pipeline": [{"$graphLookup": {"from": "o", "startWith": "$c", "connectFromField": "c", "connectToField": "_id", "as": "g"}}, {"$project": {"k": {"$size": "$g"}}}], "cursor": {}}).await;
    assert_eq!(r.len(), 3);
}

/// Acknowledged writes survive a restart that loses RocksDB's memtables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_after_a_crash() {
    let d = common::dir("recover");
    {
        let b = Bed::on(rocks_store(&d));
        b.run("d", doc! {"createIndexes": "c", "indexes": [{"key": {"a": 1}, "name": "a", "unique": true}, {"key": {"t": 1}, "name": "t"}]}).await;
        for i in 0..500 {
            b.run(
                "d",
                doc! {"insert": "c", "documents": [{"_id": i, "a": i, "t": [i, i + 1]}]},
            )
            .await;
        }
        b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": {"$lt": 100}}, "u": {"$set": {"flag": true}}, "multi": true}]}).await;
        b.run(
            "d",
            doc! {"delete": "c", "deletes": [{"q": {"_id": {"$gte": 400}}, "limit": 0}]},
        )
        .await;
    }
    let b = Bed::on(rocks_store(&d));
    // The TTL sweeper knows the tenant before anyone uses it.
    assert!(b.store.tenants().contains(&common::TENANT));
    let all = b.find_all("d", doc! {"find": "c"}).await;
    assert_eq!(all.len(), 400);
    let r = b
        .run("d", doc! {"count": "c", "query": {"flag": true}})
        .await;
    assert_eq!(r.get_i32("n").unwrap(), 100);
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [{"_id": 1000, "a": 150}]},
        )
        .await;
    assert_eq!(
        r.get_array("writeErrors").unwrap()[0]
            .as_document()
            .unwrap()
            .get_i32("code")
            .unwrap(),
        11000
    );
    common::check_indexes(&b, "d").await;
    drop(b);
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_increments_lose_nothing() {
    let b = Arc::new(Bed::mem());
    b.run("d", doc! {"insert": "c", "documents": [{"_id": 1, "n": 0}]})
        .await;
    let mut hs = Vec::new();
    for _ in 0..8 {
        let b = Arc::clone(&b);
        hs.push(tokio::spawn(async move {
            for _ in 0..50 {
                let r = b.run("d", doc! {"update": "c", "updates": [{"q": {"_id": 1}, "u": {"$inc": {"n": 1}}}]}).await;
                assert_eq!(r.get_i32("nModified").unwrap(), 1, "{r}");
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    let d: Vec<Document> = b.find_all("d", doc! {"find": "c"}).await;
    assert_eq!(d[0].get_i32("n").unwrap(), 400);
}
