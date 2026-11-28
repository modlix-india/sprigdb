//! `$graphLookup` on the engine document store, at the top of a pipeline
//! and inside a `$lookup`'s pipeline, against the answers MongoDB 8.0.34
//! gave for the same data (the order of each `as` array is not specified
//! by MongoDB, so the ids are compared as sets).

#![allow(clippy::unwrap_used, clippy::doc_markdown)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::collections::BTreeSet;

use bson::{Bson, Document, doc};
use common::Bed;

async fn seed(b: &Bed) {
    let r = b
        .run(
            "d",
            doc! {"insert": "c", "documents": [
                {"_id": 1, "owner": "a", "n": 1},
                {"_id": 2, "owner": "b", "n": 2},
                {"_id": 3, "owner": "a", "n": 300},
                {"_id": 4, "owner": "A", "n": 4},
                {"_id": 5, "owner": "a", "n": 5},
                {"_id": 6, "owner": "b", "n": 6},
            ]},
        )
        .await;
    assert_eq!(r.get_i32("n"), Ok(6), "{r}");
    let r = b
        .run(
            "d",
            doc! {"insert": "o", "documents": [{"_id": 10, "ref": 1}, {"_id": 11, "ref": 2}]},
        )
        .await;
    assert_eq!(r.get_i32("n"), Ok(2), "{r}");
}

fn ids_of(a: &Bson) -> BTreeSet<i32> {
    a.as_array()
        .unwrap()
        .iter()
        .map(|d| d.as_document().unwrap().get_i32("_id").unwrap())
        .collect()
}

/// Each output document's `_id` and the ids of its `g` array.
fn graph(docs: &[Document]) -> Vec<(i32, BTreeSet<i32>)> {
    docs.iter()
        .map(|d| (d.get_i32("_id").unwrap(), ids_of(d.get("g").unwrap())))
        .collect()
}

fn set(v: &[i32]) -> BTreeSet<i32> {
    v.iter().copied().collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_lookup_on_id_matches_mongodb() {
    let b = Bed::mem();
    seed(&b).await;
    let gl = |from: &str, start: Bson| {
        doc! {"$graphLookup": {"from": "c", "startWith": start,
        "connectFromField": from, "connectToField": "_id", "as": "g"}}
    };
    // MongoDB 8.0.34: [{_id: 10, g: [1]}, {_id: 11, g: [2]}] for both
    // `connectFromField`s.
    for from in ["n", "_id"] {
        let top = b
            .find_all(
                "d",
                doc! {"aggregate": "o", "cursor": {}, "pipeline": [gl(from, Bson::String("$ref".into()))]},
            )
            .await;
        assert_eq!(
            graph(&top),
            vec![(10, set(&[1])), (11, set(&[2]))],
            "top level, connectFromField {from}"
        );
        // Inside an uncorrelated $lookup's pipeline: every outer document
        // gets both inner ones, each with its own match.
        let nested = b
            .find_all(
                "d",
                doc! {"aggregate": "o", "cursor": {}, "pipeline": [
                    {"$lookup": {"from": "o", "as": "x", "pipeline": [gl(from, Bson::String("$ref".into()))]}},
                ]},
            )
            .await;
        assert_eq!(nested.len(), 2);
        for d in &nested {
            let x: Vec<Document> = d
                .get_array("x")
                .unwrap()
                .iter()
                .map(|v| v.as_document().unwrap().clone())
                .collect();
            assert_eq!(
                graph(&x),
                vec![(10, set(&[1])), (11, set(&[2]))],
                "inside $lookup, connectFromField {from}, outer {}",
                d.get_i32("_id").unwrap()
            );
        }
    }
    // MongoDB 8.0.34: startWith [1, 2, 5] -> g [1, 2, 5] for each document.
    let many = b
        .find_all(
            "d",
            doc! {"aggregate": "o", "cursor": {}, "pipeline": [gl("n", Bson::Array(vec![1.into(), 2.into(), 5.into()]))]},
        )
        .await;
    assert_eq!(
        graph(&many),
        vec![(10, set(&[1, 2, 5])), (11, set(&[1, 2, 5]))]
    );
}
