//! `$text`'s `textScore` against MongoDB 8.0: the same documents, text
//! indexes and searches on a gateway and on a reference server, every
//! score compared (to 1e-9) and every matched `_id`.
//!
//! - `SPRIG_TEST_MONGO_URI`: a gateway's MongoDB listener with a tenant
//!   login.
//! - `SPRIG_TEST_MONGO_REF_URI`: a private MongoDB 8.0.
//!
//! The `$**` collection also checks what such an index covers: a string
//! `_id` and a document `_id`'s strings (a document only they match is
//! found), not an object id `_id`, and not the language override
//! (`language`, at any level).
//!
//! Skipped unless both are set. Uses the database `text_score_live` on
//! both and drops it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc, oid::ObjectId};
use mongodb::{Client, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

/// Collections: (name, index key, index options, documents).
fn corpus() -> Vec<(&'static str, Document, Document, Vec<Document>)> {
    let texts = [
        "shoe",
        "Shoe",
        "shoes",
        "a shoe",
        "red shoe",
        "shoe shoe",
        "shoe shoe shoe",
        "red shoe blue",
        "don't shoe",
        "e-mail shoe",
        "café shoe",
        "running shoes run",
        "John's red shoes, and the runner's run.",
        "The quick brown fox jumps over the lazy dog",
        "foxes and dogs; a fox, a dog",
        "apple",
        "apple apple pie",
        "under_score words·and 3.14 numbers 42",
        "Résumé of a résumé writer",
        "the and of to",
    ];
    let plain: Vec<Document> = texts
        .iter()
        .enumerate()
        .map(|(i, t)| doc! {"_id": i as i32, "t": *t})
        .collect();
    let weighted = vec![
        doc! {"_id": 1, "a": "shoe", "b": "shoe shoe red"},
        doc! {"_id": 2, "a": "x y shoe", "b": "z"},
        doc! {"_id": 3, "a": ["shoe", "red shoe"], "b": "fox"},
        doc! {"_id": 4, "a": "fox", "b": ["red", "red fox"]},
    ];
    let nested = vec![
        doc! {"_id": 1, "a": "shoe", "n": {"b": "red shoe"}, "language": "english"},
        doc! {"_id": 2, "n": [{"b": "shoe"}, {"b": "fox shoe"}]},
        doc! {"_id": "shoe", "a": "red shoe"},
        doc! {"_id": "zebra crossing", "a": "x"},
        doc! {"_id": {"k": "walrus tusk"}, "a": "y"},
        doc! {"_id": ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap(), "a": "plum"},
        doc! {"_id": 9, "a": "kiwi", "language": "english"},
        doc! {"_id": 10, "n": {"language": "english", "b": "kiwi fruit"}},
    ];
    vec![
        ("plain", doc! {"t": "text"}, doc! {}, plain),
        (
            "weighted",
            doc! {"a": "text", "b": "text"},
            doc! {"weights": {"a": 10, "b": 2}},
            weighted,
        ),
        (
            "nested",
            doc! {"$**": "text"},
            doc! {"weights": {"a": 5}},
            nested,
        ),
    ]
}

const SEARCHES: [&str; 16] = [
    "shoe",
    "shoe red",
    "\"red shoe\"",
    "shoe -red",
    "run",
    "fox dog",
    "apple",
    "john",
    "resume",
    "numbers 42 under_score",
    "the",
    "zebra",
    "walrus",
    "65a1b2c3d4e5f60718293a4b",
    "english",
    "kiwi",
];

async fn scores(db: &Database, coll: &str, q: &str) -> Vec<(String, f64)> {
    let mut cur = db
        .collection::<Document>(coll)
        .find(doc! {"$text": {"$search": q}})
        .projection(doc! {"s": {"$meta": "textScore"}})
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(d) = cur.try_next().await.unwrap() {
        let id = match d.get("_id") {
            Some(Bson::String(s)) => s.clone(),
            Some(other) => other.to_string(),
            None => String::new(),
        };
        out.push((id, d.get_f64("s").unwrap_or(f64::NAN)));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_scores_match_mongodb() {
    let (Some(s), Some(r)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_REF_URI")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_REF_URI not set: skipped");
        return;
    };
    let sdb = Client::with_uri_str(&s)
        .await
        .unwrap()
        .database("text_score_live");
    let rdb = Client::with_uri_str(&r)
        .await
        .unwrap()
        .database("text_score_live");
    for db in [&sdb, &rdb] {
        db.drop().await.unwrap();
        for (name, key, opts, docs) in corpus() {
            db.collection::<Document>(name)
                .insert_many(docs)
                .await
                .unwrap();
            let mut spec = doc! {"key": key, "name": format!("{name}_text")};
            spec.extend(opts);
            db.run_command(doc! {"createIndexes": name, "indexes": [spec]})
                .await
                .unwrap();
        }
    }
    let mut n = 0;
    let mut bad = Vec::new();
    for (name, ..) in corpus() {
        for q in SEARCHES {
            // The gateway's copy waits for its writes ($text reads `latest`).
            let a = scores(&sdb, name, q).await;
            let b = scores(&rdb, name, q).await;
            n += 1;
            let same = a.len() == b.len()
                && a.iter()
                    .zip(&b)
                    .all(|(x, y)| x.0 == y.0 && (x.1 - y.1).abs() < 1e-9);
            if !same {
                bad.push(format!("{name} {q:?}\n  sprig: {a:?}\n  ref:   {b:?}"));
            }
        }
    }
    for db in [&sdb, &rdb] {
        db.drop().await.unwrap();
    }
    for b in &bad {
        eprintln!("MISMATCH {b}");
    }
    eprintln!("textScore: {n} searches, {} differ", bad.len());
    assert!(bad.is_empty(), "{} of {n} differ", bad.len());
}
