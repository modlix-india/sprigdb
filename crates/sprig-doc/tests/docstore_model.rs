//! The engine document store against an in-memory oracle: the oracle is a
//! map of documents on which every filter runs through the plain matcher
//! and every update through the plain update operators. Random writes and
//! finds (with every index kind present, and hints forcing each index)
//! must agree with it, and after every sequence each index's entries must
//! equal the entries of the live documents.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::unused_async,
    clippy::map_entry,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::collections::BTreeMap;

use bson::{Bson, Document, doc};
use common::Bed;
use sprig_doc::{filter, keyenc, update};

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
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

const PATHS: [&str; 5] = ["a", "b", "a.x", "t", "w.k"];

fn scalar(r: &mut Rng) -> Bson {
    match r.below(8) {
        0 => Bson::Null,
        1 => Bson::Double(r.below(12) as f64 / 2.0),
        2 => Bson::String(format!("s{}", r.below(5))),
        3 => Bson::Int64(r.below(6) as i64),
        4 => Bson::Boolean(r.chance(50)),
        _ => Bson::Int32(r.below(6) as i32),
    }
}

fn value(r: &mut Rng) -> Bson {
    match r.below(8) {
        0 => Bson::Array((0..r.below(4)).map(|_| scalar(r)).collect()),
        1 => Bson::Document(doc! {"x": scalar(r)}),
        2 => Bson::Array(vec![
            Bson::Document(doc! {"x": scalar(r)}),
            Bson::Document(doc! {"x": scalar(r)}),
        ]),
        _ => scalar(r),
    }
}

fn document(r: &mut Rng, id: i32) -> Document {
    let mut d = doc! {"_id": id};
    if r.chance(85) {
        d.insert("a", value(r));
    }
    if r.chance(70) {
        d.insert("b", scalar(r));
    }
    // `t` is the one array field of the compound index's documents.
    if r.chance(60) {
        d.insert(
            "t",
            Bson::Array((0..r.below(3)).map(|_| scalar(r)).collect()),
        );
    }
    if r.chance(40) {
        d.insert("w", doc! {"k": scalar(r)});
    }
    d
}

fn cond(r: &mut Rng) -> Document {
    let p = PATHS[r.below(PATHS.len() as u64) as usize];
    let v = scalar(r);
    match r.below(11) {
        0 => doc! {p: {"$gt": v}},
        1 => doc! {p: {"$gte": v}},
        2 => doc! {p: {"$lt": v}},
        3 => doc! {p: {"$lte": v}},
        4 => doc! {p: {"$in": [scalar(r), scalar(r), scalar(r)]}},
        5 => doc! {p: {"$gt": scalar(r), "$lte": scalar(r)}},
        6 => doc! {p: {"$exists": r.chance(50)}},
        7 => doc! {p: {"$regex": format!("^s{}", r.below(4))}},
        8 => doc! {"$or": [{p: scalar(r)}, {"_id": r.below(40) as i32}, {"a": {"$gt": scalar(r)}}]},
        _ => doc! {p: v},
    }
}

fn filter_doc(r: &mut Rng) -> Document {
    let mut f = cond(r);
    if r.chance(40) {
        f.extend(cond(r));
    }
    f
}

fn upd(r: &mut Rng) -> Bson {
    Bson::Document(match r.below(5) {
        0 => doc! {"$set": {"a": value(r)}},
        1 => doc! {"$set": {"b": scalar(r), "w.k": scalar(r)}},
        2 => doc! {"$unset": {"a": ""}},
        3 => doc! {"$push": {"t": scalar(r)}},
        _ => doc! {"$inc": {"n": 1}},
    })
}

const INDEXES: [&str; 8] = ["ia", "iab", "iax", "ib_desc", "ih", "iw", "isp", "ipart"];

fn specs() -> Vec<Document> {
    vec![
        doc! {"key": {"a": 1}, "name": "ia"},
        doc! {"key": {"a": 1, "b": -1}, "name": "iab"},
        doc! {"key": {"a.x": 1}, "name": "iax"},
        doc! {"key": {"b": -1, "t": 1}, "name": "ib_desc"},
        doc! {"key": {"b": "hashed"}, "name": "ih"},
        doc! {"key": {"$**": 1}, "name": "iw"},
        doc! {"key": {"w.k": 1}, "name": "isp", "sparse": true},
        doc! {"key": {"t": 1}, "name": "ipart", "partialFilterExpression": {"b": {"$exists": true}}},
    ]
}

fn id_key(d: &Document) -> Vec<u8> {
    keyenc::key(d.get("_id").unwrap())
}

fn sorted_ids(docs: &[Document]) -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = docs.iter().map(id_key).collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_matches_the_oracle() {
    let rounds: u64 = std::env::var("SPRIG_MODEL_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(6);
    let mut checked = 0usize;
    for seed in 1..=rounds {
        let mut r = Rng(0xA5A5_5A5A_1234_5678 ^ (seed * 0x9E37_79B9));
        let b = Bed::mem();
        let indexed_first = seed % 2 == 0;
        if indexed_first {
            let res = b
                .run("m", doc! {"createIndexes": "c", "indexes": specs()})
                .await;
            assert_eq!(res.get_f64("ok").unwrap(), 1.0, "{res}");
        }
        let mut oracle: BTreeMap<Vec<u8>, Document> = BTreeMap::new();
        let mut next = 0i32;
        for step in 0..400 {
            if !indexed_first && step == 200 {
                // Built over the documents present.
                let res = b
                    .run("m", doc! {"createIndexes": "c", "indexes": specs()})
                    .await;
                assert_eq!(res.get_f64("ok").unwrap(), 1.0, "{res}");
            }
            match r.below(10) {
                0..=3 => {
                    next += 1;
                    let id = if r.chance(10) {
                        (r.below(next as u64) as i32).max(1)
                    } else {
                        next
                    };
                    let d = document(&mut r, id);
                    let res = b
                        .run("m", doc! {"insert": "c", "documents": [d.clone()]})
                        .await;
                    let k = id_key(&d);
                    if oracle.contains_key(&k) {
                        assert!(res.get_array("writeErrors").is_ok(), "dup must fail: {res}");
                    } else {
                        assert_eq!(res.get_i32("n").unwrap(), 1, "{d} -> {res}");
                        oracle.insert(k, d);
                    }
                }
                4 | 5 => {
                    let f = filter_doc(&mut r);
                    let u = upd(&mut r);
                    let res = b
                        .run("m", doc! {"update": "c", "updates": [{"q": f.clone(), "u": u.clone(), "multi": true}]})
                        .await;
                    let p = filter::parse(&f).unwrap();
                    let spec = update::parse(&u).unwrap();
                    let cx = update::ApplyCx {
                        query: &p,
                        array_filters: &[],
                        inserting: false,
                        now: bson::DateTime::from_millis(0),
                    };
                    let mut n = 0;
                    let mut ok = true;
                    let mut next_oracle = oracle.clone();
                    for (k, d) in &oracle {
                        if p.matches(d) {
                            let mut nd = d.clone();
                            match update::apply(&mut nd, &spec, &cx) {
                                Ok(_) => {
                                    next_oracle.insert(k.clone(), nd);
                                    n += 1;
                                }
                                Err(_) => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    if ok {
                        assert_eq!(res.get_i32("n").unwrap(), n, "{f} {u} -> {res}");
                        oracle = next_oracle;
                    } else {
                        // A statement error undoes the whole statement.
                        assert!(res.get_array("writeErrors").is_ok(), "{f} {u} -> {res}");
                    }
                }
                6 => {
                    let f = filter_doc(&mut r);
                    let res = b
                        .run(
                            "m",
                            doc! {"delete": "c", "deletes": [{"q": f.clone(), "limit": 0}]},
                        )
                        .await;
                    let p = filter::parse(&f).unwrap();
                    let before = oracle.len();
                    oracle.retain(|_, d| !p.matches(d));
                    assert_eq!(
                        res.get_i32("n").unwrap() as usize,
                        before - oracle.len(),
                        "{f}"
                    );
                }
                _ => {
                    let f = filter_doc(&mut r);
                    let p = filter::parse(&f).unwrap();
                    let want: Vec<Document> =
                        oracle.values().filter(|d| p.matches(d)).cloned().collect();
                    let mut cmd = doc! {"find": "c", "filter": f.clone(), "batchSize": 3};
                    if r.chance(40) {
                        let h = INDEXES[r.below(INDEXES.len() as u64) as usize];
                        cmd.insert("hint", h);
                    }
                    let res = b.run("m", cmd.clone()).await;
                    if res.get_f64("ok") == Ok(0.0) {
                        // Only a hint of an index that doesn't exist yet may fail.
                        assert!(cmd.contains_key("hint") && !indexed_first, "{cmd} -> {res}");
                        continue;
                    }
                    let got = b.find_all("m", cmd.clone()).await;
                    let hinted = cmd.get_str("hint").ok();
                    // A partial or sparse index hinted reads only its own
                    // documents (as MongoDB does); compare those queries
                    // as a subset.
                    if matches!(hinted, Some("ipart" | "isp" | "iw")) {
                        for g in &got {
                            assert!(want.iter().any(|w| w == g), "{cmd}: {g} not a match");
                        }
                    } else {
                        assert_eq!(sorted_ids(&got), sorted_ids(&want), "{cmd}");
                        let mut g = got.clone();
                        g.sort_by_key(id_key);
                        assert_eq!(g, want, "{cmd}: documents differ");
                    }
                    checked += 1;
                }
            }
        }
        let all = b.find_all("m", doc! {"find": "c"}).await;
        assert_eq!(
            all,
            oracle.values().cloned().collect::<Vec<_>>(),
            "seed {seed}: final contents"
        );
        common::check_indexes(&b, "m").await;
    }
    assert!(checked > 100);
}
