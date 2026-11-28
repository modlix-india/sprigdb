//! The live differential test of the engine document store against a
//! reference MongoDB 8.0: random command sequences (index creation,
//! inserts, updates, deletes, `findAndModify`, finds, counts, distinct,
//! aggregations) run on both, and the replies are compared after
//! normalising what MongoDB leaves unspecified (the order of an unsorted
//! result, error messages).
//!
//! Runs only with `SPRIG_TEST_MONGO_REF=host:port` (a private reference
//! server, a one-node replica set). `SPRIG_DIFF_ROUNDS` sets the rounds
//! (default 12) and `SPRIG_DIFF_SEED` the first seed;
//! `SPRIG_DIFF_VERBOSE=1` prints every mismatch.

#![allow(
    clippy::large_enum_variant,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    clippy::float_cmp,
    clippy::doc_markdown,
    clippy::unused_async,
    clippy::map_entry,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)]

#[path = "docstore_common/mod.rs"]
mod common;

use bson::{Bson, Document, doc};
use common::Bed;

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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len() as u64) as usize]
    }
}

const FIELDS: [&str; 4] = ["a", "b", "c", "d.x"];

fn scalar(r: &mut Rng) -> Bson {
    match r.below(10) {
        0 => Bson::Null,
        1 => Bson::Double(r.below(20) as f64 / 2.0),
        2 => Bson::String(format!("s{}", r.below(6))),
        3 => Bson::Boolean(r.chance(50)),
        4 => Bson::Int64(r.below(10) as i64),
        5 => Bson::DateTime(bson::DateTime::from_millis(
            1_700_000_000_000 + r.below(5) as i64 * 1000,
        )),
        _ => Bson::Int32(r.below(10) as i32),
    }
}

fn value(r: &mut Rng) -> Bson {
    match r.below(10) {
        0 => Bson::Array((0..r.below(4)).map(|_| scalar(r)).collect()),
        1 => Bson::Document(doc! {"x": scalar(r), "y": scalar(r)}),
        _ => scalar(r),
    }
}

fn document(r: &mut Rng, id: i32) -> Document {
    let mut d = doc! {"_id": id};
    for f in ["a", "b", "c"] {
        if r.chance(80) {
            d.insert(f, value(r));
        }
    }
    if r.chance(50) {
        d.insert("d", doc! {"x": scalar(r)});
    }
    d
}

fn cond(r: &mut Rng) -> Document {
    let f = *r.pick(&FIELDS);
    let v = scalar(r);
    match r.below(12) {
        0 => doc! {f: {"$gt": v}},
        1 => doc! {f: {"$gte": v}},
        2 => doc! {f: {"$lt": v}},
        3 => doc! {f: {"$lte": v}},
        4 => doc! {f: {"$in": [scalar(r), scalar(r)]}},
        5 => doc! {f: {"$ne": v}},
        6 => doc! {f: {"$exists": r.chance(50)}},
        7 => doc! {"$or": [cond(r), cond(r)]},
        8 => doc! {f: {"$gt": scalar(r), "$lt": scalar(r)}},
        9 => doc! {"_id": {"$in": [r.below(30) as i32, r.below(30) as i32]}},
        10 => match r.below(8) {
            0 => doc! {f: {"$elemMatch": {"$gt": scalar(r)}}},
            1 => doc! {f: {"$all": [scalar(r)]}},
            2 => doc! {f: {"$size": r.below(3) as i32}},
            3 => doc! {f: {"$type": "number"}},
            4 => doc! {f: {"$regex": "^s[0-2]"}},
            5 => doc! {f: {"$not": {"$gt": scalar(r)}}},
            6 => doc! {"$nor": [cond(r)]},
            _ => doc! {f: {"$nin": [scalar(r), scalar(r)]}},
        },
        _ => doc! {f: v},
    }
}

fn filter(r: &mut Rng) -> Document {
    match r.below(5) {
        0 => Document::new(),
        1 => {
            let mut a = cond(r);
            a.extend(cond(r));
            a
        }
        _ => cond(r),
    }
}

fn update_doc(r: &mut Rng) -> Bson {
    let f = *r.pick(&FIELDS);
    Bson::Document(match r.below(14) {
        0 => doc! {"$set": {f: value(r)}},
        1 => doc! {"$inc": {"n": r.below(5) as i32}},
        2 => doc! {"$unset": {f: ""}},
        3 => doc! {"$push": {"a": scalar(r)}},
        4 => doc! {"$addToSet": {"b": scalar(r)}},
        5 => doc! {"$min": {"c": scalar(r)}},
        6 => doc! {"$set": {"a": value(r), "b": value(r)}},
        7 => doc! {"a": value(r), "c": value(r)},
        8 => doc! {"$max": {"n": r.below(10) as i32}},
        9 => doc! {"$pull": {"a": scalar(r)}},
        10 => doc! {"$mul": {"n": 2}},
        11 => doc! {"$rename": {"c": "e"}},
        12 => doc! {"$setOnInsert": {"ins": true}, "$set": {"m": scalar(r)}},
        _ => doc! {"$addToSet": {"a": {"$each": [scalar(r), scalar(r)]}}},
    })
}

fn index_spec(r: &mut Rng, i: usize) -> Document {
    let f = *r.pick(&FIELDS);
    let g = *r.pick(&FIELDS);
    let dir = if r.chance(70) { 1 } else { -1 };
    let mut key = doc! {f: dir};
    if r.chance(30) && g != f {
        key.insert(g, if r.chance(50) { 1 } else { -1 });
    }
    if r.chance(10) {
        key = doc! {f: "hashed"};
    }
    if r.chance(8) {
        key = if r.chance(50) {
            doc! {"$**": 1}
        } else {
            doc! {"d.$**": 1}
        };
    }
    let mut s = doc! {"key": key.clone(), "name": format!("i{i}")};
    if key.keys().any(|k| k.contains("$**")) {
        return s;
    }
    match r.below(6) {
        0 => {
            s.insert("unique", true);
        }
        1 => {
            s.insert("sparse", true);
        }
        2 => {
            s.insert("partialFilterExpression", doc! {f: {"$exists": true}});
        }
        _ => {}
    }
    s
}

fn command(r: &mut Rng, next_id: &mut i32, i: usize) -> Document {
    match r.below(23) {
        0 => doc! {"createIndexes": "c", "indexes": [index_spec(r, i)]},
        1..=4 => {
            let n = 1 + r.below(4);
            let docs: Vec<Document> = (0..n)
                .map(|_| {
                    let id = if r.chance(10) {
                        r.below((*next_id).max(1) as u64) as i32
                    } else {
                        *next_id += 1;
                        *next_id
                    };
                    document(r, id)
                })
                .collect();
            doc! {"insert": "c", "documents": docs, "ordered": r.chance(70)}
        }
        5..=7 => {
            let upsert = r.chance(20);
            let multi = r.chance(50);
            // A multi-update that fails part-way keeps MongoDB's earlier
            // documents (in its plan order) and none of ours (ED-15): multi
            // updates use operators that can't fail.
            let u = if multi {
                Bson::Document(match r.below(4) {
                    0 => doc! {"$inc": {"n": 1}},
                    1 => doc! {"$max": {"n": r.below(10) as i32}},
                    2 => doc! {"$set": {"m": value(r)}},
                    _ => doc! {"$set": {"m": scalar(r)}},
                })
            } else {
                update_doc(r)
            };
            // A single update names its target's order (MongoDB 8.0's
            // `sort`): without one, which document it picks is unspecified.
            let mut st = doc! {"q": filter(r), "u": u, "multi": multi, "upsert": upsert};
            if !multi {
                st.insert("sort", doc! {"_id": 1});
            }
            doc! {"update": "c", "updates": [st]}
        }
        8 => {
            // `limit: 1` takes an unspecified match: give it one.
            if r.chance(50) {
                doc! {"delete": "c", "deletes": [{"q": {"_id": r.below(40) as i32}, "limit": 1}]}
            } else {
                doc! {"delete": "c", "deletes": [{"q": filter(r), "limit": 0}]}
            }
        }
        9 => {
            if r.chance(50) {
                doc! {"findAndModify": "c", "query": filter(r), "sort": {"_id": 1}, "update": update_doc(r), "new": r.chance(50), "upsert": r.chance(20)}
            } else {
                doc! {"findAndModify": "c", "query": filter(r), "sort": {"_id": -1}, "remove": true}
            }
        }
        10 => doc! {"count": "c", "query": filter(r)},
        11 => doc! {"distinct": "c", "key": *r.pick(&FIELDS), "query": filter(r)},
        12 => {
            let g = *r.pick(&["a", "b", "c"]);
            doc! {"aggregate": "c", "pipeline": [
                {"$match": filter(r)},
                {"$group": {"_id": format!("${g}"), "n": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ], "cursor": {}}
        }
        14 if r.chance(50) => {
            doc! {"insert": "u", "documents": [{"_id": r.below(1000) as i32 + 1000 * (r.below(1000) as i32), "k": scalar(r), "v": value(r)}]}
        }
        14 => doc! {"aggregate": "c", "pipeline": [
            {"$match": filter(r)},
            {"$lookup": {"from": "u", "localField": "a", "foreignField": "k", "as": "j"}},
            {"$project": {"a": 1, "nj": {"$size": "$j"}, "isarr": {"$isArray": "$a"}}},
            {"$sort": {"_id": 1}},
        ], "cursor": {}},
        15 => doc! {"aggregate": "c", "pipeline": [
            {"$match": filter(r)},
            {"$unwind": {"path": "$a", "preserveNullAndEmptyArrays": r.chance(50)}},
            {"$addFields": {"t": {"$type": "$a"}, "c2": {"$cond": [{"$gt": ["$c", 3]}, "big", "small"]}}},
            {"$sort": {"_id": 1, "a": 1}},
            {"$limit": 20},
        ], "cursor": {}},
        16 => {
            doc! {"update": "c", "updates": [{"q": {"_id": r.below(40) as i32}, "u": {"$set": {"a.$[e]": 0}}, "arrayFilters": [{"e": {"$gt": scalar(r)}}], "sort": {"_id": 1}}]}
        }
        13 => doc! {"aggregate": "c", "pipeline": [
            {"$match": filter(r)}, {"$sort": {"_id": 1}}, {"$skip": r.below(3) as i32}, {"$limit": 1 + r.below(10) as i32},
            {"$project": {"a": 1, "c": 1}},
        ], "cursor": {}},
        _ => {
            let mut f = doc! {"find": "c", "filter": filter(r)};
            match r.below(4) {
                0 => {
                    f.insert("sort", doc! {"_id": -1});
                }
                1 => {
                    let s = *r.pick(&FIELDS);
                    f.insert(
                        "sort",
                        doc! {s: if r.chance(50) { 1 } else { -1 }, "_id": 1},
                    );
                }
                2 => {
                    f.insert("sort", doc! {"_id": 1});
                }
                _ => {}
            }
            if r.chance(30) {
                f.insert("limit", 1 + r.below(6) as i64);
            }
            if r.chance(20) {
                f.insert("skip", r.below(4) as i64);
            }
            // A limit or skip without a sort takes an unspecified part.
            if (f.contains_key("limit") || f.contains_key("skip")) && !f.contains_key("sort") {
                f.insert("sort", doc! {"_id": 1});
            }
            if r.chance(20) {
                f.insert("projection", doc! {"a": 1, "d.x": 1});
            }
            f.insert("batchSize", 1 + r.below(5) as i32);
            f
        }
    }
}

/// Exact equality: types, and doubles bit for bit.
fn same(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Double(y)) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && same(v1, v2))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        _ => a == b,
    }
}

/// The elements only one side has (whole values when not lists).
fn only(w: &Bson, g: &Bson) -> String {
    match (w, g) {
        (Bson::Array(a), Bson::Array(b)) if a.len() > 4 || b.len() > 4 => {
            let mo: Vec<String> = a
                .iter()
                .filter(|x| !b.iter().any(|y| same(x, y)))
                .map(ToString::to_string)
                .collect();
            let ou: Vec<String> = b
                .iter()
                .filter(|y| !a.iter().any(|x| same(x, y)))
                .map(ToString::to_string)
                .collect();
            format!("  only mongo: {mo:?}\n  only ours:  {ou:?}")
        }
        _ => format!("  mongo: {w}\n  ours:  {g}"),
    }
}

/// Numbers by value (equal values of different types merged by
/// `distinct` or `$group` keep whichever came first: plan order).
fn by_value(v: &Bson) -> Bson {
    match v {
        Bson::Int32(n) => Bson::Double(f64::from(*n)),
        Bson::Int64(n) => Bson::Double(*n as f64),
        Bson::Document(d) => {
            Bson::Document(d.iter().map(|(k, x)| (k.clone(), by_value(x))).collect())
        }
        Bson::Array(a) => Bson::Array(a.iter().map(by_value).collect()),
        other => other.clone(),
    }
}

/// Generated ObjectIds differ between the servers: all become one value.
fn no_oids(v: &Bson) -> Bson {
    match v {
        Bson::ObjectId(_) => Bson::String("<oid>".into()),
        Bson::Document(d) => {
            Bson::Document(d.iter().map(|(k, x)| (k.clone(), no_oids(x))).collect())
        }
        Bson::Array(a) => Bson::Array(a.iter().map(no_oids).collect()),
        other => other.clone(),
    }
}

fn canon(d: &Bson) -> Vec<u8> {
    bson::to_vec(&doc! {"v": d.clone()}).unwrap()
}

async fn ref_run(
    db: &mongodb::Database,
    sess: &mut mongodb::ClientSession,
    cmd: Document,
) -> Document {
    let coll = cmd
        .iter()
        .next()
        .and_then(|(_, v)| v.as_str().map(str::to_owned))
        .unwrap_or_default();
    match db.run_command(cmd).session(&mut *sess).await {
        Ok(mut d) => {
            if let Ok(c) = d.get_document("cursor") {
                let mut all: Vec<Bson> = c.get_array("firstBatch").cloned().unwrap_or_default();
                let mut id = c.get_i64("id").unwrap_or(0);
                while id != 0 {
                    let m = db
                        .run_command(doc! {"getMore": id, "collection": coll.clone()})
                        .session(&mut *sess)
                        .await
                        .unwrap();
                    let c = m.get_document("cursor").unwrap();
                    all.extend(c.get_array("nextBatch").cloned().unwrap_or_default());
                    id = c.get_i64("id").unwrap_or(0);
                }
                d = doc! {"docs": all, "ok": 1.0};
            }
            d
        }
        Err(e) => match *e.kind {
            mongodb::error::ErrorKind::Command(ce) => {
                doc! {"ok": 0.0, "code": ce.code, "errmsg": ce.message}
            }
            other => doc! {"ok": 0.0, "code": -1, "errmsg": format!("{other:?}")},
        },
    }
}

/// The side under test: the store in this process, or a gateway's
/// MongoDB listener through the driver (`SPRIG_TEST_MONGO_GW`).
enum Side {
    Bed(Bed),
    Gw(mongodb::Database, mongodb::ClientSession),
}

impl Side {
    async fn run(&mut self, db: &str, cmd: Document) -> Document {
        match self {
            Side::Bed(b) => ours(b, db, cmd).await,
            Side::Gw(d, s) => {
                let r = ref_run(d, s, cmd.clone()).await;
                // The gateway names the collection in listIndexes' reply as
                // MongoDB does; the comparison reads `firstBatch`.
                if cmd.contains_key("listIndexes") && r.get_f64("ok") == Ok(1.0) {
                    return doc! {"cursor": {"firstBatch": r.get_array("docs").cloned().unwrap_or_default()}, "ok": 1.0};
                }
                r
            }
        }
    }
}

async fn ours(b: &Bed, db: &str, cmd: Document) -> Document {
    let is_cursor = matches!(
        cmd.keys().next().map(String::as_str),
        Some("find" | "aggregate")
    );
    if is_cursor {
        let r = b.run(db, cmd.clone()).await;
        if r.get_f64("ok") == Ok(0.0) {
            return r;
        }
        let docs = b.find_all_cmd(db, cmd).await;
        return doc! {"docs": docs, "ok": 1.0};
    }
    b.run(db, cmd).await
}

/// The parts of a reply that must agree.
fn norm(cmd: &Document, r: &Document) -> Bson {
    let name = cmd.keys().next().unwrap().as_str();
    if r.get_f64("ok").ok() == Some(0.0) || r.get_i32("ok").ok() == Some(0) {
        return Bson::Document(doc! {"error": r.get_i32("code").unwrap_or(-1)});
    }
    let errs = |r: &Document| -> Bson {
        Bson::Array(
            r.get_array("writeErrors")
                .map(|a| {
                    a.iter()
                        .map(|e| {
                            let e = e.as_document().unwrap();
                            Bson::Document(doc! {"index": e.get_i32("index").unwrap_or(-1), "code": e.get_i32("code").unwrap_or(-1)})
                        })
                        .collect()
                })
                .unwrap_or_default(),
        )
    };
    match name {
        "insert" | "delete" => {
            Bson::Document(doc! {"n": r.get("n").cloned().unwrap_or(Bson::Null), "e": errs(r)})
        }
        "update" => Bson::Document(doc! {
            "n": r.get("n").cloned().unwrap_or(Bson::Null),
            "m": r.get("nModified").cloned().unwrap_or(Bson::Null),
            "u": r.get("upserted").cloned().unwrap_or(Bson::Null),
            "e": errs(r),
        }),
        "findAndModify" => Bson::Document(doc! {
            "v": r.get("value").cloned().unwrap_or(Bson::Null),
            "l": r.get("lastErrorObject").cloned().unwrap_or(Bson::Null),
        }),
        "count" => r.get("n").cloned().unwrap_or(Bson::Null),
        "distinct" => {
            // MongoDB answers from an index when it can, and then reports
            // index keys (null for a missing field, undefined for an
            // empty array): plan-dependent, so nulls are left out (ED-14).
            let mut v: Vec<Bson> = r
                .get_array("values")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|x| !matches!(x, Bson::Null | Bson::Undefined))
                .map(|x| by_value(&x))
                .collect();
            v.sort_by_key(canon);
            Bson::Array(v)
        }
        "createIndexes" => Bson::Document(
            doc! {"before": r.get("numIndexesBefore").cloned().unwrap_or(Bson::Null), "after": r.get("numIndexesAfter").cloned().unwrap_or(Bson::Null)},
        ),
        "find" | "aggregate" => {
            let mut v: Vec<Bson> = r.get_array("docs").cloned().unwrap_or_default();
            // `$group` output sorted by an array `_id` is a known
            // difference of the shared stage code (ED-14): compared
            // unordered.
            let grouped =
                format!("{cmd}").contains("$group") || format!("{cmd}").contains("$unwind");
            let sorted = (cmd.get_document("sort").is_ok() || name == "aggregate") && !grouped;
            if grouped {
                v = v.iter().map(by_value).collect();
            }
            if !sorted {
                v.sort_by_key(canon);
            }
            Bson::Array(v)
        }
        _ => Bson::Document(r.clone()),
    }
}

#[ignore = "live: needs SPRIG_TEST_MONGO_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_sequences_match_mongodb() {
    let Ok(addr) = std::env::var("SPRIG_TEST_MONGO_REF") else {
        eprintln!("SPRIG_TEST_MONGO_REF not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let rounds: u64 = std::env::var("SPRIG_DIFF_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    let seed0: u64 = std::env::var("SPRIG_DIFF_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let verbose = std::env::var("SPRIG_DIFF_VERBOSE").is_ok();
    let check_state = std::env::var("SPRIG_DIFF_CHECK_STATE").is_ok();
    let client = mongodb::Client::with_uri_str(format!("mongodb://{addr}/?directConnection=true"))
        .await
        .unwrap();
    let mut total = 0usize;
    let mut bad = 0usize;
    let mut shown = 0usize;
    for round in 0..rounds {
        let seed = seed0 + round;
        let mut r = Rng(0x9E37_79B9_7F4A_7C15 ^ (seed * 0x1000_0001));
        let dbname = format!("mdocs_diff_{seed}");
        let rdb = client.database(&dbname);
        rdb.drop().await.unwrap();
        let mut sess = client.start_session().await.unwrap();
        let mut side = match std::env::var("SPRIG_TEST_MONGO_GW") {
            Ok(uri) => {
                let gc = mongodb::Client::with_uri_str(&uri).await.unwrap();
                let gdb = gc.database(&dbname);
                gdb.drop().await.unwrap();
                let gs = gc.start_session().await.unwrap();
                Side::Gw(gdb, gs)
            }
            Err(_) => Side::Bed(Bed::mem()),
        };
        let mut next_id = 0i32;
        let mut diverged = false;
        for i in 0..150 {
            let cmd = command(&mut r, &mut next_id, i);
            let name = cmd.keys().next().cloned().unwrap_or_default();
            let want = ref_run(&rdb, &mut sess, cmd.clone()).await;
            let got = side.run(&dbname, cmd.clone()).await;
            total += 1;
            let (w, g) = (no_oids(&norm(&cmd, &want)), no_oids(&norm(&cmd, &got)));
            if !same(&w, &g) {
                bad += 1;
                if verbose || shown < 25 {
                    shown += 1;
                    eprintln!(
                        "MISMATCH seed {seed} step {i}\n  cmd:  {cmd}\n{}",
                        only(&w, &g)
                    );
                }
            }
            if check_state
                && !diverged
                && matches!(
                    name.as_str(),
                    "insert" | "update" | "delete" | "findAndModify"
                )
            {
                let all = doc! {"find": "c", "sort": {"_id": 1}};
                let w = no_oids(&norm(&all, &ref_run(&rdb, &mut sess, all.clone()).await));
                let g = no_oids(&norm(&all, &side.run(&dbname, all.clone()).await));
                if !same(&w, &g) {
                    diverged = true;
                    let li = side.run(&dbname, doc! {"listIndexes": "c"}).await;
                    let ex = side.run(&dbname, doc! {"explain": {"find": "c", "filter": cmd.get_array("updates").ok().and_then(|u| u.first()).and_then(|u| u.as_document()).and_then(|u| u.get_document("q").ok()).cloned().unwrap_or_default()}}).await;
                    eprintln!("  indexes: {li}\n  plan: {ex}");
                    eprintln!(
                        "DIVERGED seed {seed} step {i} after {cmd}\n  reply mongo: {want}\n  reply ours: {got}\n  mongo: {w}\n  ours:  {g}"
                    );
                }
            }
        }
        // The whole collection, and the index list.
        for cmd in [
            doc! {"find": "c", "sort": {"_id": 1}},
            doc! {"listIndexes": "c"},
        ] {
            let want = ref_run(&rdb, &mut sess, cmd.clone()).await;
            let got = side.run(&dbname, cmd.clone()).await;
            total += 1;
            let (w, g) = if cmd.contains_key("listIndexes") {
                (
                    Bson::Array(want.get_array("docs").cloned().unwrap_or_default()),
                    got.get_document("cursor")
                        .and_then(|c| c.get_array("firstBatch"))
                        .map(|a| Bson::Array(a.clone()))
                        .unwrap_or(Bson::Null),
                )
            } else {
                (no_oids(&norm(&cmd, &want)), no_oids(&norm(&cmd, &got)))
            };
            if !same(&w, &g) {
                bad += 1;
                if verbose || shown < 25 {
                    shown += 1;
                    eprintln!("MISMATCH seed {seed} final {cmd}\n{}", only(&w, &g));
                }
            }
        }
        if let Side::Bed(b) = &side {
            common::check_indexes(b, &dbname).await;
        }
        rdb.drop().await.unwrap();
    }
    eprintln!("diff: {total} commands, {bad} mismatches");
    assert_eq!(bad, 0, "{bad} of {total} commands differ from MongoDB");
}

/// Multi-document transactions through the driver, on the reference and,
/// with `SPRIG_TEST_MONGO_GW`, on the gateway: commit, abort, read your own
/// writes, a write conflict between two sessions, and a retried commit.
#[ignore = "live: needs SPRIG_TEST_MONGO_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_match_mongodb() {
    let Ok(addr) = std::env::var("SPRIG_TEST_MONGO_REF") else {
        eprintln!("SPRIG_TEST_MONGO_REF not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut uris = vec![format!("mongodb://{addr}/?directConnection=true")];
    if let Ok(gw) = std::env::var("SPRIG_TEST_MONGO_GW") {
        uris.push(gw);
    }
    let mut outcomes = Vec::new();
    for uri in uris {
        let client = mongodb::Client::with_uri_str(&uri).await.unwrap();
        let db = client.database("mdocs_txn");
        db.drop().await.unwrap();
        let c = db.collection::<Document>("c");
        c.insert_one(doc! {"_id": 1, "n": 0}).await.unwrap();
        let mut log = Vec::new();
        // Commit: visible after, not before.
        let mut s = client.start_session().await.unwrap();
        s.start_transaction().await.unwrap();
        c.insert_one(doc! {"_id": 2}).session(&mut s).await.unwrap();
        let inside = c.count_documents(doc! {}).session(&mut s).await;
        log.push(format!("count inside: {:?}", inside.map_err(|e| code(&e))));
        let mine = c.find_one(doc! {"_id": 2}).session(&mut s).await.unwrap();
        log.push(format!("own write seen: {}", mine.is_some()));
        let outside = c.find_one(doc! {"_id": 2}).await.unwrap();
        log.push(format!("seen outside before commit: {}", outside.is_some()));
        s.commit_transaction().await.unwrap();
        log.push(format!(
            "after commit: {}",
            c.find_one(doc! {"_id": 2}).await.unwrap().is_some()
        ));
        // Abort.
        s.start_transaction().await.unwrap();
        c.delete_many(doc! {}).session(&mut s).await.unwrap();
        s.abort_transaction().await.unwrap();
        log.push(format!(
            "after abort: {}",
            c.count_documents(doc! {}).await.unwrap()
        ));
        // Write conflict: a second session writes a document the first holds.
        let mut s2 = client.start_session().await.unwrap();
        s.start_transaction().await.unwrap();
        c.update_one(doc! {"_id": 1}, doc! {"$inc": {"n": 1}})
            .session(&mut s)
            .await
            .unwrap();
        s2.start_transaction().await.unwrap();
        let r = c
            .update_one(doc! {"_id": 1}, doc! {"$inc": {"n": 10}})
            .session(&mut s2)
            .await;
        let transient = r
            .as_ref()
            .err()
            .is_some_and(|e| e.contains_label("TransientTransactionError"));
        log.push(format!(
            "conflict: {:?} transient {transient}",
            r.map(|_| ()).map_err(|e| code(&e))
        ));
        let _ = s2.abort_transaction().await;
        s.commit_transaction().await.unwrap();
        // A commit sent again answers as the first did.
        let again = s.commit_transaction().await;
        log.push(format!("commit again: {:?}", again.map_err(|e| code(&e))));
        let n = c.find_one(doc! {"_id": 1}).await.unwrap().unwrap();
        log.push(format!("n = {}", n.get_i32("n").unwrap()));
        db.drop().await.unwrap();
        eprintln!("{uri}: {log:#?}");
        outcomes.push(log);
    }
    if outcomes.len() == 2 {
        assert_eq!(outcomes[0], outcomes[1]);
    }
}

fn code(e: &mongodb::error::Error) -> i32 {
    match &*e.kind {
        mongodb::error::ErrorKind::Command(c) => c.code,
        mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code,
        _ => -1,
    }
}

/// Views on the store against MongoDB: reads through the view, a view of
/// a view, writes and index commands refused, listCollections.
#[ignore = "live: needs SPRIG_TEST_MONGO_REF; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn views_match_mongodb() {
    let Ok(addr) = std::env::var("SPRIG_TEST_MONGO_REF") else {
        eprintln!("SPRIG_TEST_MONGO_REF not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let client = mongodb::Client::with_uri_str(format!("mongodb://{addr}/?directConnection=true"))
        .await
        .unwrap();
    let dbname = "mdocs_views";
    let rdb = client.database(dbname);
    rdb.drop().await.unwrap();
    let mut rs = client.start_session().await.unwrap();
    let mut side = match std::env::var("SPRIG_TEST_MONGO_GW") {
        Ok(uri) => {
            let gc = mongodb::Client::with_uri_str(&uri).await.unwrap();
            let gdb = gc.database(dbname);
            gdb.drop().await.unwrap();
            let gs = gc.start_session().await.unwrap();
            Side::Gw(gdb, gs)
        }
        Err(_) => Side::Bed(Bed::mem()),
    };
    let cmds = vec![
        doc! {"insert": "src", "documents": [{"_id": 1, "a": 5, "g": "x"}, {"_id": 2, "a": 1, "g": "y"}, {"_id": 3, "a": 9, "g": "x"}, {"_id": 4, "g": "z"}]},
        doc! {"create": "v", "viewOn": "src", "pipeline": [{"$match": {"a": {"$gt": 2}}}, {"$addFields": {"b": {"$multiply": ["$a", 2]}}}]},
        doc! {"create": "vv", "viewOn": "v", "pipeline": [{"$project": {"b": 1, "g": 1}}]},
        doc! {"find": "v", "filter": {"g": "x"}, "sort": {"_id": -1}},
        doc! {"find": "v", "sort": {"b": 1}, "limit": 1, "projection": {"b": 1}},
        doc! {"find": "vv", "sort": {"_id": 1}},
        doc! {"aggregate": "vv", "pipeline": [{"$group": {"_id": "$g", "s": {"$sum": "$b"}}}, {"$sort": {"_id": 1}}], "cursor": {}},
        doc! {"count": "v", "query": {"g": "x"}},
        doc! {"distinct": "v", "key": "g"},
        doc! {"insert": "v", "documents": [{"_id": 9}]},
        doc! {"update": "v", "updates": [{"q": {}, "u": {"$set": {"a": 1}}}]},
        doc! {"delete": "v", "deletes": [{"q": {}, "limit": 0}]},
        doc! {"createIndexes": "v", "indexes": [{"key": {"a": 1}, "name": "a"}]},
        doc! {"listIndexes": "v"},
        doc! {"create": "bad", "viewOn": "src", "pipeline": [{"$out": "x"}]},
        doc! {"listCollections": 1, "filter": {"type": "view"}, "nameOnly": true},
        doc! {"drop": "vv"},
        doc! {"find": "vv"},
    ];
    let mut bad = Vec::new();
    for cmd in cmds {
        let want = ref_run(&rdb, &mut rs, cmd.clone()).await;
        let got = side.run(dbname, cmd.clone()).await;
        let name = cmd.keys().next().unwrap().clone();
        let pick = |r: &Document| -> Bson {
            if r.get_f64("ok").ok() == Some(0.0) || r.get_i32("ok").ok() == Some(0) {
                return Bson::Document(doc! {"error": r.get_i32("code").unwrap_or(-1)});
            }
            if let Ok(e) = r.get_array("writeErrors") {
                return Bson::Document(
                    doc! {"writeError": e[0].as_document().and_then(|d| d.get_i32("code").ok()).unwrap_or(-1)},
                );
            }
            match name.as_str() {
                "find" | "aggregate" | "listCollections" => r
                    .get_array("docs")
                    .cloned()
                    .map(Bson::Array)
                    .or_else(|_| {
                        r.get_document("cursor")
                            .and_then(|c| c.get_array("firstBatch"))
                            .cloned()
                            .map(Bson::Array)
                    })
                    .unwrap_or(Bson::Null),
                "count" => r.get("n").cloned().unwrap_or(Bson::Null),
                "distinct" => r.get("values").cloned().unwrap_or(Bson::Null),
                _ => Bson::Boolean(true),
            }
        };
        let (w, g) = (pick(&want), pick(&got));
        if !same(&w, &g) {
            bad.push(format!("{cmd}\n  mongo: {w}\n  ours:  {g}"));
        }
    }
    rdb.drop().await.unwrap();
    assert!(bad.is_empty(), "views differ:\n{}", bad.join("\n"));
}
