//! The recorded MongoDB 8.0.34 corpora (`tests/{expr_diff,stage_diff,
//! validate_diff}/cases.json`) replayed through collections stored on the
//! engine, as `deploy/bench/doc/mongo-diff.js` replays them over a real
//! endpoint: each case's documents are inserted, then the expression is
//! evaluated by an aggregation, the pipeline run, the validator tried on an
//! insert, or the filter run by a find, and the answer compared with
//! MongoDB's (documents with exact BSON types, errors by code, validation
//! failures by code and `errInfo`).
//!
//! Cases in each corpus's `known.txt` are known differences of the shared
//! stage and expression code: reported, not failed. Every other case must
//! match. `SPRIG_CORPORA_VERBOSE=1` prints each mismatch.

#![allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::too_many_lines
)]

#[path = "docstore_common/mod.rs"]
mod common;

use bson::{Bson, Document, doc};
use common::Bed;

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

/// `errInfo` as recorded: mongosh wrote whole doubles as int32, so those
/// compare by value.
fn same_info(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Double(x), Bson::Int32(y)) | (Bson::Int32(y), Bson::Double(x)) => {
            *x == f64::from(*y)
        }
        (Bson::Document(x), Bson::Document(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && same_info(v1, v2))
        }
        (Bson::Array(x), Bson::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same_info(p, q))
        }
        _ => same(a, b),
    }
}

fn load(corpus: &str) -> (Vec<Document>, Vec<String>) {
    let dir = format!("{}/tests/{corpus}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(format!("{dir}/cases.json")).unwrap();
    let json: serde_json::Value = serde_json::from_str(&text).unwrap();
    let Bson::Array(cases) = Bson::try_from(json).unwrap() else {
        panic!("cases.json is not an array");
    };
    let known = std::fs::read_to_string(format!("{dir}/known.txt"))
        .unwrap_or_default()
        .lines()
        .map(|l| l.split_whitespace().next().unwrap_or("").to_owned())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    (
        cases
            .into_iter()
            .filter_map(|c| c.as_document().cloned())
            .collect(),
        known,
    )
}

fn docs_of(d: &Document, k: &str) -> Vec<Bson> {
    d.get_array(k).cloned().unwrap_or_default()
}

/// A cursor command's documents, or the error.
async fn all(b: &Bed, cmd: Document) -> Result<Vec<Bson>, Document> {
    let r = b.run("corp", cmd.clone()).await;
    if r.get_f64("ok") == Ok(0.0) {
        return Err(r);
    }
    Ok(b.find_all_cmd("corp", cmd)
        .await
        .into_iter()
        .map(Bson::Document)
        .collect())
}

async fn reset(b: &Bed, coll: &str, docs: Vec<Bson>) {
    b.run(
        "corp",
        doc! {"delete": coll, "deletes": [{"q": {}, "limit": 0}]},
    )
    .await;
    if !docs.is_empty() {
        let r = b
            .run("corp", doc! {"insert": coll, "documents": docs})
            .await;
        assert!(r.get_array("writeErrors").is_err(), "{r}");
    }
}

fn code_of(r: &Document) -> Option<i32> {
    r.get_i32("code").ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recorded_corpora_through_the_store() {
    let verbose = std::env::var("SPRIG_CORPORA_VERBOSE").is_ok();
    let b = Bed::mem();
    let mut report = Vec::new();
    let mut failed = Vec::new();
    for corpus in ["expr_diff", "stage_diff", "validate_diff"] {
        let (cases, known) = load(corpus);
        let (mut total, mut matched, mut known_hit) = (0, 0, 0);
        for k in &cases {
            let name = k.get_str("name").unwrap_or("?").to_owned();
            let want = k.get_document("expect").cloned().unwrap_or_default();
            total += 1;
            let ok = match corpus {
                "expr_diff" => {
                    let got = if let Ok(g) = k.get_document("group") {
                        reset(&b, "bench_diff", docs_of(k, "docs")).await;
                        all(&b, doc! {"aggregate": "bench_diff", "pipeline": [{"$group": g.clone()}, {"$sort": {"_id": 1}}], "cursor": {}}).await
                    } else {
                        reset(
                            &b,
                            "bench_diff",
                            vec![k.get("doc").cloned().unwrap_or(Bson::Null)],
                        )
                        .await;
                        all(&b, doc! {"aggregate": "bench_diff", "pipeline": [{"$replaceWith": {"r": k.get("expr").cloned().unwrap_or(Bson::Null)}}], "cursor": {}}).await
                    };
                    compare(&want, got)
                }
                "stage_diff" => {
                    reset(&b, "bench_diff", docs_of(k, "docs")).await;
                    if let Ok(colls) = k.get_document("colls") {
                        for (n, d) in colls {
                            reset(&b, n, d.as_array().cloned().unwrap_or_default()).await;
                        }
                    }
                    let agg: Bson = if k.get_bool("db").unwrap_or(false) {
                        Bson::Int32(1)
                    } else {
                        Bson::String("bench_diff".into())
                    };
                    let got = all(&b, doc! {"aggregate": agg, "pipeline": k.get("pipeline").cloned().unwrap_or(Bson::Array(vec![])), "cursor": {}}).await;
                    compare(&want, got)
                }
                _ => {
                    if let Ok(v) = k.get_document("validator") {
                        b.run("corp", doc! {"drop": "bench_val"}).await;
                        let r = b
                            .run("corp", doc! {"create": "bench_val", "validator": v.clone()})
                            .await;
                        if r.get_f64("ok") == Ok(0.0) {
                            if verbose {
                                eprintln!("GOT {name}: create {r}");
                            }
                            let wc = want.get_i32("createCode").ok().or_else(|| {
                                want.get_document("create")
                                    .ok()
                                    .and_then(|c| c.get_i32("code").ok())
                            });
                            wc.is_some() && wc == code_of(&r)
                        } else {
                            let r = b
                                .run("corp", doc! {"insert": "bench_val", "documents": [k.get("doc").cloned().unwrap_or(Bson::Null)]})
                                .await;
                            if verbose {
                                eprintln!("GOT {name}: {r}");
                            }
                            match r.get_array("writeErrors") {
                                Err(_) => want.get_bool("ok").unwrap_or(false),
                                Ok(errs) => {
                                    let e = errs[0].as_document().cloned().unwrap_or_default();
                                    want.get_i32("code").ok() == code_of(&e)
                                        && want.get("errInfo").is_none_or(|wi| {
                                            e.get("errInfo").is_some_and(|gi| same_info(wi, gi))
                                        })
                                }
                            }
                        }
                    } else {
                        reset(&b, "bench_diff", docs_of(k, "docs")).await;
                        let got = all(&b, doc! {"find": "bench_diff", "filter": k.get("filter").cloned().unwrap_or(Bson::Document(Document::new())), "projection": {"_id": 1}, "sort": {"_id": 1}}).await;
                        match (want.get_array("ids"), got) {
                            (Ok(ids), Ok(g)) => {
                                let gi: Vec<Bson> = g
                                    .iter()
                                    .filter_map(|d| d.as_document()?.get("_id").cloned())
                                    .collect();
                                same(&Bson::Array(ids.clone()), &Bson::Array(gi))
                            }
                            (Err(_), Err(e)) => want.get_i32("code").ok() == code_of(&e),
                            _ => false,
                        }
                    }
                }
            };
            if ok {
                matched += 1;
            } else if known.contains(&name) {
                known_hit += 1;
            } else {
                failed.push(format!("{corpus} {name}"));
                if verbose {
                    eprintln!("DIFF {corpus} {name}: want {want}");
                }
            }
        }
        report.push(format!(
            "{corpus}: {matched}/{total} matched, {known_hit} known differences"
        ));
    }
    eprintln!("cases: {}", report.join("; "));
    assert!(
        failed.is_empty(),
        "{} cases differ: {:?}",
        failed.len(),
        &failed[..failed.len().min(40)]
    );
}

/// A cursor answer against the recorded one: documents exactly, or the
/// error code.
fn compare(want: &Document, got: Result<Vec<Bson>, Document>) -> bool {
    match (want.get_array("ok"), got) {
        (Ok(w), Ok(g)) => same(&Bson::Array(w.clone()), &Bson::Array(g)),
        (Err(_), Err(e)) => want
            .get_i32("code")
            .ok()
            .is_some_and(|c| Some(c) == code_of(&e)),
        _ => false,
    }
}
