//! Concurrency stress for the engine document store: tasks write at random
//! over a small key space (so they collide) through a unique index, a
//! multikey index and a compound one, while other tasks move amounts
//! between accounts in multi-document transactions. Afterwards: every
//! index's entries equal the live documents' entries, no unique key is
//! held twice, and the accounts' total never changed.

#![allow(
    clippy::float_cmp,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::many_single_char_names
)]

#[path = "docstore_common/mod.rs"]
mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
        self.next() % n
    }
}

fn lsid(task: u64) -> Document {
    let mut b = vec![0u8; 16];
    b[..8].copy_from_slice(&task.to_be_bytes());
    doc! {"id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: b })}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_keep_indexes_consistent() {
    let b = Arc::new(Bed::mem());
    let r = b
        .run(
            "s",
            doc! {"createIndexes": "c", "indexes": [
                {"key": {"u": 1}, "name": "u", "unique": true, "sparse": true},
                {"key": {"tags": 1}, "name": "tags"},
                {"key": {"g": 1, "v": -1}, "name": "gv"},
            ]},
        )
        .await;
    assert_eq!(r.get_f64("ok").unwrap(), 1.0, "{r}");
    // Accounts for the transfers: 20 of 100 each.
    let accts: Vec<Document> = (0..20).map(|i| doc! {"_id": i, "bal": 100}).collect();
    b.run("s", doc! {"insert": "acct", "documents": accts})
        .await;

    let ops = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut hs = Vec::new();
    for task in 0..8u64 {
        let b = Arc::clone(&b);
        let ops = Arc::clone(&ops);
        hs.push(tokio::spawn(async move {
            let mut r = Rng(0x1234_5678_9ABC_DEF1 ^ ((task + 1) * 0x9E37));
            for _ in 0..400 {
                let id = r.below(60) as i32;
                let cmd = match r.below(6) {
                    0 | 1 => doc! {"insert": "c", "documents": [{"_id": id, "u": r.below(40) as i32, "tags": [r.below(5) as i32, r.below(5) as i32], "g": r.below(3) as i32, "v": r.below(100) as i32}]},
                    2 => doc! {"update": "c", "updates": [{"q": {"_id": id}, "u": {"$set": {"u": r.below(40) as i32, "tags": [r.below(5) as i32]}}, "upsert": true}]},
                    3 => doc! {"update": "c", "updates": [{"q": {"g": r.below(3) as i32}, "u": {"$inc": {"v": 1}, "$addToSet": {"tags": r.below(5) as i32}}, "multi": true}]},
                    4 => doc! {"delete": "c", "deletes": [{"q": {"_id": id}, "limit": 1}]},
                    _ => doc! {"findAndModify": "c", "query": {"tags": r.below(5) as i32}, "sort": {"_id": 1}, "update": {"$unset": {"u": ""}}},
                };
                let res = b.run("s", cmd.clone()).await;
                assert_eq!(res.get_f64("ok").unwrap_or(0.0), 1.0, "{cmd} -> {res}");
                ops.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for task in 0..4u64 {
        let b = Arc::clone(&b);
        let conflicts = Arc::clone(&conflicts);
        hs.push(tokio::spawn(async move {
            let mut r = Rng(0xFEED_0000 + task);
            let l = lsid(100 + task);
            let mut n = 0i64;
            for _ in 0..150 {
                n += 1;
                let from = r.below(20) as i32;
                let to = r.below(20) as i32;
                let amt = r.below(10) as i32;
                let start = doc! {"lsid": l.clone(), "txnNumber": n, "autocommit": false, "startTransaction": true};
                let mut a = doc! {"update": "acct", "updates": [{"q": {"_id": from}, "u": {"$inc": {"bal": -amt}}}]};
                a.extend(start);
                let r1 = b.run("s", a).await;
                if r1.get_f64("ok") == Ok(0.0) {
                    assert_eq!(r1.get_i32("code").unwrap(), 112, "{r1}");
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let r2 = b
                    .run("s", doc! {"update": "acct", "updates": [{"q": {"_id": to}, "u": {"$inc": {"bal": amt}}}], "lsid": l.clone(), "txnNumber": n, "autocommit": false})
                    .await;
                if r2.get_f64("ok") == Ok(0.0) {
                    assert_eq!(r2.get_i32("code").unwrap(), 112, "{r2}");
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let c = b
                    .run("admin", doc! {"commitTransaction": 1, "lsid": l.clone(), "txnNumber": n, "autocommit": false})
                    .await;
                assert_eq!(c.get_f64("ok").unwrap_or(0.0), 1.0, "{c}");
            }
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    common::check_indexes(&b, "s").await;
    let docs = b.find_all("s", doc! {"find": "c"}).await;
    let mut seen = HashSet::new();
    for d in &docs {
        if let Some(u) = d.get("u") {
            assert!(
                seen.insert(sprig_doc::keyenc::eq_key(u)),
                "unique key {u} held twice"
            );
        }
    }
    let accts = b.find_all("s", doc! {"find": "acct"}).await;
    let total: i32 = accts.iter().map(|d| d.get_i32("bal").unwrap()).sum();
    assert_eq!(total, 2000, "transfers lost or made money");
    eprintln!(
        "stress: {} writes, {} transfer conflicts, {} documents left",
        ops.load(Ordering::Relaxed),
        conflicts.load(Ordering::Relaxed),
        docs.len()
    );
}
