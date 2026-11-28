//! `$vectorSearch` indexes at size, through the MongoDB Rust driver:
//! an index is built before it reports queryable, searches during the
//! build never fail on a lock, the side table follows writes, and dropping
//! a collection drops its search indexes.
//!
//! - `SPRIG_TEST_MONGO_URI`: a SprigDB gateway's MongoDB listener with a
//!   tenant login.
//! - `SPRIG_TEST_VEC_BENCH`: `<vectors>x<dims>` (for example `100000x128`)
//!   also runs `vector_ann_numbers`, which prints recall@10 and queries per
//!   second at several `numCandidates` on synthetic clustered vectors.
//!
//! The tests use the databases `vxlive_build`, `vxlive_drop` and
//! `vxlive_bench`, and drop them at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::doc_markdown,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::many_single_char_names,
    clippy::similar_names
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use mongodb::bson::{Document, doc};
use mongodb::{Client, Collection, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
    fn range(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next()
    }
}

fn code_of(e: &mongodb::error::Error) -> i32 {
    match e.kind.as_ref() {
        mongodb::error::ErrorKind::Command(c) => c.code,
        _ => -1,
    }
}

async fn agg(c: &Collection<Document>, p: Vec<Document>) -> Result<Vec<Document>, String> {
    let r = c
        .aggregate(p)
        .await
        .map_err(|e| format!("{} {e}", code_of(&e)))?;
    r.try_collect()
        .await
        .map_err(|e| format!("{} {e}", code_of(&e)))
}

/// `n` vectors of `dims` around `clusters` centres (clustered data, as
/// real embeddings are: HNSW's recall on uniform noise is poor).
fn clustered(g: &mut Rng, n: usize, dims: usize, clusters: usize) -> Vec<Vec<f64>> {
    let centres: Vec<Vec<f64>> = (0..clusters)
        .map(|_| (0..dims).map(|_| g.range(-10.0, 10.0)).collect())
        .collect();
    (0..n)
        .map(|i| {
            let c = &centres[i % clusters];
            c.iter().map(|x| x + g.range(-1.0, 1.0)).collect()
        })
        .collect()
}

async fn load(c: &Collection<Document>, vecs: &[Vec<f64>]) {
    for (k, chunk) in vecs.chunks(1000).enumerate() {
        let docs: Vec<Document> = chunk
            .iter()
            .enumerate()
            .map(|(i, v)| doc! {"_id": (k * 1000 + i) as i64, "v": v.clone()})
            .collect();
        c.insert_many(docs).await.unwrap();
    }
}

async fn create_index(db: &Database, coll: &str, dims: usize) -> Result<(), String> {
    db.run_command(doc! {"createSearchIndexes": coll, "indexes": [
        {"name": "vx", "type": "vectorSearch", "definition": {"fields": [
            {"type": "vector", "path": "v", "numDimensions": dims as i32, "similarity": "euclidean"}]}}]})
        .await
        .map(|_| ())
        .map_err(|e| format!("{} {e}", code_of(&e)))
}

/// `(status, queryable)` of index `vx`.
async fn status(c: &Collection<Document>) -> (String, bool) {
    let l = agg(c, vec![doc! {"$listSearchIndexes": {"name": "vx"}}])
        .await
        .unwrap();
    let d = l.first().cloned().unwrap_or_default();
    (
        d.get_str("status").unwrap_or("").to_owned(),
        d.get_bool("queryable").unwrap_or(false),
    )
}

fn search(q: &[f64], nc: i32, k: i32) -> Vec<Document> {
    vec![
        doc! {"$vectorSearch": {"index": "vx", "path": "v", "queryVector": q.to_vec(), "numCandidates": nc, "limit": k}},
        doc! {"$project": {"_id": 1}},
    ]
}

fn exact(q: &[f64], k: i32) -> Vec<Document> {
    vec![
        doc! {"$vectorSearch": {"index": "vx", "path": "v", "queryVector": q.to_vec(), "exact": true, "limit": k}},
        doc! {"$project": {"_id": 1}},
    ]
}

fn ids(d: &[Document]) -> Vec<i64> {
    d.iter().filter_map(|x| x.get_i64("_id").ok()).collect()
}

/// An index on 30,000 vectors: `BUILDING` and not queryable until its side
/// table is built; eight readers and a writer meanwhile, none failing;
/// then approximate results agree with exact ones, and follow writes.
#[ignore = "live: needs SPRIG_TEST_MONGO_URI; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_is_built_before_it_is_queryable_and_reads_never_conflict() {
    let Some(s_uri) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let client = Client::with_uri_str(&s_uri).await.unwrap();
    let db = client.database("vxlive_build");
    db.drop().await.unwrap();
    let c = db.collection::<Document>("items");
    let dims = 16;
    let mut g = Rng(7);
    let vecs = clustered(&mut g, 30_000, dims, 100);
    load(&c, &vecs).await;
    let started = Instant::now();
    create_index(&db, "items", dims).await.unwrap();
    // Searches and writes during the build.
    let stop = Arc::new(AtomicBool::new(false));
    let (ok, failed) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let (c, stop, ok, failed) = (
            c.clone(),
            Arc::clone(&stop),
            Arc::clone(&ok),
            Arc::clone(&failed),
        );
        let qs: Vec<Vec<f64>> = vecs.iter().skip(t as usize).step_by(997).cloned().collect();
        tasks.push(tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                match agg(&c, search(&qs[i % qs.len()], 50, 10)).await {
                    Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                    Err(e) => {
                        eprintln!("search failed during the build: {e}");
                        failed.fetch_add(1, Ordering::Relaxed)
                    }
                };
                i += 1;
            }
        }));
    }
    {
        let (c, stop, failed) = (c.clone(), Arc::clone(&stop), Arc::clone(&failed));
        let mut g = Rng(11);
        tasks.push(tokio::spawn(async move {
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let v: Vec<f64> = (0..dims).map(|_| g.range(-10.0, 10.0)).collect();
                if let Err(e) = c
                    .update_one(doc! {"_id": i % 30_000}, doc! {"$set": {"v": v}})
                    .await
                {
                    eprintln!("write failed during the build: {e}");
                    failed.fetch_add(1, Ordering::Relaxed);
                }
                i += 37;
            }
        }));
    }
    let mut saw_building = false;
    loop {
        let (st, q) = status(&c).await;
        if q {
            assert_eq!(st, "READY");
            break;
        }
        assert_eq!(st, "BUILDING");
        saw_building = true;
        assert!(
            started.elapsed() < Duration::from_secs(600),
            "not built in 10 min"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let built = started.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await.unwrap();
    }
    let (ok, failed) = (ok.load(Ordering::Relaxed), failed.load(Ordering::Relaxed));
    println!(
        "built 30,000 x {dims} in {built:?}; during the build: {ok} searches, {failed} failures"
    );
    assert!(
        saw_building,
        "listed queryable before its side table was built"
    );
    assert_eq!(failed, 0, "searches or writes failed during the build");
    assert!(ok > 0);
    // Approximate against exact, now that it is built.
    let (mut hit, mut total) = (0usize, 0usize);
    for q in vecs.iter().step_by(601).take(50) {
        let ex = ids(&agg(&c, exact(q, 10)).await.unwrap());
        let ap = ids(&agg(&c, search(q, 100, 10)).await.unwrap());
        total += ex.len();
        hit += ex.iter().filter(|x| ap.contains(x)).count();
    }
    let recall = hit as f64 / total as f64;
    println!("recall@10 at numCandidates 100: {recall:.3}");
    assert!(recall >= 0.9, "recall@10 {recall}");
    // A new document is found within a few seconds; a deleted one at once
    // (every hit is read back from its document).
    let far: Vec<f64> = vec![500.0; dims];
    c.insert_one(doc! {"_id": 99_999_i64, "v": far.clone()})
        .await
        .unwrap();
    let t = Instant::now();
    loop {
        let got = ids(&agg(&c, search(&far, 20, 1)).await.unwrap());
        if got == [99_999] {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(10), "not found: {got:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!("a new document searchable after {:?}", t.elapsed());
    c.delete_one(doc! {"_id": 99_999_i64}).await.unwrap();
    let got = ids(&agg(&c, search(&far, 20, 1)).await.unwrap());
    assert!(!got.contains(&99_999), "{got:?}");
    db.drop().await.unwrap();
}

/// Dropping a collection drops its search indexes: the same name can be
/// created again with another definition, and searches use the new one.
#[ignore = "live: needs SPRIG_TEST_MONGO_URI; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_a_collection_drops_its_search_indexes() {
    let Some(s_uri) = uri("SPRIG_TEST_MONGO_URI") else {
        eprintln!("SPRIG_TEST_MONGO_URI not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let client = Client::with_uri_str(&s_uri).await.unwrap();
    let db = client.database("vxlive_drop");
    db.drop().await.unwrap();
    let c = db.collection::<Document>("ann");
    let mut g = Rng(3);
    load(&c, &clustered(&mut g, 2_000, 16, 20)).await;
    create_index(&db, "ann", 16).await.unwrap();
    let t = Instant::now();
    while !status(&c).await.1 {
        assert!(
            t.elapsed() < Duration::from_secs(120),
            "16-d index not built"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    c.drop().await.unwrap();
    // The same name again, with 8-dimension vectors: no index of the
    // dropped collection's is left.
    let v8 = clustered(&mut g, 2_000, 8, 20);
    load(&c, &v8).await;
    let left = agg(&c, vec![doc! {"$listSearchIndexes": {}}])
        .await
        .unwrap();
    assert!(
        left.is_empty(),
        "the index outlived its collection: {left:?}"
    );
    create_index(&db, "ann", 8).await.unwrap();
    let t = Instant::now();
    while !status(&c).await.1 {
        assert!(
            t.elapsed() < Duration::from_secs(120),
            "8-d index not built"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = ids(&agg(&c, search(&v8[5], 20, 1)).await.unwrap());
    assert_eq!(got, vec![5]);
    // And a search with the old dimensions is refused, not answered from
    // the old index.
    let e = agg(&c, search(&[0.0; 16], 20, 1)).await.unwrap_err();
    assert!(e.starts_with("39736"), "{e}");
    db.drop().await.unwrap();
}

/// recall@10 and queries per second (8 clients) at several
/// `numCandidates`, with `SPRIG_TEST_VEC_BENCH=<vectors>x<dims>`.
#[ignore = "live: needs SPRIG_TEST_MONGO_URI, SPRIG_TEST_VEC_BENCH; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn vector_ann_numbers() {
    let (Some(s_uri), Some(spec)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_VEC_BENCH"))
    else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_VEC_BENCH not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let (n, dims) = spec.split_once('x').unwrap();
    let (n, dims): (usize, usize) = (n.parse().unwrap(), dims.parse().unwrap());
    let client = Client::with_uri_str(&s_uri).await.unwrap();
    let db = client.database("vxlive_bench");
    db.drop().await.unwrap();
    let c = db.collection::<Document>("ann");
    let mut g = Rng(128);
    let vecs = clustered(&mut g, n, dims, (n / 100).max(10));
    let t = Instant::now();
    load(&c, &vecs).await;
    println!("loaded {n} x {dims} in {:?}", t.elapsed());
    let t = Instant::now();
    create_index(&db, "ann", dims).await.unwrap();
    while !status(&c).await.1 {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let build = t.elapsed();
    println!("index built (queryable) in {build:?}");
    // 1,000 queries near the data, ground truth by brute force here.
    let queries: Vec<Vec<f64>> = (0..1000)
        .map(|i| {
            vecs[(i * 7919) % n]
                .iter()
                .map(|x| x + g.range(-0.5, 0.5))
                .collect()
        })
        .collect();
    let truth: Vec<Vec<i64>> = queries
        .iter()
        .map(|q| {
            let mut d: Vec<(f64, i64)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    (
                        v.iter().zip(q).map(|(a, b)| (a - b) * (a - b)).sum::<f64>(),
                        i as i64,
                    )
                })
                .collect();
            d.select_nth_unstable_by(10, |a, b| a.0.total_cmp(&b.0));
            let mut top: Vec<(f64, i64)> = d[..10].to_vec();
            top.sort_by(|a, b| a.0.total_cmp(&b.0));
            top.into_iter().map(|x| x.1).collect()
        })
        .collect();
    let queries = Arc::new(queries);
    for nc in [16, 32, 64, 128, 256] {
        let next = Arc::new(AtomicU64::new(0));
        let t = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (c, next, queries) = (c.clone(), Arc::clone(&next), Arc::clone(&queries));
            tasks.push(tokio::spawn(async move {
                let mut out = Vec::new();
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                    if i >= queries.len() {
                        return out;
                    }
                    let s = Instant::now();
                    let got = ids(&agg(&c, search(&queries[i], nc, 10)).await.unwrap());
                    out.push((i, got, s.elapsed()));
                }
            }));
        }
        let mut hits = 0usize;
        let mut lat = Vec::new();
        for t in tasks {
            for (i, got, d) in t.await.unwrap() {
                hits += truth[i].iter().filter(|x| got.contains(x)).count();
                lat.push(d);
            }
        }
        let wall = t.elapsed();
        lat.sort();
        println!(
            "numCandidates {nc}: recall@10 {:.3}, {:.0} queries/s, p50 {:?}, p99 {:?}",
            hits as f64 / (10.0 * queries.len() as f64),
            queries.len() as f64 / wall.as_secs_f64(),
            lat[lat.len() / 2],
            lat[lat.len() * 99 / 100 - 1]
        );
    }
    db.drop().await.unwrap();
}
