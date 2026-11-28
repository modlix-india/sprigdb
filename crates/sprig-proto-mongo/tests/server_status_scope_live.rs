//! `serverStatus` shows a tenant only its own numbers: change-stream
//! counters (`metrics.changeStreams`, the older open/opened/lifespan ones
//! and the batch, event and option counters), open cursors
//! (`metrics.cursor.open.total`), spill counters (`metrics.query`) and
//! connections (`connections.current`). Two tenants on one gateway: the
//! first works (change streams with options, events read, cursors left
//! open, a command with `allowDiskUse: false`, extra connections); its own
//! `serverStatus` moves, the second tenant's doesn't.
//!
//! - `SPRIG_TEST_MONGO_URI`: a gateway's MongoDB listener, a login of
//!   tenant A.
//! - `SPRIG_TEST_MONGO_URI2`: the same gateway, a login of tenant B.
//!
//! Skipped unless both are set. Uses the database `server_status_scope`
//! of tenant A and drops it at the end.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::time::Duration;

use mongodb::bson::{Bson, Document, doc};
use mongodb::{Client, Database};

fn uri(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.is_empty())
}

/// The fields compared, as dotted paths of `serverStatus`.
const PATHS: [&str; 15] = [
    "metrics.changeStreams.cursor.totalOpened",
    "metrics.changeStreams.cursor.open.total",
    "metrics.changeStreams.cursor.lifespan.totalCount",
    "metrics.changeStreams.cursor.batchesReturned",
    "metrics.changeStreams.cursor.docsReturned",
    "metrics.changeStreams.cursor.bytesReturned",
    "metrics.changeStreams.cursor.docsExamined",
    "metrics.changeStreams.option.fullDocument.updateLookup",
    "metrics.changeStreams.option.cursor.batchSize.totalCount",
    "metrics.changeStreams.option.cursor.maxTimeMS.totalCount",
    "metrics.changeStreams.scope.collection",
    "metrics.changeStreams.error.nonRetriable.other",
    "metrics.cursor.open.total",
    "metrics.query.allowDiskUseFalse",
    "connections.current",
];

/// A number at a dotted path (0 when missing).
fn at(d: &Document, path: &str) -> i64 {
    let mut cur = d;
    let parts: Vec<&str> = path.split('.').collect();
    for k in &parts[..parts.len() - 1] {
        match cur.get_document(k) {
            Ok(x) => cur = x,
            Err(_) => return 0,
        }
    }
    match cur.get(parts[parts.len() - 1]) {
        Some(Bson::Int64(n)) => *n,
        Some(Bson::Int32(n)) => i64::from(*n),
        Some(Bson::Double(f)) => *f as i64,
        _ => 0,
    }
}

async fn status(db: &Database) -> Document {
    db.run_command(doc! {"serverStatus": 1}).await.unwrap()
}

fn values(d: &Document) -> Vec<i64> {
    PATHS.iter().map(|p| at(d, p)).collect()
}

#[ignore = "live: needs SPRIG_TEST_MONGO_URI, SPRIG_TEST_MONGO_URI2; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_status_shows_each_tenant_its_own_numbers() {
    let (Some(ua), Some(ub)) = (uri("SPRIG_TEST_MONGO_URI"), uri("SPRIG_TEST_MONGO_URI2")) else {
        eprintln!("SPRIG_TEST_MONGO_URI or SPRIG_TEST_MONGO_URI2 not set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let a = Client::with_uri_str(&ua).await.unwrap();
    let b = Client::with_uri_str(&ub).await.unwrap();
    let adb = a.database("server_status_scope");
    let badmin = b.database("admin");
    adb.drop().await.unwrap();
    adb.collection::<Document>("ev")
        .insert_one(doc! {"_id": 0, "x": "seed"})
        .await
        .unwrap();
    let a0 = values(&status(&adb).await);
    let b0 = values(&status(&badmin).await);

    // Tenant A: three change streams with options, events read through
    // getMore, two closed and one left open.
    let mut ids = Vec::new();
    for _ in 0..3 {
        let r = adb
            .run_command(doc! {
                "aggregate": "ev",
                "pipeline": [{"$changeStream": {"fullDocument": "updateLookup"}}],
                "cursor": {"batchSize": 5},
            })
            .await
            .unwrap();
        ids.push(r.get_document("cursor").unwrap().get_i64("id").unwrap());
    }
    for i in 1..=4 {
        adb.collection::<Document>("ev")
            .insert_one(doc! {"_id": i, "x": format!("event {i}")})
            .await
            .unwrap();
    }
    let mut events = 0;
    for id in &ids {
        let mut got = 0;
        for _ in 0..5 {
            let r = adb
                .run_command(doc! {"getMore": *id, "collection": "ev", "maxTimeMS": 1000})
                .await
                .unwrap();
            got += r
                .get_document("cursor")
                .unwrap()
                .get_array("nextBatch")
                .unwrap()
                .len();
            if got >= 4 {
                break;
            }
        }
        events += got;
    }
    adb.run_command(doc! {"killCursors": "ev", "cursors": [ids[0], ids[1]]})
        .await
        .unwrap();
    // A stream that fails: a resume token that isn't one.
    let _ = adb
        .run_command(doc! {
            "aggregate": "ev",
            "pipeline": [{"$changeStream": {"resumeAfter": {"_data": "zz"}}}],
            "cursor": {},
        })
        .await;
    // A find with allowDiskUse: false, and an open find cursor.
    adb.run_command(doc! {"find": "ev", "allowDiskUse": false, "batchSize": 1})
        .await
        .unwrap();
    // More connections of tenant A.
    let mut extra = Vec::new();
    for _ in 0..4 {
        let c = Client::with_uri_str(&ua).await.unwrap();
        // A data command opens the connection's gateway session.
        c.database("server_status_scope")
            .run_command(doc! {"find": "ev", "limit": 1})
            .await
            .unwrap();
        extra.push(c);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let a1 = values(&status(&adb).await);
    let b1 = values(&status(&badmin).await);

    let mut bad = Vec::new();
    println!("{events} events read by tenant A's streams");
    for (i, p) in PATHS.iter().enumerate() {
        println!("{p}: A {} -> {}, B {} -> {}", a0[i], a1[i], b0[i], b1[i]);
    }
    // A's own counters moved.
    for (p, min) in [
        ("metrics.changeStreams.cursor.totalOpened", 3),
        ("metrics.changeStreams.cursor.open.total", 1),
        ("metrics.changeStreams.cursor.lifespan.totalCount", 2),
        ("metrics.changeStreams.cursor.docsReturned", 4),
        ("metrics.changeStreams.option.fullDocument.updateLookup", 3),
        ("metrics.changeStreams.scope.collection", 3),
        ("metrics.cursor.open.total", 1),
        ("metrics.query.allowDiskUseFalse", 1),
        ("connections.current", 4),
    ] {
        let i = PATHS.iter().position(|x| *x == p).unwrap();
        if a1[i] - a0[i] < min {
            bad.push(format!(
                "tenant A's {p} moved by {} (want at least {min})",
                a1[i] - a0[i]
            ));
        }
    }
    // B's didn't. Its own connections may move by its driver's pool and
    // monitor, never by A's four clients.
    for (i, p) in PATHS.iter().enumerate() {
        let moved = b1[i] - b0[i];
        let ok = if *p == "connections.current" {
            moved < 4
        } else {
            moved == 0
        };
        if !ok {
            bad.push(format!("tenant B sees {p} move by {moved}"));
        }
    }
    drop(extra);
    adb.drop().await.unwrap();
    for x in &bad {
        eprintln!("FAIL {x}");
    }
    assert!(bad.is_empty(), "{} failures", bad.len());
}
