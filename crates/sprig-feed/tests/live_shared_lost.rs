//! One subscription whose position is purged, on a real node: it alone
//! fails, while the others on the same shared reader deliver every row
//! exactly once.
//!
//! Three tenants write rows into an outbox table while a shared reader
//! serves one subscription per tenant (the way a worker's stream
//! sequencers and queue forwarders read). Tenant 3's subscription stops;
//! the binary logs holding its position are purged. Then:
//!
//! 1. a restart: a new shared reader, every subscription back at once from
//!    its own position (tenant 3's purged). Before, the gathering reader
//!    moved its start back to tenant 3's position, the node refused it,
//!    and every subscription ended with 39800, at every retry: one lost
//!    reader stopped them all. Now tenants 1 and 2 read on, each row once,
//!    and tenant 3 alone gets 39800, again on a retry;
//! 2. a whole-tenant subscription (a trigger's or a change stream's) of
//!    tenant 3 from its purged position, a few groups behind the running
//!    reader: before, the reader replayed from there and failed everyone;
//!    now only it fails;
//! 3. at the end, with the lost subscriptions gone, tenants 1 and 2 read
//!    through one shared dump and no catch-up dump.
//!
//! It purges the node's binary logs: point it only at a node of a private
//! cluster made for tests.
//!
//! ```sh
//! SPRIG_TEST_FEED_LOST_NODE=mysql://root:pw@127.0.0.1:47311 \
//! SPRIG_TEST_FEED_LOST_TENANT=47306099900 \
//!     cargo test -p sprig-feed --test live_shared_lost -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::many_single_char_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogSource, GtidPos, Value};
use sprig_common::SprigCode;
use sprig_common::ids::{ClusterId, TenantId};
use sprig_feed::binlog::StaticClusters;
use sprig_feed::shared::{Kept, SharedFeed, SharedOpts};
use sprig_feed::{Clusters, SourceItem, TxnSource};

const TABLE: &str = "__sprig_qo";

fn schema(t: TenantId) -> String {
    format!("t{}_d1_g1_s1", t.0)
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

async fn position(c: &mut mysql_async::Conn) -> GtidPos {
    let v: Option<String> = c.query_first("SELECT @@gtid_binlog_pos").await.unwrap();
    v.unwrap_or_default().parse().unwrap()
}

/// One subscription's reads: the row ids it got (with how often), and its
/// position after the last item.
#[derive(Default)]
struct Got {
    ids: BTreeMap<i64, u32>,
    pos: GtidPos,
}

fn note(got: &mut Got, item: &SourceItem) {
    match item {
        SourceItem::Txn(t) => {
            for r in &t.rows {
                if let Some(Some(Value::Int(id))) = r.after.as_ref().map(|a| a.0[0].clone()) {
                    *got.ids.entry(id).or_default() += 1;
                }
            }
            got.pos = sprig_feed::shared::newer(&got.pos, &t.after());
        }
        SourceItem::Idle { pos } => got.pos = sprig_feed::shared::newer(&got.pos, pos),
    }
}

/// Reads `src` until `stop` (then returns what it got), or until an error.
fn reader(
    mut src: Box<dyn TxnSource>,
    stop: Arc<AtomicBool>,
    mut got: Got,
) -> tokio::task::JoinHandle<(Got, Option<sprig_common::SprigError>)> {
    tokio::spawn(async move {
        while !stop.load(Ordering::Relaxed) {
            match tokio::time::timeout(Duration::from_millis(200), src.next()).await {
                Err(_) => {}
                Ok(Ok(item)) => note(&mut got, &item),
                Ok(Err(e)) => return (got, Some(e)),
            }
        }
        (got, None)
    })
}

/// The first error `src` returns within 30 s.
async fn fails(src: &mut Box<dyn TxnSource>) -> sprig_common::SprigError {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Err(e) = src.next().await {
                return e;
            }
        }
    })
    .await
    .expect("no error within 30 s")
}

/// Writes one row per tenant per transaction, ids from 1, until `stop`.
fn writer(
    url: String,
    tenants: Vec<TenantId>,
    stop: Arc<AtomicBool>,
    n: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut c = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
            .await
            .unwrap();
        while !stop.load(Ordering::Relaxed) {
            let id = n.load(Ordering::SeqCst) + 1;
            let mut tx = c
                .start_transaction(mysql_async::TxOpts::default())
                .await
                .unwrap();
            for t in &tenants {
                tx.query_drop(format!(
                    "INSERT INTO {}.{TABLE} (id) VALUES ({id})",
                    schema(*t)
                ))
                .await
                .unwrap();
            }
            tx.commit().await.unwrap();
            n.store(id, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
}

fn exactly_once(what: &str, got: &Got, upto: u64) -> Result<(), String> {
    let missing: Vec<i64> = (1..=upto.cast_signed())
        .filter(|i| !got.ids.contains_key(i))
        .collect();
    let twice: Vec<(i64, u32)> = got
        .ids
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(i, n)| (*i, *n))
        .collect();
    if missing.is_empty() && twice.is_empty() {
        eprintln!("ok   {what}: rows 1..={upto} exactly once");
        Ok(())
    } else {
        Err(format!(
            "{what}: {} missing (first {:?}), {} more than once (first {:?})",
            missing.len(),
            missing.first(),
            twice.len(),
            twice.first()
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_subscription_fails_alone() {
    let Ok(url) = std::env::var("SPRIG_TEST_FEED_LOST_NODE") else {
        eprintln!("skipped: set SPRIG_TEST_FEED_LOST_NODE");
        return;
    };
    let base: u64 = std::env::var("SPRIG_TEST_FEED_LOST_TENANT")
        .ok()
        .and_then(|v| v.parse().ok())
        .expect("set SPRIG_TEST_FEED_LOST_TENANT to an unused tenant id of your range");
    let tenants: Vec<TenantId> = (1..=3).map(|i| TenantId(base + i)).collect();
    let (t1, t2, t3) = (tenants[0], tenants[1], tenants[2]);
    let mut n = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    for t in &tenants {
        let s = schema(*t);
        must(&mut n, &format!("DROP DATABASE IF EXISTS {s}")).await;
        must(&mut n, &format!("CREATE DATABASE {s}")).await;
        must(
            &mut n,
            &format!("CREATE TABLE {s}.{TABLE} (id BIGINT PRIMARY KEY)"),
        )
        .await;
    }
    let c = ClusterId::new("dc-a-c1").unwrap();
    let clusters: Arc<dyn Clusters> =
        Arc::new(StaticClusters::new().with(c.clone(), BinlogSource::from_url(&url).unwrap()));
    let mut failures: Vec<String> = Vec::new();
    let start = position(&mut n).await;
    eprintln!("start at {start}");

    let wstop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let w = writer(
        url.clone(),
        tenants.clone(),
        Arc::clone(&wstop),
        Arc::clone(&written),
    );

    // Round 0: every tenant read through one shared reader; tenant 3
    // stops after a few seconds, the others read on.
    let opts = SharedOpts {
        ring_groups: 200,
        ..SharedOpts::default()
    };
    let feed = SharedFeed::new(Arc::clone(&clusters), opts.clone());
    let view = feed.view(Kept::Tables(vec![TABLE.to_owned()].into()));
    let stop3 = Arc::new(AtomicBool::new(false));
    let stop12 = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for t in &tenants {
        let src = view.open(&c, *t, &start).await.unwrap();
        let stop = if *t == t3 { &stop3 } else { &stop12 };
        readers.push(reader(src, Arc::clone(stop), Got::default()));
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    stop3.store(true, Ordering::Relaxed);
    let (g3, e) = readers.pop().unwrap().await.unwrap();
    assert!(e.is_none(), "round 0: {e:?}");
    let lost_at = g3.pos.clone();
    eprintln!("tenant 3 stops at {lost_at} with {} rows", g3.ids.len());

    // More writes; then the binary logs holding tenant 3's position go,
    // while tenants 1 and 2 read on.
    tokio::time::sleep(Duration::from_secs(2)).await;
    must(&mut n, "FLUSH BINARY LOGS").await;
    let logs: Vec<(String, u64)> = n.query("SHOW BINARY LOGS").await.unwrap();
    let newest = logs.last().unwrap().0.clone();
    must(&mut n, &format!("PURGE BINARY LOGS TO '{newest}'")).await;
    let oldest: Option<String> = n
        .query_first(format!("SELECT BINLOG_GTID_POS('{newest}', 4)"))
        .await
        .unwrap();
    eprintln!(
        "purged up to {newest}, which starts at {}",
        oldest.unwrap_or_default()
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop12.store(true, Ordering::Relaxed);
    let mut got: Vec<Got> = Vec::new();
    for r in readers {
        let (g, e) = r.await.unwrap();
        assert!(e.is_none(), "round 0: {e:?}");
        got.push(g);
    }
    drop(view);
    drop(feed);
    eprintln!("tenants 1 and 2 stop at {} and {}", got[0].pos, got[1].pos);

    // 1. A restart: every subscription back at once from its own position.
    let feed = SharedFeed::new(Arc::clone(&clusters), opts.clone());
    let view = feed.view(Kept::Tables(vec![TABLE.to_owned()].into()));
    let s1 = view.open(&c, t1, &got[0].pos).await.unwrap();
    let s2 = view.open(&c, t2, &got[1].pos).await.unwrap();
    let mut s3 = view.open(&c, t3, &lost_at).await.unwrap();
    let stop1 = Arc::new(AtomicBool::new(false));
    let g1 = std::mem::take(&mut got[0]);
    let g2 = std::mem::take(&mut got[1]);
    let r1 = reader(s1, Arc::clone(&stop1), g1);
    let r2 = reader(s2, Arc::clone(&stop1), g2);
    let e = fails(&mut s3).await;
    eprintln!("tenant 3 after the restart: {e}");
    if e.code() != Some(SprigCode::FeedHistoryLost) {
        failures.push(format!("tenant 3 after the restart: {e}"));
    }
    drop(s3);
    // Its retry fails again, alone.
    let mut s3 = view.open(&c, t3, &lost_at).await.unwrap();
    let e = fails(&mut s3).await;
    if e.code() != Some(SprigCode::FeedHistoryLost) {
        failures.push(format!("tenant 3's retry: {e}"));
    }
    drop(s3);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 2. A whole-tenant subscription of tenant 3 from its purged position.
    let all = feed.view(Kept::All);
    let mut w3 = all.open(&c, t3, &lost_at).await.unwrap();
    let e = fails(&mut w3).await;
    eprintln!("tenant 3, kept whole: {e}");
    if e.code() != Some(SprigCode::FeedHistoryLost) {
        failures.push(format!("tenant 3 kept whole: {e}"));
    }
    drop(w3);
    drop(all);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 3. Writes stop; tenants 1 and 2 read to the end.
    wstop.store(true, Ordering::Relaxed);
    w.await.unwrap();
    let last = written.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let stats = feed.stats(&c).unwrap();
    stop1.store(true, Ordering::Relaxed);
    let (g1, e1) = r1.await.unwrap();
    let (g2, e2) = r2.await.unwrap();
    for (what, e) in [("tenant 1", e1), ("tenant 2", e2)] {
        if let Some(e) = e {
            failures.push(format!("{what} failed: {e}"));
        }
    }
    for (what, g) in [("tenant 1", &g1), ("tenant 2", &g2)] {
        if let Err(f) = exactly_once(what, g, last) {
            failures.push(f);
        }
    }
    eprintln!("{last} transactions; reader at the end: {stats:?}");
    if stats.catchups != 0 || stats.subscriptions != 2 {
        failures.push(format!(
            "at the end: {stats:?}, want 2 subscriptions and no catch-up dump"
        ));
    }

    for t in &tenants {
        must(&mut n, &format!("DROP DATABASE IF EXISTS {}", schema(*t))).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
