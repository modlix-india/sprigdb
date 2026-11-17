//! A Galera node's purged local GTID domain, on a real node.
//!
//! A Galera node with `wsrep_gtid_mode` writes replicated transactions in
//! the cluster's shared domain and its own local writes (`wsrep_on = OFF`,
//! an index built with `wsrep_OSU_method = RSU`) in a domain of its own.
//! After the binary logs where that local domain starts are purged, the
//! node refuses (1236) a dump from a position without it. The feed's dump
//! now adds the purged local domain and starts; a position whose shared
//! domain is purged stays refused (39800) with the loss spelled out.
//!
//! It writes node-local transactions and purges the node's binary logs:
//! point it only at a node of a private Galera cluster made for tests.
//!
//! ```sh
//! SPRIG_TEST_FEED_GALERA_NODE=mysql://root:pw@127.0.0.1:46311 \
//!     cargo test -p sprig-feed --test live_local_domains -- --nocapture
//! ```

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::time::Duration;

use mysql_async::prelude::Queryable;
use sprig_binlog::{BinlogSource, Gtid, GtidPos};
use sprig_common::SprigCode;
use sprig_common::ids::TenantId;
use sprig_feed::binlog::BinlogTxns;
use sprig_feed::{SourceItem, TxnSource};

const DB: &str = "fll_probe";
const LOCAL_DB: &str = "fll_local";

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

async fn value(c: &mut mysql_async::Conn, s: &str) -> String {
    let v: Option<Option<String>> = c.query_first(s).await.unwrap();
    v.flatten().unwrap_or_default()
}

/// The first idle position the dump reports.
async fn idle_pos(src: &mut BinlogTxns) -> GtidPos {
    let end = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let item = tokio::time::timeout_at(end, src.next())
            .await
            .expect("no idle position within 20 s")
            .unwrap();
        if let SourceItem::Idle { pos } = item {
            return pos;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_purged_local_domain_is_added_and_a_purged_shared_one_is_a_loss() {
    let Ok(url) = std::env::var("SPRIG_TEST_FEED_GALERA_NODE") else {
        eprintln!("skipped: set SPRIG_TEST_FEED_GALERA_NODE");
        return;
    };
    let mut n = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    let shared: u32 = value(&mut n, "SELECT @@global.wsrep_gtid_domain_id")
        .await
        .parse()
        .unwrap();
    let local: u32 = value(&mut n, "SELECT @@gtid_domain_id")
        .await
        .parse()
        .unwrap();
    assert_eq!(
        value(&mut n, "SELECT @@global.wsrep_gtid_mode").await,
        "1",
        "the node needs wsrep_gtid_mode"
    );
    assert_ne!(shared, local);

    must(&mut n, &format!("CREATE DATABASE IF NOT EXISTS {DB}")).await;
    must(
        &mut n,
        &format!("CREATE TABLE IF NOT EXISTS {DB}.a (id INT PRIMARY KEY, v INT)"),
    )
    .await;
    must(&mut n, &format!("REPLACE INTO {DB}.a VALUES (1, 1)")).await;
    // Local writes: a table made and written with wsrep_on = OFF, and an
    // index built with RSU.
    must(&mut n, "SET SESSION wsrep_on = OFF").await;
    must(&mut n, &format!("CREATE DATABASE IF NOT EXISTS {LOCAL_DB}")).await;
    must(
        &mut n,
        &format!("CREATE TABLE IF NOT EXISTS {LOCAL_DB}.n (id INT PRIMARY KEY)"),
    )
    .await;
    must(&mut n, &format!("REPLACE INTO {LOCAL_DB}.n VALUES (1)")).await;
    must(&mut n, "SET SESSION wsrep_on = ON").await;
    must(&mut n, "SET SESSION wsrep_OSU_method = 'RSU'").await;
    let _ = n
        .query_drop(format!("ALTER TABLE {DB}.a DROP INDEX iv"))
        .await;
    must(&mut n, &format!("ALTER TABLE {DB}.a ADD INDEX iv (v)")).await;
    must(&mut n, "SET SESSION wsrep_OSU_method = 'TOI'").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The reader's position: the shared domain only, as of now (nothing
    // replicated after it is purged below).
    let now: GtidPos = value(&mut n, "SELECT @@gtid_binlog_pos")
        .await
        .parse()
        .unwrap();
    let from = GtidPos::from_gtids(now.get(shared).copied());
    let local_now = *now.get(local).expect("the local domain was written");
    must(&mut n, "FLUSH BINARY LOGS").await;
    let logs: Vec<(String, u64)> = n.query("SHOW BINARY LOGS").await.unwrap();
    let newest = logs.last().unwrap().0.clone();
    must(&mut n, &format!("PURGE BINARY LOGS TO '{newest}'")).await;
    let start: GtidPos = value(&mut n, &format!("SELECT BINLOG_GTID_POS('{newest}', 4)"))
        .await
        .parse()
        .unwrap();
    eprintln!("from {from}; oldest binary log {newest} starts at {start}");
    assert_eq!(start.get(local), Some(&local_now));
    must(&mut n, &format!("REPLACE INTO {DB}.a VALUES (2, 2)")).await;

    let src = BinlogSource::from_url(&url).unwrap();
    // 1. The plain dump is refused by the node itself.
    let refused = sprig_binlog::BinlogStream::connect(
        &src,
        &from,
        &sprig_binlog::StreamOptions::new(4_199_999_901),
    )
    .await;
    assert!(
        refused
            .as_ref()
            .is_err_and(|e| e.to_string().contains("1236")),
        "the node should refuse {from}: {:?}",
        refused.err()
    );
    // 2. The feed's dump adds the local domain where the binary log holding
    // the position starts, and reads on.
    let mut t = BinlogTxns::connect(&src, TenantId(1), &from, 4_199_999_902)
        .await
        .unwrap();
    let pos = idle_pos(&mut t).await;
    eprintln!("the feed's dump from {from} reads on at {pos}");
    assert_eq!(pos.get(local), Some(&local_now));
    assert!(
        pos.get(shared)
            .is_some_and(|g| g.seq > from.get(shared).unwrap().seq)
    );
    drop(t);
    // 3. A shared position before the oldest binary log is a loss.
    let gone = start.get(shared).unwrap();
    let lost_from = GtidPos::from_gtids([Gtid {
        seq: gone.seq - 1,
        ..*gone
    }]);
    let e = BinlogTxns::connect(&src, TenantId(1), &lost_from, 4_199_999_903)
        .await
        .unwrap_err();
    eprintln!("from {lost_from}: {e}");
    assert_eq!(e.code(), Some(SprigCode::FeedHistoryLost));
    assert!(
        e.to_string()
            .contains(&format!("domain {shared} is needed")),
        "{e}"
    );
    // 4. A position naming another node's local domain, which this node
    // never had.
    let mut other = from.clone();
    other.update(Gtid {
        domain: 64_999,
        server: 64_999,
        seq: 7,
    });
    other.update(local_now);
    match BinlogTxns::connect(&src, TenantId(1), &other, 4_199_999_904).await {
        Ok(mut t) => eprintln!("from {other}: reads on at {}", idle_pos(&mut t).await),
        Err(e) => panic!("from {other}: {e}"),
    }

    must(&mut n, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut n, "SET SESSION wsrep_on = OFF").await;
    must(&mut n, &format!("DROP DATABASE IF EXISTS {LOCAL_DB}")).await;
}
