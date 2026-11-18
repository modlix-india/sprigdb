//! A gateway's search copies and the change feed, through the MySQL
//! listener (`FULLTEXT` indexes become search indexes):
//!
//! - a table dropped and created again under its name is searched with its
//!   new rows (the feed's name-to-object cache forgets the dropped one);
//! - a transaction larger than the feed holds, into another table, passes
//!   by: the gateway's feed buffers stay bounded, and the index stays fresh;
//! - one into the indexed table makes its copy read the table again: a
//!   search then answers with every row or an error, never without them.
//!
//! ```sh
//! export SPRIG_TEST_SEARCH=mysql://acme.app:sprig-dev-app@127.0.0.1:23307
//! export SPRIG_TEST_SEARCH_METRICS=127.0.0.1:23190   # the gateway's /metrics (optional)
//! cargo test -p sprig-search --test feed_bounds_live -- --nocapture --test-threads 1
//! ```
//!
//! Database `ftfeed_live` is created and dropped.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    missing_docs
)]

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, Pool};

fn pool() -> Option<Pool> {
    let url = std::env::var("SPRIG_TEST_SEARCH").ok()?;
    let opts = Opts::from_url(&url).expect("SPRIG_TEST_SEARCH must be a mysql:// URL");
    Some(Pool::new(
        mysql_async::OptsBuilder::from_opts(opts).pool_opts(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(0, 4).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(60)),
        ),
    ))
}

/// Series `name` (summed over its labels) of the gateway's `/metrics`.
fn metric(name: &str) -> Option<f64> {
    let addr = std::env::var("SPRIG_TEST_SEARCH_METRICS").ok()?;
    let mut s = std::net::TcpStream::connect(&addr).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    s.write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .ok()?;
    let mut body = String::new();
    let _ = s.read_to_string(&mut body);
    let mut sum = None;
    for line in body.lines() {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        if !(rest.starts_with(' ') || rest.starts_with('{')) {
            continue;
        }
        let v: f64 = rest.rsplit(' ').next()?.trim().parse().ok()?;
        *sum.get_or_insert(0.0) += v;
    }
    sum
}

const MATCH: &str = "SELECT id FROM docs WHERE MATCH(title, body) AGAINST (? IN NATURAL LANGUAGE MODE) \
     ORDER BY MATCH(title, body) AGAINST (? IN NATURAL LANGUAGE MODE) DESC LIMIT 10";

/// The ids `MATCH` finds for `word`, or the error's code.
async fn find(c: &mut Conn, word: &str) -> Result<Vec<String>, u16> {
    c.exec(MATCH, (word, word)).await.map_err(|e| match e {
        mysql_async::Error::Server(s) => s.code,
        _ => 0,
    })
}

/// Waits up to `secs` for `word` to find exactly `want`; every answer
/// before that must be an error or a subset (never rows of another table).
async fn until_found(c: &mut Conn, word: &str, want: &[&str], secs: u64) -> Duration {
    let t = Instant::now();
    loop {
        match find(c, word).await {
            Ok(mut got) => {
                got.sort();
                if got == want {
                    return t.elapsed();
                }
                assert!(
                    got.iter().all(|g| want.contains(&g.as_str())),
                    "'{word}' found {got:?}, expected within {want:?}"
                );
            }
            Err(code) => assert!(
                matches!(code, 39824 | 39826),
                "'{word}': error {code}, expected 39824 or 39826 while catching up"
            ),
        }
        assert!(
            t.elapsed() < Duration::from_secs(secs),
            "'{word}' didn't find {want:?} within {secs} s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const CREATE: &str = "CREATE TABLE docs (id VARCHAR(64) PRIMARY KEY, title TEXT, body MEDIUMTEXT, \
     FULLTEXT KEY ft (title, body))";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_copies_follow_recreated_tables_and_large_transactions() {
    let Some(pool) = pool() else {
        eprintln!("SPRIG_TEST_SEARCH not set; skipped");
        return;
    };
    let mut c = pool.get_conn().await.unwrap();
    for s in [
        "DROP DATABASE IF EXISTS ftfeed_live",
        "CREATE DATABASE ftfeed_live",
        "USE ftfeed_live",
        CREATE,
        "INSERT INTO docs VALUES ('a1', 'vitamin', 'vitamin deficiency'), ('a2', 'iron', 'iron deficiency')",
    ] {
        c.query_drop(s).await.unwrap();
    }
    until_found(&mut c, "vitamin", &["a1"], 30).await;
    // Dropped and created again under its name, in the same gateway
    // lifetime: the new rows are found, the old ones aren't.
    for s in [
        "DROP TABLE docs",
        CREATE,
        "INSERT INTO docs VALUES ('b1', 'zebra', 'zebra stripes'), ('b2', 'vitamin', 'vitamin c')",
    ] {
        c.query_drop(s).await.unwrap();
    }
    let lag = until_found(&mut c, "zebra", &["b1"], 30).await;
    until_found(&mut c, "vitamin", &["b2"], 5).await;
    println!("re-created table: its rows searchable after {lag:?}");
    // A transaction larger than the feed holds (about 400,000 rows of
    // some 400 bytes in one INSERT ... SELECT) into another table.
    for s in [
        "CREATE TABLE seq (n INT PRIMARY KEY)",
        "INSERT INTO seq WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < 999) SELECT n FROM s",
        "CREATE TABLE bulk (id INT PRIMARY KEY, a INT, b INT, c DECIMAL(15,2), d DECIMAL(15,2), \
         e CHAR(1), f CHAR(1), g DATE, h DATE, i VARCHAR(25), j VARCHAR(200))",
    ] {
        c.query_drop(s).await.unwrap();
    }
    let overflows = metric("sprig_feed_txn_overflow_total").unwrap_or(0.0);
    let watch = tokio::spawn(async {
        let mut peak = 0.0f64;
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(90) {
            if let Some(b) = metric("sprig_feed_buffered_bytes") {
                peak = peak.max(b);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        peak
    });
    let t = Instant::now();
    c.query_drop(
        "INSERT INTO bulk SELECT a.n * 1000 + b.n, a.n, b.n, a.n * 1.5, b.n * 2.5, 'A', 'F', \
         '1995-01-01', '1996-02-02', 'DELIVER IN PERSON', REPEAT('lineitem comment ', 10) \
         FROM seq a CROSS JOIN seq b WHERE a.n < 400",
    )
    .await
    .unwrap();
    println!("400,000 rows in one transaction in {:?}", t.elapsed());
    c.query_drop("INSERT INTO docs VALUES ('b3', 'quokka', 'quokka smiles')")
        .await
        .unwrap();
    let lag = until_found(&mut c, "quokka", &["b3"], 60).await;
    println!("after it, a new row searchable after {lag:?}");
    // Into the indexed table itself: the copy reads the table again. One
    // of its rows has a word of its own.
    c.query_drop(
        "INSERT INTO docs SELECT CONCAT('n', a.n * 1000 + b.n), \
         IF(a.n * 1000 + b.n = 199999, 'wombat', 'numbat'), REPEAT('numbat burrow ', 40) \
         FROM seq a CROSS JOIN seq b WHERE a.n < 200",
    )
    .await
    .unwrap();
    let lag = until_found(&mut c, "wombat", &["n199999"], 300).await;
    println!("200,000 rows in one transaction into the indexed table searchable after {lag:?}");
    let peak = watch.await.unwrap();
    if let Some(now) = metric("sprig_feed_txn_overflow_total") {
        assert!(
            now >= overflows + 2.0,
            "both large transactions were over the bound"
        );
        // Two read-ahead or subscription buffers of twice 64 MiB each, at
        // most, and what is in flight.
        println!("peak sprig_feed_buffered_bytes {peak}");
        assert!(peak <= 600.0 * 1024.0 * 1024.0, "{peak}");
    }
    c.query_drop("DROP DATABASE ftfeed_live").await.unwrap();
}
