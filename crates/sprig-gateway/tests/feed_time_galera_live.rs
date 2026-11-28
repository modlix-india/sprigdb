//! Change streams from a time on a Galera cluster. The controller writes
//! the feed time index there too, keeping only the cluster's shared GTID
//! domain (`wsrep_gtid_domain_id`), so a feed reading any node can start
//! at its rows; the node read adds its own local domains as they are now.
//! Before, a Galera cluster had no index, and every start from a time
//! answered 39802.
//!
//! The test writes node-local transactions (`wsrep_on = OFF`, the node's
//! own `gtid_domain_id`) on every node and rotates the binary logs; with
//! `SPRIG_TEST_FTG_PURGE=1` it also purges the older ones, so the oldest
//! one lists that domain, and a start position without the local domain
//! would be refused by the node (1236). Purging breaks the cluster's other
//! binlog readers whose saved positions lack the new domain (the shared
//! feed readers of gateways and workers get 1236 from then on), so use it
//! only on a cluster made for this test, and rebuild that deployment
//! afterwards. Then it starts streams from a time. A user a row policy binds gets her changes after the
//! time and nothing else; too old a time is 39800, and a time with a
//! resume token 39801.
//!
//! Runs against a private deployment whose tenant lives on a Galera
//! cluster with `wsrep_gtid_mode` on, running for a minute or more (the
//! start reads a row 30 s or more before its time). It writes node-local
//! transactions there: never point it at a shared cluster.
//!
//! ```sh
//! export SPRIG_TEST_FTG_SQL=127.0.0.1:48306     # the gateway's MySQL port
//! export SPRIG_TEST_FTG_HTTP=127.0.0.1:48080    # its HTTP listener
//! export SPRIG_TEST_FTG_TENANT=sf13t            # a tenant on the Galera cluster
//! export SPRIG_TEST_FTG_NODES=mysql://root:...@127.0.0.1:48311,...  # its nodes
//! cargo test -p sprig-gateway --test feed_time_galera_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::BTreeSet;
use std::time::Duration;

use base64::Engine as _;
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PW: &str = "Ftg-suite-pw-1";
const DB: &str = "ftg_suite";

struct Checks {
    n: usize,
    failed: Vec<String>,
}

impl Checks {
    fn ok(&mut self, what: &str, cond: bool, detail: impl FnOnce() -> String) {
        self.n += 1;
        if cond {
            eprintln!("ok   {what}");
        } else {
            let d = detail();
            eprintln!("FAIL {what}: {d}");
            self.failed.push(format!("{what}: {d}"));
        }
    }
}

fn tenant() -> String {
    std::env::var("SPRIG_TEST_FTG_TENANT").unwrap_or_else(|_| "sf13t".into())
}

fn app_pw() -> String {
    std::env::var("SPRIG_TEST_FTG_APP_PW").unwrap_or_else(|_| "sprig-dev-app".into())
}

async fn sql(addr: &str, user: &str, pw: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(pw))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

fn basic(user: &str, pw: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{pw}", tenant()))
    )
}

/// The change stream's NDJSON for `secs` seconds, as `user`.
async fn stream(addr: &str, user: &str, pw: &str, query: &str, secs: u64) -> String {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /v1/databases/{DB}/changes{query} HTTP/1.1\r\nhost: {addr}\r\nauthorization: {}\r\n\r\n",
        basic(user, pw)
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let mut chunk = vec![0u8; 65536];
    loop {
        match tokio::time::timeout_at(end, s.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// `op:id` of each event line.
fn ops(text: &str) -> Vec<String> {
    text.lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l.trim()).ok())
        .filter(|v| v.get("op").is_some() && v["op"] != "heartbeat")
        .map(|v| {
            let key: serde_json::Value = v["key"]
                .as_str()
                .and_then(|k| serde_json::from_str(k).ok())
                .unwrap_or_default();
            format!(
                "{}:{}",
                v["op"].as_str().unwrap_or("?"),
                key["id"].as_i64().unwrap_or(-1)
            )
        })
        .collect()
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// The domains of a `@@gtid_binlog_pos` value.
fn domains(pos: &str) -> BTreeSet<u64> {
    pos.split(',')
        .filter_map(|g| g.trim().split('-').next()?.parse().ok())
        .collect()
}

#[ignore = "live: needs SPRIG_TEST_FTG_SQL, SPRIG_TEST_FTG_HTTP, SPRIG_TEST_FTG_NODES; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn galera_streams_start_at_a_time() {
    let (Ok(sql_addr), Ok(http_addr), Ok(nodes)) = (
        std::env::var("SPRIG_TEST_FTG_SQL"),
        std::env::var("SPRIG_TEST_FTG_HTTP"),
        std::env::var("SPRIG_TEST_FTG_NODES"),
    ) else {
        eprintln!("skipped: set SPRIG_TEST_FTG_SQL, SPRIG_TEST_FTG_HTTP and SPRIG_TEST_FTG_NODES");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut roots = Vec::new();
    for url in nodes.split(',') {
        let opts = mysql_async::Opts::from_url(url.trim()).unwrap();
        roots.push(mysql_async::Conn::new(opts).await.unwrap());
    }

    // 1. The index is on every node, and holds the shared domain alone.
    let mut shared = None;
    for (i, r) in roots.iter_mut().enumerate() {
        let (domain, mode): (u64, String) = r
            .query_first("SELECT @@wsrep_gtid_domain_id, @@wsrep_gtid_mode")
            .await
            .unwrap()
            .unwrap();
        assert!(
            mode == "1" || mode.eq_ignore_ascii_case("ON"),
            "wsrep_gtid_mode is off on node {i}"
        );
        shared = Some(domain);
        let rows: Vec<String> = r
            .query("SELECT pos FROM __sprig_sys.__sprig_feed_time ORDER BY bucket DESC LIMIT 6")
            .await
            .unwrap_or_default();
        ck.ok(
            &format!("node {i}: the Galera cluster has feed time index rows"),
            !rows.is_empty(),
            String::new,
        );
        ck.ok(
            &format!("node {i}: every row holds the shared domain {domain} alone"),
            rows.iter().all(|p| domains(p) == BTreeSet::from([domain])),
            || format!("{rows:?}"),
        );
    }
    let shared = shared.expect("a node");

    // 2. Node-local transactions, then the logs rotated: the next binlog's
    // GTID list names each node's local domain.
    let mut rotated = Vec::new();
    for r in &mut roots {
        for s in [
            "SET SESSION wsrep_on = OFF",
            "CREATE DATABASE IF NOT EXISTS ftg_local",
            "CREATE TABLE IF NOT EXISTS ftg_local.t (i INT)",
            "INSERT INTO ftg_local.t VALUES (1)",
            "SET SESSION wsrep_on = ON",
            "FLUSH BINARY LOGS",
        ] {
            must(r, s).await;
        }
        let status: Vec<mysql_async::Row> = r.query("SHOW MASTER STATUS").await.unwrap();
        let name: String = status
            .first()
            .and_then(|row| row.get::<String, _>(0))
            .unwrap_or_default();
        let pos: String = r
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .unwrap()
            .unwrap_or_default();
        eprintln!("rotated to {name}, gtid_binlog_pos {pos}");
        ck.ok(
            "a node's position has a local domain besides the shared one",
            domains(&pos).len() > 1 && domains(&pos).contains(&shared),
            || pos.clone(),
        );
        rotated.push(name);
    }
    // A row written after the rotation, and 30 s old (the start's margin).
    let rotated_at = now_secs();
    tokio::time::sleep(Duration::from_secs(45)).await;
    let newest_ok: Option<u64> = roots[0]
        .query_first(format!(
            "SELECT MIN(bucket) FROM __sprig_sys.__sprig_feed_time WHERE bucket > {rotated_at}"
        ))
        .await
        .unwrap();
    ck.ok(
        "the index has a row from after the rotation",
        newest_ok.is_some_and(|b| b + 30 <= now_secs()),
        || format!("{newest_ok:?}"),
    );
    // Purging breaks every other binlog reader of the cluster whose saved
    // position lacks the new local domains (they get 1236 when they
    // reconnect): only on a cluster made for this test.
    let purge = std::env::var("SPRIG_TEST_FTG_PURGE").is_ok_and(|v| v == "1");
    if purge {
        for (r, name) in roots.iter_mut().zip(&rotated) {
            must(r, &format!("PURGE BINARY LOGS TO '{name}'")).await;
        }
    } else {
        eprintln!("     binary logs not purged (SPRIG_TEST_FTG_PURGE=1 purges them)");
    }

    // 3. Streams from a time.
    let apw = app_pw();
    let mut admin = sql(&sql_addr, "app", &apw).await;
    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'ftg_alice'@'%'".to_owned(),
        format!("CREATE DATABASE {DB}"),
        format!("CREATE TABLE {DB}.docs (id INT PRIMARY KEY, owner VARCHAR(64), body VARCHAR(64))"),
        format!("CREATE USER 'ftg_alice'@'%' IDENTIFIED BY '{PW}'"),
        format!("GRANT SELECT, WATCH ON {DB}.docs TO 'ftg_alice'@'%'"),
        format!("CREATE POLICY own ON {DB}.docs USING (owner = CURRENT_USER())"),
        format!("ALTER TABLE {DB}.docs ENABLE ROW LEVEL SECURITY"),
        format!(
            "INSERT INTO {DB}.docs VALUES (1, 'ftg_alice@%', 'before'), (2, 'bob@%', 'before')"
        ),
    ] {
        must(&mut admin, &s).await;
    }
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let at = now_secs() << 32;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    for s in [
        format!("INSERT INTO {DB}.docs VALUES (3, 'ftg_alice@%', 'after'), (4, 'bob@%', 'after')"),
        format!("UPDATE {DB}.docs SET owner = 'bob@%' WHERE id = 3"),
    ] {
        must(&mut admin, &s).await;
    }
    let q = format!("?objects=docs&start_at_hlc={at}");
    let t = stream(&http_addr, "ftg_alice", PW, &q, 6).await;
    let got = ops(&t);
    ck.ok(
        "Galera: a bound user's stream from a time gets her insert after it, then its delete as it leaves her view",
        t.starts_with("HTTP/1.1 200") && got == ["insert:3", "delete:3"],
        || format!("{got:?} {}", t.chars().take(600).collect::<String>()),
    );
    let t = stream(&http_addr, "app", &apw, &q, 6).await;
    let got = ops(&t);
    ck.ok(
        "Galera: the administrator's stream from the same time gets every change after it, none before",
        t.starts_with("HTTP/1.1 200") && got == ["insert:3", "insert:4", "update:3"],
        || format!("{got:?} {}", t.chars().take(600).collect::<String>()),
    );
    let t = stream(
        &http_addr,
        "app",
        &apw,
        &format!("?objects=docs&start_at_hlc={}", 1u64 << 32),
        3,
    )
    .await;
    ck.ok(
        "Galera: a start_at_hlc before the time index is refused (39800)",
        !t.starts_with("HTTP/1.1 200") && t.contains("39800"),
        || t.chars().take(400).collect(),
    );
    let t = stream(&http_addr, "app", &apw, &format!("{q}&start_after=00"), 3).await;
    ck.ok(
        "Galera: start_at_hlc with a resume token is refused (39801)",
        !t.starts_with("HTTP/1.1 200") && t.contains("39801"),
        || t.chars().take(400).collect(),
    );

    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        "DROP USER IF EXISTS 'ftg_alice'@'%'".to_owned(),
    ] {
        let _ = admin.query_drop(&s).await;
    }
    for r in &mut roots {
        for s in [
            "SET SESSION wsrep_on = OFF",
            "DROP DATABASE IF EXISTS ftg_local",
            "SET SESSION wsrep_on = ON",
        ] {
            let _ = r.query_drop(s).await;
        }
    }
    println!(
        "feed time index on Galera: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
