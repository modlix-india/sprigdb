//! One consumer whose saved feed position was purged fails alone: the
//! other consumers of the same worker and cluster keep running, each
//! change once, through the purge, the lost one's restart and a restart
//! of the worker.
//!
//! Two after-commit triggers, `copy_a` on `a_src` and `copy_b` on
//! `b_src`, in one database (one cluster, one worker, one shared feed
//! reader). `copy_a` is disabled, so its position stays where it was;
//! rows go into both tables; the cluster's binary logs are purged past
//! `copy_a`'s position.
//!
//! - Enabled again at once, `copy_a` resumes from the changes the worker
//!   still holds in memory: nothing is lost.
//! - With a worker restart between the purge and the `ENABLE`, its
//!   history is gone, so it can't resume, and it must not take `copy_b`
//!   with it: `copy_b`'s rows must keep arriving within seconds, and every
//!   one exactly once (a primary key refuses a second copy).
//! - Another worker restart, with `copy_a` subscribed from its lost
//!   position: `copy_b` again doesn't wait for it.
//!
//! Needs a gateway with a worker, a tenant user who may create databases
//! and routines, and every node of the database's cluster as root (to
//! purge their binary logs and read the catalog):
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! export SPRIG_TEST_LOGIC_ROOT=mysql://root:...@127.0.0.1:43301   # every node, comma-separated
//! export SPRIG_TEST_LOGIC_TENANT=<id>
//! # optional: the worker's metrics (the lost consumer is counted there)
//! export SPRIG_TEST_FEED_METRICS=http://127.0.0.1:43185/metrics
//! # a command that restarts the worker
//! export SPRIG_TEST_FEED_RESTART='.../deploy.sh stop worker; .../deploy.sh start worker'
//! cargo test -p sprig-gateway --test feed_lost_consumer_live -- --nocapture
//! ```
//!
//! Works in the database `feed_lost_live`, dropped afterwards. It purges
//! the binary logs of the nodes it is given: use a private deployment.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "feed_lost_live";

async fn connect(test: &str) -> Option<(Conn, Vec<Conn>)> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let Some(root) = skip::var("SPRIG_TEST_LOGIC_ROOT") else {
        skip::report_missing("SPRIG_TEST_LOGIC_ROOT", test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(std::env::var("SPRIG_TEST_LOGIC_USER").ok())
        .pass(std::env::var("SPRIG_TEST_LOGIC_PASSWORD").ok())
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    let mut nodes = Vec::new();
    for u in root.split(',').filter(|s| !s.is_empty()) {
        nodes.push(Conn::from_url(u.trim()).await.unwrap());
    }
    Some((Conn::new(opts).await.unwrap(), nodes))
}

async fn must(c: &mut Conn, q: &str) {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
}

struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            println!("ok   {name}: {detail}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

async fn copies(c: &mut Conn, t: &str) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM {DB}.{t}_copies"))
        .await
        .unwrap()
        .unwrap_or(0)
}

/// Waits up to `limit` for `want` copies of `t`; the count and the wait.
async fn wait_copies(c: &mut Conn, t: &str, want: u64, limit: Duration) -> (u64, f64) {
    let t0 = Instant::now();
    loop {
        let n = copies(c, t).await;
        if n >= want || t0.elapsed() > limit {
            return (n, t0.elapsed().as_secs_f64());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn insert(c: &mut Conn, t: &str, from: u32, to: u32) {
    let values: Vec<String> = (from..=to).map(|k| format!("({k}, 0)")).collect();
    must(
        c,
        &format!(
            "INSERT INTO {DB}.{t}_src (id, n) VALUES {}",
            values.join(", ")
        ),
    )
    .await;
}

/// `sprig_feed_history_lost_total` summed over its labels, when
/// `SPRIG_TEST_FEED_METRICS` is set.
fn lost_total() -> Option<f64> {
    let url = skip::var("SPRIG_TEST_FEED_METRICS")?;
    let out = std::process::Command::new("curl")
        .args(["-fsS", "--max-time", "5", &url])
        .output()
        .ok()?;
    Some(
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter(|l| l.starts_with("sprig_feed_history_lost_total"))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum::<f64>()
            // An empty sum is -0.
            + 0.0,
    )
}

/// Rotates every node's binary log and purges every older file; the
/// files each node has left.
async fn purge(nodes: &mut [Conn]) -> Vec<usize> {
    for n in nodes.iter_mut() {
        must(n, "FLUSH BINARY LOGS").await;
    }
    // The readers move into the new file before the old ones go (a file
    // a dump thread still reads isn't purged).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut left = Vec::new();
    for n in nodes.iter_mut() {
        let files: Vec<mysql_async::Row> = n.query("SHOW BINARY LOGS").await.unwrap();
        let last: String = files.last().unwrap().get(0).unwrap();
        must(n, &format!("PURGE BINARY LOGS TO '{last}'")).await;
        let files: Vec<mysql_async::Row> = n.query("SHOW BINARY LOGS").await.unwrap();
        left.push(files.len());
    }
    left
}

/// Inserts `b` rows `from..` in batches of 10, `batches` times, and waits
/// for each batch's copies; the slowest batch's wait, or `None` when one
/// didn't arrive within `limit`.
async fn b_flows(c: &mut Conn, from: u32, batches: u32, limit: Duration) -> Option<f64> {
    let mut worst: f64 = 0.0;
    for i in 0..batches {
        let lo = from + i * 10;
        let want = copies(c, "b").await + 10;
        insert(c, "b", lo, lo + 9).await;
        let (n, secs) = wait_copies(c, "b", want, limit).await;
        if n < want {
            return None;
        }
        worst = worst.max(secs);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Some(worst)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_with_purged_history_fails_alone() {
    let Some((mut c, mut nodes)) = connect("a_consumer_with_purged_history_fails_alone").await
    else {
        return;
    };
    let Some(restart_cmd) = skip::var("SPRIG_TEST_FEED_RESTART") else {
        skip::report_missing(
            "SPRIG_TEST_FEED_RESTART",
            "a_consumer_with_purged_history_fails_alone",
        );
        return;
    };
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    for t in ["a", "b"] {
        must(
            &mut c,
            &format!("CREATE TABLE {t}_src (id BIGINT PRIMARY KEY, n INT NOT NULL)"),
        )
        .await;
        // The key makes a second run of a row fail loudly (1062), so
        // "each once" is checked by the table itself.
        must(
            &mut c,
            &format!("CREATE TABLE {t}_copies (id BIGINT PRIMARY KEY, at DATETIME(6) NOT NULL)"),
        )
        .await;
        must(
            &mut c,
            &format!(
                "CREATE TRIGGER copy_{t} AFTER COMMIT ON {t}_src FOR INSERT LANGUAGE javascript AS $$
export default function (row) {{
  db.exec('INSERT INTO {t}_copies (id, at) VALUES (?, NOW(6))', [row.id]);
}}
$$"
            ),
        )
        .await;
    }
    let mut ck = Checks { failed: Vec::new() };
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 1. Both run.
    insert(&mut c, "a", 1, 10).await;
    insert(&mut c, "b", 1, 10).await;
    let (na, _) = wait_copies(&mut c, "a", 10, Duration::from_secs(60)).await;
    let (nb, secs) = wait_copies(&mut c, "b", 10, Duration::from_secs(60)).await;
    ck.check(
        "both triggers run",
        na == 10 && nb == 10,
        format!("a {na}, b {nb} copies after {secs:.1} s"),
    );

    // 2. copy_a stops at its position; both tables get rows; the binary
    // logs holding copy_a's position are purged while copy_b reads on.
    // The worker still holds those changes in memory (its shared reader's
    // recent groups), so copy_a's ENABLE resumes from there: nothing is
    // lost yet.
    must(&mut c, "ALTER TRIGGER copy_a DISABLE").await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    insert(&mut c, "a", 11, 20).await;
    insert(&mut c, "b", 11, 20).await;
    let (nb, _) = wait_copies(&mut c, "b", 20, Duration::from_secs(30)).await;
    // copy_b acknowledges its position at most every second.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let left = purge(&mut nodes).await;
    must(&mut c, "ALTER TRIGGER copy_a ENABLE").await;
    let (na, secs) = wait_copies(&mut c, "a", 20, Duration::from_secs(30)).await;
    let worst = b_flows(&mut c, 21, 2, Duration::from_secs(15)).await;
    ck.check(
        "a purge the worker still holds in memory loses nothing",
        nb == 20 && na == 20 && worst.is_some_and(|w| w < 10.0) && left.iter().all(|n| *n == 1),
        format!(
            "files left per node {left:?}; copy_a's rows 11 to 20 {secs:.1} s after ENABLE; copy_b's slowest batch {worst:?} s"
        ),
    );

    // 3. The same, with a worker restart between the purge and the ENABLE:
    // copy_a's history is really gone; copy_b keeps flowing.
    let restart = || {
        let st = std::process::Command::new("sh")
            .args(["-c", &restart_cmd])
            .status()
            .unwrap();
        assert!(st.success(), "{restart_cmd}: {st}");
    };
    must(&mut c, "ALTER TRIGGER copy_a DISABLE").await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    insert(&mut c, "a", 21, 30).await;
    insert(&mut c, "b", 41, 50).await;
    let (nb, _) = wait_copies(&mut c, "b", 50, Duration::from_secs(30)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let left = purge(&mut nodes).await;
    let t0 = Instant::now();
    restart();
    insert(&mut c, "b", 51, 60).await;
    let (nb2, _) = wait_copies(&mut c, "b", 60, Duration::from_secs(90)).await;
    ck.check(
        "copy_b reads on through the purge and a worker restart",
        nb == 50 && nb2 == 60 && left.iter().all(|n| *n == 1),
        format!(
            "files left per node {left:?}; rows 51 to 60 {:.1} s after the restart began",
            t0.elapsed().as_secs_f64()
        ),
    );
    let lost0 = lost_total();
    must(&mut c, "ALTER TRIGGER copy_a ENABLE").await;
    insert(&mut c, "a", 31, 40).await;
    let worst = b_flows(&mut c, 61, 4, Duration::from_secs(15)).await;
    ck.check(
        "after copy_a's ENABLE into lost history, copy_b's rows 61 to 100 arrive batch by batch",
        worst.is_some_and(|w| w < 10.0),
        format!("slowest batch {worst:?} s"),
    );
    let na = copies(&mut c, "a").await;
    ck.check(
        "copy_a doesn't resume past its lost history",
        na == 20,
        format!("a {na} copies (rows 21 to 40 have none)"),
    );
    if let (Some(l0), Some(l1)) = (lost0, lost_total()) {
        ck.check(
            "the worker counts copy_a's lost history",
            l1 > l0,
            format!("sprig_feed_history_lost_total {l0} -> {l1}"),
        );
    }

    // 4. A worker restart with copy_a subscribed: every consumer comes back
    // from its own position, copy_a's in purged history; copy_b must not
    // wait for it.
    let t0 = Instant::now();
    restart();
    insert(&mut c, "b", 101, 110).await;
    let (nb, _) = wait_copies(&mut c, "b", 110, Duration::from_secs(90)).await;
    let secs = t0.elapsed().as_secs_f64();
    let worst = b_flows(&mut c, 111, 3, Duration::from_secs(15)).await;
    ck.check(
        "after a worker restart copy_b's rows arrive, then flow as before",
        nb == 110 && worst.is_some_and(|w| w < 10.0),
        format!(
            "rows 101 to 110 {secs:.1} s after the restart began; then slowest batch {worst:?} s"
        ),
    );
    let na = copies(&mut c, "a").await;
    ck.check(
        "copy_a still doesn't resume after the restart",
        na == 20,
        format!("a {na} copies"),
    );

    // 5. Dropping the lost trigger leaves copy_b as it was.
    must(&mut c, "DROP TRIGGER copy_a").await;
    let worst = b_flows(&mut c, 201, 2, Duration::from_secs(15)).await;
    ck.check(
        "after copy_a is dropped copy_b flows as before",
        worst.is_some_and(|w| w < 10.0),
        format!("slowest batch {worst:?} s"),
    );
    let missing: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM {DB}.b_src s LEFT JOIN {DB}.b_copies k ON k.id = s.id WHERE k.id IS NULL"
        ))
        .await
        .unwrap();
    ck.check(
        "every b row has its copy",
        missing == Some(0),
        format!("{missing:?} missing"),
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
