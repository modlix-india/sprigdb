//! Change-feed readers on a Galera cluster through node-local writes and a
//! binary log purge.
//!
//! A Galera node with `wsrep_gtid_mode` writes the cluster's replicated
//! transactions in its shared GTID domain, and its own local writes (an
//! index built with `wsrep_OSU_method = RSU`, statements with
//! `wsrep_on = OFF`) in a domain of its own. Once the binary logs where
//! that local domain starts are purged, the node refuses (1236) every dump
//! from a position without the domain. Before, every feed reader whose
//! saved position predated the local writes got that refusal at every
//! reconnect, for ever: triggers, sinks and streams on the cluster stopped,
//! retrying every few seconds. Now the reader adds the purged local domain
//! to its position and reads on.
//!
//! The test sets up, in database `fld_suite`, table `fld_docs`. Then, in
//! three rounds, every node gets local writes (a table made and written
//! with `wsrep_on = OFF`, and an index on the tenant's `fld_docs` built
//! with RSU):
//!
//! 1. before any consumer exists, followed by a rotation and a purge of the
//!    older binary logs, so each node's oldest binary log names its local
//!    domain. Then the table gets a change topic, a trigger on that topic
//!    (JavaScript, writing each record into `fld_trig`), an after-commit
//!    trigger (KIRun, writing each row's body into `fld_ac`) and a sink
//!    `FROM CHANGES OF` the table to a webhook the test serves. They start
//!    from the cluster's position read through the deployment's writer
//!    pool, which need not be the node the worker's binlog dump reads, so
//!    it may lack that node's local domain: before, the node refused every
//!    such dump, and nothing ever ran. Rows 1 to 10 go in.
//! 2. between `SPRIG_TEST_FLD_STOP` and `SPRIG_TEST_FLD_START` (commands
//!    that stop and start the worker, so the readers come back from their
//!    saved positions). Rows 11 to 20 go in.
//! 3. with everything running, then a rotation, a wait and a purge; rows
//!    21 to 30 go in, the worker restarts, and rows 31 to 40 go in.
//!
//! Then (4) the worker stops, every node's binary logs are rotated and
//! purged at once, rows 41 to 50 go in, the worker starts, and rows 51 to
//! 60 go in. A stream sequencer's checkpoint in its log lies one of its
//! own writes behind (the append that writes it comes after it), so a
//! purge right after a stop used to remove an event its restart needed:
//! the change topic refused to read on, and the stream trigger got
//! nothing more. The sequencer now saves its exact position at the stop.
//! The after-commit trigger acknowledged its position at most every second
//! and not at a stop, so a write in the worker's last second (any
//! consumer's) lay after it, the purge removed it, and the trigger alone
//! failed with 39800. Every consumer that keeps its position in etcd now
//! drains at a graceful stop (reads on past the cluster's position, then
//! acknowledges it): the stream trigger, the after-commit trigger and the
//! sink must hold rows 1 to 60 exactly once, and with
//! `SPRIG_TEST_FLD_METRICS` the restarted worker must count no history
//! lost.
//!
//! After each round, the trigger tables and the sink must hold every row so
//! far exactly once (the sink by idempotency key: it is at least once, and
//! its deliveries are reported). With `SPRIG_TEST_FLD_METRICS` (the
//! worker's metrics address) the test checks `sprig_feed_history_lost_total`
//! stayed at zero and reports `sprig_feed_local_domains_added_total`; with
//! `SPRIG_TEST_FLD_EXPECT_REPAIR=1` it also checks that one was counted.
//!
//! It writes node-local transactions and purges binary logs: run it only
//! against a private Galera cluster made for it, never a shared one.
//!
//! ```sh
//! export SPRIG_TEST_FLD_SQL=127.0.0.1:46306        # the gateway's MySQL port
//! export SPRIG_TEST_FLD_TENANT=fft                # a tenant on the Galera cluster
//! export SPRIG_TEST_FLD_NODES=mysql://root:...@127.0.0.1:46311,...  # its nodes
//! export SPRIG_TEST_FLD_HOOK_PORT=46881            # on sink.destination_allowlist and sink.internal_destinations
//! export SPRIG_TEST_FLD_STOP='deploy.sh stop worker'
//! export SPRIG_TEST_FLD_START='deploy.sh start worker'
//! export SPRIG_TEST_FLD_METRICS=127.0.0.1:46185    # optional
//! export SPRIG_TEST_FLD_EXPECT_REPAIR=1             # optional
//! cargo test -p sprig-gateway --test feed_local_domain_live -- --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DB: &str = "fld_suite";
/// The node-local scratch database each node gets with `wsrep_on = OFF`.
const LOCAL_DB: &str = "fld_local";

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
    std::env::var("SPRIG_TEST_FLD_TENANT").unwrap_or_else(|_| "fft".into())
}

fn app_pw() -> String {
    std::env::var("SPRIG_TEST_FLD_APP_PW").unwrap_or_else(|_| "sprig-dev-app".into())
}

async fn sql(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", tenant())))
        .pass(Some(app_pw()))
        .db_name(Some(DB))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    let mut last = None;
    // The gateway may be starting again after the stop command.
    for _ in 0..60 {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => return c,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("connect to {addr}: {last:?}");
}

async fn admin(addr: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", tenant())))
        .pass(Some(app_pw()))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn node(url: &str) -> mysql_async::Conn {
    let opts = mysql_async::Opts::from_url(url).unwrap();
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

fn sh(what: &str, cmd: &str) {
    eprintln!("     {what}: {cmd}");
    let st = std::process::Command::new("bash")
        .arg("-c")
        .arg(cmd)
        .status()
        .unwrap();
    assert!(st.success(), "{what} failed: {st}");
}

type Received = Arc<Mutex<Vec<String>>>;

/// A webhook endpoint on 127.0.0.1:`port` keeping each request's body.
async fn webhook(port: u16) -> Received {
    let got: Received = Arc::default();
    let l = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    let g = got.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let g = g.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = vec![0u8; 65536];
                loop {
                    let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                    let len: usize = head
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .and_then(|v| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < end + 4 + len {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body = String::from_utf8_lossy(&buf[end + 4..end + 4 + len]).into_owned();
                    buf.drain(..end + 4 + len);
                    g.lock().unwrap().push(body);
                    if s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}")
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    got
}

/// How often each row's marker `fld-<id>-x` appears in `texts`.
fn counts<'a>(texts: impl IntoIterator<Item = &'a str>) -> BTreeMap<u32, usize> {
    let mut out = BTreeMap::new();
    for t in texts {
        let mut rest = t;
        while let Some(i) = rest.find("fld-") {
            rest = &rest[i + 4..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if rest[digits.len()..].starts_with("-x")
                && let Ok(id) = digits.parse()
            {
                *out.entry(id).or_insert(0) += 1;
            }
        }
    }
    out
}

/// Rows 1..=`n`, each once.
fn exactly(n: u32) -> BTreeMap<u32, usize> {
    (1..=n).map(|i| (i, 1)).collect()
}

async fn table_texts(c: &mut mysql_async::Conn, table: &str) -> Vec<String> {
    c.query::<Option<String>, _>(format!("SELECT v FROM {DB}.{table}"))
        .await
        .unwrap_or_default()
        .into_iter()
        .flatten()
        .collect()
}

struct Seen {
    trig: BTreeMap<u32, usize>,
    ac: BTreeMap<u32, usize>,
    /// Distinct events (idempotency keys) per row.
    sink: BTreeMap<u32, usize>,
    /// Deliveries per row: a sink is at least once, so a restart may send
    /// an event again under the same idempotency key.
    sink_raw: BTreeMap<u32, usize>,
}

/// The sink's records per row: distinct idempotency keys (or lines, for
/// a record without one), and deliveries. A line is one record; it names
/// its row's marker in its value and its image.
fn sink_counts(bodies: &[String]) -> (BTreeMap<u32, usize>, BTreeMap<u32, usize>) {
    let mut keys: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    let mut raw = BTreeMap::new();
    for line in bodies.iter().flat_map(|b| b.lines()) {
        let line = line.trim();
        let key = serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("idempotencyKey").map(ToString::to_string))
            .unwrap_or_else(|| line.to_owned());
        for id in counts([line]).into_keys() {
            *raw.entry(id).or_insert(0) += 1;
            keys.entry(id).or_default().insert(key.clone());
        }
    }
    (keys.into_iter().map(|(k, v)| (k, v.len())).collect(), raw)
}

async fn seen(c: &mut mysql_async::Conn, hook: &Received) -> Seen {
    let trig = counts(table_texts(c, "fld_trig").await.iter().map(String::as_str));
    let ac = counts(table_texts(c, "fld_ac").await.iter().map(String::as_str));
    let (sink, sink_raw) = sink_counts(&hook.lock().unwrap());
    Seen {
        trig,
        ac,
        sink,
        sink_raw,
    }
}

/// Waits up to `secs` for every consumer to hold rows 1..=`n`, then a few
/// seconds more for late duplicates.
async fn settle(addr: &str, hook: &Received, n: u32, secs: u64) -> Seen {
    let end = tokio::time::Instant::now() + Duration::from_secs(secs);
    let want = exactly(n);
    let mut c = sql(addr).await;
    loop {
        let s = seen(&mut c, hook).await;
        let all = |m: &BTreeMap<u32, usize>| (1..=n).all(|i| m.contains_key(&i));
        if (all(&s.trig) && all(&s.ac) && all(&s.sink)) || tokio::time::Instant::now() > end {
            if s.trig == want && s.ac == want && s.sink == want {
                tokio::time::sleep(Duration::from_secs(5)).await;
                return seen(&mut c, hook).await;
            }
            return s;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn insert(addr: &str, ids: std::ops::RangeInclusive<u32>) {
    let mut c = sql(addr).await;
    for i in ids {
        must(
            &mut c,
            &format!("INSERT INTO {DB}.fld_docs (id, body) VALUES ({i}, 'fld-{i}-x')"),
        )
        .await;
    }
}

/// A metric's value summed over its label sets, from `/metrics` at `addr`.
async fn metric(addr: &str, name: &str) -> Option<f64> {
    let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
    let req = format!("GET /metrics HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out)).await;
    let text = String::from_utf8_lossy(&out);
    Some(
        text.lines()
            .filter(|l| l.starts_with(name) && !l.starts_with('#'))
            .filter(|l| l[name.len()..].starts_with(['{', ' ']))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum(),
    )
}

/// Local writes on every node: a table made and written with
/// `wsrep_on = OFF`, and index `index` on the tenant's `fld_docs` built
/// with RSU. With `rotate`, then `FLUSH BINARY LOGS`; with `purge`, the
/// older binary logs purged, and checked: the oldest one names the node's
/// local domain.
async fn local_writes(nodes: &[String], ck: &mut Checks, index: &str, rotate: bool, purge: bool) {
    for (i, url) in nodes.iter().enumerate() {
        let mut n = node(url).await;
        let local: u32 = n
            .query_first("SELECT @@gtid_domain_id")
            .await
            .unwrap()
            .unwrap();
        must(&mut n, "SET SESSION wsrep_on = OFF").await;
        must(&mut n, &format!("CREATE DATABASE IF NOT EXISTS {LOCAL_DB}")).await;
        must(
            &mut n,
            &format!("CREATE TABLE IF NOT EXISTS {LOCAL_DB}.n (id INT PRIMARY KEY)"),
        )
        .await;
        must(&mut n, &format!("REPLACE INTO {LOCAL_DB}.n VALUES ({i})")).await;
        must(&mut n, "SET SESSION wsrep_on = ON").await;
        let schemas: Vec<String> = n
            .query(
                "SELECT table_schema FROM information_schema.tables WHERE table_name = 'fld_docs'",
            )
            .await
            .unwrap();
        must(&mut n, "SET SESSION wsrep_OSU_method = 'RSU'").await;
        for s in &schemas {
            must(
                &mut n,
                &format!("ALTER TABLE `{s}`.fld_docs ADD INDEX {index} (body)"),
            )
            .await;
        }
        must(&mut n, "SET SESSION wsrep_OSU_method = 'TOI'").await;
        // RSU desyncs the node while it builds: wait for Synced.
        for _ in 0..60 {
            let st: Option<(String, String)> = n
                .query_first("SHOW STATUS LIKE 'wsrep_local_state_comment'")
                .await
                .unwrap();
            if st.is_some_and(|(_, v)| v == "Synced") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let pos: String = n
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .unwrap()
            .unwrap_or_default();
        eprintln!("     {url}: local domain {local}, RSU index {index} on {schemas:?}; now {pos}");
        if rotate {
            must(&mut n, "FLUSH BINARY LOGS").await;
        }
        if purge {
            purge_older(&mut n, ck, i, local).await;
        }
    }
}

/// Purges every binary log but the newest, and checks the newest's
/// starting state names domain `local`.
async fn purge_older(n: &mut mysql_async::Conn, ck: &mut Checks, i: usize, local: u32) {
    let logs: Vec<(String, u64)> = n.query("SHOW BINARY LOGS").await.unwrap();
    let newest = logs.last().unwrap().0.clone();
    must(n, &format!("PURGE BINARY LOGS TO '{newest}'")).await;
    let start: String = n
        .query_first(format!("SELECT BINLOG_GTID_POS('{newest}', 4)"))
        .await
        .unwrap()
        .unwrap_or_default();
    eprintln!("     node {i}: purged to {newest}, which starts at {start}");
    ck.ok(
        &format!("node {i}'s oldest binary log names its local domain {local}"),
        start
            .split(',')
            .any(|g| g.trim().starts_with(&format!("{local}-"))),
        || start.clone(),
    );
}

/// Checks every consumer holds rows 1..=`n` exactly once.
async fn check_rows(addr: &str, hook: &Received, ck: &mut Checks, n: u32, when: &str) {
    let s = settle(addr, hook, n, 180).await;
    ck.ok(
        &format!("stream trigger {when}: rows 1-{n} once each"),
        s.trig == exactly(n),
        || format!("{:?}", s.trig),
    );
    ck.ok(
        &format!("after-commit trigger {when}: rows 1-{n} once each"),
        s.ac == exactly(n),
        || format!("{:?}", s.ac),
    );
    ck.ok(
        &format!("sink {when}: rows 1-{n} once each"),
        s.sink == exactly(n),
        || format!("{:?}", s.sink),
    );
    let extra: usize = s.sink_raw.values().map(|v| v.saturating_sub(1)).sum();
    eprintln!("     sink: {extra} deliveries again under the same idempotency key");
}

/// Adds the worker's `sprig_feed_local_domains_added_total` and
/// `sprig_feed_history_lost_total` (they count since its start: read before
/// each restart) to `totals`.
async fn add_metrics(totals: &mut (f64, f64)) {
    let Ok(m) = std::env::var("SPRIG_TEST_FLD_METRICS") else {
        return;
    };
    totals.0 += metric(&m, "sprig_feed_local_domains_added_total")
        .await
        .unwrap_or(0.0);
    totals.1 += metric(&m, "sprig_feed_history_lost_total")
        .await
        .unwrap_or(0.0);
}

const AFTER_COMMIT: &str = r#"CREATE TRIGGER {DB}.fld_ac_t AFTER COMMIT ON {DB}.fld_docs FOR INSERT LANGUAGE kirun AS '{"namespace":"App","name":"FldAc","parameters":{"body":{"parameterName":"body","schema":{"type":["STRING"]}}},"steps":{"ins":{"statementName":"ins","namespace":"Sprig.Db","name":"Exec","parameterMap":{"sql":{"s":{"key":"s","type":"VALUE","value":"INSERT INTO fld_ac (v) VALUES (?)"}},"args":{"a":{"key":"a","type":"EXPRESSION","order":1,"expression":"Arguments.body"}}}}}}'"#;

const STREAM_TRIGGER: &str = r"CREATE TRIGGER {DB}.fld_st_t ON STREAM {DB}.`fld_docs.changes` LANGUAGE javascript AS $$
export default function (ctx) {
  db.exec('INSERT INTO {DB}.fld_trig (v) VALUES (?)', [ctx.record.value]);
}
$$";

async fn drop_all(c: &mut mysql_async::Conn) {
    for s in [
        format!("DROP SINK IF EXISTS {DB}.fld_sink"),
        format!("DROP TRIGGER {DB}.fld_st_t"),
        format!("DROP TRIGGER {DB}.fld_ac_t"),
        format!("DROP CONNECTION {DB}.fld_hook"),
        format!("DROP DATABASE IF EXISTS {DB}"),
    ] {
        if let Err(e) = c.query_drop(&s).await {
            eprintln!("     cleanup {s}: {e}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn feed_readers_survive_purged_local_domains() {
    let (Ok(addr), Ok(nodes), Ok(port)) = (
        std::env::var("SPRIG_TEST_FLD_SQL"),
        std::env::var("SPRIG_TEST_FLD_NODES"),
        std::env::var("SPRIG_TEST_FLD_HOOK_PORT"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_FLD_SQL, SPRIG_TEST_FLD_NODES and SPRIG_TEST_FLD_HOOK_PORT"
        );
        return;
    };
    let nodes: Vec<String> = nodes.split(',').map(str::to_owned).collect();
    let hook = webhook(port.parse().unwrap()).await;
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };

    // Set up.
    let mut a = admin(&addr).await;
    drop_all(&mut a).await;
    must(&mut a, &format!("CREATE DATABASE {DB}")).await;
    must(&mut a, "GRANT SPRIG_SINK_ADMIN ON *.* TO 'app'@'%'").await;
    // A global privilege applies to the next connection.
    let mut a = admin(&addr).await;
    must(
        &mut a,
        &format!("CREATE TABLE {DB}.fld_docs (id INT PRIMARY KEY, body VARCHAR(64))"),
    )
    .await;
    for t in ["fld_trig", "fld_ac"] {
        must(
            &mut a,
            &format!("CREATE TABLE {DB}.{t} (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"),
        )
        .await;
    }
    drop(a);

    // 1. Local writes and a purge before any consumer exists.
    local_writes(&nodes, &mut ck, "fld_rsu1", true, true).await;
    let mut a = admin(&addr).await;
    must(
        &mut a,
        &format!("ALTER TABLE {DB}.fld_docs SPRIG SET (stream.change_topic = on)"),
    )
    .await;
    must(&mut a, &AFTER_COMMIT.replace("{DB}", DB)).await;
    must(&mut a, &STREAM_TRIGGER.replace("{DB}", DB)).await;
    must(
        &mut a,
        &format!(
            "CREATE CONNECTION {DB}.fld_hook TYPE WEBHOOK WITH (url = 'http://127.0.0.1:{port}/', \
             format = 'ndjson', signature = 'none')"
        ),
    )
    .await;
    must(
        &mut a,
        &format!(
            "CREATE SINK {DB}.fld_sink FROM CHANGES OF {DB}.fld_docs TO WEBHOOK CONNECTION fld_hook \
             WITH (batch_max_wait_ms = 100)"
        ),
    )
    .await;
    drop(a);
    insert(&addr, 1..=10).await;
    check_rows(&addr, &hook, &mut ck, 10, "after a purge").await;

    let mut totals = (0.0, 0.0);
    add_metrics(&mut totals).await;

    // 2. Local writes while the worker is stopped.
    if let Ok(cmd) = std::env::var("SPRIG_TEST_FLD_STOP") {
        sh("stop", &cmd);
    }
    local_writes(&nodes, &mut ck, "fld_rsu2", true, false).await;
    if let Ok(cmd) = std::env::var("SPRIG_TEST_FLD_START") {
        sh("start", &cmd);
    }
    insert(&addr, 11..=20).await;
    check_rows(&addr, &hook, &mut ck, 20, "after a restart").await;

    // 3. Local writes and a purge while everything runs, then a restart.
    local_writes(&nodes, &mut ck, "fld_rsu3", true, false).await;
    // Every reader reads past the rotation, and the stream sequencers save
    // positions in the newest binary log.
    tokio::time::sleep(Duration::from_secs(25)).await;
    for (i, url) in nodes.iter().enumerate() {
        let mut n = node(url).await;
        let local: u32 = n
            .query_first("SELECT @@gtid_domain_id")
            .await
            .unwrap()
            .unwrap();
        purge_older(&mut n, &mut ck, i, local).await;
    }
    insert(&addr, 21..=30).await;
    check_rows(&addr, &hook, &mut ck, 30, "after a purge while running").await;
    add_metrics(&mut totals).await;
    if let Ok(cmd) = std::env::var("SPRIG_TEST_FLD_STOP") {
        sh("stop", &cmd);
    }
    if let Ok(cmd) = std::env::var("SPRIG_TEST_FLD_START") {
        sh("start", &cmd);
    }
    insert(&addr, 31..=40).await;
    check_rows(&addr, &hook, &mut ck, 40, "after a purge and a restart").await;

    // 4. A purge right after the worker stops.
    if let (Ok(stop), Ok(start)) = (
        std::env::var("SPRIG_TEST_FLD_STOP"),
        std::env::var("SPRIG_TEST_FLD_START"),
    ) {
        add_metrics(&mut totals).await;
        // The metric checks below count the rounds before this one.
        let before = totals;
        sh("stop", &stop);
        for (i, url) in nodes.iter().enumerate() {
            let mut n = node(url).await;
            let local: u32 = n
                .query_first("SELECT @@gtid_domain_id")
                .await
                .unwrap()
                .unwrap();
            must(&mut n, "FLUSH BINARY LOGS").await;
            purge_older(&mut n, &mut ck, i, local).await;
        }
        insert(&addr, 41..=50).await;
        sh("start", &start);
        insert(&addr, 51..=60).await;
        let when = "after a purge right after a stop";
        let s = settle(&addr, &hook, 60, 180).await;
        ck.ok(
            &format!("stream trigger {when}: rows 1-60 once each"),
            s.trig == exactly(60),
            || format!("{:?}", s.trig),
        );
        ck.ok(
            &format!("sink {when}: rows 1-60 once each"),
            s.sink == exactly(60),
            || format!("{:?}", s.sink),
        );
        ck.ok(
            &format!("after-commit trigger {when}: rows 1-60 once each"),
            s.ac == exactly(60),
            || format!("{:?}", s.ac),
        );
        let mut after = totals;
        add_metrics(&mut after).await;
        let lost = after.1 - before.1;
        eprintln!("     worker in this round: sprig_feed_history_lost_total {lost}");
        if std::env::var("SPRIG_TEST_FLD_METRICS").is_ok() {
            ck.ok(
                &format!("worker {when}: no consumer lost history"),
                lost <= 0.0,
                || format!("sprig_feed_history_lost_total {lost}"),
            );
        }
        totals = after;
    } else {
        add_metrics(&mut totals).await;
    }
    if std::env::var("SPRIG_TEST_FLD_METRICS").is_ok() {
        let (added, lost) = totals;
        eprintln!(
            "     worker, summed over its runs: sprig_feed_local_domains_added_total {added}, sprig_feed_history_lost_total {lost}"
        );
        ck.ok("worker: no history lost", lost <= 0.0, || format!("{lost}"));
        if std::env::var("SPRIG_TEST_FLD_EXPECT_REPAIR").is_ok_and(|v| v == "1") {
            ck.ok("worker: a local domain added", added >= 1.0, || {
                format!("{added}")
            });
        }
    }

    // Clean up: the suite's objects, and each node's local database.
    let mut a = admin(&addr).await;
    drop_all(&mut a).await;
    for url in &nodes {
        let mut n = node(url).await;
        must(&mut n, "SET SESSION wsrep_on = OFF").await;
        must(&mut n, &format!("DROP DATABASE IF EXISTS {LOCAL_DB}")).await;
    }
    eprintln!("{} checks, {} failed", ck.n, ck.failed.len());
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
