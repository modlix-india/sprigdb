//! Live: after-commit triggers, search copies and MongoDB change streams
//! all read through their process's shared reader: one
//! binlog dump per cluster per worker and per gateway, however many
//! consumers. Against a private deployment (a gateway, one worker, the
//! controller, one MariaDB node).
//!
//! ```text
//! SPRIG_SFC_GW=127.0.0.1:37306                  # the gateway's MySQL port
//! SPRIG_SFC_MONGO='mongodb://{tenant}.app:sprig-dev-app@127.0.0.1:37717/?authSource=admin&directConnection=true&tls=true&tlsAllowInvalidCertificates=true'
//! SPRIG_SFC_TENANTS=rfa,rfb,rfc                 # tenants with user app
//! SPRIG_SFC_NODE=mysql://root:pw@127.0.0.1:37301  # the node, for PROCESSLIST
//! SPRIG_SFC_METRICS=127.0.0.1:37185,127.0.0.1:37190  # optional: /metrics
//! SPRIG_SFC_WORKER_PIDFILE=<state>/worker.pid   # optional: kill -9 at 1/3
//! SPRIG_SFC_WORKER_START='<command starting the worker again>'
//! SPRIG_SFC_GATEWAY_PIDFILE=<state>/gateway.pid # optional: kill -9 at 2/3
//! SPRIG_SFC_GATEWAY_START='<command starting the gateway again>'
//! cargo test -p sprig-gateway --test shared_feed_live -- --ignored --nocapture
//! ```
//!
//! Each tenant gets, in database `sfc`:
//!
//! - table `ord` with `SPRIG_SFC_TRIGGERS` (8) after-commit triggers in
//!   JavaScript, each writing `(trigger, id)` into `aud` once per insert;
//! - `SPRIG_SFC_INDEXES` (3) tables `s<j>`, each with a search index;
//! - `SPRIG_SFC_STREAMS` (8) collections `c<k>`, each with a collection
//!   change stream, and one database-wide change stream.
//!
//! Then `SPRIG_SFC_WRITERS` (4) SQL sessions per tenant run
//! `SPRIG_SFC_TXNS` (1,200) transactions in all, each inserting one row into
//! `ord` and each `s<j>` (every seventh rolls back), while one writer per
//! collection inserts `SPRIG_SFC_DOCS` (400) documents `{_id: i, seq: i}`
//! in order; SQL writers pause `SPRIG_SFC_PACE_MS` (10) between
//! transactions, document writers `SPRIG_SFC_DOC_PACE_MS` (40), so the
//! kills land while both write. A sampler counts `Binlog Dump` threads on the node every
//! 0.5 s (and, with `SPRIG_SFC_METRICS`, each process's
//! `sprig_feed_binlog_dumps` by kind). Checks:
//!
//! - every committed `ord` row ran each trigger exactly once, and no
//!   rolled-back one ran;
//! - each search index holds exactly its table's rows;
//! - each change stream saw each document once, in insert order, across
//!   the gateway's `kill -9` (the client resumes from its last token);
//! - with `SPRIG_SFC_METRICS`: no process ever had more than one `shared`
//!   dump per node, or any dump of kind `tenant`. Catch-up dumps are
//!   reported (sampled, and each process's
//!   `sprig_feed_resumes_total{tier="binlog"}` at the end, which counts
//!   since its last start), and fail the test under
//!   `SPRIG_SFC_NO_CATCHUP=1`: after the kills, a trigger, search copy or
//!   copy worker that falls far behind may still take one. The node's own count is
//!   reported (samples above `SPRIG_SFC_EXPECT_DUMPS`, 2): it also holds,
//!   for a moment, dumps a killed process or a reopened hub left behind.
//!
//! The worker's other feed consumers, each optional:
//!
//! - `SPRIG_SFC_AMQP=127.0.0.1:37672`: a change exchange on each tenant's
//!   `ord` (`queue.change_exchange = 'ord.x'`, a queue bound with `#` in
//!   vhost `sfc`). Every committed row's insert is published exactly once
//!   (each change's message id is taken by a queue once, so a
//!   killed worker's or a retried publish is a no-op), no rolled-back one.
//! - `SPRIG_SFC_HTTP=127.0.0.1:37080` and `SPRIG_SFC_SPRIGCTL=<sprigctl>`
//!   (with `SPRIGCTL_ENDPOINT`): a graph `sfc.g` in each tenant over two
//!   shards (`sharding.min_shards`), with async mirrors kept by the
//!   worker's graph mirror. One writer per tenant `MERGE`s
//!   `SPRIG_SFC_EDGES` (300) relationships over HTTP and reads each back
//!   through an incoming hop. At the end every relationship has exactly one
//!   mirror. The gateway's mirror waits are reported.
//! - `SPRIG_SFC_TREE=<parent>,<child>`: a materialized share of the
//!   parent's `sfci.products` to the child, kept by the copy worker. One
//!   writer inserts, updates and deletes `SPRIG_SFC_ROWS` (400) rows in the
//!   parent. At the end the child's copy serves and equals the parent's
//!   table.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    clippy::single_match_else,
    missing_docs
)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::event::ResumeToken;
use mongodb::{Client, Collection};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const DB: &str = "sfc";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn num(k: &str, d: u64) -> u64 {
    var(k).and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn pool(gw: &str, tenant: &str, max: usize) -> Pool {
    let (host, port) = gw.split_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .ssl_opts(Some(
            mysql_async::SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ))
        .pool_opts(
            PoolOpts::new()
                .with_constraints(PoolConstraints::new(0, max).unwrap())
                .with_inactive_connection_ttl(Duration::from_secs(120)),
        );
    Pool::new(opts)
}

async fn conn(p: &Pool) -> Conn {
    for _ in 0..480 {
        if let Ok(mut c) = p.get_conn().await
            && c.query_drop(format!("USE {DB}")).await.is_ok()
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("gateway unreachable");
}

/// `Binlog Dump` threads on the node.
async fn dumps(node: &str) -> u64 {
    let Ok(mut c) = Conn::new(Opts::from_url(node).unwrap()).await else {
        return 0;
    };
    let n: Option<u64> = c
        .query_first(
            "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'",
        )
        .await
        .unwrap_or(None);
    let _ = c.disconnect().await;
    n.unwrap_or(0)
}

/// `sprig_feed_binlog_dumps` of one process by `(kind, node)`.
async fn dump_gauges(addr: &str) -> BTreeMap<(String, String), f64> {
    metric(addr, "sprig_feed_binlog_dumps", "kind", "node").await
}

/// The catch-up dumps one process opened since it started, by consumer:
/// `sprig_feed_resumes_total{tier="binlog"}` (the `consumer` label).
async fn catchups(addr: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for ((tier, consumer), v) in metric(addr, "sprig_feed_resumes_total", "tier", "consumer").await
    {
        if tier == "binlog" {
            *out.entry(consumer).or_default() += v;
        }
    }
    out
}

/// Slow subscriptions the shared reader detached, by consumer:
/// `sprig_feed_shared_detached_total`.
async fn detaches(addr: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for ((consumer, _), v) in metric(
        addr,
        "sprig_feed_shared_detached_total",
        "consumer",
        "consumer",
    )
    .await
    {
        *out.entry(consumer).or_default() += v;
    }
    out
}

/// Series `name` of one process's `/metrics`, summed by labels `a` and `b`.
async fn metric(addr: &str, name: &str, a: &str, b: &str) -> BTreeMap<(String, String), f64> {
    let mut out = BTreeMap::new();
    let Ok(Ok(mut s)) =
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr)).await
    else {
        return out;
    };
    let req = format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return out;
    }
    let mut body = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_string(&mut body)).await;
    for line in body.lines() {
        let Some(rest) = line.strip_prefix(name).and_then(|r| r.strip_prefix('{')) else {
            continue;
        };
        let Some((labels, value)) = rest.split_once("} ") else {
            continue;
        };
        let label = |name: &str| {
            labels
                .split(',')
                .find_map(|l| l.strip_prefix(&format!("{name}=\"")))
                .map(|k| k.trim_end_matches('"').to_owned())
                .unwrap_or_default()
        };
        *out.entry((label(a), label(b))).or_default() += value.trim().parse::<f64>().unwrap_or(0.0);
    }
    out
}

/// `sprigctl -o json <args>` (`SPRIG_SFC_SPRIGCTL`), `None` on a failure.
fn sprigctl(args: &[&str]) -> Option<serde_json::Value> {
    let bin = var("SPRIG_SFC_SPRIGCTL")?;
    let out = std::process::Command::new(bin)
        .args(["-o", "json"])
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        println!(
            "sprigctl {args:?}: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        return None;
    }
    Some(serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null))
}

/// A graph's mirror mode and its relationships and mirrors, summed over
/// its shards.
fn mirror_view(tenant: &str, graph: &str) -> Option<(String, u64, u64)> {
    let v = sprigctl(&["graph", "show", tenant, graph])?;
    let shards = v["shards"].as_array().cloned().unwrap_or_default();
    let sum = |k: &str| shards.iter().filter_map(|s| s[k].as_u64()).sum::<u64>();
    Some((
        v["mirror"].as_str().unwrap_or_default().to_owned(),
        sum("relationships"),
        sum("mirrors"),
    ))
}

/// One Cypher statement over REST (`sql:execute`) as `tenant.app`.
async fn cypher(http: &str, tenant: &str, db: &str, q: &str) -> Result<serde_json::Value, String> {
    use base64::Engine as _;
    let body = serde_json::json!({"language": "cypher", "sql": q}).to_string();
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.app:sprig-dev-app"));
    let req = format!(
        "POST /v1/databases/{db}/sql:execute HTTP/1.0\r\nHost: {http}\r\n\
         Authorization: Basic {auth}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut s = tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(http))
        .await
        .map_err(|_| "connect timed out".to_owned())?
        .map_err(|e| e.to_string())?;
    s.write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    tokio::time::timeout(Duration::from_secs(30), s.read_to_end(&mut out))
        .await
        .map_err(|_| "no answer within 30 s".to_owned())?
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out);
    let (head, json) = text.split_once("\r\n\r\n").ok_or("no HTTP answer")?;
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "{}: {json}",
            head.lines().next().unwrap_or_default()
        ));
    }
    let v: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    if let Some(e) = v["results"][0].get("error").filter(|e| !e.is_null()) {
        return Err(e.to_string());
    }
    Ok(v)
}

/// The values of a metric (all its series summed) in one process.
async fn metric_sum(addr: &str, name: &str, label: Option<&str>) -> f64 {
    let Ok(Ok(mut s)) =
        tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr)).await
    else {
        return 0.0;
    };
    let req = format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return 0.0;
    }
    let mut body = String::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), s.read_to_string(&mut body)).await;
    body.lines()
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|r| r.starts_with(' ') || r.starts_with('{'))
                && label.is_none_or(|x| l.contains(x))
        })
        .filter_map(|l| l.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum()
}

/// An AMQP connection and channel as `tenant.app` in vhost `sfc`.
async fn amqp_channel(amqp: &str, tenant: &str) -> (lapin::Connection, lapin::Channel) {
    for _ in 0..240 {
        if let Ok(c) = lapin::Connection::connect(
            &format!("amqp://{tenant}.app:sprig-dev-app@{amqp}/{DB}"),
            lapin::ConnectionProperties::default(),
        )
        .await
            && let Ok(ch) = c.create_channel().await
        {
            return (c, ch);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("AMQP unreachable");
}

/// The ids of the inserts on `queue` until it stays empty for `quiet`.
async fn drain_ids(ch: &lapin::Channel, queue: &str, quiet: Duration) -> Vec<i64> {
    let mut out = Vec::new();
    let mut last = Instant::now();
    loop {
        match ch
            .basic_get(
                queue.into(),
                lapin::options::BasicGetOptions { no_ack: true },
            )
            .await
        {
            Ok(Some(m)) => {
                let v: serde_json::Value =
                    serde_json::from_slice(&m.delivery.data).unwrap_or_default();
                if v["op"].as_str() == Some("c") {
                    let id = &v["after"]["id"];
                    if let Some(id) = id
                        .as_i64()
                        .or_else(|| id.as_str().and_then(|x| x.parse().ok()))
                    {
                        out.push(id);
                    }
                }
                last = Instant::now();
            }
            Ok(None) | Err(_) => {
                if last.elapsed() > quiet {
                    return out;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// `SHOW SHARES`' copy state and lag (ms) of the share `name`.
async fn copy_state(c: &mut Conn, name: &str) -> Option<(String, String)> {
    let rows: Vec<mysql_async::Row> = c.query("SHOW SHARES").await.ok()?;
    rows.iter().find_map(|r| {
        let share: Option<String> = r.get::<Option<String>, _>(0).flatten();
        let role: Option<String> = r.get::<Option<String>, _>(1).flatten();
        if share.as_deref() != Some(name) && role.as_deref() != Some("received") {
            return None;
        }
        let copy: Option<String> = r.get::<Option<String>, _>(10).flatten();
        let lag: Option<String> = r.get::<Option<String>, _>(11).flatten();
        Some((copy.unwrap_or_default(), lag.unwrap_or_default()))
    })
}

/// A tree tenant's connection, in its own space and `sfci`.
async fn tree_conn(gw: &str, tenant: &str, db: bool) -> Conn {
    let p = pool(gw, tenant, 1);
    for _ in 0..480 {
        if let Ok(mut c) = p.get_conn().await
            && c.query_drop(format!("USE SPACE `{tenant}`")).await.is_ok()
            && (!db || c.query_drop("USE sfci").await.is_ok())
        {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("gateway unreachable as {tenant}");
}

/// One statement in the parent of the tree, again after a lost connection;
/// a duplicate key on a retried insert means the first one committed.
async fn tree_exec(gw: &str, tenant: &str, c: &mut Conn, q: &str) {
    for _ in 0..240 {
        match c.query_drop(q).await {
            Ok(()) => return,
            Err(mysql_async::Error::Server(e)) if e.code == 1062 => return,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                *c = tree_conn(gw, tenant, true).await;
            }
        }
    }
    panic!("{q}: never ran");
}

fn trigger_code(j: u64) -> String {
    format!(
        "export default function (row, old, op) {{
  db.exec('INSERT INTO aud (trig, id) VALUES (?, ?)', [{j}, row.id]);
}}"
    )
}

fn kill(pidfile: &str, what: &str) {
    let pid = std::fs::read_to_string(pidfile).unwrap();
    let pid = pid.trim();
    println!("kill -9 {what} {pid}");
    std::process::Command::new("kill")
        .args(["-9", pid])
        .status()
        .unwrap();
}

fn start(cmd: &str) {
    let ok = std::process::Command::new("bash")
        .args(["-c", cmd])
        .status()
        .unwrap()
        .success();
    assert!(ok, "{cmd} failed");
}

/// One change stream's documents, by collection, in the order seen.
type Seen = BTreeMap<String, Vec<i64>>;

/// Follows a change stream until `want` inserts arrived per collection
/// (or `stop`), resuming from its last token when the stream breaks.
async fn follow(
    client_uri: String,
    coll: Option<String>,
    want: BTreeMap<String, usize>,
    stop: Arc<AtomicBool>,
    ready: tokio::sync::mpsc::Sender<()>,
) -> (Seen, u64) {
    let mut seen: Seen = BTreeMap::new();
    let mut token: Option<ResumeToken> = None;
    let mut resumes = 0u64;
    let mut first = true;
    'outer: loop {
        let Ok(client) = Client::with_uri_str(&client_uri).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let db = client.database(DB);
        let opened = match (&coll, &token) {
            (Some(c), Some(t)) => {
                db.collection::<Document>(c)
                    .watch()
                    .resume_after(t.clone())
                    .await
            }
            (Some(c), None) => db.collection::<Document>(c).watch().await,
            (None, Some(t)) => db.watch().resume_after(t.clone()).await,
            (None, None) => db.watch().await,
        };
        let mut s = match opened {
            Ok(s) => s.with_type::<Document>(),
            Err(e) => {
                assert!(!first, "change stream didn't open: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if first {
            first = false;
            let _ = ready.send(()).await;
        }
        loop {
            let done = want
                .iter()
                .all(|(c, n)| seen.get(c).map_or(0, Vec::len) >= *n);
            if done || stop.load(Ordering::Relaxed) {
                break 'outer;
            }
            // `next_if_any` returns after one `getMore` (never cancelled
            // half-way, which the driver doesn't allow).
            match s.next_if_any().await {
                Ok(None) => {
                    if let Some(t) = s.resume_token() {
                        token = Some(t);
                    }
                    if !s.is_alive() {
                        break;
                    }
                }
                Err(e) => {
                    println!("change stream {coll:?} broke ({e}); resuming");
                    resumes += 1;
                    break;
                }
                Ok(Some(ev)) => {
                    if ev.get_str("operationType") == Ok("insert") {
                        let c = ev
                            .get_document("ns")
                            .ok()
                            .and_then(|n| n.get_str("coll").ok())
                            .unwrap_or_default()
                            .to_owned();
                        let seq = ev
                            .get_document("fullDocument")
                            .ok()
                            .and_then(|d| match d.get("seq") {
                                Some(Bson::Int32(v)) => Some(i64::from(*v)),
                                Some(Bson::Int64(v)) => Some(*v),
                                _ => None,
                            })
                            .unwrap_or(-1);
                        seen.entry(c).or_default().push(seq);
                    }
                    if let Some(t) = s.resume_token() {
                        token = Some(t);
                    }
                }
            }
        }
    }
    (seen, resumes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a private deployment (see the module docs)"]
async fn triggers_search_and_change_streams_share_one_dump_per_process() {
    let gw = var("SPRIG_SFC_GW").expect("SPRIG_SFC_GW");
    let mongo = var("SPRIG_SFC_MONGO").expect("SPRIG_SFC_MONGO");
    let node = var("SPRIG_SFC_NODE").expect("SPRIG_SFC_NODE");
    let tenants: Vec<String> = var("SPRIG_SFC_TENANTS")
        .expect("SPRIG_SFC_TENANTS")
        .split(',')
        .map(str::to_owned)
        .collect();
    let metrics: Vec<String> = var("SPRIG_SFC_METRICS")
        .map(|v| v.split(',').map(str::to_owned).collect())
        .unwrap_or_default();
    let triggers = num("SPRIG_SFC_TRIGGERS", 8);
    let indexes = num("SPRIG_SFC_INDEXES", 3);
    let streams = num("SPRIG_SFC_STREAMS", 8);
    let writers = num("SPRIG_SFC_WRITERS", 4);
    let txns = num("SPRIG_SFC_TXNS", 1_200);
    let docs = num("SPRIG_SFC_DOCS", 400);
    let expect_dumps = num("SPRIG_SFC_EXPECT_DUMPS", 2);
    let pace = num("SPRIG_SFC_PACE_MS", 10);
    let doc_pace = num("SPRIG_SFC_DOC_PACE_MS", 40);
    let amqp = var("SPRIG_SFC_AMQP");
    let http = var("SPRIG_SFC_HTTP").filter(|_| var("SPRIG_SFC_SPRIGCTL").is_some());
    let tree: Option<(String, String)> = var("SPRIG_SFC_TREE").and_then(|v| {
        let (a, b) = v.split_once(',')?;
        Some((a.to_owned(), b.to_owned()))
    });
    let edges = num("SPRIG_SFC_EDGES", 300);
    let tree_rows = num("SPRIG_SFC_ROWS", 400);
    let graph = format!("{DB}.g");
    let pools: Vec<Pool> = tenants
        .iter()
        .map(|t| pool(&gw, t, writers as usize + 2))
        .collect();

    // 1. Setup: tables, triggers, search indexes, collections.
    for (ti, t) in tenants.iter().enumerate() {
        let mut c = loop {
            if let Ok(c) = pools[ti].get_conn().await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        c.query_drop(format!("DROP DATABASE IF EXISTS {DB}"))
            .await
            .unwrap();
        c.query_drop(format!("CREATE DATABASE {DB}")).await.unwrap();
        c.query_drop(format!("USE {DB}")).await.unwrap();
        c.query_drop("CREATE TABLE ord (id BIGINT PRIMARY KEY, body TEXT NOT NULL)")
            .await
            .unwrap();
        c.query_drop(
            "CREATE TABLE aud (seq BIGINT AUTO_INCREMENT PRIMARY KEY, trig INT NOT NULL, \
             id BIGINT NOT NULL)",
        )
        .await
        .unwrap();
        for j in 0..indexes {
            c.query_drop(format!(
                "CREATE TABLE s{j} (id BIGINT PRIMARY KEY, body TEXT NOT NULL)"
            ))
            .await
            .unwrap();
            c.query_drop(format!("CREATE SEARCH INDEX ix{j} ON s{j} (body TEXT)"))
                .await
                .unwrap();
        }
        for j in 0..triggers {
            c.query_drop(format!(
                "CREATE TRIGGER aud{j} AFTER COMMIT ON ord FOR INSERT LANGUAGE javascript AS $${}$$",
                trigger_code(j)
            ))
            .await
            .unwrap();
        }
        let client = Client::with_uri_str(mongo.replace("{tenant}", t))
            .await
            .unwrap();
        for k in 0..streams {
            client
                .database(DB)
                .create_collection(format!("c{k}"))
                .await
                .unwrap();
        }
        if let Some(a) = &amqp {
            use lapin::options::{
                ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions, QueuePurgeOptions,
            };
            use lapin::types::FieldTable;
            let (_conn, ch) = amqp_channel(a, t).await;
            ch.exchange_declare(
                "ord.x".into(),
                lapin::ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
            ch.queue_declare(
                "ord_q".into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
            ch.queue_bind(
                "ord_q".into(),
                "ord.x".into(),
                "#".into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
            let _ = ch
                .queue_purge("ord_q".into(), QueuePurgeOptions::default())
                .await;
            c.query_drop("ALTER TABLE ord SPRIG SET (queue.change_exchange = 'ord.x')")
                .await
                .unwrap();
        }
        if http.is_some() {
            sprigctl(&[
                "settings",
                "set",
                "--scope",
                "database",
                "--tenant",
                t,
                "--db",
                DB,
                "sharding.min_shards",
                "2",
                "--reason",
                "shared feed live test",
            ])
            .expect("sharding.min_shards");
            c.query_drop(format!("CREATE GRAPH {graph}")).await.unwrap();
        }
        println!("{t}: {triggers} triggers, {indexes} search indexes, {streams} collections");
    }
    // The copy worker's tree: a materialized share from parent to child.
    if let Some((parent, child)) = &tree {
        for t in [parent, child] {
            let mut c = tree_conn(&gw, t, false).await;
            if t == parent {
                let _ = c.query_drop("DROP SHARE IF EXISTS sfcshare").await;
            }
            for q in [
                "DROP DATABASE IF EXISTS sfci",
                "CREATE DATABASE sfci",
                "USE sfci",
                "CREATE TABLE products (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)",
                "ALTER TABLE products SPRIG INHERITABLE KEY (id)",
            ] {
                c.query_drop(q)
                    .await
                    .unwrap_or_else(|e| panic!("{t}: {q}: {e}"));
            }
        }
        let mut c = tree_conn(&gw, parent, true).await;
        c.query_drop("SPRIG SET inherit.merge = 'materialized'")
            .await
            .unwrap();
        c.query_drop(format!(
            "CREATE SHARE sfcshare ON products TO SPACE `{child}`"
        ))
        .await
        .unwrap();
        let mut k = tree_conn(&gw, child, true).await;
        let t0 = Instant::now();
        loop {
            let st = copy_state(&mut k, "sfcshare").await;
            if st.as_ref().is_some_and(|s| s.0 == "serving") {
                println!(
                    "{child}: the copy serves {:.1} s after the share",
                    t0.elapsed().as_secs_f64()
                );
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(120),
                "{child}: the copy never served: {st:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    // The graphs: async mirrors once the worker's applier serves them.
    if let Some(h) = &http {
        for t in &tenants {
            let t0 = Instant::now();
            loop {
                let _ = cypher(h, t, &graph, "MATCH (n) RETURN count(n)").await;
                let m = mirror_view(t, &graph);
                if m.as_ref().is_some_and(|m| m.0 == "async/ready") {
                    println!(
                        "{t}: graph mirrors async after {:.1} s",
                        t0.elapsed().as_secs_f64()
                    );
                    break;
                }
                assert!(
                    t0.elapsed() < Duration::from_secs(120),
                    "{t}: the graph never became async: {m:?}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    // Every trigger runs: probes (negative ids) until each has written one.
    let t0 = Instant::now();
    let mut probe = 0i64;
    for p in &pools {
        let mut c = conn(p).await;
        loop {
            probe -= 1;
            c.query_drop(format!("INSERT INTO ord VALUES ({probe}, 'probe')"))
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            let n: u64 = c
                .query_first("SELECT COUNT(DISTINCT trig) FROM aud WHERE id < 0")
                .await
                .unwrap()
                .unwrap_or(0);
            if n == triggers {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(180),
                "triggers didn't start"
            );
        }
    }
    println!(
        "all triggers running after {:.1} s",
        t0.elapsed().as_secs_f64()
    );
    // Every change exchange forwarder runs: probes until one is published.
    if let Some(a) = &amqp {
        let t0 = Instant::now();
        for (ti, t) in tenants.iter().enumerate() {
            let (_conn, ch) = amqp_channel(a, t).await;
            let mut c = conn(&pools[ti]).await;
            loop {
                probe -= 1;
                c.query_drop(format!("INSERT INTO ord VALUES ({probe}, 'probe')"))
                    .await
                    .unwrap();
                if !drain_ids(&ch, "ord_q", Duration::from_millis(500))
                    .await
                    .is_empty()
                {
                    break;
                }
                assert!(
                    t0.elapsed() < Duration::from_secs(180),
                    "{t}: the change exchange never published"
                );
            }
        }
        println!(
            "change exchanges publishing after {:.1} s",
            t0.elapsed().as_secs_f64()
        );
    }

    // The dump sampler.
    let stop_sampler = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (node, metrics, stop) = (node.clone(), metrics.clone(), Arc::clone(&stop_sampler));
        tokio::spawn(async move {
            let mut hist: BTreeMap<u64, u64> = BTreeMap::new();
            // Per process and node, the most dumps of each kind seen.
            let mut most: BTreeMap<(String, String, String), f64> = BTreeMap::new();
            while !stop.load(Ordering::Relaxed) {
                let n = dumps(&node).await;
                *hist.entry(n).or_default() += 1;
                for m in &metrics {
                    for ((kind, node), v) in dump_gauges(m).await {
                        let e = most.entry((m.clone(), kind, node)).or_default();
                        *e = e.max(v);
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            (hist, most)
        })
    };

    // 2. Change streams, opened before any document is written.
    let stop_streams = Arc::new(AtomicBool::new(false));
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel(1024);
    let mut followers = Vec::new();
    for t in &tenants {
        let uri = mongo.replace("{tenant}", t);
        let all: BTreeMap<String, usize> = (0..streams)
            .map(|k| (format!("c{k}"), docs as usize))
            .collect();
        for k in 0..streams {
            let want: BTreeMap<String, usize> = [(format!("c{k}"), docs as usize)].into();
            followers.push((
                format!("{t}.c{k}"),
                tokio::spawn(follow(
                    uri.clone(),
                    Some(format!("c{k}")),
                    want,
                    Arc::clone(&stop_streams),
                    ready_tx.clone(),
                )),
            ));
        }
        followers.push((
            format!("{t}.*"),
            tokio::spawn(follow(
                uri,
                None,
                all,
                Arc::clone(&stop_streams),
                ready_tx.clone(),
            )),
        ));
    }
    for _ in 0..followers.len() {
        tokio::time::timeout(Duration::from_secs(60), ready_rx.recv())
            .await
            .expect("change streams didn't open");
    }
    println!(
        "{} change streams open; {} dumps on the node",
        followers.len(),
        dumps(&node).await
    );

    // 3. Writers, with kills along the way.
    let done = Arc::new(AtomicU64::new(0));
    let total = txns * tenants.len() as u64;
    let mut tasks = Vec::new();
    for (ti, p) in pools.iter().enumerate() {
        for w in 0..writers {
            let (p, done) = (p.clone(), Arc::clone(&done));
            tasks.push(tokio::spawn(async move {
                let mut c = conn(&p).await;
                let mut committed = Vec::new();
                let mut rolled = Vec::new();
                let mut i = w;
                while i < txns {
                    let id = i as i64 + 1;
                    let rollback = id % 7 == 0;
                    let mut stmts = vec![
                        "BEGIN".to_owned(),
                        format!("INSERT INTO ord VALUES ({id}, 'row write {id}')"),
                    ];
                    for j in 0..indexes {
                        stmts.push(format!("INSERT INTO s{j} VALUES ({id}, 'row write {id}')"));
                    }
                    stmts.push(if rollback { "ROLLBACK" } else { "COMMIT" }.to_owned());
                    tokio::time::sleep(Duration::from_millis(pace)).await;
                    let mut ok = true;
                    for s in &stmts {
                        if c.query_drop(s).await.is_err() {
                            ok = false;
                            break;
                        }
                    }
                    if !ok {
                        // A lost connection (a gateway's kill): the row
                        // tells whether the commit happened; else again.
                        c = conn(&p).await;
                        let there: Option<i64> = c
                            .query_first(format!("SELECT id FROM ord WHERE id = {id}"))
                            .await
                            .unwrap_or(None);
                        if there.is_none() {
                            continue;
                        }
                        committed.push(id);
                    } else if rollback {
                        rolled.push(id);
                    } else {
                        committed.push(id);
                    }
                    done.fetch_add(1, Ordering::Relaxed);
                    i += writers;
                }
                (ti, committed, rolled)
            }));
        }
    }
    let mut doc_tasks = Vec::new();
    for t in &tenants {
        for k in 0..streams {
            let uri = mongo.replace("{tenant}", t);
            doc_tasks.push(tokio::spawn(async move {
                let mut i = 0i64;
                while i < docs as i64 {
                    let Ok(client) = Client::with_uri_str(&uri).await else {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    };
                    let coll: Collection<Document> =
                        client.database(DB).collection(&format!("c{k}"));
                    while i < docs as i64 {
                        match coll.insert_one(doc! {"_id": i, "seq": i}).await {
                            Ok(_) => i += 1,
                            Err(e) if e.to_string().contains("11000") => i += 1,
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(doc_pace)).await;
                    }
                }
            }));
        }
    }
    // Graph writers: a relationship at a time, read back through an
    // incoming hop (the mirror, when fresh; else a scatter).
    let mut graph_tasks = Vec::new();
    if let Some(h) = &http {
        for t in &tenants {
            let (h, t, g) = (h.clone(), t.clone(), graph.clone());
            graph_tasks.push(tokio::spawn(async move {
                let (mut wrong, mut retried, mut read_ms) = (Vec::new(), 0u64, Vec::new());
                for i in 0..edges as i64 {
                    let j = i + 1_000_000;
                    let w = format!(
                        "MERGE (a:N {{k: {i}}}) MERGE (b:N {{k: {j}}}) MERGE (a)-[:R]->(b)"
                    );
                    while let Err(e) = cypher(&h, &t, &g, &w).await {
                        retried += 1;
                        if retried % 50 == 1 {
                            println!("{t}: graph write {i}: {e}");
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    let r = format!("MATCH (b:N {{k: {j}}})<-[:R]-(a:N) RETURN a.k");
                    let t0 = Instant::now();
                    let got = loop {
                        match cypher(&h, &t, &g, &r).await {
                            Ok(v) => break v,
                            Err(_) => {
                                retried += 1;
                                tokio::time::sleep(Duration::from_millis(250)).await;
                            }
                        }
                    };
                    read_ms.push(t0.elapsed().as_millis() as u64);
                    let ks: Vec<i64> = got["results"][0]["rows"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|row| {
                            let x = &row[0];
                            x.as_i64()
                                .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                        })
                        .collect();
                    if ks != vec![i] {
                        wrong.push((i, ks));
                    }
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
                (t, wrong, retried, read_ms)
            }));
        }
    }
    // The tree's writer: inserts, updates and deletes in the parent.
    let tree_task = tree.clone().map(|(parent, _)| {
        let gw = gw.clone();
        tokio::spawn(async move {
            let mut c = tree_conn(&gw, &parent, true).await;
            let mut want: BTreeMap<i64, i64> = BTreeMap::new();
            for i in 1..=tree_rows as i64 {
                tree_exec(
                    &gw,
                    &parent,
                    &mut c,
                    &format!("INSERT INTO products VALUES ({i}, {i})"),
                )
                .await;
                want.insert(i, i);
                if i % 5 == 0 && want.contains_key(&(i - 1)) {
                    tree_exec(
                        &gw,
                        &parent,
                        &mut c,
                        &format!("UPDATE products SET v = {} WHERE id = {}", -i, i - 1),
                    )
                    .await;
                    want.insert(i - 1, -i);
                }
                if i % 7 == 0 {
                    tree_exec(
                        &gw,
                        &parent,
                        &mut c,
                        &format!("DELETE FROM products WHERE id = {}", i - 3),
                    )
                    .await;
                    want.remove(&(i - 3));
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            want
        })
    });
    let worker_kill = var("SPRIG_SFC_WORKER_PIDFILE").zip(var("SPRIG_SFC_WORKER_START"));
    let gateway_kill = var("SPRIG_SFC_GATEWAY_PIDFILE").zip(var("SPRIG_SFC_GATEWAY_START"));
    let (mut wk, mut gk) = (worker_kill.is_none(), gateway_kill.is_none());
    let t1 = Instant::now();
    while tasks.iter().any(|t| !t.is_finished())
        || graph_tasks.iter().any(|t| !t.is_finished())
        || tree_task.as_ref().is_some_and(|t| !t.is_finished())
    {
        let n = done.load(Ordering::Relaxed);
        if !wk && n >= total / 3 {
            wk = true;
            let (pf, cmd) = worker_kill.clone().unwrap();
            kill(&pf, "worker");
            start(&cmd);
        }
        if !gk && n >= total * 2 / 3 {
            gk = true;
            let (pf, cmd) = gateway_kill.clone().unwrap();
            kill(&pf, "gateway");
            start(&cmd);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut committed: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); tenants.len()];
    let mut rolled: Vec<BTreeSet<i64>> = vec![BTreeSet::new(); tenants.len()];
    for t in tasks {
        let (ti, c, r) = t.await.unwrap();
        committed[ti].extend(c);
        rolled[ti].extend(r);
    }
    for t in doc_tasks {
        t.await.unwrap();
    }
    println!(
        "writes done in {:.1} s: {} transactions committed, {} rolled back, {} documents",
        t1.elapsed().as_secs_f64(),
        committed.iter().map(BTreeSet::len).sum::<usize>(),
        rolled.iter().map(BTreeSet::len).sum::<usize>(),
        docs * streams * tenants.len() as u64
    );

    // 4. Change streams: each document once, in order.
    // After 120 s every follower stops and reports what it saw.
    {
        let stop = Arc::clone(&stop_streams);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(120)).await;
            stop.store(true, Ordering::Relaxed);
        });
    }
    let mut failures = Vec::new();
    let mut resumes = 0u64;
    for (name, f) in followers {
        let (seen, n) = f.await.unwrap();
        resumes += n;
        let colls: Vec<String> = if name.ends_with(".*") {
            (0..streams).map(|k| format!("c{k}")).collect()
        } else {
            vec![name.rsplit_once('.').unwrap().1.to_owned()]
        };
        for c in colls {
            let got = seen.get(&c).cloned().unwrap_or_default();
            let want: Vec<i64> = (0..docs as i64).collect();
            if got != want {
                let mut sorted = got.clone();
                sorted.sort_unstable();
                sorted.dedup();
                failures.push(format!(
                    "{name} {c}: {} events ({} distinct, last {:?}), first differences {:?}",
                    got.len(),
                    sorted.len(),
                    got.last(),
                    got.iter()
                        .zip(&want)
                        .filter(|(a, b)| a != b)
                        .take(3)
                        .collect::<Vec<_>>()
                ));
            }
        }
    }
    println!("change streams checked ({resumes} resumes after breaks)");

    // 5. Triggers: each committed row once per trigger; rolled back none.
    let t2 = Instant::now();
    for (ti, t) in tenants.iter().enumerate() {
        let mut c = conn(&pools[ti]).await;
        let want = committed[ti].len() as u64 * triggers;
        loop {
            let n: u64 = c
                .query_first("SELECT COUNT(*) FROM aud WHERE id > 0")
                .await
                .unwrap()
                .unwrap_or(0);
            if n >= want || t2.elapsed() > Duration::from_secs(180) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let rows: Vec<(u64, i64, u64)> = c
            .query("SELECT trig, id, COUNT(*) FROM aud WHERE id > 0 GROUP BY trig, id")
            .await
            .unwrap();
        let twice = rows.iter().filter(|r| r.2 > 1).count();
        let ghost = rows
            .iter()
            .filter(|r| rolled[ti].contains(&r.1) || !committed[ti].contains(&r.1))
            .count();
        let mut missing = 0;
        for j in 0..triggers {
            let have: BTreeSet<i64> = rows.iter().filter(|r| r.0 == j).map(|r| r.1).collect();
            missing += committed[ti].difference(&have).count();
        }
        println!(
            "{t}: triggers ran {} of {want}, {twice} twice, {ghost} not committed, {missing} missing",
            rows.iter().map(|r| r.2).sum::<u64>()
        );
        if twice + ghost + missing > 0 {
            failures.push(format!(
                "{t}: triggers {twice} twice, {ghost} not committed, {missing} missing"
            ));
        }
        // 6. Search: each index holds exactly its table's rows.
        for j in 0..indexes {
            let mut last = Vec::new();
            let mut error = String::new();
            let want: Vec<i64> = committed[ti].iter().copied().collect();
            for _ in 0..120 {
                let r: Result<Vec<i64>, _> = c
                    .query(format!(
                        "SELECT id FROM sprig_search('ix{j}', 'row write', \
                         JSON_OBJECT('limit', 100000, 'consistency', 'latest')) AS s ORDER BY id"
                    ))
                    .await;
                match r {
                    Ok(ids) => {
                        last = ids;
                        if last == want {
                            break;
                        }
                    }
                    Err(e) => error = e.to_string(),
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            if last != want {
                let shown: Vec<mysql_async::Row> = c
                    .query(format!("SHOW SEARCH INDEXES FROM s{j}"))
                    .await
                    .unwrap_or_default();
                let shown: Vec<String> = shown.iter().map(|r| format!("{r:?}")).collect();
                failures.push(format!(
                    "{t}: ix{j} holds {} rows, the table {} (last error: {error}; {shown:?})",
                    last.len(),
                    want.len()
                ));
            }
        }
    }
    println!("triggers and search checked");

    // 8. Change exchanges: each committed row's insert published once, no
    // rolled-back one.
    if let Some(a) = &amqp {
        for (ti, t) in tenants.iter().enumerate() {
            let (_conn, ch) = amqp_channel(a, t).await;
            let mut ids = Vec::new();
            let t4 = Instant::now();
            loop {
                ids.extend(drain_ids(&ch, "ord_q", Duration::from_secs(3)).await);
                let have: BTreeSet<i64> = ids.iter().copied().collect();
                if committed[ti].is_subset(&have) || t4.elapsed() > Duration::from_secs(180) {
                    break;
                }
            }
            let mut n: BTreeMap<i64, u64> = BTreeMap::new();
            for i in ids.iter().filter(|i| **i > 0) {
                *n.entry(*i).or_default() += 1;
            }
            let missing = committed[ti].iter().filter(|i| !n.contains_key(i)).count();
            let ghost = n.keys().filter(|i| !committed[ti].contains(i)).count();
            let twice = n.values().filter(|c| **c > 1).count();
            println!(
                "{t}: change exchange published {} inserts for {} committed rows: {missing} \
                 missing, {ghost} not committed, {twice} more than once",
                n.values().sum::<u64>(),
                committed[ti].len()
            );
            if missing + ghost + twice > 0 {
                failures.push(format!(
                    "{t}: change exchange {missing} missing, {ghost} not committed, {twice} \
                     more than once"
                ));
            }
        }
    }

    // 9. Graph mirrors: every read found its relationship; at the end each
    // relationship has exactly one mirror.
    for task in graph_tasks {
        let (t, wrong, retried, mut ms) = task.await.unwrap();
        ms.sort_unstable();
        let pct = |p: usize| {
            ms.get(ms.len().saturating_sub(1) * p / 100)
                .copied()
                .unwrap_or(0)
        };
        let t5 = Instant::now();
        let view = loop {
            let v = mirror_view(&t, &graph);
            if v.as_ref().is_some_and(|v| v.1 == edges && v.2 == edges)
                || t5.elapsed() > Duration::from_secs(120)
            {
                break v;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        println!(
            "{t}: graph {edges} relationships written ({retried} retries), {} reads wrong; \
             incoming-hop reads p50 {} ms, p99 {} ms, max {} ms; mirrors {view:?} after {:.1} s",
            wrong.len(),
            pct(50),
            pct(99),
            ms.last().copied().unwrap_or(0),
            t5.elapsed().as_secs_f64()
        );
        if !wrong.is_empty() {
            failures.push(format!(
                "{t}: graph reads wrong: {:?}",
                &wrong[..wrong.len().min(5)]
            ));
        }
        if view.as_ref().is_none_or(|v| v.1 != edges || v.2 != edges) {
            failures.push(format!(
                "{t}: graph relationships and mirrors {view:?}, want {edges}"
            ));
        }
    }
    if http.is_some() {
        for m in &metrics {
            let ok = metric_sum(m, "sprig_graph_mirror_waits_total", Some("outcome=\"ok\"")).await;
            let lag =
                metric_sum(m, "sprig_graph_mirror_waits_total", Some("outcome=\"lag\"")).await;
            let sum = metric_sum(m, "sprig_graph_mirror_wait_seconds_sum", None).await;
            let count = metric_sum(m, "sprig_graph_mirror_wait_seconds_count", None).await;
            if ok + lag > 0.0 {
                println!(
                    "  {m}: mirror waits since its start: {ok} fresh, {lag} stale (scattered); \
                     mean wait {:.0} ms",
                    if count > 0.0 {
                        sum / count * 1000.0
                    } else {
                        0.0
                    }
                );
            }
        }
    }

    // 10. The copy worker: the child's copy serves and equals the parent.
    if let (Some((parent, child)), Some(task)) = (&tree, tree_task) {
        let want = task.await.unwrap();
        let mut k = tree_conn(&gw, child, true).await;
        let t6 = Instant::now();
        let (got, st) = loop {
            let got: BTreeMap<i64, i64> = match k
                .query::<(i64, i64), _>("SELECT id, v FROM products ORDER BY id")
                .await
            {
                Ok(v) => v.into_iter().collect(),
                Err(_) => {
                    k = tree_conn(&gw, child, true).await;
                    BTreeMap::new()
                }
            };
            let st = copy_state(&mut k, "sfcshare").await;
            let serving = st.as_ref().is_some_and(|s| s.0 == "serving");
            if (got == want && serving) || t6.elapsed() > Duration::from_secs(120) {
                break (got, st);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let mut p = tree_conn(&gw, parent, true).await;
        let parent_rows: BTreeMap<i64, i64> = p
            .query::<(i64, i64), _>("SELECT id, v FROM products ORDER BY id")
            .await
            .unwrap()
            .into_iter()
            .collect();
        let missing = want.keys().filter(|i| !got.contains_key(i)).count();
        let extra = got.keys().filter(|i| !want.contains_key(i)).count();
        let differ = want
            .iter()
            .filter(|(i, v)| got.get(i).is_some_and(|g| g != *v))
            .count();
        println!(
            "{child}: copy {st:?}; {} rows in the parent ({} by its writer), {} in the child: \
             {missing} missing, {extra} extra, {differ} different, after {:.1} s",
            parent_rows.len(),
            want.len(),
            got.len(),
            t6.elapsed().as_secs_f64()
        );
        if parent_rows != want || got != want || st.is_none_or(|s| s.0 != "serving") {
            failures.push(format!(
                "{child}: the copy: {missing} missing, {extra} extra, {differ} different \
                 (parent as written: {})",
                parent_rows == want
            ));
        }
    }

    // 7. Dumps.
    stop_sampler.store(true, Ordering::Relaxed);
    let (hist, most) = sampler.await.unwrap();
    let above = hist
        .iter()
        .filter(|(n, _)| **n > expect_dumps)
        .map(|(_, c)| c)
        .sum::<u64>();
    // The node's count includes dumps a killed process left (MariaDB ends
    // them when it next writes to them) and a reopened hub's old one for
    // a moment, so the check is each process's own count.
    println!(
        "Binlog Dump threads on the node per sample: {hist:?} ({above} samples above \
         {expect_dumps}); now {}",
        dumps(&node).await
    );
    for ((process, kind, node), v) in &most {
        println!("  {process}: most {kind} dumps on {node}: {v}");
        let bad = match kind.as_str() {
            "shared" => *v > 1.0,
            "tenant" => *v > 0.0,
            _ => false,
        };
        if bad {
            failures.push(format!("{process}: {v} {kind} dumps on {node} at once"));
        }
    }
    // Catch-up dumps (the change exchange publishes in
    // batches and keeps up after a worker's restart; a failure under
    // SPRIG_SFC_NO_CATCHUP=1).
    let allow_catchup = var("SPRIG_SFC_NO_CATCHUP").as_deref() != Some("1");
    for m in &metrics {
        let by = catchups(m).await;
        let n: f64 = by.values().sum();
        println!(
            "  {m}: catch-up dumps since its last start: {n} {by:?}; detached: {:?}",
            detaches(m).await
        );
        if n > 0.0 && !allow_catchup {
            failures.push(format!("{m}: {n} catch-up dumps since its last start"));
        }
    }
    for ((process, kind, node), v) in &most {
        if kind == "catchup" && *v > 0.0 && !allow_catchup {
            failures.push(format!("{process}: {v} catch-up dumps on {node} at once"));
        }
    }
    if metrics.is_empty() && hist.keys().max().copied().unwrap_or(0) > expect_dumps {
        failures.push("more dumps on the node than processes".to_owned());
    }
    // Clean up.
    if let Some((parent, child)) = &tree {
        let mut c = tree_conn(&gw, parent, true).await;
        let _ = c.query_drop("DROP SHARE IF EXISTS sfcshare").await;
        for t in [parent, child] {
            let mut c = tree_conn(&gw, t, false).await;
            let _ = c.query_drop("DROP DATABASE IF EXISTS sfci").await;
        }
    }
    for p in &pools {
        let mut c = conn(p).await;
        if amqp.is_some() {
            let _ = c
                .query_drop("ALTER TABLE ord SPRIG RESET (queue.change_exchange)")
                .await;
        }
        if http.is_some() {
            let _ = c.query_drop(format!("DROP GRAPH IF EXISTS {graph}")).await;
        }
        for j in 0..triggers {
            let _ = c.query_drop(format!("DROP TRIGGER IF EXISTS aud{j}")).await;
        }
        let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
