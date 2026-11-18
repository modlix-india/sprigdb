//! Reads right after a move into a cluster with replicas, live.
//!
//! The mover writes the copied rows itself, outside any client session, so
//! no session's read-your-writes position names them. Its cutover records
//! the destination writer's position after its last write as the moved
//! ranges' read fence, and every gateway adds the fence to the causal
//! target of the tenant's reads on that cluster: a replica that hasn't
//! applied the copy gets none of them.
//!
//! The test makes the replicas fall behind for certain: it stops their SQL
//! threads before the move, so they never apply the copy while the reads
//! run. The destination cluster's `reads.max_replica_lag_ms` must be high
//! (the harness sets it, say to 600000), or the gateway would skip the
//! stopped replicas for their lag alone and the test would prove nothing.
//!
//! Then, on the destination, every row and document is read through:
//!
//! - a MySQL-protocol session opened before the move (text and prepared
//!   statements) and one opened after it;
//! - a MongoDB client connected before the move and one after it.
//!
//! Each read must answer the row. With the replicas started again, reads
//! go to them once more (when `SPRIG_TEST_FENCE_METRICS` names the
//! gateway's metrics page and `SPRIG_TEST_FENCE_CLUSTER` the destination),
//! so the fence doesn't keep the tenant on the writer.
//!
//! ```sh
//! export SPRIG_TEST_FENCE_SQL=127.0.0.1:47306
//! export SPRIG_TEST_FENCE_MONGO=127.0.0.1:47737
//! export SPRIG_TEST_FENCE_TENANT=acme
//! export SPRIG_TEST_FENCE_REPLICAS='mysql://root:pw@127.0.0.1:47312,mysql://root:pw@127.0.0.1:47313'
//! export SPRIG_TEST_FENCE_MOVE_IN='sprigctl shard move acme --to dc-a-c2 --wait'
//! export SPRIG_TEST_FENCE_MOVE_OUT='sprigctl shard move acme --to dc-a-c1 --wait'
//! export SPRIG_TEST_FENCE_METRICS=http://127.0.0.1:47190/metrics   # optional
//! export SPRIG_TEST_FENCE_CLUSTER=dc-a-c2                          # optional
//! cargo test -p sprig-gateway --test move_read_fence_live -- --nocapture
//! ```
//!
//! The tenant starts on a cluster other than the destination. The test
//! uses databases `fence_live_sql` and `fence_live_doc` and drops them at
//! the end; it starts the replicas' SQL threads again whatever happens.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use mongodb::bson::{Document, doc};
use mysql_async::prelude::Queryable;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SQL_DB: &str = "fence_live_sql";
const DOC_DB: &str = "fence_live_doc";
const ROWS: i64 = 20;

struct Env {
    sql: String,
    mongo: String,
    tenant: String,
    replicas: Vec<String>,
    move_in: String,
    move_out: Option<String>,
    metrics: Option<(String, String)>,
}

fn env() -> Option<Env> {
    let get = |k: &str| std::env::var(k).ok().filter(|s| !s.is_empty());
    Some(Env {
        sql: get("SPRIG_TEST_FENCE_SQL")?,
        mongo: get("SPRIG_TEST_FENCE_MONGO")?,
        tenant: get("SPRIG_TEST_FENCE_TENANT").unwrap_or_else(|| "acme".into()),
        replicas: get("SPRIG_TEST_FENCE_REPLICAS")?
            .split(',')
            .map(str::to_owned)
            .collect(),
        move_in: get("SPRIG_TEST_FENCE_MOVE_IN")?,
        move_out: get("SPRIG_TEST_FENCE_MOVE_OUT"),
        metrics: get("SPRIG_TEST_FENCE_METRICS").zip(get("SPRIG_TEST_FENCE_CLUSTER")),
    })
}

async fn sql(e: &Env) -> mysql_async::Conn {
    let (host, port) = e.sql.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.app", e.tenant)))
        .pass(Some("sprig-dev-app"))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn mongo(e: &Env) -> mongodb::Client {
    let uri = format!(
        "mongodb://{}.app:sprig-dev-app@{}/?authSource=admin&directConnection=true&maxPoolSize=1",
        e.tenant, e.mongo
    );
    mongodb::Client::with_uri_str(&uri).await.unwrap()
}

/// Stops (`false`) or starts the replicas' SQL threads; their states.
async fn sql_threads(e: &Env, run: bool) -> Vec<String> {
    let mut out = Vec::new();
    for url in &e.replicas {
        let mut c = mysql_async::Conn::from_url(url).await.unwrap();
        let q = if run {
            "START REPLICA SQL_THREAD"
        } else {
            "STOP REPLICA SQL_THREAD"
        };
        c.query_drop(q).await.unwrap();
        let row: Option<mysql_async::Row> = c.query_first("SHOW REPLICA STATUS").await.unwrap();
        let state = row
            .and_then(|r| {
                r.get_opt::<String, _>("Slave_SQL_Running")
                    .and_then(Result::ok)
            })
            .unwrap_or_default();
        out.push(format!("{url}: SQL thread {state}"));
        c.disconnect().await.ok();
    }
    out
}

/// Runs a move command, waiting out a previous move's hold (39131).
async fn run_move(cmd: &str) {
    let started = Instant::now();
    loop {
        let c = cmd.to_owned();
        let out = tokio::task::spawn_blocking(move || {
            std::process::Command::new("sh").arg("-c").arg(c).output()
        })
        .await
        .unwrap()
        .unwrap();
        let err = String::from_utf8_lossy(&out.stderr).into_owned();
        if !out.status.success()
            && err.contains("39131")
            && started.elapsed() < Duration::from_secs(300)
        {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        eprintln!("{cmd}: {} in {:?}", out.status, started.elapsed());
        assert!(out.status.success(), "{cmd}: {err}");
        return;
    }
}

/// The value of row `id` through a text statement, as the session sees it.
async fn text_read(c: &mut mysql_async::Conn, id: i64) -> Result<Option<i64>, String> {
    c.query_first::<i64, _>(format!("SELECT v FROM {SQL_DB}.t WHERE id = {id}"))
        .await
        .map_err(|e| e.to_string())
}

async fn prepared_read(c: &mut mysql_async::Conn, id: i64) -> Result<Option<i64>, String> {
    c.exec_first::<i64, _, _>(format!("SELECT v FROM {SQL_DB}.t WHERE id = ?"), (id,))
        .await
        .map_err(|e| e.to_string())
}

async fn doc_read(c: &mongodb::Client, id: i64) -> Result<Option<i64>, String> {
    c.database(DOC_DB)
        .collection::<Document>("docs")
        .find_one(doc! {"_id": id})
        .await
        .map(|d| d.and_then(|d| d.get_i64("n").ok()))
        .map_err(|e| e.to_string())
}

/// The sum of `sprig_backend_reads_total` for `cluster` and target
/// `replica` on the gateway's metrics page.
async fn replica_reads(url: &str, cluster: &str) -> u64 {
    let rest = url.strip_prefix("http://").unwrap();
    let (hostport, path) = rest.split_once('/').unwrap_or((rest, ""));
    let mut s = tokio::net::TcpStream::connect(hostport).await.unwrap();
    s.write_all(format!("GET /{path} HTTP/1.0\r\nHost: {hostport}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut body = String::new();
    s.read_to_string(&mut body).await.unwrap();
    body.lines()
        .filter(|l| {
            l.starts_with("sprig_backend_reads_total{")
                && l.contains(&format!("cluster=\"{cluster}\""))
                && l.contains("target=\"replica\"")
        })
        // Counters print as whole numbers.
        .filter_map(|l| l.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

async fn setup(e: &Env) {
    let mut c = sql(e).await;
    for db in [SQL_DB, DOC_DB] {
        c.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
            .await
            .unwrap();
    }
    c.query_drop(format!("CREATE DATABASE {SQL_DB}"))
        .await
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {SQL_DB}.t (id INT PRIMARY KEY, v BIGINT NOT NULL)"
    ))
    .await
    .unwrap();
    let values: Vec<String> = (1..=ROWS).map(|i| format!("({i}, {})", i * 10)).collect();
    c.query_drop(format!(
        "INSERT INTO {SQL_DB}.t VALUES {}",
        values.join(", ")
    ))
    .await
    .unwrap();
    c.disconnect().await.ok();
    let m = mongo(e).await;
    let docs: Vec<Document> = (1..=ROWS).map(|i| doc! {"_id": i, "n": i}).collect();
    m.database(DOC_DB)
        .collection::<Document>("docs")
        .insert_many(docs)
        .await
        .unwrap();
}

async fn teardown(e: &Env) {
    let mut c = sql(e).await;
    for db in [SQL_DB, DOC_DB] {
        let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {db}")).await;
    }
    c.disconnect().await.ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_after_a_move_wait_for_the_copy_on_replicas() {
    let Some(e) = env() else {
        eprintln!(
            "skipped: set SPRIG_TEST_FENCE_SQL, _MONGO, _REPLICAS and _MOVE_IN (see the module docs)"
        );
        return;
    };
    setup(&e).await;
    // Sessions opened before the move, warm.
    let mut old_sql = sql(&e).await;
    let old_doc = mongo(&e).await;
    for id in 1..=ROWS {
        assert_eq!(text_read(&mut old_sql, id).await, Ok(Some(id * 10)));
        assert_eq!(prepared_read(&mut old_sql, id).await, Ok(Some(id * 10)));
        assert_eq!(doc_read(&old_doc, id).await, Ok(Some(id)));
    }

    let stopped = sql_threads(&e, false).await;
    eprintln!("before the move: {stopped:?}");
    let mut wrong = Vec::new();
    let mut reads = 0u64;
    let moved = tokio::spawn({
        let cmd = e.move_in.clone();
        async move { run_move(&cmd).await }
    })
    .await;
    if moved.is_ok() {
        let mut new_sql = sql(&e).await;
        let new_doc = mongo(&e).await;
        // Several passes over every row, for two seconds.
        let until = Instant::now() + Duration::from_secs(2);
        while Instant::now() < until {
            for id in 1..=ROWS {
                let got = [
                    ("old session, text", text_read(&mut old_sql, id).await),
                    (
                        "old session, prepared",
                        prepared_read(&mut old_sql, id).await,
                    ),
                    ("new session, text", text_read(&mut new_sql, id).await),
                    (
                        "new session, prepared",
                        prepared_read(&mut new_sql, id).await,
                    ),
                ];
                for (how, r) in got {
                    reads += 1;
                    if r != Ok(Some(id * 10)) && wrong.len() < 40 {
                        wrong.push(format!("SQL {how}, row {id}: {r:?}"));
                    }
                }
                for (how, c) in [("old client", &old_doc), ("new client", &new_doc)] {
                    let r = doc_read(c, id).await;
                    reads += 1;
                    if r != Ok(Some(id)) && wrong.len() < 40 {
                        wrong.push(format!("MongoDB {how}, _id {id}: {r:?}"));
                    }
                }
            }
        }
        new_sql.disconnect().await.ok();
    }
    let started = sql_threads(&e, true).await;
    eprintln!("after the reads: {started:?}");
    assert!(moved.is_ok(), "the move failed: {moved:?}");
    eprintln!("{reads} reads on the destination while its replicas were stopped");

    // Caught up, the replicas serve the tenant's reads again.
    if let Some((url, cluster)) = &e.metrics {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let mut c = sql(&e).await;
        let before = replica_reads(url, cluster).await;
        for _ in 0..5 {
            for id in 1..=ROWS {
                assert_eq!(text_read(&mut c, id).await, Ok(Some(id * 10)));
            }
        }
        let after = replica_reads(url, cluster).await;
        eprintln!("replica reads on {cluster}: {before} -> {after}");
        assert!(
            after > before,
            "no read went to a replica of {cluster} after it caught up"
        );
        c.disconnect().await.ok();
    }

    old_sql.disconnect().await.ok();
    drop(old_doc);
    if let Some(cmd) = &e.move_out {
        run_move(cmd).await;
    }
    teardown(&e).await;
    assert!(reads > 0, "no reads");
    assert!(wrong.is_empty(), "wrong answers: {wrong:#?}");
}
