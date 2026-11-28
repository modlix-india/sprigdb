//! Embedded columns on a cluster of replicated gateways: the node that
//! fills a tenant's embedded columns is the one leading its unit, whether
//! or not a client of the tenant reached it, so filling goes on after the
//! leader stops.
//!
//! Needs the local stack of `tools/live/stack.sh` with three gateways (the
//! bundled embedding model next to its `sprigdb` binary, in
//! `models/bge-small-en-v1.5`) and a tenant made by `stack.sh tenant`:
//!
//! ```sh
//! SPRIGDB_ENGINE_INSECURE_CLUSTER=true tools/live/stack.sh up 3
//! tools/live/stack.sh tenant live
//! eval "$(tools/live/stack.sh env live | sed 's/^/export /')"
//! export SPRIG_LIVE_STACK=tools/live/stack.sh
//! export SPRIG_LIVE_TENANT_ID=$(tools/live/stack.sh ctl -o json tenant show live | jq .tenant_id)
//! cargo test -p sprig-gateway --test ai_embed_multinode_live -- --ignored --test-threads 1
//! ```
//!
//! The variables are `stack.sh env`'s (`SPRIG_LIVE_TENANT`, `SPRIG_LIVE_USER`,
//! `SPRIG_LIVE_PASSWORD`, `SPRIG_LIVE_GW<n>_MYSQL` / `_HTTP` for gateways 1
//! to 3), `SPRIG_LIVE_STACK`, the script that stops and starts one
//! gateway, and `SPRIG_LIVE_TENANT_ID` (the tenant's id, which is its
//! unit's). Works in the database `aiemb_multi`, dropped at the end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::print_stdout,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn var(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!("{k} is not set (a live test: see this file's docs and tools/live/README.md)")
    })
}

/// Gateway n's (MySQL, HTTP) addresses.
fn gateways() -> BTreeMap<u64, (String, String)> {
    (1..=3u64)
        .map(|n| {
            (
                n,
                (
                    var(&format!("SPRIG_LIVE_GW{n}_MYSQL")),
                    var(&format!("SPRIG_LIVE_GW{n}_HTTP")),
                ),
            )
        })
        .collect()
}

async fn conn(addr: &str, db: Option<&str>) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(var("SPRIG_LIVE_USER")))
        .pass(Some(var("SPRIG_LIVE_PASSWORD")))
        .db_name(db)
        .prefer_socket(false)
        .ssl_opts(Some(
            SslOpts::default()
                .with_danger_accept_invalid_certs(true)
                .with_danger_skip_domain_validation(true),
        ));
    Conn::new(opts).await.unwrap()
}

async fn must(c: &mut Conn, q: &str) {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
}

async fn http_get(addr: &str, path: &str) -> Value {
    let Ok(mut s) = tokio::net::TcpStream::connect(addr).await else {
        return Value::Null;
    };
    let req = format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n");
    if s.write_all(req.as_bytes()).await.is_err() {
        return Value::Null;
    }
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out).await;
    let text = String::from_utf8_lossy(&out);
    let body = text.split_once("\r\n\r\n").map_or("", |(_, b)| b);
    serde_json::from_str(body).unwrap_or(Value::Null)
}

/// The gateway among `up` that leads `unit`.
async fn current_leader(
    gw: &BTreeMap<u64, (String, String)>,
    unit: u64,
    up: &[u64],
) -> Option<u64> {
    for &n in up {
        let v = http_get(&gw[&n].1, "/admin/v1/engine/units").await;
        if v["units"].as_array().is_some_and(|a| {
            a.iter()
                .any(|u| u["unit"].as_u64() == Some(unit) && u["leading"].as_bool() == Some(true))
        }) {
            return Some(n);
        }
    }
    None
}

/// `SPRIG_LIVE_STACK <cmd> gw<n>`.
fn stack(cmd: &str, n: u64) {
    let script = var("SPRIG_LIVE_STACK");
    let ok = std::process::Command::new(&script)
        .args([cmd, &format!("gw{n}")])
        .status()
        .unwrap()
        .success();
    assert!(ok, "{script} {cmd} gw{n}");
}

/// Rows of `doc` whose vector isn't the bundled model's of their body.
async fn wrong(c: &mut Conn) -> i64 {
    c.query_first::<Option<i64>, _>(
        "SELECT COUNT(*) FROM doc WHERE emb IS NULL \
         OR COALESCE(VEC_DISTANCE_COSINE(emb, AI_EMBED(body)), 1) > 0.0001",
    )
    .await
    .ok()
    .flatten()
    .flatten()
    .unwrap_or(-1)
}

async fn settle(c: &mut Conn, secs: u64) -> i64 {
    let until = Instant::now() + Duration::from_secs(secs);
    loop {
        let n = wrong(c).await;
        if n == 0 || Instant::now() >= until {
            return n;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// An embedded column keeps being filled after the node leading its
/// tenant stops: the node elected next fills the rows written after the
/// stop, though no client of the tenant reached it.
#[ignore = "live: needs SPRIG_LIVE_STACK, SPRIG_LIVE_TENANT_ID, SPRIG_LIVE_TENANT, SPRIG_LIVE_USER, SPRIG_LIVE_PASSWORD, SPRIG_LIVE_GW1..3_MYSQL/_HTTP; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn embedded_column_keeps_filling_after_its_leader_stops() {
    let gw = gateways();
    let all = [1u64, 2, 3];
    let mut c = conn(&gw[&1].0, None).await;
    let t: u64 = var("SPRIG_LIVE_TENANT_ID").parse().unwrap();
    must(
        &mut c,
        "ALTER TENANT SPRIG SET (ai.functions.enabled = TRUE)",
    )
    .await;
    must(&mut c, "DROP DATABASE IF EXISTS aiemb_multi").await;
    must(&mut c, "CREATE DATABASE aiemb_multi").await;
    must(&mut c, "USE aiemb_multi").await;
    must(
        &mut c,
        "CREATE TABLE doc (id INT PRIMARY KEY, body TEXT, \
         emb VECTOR(384) GENERATED BY EMBEDDING(body) USING MODEL 'local:bge-small')",
    )
    .await;
    let vals: Vec<String> = (1..=20)
        .map(|i| format!("({i}, 'a row written before the stop, number {i}')"))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO doc (id, body) VALUES {}", vals.join(", ")),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = settle(&mut c, 120).await;
    assert_eq!(before, 0, "the first 20 rows weren't filled");
    drop(c);
    let a = current_leader(&gw, t, &all).await.expect("a leader");
    stack("stop", a);
    let rest: Vec<u64> = all.iter().copied().filter(|n| *n != a).collect();
    let t0 = Instant::now();
    let b = loop {
        if let Some(b) = current_leader(&gw, t, &rest).await {
            break b;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "no new leader in 30 s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    // Written through the node that doesn't lead now.
    let other = *rest.iter().find(|n| **n != b).unwrap();
    println!("node {a} stopped; node {b} leads; writing through node {other}");
    let mut c = conn(&gw[&other].0, Some("aiemb_multi")).await;
    let vals: Vec<String> = (21..=40)
        .map(|i| format!("({i}, 'a row written after the stop, number {i}')"))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO doc (id, body) VALUES {}", vals.join(", ")),
    )
    .await;
    must(
        &mut c,
        "UPDATE doc SET body = CONCAT(body, ', changed after the stop') WHERE id <= 5",
    )
    .await;
    let after = settle(&mut c, 180).await;
    let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM doc").await.unwrap();
    stack("start", a);
    must(&mut c, "DROP DATABASE aiemb_multi").await;
    assert_eq!(n, Some(40));
    assert_eq!(
        after, 0,
        "node {b} (leading, no client of the tenant) didn't fill the rows written after node {a} stopped"
    );
}
