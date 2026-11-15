//! Live: a graph's asynchronous mirrors reach every relationship after the
//! `graph_mirror` job moves to another worker in the middle of a backfill.
//! Against a private deployment with two workers.
//!
//! ```text
//! SPRIG_GMM_HTTP=127.0.0.1:30080          # the gateway's HTTP listener
//! SPRIG_GMM_TENANT=ldbc                   # a tenant with user app
//! SPRIG_GMM_PASSWORD=ldbc-app-pw          # app's password
//! SPRIG_GMM_DB=graph                      # an existing database
//! SPRIG_GMM_SPRIGCTL=<sprigctl>           # with SPRIGCTL_ENDPOINT
//! SPRIG_GMM_ETCD=http://127.0.0.1:30379   # the DC's etcd (the job's owner)
//! SPRIG_GMM_DC=dc-g
//! SPRIG_GMM_WORKERS='g-wkr-1=<pidfile>,g-wkr-2=<pidfile>'
//! SPRIG_GMM_RESTART='<command starting worker $WORKER again>'  # optional
//! cargo test -p sprig-gateway --test graph_mirror_move_live -- --ignored --nocapture
//! ```
//!
//! The test creates graph `<db>.gmm` over four shards
//! (`sharding.min_shards`), waits for its mirrors to be `async/ready`, then
//! `SPRIG_GMM_WRITERS` (4) writers create `SPRIG_GMM_EDGES` (60,000)
//! relationships in batches of `SPRIG_GMM_BATCH` (1,000), one per
//! statement, as a bulk load does. Once a third are written it `kill -9`s
//! the worker that runs the tenant's `graph_mirror` job, while the
//! applier is behind. Checks:
//!
//! - the job runs again on the other worker;
//! - within `SPRIG_GMM_CATCHUP_S` (300) seconds after the last write, every
//!   relationship has exactly one mirror (`sprigctl graph show`);
//! - an incoming-hop count over every relationship (read through the
//!   mirrors once they are fresh) finds them all.
//!
//! The killed worker is started again at the end (`SPRIG_GMM_RESTART`) and
//! the graph dropped.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const GRAPH: &str = "gmm";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn num(k: &str, d: u64) -> u64 {
    var(k).and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// `sprigctl -o json <args>`, `None` on a failure.
fn sprigctl(args: &[&str]) -> Option<serde_json::Value> {
    let out = std::process::Command::new(var("SPRIG_GMM_SPRIGCTL")?)
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

/// The graph's mirror mode and its relationships and mirrors, summed over
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

/// One statement over REST (`sql:execute`) as `tenant.app`.
async fn execute(
    http: &str,
    tenant: &str,
    db: &str,
    language: &str,
    q: &str,
) -> Result<serde_json::Value, String> {
    use base64::Engine as _;
    let body = serde_json::json!({"language": language, "sql": q}).to_string();
    let pw = var("SPRIG_GMM_PASSWORD").unwrap_or_else(|| "sprig-dev-app".into());
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("{tenant}.app:{pw}"));
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
    tokio::time::timeout(Duration::from_secs(120), s.read_to_end(&mut out))
        .await
        .map_err(|_| "no answer within 120 s".to_owned())?
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

/// The first value of the first row of a result.
fn first_value(v: &serde_json::Value) -> Option<u64> {
    let row = &v["results"][0]["rows"][0];
    row.as_array()
        .and_then(|r| r.first())
        .or_else(|| row.as_object().and_then(|o| o.values().next()))
        .and_then(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()))
}

/// The worker that runs the tenant's `graph_mirror` job now (etcd's JSON
/// gateway), with the job's attempt.
async fn mirror_owner(etcd: &str, dc: &str) -> Option<(String, u64)> {
    use base64::Engine as _;
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    let prefix = format!("/sprig/dc/{dc}/jobs/");
    let mut end = prefix.clone().into_bytes();
    *end.last_mut()? += 1;
    let body = serde_json::json!({
        "key": b64(&prefix),
        "range_end": base64::engine::general_purpose::STANDARD.encode(&end),
    })
    .to_string();
    let host = etcd.trim_start_matches("http://");
    let req = format!(
        "POST /v3/kv/range HTTP/1.0\r\nHost: {host}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    let mut s = tokio::net::TcpStream::connect(host).await.ok()?;
    s.write_all(req.as_bytes()).await.ok()?;
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.ok()?;
    let text = String::from_utf8_lossy(&out);
    let (_, json) = text.split_once("\r\n\r\n")?;
    let v: serde_json::Value = serde_json::from_str(json).ok()?;
    for kv in v["kvs"].as_array()? {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(kv["value"].as_str()?)
            .ok()?;
        let Ok(j) = serde_json::from_slice::<serde_json::Value>(&raw) else {
            continue;
        };
        // A record wraps the job (`{v, ...body}` or `{body: ...}`).
        let job = if j.get("kind").is_some() {
            &j
        } else {
            &j["body"]
        };
        if job["kind"] == "graph_mirror" && job["state"] == "running" {
            return Some((
                job["owner"]["worker_id"].as_str()?.to_owned(),
                job["owner"]["attempt"].as_u64().unwrap_or(0),
            ));
        }
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a private deployment with two workers (see the module docs)"]
async fn mirrors_reach_every_edge_after_the_applier_moves() {
    let (Some(http), Some(tenant), Some(db), Some(etcd), Some(workers)) = (
        var("SPRIG_GMM_HTTP"),
        var("SPRIG_GMM_TENANT"),
        var("SPRIG_GMM_DB"),
        var("SPRIG_GMM_ETCD"),
        var("SPRIG_GMM_WORKERS"),
    ) else {
        println!("SPRIG_GMM_* not set: skipped");
        return;
    };
    let dc = var("SPRIG_GMM_DC").unwrap_or_else(|| "dc1".into());
    let edges = num("SPRIG_GMM_EDGES", 60_000);
    let batch = num("SPRIG_GMM_BATCH", 1_000).max(1);
    let writers = num("SPRIG_GMM_WRITERS", 4).max(1);
    let catchup = Duration::from_secs(num("SPRIG_GMM_CATCHUP_S", 300));
    let pidfiles: Vec<(String, String)> = workers
        .split(',')
        .filter_map(|w| w.split_once('='))
        .map(|(n, p)| (n.to_owned(), p.to_owned()))
        .collect();
    let graph = format!("{db}.{GRAPH}");

    // 1. The graph, over four shards, and its mirrors async.
    let _ = sprigctl(&[
        "settings",
        "set",
        "sharding.min_shards",
        "4",
        "--scope",
        &format!("db:{tenant}.{db}"),
        "--reason",
        "graph_mirror_move_live",
        "--yes",
    ]);
    let _ = execute(
        &http,
        &tenant,
        &db,
        "sql",
        &format!("DROP GRAPH IF EXISTS {graph}"),
    )
    .await;
    execute(&http, &tenant, &db, "sql", &format!("CREATE GRAPH {graph}"))
        .await
        .expect("create the graph");
    let t0 = Instant::now();
    loop {
        let _ = execute(
            &http,
            &tenant,
            &graph,
            "cypher",
            "MATCH (n) RETURN count(n)",
        )
        .await;
        let m = mirror_view(&tenant, &graph);
        if m.as_ref().is_some_and(|m| m.0 == "async/ready") {
            println!("mirrors async after {:.1} s", t0.elapsed().as_secs_f64());
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(120),
            "the graph's mirrors never became async: {m:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // 2. Writers, one batch a statement; the kill at a third.
    let written = Arc::new(AtomicU64::new(0));
    let next = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..writers {
        let (http, tenant, graph) = (http.clone(), tenant.clone(), graph.clone());
        let (written, next) = (Arc::clone(&written), Arc::clone(&next));
        tasks.push(tokio::spawn(async move {
            loop {
                let a = next.fetch_add(batch, Ordering::SeqCst);
                if a >= edges {
                    return;
                }
                let b = (a + batch).min(edges) - 1;
                let q = format!(
                    "UNWIND range({a}, {b}) AS i CREATE (:GmmA {{i: i}})-[:GMM {{i: i}}]->(:GmmB {{i: i}})"
                );
                let mut tries = 0;
                loop {
                    tries += 1;
                    match execute(&http, &tenant, &graph, "cypher", &q).await {
                        Ok(_) => break,
                        // Created or not: the count at the end tells.
                        Err(e) if tries < 3 && e.contains("1213") => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        Err(e) => panic!("batch {a}..={b}: {e}"),
                    }
                }
                written.fetch_add(b - a + 1, Ordering::SeqCst);
            }
        }));
    }
    let (killed, before) = loop {
        if written.load(Ordering::SeqCst) >= edges / 3 {
            let owner = mirror_owner(&etcd, &dc)
                .await
                .expect("no running graph_mirror job");
            let (_, pidfile) = pidfiles
                .iter()
                .find(|(n, _)| *n == owner.0)
                .unwrap_or_else(|| panic!("no pid file for {}", owner.0));
            let pid = std::fs::read_to_string(pidfile).expect("pid file");
            let view = mirror_view(&tenant, &graph);
            println!(
                "{} written; kill -9 {} (pid {}), mirrors then {view:?}",
                written.load(Ordering::SeqCst),
                owner.0,
                pid.trim()
            );
            let st = std::process::Command::new("kill")
                .args(["-9", pid.trim()])
                .status()
                .expect("kill");
            assert!(st.success(), "kill -9 {}", pid.trim());
            break (owner.0, view);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    for t in tasks {
        t.await.expect("writer");
    }
    let wrote_at = Instant::now();
    println!("{} written", written.load(Ordering::SeqCst));

    // 3. The job runs again elsewhere; the mirrors reach every edge.
    let mut moved = None;
    let mut last = None;
    loop {
        if moved.is_none()
            && let Some(o) = mirror_owner(&etcd, &dc).await
            && o.0 != killed
        {
            println!(
                "graph_mirror runs on {} (attempt {}) {:.1} s after the last write",
                o.0,
                o.1,
                wrote_at.elapsed().as_secs_f64()
            );
            moved = Some(o);
        }
        let v = mirror_view(&tenant, &graph);
        if let Some((_, rels, mirrors)) = &v
            && *rels == edges
            && *mirrors == edges
            && moved.is_some()
        {
            println!(
                "mirrors {mirrors} of {rels} relationships {:.1} s after the last write",
                wrote_at.elapsed().as_secs_f64()
            );
            break;
        }
        if v != last {
            println!("{:.1} s: {v:?}", wrote_at.elapsed().as_secs_f64());
            last = v.clone();
        }
        assert!(
            wrote_at.elapsed() < catchup,
            "after {catchup:?}: {v:?}, the job on {moved:?} (killed {killed}, mirrors {before:?})"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let n = execute(
        &http,
        &tenant,
        &graph,
        "cypher",
        "MATCH (b:GmmB)<-[:GMM]-(a) RETURN count(a)",
    )
    .await
    .expect("incoming count");
    assert_eq!(first_value(&n), Some(edges), "incoming hops: {n}");

    // 4. The killed worker again, and the graph dropped.
    if let Some(cmd) = var("SPRIG_GMM_RESTART") {
        let st = std::process::Command::new("sh")
            .args(["-c", &cmd])
            .env("WORKER", &killed)
            .status();
        println!("restarted {killed}: {st:?}");
    }
    let _ = execute(&http, &tenant, &db, "sql", &format!("DROP GRAPH {graph}")).await;
}
