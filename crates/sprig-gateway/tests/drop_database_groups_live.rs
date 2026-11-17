//! `DROP DATABASE` of a database whose table group lives on another
//! cluster: the group's shard schemas there go with it, the group leaves
//! `sprigctl shard list`, and the advisor groups a new database of the
//! same tables again. Before, the shards stayed (with their rows and
//! `__sprig_dtx`), the map stayed active, nothing was logged, and the next
//! grouping was refused.
//!
//! The test makes a table, pins its shard key, applies the advisor's design
//! (the table moves into its group, shard on the tenant's cluster), moves
//! the shard to the other cluster and splits it, then drops the database.
//!
//! ```sh
//! export SPRIG_TEST_DDG_SQL=127.0.0.1:45306          # a gateway's MySQL port
//! export SPRIG_TEST_DDG_TENANT=acme                  # user app, password sprig-dev-app
//! export SPRIG_TEST_DDG_TENANT_ID=45306000000
//! export SPRIG_TEST_DDG_OTHER=dc-a-c2                # a second cluster
//! export SPRIG_TEST_DDG_OTHER_URL=mysql://root:...@127.0.0.1:45311   # its writer
//! export SPRIG_TEST_DDG_CTL="/path/to/sprigctl --endpoint http://127.0.0.1:45480"
//! cargo test -p sprig-gateway --test drop_database_groups_live -- --nocapture
//! ```
//!
//! Works in the database `ddg_live`, dropped at the end. The deployment
//! should keep moved sources no longer than the test
//! (`mover.source_retain_hours = 0`).

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

const DB: &str = "ddg_live";

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
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

fn ctl(args: &[&str]) -> Result<serde_json::Value, String> {
    let cmd = var("SPRIG_TEST_DDG_CTL").ok_or("SPRIG_TEST_DDG_CTL")?;
    let mut parts = cmd.split_whitespace();
    let bin = parts.next().ok_or("no sprigctl")?;
    let out = std::process::Command::new(bin)
        .args(parts)
        .args(["-o", "json", "--yes"])
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    if !out.status.success() {
        return Err(format!(
            "sprigctl {args:?}: {text}{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    serde_json::from_str(&text).map_err(|e| format!("{e}: {text}"))
}

/// `(group, shard, cluster)` of the tenant's table groups.
fn shards(tenant: &str) -> Vec<(u64, u64, String)> {
    let v = ctl(&["shard", "list", tenant]).unwrap_or_default();
    let mut out = Vec::new();
    for g in v.as_array().into_iter().flatten() {
        let group = g["group"].as_u64().unwrap_or(0);
        for s in g["shards"].as_array().into_iter().flatten() {
            out.push((
                group,
                s["shard"].as_u64().unwrap_or(0),
                s["cluster"].as_str().unwrap_or_default().to_owned(),
            ));
        }
    }
    out
}

/// Waits for mover job `id` to end; its last state.
async fn wait_job(id: &str) -> String {
    let t0 = Instant::now();
    loop {
        let st = ctl(&["job", "show", id])
            .ok()
            .and_then(|v| v["state"].as_str().map(str::to_owned))
            .unwrap_or_default();
        if matches!(
            st.as_str(),
            "done" | "rolled_back" | "failed" | "cancelled" | "paused"
        ) || t0.elapsed() > Duration::from_secs(600)
        {
            return st;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn login(addr: &str, tenant: &str) -> Conn {
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{tenant}.app")))
        .pass(Some("sprig-dev-app"))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Conn::new(opts).await.unwrap()
}

async fn must(c: &mut Conn, q: &str) {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// The tenant's shard schemas of group `g` on the other cluster.
async fn schemas_there(other: &mut Conn, tenant_id: &str, g: u64) -> Vec<String> {
    other
        .query(format!(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name LIKE 't{tenant_id}\\_d%\\_g{g}\\_s%' ORDER BY 1"
        ))
        .await
        .unwrap()
}

/// Makes the database and table, and applies the advisor's design: the
/// table's group, or the error.
async fn grouped(c: &mut Conn, tenant: &str) -> Result<u64, String> {
    must(c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(c, &format!("CREATE DATABASE {DB}")).await;
    must(c, &format!("USE {DB}")).await;
    must(
        c,
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, v VARCHAR(20) NOT NULL)",
    )
    .await;
    let rows: Vec<String> = (1..=200).map(|i| format!("({i}, 'v{i}')")).collect();
    must(
        c,
        &format!("INSERT INTO orders (id, v) VALUES {}", rows.join(", ")),
    )
    .await;
    must(c, "SPRIG PIN orders SHARD KEY (id)").await;
    let before: Vec<u64> = shards(tenant).iter().map(|s| s.0).collect();
    let run = ctl(&["advisor", "run", tenant])?;
    let run = run["run"].as_u64().ok_or(format!("no run: {run}"))?;
    ctl(&["advisor", "apply", tenant, &run.to_string()])?;
    let t0 = Instant::now();
    loop {
        if let Some(g) = shards(tenant)
            .iter()
            .map(|s| s.0)
            .find(|g| !before.contains(g))
        {
            return Ok(g);
        }
        if t0.elapsed() > Duration::from_secs(60) {
            return Err("no new table group after the apply".into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_database_takes_its_remote_table_group() {
    let (Some(addr), Some(tenant), Some(tid), Some(other), Some(other_url), Some(_)) = (
        var("SPRIG_TEST_DDG_SQL"),
        var("SPRIG_TEST_DDG_TENANT"),
        var("SPRIG_TEST_DDG_TENANT_ID"),
        var("SPRIG_TEST_DDG_OTHER"),
        var("SPRIG_TEST_DDG_OTHER_URL"),
        var("SPRIG_TEST_DDG_CTL"),
    ) else {
        eprintln!("skipped: set the SPRIG_TEST_DDG_* variables");
        return;
    };
    let mut ck = Checks { failed: Vec::new() };
    let mut c = login(&addr, &tenant).await;
    let mut o = Conn::from_url(other_url.as_str()).await.unwrap();

    // 1. The table's group, its shard moved to the other cluster and split.
    let g = grouped(&mut c, &tenant).await;
    ck.check("the advisor groups the table", g.is_ok(), format!("{g:?}"));
    let g = g.unwrap();
    let (_, s, _) = shards(&tenant).into_iter().find(|x| x.0 == g).unwrap();
    let mv = ctl(&[
        "shard",
        "move",
        &tenant,
        &g.to_string(),
        &s.to_string(),
        "--to",
        &other,
    ]);
    let st = match mv.as_ref().ok().and_then(|v| v["id"].as_str()) {
        Some(id) => wait_job(id).await,
        None => format!("{mv:?}"),
    };
    ck.check("the shard moves to the other cluster", st == "done", &st);
    let (_, s, _) = shards(&tenant).into_iter().find(|x| x.0 == g).unwrap();
    let sp = ctl(&["shard", "split", &tenant, &g.to_string(), &s.to_string()]);
    let st = match sp.as_ref().ok().and_then(|v| v["id"].as_str()) {
        Some(id) => wait_job(id).await,
        None => format!("{sp:?}"),
    };
    ck.check("the shard splits", st == "done", &st);
    let on: Vec<(u64, u64, String)> = shards(&tenant).into_iter().filter(|x| x.0 == g).collect();
    ck.check(
        "the group's shards are all on the other cluster",
        on.len() >= 2 && on.iter().all(|x| x.2 == other),
        format!("{on:?}"),
    );
    // Wait for the moved and split sources to go (retention 0).
    let want: Vec<String> = on
        .iter()
        .map(|x| format!("_g{g}_s{}", x.1))
        .collect::<Vec<_>>();
    let t0 = Instant::now();
    let mut there = schemas_there(&mut o, &tid, g).await;
    while there.len() != want.len() && t0.elapsed() < Duration::from_secs(120) {
        tokio::time::sleep(Duration::from_secs(2)).await;
        there = schemas_there(&mut o, &tid, g).await;
    }
    ck.check(
        "its shard schemas are on the other cluster",
        there.len() == want.len() && want.iter().all(|w| there.iter().any(|s| s.ends_with(w))),
        format!("{there:?}"),
    );
    let n: Option<u64> = c.query_first("SELECT COUNT(*) FROM orders").await.unwrap();
    ck.check(
        "the rows read through the group",
        n == Some(200),
        format!("{n:?}"),
    );

    // 2. DROP DATABASE takes them.
    let r = c.query_drop(format!("DROP DATABASE {DB}")).await;
    let warnings: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap_or_default();
    ck.check(
        "DROP DATABASE answers OK without warnings",
        r.is_ok() && warnings.is_empty(),
        format!("{r:?} {warnings:?}"),
    );
    let there = schemas_there(&mut o, &tid, g).await;
    ck.check(
        "no shard schema of the group is left on the other cluster",
        there.is_empty(),
        format!("{there:?}"),
    );
    let left: Vec<(u64, u64, String)> = shards(&tenant).into_iter().filter(|x| x.0 == g).collect();
    ck.check(
        "the group is gone from shard list",
        left.is_empty(),
        format!("{left:?}"),
    );

    // 3. The same tables group again.
    let g2 = grouped(&mut c, &tenant).await;
    ck.check(
        "a new database of the same table is grouped again",
        g2.as_ref().is_ok_and(|n| *n != g),
        format!("{g2:?}"),
    );
    let _ = c.query_drop(format!("DROP DATABASE {DB}")).await;
    if let Ok(g2) = g2 {
        let left: Vec<(u64, u64, String)> =
            shards(&tenant).into_iter().filter(|x| x.0 == g2).collect();
        ck.check(
            "the second group goes with its database too (same cluster)",
            left.is_empty(),
            format!("{left:?}"),
        );
    }
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
