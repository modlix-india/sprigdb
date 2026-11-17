//! A retrieval index's chunk tables move with their table when the
//! sharding advisor's first design moves the table into its table group
//! (one `RENAME TABLE` of the table and its chunk tables, under the
//! regroup's hold). Before, the chunk tables stayed in the old schema: the
//! worker filled the group's schema again (`rebuilding`, every row embedded
//! again) and dropped the old table a minute later.
//!
//! The test makes a table of 24 rows with a retrieval index, waits for it
//! to be `ready`, pins the table's shard key and applies the advisor's
//! design. Right after the apply the chunk table must be in the group's
//! schema with every chunk, none left in the old schema; the index must
//! stay `ready` with the same rows indexed (never filled again), its
//! chunks must get their rows' routing hashes, and searches must find the
//! same rows as before the move.
//!
//! ```sh
//! export SPRIG_TEST_RGX_SQL=127.0.0.1:48306           # a gateway's MySQL port
//! export SPRIG_TEST_RGX_TENANT=sf13rx                 # a tenant of its own
//! export SPRIG_TEST_RGX_TENANT_ID=48306000003         # its id
//! export SPRIG_TEST_RGX_BACKEND=mysql://root:...@127.0.0.1:48311  # its cluster
//! export SPRIG_TEST_RGX_CTL="/path/to/sprigctl --endpoint http://127.0.0.1:48480"
//! cargo test -p sprig-gateway --test regroup_retrieval_live -- --nocapture
//! ```
//!
//! The deployment's worker needs the local embedding model
//! (`docs.model_dir`).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "rgx_live";

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

fn var(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

/// Each note's topic and text: retrieval finds a note by its own words.
const NOTES: [(&str, &str); 24] = [
    (
        "Passwords",
        "To reset your password open settings, choose security and press reset.",
    ),
    (
        "Invoices",
        "Invoices are emailed on the first day of each month as PDF files.",
    ),
    (
        "Refunds",
        "A refund reaches your card within five to ten business days.",
    ),
    (
        "Shipping",
        "Orders ship from our warehouse within two days by courier.",
    ),
    (
        "Returns",
        "Return an unused item within thirty days for a full refund.",
    ),
    (
        "Accounts",
        "Close your account from the profile page; data is deleted after a week.",
    ),
    (
        "Two factor",
        "Turn on two factor login with an authenticator app for more safety.",
    ),
    (
        "Teams",
        "Invite colleagues to a team from the members tab with their email.",
    ),
    (
        "Exports",
        "Export your data as CSV from the reports menu at any time.",
    ),
    (
        "Pricing",
        "The basic plan is free; the pro plan costs twelve dollars a month.",
    ),
    (
        "Trials",
        "A free trial lasts fourteen days and needs no credit card.",
    ),
    (
        "Mobile",
        "The mobile app works offline and syncs when you reconnect.",
    ),
    (
        "Dark mode",
        "Switch to dark mode under appearance in the display settings.",
    ),
    (
        "Languages",
        "The interface is available in English, German, French and Hindi.",
    ),
    (
        "Webhooks",
        "Webhooks post a JSON body to your URL for every new order.",
    ),
    (
        "API keys",
        "Create API keys in the developer console; each key has scopes.",
    ),
    (
        "Limits",
        "The API allows one hundred requests a minute per key.",
    ),
    (
        "Backups",
        "We back up every database each night and keep copies for a month.",
    ),
    (
        "Uptime",
        "Our status page shows uptime and any incident as it happens.",
    ),
    (
        "Support",
        "Support answers chat messages from nine to six on weekdays.",
    ),
    (
        "Gift cards",
        "Gift cards never expire and can pay for any plan.",
    ),
    ("Coupons", "Enter a coupon code at checkout before you pay."),
    (
        "Taxes",
        "Prices include sales tax where the law requires it.",
    ),
    (
        "Privacy",
        "We never sell personal data and you can download all of it.",
    ),
];

async fn login() -> mysql_async::Conn {
    let addr = var("SPRIG_TEST_RGX_SQL", "127.0.0.1:48306");
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!(
            "{}.app",
            var("SPRIG_TEST_RGX_TENANT", "sf13rx")
        )))
        .pass(Some(var("SPRIG_TEST_RGX_PASSWORD", "sprig-dev-app")))
        .prefer_socket(false)
        .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
    mysql_async::Conn::new(opts).await.unwrap()
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

/// The index's state and rows indexed.
async fn state(c: &mut mysql_async::Conn, index: &str) -> (String, u64) {
    let rows: Vec<mysql_async::Row> = c
        .query(format!("SHOW RETRIEVAL INDEXES FROM {DB}"))
        .await
        .unwrap();
    for r in rows {
        let name: String = r.get(0).unwrap();
        if name == index {
            return (r.get(4).unwrap(), r.get(5).unwrap());
        }
    }
    ("missing".into(), 0)
}

/// The tenant's chunk tables of index `id`: (schema, chunks).
async fn chunk_tables(b: &mut mysql_async::Conn, tenant: u64, id: u64) -> Vec<(String, u64)> {
    let schemas: Vec<String> = b
        .exec(
            "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = ? \
             AND TABLE_SCHEMA LIKE ? ORDER BY TABLE_SCHEMA",
            (format!("__sprig_rx_{id}"), format!("t{tenant}\\_d%")),
        )
        .await
        .unwrap();
    let mut out = Vec::new();
    for s in schemas {
        let n: u64 = b
            .query_first(format!("SELECT COUNT(*) FROM `{s}`.`__sprig_rx_{id}`"))
            .await
            .unwrap()
            .unwrap_or(0);
        out.push((s, n));
    }
    out
}

/// The schema of the tenant's table `notes`.
async fn table_schema(b: &mut mysql_async::Conn, tenant: u64) -> Option<String> {
    b.exec_first(
        "SELECT TABLE_SCHEMA FROM information_schema.TABLES WHERE TABLE_NAME = 'notes' \
         AND TABLE_SCHEMA LIKE ?",
        (format!("t{tenant}\\_d%"),),
    )
    .await
    .unwrap()
}

/// The best hit's row id for each note's own words.
async fn top_hits(c: &mut mysql_async::Conn) -> Vec<i64> {
    let mut out = Vec::new();
    for (_, text) in NOTES {
        let q = text.replace('\'', "''");
        let r: Vec<i64> = c
            .query(format!(
                "SELECT id FROM RETRIEVE(rgx_kb, '{q}', 1) ORDER BY `rank`"
            ))
            .await
            .unwrap_or_default();
        out.push(r.first().copied().unwrap_or(-1));
    }
    out
}

fn ctl(args: &[&str]) -> Result<serde_json::Value, String> {
    let cmd = std::env::var("SPRIG_TEST_RGX_CTL").map_err(|_| "SPRIG_TEST_RGX_CTL".to_owned())?;
    let mut parts = cmd.split_whitespace();
    let bin = parts.next().ok_or("no sprigctl")?;
    let out = std::process::Command::new(bin)
        .args(parts)
        .args(["-o", "json"])
        .args(args)
        .env("SPRIGCTL_ACTOR", "regroup_retrieval_live")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn regroup_moves_chunk_tables_with_their_table() {
    let (Ok(url), Ok(tenant_id), Ok(_)) = (
        std::env::var("SPRIG_TEST_RGX_BACKEND"),
        std::env::var("SPRIG_TEST_RGX_TENANT_ID"),
        std::env::var("SPRIG_TEST_RGX_CTL"),
    ) else {
        eprintln!(
            "skipped: set SPRIG_TEST_RGX_SQL, SPRIG_TEST_RGX_TENANT(_ID), SPRIG_TEST_RGX_BACKEND \
             and SPRIG_TEST_RGX_CTL"
        );
        return;
    };
    let tenant_id: u64 = tenant_id.parse().unwrap();
    let tenant = var("SPRIG_TEST_RGX_TENANT", "sf13rx");
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    let mut b = mysql_async::Conn::new(mysql_async::Opts::from_url(&url).unwrap())
        .await
        .unwrap();
    let mut c = login().await;
    for s in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE notes (id BIGINT PRIMARY KEY, title VARCHAR(100), body TEXT)".to_owned(),
    ] {
        must(&mut c, &s).await;
    }
    let values: Vec<String> = NOTES
        .iter()
        .enumerate()
        .map(|(i, (t, x))| format!("({}, '{t}', '{}')", i + 1, x.replace('\'', "''")))
        .collect();
    must(
        &mut c,
        &format!("INSERT INTO notes VALUES {}", values.join(", ")),
    )
    .await;
    must(
        &mut c,
        "CREATE RETRIEVAL INDEX rgx_kb ON notes (body) TITLE title EMBED WITH MODEL 'local:bge-small'",
    )
    .await;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(240) && state(&mut c, "rgx_kb").await.0 != "ready" {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (st0, rows0) = state(&mut c, "rgx_kb").await;
    ck.ok("the index fills (ready)", st0 == "ready", || st0.clone());
    let id: u64 = b
        .query_first(format!(
            "SELECT index_id FROM `t{tenant_id}_sprig`.ai_retrieval_indexes WHERE name = 'rgx_kb'"
        ))
        .await
        .unwrap()
        .unwrap();
    let before = chunk_tables(&mut b, tenant_id, id).await;
    let from = table_schema(&mut b, tenant_id).await.unwrap_or_default();
    let chunks0: u64 = before.iter().map(|x| x.1).sum();
    eprintln!("     before: table in {from}, chunk tables {before:?}, rows indexed {rows0}");
    let hits0 = top_hits(&mut c).await;

    // The advisor's design: the table moves into its group.
    must(&mut c, "SPRIG PIN notes SHARD KEY (id)").await;
    let run = ctl(&["advisor", "run", &tenant])
        .ok()
        .and_then(|v| v["run"].as_u64());
    ck.ok("the advisor runs", run.is_some(), String::new);
    let applied = run.map(|r| ctl(&["advisor", "apply", &tenant, &r.to_string(), "--yes"]));
    ck.ok(
        "the advisor's design is applied",
        applied.as_ref().is_some_and(Result::is_ok),
        || format!("{applied:?}"),
    );
    // At once after the apply.
    let to = table_schema(&mut b, tenant_id).await.unwrap_or_default();
    let after = chunk_tables(&mut b, tenant_id, id).await;
    let (st1, rows1) = state(&mut c, "rgx_kb").await;
    eprintln!(
        "     after: table in {to}, chunk tables {after:?}, state {st1}, rows indexed {rows1}"
    );
    ck.ok(
        "the table moved to another schema",
        !to.is_empty() && to != from,
        || format!("{from} -> {to}"),
    );
    ck.ok(
        "right after the apply the chunk table is in the table's new schema with every chunk",
        after == [(to.clone(), chunks0)],
        || format!("{after:?}, {chunks0} chunks before"),
    );
    ck.ok(
        "no chunk table is left in the old schema",
        !after.iter().any(|(s, _)| *s == from),
        || format!("{after:?}"),
    );
    ck.ok("the index stays ready", st1 == "ready", || st1.clone());
    let hits1 = top_hits(&mut c).await;
    ck.ok(
        "searches right after the move find the same rows",
        hits1 == hits0,
        || format!("{hits0:?} -> {hits1:?}"),
    );
    // A while later: nothing embedded again, the chunks carry their rows'
    // routing hashes, and the state never left ready.
    let mut wrong = u64::MAX;
    let mut states = Vec::new();
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(40) {
        states.push(state(&mut c, "rgx_kb").await.0);
        wrong = b
            .query_first(format!(
                "SELECT COUNT(*) FROM `{to}`.`__sprig_rx_{id}` x JOIN `{to}`.notes n \
                 ON n.id = x.k_id WHERE NOT (x._sprig_rk <=> n._sprig_rk)"
            ))
            .await
            .ok()
            .flatten()
            .unwrap_or(u64::MAX);
        if wrong == 0 && t.elapsed() > Duration::from_secs(20) {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let (st2, rows2) = state(&mut c, "rgx_kb").await;
    ck.ok(
        "rows indexed unchanged (the chunks moved with the table)",
        rows2 == rows0,
        || format!("{rows0} -> {rows2}"),
    );
    ck.ok(
        "the state never left ready",
        st2 == "ready" && states.iter().all(|s| s == "ready"),
        || format!("{states:?}"),
    );
    ck.ok(
        "every chunk carries its row's routing hash",
        wrong == 0,
        || format!("{wrong} chunks with another hash"),
    );
    let after2 = chunk_tables(&mut b, tenant_id, id).await;
    ck.ok(
        "still one chunk table, in the group's schema",
        after2 == [(to.clone(), chunks0)],
        || format!("{after2:?}"),
    );

    let _ = c.query_drop("DROP RETRIEVAL INDEX rgx_kb").await;
    let _ = c.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await;
    println!(
        "regroup with a retrieval index: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
