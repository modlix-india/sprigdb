//! The worker's automatic rollback of an after-commit trigger's candidate,
//! through a live deployment: version 1 copies each new row; version 2,
//! rolled out to half the rows by key, fails every run. Once the
//! candidate has run (and failed) at least 20 times, the worker that
//! counts its runs ends the rollout as `ROLLOUT ABORT` would and says why
//! in `logic_log`; the failed rows' retries then run version 1, so every
//! row is copied exactly once, by version 1.
//!
//! Needs a gateway with a worker, a tenant user who may create databases
//! and routines, and the deployment's MariaDB as root (to read the
//! catalog):
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! export SPRIG_TEST_LOGIC_ROOT=mysql://root:...@127.0.0.1:43301 SPRIG_TEST_LOGIC_TENANT=<id>
//! cargo test -p sprig-gateway --test logic_trigger_rollback_live -- --nocapture
//! ```
//!
//! Works in the database `trigger_rollback_live`, dropped afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_logic::rollout::bucket;
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "trigger_rollback_live";
/// Rows inserted: about half on the candidate, well over the 20 runs its
/// numbers need.
const ROWS: u32 = 80;

async fn connect(test: &str) -> Option<(Conn, Conn, String)> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let (Some(root), Some(t)) = (
        skip::var("SPRIG_TEST_LOGIC_ROOT"),
        skip::var("SPRIG_TEST_LOGIC_TENANT"),
    ) else {
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
    Some((
        Conn::new(opts).await.unwrap(),
        Conn::from_url(root.as_str()).await.unwrap(),
        t,
    ))
}

async fn must(c: &mut Conn, q: &str) -> String {
    c.query_drop(q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
    let w: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap();
    w.into_iter()
        .map(|(_, _, m)| m)
        .collect::<Vec<_>>()
        .join("; ")
}

/// The module id in a deploy's notes (`module M version V`).
fn module_of(notes: &str) -> u64 {
    let i = notes.find("module ").unwrap() + 7;
    notes[i..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_after_commit_candidate_rolls_back_by_itself() {
    let Some((mut c, mut root, t)) =
        connect("a_failing_after_commit_candidate_rolls_back_by_itself").await
    else {
        return;
    };
    let cat = format!("t{t}_sprig");
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    must(
        &mut c,
        "CREATE TABLE src (id BIGINT PRIMARY KEY, n INT NOT NULL)",
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE copies (id BIGINT NOT NULL, v VARCHAR(8) NOT NULL, PRIMARY KEY (id, v))",
    )
    .await;
    let mut ck = Checks { failed: Vec::new() };
    let v1 = must(
        &mut c,
        "CREATE TRIGGER copy AFTER COMMIT ON src FOR INSERT LANGUAGE javascript AS $$
export default function (row) {
  db.exec('INSERT INTO copies (id, v) VALUES (?, ?)', [row.id, 'v1']);
}
$$",
    )
    .await;
    let module = module_of(&v1);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let v2 = must(
        &mut c,
        "CREATE OR REPLACE TRIGGER copy AFTER COMMIT ON src FOR INSERT LANGUAGE javascript \
         WITH (SPRIG_ROLLOUT = 50) AS $$
export default function (row) {
  db.exec('INSERT INTO copies (id, v) VALUES (?, ?)', [row.id, 'v2']);
  throw new Error('version 2 is broken: ' + row.id);
}
$$",
    )
    .await;
    ck.check(
        "version 2 is the candidate for half the rows",
        v2.contains("50% of keys"),
        &v2,
    );
    // Workers read the rollout within two seconds.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let on_candidate: Vec<u32> = (1..=ROWS)
        .filter(|k| bucket(&format!("id={k}"), module) < 50)
        .collect();
    println!("{} of {ROWS} rows pick the candidate", on_candidate.len());
    let values: Vec<String> = (1..=ROWS).map(|k| format!("({k}, 0)")).collect();
    let t0 = Instant::now();
    must(
        &mut c,
        &format!("INSERT INTO src (id, n) VALUES {}", values.join(", ")),
    )
    .await;

    // The worker ends the rollout and says why.
    let q = format!(
        "SELECT message FROM {cat}.logic_log WHERE module_id = {module} AND \
         message LIKE '%rolled back by itself%' ORDER BY log_id"
    );
    let deadline = Instant::now() + Duration::from_secs(90);
    let lines: Vec<String> = loop {
        let lines: Vec<String> = root.query(&q).await.unwrap();
        if !lines.is_empty() || Instant::now() > deadline {
            break lines;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let after = t0.elapsed().as_secs_f64();
    ck.check(
        "the worker rolls the failing candidate back and logic_log says why",
        lines.len() == 1
            && lines[0].contains("version 2")
            && lines[0].contains("version 1 runs every row"),
        format!("after {after:.1} s: {lines:?}"),
    );
    let rollout: Option<u64> = root
        .query_first(format!(
            "SELECT candidate_version FROM {cat}.logic_rollouts WHERE module_id = {module}"
        ))
        .await
        .unwrap();
    let active: Option<u64> = root
        .query_first(format!(
            "SELECT active_version FROM {cat}.logic_modules WHERE module_id = {module}"
        ))
        .await
        .unwrap();
    ck.check(
        "no rollout is left and version 1 stays active",
        rollout.is_none() && active == Some(1),
        format!("rollout {rollout:?}, active {active:?}"),
    );
    let stats: Vec<(u64, u64, u64)> = root
        .query(format!(
            "SELECT version, SUM(calls), SUM(errors) FROM {cat}.logic_version_stats \
             WHERE module_id = {module} GROUP BY version ORDER BY version"
        ))
        .await
        .unwrap_or_default();
    println!("logic_version_stats (version, runs, failures): {stats:?}");

    // The failed rows run again with version 1: every row copied once, by
    // version 1; version 2's writes went with its failed runs.
    let deadline = Instant::now() + Duration::from_secs(120);
    let copies: Vec<(u64, String)> = loop {
        let rows: Vec<(u64, String)> = c
            .query("SELECT id, v FROM copies ORDER BY id, v")
            .await
            .unwrap();
        if rows.len() >= ROWS as usize || Instant::now() > deadline {
            break rows;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let all_v1 = copies.len() == ROWS as usize
        && copies
            .iter()
            .enumerate()
            .all(|(i, (id, v))| *id == i as u64 + 1 && v == "v1");
    ck.check(
        "every row is copied once, by version 1, after the rollback",
        all_v1,
        format!(
            "{} copies after {:.1} s, {} by v2",
            copies.len(),
            t0.elapsed().as_secs_f64(),
            copies.iter().filter(|(_, v)| v == "v2").count()
        ),
    );
    let dead: Option<u64> = root
        .query_first(format!(
            "SELECT COUNT(*) FROM {cat}.logic_dead_letters d JOIN {cat}.logic_bindings b \
             USING (binding_id) WHERE b.module_id = {module}"
        ))
        .await
        .unwrap_or(Some(0));
    ck.check(
        "no row was dead-lettered",
        dead == Some(0),
        format!("{dead:?}"),
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
