//! `ALTER TRIGGER ... DISABLE | ENABLE` of an after-commit trigger, through
//! a live deployment: rows inserted while the trigger is disabled don't
//! run; `ENABLE` resumes from where it stopped, so they run then, each
//! once, and rows inserted after it run as before. A repeat says it was
//! already in that state; `logic_log` has a line per change.
//!
//! Needs a gateway with a worker, a tenant user who may create databases
//! and routines, and the deployment's MariaDB as root (to read the
//! catalog):
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! export SPRIG_TEST_LOGIC_ROOT=mysql://root:...@127.0.0.1:43301 SPRIG_TEST_LOGIC_TENANT=<id>
//! cargo test -p sprig-gateway --test trigger_toggle_live -- --nocapture
//! ```
//!
//! Works in the database `trigger_toggle_live`, dropped afterwards.

#![allow(clippy::unwrap_used, clippy::too_many_lines, missing_docs)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "trigger_toggle_live";

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

/// Runs `q`; its warnings (the statement's notes), joined.
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

async fn copies(c: &mut Conn) -> u64 {
    c.query_first(format!("SELECT COUNT(*) FROM {DB}.copies"))
        .await
        .unwrap()
        .unwrap_or(0)
}

/// Waits up to `limit` for `want` copies; the count and the wait.
async fn wait_copies(c: &mut Conn, want: u64, limit: Duration) -> (u64, f64) {
    let t0 = Instant::now();
    loop {
        let n = copies(c).await;
        if n >= want || t0.elapsed() > limit {
            return (n, t0.elapsed().as_secs_f64());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn insert(c: &mut Conn, from: u32, to: u32) {
    let values: Vec<String> = (from..=to).map(|k| format!("({k}, 0)")).collect();
    must(
        c,
        &format!("INSERT INTO {DB}.src (id, n) VALUES {}", values.join(", ")),
    )
    .await;
}

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_ROOT, SPRIG_TEST_LOGIC_TENANT, ...; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_after_commit_trigger_disables_and_resumes_where_it_stopped() {
    let Some((mut c, mut root, t)) =
        connect("an_after_commit_trigger_disables_and_resumes_where_it_stopped").await
    else {
        panic!("live test run without its environment: see tools/live/README.md");
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
    // The key makes a second run of a row fail loudly (1062), so "each
    // once" is checked by the table itself.
    must(
        &mut c,
        "CREATE TABLE copies (id BIGINT PRIMARY KEY, at DATETIME(6) NOT NULL)",
    )
    .await;
    let mut ck = Checks { failed: Vec::new() };
    let notes = must(
        &mut c,
        "CREATE TRIGGER copy AFTER COMMIT ON src FOR INSERT LANGUAGE javascript AS $$
export default function (row) {
  db.exec('INSERT INTO copies (id, at) VALUES (?, NOW(6))', [row.id]);
}
$$",
    )
    .await;
    let module = module_of(&notes);
    tokio::time::sleep(Duration::from_secs(3)).await;

    // 1. Enabled: rows 1 to 10 run.
    insert(&mut c, 1, 10).await;
    let (n, secs) = wait_copies(&mut c, 10, Duration::from_secs(60)).await;
    ck.check(
        "enabled: rows 1 to 10 run",
        n == 10,
        format!("{n} copies after {secs:.1} s"),
    );

    // 2. Disabled: the note, the module's state, nothing runs.
    let note = must(&mut c, "ALTER TRIGGER copy DISABLE").await;
    ck.check(
        "DISABLE answers with its note",
        note.contains("after-commit trigger copy disabled"),
        &note,
    );
    let state: Option<String> = root
        .query_first(format!(
            "SELECT state FROM {cat}.logic_modules WHERE module_id = {module}"
        ))
        .await
        .unwrap();
    ck.check(
        "the module is disabled in the catalog",
        state.as_deref() == Some("disabled"),
        format!("{state:?}"),
    );
    let again = must(&mut c, "ALTER TRIGGER copy DISABLE").await;
    ck.check(
        "a second DISABLE is a note",
        again.contains("was already disabled"),
        &again,
    );
    // The job ends at its next check of the module (5 s).
    tokio::time::sleep(Duration::from_secs(7)).await;
    insert(&mut c, 11, 20).await;
    tokio::time::sleep(Duration::from_secs(15)).await;
    let n = copies(&mut c).await;
    ck.check(
        "disabled: rows 11 to 20 don't run (15 s)",
        n == 10,
        format!("{n} copies"),
    );

    // 3. Enabled again: rows 11 to 20 run from the kept position, then 21
    // to 30 as usual.
    let t_enable = Instant::now();
    let note = must(&mut c, "ALTER TRIGGER copy ENABLE").await;
    ck.check(
        "ENABLE answers with its note",
        note.contains("after-commit trigger copy enabled"),
        &note,
    );
    let (n, _) = wait_copies(&mut c, 20, Duration::from_secs(60)).await;
    let secs = t_enable.elapsed().as_secs_f64();
    ck.check(
        "enabled again: rows 11 to 20 run once, from where it stopped",
        n == 20,
        format!("{n} copies, {secs:.1} s after ENABLE"),
    );
    insert(&mut c, 21, 30).await;
    let (n, secs) = wait_copies(&mut c, 30, Duration::from_secs(60)).await;
    ck.check(
        "rows after ENABLE run as before",
        n == 30,
        format!("{n} copies after {secs:.1} s"),
    );
    let missing: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM {DB}.src s LEFT JOIN {DB}.copies k ON k.id = s.id WHERE k.id IS NULL"
        ))
        .await
        .unwrap();
    ck.check(
        "every row has its copy",
        missing == Some(0),
        format!("{missing:?} missing"),
    );
    let dead: Option<u64> = root
        .query_first(format!(
            "SELECT COUNT(*) FROM {cat}.logic_dead_letters d JOIN {cat}.logic_bindings b \
             ON b.binding_id = d.binding_id WHERE b.module_id = {module}"
        ))
        .await
        .unwrap();
    ck.check(
        "no dead letter (a second run would fail on the key)",
        dead == Some(0),
        format!("{dead:?}"),
    );
    let lines: Vec<String> = root
        .query(format!(
            "SELECT message FROM {cat}.logic_log WHERE module_id = {module} AND source = 'deploy' \
             AND message LIKE '%abled by %' \
             ORDER BY log_id"
        ))
        .await
        .unwrap();
    ck.check(
        "logic_log has a line per change",
        lines.len() == 2
            && lines[0].starts_with("disabled by")
            && lines[1].starts_with("enabled by"),
        format!("{lines:?}"),
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
