//! The wall budget of logic code through a live gateway.
//!
//! - **Nested calls:** a caller compiled into MariaDB and its engine-tier
//!   twin each call a compiled callee that sleeps, under
//!   `logic.function_budget_ms = 1000` for the database. In both tiers the
//!   caller's budget covers its nested calls: calls that fit run, calls
//!   that don't are stopped near the budget.
//! - **Many short statements:** compiled JavaScript and Lua procedures
//!   making 90,000 quick queries under `logic.function_budget_ms = 300`
//!   stop near 300 ms with 39864. MariaDB's own timer never starts for
//!   them (the wrapper sets the budget inside a procedure), so they used to
//!   run to the end; the entry procedure now keeps a deadline.
//! - **Validations and after-commit triggers:** a validation's loop stops
//!   the client's `INSERT` near the budget; an after-commit trigger whose
//!   `catch` sees MariaDB's own timeout no longer runs on past it.
//! - **Schedules** run their procedure under `logic.job_budget_ms`, not the
//!   procedure's `logic.function_budget_ms` (about a minute: the schedule
//!   fires every minute).
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:46306
//! export SPRIG_TEST_LOGIC_USER=sf7acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! # optional, for the triggers' and schedules' outcomes in the catalog:
//! export SPRIG_TEST_LOGIC_ROOT=mysql://root:...@127.0.0.1:46301 SPRIG_TEST_LOGIC_TENANT=<id>
//! cargo test -p sprig-gateway --test logic_call_budget_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the databases `sf7_budget`, `sf14_budget` and `sf14_bound`,
//! dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "sf7_budget";

async fn connect(test: &str) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
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
    Some(Conn::new(opts).await.unwrap())
}

/// The deployment's own MariaDB as root, and the tenant's id, to read the
/// catalog (`logic_log`, `logic_schedules`).
async fn catalog() -> Option<(Conn, String)> {
    let root = skip::var("SPRIG_TEST_LOGIC_ROOT")?;
    let t = skip::var("SPRIG_TEST_LOGIC_TENANT")?;
    Some((Conn::from_url(root.as_str()).await.unwrap(), t))
}

async fn run(c: &mut Conn, q: &str) -> Result<(), (u16, String)> {
    match c.query_drop(q).await {
        Ok(()) => Ok(()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) {
    run(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// Deploys `q`; the deploy's notes (which name the tier).
async fn deploy(c: &mut Conn, q: &str) -> String {
    must(c, q).await;
    let w: Vec<(String, u32, String)> = c.query("SHOW WARNINGS").await.unwrap();
    w.into_iter()
        .map(|(_, _, m)| m)
        .collect::<Vec<_>>()
        .join("; ")
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

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD, ...; tools/live/README.md"]
#[tokio::test]
async fn nested_calls_run_inside_the_callers_wall_budget() {
    let Some(mut c) = connect("nested_calls_run_inside_the_callers_wall_budget").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    must(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    // Read at deploy by the compiled tier, at each call by the engine tier.
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (logic.function_budget_ms = 1000)"),
    )
    .await;
    must(&mut c, "CREATE TABLE one (id INT PRIMARY KEY)").await;
    must(&mut c, "INSERT INTO one VALUES (1)").await;
    // `SLEEP` in the condition, so the column is typed and the callee
    // compiles.
    let n = deploy(
        &mut c,
        "CREATE FUNCTION nap(ms INT) LANGUAGE javascript AS $$
           const r = db.sqlOne('SELECT id FROM one WHERE id = 1 AND SLEEP(?) = 0', [ms / 1000]);
           return { s: r === null ? 1 : 0 };
         $$",
    )
    .await;
    let mut ck = Checks { failed: Vec::new() };
    ck.check("the callee compiles", n.contains("tier mariadb"), &n);
    let body = "let t = 0;
       for (let i = 0; i < n; i++) { db.call('nap', 300); t = t + 1; }
       return { t };";
    let n = deploy(
        &mut c,
        &format!("CREATE PROCEDURE naps_c(n INT) LANGUAGE javascript AS $${body}$$"),
    )
    .await;
    ck.check("the caller compiles", n.contains("tier mariadb"), &n);
    must(
        &mut c,
        &format!(
            "CREATE PROCEDURE naps_e(n INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $${body}$$"
        ),
    )
    .await;
    for tier in ["c", "e"] {
        for (n, fits) in [(2, true), (6, false)] {
            let t0 = Instant::now();
            let r = run(&mut c, &format!("CALL naps_{tier}({n})")).await;
            let ms = t0.elapsed().as_millis();
            let ok = if fits {
                r.is_ok()
            } else {
                // Stopped near the budget (one nested statement may finish
                // past it in the engine tier), never after all 1.8 s.
                matches!(&r, Err((1969 | 39864, _))) && ms < 1700
            };
            ck.check(&format!("naps_{tier}({n})"), ok, format!("{ms} ms, {r:?}"));
        }
    }
    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD, ...; tools/live/README.md"]
#[tokio::test]
async fn many_short_statements_stop_at_the_budget() {
    let Some(mut c) = connect("many_short_statements_stop_at_the_budget").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = "sf14_budget";
    must(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    // Before the deploys: compiled code reads it at deploy.
    must(
        &mut c,
        &format!("ALTER DATABASE {db} SPRIG SET (logic.function_budget_ms = 300)"),
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE counters (id INT PRIMARY KEY, n BIGINT NOT NULL)",
    )
    .await;
    must(&mut c, "INSERT INTO counters VALUES (1, 0), (2, 0)").await;
    let mut ck = Checks { failed: Vec::new() };
    let js = "let t = 0;
      for (let i = 0; i < n; i++) {
        const r = db.sqlOne('SELECT n FROM counters WHERE id = 1');
        if (r !== null) t = t + 1;
      }
      return { t };";
    let lua = "local t = 0
      for i = 1, n do
        local r = db.sqlOne('SELECT n FROM counters WHERE id = 1')
        if r ~= nil then t = t + 1 end
      end
      return { t = t }";
    let writes = "let t = 0;
      for (let i = 0; i < n; i++) {
        const w = db.exec('UPDATE counters SET n = n + 1 WHERE id = 2');
        t = t + w.affected;
      }
      return { t };";
    for (name, lang, src) in [
        ("reads_js", "javascript", js),
        ("reads_lua", "lua", lua),
        ("writes_js", "javascript", writes),
    ] {
        let n = deploy(
            &mut c,
            &format!("CREATE PROCEDURE {name}(n INT) LANGUAGE {lang} AS $${src}$$"),
        )
        .await;
        ck.check(&format!("{name} compiles"), n.contains("tier mariadb"), &n);
        let t0 = Instant::now();
        let r = run(&mut c, &format!("CALL {name}(50)")).await;
        ck.check(
            &format!("{name}(50) fits"),
            r.is_ok(),
            format!("{} ms, {r:?}", t0.elapsed().as_millis()),
        );
        let t0 = Instant::now();
        let r = run(&mut c, &format!("CALL {name}(90000)")).await;
        let ms = t0.elapsed().as_millis();
        ck.check(
            &format!("{name}(90000) stops near 300 ms with 39864"),
            matches!(&r, Err((39864, m)) if m.contains("wall time")) && (290..500).contains(&ms),
            format!("{ms} ms, {r:?}"),
        );
    }
    // The stopped writer's updates were rolled back with its call.
    let n: Option<i64> = c
        .query_first("SELECT n FROM counters WHERE id = 2")
        .await
        .unwrap();
    ck.check(
        "the stopped call's writes are rolled back",
        n == Some(50),
        format!("{n:?}"),
    );
    // A client's own, smaller max_statement_time is the budget.
    must(&mut c, "SET SESSION max_statement_time = 0.1").await;
    let t0 = Instant::now();
    let r = run(&mut c, "CALL reads_js(90000)").await;
    let ms = t0.elapsed().as_millis();
    must(&mut c, "SET SESSION max_statement_time = 0").await;
    ck.check(
        "the session's max_statement_time = 0.1 shortens the budget",
        // 1969 is 3024 on the MySQL persona.
        matches!(&r, Err((39864 | 1969 | 3024, _))) && ms < 250,
        format!("{ms} ms, {r:?}"),
    );
    // Through a compiled caller: its nested calls share what is left.
    let n = deploy(
        &mut c,
        "CREATE PROCEDURE outer_js(k INT) LANGUAGE javascript AS $$
           let t = 0;
           for (let i = 0; i < k; i++) { const r = db.call('reads_js', 200); t = t + r.fields.t; }
           return { t };
         $$",
    )
    .await;
    ck.check("the caller compiles", n.contains("tier mariadb"), &n);
    let t0 = Instant::now();
    let r = run(&mut c, "CALL outer_js(100000)").await;
    let ms = t0.elapsed().as_millis();
    ck.check(
        "a compiled caller making many nested calls stops near 300 ms",
        matches!(&r, Err((39864, _))) && (290..500).contains(&ms),
        format!("{ms} ms, {r:?}"),
    );
    must(&mut c, &format!("DROP DATABASE {db}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD, ...; tools/live/README.md"]
#[tokio::test]
async fn validations_triggers_and_schedules_keep_their_budgets() {
    let Some(mut c) = connect("validations_triggers_and_schedules_keep_their_budgets").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let db = "sf14_bound";
    must(&mut c, &format!("DROP DATABASE IF EXISTS {db}")).await;
    must(&mut c, &format!("CREATE DATABASE {db}")).await;
    must(&mut c, &format!("USE {db}")).await;
    must(
        &mut c,
        &format!(
            "ALTER DATABASE {db} SPRIG SET (logic.function_budget_ms = 300, logic.trigger_budget_ms = 300)"
        ),
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE ref (id INT PRIMARY KEY, n INT NOT NULL)",
    )
    .await;
    must(&mut c, "INSERT INTO ref VALUES (1, 0)").await;
    must(
        &mut c,
        "CREATE TABLE items (id INT PRIMARY KEY, n INT NOT NULL)",
    )
    .await;
    must(
        &mut c,
        "CREATE TABLE done (id INT PRIMARY KEY, caught INT NOT NULL)",
    )
    .await;
    must(&mut c, "CREATE TABLE big (id INT PRIMARY KEY)").await;
    let values: Vec<String> = (1..=600).map(|i| format!("({i})")).collect();
    must(
        &mut c,
        &format!("INSERT INTO big VALUES {}", values.join(", ")),
    )
    .await;
    let mut ck = Checks { failed: Vec::new() };

    // A validation's loop, inside the client's INSERT.
    let n = deploy(
        &mut c,
        "CREATE VALIDATION slow_check ON items FOR INSERT LANGUAGE javascript AS $$
export default function (row) {
  let t = 0;
  for (let i = 0; i < row.n; i++) {
    const r = db.sqlOne('SELECT n FROM ref WHERE id = 1');
    if (r !== null) t = t + 1;
  }
  if (t < 0) sprig.reject('never');
}
$$",
    )
    .await;
    ck.check(
        "the validation is enforced by BEFORE triggers",
        n.contains("BEFORE triggers"),
        &n,
    );
    let r = run(&mut c, "INSERT INTO items VALUES (1, 5)").await;
    ck.check("a short validation passes", r.is_ok(), format!("{r:?}"));
    let t0 = Instant::now();
    let r = run(&mut c, "INSERT INTO items VALUES (2, 90000)").await;
    let ms = t0.elapsed().as_millis();
    ck.check(
        "a validation of 90,000 reads stops the INSERT near 300 ms",
        matches!(&r, Err((39864, _))) && (290..500).contains(&ms),
        format!("{ms} ms, {r:?}"),
    );
    must(&mut c, "DROP VALIDATION slow_check").await;

    // An after-commit trigger whose catch sees MariaDB's timeout: each
    // join takes about 100 ms; before, the caught timeout let it run all
    // 20 and write `done`. A worker reads a trigger job's budget once, as
    // the job starts, from tenant settings it caches and refreshes after
    // the read: a first trigger job makes it read the new 300 ms, and the
    // real one starts after.
    must(
        &mut c,
        "CREATE TRIGGER warm AFTER COMMIT ON ref FOR INSERT LANGUAGE javascript AS $$
export default function (row) { db.exec('INSERT INTO done (id, caught) VALUES (?, ?)', [row.id + 2000, 0]); }
$$",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let n = deploy(
        &mut c,
        "CREATE TRIGGER slow_ac AFTER COMMIT ON items FOR INSERT LANGUAGE javascript AS $$
export default function (row) {
  let caught = 0;
  for (let i = 0; i < 20; i++) {
    try {
      db.sqlOne('SELECT COUNT(*) AS c FROM big a JOIN big b ON MD5(a.id * 1000 + b.id) LIKE ?', ['ab%']);
    } catch (e) {
      caught = caught + 1;
    }
  }
  db.exec('INSERT INTO done (id, caught) VALUES (?, ?)', [row.id, caught]);
}
$$",
    )
    .await;
    println!("the after-commit trigger: {n}");
    // Workers take a new trigger within a few seconds.
    tokio::time::sleep(Duration::from_secs(4)).await;
    must(&mut c, "INSERT INTO items VALUES (3, 0)").await;
    if let Some((mut r, t)) = catalog().await {
        let q = format!(
            "SELECT message FROM t{t}_sprig.logic_log WHERE level IN ('warn', 'error') AND module_id = \
             (SELECT module_id FROM t{t}_sprig.logic_modules WHERE name = 'slow_ac' AND \
             state = 'active' ORDER BY module_id DESC LIMIT 1) ORDER BY log_id"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let lines: Vec<String> = loop {
            let lines: Vec<String> = r.query(&q).await.unwrap();
            if !lines.is_empty() || Instant::now() > deadline {
                break lines;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        };
        let done: Vec<(i32, i32)> = c.query("SELECT id, caught FROM done").await.unwrap();
        ck.check(
            "the trigger's run stops at its budget although its catch sees the timeout",
            lines
                .iter()
                .any(|l| l.contains("39864") || l.contains("wall time"))
                && done.is_empty(),
            format!("{lines:?}, done {done:?}"),
        );
        drop(r);
    }
    must(&mut c, "DROP TRIGGER slow_ac").await;

    // A schedule runs its procedure under logic.job_budget_ms: the
    // procedure takes longer than its own 300 ms, and the run is ok.
    let n = deploy(
        &mut c,
        "CREATE PROCEDURE long_job(n INT) LANGUAGE javascript AS $$
           let t = 0;
           for (let i = 0; i < 60; i++) {
             const r = db.sqlOne('SELECT id FROM ref WHERE id = 1 AND SLEEP(0.01) = 0');
             if (r !== null) t = t + 1;
           }
           db.exec('INSERT INTO done (id, caught) VALUES (?, ?)', [1000, t]);
           return { t };
         $$",
    )
    .await;
    ck.check("the procedure compiles", n.contains("tier mariadb"), &n);
    let r = run(&mut c, "CALL long_job(60)").await;
    ck.check(
        "called directly, it stops at logic.function_budget_ms",
        matches!(&r, Err((39864, _))),
        format!("{r:?}"),
    );
    if let Some((mut r, t)) = catalog().await {
        must(
            &mut c,
            "CREATE SCHEDULE every_minute CRON '* * * * *' DO CALL long_job(60)",
        )
        .await;
        let q = format!(
            "SELECT last_outcome FROM t{t}_sprig.logic_schedules s JOIN t{t}_sprig.logic_modules m \
             USING (module_id) WHERE m.name = 'every_minute' AND m.state = 'active' AND \
             s.last_outcome IS NOT NULL"
        );
        let deadline = Instant::now() + Duration::from_secs(150);
        let outcome: Option<String> = loop {
            let o: Option<String> = r.query_first(&q).await.unwrap();
            if o.is_some() || Instant::now() > deadline {
                break o;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        };
        let done: Option<i32> = c
            .query_first("SELECT caught FROM done WHERE id = 1000")
            .await
            .unwrap();
        ck.check(
            "a scheduled run of it is ok, under logic.job_budget_ms",
            outcome.as_deref() == Some("ok") && done == Some(60),
            format!("{outcome:?}, done {done:?}"),
        );
        must(&mut c, "DROP SCHEDULE every_minute").await;
    }
    must(&mut c, &format!("DROP DATABASE {db}")).await;
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
