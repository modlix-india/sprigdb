//! `try`/`catch`/`finally`, helper functions and `do`/`while` in every
//! place JavaScript runs, through a live gateway: functions (compiled into
//! MariaDB and, the same body, in the engine tier), computed columns,
//! validations, after-commit triggers, and triggers on streams and queues.
//! Sink transforms run the same QuickJS in workers; `sprig-sinks` tests
//! them (`transform::tests::javascript_try_helpers_and_do_while`).
//!
//! Needs a gateway with workers (triggers run in a worker) and a tenant
//! user who may create databases and routines; the queue part also needs
//! the gateway's AMQP listener:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:43306
//! export SPRIG_TEST_LOGIC_USER=r7acme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! export SPRIG_TEST_R7JS_AMQP=127.0.0.1:43672      # optional
//! cargo test -p sprig-gateway --test logic_js_features_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `r7js_live`, dropped afterwards. Triggers on
//! streams and queues run both compiled into MariaDB and in the engine
//! tier, each checked against the other.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::{Duration, Instant};

use lapin::options::{BasicPublishOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "r7js_live";

fn user() -> (String, String) {
    (
        std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into()),
        std::env::var("SPRIG_TEST_LOGIC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
    )
}

async fn connect(test: &str) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let (user, password) = user();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(user))
        .pass(Some(password))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Some(Conn::new(opts).await.unwrap())
}

type Rows = Vec<Vec<Option<String>>>;

/// A statement's rows as text, or its error number and message.
async fn sql(c: &mut Conn, q: &str) -> Result<Rows, (u16, String)> {
    match c.query::<mysql_async::Row, _>(q).await {
        Ok(rows) => Ok(rows
            .into_iter()
            .map(|r| {
                r.unwrap()
                    .into_iter()
                    .map(|v| match v {
                        mysql_async::Value::NULL => None,
                        mysql_async::Value::Bytes(b) => Some(String::from_utf8_lossy(&b).into()),
                        other => Some(other.as_sql(true)),
                    })
                    .collect()
            })
            .collect()),
        Err(mysql_async::Error::Server(e)) => Err((e.code, e.message)),
        Err(e) => panic!("{q}: {e}"),
    }
}

async fn must(c: &mut Conn, q: &str) -> Rows {
    sql(c, q).await.unwrap_or_else(|e| panic!("{q}: {e:?}"))
}

/// The notes of the last statement (`SHOW WARNINGS`), joined.
async fn notes(c: &mut Conn) -> String {
    let w = must(c, "SHOW WARNINGS").await;
    w.into_iter()
        .filter_map(|r| r.get(2).cloned().flatten())
        .collect::<Vec<_>>()
        .join("; ")
}

/// Waits until `q`'s first column reaches `n`; the last value.
async fn wait_rows(c: &mut Conn, q: &str, n: usize, secs: u64) -> Rows {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let rows = must(c, q).await;
        if rows.len() >= n || Instant::now() > deadline {
            return rows;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

struct Checks {
    passed: usize,
    failed: Vec<String>,
}

impl Checks {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: Vec::new(),
        }
    }

    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            self.passed += 1;
            println!("ok   {name}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }

    fn done(self, what: &str) {
        println!(
            "{what}: {} of {} checks pass",
            self.passed,
            self.passed + self.failed.len()
        );
        assert!(self.failed.is_empty(), "{:#?}", self.failed);
    }
}

/// A function using all three features and the database: `{t}` is its
/// log table, so the compiled and the engine-tier copy write apart.
const FEATURES: &str = "export default function (x) {
  const twice = (n) => n * 2;
  function grade(n) { if (n > 10) return 'big'; if (n > 2) return 'mid'; return 'small'; }
  function put(what) {
    try {
      db.exec('INSERT INTO {t} (id, what) VALUES (?, ?)', [x * 100 + what.length, what]);
      return true;
    } catch (e) {
      return false;
    }
  }
  let steps = 0;
  let total = 0;
  do {
    steps++;
    total += twice(steps);
  } while (steps < x && steps < 6);
  let note = '';
  try {
    if (x < 0) throw new RangeError('negative ' + x);
    if (x === 0) throw new Error('zero');
    note = 'ok';
  } catch (e) {
    note = e.name + ': ' + e.message;
  } finally {
    note = note + ' [' + grade(total) + ']';
  }
  const first = put('aa');
  const again = put('bb');
  return { steps, total, note, first, again };
}";

async fn setup(c: &mut Conn) {
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE log_c (id BIGINT PRIMARY KEY, what VARCHAR(32) NOT NULL)".into(),
        "CREATE TABLE log_e (id BIGINT PRIMARY KEY, what VARCHAR(32) NOT NULL)".into(),
        "CREATE TABLE products (id BIGINT PRIMARY KEY, price DOUBLE NOT NULL, qty INT NOT NULL)"
            .into(),
        "CREATE TABLE sales (id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
         qty INT NOT NULL, total DOUBLE NOT NULL, discount DOUBLE NULL)"
            .into(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, qty INT NOT NULL)".into(),
        "CREATE TABLE audit (id BIGINT PRIMARY KEY, changes INT NOT NULL, last_qty INT NULL, \
         note VARCHAR(64) NULL)"
            .into(),
        "CREATE TABLE hits (src VARCHAR(8) NOT NULL, k VARCHAR(32) NOT NULL, \
         what VARCHAR(64) NOT NULL, PRIMARY KEY (src, k))"
            .into(),
    ] {
        must(c, &q).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn javascript_features_run_the_same_everywhere() {
    let Some(mut c) = connect("javascript_features_run_the_same_everywhere").await else {
        return;
    };
    let started = Instant::now();
    setup(&mut c).await;
    let mut ck = Checks::new();

    // ---- functions: compiled and engine tier, the same body ------------------
    let r = sql(
        &mut c,
        &format!(
            "CREATE FUNCTION feat_c(x INT) LANGUAGE javascript AS $${}$$",
            FEATURES.replace("{t}", "log_c")
        ),
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a function with try/catch/finally, helpers and do/while compiles into MariaDB",
        r.is_ok() && n.contains("mariadb"),
        format!("{r:?} / {n}"),
    );
    let r = sql(
        &mut c,
        &format!(
            "CREATE FUNCTION feat_e(x INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $${}$$",
            FEATURES.replace("{t}", "log_e")
        ),
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "the same body deploys in the engine tier",
        r.is_ok() && n.contains("engine"),
        format!("{r:?} / {n}"),
    );
    let mut same = 0;
    let calls = [-3, 0, 1, 4, 20, 4];
    // The row's values, in order: the event, then the fields.
    let sorted = |r: Result<Rows, (u16, String)>| {
        r.map(|rows| {
            rows.into_iter()
                .map(|mut row| {
                    row.sort();
                    row
                })
                .collect::<Rows>()
        })
    };
    for x in calls {
        let a = sorted(sql(&mut c, &format!("CALL feat_c({x})")).await);
        let b = sorted(sql(&mut c, &format!("CALL feat_e({x})")).await);
        if a == b && a.is_ok() {
            same += 1;
        } else {
            println!("  differ: x = {x}: compiled {a:?}, engine {b:?}");
        }
    }
    ck.check(
        "compiled and engine-tier calls answer the same",
        same == calls.len(),
        format!("{same} of {}", calls.len()),
    );
    let lc = must(&mut c, "SELECT id, what FROM log_c ORDER BY id").await;
    let le = must(&mut c, "SELECT id, what FROM log_e ORDER BY id").await;
    ck.check(
        "and leave the same rows (the repeated call's duplicate keys caught)",
        lc == le && lc.len() == 5,
        format!("{lc:?} / {le:?}"),
    );
    let one = must(&mut c, "CALL feat_c(4)").await;
    ck.check(
        "a call's fields: steps, total, the caught error's note, the helper's results",
        one.first().is_some_and(|r| {
            r.iter().flatten().any(|v| v == "ok [big]") && r.iter().flatten().any(|v| v == "20")
        }),
        format!("{one:?}"),
    );

    // ---- computed columns -----------------------------------------------------
    must(
        &mut c,
        "CREATE FUNCTION gross_js(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript AS $$
           const tax = (v) => v * 0.2;
           function net(v) { if (v > 100) return v * 0.95; return v; }
           return net(price * qty) + tax(price * qty);
         $$",
    )
    .await;
    must(
        &mut c,
        "CREATE FUNCTION safe_js(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE javascript AS $$
           try { return price * qty; } catch (e) { return 0; }
         $$",
    )
    .await;
    for (col, kind) in [("g_s", "STORED"), ("g_v", "VIRTUAL")] {
        let r = sql(
            &mut c,
            &format!(
                "ALTER TABLE products ADD COLUMN {col} DOUBLE AS (gross_js(price, qty)) {kind}"
            ),
        )
        .await;
        ck.check(
            &format!("a computed column ({kind}) from a function with helpers"),
            r.is_ok(),
            format!("{r:?}"),
        );
    }
    let vals: Vec<String> = (1..=12_i64)
        .map(|i| format!("({i}, {}.5, {})", i * 3, (i * 5) % 11))
        .collect();
    must(
        &mut c,
        &format!(
            "INSERT INTO products (id, price, qty) VALUES {}",
            vals.join(", ")
        ),
    )
    .await;
    let rows = must(
        &mut c,
        "SELECT ROUND(g_s, 9), ROUND(g_v, 9), \
         ROUND(IF(price * qty > 100, price * qty * 0.95, price * qty) + price * qty * 0.2, 9) \
         FROM products ORDER BY id",
    )
    .await;
    ck.check(
        "the column's values equal the formula on every row, stored and virtual",
        rows.len() == 12 && rows.iter().all(|r| r[0] == r[1] && r[1] == r[2]),
        format!("{rows:?}"),
    );
    let r = sql(
        &mut c,
        "ALTER TABLE products ADD COLUMN s DOUBLE AS (safe_js(price, qty)) STORED",
    )
    .await;
    ck.check(
        "a function with try/catch makes no computed column (39862, no expression form)",
        matches!(&r, Err((39862, m)) if m.contains("try/catch")),
        format!("{r:?}"),
    );

    // ---- validations --------------------------------------------------------------
    let v = "CREATE VALIDATION sale_rules ON sales FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row, old) {
  function limit(total) { if (total >= 1000) return 20; return 10; }
  let n = 0;
  do { n++; } while (n < row.qty && n < 3);
  try {
    if (row.qty <= 0) throw new Error('qty ' + row.qty + ' after ' + n + ' steps');
    if (old !== null && old.status === 'shipped' && row.total !== old.total)
      throw new TypeError('shipped');
  } catch (e) {
    sprig.reject(e.name + ': ' + e.message);
  } finally {
    n = 0;
  }
  if (row.discount > limit(row.total))
    sprig.reject(`discount ${row.discount}% is above ${limit(row.total)}%`);
}
$$";
    let r = sql(&mut c, v).await;
    let n = notes(&mut c).await;
    ck.check(
        "a validation with try/catch/finally, a helper and do/while deploys as BEFORE triggers",
        r.is_ok() && n.contains("BEFORE triggers"),
        format!("{r:?} / {n}"),
    );
    for (q, want) in [
        ("INSERT INTO sales VALUES (1, 'new', 2, 600, 5)", None),
        (
            "INSERT INTO sales VALUES (2, 'new', 0, 600, 5)",
            Some("Error: qty 0 after 1 steps"),
        ),
        (
            "INSERT INTO sales VALUES (3, 'new', 5, 600, 15)",
            Some("discount 15% is above 10%"),
        ),
        ("INSERT INTO sales VALUES (3, 'new', 5, 1600, 15)", None),
        ("UPDATE sales SET status = 'shipped' WHERE id = 3", None),
        (
            "UPDATE sales SET total = 1700 WHERE id = 3",
            Some("TypeError: shipped"),
        ),
        (
            "UPDATE sales SET qty = -1 WHERE id = 1",
            Some("Error: qty -1 after 1 steps"),
        ),
        ("UPDATE sales SET qty = 9, discount = 1 WHERE id = 1", None),
    ] {
        let r = sql(&mut c, q).await;
        let ok = match want {
            Some(msg) => matches!(&r, Err((39863, got)) if got == msg),
            None => r.is_ok(),
        };
        ck.check(&format!("validation: {q}"), ok, format!("{r:?}"));
    }
    // A test run (the engine tier) gives the compiled trigger's message.
    let r = sql(
        &mut c,
        r#"TEST VALIDATION sale_rules WITH '{"row": {"status": "new", "qty": 0, "total": 600, "discount": 5}, "old": null, "op": "insert"}'"#,
    )
    .await;
    let report = r
        .as_ref()
        .ok()
        .and_then(|rows| rows.first())
        .map(|row| row.iter().flatten().cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    ck.check(
        "TEST VALIDATION (engine tier) refuses with the same message",
        report.contains("39863") && report.contains("Error: qty 0 after 1 steps"),
        format!("{r:?}"),
    );

    // ---- changing columns under compiled logic -------------------------------------
    must(&mut c, "ALTER TABLE sales ADD COLUMN memo VARCHAR(16) NULL").await;
    // A column the validation reads that changes type: the statement runs
    // and the validation is recompiled for the new type (a new version);
    // one it reads that is renamed: refused.
    for (q, refused, recompiled) in [
        (
            "ALTER TABLE sales MODIFY memo VARCHAR(64) NULL",
            false,
            false,
        ),
        ("ALTER TABLE sales MODIFY qty BIGINT NOT NULL", false, false),
        ("ALTER TABLE sales MODIFY qty DOUBLE NOT NULL", false, true),
        (
            "ALTER TABLE sales RENAME COLUMN status TO state",
            true,
            false,
        ),
        (
            "ALTER TABLE sales CHANGE total total DECIMAL(12,2) NOT NULL",
            false,
            false,
        ),
        ("ALTER TABLE sales DROP COLUMN memo", false, false),
    ] {
        let r = sql(&mut c, q).await;
        let n = if r.is_ok() {
            notes(&mut c).await
        } else {
            String::new()
        };
        let ok = if refused {
            matches!(&r, Err((39481, m)) if m.contains("validation sale_rules"))
        } else if recompiled {
            r.is_ok() && n.contains("validation sale_rules was recompiled")
        } else {
            r.is_ok()
        };
        ck.check(
            &format!(
                "{q}: {}",
                if refused {
                    "refused (39481), the validation would no longer compile"
                } else if recompiled {
                    "runs, and the validation is recompiled for the new type"
                } else {
                    "runs"
                }
            ),
            ok,
            format!("{r:?} / {n}"),
        );
    }
    let r = sql(&mut c, "INSERT INTO sales VALUES (8, 'new', -0.5, 50, 1)").await;
    ck.check(
        "the recompiled validation reads the DOUBLE value as it is (-0.5, not -1)",
        matches!(&r, Err((39863, m)) if m == "Error: qty -0.5 after 1 steps"),
        format!("{r:?}"),
    );
    let r = sql(&mut c, "INSERT INTO sales VALUES (7, 'new', 0, 50, 1)").await;
    ck.check(
        "the validation still refuses with its message after the allowed changes",
        matches!(&r, Err((39863, m)) if m == "Error: qty 0 after 1 steps"),
        format!("{r:?}"),
    );

    // ---- after-commit triggers ------------------------------------------------------
    let t = "CREATE TRIGGER count_items AFTER COMMIT ON items FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row, old, op) {
  const label = (q) => q > 50 ? 'many' : 'few';
  function bump(id) {
    try {
      db.exec('INSERT INTO audit (id, changes, last_qty, note) VALUES (?, 1, ?, ?)', [id, row.qty, label(row.qty)]);
    } catch (e) {
      db.exec('UPDATE audit SET changes = changes + 1, last_qty = ?, note = ? WHERE id = ?', [row.qty, label(row.qty), id]);
    } finally {
      if (op === 'update' && old !== null && old.qty > row.qty)
        db.exec('UPDATE audit SET note = ? WHERE id = ?', ['fell', id]);
    }
  }
  let k = 0;
  do { k++; } while (k < 1);
  bump(row.id * k);
}
$$";
    let r = sql(&mut c, t).await;
    let n = notes(&mut c).await;
    ck.check(
        "an after-commit trigger with try/catch/finally, helpers and do/while deploys",
        r.is_ok() && n.contains("after-commit trigger"),
        format!("{r:?} / {n}"),
    );
    must(&mut c, "INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)").await;
    must(&mut c, "UPDATE items SET qty = qty + 50 WHERE id <= 2").await;
    must(&mut c, "UPDATE items SET qty = 5 WHERE id = 1").await;
    let want = vec![
        vec![
            Some("1".into()),
            Some("3".into()),
            Some("5".into()),
            Some("fell".into()),
        ],
        vec![
            Some("2".into()),
            Some("2".into()),
            Some("70".into()),
            Some("many".into()),
        ],
        vec![
            Some("3".into()),
            Some("1".into()),
            Some("30".into()),
            Some("few".into()),
        ],
    ];
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut got = Vec::new();
    while Instant::now() < deadline {
        got = must(
            &mut c,
            "SELECT id, changes, last_qty, note FROM audit ORDER BY id",
        )
        .await;
        if got == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "the trigger caught each duplicate key and counted every change",
        got == want,
        format!("{got:?}"),
    );
    let r = sql(&mut c, "ALTER TABLE items MODIFY qty DOUBLE NOT NULL").await;
    let n = notes(&mut c).await;
    ck.check(
        "retyping a column an after-commit trigger reads recompiles the trigger",
        r.is_ok() && n.contains("trigger count_items was recompiled"),
        format!("{r:?} / {n}"),
    );
    must(&mut c, "INSERT INTO items VALUES (4, 80.5)").await;
    let got = wait_rows(
        &mut c,
        "SELECT id FROM audit WHERE id = 4 AND note = 'many'",
        1,
        60,
    )
    .await;
    ck.check(
        "the recompiled trigger runs on the new type",
        got.len() == 1,
        format!("{got:?}"),
    );

    // ---- a trigger on a stream (engine tier, in a worker) -------------------------------
    must(&mut c, &format!("CREATE STREAM {DB}.events PARTITIONS 2")).await;
    let st = format!(
        "CREATE TRIGGER {DB}.on_event ON STREAM {DB}.events LANGUAGE javascript AS $$
export default function (ctx) {{
  function kind(v) {{
    try {{ return JSON.parse(v).kind ?? 'none'; }}
    catch (e) {{ return 'bad: ' + e.name; }}
  }}
  const pad = (s) => s.length < 3 ? pad('0' + s) : s;
  let n = 0;
  do {{ n++; }} while (n < 2);
  let what = kind(ctx.record.value);
  try {{
    if (what === 'boom') throw new Error('boom ' + ctx.record.key);
  }} catch (e) {{
    what = e.message;
  }} finally {{
    what = what + '/' + n;
  }}
  db.exec('INSERT INTO {DB}.hits (src, k, what) VALUES (?, ?, ?)', ['s', pad(ctx.record.key), what]);
  console.log('r7js event ' + ctx.record.key + ' ' + what);
}}
$$"
    );
    let r = sql(&mut c, &st).await;
    ck.check(
        "a stream trigger with the three features deploys",
        r.is_ok(),
        format!("{r:?}"),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    must(
        &mut c,
        &format!(
            "INSERT INTO {DB}.events (key, value) VALUES \
             ('1', '{{\"kind\": \"a\"}}'), ('2', 'not json'), ('3', '{{\"kind\": \"boom\"}}'), \
             ('4', '{{}}'), ('15', '{{\"kind\": \"b\"}}')"
        ),
    )
    .await;
    let rows = wait_rows(
        &mut c,
        "SELECT k, what FROM hits WHERE src = 's' ORDER BY k",
        5,
        90,
    )
    .await;
    let want: Rows = [
        ("001", "a/2"),
        ("002", "bad: SyntaxError/2"),
        ("003", "boom 3/2"),
        ("004", "none/2"),
        ("015", "b/2"),
    ]
    .iter()
    .map(|(k, w)| vec![Some((*k).to_owned()), Some((*w).to_owned())])
    .collect();
    ck.check(
        "the stream trigger ran each record once, with the caught errors and helpers' results",
        rows == want,
        format!("{rows:?}"),
    );

    // The code's log lines reach the run log (`logic_log`), read here as
    // root of the deployment's own MariaDB when the test is given it.
    if let (Some(root), Some(t)) = (
        skip::var("SPRIG_TEST_R7JS_ROOT"),
        skip::var("SPRIG_TEST_R7JS_TENANT"),
    ) {
        let mut r = Conn::from_url(root.as_str()).await.unwrap();
        let q = format!(
            "SELECT message FROM t{t}_sprig.logic_log WHERE level = 'info' \
             AND message LIKE '%r7js event%' ORDER BY log_id"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let lines: Vec<String> = loop {
            let lines: Vec<String> = r.query(&q).await.unwrap();
            if lines.len() >= 5 || Instant::now() > deadline {
                break lines;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        ck.check(
            "the stream trigger's console.log lines are in its run log, with the record",
            lines.len() == 5
                && lines.iter().any(|l| {
                    l.contains("offset") && l.ends_with("r7js event 2 bad: SyntaxError/2")
                }),
            format!("{lines:?}"),
        );
        drop(r);
    }

    // ---- a stream trigger compiled into MariaDB, against its engine-tier twin ------------
    // Both read every record of one stream (each trigger has its own
    // consumer group); a record whose JSON the compiled code can't read as
    // JavaScript does (`kind` a number where `??` reads text) runs in the
    // engine tier, record by record.
    must(&mut c, &format!("CREATE STREAM {DB}.cevents PARTITIONS 2")).await;
    let ctrig = |name: &str, src: &str, with: &str| {
        format!(
            "CREATE TRIGGER {DB}.{name} ON STREAM {DB}.cevents LANGUAGE javascript {with} AS $$
export default function (ctx) {{
  const r = ctx.record;
  const e = JSON.parse(r.value);
  const kind = e.kind ?? 'none';
  let n = 0;
  if (Array.isArray(e.items)) {{ for (const it of e.items) {{ n += it.qty; }} }}
  db.exec('INSERT INTO hits (src, k, what) VALUES (?, ?, ?)', ['{src}', r.key, kind + '/' + n]);
  console.log('ctrig {src} ' + r.key + ' ' + kind);
}}
$$"
        )
    };
    let r = sql(&mut c, &ctrig("on_cevent", "c", "")).await;
    let n = if r.is_ok() {
        notes(&mut c).await
    } else {
        String::new()
    };
    ck.check(
        "a stream trigger reading JSON.parse and ctx.record compiles into MariaDB",
        r.is_ok() && n.contains("tier mariadb"),
        format!("{r:?} / {n}"),
    );
    let r = sql(
        &mut c,
        &ctrig("on_eevent", "e", "WITH (SPRIG_TIER = 'engine')"),
    )
    .await;
    let n = if r.is_ok() {
        notes(&mut c).await
    } else {
        String::new()
    };
    ck.check(
        "its twin deploys in the engine tier",
        r.is_ok() && n.contains("tier engine"),
        format!("{r:?} / {n}"),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    must(
        &mut c,
        &format!(
            "INSERT INTO {DB}.cevents (key, value) VALUES \
             ('c1', '{{\"kind\": \"a\", \"items\": [{{\"qty\": 2}}, {{\"qty\": 3}}]}}'), \
             ('c2', '{{\"kind\": 5}}'), ('c3', '{{\"kind\": \"b\", \"items\": [{{\"qty\": 1.5}}]}}'), \
             ('c4', '{{}}')"
        ),
    )
    .await;
    let q = |src: &str| format!("SELECT k, what FROM hits WHERE src = '{src}' ORDER BY k");
    let compiled = wait_rows(&mut c, &q("c"), 4, 90).await;
    let engine = wait_rows(&mut c, &q("e"), 4, 90).await;
    let want: Rows = [
        ("c1", "a/5"),
        ("c2", "5/0"),
        ("c3", "b/1.5"),
        ("c4", "none/0"),
    ]
    .iter()
    .map(|(k, w)| vec![Some((*k).to_owned()), Some((*w).to_owned())])
    .collect();
    ck.check(
        "the compiled stream trigger ran each record once, as its engine-tier twin did",
        compiled == want && engine == want,
        format!("compiled {compiled:?} / engine {engine:?}"),
    );
    if let (Some(root), Some(t)) = (
        skip::var("SPRIG_TEST_R7JS_ROOT"),
        skip::var("SPRIG_TEST_R7JS_TENANT"),
    ) {
        let mut r = Conn::from_url(root.as_str()).await.unwrap();
        let q = format!(
            "SELECT message FROM t{t}_sprig.logic_log WHERE level = 'info' \
             AND message LIKE '%ctrig c %' ORDER BY log_id"
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let lines: Vec<String> = loop {
            let lines: Vec<String> = r.query(&q).await.unwrap();
            if lines.len() >= 4 || Instant::now() > deadline {
                break lines;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        ck.check(
            "the compiled trigger's console.log lines are in its run log, also for the record \
             the engine tier ran",
            lines.len() == 4
                && lines.iter().any(|l| l.ends_with("ctrig c c1 a"))
                && lines.iter().any(|l| l.ends_with("ctrig c c2 5")),
            format!("{lines:?}"),
        );
        drop(r);
    }

    // ---- a trigger on a queue (engine tier, in a worker) ---------------------------------
    if let Some(amqp) = skip::var("SPRIG_TEST_R7JS_AMQP") {
        let (user, password) = user();
        let a = Connection::connect(
            &format!("amqp://{user}:{password}@{amqp}/{DB}"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let ch = a.create_channel().await.unwrap();
        ch.queue_declare(
            "jobs".into(),
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
        let qt = format!(
            "CREATE TRIGGER {DB}.on_job ON QUEUE {DB}.jobs LANGUAGE javascript AS $$
export default function (ctx) {{
  function parse(b) {{ try {{ return JSON.parse(b); }} catch {{ return {{ n: -1 }}; }} }}
  const job = parse(ctx.message.body);
  let tries = 0;
  let done = 'no';
  do {{
    tries++;
    try {{
      if (job.n < 0) throw new Error('unreadable');
      done = 'n' + job.n;
    }} catch (e) {{
      done = e.message;
    }} finally {{
      done = done + '#' + tries;
    }}
  }} while (tries < 2 && job.n === 2);
  db.exec('INSERT INTO {DB}.hits (src, k, what) VALUES (?, ?, ?)', ['q', ctx.message.body, done]);
}}
$$"
        );
        let r = sql(&mut c, &qt).await;
        ck.check(
            "a queue trigger with the three features deploys",
            r.is_ok(),
            format!("{r:?}"),
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
        for body in ["{\"n\": 1}", "{\"n\": 2}", "garbage"] {
            ch.basic_publish(
                "".into(),
                "jobs".into(),
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        }
        let mut rows = wait_rows(
            &mut c,
            "SELECT k, what FROM hits WHERE src = 'q' ORDER BY k",
            3,
            90,
        )
        .await;
        rows.sort();
        let want: Rows = [
            ("garbage", "unreadable#1"),
            ("{\"n\": 1}", "n1#1"),
            ("{\"n\": 2}", "n2#2"),
        ]
        .iter()
        .map(|(k, w)| vec![Some((*k).to_owned()), Some((*w).to_owned())])
        .collect();
        ck.check(
            "the queue trigger ran each message once, with the caught errors and helpers' results",
            rows == want,
            format!("{rows:?}"),
        );
        // A compiled queue trigger and its engine-tier twin, each on its
        // own queue (a queue's triggers compete for its messages), given
        // the same messages.
        let mut deployed = Vec::new();
        for (queue, name, src, with) in [
            ("cjobs", "on_cjob", "qc", ""),
            ("ejobs", "on_ejob", "qe", "WITH (SPRIG_TIER = 'engine')"),
        ] {
            ch.queue_declare(
                queue.into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
            let t = format!(
                "CREATE TRIGGER {DB}.{name} ON QUEUE {DB}.{queue} LANGUAGE javascript {with} AS $$
export default function (ctx) {{
  const job = JSON.parse(ctx.message.body);
  db.exec('INSERT INTO hits (src, k, what) VALUES (?, ?, ?)',
          ['{src}', ctx.message.body, String(job.n ?? -1) + '#' + ctx.message.deliveryCount]);
}}
$$"
            );
            let r = sql(&mut c, &t).await;
            let n = if r.is_ok() {
                notes(&mut c).await
            } else {
                String::new()
            };
            deployed.push((r.is_ok(), n));
        }
        ck.check(
            "a queue trigger reading JSON.parse and ctx.message compiles; its twin stays in the \
             engine tier",
            deployed[0].0
                && deployed[0].1.contains("tier mariadb")
                && deployed[1].0
                && deployed[1].1.contains("tier engine"),
            format!("{deployed:?}"),
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
        for queue in ["cjobs", "ejobs"] {
            for body in ["{\"n\": 1}", "{\"n\": 2.5}", "{\"n\": \"x\"}", "{}"] {
                ch.basic_publish(
                    "".into(),
                    queue.into(),
                    BasicPublishOptions::default(),
                    body.as_bytes(),
                    BasicProperties::default().with_delivery_mode(2),
                )
                .await
                .unwrap()
                .await
                .unwrap();
            }
        }
        let mut rows = Vec::new();
        for src in ["qc", "qe"] {
            let mut got = wait_rows(
                &mut c,
                &format!("SELECT k, what FROM hits WHERE src = '{src}' ORDER BY k"),
                4,
                90,
            )
            .await;
            got.sort();
            rows.push(got);
        }
        let mut want: Rows = [
            ("{\"n\": 1}", "1#0"),
            ("{\"n\": 2.5}", "2.5#0"),
            ("{\"n\": \"x\"}", "x#0"),
            ("{}", "-1#0"),
        ]
        .iter()
        .map(|(k, w)| vec![Some((*k).to_owned()), Some((*w).to_owned())])
        .collect();
        want.sort();
        ck.check(
            "the compiled queue trigger ran each message once, as its engine-tier twin did",
            rows[0] == want && rows[1] == want,
            format!("compiled {:?} / engine {:?}", rows[0], rows[1]),
        );
        a.close(200, "done".into()).await.unwrap();
    } else {
        println!("no SPRIG_TEST_R7JS_AMQP: the queue trigger part is skipped");
    }

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!("took {:.1} s", started.elapsed().as_secs_f64());
    ck.done("JavaScript features in every place");
}
