//! A column type change and the recompile of the compiled logic that reads
//! the column, as one DDL job. Before, the gateway ran the `ALTER TABLE`
//! and then deployed each validation and after-commit trigger again: in
//! between, the old version judged or handled the column's new values
//! converted to the old type, so a validation could pass a row it refuses
//! and a trigger could copy a rounded value. Now each new version is in
//! place before the statement (a validation's `BEFORE` triggers beside the
//! old ones, a `CHECK` swapped inside the statement, an after-commit
//! trigger's version switched first), and the old validations go after it.
//!
//! A writer inserts rows the whole time `ALTER TABLE sales MODIFY qty
//! DOUBLE` runs: `-0.4` (an integer column stores 0, which passes; a
//! `DOUBLE` stores -0.4, which the validation refuses) and `2.5` (stored 3,
//! then 2.5). No row may end up negative, and the after-commit trigger's
//! copy of every row must equal the row. A `CHECK` validation's type change
//! leaves exactly one constraint, the new version's. A change that fails
//! (a value out of `INT`'s range) leaves the old versions working.
//!
//! ```sh
//! export SPRIG_TEST_LCC_GATEWAY=127.0.0.1:48306
//! export SPRIG_TEST_LCC_USER=sf13t.app SPRIG_TEST_LCC_PASSWORD=sprig-dev-app
//! cargo test -p sprig-gateway --test logic_column_change_live -- --nocapture
//! ```
//!
//! Needs a worker (after-commit triggers run there). Works in the database
//! `lcc_live`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    // `c`, `r`, `n`, `w`, `t`: the session, a result, its notes, the writer
    // task and a timer, as the other live tests name them.
    clippy::many_single_char_names,
    missing_docs
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};

const DB: &str = "lcc_live";

async fn connect() -> Option<Conn> {
    let addr = std::env::var("SPRIG_TEST_LCC_GATEWAY").ok()?;
    let (host, port) = addr.rsplit_once(':').unwrap();
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(
            std::env::var("SPRIG_TEST_LCC_USER").unwrap_or_else(|_| "sf13t.app".into()),
        ))
        .pass(Some(
            std::env::var("SPRIG_TEST_LCC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
        ))
        .db_name(Some(DB))
        .ssl_opts(Some(
            SslOpts::default().with_danger_accept_invalid_certs(true),
        ));
    Some(Conn::new(opts).await.unwrap())
}

type Rows = Vec<Vec<Option<String>>>;

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

async fn notes(c: &mut Conn) -> String {
    must(c, "SHOW WARNINGS")
        .await
        .into_iter()
        .filter_map(|r| r.get(2).cloned().flatten())
        .collect::<Vec<_>>()
        .join("; ")
}

async fn one(c: &mut Conn, q: &str) -> String {
    must(c, q)
        .await
        .first()
        .and_then(|r| r.first().cloned().flatten())
        .unwrap_or_default()
}

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

/// Inserts rows into `sales` until `stop`, from id `first`: `-0.4` and
/// `2.5` in turn. The (id, value, accepted) of each.
async fn writer(mut c: Conn, first: i64, stop: Arc<AtomicBool>) -> Vec<(i64, &'static str, bool)> {
    let mut out = Vec::new();
    let mut id = first;
    while !stop.load(Ordering::Relaxed) {
        let v = if id % 2 == 0 { "-0.4" } else { "2.5" };
        let r = c
            .query_drop(format!("INSERT INTO sales (id, qty) VALUES ({id}, {v})"))
            .await;
        out.push((id, v, r.is_ok()));
        id += 1;
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let _ = c.disconnect().await;
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_change_and_its_recompile_are_one_job() {
    let Some(mut setup) = ({
        // The database first, from a session without it.
        let Ok(addr) = std::env::var("SPRIG_TEST_LCC_GATEWAY") else {
            eprintln!("skipped: set SPRIG_TEST_LCC_GATEWAY");
            return;
        };
        let (host, port) = addr.rsplit_once(':').unwrap();
        let opts = OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port.parse().unwrap())
            .user(Some(
                std::env::var("SPRIG_TEST_LCC_USER").unwrap_or_else(|_| "sf13t.app".into()),
            ))
            .pass(Some(
                std::env::var("SPRIG_TEST_LCC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
            ))
            .ssl_opts(Some(
                SslOpts::default().with_danger_accept_invalid_certs(true),
            ));
        Some(Conn::new(opts).await.unwrap())
    }) else {
        return;
    };
    must(&mut setup, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut setup, &format!("CREATE DATABASE {DB}")).await;
    drop(setup);
    let mut c = connect().await.unwrap();
    let mut ck = Checks {
        n: 0,
        failed: Vec::new(),
    };
    for q in [
        "CREATE TABLE sales (id INT PRIMARY KEY, qty BIGINT NOT NULL)",
        "CREATE TABLE copies (id INT PRIMARY KEY, qty DOUBLE NOT NULL)",
        "CREATE TABLE chk (id INT PRIMARY KEY, qty BIGINT NOT NULL)",
    ] {
        must(&mut c, q).await;
    }
    // A message built from the value: BEFORE triggers, not a CHECK.
    let r = sql(
        &mut c,
        "CREATE VALIDATION no_neg ON sales FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row) {
  if (row.qty < 0) sprig.reject('negative ' + row.qty);
}
$$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.ok(
        "a validation enforced by BEFORE triggers",
        r.is_ok() && n.contains("BEFORE triggers"),
        || format!("{r:?} {n}"),
    );
    let r = sql(
        &mut c,
        "CREATE TRIGGER copy_qty AFTER COMMIT ON sales FOR INSERT LANGUAGE javascript AS $$
export default function (row) {
  db.exec('INSERT INTO copies (id, qty) VALUES (?, ?)', [row.id, row.qty]);
}
$$",
    )
    .await;
    ck.ok(
        "an after-commit trigger copying each row",
        r.is_ok(),
        || format!("{r:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE VALIDATION chk_pos ON chk FOR INSERT, UPDATE LANGUAGE javascript AS $$
export default function (row) {
  if (row.qty < 0) sprig.reject('negative');
}
$$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.ok(
        "a validation enforced by a CHECK",
        r.is_ok() && n.contains("CHECK"),
        || format!("{r:?} {n}"),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let r = sql(&mut c, "INSERT INTO sales VALUES (1, 5)").await;
    ck.ok("a row the validation passes", r.is_ok(), || {
        format!("{r:?}")
    });
    let r = sql(&mut c, "INSERT INTO sales VALUES (2, -1)").await;
    ck.ok(
        "a row it refuses (39863)",
        matches!(&r, Err((39863, _))),
        || format!("{r:?}"),
    );

    // The change, with a writer the whole time.
    let stop = Arc::new(AtomicBool::new(false));
    let w = tokio::spawn(writer(connect().await.unwrap(), 1000, Arc::clone(&stop)));
    tokio::time::sleep(Duration::from_millis(800)).await;
    let t0 = Instant::now();
    let r = sql(&mut c, "ALTER TABLE sales MODIFY qty DOUBLE NOT NULL").await;
    let took = t0.elapsed();
    let n = notes(&mut c).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    stop.store(true, Ordering::Relaxed);
    let written = w.await.unwrap();
    eprintln!(
        "     ALTER took {} ms; the writer made {} inserts ({} accepted)",
        took.as_millis(),
        written.len(),
        written.iter().filter(|x| x.2).count()
    );
    ck.ok(
        "the change runs, with both recompiled as part of it",
        r.is_ok()
            && n.contains("validation no_neg was recompiled")
            && n.contains("trigger copy_qty was recompiled"),
        || format!("{r:?} {n}"),
    );
    let neg = one(&mut c, "SELECT COUNT(*) FROM sales WHERE qty < 0").await;
    ck.ok(
        "LEAK: no row the validation refuses was written while the change ran",
        neg == "0",
        || {
            format!(
                "{neg} negative rows: {:?}",
                written.iter().filter(|x| x.1 == "-0.4" && x.2).count()
            )
        },
    );
    let refused_after = written
        .iter()
        .rev()
        .take(20)
        .filter(|x| x.1 == "-0.4")
        .all(|x| !x.2);
    ck.ok(
        "after the change the validation refuses -0.4",
        refused_after,
        || format!("{:?}", written.iter().rev().take(20).collect::<Vec<_>>()),
    );
    // Every copy as its row.
    let total = one(&mut c, "SELECT COUNT(*) FROM sales WHERE id >= 1000").await;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(60)
        && one(&mut c, "SELECT COUNT(*) FROM copies WHERE id >= 1000").await != total
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let copied = one(&mut c, "SELECT COUNT(*) FROM copies WHERE id >= 1000").await;
    let wrong = must(
        &mut c,
        "SELECT s.id, s.qty, c.qty FROM sales s JOIN copies c ON c.id = s.id WHERE s.qty <> c.qty \
         ORDER BY s.id LIMIT 10",
    )
    .await;
    let exact = one(
        &mut c,
        "SELECT COUNT(*) FROM copies c JOIN sales s ON s.id = c.id WHERE c.qty = 2.5",
    )
    .await;
    ck.ok(
        "the trigger copied every row written around the change",
        copied == total,
        || format!("{copied} of {total}"),
    );
    ck.ok(
        "every copy equals its row (no value rounded by the old version)",
        wrong.is_empty(),
        || format!("{wrong:?}"),
    );
    ck.ok(
        "rows written after the change were copied as 2.5",
        exact.parse::<u64>().unwrap_or(0) > 0,
        || exact.clone(),
    );

    // A CHECK: swapped inside the statement.
    let r = sql(&mut c, "ALTER TABLE chk MODIFY qty DOUBLE NOT NULL").await;
    let n = notes(&mut c).await;
    ck.ok(
        "a CHECK validation's column change runs with its recompile",
        r.is_ok() && n.contains("validation chk_pos was recompiled"),
        || format!("{r:?} {n}"),
    );
    let create = must(&mut c, "SHOW CREATE TABLE chk").await;
    let text = create
        .first()
        .and_then(|r| r.get(1).cloned().flatten())
        .unwrap_or_default();
    ck.ok(
        "the table has exactly one validation constraint, the new version's",
        text.matches("CONSTRAINT `sprig_lc").count() == 1 && text.contains("_v2`"),
        || text.clone(),
    );
    let r = sql(&mut c, "INSERT INTO chk VALUES (10, -0.4)").await;
    ck.ok(
        "the new CHECK refuses -0.4",
        matches!(&r, Err((39863 | 4025, _))),
        || format!("{r:?}"),
    );
    let r = sql(&mut c, "INSERT INTO chk VALUES (11, 0.4)").await;
    ck.ok("and accepts 0.4", r.is_ok(), || format!("{r:?}"));

    // A change that fails: nothing changes, the old versions work.
    must(&mut c, "INSERT INTO sales VALUES (3, 10000000000)").await;
    let r = sql(&mut c, "ALTER TABLE sales MODIFY qty INT NOT NULL").await;
    ck.ok("a change the rows refuse fails", r.is_err(), || {
        format!("{r:?}")
    });
    let ty = one(
        &mut c,
        &format!(
            "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{DB}' \
             AND TABLE_NAME = 'sales' AND COLUMN_NAME = 'qty'"
        ),
    )
    .await;
    ck.ok("the column keeps its type", ty == "double", || ty.clone());
    let r = sql(&mut c, "INSERT INTO sales VALUES (4, -0.25)").await;
    ck.ok(
        "the validation still refuses -0.25",
        matches!(&r, Err((39863, m)) if m.contains("-0.25")),
        || format!("{r:?}"),
    );
    must(&mut c, "INSERT INTO sales VALUES (5, 7.25)").await;
    let t = Instant::now();
    let mut got = String::new();
    while t.elapsed() < Duration::from_secs(30) {
        got = one(&mut c, "SELECT qty FROM copies WHERE id = 5").await;
        if !got.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    ck.ok(
        "the trigger still copies exactly (7.25)",
        got == "7.25",
        || got.clone(),
    );

    let _ = sql(&mut c, "DROP TRIGGER copy_qty").await;
    let _ = sql(&mut c, "DROP VALIDATION no_neg").await;
    let _ = sql(&mut c, "DROP VALIDATION chk_pos").await;
    let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    println!(
        "column change with its recompile: {} checks, {} failed",
        ck.n,
        ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
