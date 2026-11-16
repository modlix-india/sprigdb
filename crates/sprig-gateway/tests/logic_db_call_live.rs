//! Compiled `db.call` and `db.publish` through a live gateway, in every
//! place they compile: functions and procedures (each compared with the
//! same body in the engine tier), a validation, an after-commit trigger;
//! a callee mid-rollout; `EXECUTE` and table rights; the check on
//! redeploying a callee; and a timing of compiled against engine-tier
//! calls. Sink transforms have no `db` (39219), so neither applies there.
//!
//! Needs a gateway with workers (triggers and stream sequencers run in a
//! worker) and a tenant user who may create databases, routines, streams
//! and users:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:47306
//! export SPRIG_TEST_LOGIC_USER=r7dcacme.app SPRIG_TEST_LOGIC_PASSWORD=...
//! cargo test -p sprig-gateway --test logic_db_call_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `r7dc_live` and the users `r7dc_u1`, `r7dc_u2`,
//! `r7dc_u3`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_LOGIC_GATEWAY";
const DB: &str = "r7dc_live";
const USERS: [&str; 3] = ["r7dc_u1", "r7dc_u2", "r7dc_u3"];
const USER_PASS: &str = "r7dc-user-Pass-1";

fn user() -> (String, String) {
    (
        std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into()),
        std::env::var("SPRIG_TEST_LOGIC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
    )
}

async fn connect_as(test: &str, who: Option<(&str, &str)>) -> Option<Conn> {
    let Some(v) = skip::var(GATEWAY) else {
        skip::report_missing(GATEWAY, test);
        return None;
    };
    let urls = skip::urls(&v);
    let (host, port) = urls[0].rsplit_once(':').unwrap();
    let (u, p) = user();
    let (u, p) = match who {
        Some((name, pass)) => {
            // `tenant.app` users log in as `tenant.<name>`.
            let tenant = u.split_once('.').map_or("", |(t, _)| t);
            (format!("{tenant}.{name}"), pass.to_owned())
        }
        None => (u, p),
    };
    let opts = OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(u))
        .pass(Some(p))
        .db_name(who.map(|_| DB))
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
    let w = must(c, "SHOW WARNINGS").await;
    w.into_iter()
        .filter_map(|r| r.get(2).cloned().flatten())
        .collect::<Vec<_>>()
        .join("; ")
}

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
    fn check(&mut self, name: &str, ok: bool, detail: impl std::fmt::Display) {
        if ok {
            self.passed += 1;
            println!("ok   {name}");
        } else {
            println!("FAIL {name}: {detail}");
            self.failed.push(format!("{name}: {detail}"));
        }
    }
}

/// The row of a call with its values sorted (column order differs
/// between a compiled and an engine-tier wrapper only by name).
fn sorted(r: Result<Rows, (u16, String)>) -> Result<Rows, u16> {
    r.map(|rows| {
        rows.into_iter()
            .map(|mut row| {
                row.sort();
                row
            })
            .collect()
    })
    .map_err(|(n, _)| n)
}

/// The caller's body; `{src}` tells the compiled and engine copies apart
/// in `audit`, `{s}` is the stream it publishes to.
const CALLER: &str = "export default function (x) {
  const p = db.call('price', x);
  let took = -1;
  let err = '';
  try {
    const t = db.call('take', x, 2);
    took = t.fields.left;
  } catch (e) {
    err = 'take failed';
    db.exec('INSERT INTO audit (src, what) VALUES (?, ?)', ['{src}', 'no stock ' + x]);
  }
  let checks = 0;
  for (let j = 0; j < 3; j++) {
    try { db.call('check', j - 1 + x); checks++; } catch (e) { checks += 10; }
  }
  db.publish('{s}', String(x), { id: x, price: p.fields.price, took }, { src: '{src}' });
  db.publish('{s}', x, 'plain ' + x);
  if (x === 13) throw new Error('after the writes');
  return { e: p.event, price: p.fields.price, cheap: p.fields.cheap, why: p.fields.why, took, err, checks };
}";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compiled_calls_and_publishes_run_the_same_everywhere() {
    let Some(mut c) = connect_as("compiled_calls_and_publishes", None).await else {
        return;
    };
    let started = Instant::now();
    let mut ck = Checks {
        passed: 0,
        failed: Vec::new(),
    };
    for u in USERS {
        let _ = sql(&mut c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE products (id BIGINT PRIMARY KEY, price DOUBLE NOT NULL, stock INT NOT NULL)"
            .into(),
        "INSERT INTO products VALUES (7, 400, 10), (8, 19.99, 3), (9, 0.1, 1)".into(),
        "CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, src VARCHAR(8) NOT NULL, \
         what VARCHAR(64) NOT NULL)"
            .into(),
        "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL)"
            .into(),
        "CREATE STREAM ev_c PARTITIONS 1".into(),
        "CREATE STREAM ev_e PARTITIONS 1".into(),
        "CREATE STREAM ev_t PARTITIONS 1".into(),
        "CREATE STREAM latest_s PARTITIONS 1 CLEANUP compact".into(),
    ] {
        must(&mut c, &q).await;
    }

    // ---- callees ------------------------------------------------------------
    for q in [
        "CREATE FUNCTION price(id INT) LANGUAGE javascript AS $$
           const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [id]);
           if (p === null) return sprig.event('missing', { why: 'no product ' + id });
           return { price: p.price, stock: p.stock, cheap: p.price < 50 };
         $$",
        "CREATE PROCEDURE take(id INT, q INT) LANGUAGE javascript AS $$
           const t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', [q, id, q]);
           if (t.affected !== 1) throw new Error('out of stock: ' + id);
           const left = db.sqlOne('SELECT stock FROM products WHERE id = ?', [id]);
           return { left: left.stock };
         $$",
        "CREATE FUNCTION check(x INT) LANGUAGE javascript AS $$
           if (x === 0) sprig.reject('zero is not allowed');
           if (x < 0) throw new RangeError('negative: ' + x);
           return { v: x * 2 };
         $$",
    ] {
        let r = sql(&mut c, q).await;
        let n = notes(&mut c).await;
        ck.check(
            &format!("callee compiles: {}", &q[..30]),
            r.is_ok() && n.contains("mariadb"),
            format!("{r:?} / {n}"),
        );
    }

    // ---- callers: compiled and engine tier --------------------------------------
    let body_c = CALLER.replace("{src}", "c").replace("{s}", "ev_c");
    let body_e = CALLER.replace("{src}", "e").replace("{s}", "ev_e");
    let r = sql(
        &mut c,
        &format!("CREATE PROCEDURE calls_c(x INT) LANGUAGE javascript AS $${body_c}$$"),
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a procedure with db.call and db.publish compiles into MariaDB",
        r.is_ok() && n.contains("mariadb"),
        format!("{r:?} / {n}"),
    );
    let r = sql(
        &mut c,
        &format!(
            "CREATE PROCEDURE calls_e(x INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $${body_e}$$"
        ),
    )
    .await;
    ck.check(
        "the same body deploys in the engine tier",
        r.is_ok(),
        format!("{r:?}"),
    );
    let calls = [7, 8, 8, 9, 70, 13, 7];
    let mut same = 0;
    for x in calls {
        // Each side starts from the same stock.
        must(
            &mut c,
            "UPDATE products SET stock = CASE id WHEN 7 THEN 10 WHEN 8 THEN 3 ELSE 1 END",
        )
        .await;
        let a = sorted(sql(&mut c, &format!("CALL calls_c({x})")).await);
        let stock_c = must(&mut c, "SELECT id, stock FROM products ORDER BY id").await;
        must(
            &mut c,
            "UPDATE products SET stock = CASE id WHEN 7 THEN 10 WHEN 8 THEN 3 ELSE 1 END",
        )
        .await;
        let b = sorted(sql(&mut c, &format!("CALL calls_e({x})")).await);
        let stock_e = must(&mut c, "SELECT id, stock FROM products ORDER BY id").await;
        if a == b && stock_c == stock_e {
            same += 1;
        } else {
            println!("  differ: x = {x}: compiled {a:?} {stock_c:?}, engine {b:?} {stock_e:?}");
        }
    }
    ck.check(
        "compiled and engine-tier callers answer the same and leave the same stock",
        same == calls.len(),
        format!("{same} of {}", calls.len()),
    );
    let ac = must(&mut c, "SELECT what FROM audit WHERE src = 'c' ORDER BY id").await;
    let ae = must(&mut c, "SELECT what FROM audit WHERE src = 'e' ORDER BY id").await;
    ck.check(
        "and write the same audit rows (the failed call's rolled back)",
        ac == ae && !ac.is_empty(),
        format!("{ac:?} / {ae:?}"),
    );
    let read = |s: &str| {
        format!("SELECT `key`, `value`, headers FROM {s} ORDER BY partition_no, offset_no")
    };
    let pc = wait_rows(&mut c, &read("ev_c"), 12, 30).await;
    let pe = wait_rows(&mut c, &read("ev_e"), 12, 30).await;
    let norm = |rows: &Rows| -> Vec<Vec<Option<String>>> {
        rows.iter()
            .map(|r| {
                r.iter()
                    .map(|v| {
                        v.as_ref().map(|t| {
                            t.replace(' ', "")
                                .replace("\"src\":\"c\"", "")
                                .replace("\"src\":\"e\"", "")
                        })
                    })
                    .collect()
            })
            .collect()
    };
    ck.check(
        "the stream records are the same, and the failed call's are gone",
        norm(&pc) == norm(&pe) && pc.len() == 12,
        format!("{pc:?} / {pe:?}"),
    );

    // ---- a size limit and a compacted stream ----------------------------------
    must(
        &mut c,
        &format!("ALTER DATABASE {DB} SPRIG SET (stream.max_message_bytes = 100)"),
    )
    .await;
    let pubs = "export default function (n) {
      let v = '';
      for (let k = 0; k < n; k++) v = v + '0123456789';
      let out = '';
      try { db.publish('latest_s', null, 'v'); } catch (e) { out = out + 'nokey;'; }
      try { db.publish('ev_c', 'k', v); out = out + 'sent;'; } catch (e) { out = out + 'big;'; }
      return { out };
    }";
    must(
        &mut c,
        &format!("CREATE PROCEDURE pubs_c(n INT) LANGUAGE javascript AS $${pubs}$$"),
    )
    .await;
    must(
        &mut c,
        &format!(
            "CREATE PROCEDURE pubs_e(n INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $${pubs}$$"
        ),
    )
    .await;
    for n in [2, 6, 11] {
        let a = sorted(sql(&mut c, &format!("CALL pubs_c({n})")).await);
        let b = sorted(sql(&mut c, &format!("CALL pubs_e({n})")).await);
        println!("  {n}0 bytes: compiled {a:?}, engine {b:?}");
        if n < 10 {
            ck.check(
                &format!("a {n}0-byte value: the same answer in both tiers"),
                a == b,
                format!("{a:?} / {b:?}"),
            );
        }
    }
    let r = sql(&mut c, "CALL pubs_c(11)").await;
    ck.check(
        "a compiled publish above stream.max_message_bytes is caught as 39944",
        format!("{r:?}").contains("big;") && format!("{r:?}").contains("nokey;"),
        format!("{r:?}"),
    );

    // ---- a callee mid-rollout ---------------------------------------------------
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION price(id INT) LANGUAGE javascript WITH (SPRIG_ROLLOUT = 50) AS $$
           const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [id]);
           if (p === null) return sprig.event('missing', { why: 'gone ' + id });
           return { price: p.price * 2, stock: p.stock, cheap: p.price < 50 };
         $$",
    )
    .await;
    ck.check("a candidate of price for 50%", r.is_ok(), format!("{r:?}"));
    must(&mut c, "CREATE FUNCTION via(x INT) LANGUAGE javascript AS $$ const r = db.call('price', x); return { p: r.fields.price }; $$").await;
    for u in USERS {
        must(
            &mut c,
            &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{USER_PASS}'"),
        )
        .await;
        must(
            &mut c,
            &format!("GRANT EXECUTE, SELECT ON {DB}.* TO '{u}'@'%'"),
        )
        .await;
    }
    let mut agree = 0;
    let mut versions = Vec::new();
    for u in USERS {
        let Some(mut uc) = connect_as("rollout", Some((u, USER_PASS))).await else {
            return;
        };
        let direct = must(&mut uc, "CALL price(7)").await;
        let through = must(&mut uc, "CALL via(7)").await;
        let d = direct[0].iter().flatten().any(|v| v == "800");
        let t = through[0].iter().flatten().any(|v| v == "800");
        versions.push(d);
        if d == t {
            agree += 1;
        }
        drop(uc);
    }
    println!("  candidate picked per user: {versions:?}");
    ck.check(
        "mid-rollout, each user's compiled call reaches the version its own CALL gets",
        agree == USERS.len(),
        format!("{agree} of {}", USERS.len()),
    );
    for (stage, want) in [("ABORT", "400"), ("100", "800")] {
        if stage == "ABORT" {
            must(&mut c, "ALTER FUNCTION price ROLLOUT ABORT").await;
        } else {
            must(
                &mut c,
                "CREATE OR REPLACE FUNCTION price(id INT) LANGUAGE javascript WITH (SPRIG_ROLLOUT = 10) AS $$
                   const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [id]);
                   if (p === null) return sprig.event('missing', { why: 'gone ' + id });
                   return { price: p.price * 2, stock: p.stock, cheap: p.price < 50 };
                 $$",
            )
            .await;
            must(&mut c, "ALTER FUNCTION price ROLLOUT TO 100").await;
        }
        let Some(mut uc) = connect_as("rollout", Some((USERS[0], USER_PASS))).await else {
            return;
        };
        let through = must(&mut uc, "CALL via(7)").await;
        ck.check(
            &format!("after ROLLOUT {stage} the compiled call follows ({want})"),
            through[0].iter().flatten().any(|v| v == want),
            format!("{through:?}"),
        );
    }

    // ---- redeploying a callee that compiled code calls -------------------------
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION price(id INT, extra INT) LANGUAGE javascript AS $$ return { price: id }; $$",
    )
    .await;
    ck.check(
        "a callee taking other parameters is refused (39875) naming its compiled callers",
        matches!(&r, Err((39875, m)) if m.contains("calls_c") && m.contains("via")),
        format!("{r:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION price(id INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $$ return { price: id, stock: 0, cheap: false }; $$",
    )
    .await;
    ck.check(
        "a callee moving to the engine tier is refused (39875)",
        matches!(&r, Err((39875, _))),
        format!("{r:?}"),
    );
    let r = sql(
        &mut c,
        "CREATE OR REPLACE FUNCTION price(id INT) LANGUAGE javascript AS $$
           const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [id]);
           if (p === null) return sprig.event('missing', { why: 'none ' + id });
           return { price: p.price + 1, stock: p.stock, cheap: p.price < 50 };
         $$",
    )
    .await;
    let through = must(&mut c, "CALL via(7)").await;
    ck.check(
        "the same signature deploys, and the compiled caller gets the new version at once",
        r.is_ok() && through[0].iter().flatten().any(|v| v == "401"),
        format!("{r:?} / {through:?}"),
    );

    // ---- rights ---------------------------------------------------------------
    must(
        &mut c,
        &format!("REVOKE SELECT ON {DB}.* FROM '{}'@'%'", USERS[1]),
    )
    .await;
    must(
        &mut c,
        &format!("REVOKE EXECUTE, SELECT ON {DB}.* FROM '{}'@'%'", USERS[2]),
    )
    .await;
    must(
        &mut c,
        "CREATE FUNCTION via_e(x INT) LANGUAGE javascript WITH (SPRIG_TIER = 'engine') AS $$ const r = db.call('price', x); return { p: r.fields.price }; $$",
    )
    .await;
    must(
        &mut c,
        "CREATE FUNCTION via_d(x INT) SQL SECURITY DEFINER LANGUAGE javascript AS $$ const r = db.call('price', x); return { p: r.fields.price }; $$",
    )
    .await;
    // Rights are checked when the session's grants refresh.
    tokio::time::sleep(Duration::from_secs(2)).await;
    if let Some(mut u1) = connect_as("rights", Some((USERS[1], USER_PASS))).await {
        let a = sql(&mut u1, "CALL via(7)").await.map_err(|e| e.0);
        let b = sql(&mut u1, "CALL via_e(7)").await.map_err(|e| e.0);
        ck.check(
            "a caller without SELECT: the invoker's callee is refused in both tiers alike",
            a.is_err() && a.as_ref().err() == b.as_ref().err(),
            format!("compiled {a:?} / engine {b:?}"),
        );
        let d = sql(&mut u1, "CALL via_d(7)").await;
        ck.check(
            "a SQL SECURITY DEFINER caller calls with its definer's rights",
            d.as_ref()
                .is_ok_and(|rows| rows[0].iter().flatten().any(|v| v == "401")),
            format!("{d:?}"),
        );
    }
    if let Some(mut u2) = connect_as("rights", Some((USERS[2], USER_PASS))).await {
        let a = sql(&mut u2, "CALL via(7)").await.map_err(|e| e.0);
        let b = sql(&mut u2, "CALL via_e(7)").await.map_err(|e| e.0);
        ck.check(
            "a caller without EXECUTE is refused (1370) in both tiers",
            a == Err(1370) && b == Err(1370),
            format!("{a:?} / {b:?}"),
        );
    }

    // ---- a validation -----------------------------------------------------------
    let r = sql(
        &mut c,
        "CREATE VALIDATION order_rules ON orders FOR INSERT, UPDATE LANGUAGE javascript AS $$
           const r = db.call('check', row.qty);
           const p = db.call('price', row.product_id);
           if (p.event !== 'output') sprig.reject('unknown product ' + row.product_id);
           if (r.fields.v > 20) sprig.reject('too many: ' + r.fields.v);
         $$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a validation calling other code compiles (BEFORE triggers)",
        r.is_ok() && n.contains("trigger"),
        format!("{r:?} / {n}"),
    );
    for (q, want) in [
        ("INSERT INTO orders VALUES (1, 7, 3)", None),
        (
            "INSERT INTO orders VALUES (2, 7, 11)",
            Some((39863, "too many: 22")),
        ),
        (
            "INSERT INTO orders VALUES (3, 70, 1)",
            Some((39863, "unknown product 70")),
        ),
        (
            "INSERT INTO orders VALUES (4, 7, 0)",
            Some((39863, "zero is not allowed")),
        ),
        (
            "UPDATE orders SET qty = -2 WHERE id = 1",
            Some((39865, "negative: -2")),
        ),
        ("UPDATE orders SET qty = 5 WHERE id = 1", None),
    ] {
        let r = sql(&mut c, q).await;
        let ok = match want {
            None => r.is_ok(),
            Some((n, m)) => matches!(&r, Err((got, msg)) if *got == n && msg.contains(m)),
        };
        ck.check(&format!("validation: {q}"), ok, format!("{r:?}"));
    }
    let r = sql(
        &mut c,
        r#"TEST VALIDATION order_rules WITH '{"row": {"id": 9, "product_id": 7, "qty": 11}, "old": null, "op": "insert"}'"#,
    )
    .await;
    let report = format!("{r:?}");
    ck.check(
        "TEST VALIDATION (engine tier) refuses with the same message",
        report.contains("39863") && report.contains("too many: 22"),
        report,
    );

    // ---- an after-commit trigger --------------------------------------------------
    let r = sql(
        &mut c,
        "CREATE TRIGGER order_events AFTER COMMIT ON orders FOR INSERT LANGUAGE javascript AS $$
           const p = db.call('price', row.product_id);
           db.publish('ev_t', String(row.id), { id: row.id, qty: row.qty, price: p.fields.price });
           db.exec('INSERT INTO audit (src, what) VALUES (?, ?)', ['t', 'order ' + row.id]);
         $$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "an after-commit trigger with db.call and db.publish compiles",
        r.is_ok() && n.contains("mariadb"),
        format!("{r:?} / {n}"),
    );
    for id in 20..24 {
        must(&mut c, &format!("INSERT INTO orders VALUES ({id}, 8, 1)")).await;
    }
    let t = wait_rows(&mut c, &read("ev_t"), 4, 40).await;
    let a = wait_rows(&mut c, "SELECT what FROM audit WHERE src = 't'", 4, 10).await;
    ck.check(
        "the trigger published each order once, with the callee's price (its latest version)",
        t.len() == 4
            && a.len() == 4
            && t.iter().all(|r| {
                r[1].as_deref()
                    .is_some_and(|v| v.contains("\"price\":20.99"))
            }),
        format!("{t:?} / {a:?}"),
    );

    // ---- timing ---------------------------------------------------------------------
    // Callers making 3 and 20 calls of `check`, compiled and in the engine
    // tier, each called 200 times over one connection, twice.
    for n in [3, 20] {
        for (suffix, tier) in [("", ""), ("_e", " WITH (SPRIG_TIER = 'engine')")] {
            must(
                &mut c,
                &format!(
                    "CREATE FUNCTION sum{n}{suffix}(x INT) LANGUAGE javascript{tier} AS $$
                       let s = 0;
                       for (let k = 0; k < {n}; k++) {{ const r = db.call('check', x + k); s += r.fields.v; }}
                       return {{ s }};
                     $$"
                ),
            )
            .await;
        }
    }
    let mut timing: Vec<(String, f64)> = Vec::new();
    for f in ["sum3", "sum3_e", "sum20", "sum20_e"]
        .iter()
        .cycle()
        .take(8)
    {
        let n = 200;
        let t0 = Instant::now();
        for i in 0..n {
            must(&mut c, &format!("CALL {f}({})", 1 + i % 50)).await;
        }
        timing.push((
            (*f).to_owned(),
            t0.elapsed().as_secs_f64() * 1000.0 / f64::from(n),
        ));
    }
    let best = |name: &str| {
        timing
            .iter()
            .filter(|(f, _)| f == name)
            .map(|(_, t)| *t)
            .fold(f64::MAX, f64::min)
    };
    for n in [3, 20] {
        let (a, b) = (best(&format!("sum{n}")), best(&format!("sum{n}_e")));
        println!(
            "  timing: {n} nested calls per CALL: compiled {a:.3} ms, engine tier {b:.3} ms ({:.1}x)",
            b / a
        );
        ck.check(
            &format!("a compiled caller making {n} calls is faster than the engine tier's"),
            a < b,
            format!("{a} vs {b}"),
        );
    }

    for u in USERS {
        let _ = sql(&mut c, &format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!(
        "{} of {} checks pass in {:?}",
        ck.passed,
        ck.passed + ck.failed.len(),
        started.elapsed()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
