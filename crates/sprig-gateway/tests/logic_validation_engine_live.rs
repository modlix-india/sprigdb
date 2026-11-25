//! Validations on engine tables through a live gateway: `CREATE
//! VALIDATION` in KIRun, JavaScript and Lua, checked before each row is
//! written by every form of write (single and multi-row inserts, updates,
//! upserts, `REPLACE`, `INSERT IGNORE`, `INSERT ... SELECT`, prepared
//! statements, multi-table updates), inside and outside transactions,
//! after SQL `BEFORE` triggers, with reads of other tables, budgets and
//! refused writes; from logic functions and, when given, over HTTP,
//! MongoDB and Redis.
//!
//! Needs a gateway whose new databases are stored on the engine, and a
//! tenant user who may create databases, triggers and routines:
//!
//! ```sh
//! export SPRIG_TEST_VALID_GATEWAY=127.0.0.1:61307        # MySQL protocol
//! export SPRIG_TEST_VALID_USER=e0sql.app SPRIG_TEST_VALID_PASSWORD=e0sql-app
//! export SPRIG_TEST_VALID_HTTP=127.0.0.1:61081           # optional
//! export SPRIG_TEST_VALID_MONGO='mongodb://...'          # optional
//! export SPRIG_TEST_VALID_REDIS='redis://...'            # optional
//! cargo test -p sprig-gateway --test logic_validation_engine_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the database `rvalid_live`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::many_single_char_names,
    missing_docs
)]

use std::time::Instant;

use base64::Engine as _;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, SslOpts};
use serde_json::{Value, json};
use sprig_testkit::skip;

const GATEWAY: &str = "SPRIG_TEST_VALID_GATEWAY";
const DB: &str = "rvalid_live";

fn user() -> String {
    std::env::var("SPRIG_TEST_VALID_USER").unwrap_or_else(|_| "acme.app".into())
}

fn password() -> String {
    std::env::var("SPRIG_TEST_VALID_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into())
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

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
        .user(Some(user()))
        .pass(Some(password()))
        .prefer_socket(false)
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

/// The first column of every row, joined with commas.
async fn col(c: &mut Conn, q: &str) -> String {
    must(c, q)
        .await
        .iter()
        .map(|r| {
            r.first()
                .cloned()
                .flatten()
                .unwrap_or_else(|| "NULL".into())
        })
        .collect::<Vec<_>>()
        .join(",")
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

    /// `r` is the error `code` with `msg` in its message (`None`: success).
    fn outcome(&mut self, name: &str, r: &Result<Rows, (u16, String)>, want: Option<(u16, &str)>) {
        let ok = match want {
            None => r.is_ok(),
            Some((code, msg)) => matches!(r, Err((c, m)) if *c == code && m.contains(msg)),
        };
        self.check(name, ok, format!("{r:?}"));
    }
}

const QTY_RULE: &str = r#"{"namespace": "Test", "name": "QtyPositive",
 "parameters": {"qty": {"parameterName": "qty", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {}},
            "reject": {"name": "reject", "parameters": {"message": {"type": ["STRING"]}}}},
 "steps": {
  "bad": {"statementName": "bad", "namespace": "System", "name": "If",
    "parameterMap": {"condition": {"c": {"type": "EXPRESSION", "expression": "Arguments.qty <= 0"}}}},
  "no": {"statementName": "no", "namespace": "System", "name": "GenerateEvent",
    "executeIftrue": {"Steps.bad.true": true},
    "parameterMap": {"eventName": {"e": {"type": "VALUE", "value": "reject"}},
      "results": {"r": {"type": "VALUE", "value": {"name": "message", "value": {"isExpression": false, "value": "qty must be positive"}}}}}}}}"#;

const BAD_PARAM: &str = r#"{"namespace": "Test", "name": "Bad",
 "parameters": {"bogus": {"parameterName": "bogus", "schema": {"type": ["INTEGER"]}}},
 "events": {"output": {"name": "output", "parameters": {}}},
 "steps": {}}"#;

const BOOK_JS: &str = "export default function (row, old) {
  if (row.discount_pct > 0) {
    const limit = row.total >= 1000 ? 20 : 10;
    if (row.discount_pct > limit)
      sprig.reject(`discount ${row.discount_pct}% is above ${limit}%`);
  }
  if (old !== null && old.status === \"shipped\" && row.total !== old.total)
    sprig.reject(\"the total of a shipped order can't change\");
}";

const SHIPPED_LUA: &str = "return function (ctx)
  local old, row = ctx.old, ctx.row
  if old ~= sprig.null and old.status == 'shipped' and row.total ~= old.total then
    sprig.reject(\"the total of a shipped order can't change from \" .. old.total)
  end
end";

const STOCK_JS: &str =
    "const p = db.sqlOne('SELECT stock FROM products WHERE id = ?', [row.product_id]);
if (p === null) sprig.reject('unknown product ' + row.product_id);
if (row.qty > p.stock) sprig.reject('only ' + p.stock + ' left');";

async fn deploy(ck: &mut Checks, c: &mut Conn, what: &str, q: &str) {
    let r = sql(c, q).await;
    ck.check(&format!("deploy {what}"), r.is_ok(), format!("{r:?}"));
}

fn basic() -> String {
    let token =
        base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", user(), password()));
    format!("Basic {token}")
}

async fn rest(addr: &str, path: &str, body: &Value) -> (u16, Value) {
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", addr)
        .header("authorization", basic())
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let (mut tx, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    tokio::spawn(conn);
    let resp = tx.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let b = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// Milliseconds per row of `n` one-row inserts made by `q(i)`.
async fn per_row(c: &mut Conn, n: u32, q: impl Fn(u32) -> String) -> f64 {
    let started = Instant::now();
    for i in 0..n {
        must(c, &q(i)).await;
    }
    started.elapsed().as_secs_f64() * 1000.0 / f64::from(n)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validations_on_engine_tables() {
    let Some(mut c) = connect("validations_on_engine_tables").await else {
        return;
    };
    let mut ck = Checks {
        passed: 0,
        failed: Vec::new(),
    };
    let _ = sql(&mut c, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut c, &format!("CREATE DATABASE {DB}")).await;
    must(&mut c, &format!("USE {DB}")).await;
    for t in [
        "CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, qty INT NOT NULL, note VARCHAR(32))",
        "CREATE TABLE items (id INT PRIMARY KEY, qty INT)",
        "CREATE TABLE products (id INT PRIMARY KEY, stock INT)",
        "CREATE TABLE stock_orders (id INT PRIMARY KEY, product_id INT, qty INT)",
        "CREATE TABLE book (id INT PRIMARY KEY, status VARCHAR(16), total DECIMAL(12,2) NOT NULL, discount_pct DECIMAL(5,2) NULL)",
        "CREATE TABLE lua_t (id INT PRIMARY KEY, status VARCHAR(16), total DECIMAL(10,2))",
        "CREATE TABLE trig_t (id INT PRIMARY KEY, qty INT)",
        "CREATE TABLE audit (id INT AUTO_INCREMENT PRIMARY KEY, what VARCHAR(64))",
        "CREATE TABLE odd (id INT PRIMARY KEY, n INT)",
        "CREATE TABLE plain (id INT PRIMARY KEY, qty INT NOT NULL)",
        "CREATE TABLE fast_k (id INT PRIMARY KEY, qty INT NOT NULL)",
        "CREATE TABLE fast_js (id INT PRIMARY KEY, qty INT NOT NULL)",
    ] {
        must(&mut c, t).await;
    }
    must(&mut c, "INSERT INTO products VALUES (7, 10), (8, 0)").await;

    // ---- declarations ---------------------------------------------------------
    let r = sql(
        &mut c,
        &format!("CREATE VALIDATION v_none ON nosuch FOR INSERT LANGUAGE kirun AS '{QTY_RULE}'"),
    )
    .await;
    ck.outcome("a missing table is 1146", &r, Some((1146, "nosuch")));
    let r = sql(
        &mut c,
        &format!("CREATE VALIDATION v_bad ON orders FOR INSERT LANGUAGE kirun AS '{BAD_PARAM}'"),
    )
    .await;
    ck.outcome(
        "a KIRun parameter naming no column is 39874",
        &r,
        Some((39874, "bogus")),
    );
    let r = sql(
        &mut c,
        "CREATE VALIDATION v_syntax ON orders FOR INSERT LANGUAGE javascript AS $$ if ( $$",
    )
    .await;
    ck.check(
        "JavaScript that doesn't parse is refused",
        matches!(&r, Err((39861, _))),
        format!("{r:?}"),
    );
    deploy(
        &mut ck,
        &mut c,
        "qty_positive (KIRun)",
        &format!(
            "CREATE VALIDATION qty_positive ON orders FOR INSERT, UPDATE LANGUAGE kirun AS '{QTY_RULE}'"
        ),
    )
    .await;
    let r = sql(
        &mut c,
        &format!(
            "CREATE VALIDATION qty_positive ON orders FOR INSERT LANGUAGE kirun AS '{QTY_RULE}'"
        ),
    )
    .await;
    ck.outcome(
        "a second declaration is 1304",
        &r,
        Some((1304, "already exists")),
    );

    // ---- every form of write ------------------------------------------------------
    let refused = Some((39863, "qty must be positive"));
    let r = sql(&mut c, "INSERT INTO orders VALUES (1, 7, 3, NULL)").await;
    ck.outcome("a valid insert", &r, None);
    let r = sql(&mut c, "INSERT INTO orders VALUES (2, 7, 0, NULL)").await;
    ck.outcome("an invalid insert is 39863", &r, refused);
    let r = sql(
        &mut c,
        "INSERT INTO orders VALUES (3, 7, 1, NULL), (4, 7, -1, NULL), (5, 7, 2, NULL)",
    )
    .await;
    ck.outcome("a multi-row insert with one bad row", &r, refused);
    let ids = col(&mut c, "SELECT id FROM orders ORDER BY id").await;
    ck.check("... writes none of its rows", ids == "1", &ids);
    let r = sql(&mut c, "UPDATE orders SET qty = -1 WHERE id = 1").await;
    ck.outcome("an invalid update", &r, refused);
    let r = sql(&mut c, "UPDATE orders SET note = 'x' WHERE id = 1").await;
    ck.outcome(
        "an update of another column (the row stays valid)",
        &r,
        None,
    );
    let q = col(&mut c, "SELECT qty FROM orders WHERE id = 1").await;
    ck.check("... and the refused update changed nothing", q == "3", &q);

    // Inside a transaction: the refused statement alone is undone.
    must(&mut c, "START TRANSACTION").await;
    let a = sql(&mut c, "INSERT INTO orders VALUES (6, 7, 2, NULL)").await;
    let b = sql(&mut c, "INSERT INTO orders VALUES (7, 7, 0, NULL)").await;
    let d = sql(&mut c, "INSERT INTO orders VALUES (8, 7, 5, NULL)").await;
    must(&mut c, "COMMIT").await;
    ck.check(
        "in a transaction: good, refused, good",
        a.is_ok() && matches!(b, Err((39863, _))) && d.is_ok(),
        format!("{a:?} {b:?} {d:?}"),
    );
    let ids = col(&mut c, "SELECT id FROM orders ORDER BY id").await;
    ck.check(
        "... the transaction commits the others",
        ids == "1,6,8",
        &ids,
    );
    must(&mut c, "START TRANSACTION").await;
    must(&mut c, "INSERT INTO orders VALUES (9, 7, 1, NULL)").await;
    let _ = sql(&mut c, "UPDATE orders SET qty = 0 WHERE id = 9").await;
    must(&mut c, "ROLLBACK").await;
    let ids = col(&mut c, "SELECT id FROM orders ORDER BY id").await;
    ck.check("... and a rollback undoes the rest", ids == "1,6,8", &ids);

    let r = sql(
        &mut c,
        "INSERT INTO orders VALUES (1, 7, 4, NULL) ON DUPLICATE KEY UPDATE qty = -2",
    )
    .await;
    ck.outcome("an upsert's invalid update", &r, refused);
    let r = sql(
        &mut c,
        "INSERT INTO orders VALUES (1, 7, 4, NULL) ON DUPLICATE KEY UPDATE qty = 9",
    )
    .await;
    ck.outcome("an upsert's valid update", &r, None);
    let r = sql(
        &mut c,
        "INSERT INTO orders VALUES (1, 7, 0, NULL) ON DUPLICATE KEY UPDATE qty = 5",
    )
    .await;
    ck.outcome(
        "an upsert's proposed row is checked first, as a BEFORE INSERT trigger is",
        &r,
        refused,
    );
    let r = sql(&mut c, "REPLACE INTO orders VALUES (1, 7, 0, NULL)").await;
    ck.outcome("an invalid REPLACE", &r, refused);
    let r = sql(&mut c, "INSERT IGNORE INTO orders VALUES (10, 7, 0, NULL)").await;
    ck.outcome("INSERT IGNORE doesn't ignore a refusal", &r, refused);
    let r = sql(
        &mut c,
        "INSERT INTO orders SELECT 11, 7, qty - 100, NULL FROM orders WHERE id = 1",
    )
    .await;
    ck.outcome("an invalid INSERT ... SELECT", &r, refused);
    let q = col(&mut c, "SELECT qty FROM orders WHERE id = 1").await;
    ck.check("... row 1 as the valid upsert left it", q == "9", &q);

    let p = c
        .exec_drop(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            (12, 7, 0, None::<String>),
        )
        .await;
    ck.check(
        "a prepared invalid insert",
        matches!(&p, Err(mysql_async::Error::Server(e)) if e.code == 39863),
        format!("{p:?}"),
    );
    let p = c
        .exec_drop(
            "INSERT INTO orders VALUES (?, ?, ?, ?)",
            (12, 7, 2, None::<String>),
        )
        .await;
    ck.check("a prepared valid insert", p.is_ok(), format!("{p:?}"));

    must(&mut c, "INSERT INTO items VALUES (1, -3), (6, 4)").await;
    let r = sql(
        &mut c,
        "UPDATE orders o JOIN items i ON i.id = o.id SET o.qty = i.qty",
    )
    .await;
    ck.outcome("an invalid multi-table update", &r, refused);
    must(&mut c, "UPDATE items SET qty = 5 WHERE id = 1").await;
    let r = sql(
        &mut c,
        "UPDATE orders o JOIN items i ON i.id = o.id SET o.qty = i.qty",
    )
    .await;
    ck.outcome("a valid multi-table update", &r, None);
    let q = col(
        &mut c,
        "SELECT qty FROM orders WHERE id IN (1, 6) ORDER BY id",
    )
    .await;
    ck.check("... wrote both rows", q == "5,4", &q);
    let r = sql(&mut c, "DELETE FROM orders WHERE id = 12").await;
    ck.outcome("a delete isn't checked", &r, None);

    // FOR INSERT only.
    deploy(
        &mut ck,
        &mut c,
        "stock_ok (JavaScript, FOR INSERT)",
        "CREATE VALIDATION stock_ok ON products FOR INSERT LANGUAGE javascript AS $$
           if (row.stock < 0) sprig.reject('negative stock');
         $$",
    )
    .await;
    let r = sql(&mut c, "INSERT INTO products VALUES (9, -1)").await;
    ck.outcome(
        "FOR INSERT checks an insert",
        &r,
        Some((39863, "negative stock")),
    );
    let r = sql(&mut c, "UPDATE products SET stock = -1 WHERE id = 8").await;
    ck.outcome("... and not an update", &r, None);
    must(&mut c, "UPDATE products SET stock = 0 WHERE id = 8").await;

    // ---- JavaScript: the design's worked example ------------------------------
    deploy(
        &mut ck,
        &mut c,
        "discount_rules (JavaScript)",
        &format!(
            "CREATE VALIDATION discount_rules ON book FOR INSERT, UPDATE LANGUAGE javascript AS $${BOOK_JS}$$"
        ),
    )
    .await;
    for (q, want) in [
        (
            "INSERT INTO book VALUES (1, 'new', 600, 25)",
            Some("discount 25% is above 10%"),
        ),
        (
            "INSERT INTO book VALUES (1, 'new', 1600, 25)",
            Some("discount 25% is above 20%"),
        ),
        ("INSERT INTO book VALUES (1, 'new', 1600, 15)", None),
        ("UPDATE book SET status = 'shipped' WHERE id = 1", None),
        (
            "UPDATE book SET total = 1700 WHERE id = 1",
            Some("the total of a shipped order can't change"),
        ),
        (
            "UPDATE book SET discount_pct = 12.5, total = 1600 WHERE id = 1",
            None,
        ),
    ] {
        let r = sql(&mut c, q).await;
        let ok = match want {
            Some(msg) => matches!(&r, Err((39863, got)) if got == msg),
            None => r.is_ok(),
        };
        ck.check(&format!("book: {q}"), ok, format!("{r:?}"));
    }

    // ---- Lua ---------------------------------------------------------------------
    deploy(
        &mut ck,
        &mut c,
        "ship_lua (Lua, FOR UPDATE)",
        &format!(
            "CREATE VALIDATION ship_lua ON lua_t FOR UPDATE LANGUAGE lua AS $${SHIPPED_LUA}$$"
        ),
    )
    .await;
    must(&mut c, "INSERT INTO lua_t VALUES (1, 'shipped', 800)").await;
    let r = sql(&mut c, "UPDATE lua_t SET total = 900 WHERE id = 1").await;
    ck.outcome(
        "Lua: a shipped total can't change",
        &r,
        Some((39863, "can't change from 800")),
    );
    let r = sql(
        &mut c,
        "UPDATE lua_t SET status = 'open', total = 900 WHERE id = 1",
    )
    .await;
    ck.check(
        "Lua: OLD is the stored row (status was shipped)",
        matches!(&r, Err((39863, _))),
        format!("{r:?}"),
    );
    must(&mut c, "UPDATE lua_t SET status = 'open' WHERE id = 1").await;
    let r = sql(&mut c, "UPDATE lua_t SET total = 900 WHERE id = 1").await;
    ck.outcome("Lua: an open order's total may change", &r, None);

    // ---- after SQL BEFORE triggers -------------------------------------------------
    for t in [
        "CREATE TRIGGER trig_abs BEFORE INSERT ON trig_t FOR EACH ROW SET NEW.qty = ABS(NEW.qty)",
        "CREATE TRIGGER trig_zero BEFORE UPDATE ON trig_t FOR EACH ROW BEGIN IF NEW.qty = 99 THEN SET NEW.qty = 0; END IF; END",
        "CREATE TRIGGER trig_audit AFTER INSERT ON trig_t FOR EACH ROW INSERT INTO audit (what) VALUES (CONCAT('ins ', NEW.id))",
    ] {
        must(&mut c, t).await;
    }
    deploy(
        &mut ck,
        &mut c,
        "trig_qty (KIRun)",
        &format!(
            "CREATE VALIDATION trig_qty ON trig_t FOR INSERT, UPDATE LANGUAGE kirun AS '{QTY_RULE}'"
        ),
    )
    .await;
    let r = sql(&mut c, "INSERT INTO trig_t VALUES (1, -4)").await;
    ck.outcome(
        "the validation sees NEW after the BEFORE trigger (-4 became 4)",
        &r,
        None,
    );
    let r = sql(&mut c, "INSERT INTO trig_t VALUES (2, 0)").await;
    ck.outcome("a row the trigger leaves invalid", &r, refused);
    let r = sql(&mut c, "UPDATE trig_t SET qty = 99 WHERE id = 1").await;
    ck.outcome("a BEFORE UPDATE trigger's NEW (99 became 0)", &r, refused);
    let a = col(&mut c, "SELECT what FROM audit ORDER BY id").await;
    ck.check(
        "AFTER triggers ran only for the row that was written",
        a == "ins 1",
        &a,
    );
    let q = col(&mut c, "SELECT qty FROM trig_t ORDER BY id").await;
    ck.check("... stored as the trigger changed it", q == "4", &q);

    // ---- reads, writes, budgets, events ------------------------------------------
    deploy(
        &mut ck,
        &mut c,
        "enough_stock (JavaScript reading products)",
        &format!(
            "CREATE VALIDATION enough_stock ON stock_orders FOR INSERT, UPDATE LANGUAGE javascript AS $${STOCK_JS}$$"
        ),
    )
    .await;
    let r = sql(&mut c, "INSERT INTO stock_orders VALUES (1, 7, 3)").await;
    ck.outcome("a validation reading another table: enough", &r, None);
    let r = sql(&mut c, "INSERT INTO stock_orders VALUES (2, 7, 30)").await;
    ck.outcome("... not enough", &r, Some((39863, "only 10 left")));
    let r = sql(&mut c, "INSERT INTO stock_orders VALUES (3, 70, 1)").await;
    ck.outcome("... unknown", &r, Some((39863, "unknown product 70")));
    deploy(
        &mut ck,
        &mut c,
        "odd_n (JavaScript)",
        "CREATE VALIDATION odd_n ON odd FOR INSERT, UPDATE LANGUAGE javascript AS $$
           if (row.n === 1) db.exec('DELETE FROM products');
           if (row.n === 2) { while (true) {} }
           if (row.n === 3) return sprig.event('error', { message: 'three is not allowed' });
           if (row.n === 4) throw new Error('four');
           if (row.n === 5) db.call('add_order', 30, 1);
         $$",
    )
    .await;
    deploy(
        &mut ck,
        &mut c,
        "add_order (a function that writes)",
        "CREATE FUNCTION add_order(id INT, qty INT) LANGUAGE javascript EXPOSE (sql, http, mongo, redis) AS $$
           db.exec('INSERT INTO orders VALUES (?, 7, ?, NULL)', [id, qty]);
           return { ok: true };
         $$",
    )
    .await;
    let r = sql(&mut c, "INSERT INTO odd VALUES (1, 1)").await;
    ck.outcome(
        "a validation may not write",
        &r,
        Some((39862, "may only read")),
    );
    let n = col(&mut c, "SELECT COUNT(*) FROM products").await;
    ck.check("... and wrote nothing", n == "2", &n);
    let started = Instant::now();
    let r = sql(&mut c, "INSERT INTO odd VALUES (2, 2)").await;
    let ms = started.elapsed().as_millis();
    ck.outcome(
        "an endless validation stops at logic.validation_budget_ms",
        &r,
        Some((39864, "logic.validation_budget_ms")),
    );
    ck.check("... within a second", ms < 1000, format!("{ms} ms"));
    let r = sql(&mut c, "INSERT INTO odd VALUES (3, 3)").await;
    ck.outcome(
        "an error event refuses",
        &r,
        Some((39863, "three is not allowed")),
    );
    let r = sql(&mut c, "INSERT INTO odd VALUES (4, 4)").await;
    ck.outcome("a thrown error is 39865", &r, Some((39865, "four")));
    let r = sql(&mut c, "INSERT INTO odd VALUES (5, 5)").await;
    ck.outcome(
        "a function a validation calls may not write either",
        &r,
        Some((39862, "may only read")),
    );
    let r = sql(&mut c, "INSERT INTO odd VALUES (6, 6)").await;
    ck.outcome("a row it lets through", &r, None);

    // ---- other ways in -----------------------------------------------------------
    let r = sql(&mut c, "CALL add_order(20, 0)").await;
    ck.outcome("a logic function's write is checked", &r, refused);
    let r = sql(&mut c, "CALL add_order(21, 2)").await;
    ck.outcome("... and a valid one goes in", &r, None);
    if let Some(http) = optional("SPRIG_TEST_VALID_HTTP") {
        let (s, v) = rest(
            &http,
            &format!("/v1/databases/{DB}/sql:execute"),
            &json!({"sql": "INSERT INTO orders VALUES (22, 7, 0, NULL)"}),
        )
        .await;
        ck.check(
            "HTTP Sql.Execute: refused with 39863",
            s != 200 && v.to_string().contains("39863"),
            format!("{s} {v}"),
        );
        let (s, v) = rest(
            &http,
            &format!("/v1/databases/{DB}/functions/add_order:call"),
            &json!({"args": [23, 0]}),
        )
        .await;
        ck.check(
            "HTTP Functions.Call: the function's write refused",
            s != 200 && v.to_string().contains("qty must be positive"),
            format!("{s} {v}"),
        );
        let (s, v) = rest(
            &http,
            &format!("/v1/databases/{DB}/sql:execute"),
            &json!({"sql": "INSERT INTO orders VALUES (24, 7, 1, NULL)"}),
        )
        .await;
        ck.check(
            "HTTP Sql.Execute: a valid row",
            s == 200,
            format!("{s} {v}"),
        );
    }
    if let Some(url) = optional("SPRIG_TEST_VALID_MONGO") {
        let client = mongodb::Client::with_uri_str(&url).await.unwrap();
        let r = client
            .database(DB)
            .run_command(mongodb::bson::doc! {"sprigCall": "add_order", "args": [25, 0]})
            .await;
        let code = match &r {
            Err(e) => match e.kind.as_ref() {
                mongodb::error::ErrorKind::Command(c) => c.code,
                _ => 0,
            },
            Ok(_) => 0,
        };
        ck.check(
            "MongoDB sprigCall: the function's write refused (121)",
            code == 121,
            format!("{r:?}"),
        );
    }
    if let Some(url) = optional("SPRIG_TEST_VALID_REDIS") {
        let client = redis::Client::open(url.as_str()).unwrap();
        let mut rc = client.get_multiplexed_async_connection().await.unwrap();
        let r: redis::RedisResult<redis::Value> = redis::cmd("FCALL")
            .arg("add_order")
            .arg(0)
            .arg(26)
            .arg(0)
            .query_async(&mut rc)
            .await;
        ck.check(
            "Redis FCALL: the function's write refused",
            r.as_ref()
                .is_err_and(|e| e.to_string().contains("qty must be positive")),
            format!("{r:?}"),
        );
    }
    let ids = col(&mut c, "SELECT id FROM orders WHERE id >= 20 ORDER BY id").await;
    ck.check(
        "only the valid rows from other ways in",
        ids == "21,24" || ids == "21",
        &ids,
    );

    // ---- replace and drop ----------------------------------------------------------
    deploy(
        &mut ck,
        &mut c,
        "OR REPLACE qty_positive (JavaScript, a new rule)",
        "CREATE OR REPLACE VALIDATION qty_positive ON orders FOR INSERT, UPDATE LANGUAGE javascript AS $$
           export default function (qty) { if (qty > 100) sprig.reject('at most 100'); }
         $$",
    )
    .await;
    let r = sql(&mut c, "INSERT INTO orders VALUES (40, 7, 0, NULL)").await;
    ck.outcome("the replaced rule lets 0 in", &r, None);
    let r = sql(&mut c, "INSERT INTO orders VALUES (41, 7, 101, NULL)").await;
    ck.outcome("... and checks its own", &r, Some((39863, "at most 100")));
    let r = sql(&mut c, "DROP VALIDATION qty_positive").await;
    ck.outcome("DROP VALIDATION", &r, None);
    let r = sql(&mut c, "INSERT INTO orders VALUES (41, 7, 101, NULL)").await;
    ck.outcome("... writes are no longer checked", &r, None);

    // ---- what a check costs ------------------------------------------------------
    deploy(
        &mut ck,
        &mut c,
        "fast_k (KIRun)",
        &format!("CREATE VALIDATION fast_k ON fast_k FOR INSERT LANGUAGE kirun AS '{QTY_RULE}'"),
    )
    .await;
    deploy(
        &mut ck,
        &mut c,
        "fast_js (JavaScript)",
        "CREATE VALIDATION fast_js ON fast_js FOR INSERT LANGUAGE javascript AS $$
           export default function (qty) { if (qty <= 0) sprig.reject('qty must be positive'); }
         $$",
    )
    .await;
    let n = 100;
    let plain = per_row(&mut c, n, |i| format!("INSERT INTO plain VALUES ({i}, 1)")).await;
    let kirun = per_row(&mut c, n, |i| format!("INSERT INTO fast_k VALUES ({i}, 1)")).await;
    let js = per_row(&mut c, n, |i| {
        format!("INSERT INTO fast_js VALUES ({i}, 1)")
    })
    .await;
    println!(
        "one-row inserts, ms per row ({n} each): no validation {plain:.2}, KIRun {kirun:.2}, JavaScript {js:.2}"
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!(
        "validations on engine tables: {} of {} checks pass",
        ck.passed,
        ck.passed + ck.failed.len()
    );
    assert!(ck.failed.is_empty(), "{:#?}", ck.failed);
}
