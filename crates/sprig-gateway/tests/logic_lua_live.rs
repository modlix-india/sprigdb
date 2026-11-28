//! Lua logic through a live gateway, compiled by the Lua front end. Each
//! compiled Lua function is deployed a second time with
//! `SPRIG_TIER = 'engine'`, and both must answer every call alike and leave
//! the same rows; Lua validations, after-commit triggers and computed
//! columns each have a JavaScript twin on their own table, and the two
//! must agree.
//!
//! Needs a gateway with workers (after-commit triggers run in a worker)
//! and a tenant user who may create databases and routines:
//!
//! ```sh
//! export SPRIG_TEST_LOGIC_GATEWAY=127.0.0.1:51306
//! export SPRIG_TEST_LOGIC_USER=acme.app SPRIG_TEST_LOGIC_PASSWORD=sprig-dev-app
//! cargo test -p sprig-gateway --test logic_lua_live -- --nocapture --test-threads 1
//! ```
//!
//! Works in the databases `lua_live_db` and `lua_msg_live`, dropped
//! afterwards. Lua triggers on streams and queues (decoded JSON) run
//! compiled and in the engine tier on the same records; the queue part
//! needs the gateway's AMQP listener (`SPRIG_TEST_LOGIC_AMQP`), and the
//! run log's lines are read as root of the deployment's own MariaDB when
//! `SPRIG_TEST_LOGIC_ROOT` and `SPRIG_TEST_LOGIC_TENANT` are given.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    // Short names for a call's two answers (`a`, `b`) and rows read the
    // way the other live tests name them.
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
const DB: &str = "lua_live_db";

fn user() -> (String, String) {
    (
        std::env::var("SPRIG_TEST_LOGIC_USER").unwrap_or_else(|_| "acme.app".into()),
        std::env::var("SPRIG_TEST_LOGIC_PASSWORD").unwrap_or_else(|_| "sprig-dev-app".into()),
    )
}

/// `q`'s rows once there are `n` of them (or after `secs`).
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

/// Functions written once, deployed twice: `<name>_c` (compiled) and
/// `<name>_e` (the engine tier), each writing its own table `<t>_c` or
/// `<t>_e`. `{t}` in the source names that table.
struct Twin {
    name: &'static str,
    params: &'static str,
    returns: &'static str,
    src: &'static str,
    calls: &'static [&'static str],
}

const TWINS: &[Twin] = &[
    Twin {
        name: "quote",
        params: "product_id BIGINT, qty INT",
        returns: "",
        src: "local p = db.sqlOne('SELECT price, stock FROM {t} WHERE id = ?', product_id)
if p == sprig.null or not (p.stock >= qty) then
  return sprig.event('error', { message = 'not enough stock for ' .. product_id })
end
return { total = p.price * qty + 40, cheap = p.price < 50 }",
        calls: &["7, 3", "7, 1000", "999, 1", "8, 2", "7, NULL"],
    },
    Twin {
        name: "take",
        params: "order_id BIGINT, product_id BIGINT, qty INT",
        returns: "",
        src: "local t = db.exec('UPDATE {t} SET stock = stock - ? WHERE id = ? AND stock >= ?', qty, product_id, qty)
if t.affected ~= 1 then error('out of stock: ' .. product_id) end
local left = db.sqlOne('SELECT stock FROM {t} WHERE id = ?', product_id)
local names = ''
for i, r in ipairs(db.sql('SELECT id, label FROM {t} WHERE stock > ? ORDER BY id', 0)) do
  if r.label ~= sprig.null then names = names .. i .. ':' .. r.label:upper() .. ';' end
end
return { left = left.stock, names = names, len = #names }",
        calls: &["1, 7, 2", "2, 8, 3", "3, 8, 1", "4, 9, 1"],
    },
    Twin {
        name: "grade",
        params: "score INT, name VARCHAR(32)",
        returns: "VARCHAR(64)",
        src: "return function (s, n)
  local function band(v)
    if v >= 90 then return 'A' end
    if v >= 50 then return 'pass' end
    return 'fail'
  end
  if s == sprig.null then return 'no score' end
  local ok, e = pcall(function ()
    if n == sprig.null then error('no name', 0) end
    return n
  end)
  local who = ok and e or ('anonymous (' .. e .. ')')
  return who .. ': ' .. band(s) .. ' ' .. s % 7
end",
        calls: &["95, 'Zoë'", "60, NULL", "NULL, 'x'", "-9, 'neg'"],
    },
];

async fn setup(c: &mut Conn) {
    for q in [
        format!("DROP DATABASE IF EXISTS {DB}"),
        format!("CREATE DATABASE {DB}"),
        format!("USE {DB}"),
        "CREATE TABLE stock_c (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, \
         stock INT NOT NULL, label VARCHAR(32) NULL)"
            .into(),
        "CREATE TABLE stock_e LIKE stock_c".into(),
        "INSERT INTO stock_c VALUES (7, 400.00, 10, 'lamp'), (8, 19.99, 3, NULL), (9, 0.10, 0, 'pin')".into(),
        "INSERT INTO stock_e SELECT * FROM stock_c".into(),
        "CREATE TABLE products (id BIGINT PRIMARY KEY, price DOUBLE NOT NULL, qty INT NOT NULL)"
            .into(),
        "CREATE TABLE sales_js (id BIGINT PRIMARY KEY, status VARCHAR(16) NOT NULL, \
         total DOUBLE NOT NULL, discount_pct DOUBLE NULL)"
            .into(),
        "CREATE TABLE sales_lua LIKE sales_js".into(),
        "CREATE TABLE items_js (id BIGINT PRIMARY KEY, qty INT NULL)".into(),
        "CREATE TABLE items_lua LIKE items_js".into(),
        "CREATE TABLE audit_js (id BIGINT NOT NULL, op VARCHAR(8) NOT NULL, qty INT NULL, \
         prev_qty INT NULL, PRIMARY KEY (id, op))"
            .into(),
        "CREATE TABLE audit_lua LIKE audit_js".into(),
    ] {
        must(c, &q).await;
    }
}

const DISCOUNT_JS: &str = "export default function (row) {
  if (row.discount_pct > 20) sprig.reject('discount above 20%');
}";

const DISCOUNT_LUA: &str = "local d = ctx.row.discount_pct
if d ~= sprig.null and d > 20 then sprig.reject('discount above 20%') end";

const SHIPPED_JS: &str = "if (old !== null && old.status === 'shipped' && row.total !== old.total)
  sprig.reject(\"the total of a shipped order can't change from \" + old.total);";

const SHIPPED_LUA: &str = "return function (ctx)
  local old, row = ctx.old, ctx.row
  if old ~= sprig.null and old.status == 'shipped' and row.total ~= old.total then
    sprig.reject(\"the total of a shipped order can't change from \" .. old.total)
  end
end";

const AUDIT_JS: &str = "export default function (row, old, op) {
  db.exec('INSERT INTO audit_js (id, op, qty, prev_qty) VALUES (?, ?, ?, ?)',
          [row.id, op, row.qty, old === null ? null : old.qty]);
}";

const AUDIT_LUA: &str = "local old = ctx.trigger.before
local prev = nil
if old ~= sprig.null then prev = old.qty end
db.exec('INSERT INTO audit_lua (id, op, qty, prev_qty) VALUES (?, ?, ?, ?)',
        ctx.row.id, ctx.trigger.op, ctx.row.qty, prev)";

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD, ...; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lua_compiles_and_matches_the_engine_tier() {
    let Some(mut c) = connect("lua_compiles_and_matches_the_engine_tier").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let started = Instant::now();
    setup(&mut c).await;
    let mut ck = Checks::new();

    // ---- functions: compiled against the engine tier ----------------------------
    for t in TWINS {
        for (suffix, tier, table) in [
            ("c", "", "stock_c"),
            ("e", " WITH (SPRIG_TIER = 'engine')", "stock_e"),
        ] {
            let returns = if t.returns.is_empty() {
                String::new()
            } else {
                format!(" RETURNS {}", t.returns)
            };
            let q = format!(
                "CREATE FUNCTION {}_{suffix}({}){returns} LANGUAGE lua{tier} AS $${}$$",
                t.name,
                t.params,
                t.src.replace("{t}", table)
            );
            let r = sql(&mut c, &q).await;
            let n = notes(&mut c).await;
            let want = if suffix == "c" {
                "tier mariadb"
            } else {
                "tier engine"
            };
            ck.check(
                &format!("{}_{suffix} deploys, {want}", t.name),
                r.is_ok() && n.contains(want),
                format!("{r:?} / {n}"),
            );
        }
        for args in t.calls {
            let a = sql(&mut c, &format!("CALL {}_c({args})", t.name)).await;
            let b = sql(&mut c, &format!("CALL {}_e({args})", t.name)).await;
            let same = match (&a, &b) {
                (Ok(x), Ok(y)) => x == y,
                (Err((x, _)), Err((y, _))) => x == y,
                _ => false,
            };
            ck.check(
                &format!("CALL {}({args}): compiled and engine tier agree", t.name),
                same,
                format!("compiled {a:?} / engine {b:?}"),
            );
        }
    }
    let tc = must(&mut c, "SELECT * FROM stock_c ORDER BY id").await;
    let te = must(&mut c, "SELECT * FROM stock_e ORDER BY id").await;
    ck.check(
        "compiled and engine-tier writes left the same rows",
        tc == te,
        format!("{tc:?} / {te:?}"),
    );
    let s = sql(&mut c, "SELECT grade_c(95, 'Zoë'), grade_e(95, 'Zoë')").await;
    ck.check(
        "SELECT of the compiled and the engine-tier function agree",
        matches!(&s, Ok(r) if r[0][0] == r[0][1] && r[0][0].as_deref() == Some("Zoë: A 4")),
        format!("{s:?}"),
    );
    // A compiled caller of a compiled callee.
    let r = sql(
        &mut c,
        "CREATE FUNCTION total_c(product_id BIGINT, n INT) LANGUAGE lua AS $$
local a = db.call('quote_c', product_id, n)
if a.event ~= 'output' then return sprig.event('missing', { why = a.fields.message }) end
return { total = a.fields.total, twice = a.fields.total * 2 }
$$",
    )
    .await;
    let n = notes(&mut c).await;
    ck.check(
        "a Lua caller of a compiled Lua function compiles",
        r.is_ok() && n.contains("tier mariadb"),
        format!("{r:?} / {n}"),
    );
    let a = sql(&mut c, "CALL total_c(7, 2)").await;
    let b = sql(&mut c, "CALL total_c(999, 2)").await;
    ck.check(
        "db.call between compiled Lua functions",
        matches!(&a, Ok(r) if r[0][0].as_deref() == Some("output") && r[0][1].as_deref() == Some("840"))
            && matches!(&b, Ok(r) if r[0][0].as_deref() == Some("missing")),
        format!("{a:?} / {b:?}"),
    );
    // Outside the subset: the engine tier, with the reason.
    let r = sql(
        &mut c,
        "CREATE FUNCTION keys_e(n INT) LANGUAGE lua AS $$
local t, out = { b = 2, a = 1 }, 0
for k, v in pairs(t) do out = out + v end
return { sum = out + n }
$$",
    )
    .await;
    let n = notes(&mut c).await;
    let call = sql(&mut c, "CALL keys_e(4)").await;
    ck.check(
        "a Lua function outside the subset runs in the engine tier, with lua.pairs",
        r.is_ok()
            && n.contains("tier engine")
            && n.contains("lua.pairs")
            && matches!(&call, Ok(rows) if rows[0][1].as_deref() == Some("7")),
        format!("{r:?} / {n} / {call:?}"),
    );

    // ---- computed columns --------------------------------------------------------
    must(
        &mut c,
        "CREATE FUNCTION net_lua(price DOUBLE, qty INT) RETURNS DOUBLE LANGUAGE lua AS $$
if price == sprig.null or qty == sprig.null then return nil end
if qty >= 10 then return price * qty * 0.9 end
return price * qty
$$",
    )
    .await;
    for kind in ["STORED", "VIRTUAL"] {
        let r = sql(
            &mut c,
            &format!(
                "ALTER TABLE products ADD COLUMN {}_lua DOUBLE AS (net_lua(price, qty)) {kind}",
                kind.to_ascii_lowercase()
            ),
        )
        .await;
        ck.check(
            &format!("a computed column from a Lua function ({kind})"),
            r.is_ok(),
            format!("{r:?}"),
        );
    }
    let vals: Vec<String> = (1..=12_i64)
        .map(|i| format!("({i}, {}.25, {})", i, (i * 5) % 17))
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
        "SELECT ROUND(stored_lua, 9), ROUND(virtual_lua, 9), \
         ROUND(IF(qty >= 10, price * qty * 0.9, price * qty), 9) FROM products ORDER BY id",
    )
    .await;
    let agree = rows.iter().filter(|r| r[0] == r[1] && r[1] == r[2]).count();
    ck.check(
        "the Lua computed columns equal the formula on every row",
        agree == 12,
        format!("{agree} of {}: {rows:?}", rows.len()),
    );
    let r = sql(
        &mut c,
        "CREATE FUNCTION raw_lua(price DOUBLE) RETURNS DOUBLE LANGUAGE lua AS $$ return price * 2 $$",
    )
    .await;
    let col = sql(
        &mut c,
        "ALTER TABLE products ADD COLUMN raw DOUBLE AS (raw_lua(price)) VIRTUAL",
    )
    .await;
    ck.check(
        "a Lua function that raises Lua's error on NULL makes no computed column (39862)",
        r.is_ok() && matches!(&col, Err((39862, _))),
        format!("{r:?} / {col:?}"),
    );

    // ---- validations ---------------------------------------------------------------
    for (q, want) in [
        (
            format!(
                "CREATE VALIDATION disc_js ON sales_js FOR INSERT, UPDATE LANGUAGE javascript AS $${DISCOUNT_JS}$$"
            ),
            "enforced by CHECK",
        ),
        (
            format!(
                "CREATE VALIDATION disc_lua ON sales_lua FOR INSERT, UPDATE LANGUAGE lua AS $${DISCOUNT_LUA}$$"
            ),
            "enforced by CHECK",
        ),
        (
            format!(
                "CREATE VALIDATION ship_js ON sales_js FOR UPDATE LANGUAGE javascript AS $${SHIPPED_JS}$$"
            ),
            "enforced by BEFORE triggers",
        ),
        (
            format!(
                "CREATE VALIDATION ship_lua ON sales_lua FOR UPDATE LANGUAGE lua AS $${SHIPPED_LUA}$$"
            ),
            "enforced by BEFORE triggers",
        ),
    ] {
        let r = sql(&mut c, &q).await;
        let n = notes(&mut c).await;
        ck.check(
            &format!("{}: deployed, {want}", &q[..42]),
            r.is_ok() && n.contains(want),
            format!("{r:?} / {n}"),
        );
    }
    let script = [
        "INSERT INTO {t} VALUES (1, 'new', 100, 10)",
        "INSERT INTO {t} VALUES (2, 'new', 100, 25)",
        "INSERT INTO {t} VALUES (3, 'shipped', 50, NULL)",
        "INSERT INTO {t} VALUES (4, 'new', 10, 20.5), (5, 'new', 10, 1)",
        "UPDATE {t} SET discount_pct = 30 WHERE id = 1",
        "UPDATE {t} SET total = 60 WHERE id = 3",
        "UPDATE {t} SET status = 'done' WHERE id = 3",
        "UPDATE {t} SET total = 70 WHERE id = 3",
        "UPDATE {t} SET total = 200 WHERE id = 1",
        "UPDATE {t} SET discount_pct = 21 WHERE id IN (1, 3)",
        "UPDATE {t} SET status = 'shipped' WHERE id = 1",
        "UPDATE {t} SET total = total + 1, discount_pct = 5 WHERE id IN (1, 5)",
        "INSERT INTO {t} VALUES (6, 'new', 1, 20)",
    ];
    let mut agree = 0;
    let mut refused = 0;
    for s in script {
        let js = sql(&mut c, &s.replace("{t}", "sales_js")).await.map(|_| ());
        let lua = sql(&mut c, &s.replace("{t}", "sales_lua"))
            .await
            .map(|_| ());
        if js == lua {
            agree += 1;
        } else {
            println!("  differ: {s}: javascript {js:?}, lua {lua:?}");
        }
        if matches!(lua, Err((39863, _))) {
            refused += 1;
        }
    }
    ck.check(
        "Lua and JavaScript validations give the same outcome and message for every write",
        agree == script.len(),
        format!("{agree} of {}", script.len()),
    );
    ck.check(
        "both refused the same 6 writes with 39863",
        refused == 6,
        format!("{refused} refused"),
    );
    let js = must(&mut c, "SELECT * FROM sales_js ORDER BY id").await;
    let lua = must(&mut c, "SELECT * FROM sales_lua ORDER BY id").await;
    ck.check(
        "the two tables hold the same rows",
        js == lua && lua.len() == 3,
        format!("{js:?} / {lua:?}"),
    );
    let r = sql(
        &mut c,
        r#"TEST VALIDATION ship_lua WITH '{"row": {"status": "shipped", "total": 900, "discount_pct": 0}, "old": {"status": "shipped", "total": 800, "discount_pct": 0}, "op": "update"}'"#,
    )
    .await;
    let report = r
        .as_ref()
        .ok()
        .and_then(|rows| rows.first())
        .and_then(|row| row.get(2).cloned().flatten())
        .unwrap_or_default();
    ck.check(
        "TEST VALIDATION runs the Lua source on the engine tier with the same message",
        report.contains("39863") && report.contains("can't change from 800"),
        format!("{r:?}"),
    );

    // ---- after-commit triggers ---------------------------------------------------
    for q in [
        format!(
            "CREATE TRIGGER audit_js_t AFTER COMMIT ON items_js FOR INSERT, UPDATE, DELETE LANGUAGE javascript AS $${AUDIT_JS}$$"
        ),
        format!(
            "CREATE TRIGGER audit_lua_t AFTER COMMIT ON items_lua FOR INSERT, UPDATE, DELETE LANGUAGE lua AS $${AUDIT_LUA}$$"
        ),
    ] {
        let r = sql(&mut c, &q).await;
        let n = notes(&mut c).await;
        ck.check(
            &format!("{}: deployed", &q[..32]),
            r.is_ok() && n.contains("after-commit trigger"),
            format!("{r:?} / {n}"),
        );
    }
    for t in ["items_js", "items_lua"] {
        let vals: Vec<String> = (1..=10)
            .map(|i| {
                if i == 4 {
                    format!("({i}, NULL)")
                } else {
                    format!("({i}, {i})")
                }
            })
            .collect();
        must(
            &mut c,
            &format!("INSERT INTO {t} VALUES {}", vals.join(", ")),
        )
        .await;
        must(
            &mut c,
            &format!("UPDATE {t} SET qty = COALESCE(qty, 0) + 100 WHERE id <= 5"),
        )
        .await;
        must(&mut c, &format!("DELETE FROM {t} WHERE id BETWEEN 6 AND 8")).await;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    let (mut js, mut lua) = (Vec::new(), Vec::new());
    while Instant::now() < deadline {
        js = must(
            &mut c,
            "SELECT id, op, qty, prev_qty FROM audit_js ORDER BY id, op",
        )
        .await;
        lua = must(
            &mut c,
            "SELECT id, op, qty, prev_qty FROM audit_lua ORDER BY id, op",
        )
        .await;
        if js.len() >= 18 && lua.len() >= 18 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    ck.check(
        "both triggers wrote one audit row per change (18)",
        js.len() == 18 && lua.len() == 18,
        format!("javascript {}, lua {}", js.len(), lua.len()),
    );
    ck.check(
        "the Lua trigger's audit rows equal the JavaScript trigger's",
        js == lua,
        format!("{js:?} / {lua:?}"),
    );

    must(&mut c, &format!("DROP DATABASE {DB}")).await;
    println!("took {:.1} s", started.elapsed().as_secs_f64());
    ck.done("Lua logic");
}

/// The Lua stream trigger of the message-trigger test: `{src}` names the
/// twin (`c` compiled, `e` the engine tier).
const STREAM_LUA: &str = "local r = ctx.record
local e = cjson.decode(r.value)
local kind = e.kind or 'none'
local n = 0
if type(e.items) == 'table' then
  for _, it in ipairs(e.items) do n = n + it.qty end
end
local who = 'anon'
if e.user ~= nil and e.user ~= cjson.null then who = e.user.name or 'anon' end
db.exec('INSERT INTO hits (src, k, what, n) VALUES (?, ?, ?, ?)', '{src}', r.key, kind .. '/' .. who, n)
print('ltrig {src}', r.key, kind, n, e.flag == nil)";

/// The Lua queue trigger: `{src}` as above.
const QUEUE_LUA: &str = "return function (c)
  local m = c.message
  local job = cjson.decode(m.body)
  local urgent = 0
  if m.properties.headers ~= nil and m.properties.headers.urgent then urgent = 1 end
  db.exec('INSERT INTO jobs (src, id, qty, urgent) VALUES (?, ?, ?, ?)', '{src}', job.id, job.qty, urgent)
  print('lq {src} job ' .. job.id .. ' x' .. job.qty)
end";

#[ignore = "live: needs SPRIG_TEST_LOGIC_GATEWAY, SPRIG_TEST_LOGIC_USER, SPRIG_TEST_LOGIC_PASSWORD, ...; tools/live/README.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lua_message_triggers_match_the_engine_tier() {
    const MDB: &str = "lua_msg_live";
    let Some(mut c) = connect("lua_message_triggers_match_the_engine_tier").await else {
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let started = Instant::now();
    let mut ck = Checks::new();
    for q in [
        format!("DROP DATABASE IF EXISTS {MDB}"),
        format!("CREATE DATABASE {MDB}"),
        format!("USE {MDB}"),
        "CREATE TABLE hits (id BIGINT AUTO_INCREMENT PRIMARY KEY, src VARCHAR(4) NOT NULL, \
         k VARCHAR(32) NULL, what VARCHAR(64) NULL, n DOUBLE NULL)"
            .into(),
        "CREATE TABLE jobs (src VARCHAR(4) NOT NULL, id BIGINT NOT NULL, qty INT NOT NULL, \
         urgent INT NOT NULL, PRIMARY KEY (src, id))"
            .into(),
        format!("CREATE STREAM {MDB}.ev PARTITIONS 2"),
    ] {
        must(&mut c, &q).await;
    }
    // ---- a stream trigger, compiled and in the engine tier, on one stream --------------------
    for (src, with, tier) in [
        ("c", "", "tier mariadb"),
        ("e", "WITH (SPRIG_TIER = 'engine')", "tier engine"),
    ] {
        let q = format!(
            "CREATE TRIGGER {MDB}.lt_{src} ON STREAM {MDB}.ev LANGUAGE lua {with} AS $${}$$",
            STREAM_LUA.replace("{src}", src)
        );
        let r = sql(&mut c, &q).await;
        let n = if r.is_ok() {
            notes(&mut c).await
        } else {
            String::new()
        };
        ck.check(
            &format!("a Lua stream trigger reading cjson.decode deploys ({tier})"),
            r.is_ok() && n.contains(tier),
            format!("{r:?} / {n}"),
        );
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    // `kind` a number: compiled code reads it as text, so the engine tier
    // runs that record (and gives Lua's answer: 5 .. '/' is '5/').
    must(
        &mut c,
        &format!(
            "INSERT INTO {MDB}.ev (key, value) VALUES \
             ('c1', '{{\"kind\": \"a\", \"items\": [{{\"qty\": 2}}, {{\"qty\": 3}}], \"user\": {{\"name\": \"ann\"}}}}'), \
             ('c2', '{{\"kind\": 5, \"flag\": null}}'), \
             ('c3', '{{\"kind\": \"b\", \"items\": [{{\"qty\": 1.5}}], \"user\": null}}'), \
             ('c4', '{{}}')"
        ),
    )
    .await;
    let q = |src: &str| format!("SELECT k, what, n FROM hits WHERE src = '{src}' ORDER BY k");
    let compiled = wait_rows(&mut c, &q("c"), 4, 90).await;
    let engine = wait_rows(&mut c, &q("e"), 4, 90).await;
    ck.check(
        "the compiled Lua stream trigger ran each record once, as its engine-tier twin did",
        compiled.len() == 4 && compiled == engine,
        format!("compiled {compiled:?} / engine {engine:?}"),
    );
    ck.check(
        "the rows are Lua's (a number kind joined as text, a nil and a cjson.null user)",
        compiled
            .iter()
            .map(|r| r[1].clone().unwrap_or_default())
            .collect::<Vec<_>>()
            == ["a/ann", "5/anon", "b/anon", "none/anon"],
        format!("{compiled:?}"),
    );
    if let (Some(root), Some(t)) = (
        skip::var("SPRIG_TEST_LOGIC_ROOT"),
        skip::var("SPRIG_TEST_LOGIC_TENANT"),
    ) {
        let mut r = Conn::from_url(root.as_str()).await.unwrap();
        let lines = |name: &str| {
            format!(
                "SELECT message FROM t{t}_sprig.logic_log WHERE level = 'info' \
                 AND message LIKE '%ltrig%' AND module_id = (SELECT module_id FROM \
                 t{t}_sprig.logic_modules WHERE name = '{name}' AND state = 'active') \
                 ORDER BY log_id"
            )
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let (lc, le) = loop {
            let lc: Vec<String> = r.query(lines("lt_c")).await.unwrap();
            let le: Vec<String> = r.query(lines("lt_e")).await.unwrap();
            if (lc.len() >= 4 && le.len() >= 4) || Instant::now() > deadline {
                break (lc, le);
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        let tail = |v: &[String]| -> Vec<String> {
            let mut t: Vec<String> = v
                .iter()
                .map(|l| {
                    l.split("ltrig ")
                        .nth(1)
                        .unwrap_or_default()
                        .trim_start_matches(['c', 'e'])
                        .to_owned()
                })
                .collect();
            t.sort();
            t
        };
        ck.check(
            "the compiled trigger's print lines reach its run log as the engine tier writes them",
            lc.len() == 4 && tail(&lc) == tail(&le),
            format!("{lc:?} / {le:?}"),
        );
        drop(r);
    }
    // ---- a queue trigger, compiled and in the engine tier, each on its own queue ------------
    if let Some(amqp) = skip::var("SPRIG_TEST_LOGIC_AMQP") {
        let (user, password) = user();
        let a = Connection::connect(
            &format!("amqp://{user}:{password}@{amqp}/{MDB}"),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();
        let ch = a.create_channel().await.unwrap();
        for src in ["c", "e"] {
            ch.queue_declare(
                format!("jobs_{src}").as_str().into(),
                QueueDeclareOptions {
                    durable: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .unwrap();
        }
        for (src, with, tier) in [
            ("c", "", "tier mariadb"),
            ("e", "WITH (SPRIG_TIER = 'engine')", "tier engine"),
        ] {
            let q = format!(
                "CREATE TRIGGER {MDB}.lq_{src} ON QUEUE {MDB}.jobs_{src} LANGUAGE lua {with} AS $${}$$",
                QUEUE_LUA.replace("{src}", src)
            );
            let r = sql(&mut c, &q).await;
            let n = if r.is_ok() {
                notes(&mut c).await
            } else {
                String::new()
            };
            ck.check(
                &format!("a Lua queue trigger deploys ({tier})"),
                r.is_ok() && n.contains(tier),
                format!("{r:?} / {n}"),
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let bodies = [
            (r#"{"id": 1, "qty": 2}"#, true),
            (r#"{"id": 2, "qty": 2.5}"#, false),
            (r#"{"id": "3", "qty": 1}"#, false),
            (r#"{"id": 4, "qty": 7}"#, false),
        ];
        for src in ["c", "e"] {
            for (b, urgent) in bodies {
                let mut headers = FieldTable::default();
                if urgent {
                    headers.insert("urgent".into(), lapin::types::AMQPValue::Boolean(true));
                }
                ch.basic_publish(
                    "".into(),
                    format!("jobs_{src}").as_str().into(),
                    BasicPublishOptions::default(),
                    b.as_bytes(),
                    BasicProperties::default()
                        .with_delivery_mode(2)
                        .with_headers(headers),
                )
                .await
                .unwrap()
                .await
                .unwrap();
            }
        }
        let q =
            |src: &str| format!("SELECT id, qty, urgent FROM jobs WHERE src = '{src}' ORDER BY id");
        let compiled = wait_rows(&mut c, &q("c"), 4, 90).await;
        let engine = wait_rows(&mut c, &q("e"), 4, 90).await;
        ck.check(
            "the compiled Lua queue trigger wrote the rows its engine-tier twin did",
            compiled.len() == 4 && compiled == engine,
            format!("compiled {compiled:?} / engine {engine:?}"),
        );
        a.close(200, "done".into()).await.unwrap();
    } else {
        println!("no SPRIG_TEST_LOGIC_AMQP: the queue part is skipped");
    }
    must(&mut c, &format!("DROP DATABASE {MDB}")).await;
    println!("took {:.1} s", started.elapsed().as_secs_f64());
    ck.done("Lua triggers on streams and queues");
}
