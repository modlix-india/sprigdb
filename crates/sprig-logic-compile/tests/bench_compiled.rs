//! A timing benchmark of the compiled tier: existing JavaScript and KIRun
//! functions compiled into MariaDB and called in a server-side loop, so the
//! time is the routines' own, without client round trips. It also writes
//! each function's generated PL/SQL to a directory, so two builds of the
//! compiler can be compared text for text.
//!
//! Skipped unless both are set:
//!
//! ```sh
//! export SPRIG_TEST_MARIADB_SINGLE=mysql://root:...@127.0.0.1:<port>
//! export SPRIG_LOGIC_BENCH_OUT=/path/to/dir   # PL/SQL files and timings.txt
//! cargo test -p sprig-logic-compile --test bench_compiled -- --nocapture
//! ```
//!
//! Works in the schema `r7_bench_compiled`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    missing_docs
)]

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::js::Signature;
use sprig_logic_compile::{Compiled, Options, Runtime, StaticColumns, compile, plsql, sql};

const SCHEMA: &str = "r7_bench_compiled";

const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL, note VARCHAR(64))",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "INSERT INTO orders VALUES (100, 7, 1, 'first'), (101, 8, 2, NULL), (102, 7, 5, 'x')",
];

const QUOTE_JS: &str = r"
import { db, sprig } from 'sprig:db';
export default function (productId, qty) {
  const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [productId]);
  if (p === null || !(p.stock >= qty))
    return sprig.event('error', { message: 'not enough stock' });
  return { total: p.price * qty + 40 };
}";

const BRANCHES_JS: &str = r"
if (score === null) return sprig.event('error', { message: 'no score' });
if (score >= 90) return { grade: 'A', bonus: score + 5 };
return { grade: score >= 50 ? 'pass' : 'fail', bonus: 0 };";

const TEXT_JS: &str = r"
let s = `hi ${name}`;
if (name !== null && name.startsWith('A')) s = s + '!';
const up = name === null ? 'none' : name.toUpperCase();
return {
  s,
  up,
  len: name === null ? -1 : name.length,
  idx: name === null ? -2 : name.indexOf('b'),
  has: name !== null && name.includes('e'),
  n2: n * 2 + 0.5,
  empty: !name,
  joined: 'n=' + n + '/' + (n > 1),
};";

const NULLS_JS: &str = r"
return {
  s: a + b,
  d: a - 1,
  p: a * b,
  q: b / 2,
  lt: a < b,
  m: Math.max(a, 3),
  r: Math.round(b),
  f: Math.floor(b * 10) / 10,
  eq: a === 3,
  ne: a !== null,
  c: a ?? 42,
  o: a || 9,
  pw: 2 ** 3,
  ab: Math.abs(a),
};";

const FACT_JS: &str = r"
export default function (n) {
  let f = 1;
  for (let i = 1; i < 21; i++) {
    if (i > n) break;
    f = f * i;
  }
  return f;
}";

const FLOW_JS: &str = r"
let sum = 0;
for (let i = 0; i < n; i++) {
  if (i % 3 === 0) continue;
  sum += i;
}
let k = 0;
let kept = 0;
while (k < n) {
  k++;
  if (k % 2 === 0) { continue; }
  kept++;
}
let seen = 0;
for (const o of db.sql('SELECT qty, note FROM orders ORDER BY id', [])) {
  if (o.note === null) continue;
  seen += o.qty;
}
let label = 'none';
switch (kind) {
  case 'a':
  case 'b':
    label = 'ab';
    break;
  case 'z':
    return { sum, kept, seen, label: 'zed' };
  default:
    label = 'other';
}
return { sum, kept, seen, label };";

/// A function to time: its name, language and source, its signature,
/// and the arguments of each call (SQL literals).
struct Case {
    name: &'static str,
    js: Option<(&'static str, Signature)>,
    kirun: Option<&'static str>,
    args: &'static str,
}

fn sig(params: &[(&str, Ty)], returns: Option<Ty>) -> Signature {
    Signature {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        returns,
        row: None,
        on: None,
    }
}

fn cases() -> Vec<Case> {
    let i = Ty::Int;
    vec![
        Case {
            name: "quote_kirun",
            js: None,
            kirun: Some("quote"),
            args: "7, 3",
        },
        Case {
            name: "quote_js",
            js: Some((QUOTE_JS, sig(&[("productId", i), ("qty", i)], None))),
            kirun: None,
            args: "7, 3",
        },
        Case {
            name: "branches_js",
            js: Some((BRANCHES_JS, sig(&[("score", i), ("vip", Ty::Bool)], None))),
            kirun: None,
            args: "60, TRUE",
        },
        Case {
            name: "text_js",
            js: Some((TEXT_JS, sig(&[("name", Ty::Str), ("n", i)], None))),
            kirun: None,
            args: "'Abba', 2",
        },
        Case {
            name: "nulls_js",
            js: Some((NULLS_JS, sig(&[("a", i), ("b", Ty::Float)], None))),
            kirun: None,
            args: "4, 2.5",
        },
        Case {
            name: "fact_js",
            js: Some((FACT_JS, sig(&[("n", i)], Some(Ty::Int)))),
            kirun: None,
            args: "12",
        },
        Case {
            name: "flow_js",
            js: Some((FLOW_JS, sig(&[("n", i), ("kind", Ty::Str)], None))),
            kirun: None,
            args: "7, 'a'",
        },
    ]
}

fn columns(rt: &tokio::runtime::Runtime, c: &mut Conn) -> StaticColumns {
    let rows: Vec<(String, String, String, String)> = rt
        .block_on(c.exec(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?",
            (SCHEMA,),
        ))
        .unwrap();
    let mut cols = StaticColumns::new();
    for (t, col, dt, ct) in rows {
        if let Some(ty) = sql::ty_of_column(&dt, &ct) {
            cols.add(&t, &col, ty);
        }
    }
    cols
}

#[ignore = "live: needs SPRIG_TEST_MARIADB_SINGLE, SPRIG_LOGIC_BENCH_OUT; tools/live/README.md"]
#[test]
fn compiled_tier_timings() {
    let (Ok(url), Ok(out)) = (
        std::env::var("SPRIG_TEST_MARIADB_SINGLE"),
        std::env::var("SPRIG_LOGIC_BENCH_OUT"),
    ) else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE and SPRIG_LOGIC_BENCH_OUT are not both set");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let calls: u64 = std::env::var("SPRIG_LOGIC_BENCH_CALLS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    std::fs::create_dir_all(&out).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let opts = OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
        PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 2).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(30)),
    );
    let pool = Pool::new(opts);
    let mut c = rt.block_on(pool.get_conn()).unwrap();
    rt.block_on(async {
        c.query_drop(format!("DROP DATABASE IF EXISTS `{SCHEMA}`"))
            .await?;
        c.query_drop(format!("CREATE DATABASE `{SCHEMA}` CHARACTER SET utf8mb4"))
            .await?;
        c.query_drop(format!("USE `{SCHEMA}`")).await?;
        for q in SEED {
            c.query_drop(*q).await?;
        }
        Ok::<_, mysql_async::Error>(())
    })
    .unwrap();
    let cols = columns(&rt, &mut c);
    let mut report = String::new();
    for (n, case) in cases().into_iter().enumerate() {
        let o = Options {
            module_id: 9900 + n as u64,
            version: 1,
            ..Options::default()
        };
        let compiled: Compiled = match (&case.js, case.kirun) {
            (Some((src, s)), _) => {
                sprig_logic_compile::js::compile_with(case.name, src, s, &cols, &o).unwrap()
            }
            (None, Some(k)) => {
                let def = std::fs::read_to_string(format!(
                    "{}/tests/kirun/{k}.json",
                    env!("CARGO_MANIFEST_DIR")
                ))
                .unwrap();
                compile(Runtime::Kirun, &def, &cols, &o).unwrap()
            }
            (None, None) => unreachable!(),
        };
        assert!(
            compiled.compilable(),
            "{}: {:#?}",
            case.name,
            compiled.analysis.reasons
        );
        let p = compiled.plsql.unwrap();
        std::fs::write(
            format!("{out}/{}.sql", case.name),
            p.statements.join(";\n\n") + ";\n",
        )
        .unwrap();
        let entry = p.entry.clone();
        let times: Vec<Duration> = rt.block_on(async {
            c.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE))
                .await?;
            for s in &p.statements {
                c.query_drop(s.as_str()).await?;
            }
            c.query_drop("SET SESSION sql_mode = DEFAULT").await?;
            let lp = format!("bench_{}", case.name);
            c.query_drop(format!(
                "CREATE OR REPLACE PROCEDURE {lp}(n INT)\nBEGIN\n  DECLARE i INT DEFAULT 0;\n  \
                 WHILE i < n DO\n    CALL {entry}({}, @e, @r);\n    SET i = i + 1;\n  END WHILE;\nEND",
                case.args
            ))
            .await?;
            // Warm up, then seven timed runs (the machine is shared: the
            // minimum is the least disturbed).
            c.query_drop(format!("CALL {lp}(200)")).await?;
            let mut ts = Vec::new();
            for _ in 0..7 {
                let t = Instant::now();
                c.query_drop(format!("CALL {lp}({calls})")).await?;
                ts.push(t.elapsed());
            }
            Ok::<_, mysql_async::Error>(ts)
        })
        .unwrap();
        let mut us: Vec<f64> = times
            .iter()
            .map(|d| d.as_secs_f64() * 1e6 / calls as f64)
            .collect();
        us.sort_by(f64::total_cmp);
        let line = format!(
            "{:<14} min {:>7.2} us/call, median {:>7.2} (7 runs of {calls} calls)",
            case.name, us[0], us[3]
        );
        println!("{line}");
        let _ = writeln!(report, "{line}");
    }
    rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{SCHEMA}`")))
        .unwrap();
    drop(c);
    rt.block_on(pool.disconnect()).unwrap();
    std::fs::write(format!("{out}/timings.txt"), report).unwrap();
}
