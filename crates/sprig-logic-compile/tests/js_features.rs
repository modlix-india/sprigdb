//! `try`/`catch`/`finally`, helper functions and `do`/`while`: each case
//! runs a JavaScript function compiled into MariaDB by the JavaScript front
//! end and the same source on QuickJS (`sprig_logic_js::Program`), each on
//! its own copy of the same tables, and compares the event, its fields
//! (doubles to 1e-9), the error number and every table's rows afterwards.
//!
//! Runs on a MariaDB 11.8 server; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `r7js_feat_js` and
//! `r7js_feat_jsc`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    missing_docs
)]

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, StaticColumns, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

const JS: &str = "r7js_feat_js";
const JSC: &str = "r7js_feat_jsc";
const SCHEMAS: [&str; 2] = [JS, JSC];

const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE orders (id BIGINT PRIMARY KEY, product_id BIGINT NOT NULL, qty INT NOT NULL CHECK (qty > 0), note VARCHAR(64))",
    "CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, what VARCHAR(64) NOT NULL)",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "INSERT INTO orders VALUES (100, 7, 1, 'first')",
];

fn text(v: &My) -> Option<String> {
    match v {
        My::NULL => None,
        My::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        My::Int(i) => Some(i.to_string()),
        My::UInt(u) => Some(u.to_string()),
        My::Float(f) => Some(f.to_string()),
        My::Double(f) => Some(f.to_string()),
        other => Some(other.as_sql(true)),
    }
}

fn my_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => SprigError::backend(s.code, *b"HY000", s.message),
        other => SprigError::internal(other.to_string()),
    }
}

/// The engine's `db`: each statement as text on its own schema.
struct EngineDb {
    pool: Pool,
    schema: &'static str,
}

#[async_trait::async_trait]
impl DbBridge for EngineDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let mut c = self.pool.get_conn().await.map_err(my_err)?;
        c.query_drop(format!("USE `{}`", self.schema))
            .await
            .map_err(my_err)?;
        if op.shape == Shape::Exec {
            c.query_drop(op.sql.as_str()).await.map_err(my_err)?;
            return Ok(Reply::Affected(c.affected_rows()));
        }
        let mut r = c.query_iter(op.sql.as_str()).await.map_err(my_err)?;
        let columns: Vec<Column> = r
            .columns()
            .map(|cs| {
                cs.iter()
                    .map(|col| Column {
                        name: col.name_str().into_owned(),
                        kind: eb::kind_of_type(
                            col.column_type() as u8,
                            col.flags()
                                .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG),
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let rows: Vec<mysql_async::Row> = r.collect().await.map_err(my_err)?;
        Ok(Reply::Rows {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.unwrap().iter().map(text).collect())
                .collect(),
        })
    }
}

fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

fn close(a: &J, b: &J) -> bool {
    match (a, b) {
        (J::Number(x), J::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            x == y || (x - y).abs() <= 1e-9 * x.abs().max(y.abs())
        }
        (J::Object(x), J::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| close(v, w)))
        }
        (J::Array(x), J::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| close(v, w))
        }
        _ => a == b,
    }
}

type Got = std::result::Result<(String, J), u16>;

fn same(a: &Got, b: &Got) -> bool {
    match (a, b) {
        (Ok((e1, r1)), Ok((e2, r2))) => e1 == e2 && close(r1, r2),
        (Err(x), Err(y)) => x == y,
        _ => false,
    }
}

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    cases: usize,
    failures: Vec<String>,
}

fn ty_of(t: ParamTy) -> Ty {
    match t {
        ParamTy::Int => Ty::Int,
        ParamTy::Float => Ty::Float,
        ParamTy::Bool => Ty::Bool,
        ParamTy::Json => Ty::Json,
        ParamTy::Str => Ty::Str,
    }
}

impl Suite {
    fn conn(&self, schema: Option<&str>) -> Conn {
        let mut c = self.rt.block_on(self.pool.get_conn()).unwrap();
        if let Some(s) = schema {
            self.rt
                .block_on(c.query_drop(format!("USE `{s}`")))
                .unwrap();
        }
        c
    }

    fn reset(&self) {
        let mut c = self.conn(None);
        for s in SCHEMAS {
            self.rt
                .block_on(async {
                    c.query_drop(format!("USE `{s}`")).await?;
                    c.query_drop("DROP TABLE IF EXISTS products, orders, audit")
                        .await?;
                    for q in SEED {
                        c.query_drop(*q).await?;
                    }
                    Ok::<_, mysql_async::Error>(())
                })
                .unwrap();
        }
    }

    fn columns(&self) -> StaticColumns {
        let mut c = self.conn(None);
        let rows: Vec<(String, String, String, String)> = self
            .rt
            .block_on(c.exec(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?",
                (JSC,),
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

    /// Compiles `js` and creates it in `JSC`; returns its entry routine.
    fn deploy(&self, name: &str, js: &str, sig: &ScriptSig, module: u64) -> String {
        let opts = Options {
            module_id: module,
            version: 1,
            ..Options::default()
        };
        let jsig = sprig_logic_compile::js::Signature {
            params: sig
                .params
                .iter()
                .map(|(n, t)| (n.clone(), ty_of(*t)))
                .collect(),
            returns: None,
            row: None,
            on: None,
        };
        let c =
            sprig_logic_compile::js::compile_with(name, js, &jsig, &self.columns(), &opts).unwrap();
        assert!(
            c.compilable(),
            "{name}: the JavaScript doesn't compile: {:#?}",
            c.analysis.reasons
        );
        let mut conn = self.conn(Some(JSC));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        let p = c.plsql.unwrap();
        for s in &p.statements {
            if let Err(e) = self.rt.block_on(conn.query_drop(s.as_str())) {
                panic!("{name}: {e}\n{s}");
            }
        }
        p.entry
    }

    fn compiled(&self, entry: &str, args: &[J]) -> Got {
        let mut conn = self.conn(Some(JSC));
        let marks = vec!["?"; args.len()];
        let mut call = format!("CALL {entry}({}", marks.join(", "));
        call.push_str(if args.is_empty() {
            "@e, @r)"
        } else {
            ", @e, @r)"
        });
        let params: Vec<My> = args
            .iter()
            .map(|v| match v {
                J::Null => My::NULL,
                J::Bool(b) => My::Int(i64::from(*b)),
                J::Number(n) => n
                    .as_i64()
                    .map_or_else(|| My::Double(n.as_f64().unwrap()), My::Int),
                J::String(s) => My::Bytes(s.clone().into_bytes()),
                other => My::Bytes(other.to_string().into_bytes()),
            })
            .collect();
        let r: std::result::Result<Option<(Option<String>, Option<String>)>, _> =
            self.rt.block_on(async {
                conn.exec_drop(call.as_str(), params).await?;
                conn.query_first("SELECT @e, @r").await
            });
        match r {
            Ok(Some((Some(e), r))) => Ok((
                e,
                serde_json::from_str(&r.unwrap_or_else(|| "null".into())).unwrap(),
            )),
            Ok(_) => Err(0),
            Err(e) => {
                println!("  compiled error: {e}");
                Err(class(&my_err(e)))
            }
        }
    }

    fn engine(&self, name: &str, js: &str, sig: &ScriptSig, args: &[J]) -> Got {
        let p = Program::new("javascript", name, js, sig.clone()).unwrap();
        let host = Host {
            db: Arc::new(EngineDb {
                pool: self.pool.clone(),
                schema: JS,
            }),
            outbound: Arc::new(NoOutbound),
            default_db: Some(JS.to_owned()),
            info: J::Null,
        };
        let args = p.coerce(name, args.to_vec()).map_err(|e| class(&e))?;
        self.rt
            .block_on(p.run(args, host, Limits::default()))
            .map(|o| (o.event, o.result))
            .map_err(|e| {
                println!("  quickjs error: {e}");
                class(&e)
            })
    }

    fn tables(&self, schema: &str) -> Vec<Vec<Option<String>>> {
        let mut conn = self.conn(Some(schema));
        let mut out = Vec::new();
        for q in [
            "SELECT id, price, stock FROM products ORDER BY id",
            "SELECT id, product_id, qty, note FROM orders ORDER BY id",
            "SELECT id, what FROM audit ORDER BY id",
        ] {
            let rows: Vec<mysql_async::Row> = self.rt.block_on(conn.query(q)).unwrap();
            out.extend(
                rows.into_iter()
                    .map(|r| r.unwrap().iter().map(text).collect::<Vec<_>>()),
            );
        }
        out
    }

    fn case(&mut self, name: &str, js: &str, sig: &ScriptSig, entry: &str, args: &[J]) {
        self.cases += 1;
        self.reset();
        let want = self.engine(name, js, sig, args);
        let got = self.compiled(entry, args);
        let label = format!("{name}{args:?}");
        if !same(&want, &got) {
            self.failures
                .push(format!("{label}: quickjs {want:?} vs compiled {got:?}"));
        }
        let (a, b) = (self.tables(JS), self.tables(JSC));
        if a != b {
            self.failures
                .push(format!("{label}: tables differ: {a:?} vs {b:?}"));
        }
        println!("{label}: {want:?}");
    }
}

fn sig(params: &[(&str, ParamTy)]) -> ScriptSig {
    ScriptSig {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        scalar: false,
    }
}

/// The functions and the arguments of each call.
fn corpus() -> Vec<(&'static str, &'static str, ScriptSig, Vec<Vec<J>>)> {
    let i = ParamTy::Int;
    let x = || sig(&[("x", i)]);
    vec![
        (
            "names",
            "let r = 'none';
             try {
               if (x < 0) throw new Error('neg ' + x);
               if (x === 0) throw new RangeError('zero');
               if (x === 9) throw new Error();
               r = 'ok';
             } catch (e) {
               r = e.name + ':' + e.message;
             }
             return { r };",
            x(),
            vec![vec![json!(-2)], vec![json!(0)], vec![json!(9)], vec![json!(5)]],
        ),
        (
            "finally_paths",
            "let log = '';
             function step(t) { log = log + t; }
             for (let i = 0; i < 3; i++) {
               try {
                 step('t' + i);
                 if (i === 1) continue;
                 if (i === x) break;
                 step('a');
               } finally {
                 step('f');
               }
               step('n');
             }
             try {
               return { log, v: x };
             } finally {
               log = log + 'X';
             }",
            x(),
            vec![vec![json!(2)], vec![json!(0)], vec![json!(7)]],
        ),
        (
            "override",
            "function f1() { try { return 1; } finally { return 2; } }
             function f2(a) {
               try { if (a > 0) throw new Error('x'); return 'n'; }
               finally { if (a > 1) return 'swallowed'; }
             }
             let out = '';
             try { out = f2(x); } catch (e) { out = 'caught ' + e.message; }
             return { a: f1(), b: out };",
            x(),
            vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
        ),
        (
            "rethrow",
            "try {
               try { throw new TypeError('inner'); }
               catch (e) { if (x > 0) throw e; return { w: 'kept ' + e.name }; }
             } catch (e2) {
               if (x > 1) throw e2;
               return { w: 'outer ' + e2.name + ' ' + e2.message };
             }",
            x(),
            vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
        ),
        (
            "not_caught",
            "let n = 0;
             try {
               if (x === 1) sprig.reject('no ' + x);
               if (x === 2) { while (true) { n++; } }
               n = 5;
             } catch (e) {
               db.exec('INSERT INTO audit (what) VALUES (?)', ['caught']);
               return { c: 1 };
             } finally {
               db.exec('INSERT INTO audit (what) VALUES (?)', ['finally']);
             }
             return { n };",
            x(),
            vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
        ),
        (
            "loop_catch",
            "let bad = 0; let good = 0; let last = '';
             for (let i = 0; i < 6; i++) {
               try {
                 if (i % 3 === x) throw 'skip ' + i;
                 if (i === 4) throw new Error('four');
                 good++;
               } catch (e) {
                 bad++;
                 if (i === 4) { last = 'four'; }
                 continue;
               }
               db.exec('INSERT INTO audit (what) VALUES (?)', ['ok ' + i]);
             }
             try { throw 'plain'; } catch { bad = bad + 100; }
             if (x === 2) throw 'uncaught ' + x;
             return { bad, good, last };",
            x(),
            vec![vec![json!(0)], vec![json!(1)], vec![json!(2)]],
        ),
        (
            "helper_loops",
            "function find(n) {
               for (let i = 0; i < 10; i++) {
                 if (i * i >= n) return i;
                 if (i === 7) break;
               }
               return -1;
             }
             function sum(n) {
               let t = 0;
               let i = 0;
               while (true) {
                 i++;
                 if (i > n) break;
                 if (i % 2 === 0) continue;
                 t += i;
               }
               return t;
             }
             let found = 0;
             for (let k = 0; k < 4; k++) {
               if (find(k * x) < 0) break;
               found += find(k * x);
             }
             return { a: find(x), b: sum(x), found };",
            x(),
            vec![vec![json!(0)], vec![json!(10)], vec![json!(80)]],
        ),
        (
            "null_row",
            "const p = db.sqlOne('SELECT stock FROM products WHERE id = ?', [x]);
             try { return { s: p.stock }; } catch (e) { return { n: e.name, m: e.message }; }",
            x(),
            vec![vec![json!(7)], vec![json!(70)]],
        ),
        (
            "db_errors",
            "let ok = 0;
             for (let k = 0; k < 3; k++) {
               const id = k === 1 ? x + 1 : x;
               try {
                 db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [id, 7, 1]);
                 ok++;
               } catch (e) {
                 db.exec('INSERT INTO audit (what) VALUES (?)', ['dup ' + id]);
               }
             }
             try {
               db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [500, 7, 0]);
             } catch {
               db.exec('INSERT INTO audit (what) VALUES (?)', ['check']);
             }
             return { ok };",
            x(),
            vec![vec![json!(1)], vec![json!(100)]],
        ),
        (
            "db_rethrow",
            "try {
               db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [x, 7, 1]);
             } catch (e) {
               db.exec('INSERT INTO audit (what) VALUES (?)', ['seen']);
               throw e;
             } finally {
               db.exec('INSERT INTO audit (what) VALUES (?)', ['finally']);
             }
             return { done: true };",
            x(),
            vec![vec![json!(100)], vec![json!(101)]],
        ),
        (
            "do_while",
            "let i = 0; let s = 0;
             do { i++; if (i === 2) continue; s += i; } while (i < x);
             let k = 0;
             do {
               k++;
               db.exec('INSERT INTO audit (what) VALUES (?)', ['k' + k]);
               if (k > 5) break;
             } while (k < x);
             return { i, s, k };",
            x(),
            vec![vec![json!(0)], vec![json!(3)], vec![json!(9)]],
        ),
        (
            "helpers",
            "const tax = (p) => p * 0.25;
             function net(p, q) { const g = p * q; return g + tax(g); }
             function label(n) { if (n > 100) return 'big'; if (n > 10) return 'mid'; return 'small'; }
             function stock(id) {
               const r = db.sqlOne('SELECT stock FROM products WHERE id = ?', [id]);
               if (r === null) return -1;
               return r.stock;
             }
             function more(k) { return k < x; }
             let total = 0;
             for (let i = 1; i <= x; i++) total += net(i, 2);
             if (label(total) === 'mid') total = total + 1000;
             let n = 0;
             while (more(n)) { n++; }
             return { total, l: label(total), t: tax(4), s: stock(7) + stock(99), n };",
            x(),
            vec![vec![json!(3)], vec![json!(1)], vec![json!(0)]],
        ),
        (
            "helper_try",
            "function buy(id, q) {
               try {
                 const t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', [q, id, q]);
                 if (t.affected !== 1) throw new Error('out of stock for ' + id);
                 db.exec('INSERT INTO orders (id, product_id, qty) VALUES (?, ?, ?)', [id * 10 + q, id, q]);
                 return 'bought';
               } catch (e) {
                 db.exec('INSERT INTO audit (what) VALUES (?)', ['failed ' + id]);
                 return 'failed';
               }
             }
             return { a: buy(7, x), b: buy(8, x), c: buy(9, x) };",
            x(),
            vec![vec![json!(1)], vec![json!(3)], vec![json!(20)]],
        ),
        (
            "module_helpers",
            "import { db, sprig } from 'sprig:db';
             const twice = (n) => n * 2;
             function pick(a, b) { if (a > b) { return a; } return b; }
             function check(v) {
               if (v > 100) sprig.reject('too big: ' + v);
               return v;
             }
             export default function (x) {
               let tries = 0;
               let v = 0;
               do {
                 tries++;
                 try { v = check(pick(twice(x), 10) * tries); }
                 catch (e) { v = -1; }
               } while (tries < 3);
               return { v, tries };
             }",
            x(),
            vec![vec![json!(7)], vec![json!(2)], vec![json!(30)]],
        ),
    ]
}

#[test]
fn compiled_javascript_agrees_with_quickjs() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        return;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let opts = OptsBuilder::from_opts(Opts::from_url(&url).unwrap()).pool_opts(
        PoolOpts::default()
            .with_constraints(PoolConstraints::new(0, 4).unwrap())
            .with_inactive_connection_ttl(std::time::Duration::from_secs(30)),
    );
    let mut s = Suite {
        rt,
        pool: Pool::new(opts),
        cases: 0,
        failures: Vec::new(),
    };
    {
        let mut c = s.conn(None);
        for db in SCHEMAS {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
            s.rt.block_on(c.query_drop(format!("CREATE DATABASE `{db}` CHARACTER SET utf8mb4")))
                .unwrap();
        }
    }
    s.reset();
    let start = std::time::Instant::now();
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for (n, (name, js, sig, calls)) in corpus().into_iter().enumerate() {
            let entry = s.deploy(name, js, &sig, 9700 + n as u64);
            for args in calls {
                s.case(name, js, &sig, &entry, &args);
            }
        }
    }));
    let elapsed = start.elapsed();
    {
        let mut c = s.conn(None);
        for db in SCHEMAS {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
        }
        drop(c);
    }
    s.rt.block_on(s.pool.clone().disconnect()).unwrap();
    if let Err(p) = run {
        std::panic::resume_unwind(p);
    }
    println!(
        "{} cases in {elapsed:?}, {} mismatches",
        s.cases,
        s.failures.len()
    );
    assert!(
        s.failures.is_empty(),
        "mismatches:\n{}",
        s.failures.join("\n")
    );
}
