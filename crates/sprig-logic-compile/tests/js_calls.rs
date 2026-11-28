//! Compiled `db.call` and `db.publish` against QuickJS: each caller runs
//! compiled into MariaDB (calling the callee's call entry, writing the
//! stream's outbox) and on QuickJS (calling the callee's wrapper by name,
//! publishing through the gateway's `INSERT INTO <stream>` rewrite, which
//! the engine side's `db` does here as the gateway does), each on its own
//! copy of the same tables, and compares the event, its fields (doubles to
//! 1e-9), the error number and every table's rows afterwards.
//!
//! The callees are compiled into both schemas, with the wrapper the
//! deploy gives them (the engine side calls it) and their call entry (the
//! compiled side calls it).
//!
//! Runs on a MariaDB 11.8 server; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `r7dc_call_js` and
//! `r7dc_call_jsc`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    missing_docs
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{Function, Ty};
use sprig_logic_compile::link::{Callee, StreamTarget};
use sprig_logic_compile::{Columns, Options, StaticColumns, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

const JS: &str = "r7dc_call_js";
const JSC: &str = "r7dc_call_jsc";
const SCHEMAS: [&str; 2] = [JS, JSC];
/// The small `stream.max_message_bytes` the streams get here.
const MAX_BYTES: u64 = 64;

const SEED: &[&str] = &[
    "CREATE TABLE products (id BIGINT PRIMARY KEY, price DECIMAL(10,2) NOT NULL, stock INT NOT NULL)",
    "CREATE TABLE audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, what VARCHAR(64) NOT NULL)",
    "INSERT INTO products VALUES (7, 400.00, 10), (8, 19.99, 3), (9, 0.10, 0)",
    "CREATE TABLE __sprig_eo_901 (event_id BIGINT AUTO_INCREMENT PRIMARY KEY, partition_no INT UNSIGNED NULL, \
     k VARBINARY(4096) NULL, v LONGBLOB NULL, headers JSON NULL)",
    "CREATE TABLE __sprig_eo_902 (event_id BIGINT AUTO_INCREMENT PRIMARY KEY, partition_no INT UNSIGNED NULL, \
     k VARBINARY(4096) NULL, v LONGBLOB NULL, headers JSON NULL, \
     CONSTRAINT sprig_sc_needs_key CHECK (k IS NOT NULL))",
];

/// Streams: name → (outbox, compacted).
fn streams() -> BTreeMap<&'static str, (&'static str, bool)> {
    BTreeMap::from([
        ("orders_s", ("__sprig_eo_901", false)),
        ("latest_s", ("__sprig_eo_902", true)),
    ])
}

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

/// A literal of `bridge::literal`'s forms: its bytes, `None` for `NULL`.
fn literal_bytes(lit: &str) -> Option<Vec<u8>> {
    let t = lit.trim();
    if t.eq_ignore_ascii_case("NULL") {
        return None;
    }
    if t == "_utf8mb4''" {
        return Some(Vec::new());
    }
    let hex = t.strip_prefix("_utf8mb4 X'")?.strip_suffix('\'')?;
    Some(
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect(),
    )
}

/// The engine's `db`: each statement as text on its own schema; a publish
/// as the gateway rewrites it (`INSERT INTO <outbox> (k, v, headers)`),
/// after its checks.
struct EngineDb {
    pool: Pool,
    schema: &'static str,
}

impl EngineDb {
    fn publish(sql: &str) -> Option<std::result::Result<String, SprigError>> {
        let rest = sql.strip_prefix("INSERT INTO ")?;
        let (target, rest) = rest.split_once(" (key, value, headers) VALUES (")?;
        let name = target.rsplit('.').next()?.trim_matches('`');
        let (outbox, compact) = *streams().get(name)?;
        let body = rest.strip_suffix(')')?;
        let lits: Vec<&str> = body.split(", ").collect();
        let sizes: Vec<Option<Vec<u8>>> = lits.iter().map(|l| literal_bytes(l)).collect();
        // As the gateway measures every publish: a key above 4096 bytes,
        // or key, value and headers together above the limit.
        let key = sizes[0].as_ref().map_or(0, Vec::len);
        let record: usize = sizes.iter().flatten().map(Vec::len).sum();
        if key > 4096 || record as u64 > MAX_BYTES {
            return Some(Err(SprigError::new(
                SprigCode::StreamRecordTooLarge,
                "ER_SPRIG_STREAM_RECORD_TOO_LARGE",
            )));
        }
        if compact && sizes[0].is_none() {
            return Some(Err(SprigError::new(
                SprigCode::StreamCompactNeedsKey,
                "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY",
            )));
        }
        Some(Ok(format!(
            "INSERT INTO `{outbox}` (k, v, headers) VALUES ({body})"
        )))
    }
}

#[async_trait::async_trait]
impl DbBridge for EngineDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let mut c = self.pool.get_conn().await.map_err(my_err)?;
        c.query_drop(format!("USE `{}`", self.schema))
            .await
            .map_err(my_err)?;
        if op.shape == Shape::Exec {
            let sql = match Self::publish(&op.sql) {
                Some(r) => r?,
                None => op.sql.clone(),
            };
            c.query_drop(sql.as_str()).await.map_err(my_err)?;
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
        drop(r);
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

fn ty_of(t: ParamTy) -> Ty {
    match t {
        ParamTy::Int => Ty::Int,
        ParamTy::Float => Ty::Float,
        ParamTy::Bool => Ty::Bool,
        ParamTy::Json => Ty::Json,
        ParamTy::Str => Ty::Str,
    }
}

fn sig(params: &[(&str, ParamTy)]) -> ScriptSig {
    ScriptSig {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        scalar: false,
    }
}

/// The wrapper's fields of `f` (every event's fields once, booleans as
/// integers), as `sprig_logic::calls::call_fields` gives them.
fn call_fields(f: &Function) -> Vec<(String, Ty)> {
    let mut cols: Vec<(String, Ty)> = Vec::new();
    for e in &f.events {
        for (n, t) in &e.fields {
            if n != "event" && !cols.iter().any(|(c, _)| c == n) {
                cols.push((n.clone(), if *t == Ty::Bool { Ty::Int } else { *t }));
            }
        }
    }
    cols
}

/// The compile-time view: the compiled schema's columns, the callees
/// deployed so far, and the streams.
#[derive(Clone)]
struct Env {
    cols: StaticColumns,
    callees: BTreeMap<String, Callee>,
}

impl Columns for Env {
    fn column(&self, table: &str, column: &str) -> Option<Ty> {
        self.cols.column(table, column)
    }
    fn has_table(&self, table: &str) -> bool {
        self.cols.has_table(table)
    }
    fn callee(&self, name: &str) -> Option<std::result::Result<Callee, String>> {
        self.callees.get(name).cloned().map(Ok)
    }
    fn stream(&self, name: &str) -> Option<std::result::Result<StreamTarget, String>> {
        streams().get(name).map(|(outbox, compact)| {
            Ok(StreamTarget {
                name: name.to_owned(),
                outbox: (*outbox).to_owned(),
                max_bytes: MAX_BYTES,
                compact: *compact,
            })
        })
    }
}

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    env: Env,
    cases: usize,
    failures: Vec<String>,
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
                    c.query_drop(
                        "DROP TABLE IF EXISTS products, audit, __sprig_eo_901, __sprig_eo_902",
                    )
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

    fn run_sql(&self, schema: &str, stmts: &[String], oracle: bool) {
        let mut conn = self.conn(Some(schema));
        let mode = if oracle {
            plsql::SQL_MODE
        } else {
            "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
        };
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{mode}'")))
            .unwrap();
        for s in stmts {
            if let Err(e) = self.rt.block_on(conn.query_drop(s.as_str())) {
                panic!("{schema}: {e}\n{s}");
            }
        }
    }

    /// Compiles `js`; panics unless it compiles.
    fn compile(
        &self,
        name: &str,
        js: &str,
        sig: &ScriptSig,
        module: u64,
    ) -> sprig_logic_compile::Compiled {
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
        let c = sprig_logic_compile::js::compile_with(name, js, &jsig, &self.env, &opts).unwrap();
        assert!(
            c.compilable(),
            "{name}: the JavaScript doesn't compile: {:#?}",
            c.analysis.reasons
        );
        c
    }

    /// A callee: compiled into both schemas with its wrapper (engine side)
    /// and its call entry (compiled side), then known to later compiles.
    fn callee(&mut self, name: &str, js: &str, sig: &ScriptSig, module: u64) {
        let c = self.compile(name, js, sig, module);
        let f = c.ir.clone().unwrap();
        let p = c.plsql.clone().unwrap();
        let params: Vec<(String, Ty)> = f
            .params
            .iter()
            .map(|q| (format!("p_{}", q.name), q.ty))
            .collect();
        let decl: Vec<String> = params
            .iter()
            .map(|(n, t)| format!("IN {n} {}", plsql::sql_type(*t)))
            .collect();
        let args: Vec<String> = params.iter().map(|(n, _)| n.clone()).collect();
        let fields = call_fields(&f);
        let mut select = vec!["v__event AS `event`".to_owned()];
        for (n, t) in &fields {
            let path = format!("'$.\"{n}\"'");
            select.push(match t {
                Ty::Int | Ty::Bool => {
                    format!("CAST(JSON_VALUE(v__result, {path}) AS SIGNED) AS `{n}`")
                }
                Ty::Float => format!("CAST(JSON_VALUE(v__result, {path}) AS DOUBLE) AS `{n}`"),
                _ => format!("JSON_VALUE(v__result, {path}) AS `{n}`"),
            });
        }
        let sep = if args.is_empty() { "" } else { ", " };
        let wrapper = format!(
            "CREATE OR REPLACE PROCEDURE `{name}`({}) SQL SECURITY INVOKER\nBEGIN\n  DECLARE v__event VARCHAR(64);\n  \
             DECLARE v__result LONGTEXT;\n  CALL `{}`({}{sep}v__event, v__result);\n  SELECT {};\nEND",
            decl.join(", "),
            p.entry,
            args.join(", "),
            select.join(", ")
        );
        let mut entry_decl = decl.clone();
        entry_decl.push("OUT p__event VARCHAR(64)".into());
        entry_decl.push("OUT p__result LONGTEXT".into());
        let call_entry = format!(
            "CREATE OR REPLACE PROCEDURE `__sprig_l{module}_call`({}) SQL SECURITY INVOKER\nBEGIN\n  \
             CALL `{}`({}{sep}p__event, p__result);\nEND",
            entry_decl.join(", "),
            p.entry,
            args.join(", ")
        );
        for s in SCHEMAS {
            self.run_sql(s, &p.statements, true);
            self.run_sql(s, &[wrapper.clone(), call_entry.clone()], false);
        }
        let mut reaches = c.analysis.reaches.clone();
        reaches.extend(c.analysis.calls.iter().map(|d| d.module));
        self.env.callees.insert(
            name.to_owned(),
            Callee {
                module_id: module,
                name: name.to_owned(),
                params: f.params.iter().map(|q| q.ty).collect(),
                fields,
                reads: c.analysis.reads.clone(),
                writes: c.analysis.writes.clone(),
                reaches,
                depth: c.analysis.call_depth,
            },
        );
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
            "SELECT id, what FROM audit ORDER BY id",
            "SELECT event_id, k, v, headers FROM __sprig_eo_901 ORDER BY event_id",
            "SELECT event_id, k, v, headers FROM __sprig_eo_902 ORDER BY event_id",
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

/// The callees, compiled first, in order.
fn callees() -> Vec<(&'static str, &'static str, ScriptSig)> {
    let i = ParamTy::Int;
    vec![
        (
            "price",
            "const p = db.sqlOne('SELECT price, stock FROM products WHERE id = ?', [id]);
             if (p === null) return sprig.event('missing', { id, why: 'no product ' + id });
             return { price: p.price, stock: p.stock, cheap: p.price < 50 };",
            sig(&[("id", i)]),
        ),
        (
            "take",
            "const t = db.exec('UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?', [q, id, q]);
             if (t.affected !== 1) throw new Error('out of stock: ' + id);
             db.exec('INSERT INTO audit (what) VALUES (?)', ['took ' + q + ' of ' + id]);
             const left = db.sqlOne('SELECT stock FROM products WHERE id = ?', [id]);
             return { left: left.stock };",
            sig(&[("id", i), ("q", i)]),
        ),
        (
            "check",
            "if (x === 0) sprig.reject('zero is not allowed');
             if (x < 0) throw new RangeError('negative: ' + x);
             return { v: x * 2, label: 'ok ' + x };",
            sig(&[("x", i)]),
        ),
        (
            "total",
            "const a = db.call('price', id);
             if (a.event !== 'output') return sprig.event('missing', { why: a.fields.why });
             return { total: a.fields.price * n, stock: a.fields.stock };",
            sig(&[("id", i), ("n", i)]),
        ),
    ]
}

/// The callers and the arguments of each call.
fn corpus() -> Vec<(&'static str, &'static str, ScriptSig, Vec<Vec<J>>)> {
    let i = ParamTy::Int;
    let x = || sig(&[("x", i)]);
    vec![
        (
            "fields",
            "const r = db.call('price', x);
             return { e: r.event, p: r.fields.price, s: r.fields.stock, c: r.fields.cheap,
                      w: r.fields.why };",
            x(),
            vec![vec![json!(7)], vec![json!(8)], vec![json!(70)]],
        ),
        (
            "whole",
            "const r = db.call('price', x);
             return { r, f: r.fields };",
            x(),
            vec![vec![json!(8)], vec![json!(71)]],
        ),
        (
            "writes",
            "let n = 0; let last = -1;
             for (let k = 0; k < x; k++) {
               const t = db.call('take', 8, 1);
               last = t.fields.left;
               n++;
             }
             return { n, last };",
            x(),
            vec![vec![json!(0)], vec![json!(2)], vec![json!(5)]],
        ),
        (
            "errors",
            "let caught = 0; let out = '';
             for (let j = 0; j < 3; j++) {
               const k = j - 1;
               try {
                 const r = db.call('check', k * x);
                 out = out + r.fields.label + ';';
               } catch (e) {
                 caught++;
                 db.exec('INSERT INTO audit (what) VALUES (?)', ['caught ' + k]);
               }
             }
             if (x === 3) db.call('check', 0);
             return { caught, out };",
            x(),
            vec![vec![json!(1)], vec![json!(0)], vec![json!(3)]],
        ),
        (
            "nested",
            "const t = db.call('total', x, 3);
             const u = db.call('take', x, 2);
             return { e: t.event, total: t.fields.total, left: u.fields.left };",
            x(),
            vec![vec![json!(7)], vec![json!(8)], vec![json!(9)]],
        ),
        (
            "helpers",
            "function cost(id) { const r = db.call('price', id); return r.event === 'output' ? r.fields.price : 0; }
             let s = 0;
             s += cost(7) + cost(8);
             s += cost(9);
             s += cost(x);
             return { s };",
            x(),
            vec![vec![json!(7)], vec![json!(99)]],
        ),
        (
            "publish",
            "const a = db.publish('orders_s', String(x), 'value ' + x);
             db.publish('orders_s', x, { id: x, name: 'n' + x, ok: x > 1, price: x / 4 }, { source: 'calls', n: 'x' + x });
             db.publish('orders_s', null, null);
             db.publish('orders_s', x * 1.5, true, null);
             db.publish('latest_s', 'k' + x, 'v');
             return { a: a.affected };",
            x(),
            vec![vec![json!(1)], vec![json!(6)]],
        ),
        (
            "publish_errors",
            "let caught = '';
             try {
               let s = '';
               for (let k = 0; k < 7; k++) s = s + '0123456789';
               db.publish('orders_s', 'big', s);
             } catch (e) { caught = caught + 'a'; }
             try {
               db.publish('latest_s', x > 1 ? null : 'k', 'v');
             } catch (e) {
               caught = caught + 'b';
               db.exec('INSERT INTO audit (what) VALUES (?)', ['no key']);
             }
             let big = '';
             for (let k = 0; k < x; k++) big = big + '0123456789';
             db.publish('orders_s', 'k', big);
             return { caught };",
            x(),
            vec![vec![json!(1)], vec![json!(2)], vec![json!(7)]],
        ),
        (
            // Calls and publishes inside larger expressions run first,
            // before the statement they are in.
            "inline",
            "const t = db.call('total', x, 3).fields.total;
             let out = 'e:' + db.call('price', x).event;
             if (db.call('check', x).event === 'output') out = out + ';checked';
             const n = db.publish('orders_s', 'i' + x, out).affected;
             return { t, out, n, w: db.call('price', x).fields.why, all: db.call('total', x, 1) };",
            x(),
            vec![vec![json!(7)], vec![json!(8)], vec![json!(70)]],
        ),
        (
            // The limit is on the whole record: key 1, value 50 and headers
            // `{"h":"..."}` 8 + x bytes fill 64 at x = 5.
            "publish_sizes",
            "let v = '';
             for (let k = 0; k < 5; k++) v = v + '0123456789';
             let h = '';
             for (let k = 0; k < x; k++) h = h + 'a';
             let caught = 0;
             try { db.publish('orders_s', 'k', v, { h: h }); } catch (e) { caught = 1; }
             try { db.publish('orders_s', v, h); } catch (e) { caught = caught + 2; }
             return { caught };",
            x(),
            vec![vec![json!(0)], vec![json!(5)], vec![json!(6)], vec![json!(14)], vec![json!(15)]],
        ),
    ]
}

#[ignore = "live: needs SPRIG_TEST_MARIADB_SINGLE; tools/live/README.md"]
#[test]
fn compiled_calls_and_publishes_agree_with_quickjs() {
    let Ok(url) = std::env::var("SPRIG_TEST_MARIADB_SINGLE") else {
        eprintln!("skipped: SPRIG_TEST_MARIADB_SINGLE is not set");
        panic!("live test run without its environment: see tools/live/README.md");
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
        env: Env {
            cols: StaticColumns::new(),
            callees: BTreeMap::new(),
        },
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
    s.env.cols = s.columns();
    let start = std::time::Instant::now();
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for (n, (name, js, sig)) in callees().into_iter().enumerate() {
            s.callee(name, js, &sig, 9800 + n as u64);
        }
        for (n, (name, js, sig, calls)) in corpus().into_iter().enumerate() {
            let c = s.compile(name, js, &sig, 9900 + n as u64);
            let p = c.plsql.unwrap();
            s.run_sql(JSC, &p.statements, true);
            for args in calls {
                s.case(name, js, &sig, &p.entry, &args);
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
