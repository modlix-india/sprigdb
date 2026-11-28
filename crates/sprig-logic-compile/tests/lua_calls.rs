//! Lua compiled into MariaDB by the Lua front end, against the same
//! source on the engine tier's Lua 5.1 runtime (`sprig_logic_js::Program`),
//! each on its own copy of the same tables: both must agree on the event,
//! its fields (doubles to 1e-9), the error class and every table's rows
//! after each call. The corpus is `lua_cases`: functions without data
//! access, functions reading and writing tables, and functions calling
//! other functions and publishing to streams.
//!
//! Callees are compiled into both schemas with the wrapper the deploy
//! gives them (the engine side's `db.call` calls it) and their call entry
//! (compiled callers call it). The engine side publishes through the
//! gateway's rewrite of `INSERT INTO <stream>` into the outbox, with its
//! checks; compiled code writes the outbox itself.
//!
//! Runs on the Compose single node; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `r7lua_compiled` and
//! `r7lua_engine`, dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    missing_docs
)]

mod lua_cases;

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::Value as J;
use sprig_common::{ErrorKind, Result, SprigError};
use std::collections::BTreeMap;

use sprig_common::SprigCode;
use sprig_logic_compile::ir::{Function, Ty};
use sprig_logic_compile::link::{Callee, StreamTarget};
use sprig_logic_compile::{Columns, Options, StaticColumns, lua, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

use lua_cases::Case;

const COMPILED: &str = "r7lua_compiled";
const ENGINE: &str = "r7lua_engine";
const SCHEMAS: [&str; 2] = [COMPILED, ENGINE];

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

/// A literal of the gateway's publish statement as bytes (`None` for
/// `NULL`).
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
/// as the gateway rewrites it, after its checks.
struct EngineDb {
    pool: Pool,
}

impl EngineDb {
    fn publish(sql: &str) -> Option<std::result::Result<String, SprigError>> {
        let rest = sql.strip_prefix("INSERT INTO ")?;
        let (target, rest) = rest.split_once(" (key, value, headers) VALUES (")?;
        let name = target.rsplit('.').next()?.trim_matches('`');
        let &(_, outbox, compact) = lua_cases::STREAMS.iter().find(|(n, _, _)| *n == name)?;
        let body = rest.strip_suffix(')')?;
        let lits: Vec<&str> = body.split(", ").collect();
        let sizes: Vec<Option<Vec<u8>>> = lits.iter().map(|l| literal_bytes(l)).collect();
        let key = sizes[0].as_ref().map_or(0, Vec::len);
        let record: usize = sizes.iter().flatten().map(Vec::len).sum();
        if key > 4096 || record as u64 > lua_cases::MAX_BYTES {
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
        c.query_drop(format!("USE `{ENGINE}`"))
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

/// MariaDB's runtime errors for what the IR calls 39866.
fn normal(code: u16) -> u16 {
    if matches!(code, 1365 | 1690) {
        39866
    } else {
        code
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
        (Err(x), Err(y)) => normal(*x) == normal(*y),
        _ => false,
    }
}

fn ir_ty(t: ParamTy) -> Ty {
    match t {
        ParamTy::Int => Ty::Int,
        ParamTy::Float => Ty::Float,
        ParamTy::Bool => Ty::Bool,
        ParamTy::Json => Ty::Json,
        ParamTy::Str => Ty::Str,
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
#[derive(Clone, Default)]
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
        lua_cases::STREAMS
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(n, outbox, compact)| {
                Ok(StreamTarget {
                    name: (*n).to_owned(),
                    outbox: (*outbox).to_owned(),
                    max_bytes: lua_cases::MAX_BYTES,
                    compact: *compact,
                })
            })
    }
}

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    callees: BTreeMap<String, Callee>,
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
                    c.query_drop(format!("DROP TABLE IF EXISTS {}", lua_cases::SEEDED))
                        .await?;
                    for q in lua_cases::SEED {
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
                (COMPILED,),
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

    fn env(&self) -> Env {
        Env {
            cols: self.columns(),
            callees: self.callees.clone(),
        }
    }

    fn run_sql(
        &self,
        schema: &str,
        stmts: &[String],
        oracle: bool,
    ) -> std::result::Result<(), String> {
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
            self.rt
                .block_on(conn.query_drop(s.as_str()))
                .map_err(|e| format!("{schema}: {e}\n{s}"))?;
        }
        Ok(())
    }

    /// A callee: compiled by the Lua front end into both schemas with its
    /// wrapper (engine side) and its call entry (compiled side), then known
    /// to later compiles.
    fn callee(&mut self, c: &Case, module: u64) -> std::result::Result<(), String> {
        let compiled = self.compile(c, module)?;
        let f = compiled.ir.clone().unwrap();
        let p = compiled.plsql.clone().unwrap();
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
        let name = c.name;
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
            self.run_sql(s, &p.statements, true)?;
            self.run_sql(s, &[wrapper.clone(), call_entry.clone()], false)?;
        }
        let mut reaches = compiled.analysis.reaches.clone();
        reaches.extend(compiled.analysis.calls.iter().map(|d| d.module));
        self.callees.insert(
            name.to_owned(),
            Callee {
                module_id: module,
                name: name.to_owned(),
                params: f.params.iter().map(|q| q.ty).collect(),
                fields,
                reads: compiled.analysis.reads.clone(),
                writes: compiled.analysis.writes.clone(),
                reaches,
                depth: compiled.analysis.call_depth,
            },
        );
        Ok(())
    }

    /// Compiles the case with the Lua front end; why it doesn't compile
    /// otherwise.
    fn compile(
        &self,
        c: &Case,
        module: u64,
    ) -> std::result::Result<sprig_logic_compile::Compiled, String> {
        let opts = Options {
            module_id: module,
            version: 1,
            ..Options::default()
        };
        let sig = lua::Signature {
            params: c
                .params
                .iter()
                .map(|(n, t)| ((*n).to_owned(), ir_ty(*t)))
                .collect(),
            returns: c.scalar.map(ir_ty),
            row: None,
            on: None,
        };
        let compiled = lua::compile_with(c.name, c.src, &sig, &self.env(), &opts)
            .map_err(|e| e.to_string())?;
        if !compiled.compilable() {
            return Err(format!("{:?}", compiled.analysis.reasons));
        }
        Ok(compiled)
    }

    /// Compiles the case with the Lua front end and creates it in
    /// `COMPILED`; returns its entry routine, or why it doesn't compile.
    fn deploy(&self, c: &Case, module: u64) -> std::result::Result<String, String> {
        let compiled = self.compile(c, module)?;
        let mut conn = self.conn(Some(COMPILED));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        let p = compiled.plsql.unwrap();
        for s in &p.statements {
            if let Err(e) = self.rt.block_on(conn.query_drop(s.as_str())) {
                return Err(format!("{e}: {s}"));
            }
        }
        Ok(p.entry)
    }

    fn compiled(&self, entry: &str, args: &[J]) -> Got {
        let mut conn = self.conn(Some(COMPILED));
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

    fn engine(&self, c: &Case, args: &[J]) -> Got {
        let sig = ScriptSig {
            params: c
                .params
                .iter()
                .map(|(n, t)| ((*n).to_owned(), *t))
                .collect(),
            scalar: c.scalar.is_some(),
        };
        let p = Program::new("lua", c.name, c.src, sig).unwrap();
        let host = Host {
            db: Arc::new(EngineDb {
                pool: self.pool.clone(),
            }),
            outbound: Arc::new(NoOutbound),
            default_db: Some(ENGINE.to_owned()),
            info: J::Null,
        };
        let args = p.coerce(c.name, args.to_vec()).map_err(|e| class(&e))?;
        self.rt
            .block_on(p.run(args, host, Limits::default()))
            .map(|o| (o.event, o.result))
            .map_err(|e| {
                println!("  lua error: {e}");
                class(&e)
            })
    }

    fn tables(&self, schema: &str) -> Vec<Vec<Option<String>>> {
        let mut conn = self.conn(Some(schema));
        let mut out = Vec::new();
        for q in lua_cases::TABLES {
            let rows: Vec<mysql_async::Row> = self.rt.block_on(conn.query(*q)).unwrap();
            out.extend(
                rows.into_iter()
                    .map(|r| r.unwrap().iter().map(text).collect::<Vec<_>>()),
            );
        }
        out
    }

    fn case(&mut self, c: &Case, entry: &str, args: &[J]) {
        self.cases += 1;
        self.reset();
        let want = self.engine(c, args);
        let got = self.compiled(entry, args);
        let label = format!("{}{args:?}", c.name);
        if !same(&want, &got) {
            self.failures
                .push(format!("{label}: lua {want:?} vs compiled {got:?}"));
        }
        let (a, b) = (self.tables(ENGINE), self.tables(COMPILED));
        if a != b {
            self.failures.push(format!(
                "{label}: tables differ: lua {a:?} vs compiled {b:?}"
            ));
        }
        println!("{label}: {want:?}");
    }
}

#[ignore = "live: needs SPRIG_TEST_MARIADB_SINGLE; tools/live/README.md"]
#[test]
fn compiled_lua_agrees_with_the_engine_tier_on_mariadb() {
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
        callees: BTreeMap::new(),
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
        let mut module = 7700;
        for c in lua_cases::callees() {
            module += 1;
            if let Err(why) = s.callee(&c, module) {
                s.failures.push(format!("callee {}: {why}", c.name));
            }
        }
        for c in lua_cases::pure()
            .into_iter()
            .chain(lua_cases::data())
            .chain(lua_cases::calls())
        {
            module += 1;
            match s.deploy(&c, module) {
                Ok(entry) => {
                    for args in &c.calls {
                        s.case(&c, &entry, args);
                    }
                }
                Err(why) => s.failures.push(format!("{}: {why}", c.name)),
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
        "{} calls in {elapsed:?}, {} mismatches",
        s.cases,
        s.failures.len()
    );
    assert!(
        s.failures.is_empty(),
        "mismatches:\n{}",
        s.failures.join("\n")
    );
}
