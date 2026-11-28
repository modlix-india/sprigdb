//! The wall budget of compiled code against the engine tier and the
//! reference interpreter.
//!
//! MariaDB starts its `max_statement_time` timer only for a client's
//! top-level statement: the deploy's wrapper sets the budget with `SET
//! STATEMENT ... FOR CALL` inside a procedure, which only sets the
//! variable, so a compiled call of many short statements used to run on
//! past it. The entry procedure now keeps a deadline of its own. Each case
//! here runs the same code with the same budget:
//!
//! - compiled, called through a wrapper made as the deploy makes it (or,
//!   for triggers, at the top level, as the trigger runner calls them);
//! - on the engine tier (QuickJS, or the engine's Lua);
//! - in the reference interpreter (functions only).
//!
//! It checks the error (39864, or MariaDB's 1969 where its own timer may
//! come first), that it came within the budget and a margin, and, for
//! runs that fit, that the answers and rows agree. Covered: a loop of
//! short statements in JavaScript and Lua, a caller making many short
//! nested calls, a nested call that gets only what is left of its
//! caller's budget, stream triggers in both languages, and a trigger whose
//! `catch` or `pcall` sees MariaDB's own timeout (which used to let it run
//! on). It also measures what the checks cost.
//!
//! Runs on a MariaDB 11.8 server; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `r7_wall_c` and `r7_wall_e`,
//! dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::many_single_char_names,
    missing_docs
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_logic_compile::interp::{self, Db, Value};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::js::{MsgSource, Signature};
use sprig_logic_compile::link::Callee;
use sprig_logic_compile::{Columns, Compiled, Options, StaticColumns, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};

const C: &str = "r7_wall_c";
const E: &str = "r7_wall_e";
/// The budget every case gets.
const BUDGET_MS: u64 = 300;
/// How late a stop may come: one short statement, the round trip and a
/// loaded laptop.
const MARGIN_MS: u128 = 150;
/// No loop guard in the way of the wall budget.
const MAX_LOOPS: u64 = 100_000_000;

const SEED: &[&str] = &[
    "CREATE TABLE counters (id BIGINT PRIMARY KEY, n BIGINT NOT NULL)",
    "CREATE TABLE hits (id BIGINT AUTO_INCREMENT PRIMARY KEY, what VARCHAR(64) NOT NULL)",
    "INSERT INTO counters VALUES (1, 0), (2, 0)",
];

fn my_err(e: mysql_async::Error) -> SprigError {
    match e {
        mysql_async::Error::Server(s) => SprigError::backend(s.code, *b"HY000", s.message),
        other => SprigError::internal(other.to_string()),
    }
}

fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

fn text(v: &My) -> Option<String> {
    match v {
        My::NULL => None,
        My::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        My::Int(i) => Some(i.to_string()),
        My::UInt(u) => Some(u.to_string()),
        My::Double(f) => Some(f.to_string()),
        other => Some(other.as_sql(true)),
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

/// The engine's `db`: each statement as text on `E`, on one connection
/// (so a run's transaction holds).
struct EngineDb {
    conn: tokio::sync::Mutex<Conn>,
    lines: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DbBridge for EngineDb {
    async fn exec(&self, op: DbOp) -> Result<Reply> {
        let mut c = self.conn.lock().await;
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
        drop(r);
        Ok(Reply::Rows {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.unwrap().iter().map(text).collect())
                .collect(),
        })
    }

    fn log(&self, line: &str) {
        self.lines.lock().unwrap().push(line.to_owned());
    }
}

/// The reference interpreter's `db`: statements with binds on `E`.
struct InterpDb<'a> {
    rt: &'a tokio::runtime::Runtime,
    conn: &'a mut Conn,
}

fn to_my(v: &Value) -> My {
    match v {
        Value::Null => My::NULL,
        Value::Bool(b) => My::Int(i64::from(*b)),
        Value::Int(i) => My::Int(*i),
        Value::Float(x) => My::Double(*x),
        Value::Str(s) => My::Bytes(s.clone().into_bytes()),
        Value::Json(j) => My::Bytes(j.to_string().into_bytes()),
    }
}

impl Db for InterpDb<'_> {
    fn query(&mut self, sql: &str, binds: &[Value]) -> Result<Vec<Vec<Option<String>>>> {
        let params: Vec<My> = binds.iter().map(to_my).collect();
        let rows: Vec<mysql_async::Row> = self
            .rt
            .block_on(self.conn.exec(sql, params))
            .map_err(my_err)?;
        Ok(rows
            .into_iter()
            .map(|r| r.unwrap().iter().map(text).collect())
            .collect())
    }

    fn exec(&mut self, sql: &str, binds: &[Value]) -> Result<u64> {
        let params: Vec<My> = binds.iter().map(to_my).collect();
        self.rt
            .block_on(self.conn.exec_drop(sql, params))
            .map_err(my_err)?;
        Ok(self.conn.affected_rows())
    }
}

/// The wrapper's fields of `f` (every event's fields once, booleans as
/// integers).
fn call_fields(f: &sprig_logic_compile::ir::Function) -> Vec<(String, Ty)> {
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

/// The compile-time view: `C`'s columns and the callees made so far.
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
}

/// What a run gave: the result (or the error number) and how long it took.
#[derive(Debug)]
struct Ran {
    got: std::result::Result<J, u16>,
    ms: u128,
}

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    env: Env,
    checks: usize,
    failures: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lang {
    Js,
    Lua,
}

impl Lang {
    fn name(self) -> &'static str {
        match self {
            Lang::Js => "javascript",
            Lang::Lua => "lua",
        }
    }
}

impl Suite {
    fn conn(&self, schema: &str) -> Conn {
        let mut c = self.rt.block_on(self.pool.get_conn()).unwrap();
        self.rt
            .block_on(c.query_drop(format!("USE `{schema}`")))
            .unwrap();
        c
    }

    fn reset(&self) {
        for s in [C, E] {
            let mut c = self.conn(s);
            self.rt
                .block_on(async {
                    c.query_drop("DROP TABLE IF EXISTS counters, hits").await?;
                    for q in SEED {
                        c.query_drop(*q).await?;
                    }
                    Ok::<_, mysql_async::Error>(())
                })
                .unwrap();
        }
    }

    fn columns(&self) -> StaticColumns {
        let mut c = self.conn(C);
        let rows: Vec<(String, String, String, String)> = self
            .rt
            .block_on(c.exec(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?",
                (C,),
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

    fn run_sql(&self, stmts: &[String], oracle: bool) {
        let mut conn = self.conn(C);
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
                panic!("{e}\n{s}");
            }
        }
    }

    fn compile(&self, lang: Lang, name: &str, src: &str, sig: &Signature, module: u64) -> Compiled {
        let opts = Options {
            module_id: module,
            version: 1,
            max_loop_iterations: MAX_LOOPS,
            compile_max_cost: u64::MAX / 4,
            ..Options::default()
        };
        let c = match lang {
            Lang::Js => {
                sprig_logic_compile::js::compile_with(name, src, sig, &self.env, &opts).unwrap()
            }
            Lang::Lua => {
                sprig_logic_compile::lua::compile_with(name, src, sig, &self.env, &opts).unwrap()
            }
        };
        assert!(
            c.compilable(),
            "{name}: doesn't compile: {:#?}",
            c.analysis.reasons
        );
        c
    }

    /// Compiles a function into `C` with a wrapper made as the deploy makes
    /// it (the budget passed with `SET STATEMENT` inside the procedure, in
    /// its own transaction) and its call entry, and makes it known to
    /// later compiles. Returns the compiled function.
    fn function(
        &mut self,
        lang: Lang,
        name: &str,
        src: &str,
        params: &[(&str, ParamTy)],
        module: u64,
    ) -> Compiled {
        let sig = Signature {
            params: params
                .iter()
                .map(|(n, t)| ((*n).to_owned(), ty_of(*t)))
                .collect(),
            ..Signature::default()
        };
        let c = self.compile(lang, name, src, &sig, module);
        let f = c.ir.clone().unwrap();
        let p = c.plsql.clone().unwrap();
        let decl: Vec<String> = f
            .params
            .iter()
            .map(|q| format!("IN p_{} {}", q.name, plsql::sql_type(q.ty)))
            .collect();
        let args: Vec<String> = f.params.iter().map(|q| format!("p_{}", q.name)).collect();
        let sep = if args.is_empty() { "" } else { ", " };
        let budget = BUDGET_MS as f64 / 1000.0;
        let wrapper = format!(
            "CREATE OR REPLACE PROCEDURE `{name}`({}) SQL SECURITY INVOKER\nBEGIN\n  \
             DECLARE v__event VARCHAR(64);\n  DECLARE v__result LONGTEXT;\n  \
             DECLARE v__own INT DEFAULT 0;\n  DECLARE v__budget DOUBLE DEFAULT {budget};\n  \
             DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN IF v__own = 1 THEN ROLLBACK; END IF; RESIGNAL; END;\n  \
             IF @@max_statement_time > 0 AND @@max_statement_time < v__budget THEN SET v__budget = @@max_statement_time; END IF;\n  \
             IF @@in_transaction = 0 AND @@autocommit = 1 THEN START TRANSACTION; SET v__own = 1; END IF;\n  \
             SET STATEMENT max_statement_time = v__budget FOR CALL `{}`({}{sep}v__event, v__result);\n  \
             IF v__own = 1 THEN COMMIT; END IF;\n  SELECT v__event AS `event`, v__result AS `result`;\nEND",
            decl.join(", "),
            p.entry,
            args.join(", "),
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
        self.run_sql(&p.statements, true);
        self.run_sql(&[wrapper, call_entry], false);
        let fields = call_fields(&f);
        self.env.callees.insert(
            name.to_owned(),
            Callee {
                module_id: module,
                name: name.to_owned(),
                params: f.params.iter().map(|q| q.ty).collect(),
                fields,
                reads: c.analysis.reads.clone(),
                writes: c.analysis.writes.clone(),
                reaches: {
                    let mut r = c.analysis.reaches.clone();
                    r.extend(c.analysis.calls.iter().map(|d| d.module));
                    r
                },
                depth: c.analysis.call_depth,
            },
        );
        c
    }

    /// The compiled side: `CALL name(args)` from a client, as the gateway
    /// passes it on.
    fn compiled(&self, name: &str, args: &[J]) -> Ran {
        let mut conn = self.conn(C);
        let marks = vec!["?"; args.len()];
        let call = format!("CALL `{name}`({})", marks.join(", "));
        let params: Vec<My> = args.iter().map(json_my).collect();
        let t0 = Instant::now();
        let r: std::result::Result<Option<(Option<String>, Option<String>)>, _> =
            self.rt.block_on(conn.exec_first(call.as_str(), params));
        let ms = t0.elapsed().as_millis();
        let got = match r {
            Ok(Some((Some(e), r))) => Ok(json!({
                "event": e,
                "result": serde_json::from_str::<J>(&r.unwrap_or_else(|| "null".into())).unwrap(),
            })),
            Ok(_) => Err(0),
            Err(e) => {
                println!("    compiled: {e}");
                Err(class(&my_err(e)))
            }
        };
        Ran { got, ms }
    }

    /// The engine tier with the same budget (and no other limit in the
    /// way), on `E`.
    fn engine(&self, lang: Lang, name: &str, src: &str, sig: ScriptSig, args: Vec<J>) -> Ran {
        let p = Program::new(lang.name(), name, src, sig).unwrap();
        let conn = self.conn(E);
        let db = Arc::new(EngineDb {
            conn: tokio::sync::Mutex::new(conn),
            lines: std::sync::Mutex::new(Vec::new()),
        });
        let host = Host {
            db: Arc::clone(&db) as Arc<dyn DbBridge>,
            outbound: Arc::new(NoOutbound),
            default_db: Some(E.to_owned()),
            info: args.first().cloned().unwrap_or(J::Null),
        };
        let limits = Limits {
            wall: Duration::from_millis(BUDGET_MS),
            max_data_ops: u64::MAX,
            max_rows_read: u64::MAX,
            max_steps: u64::MAX,
            max_loop_iterations: MAX_LOOPS,
            ..Limits::default()
        };
        let t0 = Instant::now();
        let r = self.rt.block_on(p.run(args, host, limits));
        let ms = t0.elapsed().as_millis();
        let got = match r {
            Ok(o) => Ok(json!({"event": o.event, "result": o.result})),
            Err(e) => {
                println!("    engine: {e}");
                Err(class(&e))
            }
        };
        Ran { got, ms }
    }

    /// The reference interpreter with the same budget, on `E`.
    fn interp(&self, c: &Compiled, args: &[Value]) -> Ran {
        let mut conn = self.conn(E);
        let mut db = InterpDb {
            rt: &self.rt,
            conn: &mut conn,
        };
        let lim = interp::Limits {
            wall: Some(Duration::from_millis(BUDGET_MS)),
            max_loop_iterations: MAX_LOOPS,
        };
        let t0 = Instant::now();
        let r = interp::run(c.ir.as_ref().unwrap(), args, &mut db, &lim);
        let ms = t0.elapsed().as_millis();
        let got = match r {
            Ok(o) => Ok(json!({"event": o.event, "result": o.result})),
            Err(e) => {
                println!("    interpreter: {e}");
                Err(class(&e))
            }
        };
        Ran { got, ms }
    }

    fn check(&mut self, ok: bool, what: String) {
        self.checks += 1;
        println!("  {} {what}", if ok { "ok  " } else { "FAIL" });
        if !ok {
            self.failures.push(what);
        }
    }

    /// A run that must stop on the budget: 39864 (or 1969, MariaDB's own
    /// timer, where the runner sets one) within the budget and the margin.
    fn stopped(&mut self, label: &str, r: &Ran, timer: bool) {
        let code_ok = match r.got {
            Err(39864) => true,
            Err(1969) => timer,
            _ => false,
        };
        let late = r.ms > u128::from(BUDGET_MS) + MARGIN_MS;
        let early = r.ms + 20 < u128::from(BUDGET_MS);
        self.check(
            code_ok && !late && !early,
            format!("{label}: stopped after {} ms with {:?}", r.ms, r.got),
        );
    }

    fn counter(&self, schema: &str, id: u32) -> i64 {
        let mut c = self.conn(schema);
        self.rt
            .block_on(c.exec_first("SELECT n FROM counters WHERE id = ?", (id,)))
            .unwrap()
            .unwrap()
    }
}

fn json_my(v: &J) -> My {
    match v {
        J::Null => My::NULL,
        J::Bool(b) => My::Int(i64::from(*b)),
        J::Number(n) => n
            .as_i64()
            .map_or_else(|| My::Double(n.as_f64().unwrap()), My::Int),
        J::String(s) => My::Bytes(s.clone().into_bytes()),
        other => My::Bytes(other.to_string().into_bytes()),
    }
}

fn sig(params: &[(&str, ParamTy)]) -> ScriptSig {
    ScriptSig {
        params: params.iter().map(|(n, t)| ((*n).to_owned(), *t)).collect(),
        scalar: false,
    }
}

/// Short statements in a loop: a write each time round.
const SPIN_JS: &str = "let t = 0;
  for (let i = 0; i < n; i++) {
    const w = db.exec('UPDATE counters SET n = n + 1 WHERE id = 2');
    t = t + w.affected;
  }
  return { t };";

const SPIN_LUA: &str = "local t = 0
  for i = 1, n do
    local w = db.exec('UPDATE counters SET n = n + 1 WHERE id = 2')
    t = t + w.affected
  end
  return { t = t }";

/// A stream trigger doing the same per record.
const TRIG_JS: &str = "let t = 0;
  for (let i = 0; i < 100000000; i++) {
    const r = db.sqlOne('SELECT n FROM counters WHERE id = 1');
    if (r !== null) t = t + 1;
  }
  db.exec('INSERT INTO hits (what) VALUES (?)', [ctx.record.key]);";

const TRIG_LUA: &str = "local t = 0
  for i = 1, 100000000 do
    local r = db.sqlOne('SELECT n FROM counters WHERE id = 1')
    if r ~= nil then t = t + 1 end
  end
  db.exec('INSERT INTO hits (what) VALUES (?)', ctx.record.key)";

/// A trigger whose `catch` (`pcall`) sees each slow statement's error: once
/// MariaDB's timer has fired and been caught, nothing else stopped it.
const CAUGHT_JS: &str = "let caught = 0;
  for (let i = 0; i < 20; i++) {
    try {
      db.sqlOne('SELECT COUNT(*) AS c FROM big WHERE MD5(id) LIKE ?', ['ab%']);
    } catch (e) {
      caught = caught + 1;
    }
  }
  db.exec('INSERT INTO hits (what) VALUES (?)', ['caught ' + caught]);";

const CAUGHT_LUA: &str = "local caught = 0
  for i = 1, 20 do
    local ok = pcall(function ()
      db.sqlOne('SELECT COUNT(*) AS c FROM big WHERE MD5(id) LIKE ?', 'ab%')
    end)
    if not ok then caught = caught + 1 end
  end
  db.exec('INSERT INTO hits (what) VALUES (?)', 'caught ' .. caught)";

/// A slow statement MariaDB's timer interrupts without an error
/// (`SLEEP` just returns early), so the run went on.
const NAPS_JS: &str = "let t = 0;
  for (let i = 0; i < 40; i++) {
    const r = db.sqlOne('SELECT n FROM counters WHERE id = 1 AND SLEEP(0.05) = 0');
    if (r !== null) t = t + 1;
  }
  db.exec('INSERT INTO hits (what) VALUES (?)', ['naps ' + t]);";

const NAPS_LUA: &str = "local t = 0
  for i = 1, 40 do
    local r = db.sqlOne('SELECT n FROM counters WHERE id = 1 AND SLEEP(0.05) = 0')
    if r ~= nil then t = t + 1 end
  end
  db.exec('INSERT INTO hits (what) VALUES (?)', 'naps ' .. t)";

/// Logs, then fails: a compiled run keeps the lines written before the
/// error, as the engine tier does.
const LOGS_JS: &str = "console.log('first', ctx.record.key);
  console.warn('second');
  db.exec('INSERT INTO hits (what) VALUES (?)', ['x']);
  if (ctx.record.key === 'boom') throw new Error('boom');";

const LOGS_LUA: &str = "print('first', ctx.record.key)
  ctx.log('second')
  db.exec('INSERT INTO hits (what) VALUES (?)', 'x')
  if ctx.record.key == 'boom' then error('boom') end";

impl Suite {
    /// The function cases, in one language.
    fn functions(&mut self, lang: Lang, base: u64) {
        let i = ParamTy::Int;
        let spin = match lang {
            Lang::Js => SPIN_JS,
            Lang::Lua => SPIN_LUA,
        };
        let l = lang.name();
        let c = self.function(lang, "spin", spin, &[("n", i)], base);

        // Fits: the same answer and rows everywhere.
        self.reset();
        let a = self.compiled("spin", &[json!(50)]);
        let b = self.engine(lang, "spin", spin, sig(&[("n", i)]), vec![json!(50)]);
        let r = self.interp(&c, &[Value::Int(50)]);
        let same = a.got.is_ok() && a.got == b.got && b.got == r.got;
        let rows = (self.counter(C, 2), self.counter(E, 2));
        self.check(
            same && rows == (50, 100),
            format!(
                "{l} spin(50) fits: compiled {:?}, engine {:?}, interpreter {:?}, rows {rows:?}",
                a.got, b.got, r.got
            ),
        );

        // 1,000,000 short statements under 300 ms.
        self.reset();
        let a = self.compiled("spin", &[json!(1_000_000)]);
        self.stopped(&format!("{l} spin(1e6) compiled"), &a, false);
        let n = self.counter(C, 2);
        self.check(
            n == 0,
            format!("{l} spin(1e6) compiled: its writes rolled back ({n} left)"),
        );
        let b = self.engine(lang, "spin", spin, sig(&[("n", i)]), vec![json!(1_000_000)]);
        self.stopped(&format!("{l} spin(1e6) engine"), &b, false);
        if lang == Lang::Js {
            let r = self.interp(&c, &[Value::Int(1_000_000)]);
            self.stopped(&format!("{l} spin(1e6) interpreter"), &r, false);
        }

        // A client's own, smaller max_statement_time is the budget.
        self.reset();
        let mut conn = self.conn(C);
        let t0 = Instant::now();
        let r = self.rt.block_on(async {
            conn.query_drop("SET SESSION max_statement_time = 0.1")
                .await?;
            let r = conn.query_drop("CALL spin(1000000)").await;
            conn.query_drop("SET SESSION max_statement_time = 0")
                .await?;
            r
        });
        let ms = t0.elapsed().as_millis();
        let code = r.err().map(|e| class(&my_err(e)));
        self.check(
            matches!(code, Some(39864 | 1969)) && ms < 100 + MARGIN_MS,
            format!("{l} spin(1e6) with the session's max_statement_time = 0.1: {ms} ms, {code:?}"),
        );
    }

    /// Nested compiled calls (JavaScript and Lua callers of a callee in
    /// the same language).
    fn nested(&mut self, lang: Lang, base: u64) {
        let i = ParamTy::Int;
        let l = lang.name();
        let (few, many, late) = match lang {
            Lang::Js => (
                "let t = 0;
                 for (let i = 0; i < m; i++) {
                   const r = db.sqlOne('SELECT n FROM counters WHERE id = 1');
                   if (r !== null) t = t + 1;
                 }
                 return { t };",
                "let t = 0;
                 for (let i = 0; i < k; i++) { const r = db.call('few', 20); t = t + r.fields.t; }
                 return { t };",
                "let t = 0;
                 for (let i = 0; i < a; i++) {
                   const r = db.sqlOne('SELECT n FROM counters WHERE id = 1');
                   if (r !== null) t = t + 1;
                 }
                 const r = db.call('few', 100000000);
                 return { t: t + r.fields.t };",
            ),
            Lang::Lua => (
                "local t = 0
                 for i = 1, m do
                   local r = db.sqlOne('SELECT n FROM counters WHERE id = 1')
                   if r ~= nil then t = t + 1 end
                 end
                 return { t = t }",
                "local t = 0
                 for i = 1, k do local r = db.call('few', 20); t = t + r.fields.t end
                 return { t = t }",
                "local t = 0
                 for i = 1, a do
                   local r = db.sqlOne('SELECT n FROM counters WHERE id = 1')
                   if r ~= nil then t = t + 1 end
                 end
                 local r = db.call('few', 100000000)
                 return { t = t + r.fields.t }",
            ),
        };
        self.env.callees.clear();
        self.function(lang, "few", few, &[("m", i)], base);
        self.function(lang, "many", many, &[("k", i)], base + 1);
        self.function(lang, "late", late, &[("a", i)], base + 2);
        self.reset();
        // Fits.
        let a = self.compiled("many", &[json!(3)]);
        self.check(
            a.got == Ok(json!({"event": "output", "result": {"t": 60}})),
            format!("{l} many(3) fits: {:?}", a.got),
        );
        // Many short nested calls.
        let a = self.compiled("many", &[json!(1_000_000)]);
        self.stopped(&format!("{l} many(1e6) compiled"), &a, false);
        // The callee gets only what is left: about 60% of the budget goes
        // on the caller's own statements first (calibrated here), so a
        // fresh budget for the callee would end about 180 ms late.
        let probe = self.compiled("few", &[json!(2000)]);
        let per_ms = 2000.0 / probe.ms.max(1) as f64;
        let a_n = (per_ms * BUDGET_MS as f64 * 0.6) as i64;
        let a = self.compiled("late", &[json!(a_n)]);
        self.stopped(
            &format!("{l} late({a_n}): the nested call shares what is left"),
            &a,
            false,
        );
    }

    /// Stream triggers as the gateway runs them: `SET STATEMENT
    /// max_statement_time = budget FOR CALL entry(...)` at the top level,
    /// in a transaction; the engine tier with the same budget.
    fn triggers(&mut self, lang: Lang, base: u64) {
        let l = lang.name();
        let cases: [(&str, &str); 3] = match lang {
            Lang::Js => [("loop", TRIG_JS), ("caught", CAUGHT_JS), ("naps", NAPS_JS)],
            Lang::Lua => [
                ("loop", TRIG_LUA),
                ("caught", CAUGHT_LUA),
                ("naps", NAPS_LUA),
            ],
        };
        for (n, (name, src)) in cases.into_iter().enumerate() {
            self.reset();
            let (entry, params) = self.trigger(lang, name, src, base + n as u64);
            let ev = json!({"stream": "shop.events", "partition": 0, "offset": 1, "key": "k1",
                            "value": null, "headers": {}});
            let a = self.run_trigger(&entry, &params, &ev);
            self.stopped(&format!("{l} trigger {name} compiled"), &a.0, true);
            let b = self.engine_trigger(lang, name, src, &ev);
            self.stopped(&format!("{l} trigger {name} engine"), &b.0, false);
        }
        // Log lines of a failed compiled run are kept, as the engine
        // tier keeps them.
        self.reset();
        let src = match lang {
            Lang::Js => LOGS_JS,
            Lang::Lua => LOGS_LUA,
        };
        let (entry, params) = self.trigger(lang, "logs", src, base + 9);
        for key in ["ok", "boom"] {
            let ev = json!({"stream": "shop.events", "partition": 0, "offset": 1, "key": key,
                            "value": null, "headers": {}});
            let (a, al) = self.run_trigger(&entry, &params, &ev);
            let (b, bl) = self.engine_trigger(lang, "logs", src, &ev);
            self.check(
                al == bl && a.got.is_ok() == b.got.is_ok() && !al.is_empty(),
                format!(
                    "{l} trigger logs({key}): compiled {:?} {al:?}, engine {:?} {bl:?}",
                    a.got, b.got
                ),
            );
        }
    }

    fn trigger(
        &self,
        lang: Lang,
        name: &str,
        src: &str,
        module: u64,
    ) -> (String, Vec<(String, Ty)>) {
        let opts = Options {
            module_id: module,
            version: 1,
            definer_security: true,
            max_loop_iterations: MAX_LOOPS,
            compile_max_cost: u64::MAX / 4,
        };
        let s = Signature::message(MsgSource::Stream);
        let c = match lang {
            Lang::Js => {
                sprig_logic_compile::js::compile_with(name, src, &s, &self.columns(), &opts)
                    .unwrap()
            }
            Lang::Lua => {
                sprig_logic_compile::lua::compile_with(name, src, &s, &self.columns(), &opts)
                    .unwrap()
            }
        };
        assert!(
            c.compilable(),
            "{name}: doesn't compile: {:#?}",
            c.analysis.reasons
        );
        let p = c.plsql.unwrap();
        self.run_sql(&p.statements, true);
        let f = c.ir.unwrap();
        (
            p.entry,
            f.params.iter().map(|q| (q.name.clone(), q.ty)).collect(),
        )
    }

    /// One compiled run, as the gateway's trigger runner makes it; its log
    /// lines (read back from the variable on a failure, as the runner
    /// does).
    fn run_trigger(&self, entry: &str, params: &[(String, Ty)], ev: &J) -> (Ran, Vec<String>) {
        let args: Vec<My> = params
            .iter()
            .map(|(n, _)| json_my(ev.get(n.as_str()).unwrap_or(&J::Null)))
            .collect();
        let mut marks = vec!["?"; args.len()];
        marks.extend(["@__sprig_e", "@__sprig_r", plsql::LOG_VAR]);
        let logs = self.has_log(entry);
        if !logs {
            marks.pop();
        }
        let call = format!(
            "SET STATEMENT max_statement_time = {} FOR CALL {entry}({})",
            BUDGET_MS as f64 / 1000.0,
            marks.join(", ")
        );
        let mut conn = self.conn(C);
        let t0 = Instant::now();
        let r = self.rt.block_on(async {
            conn.query_drop("START TRANSACTION").await?;
            conn.exec_drop(call.as_str(), args).await
        });
        let ms = t0.elapsed().as_millis();
        let read = format!("SELECT {}", plsql::LOG_VAR);
        let lines: Option<(Option<String>,)> = self.rt.block_on(conn.query_first(read)).unwrap();
        let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
        self.rt.block_on(conn.query_drop(end)).unwrap();
        let lines: Vec<String> = lines
            .and_then(|(l,)| l)
            .map(|l| serde_json::from_str(&l).unwrap())
            .unwrap_or_default();
        let got = match r {
            Ok(()) => Ok(J::Null),
            Err(e) => {
                println!("    compiled: {e}");
                Err(class(&my_err(e)))
            }
        };
        (Ran { got, ms }, lines)
    }

    fn engine_trigger(&self, lang: Lang, name: &str, src: &str, ev: &J) -> (Ran, Vec<String>) {
        let ctx = json!({"user": "app", "tenant": 1, "database": "shop",
                         "trigger": {"binding": 1, "kind": "stream"}, "record": ev});
        let p = Program::new(lang.name(), name, src, sig(&[("ctx", ParamTy::Json)])).unwrap();
        let mut conn = self.conn(E);
        self.rt
            .block_on(conn.query_drop("START TRANSACTION"))
            .unwrap();
        let db = Arc::new(EngineDb {
            conn: tokio::sync::Mutex::new(conn),
            lines: std::sync::Mutex::new(Vec::new()),
        });
        let host = Host {
            db: Arc::clone(&db) as Arc<dyn DbBridge>,
            outbound: Arc::new(NoOutbound),
            default_db: Some(E.to_owned()),
            info: ctx.clone(),
        };
        let limits = Limits {
            wall: Duration::from_millis(BUDGET_MS),
            max_data_ops: u64::MAX,
            max_rows_read: u64::MAX,
            max_steps: u64::MAX,
            max_loop_iterations: MAX_LOOPS,
            ..Limits::default()
        };
        let t0 = Instant::now();
        let r = self.rt.block_on(p.run(vec![ctx], host, limits));
        let ms = t0.elapsed().as_millis();
        let end = if r.is_ok() { "COMMIT" } else { "ROLLBACK" };
        self.rt
            .block_on(async { db.conn.lock().await.query_drop(end).await })
            .unwrap();
        let got = match r {
            Ok(o) if matches!(o.event.as_str(), "error" | "reject") => Err(39865),
            Ok(_) => Ok(J::Null),
            Err(e) => {
                println!("    engine: {e}");
                Err(class(&e))
            }
        };
        let lines = db.lines.lock().unwrap().clone();
        (Ran { got, ms }, lines)
    }

    fn has_log(&self, entry: &str) -> bool {
        let mut c = self.conn(C);
        let n: Option<u64> = self
            .rt
            .block_on(c.exec_first(
                "SELECT COUNT(*) FROM information_schema.PARAMETERS WHERE SPECIFIC_SCHEMA = ? \
                 AND SPECIFIC_NAME = ? AND PARAMETER_NAME = 'p__log'",
                (C, entry),
            ))
            .unwrap();
        n == Some(1)
    }

    /// What the checks cost: each loop compiled as the deploy compiles it,
    /// and the same text with the checks taken out, run in a transaction
    /// under a budget that never runs out; the best of five, per iteration.
    fn overhead(&mut self) {
        self.env.callees.clear();
        self.reset();
        let loops = [
            (
                "a primary-key read",
                "let t = 0;
                 for (let i = 0; i < n; i++) {
                   const r = db.sqlOne('SELECT n FROM counters WHERE id = 1');
                   if (r !== null) t = t + r.n;
                 }
                 return { t };",
                50_000,
            ),
            (
                "an update",
                "let t = 0;
                 for (let i = 0; i < n; i++) {
                   const w = db.exec('UPDATE counters SET n = n + 1 WHERE id = 2');
                   t = t + w.affected;
                 }
                 return { t };",
                20_000,
            ),
            (
                "no statement",
                "let t = 0;
                 for (let i = 0; i < n; i++) { t = t + i % 7; }
                 return { t };",
                500_000,
            ),
        ];
        for (k, (what, src, n)) in loops.into_iter().enumerate() {
            let module = 9790 + 2 * k as u64;
            let c = self.compile(
                Lang::Js,
                "cost",
                src,
                &Signature {
                    params: vec![("n".to_owned(), Ty::Int)],
                    ..Signature::default()
                },
                module,
            );
            let p = c.plsql.unwrap();
            let (with, without) = (plsql::names(module, 1).1, plsql::names(module + 1, 1).1);
            let plain: Vec<String> = p
                .statements
                .iter()
                .map(|s| {
                    s.replace(&with, &without)
                        .lines()
                        .filter(|l| !l.contains("SYSDATE(6) > v__deadline"))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .collect();
            self.run_sql(&p.statements, true);
            self.run_sql(&plain, true);
            let mut conn = self.conn(C);
            let mut best = |entry: &str| {
                (0..5)
                    .map(|_| {
                        let sql = format!(
                            "SET STATEMENT max_statement_time = 1000 FOR CALL `{entry}`({n}, @e, @r)"
                        );
                        self.rt.block_on(conn.query_drop("START TRANSACTION")).unwrap();
                        let t0 = Instant::now();
                        self.rt.block_on(conn.query_drop(sql)).unwrap();
                        let us = t0.elapsed().as_secs_f64() * 1e6 / f64::from(n);
                        self.rt.block_on(conn.query_drop("ROLLBACK")).unwrap();
                        us
                    })
                    .fold(f64::MAX, f64::min)
            };
            let checked = best(&with);
            let unchecked = best(&without);
            println!(
                "  cost, a loop of {what}: {checked:.2} µs per iteration with the checks, \
                 {unchecked:.2} without ({:+.2} µs, {:+.0}%)",
                checked - unchecked,
                (checked - unchecked) / unchecked * 100.0
            );
        }
    }
}

#[ignore = "live: needs SPRIG_TEST_MARIADB_SINGLE; tools/live/README.md"]
#[test]
fn compiled_code_stops_at_its_wall_budget() {
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
            .with_inactive_connection_ttl(Duration::from_secs(30)),
    );
    let mut s = Suite {
        rt,
        pool: Pool::new(opts),
        env: Env::default(),
        checks: 0,
        failures: Vec::new(),
    };
    {
        let mut c = s.rt.block_on(s.pool.get_conn()).unwrap();
        for db in [C, E] {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
            s.rt.block_on(c.query_drop(format!("CREATE DATABASE `{db}` CHARACTER SET utf8mb4")))
                .unwrap();
            // 300,000 rows: a scan of them takes about 100 ms.
            s.rt.block_on(c.query_drop(format!(
                "CREATE TABLE `{db}`.big (id BIGINT PRIMARY KEY) \
                 SELECT seq AS id FROM `{db}`.seq_1_to_300000"
            )))
            .unwrap();
        }
    }
    s.reset();
    s.env.cols = s.columns();
    let run = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for (k, lang) in [Lang::Js, Lang::Lua].into_iter().enumerate() {
            let base = 9700 + 20 * k as u64;
            s.env.callees.clear();
            s.functions(lang, base);
            s.nested(lang, base + 5);
            s.triggers(lang, base + 10);
        }
        s.overhead();
    }));
    {
        let mut c = s.rt.block_on(s.pool.get_conn()).unwrap();
        for db in [C, E] {
            s.rt.block_on(c.query_drop(format!("DROP DATABASE IF EXISTS `{db}`")))
                .unwrap();
        }
    }
    s.rt.block_on(s.pool.clone().disconnect()).unwrap();
    if let Err(p) = run {
        std::panic::resume_unwind(p);
    }
    println!("{} checks, {} failed", s.checks, s.failures.len());
    assert!(s.failures.is_empty(), "failed:\n{}", s.failures.join("\n"));
}
