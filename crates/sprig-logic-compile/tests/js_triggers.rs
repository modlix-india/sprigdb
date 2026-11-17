//! Triggers on streams and queues compiled into MariaDB against QuickJS:
//! each trigger runs once per record or message, as a gateway runs it, on
//! its own copy of the same tables:
//!
//! ```text
//! engine side:   START TRANSACTION; QuickJS with ctx.record or ctx.message;
//!                COMMIT, or ROLLBACK when the run fails
//! compiled side: START TRANSACTION; SAVEPOINT; CALL the version's entry
//!                with the fields it reads; on 39876 (a record it must run
//!                in the engine tier) ROLLBACK TO SAVEPOINT and QuickJS;
//!                COMMIT, or ROLLBACK when the run fails
//! ```
//!
//! and compares whether the run failed (and its error number), its log
//! lines and every table's rows afterwards. It also counts the runs the
//! compiled side handed to QuickJS, which must be exactly the records
//! whose JSON MariaDB can't treat as JavaScript does.
//!
//! Runs on a MariaDB 11.8 server; skipped without
//! `SPRIG_TEST_MARIADB_SINGLE`. Works in `trig_diff_js` and `trig_diff_jsc`,
//! dropped afterwards.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::float_cmp,
    clippy::needless_pass_by_value,
    missing_docs
)]

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value as My};
use serde_json::{Value as J, json};
use sprig_common::{ErrorKind, Result, SprigError};
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::js::{MsgSource, Signature};
use sprig_logic_compile::{Options, StaticColumns, plsql, sql};
use sprig_logic_js::bridge::{self as eb, Column, DbBridge, DbOp, Reply, Shape};
use sprig_logic_js::outbound::NoOutbound;
use sprig_logic_js::{Host, Limits, ParamTy, Program, ScriptSig};
use tokio::sync::Mutex;

const JS: &str = "trig_diff_js";
const JSC: &str = "trig_diff_jsc";
const SCHEMAS: [&str; 2] = [JS, JSC];

const SEED: &[&str] = &[
    "CREATE TABLE hits (id BIGINT AUTO_INCREMENT PRIMARY KEY, k VARCHAR(64) NULL, \
     what VARCHAR(255) NULL, n DOUBLE NULL)",
    "CREATE TABLE stock (sku VARCHAR(16) PRIMARY KEY, qty INT NOT NULL)",
    "CREATE TABLE jobs (id BIGINT PRIMARY KEY, qty INT NOT NULL, note VARCHAR(32) NULL, \
     urgent BOOLEAN NULL)",
    "INSERT INTO stock VALUES ('a', 10), ('b', 0)",
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

fn class(e: &SprigError) -> u16 {
    match e.kind {
        ErrorKind::Sprig(c) => c.number(),
        ErrorKind::Backend { errno, .. } | ErrorKind::Standard { errno, .. } => errno,
    }
}

/// The engine's `db`: each statement on the run's own connection, inside
/// its transaction, as the gateway runs it in the definer's session; log
/// lines kept.
struct EngineDb {
    conn: Arc<Mutex<Conn>>,
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
        Ok(Reply::Rows {
            columns,
            rows: rows
                .into_iter()
                .map(|row| row.unwrap().iter().map(text).collect())
                .collect(),
        })
    }

    fn log(&self, line: &str) {
        let mut v = self.lines.lock().unwrap();
        if v.len() < 100 {
            v.push(line.to_owned());
        }
    }
}

/// A run's outcome: its log lines, or the error number it failed with.
type Got = std::result::Result<Vec<String>, u16>;

struct Suite {
    rt: tokio::runtime::Runtime,
    pool: Pool,
    cases: usize,
    reruns: usize,
    failures: Vec<String>,
}

/// A trigger: its source, what it reads, and the records or messages.
struct Case {
    name: &'static str,
    on: MsgSource,
    js: &'static str,
    events: Vec<J>,
    /// The events the compiled side must hand to QuickJS, by index.
    reruns: Vec<usize>,
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
                    c.query_drop("DROP TABLE IF EXISTS hits, stock, jobs")
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

    /// Compiles the trigger and creates it in `JSC`: its entry routine
    /// and its parameters (names and types).
    fn deploy(&self, c: &Case, module: u64) -> (String, Vec<(String, Ty)>) {
        let opts = Options {
            module_id: module,
            version: 1,
            definer_security: true,
            ..Options::default()
        };
        let compiled = sprig_logic_compile::js::compile_with(
            c.name,
            c.js,
            &Signature::message(c.on),
            &self.columns(),
            &opts,
        )
        .unwrap();
        assert!(
            compiled.compilable(),
            "{}: the trigger doesn't compile: {:#?}",
            c.name,
            compiled.analysis.reasons
        );
        let f = compiled.ir.unwrap();
        let mut conn = self.conn(Some(JSC));
        self.rt
            .block_on(conn.query_drop(format!("SET SESSION sql_mode = '{}'", plsql::SQL_MODE)))
            .unwrap();
        let p = compiled.plsql.unwrap();
        for s in &p.statements {
            if let Err(e) = self.rt.block_on(conn.query_drop(s.as_str())) {
                panic!("{}: {e}\n{s}", c.name);
            }
        }
        (
            p.entry,
            f.params.iter().map(|p| (p.name.clone(), p.ty)).collect(),
        )
    }

    /// QuickJS, with `ctx.record` or `ctx.message` = `event`, on `conn`.
    fn quickjs(&self, c: &Case, event: &J, conn: &Arc<Mutex<Conn>>) -> Got {
        let sig = ScriptSig {
            params: vec![("ctx".to_owned(), ParamTy::Json)],
            scalar: false,
        };
        let p = Program::new("javascript", c.name, c.js, sig).unwrap();
        let mut ctx = json!({"user": "app", "tenant": 1, "database": "shop",
            "trigger": {"binding": 1, "kind": if c.on == MsgSource::Stream { "stream" } else { "queue" }}});
        ctx[c.on.member()] = event.clone();
        let db = Arc::new(EngineDb {
            conn: Arc::clone(conn),
            lines: std::sync::Mutex::new(Vec::new()),
        });
        let host = Host {
            db: Arc::clone(&db) as Arc<dyn DbBridge>,
            outbound: Arc::new(NoOutbound),
            default_db: None,
            info: ctx.clone(),
        };
        let r = self.rt.block_on(p.run(vec![ctx], host, Limits::default()));
        match r {
            Ok(o) if matches!(o.event.as_str(), "error" | "reject") => Err(39865),
            Ok(_) => Ok(db.lines.lock().unwrap().clone()),
            Err(e) => {
                println!("  quickjs error: {e}");
                Err(class(&e))
            }
        }
    }

    /// One run in a transaction on `schema`'s connection: committed when
    /// it works, rolled back when it fails.
    fn in_txn(&self, conn: &Arc<Mutex<Conn>>, run: impl FnOnce() -> Got) -> Got {
        self.rt
            .block_on(async { conn.lock().await.query_drop("START TRANSACTION").await })
            .unwrap();
        let got = run();
        let end = if got.is_ok() { "COMMIT" } else { "ROLLBACK" };
        self.rt
            .block_on(async { conn.lock().await.query_drop(end).await })
            .unwrap();
        got
    }

    /// The compiled side, as the gateway runs it: the entry procedure,
    /// and QuickJS for a record the compiled code hands back (39876).
    fn compiled(
        &mut self,
        c: &Case,
        entry: &str,
        params: &[(String, Ty)],
        event: &J,
        conn: &Arc<Mutex<Conn>>,
    ) -> (Got, bool) {
        let args: Vec<My> = params
            .iter()
            .map(|(n, t)| {
                let v = event
                    .as_object()
                    .unwrap()
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(n))
                    .map_or(J::Null, |(_, v)| v.clone());
                match (t, v) {
                    (_, J::Null) => My::NULL,
                    (_, J::Bool(b)) => My::Int(i64::from(b)),
                    (_, J::String(s)) => My::Bytes(s.into_bytes()),
                    (_, J::Number(n)) => n
                        .as_i64()
                        .map_or_else(|| My::Double(n.as_f64().unwrap()), My::Int),
                    (_, other) => My::Bytes(other.to_string().into_bytes()),
                }
            })
            .collect();
        let marks = vec!["?"; args.len()];
        let mut call = format!("CALL {entry}({}", marks.join(", "));
        call.push_str(if args.is_empty() { "" } else { ", " });
        call.push_str("@e, @r, @l)");
        let logs = !call.is_empty() && self.has_log(entry);
        let call = if logs {
            call
        } else {
            call.replace(", @l)", ")")
        };
        let r: std::result::Result<(Option<String>, Option<String>, Option<String>), _> =
            self.rt.block_on(async {
                let mut k = conn.lock().await;
                k.query_drop("SAVEPOINT __sprig_compiled").await?;
                k.exec_drop(call.as_str(), args).await?;
                let row: Option<(Option<String>, Option<String>, Option<String>)> =
                    k.query_first("SELECT @e, @r, @l").await?;
                Ok::<_, mysql_async::Error>(row.unwrap_or_default())
            });
        match r {
            Ok((e, _, l)) => {
                if matches!(e.as_deref(), Some("error" | "reject")) {
                    return (Err(39865), false);
                }
                let lines: Vec<String> = if logs {
                    serde_json::from_str(l.as_deref().unwrap_or("[]")).unwrap()
                } else {
                    Vec::new()
                };
                (Ok(lines), false)
            }
            Err(mysql_async::Error::Server(s)) if s.code == 39876 => {
                println!("  compiled hands it to QuickJS: {}", s.message);
                self.rt
                    .block_on(async {
                        conn.lock()
                            .await
                            .query_drop("ROLLBACK TO SAVEPOINT __sprig_compiled")
                            .await
                    })
                    .unwrap();
                self.reruns += 1;
                (self.quickjs(c, event, conn), true)
            }
            Err(e) => {
                println!("  compiled error: {e}");
                (Err(class(&my_err(e))), false)
            }
        }
    }

    fn has_log(&self, entry: &str) -> bool {
        let mut c = self.conn(None);
        let n: Option<u64> = self
            .rt
            .block_on(c.exec_first(
                "SELECT COUNT(*) FROM information_schema.PARAMETERS WHERE SPECIFIC_SCHEMA = ? \
                 AND SPECIFIC_NAME = ? AND PARAMETER_NAME = 'p__log'",
                (JSC, entry),
            ))
            .unwrap();
        n == Some(1)
    }

    fn tables(&self, schema: &str) -> Vec<Vec<Option<String>>> {
        let mut conn = self.conn(Some(schema));
        let mut out = Vec::new();
        for q in [
            "SELECT id, k, what, n FROM hits ORDER BY id",
            "SELECT sku, qty FROM stock ORDER BY sku",
            "SELECT id, qty, note, urgent FROM jobs ORDER BY id",
        ] {
            let rows: Vec<mysql_async::Row> = self.rt.block_on(conn.query(q)).unwrap();
            out.extend(
                rows.into_iter()
                    .map(|r| r.unwrap().iter().map(text).collect::<Vec<_>>()),
            );
        }
        out
    }

    fn case(&mut self, c: &Case, module: u64) {
        self.reset();
        let (entry, params) = self.deploy(c, module);
        let js = Arc::new(Mutex::new(self.conn(Some(JS))));
        let jsc = Arc::new(Mutex::new(self.conn(Some(JSC))));
        for (i, ev) in c.events.iter().enumerate() {
            self.cases += 1;
            let want = self.in_txn(&js, || self.quickjs(c, ev, &js));
            self.rt
                .block_on(async { jsc.lock().await.query_drop("START TRANSACTION").await })
                .unwrap();
            let (got, reran) = self.compiled(c, &entry, &params, ev, &jsc);
            let end = if got.is_ok() { "COMMIT" } else { "ROLLBACK" };
            self.rt
                .block_on(async { jsc.lock().await.query_drop(end).await })
                .unwrap();
            let label = format!("{} #{i}", c.name);
            if want != got {
                self.failures
                    .push(format!("{label}: quickjs {want:?} vs compiled {got:?}"));
            }
            if reran != c.reruns.contains(&i) {
                self.failures.push(format!(
                    "{label}: the compiled side {} QuickJS",
                    if reran {
                        "handed it to"
                    } else {
                        "kept it from"
                    }
                ));
            }
            println!("{label}: {want:?}{}", if reran { " (QuickJS)" } else { "" });
        }
        drop((js, jsc));
        let (a, b) = (self.tables(JS), self.tables(JSC));
        if a != b {
            self.failures
                .push(format!("{}: tables differ: {a:?} vs {b:?}", c.name));
        }
    }
}

fn record(i: u64, key: Option<&str>, value: Option<&str>, headers: J) -> J {
    json!({"stream": "shop.events", "partition": i % 2, "offset": i, "key": key,
           "value": value, "headers": headers, "timestamp": 1_700_000_000_000_u64 + i})
}

fn message(body: &str, redelivered: bool, count: u32, props: J) -> J {
    json!({"queue": "jobs", "vhost": "shop", "exchange": "", "routingKey": "jobs",
           "body": body, "properties": props, "redelivered": redelivered,
           "deliveryCount": count})
}

/// The triggers and the events each runs for.
fn corpus() -> Vec<Case> {
    let h = |s: &str| json!({ "x-src": s });
    vec![
        Case {
            name: "orders",
            on: MsgSource::Stream,
            js: "export default function (ctx) {
                   const r = ctx.record;
                   const e = JSON.parse(r.value);
                   if (e === null) {
                     db.exec('INSERT INTO hits (k, what) VALUES (?, ?)', [r.key, 'tombstone']);
                     return;
                   }
                   const kind = e.type ?? 'none';
                   let total = 0;
                   let n = 0;
                   if (Array.isArray(e.items)) {
                     for (const it of e.items) { total += it.qty * it.price; n++; }
                   }
                   const who = e.user?.name ?? 'anon';
                   db.exec('INSERT INTO hits (k, what, n) VALUES (?, ?, ?)',
                           [r.key, kind + '/' + who + '/' + n, total]);
                   if (e.sku !== undefined) {
                     db.exec('UPDATE stock SET qty = qty - ? WHERE sku = ?', [e.take, e.sku]);
                   }
                   console.log('order', r.key, kind, total, r.partition, r.offset);
                   console.log(ctx.record.headers['x-src'] ?? 'nosrc', typeof e.items === 'object');
                 }",
            events: vec![
                record(1, Some("k1"), Some(r#"{"type":"add","user":{"name":"ann"},"items":[{"qty":2,"price":2.5},{"qty":1,"price":10}]}"#), h("app")),
                record(2, Some("k2"), Some(r#"{"type":"add","items":[],"sku":"a","take":3}"#), json!({})),
                record(3, Some("k3"), None, h("app")),
                record(4, None, Some("{}"), h("x")),
                record(5, Some("k5"), Some(r#"{"type":"add","sku":"b","take":1}"#), json!({})),
                record(6, Some("k6"), Some("not json"), json!({})),
                record(7, Some("k7"), Some(r#"{"type":"x","items":[{"qty":"2","price":1}]}"#), json!({})),
                record(8, Some("k8"), Some(r#"{"type":"dup","type":"later"}"#), json!({})),
                record(9, Some("k9"), Some(r#"{"type":"num","items":[{"qty":1.}]}"#), json!({})),
                record(10, Some("k10"), Some(r#"{"type":"été 😀","user":null}"#), h("é")),
                record(11, Some("k11"), Some(r#"{"type":5}"#), json!({})),
                record(12, Some("k12"), Some(r#"{"type":"big","items":[{"qty":1e400,"price":1}]}"#), json!({})),
            ],
            // QuickJS runs: 5 not JSON; 6 `"2" * 1` converts text; 7 a
            // repeated member (the last one wins in JavaScript); 8 `1.`,
            // which MariaDB reads and JSON.parse refuses; 10 a number where
            // text is joined; 11 Infinity. 9 (`?.` of null, non-ASCII text)
            // stays compiled.
            reruns: vec![5, 6, 7, 8, 10, 11],
        },
        Case {
            name: "kinds",
            on: MsgSource::Stream,
            js: "const v = JSON.parse(ctx.record.value);
                 let t = '';
                 if (v.s) t = t + 's';
                 if (v.z) t = t + 'z';
                 if (v.e) t = t + 'e';
                 if (v.o) t = t + 'o';
                 if (v.a) t = t + 'a';
                 if (v.f) t = t + 'f';
                 if (v.nul) t = t + 'n';
                 if (v.missing) t = t + 'm';
                 const kinds = (typeof v.s === 'string') + ',' + (typeof v.z === 'number') + ',' +
                   (typeof v.f === 'boolean') + ',' + (typeof v.o === 'object') + ',' +
                   (typeof v.a === 'object') + ',' + (typeof v.nul === 'object') + ',' +
                   (typeof v.missing === 'undefined') + ',' + (typeof v.s === 'number');
                 const eq = (v.s === 'x') + ' ' + (v.z === 0) + ' ' + (v.f === false) + ' ' +
                   (v.nul === null) + ' ' + (v.missing === undefined) + ' ' + (v.nul == null) + ' ' +
                   (v.missing == null) + ' ' + (v.s !== 'y') + ' ' + (v.z === '0');
                 console.log(t, kinds, eq, String(v.s), `${v.z}|${v.f}|${v.nul}|${v.missing}`,
                   v.s.length, v.a.length, v.a[1], v.a[5] === undefined, Array.isArray(v.o));
                 db.exec('INSERT INTO hits (k, what) VALUES (?, ?)', [v.s, t + ':' + kinds]);",
            events: vec![
                record(1, Some("a"), Some(r#"{"s":"x","z":0,"e":"","o":{},"a":[1,2,3],"f":false,"nul":null}"#), json!({})),
                record(2, Some("b"), Some(r#"{"s":"xyz","z":2.5,"e":"0","o":{"k":1},"a":[],"f":true,"nul":null}"#), json!({})),
                record(3, Some("c"), Some(r#"{"s":"x","z":-0,"e":" ","o":[],"a":["p","q"],"f":false,"nul":1}"#), json!({})),
            ],
            // Every kind's truth, typeof, equality and text compile.
            reruns: vec![],
        },
        Case {
            name: "failures",
            on: MsgSource::Stream,
            js: "export default function (ctx) {
                   const m = JSON.parse(ctx.record.value);
                   db.exec('INSERT INTO hits (k, what) VALUES (?, ?)', [ctx.record.key, m.what]);
                   if (m.what === 'reject') sprig.reject('no ' + m.n);
                   if (m.what === 'throw') throw new Error('bad ' + m.n);
                   if (m.what === 'event') return sprig.event('error', { message: 'as event' });
                   let caught = 'none';
                   try {
                     if (m.n > 5) throw new RangeError('big ' + m.n);
                   } catch (e) {
                     caught = e.name + ': ' + e.message;
                   }
                   db.exec('INSERT INTO hits (k, what) VALUES (?, ?)', [ctx.record.key, caught]);
                 }",
            events: vec![
                record(1, Some("a"), Some(r#"{"what":"ok","n":1}"#), json!({})),
                record(2, Some("b"), Some(r#"{"what":"reject","n":2}"#), json!({})),
                record(3, Some("c"), Some(r#"{"what":"throw","n":3}"#), json!({})),
                record(4, Some("d"), Some(r#"{"what":"event","n":4}"#), json!({})),
                record(5, Some("e"), Some(r#"{"what":"ok","n":9}"#), json!({})),
                record(6, Some("f"), Some(r#"{"what":7,"n":1}"#), json!({})),
            ],
            // 5: a number bound to a text column.
            reruns: vec![5],
        },
        Case {
            // Operands are read left to right, also around a helper that
            // changes a variable, in log lines, binds and computed members.
            name: "order",
            on: MsgSource::Stream,
            js: "const e = JSON.parse(ctx.record.value);
                 let n = Number(e.n);
                 let arr = e.a;
                 function bump() { n = n + 1; return n; }
                 function other() { arr = e.b; return 0; }
                 console.log('n', n, bump(), n);
                 const pick = arr[other()];
                 console.log('pick', pick);
                 db.exec('INSERT INTO hits (k, what, n) VALUES (?, ?, ?)',
                         [ctx.record.key, `${n}/${bump()}/${n}`, n + bump()]);",
            events: vec![
                record(1, Some("o1"), Some(r#"{"n":1,"a":[10],"b":[20]}"#), json!({})),
                record(2, Some("o2"), Some(r#"{"n":5,"a":["x"],"b":["y"]}"#), json!({})),
            ],
            reruns: vec![],
        },
        Case {
            name: "jobs",
            on: MsgSource::Queue,
            js: "export default function (ctx) {
                   const m = ctx.message;
                   const job = JSON.parse(m.body);
                   const urgent = m.properties.headers?.urgent ?? false;
                   db.exec('INSERT INTO jobs (id, qty, note, urgent) VALUES (?, ?, ?, ?)',
                           [job.id, job.qty, job.note, urgent]);
                   if (m.redelivered) console.log('again', m.deliveryCount, m.queue, m.routingKey);
                   console.log(`job ${job.id} x${job.qty}`, job.note ?? '-');
                 }",
            events: vec![
                message(r#"{"id":1,"qty":2,"note":"first"}"#, false, 0, json!({})),
                message(r#"{"id":2,"qty":5}"#, true, 2, json!({"headers": {"urgent": true}})),
                message(r#"{"id":3,"qty":1,"note":null}"#, false, 0, json!({"headers": {}})),
                message(r#"{"id":4,"qty":2.5}"#, false, 0, json!({})),
                message(r#"{"id":"5","qty":1}"#, false, 0, json!({})),
                message(r#"{"id":1,"qty":1}"#, false, 0, json!({})),
            ],
            // 3: 2.5 into an INT column; 4: "5" into a BIGINT one. 5: a
            // repeated id fails in both (1062).
            reruns: vec![3, 4],
        },
    ]
}

#[test]
fn compiled_triggers_agree_with_quickjs() {
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
            .with_constraints(PoolConstraints::new(0, 6).unwrap())
            .with_inactive_connection_ttl(std::time::Duration::from_secs(30)),
    );
    let mut s = Suite {
        rt,
        pool: Pool::new(opts),
        cases: 0,
        reruns: 0,
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
        for (n, c) in corpus().iter().enumerate() {
            s.case(c, 9800 + n as u64);
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
        "{} runs in {elapsed:?}, {} handed to QuickJS, {} mismatches",
        s.cases,
        s.reruns,
        s.failures.len()
    );
    assert!(
        s.failures.is_empty(),
        "mismatches:\n{}",
        s.failures.join("\n")
    );
}
