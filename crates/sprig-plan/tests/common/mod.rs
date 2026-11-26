//! Shared by the planner's tests: table specifications that become both
//! `CREATE TABLE` text for the reference servers and in-memory tables,
//! running a statement through the planner, and reaching the servers
//! through shell commands.

#![allow(
    clippy::explicit_auto_deref,
    clippy::manual_is_multiple_of,
    clippy::explicit_iter_loop,
    clippy::collapsible_if,
    clippy::default_trait_access,
    clippy::type_complexity,
    clippy::items_after_statements,
    clippy::redundant_closure_for_method_calls,
    clippy::match_same_arms,
    clippy::if_not_else,
    clippy::single_match_else,
    clippy::unnested_or_patterns,
    clippy::uninlined_format_args,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::cast_lossless,
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::must_use_candidate,
    clippy::format_push_string,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::many_single_char_names
)]

pub mod world;

use std::collections::HashMap;
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::sync::Arc;

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::ops::Row;
use sprig_exec::{Ctx, Persona, Session, value_text};
use sprig_plan::catalog::{ColumnDef, IndexDef, IndexPart, TableDef};
use sprig_plan::mem::MemDb;
use sprig_plan::{ExplainRow, PlanOptions, Planner};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// xorshift64*: small, deterministic, good enough to vary test cases.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn chance(&mut self, num: usize, den: usize) -> bool {
        self.below(den) < num
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// One column of a test table.
#[derive(Clone)]
pub struct Col {
    pub name: &'static str,
    pub sql: String,
    pub ty: ColumnType,
    pub nullable: bool,
}

/// One key part: column name, prefix length, descending.
pub type Part = (&'static str, Option<u32>, bool);

/// A test table: its definition and rows.
#[derive(Clone)]
pub struct Spec {
    pub name: &'static str,
    pub cols: Vec<Col>,
    pub pk: Vec<Part>,
    /// Name, unique, parts.
    pub keys: Vec<(&'static str, bool, Vec<Part>)>,
    pub rows: Vec<Row>,
}

pub fn col(name: &'static str, sql: &str, ty: ColumnType) -> Col {
    Col {
        name,
        sql: sql.to_owned(),
        ty,
        nullable: true,
    }
}

pub fn col_nn(name: &'static str, sql: &str, ty: ColumnType) -> Col {
    Col {
        name,
        sql: format!("{sql} NOT NULL"),
        ty,
        nullable: false,
    }
}

/// A string column with a collation, as both servers spell it.
pub fn text_col(name: &'static str, chars: u32, collation: Collation) -> Col {
    col(
        name,
        &format!(
            "VARCHAR({chars}) CHARACTER SET utf8mb4 COLLATE {}",
            collation.name()
        ),
        ColumnType::VarChar { chars, collation },
    )
}

/// The persona's default collation.
pub fn default_collation(persona: Persona) -> Collation {
    match persona {
        Persona::MariaDb => Collation::Utf8mb4Uca1400AiCi,
        Persona::MySql => Collation::Utf8mb4Mysql0900AiCi,
    }
}

impl Spec {
    fn part(&self, p: &Part) -> IndexPart {
        IndexPart {
            column: self.cols.iter().position(|c| c.name == p.0).unwrap(),
            prefix: p.1,
            desc: p.2,
        }
    }

    pub fn def(&self, schema: &str) -> TableDef {
        TableDef {
            schema: schema.to_owned(),
            name: self.name.to_owned(),
            columns: self
                .cols
                .iter()
                .map(|c| ColumnDef {
                    name: c.name.to_owned(),
                    ty: c.ty,
                    nullable: c.nullable,
                    invisible: false,
                    mb3: false,
                })
                .collect(),
            primary: (!self.pk.is_empty()).then(|| {
                IndexDef::new(
                    "PRIMARY",
                    true,
                    self.pk.iter().map(|p| self.part(p)).collect(),
                )
            }),
            indexes: self
                .keys
                .iter()
                .map(|(name, unique, parts)| {
                    IndexDef::new(name, *unique, parts.iter().map(|p| self.part(p)).collect())
                })
                .collect(),
        }
    }

    pub fn ddl(&self, db: &str) -> String {
        let part = |p: &Part| {
            let mut s = format!("`{}`", p.0);
            if let Some(n) = p.1 {
                s.push_str(&format!("({n})"));
            }
            if p.2 {
                s.push_str(" DESC");
            }
            s
        };
        let mut items: Vec<String> = self
            .cols
            .iter()
            .map(|c| format!("`{}` {}", c.name, c.sql))
            .collect();
        if !self.pk.is_empty() {
            items.push(format!(
                "PRIMARY KEY ({})",
                self.pk.iter().map(part).collect::<Vec<_>>().join(", ")
            ));
        }
        for (name, unique, parts) in &self.keys {
            items.push(format!(
                "{} `{name}` ({})",
                if *unique { "UNIQUE KEY" } else { "KEY" },
                parts.iter().map(part).collect::<Vec<_>>().join(", ")
            ));
        }
        format!(
            "CREATE TABLE {db}.`{}` ({}) ENGINE=InnoDB",
            self.name,
            items.join(", ")
        )
    }

    pub fn inserts(&self, db: &str) -> Vec<String> {
        self.rows
            .chunks(50)
            .map(|chunk| {
                let rows: Vec<String> = chunk
                    .iter()
                    .map(|r| {
                        format!(
                            "({})",
                            r.iter().map(sql_literal).collect::<Vec<_>>().join(", ")
                        )
                    })
                    .collect();
                format!(
                    "INSERT INTO {db}.`{}` VALUES {}",
                    self.name,
                    rows.join(", ")
                )
            })
            .collect()
    }
}

/// A value as a SQL literal for loading a table.
pub fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Double(x) => format!("{x:e}"),
        Value::Str(s) | Value::Bytes(s) => {
            if s.is_empty() {
                return "''".to_owned();
            }
            let mut out = String::from("0x");
            for b in s.iter() {
                out.push_str(&format!("{b:02X}"));
            }
            out
        }
        Value::Date(d) => format!("'{d}'"),
        Value::Time(t) => format!("'{t}'"),
        Value::DateTime(d) => format!("'{d}'"),
        other => panic!("no literal for {other:?}"),
    }
}

/// The tables in memory.
pub fn load_mem(schema: &str, specs: &[Spec]) -> Arc<MemDb> {
    let mut db = MemDb::new(schema);
    for s in specs {
        db.create_table(s.def(schema));
        for row in &s.rows {
            db.insert(s.name, row.clone())
                .unwrap_or_else(|e| panic!("loading {}: {e}", s.name));
        }
    }
    Arc::new(db)
}

/// What a statement answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The rows, each as the `mysql` client prints it in batch mode.
    Rows(Vec<Vec<u8>>),
    /// The error number and message.
    Error(u16, String),
}

/// The client's batch-mode escaping of a value.
pub fn client_escape(v: Option<&[u8]>, out: &mut Vec<u8>) {
    let Some(v) = v else {
        out.extend_from_slice(b"NULL");
        return;
    };
    for &b in v {
        match b {
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0 => out.extend_from_slice(b"\\0"),
            b'\\' => out.extend_from_slice(b"\\\\"),
            other => out.push(other),
        }
    }
}

/// What running a statement through the planner gave.
pub struct Ours {
    pub outcome: Outcome,
    pub explain: Vec<ExplainRow>,
    pub rows_examined: u64,
    /// Rows examined per table access, in plan order.
    pub per_table: Vec<(String, u64)>,
}

/// Plans and runs one statement over the in-memory tables.
pub fn run_ours(
    db: &Arc<MemDb>,
    session: &Session,
    sql: &str,
    options: Option<PlanOptions>,
) -> Ours {
    run_ours_params(db, session, sql, options, &[])
}

pub fn run_ours_params(
    db: &Arc<MemDb>,
    session: &Session,
    sql: &str,
    options: Option<PlanOptions>,
    params: &[Value],
) -> Ours {
    let fail = |code: u16, msg: String| Ours {
        outcome: Outcome::Error(code, msg),
        explain: Vec::new(),
        rows_examined: 0,
        per_table: Vec::new(),
    };
    let stmt = match Parser::parse_sql(&MySqlDialect {}, sql) {
        Ok(mut s) if s.len() == 1 => s.remove(0),
        Ok(_) => return fail(1064, "not one statement".into()),
        Err(e) => return fail(1064, format!("parser: {e}")),
    };
    let mut planner = Planner::new(&**db, db.clone(), session);
    if let Some(o) = options {
        planner.options = o;
    }
    let mut plan = match planner.plan(&stmt, params) {
        Ok(p) => p,
        Err(e) => return fail(e.code, e.message),
    };
    let types: Vec<_> = plan.columns.iter().map(|c| c.ty).collect();
    let mut ctx = Ctx::new(session);
    let mut rows = Vec::new();
    loop {
        match plan.source.next(&mut ctx) {
            Ok(Some(row)) => {
                let mut line = Vec::new();
                for (i, (v, ty)) in row.iter().zip(&types).enumerate() {
                    if i > 0 {
                        line.push(b'\t');
                    }
                    let text = value_text(v, ty, session.persona);
                    client_escape(text.as_deref(), &mut line);
                }
                rows.push(line);
            }
            Ok(None) => break,
            Err(e) => return fail(e.code, e.message),
        }
    }
    Ours {
        outcome: Outcome::Rows(rows),
        explain: plan.explain.clone(),
        rows_examined: plan.rows_examined(),
        per_table: plan
            .accesses
            .iter()
            .map(|a| (a.table.clone(), a.counters.rows()))
            .collect(),
    }
}

/// The session both sides run under.
pub fn session(persona: Persona) -> Session {
    let mut s = Session::new(persona);
    s.now_seconds = 1_700_000_000;
    s.time_zone_offset = 19_800;
    s
}

/// A reference server reached through a shell command that reads SQL on
/// stdin and prints rows tab-separated without a header.
pub struct Oracle {
    pub persona: Persona,
    pub label: &'static str,
    command: String,
}

impl Oracle {
    /// The servers named by `SPRIG_PLAN_ORACLE_MARIADB` and
    /// `SPRIG_PLAN_ORACLE_MYSQL`.
    pub fn from_env() -> Vec<Oracle> {
        let mut out = Vec::new();
        for (var, persona, label) in [
            ("SPRIG_PLAN_ORACLE_MARIADB", Persona::MariaDb, "mariadb"),
            ("SPRIG_PLAN_ORACLE_MYSQL", Persona::MySql, "mysql"),
        ] {
            if let Ok(command) = std::env::var(var) {
                if !command.trim().is_empty() {
                    out.push(Oracle {
                        persona,
                        label,
                        command,
                    });
                }
            }
        }
        out
    }

    /// Runs a script; returns what the client wrote to stdout and stderr.
    pub fn run(&self, sql: &[u8], extra: &str) -> (Vec<u8>, String) {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("{} --force {extra}", self.command))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the oracle command starts");
        let mut stdin = child.stdin.take().unwrap();
        let sql = sql.to_vec();
        let writer = std::thread::spawn(move || {
            let _ = stdin.write_all(&sql);
        });
        let out = child.wait_with_output().expect("the oracle command ends");
        let _ = writer.join();
        (
            out.stdout,
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// The statements that put the server's session into the state of
    /// [`session`].
    pub fn header(&self, db: &str) -> String {
        let coll = match self.persona {
            Persona::MariaDb => "utf8mb4_uca1400_ai_ci",
            Persona::MySql => "utf8mb4_0900_ai_ci",
        };
        format!(
            "SET NAMES utf8mb4 COLLATE {coll}; SET time_zone='+05:30'; SET timestamp=1700000000; USE {db};\n"
        )
    }

    /// Creates database `db` with the tables.
    pub fn load(&self, db: &str, specs: &[Spec]) {
        let mut sql =
            format!("SET NAMES utf8mb4; DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db};\n");
        for s in specs {
            sql.push_str(&s.ddl(db));
            sql.push_str(";\n");
            for i in s.inserts(db) {
                sql.push_str(&i);
                sql.push_str(";\n");
            }
        }
        let (_, err) = self.run(sql.as_bytes(), "");
        assert!(err.trim().is_empty(), "loading on {}: {err}", self.label);
        // Fresh statistics, so that the server's plans are its best.
        let mut sql = String::new();
        for s in specs {
            sql.push_str(&format!("ANALYZE TABLE {db}.`{}`;\n", s.name));
        }
        let _ = self.run(sql.as_bytes(), "");
    }

    /// Drops database `db`.
    pub fn drop_db(&self, db: &str) {
        let _ = self.run(format!("DROP DATABASE IF EXISTS {db};\n").as_bytes(), "");
    }

    /// Runs statements (each on one line) and gives each one's rows or
    /// error.
    pub fn answers(&self, db: &str, statements: &[String]) -> Vec<Outcome> {
        let mut sql = self.header(db).into_bytes();
        for (i, s) in statements.iter().enumerate() {
            assert!(!s.contains('\n'), "one line per statement: {s}");
            sql.extend_from_slice(format!("SELECT '\u{1}#{i}';\n{s};\n").as_bytes());
        }
        let (out, err) = self.run(&sql, "");
        let errors = error_lines(&err);
        let mut answers: Vec<Outcome> = (0..statements.len())
            .map(|i| match errors.get(&(3 + 2 * i)) {
                Some((code, msg)) => Outcome::Error(*code, msg.clone()),
                None => Outcome::Rows(Vec::new()),
            })
            .collect();
        let mut cur: Option<usize> = None;
        let mut lines: Vec<&[u8]> = out.split(|&b| b == b'\n').collect();
        if lines.last().is_some_and(|l| l.is_empty()) {
            lines.pop();
        }
        for line in lines {
            if line.starts_with(b"\x01#") {
                cur = std::str::from_utf8(&line[2..])
                    .ok()
                    .and_then(|t| t.parse().ok());
                continue;
            }
            let Some(i) = cur else { continue };
            if let Outcome::Rows(r) = &mut answers[i] {
                r.push(line.to_vec());
            }
        }
        answers
    }
}

fn error_lines(stderr: &str) -> HashMap<usize, (u16, String)> {
    let mut out = HashMap::new();
    for line in stderr.lines() {
        // ERROR 1054 (42S22) at line 5: Unknown column ...
        let Some(rest) = line.strip_prefix("ERROR ") else {
            continue;
        };
        let code: u16 = rest
            .split(' ')
            .next()
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        if let Some(pos) = rest.find(" at line ") {
            let tail = &rest[pos + 9..];
            let n: usize = tail
                .split(':')
                .next()
                .and_then(|c| c.trim().parse().ok())
                .unwrap_or(0);
            let msg = tail.split_once(": ").map_or("", |x| x.1).to_owned();
            out.insert(n, (code, msg));
        }
    }
    out
}

/// Whether two answers agree: the same error number, or the same rows (in
/// order when `ordered`, as multisets otherwise).
pub fn same(a: &Outcome, b: &Outcome, ordered: bool) -> bool {
    match (a, b) {
        (Outcome::Error(x, _), Outcome::Error(y, _)) => x == y,
        (Outcome::Rows(x), Outcome::Rows(y)) => {
            if ordered {
                x == y
            } else {
                let (mut x, mut y) = (x.clone(), y.clone());
                x.sort();
                y.sort();
                x == y
            }
        }
        _ => false,
    }
}

pub fn show(o: &Outcome) -> String {
    match o {
        Outcome::Error(code, msg) => format!("ERROR {code}: {msg}"),
        Outcome::Rows(rows) => {
            let mut s = format!("{} rows", rows.len());
            for r in rows.iter().take(12) {
                s.push_str("\n    ");
                s.push_str(&String::from_utf8_lossy(r));
            }
            if rows.len() > 12 {
                s.push_str("\n    ...");
            }
            s
        }
    }
}
