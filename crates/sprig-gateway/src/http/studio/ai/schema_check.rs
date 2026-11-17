//! The DBA assistant's schema checks, deterministic and run in the
//! gateway: `check_ddl` (statements the assistant is about to propose)
//! and `migration_check` (a schema coming from MySQL, MariaDB or MongoDB).
//! The model explains a report; it never decides what is in it.
//!
//! **SQL.** The text is split as a client's would be (`DELIMITER` lines
//! included, as in dumps), and each statement goes through SprigDB's own
//! rules:
//!
//! | Rule | Level | From |
//! |---|---|---|
//! | the name layer refuses it (reserved names, positions it can't map) | error | the statement rewrite the gateway runs on every statement |
//! | a name longer than 64 characters (1059) | error | the MySQL persona's DDL rules |
//! | a storage engine that is never created (`BLACKHOLE`, `FEDERATED`, ...) | error | the engine rules |
//! | `MyISAM`, `MEMORY`, `Aria`, `CSV`, `ARCHIVE` | change: created as `InnoDB` | the engine rules (`compat.engine_substitution`) |
//! | a `FULLTEXT` key | change: becomes a search index | `search.fulltext_ddl` |
//! | `DEFINER` naming another account | change: needs `SET_ANY_DEFINER` | definers |
//! | `NOT NULL` on a generated column, `DEFAULT (NOW())` | note: kept as MySQL means it | the MySQL persona's DDL rules |
//! | replication and server statements (`CHANGE MASTER`, `SET @@GLOBAL.GTID_PURGED`, `SQL_LOG_BIN`) | error: 1227 | the protocol rules |
//! | a table without a primary key, `AUTO_INCREMENT` | note | clusters and sharding |
//! | `CREATE TABLE`, `ALTER TABLE` and `CREATE INDEX` that the statement parser can't read | error for `check_ddl`, note for a migration | the router's parser |
//!
//! A migration report adds **table groups**: tables are grouped along
//! their foreign keys under a root with a one-column primary key that can
//! be a distribution key; children that hold the root's key are co-located
//! with it, and small code tables referenced from several groups are
//! suggested as `REFERENCE`. The groups come as `SPRIG PIN` statements,
//! which are optional: the sharding advisor designs groups by itself.
//!
//! **MongoDB.** Each collection's options go through the same checks as
//! `create` (`$jsonSchema` validators included), and each index through
//! the index rules of the MongoDB front end.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use bytes::Bytes;
use serde::Serialize;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_proto_mysql::packets::Persona;
use sprig_sql::compat::{ddl, engine, mysqlddl};
use sprig_sql::lex::{self, LexOptions, Token, TokenKind};
use sprig_sql::names::{MemoryNameMap, RewriteCtx, rewrite};
use sqlparser::ast::{ColumnOption, CreateTable, ObjectName, Statement, TableConstraint};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// How serious a finding is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// It won't run on SprigDB as written.
    Error,
    /// It runs, but SprigDB does something different from the source.
    Change,
    /// Worth knowing; nothing to fix.
    Note,
}

/// One finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// How serious.
    pub level: Level,
    /// The statement (1-based) or collection it is about.
    pub at: String,
    /// The object, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    /// The rule's short name.
    pub rule: &'static str,
    /// What and why, and what to do.
    pub message: String,
    /// The error SprigDB answers with, when it refuses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u32>,
}

/// What `check_ddl` and `migration_check` learn of one table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    /// Its name, unqualified.
    pub name: String,
    /// Columns: name, type as written, and the type's base word.
    pub columns: Vec<(String, String, String)>,
    /// The primary key's columns.
    pub pk: Vec<String>,
    /// Foreign keys: columns, the referenced table, its columns.
    pub fks: Vec<(Vec<String>, String, Vec<String>)>,
}

/// The checks of a SQL text.
#[derive(Debug, Clone, Default)]
pub struct SqlReport {
    /// Statements read.
    pub statements: usize,
    /// Statements by kind (`create table`, `insert`, ...).
    pub kinds: BTreeMap<String, usize>,
    /// What was found, in statement order.
    pub findings: Vec<Finding>,
    /// The tables created, in order.
    pub tables: Vec<Table>,
}

impl SqlReport {
    /// Whether any finding is an error.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.findings.iter().any(|f| f.level == Level::Error)
    }
}

/// The longest text one check reads, in bytes.
pub const MAX_TEXT: usize = 512 * 1024;
/// Findings one report lists at most; the rest are counted.
const MAX_FINDINGS: usize = 200;

/// Splits `text` into statements: `DELIMITER` lines as the `mysql` client
/// reads them, everything else with the gateway's own splitter.
#[must_use]
pub fn statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut delim = ";".to_owned();
    let mut chunk = String::new();
    let flush = |chunk: &mut String, delim: &str, out: &mut Vec<String>| {
        if delim == ";" {
            for s in crate::exec::split::split(&Bytes::copy_from_slice(chunk.as_bytes())) {
                let s = String::from_utf8_lossy(&s).trim().to_owned();
                if !s.is_empty() && s != ";" {
                    out.push(s.trim_end_matches(';').trim().to_owned());
                }
            }
        } else {
            for s in chunk.split(delim) {
                let s = s.trim();
                if !s.is_empty() {
                    out.push(s.to_owned());
                }
            }
        }
        chunk.clear();
    };
    for line in text.lines() {
        let t = line.trim();
        if t.len() > 10 && t[..10].eq_ignore_ascii_case("delimiter ") {
            flush(&mut chunk, &delim, &mut out);
            t[10..].trim().clone_into(&mut delim);
            if delim.is_empty() {
                ";".clone_into(&mut delim);
            }
            continue;
        }
        chunk.push_str(line);
        chunk.push('\n');
    }
    flush(&mut chunk, &delim, &mut out);
    out.retain(|s| !only_comments(s));
    out
}

fn only_comments(s: &str) -> bool {
    let opts = LexOptions::mysql84();
    lex::lex(s.as_bytes(), &opts).is_ok_and(|t| {
        t.iter().all(|t| {
            t.kind.is_trivia() || matches!(t.kind, TokenKind::InactiveExec | TokenKind::Hint)
        })
    })
}

/// A statement's significant tokens: words and punctuation, executable
/// comment markers left out so `/*!40101 SET ... */` reads as `SET ...`.
struct Sig<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
}

impl<'a> Sig<'a> {
    fn new(sql: &'a str, opts: LexOptions) -> Self {
        let t = lex::lex(sql.as_bytes(), &opts)
            .unwrap_or_default()
            .into_iter()
            .filter(|t| {
                !t.kind.is_trivia()
                    && !matches!(
                        t.kind,
                        TokenKind::ExecOpen
                            | TokenKind::ExecClose
                            | TokenKind::InactiveExec
                            | TokenKind::Hint
                    )
            })
            .collect();
        Self {
            sql: sql.as_bytes(),
            t,
        }
    }

    fn word(&self, i: usize) -> Option<String> {
        let t = self.t.get(i)?;
        (t.kind == TokenKind::Word)
            .then(|| String::from_utf8_lossy(t.bytes(self.sql)).to_ascii_uppercase())
    }

    fn text(&self, i: usize) -> Option<String> {
        let t = self.t.get(i)?;
        let b = t.bytes(self.sql);
        Some(match t.kind {
            TokenKind::QuotedIdent => String::from_utf8_lossy(&lex::ident_value(b)).into_owned(),
            TokenKind::String => {
                String::from_utf8_lossy(b.get(1..b.len().saturating_sub(1)).unwrap_or(b))
                    .into_owned()
            }
            _ => String::from_utf8_lossy(b).into_owned(),
        })
    }

    fn is(&self, i: usize, w: &str) -> bool {
        self.word(i).is_some_and(|x| x == w)
    }

    /// The token at `i` is exactly the symbol `c` (`=`, `@`, `.`),
    /// whatever kind the lexer gave it.
    fn punct(&self, i: usize, c: u8) -> bool {
        self.t.get(i).is_some_and(|t| t.bytes(self.sql) == [c])
    }

    /// The first words, skipping `OR REPLACE`, `DEFINER = ...`,
    /// `ALGORITHM = ...`, `SQL SECURITY ...`, `TEMPORARY` and the like, so
    /// `CREATE DEFINER=x VIEW` reads as `CREATE VIEW`.
    fn kind(&self) -> String {
        let first = self.word(0).unwrap_or_default();
        if first != "CREATE" && first != "ALTER" && first != "DROP" {
            return first.to_ascii_lowercase();
        }
        let mut i = 1;
        while i < self.t.len() {
            match self.word(i).as_deref() {
                Some(
                    "OR" | "REPLACE" | "TEMPORARY" | "UNIQUE" | "FULLTEXT" | "SPATIAL" | "ONLINE"
                    | "IGNORE" | "AGGREGATE",
                )
                | None => i += 1,
                Some("DEFINER" | "ALGORITHM") => {
                    // `= value`, where a definer may be `a`@`b`: on to the
                    // next word that starts the rest of the statement.
                    i += 1;
                    while i < self.t.len()
                        && !self.word(i).is_some_and(|w| {
                            matches!(
                                w.as_str(),
                                "VIEW"
                                    | "PROCEDURE"
                                    | "FUNCTION"
                                    | "TRIGGER"
                                    | "EVENT"
                                    | "SQL"
                                    | "ALGORITHM"
                                    | "DEFINER"
                                    | "AGGREGATE"
                            )
                        })
                    {
                        i += 1;
                    }
                }
                Some("SQL") if self.is(i + 1, "SECURITY") => i += 3,
                Some(w) => return format!("{first} {w}").to_ascii_lowercase(),
            }
        }
        first.to_ascii_lowercase()
    }

    /// The account a `DEFINER = ...` clause names, unless it is
    /// `CURRENT_USER`.
    fn definer(&self) -> Option<String> {
        let i = (0..self.t.len()).find(|&i| self.is(i, "DEFINER") && self.punct(i + 1, b'='))?;
        if self.is(i + 2, "CURRENT_USER") {
            return None;
        }
        let user = self.text(i + 2)?;
        // `@host` may lex as one token or as `@` and the host.
        let host = match self.t.get(i + 3).map(|t| t.bytes(self.sql)) {
            Some(b"@") => self.text(i + 4).unwrap_or_default(),
            Some(b) if b.first() == Some(&b'@') => String::from_utf8_lossy(&b[1..])
                .trim_matches(|c| c == '`' || c == '\'' || c == '"')
                .to_owned(),
            _ => String::new(),
        };
        Some(if host.is_empty() {
            user
        } else {
            format!("{user}@{host}")
        })
    }

    /// The engine an `ENGINE [=] name` clause names, outside parentheses
    /// (a column can't have one) or in a partition definition.
    fn engines(&self) -> Vec<String> {
        let mut out = Vec::new();
        for i in 0..self.t.len() {
            if self.is(i, "ENGINE") {
                let j = if self.punct(i + 1, b'=') {
                    i + 2
                } else {
                    i + 1
                };
                if let Some(n) = self.text(j).filter(|n| !n.is_empty()) {
                    out.push(n);
                }
            }
        }
        out
    }

    fn has_word(&self, w: &str) -> bool {
        (0..self.t.len()).any(|i| self.is(i, w))
    }
}

fn add(
    r: &mut SqlReport,
    level: Level,
    at: usize,
    object: Option<&str>,
    rule: &'static str,
    message: String,
    code: Option<u32>,
) {
    r.findings.push(Finding {
        level,
        at: format!("statement {at}"),
        object: object.map(str::to_owned),
        rule,
        message,
        code,
    });
}

fn last_part(n: &ObjectName) -> String {
    n.0.last()
        .map(|p| p.to_string().trim_matches('`').to_owned())
        .unwrap_or_default()
}

fn base_type(t: &str) -> String {
    t.split(|c: char| c == '(' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase()
}

fn idents(cols: &[sqlparser::ast::IndexColumn]) -> Vec<String> {
    cols.iter()
        .map(|c| c.column.expr.to_string().trim_matches('`').to_owned())
        .collect()
}

fn table_of(ct: &CreateTable) -> Table {
    let mut t = Table {
        name: last_part(&ct.name),
        ..Table::default()
    };
    for c in &ct.columns {
        let ty = c.data_type.to_string();
        let name = c.name.value.clone();
        for o in &c.options {
            match &o.option {
                ColumnOption::PrimaryKey(_) => t.pk = vec![name.clone()],
                ColumnOption::ForeignKey(fk) => t.fks.push((
                    vec![name.clone()],
                    last_part(&fk.foreign_table),
                    fk.referred_columns
                        .iter()
                        .map(|i| i.value.clone())
                        .collect(),
                )),
                _ => {}
            }
        }
        t.columns.push((name, ty.clone(), base_type(&ty)));
    }
    for k in &ct.constraints {
        match k {
            TableConstraint::PrimaryKey(p) => t.pk = idents(&p.columns),
            TableConstraint::ForeignKey(fk) => t.fks.push((
                fk.columns.iter().map(|i| i.value.clone()).collect(),
                last_part(&fk.foreign_table),
                fk.referred_columns
                    .iter()
                    .map(|i| i.value.clone())
                    .collect(),
            )),
            _ => {}
        }
    }
    t
}

/// The column types `information_schema` would name for a declared type.
fn info_type(base: &str) -> &str {
    match base {
        "integer" => "int",
        "character" => "char",
        "bool" | "boolean" => "tinyint",
        "dec" | "fixed" => "decimal",
        other => other,
    }
}

/// Whether a column of base type `base` can be a distribution key part,
/// by the pin rules.
fn key_type(base: &str) -> bool {
    sprig_shard::pins::column_class(info_type(base), Some("utf8mb4_bin")).is_some()
}

/// Checks `text` as SprigDB would run it on the MySQL persona (`persona`),
/// in database `db`. `strict`: a statement the parser can't read is an
/// error (`check_ddl`), else a note (a migration, where it still reaches
/// MariaDB).
#[must_use]
#[allow(clippy::too_many_lines)] // one rule after another
pub fn check_sql(text: &str, db: &str, persona: Persona, strict: bool) -> SqlReport {
    let mut r = SqlReport::default();
    let opts = LexOptions::for_persona(persona);
    let db = if db.trim().is_empty() {
        "app"
    } else {
        db.trim()
    };
    let map = MemoryNameMap::new(TenantId(1)).with_db(1, db, 1, 1);
    let mut current = db.to_owned();
    let mut extra_dbs: Vec<String> = Vec::new();
    let mut map = map;
    for (n, stmt) in statements(text).iter().enumerate() {
        let at = n + 1;
        r.statements += 1;
        let sig = Sig::new(stmt, opts);
        let kind = sig.kind();
        *r.kinds.entry(kind.clone()).or_default() += 1;
        // Databases the dump creates or switches to are the tenant's own.
        if (kind == "create database" || kind == "create schema" || kind == "use")
            && let Some(name) = (0..sig.t.len())
                .rev()
                .find_map(|i| sig.text(i).filter(|_| sig.t[i].kind != TokenKind::Punct))
        {
            if kind == "use" {
                current.clone_from(&name);
            }
            if name != db && !extra_dbs.contains(&name) {
                extra_dbs.push(name.clone());
                map = map.with_db(u32::try_from(extra_dbs.len() + 1).unwrap_or(2), &name, 1, 1);
            }
            continue;
        }
        let object = object_name(&sig, &kind);
        let obj = object.as_deref();
        // Replication and server statements.
        let upper = stmt.to_ascii_uppercase();
        if kind.starts_with("change master")
            || kind == "change replication"
            || kind == "start" && (upper.contains("SLAVE") || upper.contains("REPLICA"))
            || kind == "reset" && upper.contains("MASTER")
        {
            add(&mut r, Level::Error, at, obj, "replication",
                "SprigDB isn't a replication target or source; leave replication statements out of the dump".into(),
                Some(1227));
            continue;
        }
        if kind == "set" {
            if upper.contains("GTID_PURGED") {
                add(&mut r, Level::Error, at, None, "gtid_purged",
                    "Setting @@GLOBAL.GTID_PURGED changes the server for every tenant and fails with 1227; take the dump with --set-gtid-purged=OFF".into(),
                    Some(1227));
            } else if upper.contains("SQL_LOG_BIN") {
                add(&mut r, Level::Error, at, None, "sql_log_bin",
                    "SQL_LOG_BIN needs a server privilege tenants don't have (1227); SprigDB's change feed reads the binlog, so writes must be logged. Take the dump with --set-gtid-purged=OFF, which leaves this out".into(),
                    Some(1227));
            } else if upper.contains("GLOBAL") {
                add(&mut r, Level::Change, at, None, "set_global",
                    "SET GLOBAL sets the tenant's default only for sql_mode, time_zone, transaction_isolation, max_execution_time and wait_timeout, with SYSTEM_VARIABLES_ADMIN; anything else fails with 1227".into(),
                    Some(1227));
            }
            continue;
        }
        // The name layer, as every client statement meets it.
        let ctx = RewriteCtx::new(&map, persona).current_db(Some(&current));
        if let Err(e) = rewrite(stmt.as_bytes(), &ctx) {
            add(
                &mut r,
                Level::Error,
                at,
                obj,
                "names",
                format!("SprigDB refuses it: {e}"),
                Some(u32::from(e.errno())),
            );
            continue;
        }
        if let Some(e) = mysqlddl::long_name(stmt.as_bytes(), &opts) {
            add(
                &mut r,
                Level::Error,
                at,
                obj,
                "long_name",
                e.message,
                Some(u32::from(e.code)),
            );
        }
        let ddl_kind = kind.starts_with("create") || kind.starts_with("alter");
        if !ddl_kind {
            continue;
        }
        if let Some(who) = sig.definer() {
            add(
                &mut r,
                Level::Change,
                at,
                obj,
                "definer",
                format!(
                    "DEFINER = {who} names another account: that needs SET_ANY_DEFINER (else 1227). Drop the clause (mariadb-dump --skip-definer, or edit the dump) so the object gets the importing account as its definer"
                ),
                Some(1227),
            );
        }
        if !sig.has_word("TEMPORARY") {
            for e in sig.engines() {
                match engine::classify(&e) {
                    engine::Engine::Substituted(name) => add(
                        &mut r,
                        Level::Change,
                        at,
                        obj,
                        "engine",
                        format!(
                            "ENGINE={name}: every SprigDB table is InnoDB, so the table is created as InnoDB with a warning (it fails instead when compat.engine_substitution is reject). {name}'s own behaviour, such as no transactions, doesn't carry over"
                        ),
                        None,
                    ),
                    engine::Engine::Refused(name) => add(
                        &mut r,
                        Level::Error,
                        at,
                        obj,
                        "engine",
                        format!(
                            "ENGINE={name} isn't available: SprigDB tables hold their rows in InnoDB. Use InnoDB, or keep this data outside SprigDB"
                        ),
                        Some(if persona == Persona::MySql84 {
                            3161
                        } else {
                            1286
                        }),
                    ),
                    engine::Engine::InnoDb | engine::Engine::Unknown => {}
                }
            }
        }
        if sig.has_word("FULLTEXT") {
            add(&mut r, Level::Change, at, obj, "fulltext",
                "A FULLTEXT key becomes a SprigDB search index (search.fulltext_ddl = convert, the default), and MATCH ... AGAINST uses it; relevance scores differ slightly from MariaDB's".into(),
                None);
        }
        if ddl::generated_not_null(stmt.as_bytes(), &opts).is_some() {
            add(
                &mut r,
                Level::Note,
                at,
                obj,
                "generated_not_null",
                "NOT NULL on a generated column is kept as a check constraint, as MySQL means it"
                    .into(),
                None,
            );
        }
        if mysqlddl::now_defaults(stmt.as_bytes(), &opts).is_some() {
            add(&mut r, Level::Note, at, obj, "now_default",
                "DEFAULT (NOW()) is stored as MariaDB's equivalent and reads back as MySQL writes it".into(),
                None);
        }
        // The statement parser, for what follows and for the router.
        let parsed = Parser::parse_sql(&MySqlDialect {}, stmt);
        let parse_matters = matches!(
            kind.as_str(),
            "create table" | "alter table" | "create index"
        );
        match parsed {
            Err(e) if parse_matters => {
                let (level, msg) = if strict {
                    (
                        Level::Error,
                        format!(
                            "SprigDB's statement parser can't read it ({e}). Write it in plain MySQL syntax"
                        ),
                    )
                } else {
                    (
                        Level::Note,
                        format!(
                            "SprigDB's statement parser can't read it ({e}), so its tables weren't analysed for table groups; it still runs on MariaDB if MariaDB accepts it"
                        ),
                    )
                };
                add(&mut r, level, at, obj, "parse", msg, None);
            }
            Err(_) => {}
            Ok(stmts) => {
                for s in stmts {
                    match s {
                        Statement::CreateTable(ct) if ct.query.is_none() && ct.like.is_none() => {
                            let t = table_of(&ct);
                            if !ct.temporary {
                                if t.pk.is_empty() {
                                    add(&mut r, Level::Note, at, Some(&t.name), "no_primary_key",
                                        "No primary key: Galera clusters need one on every table for safe, fast replication, and SprigDB's data moves copy rows in key order. Add one".into(),
                                        None);
                                }
                                if upper.contains("AUTO_INCREMENT") {
                                    add(&mut r, Level::Note, at, Some(&t.name), "auto_increment",
                                        "AUTO_INCREMENT values stay unique, but once the table is sharded they come from ID blocks shared by the gateways: not gap-free and not in insert order".into(),
                                        None);
                                }
                                r.tables.push(t);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    if r.findings.len() > MAX_FINDINGS {
        let more = r.findings.len() - MAX_FINDINGS;
        r.findings.truncate(MAX_FINDINGS);
        r.findings.push(Finding {
            level: Level::Note,
            at: "report".into(),
            object: None,
            rule: "more",
            message: format!("{more} more findings not listed; check the schema in parts"),
            code: None,
        });
    }
    r
}

/// The object a statement creates or changes, by its kind.
fn object_name(sig: &Sig<'_>, kind: &str) -> Option<String> {
    let after = match kind {
        "create table" | "alter table" | "drop table" => "TABLE",
        "create view" | "alter view" => "VIEW",
        "create procedure" => "PROCEDURE",
        "create function" => "FUNCTION",
        "create trigger" => "TRIGGER",
        "create event" => "EVENT",
        "create index" => "INDEX",
        "insert" => "INTO",
        _ => return None,
    };
    let i = (0..sig.t.len()).find(|&i| sig.is(i, after))?;
    let mut j = i + 1;
    while sig.is(j, "IF") || sig.is(j, "NOT") || sig.is(j, "EXISTS") {
        j += 1;
    }
    let mut name = sig.text(j)?;
    if sig.punct(j + 1, b'.') {
        name = sig.text(j + 2).unwrap_or(name);
    }
    Some(name)
}

/// A suggested table group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Group {
    /// The root table.
    pub root: String,
    /// Its key column.
    pub key: String,
    /// Members co-located with the root: table and its key column.
    pub members: Vec<(String, String)>,
}

/// What [`groups`] suggests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Design {
    /// Table groups.
    pub groups: Vec<Group>,
    /// Small code tables referenced from more than one group.
    pub reference: Vec<String>,
    /// Tables left in the default group, with why.
    pub unsharded: Vec<(String, String)>,
    /// `SPRIG PIN` statements for all of it.
    pub pins: Vec<String>,
}

fn small_code_key(t: &Table) -> bool {
    let [k] = t.pk.as_slice() else {
        return false;
    };
    t.columns.iter().any(|(n, ty, base)| {
        n.eq_ignore_ascii_case(k)
            && (matches!(base.as_str(), "tinyint" | "smallint")
                || (matches!(base.as_str(), "char" | "varchar")
                    && ty
                        .trim_start_matches(|c: char| c.is_ascii_alphabetic())
                        .trim_matches(|c| c == '(' || c == ')')
                        .parse::<u32>()
                        .is_ok_and(|n| n <= 8)))
    })
}

fn col_base<'a>(t: &'a Table, col: &str) -> Option<&'a str> {
    t.columns
        .iter()
        .find(|(n, _, _)| n.eq_ignore_ascii_case(col))
        .map(|(_, _, b)| b.as_str())
}

fn q(name: &str) -> String {
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit())
    {
        name.to_owned()
    } else {
        format!("`{}`", name.replace('`', "``"))
    }
}

/// Table groups from the tables' foreign keys (see the module docs).
#[must_use]
#[allow(clippy::too_many_lines)] // roots, members, the rest, then the pins
pub fn groups(tables: &[Table]) -> Design {
    let by: BTreeMap<String, &Table> = tables
        .iter()
        .map(|t| (t.name.to_ascii_lowercase(), t))
        .collect();
    let referenced_by = |name: &str| -> BTreeSet<String> {
        tables
            .iter()
            .filter(|t| {
                t.fks.iter().any(|(_, rt, _)| {
                    rt.eq_ignore_ascii_case(name) && !rt.eq_ignore_ascii_case(&t.name)
                })
            })
            .map(|t| t.name.clone())
            .collect()
    };
    // Code tables: a short key, nothing referenced out, referenced from
    // tables that reference something else too.
    let reference: BTreeSet<String> = tables
        .iter()
        .filter(|t| t.fks.is_empty() && small_code_key(t) && !referenced_by(&t.name).is_empty())
        .map(|t| t.name.clone())
        .collect();
    let is_ref = |n: &str| reference.iter().any(|r| r.eq_ignore_ascii_case(n));
    // Parent links that count: foreign keys to non-reference tables.
    let parents = |t: &Table| -> Vec<(Vec<String>, String, Vec<String>)> {
        t.fks
            .iter()
            .filter(|(_, rt, _)| !is_ref(rt) && !rt.eq_ignore_ascii_case(&t.name))
            .filter(|(_, rt, _)| by.contains_key(&rt.to_ascii_lowercase()))
            .cloned()
            .collect()
    };
    let mut d = Design::default();
    let mut placed: BTreeMap<String, usize> = BTreeMap::new();
    // Roots: one-column keys that can be distribution keys, no parent.
    for t in tables {
        if is_ref(&t.name) || !parents(t).is_empty() {
            continue;
        }
        let [k] = t.pk.as_slice() else { continue };
        if referenced_by(&t.name).is_empty() || !col_base(t, k).is_some_and(key_type) {
            continue;
        }
        placed.insert(t.name.to_ascii_lowercase(), d.groups.len());
        d.groups.push(Group {
            root: t.name.clone(),
            key: k.clone(),
            members: Vec::new(),
        });
    }
    // Members: tables whose parent is placed and that hold the group's key
    // (the column referencing the root, directly or as the parent's own
    // root-key column), in rounds so grandchildren follow children.
    let mut key_col: BTreeMap<String, String> = d
        .groups
        .iter()
        .map(|g| (g.root.to_ascii_lowercase(), g.key.clone()))
        .collect();
    for _ in 0..8 {
        let mut changed = false;
        for t in tables {
            let lt = t.name.to_ascii_lowercase();
            if placed.contains_key(&lt) || is_ref(&t.name) {
                continue;
            }
            for (cols, rt, rcols) in parents(t) {
                let lr = rt.to_ascii_lowercase();
                let (Some(&g), Some(pkey)) = (placed.get(&lr), key_col.get(&lr)) else {
                    continue;
                };
                // The referencing column that maps the parent's key column.
                let mine = rcols
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(pkey))
                    .and_then(|i| cols.get(i).cloned())
                    .or_else(|| {
                        // The parent's group key, held under the same name.
                        t.columns
                            .iter()
                            .find(|(n, _, _)| {
                                n.eq_ignore_ascii_case(pkey)
                                    && lr != d.groups[g].root.to_ascii_lowercase()
                            })
                            .map(|(n, _, _)| n.clone())
                    });
                if let Some(c) = mine {
                    d.groups[g].members.push((t.name.clone(), c.clone()));
                    placed.insert(lt.clone(), g);
                    key_col.insert(lt.clone(), c);
                    changed = true;
                    break;
                }
            }
        }
        if !changed {
            break;
        }
    }
    for t in tables {
        let lt = t.name.to_ascii_lowercase();
        if placed.contains_key(&lt) || is_ref(&t.name) {
            continue;
        }
        let why = if let Some((_, rt, _)) = parents(t).first() {
            format!(
                "it references {rt} but doesn't hold its group's key column; add that column to co-locate it, or the gateway checks the foreign key across groups"
            )
        } else if t.pk.len() != 1 {
            "no one-column primary key to shard by and no parent".to_owned()
        } else {
            "nothing references it and it references nothing; the advisor decides from the workload"
                .to_owned()
        };
        d.unsharded.push((t.name.clone(), why));
    }
    for g in &d.groups {
        d.pins.push(format!(
            "SPRIG PIN {} SHARD KEY ({});",
            q(&g.root),
            q(&g.key)
        ));
        for (m, c) in &g.members {
            // Co-locate with the table the key column came through.
            let parent = by
                .get(&m.to_ascii_lowercase())
                .and_then(|t| {
                    t.fks.iter().find(|(cols, rt, _)| {
                        cols.iter().any(|x| x.eq_ignore_ascii_case(c))
                            && placed.contains_key(&rt.to_ascii_lowercase())
                    })
                })
                .map_or_else(|| g.root.clone(), |(_, rt, _)| rt.clone());
            d.pins.push(format!(
                "SPRIG PIN {} COLOCATE WITH {} ON ({});",
                q(m),
                q(&parent),
                q(c)
            ));
        }
    }
    for r in &reference {
        d.pins.push(format!("SPRIG PIN {} REFERENCE;", q(r)));
    }
    d.reference = reference.into_iter().collect();
    d
}

/// Tables in an order where every parent comes before its children.
#[must_use]
pub fn load_order(tables: &[Table]) -> Vec<String> {
    let mut done: Vec<String> = Vec::new();
    let mut left: Vec<&Table> = tables.iter().collect();
    while !left.is_empty() {
        let before = left.len();
        left.retain(|t| {
            let ready = t.fks.iter().all(|(_, rt, _)| {
                rt.eq_ignore_ascii_case(&t.name)
                    || done.iter().any(|d| d.eq_ignore_ascii_case(rt))
                    || !tables.iter().any(|x| x.name.eq_ignore_ascii_case(rt))
            });
            if ready {
                done.push(t.name.clone());
            }
            !ready
        });
        if left.len() == before {
            // A cycle: the rest in dump order (foreign_key_checks off).
            done.extend(left.iter().map(|t| t.name.clone()));
            break;
        }
    }
    done
}

fn counts(f: &[Finding]) -> Value {
    json!({
        "errors": f.iter().filter(|x| x.level == Level::Error).count(),
        "changes": f.iter().filter(|x| x.level == Level::Change).count(),
        "notes": f.iter().filter(|x| x.level == Level::Note).count(),
    })
}

/// `check_ddl`'s result.
#[must_use]
pub fn check_ddl_json(sql: &str, db: &str) -> Value {
    let r = check_sql(sql, db, Persona::MySql84, true);
    json!({
        "statements": r.statements,
        "passed": !r.has_errors(),
        "counts": counts(&r.findings),
        "findings": r.findings,
        "next": if r.has_errors() {
            "Fix every error and check again before proposing."
        } else {
            "It passes SprigDB's rules; propose it with propose_sql."
        },
    })
}

/// `migration_check`'s report for a MySQL or MariaDB schema.
#[must_use]
pub fn mysql_report(ddl: &str, db: &str, source: &str) -> Value {
    let persona = if source == "mariadb" {
        Persona::MariaDb
    } else {
        Persona::MySql84
    };
    let r = check_sql(ddl, db, persona, false);
    let d = groups(&r.tables);
    let order = load_order(&r.tables);
    let port = if persona == Persona::MySql84 {
        3306
    } else {
        3307
    };
    let dump = if source == "mariadb" {
        "mariadb-dump --single-transaction --routines --triggers --events --skip-definer"
    } else {
        "mysqldump --single-transaction --routines --triggers --events --set-gtid-purged=OFF --no-tablespaces"
    };
    let mut plan = vec![
        format!(
            "Take the dump with {dump} (definers: remove DEFINER clauses, or import as an account with SET_ANY_DEFINER)."
        ),
        format!(
            "Fix the errors below in the dump; {} found.",
            r.findings
                .iter()
                .filter(|f| f.level == Level::Error)
                .count()
        ),
        format!("Create the database: CREATE DATABASE {};", q(db)),
        format!(
            "Load the schema through the {} persona (port {port}), so its DDL is read as {} writes it: mysql -h <gateway> -P {port} -u <tenant>.<user> {} < schema.sql",
            if port == 3306 { "MySQL" } else { "MariaDB" },
            if port == 3306 { "MySQL" } else { "MariaDB" },
            q(db)
        ),
    ];
    if d.pins.is_empty() {
        plan.push("No table groups to pin: the sharding advisor designs them from the workload once data and traffic arrive.".into());
    } else {
        plan.push(format!("Optional: run the {} SPRIG PIN statements before loading, to fix the table groups now; otherwise the sharding advisor designs them from the workload.", d.pins.len()));
    }
    plan.push(format!("Load the data in this order, parents first: {}. A big table loads fastest with LOAD DATA LOCAL INFILE.", order.join(", ")));
    plan.push("Check: row counts per table, SPRIG SHOW ADVICE after a day of traffic, and the workload view for slow statements.".into());
    json!({
        "source": source,
        "db": db,
        "statements": r.statements,
        "kinds": r.kinds,
        "tables": r.tables.len(),
        "counts": counts(&r.findings),
        "findings": r.findings,
        "groups": d.groups.iter().map(|g| json!({
            "root": g.root, "key": g.key,
            "members": g.members.iter().map(|(t, c)| json!({"table": t, "key_column": c})).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "reference": d.reference,
        "unsharded": d.unsharded.iter().map(|(t, w)| json!({"table": t, "why": w})).collect::<Vec<_>>(),
        "pins": d.pins,
        "load_order": order,
        "plan": plan,
        "note": "Deterministic: from SprigDB's own rules, not the model. Table groups follow foreign keys and are suggestions; the sharding advisor decides from the real workload unless tables are pinned.",
    })
}

// ---- MongoDB ----

/// Index key values the MongoDB front end builds, and how.
fn index_kind(v: &Value) -> (&'static str, Level, String) {
    match v {
        Value::String(s) => match s.as_str() {
            "text" => (
                "text",
                Level::Change,
                "becomes a SprigDB search index (one per collection)".into(),
            ),
            "2dsphere" => (
                "2dsphere",
                Level::Note,
                "supported on one path; a compound 2dsphere index fails with 39662".into(),
            ),
            "hashed" => (
                "hashed",
                Level::Change,
                "accepted, but built as an ascending index".into(),
            ),
            "2d" => (
                "2d",
                Level::Error,
                "2d indexes aren't supported (39662); store GeoJSON points and use 2dsphere".into(),
            ),
            "geoHaystack" => (
                "geoHaystack",
                Level::Error,
                "geoHaystack indexes aren't supported (39662); use 2dsphere".into(),
            ),
            other => (
                "other",
                Level::Error,
                format!("index type {other} isn't supported (39662)"),
            ),
        },
        _ => ("btree", Level::Note, String::new()),
    }
}

/// `migration_check`'s report for MongoDB collections: a JSON array of
/// `{name, options?, indexes?}` (or `{name, type}` for views).
#[must_use]
#[allow(clippy::too_many_lines)] // one rule after another
pub fn mongo_report(collections: &Value, db: &str) -> Value {
    let mut findings: Vec<Finding> = Vec::new();
    let mut add = |level, at: &str, rule, message: String, code| {
        findings.push(Finding {
            level,
            at: at.to_owned(),
            object: None,
            rule,
            message,
            code,
        });
    };
    let empty = Vec::new();
    let list = collections.as_array().unwrap_or(&empty);
    let mut names = Vec::new();
    for c in list {
        let name = c["name"].as_str().unwrap_or("?").to_owned();
        let at = format!("collection {name}");
        names.push(name.clone());
        let opts = c.get("options").cloned().unwrap_or_else(|| json!({}));
        if c["type"] == "view" || opts.get("viewOn").is_some() {
            add(Level::Error, &at, "view", "Views aren't supported (39662): recreate the pipeline as an aggregation the application runs, or as a SQL view over the collection".into(), Some(39662));
            continue;
        }
        if c["type"] == "timeseries" || opts.get("timeseries").is_some() {
            add(Level::Error, &at, "timeseries", "Time-series collections aren't supported (39662): use an ordinary collection with an index on the time field, or a SprigDB stream".into(), Some(39662));
        }
        if opts.get("clusteredIndex").is_some() {
            add(
                Level::Error,
                &at,
                "clustered",
                "Clustered collections aren't supported (39662): create it without clusteredIndex"
                    .into(),
                Some(39662),
            );
        }
        if opts.get("encryptedFields").is_some() {
            add(
                Level::Error,
                &at,
                "encryption",
                "Queryable Encryption isn't supported (39662)".into(),
                Some(39662),
            );
        }
        if opts.get("collation").is_some() {
            add(Level::Change, &at, "collation", "A collection's default collation isn't kept; pass the collation per command instead".into(), None);
        }
        if opts.get("capped").and_then(Value::as_bool) == Some(true) {
            add(
                Level::Note,
                &at,
                "capped",
                "Capped collections work, with their size and max".into(),
                None,
            );
        }
        // The validator, through the same checks as `create`.
        let mut v = serde_json::Map::new();
        for k in ["validator", "validationLevel", "validationAction"] {
            if let Some(x) = opts.get(k) {
                v.insert(k.to_owned(), x.clone());
            }
        }
        if !v.is_empty() {
            match bson::to_document(&Value::Object(v)) {
                Ok(d) => {
                    if let Err(e) = sprig_doc::validate::check_create(&d) {
                        add(
                            Level::Error,
                            &at,
                            "validator",
                            format!("The validator is refused: {} ({})", e.message, e.code),
                            Some(u32::try_from(e.code).unwrap_or(0)),
                        );
                    } else {
                        add(
                            Level::Note,
                            &at,
                            "validator",
                            "The validator is kept and checked on every write, as in MongoDB"
                                .into(),
                            None,
                        );
                    }
                }
                Err(e) => add(
                    Level::Error,
                    &at,
                    "validator",
                    format!("The validator isn't valid BSON: {e}"),
                    None,
                ),
            }
        }
        for ix in c["indexes"].as_array().unwrap_or(&empty) {
            let iname = ix["name"].as_str().unwrap_or("?");
            if iname == "_id_" {
                continue;
            }
            let at = format!("{at}, index {iname}");
            let key = ix["key"].as_object();
            let mut wildcard = false;
            if let Some(k) = key {
                for (path, dir) in k {
                    if path.contains("$**") {
                        wildcard = true;
                    }
                    let (_, level, msg) = index_kind(dir);
                    if !msg.is_empty() {
                        add(
                            level,
                            &at,
                            "index_type",
                            format!("{path}: {msg}"),
                            (level == Level::Error).then_some(39662),
                        );
                    }
                }
                if k.values().any(|x| x == "2dsphere") && k.len() > 1 {
                    add(
                        Level::Error,
                        &at,
                        "index_type",
                        "Compound 2dsphere indexes aren't supported (39662)".into(),
                        Some(39662),
                    );
                }
            }
            if wildcard {
                add(
                    Level::Error,
                    &at,
                    "wildcard",
                    "Wildcard indexes aren't supported (39662); index the paths queries use".into(),
                    Some(39662),
                );
            }
            for (k, what) in [
                ("sparse", "sparse"),
                ("partialFilterExpression", "partial"),
                ("hidden", "hidden"),
                ("collation", "collation"),
            ] {
                if ix.get(k).is_some_and(|x| x != &Value::Bool(false)) {
                    add(
                        Level::Change,
                        &at,
                        "index_option",
                        format!(
                            "The {what} option is kept and listed, but the index holds every document"
                        ),
                        None,
                    );
                }
            }
            if ix.get("expireAfterSeconds").is_some() {
                add(
                    Level::Note,
                    &at,
                    "ttl",
                    "TTL works: expired documents are deleted in the background".into(),
                    None,
                );
            }
            add(Level::Note, &at, "multikey", "If a key path holds arrays, the index doesn't index the elements (no multikey); queries on array fields are checked in the gateway, so filter on other indexed fields too".into(), None);
        }
    }
    let plan = vec![
        "Dump with mongodump --db <source> (BSON keeps types).".to_owned(),
        "Fix the errors below: views, time-series and clustered collections, and unsupported index types.".to_owned(),
        format!("Restore through SprigDB's MongoDB listener with your tenant login: mongorestore --uri 'mongodb://<tenant>.<user>@<gateway>:<port>/?authMechanism=SCRAM-SHA-256' --nsInclude '<source>.*' --nsFrom '<source>.*' --nsTo '{db}.*'"),
        "Indexes and validators are restored with the collections; the automatic indexer adds more from the queries it sees.".to_owned(),
        "Check: document counts per collection, and the workload view for slow commands.".to_owned(),
    ];
    json!({
        "source": "mongodb",
        "db": db,
        "collections": names,
        "counts": counts(&findings),
        "findings": findings,
        "plan": plan,
        "note": "Deterministic: from SprigDB's own rules, not the model. Sample documents aren't read.",
    })
}

/// Writes a report's findings as lines, for tests and logs.
#[must_use]
pub fn lines(findings: &[Finding]) -> String {
    let mut s = String::new();
    for f in findings {
        let _ = writeln!(s, "{:?} {} {}: {}", f.level, f.at, f.rule, f.message);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(r: &SqlReport) -> Vec<(&'static str, Level)> {
        r.findings.iter().map(|f| (f.rule, f.level)).collect()
    }

    #[test]
    fn a_plain_table_passes() {
        let r = check_sql(
            "CREATE TABLE customers (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, KEY ix_email (email))",
            "shop",
            Persona::MySql84,
            true,
        );
        assert!(!r.has_errors(), "{}", lines(&r.findings));
        assert_eq!(r.tables.len(), 1);
        assert_eq!(r.tables[0].pk, ["id"]);
    }

    #[test]
    fn dump_statements_meet_the_rules() {
        let dump = r"
-- MySQL dump 10.13
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
SET @@SESSION.SQL_LOG_BIN= 0;
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'abc:1-5';
CREATE TABLE `logs` (`id` int NOT NULL AUTO_INCREMENT, `msg` text, PRIMARY KEY (`id`), FULLTEXT KEY `ft` (`msg`)) ENGINE=MyISAM;
CREATE TABLE `bh` (`id` int PRIMARY KEY) ENGINE=BLACKHOLE;
CREATE TABLE `nokey` (`a` int);
CREATE TABLE `__sprig_x` (`id` int PRIMARY KEY);
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `p`() BEGIN SELECT 1; END ;;
DELIMITER ;
INSERT INTO `logs` VALUES (1,'a');
";
        let r = check_sql(dump, "app", Persona::MySql84, false);
        let got = rules(&r);
        for want in [
            ("sql_log_bin", Level::Error),
            ("gtid_purged", Level::Error),
            ("engine", Level::Change),
            ("fulltext", Level::Change),
            ("engine", Level::Error),
            ("no_primary_key", Level::Note),
            ("names", Level::Error),
            ("definer", Level::Change),
            ("auto_increment", Level::Note),
        ] {
            assert!(
                got.contains(&want),
                "{want:?} missing:\n{}",
                lines(&r.findings)
            );
        }
        assert_eq!(r.kinds.get("create procedure"), Some(&1), "{:?}", r.kinds);
        assert_eq!(r.kinds.get("insert"), Some(&1), "{:?}", r.kinds);
        // A reserved column name, as the name layer refuses it.
        let r = check_sql(
            "ALTER TABLE users ADD COLUMN _sprig_flags INT NOT NULL DEFAULT 0",
            "app",
            Persona::MySql84,
            true,
        );
        assert_eq!(r.findings[0].code, Some(39050), "{}", lines(&r.findings));
    }

    #[test]
    fn long_names_and_unparseable_ddl_fail_checks() {
        let long = "x".repeat(70);
        let r = check_sql(
            &format!("CREATE TABLE {long} (id INT PRIMARY KEY)"),
            "d",
            Persona::MySql84,
            true,
        );
        assert!(
            rules(&r).contains(&("long_name", Level::Error)),
            "{}",
            lines(&r.findings)
        );
        let r = check_sql(
            "CREATE TABLE t (id INT PRIMARY KEY,, x)",
            "d",
            Persona::MySql84,
            true,
        );
        assert!(r.has_errors(), "{}", lines(&r.findings));
    }

    #[test]
    fn groups_follow_foreign_keys() {
        let ddl = "
CREATE TABLE countries (code CHAR(2) PRIMARY KEY, name VARCHAR(80));
CREATE TABLE customers (id BIGINT PRIMARY KEY, country CHAR(2), FOREIGN KEY (country) REFERENCES countries(code));
CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, country CHAR(2),
  FOREIGN KEY (customer_id) REFERENCES customers(id), FOREIGN KEY (country) REFERENCES countries(code));
CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT, customer_id BIGINT,
  FOREIGN KEY (order_id) REFERENCES orders(id));
CREATE TABLE audit (id BIGINT PRIMARY KEY, note TEXT);
";
        let r = check_sql(ddl, "shop", Persona::MySql84, false);
        let d = groups(&r.tables);
        assert_eq!(d.groups.len(), 1, "{d:?}");
        assert_eq!(d.groups[0].root, "customers");
        assert_eq!(
            d.groups[0].members,
            [
                ("orders".to_owned(), "customer_id".to_owned()),
                ("order_items".to_owned(), "customer_id".to_owned())
            ]
        );
        assert_eq!(d.reference, ["countries"]);
        assert!(
            d.pins
                .contains(&"SPRIG PIN customers SHARD KEY (id);".to_owned())
        );
        assert!(
            d.pins
                .contains(&"SPRIG PIN orders COLOCATE WITH customers ON (customer_id);".to_owned())
        );
        assert!(
            d.pins
                .contains(&"SPRIG PIN countries REFERENCE;".to_owned())
        );
        assert_eq!(d.unsharded[0].0, "audit");
        assert_eq!(load_order(&r.tables)[..2], ["countries", "customers"]);
        // Every pin parses as SprigDB reads pins.
        for p in &d.pins {
            let opts = LexOptions::mysql84();
            assert!(
                matches!(
                    sprig_sql::pin_stmt::parse(p.trim_end_matches(';').as_bytes(), &opts),
                    Some(Ok(_))
                ),
                "{p}"
            );
        }
    }

    #[test]
    fn mongo_rules() {
        let colls = json!([
            {"name": "events", "options": {"timeseries": {"timeField": "ts"}}},
            {"name": "v", "type": "view", "options": {"viewOn": "events"}},
            {"name": "users", "options": {"validator": {"$jsonSchema": {"bsonType": "object", "required": ["email"]}}},
             "indexes": [{"name": "_id_", "key": {"_id": 1}}, {"name": "loc", "key": {"loc": "2d"}},
                         {"name": "all", "key": {"$**": 1}}, {"name": "e", "key": {"email": 1}, "sparse": true}]},
        ]);
        let r = mongo_report(&colls, "app");
        let f = r["findings"].as_array().unwrap();
        let has =
            |rule: &str, level: &str| f.iter().any(|x| x["rule"] == rule && x["level"] == level);
        assert!(has("timeseries", "error") && has("view", "error"), "{r}");
        assert!(
            has("index_type", "error") && has("wildcard", "error"),
            "{r}"
        );
        assert!(
            has("index_option", "change") && has("validator", "note"),
            "{r}"
        );
        let bad = mongo_report(
            &json!([{"name": "x", "options": {"validator": {"$jsonSchema": {"bsonType": 5}}}}]),
            "app",
        );
        assert!(
            bad["findings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|x| x["rule"] == "validator" && x["level"] == "error"),
            "{bad}"
        );
    }
}
