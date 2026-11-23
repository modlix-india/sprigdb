//! Table definitions produced by the engine itself: `CREATE TABLE`,
//! `CREATE TABLE ... LIKE`, `ALTER TABLE`, `CREATE INDEX` and `DROP INDEX`
//! read into a definition, normalized as the servers normalize them, checked
//! with the servers' errors, and printed as MariaDB 11.8's `SHOW CREATE
//! TABLE` (the text the catalog stores and `table::parse` reads).
//!
//! The rules were taken from black-box observation of MariaDB 11.8 and
//! MySQL 8.4:
//! - types in the server's spelling (`int(11)`, `tinyint(1)` for `BOOL`,
//!   `decimal(10,0)`, `double` for `REAL` and `FLOAT(25..53)`, `TEXT(n)` as
//!   the smallest text type holding n characters, `JSON` as `longtext` in
//!   `utf8mb4_bin` with a `json_valid` check, `SERIAL` as `bigint(20)
//!   unsigned NOT NULL AUTO_INCREMENT` with a unique key);
//! - a string column's character set and collation printed (both) only
//!   when they differ from the table's;
//! - a nullable column without a default shows `DEFAULT NULL` (a
//!   `TIMESTAMP` also shows `NULL`); constants of `DEFAULT` in the
//!   column's own form; `DEFAULT (constant)` as the constant; a function
//!   call bare; any other expression in parentheses;
//! - primary key columns become `NOT NULL`, as does an `AUTO_INCREMENT`
//!   column;
//! - unnamed keys take their first column's name (`a`, then `a_2`, ...);
//!   keys are listed primary first, then unique keys without nullable
//!   parts, unique keys with prefix parts, other unique keys, plain keys
//!   and full-text keys last, each group in definition order;
//! - a key on a `TEXT` / `BLOB` column, or one longer than 3072 bytes, takes
//!   a 768-character prefix;
//! - unnamed `CHECK` constraints are `CONSTRAINT_n` with the smallest
//!   unused n; a column's own check stays on its column;
//! - unnamed foreign keys are `<table>_ibfk_n`, and get an index (named
//!   after the constraint, or else the first column) unless one starts
//!   with their columns.

mod alter;
pub(crate) mod expr;
pub(crate) mod fkcheck;
mod lex;
mod types;

#[cfg(test)]
mod tests;

use sprig_common::SprigError;
use std::fmt::Write as _;

use self::expr::{E, quote_name, quote_str};
use self::lex::{T, Tok};
use self::types::{CsSpec, Ty};
// Character set names and defaults, for database-level DDL.
pub(crate) use self::types::{charset_name, collation, default_collation};

/// Why reading or checking a definition failed.
#[derive(Debug)]
pub(crate) enum Fail {
    /// A syntax error at token index `usize` (1064, quoting the statement
    /// from there).
    Syntax(usize),
    /// A definition error.
    Err(SprigError),
}

/// A definition error with a standard code.
pub(crate) fn err(code: u16, state: &str, msg: impl Into<String>) -> Fail {
    let mut s = [b'H', b'Y', b'0', b'0', b'0'];
    s.copy_from_slice(&state.as_bytes()[..5]);
    Fail::Err(SprigError::standard(code, s, msg.into()))
}

/// 39960 for a definition the engine can't produce yet.
fn refuse(what: &str) -> Fail {
    Fail::Err(super::not_on_engine(what))
}

/// What a statement is read against.
#[derive(Debug, Clone)]
pub(crate) struct Ctx {
    /// The MySQL port (errors in MySQL 8.4's words where they differ).
    pub mysql: bool,
    /// The database's default character set and collation.
    pub db_cs: (String, String),
}

/// A column's `DEFAULT`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Default {
    /// `DEFAULT NULL`.
    Null,
    /// A constant as printed.
    Lit(String),
    /// A constant still to be put in the column's form.
    Raw(E),
    /// `current_timestamp(p)`.
    Now(u8),
    /// An expression.
    Expr(E),
}

/// One column.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)] // a column definition's independent attributes
pub(crate) struct Col {
    pub name: String,
    pub ty: Ty,
    pub not_null: bool,
    /// `NULL` was written (a `TIMESTAMP` prints it).
    pub null_written: bool,
    pub default: Option<Default>,
    pub on_update: Option<u8>,
    pub auto_inc: bool,
    pub comment: Option<String>,
    pub invisible: bool,
    /// `GENERATED ALWAYS AS (e) VIRTUAL|STORED`.
    pub generated: Option<(E, bool)>,
    /// The column's own `CHECK`.
    pub check: Option<E>,
}

/// A key's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyKind {
    Primary,
    Unique,
    Plain,
    Fulltext,
    Spatial,
}

/// One part of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Part {
    pub col: String,
    pub len: Option<u32>,
    pub desc: bool,
}

/// One key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Key {
    pub kind: KeyKind,
    /// The name (`PRIMARY` for the primary key); empty until named.
    pub name: String,
    pub parts: Vec<Part>,
    /// `USING BTREE` / `USING HASH`.
    pub using: Option<String>,
    pub comment: Option<String>,
    /// MariaDB's `IGNORED`.
    pub ignored: bool,
}

/// A table `CHECK` constraint.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Check {
    pub name: String,
    pub expr: E,
}

/// A foreign key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Fk {
    pub name: String,
    pub cols: Vec<String>,
    /// The parent's database (logical or physical name, as written), if
    /// named.
    pub ref_db: Option<String>,
    pub ref_table: String,
    pub ref_cols: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

/// A table's definition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Def {
    pub name: String,
    pub cols: Vec<Col>,
    pub keys: Vec<Key>,
    pub fks: Vec<Fk>,
    pub checks: Vec<Check>,
    /// The table's character set and collation.
    pub cs: (String, String),
    /// `AUTO_INCREMENT=n`.
    pub auto_increment: Option<u64>,
    pub comment: Option<String>,
    /// `ROW_FORMAT=...` as printed.
    pub row_format: Option<String>,
    /// `KEY_BLOCK_SIZE=n`.
    pub key_block_size: Option<u64>,
    /// `STATS_PERSISTENT=n`.
    pub stats_persistent: Option<String>,
}

impl Def {
    /// The column named `n` (case-insensitive).
    pub(crate) fn col(&self, n: &str) -> Option<usize> {
        self.cols
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(n))
    }

    /// The key both servers treat as the primary key: `PRIMARY KEY`, or,
    /// without one, the first unique key whose parts are whole `NOT NULL`
    /// columns (its columns report `PRI`).
    pub(crate) fn primary(&self) -> Option<&Key> {
        self.keys
            .iter()
            .find(|k| k.kind == KeyKind::Primary)
            .or_else(|| {
                self.keys.iter().find(|k| {
                    k.kind == KeyKind::Unique
                        && k.parts.iter().all(|p| {
                            p.len.is_none()
                                && self.col(&p.col).is_some_and(|i| self.cols[i].not_null)
                        })
                })
            })
    }

    /// The key named `n` (case-insensitive).
    pub(crate) fn key(&self, n: &str) -> Option<usize> {
        self.keys
            .iter()
            .position(|k| k.name.eq_ignore_ascii_case(n))
    }
}

/// The statement as a SQL error: 1064 quoting from token `at`.
pub(crate) fn syntax_error(sql: &str, toks: &[Tok], at: usize, mysql: bool) -> SprigError {
    let (near, line) = match toks.get(at) {
        Some(t) => (&sql[t.at..], sql[..t.at].matches('\n').count() + 1),
        None => ("", sql.matches('\n').count() + 1),
    };
    // The servers quote at most 80 characters.
    let near: String = near.chars().take(80).collect();
    let server = if mysql { "MySQL" } else { "MariaDB" };
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your {server} server version for the right syntax to use near '{near}' at line {line}"
        ),
    )
}

/// `Fail` as the statement's error.
pub(crate) fn to_error(f: Fail, sql: &str, toks: &[Tok], mysql: bool) -> SprigError {
    match f {
        Fail::Syntax(at) => syntax_error(sql, toks, at, mysql),
        Fail::Err(e) => e,
    }
}

/// The tokens of `sql`, or its syntax error.
///
/// # Errors
/// 1064 where a quote or comment never ends.
pub(crate) fn tokens(sql: &str, mysql: bool) -> Result<Vec<Tok>, SprigError> {
    lex::lex(sql).map_err(|at| {
        let near: String = sql[at..].chars().take(80).collect();
        let server = if mysql { "MySQL" } else { "MariaDB" };
        SprigError::standard(
            1064,
            *b"42000",
            format!(
                "You have an error in your SQL syntax; check the manual that corresponds to your {server} server version for the right syntax to use near '{near}' at line 1"
            ),
        )
    })
}

/// A cursor over tokens.
pub(crate) struct P<'a> {
    pub t: &'a [Tok],
    pub i: usize,
}

impl P<'_> {
    pub(crate) fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    pub(crate) fn is(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| t.is(w))
    }
    pub(crate) fn is_at(&self, k: usize, w: &str) -> bool {
        self.t.get(self.i + k).is_some_and(|t| t.is(w))
    }
    pub(crate) fn eat(&mut self, w: &str) -> bool {
        if self.is(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    pub(crate) fn want(&mut self, w: &str) -> Result<(), Fail> {
        if self.eat(w) {
            Ok(())
        } else {
            Err(Fail::Syntax(self.i))
        }
    }
    pub(crate) fn op(&self, o: &str) -> bool {
        self.peek().is_some_and(|t| t.op(o))
    }
    pub(crate) fn eat_op(&mut self, o: &str) -> bool {
        if self.op(o) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    pub(crate) fn want_op(&mut self, o: &str) -> Result<(), Fail> {
        if self.eat_op(o) {
            Ok(())
        } else {
            Err(Fail::Syntax(self.i))
        }
    }
    /// An identifier: back-quoted, or a bare word that isn't reserved here.
    pub(crate) fn ident(&mut self) -> Result<String, Fail> {
        match self.peek().map(|t| &t.t) {
            Some(T::Quoted(q)) => {
                let q = q.clone();
                self.i += 1;
                Ok(q)
            }
            Some(T::Word(w)) if !reserved(w) => {
                let w = w.clone();
                self.i += 1;
                Ok(w)
            }
            _ => Err(Fail::Syntax(self.i)),
        }
    }
    /// `name` or `db.name`: (db, name).
    pub(crate) fn qualified(&mut self) -> Result<(Option<String>, String), Fail> {
        let a = self.ident()?;
        if self.eat_op(".") {
            let b = self.ident()?;
            return Ok((Some(a), b));
        }
        Ok((None, a))
    }
    /// A string literal.
    pub(crate) fn string(&mut self) -> Result<String, Fail> {
        match self.peek().map(|t| &t.t) {
            Some(T::Str(s)) => {
                let s = s.clone();
                self.i += 1;
                Ok(s)
            }
            _ => Err(Fail::Syntax(self.i)),
        }
    }
    /// An unsigned integer literal.
    pub(crate) fn number(&mut self) -> Result<u64, Fail> {
        match self.peek().map(|t| &t.t) {
            Some(T::Num(n)) => {
                let v = n.parse::<u64>().map_err(|_| Fail::Syntax(self.i))?;
                self.i += 1;
                Ok(v)
            }
            _ => Err(Fail::Syntax(self.i)),
        }
    }
    /// An expression (in a definition).
    pub(crate) fn expr(&mut self, charset: &str) -> Result<E, Fail> {
        let mut r = expr::Reader {
            t: self.t,
            i: self.i,
            charset,
        };
        let e = r.expr().map_err(|b| match b {
            expr::Bad::Syntax(at) => Fail::Syntax(at),
            expr::Bad::Subquery => err(
                1970,
                "42000",
                "CHECK does not support subqueries or stored functions",
            ),
            expr::Bad::Variable => Fail::Syntax(r.i.saturating_sub(1)),
        })?;
        self.i = r.i;
        Ok(expr::normalize(e))
    }
    /// `( expr )`.
    pub(crate) fn paren_expr(&mut self, charset: &str) -> Result<E, Fail> {
        self.want_op("(")?;
        let e = self.expr(charset)?;
        self.want_op(")")?;
        Ok(e)
    }
}

/// Words that can't be a bare name in a definition.
fn reserved(w: &str) -> bool {
    const R: [&str; 44] = [
        "ADD",
        "ALTER",
        "AND",
        "AS",
        "BETWEEN",
        "BY",
        "CHECK",
        "COLLATE",
        "COLUMN",
        "CONSTRAINT",
        "CREATE",
        "DEFAULT",
        "DELETE",
        "DESC",
        "DROP",
        "EXISTS",
        "FOREIGN",
        "FROM",
        "FULLTEXT",
        "IF",
        "IN",
        "INDEX",
        "IS",
        "KEY",
        "LIKE",
        "NOT",
        "NULL",
        "ON",
        "OR",
        "PRIMARY",
        "REFERENCES",
        "RENAME",
        "SELECT",
        "SET",
        "SPATIAL",
        "TABLE",
        "TO",
        "UNIQUE",
        "UPDATE",
        "USING",
        "WHERE",
        "WITH",
        "CHANGE",
        "ORDER",
    ];
    R.iter().any(|r| r.eq_ignore_ascii_case(w))
}

/// What a `CREATE TABLE` statement is.
#[derive(Debug)]
pub(crate) enum Created {
    /// A new definition.
    Def(Box<Def>),
    /// `CREATE TABLE t LIKE (db.)src`.
    Like(Option<String>, String),
}

/// Reads a `CREATE TABLE` statement.
///
/// # Errors
/// 1064 and the definition errors.
pub(crate) fn create(sql: &str, cx: &Ctx) -> Result<Created, SprigError> {
    let toks = tokens(sql, cx.mysql)?;
    create_in(&toks, cx).map_err(|f| to_error(f, sql, &toks, cx.mysql))
}

fn create_in(toks: &[Tok], cx: &Ctx) -> Result<Created, Fail> {
    let mut p = P { t: toks, i: 0 };
    p.want("CREATE")?;
    if p.eat("OR") {
        p.want("REPLACE")?;
    }
    if p.is("TEMPORARY") {
        return Err(refuse("temporary tables"));
    }
    p.want("TABLE")?;
    if p.eat("IF") {
        p.want("NOT")?;
        p.want("EXISTS")?;
    }
    let (_, name) = p.qualified()?;
    if p.eat("LIKE") {
        let (db, src) = p.qualified()?;
        return end(&p).map(|()| Created::Like(db, src));
    }
    if p.op("(") && p.is_at(1, "LIKE") {
        p.i += 2;
        let (db, src) = p.qualified()?;
        p.want_op(")")?;
        return end(&p).map(|()| Created::Like(db, src));
    }
    let mut raw = Raw::default();
    if p.eat_op("(") {
        // `CREATE TABLE t (SELECT ...)` is a query.
        if p.is("SELECT") || p.is("WITH") {
            return Err(refuse("CREATE TABLE ... SELECT"));
        }
        loop {
            element(&mut p, &mut raw, &cx.db_cs.0)?;
            if p.eat_op(",") {
                continue;
            }
            p.want_op(")")?;
            break;
        }
    } else if !p.is("SELECT") {
        return Err(Fail::Syntax(p.i));
    }
    table_options(&mut p, &mut raw.opts)?;
    if p.is("PARTITION") {
        return Err(refuse("partitioned tables"));
    }
    if p.is("AS") || p.is("SELECT") || p.is("IGNORE") || p.is("REPLACE") || p.is("WITH") {
        return Err(refuse("CREATE TABLE ... SELECT"));
    }
    end(&p)?;
    if raw.cols.is_empty() {
        return Err(err(1113, "42000", "A table must have at least 1 column"));
    }
    let def = finish(name, raw, cx)?;
    Ok(Created::Def(Box::new(def)))
}

/// The statement must end here (an optional `;`).
fn end(p: &P<'_>) -> Result<(), Fail> {
    let mut i = p.i;
    while p.t.get(i).is_some_and(|t| t.op(";")) {
        i += 1;
    }
    if i < p.t.len() {
        Err(Fail::Syntax(i))
    } else {
        Ok(())
    }
}

/// A definition as read, before it is normalized.
#[derive(Debug, Default)]
pub(crate) struct Raw {
    pub cols: Vec<RawCol>,
    pub keys: Vec<Key>,
    pub fks: Vec<Fk>,
    pub checks: Vec<(Option<String>, E)>,
    pub opts: Opts,
}

/// A column as read.
#[derive(Debug, Clone)]
pub(crate) struct RawCol {
    pub col: Col,
    pub cs: CsSpec,
    /// `PRIMARY KEY` / `KEY` on the column.
    pub primary: bool,
    /// `UNIQUE [KEY]` on the column.
    pub unique: bool,
    /// `NULL` / `NOT NULL` was written.
    pub null_said: Option<bool>,
    /// `FIRST` / `AFTER x` (ALTER).
    #[allow(clippy::option_option)] // no place, `FIRST`, or `AFTER x`
    pub place: Option<Option<String>>,
}

/// Table options as read.
#[derive(Debug, Default, Clone)]
pub(crate) struct Opts {
    pub engine: Option<String>,
    pub charset: Option<String>,
    pub collate: Option<String>,
    pub auto_increment: Option<u64>,
    pub comment: Option<String>,
    pub row_format: Option<String>,
    pub key_block_size: Option<u64>,
    pub stats_persistent: Option<String>,
}

/// One element of a `CREATE TABLE` list.
#[allow(clippy::too_many_lines)] // one branch per element kind
pub(crate) fn element(p: &mut P<'_>, raw: &mut Raw, cs: &str) -> Result<(), Fail> {
    let mut cname: Option<String> = None;
    if p.eat("CONSTRAINT")
        && !(p.is("PRIMARY") || p.is("UNIQUE") || p.is("FOREIGN") || p.is("CHECK"))
    {
        cname = Some(p.ident()?);
    }
    if p.eat("PRIMARY") {
        p.want("KEY")?;
        let mut k = key_body(p, KeyKind::Primary, false)?;
        k.name = "PRIMARY".into();
        raw.keys.push(k);
        return Ok(());
    }
    if p.eat("UNIQUE") {
        if !p.eat("KEY") {
            p.eat("INDEX");
        }
        let mut k = key_body(p, KeyKind::Unique, true)?;
        if k.name.is_empty()
            && let Some(c) = cname
        {
            k.name = c;
        }
        raw.keys.push(k);
        return Ok(());
    }
    if p.eat("FOREIGN") {
        p.want("KEY")?;
        let idx_name = if p.op("(") { None } else { Some(p.ident()?) };
        let cols = name_list(p)?;
        p.want("REFERENCES")?;
        let (ref_db, ref_table) = p.qualified()?;
        let ref_cols = name_list(p)?;
        let (mut on_delete, mut on_update) = (None, None);
        while p.eat("ON") {
            let del = if p.eat("DELETE") {
                true
            } else {
                p.want("UPDATE")?;
                false
            };
            let action = if p.eat("CASCADE") {
                "CASCADE"
            } else if p.eat("RESTRICT") {
                "RESTRICT"
            } else if p.eat("SET") {
                if p.eat("NULL") {
                    "SET NULL"
                } else {
                    p.want("DEFAULT")?;
                    "SET DEFAULT"
                }
            } else {
                p.want("NO")?;
                p.want("ACTION")?;
                "NO ACTION"
            };
            if del {
                on_delete = Some(action.to_owned());
            } else {
                on_update = Some(action.to_owned());
            }
        }
        if p.is("MATCH") {
            return Err(refuse("MATCH in a foreign key"));
        }
        raw.fks.push(Fk {
            name: cname.or(idx_name).unwrap_or_default(),
            cols,
            ref_db,
            ref_table,
            ref_cols,
            on_delete,
            on_update,
        });
        return Ok(());
    }
    if p.eat("CHECK") {
        let e = p.paren_expr(cs)?;
        raw.checks.push((cname, e));
        return Ok(());
    }
    if cname.is_some() {
        return Err(Fail::Syntax(p.i));
    }
    if p.is("KEY") || p.is("INDEX") {
        p.i += 1;
        raw.keys.push(key_body(p, KeyKind::Plain, true)?);
        return Ok(());
    }
    if p.is("FULLTEXT") || p.is("SPATIAL") {
        let kind = if p.is("FULLTEXT") {
            KeyKind::Fulltext
        } else {
            KeyKind::Spatial
        };
        p.i += 1;
        if !p.eat("KEY") {
            p.eat("INDEX");
        }
        raw.keys.push(key_body(p, kind, true)?);
        return Ok(());
    }
    if p.is("PERIOD") {
        return Err(refuse("PERIOD FOR"));
    }
    let c = column(p, cs)?;
    raw.cols.push(c);
    Ok(())
}

/// `(a, b)`.
fn name_list(p: &mut P<'_>) -> Result<Vec<String>, Fail> {
    p.want_op("(")?;
    let mut v = vec![p.ident()?];
    while p.eat_op(",") {
        v.push(p.ident()?);
    }
    p.want_op(")")?;
    Ok(v)
}

/// `[name] [USING x] (parts) [options]`.
pub(crate) fn key_body(p: &mut P<'_>, kind: KeyKind, named: bool) -> Result<Key, Fail> {
    let mut k = Key {
        kind,
        name: String::new(),
        parts: Vec::new(),
        using: None,
        comment: None,
        ignored: false,
    };
    if named && !p.op("(") && !p.is("USING") {
        k.name = p.ident()?;
    } else if !named && !p.op("(") && !p.is("USING") {
        // `PRIMARY KEY name (...)`: the name is ignored.
        p.ident()?;
    }
    if p.eat("USING") || p.eat("TYPE") {
        k.using = Some(index_type(p)?);
    }
    p.want_op("(")?;
    loop {
        if p.op("(") {
            return Err(refuse("functional key parts"));
        }
        let col = p.ident()?;
        let len = if p.eat_op("(") {
            let n = p.number()?;
            p.want_op(")")?;
            Some(u32::try_from(n).unwrap_or(u32::MAX))
        } else {
            None
        };
        let desc = if p.eat("DESC") {
            true
        } else {
            p.eat("ASC");
            false
        };
        k.parts.push(Part { col, len, desc });
        if p.eat_op(",") {
            continue;
        }
        p.want_op(")")?;
        break;
    }
    loop {
        if p.eat("USING") || p.eat("TYPE") {
            k.using = Some(index_type(p)?);
        } else if p.eat("COMMENT") {
            k.comment = Some(p.string()?);
        } else if p.eat("KEY_BLOCK_SIZE") {
            p.eat_op("=");
            p.number()?;
        } else if p.eat("IGNORED") {
            k.ignored = true;
        } else if p.eat("NOT") {
            p.want("IGNORED")?;
        } else if p.eat("VISIBLE") {
        } else if p.eat("INVISIBLE") {
            k.ignored = true;
        } else if p.is("WITH") && p.is_at(1, "PARSER") {
            return Err(refuse("full-text parsers"));
        } else if p.is("CLUSTERING") {
            return Err(refuse("CLUSTERING keys"));
        } else {
            break;
        }
    }
    Ok(k)
}

fn index_type(p: &mut P<'_>) -> Result<String, Fail> {
    if p.eat("BTREE") {
        Ok("BTREE".into())
    } else if p.eat("HASH") {
        Ok("HASH".into())
    } else if p.eat("RTREE") {
        Ok("RTREE".into())
    } else {
        Err(Fail::Syntax(p.i))
    }
}

/// A column definition: `name type attributes`.
#[allow(clippy::too_many_lines)] // one arm per attribute
pub(crate) fn column(p: &mut P<'_>, table_cs: &str) -> Result<RawCol, Fail> {
    let name = p.ident()?;
    let mut cs = CsSpec::default();
    let ty = types::read(p.t, &mut p.i, &name, &mut cs)?;
    let mut c = RawCol {
        col: Col {
            name,
            ty,
            not_null: false,
            null_written: false,
            default: None,
            on_update: None,
            auto_inc: false,
            comment: None,
            invisible: false,
            generated: None,
            check: None,
        },
        cs,
        primary: false,
        unique: false,
        null_said: None,
        place: None,
    };
    let charset = table_cs.to_owned();
    loop {
        if p.eat("NOT") {
            p.want("NULL")?;
            if c.col.generated.is_some() {
                return Err(Fail::Syntax(p.i - 2));
            }
            c.null_said = Some(false);
        } else if p.eat("NULL") {
            c.null_said = Some(true);
        } else if p.eat("DEFAULT") {
            if c.col.generated.is_some() {
                return Err(Fail::Syntax(p.i - 1));
            }
            c.col.default = Some(default_value(p, &charset)?);
        } else if p.eat("ON") {
            p.want("UPDATE")?;
            c.col.on_update = Some(now_fn(p)?.ok_or(Fail::Syntax(p.i))?);
        } else if p.eat("AUTO_INCREMENT") {
            c.col.auto_inc = true;
        } else if p.eat("PRIMARY") {
            p.want("KEY")?;
            c.primary = true;
        } else if p.eat("KEY") {
            c.primary = true;
        } else if p.eat("UNIQUE") {
            p.eat("KEY");
            c.unique = true;
        } else if p.eat("COMMENT") {
            c.col.comment = Some(p.string()?);
        } else if p.eat("INVISIBLE") {
            c.col.invisible = true;
        } else if p.eat("VISIBLE") {
        } else if p.eat("COLLATE") {
            c.cs.collate = Some(name_or_string(p)?);
        } else if p.is("CHARACTER") && p.is_at(1, "SET") {
            p.i += 2;
            c.cs.charset = Some(name_or_string(p)?);
        } else if p.eat("CHARSET") {
            c.cs.charset = Some(name_or_string(p)?);
        } else if p.eat("BINARY") {
            if c.col.ty.is_string() {
                c.cs.binary = true;
            } else {
                return Err(Fail::Syntax(p.i - 1));
            }
        } else if p.eat("ASCII") {
            c.cs.short = Some("latin1");
        } else if p.eat("UNICODE") {
            c.cs.short = Some("ucs2");
        } else if p.eat("BYTE") {
            c.cs.short = Some("binary");
        } else if p.eat("SERIAL") {
            p.want("DEFAULT")?;
            p.want("VALUE")?;
            c.col.not_null = true;
            c.null_said = Some(false);
            c.col.auto_inc = true;
            c.unique = true;
        } else if p.eat("GENERATED") {
            p.want("ALWAYS")?;
            p.want("AS")?;
            c.col.generated = Some((generated_expr(p, &charset)?, false));
            gen_kind(p, &mut c);
        } else if p.eat("AS") {
            c.col.generated = Some((generated_expr(p, &charset)?, false));
            gen_kind(p, &mut c);
        } else if p.eat("CHECK") {
            c.col.check = Some(p.paren_expr(&charset)?);
        } else if p.eat("CONSTRAINT") {
            // A named column check is a table constraint.
            p.ident()?;
            p.want("CHECK")?;
            c.col.check = Some(p.paren_expr(&charset)?);
        } else if p.eat("REFERENCES") {
            return Err(refuse("a foreign key written on its column"));
        } else if p.eat("COLUMN_FORMAT") || p.eat("STORAGE") {
            p.i += 1;
        } else if p.is("WITH") || p.is("WITHOUT") {
            return Err(refuse("system-versioned columns"));
        } else if p.eat("SRID") {
            p.number()?;
        } else {
            break;
        }
    }
    Ok(c)
}

fn gen_kind(p: &mut P<'_>, c: &mut RawCol) {
    if p.eat("STORED") || p.eat("PERSISTENT") {
        if let Some(g) = &mut c.col.generated {
            g.1 = true;
        }
    } else {
        p.eat("VIRTUAL");
    }
}

fn generated_expr(p: &mut P<'_>, cs: &str) -> Result<E, Fail> {
    p.want_op("(")?;
    let mut r = expr::Reader {
        t: p.t,
        i: p.i,
        charset: cs,
    };
    let e = r.expr().map_err(|b| match b {
        expr::Bad::Syntax(at) => Fail::Syntax(at),
        expr::Bad::Subquery | expr::Bad::Variable => err(
            1901,
            "HY000",
            "Function or expression 'select ...' cannot be used in the GENERATED ALWAYS AS clause",
        ),
    })?;
    p.i = r.i;
    p.want_op(")")?;
    Ok(expr::normalize(e))
}

fn name_or_string(p: &mut P<'_>) -> Result<String, Fail> {
    if let Some(T::Str(s)) = p.peek().map(|t| &t.t) {
        let s = s.clone();
        p.i += 1;
        return Ok(s);
    }
    p.eat_op("=");
    match p.peek().map(|t| &t.t) {
        Some(T::Word(w) | T::Quoted(w)) => {
            let w = w.clone();
            p.i += 1;
            Ok(w)
        }
        Some(T::Str(s)) => {
            let s = s.clone();
            p.i += 1;
            Ok(s)
        }
        _ => Err(Fail::Syntax(p.i)),
    }
}

/// `CURRENT_TIMESTAMP[(p)]`, `NOW([p])`, `LOCALTIME[(p)]`, ...: the
/// precision; `None` if not one of them.
fn now_fn(p: &mut P<'_>) -> Result<Option<u8>, Fail> {
    let is_now = p.is("CURRENT_TIMESTAMP") || p.is("LOCALTIME") || p.is("LOCALTIMESTAMP");
    let needs_parens = p.is("NOW");
    if !(is_now || needs_parens) {
        return Ok(None);
    }
    p.i += 1;
    let mut prec = 0;
    if p.eat_op("(") {
        if !p.op(")") {
            prec = u8::try_from(p.number()?).map_err(|_| Fail::Syntax(p.i))?;
        }
        p.want_op(")")?;
    } else if needs_parens {
        return Err(Fail::Syntax(p.i));
    }
    Ok(Some(prec))
}

/// What follows `DEFAULT`.
fn default_value(p: &mut P<'_>, cs: &str) -> Result<Default, Fail> {
    if let Some(prec) = now_fn(p)? {
        return Ok(Default::Now(prec));
    }
    if p.eat("NULL") {
        return Ok(Default::Null);
    }
    if p.op("(") {
        let e = p.paren_expr(cs)?;
        return Ok(match e {
            E::Null => Default::Null,
            E::Func(ref n, ref a) if n == "current_timestamp" && a.len() <= 1 => {
                let prec = match a.first() {
                    Some(E::Num(n)) => n.parse().unwrap_or(0),
                    _ => 0,
                };
                Default::Now(prec)
            }
            e if expr::is_literal(&e) => Default::Raw(e),
            e => Default::Expr(e),
        });
    }
    // A signed number, a string (with introducer), hex / bit, TRUE / FALSE.
    let neg = if p.eat_op("-") {
        true
    } else {
        p.eat_op("+");
        false
    };
    let t = p.peek().cloned().ok_or(Fail::Syntax(p.i))?;
    p.i += 1;
    let e = match t.t {
        T::Num(n) => E::Num(if neg { format!("-{n}") } else { n }),
        T::Str(s) if !neg => E::Str(s, None),
        T::Hex(h) if !neg => E::Hex(h),
        T::HexNum(h) if !neg => E::HexNum(h),
        T::Bin(b) if !neg => E::Bin(b),
        T::Word(w) if !neg && w.eq_ignore_ascii_case("TRUE") => E::Num("1".into()),
        T::Word(w) if !neg && w.eq_ignore_ascii_case("FALSE") => E::Num("0".into()),
        T::Word(w) if !neg && w.starts_with('_') => {
            let s = p.string()?;
            E::Str(s, Some(w[1..].to_ascii_lowercase()))
        }
        _ => return Err(Fail::Syntax(p.i - 1)),
    };
    Ok(Default::Raw(e))
}

/// Table options, as many as there are.
pub(crate) fn table_options(p: &mut P<'_>, o: &mut Opts) -> Result<(), Fail> {
    loop {
        p.eat_op(",");
        if p.eat("ENGINE") || p.eat("TYPE") {
            p.eat_op("=");
            o.engine = Some(name_or_string(p)?);
        } else if p.is("DEFAULT")
            && (p.is_at(1, "CHARSET") || p.is_at(1, "CHARACTER") || p.is_at(1, "COLLATE"))
        {
            p.i += 1;
        } else if p.eat("CHARSET") || (p.is("CHARACTER") && p.is_at(1, "SET")) {
            if p.is("CHARACTER") {
                p.i += 2;
            }
            o.charset = Some(name_or_string(p)?);
        } else if p.eat("COLLATE") {
            o.collate = Some(name_or_string(p)?);
        } else if p.eat("AUTO_INCREMENT") {
            p.eat_op("=");
            o.auto_increment = Some(p.number()?);
        } else if p.eat("COMMENT") {
            p.eat_op("=");
            o.comment = Some(p.string()?);
        } else if p.eat("ROW_FORMAT") {
            p.eat_op("=");
            let v = name_or_string(p)?.to_ascii_uppercase();
            o.row_format = Some(v);
        } else if p.eat("KEY_BLOCK_SIZE") {
            p.eat_op("=");
            o.key_block_size = Some(p.number()?);
        } else if p.eat("STATS_PERSISTENT") {
            p.eat_op("=");
            let v = match p.peek().map(|t| &t.t) {
                Some(T::Num(n)) => n.clone(),
                Some(T::Word(w)) if w.eq_ignore_ascii_case("DEFAULT") => "DEFAULT".into(),
                _ => return Err(Fail::Syntax(p.i)),
            };
            p.i += 1;
            o.stats_persistent = Some(v);
        } else if p.is("DATA")
            || p.is("INDEX") && p.is_at(1, "DIRECTORY")
            || p.is("ENCRYPTED")
            || p.is("PAGE_COMPRESSED")
            || p.is("MAX_ROWS")
            || p.is("MIN_ROWS")
            || p.is("AVG_ROW_LENGTH")
            || p.is("CHECKSUM")
            || p.is("PACK_KEYS")
            || p.is("DELAY_KEY_WRITE")
            || p.is("STATS_AUTO_RECALC")
            || p.is("STATS_SAMPLE_PAGES")
            || p.is("TRANSACTIONAL")
            || p.is("UNION")
            || p.is("INSERT_METHOD")
            || p.is("WITH")
            || p.is("TABLESPACE")
            || p.is("COMPRESSION")
            || p.is("SEQUENCE")
        {
            return Err(refuse("this table option"));
        } else {
            return Ok(());
        }
    }
}

/// Normalizes a definition as read (see the module docs).
pub(crate) fn finish(name: String, raw: Raw, cx: &Ctx) -> Result<Def, Fail> {
    let Raw {
        cols: rcols,
        keys: rkeys,
        fks,
        checks: rchecks,
        opts,
    } = raw;
    let cs = table_cs(&opts, &cx.db_cs)?;
    if let Some(e) = &opts.engine
        && !e.eq_ignore_ascii_case("InnoDB")
    {
        return Err(err(1286, "42000", format!("Unknown storage engine '{e}'")));
    }
    let mut def = Def {
        name,
        cols: Vec::new(),
        keys: Vec::new(),
        fks: Vec::new(),
        checks: Vec::new(),
        cs,
        auto_increment: opts.auto_increment,
        comment: opts.comment,
        row_format: opts.row_format,
        key_block_size: opts.key_block_size,
        stats_persistent: opts.stats_persistent,
    };
    let mut keys = Vec::new();
    for rc in rcols {
        add_column(&mut def, rc, &mut keys, None)?;
    }
    keys.extend(rkeys);
    for k in keys {
        add_key(&mut def, k)?;
    }
    for f in fks {
        add_fk(&mut def, f)?;
    }
    for (n, e) in rchecks {
        add_check(&mut def, n, e)?;
    }
    settle(&mut def, cx)?;
    Ok(def)
}

/// The table's character set and collation.
fn table_cs(o: &Opts, db: &(String, String)) -> Result<(String, String), Fail> {
    let spec = CsSpec {
        charset: o.charset.clone(),
        collate: o.collate.clone(),
        ..CsSpec::default()
    };
    if spec.charset.is_none() && spec.collate.is_none() {
        return Ok(db.clone());
    }
    let (cs, co) = types::resolve_cs(&spec, (&db.0, &db.1))?;
    Ok((cs, co))
}

/// Adds a column read from a definition (at `place` for `ALTER`), with
/// the keys it declares on itself.
#[allow(clippy::too_many_lines, clippy::option_option)] // the column rules in order; no place, `FIRST`, or `AFTER x`
pub(crate) fn add_column(
    def: &mut Def,
    rc: RawCol,
    keys: &mut Vec<Key>,
    place: Option<Option<&str>>,
) -> Result<(), Fail> {
    let RawCol {
        mut col,
        cs,
        primary,
        unique,
        null_said,
        ..
    } = rc;
    if def.col(&col.name).is_some() {
        return Err(err(
            1060,
            "42S21",
            format!("Duplicate column name '{}'", col.name),
        ));
    }
    if col.name.is_empty() {
        return Err(err(1166, "42000", "Incorrect column name ''"));
    }
    if col.ty.is_string() || col.ty.json {
        let resolved = if col.ty.json {
            ("utf8mb4".to_owned(), "utf8mb4_bin".to_owned())
        } else {
            types::resolve_cs(&cs, (&def.cs.0, &def.cs.1))?
        };
        // `CHARACTER SET binary` makes byte types.
        if resolved.0 == "binary" {
            col.ty.base = match col.ty.base.as_str() {
                "char" => "binary".into(),
                "varchar" => "varbinary".into(),
                "tinytext" => "tinyblob".into(),
                "text" => "blob".into(),
                "mediumtext" => "mediumblob".into(),
                "longtext" => "longblob".into(),
                b => b.to_owned(),
            };
            col.ty.cs = None;
        } else {
            col.ty.cs = Some(resolved);
        }
        if col.ty.base == "varchar" {
            let max = 65532 / col.ty.mbmax().max(1);
            if col.ty.len > max {
                return Err(err(
                    1074,
                    "42000",
                    format!(
                        "Column length too big for column '{}' (max = {max}); use BLOB or TEXT instead",
                        col.name
                    ),
                ));
            }
        }
        // Members of an ENUM / SET may not repeat (compared in the
        // column's collation: case-insensitive for `_ci` ones).
        if matches!(col.ty.base.as_str(), "enum" | "set") {
            let ci = col.ty.cs.as_ref().is_some_and(|(_, co)| co.contains("_ci"));
            for (k, m) in col.ty.members.iter().enumerate() {
                let dup = col.ty.members[..k].iter().find(|x| {
                    if ci {
                        x.eq_ignore_ascii_case(m)
                    } else {
                        *x == m
                    }
                });
                if let Some(m) = dup {
                    let what = if col.ty.base == "enum" { "ENUM" } else { "SET" };
                    let (code, state) = if col.ty.base == "enum" {
                        (1291, "HY000")
                    } else {
                        (1097, "HY000")
                    };
                    return Err(err(
                        code,
                        state,
                        format!("Column '{}' has duplicated value '{m}' in {what}", col.name),
                    ));
                }
            }
        }
    }
    if col.ty.serial {
        col.not_null = true;
        col.auto_inc = true;
        keys.push(Key {
            kind: KeyKind::Unique,
            name: String::new(),
            parts: vec![Part {
                col: col.name.clone(),
                len: None,
                desc: false,
            }],
            using: None,
            comment: None,
            ignored: false,
        });
    }
    match null_said {
        Some(false) => col.not_null = true,
        Some(true) => {
            col.null_written = true;
            col.not_null = false;
        }
        None => {}
    }
    if col.ty.json {
        col.check = Some(match col.check.take() {
            None => E::Func("json_valid".into(), vec![E::Col(col.name.clone())]),
            Some(c) => c,
        });
    }
    if primary && matches!(col.default, Some(Default::Null)) {
        return Err(err(
            1067,
            "42000",
            format!("Invalid default value for '{}'", col.name),
        ));
    }
    if primary {
        keys.push(Key {
            kind: KeyKind::Primary,
            name: "PRIMARY".into(),
            parts: vec![Part {
                col: col.name.clone(),
                len: None,
                desc: false,
            }],
            using: None,
            comment: None,
            ignored: false,
        });
    }
    if unique {
        keys.push(Key {
            kind: KeyKind::Unique,
            name: String::new(),
            parts: vec![Part {
                col: col.name.clone(),
                len: None,
                desc: false,
            }],
            using: None,
            comment: None,
            ignored: false,
        });
    }
    let at = match place {
        None => def.cols.len(),
        Some(None) => 0,
        Some(Some(after)) => def.col(after).map(|k| k + 1).ok_or_else(|| {
            err(
                1054,
                "42S22",
                format!("Unknown column '{after}' in '{}'", def.name),
            )
        })?,
    };
    def.cols.insert(at, col);
    Ok(())
}

/// Adds a key, naming it if unnamed.
pub(crate) fn add_key(def: &mut Def, mut k: Key) -> Result<(), Fail> {
    if k.kind == KeyKind::Primary && def.keys.iter().any(|x| x.kind == KeyKind::Primary) {
        return Err(err(1068, "42000", "Multiple primary key defined"));
    }
    for part in &mut k.parts {
        let Some(ci) = def.col(&part.col) else {
            return Err(err(
                1072,
                "42000",
                format!("Key column '{}' doesn't exist in table", part.col),
            ));
        };
        part.col.clone_from(&def.cols[ci].name);
    }
    if k.kind == KeyKind::Primary {
        k.name = "PRIMARY".into();
        for part in &k.parts {
            if let Some(ci) = def.col(&part.col) {
                let c = &mut def.cols[ci];
                if c.generated.as_ref().is_some_and(|g| !g.1) {
                    return Err(err(
                        1903,
                        "HY000",
                        "Primary key cannot be defined upon a generated column",
                    ));
                }
                c.not_null = true;
                // A key column's `DEFAULT NULL` goes with its NULLs.
                if matches!(c.default, Some(Default::Null)) {
                    c.default = None;
                }
            }
        }
    } else if k.name.is_empty() {
        let base = k.parts[0].col.clone();
        let mut name = base.clone();
        let mut n = 2;
        while def.key(&name).is_some() || name.eq_ignore_ascii_case("PRIMARY") {
            name = format!("{base}_{n}");
            n += 1;
        }
        k.name = name;
    } else if k.name.eq_ignore_ascii_case("PRIMARY") {
        return Err(err(
            1280,
            "42000",
            format!("Incorrect index name '{}'", k.name),
        ));
    } else if def.key(&k.name).is_some() {
        return Err(err(
            1061,
            "42000",
            format!("Duplicate key name '{}'", k.name),
        ));
    }
    def.keys.push(k);
    Ok(())
}

/// Adds a foreign key, naming it and giving it an index.
pub(crate) fn add_fk(def: &mut Def, mut f: Fk) -> Result<(), Fail> {
    for c in &mut f.cols {
        let Some(ci) = def.col(c) else {
            return Err(err(
                1072,
                "42000",
                format!("Key column '{c}' doesn't exist in table"),
            ));
        };
        c.clone_from(&def.cols[ci].name);
    }
    if f.cols.len() != f.ref_cols.len() {
        return Err(err(
            1239,
            "42000",
            format!(
                "Incorrect foreign key definition for '{}': Key reference and table reference don't match",
                if f.name.is_empty() {
                    "foreign key without name"
                } else {
                    &f.name
                }
            ),
        ));
    }
    let explicit = !f.name.is_empty();
    if !explicit {
        let mut n = 1;
        loop {
            let cand = format!("{}_ibfk_{n}", def.name);
            if !def.fks.iter().any(|x| x.name.eq_ignore_ascii_case(&cand)) {
                f.name = cand;
                break;
            }
            n += 1;
        }
    } else if def.fks.iter().any(|x| x.name.eq_ignore_ascii_case(&f.name)) {
        return Err(err(
            1826,
            "HY000",
            format!("Duplicate FOREIGN KEY constraint name '{}'", f.name),
        ));
    }
    // An index whose first columns are the foreign key's.
    let covered = def.keys.iter().any(|k| {
        k.kind != KeyKind::Fulltext
            && k.parts.len() >= f.cols.len()
            && k.parts
                .iter()
                .zip(&f.cols)
                .all(|(p, c)| p.col.eq_ignore_ascii_case(c) && p.len.is_none())
    });
    // An index made for another foreign key on the same columns (named
    // after it) is made again for the new key, under its name, as both
    // servers do.
    if covered && explicit && def.key(&f.name).is_none() {
        let made_for_fk = def.keys.iter().position(|k| {
            k.kind == KeyKind::Plain
                && k.parts.len() == f.cols.len()
                && k.parts
                    .iter()
                    .zip(&f.cols)
                    .all(|(p, c)| p.col.eq_ignore_ascii_case(c) && p.len.is_none())
                && def.fks.iter().any(|o| {
                    o.name.eq_ignore_ascii_case(&k.name)
                        && o.cols.len() == k.parts.len()
                        && o.cols
                            .iter()
                            .zip(&k.parts)
                            .all(|(c, p)| c.eq_ignore_ascii_case(&p.col))
                })
        });
        if let Some(i) = made_for_fk {
            def.keys[i].name.clone_from(&f.name);
        }
    }
    if !covered {
        let k = Key {
            kind: KeyKind::Plain,
            name: if explicit && def.key(&f.name).is_none() {
                f.name.clone()
            } else {
                String::new()
            },
            parts: f
                .cols
                .iter()
                .map(|c| Part {
                    col: c.clone(),
                    len: None,
                    desc: false,
                })
                .collect(),
            using: None,
            comment: None,
            ignored: false,
        };
        add_key(def, k)?;
    }
    def.fks.push(f);
    Ok(())
}

/// Adds a table check, naming it if unnamed.
pub(crate) fn add_check(def: &mut Def, name: Option<String>, e: E) -> Result<(), Fail> {
    let name = if let Some(n) = name {
        if def.checks.iter().any(|c| c.name.eq_ignore_ascii_case(&n)) {
            return Err(err(
                1826,
                "HY000",
                format!("Duplicate CHECK constraint name '{n}'"),
            ));
        }
        n
    } else {
        let mut k = 1;
        loop {
            let cand = format!("CONSTRAINT_{k}");
            if !def
                .checks
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&cand))
            {
                break cand;
            }
            k += 1;
        }
    };
    let mut used = Vec::new();
    expr::columns(&e, &mut used);
    for u in &used {
        if def.col(u).is_none() {
            return Err(err(
                1054,
                "42S22",
                format!("Unknown column '{u}' in 'CHECK'"),
            ));
        }
    }
    def.checks.push(Check { name, expr: e });
    Ok(())
}

/// The checks every finished definition passes, and the server's
/// rewrites that depend on the whole table: key prefixes, the order of
/// keys, `AUTO_INCREMENT` rules, defaults in each column's form.
#[allow(clippy::too_many_lines)] // the rules in the order the servers apply them
pub(crate) fn settle(def: &mut Def, cx: &Ctx) -> Result<(), Fail> {
    if def.cols.iter().all(|c| c.invisible) {
        return Err(if cx.mysql {
            err(
                4028,
                "HY000",
                "A table must have at least one visible column.",
            )
        } else {
            err(1113, "42000", "A table must have at least 1 column")
        });
    }
    // Keys: prefixes and lengths.
    for k in &mut def.keys {
        for part in &mut k.parts {
            let Some(ci) = def
                .cols
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&part.col))
            else {
                return Err(err(
                    1072,
                    "42000",
                    format!("Key column '{}' doesn't exist in table", part.col),
                ));
            };
            let c = &def.cols[ci];
            if let Some(len) = part.len {
                let full = if c.ty.is_lob() { u32::MAX } else { c.ty.len };
                if !c.ty.prefixable() || len > full || len == 0 {
                    return Err(err(
                        1089,
                        "HY000",
                        "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys",
                    ));
                }
                if len == c.ty.len && !c.ty.is_lob() {
                    // A prefix as long as the column is the whole column.
                    part.len = None;
                }
            } else if k.kind != KeyKind::Fulltext
                && (c.ty.is_lob() || c.ty.len.saturating_mul(c.ty.mbmax()) > 3072)
                && c.ty.prefixable()
            {
                if k.kind == KeyKind::Plain {
                    part.len = Some(3072 / c.ty.mbmax().max(1));
                } else if c.ty.is_lob() && k.kind != KeyKind::Primary {
                    return Err(refuse("a unique key on a whole BLOB or TEXT column"));
                } else if c.ty.is_lob() {
                    return Err(err(
                        1170,
                        "42000",
                        format!(
                            "BLOB/TEXT column '{}' used in key specification without a key length",
                            c.name
                        ),
                    ));
                } else {
                    return Err(err(
                        1071,
                        "42000",
                        "Specified key was too long; max key length is 3072 bytes",
                    ));
                }
            }
        }
        if k.kind == KeyKind::Fulltext || k.kind == KeyKind::Spatial {
            return Err(refuse("FULLTEXT and SPATIAL keys"));
        }
    }
    // AUTO_INCREMENT: one column, numeric, first in some key, NOT NULL.
    let autos: Vec<usize> = (0..def.cols.len())
        .filter(|&i| def.cols[i].auto_inc)
        .collect();
    for &a in &autos {
        let c = &def.cols[a];
        if !c.ty.is_numeric() {
            return Err(err(
                1063,
                "42000",
                format!("Incorrect column specifier for column '{}'", c.name),
            ));
        }
        if c.default
            .as_ref()
            .is_some_and(|d| !matches!(d, Default::Null))
        {
            return Err(err(
                1067,
                "42000",
                format!("Invalid default value for '{}'", c.name),
            ));
        }
    }
    if autos.len() > 1
        || autos.first().is_some_and(|&a| {
            !def.keys.iter().any(|k| {
                k.parts
                    .first()
                    .is_some_and(|p| p.col.eq_ignore_ascii_case(&def.cols[a].name))
            })
        })
    {
        return Err(err(
            1075,
            "42000",
            "Incorrect table definition; there can be only one auto column and it must be defined as a key",
        ));
    }
    for &a in &autos {
        def.cols[a].not_null = true;
        def.cols[a].default = None;
    }
    // Generated columns: the expressions read existing columns.
    for c in &def.cols {
        if let Some((g, _)) = &c.generated {
            let mut used = Vec::new();
            expr::columns(g, &mut used);
            for u in used {
                if def.col(&u).is_none() {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{u}' in 'GENERATED ALWAYS AS'"),
                    ));
                }
            }
        }
        if let Some(ch) = &c.check {
            let mut used = Vec::new();
            expr::columns(ch, &mut used);
            for u in used {
                if def.col(&u).is_none() {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{u}' in 'CHECK'"),
                    ));
                }
            }
        }
        if let Some(Default::Expr(e)) = &c.default {
            let mut used = Vec::new();
            expr::columns(e, &mut used);
            for u in used {
                if def.col(&u).is_none() {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{u}' in 'DEFAULT'"),
                    ));
                }
            }
        }
    }
    // Defaults in the column's form.
    for i in 0..def.cols.len() {
        normalize_default(def, i, cx)?;
    }
    sort_keys(def);
    Ok(())
}

/// The order the servers list keys in.
fn sort_keys(def: &mut Def) {
    let nullable = |def: &Def, k: &Key| {
        k.parts
            .iter()
            .any(|p| def.col(&p.col).is_some_and(|ci| !def.cols[ci].not_null))
    };
    let rank = |def: &Def, k: &Key| -> u8 {
        match k.kind {
            KeyKind::Primary => 0,
            KeyKind::Unique => {
                if nullable(def, k) {
                    3
                } else if k.parts.iter().any(|p| p.len.is_some()) {
                    2
                } else {
                    1
                }
            }
            KeyKind::Plain => 4,
            KeyKind::Spatial => 5,
            KeyKind::Fulltext => 6,
        }
    };
    let ranks: Vec<u8> = def.keys.iter().map(|k| rank(def, k)).collect();
    let mut idx: Vec<usize> = (0..def.keys.len()).collect();
    idx.sort_by_key(|&i| ranks[i]);
    let keys = std::mem::take(&mut def.keys);
    let mut slots: Vec<Option<Key>> = keys.into_iter().map(Some).collect();
    def.keys = idx.into_iter().filter_map(|i| slots[i].take()).collect();
}

/// Puts column `i`'s `DEFAULT` in the column's own form (1067 when it
/// can't hold it).
fn normalize_default(def: &mut Def, i: usize, cx: &Ctx) -> Result<(), Fail> {
    let c = &def.cols[i];
    let invalid = || {
        err(
            1067,
            "42000",
            format!("Invalid default value for '{}'", c.name),
        )
    };
    match &c.default {
        Some(Default::Null) if c.not_null => return Err(invalid()),
        Some(Default::Null) if c.ty.base == "timestamp" => {}
        Some(Default::Now(p)) => {
            if !matches!(c.ty.base.as_str(), "datetime" | "timestamp") {
                return Err(invalid());
            }
            let fsp: u8 =
                c.ty.args
                    .trim_matches(|ch| ch == '(' || ch == ')')
                    .parse()
                    .unwrap_or(0);
            if *p > fsp {
                return Err(invalid());
            }
        }
        Some(Default::Raw(e)) => {
            if c.ty.is_lob() && cx.mysql {
                return Err(err(
                    1101,
                    "42000",
                    format!(
                        "BLOB, TEXT, GEOMETRY or JSON column '{}' can't have a default value",
                        c.name
                    ),
                ));
            }
            let lit = literal_default(def, i, e).ok_or_else(invalid)?;
            def.cols[i].default = Some(Default::Lit(lit));
        }
        _ => {}
    }
    if let Some(p) = def.cols[i].on_update {
        let c = &def.cols[i];
        if !matches!(c.ty.base.as_str(), "datetime" | "timestamp") {
            return Err(err(
                1294,
                "HY000",
                format!("Invalid ON UPDATE clause for '{}' column", c.name),
            ));
        }
        let _ = p;
    }
    Ok(())
}

/// A constant `DEFAULT` printed in the column's form, through the
/// engine's own conversion of the value into the column; `None` when the
/// column can't hold it.
fn literal_default(def: &Def, i: usize, e: &E) -> Option<String> {
    use super::row::Datum;
    let c = &def.cols[i];
    let d = match e {
        E::Null => return Some("NULL".into()),
        E::Num(n) => {
            if n.contains(['e', 'E']) {
                Datum::Double(n.parse().ok()?)
            } else if n.contains('.') {
                Datum::Decimal(sprig_engine::row::Decimal::parse(n)?)
            } else if let Ok(v) = n.parse::<i64>() {
                Datum::Int(v)
            } else {
                Datum::UInt(n.parse().ok()?)
            }
        }
        E::Str(s, _) => Datum::Str(bytes::Bytes::from(s.clone().into_bytes())),
        E::Hex(h) | E::HexNum(h) => Datum::Bytes(bytes::Bytes::from(hex_bytes(h)?)),
        E::Bin(b) => Datum::Bytes(bytes::Bytes::from(bin_bytes(b))),
        _ => return None,
    };
    // A one-column table of this column, read by the engine's parser.
    let mut one = def.clone();
    // The engine serves tables with a primary key: give the probe one.
    let mut key_col = c.clone();
    key_col.name = if c.name == "k" {
        "kk".into()
    } else {
        "k".into()
    };
    key_col.ty = types_int();
    key_col.not_null = true;
    key_col.default = None;
    key_col.check = None;
    key_col.generated = None;
    key_col.comment = None;
    key_col.invisible = false;
    one.cols = vec![
        key_col.clone(),
        Col {
            default: None,
            check: None,
            generated: None,
            auto_inc: false,
            not_null: false,
            ..c.clone()
        },
    ];
    one.keys.clear();
    one.keys.push(Key {
        kind: KeyKind::Primary,
        name: "PRIMARY".into(),
        parts: vec![Part {
            col: key_col.name.clone(),
            len: None,
            desc: false,
        }],
        using: None,
        comment: None,
        ignored: false,
    });
    one.fks.clear();
    one.checks.clear();
    let text = render(&one);
    let parsed = super::table::parse(
        sprig_common::ids::ObjectId(0),
        1,
        sprig_common::ids::DbId(0),
        &text,
    );
    // A type the engine doesn't serve yet: the constant as written.
    let Ok(td) = parsed else {
        return Some(expr::print(e));
    };
    let col = td.columns.get(1)?;
    let v = super::assign::constant(col, d)?;
    Some(print_value(c, col, &v))
}

fn hex_bytes(h: &str) -> Option<Vec<u8>> {
    let h = if h.len() % 2 == 1 {
        format!("0{h}")
    } else {
        h.to_owned()
    };
    (0..h.len())
        .step_by(2)
        .map(|k| u8::from_str_radix(&h[k..k + 2], 16).ok())
        .collect()
}

fn bin_bytes(b: &str) -> Vec<u8> {
    let mut v: u128 = 0;
    for ch in b.chars() {
        v = v.wrapping_mul(2).wrapping_add(u128::from(ch == '1'));
    }
    let n = b.len().div_ceil(8).max(1);
    (0..n)
        .rev()
        .map(|k| ((v >> (8 * k)) & 0xff) as u8)
        .collect()
}

/// A stored default value as `SHOW CREATE TABLE` prints it.
fn print_value(c: &Col, col: &super::table::Column, v: &super::row::Datum) -> String {
    use super::row::Datum;
    match v {
        Datum::Null => "NULL".into(),
        Datum::Bit(b) => {
            let bits = format!("{b:b}");
            format!("b'{bits}'")
        }
        _ => {
            let text = super::meta::text(col, v).unwrap_or_default();
            let s = String::from_utf8_lossy(&text).into_owned();
            let numeric = c.ty.is_numeric() || c.ty.base == "year";
            if numeric { s } else { quote_str(&s) }
        }
    }
}

/// The definition as MariaDB 11.8's `SHOW CREATE TABLE` prints it.
pub(crate) fn render(def: &Def) -> String {
    let mut s = format!("CREATE TABLE {} (\n", quote_name(&def.name));
    let mut lines: Vec<String> = Vec::new();
    for c in &def.cols {
        lines.push(format!("  {}", column_text(def, c)));
    }
    for k in &def.keys {
        lines.push(format!("  {}", key_text(k)));
    }
    for f in &def.fks {
        let mut l = format!(
            "  CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}{} ({})",
            quote_name(&f.name),
            f.cols
                .iter()
                .map(|c| quote_name(c))
                .collect::<Vec<_>>()
                .join(", "),
            f.ref_db
                .as_ref()
                .map(|d| format!("{}.", quote_name(d)))
                .unwrap_or_default(),
            quote_name(&f.ref_table),
            f.ref_cols
                .iter()
                .map(|c| quote_name(c))
                .collect::<Vec<_>>()
                .join(", "),
        );
        if let Some(d) = &f.on_delete {
            _ = write!(l, " ON DELETE {d}");
        }
        if let Some(u) = &f.on_update {
            _ = write!(l, " ON UPDATE {u}");
        }
        lines.push(l);
    }
    for ch in &def.checks {
        lines.push(format!(
            "  CONSTRAINT {} CHECK ({})",
            quote_name(&ch.name),
            expr::print(&ch.expr)
        ));
    }
    s.push_str(&lines.join(",\n"));
    s.push_str("\n) ENGINE=InnoDB");
    if let Some(a) = def.auto_increment
        && a > 1
        && def.cols.iter().any(|c| c.auto_inc)
    {
        _ = write!(s, " AUTO_INCREMENT={a}");
    }
    _ = write!(s, " DEFAULT CHARSET={} COLLATE={}", def.cs.0, def.cs.1);
    if let Some(sp) = &def.stats_persistent {
        _ = write!(s, " STATS_PERSISTENT={sp}");
    }
    if let Some(r) = &def.row_format {
        _ = write!(s, " ROW_FORMAT={r}");
    }
    if let Some(k) = def.key_block_size {
        _ = write!(s, " KEY_BLOCK_SIZE={k}");
    }
    if let Some(c) = &def.comment {
        _ = write!(s, " COMMENT={}", quote_str(c).replace("\\'", "''"));
    }
    s
}

fn column_text(def: &Def, c: &Col) -> String {
    let mut s = format!("{} {}", quote_name(&c.name), c.ty.print());
    if let Some((cs, co)) = &c.ty.cs
        && (cs != &def.cs.0 || co != &def.cs.1)
    {
        _ = write!(s, " CHARACTER SET {cs} COLLATE {co}");
    }
    if let Some((g, stored)) = &c.generated {
        _ = write!(
            s,
            " GENERATED ALWAYS AS ({}) {}",
            expr::print(g),
            if *stored { "STORED" } else { "VIRTUAL" }
        );
    } else {
        if c.not_null {
            s.push_str(" NOT NULL");
        } else if c.ty.base == "timestamp" {
            s.push_str(" NULL");
        }
        match &c.default {
            Some(Default::Null) | None if !c.not_null && !c.auto_inc => s.push_str(" DEFAULT NULL"),
            Some(Default::Lit(l)) => _ = write!(s, " DEFAULT {l}"),
            Some(Default::Raw(e)) => _ = write!(s, " DEFAULT {}", expr::print(e)),
            Some(Default::Now(p)) => _ = write!(s, " DEFAULT {}", now_text(*p)),
            Some(Default::Expr(e)) => {
                if expr::bare_default(e) {
                    _ = write!(s, " DEFAULT {}", expr::print(e));
                } else {
                    _ = write!(s, " DEFAULT ({})", expr::print(e));
                }
            }
            _ => {}
        }
        if let Some(p) = c.on_update {
            _ = write!(s, " ON UPDATE {}", now_text(p));
        }
        if c.auto_inc {
            s.push_str(" AUTO_INCREMENT");
        }
    }
    if c.invisible {
        s.push_str(" INVISIBLE");
    }
    if let Some(cm) = &c.comment {
        _ = write!(s, " COMMENT {}", quote_str(cm).replace("\\'", "''"));
    }
    if let Some(ch) = &c.check {
        _ = write!(s, " CHECK ({})", expr::print(ch));
    }
    s
}

fn now_text(p: u8) -> String {
    if p == 0 {
        "current_timestamp()".into()
    } else {
        format!("current_timestamp({p})")
    }
}

fn key_text(k: &Key) -> String {
    let parts = k
        .parts
        .iter()
        .map(|p| {
            let mut s = quote_name(&p.col);
            if let Some(l) = p.len {
                _ = write!(s, "({l})");
            }
            if p.desc {
                s.push_str(" DESC");
            }
            s
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut s = match k.kind {
        KeyKind::Primary => format!("PRIMARY KEY ({parts})"),
        KeyKind::Unique => format!("UNIQUE KEY {} ({parts})", quote_name(&k.name)),
        KeyKind::Plain => format!("KEY {} ({parts})", quote_name(&k.name)),
        KeyKind::Fulltext => format!("FULLTEXT KEY {} ({parts})", quote_name(&k.name)),
        KeyKind::Spatial => format!("SPATIAL KEY {} ({parts})", quote_name(&k.name)),
    };
    if let Some(u) = &k.using {
        _ = write!(s, " USING {u}");
    }
    if let Some(c) = &k.comment {
        _ = write!(s, " COMMENT {}", quote_str(c).replace("\\'", "''"));
    }
    if k.ignored {
        s.push_str(" IGNORED");
    }
    s
}

/// Reads a stored definition (the engine's own text).
///
/// # Errors
/// The text doesn't read (a bug: the engine printed it).
pub(crate) fn parse_stored(text: &str) -> Result<Def, SprigError> {
    let cx = Ctx {
        mysql: false,
        db_cs: ("utf8mb4".into(), "utf8mb4_uca1400_ai_ci".into()),
    };
    match create(text, &cx)? {
        Created::Def(d) => Ok(*d),
        Created::Like(..) => Err(SprigError::internal(
            "engine: a stored definition reads as LIKE",
        )),
    }
}

/// `CREATE TABLE new LIKE src`: the source's definition under the new
/// name (foreign keys aren't copied, as on the servers).
pub(crate) fn like(src: &Def, name: &str) -> Def {
    let mut d = src.clone();
    name.clone_into(&mut d.name);
    d.fks.clear();
    d
}

/// Applies an `ALTER TABLE` (or `CREATE INDEX` / `DROP INDEX`) statement to
/// `def`.
///
/// # Errors
/// 1064 and the definition errors.
pub(crate) fn alter(def: &Def, sql: &str, cx: &Ctx) -> Result<Def, SprigError> {
    let toks = tokens(sql, cx.mysql)?;
    alter::apply(def, &toks, cx).map_err(|f| to_error(f, sql, &toks, cx.mysql))
}

/// The definition renamed (`RENAME TABLE`).
pub(crate) fn renamed(def: &Def, name: &str) -> Def {
    let mut d = def.clone();
    name.clone_into(&mut d.name);
    d
}

/// `int(11) NOT NULL`, for probe tables.
fn types_int() -> Ty {
    Ty {
        base: "int".into(),
        args: "(11)".into(),
        unsigned: false,
        zerofill: false,
        cs: None,
        json: false,
        serial: false,
        members: Vec::new(),
        len: 11,
    }
}
