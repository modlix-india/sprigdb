//! SQL writes to MongoDB collections: `INSERT`, `INSERT ... SELECT`,
//! `REPLACE`, `UPDATE` and `DELETE` naming a collection read as a table
//! ([`super::collection`]) run through the document store's own write path
//! (`sprig_doc::docstore`), inside the SQL statement's engine transaction.
//!
//! ```text
//! 1. SELECT the rows: an INSERT's values (one SELECT per VALUES list,
//!    joined with UNION ALL) or its query; an UPDATE's or DELETE's target
//!    rows (`_id`, `doc`, `_types`) with the new values worked out from
//!    them, through the planner, the target's documents locked
//! 2. per row: the new document from `doc` and `_types`
//!    (`sprig_doc::json::to_bson`), then the store's insert, replace or
//!    delete in the same engine transaction: `_id` key, index entries,
//!    unique checks, validator, change-log note
//! ```
//!
//! - **One transaction.** The statement's writes join the SQL transaction,
//!   so a table and a collection written in one transaction commit
//!   together; a failed statement undoes its document writes too. The
//!   transaction holds the store's DDL lock shared from its first such
//!   write until it ends, so no index build runs between a write and its
//!   commit (waiting for it at most `lock_wait_timeout`, then 1205).
//! - **Types are kept.** The new `doc` is read back with its `_types` map,
//!   the row's own unless the statement sets `_types`: a field whose text
//!   an `UPDATE` leaves alone keeps its BSON type.
//! - **`_id`.** Taken from `doc`, else from the `_id` column (as a string),
//!   else a new `ObjectId`. `_id` and `doc._id` must agree (39667); an
//!   `UPDATE` can't change it (39668); a new `doc` without `_id` keeps the
//!   old one, as a MongoDB replacement does.
//! - **Errors.** A duplicate `_id` or unique key is 1062 (`INSERT IGNORE`:
//!   a warning, the row skipped); a document the validator or the store
//!   refuses is 39667; `ON DUPLICATE KEY UPDATE`, a write of a table and a
//!   collection in one statement, and the other forms not built answer
//!   39960. `REPLACE` replaces the document with the same `_id`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bson::{Bson, Document};
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_doc::docstore::{Coll, DocTxn, Name};
use sprig_engine::observe::hint::HintOp;
use sprig_engine::txn::{LockMode, Wait};
use sprig_proto_mongo::errors::{MongoError, code};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex};
use sqlparser::ast::{FromTable, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject};
use sqlparser::parser::Parser;

use super::assign::{Condition, Level};
use super::collection::{CollTable, id_text};
use super::exec::Outcome;
use super::general::{General, RunArgs};
use super::plan::{self, BindCtx};
use super::row::Datum;
use super::session::SessionTxn;
use super::store::Tx;
use super::{Engine, not_on_engine};

/// The columns a collection reads as, by position.
const COLUMNS: [&str; 3] = ["_id", "doc", "_types"];

/// How long a write waits for the document store's DDL lock (an index
/// build) when the session sets no lock wait.
const GUARD_WAIT: Duration = Duration::from_secs(50);

/// What the write does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// `INSERT` (`replace`: `REPLACE`).
    Insert { replace: bool },
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
}

/// A bound SQL write of a collection.
#[derive(Debug)]
pub(crate) struct CollWrite {
    /// What it does.
    pub kind: Kind,
    /// `INSERT IGNORE` / `UPDATE IGNORE` / `DELETE IGNORE`.
    pub ignore: bool,
    /// The collection written.
    pub coll: Arc<CollTable>,
    /// Its SQL database.
    pub db: DbId,
    /// The `SELECT` that gives the rows.
    pub select: General,
    /// Where the new value of `_id`, `doc` and `_types` is in a row
    /// (`None`: not given; an `UPDATE` keeps the old one). An `UPDATE`'s
    /// or `DELETE`'s rows start with the old `_id`, `doc` and `_types`.
    pub new_at: [Option<usize>; 3],
    /// The number of values each row of an `INSERT ... SELECT` must have.
    pub width: usize,
    /// The OK packet says `Records: ...` (several rows, or a query).
    pub records_info: bool,
    /// `INSERT ... ON DUPLICATE KEY UPDATE`: what a conflicting document
    /// gets.
    pub odku: Option<Odku>,
    /// Tables the same `UPDATE` or `DELETE` writes (beside the collection).
    pub tables: Vec<TableTarget>,
}

/// `ON DUPLICATE KEY UPDATE` of an `INSERT` into a collection.
#[derive(Debug)]
pub(crate) struct Odku {
    /// The conflicting document's row and its new values; its parameters:
    /// the new row's columns `params`, then the conflicting `_id`.
    pub select: General,
    /// Where each new value is in its rows (as [`CollWrite::new_at`]).
    pub new_at: [Option<usize>; 3],
    /// The new row's column (by position in `COLUMNS`) each `VALUES(col)`
    /// parameter stands for, in the text's order.
    pub params: Vec<usize>,
    /// The client wrote `VALUES(col)` (MySQL deprecates it: 1287).
    pub values_fn: bool,
}

/// A table written by the same statement as a collection.
#[derive(Debug)]
pub(crate) struct TableTarget {
    /// The write of one row: its parameters are `values`, then the key.
    pub plan: plan::Plan,
    /// Where the row's primary key is in the `SELECT`'s rows.
    pub keys: std::ops::Range<usize>,
    /// Where each value parameter is in the `SELECT`'s rows.
    pub values: Vec<usize>,
}

impl CollWrite {
    /// The privilege the write needs on the collection's database.
    pub(crate) fn actions(&self) -> &'static [sprig_auth::authz::Action] {
        use sprig_auth::authz::Action;
        match self.kind {
            Kind::Insert { replace: false } => &[Action::Insert],
            Kind::Insert { replace: true } => &[Action::Insert, Action::Delete],
            Kind::Update => &[Action::Update],
            Kind::Delete => &[Action::Delete],
        }
    }
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn unknown_column(name: &str) -> SprigError {
    SprigError::standard(
        1054,
        *b"42S22",
        format!("Unknown column '{name}' in 'field list'"),
    )
}

fn count_mismatch(row: usize) -> SprigError {
    SprigError::standard(
        1136,
        *b"21S01",
        format!("Column count doesn't match value count at row {row}"),
    )
}

fn invalid(msg: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::DocInvalid,
        format!("Not a valid document: {msg}"),
    )
}

fn id_changed() -> SprigError {
    SprigError::new(
        SprigCode::DocIdImmutable,
        "A document's _id can't change: the write would change _id",
    )
}

/// The position of column `name` of a collection.
fn column_of(name: &str) -> Option<usize> {
    COLUMNS.iter().position(|c| c.eq_ignore_ascii_case(name))
}

fn ident_parts(n: &ObjectName) -> Option<Vec<String>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// The collection a table name names, with its database; `None` when it
/// names a table, a view or nothing of an engine database.
fn collection_named(cx: &BindCtx<'_>, parts: &[String]) -> Option<(Arc<CollTable>, DbId)> {
    let (db, table) = match parts {
        [t] => (cx.current?, t),
        [s, t] => (
            plan::physical_db(cx.tenant, s).or_else(|| cx.names.db_by_name(s))?,
            t,
        ),
        _ => return None,
    };
    if !cx.names.engine_db(db)
        || cx.names.table(db, table).is_some()
        || cx.names.other_relation(db, table)
    {
        return None;
    }
    cx.names.collection(db, table).map(|c| (c, db))
}

/// Tokens of `text` without trivia, with each token's parenthesis depth.
struct Toks<'a> {
    text: &'a str,
    t: Vec<Token>,
    depth: Vec<u32>,
}

impl<'a> Toks<'a> {
    fn new(text: &'a str, opts: LexOptions) -> Option<Self> {
        let t: Vec<Token> = lex(text.as_bytes(), &opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        let mut depth = Vec::with_capacity(t.len());
        let mut d = 0u32;
        for x in &t {
            let b = x.bytes(text.as_bytes());
            if x.kind == TokenKind::Punct && b == b")" {
                d = d.saturating_sub(1);
            }
            depth.push(d);
            if x.kind == TokenKind::Punct && b == b"(" {
                d += 1;
            }
        }
        Some(Self { text, t, depth })
    }

    fn bytes(&self, i: usize) -> &'a [u8] {
        self.t[i].bytes(self.text.as_bytes())
    }

    fn word(&self, i: usize, w: &str) -> bool {
        i < self.t.len()
            && self.t[i].kind == TokenKind::Word
            && self.bytes(i).eq_ignore_ascii_case(w.as_bytes())
    }

    fn punct(&self, i: usize, p: &[u8]) -> bool {
        i < self.t.len()
            && matches!(self.t[i].kind, TokenKind::Punct | TokenKind::Op)
            && self.bytes(i) == p
    }

    fn span(&self, a: usize, b: usize) -> &'a str {
        if a >= b {
            return "";
        }
        &self.text[self.t[a].start as usize..self.t[b - 1].end as usize]
    }

    /// The index of the `)` closing the `(` at `open`.
    fn close(&self, open: usize) -> Option<usize> {
        let d = self.depth[open];
        (open + 1..self.t.len()).find(|&i| self.depth[i] == d && self.punct(i, b")"))
    }

    /// Splits `a..b` at commas of depth `d`.
    fn list(&self, a: usize, b: usize, d: u32) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut s = a;
        for i in a..b {
            if self.depth[i] == d && self.punct(i, b",") {
                out.push((s, i));
                s = i + 1;
            }
        }
        out.push((s, b));
        out
    }
}

/// An `INSERT`'s values as text: rows of value texts (`None`: `DEFAULT`),
/// a query, or a `SET` list.
enum Source<'a> {
    Rows(Vec<Vec<Option<&'a str>>>),
    Query(&'a str),
    Set(Vec<(String, Option<&'a str>)>),
}

/// The text of an `INSERT`'s source: the first token after the table name
/// and its column list.
fn insert_source<'a>(k: &Toks<'a>, has_columns: bool, set: bool) -> Option<Source<'a>> {
    let n = k.t.len();
    let mut end = n;
    if k.punct(n.saturating_sub(1), b";") {
        end -= 1;
    }
    if let Some(o) = odku_at(k) {
        end = o;
    }
    // The table name: after INTO, or after the verb and its modifiers.
    let mut i = (0..end).find(|&i| k.depth[i] == 0 && k.word(i, "INTO"));
    if i.is_none() {
        let mut j = 1;
        while j < end
            && ["LOW_PRIORITY", "DELAYED", "HIGH_PRIORITY", "IGNORE"]
                .iter()
                .any(|w| k.word(j, w))
        {
            j += 1;
        }
        i = Some(j - 1);
    }
    let mut i = i? + 1;
    // Name parts: `a`, `a.b`.
    i += 1;
    while k.punct(i, b".") {
        i += 2;
    }
    if has_columns && k.punct(i, b"(") {
        i = k.close(i)? + 1;
    }
    if set {
        if !k.word(i, "SET") {
            return None;
        }
        let mut out = Vec::new();
        for (a, b) in k.list(i + 1, end, 0) {
            let eq = (a..b).find(|&x| k.depth[x] == 0 && k.punct(x, b"="))?;
            let name = k.span(eq - 1, eq);
            let name = String::from_utf8_lossy(&ident_value(name.as_bytes())).into_owned();
            let v = k.span(eq + 1, b);
            out.push((name, (!v.eq_ignore_ascii_case("DEFAULT")).then_some(v)));
        }
        return Some(Source::Set(out));
    }
    if k.word(i, "VALUES") || k.word(i, "VALUE") {
        let mut rows = Vec::new();
        for (a, b) in k.list(i + 1, end, 0) {
            let mut a = a;
            if k.word(a, "ROW") {
                a += 1;
            }
            if !k.punct(a, b"(") || k.close(a)? + 1 != b {
                return None;
            }
            let inner = k.close(a)?;
            let row = if inner == a + 1 {
                Vec::new()
            } else {
                k.list(a + 1, inner, 1)
                    .into_iter()
                    .map(|(x, y)| {
                        let v = k.span(x, y);
                        (!v.eq_ignore_ascii_case("DEFAULT")).then_some(v)
                    })
                    .collect()
            };
            rows.push(row);
        }
        return Some(Source::Rows(rows));
    }
    (i < end).then(|| Source::Query(k.span(i, end)))
}

/// Where `ON DUPLICATE KEY UPDATE` starts (its `ON`), at depth 0.
fn odku_at(k: &Toks<'_>) -> Option<usize> {
    (0..k.t.len()).find(|&i| {
        k.depth[i] == 0
            && k.word(i, "ON")
            && k.word(i + 1, "DUPLICATE")
            && k.word(i + 2, "KEY")
            && k.word(i + 3, "UPDATE")
    })
}

/// An `UPDATE` or `DELETE` the parser can't read as one statement
/// (`UPDATE a, b SET ...`), from its text, as a write of a MongoDB
/// collection; `None` when it writes none.
///
/// # Errors
/// As [`bind`].
pub(crate) fn bind_text(
    cx: &BindCtx<'_>,
    text: &str,
    opts: LexOptions,
) -> Result<Option<CollWrite>> {
    let head = text.trim_start().get(..6).map(str::to_ascii_uppercase);
    if !matches!(head.as_deref(), Some("UPDATE" | "DELETE")) {
        return Ok(None);
    }
    bind_change(cx, text, opts)
}

/// `ON DUPLICATE KEY UPDATE`'s assignments, as a `SELECT` of the
/// conflicting document's row with the values it gets:
///
/// ```text
/// SELECT c._id, c.doc, c._types, (value 1), ... FROM <collection> WHERE c._id = ?
/// ```
///
/// with each `VALUES(col)` (and `VALUE(col)`, the MySQL port's row alias)
/// a parameter: the new row's column.
fn bind_odku(cx: &BindCtx<'_>, k: &Toks<'_>, name: &ObjectName, coll: &CollTable) -> Result<Odku> {
    let at = odku_at(k).ok_or_else(|| not_on_engine("an ON DUPLICATE KEY UPDATE in this form"))?;
    let mut end = k.t.len();
    if k.punct(end.saturating_sub(1), b";") {
        end -= 1;
    }
    let parts = ident_parts(name).unwrap_or_default();
    let visible = q(parts.last().map_or(coll.name.as_str(), String::as_str));
    let from = parts.iter().map(|p| q(p)).collect::<Vec<_>>().join(".");
    let mut items = vec![
        format!("{visible}.`_id`"),
        format!("{visible}.`doc`"),
        format!("{visible}.`_types`"),
    ];
    let mut new_at = [None; 3];
    let mut params = Vec::new();
    for (a, b) in k.list(at + 4, end, 0) {
        let eq = (a..b)
            .find(|&x| k.depth[x] == 0 && k.punct(x, b"="))
            .ok_or_else(|| not_on_engine("an ON DUPLICATE KEY UPDATE assignment in this form"))?;
        let col = String::from_utf8_lossy(&ident_value(k.span(eq - 1, eq).as_bytes())).into_owned();
        let c = column_of(&col).ok_or_else(|| unknown_column(&col))?;
        // A column assigned twice takes the last value.
        new_at[c] = Some(items.len());
        items.push(format!(
            "({})",
            values_rewritten(k, eq + 1, b, &mut params)?
        ));
    }
    let select = format!(
        "SELECT {} FROM {from} WHERE {visible}.`_id` = ?",
        items.join(", ")
    );
    let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &select)
        .ok()
        .and_then(|mut v| v.pop())
        .ok_or_else(|| not_on_engine("an ON DUPLICATE KEY UPDATE the planner can't parse"))?;
    let lock = plan::RowLock {
        exclusive: true,
        wait: Wait::Block,
    };
    let mut g = super::general::bind(cx, &stmt, Some(&select), Some(lock))?
        .ok_or_else(|| not_on_engine("an ON DUPLICATE KEY UPDATE the planner can't plan"))?;
    g.lock_each = Some(g.tables.iter().map(|u| u.coll.is_some()).collect());
    let values_fn = (at + 4..end).any(|i| k.word(i, "VALUES") && k.punct(i + 1, b"("));
    Ok(Odku {
        select: g,
        new_at,
        params,
        values_fn,
    })
}

/// The text of tokens `a..b` with `VALUES(col)` and `VALUE(col)` of a
/// collection's column written as a parameter, its column pushed to
/// `params`.
fn values_rewritten(k: &Toks<'_>, a: usize, b: usize, params: &mut Vec<usize>) -> Result<String> {
    let mut out = String::new();
    let mut last = if a < b {
        k.t[a].start as usize
    } else {
        return Ok(String::new());
    };
    let mut i = a;
    while i < b {
        if (k.word(i, "VALUES") || k.word(i, "VALUE"))
            && k.punct(i + 1, b"(")
            && i + 3 < b
            && k.punct(i + 3, b")")
        {
            let col =
                String::from_utf8_lossy(&ident_value(k.span(i + 2, i + 3).as_bytes())).into_owned();
            let c = column_of(&col).ok_or_else(|| unknown_column(&col))?;
            out.push_str(&k.text[last..k.t[i].start as usize]);
            out.push('?');
            params.push(c);
            last = k.t[i + 3].end as usize;
            i += 4;
            continue;
        }
        i += 1;
    }
    out.push_str(&k.text[last..k.t[b - 1].end as usize]);
    Ok(out)
}

/// The Redis database's table `stmt` writes, if it writes one
/// (`engine::redistable`: read-only from SQL).
fn redis_target(cx: &BindCtx<'_>, stmt: &Statement) -> Option<String> {
    let is_redis = |name: &ObjectName| -> Option<String> {
        let parts = ident_parts(name)?;
        let (db, table) = match parts.as_slice() {
            [t] => (cx.current?, t),
            [s, t] => (
                plan::physical_db(cx.tenant, s).or_else(|| cx.names.db_by_name(s))?,
                t,
            ),
            _ => return None,
        };
        if !cx.names.engine_db(db)
            || cx.names.table(db, table).is_some()
            || cx.names.other_relation(db, table)
            || cx.names.collection(db, table).is_some()
        {
            return None;
        }
        cx.names
            .stream(db, table)
            .is_some_and(|s| s.redis.is_some())
            .then(|| table.clone())
    };
    let of_factor = |f: &TableFactor| match f {
        TableFactor::Table { name, .. } => is_redis(name),
        _ => None,
    };
    match stmt {
        Statement::Insert(ins) => match &ins.table {
            TableObject::TableName(name) => is_redis(name),
            _ => None,
        },
        Statement::Update(u) => of_factor(&u.table.relation),
        Statement::Delete(d) => {
            if let Some(t) = d.tables.iter().find_map(&is_redis) {
                return Some(t);
            }
            match &d.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => {
                    v.first().and_then(|twj| of_factor(&twj.relation))
                }
            }
        }
        _ => None,
    }
}

/// `stmt` (whose text is `text`) as an SQL write of a MongoDB collection;
/// `None` when it writes none.
///
/// # Errors
/// The servers' errors for the statement (1054, 1110, 1136); 39960 for a
/// form not run here.
pub(crate) fn bind(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: &str,
    opts: LexOptions,
) -> Result<Option<CollWrite>> {
    if let Some(t) = redis_target(cx, stmt) {
        return Err(super::redistable::read_only(&t));
    }
    match stmt {
        Statement::Insert(ins) => {
            let TableObject::TableName(name) = &ins.table else {
                return Ok(None);
            };
            let Some(parts) = ident_parts(name) else {
                return Ok(None);
            };
            let Some((coll, db)) = collection_named(cx, &parts) else {
                return Ok(None);
            };
            bind_insert(cx, ins, coll, db, text, opts).map(Some)
        }
        Statement::Update(_) | Statement::Delete(_) => bind_change(cx, text, opts),
        _ => Ok(None),
    }
}

fn bind_insert(
    cx: &BindCtx<'_>,
    ins: &sqlparser::ast::Insert,
    coll: Arc<CollTable>,
    db: DbId,
    text: &str,
    opts: LexOptions,
) -> Result<CollWrite> {
    let odku_wanted = match &ins.on {
        None => false,
        Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(_)) => true,
        Some(_) => {
            return Err(not_on_engine(
                "INSERT ... ON CONFLICT into a MongoDB collection",
            ));
        }
    };
    if ins.returning.is_some() {
        return Err(not_on_engine(
            "INSERT ... RETURNING into a MongoDB collection",
        ));
    }
    let k =
        Toks::new(text, opts).ok_or_else(|| not_on_engine("a statement the engine can't read"))?;
    let set = !ins.assignments.is_empty();
    let src = insert_source(&k, !ins.columns.is_empty(), set)
        .ok_or_else(|| not_on_engine("an INSERT into a MongoDB collection in this form"))?;
    // The target columns.
    let named: Vec<String> = match &src {
        Source::Set(s) => s.iter().map(|(n, _)| n.clone()).collect(),
        _ if ins.columns.is_empty() => vec!["_id".to_owned(), "doc".to_owned()],
        _ => ins
            .columns
            .iter()
            .map(|c| {
                ident_parts(c)
                    .and_then(|p| p.last().cloned())
                    .unwrap_or_default()
            })
            .collect(),
    };
    let mut new_at = [None; 3];
    for (i, n) in named.iter().enumerate() {
        let c = column_of(n).ok_or_else(|| unknown_column(n))?;
        if new_at[c].is_some() {
            return Err(SprigError::standard(
                1110,
                *b"42000",
                format!("Column '{}' specified twice", COLUMNS[c]),
            ));
        }
        new_at[c] = Some(i);
    }
    let width = named.len();
    let records_info = match &src {
        Source::Rows(r) => r.len() > 1,
        Source::Query(_) => true,
        Source::Set(_) => false,
    };
    let select = match &src {
        Source::Query(qt) => (*qt).to_owned(),
        Source::Rows(rows) => {
            let mut parts = Vec::with_capacity(rows.len());
            for (r, row) in rows.iter().enumerate() {
                if row.len() != width {
                    return Err(count_mismatch(r + 1));
                }
                parts.push(select_of(row.iter().copied()));
            }
            parts.join(" UNION ALL ")
        }
        Source::Set(s) => select_of(s.iter().map(|(_, v)| *v)),
    };
    let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &select)
        .ok()
        .and_then(|mut v| v.pop())
        .ok_or_else(|| not_on_engine("an INSERT's values the planner can't parse"))?;
    let g = super::general::bind_view(cx, &stmt, Some(&select))?
        .ok_or_else(|| not_on_engine("an INSERT's values the planner can't plan"))?;
    let odku = if odku_wanted {
        let TableObject::TableName(name) = &ins.table else {
            return Err(not_on_engine("an ON DUPLICATE KEY UPDATE in this form"));
        };
        Some(bind_odku(cx, &k, name, &coll)?)
    } else {
        None
    };
    Ok(CollWrite {
        kind: Kind::Insert {
            replace: ins.replace_into,
        },
        ignore: ins.ignore,
        coll,
        db,
        select: g,
        new_at,
        width,
        records_info,
        odku,
        tables: Vec::new(),
    })
}

/// `SELECT v1, v2, ...` of one row's values (`None`: `DEFAULT`, `NULL`).
fn select_of<'a>(vals: impl Iterator<Item = Option<&'a str>>) -> String {
    let items: Vec<String> = vals
        .enumerate()
        .map(|(i, v)| format!("{} AS `c{i}`", v.unwrap_or("NULL")))
        .collect();
    if items.is_empty() {
        "SELECT NULL".to_owned()
    } else {
        format!("SELECT {}", items.join(", "))
    }
}

/// One table factor of an `UPDATE`'s or `DELETE`'s references: the name
/// it is known by, and the collection or engine table it reads, if one.
struct Factor {
    visible: String,
    /// The name as written (its last part).
    table: String,
    coll: Option<(Arc<CollTable>, DbId)>,
    def: Option<(Arc<super::table::TableDef>, DbId)>,
}

/// The engine table a name names, with its database.
fn table_named(cx: &BindCtx<'_>, parts: &[String]) -> Option<(Arc<super::table::TableDef>, DbId)> {
    let (db, table) = match parts {
        [t] => (cx.current?, t),
        [s, t] => (
            plan::physical_db(cx.tenant, s).or_else(|| cx.names.db_by_name(s))?,
            t,
        ),
        _ => return None,
    };
    if !cx.names.engine_db(db) {
        return None;
    }
    match cx.names.table(db, table)?.as_ref() {
        Ok(def) => Some((Arc::clone(def), db)),
        Err(_) => None,
    }
}

/// The table factors of `refs`, in order.
fn factors(cx: &BindCtx<'_>, refs: &str) -> Option<Vec<Factor>> {
    let probe = format!("SELECT 1 FROM {refs}");
    let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &probe)
        .ok()?
        .pop()?;
    let Statement::Query(qy) = stmt else {
        return None;
    };
    let sqlparser::ast::SetExpr::Select(sel) = &*qy.body else {
        return None;
    };
    let mut out = Vec::new();
    let mut add = |f: &TableFactor| {
        if let TableFactor::Table { name, alias, .. } = f {
            let parts = ident_parts(name).unwrap_or_default();
            let visible = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .or_else(|| parts.last().cloned())
                .unwrap_or_default();
            out.push(Factor {
                visible,
                table: parts.last().cloned().unwrap_or_default(),
                coll: collection_named(cx, &parts),
                def: table_named(cx, &parts),
            });
        } else {
            out.push(Factor {
                visible: String::new(),
                table: String::new(),
                coll: None,
                def: None,
            });
        }
    };
    for twj in &sel.from {
        add(&twj.relation);
        for j in &twj.joins {
            add(&j.relation);
        }
    }
    Some(out)
}

/// The single-row writes of the tables an `UPDATE` or `DELETE` of a
/// collection also writes: `UPDATE t SET c = ?, ... WHERE <key> = ?` (or
/// `DELETE`), each value worked out by the `SELECT` (pushed to `items`)
/// from the rows as they were before the statement, as both servers do
/// with several tables.
fn table_targets(
    cx: &BindCtx<'_>,
    fs: &[Factor],
    chosen: &[usize],
    sets: &[(usize, usize, &str)],
    update: bool,
    items: &mut Vec<String>,
) -> Result<Vec<TableTarget>> {
    let mut out = Vec::new();
    for &fi in chosen {
        let f = &fs[fi];
        let Some((def, db)) = &f.def else {
            return Err(not_on_engine(
                "a target of this kind beside a MongoDB collection",
            ));
        };
        if def.hidden {
            return Err(not_on_engine(
                "a table without a primary key written in one statement with a MongoDB collection",
            ));
        }
        let start = items.len();
        for &c in &def.pk {
            items.push(format!("{}.{}", q(&f.visible), q(&def.columns[c].name)));
        }
        let keys = start..items.len();
        let mut values = Vec::new();
        let mut assigns = Vec::new();
        for (sfi, col, v) in sets {
            if *sfi != fi {
                continue;
            }
            values.push(items.len());
            items.push(format!("({v})"));
            assigns.push(format!("{} = ?", q(&def.columns[*col].name)));
        }
        let key: Vec<String> = def
            .pk
            .iter()
            .map(|&c| format!("{} = ?", q(&def.columns[c].name)))
            .collect();
        let alias = if f.visible.eq_ignore_ascii_case(&f.table) {
            String::new()
        } else {
            format!(" AS {}", q(&f.visible))
        };
        let sql = if update {
            format!(
                "UPDATE {}{alias} SET {} WHERE {}",
                q(&def.name),
                assigns.join(", "),
                key.join(" AND ")
            )
        } else {
            format!("DELETE FROM {} WHERE {}", q(&def.name), key.join(" AND "))
        };
        let cx2 = BindCtx {
            tenant: cx.tenant,
            names: cx.names,
            current: Some(*db),
            backslash: cx.backslash,
        };
        let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &sql)
            .ok()
            .and_then(|mut v| v.pop())
            .ok_or_else(|| not_on_engine("an assignment the engine can't parse"))?;
        let plan = plan::bind(&cx2, &stmt)?
            .ok_or_else(|| SprigError::internal("a row write bound to no table"))?;
        out.push(TableTarget { plan, keys, values });
    }
    Ok(out)
}

/// An `UPDATE` or `DELETE` whose targets include a collection (and maybe
/// tables).
#[allow(clippy::too_many_lines)] // the target rules, then the SELECT
fn bind_change(cx: &BindCtx<'_>, text: &str, opts: LexOptions) -> Result<Option<CollWrite>> {
    let Some(p) = super::multiwrite::split(text, opts) else {
        return Ok(None);
    };
    let Some(fs) = factors(cx, p.refs) else {
        return Ok(None);
    };
    if fs.iter().all(|f| f.coll.is_none()) {
        return Ok(None);
    }
    let ci = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    let by_name = |n: &str| fs.iter().position(|f| ci(&f.visible, n));
    // The targets: every factor an assignment or the delete list names.
    let mut targets: Vec<usize> = Vec::new();
    // (factor, column, value) of each assignment: a collection's column by
    // its position in `COLUMNS`, a table's by its position in the table.
    let mut sets: Vec<(usize, usize, &str)> = Vec::new();
    if p.update {
        let colls: Vec<usize> = (0..fs.len()).filter(|&i| fs[i].coll.is_some()).collect();
        for (name, v) in &p.sets {
            let (fi, col) = match name.as_slice() {
                [c] => {
                    // An unqualified name: the collection's column when it
                    // is the only collection and the name is one of its
                    // columns (or the only table); else the one table
                    // with such a column.
                    let in_tables: Vec<usize> = (0..fs.len())
                        .filter(|&i| {
                            fs[i]
                                .def
                                .as_ref()
                                .is_some_and(|(d, _)| d.columns.iter().any(|x| ci(&x.name, c)))
                        })
                        .collect();
                    match (colls.as_slice(), in_tables.as_slice()) {
                        ([one], _) if fs.len() == 1 || column_of(c).is_some() => (*one, c),
                        (_, [t]) => (*t, c),
                        _ => return Ok(None),
                    }
                }
                [t, c] | [_, t, c] => match by_name(t) {
                    Some(fi) => (fi, c),
                    None => return Ok(None),
                },
                _ => return Ok(None),
            };
            let c = if fs[fi].coll.is_some() {
                column_of(col).ok_or_else(|| unknown_column(col))?
            } else if let Some((def, _)) = &fs[fi].def {
                def.columns
                    .iter()
                    .position(|x| ci(&x.name, col))
                    .ok_or_else(|| unknown_column(col))?
            } else {
                return Ok(None);
            };
            if !targets.contains(&fi) {
                targets.push(fi);
            }
            sets.push((fi, c, *v));
        }
        if targets.is_empty() {
            return Ok(None);
        }
    } else if p.listed {
        for t in &p.targets {
            let Some(fi) = t.last().and_then(|n| by_name(n)) else {
                return Ok(None);
            };
            if !targets.contains(&fi) {
                targets.push(fi);
            }
        }
    } else {
        if fs.len() != 1 {
            return Ok(None);
        }
        targets.push(0);
    }
    let coll_targets: Vec<usize> = targets
        .iter()
        .copied()
        .filter(|&t| fs[t].coll.is_some())
        .collect();
    let t = match coll_targets.as_slice() {
        // Only tables: the multi-table path's.
        [] => return Ok(None),
        [one] => *one,
        _ => {
            return Err(not_on_engine(
                "a write of two MongoDB collections in one statement",
            ));
        }
    };
    let Some((coll, db)) = fs[t].coll.clone() else {
        return Ok(None);
    };
    let v = q(&fs[t].visible);
    let mut items = vec![
        format!("{v}.`_id`"),
        format!("{v}.`doc`"),
        format!("{v}.`_types`"),
    ];
    let mut new_at = [None; 3];
    for (fi, c, val) in &sets {
        if *fi != t {
            continue;
        }
        // A column assigned twice takes the last value.
        new_at[*c] = Some(items.len());
        items.push(format!("({val})"));
    }
    let table_factors: Vec<usize> = targets.iter().copied().filter(|&x| x != t).collect();
    let tables = table_targets(cx, &fs, &table_factors, &sets, p.update, &mut items)?;
    let mut select = format!("SELECT {} FROM {}", items.join(", "), p.refs);
    if let Some(w) = p.filter {
        select.push_str(" WHERE ");
        select.push_str(w);
    }
    if let Some(o) = p.order {
        select.push_str(" ORDER BY ");
        select.push_str(o);
    }
    if let Some(l) = p.limit {
        select.push_str(" LIMIT ");
        select.push_str(l);
    }
    let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &select)
        .ok()
        .and_then(|mut v| v.pop())
        .ok_or_else(|| not_on_engine("a write's tables or conditions the planner can't parse"))?;
    let lock = plan::RowLock {
        exclusive: true,
        wait: Wait::Block,
    };
    let Some(mut g) = super::general::bind(cx, &stmt, Some(&select), Some(lock))? else {
        return Ok(None);
    };
    let objects: HashSet<_> = tables.iter().map(|x| x.plan.table.object).collect();
    g.lock_each = Some(
        g.tables
            .iter()
            .map(|u| {
                u.coll
                    .as_ref()
                    .is_some_and(|c| c.db == coll.db && c.name == coll.name)
                    || (u.coll.is_none() && objects.contains(&u.def.object))
            })
            .collect(),
    );
    Ok(Some(CollWrite {
        kind: if p.update { Kind::Update } else { Kind::Delete },
        ignore: p.ignore,
        coll,
        db,
        select: g,
        new_at,
        width: 0,
        records_info: false,
        odku: None,
        tables,
    }))
}

/// A value of a row as text (`None`: `NULL`).
fn text_at(row: &[Datum], at: Option<usize>) -> Option<String> {
    let d = row.get(at?)?;
    if matches!(d, Datum::Null) {
        return None;
    }
    Some(String::from_utf8_lossy(&super::assign::text_of(d)).into_owned())
}

/// A document from `doc` text and its type map (`None` doc: empty).
fn document(doc: Option<&str>, types: Option<&str>) -> Result<Document> {
    match doc {
        None => Ok(Document::new()),
        Some(t) => sprig_doc::json::to_bson(t, types).map_err(|e| invalid(e.message)),
    }
}

/// `d` with `id` as its `_id`, first.
fn with_first_id(d: Document, id: Bson) -> Document {
    let mut n = Document::new();
    n.insert("_id", id);
    for (k, v) in d {
        if k != "_id" {
            n.insert(k, v);
        }
    }
    n
}

/// The `_id` key of `d`.
fn id_key_of(d: &Document) -> Result<Vec<u8>> {
    let id = d.get("_id").ok_or_else(|| invalid("no _id"))?;
    sprig_doc::json::id_key(id).map_err(|e| invalid(e.message))
}

/// A store error as the SQL statement's: the engine's own error when the
/// store met one (a lock wait, a deadlock), else 1062 for a duplicate
/// key and 39667 for a document refused.
fn store_error(e: &MongoError, dt: &mut DocTxn) -> SprigError {
    if let Some(engine) = dt.take_engine_error() {
        return engine;
    }
    match e.code {
        code::DUPLICATE_KEY => duplicate(e),
        code::DOCUMENT_VALIDATION_FAILURE => SprigError::new(
            SprigCode::DocInvalid,
            format!("Document failed validation: {}", e.message),
        ),
        // Row-level security's WITH CHECK, as on tables.
        sprig_doc::docstore::guard::RLS_CHECK_FAILED => {
            SprigError::new(SprigCode::RlsCheckFailed, e.message.clone())
        }
        _ => invalid(format!("{} ({})", e.message, e.code)),
    }
}

/// MySQL's 1062 for a MongoDB duplicate key error.
fn duplicate(e: &MongoError) -> SprigError {
    let index = e
        .message
        .split_once(" index: ")
        .and_then(|(_, r)| r.split_once(" dup key"))
        .map_or("_id_", |(n, _)| n);
    let value = e
        .extra
        .as_ref()
        .and_then(|x| x.get_document("keyValue").ok())
        .map(|kv| {
            kv.values()
                .map(|v| id_text(v).unwrap_or_default())
                .collect::<Vec<_>>()
                .join("-")
        })
        .unwrap_or_default();
    SprigError::standard(
        1062,
        *b"23000",
        format!("Duplicate entry '{value}' for key '{index}'"),
    )
}

/// The SQL transaction's engine transaction, lent to the document store
/// for a statement's writes and given back when this is dropped (also
/// when the statement's task is dropped half way).
struct Lent<'a> {
    tx: &'a mut Tx,
    doc: Option<DocTxn>,
}

impl Lent<'_> {
    fn doc(&mut self) -> Result<&mut DocTxn> {
        self.doc
            .as_mut()
            .ok_or_else(|| SprigError::internal("engine: a document write lost its transaction"))
    }

    fn give_back(&mut self) {
        if let Some(d) = self.doc.take() {
            let stand_in = std::mem::replace(self.tx.engine_txn(), d.into_txn());
            stand_in.rollback();
        }
    }
}

impl Drop for Lent<'_> {
    fn drop(&mut self) {
        self.give_back();
    }
}

/// Counts of one statement.
#[derive(Default)]
struct Counts {
    matched: u64,
    changed: u64,
    affected: u64,
    records: u64,
    duplicates: u64,
    conds: Vec<Condition>,
}

impl Engine {
    /// Runs `w` as one statement of the session's transaction `txn`: the
    /// `SELECT`, then each row's document write. On an error every write of
    /// the statement is undone and the transaction stays open.
    ///
    /// # Errors
    /// The statement's, or the engine's.
    pub(crate) async fn coll_write_in(
        &self,
        w: &CollWrite,
        a: &RunArgs<'_>,
        txn: &mut SessionTxn,
        found_rows: bool,
    ) -> Result<Outcome> {
        let store = crate::mongo::engine::store()
            .ok_or_else(|| SprigError::internal("engine: the document store isn't running"))?;
        self.coll_write_with(&store, w, a, txn, found_rows).await
    }

    /// [`Engine::coll_write_in`] on document store `store`.
    ///
    /// # Errors
    /// As for [`Engine::coll_write_in`].
    #[allow(clippy::too_many_lines)] // the lock, the rows, the writes, the answer
    pub(crate) async fn coll_write_with(
        &self,
        store: &sprig_doc::docstore::DocStore,
        w: &CollWrite,
        a: &RunArgs<'_>,
        txn: &mut SessionTxn,
        found_rows: bool,
    ) -> Result<Outcome> {
        let Some(tx) = txn.open_tx() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        // The store's DDL lock first: the index list read below is the one
        // every write of the transaction maintains.
        if !tx.holds_doc_guard() {
            let wait = tx.options().lock_wait_timeout.unwrap_or(GUARD_WAIT);
            let g = tokio::time::timeout(wait, store.write_guard(w.coll.tenant))
                .await
                .map_err(|_| {
                    SprigError::standard(
                        1205,
                        *b"HY000",
                        "Lock wait timeout exceeded; try restarting transaction",
                    )
                })?;
            tx.keep_doc_guard(g);
        }
        let name = Name::new(&w.coll.db, &w.coll.name);
        let coll = store
            .coll(w.coll.tenant, &name)
            .map_err(|e| SprigError::internal(format!("engine: {}", e.message)))?
            .ok_or_else(|| {
                SprigError::standard(
                    1146,
                    *b"42S02",
                    format!("Table '{}.{}' doesn't exist", w.coll.db, w.coll.name),
                )
            })?;
        let rows = self.general_run_in(&w.select, a, txn).await?;
        let Some(tx) = txn.open_tx() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        let mut counts = Counts {
            conds: rows
                .conds
                .iter()
                .map(|c| Condition {
                    level: if c.level == sprig_exec::Level::Note {
                        Level::Note
                    } else {
                        Level::Warning
                    },
                    code: c.code,
                    message: c.message.clone(),
                })
                .collect(),
            ..Counts::default()
        };
        // The tables the statement writes too: through the single-table
        // path, as a multi-table write's.
        if !w.tables.is_empty()
            && let Err(e) = self
                .table_writes(w, a.tenant, &rows.rows, tx, &mut counts)
                .await
        {
            tx.rollback_statement();
            return Err(e);
        }
        let r = if let Some(o) = &w.odku {
            self.odku_rows(
                store,
                &coll,
                w,
                o,
                a,
                &rows.rows,
                txn,
                found_rows,
                &mut counts,
            )
            .await
        } else {
            let stand_in = self.eng.begin(sprig_engine::txn::TxnOptions::default())?;
            let check = tx
                .row_lock_request(LockMode::Exclusive, Wait::Block)
                .check_snapshot;
            let real = std::mem::replace(tx.engine_txn(), stand_in);
            let mut lent = Lent {
                tx,
                doc: Some(DocTxn::adopt(real, check)),
            };
            let r = write_rows(store, &coll, w, &rows.rows, &mut lent, &mut counts).await;
            drop(lent);
            r
        };
        let Some(tx) = txn.open_tx() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        if let Err(e) = r {
            tx.rollback_statement();
            return Err(e);
        }
        if let Err(e) = tx.gate().await {
            tx.rollback_statement();
            return Err(e);
        }
        // MySQL deprecates `VALUES(col)` in `ON DUPLICATE KEY UPDATE`.
        if a.seed.session.persona == sprig_exec::session::Persona::MySql
            && w.odku.as_ref().is_some_and(|x| x.values_fn)
        {
            counts.conds.push(Condition {
                level: Level::Warning,
                code: 1287,
                message: "'VALUES function' is deprecated and will be removed in a future release. Please use an alias (INSERT INTO ... VALUES (...) AS alias) and replace VALUES(col) in the ON DUPLICATE KEY UPDATE clause with alias.col instead".to_owned(),
            });
        }
        let warnings = counts.conds.len();
        let (affected, info) = match w.kind {
            Kind::Update => (
                if found_rows {
                    counts.matched
                } else {
                    counts.changed
                },
                Some(format!(
                    "Rows matched: {}  Changed: {}  Warnings: {warnings}",
                    counts.matched, counts.changed
                )),
            ),
            Kind::Delete => (counts.affected, None),
            Kind::Insert { .. } => (
                counts.affected,
                w.records_info.then(|| {
                    format!(
                        "Records: {}  Duplicates: {}  Warnings: {warnings}",
                        counts.records, counts.duplicates
                    )
                }),
            ),
        };
        Ok(Outcome {
            rows: Vec::new(),
            affected,
            last_insert_id: 0,
            reported_id: 0,
            info,
            no_index: rows.no_index || rows.full_scan,
            examined: rows.examined,
            conds: counts.conds,
        })
    }
}

impl Engine {
    /// The table rows an `UPDATE` or `DELETE` of a collection writes too:
    /// each target row once, with what its first match gave.
    async fn table_writes(
        &self,
        w: &CollWrite,
        tenant: u64,
        rows: &[Vec<Datum>],
        tx: &mut Tx,
        counts: &mut Counts,
    ) -> Result<()> {
        let mut seen: Vec<HashSet<String>> = vec![HashSet::new(); w.tables.len()];
        let mut params: Vec<Datum> = Vec::new();
        for row in rows {
            for (t, seen) in w.tables.iter().zip(seen.iter_mut()) {
                let key = &row[t.keys.clone()];
                if !seen.insert(format!("{key:?}")) {
                    continue;
                }
                params.clear();
                params.extend(t.values.iter().map(|&i| row[i].clone()));
                params.extend(key.iter().cloned());
                let o = self.run_part(tenant, &t.plan, &params, false, tx).await?;
                match w.kind {
                    Kind::Update => {
                        counts.matched += 1;
                        counts.changed += o.affected;
                    }
                    _ => counts.affected += o.affected,
                }
                counts.conds.extend(o.conds);
            }
        }
        Ok(())
    }

    /// The rows of an `INSERT ... ON DUPLICATE KEY UPDATE`: each inserted,
    /// or its conflicting document given the assignments' values (worked
    /// out by `o`'s `SELECT`, between two loans of the transaction).
    #[allow(clippy::too_many_arguments)]
    async fn odku_rows(
        &self,
        store: &sprig_doc::docstore::DocStore,
        coll: &Coll,
        w: &CollWrite,
        o: &Odku,
        a: &RunArgs<'_>,
        rows: &[Vec<Datum>],
        txn: &mut SessionTxn,
        found_rows: bool,
        counts: &mut Counts,
    ) -> Result<()> {
        let text = |s: Option<String>| s.map_or(Datum::Null, |t| Datum::Str(t.into()));
        for row in rows {
            if row.len() != w.width {
                return Err(count_mismatch(1));
            }
            let conflict = {
                let tx = txn
                    .open_tx()
                    .ok_or_else(|| SprigError::internal("engine: no open transaction"))?;
                let stand_in = self.eng.begin(sprig_engine::txn::TxnOptions::default())?;
                let check = tx
                    .row_lock_request(LockMode::Exclusive, Wait::Block)
                    .check_snapshot;
                let real = std::mem::replace(tx.engine_txn(), stand_in);
                let mut lent = Lent {
                    tx,
                    doc: Some(DocTxn::adopt(real, check)),
                };
                insert_or_conflict(store, coll, w, row, &mut lent, counts).await?
            };
            let Some((idk, cur, new_id)) = conflict else {
                continue;
            };
            counts.duplicates += 1;
            let new_row = [
                text_at(row, w.new_at[0]).or(new_id),
                text_at(row, w.new_at[1]),
                text_at(row, w.new_at[2]),
            ];
            let mut params: Vec<Datum> =
                o.params.iter().map(|&c| text(new_row[c].clone())).collect();
            params.push(text(cur.get("_id").and_then(id_text)));
            let args = RunArgs {
                tenant: a.tenant,
                params: &params,
                seed: a.seed,
                deadline: a.deadline,
                stop: a.stop.clone(),
            };
            let got = self.general_run_part(&o.select, &args, txn).await?;
            counts.conds.extend(got.conds.iter().map(|c| Condition {
                level: if c.level == sprig_exec::Level::Note {
                    Level::Note
                } else {
                    Level::Warning
                },
                code: c.code,
                message: c.message.clone(),
            }));
            // The conflicting document's row (another document may print
            // its `_id` the same).
            let mut mine = None;
            for r in got.rows {
                let d = document(
                    text_at(&r, Some(1)).as_deref(),
                    text_at(&r, Some(2)).as_deref(),
                )?;
                if id_key_of(&d)? == idk {
                    mine = Some(r);
                    break;
                }
            }
            let Some(r) = mine else {
                return Err(SprigError::internal(
                    "engine: the conflicting document wasn't found again",
                ));
            };
            let old_types = text_at(&r, Some(2));
            let new = new_version_at(&o.new_at, &r, &cur, &idk, old_types.as_deref())?;
            let before = bson::to_vec(&cur).unwrap_or_default();
            let after = bson::to_vec(&new).unwrap_or_default();
            if before == after {
                // Unchanged: 0 rows (1 with CLIENT_FOUND_ROWS).
                counts.affected += u64::from(found_rows);
                continue;
            }
            let tx = txn
                .open_tx()
                .ok_or_else(|| SprigError::internal("engine: no open transaction"))?;
            let stand_in = self.eng.begin(sprig_engine::txn::TxnOptions::default())?;
            let check = tx
                .row_lock_request(LockMode::Exclusive, Wait::Block)
                .check_snapshot;
            let real = std::mem::replace(tx.engine_txn(), stand_in);
            let mut lent = Lent {
                tx,
                doc: Some(DocTxn::adopt(real, check)),
            };
            let dt = lent.doc()?;
            if let Err(e) = store
                .replace_doc(dt, coll, &idk, &cur, &new, HintOp::Replace)
                .await
            {
                return Err(store_error(&e, dt));
            }
            counts.affected += 2;
        }
        Ok(())
    }
}

/// Every row's document write.
async fn write_rows(
    store: &sprig_doc::docstore::DocStore,
    coll: &Coll,
    w: &CollWrite,
    rows: &[Vec<Datum>],
    lent: &mut Lent<'_>,
    counts: &mut Counts,
) -> Result<()> {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    for row in rows {
        match w.kind {
            Kind::Insert { replace } => {
                if row.len() != w.width {
                    return Err(count_mismatch(1));
                }
                insert_row(store, coll, w, row, replace, lent, counts).await?;
            }
            Kind::Update | Kind::Delete => {
                let old_doc = text_at(row, Some(1));
                let old_types = text_at(row, Some(2));
                let old = document(old_doc.as_deref(), old_types.as_deref())?;
                let idk = id_key_of(&old)?;
                // A document matched again (another row of a joined table)
                // is written once, with what its first match gave.
                if !seen.insert(idk.clone()) {
                    continue;
                }
                let key = coll.space.doc(&idk);
                let dt = lent.doc()?;
                let cur = match dt.get_current(&key) {
                    Ok(Some(v)) => {
                        sprig_doc::docstore::read::decode(&v).map_err(|e| invalid(e.message))?
                    }
                    // Gone meanwhile (it was read locked: not by another).
                    Ok(None) => continue,
                    Err(e) => return Err(store_error(&e, dt)),
                };
                if w.kind == Kind::Delete {
                    if let Err(e) = store.delete_doc(dt, coll, &idk, &cur).await {
                        return Err(store_error(&e, dt));
                    }
                    counts.affected += 1;
                    continue;
                }
                counts.matched += 1;
                let new = new_version(w, row, &cur, &idk, old_types.as_deref())?;
                let before = bson::to_vec(&cur).unwrap_or_default();
                let after = bson::to_vec(&new).unwrap_or_default();
                if before == after {
                    continue;
                }
                if let Err(e) = store
                    .replace_doc(dt, coll, &idk, &cur, &new, HintOp::Replace)
                    .await
                {
                    return Err(store_error(&e, dt));
                }
                counts.changed += 1;
            }
        }
    }
    Ok(())
}

/// The new version of document `cur` (key `idk`) an `UPDATE` row gives.
fn new_version(
    w: &CollWrite,
    row: &[Datum],
    cur: &Document,
    idk: &[u8],
    old_types: Option<&str>,
) -> Result<Document> {
    new_version_at(&w.new_at, row, cur, idk, old_types)
}

/// [`new_version`] with the new values at `new_at` of `row`.
fn new_version_at(
    new_at: &[Option<usize>; 3],
    row: &[Datum],
    cur: &Document,
    idk: &[u8],
    old_types: Option<&str>,
) -> Result<Document> {
    let w = NewAt { new_at: *new_at };
    let old_id = cur.get("_id").cloned().unwrap_or(Bson::Null);
    if let Some(at) = w.new_at[0] {
        let id = text_at(row, Some(at));
        if id.as_deref() != id_text(&old_id).as_deref() {
            return Err(id_changed());
        }
    }
    if w.new_at[1].is_none() && w.new_at[2].is_none() {
        return Ok(cur.clone());
    }
    let doc = match w.new_at[1] {
        Some(at) => text_at(row, Some(at)).ok_or_else(|| invalid("doc can't be NULL"))?,
        None => text_at(row, Some(1)).ok_or_else(|| invalid("doc can't be NULL"))?,
    };
    let types = match w.new_at[2] {
        Some(at) => text_at(row, Some(at)),
        None => old_types.map(str::to_owned),
    };
    let new = document(Some(&doc), types.as_deref())?;
    if new.get("_id").is_none() {
        return Ok(with_first_id(new, old_id));
    }
    if id_key_of(&new)? != idk {
        return Err(id_changed());
    }
    Ok(new)
}

/// Where an `UPDATE`'s (or an `ON DUPLICATE KEY UPDATE`'s) new values are.
struct NewAt {
    new_at: [Option<usize>; 3],
}

/// The document an `INSERT` row gives, with its `_id`.
fn insert_doc_of(w: &CollWrite, row: &[Datum]) -> Result<Document> {
    let doc = text_at(row, w.new_at[1]);
    let types = text_at(row, w.new_at[2]);
    let mut d = document(doc.as_deref(), types.as_deref())?;
    if let Some(id) = text_at(row, w.new_at[0]) {
        match d.get("_id") {
            Some(v) if id_text(v).as_deref() != Some(id.as_str()) => {
                return Err(invalid(format!(
                    "the _id column ('{id}') and the document's _id differ"
                )));
            }
            Some(_) => {}
            None => d = with_first_id(d, Bson::String(id)),
        }
    }
    Ok(sprig_doc::docstore::write::with_id(d))
}

/// The key of the document an insert of `d` met on a unique index (the
/// duplicate key error `e`): `d`'s own `_id`, or the owner of the index
/// entry `d` would have taken.
fn conflicting_key(coll: &Coll, d: &Document, e: &MongoError, dt: &mut DocTxn) -> Result<Vec<u8>> {
    let own = id_key_of(d)?;
    let index = e
        .message
        .split_once(" index: ")
        .and_then(|(_, r)| r.split_once(" dup key"))
        .map(|(n, _)| n.trim().to_owned());
    let Some(idx) = index
        .as_deref()
        .filter(|n| *n != "_id_")
        .and_then(|n| coll.meta.indexes.iter().find(|i| i.name == n))
    else {
        return Ok(own);
    };
    let (entries, _) =
        sprig_doc::docstore::keys::entries(d, idx).map_err(|x| invalid(x.message))?;
    for en in entries {
        let mut k = coll.space.index(idx.id, true);
        k.extend_from_slice(&en);
        match dt.get_current(&k) {
            Ok(Some(v)) if v.as_ref() != own.as_slice() => return Ok(v.to_vec()),
            Ok(_) => {}
            Err(x) => return Err(store_error(&x, dt)),
        }
    }
    Ok(own)
}

/// One row of an `INSERT ... ON DUPLICATE KEY UPDATE`: inserted, or the
/// document it conflicts with (its key, as it is now) with the new row's
/// `_id` text.
async fn insert_or_conflict(
    store: &sprig_doc::docstore::DocStore,
    coll: &Coll,
    w: &CollWrite,
    row: &[Datum],
    lent: &mut Lent<'_>,
    counts: &mut Counts,
) -> Result<Option<(Vec<u8>, Document, Option<String>)>> {
    let d = insert_doc_of(w, row)?;
    let new_id = d.get("_id").and_then(id_text);
    counts.records += 1;
    let dt = lent.doc()?;
    let sp = dt.savepoint();
    match store.insert_doc(dt, coll, d.clone()).await {
        Ok(_) => {
            counts.affected += 1;
            Ok(None)
        }
        Err(e) => {
            if let Some(engine) = dt.take_engine_error() {
                return Err(engine);
            }
            if e.code != code::DUPLICATE_KEY {
                return Err(store_error(&e, dt));
            }
            dt.rollback_to(sp);
            let idk = conflicting_key(coll, &d, &e, dt)?;
            let key = coll.space.doc(&idk);
            if let Err(x) = dt.lock_doc(&key).await {
                return Err(store_error(&x, dt));
            }
            let cur = match dt.get_current(&key) {
                Ok(Some(v)) => {
                    sprig_doc::docstore::read::decode(&v).map_err(|x| invalid(x.message))?
                }
                Ok(None) => return Err(duplicate(&e)),
                Err(x) => return Err(store_error(&x, dt)),
            };
            Ok(Some((idk, cur, new_id)))
        }
    }
}

/// One `INSERT` (or `REPLACE`) row.
async fn insert_row(
    store: &sprig_doc::docstore::DocStore,
    coll: &Coll,
    w: &CollWrite,
    row: &[Datum],
    replace: bool,
    lent: &mut Lent<'_>,
    counts: &mut Counts,
) -> Result<()> {
    let d = insert_doc_of(w, row)?;
    counts.records += 1;
    let dt = lent.doc()?;
    let sp = dt.savepoint();
    if replace {
        let idk = id_key_of(&d)?;
        let key = coll.space.doc(&idk);
        if let Err(e) = dt.lock_doc(&key).await {
            return Err(store_error(&e, dt));
        }
        let cur = match dt.get_current(&key) {
            Ok(v) => v,
            Err(e) => return Err(store_error(&e, dt)),
        };
        if let Some(v) = cur {
            let old = sprig_doc::docstore::read::decode(&v).map_err(|e| invalid(e.message))?;
            if let Err(e) = store
                .replace_doc(dt, coll, &idk, &old, &d, HintOp::Replace)
                .await
            {
                return Err(store_error(&e, dt));
            }
            counts.affected += 2;
            counts.duplicates += 1;
            return Ok(());
        }
    }
    match store.insert_doc(dt, coll, d).await {
        Ok(_) => {
            counts.affected += 1;
            Ok(())
        }
        Err(e) => {
            if let Some(engine) = dt.take_engine_error() {
                return Err(engine);
            }
            if !(w.ignore && e.code == code::DUPLICATE_KEY) {
                return Err(store_error(&e, dt));
            }
            // INSERT IGNORE: the row's writes undone, a warning.
            dt.rollback_to(sp);
            let err = duplicate(&e);
            counts.duplicates += 1;
            counts.conds.push(Condition {
                level: Level::Warning,
                code: 1062,
                message: err.message,
            });
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn src(text: &str, cols: bool, set: bool) -> Option<Vec<Vec<Option<String>>>> {
        let k = Toks::new(text, LexOptions::mariadb())?;
        match insert_source(&k, cols, set)? {
            Source::Rows(r) => Some(
                r.into_iter()
                    .map(|row| row.into_iter().map(|v| v.map(str::to_owned)).collect())
                    .collect(),
            ),
            Source::Query(q) => Some(vec![vec![Some(format!("Q:{q}"))]]),
            Source::Set(s) => Some(vec![
                s.into_iter()
                    .map(|(n, v)| Some(format!("{n}={}", v.unwrap_or("DEFAULT"))))
                    .collect(),
            ]),
        }
    }

    #[test]
    fn insert_sources_are_read_off_the_text() {
        let r = src(
            r#"INSERT INTO c (doc) VALUES ('{"a":"x\\"y"}'), (JSON_OBJECT('b', 1, 'c', (1+2)))"#,
            true,
            false,
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0].as_deref(), Some(r#"'{"a":"x\\"y"}'"#));
        assert_eq!(r[1][0].as_deref(), Some("JSON_OBJECT('b', 1, 'c', (1+2))"));
        let r = src(
            "insert ignore into db.c values ('k', DEFAULT), ROW('j', '{}');",
            false,
            false,
        )
        .unwrap();
        assert_eq!(r[0], vec![Some("'k'".to_owned()), None]);
        assert_eq!(r[1], vec![Some("'j'".to_owned()), Some("'{}'".to_owned())]);
        let r = src(
            "INSERT INTO c (_id, doc) SELECT id, JSON_OBJECT('v', v) FROM t",
            true,
            false,
        )
        .unwrap();
        assert_eq!(
            r[0][0].as_deref(),
            Some("Q:SELECT id, JSON_OBJECT('v', v) FROM t")
        );
        let r = src("REPLACE c SET doc = '{}', `_id` = 'x'", false, true).unwrap();
        assert_eq!(
            r[0],
            vec![Some("doc='{}'".to_owned()), Some("_id='x'".to_owned())]
        );
    }

    #[test]
    fn update_keeps_types_and_id() {
        let mut d = Document::new();
        d.insert(
            "_id",
            bson::oid::ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap(),
        );
        d.insert("n", Bson::Int64(5));
        d.insert(
            "when",
            Bson::DateTime(bson::DateTime::from_millis(1_700_000_000_000)),
        );
        let (j, t) =
            sprig_doc::json::render(&d, &sprig_doc::json::RenderOpts { type_map: true }).unwrap();
        // The same text and type map give the same document back.
        let back = document(Some(&j), t.as_deref()).unwrap();
        assert_eq!(back, d);
        // A changed field keeps its type when its text still fits it.
        let j2 = j.replace("\"n\":5", "\"n\":6");
        let back = document(Some(&j2), t.as_deref()).unwrap();
        assert_eq!(back.get("n"), Some(&Bson::Int64(6)));
        assert_eq!(back.get("when"), d.get("when"));
        assert_eq!(id_key_of(&back).unwrap(), id_key_of(&d).unwrap());
    }
}
