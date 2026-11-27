//! `UPDATE` and `DELETE` beyond one table's shapes: joins (`UPDATE a JOIN
//! b ON ... SET ...`, `UPDATE a, b SET ...`, `DELETE a, b FROM ...`,
//! `DELETE FROM a USING ...`), subqueries, and one table with `ORDER BY`.
//!
//! Such a statement runs in two parts, inside one statement of one
//! transaction:
//!
//! ```text
//! 1. SELECT <each target's primary key>, <values the rows get>
//!      FROM <the tables> WHERE ... [ORDER BY ...] [LIMIT ...]
//!    through the planner, locking as the servers do: the target tables'
//!    rows exclusively, every other row read (joined tables, subqueries)
//!    in shared mode.
//! 2. per row found, per target table, once per target row:
//!      UPDATE t SET c = ?, ... WHERE <primary key> = ?   (or DELETE)
//!    through the single-table path, so constraints, foreign keys,
//!    row-security checks and ON UPDATE columns apply as for any write.
//! ```
//!
//! A write that fails undoes the whole statement. Values: with more than
//! one table, every assignment is worked out from the rows as they were
//! before the statement (both servers do so); with one table, the
//! assignments run left to right on the row as the single-table path
//! does, and only one holding a subquery is worked out beforehand.
//!
//! Rules that differ between the ports: MySQL 8.4 refuses `ORDER BY` and
//! `LIMIT` on a multi-table `UPDATE` (1221) and `DELETE` (1064), and a
//! target table read again in a subquery (1093); MariaDB 11.8 takes all
//! three.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_sql::lex::{LexOptions, Token, TokenKind, ident_value, lex};
use sqlparser::ast::{SetExpr, Statement, TableFactor};
use sqlparser::parser::Parser;

use super::exec::Outcome;
use super::general::{General, RunArgs};
use super::plan::{self, BindCtx, Plan};
use super::row::Datum;
use super::session::SessionTxn;
use super::table::TableDef;
use super::{Engine, not_on_engine};

/// A multi-table (or subquery) `UPDATE` or `DELETE`, bound.
#[derive(Debug)]
pub(crate) struct MultiWrite {
    /// The `SELECT` that finds the rows and works out the values.
    pub select: General,
    /// The target tables.
    pub targets: Vec<Target>,
    /// An `UPDATE` (else a `DELETE`).
    pub update: bool,
}

/// One target table.
#[derive(Debug)]
pub(crate) struct Target {
    /// The write of one row: its parameters are `values`, then the key.
    pub plan: Plan,
    /// Where the row's primary key is in the `SELECT`'s rows.
    pub keys: Range<usize>,
    /// Where each value parameter is in the `SELECT`'s rows.
    pub values: Vec<usize>,
}

/// The statement's parts, as text.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Parts<'a> {
    pub(crate) update: bool,
    pub(crate) ignore: bool,
    /// The table references.
    pub(crate) refs: &'a str,
    /// `UPDATE`: each assignment's target name parts and value text.
    pub(crate) sets: Vec<(Vec<String>, &'a str)>,
    /// `DELETE`: the listed targets' name parts.
    pub(crate) targets: Vec<Vec<String>>,
    /// `DELETE t FROM ...` or `DELETE FROM t USING ...`.
    pub(crate) listed: bool,
    pub(crate) filter: Option<&'a str>,
    /// After `ORDER BY`.
    pub(crate) order: Option<&'a str>,
    pub(crate) limit: Option<&'a str>,
    /// Where `ORDER BY` or `LIMIT` starts (the MySQL port's syntax error).
    pub(crate) tail_at: Option<usize>,
}

struct Toks<'a> {
    text: &'a str,
    t: Vec<Token>,
    depth: Vec<u32>,
}

impl<'a> Toks<'a> {
    fn new(text: &'a str, opts: LexOptions) -> Option<Self> {
        let mut t: Vec<Token> = lex(text.as_bytes(), &opts)
            .ok()?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        if t.last()
            .is_some_and(|x| x.kind == TokenKind::Punct && x.bytes(text.as_bytes()) == b";")
        {
            t.pop();
        }
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

    /// The first index at or after `from` (and before `to`) at depth 0
    /// where `f` holds.
    fn find(&self, from: usize, to: usize, f: impl Fn(usize) -> bool) -> Option<usize> {
        (from..to).find(|&i| self.depth[i] == 0 && f(i))
    }

    /// The text of tokens `a..b`.
    fn span(&self, a: usize, b: usize) -> &'a str {
        if a >= b {
            return "";
        }
        &self.text[self.t[a].start as usize..self.t[b - 1].end as usize]
    }

    /// Name parts (`a`, `a.b`, `` `a`.`b` ``) over tokens `a..b`; `None`
    /// when they are anything else. A final `.*` is dropped.
    fn name(&self, a: usize, b: usize) -> Option<Vec<String>> {
        let mut out = Vec::new();
        let mut i = a;
        let mut b = b;
        if b >= a + 2 && self.punct(b - 1, b"*") && self.punct(b - 2, b".") {
            b -= 2;
        }
        while i < b {
            if !self.t[i].kind.is_ident_like() {
                return None;
            }
            out.push(String::from_utf8_lossy(&ident_value(self.bytes(i))).into_owned());
            i += 1;
            if i < b {
                if !self.punct(i, b".") {
                    return None;
                }
                i += 1;
                if i == b {
                    return None;
                }
            }
        }
        (!out.is_empty()).then_some(out)
    }

    /// Splits `a..b` at depth-0 commas.
    fn list(&self, a: usize, b: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut s = a;
        for i in a..b {
            if self.depth[i] == 0 && self.punct(i, b",") {
                out.push((s, i));
                s = i + 1;
            }
        }
        out.push((s, b));
        out
    }

    /// `WHERE`, `ORDER BY` and `LIMIT` from `from` on.
    fn tail(&self, from: usize, p: &mut Parts<'a>) -> Option<()> {
        let n = self.t.len();
        let w = self.find(from, n, |i| self.word(i, "WHERE"));
        let o = self.find(from, n, |i| self.word(i, "ORDER") && self.word(i + 1, "BY"));
        let l = self.find(from, n, |i| self.word(i, "LIMIT"));
        let end_of = |start: usize| {
            [w, o, l]
                .into_iter()
                .flatten()
                .filter(|&x| x > start)
                .min()
                .unwrap_or(n)
        };
        if let Some(w) = w {
            p.filter = Some(self.span(w + 1, end_of(w)));
        }
        if let Some(o) = o {
            p.order = Some(self.span(o + 2, end_of(o + 1)));
        }
        if let Some(l) = l {
            p.limit = Some(self.span(l + 1, n));
        }
        p.tail_at = [o, l]
            .into_iter()
            .flatten()
            .min()
            .map(|i| self.t[i].start as usize);
        // Clauses out of order, or text after the last: not this shape.
        if let (Some(w), Some(o)) = (w, o)
            && o < w
        {
            return None;
        }
        if let (Some(o), Some(l)) = (o, l)
            && l < o
        {
            return None;
        }
        if let (Some(w), Some(l)) = (w, l)
            && l < w
        {
            return None;
        }
        Some(())
    }
}

/// The statement's parts, `None` for text that isn't an `UPDATE` or
/// `DELETE` of these shapes.
pub(crate) fn split(text: &str, opts: LexOptions) -> Option<Parts<'_>> {
    let k = Toks::new(text, opts)?;
    let n = k.t.len();
    let mut p = Parts::default();
    let mut i = 1;
    if k.word(0, "UPDATE") {
        p.update = true;
        while k.word(i, "LOW_PRIORITY") || k.word(i, "IGNORE") {
            p.ignore |= k.word(i, "IGNORE");
            i += 1;
        }
        let set = k.find(i, n, |j| k.word(j, "SET"))?;
        p.refs = k.span(i, set);
        let end = k
            .find(set + 1, n, |j| {
                k.word(j, "WHERE")
                    || (k.word(j, "ORDER") && k.word(j + 1, "BY"))
                    || k.word(j, "LIMIT")
            })
            .unwrap_or(n);
        for (a, b) in k.list(set + 1, end) {
            let eq = k.find(a, b, |j| k.punct(j, b"="))?;
            let target = k.name(a, eq)?;
            if eq + 1 >= b {
                return None;
            }
            p.sets.push((target, k.span(eq + 1, b)));
        }
        k.tail(end, &mut p)?;
    } else if k.word(0, "DELETE") {
        while k.word(i, "LOW_PRIORITY") || k.word(i, "QUICK") || k.word(i, "IGNORE") {
            p.ignore |= k.word(i, "IGNORE");
            i += 1;
        }
        let stop = |j: usize| {
            k.word(j, "WHERE")
                || (k.word(j, "ORDER") && k.word(j + 1, "BY"))
                || k.word(j, "LIMIT")
                || k.word(j, "RETURNING")
        };
        if k.word(i, "FROM") {
            let end = k
                .find(i + 1, n, |j| stop(j) || k.word(j, "USING"))
                .unwrap_or(n);
            if k.word(end, "USING") {
                p.listed = true;
                for (a, b) in k.list(i + 1, end) {
                    p.targets.push(k.name(a, b)?);
                }
                let e2 = k.find(end + 1, n, stop).unwrap_or(n);
                p.refs = k.span(end + 1, e2);
                k.tail(e2, &mut p)?;
            } else {
                p.refs = k.span(i + 1, end);
                k.tail(end, &mut p)?;
            }
        } else {
            let from = k.find(i, n, |j| k.word(j, "FROM"))?;
            p.listed = true;
            for (a, b) in k.list(i, from) {
                p.targets.push(k.name(a, b)?);
            }
            let end = k.find(from + 1, n, stop).unwrap_or(n);
            p.refs = k.span(from + 1, end);
            k.tail(end, &mut p)?;
        }
        if k.find(0, n, |j| k.word(j, "RETURNING")).is_some() {
            return None;
        }
    } else {
        return None;
    }
    (!p.refs.is_empty()).then_some(p)
}

/// One table of the statement's `FROM`.
struct Factor {
    /// The name it is known by: its alias, else its table name.
    visible: String,
    /// The table name as written (last part), and its database part.
    table: String,
    db: Option<String>,
    /// The engine table (`None` for a derived table).
    def: Option<(Arc<TableDef>, DbId)>,
}

/// The tables `refs` names at its top level.
fn factors(cx: &BindCtx<'_>, refs: &str) -> Result<Option<Vec<Factor>>> {
    let probe = format!("SELECT 1 FROM {refs}");
    let Ok(mut v) = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &probe) else {
        return Ok(None);
    };
    let Some(Statement::Query(q)) = v.pop() else {
        return Ok(None);
    };
    let SetExpr::Select(s) = &*q.body else {
        return Ok(None);
    };
    let mut out = Vec::new();
    let mut engine = false;
    for twj in &s.from {
        for f in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
            match f {
                TableFactor::Table { name, alias, .. } => {
                    let parts: Vec<String> = name
                        .0
                        .iter()
                        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
                        .collect();
                    let Some(table) = parts.last().cloned() else {
                        return Ok(None);
                    };
                    let def = match plan::resolve(cx, name)? {
                        plan::Resolved::Engine(def, db) => {
                            engine = true;
                            Some((def, db))
                        }
                        plan::Resolved::Elsewhere => None,
                    };
                    if def.is_none() {
                        // A table of a MariaDB-backed database next to
                        // engine tables: never one statement.
                        return Err(not_on_engine(
                            "an engine table next to another kind of table",
                        ));
                    }
                    out.push(Factor {
                        visible: alias
                            .as_ref()
                            .map_or_else(|| table.clone(), |a| a.name.value.clone()),
                        db: (parts.len() > 1).then(|| parts[0].clone()),
                        table,
                        def,
                    });
                }
                TableFactor::Derived { alias: Some(a), .. } => out.push(Factor {
                    visible: a.name.value.clone(),
                    table: a.name.value.clone(),
                    db: None,
                    def: None,
                }),
                _ => {
                    return Err(not_on_engine(
                        "a table function or a nested join in a write",
                    ));
                }
            }
        }
    }
    Ok(engine.then_some(out))
}

/// `name` as a quoted identifier.
fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The error for a column the assignments name that no table has.
fn unknown_column(name: &str, mysql: bool) -> SprigError {
    SprigError::standard(
        1054,
        *b"42S22",
        format!(
            "Unknown column '{name}' in '{}'",
            if mysql { "field list" } else { "SET" }
        ),
    )
}

fn not_updatable(name: &str, update: bool) -> SprigError {
    SprigError::standard(
        1288,
        *b"HY000",
        format!(
            "The target table {name} of the {} is not updatable",
            if update { "UPDATE" } else { "DELETE" }
        ),
    )
}

fn syntax_near(text: &str, at: usize) -> SprigError {
    let rest: String = text
        .get(at..)
        .unwrap_or("")
        .trim_end()
        .chars()
        .take(80)
        .collect();
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             MySQL server version for the right syntax to use near '{rest}' at line 1"
        ),
    )
}

/// Whether `text` holds a subquery.
fn has_subquery(text: &str, opts: LexOptions) -> bool {
    Toks::new(text, opts).is_some_and(|k| (0..k.t.len()).any(|i| k.word(i, "SELECT")))
}

/// Whether a subquery in `text` reads table `name` (by its last name
/// part, after `FROM`, `JOIN` or a `,` of a `FROM` list).
fn subquery_reads(text: &str, name: &str, opts: LexOptions) -> bool {
    let Some(k) = Toks::new(text, opts) else {
        return false;
    };
    let mut in_from = false;
    for i in 0..k.t.len() {
        if k.word(i, "FROM") || k.word(i, "JOIN") {
            in_from = true;
        } else if k.word(i, "WHERE") || k.word(i, "ON") || k.word(i, "SELECT") {
            in_from = false;
        }
        let after = i > 0
            && (k.word(i - 1, "FROM")
                || k.word(i - 1, "JOIN")
                || (in_from && k.punct(i - 1, b",")));
        if after && k.depth[i] > 0 {
            let mut j = i;
            while j + 2 < k.t.len() && k.punct(j + 1, b".") {
                j += 2;
            }
            if k.t[j].kind.is_ident_like()
                && String::from_utf8_lossy(&ident_value(k.bytes(j))).eq_ignore_ascii_case(name)
            {
                return true;
            }
        }
    }
    false
}

/// Binds `text` as a multi-table or subquery `UPDATE`/`DELETE`; `None`
/// when it isn't one, or names no engine table.
///
/// # Errors
/// The servers' errors for the statement (1054, 1052, 1109, 1288, 1093,
/// 1221, 1064, 1146); `ER_SPRIG_NOT_ON_ENGINE` for a form not run here.
pub(crate) fn bind(
    cx: &BindCtx<'_>,
    text: &str,
    opts: LexOptions,
    mysql: bool,
) -> Result<Option<MultiWrite>> {
    let Some(r) = resolve_parts(cx, text, opts, mysql)? else {
        return Ok(None);
    };
    plans(cx, r, opts, mysql)
}

/// A write's parts with its tables, targets and assignments resolved.
struct Resolved<'a> {
    p: Parts<'a>,
    fs: Vec<Factor>,
    /// The target factors, in the order first named.
    chosen: Vec<usize>,
    /// Per assignment: (factor, column, value text).
    sets: Vec<(usize, usize, &'a str)>,
    /// More than one table, or `DELETE t FROM ...`.
    multi: bool,
}

/// [`bind`]'s first part: the statement read, the port rules checked.
#[allow(clippy::too_many_lines)] // the port rules, then the targets
fn resolve_parts<'a>(
    cx: &BindCtx<'_>,
    text: &'a str,
    opts: LexOptions,
    mysql: bool,
) -> Result<Option<Resolved<'a>>> {
    let Some(p) = split(text, opts) else {
        return Ok(None);
    };
    let Some(fs) = factors(cx, p.refs)? else {
        return Ok(None);
    };
    let multi = fs.len() > 1 || p.listed;
    if p.ignore {
        return Err(not_on_engine(
            "UPDATE IGNORE or DELETE IGNORE through a join or subquery",
        ));
    }
    if mysql && multi {
        if p.update && fs.len() > 1 {
            if p.order.is_some() {
                return Err(SprigError::standard(
                    1221,
                    *b"HY000",
                    "Incorrect usage of UPDATE and ORDER BY",
                ));
            }
            if p.limit.is_some() {
                return Err(SprigError::standard(
                    1221,
                    *b"HY000",
                    "Incorrect usage of UPDATE and LIMIT",
                ));
            }
        }
        if !p.update
            && let Some(at) = p.tail_at
        {
            return Err(syntax_near(text, at));
        }
    }
    let ci = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    // The target factors, in the order first named.
    let mut chosen: Vec<usize> = Vec::new();
    let choose = |i: usize, chosen: &mut Vec<usize>| {
        if !chosen.contains(&i) {
            chosen.push(i);
        }
    };
    // Per assignment: (factor, column, value text).
    let mut sets: Vec<(usize, usize, &str)> = Vec::new();
    if p.update {
        for (target, value) in &p.sets {
            let (fi, col) = match target.as_slice() {
                [c] => {
                    let found: Vec<usize> = fs
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| f.def.as_ref().is_some_and(|(d, _)| d.column(c).is_some()))
                        .map(|(i, _)| i)
                        .collect();
                    match found.as_slice() {
                        [] => return Err(unknown_column(c, mysql)),
                        [i] => (*i, c),
                        _ => {
                            return Err(SprigError::standard(
                                1052,
                                *b"23000",
                                format!(
                                    "Column '{c}' in {} is ambiguous",
                                    if mysql { "field list" } else { "SET" }
                                ),
                            ));
                        }
                    }
                }
                [t, c] | [_, t, c] => {
                    let db = (target.len() == 3).then(|| &target[0]);
                    let Some(i) = fs.iter().position(|f| {
                        ci(&f.visible, t)
                            && db.is_none_or(|d| f.db.as_deref().is_some_and(|x| ci(x, d)))
                    }) else {
                        return Err(unknown_column(&target.join("."), mysql));
                    };
                    (i, c)
                }
                _ => return Err(not_on_engine("an assignment target")),
            };
            let Some((def, _)) = &fs[fi].def else {
                return Err(not_updatable(&fs[fi].visible, true));
            };
            let Some(ci_) = def.column(col) else {
                return Err(unknown_column(&target.join("."), mysql));
            };
            choose(fi, &mut chosen);
            sets.push((fi, ci_, value));
        }
    } else if p.listed {
        for t in &p.targets {
            let (db, name) = match t.as_slice() {
                [n] => (None, n),
                [d, n] => (Some(d), n),
                _ => return Err(not_on_engine("a DELETE target")),
            };
            let Some(i) = fs.iter().position(|f| {
                ci(&f.visible, name) && db.is_none_or(|d| f.db.as_deref().is_some_and(|x| ci(x, d)))
            }) else {
                return Err(SprigError::standard(
                    1109,
                    *b"42S02",
                    format!("Unknown table '{name}' in MULTI DELETE"),
                ));
            };
            if fs[i].def.is_none() {
                return Err(not_updatable(name, false));
            }
            choose(i, &mut chosen);
        }
    } else {
        if fs[0].def.is_none() {
            return Err(not_updatable(&fs[0].visible, false));
        }
        choose(0, &mut chosen);
    }
    // MySQL: a target table read again by a subquery.
    if mysql {
        for &fi in &chosen {
            let f = &fs[fi];
            let read = p
                .filter
                .into_iter()
                .chain(p.sets.iter().map(|(_, v)| *v))
                .any(|t| subquery_reads(t, &f.table, opts));
            if read {
                return Err(SprigError::standard(
                    1093,
                    *b"HY000",
                    format!(
                        "You can't specify target table '{}' for update in FROM clause",
                        f.visible
                    ),
                ));
            }
        }
    }
    Ok(Some(Resolved {
        p,
        fs,
        chosen,
        sets,
        multi,
    }))
}

/// [`bind`]'s second part: the `SELECT` and each target's one-row write.
#[allow(clippy::too_many_lines)] // the SELECT's items, then each target's write
fn plans(
    cx: &BindCtx<'_>,
    r: Resolved<'_>,
    opts: LexOptions,
    mysql: bool,
) -> Result<Option<MultiWrite>> {
    // MySQL: a later assignment reads an earlier one's new value.
    let chained = (mysql && r.p.update && r.fs.len() > 1).then(|| chained_values(&r, opts));
    let Resolved {
        p,
        fs,
        chosen,
        sets,
        ..
    } = r;
    let sets: Vec<(usize, usize, &str)> = match &chained {
        Some(c) => sets
            .iter()
            .zip(c)
            .map(|((f, col, _), v)| (*f, *col, v.as_str()))
            .collect(),
        None => sets,
    };
    let ci = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    // Which values the SELECT works out: every one with several tables;
    // with one, those holding a subquery (the rest run on the row).
    let hoist_all = fs.len() > 1;
    let mut hoisted: Vec<bool> = sets
        .iter()
        .map(|(_, _, v)| hoist_all || has_subquery(v, opts))
        .collect();
    let targets_and_select = |hoisted: &[bool]| -> Result<(Vec<Target>, String)> {
        let mut items: Vec<String> = Vec::new();
        let mut targets = Vec::new();
        let mut key_ranges = Vec::new();
        for &fi in &chosen {
            let f = &fs[fi];
            let Some((def, _)) = &f.def else {
                return Err(not_updatable(&f.visible, p.update));
            };
            let start = items.len();
            if def.hidden {
                // Its rows by their hidden row number.
                items.push(format!(
                    "{}.{}",
                    q(&f.visible),
                    q(super::general::names::HIDDEN_ROW)
                ));
            } else {
                for &c in &def.pk {
                    items.push(format!("{}.{}", q(&f.visible), q(&def.columns[c].name)));
                }
            }
            key_ranges.push(start..items.len());
        }
        let mut value_at = vec![usize::MAX; sets.len()];
        for (k, (_, _, v)) in sets.iter().enumerate() {
            if hoisted[k] {
                value_at[k] = items.len();
                items.push(format!("({v})"));
            }
        }
        for (n, &fi) in chosen.iter().enumerate() {
            let f = &fs[fi];
            let Some((def, db)) = &f.def else {
                return Err(not_updatable(&f.visible, p.update));
            };
            let alias = if ci(&f.visible, &f.table) {
                String::new()
            } else {
                format!(" AS {}", q(&f.visible))
            };
            // (A table keyed by a hidden row number: no condition here; the
            // write's access is set to that key below.)
            let key: Vec<String> = if def.hidden {
                Vec::new()
            } else {
                def.pk
                    .iter()
                    .map(|&c| format!("{} = ?", q(&def.columns[c].name)))
                    .collect()
            };
            let where_key = if key.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", key.join(" AND "))
            };
            let mut values = Vec::new();
            let sql = if p.update {
                let mut assigns = Vec::new();
                for (k, (sfi, col, v)) in sets.iter().enumerate() {
                    if *sfi != fi {
                        continue;
                    }
                    if hoisted[k] {
                        values.push(value_at[k]);
                        assigns.push(format!("{} = ?", q(&def.columns[*col].name)));
                    } else {
                        assigns.push(format!("{} = {v}", q(&def.columns[*col].name)));
                    }
                }
                format!(
                    "UPDATE {}{alias} SET {}{where_key}",
                    q(&def.name),
                    assigns.join(", "),
                )
            } else {
                format!("DELETE FROM {}{where_key}", q(&def.name))
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
            let Some(mut plan) = plan::bind(&cx2, &stmt)? else {
                return Err(SprigError::internal("a row write bound to no table"));
            };
            if def.hidden {
                // The row by its hidden row number, the last parameter.
                plan.access = plan::Access::Point(vec![super::expr::CExpr::Param(plan.params)]);
                plan.params += 1;
            }
            targets.push(Target {
                plan,
                keys: key_ranges[n].clone(),
                values,
            });
        }
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
        Ok((targets, select))
    };
    let (targets, select) = match targets_and_select(&hoisted) {
        // One table: an assignment the single-table path can't work out is
        // worked out by the SELECT instead.
        Err(e)
            if !hoist_all
                && e.code() == Some(sprig_common::SprigCode::NotOnEngine)
                && hoisted.iter().any(|h| !h) =>
        {
            hoisted.fill(true);
            targets_and_select(&hoisted)?
        }
        r => r?,
    };
    let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &select)
        .ok()
        .and_then(|mut v| v.pop())
        .ok_or_else(|| not_on_engine("a write's tables or conditions the planner can't parse"))?;
    let lock = plan::RowLock {
        exclusive: true,
        wait: sprig_engine::txn::Wait::Block,
    };
    let Some(mut g) = super::general::bind(cx, &stmt, Some(&select), Some(lock))? else {
        return Ok(None);
    };
    let objects: HashSet<_> = targets.iter().map(|t| t.plan.table.object).collect();
    g.lock_each = Some(
        g.tables
            .iter()
            .map(|t| objects.contains(&t.def.object))
            .collect(),
    );
    Ok(Some(MultiWrite {
        select: g,
        targets,
        update: p.update,
    }))
}

/// A multi-table `UPDATE` or `DELETE` (or `DELETE t FROM ...`) run row
/// by row by the session, for tables with SQL triggers: the `SELECT` that
/// finds every target's rows (all their columns) and works out the values.
#[derive(Debug)]
pub(crate) struct RowsSelect {
    /// An `UPDATE` (else a `DELETE`).
    pub update: bool,
    /// `SELECT <each target's columns>, <values> FROM ... WHERE ...`.
    pub select: String,
    /// The targets, in the order first named.
    pub targets: Vec<RowsTarget>,
    /// Every engine table the statement reads.
    pub tables: Vec<Arc<TableDef>>,
}

/// One target of a [`RowsSelect`].
#[derive(Debug)]
pub(crate) struct RowsTarget {
    pub def: Arc<TableDef>,
    pub db: DbId,
    /// Where its columns are in the `SELECT`'s rows.
    pub cols: Range<usize>,
    /// Its assignments: (column, where the value is in the rows).
    pub sets: Vec<(usize, usize)>,
}

/// MySQL 8.4's multi-table `UPDATE` values: the tables are written one
/// after another in their order in the statement, so an assignment reads
/// the new values of the earlier tables' columns and of its own table's
/// columns assigned before it: a reference to one (`t.c`, or a bare `c`
/// only one table has) is replaced by that assignment's value. MariaDB
/// reads every value from the rows as they were.
fn chained_values(r: &Resolved<'_>, opts: LexOptions) -> Vec<String> {
    let ci = |a: &str, b: &str| a.eq_ignore_ascii_case(b);
    let column_of = |fi: usize, name: &str| {
        r.fs[fi]
            .def
            .as_ref()
            .and_then(|(d, _)| d.columns.iter().position(|c| ci(&c.name, name)))
    };
    let mut done: Vec<(usize, usize, String)> = Vec::new();
    let mut out = vec![String::new(); r.sets.len()];
    // The tables are written in their order in the statement: one reads
    // the new values of the tables before it and of its own columns
    // assigned before, the old values of the tables after it.
    let mut order: Vec<usize> = (0..r.sets.len()).collect();
    order.sort_by_key(|&k| r.sets[k].0);
    for k in order {
        let (fi, col, v) = &r.sets[k];
        let value = match Toks::new(v, opts) {
            Some(k) if !done.is_empty() => {
                let mut s = String::with_capacity(v.len());
                let mut last = 0usize;
                let n = k.t.len();
                let mut i = 0;
                while i < n {
                    let ident = |j: usize| j < n && k.t[j].kind.is_ident_like();
                    let after_dot = i > 0 && k.punct(i - 1, b".");
                    if !ident(i) || after_dot || k.punct(i + 1, b"(") {
                        i += 1;
                        continue;
                    }
                    let name =
                        |j: usize| String::from_utf8_lossy(&ident_value(k.bytes(j))).into_owned();
                    let (target, end) =
                        if k.punct(i + 1, b".") && ident(i + 2) && !k.punct(i + 3, b".") {
                            let f = r.fs.iter().position(|f| ci(&f.visible, &name(i)));
                            (
                                f.and_then(|f| column_of(f, &name(i + 2)).map(|c| (f, c))),
                                i + 3,
                            )
                        } else if k.punct(i + 1, b".") {
                            (None, i + 1)
                        } else {
                            let mut hits = (0..r.fs.len())
                                .filter_map(|f| column_of(f, &name(i)).map(|c| (f, c)));
                            let one = hits.next();
                            (if hits.next().is_none() { one } else { None }, i + 1)
                        };
                    if let Some((f, c)) = target
                        && let Some((_, _, sub)) =
                            done.iter().find(|(df, dc, _)| *df == f && *dc == c)
                    {
                        s.push_str(&v[last..k.t[i].start as usize]);
                        s.push('(');
                        s.push_str(sub);
                        s.push(')');
                        last = k.t[end - 1].end as usize;
                    }
                    i = end;
                }
                s.push_str(&v[last..]);
                s
            }
            _ => (*v).to_owned(),
        };
        done.retain(|(df, dc, _)| !(df == fi && dc == col));
        done.push((*fi, *col, value.clone()));
        out[k] = value;
    }
    out
}

/// `text` as a [`RowsSelect`]; `None` when it isn't a multi-table write
/// of engine tables.
///
/// # Errors
/// As [`bind`].
pub(crate) fn rows_select(
    cx: &BindCtx<'_>,
    text: &str,
    opts: LexOptions,
    mysql: bool,
) -> Result<Option<RowsSelect>> {
    let Some(r) = resolve_parts(cx, text, opts, mysql)? else {
        return Ok(None);
    };
    if !r.multi {
        return Ok(None);
    }
    let mut items: Vec<String> = Vec::new();
    let mut targets = Vec::new();
    for &fi in &r.chosen {
        let f = &r.fs[fi];
        let Some((def, db)) = &f.def else {
            return Err(not_updatable(&f.visible, r.p.update));
        };
        let start = items.len();
        for c in &def.columns {
            items.push(format!("{}.{}", q(&f.visible), q(&c.name)));
        }
        targets.push(RowsTarget {
            def: Arc::clone(def),
            db: *db,
            cols: start..items.len(),
            sets: Vec::new(),
        });
    }
    // MySQL: a later assignment reads an earlier one's new value.
    let values: Vec<String> = if mysql && r.p.update {
        chained_values(&r, opts)
    } else {
        r.sets.iter().map(|(_, _, v)| (*v).to_owned()).collect()
    };
    for ((sfi, col, _), v) in r.sets.iter().zip(&values) {
        let Some(n) = r.chosen.iter().position(|c| c == sfi) else {
            continue;
        };
        targets[n].sets.push((*col, items.len()));
        items.push(format!("({v})"));
    }
    let mut select = format!("SELECT {} FROM {}", items.join(", "), r.p.refs);
    if let Some(w) = r.p.filter {
        select.push_str(" WHERE ");
        select.push_str(w);
    }
    if let Some(o) = r.p.order {
        select.push_str(" ORDER BY ");
        select.push_str(o);
    }
    if let Some(l) = r.p.limit {
        select.push_str(" LIMIT ");
        select.push_str(l);
    }
    Ok(Some(RowsSelect {
        update: r.p.update,
        select,
        targets,
        tables: r
            .fs
            .iter()
            .filter_map(|f| f.def.as_ref().map(|(d, _)| Arc::clone(d)))
            .collect(),
    }))
}

impl Engine {
    /// Runs `w` as one statement of the session's transaction `txn`: the
    /// `SELECT`, then each target row's write. On an error every write of
    /// the statement is undone and the transaction stays open.
    ///
    /// # Errors
    /// The statement's, or the engine's.
    pub(crate) async fn multi_write_in(
        &self,
        w: &MultiWrite,
        a: &RunArgs<'_>,
        txn: &mut SessionTxn,
        found_rows: bool,
    ) -> Result<Outcome> {
        let rows = self.general_run_in(&w.select, a, txn).await?;
        let Some(tx) = txn.open_tx() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        let mut seen: Vec<HashSet<String>> = vec![HashSet::new(); w.targets.len()];
        let mut matched = 0u64;
        let mut changed = 0u64;
        let mut conds: Vec<super::assign::Condition> = rows
            .conds
            .iter()
            .map(|c| super::assign::Condition {
                level: if c.level == sprig_exec::Level::Note {
                    super::assign::Level::Note
                } else {
                    super::assign::Level::Warning
                },
                code: c.code,
                message: c.message.clone(),
            })
            .collect();
        let mut params: Vec<Datum> = Vec::new();
        for row in &rows.rows {
            for (t, seen) in w.targets.iter().zip(seen.iter_mut()) {
                let key = &row[t.keys.clone()];
                // A row matched again (another row of a joined table) is
                // written once, with what its first match gave.
                if !seen.insert(format!("{key:?}")) {
                    continue;
                }
                params.clear();
                params.extend(t.values.iter().map(|&i| row[i].clone()));
                params.extend(key.iter().cloned());
                match self.run_part(a.tenant, &t.plan, &params, false, tx).await {
                    Ok(o) => {
                        matched += 1;
                        changed += o.affected;
                        conds.extend(o.conds);
                    }
                    Err(e) => {
                        tx.rollback_statement();
                        return Err(e);
                    }
                }
            }
        }
        if let Err(e) = tx.gate().await {
            tx.rollback_statement();
            return Err(e);
        }
        let warnings = conds.len();
        Ok(Outcome {
            rows: Vec::new(),
            affected: if w.update && found_rows {
                matched
            } else {
                changed
            },
            last_insert_id: 0,
            reported_id: 0,
            info: w.update.then(|| {
                format!("Rows matched: {matched}  Changed: {changed}  Warnings: {warnings}")
            }),
            no_index: rows.no_index || rows.full_scan,
            examined: rows.examined,
            conds,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(s: &str) -> Parts<'_> {
        split(s, LexOptions::mariadb()).unwrap()
    }

    #[test]
    fn statement_parts() {
        let p =
            parts("UPDATE a JOIN b ON b.x = a.id SET a.v = b.w, w = (SELECT 1) WHERE a.id > 1;");
        assert!(p.update);
        assert_eq!(p.refs, "a JOIN b ON b.x = a.id");
        assert_eq!(p.sets[0], (vec!["a".to_owned(), "v".to_owned()], "b.w"));
        assert_eq!(p.sets[1], (vec!["w".to_owned()], "(SELECT 1)"));
        assert_eq!(p.filter, Some("a.id > 1"));
        let p = parts(
            "UPDATE `t` SET v = v + 1 WHERE id IN (SELECT id FROM u ORDER BY id) ORDER BY id DESC LIMIT 2",
        );
        assert_eq!(p.refs, "`t`");
        assert_eq!(p.filter, Some("id IN (SELECT id FROM u ORDER BY id)"));
        assert_eq!(p.order, Some("id DESC"));
        assert_eq!(p.limit, Some("2"));
        let p = parts("DELETE a, b.* FROM a JOIN b ON b.x = a.id WHERE b.id = 1");
        assert!(!p.update && p.listed);
        assert_eq!(p.targets, vec![vec!["a".to_owned()], vec!["b".to_owned()]]);
        assert_eq!(p.refs, "a JOIN b ON b.x = a.id");
        let p = parts("DELETE FROM b USING a AS a JOIN b AS b ON b.aid = a.id WHERE a.id = 2");
        assert_eq!(p.targets, vec![vec!["b".to_owned()]]);
        assert_eq!(p.refs, "a AS a JOIN b AS b ON b.aid = a.id");
        let p = parts("DELETE FROM t WHERE id IN (SELECT id FROM t) ORDER BY id LIMIT 1");
        assert!(!p.listed);
        assert_eq!(p.refs, "t");
        assert!(p.tail_at.is_some());
        let p = parts("UPDATE IGNORE a, b SET a.v = 1");
        assert!(p.ignore);
        assert_eq!(p.refs, "a, b");
        assert!(split("SELECT 1", LexOptions::mariadb()).is_none());
        assert!(split("DELETE FROM t RETURNING id", LexOptions::mariadb()).is_none());
    }

    #[test]
    fn subqueries_reading_a_table() {
        let o = LexOptions::mariadb();
        assert!(subquery_reads(
            "id IN (SELECT id FROM t WHERE v > 1)",
            "t",
            o
        ));
        assert!(subquery_reads("id IN (SELECT id FROM u, db.T)", "t", o));
        assert!(!subquery_reads(
            "id IN (SELECT id FROM u WHERE t = 1)",
            "t",
            o
        ));
        assert!(!subquery_reads("t.id = 1", "t", o));
        assert!(has_subquery("1 + (SELECT 2)", o));
        assert!(!has_subquery("a.v + 1", o));
    }
}
