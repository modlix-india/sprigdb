//! Calls over row values bound by their answers: in a `SELECT`'s list,
//! `WHERE` and `ORDER BY`, and in a single-table `UPDATE ... SET`.
//!
//! The gateway reads each call's distinct inputs first, through the
//! caller's own session, answers them, and runs the statement with every
//! call replaced by a lookup of its answers keyed by the SHA-256 of its
//! input ([`case_of`]):
//!
//! ```text
//! row inputs     (WHERE; inside an aggregate or window function; UPDATE ... SET)
//!   SELECT:  [WITH ...] SELECT DISTINCT SHA2((x),256), (x), ... [FROM refs]
//!                       [WHERE the conditions AND-ed beside the calls']
//!   UPDATE:  SELECT SHA2((x),256), (x), ... FROM ref [WHERE ...] [ORDER BY ... LIMIT n] FOR UPDATE
//! output inputs  (the rest of the SELECT list and ORDER BY), after the row inputs are bound:
//!            the statement itself, its WHERE bound, those calls NULL, and
//!            SHA2((x),256), (x) appended to its list; without ORDER BY and
//!            LIMIT when they would order or cut on an answer
//! the statement  every call replaced by CASE SHA2((x),256) WHEN '<hash>' THEN <answer> ... END
//! ```
//!
//! An input that isn't among the answers (a row that changed between the
//! reads outside a transaction) is `NULL`.

use std::ops::Range;

use super::{Call, ColumnAt, Outer, Plan, Toks, is_wildcard, not_here, string_alias};
use crate::lex::TokenKind;
use crate::names::NameError;

/// When a bound call's inputs are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Over the rows the statement reads, before it filters, groups or
    /// cuts them: `WHERE`, inside an aggregate or window function, and
    /// `UPDATE ... SET`.
    Row,
    /// Over the rows the statement returns: the rest of the `SELECT` list
    /// and `ORDER BY`.
    Output,
}

/// How a statement with bound calls is read and run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bind {
    /// An `UPDATE` (its inputs read `FOR UPDATE`), else a `SELECT`.
    pub update: bool,
    /// The text before the outermost `SELECT` (a `WITH` clause).
    prefix: Range<usize>,
    /// The table references: after `FROM`, or the `UPDATE`'s table.
    from: Option<Range<usize>>,
    /// The `WHERE` condition.
    cond: Option<Range<usize>>,
    /// The `WHERE` condition's parts joined by `AND` that call no AI
    /// function.
    kept: Vec<Range<usize>>,
    /// An `UPDATE`'s `ORDER BY` and `LIMIT`.
    tail: Option<Range<usize>>,
    /// The `UPDATE` has its own `LIMIT`.
    limited: bool,
    /// Where the outermost `SELECT` list ends.
    list_end: usize,
    /// What the output inputs' read leaves out (`ORDER BY`, `LIMIT`).
    strip: Vec<Range<usize>>,
    /// `SELECT` list items holding a bound call, without an alias: their
    /// column and the name the item's text gives it.
    pub names: Vec<(ColumnAt, String)>,
}

/// Aggregate and window functions: a call inside one is read over the
/// rows, before grouping and windows.
const OVER_ROWS: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "MEDIAN",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "LAG",
    "LEAD",
];

/// A top-level clause of the outermost `SELECT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Clause {
    From,
    Where,
    Group,
    Having,
    Window,
    Order,
    Limit,
    Lock,
    Into,
    Compound,
}

/// The clauses after the outermost `SELECT` list, keyword included, by
/// token index.
pub(super) fn clauses(tk: &Toks<'_>, from: usize) -> Vec<(Clause, Range<usize>)> {
    let mut out: Vec<(Clause, Range<usize>)> = Vec::new();
    let mut open: Option<(Clause, usize)> = None;
    let mut end = tk.t.len();
    for k in from..tk.t.len() {
        if tk.depth[k] != 0 {
            continue;
        }
        if tk.punct(k, b';') {
            end = k;
            break;
        }
        let Some(w) = tk.word(k) else { continue };
        let cur = open.map(|(c, _)| c);
        let c = match w.as_str() {
            "FROM" => Clause::From,
            "WHERE" => Clause::Where,
            "GROUP" if tk.is_word(k + 1, "BY") => Clause::Group,
            "HAVING" => Clause::Having,
            "WINDOW" => Clause::Window,
            "ORDER" if tk.is_word(k + 1, "BY") => Clause::Order,
            "LIMIT" => Clause::Limit,
            "OFFSET" if cur == Some(Clause::Order) => Clause::Limit,
            "FETCH" if cur != Some(Clause::Limit) => Clause::Limit,
            "FOR" if tk.is_word(k + 1, "UPDATE") || tk.is_word(k + 1, "SHARE") => Clause::Lock,
            "LOCK" if tk.is_word(k + 1, "IN") => Clause::Lock,
            "INTO" => Clause::Into,
            "UNION" | "EXCEPT" | "INTERSECT" => Clause::Compound,
            _ => continue,
        };
        if let Some((c0, s)) = open.take() {
            out.push((c0, s..k));
        }
        open = Some((c, k));
    }
    if let Some((c, s)) = open {
        out.push((c, s..end));
    }
    out
}

/// The parts of condition `r` (token indexes) joined by `AND` at its top
/// level (not `BETWEEN ... AND`, not inside `CASE ... END`).
fn conjuncts(tk: &Toks<'_>, r: Range<usize>) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    if r.is_empty() {
        return out;
    }
    let d0 = tk.depth[r.start];
    let mut from = r.start;
    let mut between = 0u32;
    let mut case = 0u32;
    for i in r.clone() {
        if tk.depth[i] != d0 {
            continue;
        }
        let and = tk.is_word(i, "AND")
            || (tk.t[i].kind == TokenKind::Op && tk.t[i].bytes(tk.sql) == b"&&");
        match tk.word(i).as_deref() {
            Some("CASE") => case += 1,
            Some("END") if case > 0 => case -= 1,
            Some("BETWEEN") => between += 1,
            _ if and && case == 0 => {
                if between > 0 {
                    between -= 1;
                } else {
                    out.push(from..i);
                    from = i + 1;
                }
            }
            _ => {}
        }
    }
    out.push(from..r.end);
    out.retain(|p| !p.is_empty());
    out
}

/// Whether token `i` is inside a subquery or derived table.
fn in_subquery(tk: &Toks<'_>, i: usize) -> bool {
    let mut p = tk.parent[i];
    while let Some(o) = p {
        if tk.is_word(o + 1, "SELECT") || tk.is_word(o + 1, "WITH") || tk.is_word(o + 1, "VALUES") {
            return true;
        }
        p = tk.parent[o];
    }
    false
}

/// Whether token `i` is inside an aggregate or window function's
/// arguments (or its `OVER (...)`).
fn in_rows_function(tk: &Toks<'_>, i: usize) -> bool {
    let mut p = tk.parent[i];
    while let Some(o) = p {
        if o > 0
            && let Some(w) = tk.word(o - 1)
            && (OVER_ROWS.contains(&w.as_str()) || w == "OVER")
        {
            return true;
        }
        p = tk.parent[o];
    }
    false
}

fn bytes(tk: &Toks<'_>, r: &Range<usize>) -> Option<Range<usize>> {
    (!r.is_empty()).then(|| tk.start(r.start)..tk.end(r.end - 1))
}

/// The identifiers an expression reads (lower case; the last part of a
/// qualified name), not function names.
fn idents(tk: &Toks<'_>, r: impl Iterator<Item = usize>) -> Vec<String> {
    r.filter(|&i| tk.t[i].kind.is_ident_like() && !tk.punct(i + 1, b'(') && !tk.punct(i + 1, b'.'))
        .filter_map(|i| tk.ident(i))
        .map(|s| s.to_lowercase())
        .collect()
}

/// An item's alias: `AS name`, or a name right after a closing `)`.
fn alias_of(tk: &Toks<'_>, r: &Range<usize>) -> Option<String> {
    if r.end - r.start < 2 {
        return None;
    }
    let last = r.end - 1;
    let name = tk.ident(last).or_else(|| string_alias(tk, last))?;
    (tk.is_word(last - 1, "AS") || tk.punct(last - 1, b')')).then_some(name)
}

/// The name of the first call over rows in item `r` (token indexes).
fn first_call(calls: &[Call], spans: &[(usize, usize)], r: &Range<usize>) -> &'static str {
    calls
        .iter()
        .zip(spans)
        .find(|(c, s)| !c.constant() && r.contains(&s.0))
        .map_or("AI_COMPLETE", |(c, _)| c.name)
}

/// Plans a `SELECT` (`WITH ... SELECT`) whose calls over rows are bound:
/// each such call's level, or the 1235 that refuses it.
#[allow(clippy::too_many_lines)] // where each call stands, then the items, then the reads
pub(super) fn select(
    tk: &Toks<'_>,
    o: &Outer,
    calls: &[Call],
    spans: &[(usize, usize)],
) -> Result<(Bind, Vec<Option<Level>>), NameError> {
    let part = |c: Clause| {
        o.parts
            .iter()
            .find(|(k, _)| *k == c)
            .map(|(_, r)| r.clone())
    };
    let clause_of = |i: usize| {
        o.parts
            .iter()
            .find(|(_, r)| r.contains(&i))
            .map(|(c, _)| *c)
    };
    let mut levels = vec![None; calls.len()];
    // Which items hold a bound call, and whether an output one.
    let mut item_has: Vec<Option<bool>> = vec![None; o.items.len()];
    let mut order_output = false;
    for (k, c) in calls.iter().enumerate() {
        if c.constant() {
            continue;
        }
        let at = spans[k].0;
        let n = c.name;
        if o.parts.iter().any(|(k, _)| *k == Clause::Compound) {
            return Err(not_here(n, "in a UNION, EXCEPT or INTERSECT"));
        }
        if o.parts.iter().any(|(k, _)| *k == Clause::Into) {
            return Err(not_here(n, "with SELECT ... INTO"));
        }
        if at < o.sel || in_subquery(tk, at) {
            return Err(not_here(n, "inside a subquery or derived table"));
        }
        let rows = in_rows_function(tk, at);
        let level = if at < o.list_end {
            let Some(item) = o.items.iter().position(|r| r.contains(&at)) else {
                return Err(not_here(n, "here"));
            };
            let out = !rows;
            item_has[item] = Some(item_has[item].unwrap_or(false) | out);
            if rows { Level::Row } else { Level::Output }
        } else {
            match clause_of(at) {
                Some(Clause::Where) => Level::Row,
                Some(Clause::Order) => {
                    order_output |= !rows;
                    if rows { Level::Row } else { Level::Output }
                }
                Some(Clause::From) => return Err(not_here(n, "in FROM or a join condition")),
                Some(Clause::Group | Clause::Having | Clause::Window) => {
                    return Err(not_here(n, "in GROUP BY, HAVING or WINDOW"));
                }
                _ => return Err(not_here(n, "here")),
            }
        };
        levels[k] = Some(level);
    }
    // The items holding calls: named by alias or position in GROUP BY,
    // HAVING or WINDOW (refused) or ORDER BY (read without its order).
    let mut names = Vec::new();
    let mut order_names_output = false;
    for (n, has) in item_has.iter().enumerate() {
        let Some(output) = *has else { continue };
        let r = &o.items[n];
        let alias = alias_of(tk, r);
        let named_in = |c: Clause| {
            o.parts.iter().filter(|(k, _)| *k == c).any(|(_, cr)| {
                cr.clone().any(|k| {
                    let t = &tk.t[k];
                    (t.kind == TokenKind::Number
                        && t.bytes(tk.sql) == (n + 1).to_string().as_bytes()
                        && (tk.is_word(k.wrapping_sub(1), "BY")
                            || tk.punct(k.wrapping_sub(1), b','))
                        && !o.items[..n].iter().any(|r| is_wildcard(tk, r)))
                        || alias.as_ref().is_some_and(|a| {
                            tk.ident(k).is_some_and(|x| x.eq_ignore_ascii_case(a))
                                && !tk.punct(k + 1, b'.')
                                && !tk.punct(k.wrapping_sub(1), b'.')
                                && !tk.punct(k + 1, b'(')
                        })
                })
            })
        };
        if [Clause::Group, Clause::Having, Clause::Window]
            .into_iter()
            .any(&named_in)
        {
            return Err(not_here(
                first_call(calls, spans, r),
                "when GROUP BY, HAVING or WINDOW names its column",
            ));
        }
        order_names_output |= output && named_in(Clause::Order);
        if alias.is_none() {
            let before = o.items[..n].iter().any(|r| is_wildcard(tk, r));
            let after = o.items[n + 1..].iter().any(|r| is_wildcard(tk, r));
            let column = match (before, after) {
                (false, _) => ColumnAt::FromStart(n),
                (true, false) => ColumnAt::FromEnd(o.items.len() - 1 - n),
                (true, true) => {
                    return Err(not_here(
                        first_call(calls, spans, r),
                        "between two * items without an alias",
                    ));
                }
            };
            if let Some(b) = bytes(tk, r) {
                names.push((column, String::from_utf8_lossy(&tk.sql[b]).into_owned()));
            }
        }
    }
    let limit = part(Clause::Limit);
    let strip = if order_output || order_names_output || (o.distinct && limit.is_some()) {
        [part(Clause::Order), limit]
            .into_iter()
            .flatten()
            .filter_map(|r| bytes(tk, &r))
            .collect()
    } else {
        Vec::new()
    };
    let cond_toks = part(Clause::Where).map(|r| r.start + 1..r.end);
    let kept: Vec<Range<usize>> = cond_toks
        .clone()
        .map(|r| {
            conjuncts(tk, r)
                .into_iter()
                .filter(|p| {
                    !calls
                        .iter()
                        .zip(spans)
                        .any(|(c, s)| !c.constant() && p.contains(&s.0))
                })
                .filter_map(|p| bytes(tk, &p))
                .collect()
        })
        .unwrap_or_default();
    let list_end = if o.list_end < tk.t.len() {
        tk.start(o.list_end)
    } else {
        tk.t.last().map_or(0, |t| t.end as usize)
    };
    Ok((
        Bind {
            update: false,
            prefix: 0..tk.start(o.sel),
            from: part(Clause::From).and_then(|r| bytes(tk, &(r.start + 1..r.end))),
            cond: cond_toks.and_then(|r| bytes(tk, &r)),
            kept,
            tail: None,
            limited: false,
            list_end,
            strip,
            names,
        },
        levels,
    ))
}

/// A single-table `UPDATE`'s parts, by token index.
pub(super) struct Update {
    refs: Range<usize>,
    multi: bool,
    /// Each assignment's target and value.
    assigns: Vec<(Range<usize>, Range<usize>)>,
    cond: Option<Range<usize>>,
    tail: Option<Range<usize>>,
}

/// Splits an `UPDATE`; `None` when it has no `SET`.
pub(super) fn update_parts(tk: &Toks<'_>) -> Option<Update> {
    let mut i = 1;
    while tk.is_word(i, "LOW_PRIORITY") || tk.is_word(i, "IGNORE") {
        i += 1;
    }
    let set = (i..tk.t.len()).find(|&k| tk.depth[k] == 0 && tk.is_word(k, "SET"))?;
    let refs = i..set;
    let multi = refs.clone().any(|k| {
        tk.depth[k] == 0
            && (tk.punct(k, b',') || tk.is_word(k, "JOIN") || tk.is_word(k, "STRAIGHT_JOIN"))
    });
    let mut end = tk.t.len();
    let mut marks: Vec<(&str, usize)> = Vec::new();
    for k in set + 1..tk.t.len() {
        if tk.depth[k] != 0 {
            continue;
        }
        if tk.punct(k, b';') {
            end = k;
            break;
        }
        match tk.word(k).as_deref() {
            Some("WHERE") if marks.is_empty() => marks.push(("WHERE", k)),
            Some("ORDER") if tk.is_word(k + 1, "BY") && !marks.iter().any(|m| m.0 != "WHERE") => {
                marks.push(("TAIL", k));
            }
            Some("LIMIT") if !marks.iter().any(|m| m.0 == "TAIL") => marks.push(("TAIL", k)),
            _ => {}
        }
    }
    let set_end = marks.first().map_or(end, |m| m.1);
    let mut assigns = Vec::new();
    let mut from = set + 1;
    for k in set + 1..=set_end {
        if k == set_end || (tk.depth[k] == 0 && tk.punct(k, b',')) {
            let a = from..k;
            let eq = a.clone().find(|&j| {
                tk.depth[j] == 0 && tk.t[j].kind == TokenKind::Op && tk.t[j].bytes(tk.sql) == b"="
            });
            if let Some(eq) = eq {
                assigns.push((a.start..eq, eq + 1..a.end));
            }
            from = k + 1;
        }
    }
    let where_at = marks.iter().find(|m| m.0 == "WHERE").map(|m| m.1);
    let tail_at = marks.iter().find(|m| m.0 == "TAIL").map(|m| m.1);
    let cond = where_at.map(|w| w + 1..tail_at.unwrap_or(end));
    let tail = tail_at.map(|t| t..end);
    Some(Update {
        refs,
        multi,
        assigns,
        cond,
        tail,
    })
}

/// Plans a single-table `UPDATE` whose `SET` calls over rows: every such
/// call is read over the rows the `UPDATE` matches.
pub(super) fn update(
    tk: &Toks<'_>,
    u: &Update,
    calls: &[Call],
    spans: &[(usize, usize)],
) -> Result<(Bind, Vec<Option<Level>>), NameError> {
    let mut levels = vec![None; calls.len()];
    for (k, c) in calls.iter().enumerate() {
        if c.constant() {
            continue;
        }
        let at = spans[k].0;
        let n = c.name;
        if u.multi {
            return Err(not_here(n, "in a multiple-table UPDATE"));
        }
        if in_subquery(tk, at) {
            return Err(not_here(n, "inside a subquery"));
        }
        let Some(a) = u.assigns.iter().position(|(_, v)| v.contains(&at)) else {
            return Err(not_here(n, "in an UPDATE's WHERE, ORDER BY or LIMIT"));
        };
        // MariaDB assigns left to right: an input reading a column an
        // earlier assignment sets would see the new value.
        let set_before: Vec<String> = u.assigns[..a]
            .iter()
            .filter_map(|(t, _)| t.clone().rev().find_map(|j| tk.ident(j)))
            .map(|s| s.to_lowercase())
            .collect();
        let input_toks = (spans[k].0 + 2..spans[k].1).filter(|&j| c.input.contains(&tk.start(j)));
        if idents(tk, input_toks)
            .iter()
            .any(|i| set_before.contains(i))
        {
            return Err(not_here(
                n,
                "reading a column an earlier assignment of the same SET changes",
            ));
        }
        levels[k] = Some(Level::Row);
    }
    Ok((
        Bind {
            update: true,
            prefix: 0..0,
            from: bytes(tk, &u.refs),
            cond: u.cond.as_ref().and_then(|r| bytes(tk, r)),
            kept: Vec::new(),
            tail: u.tail.as_ref().and_then(|r| bytes(tk, r)),
            limited: u
                .tail
                .clone()
                .is_some_and(|mut r| r.any(|k| tk.depth[k] == 0 && tk.is_word(k, "LIMIT"))),
            list_end: 0,
            strip: Vec::new(),
            names: Vec::new(),
        },
        levels,
    ))
}

/// The key of an input: its SHA-256 as MariaDB writes it.
fn hash_of(x: &str) -> String {
    format!("SHA2(({x}), 256)")
}

/// Branches above which [`case_of`] looks up in two steps.
const FLAT_BRANCHES: usize = 64;

/// The lookup that replaces a bound call: `answers` are each distinct
/// input's SHA-256 (as `SHA2(x, 256)` gives it) and its answer as a SQL
/// literal. An input with no answer is `NULL`. Past [`FLAT_BRANCHES`],
/// the lookup goes by the hash's first two digits first, so MariaDB
/// compares a few dozen hashes per row, not thousands.
#[must_use]
pub fn case_of(input: &str, answers: &[(String, String)]) -> String {
    use std::fmt::Write as _;
    if answers.is_empty() {
        return "NULL".to_owned();
    }
    let h = hash_of(input);
    let flat = |list: &[&(String, String)]| {
        let mut s = format!("CASE {h}");
        for (k, v) in list {
            let _ = write!(s, " WHEN '{k}' THEN {v}");
        }
        s.push_str(" END");
        s
    };
    if answers.len() <= FLAT_BRANCHES {
        return format!("({})", flat(&answers.iter().collect::<Vec<_>>()));
    }
    let mut buckets: std::collections::BTreeMap<&str, Vec<&(String, String)>> =
        std::collections::BTreeMap::new();
    for a in answers {
        buckets
            .entry(a.0.get(..2).unwrap_or(""))
            .or_default()
            .push(a);
    }
    let mut s = format!("(CASE LEFT({h}, 2)");
    for (p, list) in &buckets {
        let _ = write!(s, " WHEN '{p}' THEN {}", flat(list));
    }
    s.push_str(" END)");
    s
}

impl Plan {
    /// The distinct inputs (as written) of the calls bound at `level`, and
    /// for every call the index of its input among them.
    #[must_use]
    pub fn bound_inputs(&self, text: &[u8], level: Level) -> (Vec<String>, Vec<Option<usize>>) {
        let mut inputs: Vec<String> = Vec::new();
        let at = self
            .calls
            .iter()
            .map(|c| {
                if c.level != Some(level) {
                    return None;
                }
                let x = String::from_utf8_lossy(&text[c.input.clone()]).into_owned();
                Some(inputs.iter().position(|i| *i == x).unwrap_or_else(|| {
                    inputs.push(x);
                    inputs.len() - 1
                }))
            })
            .collect();
        (inputs, at)
    }

    /// The read of the row inputs: for each of [`Self::bound_inputs`]'s
    /// row inputs, its hash and value, at most `cap` rows (unless an
    /// `UPDATE`'s own `LIMIT` says fewer). `None` when there are none.
    #[must_use]
    pub fn row_keys_sql(&self, text: &[u8], cap: usize) -> Option<String> {
        use std::fmt::Write as _;
        let b = self.bind.as_ref()?;
        let (inputs, _) = self.bound_inputs(text, Level::Row);
        if inputs.is_empty() {
            return None;
        }
        let s = |r: &Range<usize>| String::from_utf8_lossy(&text[r.clone()]).into_owned();
        let pairs: Vec<String> = inputs
            .iter()
            .map(|x| format!("{}, ({x})", hash_of(x)))
            .collect();
        let mut sql = s(&b.prefix);
        sql.push_str(if b.update {
            "SELECT "
        } else {
            "SELECT DISTINCT "
        });
        sql.push_str(&pairs.join(", "));
        if let Some(f) = &b.from {
            sql.push_str(" FROM ");
            sql.push_str(&s(f));
        }
        if b.update {
            if let Some(c) = &b.cond {
                sql.push_str(" WHERE ");
                sql.push_str(&s(c));
            }
            if let Some(t) = &b.tail {
                sql.push(' ');
                sql.push_str(&s(t));
            }
            if !b.limited {
                let _ = write!(sql, " LIMIT {cap}");
            }
            sql.push_str(" FOR UPDATE");
        } else {
            if !b.kept.is_empty() {
                let kept: Vec<String> = b.kept.iter().map(|r| format!("({})", s(r))).collect();
                sql.push_str(" WHERE ");
                sql.push_str(&kept.join(" AND "));
            }
            let _ = write!(sql, " LIMIT {cap}");
        }
        Some(sql)
    }

    /// The read of the output inputs: the statement with the row calls
    /// bound (`values`, as for [`Plan::render`]), the output calls `NULL`,
    /// and each output input's hash and value appended to its list (the
    /// last columns, two per input). `None` when there are none.
    #[must_use]
    pub fn output_keys_sql(&self, text: &[u8], values: &[String]) -> Option<String> {
        use std::fmt::Write as _;
        let b = self.bind.as_ref()?;
        let (inputs, _) = self.bound_inputs(text, Level::Output);
        if inputs.is_empty() || b.update {
            return None;
        }
        let mut edits: Vec<(Range<usize>, String)> = Vec::new();
        for (c, v) in self.calls.iter().zip(values) {
            match c.level {
                Some(Level::Output) => edits.push((c.range.clone(), "NULL".to_owned())),
                Some(Level::Row) => edits.push((c.range.clone(), v.clone())),
                None if c.constant() => edits.push((c.range.clone(), v.clone())),
                None => {}
            }
        }
        let mut extra = String::new();
        for x in &inputs {
            let _ = write!(extra, ", {}, ({x})", hash_of(x));
        }
        edits.push((b.list_end..b.list_end, format!("{extra} ")));
        for r in &b.strip {
            edits.push((r.clone(), String::new()));
        }
        // Calls inside a stripped clause go with it; the appended items
        // come before a clause stripped at the same place.
        edits.sort_by_key(|(r, _)| (r.start, r.end > r.start, std::cmp::Reverse(r.end)));
        let mut out = Vec::with_capacity(text.len() + extra.len());
        let mut at = 0;
        for (r, v) in edits {
            if r.start < at {
                continue;
            }
            out.extend_from_slice(&text[at..r.start]);
            out.extend_from_slice(v.as_bytes());
            at = r.end;
        }
        out.extend_from_slice(&text[at..]);
        Some(String::from_utf8_lossy(&out).into_owned())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::ai_fn::analyze;
    use crate::lex::LexOptions;

    fn plan(sql: &str) -> Plan {
        analyze(sql.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .unwrap()
    }

    fn err(sql: &str) -> String {
        let e = analyze(sql.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .unwrap_err();
        assert_eq!(e.errno(), 1235, "{sql}: {e}");
        e.to_string()
    }

    fn levels(p: &Plan) -> Vec<Option<Level>> {
        p.calls.iter().map(|c| c.level).collect()
    }

    #[test]
    fn where_calls_are_read_over_the_rows() {
        let sql = "SELECT id, body FROM tickets t WHERE t.open = 1 AND price BETWEEN 1 AND 5 \
                   AND AI_CLASSIFY(body, 'bug', 'idea') = 'bug' ORDER BY id LIMIT 10";
        let p = plan(sql);
        assert_eq!(levels(&p), [Some(Level::Row)]);
        assert_eq!(
            p.row_keys_sql(sql.as_bytes(), 9).unwrap(),
            "SELECT DISTINCT SHA2((body), 256), (body) FROM tickets t \
             WHERE (t.open = 1) AND (price BETWEEN 1 AND 5) LIMIT 9"
        );
        assert!(
            p.output_keys_sql(sql.as_bytes(), &[String::new()])
                .is_none()
        );
        assert_eq!(
            p.render(sql.as_bytes(), &["(CASE ...)".into()]),
            "SELECT id, body FROM tickets t WHERE t.open = 1 AND price BETWEEN 1 AND 5 \
                   AND (CASE ...) = 'bug' ORDER BY id LIMIT 10"
        );
        assert!(p.bind.as_ref().unwrap().names.is_empty());
        // An OR keeps nothing: every row's input is read.
        let sql =
            "WITH x AS (SELECT * FROM t) SELECT * FROM x WHERE a = 1 OR AI_SUMMARIZE(b) = 'c'";
        assert_eq!(
            plan(sql).row_keys_sql(sql.as_bytes(), 9).unwrap(),
            "WITH x AS (SELECT * FROM t) SELECT DISTINCT SHA2((b), 256), (b) FROM x LIMIT 9"
        );
    }

    #[test]
    fn list_calls_inside_expressions_are_read_from_the_result() {
        let sql = "SELECT id, UPPER(AI_SUMMARIZE(body, 5)), AI_CLASSIFY(body, 'a', 'b') AS k \
                   FROM t WHERE AI_CLASSIFY(title, 'x', 'y') = 'x' ORDER BY id DESC LIMIT 3";
        let p = plan(sql);
        assert_eq!(
            levels(&p),
            [Some(Level::Output), Some(Level::Output), Some(Level::Row)]
        );
        let (inputs, at) = p.bound_inputs(sql.as_bytes(), Level::Output);
        assert_eq!(inputs, ["body"]);
        assert_eq!(at, [Some(0), Some(0), None]);
        assert_eq!(
            p.output_keys_sql(
                sql.as_bytes(),
                &[String::new(), String::new(), "'B'".into()]
            )
            .unwrap(),
            "SELECT id, UPPER(NULL), NULL AS k \
                   , SHA2((body), 256), (body) FROM t WHERE 'B' = 'x' ORDER BY id DESC LIMIT 3"
        );
        let b = p.bind.as_ref().unwrap();
        assert_eq!(
            b.names,
            [(
                ColumnAt::FromStart(1),
                "UPPER(AI_SUMMARIZE(body, 5))".to_owned()
            )]
        );
        // Ordered by an answer: read without ORDER BY and LIMIT.
        let sql = "SELECT id, AI_SUMMARIZE(body) s FROM t ORDER BY s LIMIT 3";
        let p = plan(sql);
        assert_eq!(
            p.output_keys_sql(sql.as_bytes(), &[String::new()]).unwrap(),
            "SELECT id, NULL s , SHA2((body), 256), (body) FROM t  "
        );
        let sql = "SELECT DISTINCT AI_CLASSIFY(body, 'a', 'b') FROM t LIMIT 2";
        assert_eq!(
            plan(sql)
                .output_keys_sql(sql.as_bytes(), &[String::new()])
                .unwrap(),
            "SELECT DISTINCT NULL , SHA2((body), 256), (body) FROM t "
        );
        // Inside an aggregate: over the rows.
        let sql = "SELECT cat, SUM(AI_CLASSIFY(body, 'a', 'b') = 'a') n, \
                   AI_SUMMARIZE(GROUP_CONCAT(body)) FROM t GROUP BY cat";
        let p = plan(sql);
        assert_eq!(levels(&p), [Some(Level::Row), Some(Level::Output)]);
        assert_eq!(
            p.row_keys_sql(sql.as_bytes(), 9).unwrap(),
            "SELECT DISTINCT SHA2((body), 256), (body) FROM t LIMIT 9"
        );
    }

    #[test]
    fn updates_read_their_rows_for_update() {
        let sql = "UPDATE LOW_PRIORITY tickets SET gist = AI_SUMMARIZE(body, 20), \
                   kind = COALESCE(AI_CLASSIFY(CONCAT(title, body), 'bug', 'idea'), 'other'), n = n + 1 \
                   WHERE id > 3 ORDER BY id LIMIT 50";
        let p = plan(sql);
        assert_eq!(levels(&p), [Some(Level::Row), Some(Level::Row)]);
        assert_eq!(
            p.row_keys_sql(sql.as_bytes(), 9).unwrap(),
            "SELECT SHA2((body), 256), (body), SHA2((CONCAT(title, body)), 256), (CONCAT(title, body)) \
             FROM tickets WHERE id > 3 ORDER BY id LIMIT 50 FOR UPDATE"
        );
        let p = plan("UPDATE t SET s = AI_SUMMARIZE(s)");
        assert!(p.bind.as_ref().unwrap().update);
        let sql = "UPDATE t AS x SET x.e = AI_EMBED(x.name)";
        assert_eq!(
            plan(sql).row_keys_sql(sql.as_bytes(), 9).unwrap(),
            "SELECT SHA2((x.name), 256), (x.name) FROM t AS x LIMIT 9 FOR UPDATE"
        );
    }

    #[test]
    fn what_stays_refused() {
        assert!(err("UPDATE t SET a = 1 WHERE AI_CLASSIFY(b, 'x', 'y') = 'x'").contains("WHERE"));
        assert!(err("UPDATE t, u SET t.a = AI_SUMMARIZE(u.b)").contains("multiple-table"));
        assert!(
            err("UPDATE t JOIN u ON t.id = u.id SET t.a = AI_SUMMARIZE(u.b)")
                .contains("multiple-table")
        );
        assert!(
            err("UPDATE t SET body = 'x', s = AI_SUMMARIZE(body)").contains("earlier assignment")
        );
        assert!(err("DELETE FROM t WHERE AI_CLASSIFY(b, 'x', 'y') = 'x'").contains("DELETE"));
        assert!(err("INSERT INTO s SELECT AI_SUMMARIZE(body) FROM t").contains("INSERT"));
        assert!(err("SELECT AI_SUMMARIZE(body) FROM t UNION SELECT 'x'").contains("UNION"));
        assert!(err("SELECT AI_SUMMARIZE(body) INTO @x FROM t").contains("INTO"));
        assert!(err("SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE AI_CLASSIFY(b, 'x', 'y') = 'x')").contains("subquery"));
        assert!(err("SELECT (SELECT AI_SUMMARIZE(b) FROM u LIMIT 1) FROM t").contains("subquery"));
        assert!(
            err("SELECT k, COUNT(*) FROM t GROUP BY AI_CLASSIFY(b, 'x', 'y')").contains("GROUP BY")
        );
        assert!(
            err("SELECT AI_CLASSIFY(b, 'x', 'y') k, COUNT(*) FROM t GROUP BY k")
                .contains("names its column")
        );
        assert!(
            err("SELECT AI_CLASSIFY(b, 'x', 'y') k, COUNT(*) FROM t GROUP BY 1")
                .contains("names its column")
        );
        assert!(
            err("SELECT a, COUNT(*) FROM t GROUP BY a HAVING MAX(AI_SUMMARIZE(b)) = 'x'")
                .contains("HAVING")
        );
        assert!(err("SELECT * FROM t JOIN u ON AI_CLASSIFY(t.b, 'x', 'y') = u.k").contains("FROM"));
        assert!(
            err("SET @x = (SELECT 1) + LENGTH(AI_SUMMARIZE(@y))").contains("SELECT and UPDATE")
        );
    }

    #[test]
    fn lookups() {
        assert_eq!(case_of("b", &[]), "NULL");
        assert_eq!(
            case_of("b", &[("ab".repeat(32), "'x'".into())]),
            format!(
                "(CASE SHA2((b), 256) WHEN '{}' THEN 'x' END)",
                "ab".repeat(32)
            )
        );
        let many: Vec<(String, String)> = (0..200)
            .map(|i| (format!("{:02x}{}", i % 7, "0".repeat(62)), format!("'{i}'")))
            .collect();
        let s = case_of("b", &many);
        assert!(
            s.starts_with("(CASE LEFT(SHA2((b), 256), 2) WHEN '00' THEN CASE SHA2((b), 256) WHEN")
        );
        assert_eq!(s.matches(" THEN CASE ").count(), 7);
        assert_eq!(s.matches(" END").count(), 8);
    }
}
