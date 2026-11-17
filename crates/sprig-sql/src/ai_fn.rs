//! Finding the AI functions in a statement: `AI_EMBED`, `AI_CLASSIFY`,
//! `AI_EXTRACT`, `AI_SUMMARIZE` and `AI_COMPLETE`.
//!
//! The gateway evaluates them; MariaDB never sees them. [`analyze`] finds
//! each call, its arguments and where it stands, and [`Plan::render`]
//! writes the statement that runs instead:
//!
//! - **A call whose input is a literal** (`AI_EMBED('red shoes')`) may stand
//!   anywhere: it is answered first and replaced by its value as a literal
//!   (a vector as `X'...'`). A whole item of the outermost `SELECT` list
//!   keeps its column name (`AS` the call's text).
//! - **A call over row values** (`AI_CLASSIFY(body, 'bug', 'idea')`) that
//!   is a whole item of the outermost `SELECT` list of a `SELECT` or
//!   `WITH ... SELECT` (an alias may follow), in a statement that doesn't
//!   act on its column (`DISTINCT`, `ORDER BY` and the like), has its
//!   input expression put in its place, so the statement returns the
//!   inputs in that column; the gateway answers them and puts the answers
//!   in their place.
//! - **Any other call over row values is bound** ([`bind`]): in the
//!   outermost `SELECT`'s list (inside any expression), `WHERE` and
//!   `ORDER BY`, and in a single-table `UPDATE ... SET`. The gateway reads
//!   the distinct inputs first, answers them, and runs the statement with
//!   each call replaced by a lookup of its answers. Refused (1235): in a
//!   subquery, `FROM` or a join condition, `GROUP BY`, `HAVING`, `WINDOW`,
//!   or an item they name; with `UNION`, `EXCEPT`, `INTERSECT` or `INTO`;
//!   in an `UPDATE`'s `WHERE`, `ORDER BY` or `LIMIT`, a multiple-table
//!   `UPDATE`, or reading a column an earlier assignment sets; in
//!   `INSERT`, `REPLACE`, `DELETE` and every other statement.
//! - **Every argument after the input is a literal**: labels, the JSON
//!   schema, the word limit, the model (1210 otherwise). Calls don't nest.
//!
//! The names are reserved: an unqualified call to a stored function of
//! that name is taken as the AI function. A qualified one
//! (`db.ai_embed(...)`) isn't.

use std::ops::Range;

use crate::lex::{self, LexOptions, Token, TokenKind, ident_value, string_value};
use crate::names::NameError;

pub mod bind;

pub use bind::{Bind, Level, case_of};

/// The functions' names, upper case.
pub const NAMES: [&str; 5] = [
    "AI_EMBED",
    "AI_CLASSIFY",
    "AI_EXTRACT",
    "AI_SUMMARIZE",
    "AI_COMPLETE",
];

/// The leading comment that names the database an internal statement
/// reads for (the MongoDB engine's): `/* sprig-ai-db:<name> */`.
pub const DB_HINT: &str = "sprig-ai-db:";

/// Whether `text` might call one of the functions (a byte search, before
/// lexing).
#[must_use]
pub fn mentions(text: &[u8]) -> bool {
    text.windows(3).enumerate().any(|(i, w)| {
        w.eq_ignore_ascii_case(b"AI_")
            && NAMES.iter().any(|n| {
                text.get(i..i + n.len())
                    .is_some_and(|s| s.eq_ignore_ascii_case(n.as_bytes()))
            })
    })
}

/// Where a call stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// The item's bytes in the statement.
    pub range: Range<usize>,
    /// Its alias, if it has one.
    pub alias: Option<String>,
    /// Its column in the result.
    pub column: ColumnAt,
}

/// A result column's position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAt {
    /// The n-th column from the first (0-based).
    FromStart(usize),
    /// The n-th column from the last (0: the last).
    FromEnd(usize),
}

impl ColumnAt {
    /// The index among `n` columns.
    #[must_use]
    pub fn index(self, n: usize) -> Option<usize> {
        match self {
            ColumnAt::FromStart(i) => (i < n).then_some(i),
            ColumnAt::FromEnd(i) => n.checked_sub(i + 1),
        }
    }
}

/// One call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The function, upper case (one of [`NAMES`]).
    pub name: &'static str,
    /// The call's bytes.
    pub range: Range<usize>,
    /// The input argument's bytes.
    pub input: Range<usize>,
    /// The input's value when it is a literal (`Some(None)` for `NULL`);
    /// `None` when it reads the row.
    pub literal: Option<Option<String>>,
    /// The later arguments' values (`None` for `NULL`).
    pub rest: Vec<Option<String>>,
    /// The item of the outermost `SELECT` list the call is, if it is one.
    pub item: Option<Item>,
    /// When the statement binds the call (see [`bind`]): when its inputs
    /// are read. `None` for a literal input and a whole item answered in
    /// its column.
    pub level: Option<Level>,
}

impl Call {
    /// Whether the input is a literal: answered before the statement runs.
    #[must_use]
    pub fn constant(&self) -> bool {
        self.literal.is_some()
    }

    /// The column name of an item without an alias: the call as written.
    #[must_use]
    pub fn written<'a>(&self, text: &'a [u8]) -> std::borrow::Cow<'a, str> {
        String::from_utf8_lossy(&text[self.range.clone()])
    }
}

/// The calls of a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// In text order.
    pub calls: Vec<Call>,
    /// The database a [`DB_HINT`] comment names.
    pub hint_db: Option<String>,
    /// Every name that qualifies another (`x` in `x.y`): the databases
    /// among them are the ones the statement reads.
    pub qualifiers: Vec<String>,
    /// How the statement runs when it binds calls over row values.
    pub bind: Option<Bind>,
}

fn refused(errno: u16, sqlstate: [u8; 5], message: String) -> NameError {
    NameError::Refused {
        errno,
        sqlstate,
        message,
    }
}

fn not_here(name: &str, why: &str) -> NameError {
    refused(
        1235,
        *b"42000",
        format!(
            "This version of SprigDB doesn't yet support '{name} over row values {why}': \
             such a call may stand in the SELECT list, WHERE or ORDER BY of the outermost \
             SELECT, or in a single-table UPDATE ... SET; or give it a literal input"
        ),
    )
}

fn bad_args(name: &str, why: &str) -> NameError {
    refused(
        1210,
        *b"HY000",
        format!("Incorrect arguments to {name}: {why}"),
    )
}

/// The statement's tokens without trivia, with their nesting depth.
struct Toks<'a> {
    sql: &'a [u8],
    t: Vec<Token>,
    depth: Vec<u32>,
    /// The innermost `(` around each token.
    parent: Vec<Option<usize>>,
}

impl Toks<'_> {
    fn word(&self, i: usize) -> Option<String> {
        let t = self.t.get(i)?;
        (t.kind == TokenKind::Word)
            .then(|| String::from_utf8_lossy(t.bytes(self.sql)).to_ascii_uppercase())
    }

    fn is_word(&self, i: usize, w: &str) -> bool {
        self.word(i).as_deref() == Some(w)
    }

    fn punct(&self, i: usize, p: u8) -> bool {
        self.t
            .get(i)
            .is_some_and(|t| t.kind == TokenKind::Punct && t.bytes(self.sql) == [p])
    }

    fn start(&self, i: usize) -> usize {
        self.t[i].start as usize
    }

    fn end(&self, i: usize) -> usize {
        self.t[i].end as usize
    }

    /// The index of the `)` closing the `(` at `open`.
    fn close(&self, open: usize) -> Option<usize> {
        let d = self.depth[open];
        (open + 1..self.t.len()).find(|&j| self.punct(j, b')') && self.depth[j] == d)
    }

    fn ident(&self, i: usize) -> Option<String> {
        let t = self.t.get(i)?;
        t.kind
            .is_ident_like()
            .then(|| String::from_utf8_lossy(&ident_value(t.bytes(self.sql))).into_owned())
    }
}

/// A literal argument's value: `Some(None)` for `NULL`.
#[allow(clippy::option_option, clippy::many_single_char_names)]
fn literal(tk: &Toks<'_>, r: Range<usize>, nbe: bool) -> Option<Option<String>> {
    let toks = &tk.t[r];
    let sql = tk.sql;
    match toks {
        [t] if t.kind == TokenKind::Word && t.bytes(sql).eq_ignore_ascii_case(b"NULL") => {
            Some(None)
        }
        [t] if t.kind == TokenKind::Number => {
            Some(Some(String::from_utf8_lossy(t.bytes(sql)).into_owned()))
        }
        [m, t]
            if m.kind == TokenKind::Op && m.bytes(sql) == b"-" && t.kind == TokenKind::Number =>
        {
            Some(Some(format!("-{}", String::from_utf8_lossy(t.bytes(sql)))))
        }
        [w, t]
            if w.kind == TokenKind::Word
                && w.bytes(sql).first() == Some(&b'_')
                && t.kind == TokenKind::HexString =>
        {
            // `_utf8mb4 X'..'`: text written in hexadecimal.
            let b = t.bytes(sql);
            let h = b.get(2..b.len().saturating_sub(1))?;
            let bytes: Option<Vec<u8>> = (0..h.len())
                .step_by(2)
                .map(|i| {
                    std::str::from_utf8(h.get(i..i + 2)?)
                        .ok()
                        .and_then(|x| u8::from_str_radix(x, 16).ok())
                })
                .collect();
            Some(Some(String::from_utf8_lossy(&bytes?).into_owned()))
        }
        [t] if t.kind == TokenKind::NationalString => {
            let b = t.bytes(sql);
            string_value(&b[1..], nbe).map(|v| Some(String::from_utf8_lossy(&v).into_owned()))
        }
        _ => {
            // `_charset'...'`, then one or more adjacent strings.
            let mut rest = toks;
            if let [w, tail @ ..] = rest
                && w.kind == TokenKind::Word
                && w.bytes(sql).first() == Some(&b'_')
                && !tail.is_empty()
            {
                rest = tail;
            }
            if rest.is_empty() || rest.iter().any(|t| t.kind != TokenKind::String) {
                return None;
            }
            let mut v = Vec::new();
            for t in rest {
                v.extend(string_value(t.bytes(sql), nbe)?);
            }
            Some(Some(String::from_utf8_lossy(&v).into_owned()))
        }
    }
}

/// The outermost `SELECT` list: its items' token ranges, and what the
/// statement around it does.
struct Outer {
    /// The `SELECT` keyword's token.
    sel: usize,
    items: Vec<Range<usize>>,
    /// The token that ends the list.
    list_end: usize,
    distinct: bool,
    /// `UNION`, `EXCEPT`, `INTERSECT` or `INTO` at the top level.
    compound: bool,
    /// The token ranges of `GROUP BY`, `HAVING`, `ORDER BY` and `WINDOW`
    /// clauses at the top level.
    clauses: Vec<Range<usize>>,
    /// Every top-level clause after the list.
    parts: Vec<(bind::Clause, Range<usize>)>,
}

const MODIFIERS: &[&str] = &[
    "ALL",
    "DISTINCT",
    "DISTINCTROW",
    "HIGH_PRIORITY",
    "STRAIGHT_JOIN",
    "SQL_SMALL_RESULT",
    "SQL_BIG_RESULT",
    "SQL_BUFFER_RESULT",
    "SQL_NO_CACHE",
    "SQL_CACHE",
    "SQL_CALC_FOUND_ROWS",
];

const LIST_END: &[&str] = &[
    "FROM",
    "INTO",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "WINDOW",
    "FOR",
    "LOCK",
    "UNION",
    "EXCEPT",
    "INTERSECT",
];

fn outer_select(tk: &Toks<'_>) -> Option<Outer> {
    let first = tk.word(0)?;
    if first != "SELECT" && first != "WITH" {
        return None;
    }
    let sel = (0..tk.t.len()).find(|&i| tk.depth[i] == 0 && tk.is_word(i, "SELECT"))?;
    let mut i = sel + 1;
    let mut distinct = false;
    while let Some(w) = tk.word(i) {
        if !MODIFIERS.contains(&w.as_str()) {
            break;
        }
        distinct |= w == "DISTINCT" || w == "DISTINCTROW";
        i += 1;
    }
    let mut items = Vec::new();
    let mut from = i;
    let mut j = i;
    while j < tk.t.len() {
        if tk.depth[j] == 0 {
            if tk.punct(j, b';') || tk.word(j).is_some_and(|w| LIST_END.contains(&w.as_str())) {
                break;
            }
            if tk.punct(j, b',') {
                items.push(from..j);
                from = j + 1;
            }
        }
        j += 1;
    }
    items.push(from..j);
    let mut compound = false;
    let mut clauses = Vec::new();
    let mut open: Option<usize> = None;
    for k in j..tk.t.len() {
        if tk.depth[k] != 0 {
            continue;
        }
        let Some(w) = tk.word(k) else { continue };
        match w.as_str() {
            "UNION" | "EXCEPT" | "INTERSECT" | "INTO" => compound = true,
            "GROUP" | "HAVING" | "ORDER" | "WINDOW" => {
                if let Some(o) = open.take() {
                    clauses.push(o..k);
                }
                open = Some(k);
            }
            "LIMIT" | "FOR" | "LOCK" | "WHERE" | "FROM" => {
                if let Some(o) = open.take() {
                    clauses.push(o..k);
                }
            }
            _ => {}
        }
    }
    if let Some(o) = open {
        clauses.push(o..tk.t.len());
    }
    Some(Outer {
        sel,
        items,
        list_end: j,
        distinct,
        compound,
        clauses,
        parts: bind::clauses(tk, j),
    })
}

/// Whether item `r` is exactly the call `c` (token indexes), with an
/// optional alias after it; `Some(alias)` when it is.
#[allow(clippy::option_option)]
fn item_alias(tk: &Toks<'_>, r: &Range<usize>, c: &Range<usize>) -> Option<Option<String>> {
    if r.start != c.start {
        return None;
    }
    match r.end - c.end {
        0 => Some(None),
        1 => tk
            .ident(c.end)
            .or_else(|| string_alias(tk, c.end))
            .map(Some),
        2 if tk.is_word(c.end, "AS") => tk
            .ident(c.end + 1)
            .or_else(|| string_alias(tk, c.end + 1))
            .map(Some),
        _ => None,
    }
}

fn string_alias(tk: &Toks<'_>, i: usize) -> Option<String> {
    let t = tk.t.get(i)?;
    (t.kind == TokenKind::String)
        .then(|| string_value(t.bytes(tk.sql), false))
        .flatten()
        .map(|v| String::from_utf8_lossy(&v).into_owned())
}

fn is_wildcard(tk: &Toks<'_>, r: &Range<usize>) -> bool {
    r.end > r.start
        && tk.t[r.end - 1].kind == TokenKind::Op
        && tk.t[r.end - 1].bytes(tk.sql) == b"*"
        && (r.end - r.start == 1 || tk.punct(r.end - 2, b'.'))
}

/// The statement's AI function calls; `None` when it has none.
///
/// # Errors
/// 1235 for a call over row values that isn't a whole item of the
/// outermost `SELECT` list (or where the statement would act on its
/// inputs); 1210 for a later argument that isn't a literal, or a nested
/// call; 1582 for a call without arguments.
#[allow(clippy::too_many_lines)] // one pass over the calls
#[must_use]
pub fn analyze(text: &[u8], opts: &LexOptions) -> Option<Result<Plan, NameError>> {
    if !mentions(text) {
        return None;
    }
    let all = lex::lex(text, opts).ok()?;
    let hint_db = all
        .iter()
        .find(|t| !matches!(t.kind, TokenKind::Whitespace))
        .filter(|t| t.kind == TokenKind::BlockComment)
        .and_then(|t| {
            let b = String::from_utf8_lossy(t.bytes(text)).into_owned();
            let inner = b.strip_prefix("/*")?.strip_suffix("*/")?.trim().to_owned();
            inner.strip_prefix(DB_HINT).map(|d| d.trim().to_owned())
        })
        .filter(|d| !d.is_empty());
    let mut tk = Toks {
        sql: text,
        t: all.into_iter().filter(|t| !t.kind.is_trivia()).collect(),
        depth: Vec::new(),
        parent: Vec::new(),
    };
    let mut d = 0u32;
    let mut open: Vec<usize> = Vec::new();
    for (i, t) in tk.t.iter().enumerate() {
        let b = t.bytes(text);
        if t.kind == TokenKind::Punct && b == b")" {
            d = d.saturating_sub(1);
            open.pop();
        }
        tk.depth.push(d);
        tk.parent.push(open.last().copied());
        if t.kind == TokenKind::Punct && b == b"(" {
            d += 1;
            open.push(i);
        }
    }
    let first = tk.word(0).unwrap_or_default();
    if !matches!(
        first.as_str(),
        "SELECT" | "WITH" | "INSERT" | "REPLACE" | "UPDATE" | "DELETE" | "SET" | "DO" | "VALUES"
    ) && !tk.punct(0, b'(')
    {
        return None;
    }
    // The calls, as token ranges.
    let mut found: Vec<(&'static str, usize, usize)> = Vec::new();
    for i in 0..tk.t.len() {
        if tk.t[i].kind != TokenKind::Word || !tk.punct(i + 1, b'(') {
            continue;
        }
        if i > 0 && tk.punct(i - 1, b'.') {
            continue;
        }
        let w = tk.t[i].bytes(text);
        let Some(name) = NAMES.iter().find(|n| w.eq_ignore_ascii_case(n.as_bytes())) else {
            continue;
        };
        let close = tk.close(i + 1)?;
        found.push((name, i, close));
    }
    if found.is_empty() {
        return None;
    }
    let qualifiers: Vec<String> = (0..tk.t.len().saturating_sub(2))
        .filter(|&i| tk.punct(i + 1, b'.') && (i == 0 || !tk.punct(i - 1, b'.')))
        .filter_map(|i| tk.ident(i))
        .collect();
    let outer = outer_select(&tk);
    let nbe = opts.no_backslash_escapes;
    let mut calls = Vec::new();
    // Whether every call over rows can be answered in its column.
    let mut in_column = true;
    for &(name, open_at, close) in &found {
        if found
            .iter()
            .any(|&(_, o, c)| o < open_at && close < c && (o, c) != (open_at, close))
        {
            return Some(Err(bad_args(name, "AI functions can't be nested")));
        }
        // Split the arguments at top-level commas.
        let inner = tk.depth[open_at + 1] + 1;
        let mut args: Vec<Range<usize>> = Vec::new();
        let mut from = open_at + 2;
        for j in open_at + 2..close {
            if tk.depth[j] == inner && tk.punct(j, b',') {
                args.push(from..j);
                from = j + 1;
            }
        }
        args.push(from..close);
        if args.iter().any(Range::is_empty) {
            return Some(Err(refused(
                1582,
                *b"42000",
                format!("Incorrect parameter count in the call to native function '{name}'"),
            )));
        }
        let input = args[0].clone();
        let lit = literal(&tk, input.clone(), nbe);
        let mut rest = Vec::new();
        for a in &args[1..] {
            match literal(&tk, a.clone(), nbe) {
                Some(v) => rest.push(v),
                None => {
                    return Some(Err(bad_args(
                        name,
                        "every argument after the input must be a literal",
                    )));
                }
            }
        }
        let call_toks = open_at..close + 1;
        let item = outer.as_ref().and_then(|o| {
            let (n, r) = o
                .items
                .iter()
                .enumerate()
                .find(|(_, r)| r.contains(&open_at))?;
            let alias = item_alias(&tk, r, &call_toks)?;
            let before = o.items[..n].iter().any(|r| is_wildcard(&tk, r));
            let after = o.items[n + 1..].iter().any(|r| is_wildcard(&tk, r));
            let column = match (before, after) {
                (false, _) => ColumnAt::FromStart(n),
                (true, false) => ColumnAt::FromEnd(o.items.len() - 1 - n),
                (true, true) => return None,
            };
            Some(Item {
                range: tk.start(r.start)..tk.end(r.end - 1),
                alias,
                column,
            })
        });
        if lit.is_none() {
            in_column &= item.as_ref().is_some_and(|it| {
                let o = outer
                    .as_ref()
                    .unwrap_or_else(|| unreachable!("an item has an outer list"));
                // Not where the statement acts on the column: DISTINCT,
                // UNION, INTO, or the column named by alias or position in
                // GROUP BY, HAVING, ORDER BY or WINDOW.
                let pos = match it.column {
                    ColumnAt::FromStart(i) => Some((i + 1).to_string()),
                    ColumnAt::FromEnd(_) => None,
                };
                let named = o.clauses.iter().any(|c| {
                    c.clone().any(|k| {
                        let t = &tk.t[k];
                        (t.kind == TokenKind::Number
                            && pos
                                .as_deref()
                                .is_some_and(|p| t.bytes(text) == p.as_bytes())
                            && (tk.is_word(k - 1, "BY") || tk.punct(k - 1, b',')))
                            || it.alias.as_ref().is_some_and(|a| {
                                tk.ident(k).is_some_and(|x| x.eq_ignore_ascii_case(a))
                                    && !tk.punct(k + 1, b'.')
                                    && !tk.punct(k.wrapping_sub(1), b'.')
                            })
                    })
                });
                !o.distinct && !o.compound && !named
            });
        }
        calls.push(Call {
            name,
            range: tk.start(open_at)..tk.end(close),
            input: tk.start(input.start)..tk.end(input.end - 1),
            literal: lit,
            rest,
            item,
            level: None,
        });
    }
    let mut plan = Plan {
        calls,
        hint_db,
        qualifiers,
        bind: None,
    };
    if in_column {
        return Some(Ok(plan));
    }
    // Calls over rows bound by their answers.
    let spans: Vec<(usize, usize)> = found.iter().map(|&(_, o, c)| (o, c)).collect();
    let bound = match (first.as_str(), &outer) {
        ("SELECT" | "WITH", Some(o)) => bind::select(&tk, o, &plan.calls, &spans),
        ("UPDATE", _) => {
            if let Some(u) = bind::update_parts(&tk) {
                bind::update(&tk, &u, &plan.calls, &spans)
            } else {
                let c = plan.calls.iter().find(|c| !c.constant());
                Err(not_here(c.map_or("AI_COMPLETE", |c| c.name), "here"))
            }
        }
        (w, _) => {
            let why = match w {
                "INSERT" | "REPLACE" => "in INSERT or REPLACE",
                "DELETE" => "in DELETE",
                _ => "outside SELECT and UPDATE",
            };
            let c = plan.calls.iter().find(|c| !c.constant());
            return Some(Err(not_here(c.map_or("AI_COMPLETE", |c| c.name), why)));
        }
    };
    match bound {
        Ok((b, levels)) => {
            for (c, l) in plan.calls.iter_mut().zip(levels) {
                c.level = l;
            }
            plan.bind = Some(b);
            Some(Ok(plan))
        }
        Err(e) => Some(Err(e)),
    }
}

impl Plan {
    /// Whether any call reads the row.
    #[must_use]
    pub fn has_columns(&self) -> bool {
        self.calls.iter().any(|c| !c.constant())
    }

    /// The statement to run: each constant or bound call replaced by
    /// `values[i]` (SQL text, one per call in order; ignored for the other
    /// calls over rows), each other call over rows by its input
    /// expression.
    #[must_use]
    pub fn render(&self, text: &[u8], values: &[String]) -> String {
        let mut out = Vec::with_capacity(text.len());
        let mut at = 0;
        for (c, v) in self.calls.iter().zip(values) {
            out.extend_from_slice(&text[at..c.range.start]);
            if c.level.is_some() {
                out.extend_from_slice(v.as_bytes());
            } else if c.constant() {
                out.extend_from_slice(v.as_bytes());
                if let Some(it) = &c.item
                    && it.alias.is_none()
                {
                    out.extend_from_slice(
                        format!(" AS `{}`", c.written(text).replace('`', "``")).as_bytes(),
                    );
                }
            } else {
                out.extend_from_slice(&text[c.input.clone()]);
            }
            at = c.range.end;
        }
        out.extend_from_slice(&text[at..]);
        String::from_utf8_lossy(&out).into_owned()
    }
}

/// A string as a SQL literal.
#[must_use]
pub fn string_literal(s: &str, no_backslash_escapes: bool) -> String {
    crate::names::rewrite::sql_string(s, no_backslash_escapes)
}

/// Bytes as a hexadecimal literal, `X'...'`.
#[must_use]
pub fn hex_literal(b: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(b.len() * 2 + 3);
    s.push_str("X'");
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s.push('\'');
    s
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn plan(sql: &str) -> Plan {
        analyze(sql.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .unwrap()
    }

    fn err(sql: &str) -> u16 {
        analyze(sql.as_bytes(), &LexOptions::mariadb())
            .unwrap()
            .unwrap_err()
            .errno()
    }

    #[test]
    fn statements_without_calls_are_left_alone() {
        let o = LexOptions::mariadb();
        for s in [
            "SELECT 1",
            "SELECT ai_embedding FROM t",
            "SELECT db.ai_embed(x) FROM t",
            "SELECT 'AI_EMBED(x)'",
            "CREATE FUNCTION ai_embed(x TEXT) RETURNS TEXT RETURN x",
        ] {
            assert!(analyze(s.as_bytes(), &o).is_none(), "{s}");
        }
        assert!(mentions(b"select ai_classify(x"));
        assert!(!mentions(b"select ai_x(1)"));
    }

    #[test]
    fn literal_inputs_go_anywhere() {
        let sql = "SELECT id FROM products ORDER BY VEC_DISTANCE_COSINE(embedding, AI_EMBED('red shoes')) LIMIT 10";
        let p = plan(sql);
        assert_eq!(p.calls.len(), 1);
        let c = &p.calls[0];
        assert_eq!(c.name, "AI_EMBED");
        assert_eq!(c.literal, Some(Some("red shoes".into())));
        assert!(c.item.is_none());
        assert!(!p.has_columns());
        assert_eq!(
            p.render(sql.as_bytes(), &["X'00'".into()]),
            "SELECT id FROM products ORDER BY VEC_DISTANCE_COSINE(embedding, X'00') LIMIT 10"
        );
        // A whole item keeps its column name.
        let sql = "SELECT ai_complete(_utf8mb4'a' 'b'), AI_SUMMARIZE(N'x', 20) s";
        let p = plan(sql);
        assert_eq!(p.calls[0].literal, Some(Some("ab".into())));
        assert_eq!(p.calls[1].rest, [Some("20".into())]);
        assert_eq!(
            p.render(sql.as_bytes(), &["'A'".into(), "'S'".into()]),
            "SELECT 'A' AS `ai_complete(_utf8mb4'a' 'b')`, 'S' s"
        );
        // In writes too.
        let p = plan("UPDATE t SET v = AI_EMBED('x') WHERE id = 1");
        assert!(p.calls[0].constant());
        let p = plan("SELECT AI_CLASSIFY(_utf8mb4 X'6869', _utf8mb4 X'61', 'b')");
        assert_eq!(p.calls[0].literal, Some(Some("hi".into())));
        assert_eq!(p.calls[0].rest[0].as_deref(), Some("a"));
        let p = plan("INSERT INTO t VALUES (1, AI_CLASSIFY(NULL, 'a', 'b'))");
        assert_eq!(p.calls[0].literal, Some(None));
    }

    #[test]
    fn row_inputs_become_columns() {
        let sql = "SELECT id, AI_CLASSIFY(CONCAT(title, ' ', body), 'bug', 'idea') AS kind, \
                   AI_SUMMARIZE(body, 30) FROM tickets WHERE id > 3 ORDER BY id";
        let p = plan(sql);
        assert!(p.has_columns());
        let k = &p.calls[0];
        assert_eq!(k.literal, None);
        assert_eq!(k.rest, [Some("bug".into()), Some("idea".into())]);
        let it = k.item.as_ref().unwrap();
        assert_eq!(it.alias.as_deref(), Some("kind"));
        assert_eq!(it.column, ColumnAt::FromStart(1));
        assert_eq!(
            p.calls[1].item.as_ref().unwrap().column,
            ColumnAt::FromStart(2)
        );
        assert_eq!(p.calls[1].written(sql.as_bytes()), "AI_SUMMARIZE(body, 30)");
        assert_eq!(
            p.render(sql.as_bytes(), &[String::new(), String::new()]),
            "SELECT id, CONCAT(title, ' ', body) AS kind, \
                   body FROM tickets WHERE id > 3 ORDER BY id"
        );
        // After `*`, counted from the end.
        let p = plan("WITH x AS (SELECT * FROM t) SELECT *, AI_EMBED(body) e FROM x");
        assert_eq!(
            p.calls[0].item.as_ref().unwrap().column,
            ColumnAt::FromEnd(0)
        );
        assert_eq!(ColumnAt::FromEnd(0).index(4), Some(3));
        assert!(
            plan("SELECT t.*, AI_EMBED('x') FROM t").calls[0]
                .item
                .is_some()
        );
        // Qualifiers, for the databases read.
        let p = plan("SELECT AI_EMBED(p.name) FROM shop.products p");
        assert_eq!(p.qualifiers, ["p", "shop"]);
        // The MongoDB engine's hint.
        let p = plan("/* sprig-ai-db:media */ SELECT i, AI_EMBED(v) FROM (SELECT 1 i, 'a' v) d");
        assert_eq!(p.hint_db.as_deref(), Some("media"));
    }

    #[test]
    fn row_inputs_elsewhere_are_bound_or_refused() {
        let bound = |s: &str| {
            let p = plan(s);
            assert!(p.bind.is_some(), "{s}");
            assert!(
                p.calls.iter().all(|c| c.constant() || c.level.is_some()),
                "{s}"
            );
        };
        bound("SELECT id FROM t WHERE AI_CLASSIFY(body, 'a', 'b') = 'a'");
        bound("SELECT UPPER(AI_SUMMARIZE(body)) FROM t");
        bound("UPDATE t SET s = AI_SUMMARIZE(body)");
        bound("SELECT DISTINCT AI_CLASSIFY(body, 'a', 'b') FROM t");
        bound("SELECT AI_CLASSIFY(b, 'a', 'b') k FROM t ORDER BY k");
        // A whole item beside a bound call is bound too.
        let p = plan("SELECT AI_SUMMARIZE(b), id FROM t WHERE AI_CLASSIFY(b, 'a', 'b') = 'a'");
        assert_eq!(p.calls[0].level, Some(Level::Output));
        assert!(p.calls[0].item.is_some());
        assert_eq!(err("INSERT INTO s SELECT AI_SUMMARIZE(body) FROM t"), 1235);
        assert_eq!(
            err("DELETE FROM t WHERE AI_CLASSIFY(b, 'a', 'b') = 'a'"),
            1235
        );
        assert_eq!(
            err("SELECT AI_SUMMARIZE(body) FROM t UNION SELECT 'x'"),
            1235
        );
        assert_eq!(
            err("SELECT id, AI_CLASSIFY(b, 'a', 'b') FROM t GROUP BY 2"),
            1235
        );
        assert_eq!(err("SELECT AI_SUMMARIZE(body) INTO @x FROM t"), 1235);
        assert_eq!(err("SELECT t.*, AI_EMBED(b), u.* FROM t, u"), 1235);
        assert_eq!(err("SELECT AI_CLASSIFY(body, label, 'b') FROM t"), 1210);
        assert_eq!(err("SELECT AI_EMBED(AI_SUMMARIZE('x'))"), 1210);
        assert_eq!(err("SELECT AI_COMPLETE()"), 1582);
        // An alias named in WHERE is the table's column, not the item's.
        assert!(plan("SELECT AI_SUMMARIZE(body) AS body FROM t WHERE body <> ''").has_columns());
        // ORDER BY another column is fine.
        assert!(
            plan("SELECT id, AI_SUMMARIZE(body) s FROM t ORDER BY 1 DESC LIMIT 5").has_columns()
        );
    }

    #[test]
    fn literals() {
        assert_eq!(hex_literal(&[0, 0x80, 0x3f]), "X'00803F'");
        assert_eq!(string_literal("it's", false), "'it''s'");
    }
}
