//! Finding search calls in SQL text: `MATCH (columns) AGAINST ('text' [modifier])` and
//! `sprig_search('index', 'query' [, options])`, with their byte spans,
//! the clause each sits in, and the tables named after `FROM` and `JOIN`.
//!
//! The gateway replaces each call by SQL over the table's key, so the
//! statement then runs as any other: names, privileges, row-level
//! security and the router apply to it unchanged.

use sprig_common::{Result, SprigCode, SprigError};

/// `AGAINST`'s modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Natural language mode (the default).
    Natural,
    /// `IN BOOLEAN MODE`.
    Boolean,
    /// `WITH QUERY EXPANSION` (refused: 39823).
    Expansion,
}

/// One `MATCH ... AGAINST`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchCall {
    /// Byte span of the whole call.
    pub span: (usize, usize),
    /// Columns: (qualifier, name).
    pub columns: Vec<(Option<String>, String)>,
    /// The search text; `None` for a placeholder or an expression.
    pub text: Option<String>,
    /// The modifier.
    pub mode: Mode,
    /// In `WHERE` or `ON` (a condition) rather than a value.
    pub condition: bool,
}

/// One `sprig_search(...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCall {
    /// Byte span of the call.
    pub span: (usize, usize),
    /// The index name (maybe `db.name`).
    pub index: String,
    /// The query string.
    pub query: String,
    /// The options (`JSON_OBJECT(...)` or a JSON string), as SQL text.
    pub options: Option<String>,
}

/// A table after `FROM` or `JOIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// Database, when qualified.
    pub db: Option<String>,
    /// The table.
    pub table: String,
    /// Its alias.
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tk {
    Word(String),
    Quoted(String),
    Str(String),
    Other(char),
    Param,
}

#[derive(Debug, Clone)]
struct Tok {
    t: Tk,
    from: usize,
    to: usize,
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, m)
}

fn lex(sql: &str) -> Vec<Tok> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'#' || (c == b'-' && b.get(i + 1) == Some(&b'-')) {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(b.len());
        } else if c == b'\'' || c == b'"' || c == b'`' {
            let start = i;
            let q = c;
            let mut v = Vec::new();
            i += 1;
            while i < b.len() {
                let x = b[i];
                if x == b'\\' && q != b'`' && i + 1 < b.len() {
                    v.push(match b[i + 1] {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'0' => 0,
                        o => o,
                    });
                    i += 2;
                    continue;
                }
                if x == q {
                    if b.get(i + 1) == Some(&q) {
                        v.push(q);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                v.push(x);
                i += 1;
            }
            let s = String::from_utf8_lossy(&v).into_owned();
            out.push(Tok {
                t: if q == b'`' { Tk::Quoted(s) } else { Tk::Str(s) },
                from: start,
                to: i,
            });
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Tok {
                t: Tk::Word(sql[start..i].to_owned()),
                from: start,
                to: i,
            });
        } else if c == b'?' {
            out.push(Tok {
                t: Tk::Param,
                from: i,
                to: i + 1,
            });
            i += 1;
        } else {
            let ch = sql[i..].chars().next().unwrap_or('?');
            out.push(Tok {
                t: Tk::Other(ch),
                from: i,
                to: i + ch.len_utf8(),
            });
            i += ch.len_utf8();
        }
    }
    out
}

fn is_kw(t: &Tk, w: &str) -> bool {
    matches!(t, Tk::Word(x) if x.eq_ignore_ascii_case(w))
}

fn name(t: &Tk) -> Option<String> {
    match t {
        Tk::Word(x) | Tk::Quoted(x) => Some(x.clone()),
        _ => None,
    }
}

const CLAUSES: [&str; 12] = [
    "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "ON", "SET", "VALUES", "JOIN",
    "UNION",
];

const NOT_ALIAS: [&str; 22] = [
    "WHERE",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "CROSS",
    "NATURAL",
    "ON",
    "USING",
    "GROUP",
    "ORDER",
    "LIMIT",
    "HAVING",
    "UNION",
    "STRAIGHT_JOIN",
    "FOR",
    "LOCK",
    "WINDOW",
    "SET",
    "INTO",
    "PARTITION",
    "FULL",
];

/// What a statement holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scan {
    /// `MATCH ... AGAINST` calls, in order.
    pub matches: Vec<MatchCall>,
    /// `sprig_search(...)` calls, in order.
    pub searches: Vec<SearchCall>,
    /// Tables after `FROM` and `JOIN`.
    pub tables: Vec<TableRef>,
}

/// Whether the text may hold a search call (cheap pre-check).
#[must_use]
pub fn mentions(sql: &[u8]) -> bool {
    let has = |w: &[u8]| sql.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    has(b"against") || has(b"sprig_search")
}

/// The byte spans of `sql`'s `?` placeholders, in order (outside strings,
/// quoted names and comments).
#[must_use]
pub fn placeholders(sql: &str) -> Vec<(usize, usize)> {
    lex(sql)
        .into_iter()
        .filter(|t| t.t == Tk::Param)
        .map(|t| (t.from, t.to))
        .collect()
}

/// `sql` with its placeholders replaced, in order, by `values` (SQL
/// literals); `None` when the counts differ.
#[must_use]
pub fn interpolate(sql: &str, values: &[String]) -> Option<String> {
    let spans = placeholders(sql);
    if spans.len() != values.len() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + values.iter().map(String::len).sum::<usize>());
    let mut last = 0;
    for ((a, b), v) in spans.iter().zip(values) {
        out.push_str(&sql[last..*a]);
        out.push_str(v);
        last = *b;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// Scans `sql`.
///
/// # Errors
/// 39822 for a `MATCH` or `sprig_search` that isn't well formed.
pub fn scan(sql: &str) -> Result<Scan> {
    let t = lex(sql);
    let mut out = Scan::default();
    // The clause each parenthesis depth is in.
    let mut clause: Vec<String> = vec![String::new()];
    let mut i = 0;
    while i < t.len() {
        match &t[i].t {
            Tk::Other('(') => {
                clause.push(String::new());
                i += 1;
                continue;
            }
            Tk::Other(')') => {
                if clause.len() > 1 {
                    clause.pop();
                }
                i += 1;
                continue;
            }
            Tk::Word(w) => {
                let up = w.to_ascii_uppercase();
                if is_kw(&t[i].t, "MATCH")
                    && matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('(')))
                {
                    let (m, next) =
                        match_call(sql, &t, i, clause.last().map_or("", String::as_str))?;
                    out.matches.push(m);
                    i = next;
                    continue;
                }
                if up == "SPRIG_SEARCH"
                    && matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('(')))
                {
                    let (s, next) = search_call(sql, &t, i)?;
                    out.searches.push(s);
                    i = next;
                    continue;
                }
                if CLAUSES.contains(&up.as_str())
                    && let Some(c) = clause.last_mut()
                {
                    c.clone_from(&up);
                }
                if up == "FROM" || up == "JOIN" || up == "UPDATE" || up == "INTO" {
                    i = table_list(&t, i + 1, &mut out.tables);
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }
    Ok(out)
}

fn table_list(t: &[Tok], mut i: usize, out: &mut Vec<TableRef>) -> usize {
    loop {
        let Some(first) = t.get(i).and_then(|x| name(&x.t)) else {
            return i;
        };
        if matches!(t[i].t, Tk::Word(ref w) if NOT_ALIAS.iter().any(|k| w.eq_ignore_ascii_case(k)) || w.eq_ignore_ascii_case("SELECT"))
        {
            return i;
        }
        // A function call (`sprig_search(...)`, `JSON_TABLE(...)`) isn't a table.
        if matches!(t.get(i + 1).map(|x| &x.t), Some(Tk::Other('('))) {
            return i;
        }
        i += 1;
        let (db, table) = if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
            let Some(tb) = t.get(i + 1).and_then(|x| name(&x.t)) else {
                return i;
            };
            i += 2;
            (Some(first), tb)
        } else {
            (None, first)
        };
        if t.get(i).is_some_and(|x| is_kw(&x.t, "AS")) {
            i += 1;
        }
        let alias = match t.get(i).map(|x| &x.t) {
            Some(Tk::Word(w)) if !NOT_ALIAS.iter().any(|k| w.eq_ignore_ascii_case(k)) => {
                i += 1;
                Some(w.clone())
            }
            Some(Tk::Quoted(w)) => {
                i += 1;
                Some(w.clone())
            }
            _ => None,
        };
        out.push(TableRef { db, table, alias });
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
            i += 1;
            continue;
        }
        return i;
    }
}

fn need(t: &[Tok], i: usize, c: char) -> Result<()> {
    if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(x)) if *x == c) {
        Ok(())
    } else {
        Err(syntax(format!("expected '{c}' in a search call")))
    }
}

fn match_call(sql: &str, t: &[Tok], at: usize, clause: &str) -> Result<(MatchCall, usize)> {
    let mut i = at + 2;
    let mut columns = Vec::new();
    loop {
        let a = t
            .get(i)
            .and_then(|x| name(&x.t))
            .ok_or_else(|| syntax("MATCH needs column names"))?;
        i += 1;
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
            let b = t
                .get(i + 1)
                .and_then(|x| name(&x.t))
                .ok_or_else(|| syntax("MATCH needs column names"))?;
            i += 2;
            // `db.table.column`: keep the table as the qualifier.
            if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other('.'))) {
                let c = t
                    .get(i + 1)
                    .and_then(|x| name(&x.t))
                    .ok_or_else(|| syntax("MATCH needs column names"))?;
                i += 2;
                columns.push((Some(b), c));
            } else {
                columns.push((Some(a), b));
            }
        } else {
            columns.push((None, a));
        }
        if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
            i += 1;
            continue;
        }
        break;
    }
    need(t, i, ')')?;
    i += 1;
    if !t.get(i).is_some_and(|x| is_kw(&x.t, "AGAINST")) {
        return Err(syntax("MATCH (...) must be followed by AGAINST"));
    }
    i += 1;
    need(t, i, '(')?;
    i += 1;
    let text = match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Some(s.clone()),
        _ => None,
    };
    // Skip to the matching ')', reading the modifier.
    let mut depth = 0i32;
    let mut mode = Mode::Natural;
    while i < t.len() {
        match &t[i].t {
            Tk::Other('(') => depth += 1,
            Tk::Other(')') => {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            }
            x if is_kw(x, "BOOLEAN") => mode = Mode::Boolean,
            x if is_kw(x, "EXPANSION") => mode = Mode::Expansion,
            _ => {}
        }
        i += 1;
    }
    need(t, i, ')')?;
    let end = t[i].to;
    let _ = sql;
    Ok((
        MatchCall {
            span: (t[at].from, end),
            columns,
            text,
            mode,
            condition: matches!(clause, "WHERE" | "ON"),
        },
        i + 1,
    ))
}

fn search_call(sql: &str, t: &[Tok], at: usize) -> Result<(SearchCall, usize)> {
    let mut i = at + 2;
    let str_at = |i: usize| match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Ok(s.clone()),
        _ => Err(syntax(
            "sprig_search() takes an index name and a query string",
        )),
    };
    let index = str_at(i)?;
    i += 1;
    need(t, i, ',')?;
    i += 1;
    let query = str_at(i)?;
    i += 1;
    let mut options = None;
    if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
        i += 1;
        let start = t.get(i).map_or(sql.len(), |x| x.from);
        let mut depth = 0i32;
        while i < t.len() {
            match &t[i].t {
                Tk::Other('(') => depth += 1,
                Tk::Other(')') => {
                    if depth == 0 {
                        break;
                    }
                    depth -= 1;
                }
                _ => {}
            }
            i += 1;
        }
        let end = t.get(i).map_or(sql.len(), |x| x.from);
        options = Some(sql[start..end].trim().to_owned());
    }
    need(t, i, ')')?;
    Ok((
        SearchCall {
            span: (t[at].from, t[i].to),
            index,
            query,
            options,
        },
        i + 1,
    ))
}

/// One `column LIKE 'pattern' [ESCAPE 'c']` condition a search index may
/// narrow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LikeCall {
    /// Byte span of the whole condition.
    pub span: (usize, usize),
    /// Byte span of the column (with its qualifier).
    pub column_span: (usize, usize),
    /// The column: (qualifier, name).
    pub column: (Option<String>, String),
    /// The pattern's text.
    pub pattern: String,
    /// Byte span of the pattern literal.
    pub pattern_span: (usize, usize),
    /// The escape character (`\\` by default).
    pub escape: char,
    /// Byte span of `ESCAPE 'c'`, when given.
    pub escape_span: Option<(usize, usize)>,
}

/// Whether the text may hold a `LIKE` (cheap pre-check).
#[must_use]
pub fn mentions_like(sql: &[u8]) -> bool {
    sql.windows(4).any(|x| x.eq_ignore_ascii_case(b"like"))
}

/// Words that make `(c LIKE p AND k IN (...))` differ from `c LIKE p`:
/// they tell NULL from FALSE.
const NULL_AWARE: [&str; 9] = [
    "NOT", "XOR", "IS", "CASE", "IF", "IFNULL", "COALESCE", "NULLIF", "ISNULL",
];

/// What may stand before a narrowed `LIKE`'s column.
fn opens_condition(t: Option<&Tk>) -> bool {
    match t {
        Some(Tk::Word(w)) => ["WHERE", "ON", "AND", "OR"]
            .iter()
            .any(|k| w.eq_ignore_ascii_case(k)),
        Some(Tk::Other(c)) => matches!(c, '(' | '&' | '|'),
        _ => false,
    }
}

/// What may follow a narrowed `LIKE`'s pattern.
fn closes_condition(t: Option<&Tk>) -> bool {
    match t {
        None => true,
        Some(Tk::Word(w)) => [
            "AND", "OR", "GROUP", "ORDER", "LIMIT", "UNION", "FOR", "LOCK", "INTO", "WINDOW",
        ]
        .iter()
        .any(|k| w.eq_ignore_ascii_case(k)),
        Some(Tk::Other(c)) => matches!(c, ')' | ';' | '&' | '|'),
        _ => false,
    }
}

/// The `LIKE` conditions of `sql` that a search may narrow without
/// changing the result: `column LIKE 'literal' [ESCAPE 'c']` directly in a
/// `WHERE` or `ON` clause (or in parentheses there), as an operand of
/// `AND`/`OR`. Replacing one by `(column LIKE ('literal') AND key IN
/// (candidates))` is the same condition when the candidates hold every
/// row the pattern matches, except that a NULL column then reads FALSE
/// rather than NULL: so a statement with `NOT`, `XOR`, `IS`, `<=>`, `!`,
/// `CASE`, `IF`, `IFNULL`, `COALESCE`, `NULLIF` or `ISNULL` anywhere has
/// none.
#[must_use]
pub fn likes(sql: &str) -> Vec<LikeCall> {
    let t = lex(sql);
    let unsafe_word = t.iter().any(|x| match &x.t {
        Tk::Word(w) => NULL_AWARE.iter().any(|k| w.eq_ignore_ascii_case(k)),
        Tk::Other('!') => true,
        _ => false,
    });
    let spaceship = t.windows(3).any(|w| {
        matches!(
            (&w[0].t, &w[1].t, &w[2].t),
            (Tk::Other('<'), Tk::Other('='), Tk::Other('>'))
        ) && w[0].to == w[1].from
            && w[1].to == w[2].from
    });
    if unsafe_word || spaceship {
        return Vec::new();
    }
    let mut out = Vec::new();
    // The clause at each depth; a parenthesis starts in its parent's.
    let mut clause: Vec<String> = vec![String::new()];
    for i in 0..t.len() {
        match &t[i].t {
            Tk::Other('(') => {
                let parent = clause.last().cloned().unwrap_or_default();
                clause.push(parent);
                continue;
            }
            Tk::Other(')') => {
                if clause.len() > 1 {
                    clause.pop();
                }
                continue;
            }
            Tk::Word(w) => {
                let up = w.to_ascii_uppercase();
                if CLAUSES.contains(&up.as_str()) {
                    if let Some(c) = clause.last_mut() {
                        c.clone_from(&up);
                    }
                    continue;
                }
                if up != "LIKE" {
                    continue;
                }
            }
            _ => continue,
        }
        // `LIKE` at `i`.
        if !matches!(clause.last().map(String::as_str), Some("WHERE" | "ON")) || i == 0 {
            continue;
        }
        let Some(col) = name(&t[i - 1].t) else {
            continue;
        };
        let (start, qual) = if i >= 3 && matches!(t[i - 2].t, Tk::Other('.')) {
            match name(&t[i - 3].t) {
                Some(q) => (i - 3, Some(q)),
                None => continue,
            }
        } else {
            (i - 1, None)
        };
        if start > 0 && matches!(t[start - 1].t, Tk::Other('.')) {
            // `db.table.column`: three parts are left alone.
            continue;
        }
        if !opens_condition(start.checked_sub(1).map(|j| &t[j].t)) {
            continue;
        }
        let Some(Tk::Str(pattern)) = t.get(i + 1).map(|x| &x.t) else {
            continue;
        };
        let mut end = i + 2;
        let mut escape = '\\';
        let mut escape_span = None;
        if t.get(end).is_some_and(|x| is_kw(&x.t, "ESCAPE")) {
            let Some(Tk::Str(e)) = t.get(end + 1).map(|x| &x.t) else {
                continue;
            };
            let mut cs = e.chars();
            match (cs.next(), cs.next()) {
                (Some(c), None) => escape = c,
                _ => continue,
            }
            escape_span = Some((t[end].from, t[end + 1].to));
            end += 2;
        }
        if !closes_condition(t.get(end).map(|x| &x.t)) {
            continue;
        }
        out.push(LikeCall {
            span: (t[start].from, t[end - 1].to),
            column_span: (t[start].from, t[i - 1].to),
            column: (qual, col),
            pattern: pattern.clone(),
            pattern_span: (t[i + 1].from, t[i + 1].to),
            escape,
            escape_span,
        });
    }
    out
}

/// The literal runs of a `LIKE` pattern: the text between wildcards (`%`,
/// `_`), with `escape` making the next character literal.
#[must_use]
pub fn like_runs(pattern: &str, escape: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == escape {
            if let Some(n) = chars.next() {
                cur.push(n);
            }
            continue;
        }
        if c == '%' || c == '_' {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// A `SELECT ... ORDER BY <score> DESC LIMIT [m,] n` over one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopK {
    /// Rows skipped (`OFFSET`).
    pub offset: u64,
    /// Rows returned (`LIMIT`).
    pub limit: u64,
}

/// Words that keep a statement from paging its search: they combine,
/// group or lock rows, or nest statements.
const NOT_TOP_K: [&str; 21] = [
    "GROUP",
    "HAVING",
    "DISTINCT",
    "DISTINCTROW",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "FOR",
    "LOCK",
    "INTO",
    "WINDOW",
    "OVER",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "GROUP_CONCAT",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "SQL_CALC_FOUND_ROWS",
];

/// Whether `sql`, already scanned, reads the best rows of one search:
/// a single `SELECT` on one table whose `MATCH` calls are all the same
/// literal search, ordered by that `MATCH` (or its alias in the select
/// list) `DESC` alone, with a `LIMIT`, and nothing that groups, combines
/// or nests. Such a statement's answer lies among the search's best hits,
/// so it can read them a page at a time.
#[must_use]
pub fn top_k(sql: &str, scan: &Scan) -> Option<TopK> {
    let first = scan.matches.first()?;
    first.text.as_ref()?;
    if scan.tables.len() != 1
        || !scan.searches.is_empty()
        || first.mode == Mode::Expansion
        || scan
            .matches
            .iter()
            .any(|m| m.columns != first.columns || m.text != first.text || m.mode != first.mode)
    {
        return None;
    }
    let t = lex(sql);
    if !t.first().is_some_and(|x| is_kw(&x.t, "SELECT")) {
        return None;
    }
    let selects = t.iter().filter(|x| is_kw(&x.t, "SELECT")).count();
    if selects != 1
        || t.iter().any(|x| match &x.t {
            Tk::Word(w) => NOT_TOP_K.iter().any(|k| w.eq_ignore_ascii_case(k)),
            Tk::Param => true,
            _ => false,
        })
    {
        return None;
    }
    // Aliases of the MATCH values in the select list.
    let mut aliases: Vec<String> = Vec::new();
    for m in scan.matches.iter().filter(|m| !m.condition) {
        let Some(j) = t.iter().position(|x| x.from >= m.span.1) else {
            continue;
        };
        let j = if t.get(j).is_some_and(|x| is_kw(&x.t, "AS")) {
            j + 1
        } else {
            j
        };
        if let Some(a) = t.get(j).and_then(|x| name(&x.t))
            && !is_kw(&t[j].t, "FROM")
        {
            aliases.push(a);
        }
    }
    // The top-level ORDER BY.
    let mut depth = 0i32;
    let mut order = None;
    for (i, x) in t.iter().enumerate() {
        match x.t {
            Tk::Other('(') => depth += 1,
            Tk::Other(')') => depth -= 1,
            _ if depth == 0
                && is_kw(&x.t, "ORDER")
                && t.get(i + 1).is_some_and(|y| is_kw(&y.t, "BY")) =>
            {
                order = Some(i + 2);
            }
            _ => {}
        }
    }
    let mut i = order?;
    // Its one key: a MATCH call, or a MATCH's alias.
    let key = t.get(i)?;
    if let Some(m) = scan.matches.iter().find(|m| m.span.0 == key.from) {
        i = t.iter().position(|x| x.from >= m.span.1)?;
    } else if name(&key.t).is_some_and(|n| aliases.iter().any(|a| a.eq_ignore_ascii_case(&n))) {
        i += 1;
    } else {
        return None;
    }
    if !t.get(i).is_some_and(|x| is_kw(&x.t, "DESC")) {
        return None;
    }
    i += 1;
    if !t.get(i).is_some_and(|x| is_kw(&x.t, "LIMIT")) {
        return None;
    }
    let num = |j: usize| -> Option<u64> {
        match t.get(j).map(|x| &x.t) {
            Some(Tk::Word(w)) => w.parse().ok(),
            _ => None,
        }
    };
    let a = num(i + 1)?;
    let (offset, limit, next) = if matches!(t.get(i + 2).map(|x| &x.t), Some(Tk::Other(','))) {
        (a, num(i + 3)?, i + 4)
    } else if t.get(i + 2).is_some_and(|x| is_kw(&x.t, "OFFSET")) {
        (num(i + 3)?, a, i + 4)
    } else {
        (0, a, i + 2)
    };
    match t.get(next).map(|x| &x.t) {
        None | Some(Tk::Other(';')) if next + 1 >= t.len() => Some(TopK { offset, limit }),
        _ => None,
    }
}

/// The options of `sprig_search()` as JSON: a JSON string literal's text,
/// or `JSON_OBJECT('k', v, ...)` with literal values.
///
/// # Errors
/// 39822 for anything else.
pub fn options_json(text: &str) -> Result<serde_json::Value> {
    let t = lex(text);
    match t.first().map(|x| &x.t) {
        None => Ok(serde_json::Value::Null),
        Some(Tk::Str(s)) if t.len() == 1 => {
            serde_json::from_str(s).map_err(|e| syntax(format!("sprig_search() options: {e}")))
        }
        Some(x) if is_kw(x, "JSON_OBJECT") => {
            let mut m = serde_json::Map::new();
            let mut i = 2;
            loop {
                let Some(Tk::Str(k)) = t.get(i).map(|x| x.t.clone()) else {
                    break;
                };
                i += 1;
                need(&t, i, ',')?;
                i += 1;
                let (v, next) = json_value(&t, i)?;
                m.insert(k, v);
                i = next;
                if matches!(t.get(i).map(|x| &x.t), Some(Tk::Other(','))) {
                    i += 1;
                    continue;
                }
                break;
            }
            Ok(serde_json::Value::Object(m))
        }
        _ => Err(syntax(
            "sprig_search() options must be JSON_OBJECT(...) or a JSON string",
        )),
    }
}

fn json_value(t: &[Tok], i: usize) -> Result<(serde_json::Value, usize)> {
    match t.get(i).map(|x| &x.t) {
        Some(Tk::Str(s)) => Ok((serde_json::Value::String(s.clone()), i + 1)),
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("true") => {
            Ok((serde_json::Value::Bool(true), i + 1))
        }
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("false") => {
            Ok((serde_json::Value::Bool(false), i + 1))
        }
        Some(Tk::Word(w)) if w.eq_ignore_ascii_case("JSON_ARRAY") => {
            let mut v = Vec::new();
            let mut j = i + 2;
            while !matches!(t.get(j).map(|x| &x.t), Some(Tk::Other(')')) | None) {
                let (x, n) = json_value(t, j)?;
                v.push(x);
                j = n;
                if matches!(t.get(j).map(|x| &x.t), Some(Tk::Other(','))) {
                    j += 1;
                }
            }
            Ok((serde_json::Value::Array(v), j + 1))
        }
        Some(Tk::Word(w)) => serde_json::from_str::<serde_json::Value>(w)
            .map(|v| (v, i + 1))
            .map_err(|_| syntax(format!("unsupported option value {w}"))),
        _ => Err(syntax("unsupported option value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn likes_a_search_may_narrow() {
        let sql = "SELECT * FROM t AS a WHERE a.name LIKE '%red shoe%' AND (b LIKE 'x\\_y%' ESCAPE '\\\\' OR c = 1)";
        let l = likes(sql);
        assert_eq!(l.len(), 2);
        assert_eq!(l[0].column, (Some("a".into()), "name".into()));
        assert_eq!(&sql[l[0].span.0..l[0].span.1], "a.name LIKE '%red shoe%'");
        assert_eq!(&sql[l[0].column_span.0..l[0].column_span.1], "a.name");
        assert_eq!(l[0].pattern, "%red shoe%");
        assert_eq!(l[1].escape, '\\');
        assert!(l[1].escape_span.is_some());
        // Not narrowed: NOT anywhere, a value, an expression, a placeholder,
        // a comparison around it, a COLLATE after it.
        for q in [
            "SELECT * FROM t WHERE name NOT LIKE '%x%'",
            "SELECT * FROM t WHERE name LIKE '%x%' AND NOT b",
            "SELECT name LIKE '%x%' FROM t",
            "SELECT * FROM t WHERE LOWER(name) LIKE '%x%'",
            "SELECT * FROM t WHERE name LIKE ?",
            "SELECT * FROM t WHERE 1 = name LIKE '%x%'",
            "SELECT * FROM t WHERE name LIKE '%x%' COLLATE utf8mb4_bin",
            "SELECT * FROM t WHERE (name LIKE '%x%') IS NULL",
            "SELECT * FROM t WHERE a <=> b AND name LIKE '%xyz%'",
        ] {
            assert!(likes(q).is_empty(), "{q}");
        }
        // In a subquery's WHERE, and in ON.
        assert_eq!(
            likes("SELECT * FROM u WHERE id IN (SELECT id FROM t WHERE name LIKE '%abc%')").len(),
            1
        );
        assert_eq!(
            likes("SELECT * FROM u JOIN t ON t.id = u.id AND t.name LIKE '%abc%'").len(),
            1
        );
        assert_eq!(
            like_runs("%red_sh%oe\\%x%", '\\'),
            vec!["red", "sh", "oe%x"]
        );
        assert_eq!(like_runs("abc", '\\'), vec!["abc"]);
        assert!(like_runs("%_%", '\\').is_empty());
        assert!(mentions_like(b"select 1 from t where a LiKe 'x'"));
    }

    #[test]
    fn top_k_statements() {
        let tk = |q: &str| top_k(q, &scan(q).unwrap());
        assert_eq!(
            tk(
                "SELECT id, MATCH(name) AGAINST ('red') AS score FROM t WHERE MATCH(name) AGAINST ('red') ORDER BY score DESC LIMIT 10"
            ),
            Some(TopK {
                offset: 0,
                limit: 10
            })
        );
        assert_eq!(
            tk(
                "SELECT id FROM t WHERE MATCH(name) AGAINST ('red') AND price < 5 ORDER BY MATCH(name) AGAINST ('red') DESC LIMIT 5, 10;"
            ),
            Some(TopK {
                offset: 5,
                limit: 10
            })
        );
        assert_eq!(
            tk("SELECT id, MATCH(name) AGAINST ('red') s FROM t ORDER BY s DESC LIMIT 3 OFFSET 2"),
            Some(TopK {
                offset: 2,
                limit: 3
            })
        );
        for q in [
            // Ascending, a second key, no LIMIT, a different search, a
            // join, a group, a subquery, an aggregate, a placeholder.
            "SELECT id, MATCH(name) AGAINST ('red') AS score FROM t ORDER BY score LIMIT 10",
            "SELECT id, MATCH(name) AGAINST ('red') AS score FROM t ORDER BY score DESC, id LIMIT 10",
            "SELECT id, MATCH(name) AGAINST ('red') AS score FROM t ORDER BY score DESC",
            "SELECT id FROM t WHERE MATCH(name) AGAINST ('red') ORDER BY MATCH(name) AGAINST ('blue') DESC LIMIT 1",
            "SELECT t.id, MATCH(name) AGAINST ('red') AS s FROM t JOIN u ON u.id = t.id ORDER BY s DESC LIMIT 1",
            "SELECT k, MATCH(name) AGAINST ('red') AS s FROM t GROUP BY k ORDER BY s DESC LIMIT 1",
            "SELECT id, MATCH(name) AGAINST ('red') AS s FROM t WHERE id IN (SELECT id FROM u) ORDER BY s DESC LIMIT 1",
            "SELECT COUNT(*), MATCH(name) AGAINST ('red') AS s FROM t ORDER BY s DESC LIMIT 1",
            "SELECT id, MATCH(name) AGAINST ('red') AS s FROM t WHERE x = ? ORDER BY s DESC LIMIT 1",
            "SELECT id, MATCH(name) AGAINST ('red') AS s FROM t ORDER BY s DESC LIMIT 1 FOR UPDATE",
        ] {
            assert_eq!(tk(q), None, "{q}");
        }
    }

    #[test]
    fn finds_match_calls_and_context() {
        let sql = "SELECT id, MATCH(name, p.description) AGAINST ('red shoe') AS score \
                   FROM shop.products AS p JOIN inv i ON i.pid = p.id \
                   WHERE MATCH(name) AGAINST ('+red -kids' IN BOOLEAN MODE) AND price < 100 \
                   ORDER BY score DESC";
        let s = scan(sql).unwrap();
        assert_eq!(s.matches.len(), 2);
        assert!(!s.matches[0].condition);
        assert!(s.matches[1].condition);
        assert_eq!(s.matches[1].mode, Mode::Boolean);
        assert_eq!(
            s.matches[0].columns[1],
            (Some("p".into()), "description".into())
        );
        assert_eq!(
            &sql[s.matches[0].span.0..s.matches[0].span.1],
            "MATCH(name, p.description) AGAINST ('red shoe')"
        );
        assert_eq!(
            s.tables[0],
            TableRef {
                db: Some("shop".into()),
                table: "products".into(),
                alias: Some("p".into())
            }
        );
        assert_eq!(s.tables[1].alias.as_deref(), Some("i"));
    }

    #[test]
    fn placeholders_and_subqueries() {
        let s = scan("SELECT * FROM t WHERE id IN (SELECT id FROM u WHERE MATCH(a) AGAINST (?))")
            .unwrap();
        assert!(s.matches[0].text.is_none());
        assert!(s.matches[0].condition);
        let s = scan("SELECT MATCH(a) AGAINST ('x' WITH QUERY EXPANSION) FROM t").unwrap();
        assert_eq!(s.matches[0].mode, Mode::Expansion);
        // In a string, not a call.
        assert!(
            scan("SELECT 'MATCH(a) AGAINST (1)'")
                .unwrap()
                .matches
                .is_empty()
        );
    }

    #[test]
    fn finds_sprig_search() {
        let sql = "SELECT p.id, s.score FROM sprig_search('product_text', 'red \"running shoe\"', \
                   JSON_OBJECT('limit', 20, 'consistency', 'own_writes', 'highlight', JSON_ARRAY('name'))) AS s \
                   JOIN products p ON p.id = s.id";
        let s = scan(sql).unwrap();
        assert_eq!(s.searches.len(), 1);
        let c = &s.searches[0];
        assert_eq!(c.index, "product_text");
        let o = options_json(c.options.as_deref().unwrap()).unwrap();
        assert_eq!(o["limit"], 20);
        assert_eq!(o["highlight"][0], "name");
        assert_eq!(s.tables.last().unwrap().table, "products");
    }

    #[test]
    fn interpolates_placeholders_only() {
        let sql = "SELECT '?', `a?` FROM t WHERE MATCH(a) AGAINST (?) AND id > ? -- ?";
        assert_eq!(placeholders(sql).len(), 2);
        assert_eq!(
            interpolate(sql, &["'red shoe'".into(), "5".into()]).unwrap(),
            "SELECT '?', `a?` FROM t WHERE MATCH(a) AGAINST ('red shoe') AND id > 5 -- ?"
        );
        assert!(interpolate(sql, &["1".into()]).is_none());
    }
}
