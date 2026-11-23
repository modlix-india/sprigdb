//! `GRAPH_TABLE` in SQL (SQL/PGQ) as text: finding the calls in a
//! statement, their Cypher, and the derived table of literal rows a call's
//! result becomes.
//!
//! ```text
//! find(sql)                       → each GRAPH_TABLE (g MATCH … COLUMNS (…)): span, graph, Cypher, columns
//! bind_params(sql, sql, cypher)   → a prepared statement's `?` written in as literals
//! rows_sql(columns, rows)         → SELECT … UNION ALL VALUES (…), …: the rows as a derived table
//! ```

use std::fmt::Write as _;

use sprig_common::{Result, SprigCode, SprigError};

/// One `GRAPH_TABLE (...)` in a statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphCall {
    /// Byte range of `GRAPH_TABLE ( ... )` in the statement it was found in.
    pub span: (usize, usize),
    /// The database, if the graph name is qualified.
    pub db: Option<String>,
    /// The graph.
    pub graph: String,
    /// The Cypher read it runs.
    pub cypher: String,
    /// The output column names.
    pub columns: Vec<String>,
    /// `MATCH <pattern>`, before the call's own `WHERE`.
    pub head: String,
    /// The call's own condition, in Cypher.
    pub cond: Option<String>,
    /// Per column, the Cypher expression it returns.
    pub returns: Vec<String>,
}

impl GraphCall {
    /// The Cypher read with `extra` conditions (Cypher, from the outer
    /// statement's filters) added to its `WHERE`; the call's
    /// own [`Self::cypher`] when there are none.
    #[must_use]
    pub fn cypher_with(&self, extra: &[String]) -> String {
        if extra.is_empty() || self.returns.len() != self.columns.len() {
            return self.cypher.clone();
        }
        let mut conds: Vec<String> = self.cond.iter().map(|c| format!("({c})")).collect();
        conds.extend(extra.iter().cloned());
        let ret: Vec<String> = self
            .returns
            .iter()
            .zip(&self.columns)
            .map(|(e, c)| format!("{e} AS `{}`", c.replace('`', "``")))
            .collect();
        format!(
            "{} WHERE {} RETURN {}",
            self.head,
            conds.join(" AND "),
            ret.join(", ")
        )
    }
}

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::GraphSyntax, m.into())
}

/// The index after the `)` matching the `(` at `open`, skipping quoted
/// text.
fn matching(b: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The position of keyword `kw` at paren depth 0 of `s` (outside quotes),
/// as a whole word.
fn keyword(s: &str, kw: &str) -> Option<usize> {
    let b = s.as_bytes();
    let k = kw.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == b'\\' && q != b'`' {
                i += 2;
                continue;
            }
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match c {
            b'\'' | b'"' | b'`' => quote = Some(c),
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            _ if depth == 0
                && i + k.len() <= b.len()
                && b[i..i + k.len()].eq_ignore_ascii_case(k)
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_'))
                && b.get(i + k.len())
                    .is_none_or(|x| !(x.is_ascii_alphanumeric() || *x == b'_')) =>
            {
                return Some(i);
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// A pattern in SQL/PGQ form as Cypher: `(c IS Person)` → `(c:Person)`,
/// `[o IS BOUGHT]` → `[o:BOUGHT]`.
#[must_use]
pub fn pattern_to_cypher(p: &str) -> String {
    let mut out = String::with_capacity(p.len());
    let b = p.as_bytes();
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            let ch = p[i..].chars().next().unwrap_or(' ');
            out.push(ch);
            if c == q {
                quote = None;
            }
            i += ch.len_utf8();
            continue;
        }
        if matches!(c, b'\'' | b'"' | b'`') {
            quote = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }
        let word_start = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if word_start
            && i + 2 <= b.len()
            && b[i..i + 2].eq_ignore_ascii_case(b"IS")
            && b.get(i + 2).is_some_and(u8::is_ascii_whitespace)
        {
            while out.ends_with(' ') {
                out.pop();
            }
            out.push(':');
            i += 2;
            while i < b.len() && b[i].is_ascii_whitespace() {
                i += 1;
            }
            continue;
        }
        // Copy one UTF-8 character.
        let ch = p[i..].chars().next().unwrap_or(' ');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Splits `s` at commas of paren depth 0.
fn split_top(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut quote: Option<char> = None;
    for c in s.chars() {
        if let Some(q) = quote {
            cur.push(c);
            if c == q {
                quote = None;
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                quote = Some(c);
                cur.push(c);
            }
            '(' | '[' | '{' => {
                depth += 1;
                cur.push(c);
            }
            ')' | ']' | '}' => {
                depth -= 1;
                cur.push(c);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

fn unquote(s: &str) -> String {
    let s = s.trim();
    s.strip_prefix('`')
        .and_then(|x| x.strip_suffix('`'))
        .map_or_else(|| s.to_owned(), |x| x.replace("``", "`"))
}

/// Whether `sql` may hold a `GRAPH_TABLE` (a cheap word scan).
#[must_use]
pub fn mentions(sql: &str) -> bool {
    sql.len() < (1 << 20)
        && sql
            .as_bytes()
            .windows(11)
            .any(|w| w.eq_ignore_ascii_case(b"graph_table"))
}

/// Every `GRAPH_TABLE (...)` of `sql`, in order.
///
/// # Errors
/// 39900 for a malformed one.
pub fn find(sql: &str) -> Result<Vec<GraphCall>> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = keyword(&sql[from..], "GRAPH_TABLE") {
        let start = from + rel;
        let mut open = start + "GRAPH_TABLE".len();
        while open < b.len() && b[open].is_ascii_whitespace() {
            open += 1;
        }
        if b.get(open) != Some(&b'(') {
            return Err(syntax(
                "GRAPH_TABLE needs ( graph MATCH ... COLUMNS (...) )",
            ));
        }
        let end = matching(b, open).ok_or_else(|| syntax("unbalanced GRAPH_TABLE ( ... )"))?;
        let inner = sql[open + 1..end - 1].trim();
        let m = keyword(inner, "MATCH").ok_or_else(|| syntax("GRAPH_TABLE needs MATCH"))?;
        let name = inner[..m].trim().trim_end_matches(',').trim();
        if name.is_empty() {
            return Err(syntax("GRAPH_TABLE needs a graph name"));
        }
        let (db, graph) = match name.split_once('.') {
            Some((d, g)) => (Some(unquote(d)), unquote(g)),
            None => (None, unquote(name)),
        };
        let rest = &inner[m + "MATCH".len()..];
        let c =
            keyword(rest, "COLUMNS").ok_or_else(|| syntax("GRAPH_TABLE needs COLUMNS (...)"))?;
        let body = &rest[..c];
        let (pattern, cond) = match keyword(body, "WHERE") {
            Some(w) => (&body[..w], Some(&body[w + "WHERE".len()..])),
            None => (body, None),
        };
        let cols_text = rest[c + "COLUMNS".len()..].trim();
        let cols_text = cols_text
            .strip_prefix('(')
            .and_then(|x| x.strip_suffix(')'))
            .ok_or_else(|| syntax("COLUMNS needs ( expr AS name, ... )"))?;
        let mut returned = Vec::new();
        let mut returns = Vec::new();
        let mut columns = Vec::new();
        for item in split_top(cols_text) {
            let item = item.trim();
            let (expr, name) = if let Some(a) = keyword(item, "AS") {
                (item[..a].trim(), unquote(&item[a + 2..]))
            } else {
                let n = item.rsplit('.').next().unwrap_or(item);
                (item, unquote(n))
            };
            returned.push(format!(
                "{} AS `{}`",
                pattern_to_cypher(expr),
                name.replace('`', "``")
            ));
            returns.push(pattern_to_cypher(expr));
            columns.push(name);
        }
        if columns.is_empty() {
            return Err(syntax("COLUMNS needs at least one column"));
        }
        let head = format!("MATCH {}", pattern_to_cypher(pattern.trim()));
        let cond = cond.map(|w| pattern_to_cypher(w.trim()));
        let mut cypher = head.clone();
        if let Some(w) = &cond {
            let _ = write!(cypher, " WHERE {w}");
        }
        let _ = write!(cypher, " RETURN {}", returned.join(", "));
        out.push(GraphCall {
            span: (start, end),
            db,
            graph,
            cypher,
            columns,
            head,
            cond,
            returns,
        });
        from = end;
    }
    Ok(out)
}

fn quote_ident(c: &str) -> String {
    format!("`{}`", c.replace('`', "``"))
}

/// The byte ranges of the `?` placeholders of `sql`, outside quotes and
/// comments, in order.
#[must_use]
pub fn param_spans(sql: &str) -> Vec<(usize, usize)> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            q @ (b'\'' | b'"' | b'`') => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == q {
                        if b.get(i + 1) == Some(&q) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b'?' => out.push((i, i + 1)),
            _ => {}
        }
        i += 1;
    }
    out
}

/// `sql` (a statement with `GRAPH_TABLE`) with each `?` replaced by its
/// value: `sql_lits[i]` outside the calls,
/// `cypher_lits[i]` inside a call's pattern, condition or columns. `None`
/// when the counts differ.
#[must_use]
pub fn bind_params(sql: &str, sql_lits: &[String], cypher_lits: &[String]) -> Option<String> {
    let spans = param_spans(sql);
    if spans.len() != sql_lits.len() || spans.len() != cypher_lits.len() {
        return None;
    }
    let calls: Vec<(usize, usize)> = find(sql).ok()?.iter().map(|c| c.span).collect();
    let mut out = String::with_capacity(sql.len() + 16 * spans.len());
    let mut last = 0;
    for (k, (a, b)) in spans.iter().enumerate() {
        out.push_str(&sql[last..*a]);
        let inside = calls.iter().any(|(s, e)| *s <= *a && *a < *e);
        out.push_str(if inside {
            &cypher_lits[k]
        } else {
            &sql_lits[k]
        });
        last = *b;
    }
    out.push_str(&sql[last..]);
    Some(out)
}

/// One value of a graph row, as the router needs it: its SQL literal and,
/// for integers and strings, the value (to route a key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphLit {
    /// `NULL`.
    Null,
    /// An integer.
    Int(i64),
    /// A string (its literal is quoted and escaped).
    Str(String),
    /// Anything else, as its SQL literal (floats, booleans, JSON text).
    Other(String),
}

impl GraphLit {
    /// The SQL literal.
    #[must_use]
    pub fn sql(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Int(i) => i.to_string(),
            Self::Str(s) => sql_string(s),
            Self::Other(t) => t.clone(),
        }
    }
}

/// A string as a MariaDB literal (backslashes escaped, as the default
/// `sql_mode` reads them).
#[must_use]
pub fn sql_string(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for c in s.chars() {
        match c {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            _ => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// The rows as a derived table's body: `SELECT a AS c1, b AS c2 UNION ALL
/// VALUES (…), (…)` (one flat list, so parsers don't nest a set operation
/// per row), or an empty one.
#[must_use]
pub fn rows_sql(columns: &[String], rows: &[Vec<GraphLit>]) -> String {
    let Some((first, rest)) = rows.split_first() else {
        let cols: Vec<String> = columns
            .iter()
            .map(|c| format!("NULL AS {}", quote_ident(c)))
            .collect();
        return format!("SELECT {} FROM DUAL WHERE FALSE", cols.join(", "));
    };
    let mut s = String::from("SELECT ");
    for (k, v) in first.iter().enumerate() {
        if k > 0 {
            s.push_str(", ");
        }
        s.push_str(&v.sql());
        let _ = write!(
            s,
            " AS {}",
            quote_ident(columns.get(k).map_or("c", String::as_str))
        );
    }
    for (i, r) in rest.iter().enumerate() {
        s.push_str(if i == 0 { " UNION ALL VALUES (" } else { ", (" });
        for (k, v) in r.iter().enumerate() {
            if k > 0 {
                s.push_str(", ");
            }
            s.push_str(&v.sql());
        }
        s.push(')');
    }
    s
}

#[cfg(test)]
mod tests;
