//! Where a fragment of a body (a statement or an expression) names a
//! visible variable: both servers read an unqualified name that matches a
//! variable or parameter as the variable, before any column. The runtime
//! puts each variable's value there as a literal when the statement runs.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, visit_expressions};

use super::super::define::lex::{self, T};

/// One use of a variable: the bytes `start..end` of the fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ref {
    /// First byte.
    pub start: usize,
    /// One past the last byte.
    pub end: usize,
    /// The variable's slot.
    pub slot: usize,
}

/// A fragment ready to run: its text, the variables it reads, and the
/// aliases its select list needs so its columns keep their written names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Text {
    /// The fragment as written.
    pub sql: String,
    /// Variables, in text order.
    pub refs: Vec<Ref>,
    /// (byte offset, alias): `AS` and the quoted alias go there.
    pub aliases: Vec<(usize, String)>,
}

/// Byte offset of a 1-based (line, column-in-characters) location.
pub(crate) fn offset(text: &str, line: u64, col: u64) -> Option<usize> {
    if line == 0 || col == 0 {
        return None;
    }
    let mut start = 0usize;
    for _ in 1..line {
        start += text[start..].find('\n')? + 1;
    }
    let mut chars = text[start..].char_indices();
    let mut at = start;
    for _ in 1..col {
        let (_, c) = chars.next()?;
        at += c.len_utf8();
    }
    Some(at)
}

/// The variables `sql` reads (`expr`: `sql` is an expression), by
/// `find` (a name's visible slot).
pub(crate) fn text(sql: &str, expr: bool, find: &dyn Fn(&str) -> Option<usize>) -> Text {
    let wrapped;
    let (src, shift) = if expr {
        wrapped = format!("SELECT {sql}");
        (wrapped.as_str(), 7usize)
    } else {
        (sql, 0)
    };
    let mut refs = match sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, src)
    {
        Ok(stmts) => {
            let mut out = Vec::new();
            let _ = visit_expressions(&stmts, |e| {
                if let Expr::Identifier(id) = e
                    && matches!(id.quote_style, None | Some('`'))
                    && let Some(slot) = find(&id.value)
                    && let (Some(a), Some(b)) = (
                        offset(src, id.span.start.line, id.span.start.column),
                        offset(src, id.span.end.line, id.span.end.column),
                    )
                    && a >= shift
                    && b > a
                {
                    out.push(Ref {
                        start: a - shift,
                        end: b - shift,
                        slot,
                    });
                }
                // A trigger's `NEW.col` / `OLD.col`.
                if let Expr::CompoundIdentifier(ids) = e
                    && let [row, col] = ids.as_slice()
                    && row.quote_style.is_none()
                    && (row.value.eq_ignore_ascii_case("NEW")
                        || row.value.eq_ignore_ascii_case("OLD"))
                    && let Some(slot) =
                        find(&format!("{}.{}", row.value.to_ascii_uppercase(), col.value))
                    && let (Some(a), Some(b)) = (
                        offset(src, row.span.start.line, row.span.start.column),
                        offset(src, col.span.end.line, col.span.end.column),
                    )
                    && a >= shift
                    && b > a
                {
                    out.push(Ref {
                        start: a - shift,
                        end: b - shift,
                        slot,
                    });
                }
                ControlFlow::<()>::Continue(())
            });
            out
        }
        Err(_) => tokens(sql, find),
    };
    refs.sort_by_key(|r| r.start);
    refs.dedup_by_key(|r| r.start);
    let aliases = if expr {
        Vec::new()
    } else {
        select_aliases(sql, &refs)
    };
    Text {
        sql: sql.to_owned(),
        refs,
        aliases,
    }
}

/// The token reading of [`text`], for statements the parser doesn't take:
/// a name that is a variable, not after `.` or `AS`, not before `.` or `(`.
fn tokens(sql: &str, find: &dyn Fn(&str) -> Option<usize>) -> Vec<Ref> {
    let Ok(toks) = lex::lex(sql) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (i, t) in toks.iter().enumerate() {
        let (T::Word(name) | T::Quoted(name)) = &t.t else {
            continue;
        };
        let prev = i.checked_sub(1).and_then(|k| toks.get(k));
        let next = toks.get(i + 1);
        // A trigger's `NEW.col` / `OLD.col`.
        if (name.eq_ignore_ascii_case("NEW") || name.eq_ignore_ascii_case("OLD"))
            && !prev.is_some_and(|p| p.op("."))
            && next.is_some_and(|n| n.op("."))
            && let Some(c) = toks.get(i + 2)
            && let T::Word(col) | T::Quoted(col) = &c.t
        {
            if let Some(slot) = find(&format!("{}.{col}", name.to_ascii_uppercase())) {
                let end = match &c.t {
                    T::Quoted(_) => sql[c.at + 1..]
                        .find('`')
                        .map_or(sql.len(), |k| c.at + 2 + k),
                    _ => c.at + col.len(),
                };
                out.push(Ref {
                    start: t.at,
                    end,
                    slot,
                });
            }
            continue;
        }
        if prev.is_some_and(|p| p.op(".") || p.is("AS"))
            || next.is_some_and(|n| n.op(".") || n.op("("))
        {
            continue;
        }
        let Some(slot) = find(name) else { continue };
        let end = match &t.t {
            T::Quoted(_) => sql[t.at + 1..]
                .find('`')
                .map_or(sql.len(), |k| t.at + 2 + k),
            _ => t.at + name.len(),
        };
        out.push(Ref {
            start: t.at,
            end,
            slot,
        });
    }
    out
}

/// Words that end a select list.
const LIST_END: [&str; 14] = [
    "FROM",
    "INTO",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "LIMIT",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WINDOW",
    "FOR",
    "LOCK",
    "PROCEDURE",
];

/// For a `SELECT`, the items of its first select list that read a variable
/// and have no alias: each gets its written text as its alias.
pub(crate) fn select_aliases(sql: &str, refs: &[Ref]) -> Vec<(usize, String)> {
    if refs.is_empty() {
        return Vec::new();
    }
    let Ok(toks) = lex::lex(sql) else {
        return Vec::new();
    };
    let mut i = 0;
    while i < toks.len() && toks[i].op("(") {
        i += 1;
    }
    if !toks.get(i).is_some_and(|t| t.is("SELECT")) {
        return Vec::new();
    }
    i += 1;
    while toks.get(i).is_some_and(|t| {
        [
            "DISTINCT",
            "ALL",
            "DISTINCTROW",
            "HIGH_PRIORITY",
            "STRAIGHT_JOIN",
        ]
        .iter()
        .any(|w| t.is(w))
            || matches!(&t.t, T::Word(w) if w.to_ascii_uppercase().starts_with("SQL_"))
    }) {
        i += 1;
    }
    let mut items: Vec<(usize, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut cases = 0usize;
    let mut first = i;
    let mut j = i;
    while j < toks.len() {
        let t = &toks[j];
        if t.op("(") {
            depth += 1;
        } else if t.op(")") {
            if depth == 0 {
                break;
            }
            depth -= 1;
        } else if depth == 0 {
            if t.is("CASE") {
                cases += 1;
            } else if t.is("END") && cases > 0 {
                cases -= 1;
            } else if t.op(",") {
                items.push((first, j));
                first = j + 1;
            } else if LIST_END.iter().any(|w| t.is(w)) || t.op(";") {
                break;
            }
        }
        j += 1;
    }
    items.push((first, j));
    let end_of = |k: usize| toks.get(k).map_or(sql.len(), |t| t.at);
    let mut out = Vec::new();
    for (a, b) in items {
        if b <= a {
            continue;
        }
        let (s, e) = (toks[a].at, end_of(b));
        let item = sql[s..e].trim_end();
        let e = s + item.len();
        if !refs.iter().any(|r| r.start >= s && r.end <= e) {
            continue;
        }
        // An alias written: `AS name`, or a name after a complete operand.
        let last = &toks[b - 1];
        let named = matches!(&last.t, T::Word(_) | T::Quoted(_) | T::Str(_)) && b - a >= 2 && {
            let before = &toks[b - 2];
            before.is("AS")
                || (matches!(&last.t, T::Word(w) if !is_operand_word(w))
                    || matches!(&last.t, T::Quoted(_)))
                    && (matches!(&before.t, T::Word(_) | T::Quoted(_) | T::Num(_) | T::Str(_))
                        || before.op(")"))
                    && !before.op(".")
        };
        if named {
            continue;
        }
        out.push((e, item.to_owned()));
    }
    out
}

/// Words that end an expression without being an alias.
fn is_operand_word(w: &str) -> bool {
    [
        "END",
        "NULL",
        "TRUE",
        "FALSE",
        "UNKNOWN",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MONTH",
        "YEAR",
        "WEEK",
        "QUARTER",
        "MICROSECOND",
    ]
    .iter()
    .any(|k| w.eq_ignore_ascii_case(k))
}

/// `name` quoted for an alias.
pub(crate) fn quote(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// `t` with each variable replaced by `lit(slot)` and the aliases added.
pub(crate) fn render(t: &Text, lit: &dyn Fn(usize) -> String) -> String {
    let mut out = String::with_capacity(t.sql.len() + 16);
    let mut at = 0;
    let mut aliases = t.aliases.iter().peekable();
    for r in &t.refs {
        while let Some((pos, a)) = aliases.peek() {
            if *pos > r.start {
                break;
            }
            out.push_str(&t.sql[at..*pos]);
            out.push_str(" AS ");
            out.push_str(&quote(a));
            at = *pos;
            aliases.next();
        }
        out.push_str(&t.sql[at..r.start]);
        out.push_str(&lit(r.slot));
        at = r.end;
    }
    for (pos, a) in aliases {
        out.push_str(&t.sql[at..*pos]);
        out.push_str(" AS ");
        out.push_str(&quote(a));
        at = *pos;
    }
    out.push_str(&t.sql[at..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find(n: &str) -> Option<usize> {
        match n.to_ascii_lowercase().as_str() {
            "x" => Some(0),
            "a" => Some(1),
            _ => None,
        }
    }

    #[test]
    fn variables_are_found_where_expressions_are() {
        let t = text(
            "SELECT x, a + 1, t.a, b AS a FROM t WHERE a = x LIMIT x",
            false,
            &find,
        );
        assert_eq!(t.refs.len(), 5, "{t:?}");
        let s = render(&t, &|s| format!("<{s}>"));
        assert_eq!(
            s,
            "SELECT <0> AS `x`, <1> + 1 AS `a + 1`, t.a, b AS a FROM t WHERE <1> = <0> LIMIT <0>"
        );
        let t = text("INSERT INTO t (a, x) VALUES (a, x + 1)", false, &find);
        assert_eq!(
            render(&t, &|s| format!("<{s}>")),
            "INSERT INTO t (a, x) VALUES (<1>, <0> + 1)"
        );
        let t = text("UPDATE t SET a = a + x WHERE t.x = 1", false, &find);
        assert_eq!(
            render(&t, &|s| format!("<{s}>")),
            "UPDATE t SET a = <1> + <0> WHERE t.x = 1"
        );
        let t = text("CONCAT(`x`, 'x', a)", true, &find);
        assert_eq!(render(&t, &|s| format!("<{s}>")), "CONCAT(<0>, 'x', <1>)");
    }
}
