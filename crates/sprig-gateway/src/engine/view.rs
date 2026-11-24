//! SQL views on engine databases.
//!
//! A view is a catalog object (kind `View`) whose definition text is the
//! `SHOW CREATE VIEW` text, as the creating port's server prints it:
//!
//! ```text
//! CREATE [OR REPLACE] | ALTER  [ALGORITHM=..] [DEFINER=..] [SQL SECURITY ..]
//!   VIEW v [(cols)] AS select [WITH [CASCADED|LOCAL] CHECK OPTION]
//!     header read here (`parse_header`); the select is bound by the
//!     planner against the session's catalog (unknown tables and columns
//!     are the servers' errors) and described (the view's column names)
//!   → the stored text: `CREATE ALGORITHM=UNDEFINED DEFINER=`u`@`h`
//!     SQL SECURITY DEFINER VIEW `v` AS select `t`.`a` AS `a`,... from `t`
//!     [WITH CASCADED CHECK OPTION]` (`render`): every column qualified,
//!     every select item named, keywords in lower case; MariaDB with the
//!     fewest parentheses, MySQL with every operator in parentheses
//! reading: the catalog module parses each view once (`load`), with its
//!   tables qualified by the view's database; a statement naming the view
//!   reads it as a derived table of that select (`expand`), which the
//!   planner merges into the statement (ALGORITHM=MERGE) or, where it
//!   can't, computes first (TEMPTABLE)
//! ```

use std::fmt::Write as _;
use std::sync::Arc;

use sprig_common::ids::{DbId, ObjectId};
use sprig_common::{Result, SprigError};
use sqlparser::ast::{
    BinaryOperator, CaseWhen, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause, ObjectName,
    ObjectNamePart, OrderByKind, Query, Select, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, SetOperator, SetQuantifier, Spanned, TableAlias, TableAliasColumnDef, TableFactor,
    TableWithJoins, UnaryOperator, Value, VisitMut, VisitorMut,
};

use super::define::expr::{quote_name, quote_str};
use super::define::lex::{self, T, Tok};

/// `ALGORITHM=`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Algorithm {
    Undefined,
    Merge,
    Temptable,
}

impl Algorithm {
    pub(crate) fn word(self) -> &'static str {
        match self {
            Self::Undefined => "UNDEFINED",
            Self::Merge => "MERGE",
            Self::Temptable => "TEMPTABLE",
        }
    }
}

/// `WITH ... CHECK OPTION`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckOption {
    Cascaded,
    Local,
}

/// A view statement's header, and where its select is.
#[derive(Debug, Clone, Default)]
pub(crate) struct Header {
    pub alter: bool,
    pub algorithm: Option<Algorithm>,
    /// `DEFINER = user@host`; `None` when not written (or `CURRENT_USER`).
    pub definer: Option<(String, String)>,
    /// `SQL SECURITY INVOKER` (`Some(false)`: `DEFINER` written).
    pub invoker: Option<bool>,
    pub db: Option<String>,
    pub name: String,
    pub columns: Option<Vec<String>>,
    /// The select's text.
    pub body: String,
    pub check: Option<CheckOption>,
}

fn syntax(sql: &str, at: usize) -> SprigError {
    let near: String = sql.get(at..).unwrap_or("").chars().take(80).collect();
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             MariaDB server version for the right syntax to use near '{near}' at line 1"
        ),
    )
}

fn name_of(t: &Tok) -> Option<String> {
    match &t.t {
        T::Word(w) | T::Quoted(w) | T::Str(w) => Some(w.clone()),
        _ => None,
    }
}

/// Reads `CREATE ... VIEW` / `ALTER ... VIEW`.
///
/// # Errors
/// 1064 for a header it can't read.
#[allow(clippy::too_many_lines)] // one clause after another
pub(crate) fn parse_header(sql: &str) -> Result<Header> {
    let toks = lex::lex(sql).map_err(|at| syntax(sql, at))?;
    let mut i = 0;
    let at = |i: usize| toks.get(i).map_or(sql.len(), |t| t.at);
    let word = |i: usize, w: &str| toks.get(i).is_some_and(|t| t.is(w));
    let op = |i: usize, o: &str| toks.get(i).is_some_and(|t| t.op(o));
    let mut h = Header::default();
    if word(i, "CREATE") {
        i += 1;
        if word(i, "OR") && word(i + 1, "REPLACE") {
            i += 2;
        }
    } else if word(i, "ALTER") {
        h.alter = true;
        i += 1;
    } else {
        return Err(syntax(sql, at(i)));
    }
    loop {
        if word(i, "ALGORITHM") && op(i + 1, "=") {
            h.algorithm = Some(match toks.get(i + 2) {
                Some(t) if t.is("UNDEFINED") => Algorithm::Undefined,
                Some(t) if t.is("MERGE") => Algorithm::Merge,
                Some(t) if t.is("TEMPTABLE") => Algorithm::Temptable,
                _ => return Err(syntax(sql, at(i + 2))),
            });
            i += 3;
        } else if word(i, "DEFINER") && op(i + 1, "=") {
            i += 2;
            if word(i, "CURRENT_USER") {
                i += 1;
                if op(i, "(") && op(i + 1, ")") {
                    i += 2;
                }
                h.definer = None;
            } else {
                let user = toks
                    .get(i)
                    .and_then(name_of)
                    .ok_or_else(|| syntax(sql, at(i)))?;
                i += 1;
                let host = match toks.get(i).map(|t| &t.t) {
                    Some(T::Var(v)) if v == "@" => {
                        let h = toks
                            .get(i + 1)
                            .and_then(name_of)
                            .ok_or_else(|| syntax(sql, at(i + 1)))?;
                        i += 2;
                        h
                    }
                    Some(T::Var(v)) if v.starts_with('@') && !v.starts_with("@@") => {
                        i += 1;
                        v[1..].to_owned()
                    }
                    _ => "%".to_owned(),
                };
                h.definer = Some((user, host));
            }
        } else if word(i, "SQL") && word(i + 1, "SECURITY") {
            h.invoker = Some(match toks.get(i + 2) {
                Some(t) if t.is("INVOKER") => true,
                Some(t) if t.is("DEFINER") => false,
                _ => return Err(syntax(sql, at(i + 2))),
            });
            i += 3;
        } else {
            break;
        }
    }
    if !word(i, "VIEW") {
        return Err(syntax(sql, at(i)));
    }
    i += 1;
    if word(i, "IF") && word(i + 1, "NOT") && word(i + 2, "EXISTS") {
        i += 3;
    }
    let first = toks
        .get(i)
        .and_then(name_of)
        .ok_or_else(|| syntax(sql, at(i)))?;
    i += 1;
    if op(i, ".") {
        h.db = Some(first);
        h.name = toks
            .get(i + 1)
            .and_then(name_of)
            .ok_or_else(|| syntax(sql, at(i + 1)))?;
        i += 2;
    } else {
        h.name = first;
    }
    if op(i, "(") {
        i += 1;
        let mut cols = Vec::new();
        loop {
            cols.push(
                toks.get(i)
                    .and_then(name_of)
                    .ok_or_else(|| syntax(sql, at(i)))?,
            );
            i += 1;
            if op(i, ",") {
                i += 1;
            } else if op(i, ")") {
                i += 1;
                break;
            } else {
                return Err(syntax(sql, at(i)));
            }
        }
        h.columns = Some(cols);
    }
    if !word(i, "AS") {
        return Err(syntax(sql, at(i)));
    }
    let start = at(i + 1);
    // The tail: `WITH [CASCADED | LOCAL] CHECK OPTION` [;].
    let mut end = toks.len();
    while end > i + 1 && toks[end - 1].op(";") {
        end -= 1;
    }
    let mut stop = toks.get(end).map_or(sql.len(), |t| t.at);
    if end >= 3 && toks[end - 1].is("OPTION") && toks[end - 2].is("CHECK") {
        let (kind, w) = if toks[end - 3].is("CASCADED") {
            (CheckOption::Cascaded, end - 4)
        } else if toks[end - 3].is("LOCAL") {
            (CheckOption::Local, end - 4)
        } else {
            (CheckOption::Cascaded, end - 3)
        };
        if w > i && toks[w].is("WITH") {
            h.check = Some(kind);
            stop = toks[w].at;
        }
    }
    sql.get(start..stop)
        .unwrap_or("")
        .trim()
        .clone_into(&mut h.body);
    if h.body.is_empty() {
        return Err(syntax(sql, start));
    }
    Ok(h)
}

/// Parses a view's select.
///
/// # Errors
/// 1064 when it isn't one query.
pub(crate) fn parse_query(body: &str) -> Result<Query> {
    // MariaDB prints a negation as `!(...)`, which the parser library
    // doesn't read: it is `NOT (...)`.
    let owned;
    let body = match lex::lex(body) {
        Ok(toks) if toks.iter().any(|t| t.op("!")) => {
            let mut s = body.to_owned();
            for t in toks.iter().rev().filter(|t| t.op("!")) {
                s.replace_range(t.at..=t.at, " NOT ");
            }
            owned = s;
            owned.as_str()
        }
        _ => body,
    };
    let mut stmts = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, body)
        .map_err(|e| {
            SprigError::standard(
                1064,
                *b"42000",
                format!("You have an error in your SQL syntax; {e}"),
            )
        })?;
    match (stmts.pop(), stmts.is_empty()) {
        (Some(sqlparser::ast::Statement::Query(q)), true) => Ok(*q),
        _ => Err(syntax(body, 0)),
    }
}

/// A view as the catalog module holds it.
#[derive(Debug, Clone)]
pub(crate) struct ViewDef {
    pub object: ObjectId,
    pub db: DbId,
    pub name: String,
    pub algorithm: Algorithm,
    pub definer: (String, String),
    pub invoker: bool,
    pub check: Option<CheckOption>,
    /// The view's column names.
    pub columns: Vec<String>,
    /// Its select, every table qualified by its database.
    pub query: Query,
    /// The stored text.
    pub text: Arc<str>,
    /// The select as stored (`VIEW_DEFINITION` is it with the database
    /// written in).
    pub body: String,
    /// `IS_UPDATABLE`.
    pub updatable: bool,
}

impl ViewDef {
    /// The `DEFINER` as `information_schema.VIEWS` shows it.
    pub(crate) fn definer_text(&self) -> String {
        format!("{}@{}", self.definer.0, self.definer.1)
    }
}

/// Reads a stored view text.
///
/// # Errors
/// The text isn't one the engine wrote.
pub(crate) fn load(object: ObjectId, db: DbId, db_name: &str, text: &str) -> Result<ViewDef> {
    let h = parse_header(text)?;
    let mut query = parse_query(&h.body)?;
    let columns = match &h.columns {
        Some(c) => c.clone(),
        None => output_names(&query, &h.body),
    };
    let algorithm = h.algorithm.unwrap_or(Algorithm::Undefined);
    let updatable = updatable(&query, algorithm, false);
    qualify_tables(&mut query, db_name);
    Ok(ViewDef {
        object,
        db,
        name: h.name,
        algorithm,
        definer: h.definer.unwrap_or_else(|| ("root".into(), "%".into())),
        invoker: h.invoker.unwrap_or(false),
        check: h.check,
        columns,
        query,
        text: Arc::from(text),
        body: h.body,
        updatable,
    })
}

/// Every unqualified table name of `q` (not a common table expression's)
/// qualified by `db`.
pub(crate) fn qualify_tables(q: &mut Query, db: &str) {
    struct Q<'a> {
        db: &'a str,
        ctes: Vec<String>,
    }
    impl VisitorMut for Q<'_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &mut Query) -> std::ops::ControlFlow<()> {
            if let Some(w) = &q.with {
                for c in &w.cte_tables {
                    self.ctes.push(c.alias.name.value.to_lowercase());
                }
            }
            std::ops::ControlFlow::Continue(())
        }
        fn pre_visit_relation(&mut self, n: &mut ObjectName) -> std::ops::ControlFlow<()> {
            if let [ObjectNamePart::Identifier(t)] = n.0.as_slice()
                && !self.ctes.contains(&t.value.to_lowercase())
            {
                n.0.insert(
                    0,
                    ObjectNamePart::Identifier(Ident::with_quote('`', self.db)),
                );
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let _ = VisitMut::visit(
        q,
        &mut Q {
            db,
            ctes: Vec::new(),
        },
    );
}

const AGGREGATES: [&str; 18] = [
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "group_concat",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "bit_and",
    "bit_or",
    "bit_xor",
    "json_arrayagg",
    "json_objectagg",
];

fn has_aggregate(e: &Expr) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if let Expr::Function(f) = x {
            let n = f.name.to_string().to_lowercase();
            if f.over.is_some() || AGGREGATES.contains(&n.as_str()) {
                found = true;
                return std::ops::ControlFlow::Break(());
            }
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

fn has_subquery(e: &Expr) -> bool {
    let mut found = false;
    let _ = sqlparser::ast::visit_expressions(e, |x| {
        if matches!(
            x,
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
        ) {
            found = true;
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    });
    found
}

/// Whether a view of `q` is updatable (`IS_UPDATABLE`): one plain select
/// over at least one table, no `DISTINCT`, aggregate, window function,
/// `GROUP BY` or `HAVING`, not `TEMPTABLE`; on MySQL also no subquery in
/// the select list.
pub(crate) fn updatable(q: &Query, algorithm: Algorithm, mysql: bool) -> bool {
    if algorithm == Algorithm::Temptable || q.with.is_some() {
        return false;
    }
    let SetExpr::Select(s) = &*q.body else {
        return false;
    };
    let grouped = match &s.group_by {
        GroupByExpr::Expressions(v, _) => !v.is_empty(),
        GroupByExpr::All(_) => true,
    };
    if s.distinct.is_some() || grouped || s.having.is_some() || s.from.is_empty() {
        return false;
    }
    !s.projection.iter().any(|it| match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            has_aggregate(e) || (mysql && has_subquery(e))
        }
        _ => false,
    })
}

/// The base tables a view's select names directly (`(database, table)`,
/// database `None` when unqualified), and the views among them are read
/// by the caller.
pub(crate) fn relations(q: &Query) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    let _ = sqlparser::ast::visit_relations(q, |n| {
        let parts: Vec<String> =
            n.0.iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                    ObjectNamePart::Function(_) => None,
                })
                .collect();
        match parts.as_slice() {
            [t] => out.push((None, t.clone())),
            [d, t] => out.push((Some(d.clone()), t.clone())),
            _ => {}
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    out
}

/// The text of `e` in `src`, from its span.
fn text_of(e: &Expr, src: &str) -> Option<String> {
    let sp = e.span();
    let off = |l: sqlparser::tokenizer::Location| -> Option<usize> {
        if l.line == 0 {
            return None;
        }
        let mut line = 1;
        let mut col = 1;
        for (i, ch) in src.char_indices() {
            if line == l.line && col == l.column {
                return Some(i);
            }
            if ch == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        (line == l.line && col == l.column).then_some(src.len())
    };
    let (a, mut b) = (off(sp.start)?, off(sp.end)?);
    // A call's span can stop before its closing parentheses.
    let open = |t: &str| {
        t.bytes()
            .filter(|c| *c == b'(')
            .count()
            .saturating_sub(t.bytes().filter(|c| *c == b')').count())
    };
    let mut depth = open(src.get(a..b)?);
    while depth > 0 {
        let rest = src.get(b..)?;
        let skip = rest.len() - rest.trim_start().len();
        if !rest[skip..].starts_with(')') {
            break;
        }
        b += skip + 1;
        depth -= 1;
    }
    src.get(a..b).map(str::to_owned)
}

/// The names of `q`'s result columns as written (alias, column name, or
/// the expression's text).
pub(crate) fn output_names(q: &Query, src: &str) -> Vec<String> {
    let mut body = &*q.body;
    loop {
        match body {
            SetExpr::SetOperation { left, .. } => body = left,
            SetExpr::Query(q) => body = &q.body,
            _ => break,
        }
    }
    let SetExpr::Select(s) = body else {
        return Vec::new();
    };
    s.projection
        .iter()
        .map(|it| match it {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(Expr::Identifier(i)) => i.value.clone(),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(p)) => {
                p.last().map_or_else(String::new, |i| i.value.clone())
            }
            SelectItem::UnnamedExpr(e) => text_of(e, src).unwrap_or_else(|| e.to_string()),
            other => other.to_string(),
        })
        .collect()
}

// ---- printing ----

/// One `FROM` item a column may come from: the qualifier it prints with,
/// and its columns.
#[derive(Debug, Clone)]
struct Factor {
    /// The name columns are qualified by in the text (unquoted).
    name: String,
    qual: String,
    cols: Vec<String>,
}

/// Prints a view's select the way the servers store it.
pub(crate) struct Printer<'a> {
    /// MySQL 8.4's forms (else MariaDB 11.8's).
    pub mysql: bool,
    /// The view's database: its tables print unqualified.
    pub db: &'a str,
    /// The select's text (the names of unnamed items).
    pub src: &'a str,
    /// The columns of table (or view) `table` of database `db`.
    pub cols: &'a dyn Fn(&str, &str) -> Option<Vec<String>>,
    scopes: Vec<Vec<Factor>>,
    /// Select-list names of the select being printed (`ORDER BY` may name
    /// them).
    aliases: Vec<Vec<String>>,
    ctes: Vec<(String, Vec<String>)>,
    /// The next query printed is an expression's subquery: its items
    /// print without names.
    bare_next: bool,
    bare: Vec<bool>,
}

// Binding strengths, loosest first (MariaDB's printing).
const P_OR: u8 = 2;
const P_XOR: u8 = 3;
const P_AND: u8 = 4;
const P_CMP: u8 = 7;
const P_BITOR: u8 = 8;
const P_BITAND: u8 = 9;
const P_SHIFT: u8 = 10;
const P_ADD: u8 = 11;
const P_MUL: u8 = 12;
const P_BITXOR: u8 = 13;
const P_TOP: u8 = 20;

fn bin_op(op: &BinaryOperator) -> Option<(&'static str, u8)> {
    use BinaryOperator as B;
    Some(match op {
        B::Or => ("or", P_OR),
        B::Xor => ("xor", P_XOR),
        B::And => ("and", P_AND),
        B::Eq => ("=", P_CMP),
        B::NotEq => ("<>", P_CMP),
        B::Lt => ("<", P_CMP),
        B::LtEq => ("<=", P_CMP),
        B::Gt => (">", P_CMP),
        B::GtEq => (">=", P_CMP),
        B::Spaceship => ("<=>", P_CMP),
        B::BitwiseOr => ("|", P_BITOR),
        B::BitwiseAnd => ("&", P_BITAND),
        B::PGBitwiseShiftLeft => ("<<", P_SHIFT),
        B::PGBitwiseShiftRight => (">>", P_SHIFT),
        B::Plus => ("+", P_ADD),
        B::Minus => ("-", P_ADD),
        B::Multiply => ("*", P_MUL),
        B::Divide => ("/", P_MUL),
        B::MyIntegerDivide => ("DIV", P_MUL),
        B::Modulo => ("MOD", P_MUL),
        B::BitwiseXor => ("^", P_BITXOR),
        _ => return None,
    })
}

fn negate_cmp(op: &BinaryOperator) -> Option<BinaryOperator> {
    use BinaryOperator as B;
    Some(match op {
        B::Eq => B::NotEq,
        B::NotEq => B::Eq,
        B::Lt => B::GtEq,
        B::GtEq => B::Lt,
        B::Gt => B::LtEq,
        B::LtEq => B::Gt,
        _ => return None,
    })
}

impl<'a> Printer<'a> {
    pub(crate) fn new(
        mysql: bool,
        db: &'a str,
        src: &'a str,
        cols: &'a dyn Fn(&str, &str) -> Option<Vec<String>>,
    ) -> Self {
        Self {
            mysql,
            db,
            src,
            cols,
            scopes: Vec::new(),
            aliases: Vec::new(),
            ctes: Vec::new(),
            bare_next: false,
            bare: Vec::new(),
        }
    }

    /// `q` as stored; `names`: the names of its result columns (the
    /// outermost select's items take them).
    pub(crate) fn query(&mut self, q: &Query, names: Option<&[String]>) -> String {
        let mut out = String::new();
        let bare = std::mem::take(&mut self.bare_next);
        self.bare.push(bare);
        let pushed = self.ctes.len();
        if let Some(w) = &q.with {
            out.push_str("with ");
            if w.recursive {
                out.push_str("recursive ");
            }
            for (i, c) in w.cte_tables.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let body = self.query(&c.query, None);
                let mut cols: Vec<String> = c
                    .alias
                    .columns
                    .iter()
                    .map(|d| d.name.value.clone())
                    .collect();
                if cols.is_empty() {
                    cols = output_names(&c.query, self.src);
                }
                out.push_str(&quote_name(&c.alias.name.value));
                if !c.alias.columns.is_empty() {
                    out.push_str(" (");
                    out.push_str(
                        &cols
                            .iter()
                            .map(|c| quote_name(c))
                            .collect::<Vec<_>>()
                            .join(","),
                    );
                    out.push(')');
                }
                out.push_str(" as (");
                out.push_str(&body);
                out.push(')');
                self.ctes.push((c.alias.name.value.to_lowercase(), cols));
            }
            out.push_str(if self.mysql { " " } else { "" });
        }
        out.push_str(&self.set_expr(&q.body, names));
        if let Some(ob) = &q.order_by
            && let OrderByKind::Expressions(es) = &ob.kind
        {
            out.push_str(" order by ");
            let items: Vec<String> = es
                .iter()
                .map(|o| {
                    let mut s = self.expr_top(&o.expr);
                    if matches!(o.options.sort, Some(sqlparser::ast::OrderBySort::Desc)) {
                        s.push_str(" desc");
                    }
                    s
                })
                .collect();
            out.push_str(&items.join(","));
        }
        match &q.limit_clause {
            Some(LimitClause::LimitOffset {
                limit: Some(l),
                offset,
                ..
            }) => {
                out.push_str(" limit ");
                if let Some(o) = offset {
                    out.push_str(&self.expr_top(&o.value));
                    out.push(',');
                }
                out.push_str(&self.expr_top(l));
            }
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                out.push_str(" limit ");
                out.push_str(&self.expr_top(offset));
                out.push(',');
                out.push_str(&self.expr_top(limit));
            }
            _ => {}
        }
        self.ctes.truncate(pushed);
        self.bare.pop();
        out
    }

    fn set_expr(&mut self, s: &SetExpr, names: Option<&[String]>) -> String {
        match s {
            SetExpr::Select(sel) => self.select(sel, names),
            SetExpr::Query(q) => format!("({})", self.query(q, names)),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let l = self.set_expr(left, names);
                let r = self.set_expr(right, None);
                let w = match op {
                    SetOperator::Union => "union",
                    SetOperator::Except | SetOperator::Minus => "except",
                    SetOperator::Intersect => "intersect",
                };
                let q = match set_quantifier {
                    SetQuantifier::All => " all",
                    _ => "",
                };
                format!("{l} {w}{q} {r}")
            }
            other => other.to_string(),
        }
    }

    fn base_factor(&self, name: &ObjectName) -> (String, String, Option<Vec<String>>) {
        let parts: Vec<&Ident> = name
            .0
            .iter()
            .filter_map(|p| match p {
                ObjectNamePart::Identifier(i) => Some(i),
                ObjectNamePart::Function(_) => None,
            })
            .collect();
        match parts.as_slice() {
            [t] => {
                let low = t.value.to_lowercase();
                if let Some((_, c)) = self.ctes.iter().rev().find(|(n, _)| *n == low) {
                    return (quote_name(&t.value), quote_name(&t.value), Some(c.clone()));
                }
                (
                    quote_name(&t.value),
                    quote_name(&t.value),
                    (self.cols)(self.db, &t.value),
                )
            }
            [d, t] if d.value == self.db => (
                quote_name(&t.value),
                quote_name(&t.value),
                (self.cols)(&d.value, &t.value),
            ),
            [d, t] => {
                let q = format!("{}.{}", quote_name(&d.value), quote_name(&t.value));
                (q.clone(), q, (self.cols)(&d.value, &t.value))
            }
            _ => (name.to_string(), name.to_string(), None),
        }
    }

    /// One `FROM` factor printed; its columns registered in `scope`.
    fn factor(&mut self, f: &TableFactor, scope: &mut Vec<Factor>) -> String {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let (printed, qual, cols) = self.base_factor(name);
                let mut out = printed;
                let (fname, qual) = match alias {
                    Some(a) => {
                        out.push(' ');
                        out.push_str(&quote_name(&a.name.value));
                        (a.name.value.clone(), quote_name(&a.name.value))
                    }
                    None => (
                        name.0
                            .last()
                            .and_then(|p| match p {
                                ObjectNamePart::Identifier(i) => Some(i.value.clone()),
                                ObjectNamePart::Function(_) => None,
                            })
                            .unwrap_or_default(),
                        qual,
                    ),
                };
                scope.push(Factor {
                    name: fname,
                    qual,
                    cols: cols.unwrap_or_default(),
                });
                out
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let body = self.query(subquery, None);
                let mut cols = output_names(subquery, self.src);
                let mut out = format!("({body})");
                if let Some(a) = alias {
                    if !a.columns.is_empty() {
                        cols = a.columns.iter().map(|c| c.name.value.clone()).collect();
                    }
                    out.push(' ');
                    out.push_str(&quote_name(&a.name.value));
                    scope.push(Factor {
                        name: a.name.value.clone(),
                        qual: quote_name(&a.name.value),
                        cols,
                    });
                }
                out
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.join_text(table_with_joins, scope),
            other => other.to_string(),
        }
    }

    fn join_text(&mut self, t: &TableWithJoins, scope: &mut Vec<Factor>) -> String {
        let mut out = self.factor(&t.relation, scope);
        for j in &t.joins {
            let (w, c) = match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    ("join", Some(c))
                }
                JoinOperator::StraightJoin(c) => ("straight_join", Some(c)),
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => ("left join", Some(c)),
                JoinOperator::Right(c) | JoinOperator::RightOuter(c) => ("right join", Some(c)),
                _ => ("join", None),
            };
            let r = self.factor(&j.relation, scope);
            let cond = match c {
                Some(JoinConstraint::On(e)) => {
                    self.scopes.push(scope.clone());
                    let s = self.expr_top(e);
                    self.scopes.pop();
                    format!(" on({s})")
                }
                Some(JoinConstraint::Using(cols)) => format!(
                    " using ({})",
                    cols.iter()
                        .map(|c| quote_name(&c.to_string().replace('`', "")))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                _ => String::new(),
            };
            out = format!("({out} {w} {r}{cond})");
        }
        out
    }

    fn star(scope: &[Factor], only: Option<&str>) -> Vec<String> {
        let mut out = Vec::new();
        for f in scope {
            if only.is_some_and(|q| !f.name.eq_ignore_ascii_case(q.trim_matches('`'))) {
                continue;
            }
            for c in &f.cols {
                out.push(format!("{}.{} AS {}", f.qual, quote_name(c), quote_name(c)));
            }
        }
        out
    }

    fn select(&mut self, s: &Select, names: Option<&[String]>) -> String {
        let mut scope = Vec::new();
        let from: Vec<String> = s
            .from
            .iter()
            .map(|t| self.join_text(t, &mut scope))
            .collect();
        self.scopes.push(scope.clone());
        let mut own: Vec<String> = Vec::new();
        let mut items = Vec::new();
        let mut at = 0usize;
        let written = output_names(
            &Query {
                body: Box::new(SetExpr::Select(Box::new(s.clone()))),
                ..empty_query()
            },
            self.src,
        );
        self.aliases.push(written.clone());
        for (i, it) in s.projection.iter().enumerate() {
            match it {
                SelectItem::Wildcard(_) => {
                    for x in Self::star(&scope, None) {
                        items.push(x);
                        at += 1;
                    }
                }
                SelectItem::QualifiedWildcard(k, _) => {
                    let q = match k {
                        SelectItemQualifiedWildcardKind::ObjectName(n) => {
                            n.0.last().map_or_else(String::new, |p| {
                                quote_name(&p.to_string().replace('`', ""))
                            })
                        }
                        SelectItemQualifiedWildcardKind::Expr(e) => e.to_string(),
                    };
                    for x in Self::star(&scope, Some(&q)) {
                        items.push(x);
                        at += 1;
                    }
                }
                SelectItem::ExprWithAliases { expr: e, .. }
                | SelectItem::UnnamedExpr(e)
                | SelectItem::ExprWithAlias { expr: e, .. } => {
                    let name = names
                        .and_then(|n| n.get(at).cloned())
                        .or_else(|| written.get(i).cloned())
                        .unwrap_or_default();
                    let mut x = self.expr_top(e);
                    if matches!(e, Expr::Subquery(_)) && !x.starts_with('(') {
                        x = format!("({x})");
                    }
                    if self.bare.last() == Some(&true) {
                        items.push(x);
                    } else {
                        items.push(format!("{x} AS {}", quote_name(&name)));
                    }
                    own.push(name);
                    at += 1;
                }
            }
        }
        let mut out = String::from("select ");
        if s.distinct.is_some() {
            out.push_str("distinct ");
        }
        out.push_str(&items.join(","));
        if !from.is_empty() {
            out.push_str(" from ");
            out.push_str(&from.join(","));
        }
        if let Some(w) = &s.selection {
            out.push_str(" where ");
            out.push_str(&self.expr_top(w));
        }
        if let GroupByExpr::Expressions(g, _) = &s.group_by
            && !g.is_empty()
        {
            out.push_str(" group by ");
            let v: Vec<String> = g.iter().map(|e| self.expr_top(e)).collect();
            out.push_str(&v.join(","));
        }
        if let Some(h) = &s.having {
            out.push_str(" having ");
            out.push_str(&self.expr_top(h));
        }
        self.aliases.pop();
        self.scopes.pop();
        out
    }

    fn column(&self, name: &str) -> String {
        for scope in self.scopes.iter().rev() {
            for f in scope {
                if let Some(c) = f.cols.iter().find(|c| c.eq_ignore_ascii_case(name)) {
                    return format!("{}.{}", f.qual, quote_name(c));
                }
            }
        }
        quote_name(name)
    }

    fn qualified(&self, parts: &[Ident]) -> String {
        match parts {
            [q, c] => {
                for scope in self.scopes.iter().rev() {
                    for f in scope {
                        if f.name.eq_ignore_ascii_case(&q.value) {
                            let col = f
                                .cols
                                .iter()
                                .find(|x| x.eq_ignore_ascii_case(&c.value))
                                .unwrap_or(&c.value);
                            return format!("{}.{}", f.qual, quote_name(col));
                        }
                    }
                }
                format!("{}.{}", quote_name(&q.value), quote_name(&c.value))
            }
            [_, t, c] => self.qualified(&[t.clone(), c.clone()]),
            _ => parts
                .iter()
                .map(|p| quote_name(&p.value))
                .collect::<Vec<_>>()
                .join("."),
        }
    }

    fn expr_top(&mut self, e: &Expr) -> String {
        self.expr(e, 0, false)
    }

    /// `e`, parenthesized for MariaDB when it binds looser than `ctx`
    /// (or as loose, on an operator's right).
    fn expr(&mut self, e: &Expr, ctx: u8, right: bool) -> String {
        let (s, p) = self.expr_p(e);
        let wrap = if self.mysql {
            false
        } else {
            p < ctx || (right && p == ctx && p != P_AND && p != P_OR)
        };
        if wrap { format!("({s})") } else { s }
    }

    /// For MySQL: an operator's text in parentheses.
    fn op_text(&self, s: String) -> String {
        if self.mysql { format!("({s})") } else { s }
    }

    #[allow(clippy::too_many_lines)] // one arm per expression form
    fn expr_p(&mut self, e: &Expr) -> (String, u8) {
        match e {
            Expr::Identifier(i) => {
                if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("null") {
                    return ("NULL".into(), P_TOP);
                }
                let in_scope = self.scopes.iter().any(|s| {
                    s.iter()
                        .any(|f| f.cols.iter().any(|c| c.eq_ignore_ascii_case(&i.value)))
                });
                if !in_scope
                    && self
                        .aliases
                        .last()
                        .is_some_and(|a| a.iter().any(|x| x.eq_ignore_ascii_case(&i.value)))
                {
                    return (quote_name(&i.value), P_TOP);
                }
                (self.column(&i.value), P_TOP)
            }
            Expr::CompoundIdentifier(p) => (self.qualified(p), P_TOP),
            Expr::Nested(x) => self.expr_p(x),
            Expr::Value(v) => (self.value(&v.value), P_TOP),
            Expr::BinaryOp { left, op, right } => {
                let Some((w, p)) = bin_op(op) else {
                    return (e.to_string(), P_TOP);
                };
                if self.mysql && matches!(op, BinaryOperator::And | BinaryOperator::Or) {
                    // A chain prints flat in one pair of parentheses.
                    let mut parts = Vec::new();
                    flatten(e, op, &mut parts);
                    let v: Vec<String> = parts.iter().map(|x| self.expr(x, p, false)).collect();
                    return (format!("({})", v.join(&format!(" {w} "))), P_TOP);
                }
                let l = self.expr(left, p, false);
                let r = self.expr(right, p, true);
                (self.op_text(format!("{l} {w} {r}")), p)
            }
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => self.not(expr),
                UnaryOperator::Minus => {
                    let x = self.expr(expr, 15, false);
                    (format!("-{x}"), 15)
                }
                UnaryOperator::Plus => self.expr_p(expr),
                UnaryOperator::BitwiseNot => {
                    let x = self.expr(expr, 15, false);
                    (format!("~{x}"), 15)
                }
                _ => (e.to_string(), P_TOP),
            },
            Expr::IsNull(x) => {
                let s = self.expr(x, P_CMP, false);
                (self.op_text(format!("{s} is null")), P_CMP)
            }
            Expr::IsNotNull(x) => {
                let s = self.expr(x, P_CMP, false);
                (self.op_text(format!("{s} is not null")), P_CMP)
            }
            Expr::IsTrue(x) | Expr::IsNotTrue(x) | Expr::IsFalse(x) | Expr::IsNotFalse(x) => {
                let s = self.expr(x, P_CMP, false);
                let w = match e {
                    Expr::IsTrue(_) => "is true",
                    Expr::IsNotTrue(_) => "is not true",
                    Expr::IsFalse(_) => "is false",
                    _ => "is not false",
                };
                (self.op_text(format!("{s} {w}")), P_CMP)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let x = self.expr(expr, P_CMP, false);
                let l = self.expr(low, P_CMP, false);
                let h = self.expr(high, P_CMP, true);
                let n = if *negated { "not " } else { "" };
                (self.op_text(format!("{x} {n}between {l} and {h}")), 6)
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                let x = self.expr(expr, P_CMP, false);
                let pt = self.expr(pattern, P_CMP, true);
                let n = if *negated { " not" } else { "" };
                let esc = match escape_char {
                    Some(c) => format!(" escape {}", self.expr_top(c)),
                    None => String::new(),
                };
                (self.op_text(format!("{x}{n} like {pt}{esc}")), P_CMP)
            }
            Expr::RLike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let x = self.expr(expr, P_CMP, false);
                let pt = self.expr(pattern, P_CMP, true);
                let s = format!("{x} regexp {pt}");
                if *negated {
                    (format!("!({s})"), P_TOP)
                } else {
                    (self.op_text(s), P_CMP)
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let x = self.expr(expr, P_CMP, false);
                let v: Vec<String> = list.iter().map(|i| self.expr_top(i)).collect();
                let n = if *negated { "not " } else { "" };
                (self.op_text(format!("{x} {n}in ({})", v.join(","))), P_CMP)
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let x = self.expr(expr, P_CMP, false);
                self.bare_next = true;
                let q = self.query(subquery, None);
                if *negated && !self.mysql {
                    return (format!("!({x} in ({q}))"), P_TOP);
                }
                if *negated {
                    return (format!("{x} in ({q}) is false"), P_CMP);
                }
                let n = if *negated { "not " } else { "" };
                (self.op_text(format!("{x} {n}in ({q})")), P_CMP)
            }
            Expr::Exists { subquery, negated } => {
                self.bare_next = true;
                let q = self.query(subquery, None);
                let s = format!("exists({q})");
                if *negated {
                    (self.op_text(format!("!{s}")), P_TOP)
                } else {
                    (s, P_TOP)
                }
            }
            Expr::Subquery(q) => {
                self.bare_next = true;
                (format!("({})", self.query(q, None)), P_TOP)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut s = String::from("case ");
                if let Some(o) = operand {
                    s.push_str(&self.expr_top(o));
                    s.push(' ');
                }
                for CaseWhen { condition, result } in conditions {
                    let c = self.expr_top(condition);
                    let r = self.expr_top(result);
                    let _ = write!(s, "when {c} then {r} ");
                }
                if let Some(el) = else_result {
                    let e = self.expr_top(el);
                    let _ = write!(s, "else {e} ");
                }
                s.push_str("end");
                (self.op_text(s), P_TOP)
            }
            Expr::Cast {
                expr, data_type, ..
            } => {
                let x = self.expr_top(expr);
                (
                    format!("cast({x} as {})", data_type.to_string().to_lowercase()),
                    P_TOP,
                )
            }
            Expr::Function(f) if f.over.is_none() && f.filter.is_none() => {
                let mut name = f.name.to_string().to_lowercase();
                if !self.mysql {
                    name = match name.as_str() {
                        "upper" => "ucase".into(),
                        "lower" => "lcase".into(),
                        "length" => "octet_length".into(),
                        "now" => "current_timestamp".into(),
                        _ => name,
                    };
                }
                let (distinct, args) = match &f.args {
                    FunctionArguments::None => (false, Vec::new()),
                    FunctionArguments::List(l) => (
                        matches!(l.duplicate_treatment, Some(DuplicateTreatment::Distinct)),
                        l.args
                            .iter()
                            .map(|a| match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => self.expr_top(x),
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                                    if name == "count" {
                                        "0".into()
                                    } else {
                                        "*".into()
                                    }
                                }
                                other => other.to_string(),
                            })
                            .collect(),
                    ),
                    FunctionArguments::Subquery(q) => (false, vec![self.query(q, None)]),
                };
                if matches!(f.args, FunctionArguments::None) {
                    return (name, P_TOP);
                }
                let d = if distinct { "distinct " } else { "" };
                (format!("{name}({d}{})", args.join(",")), P_TOP)
            }
            other => (other.to_string(), P_TOP),
        }
    }

    fn not(&mut self, x: &Expr) -> (String, u8) {
        let inner = match x {
            Expr::Nested(n) => &**n,
            other => other,
        };
        match inner {
            Expr::BinaryOp { left, op, right } if negate_cmp(op).is_some() => {
                let flipped = Expr::BinaryOp {
                    left: left.clone(),
                    op: negate_cmp(op).unwrap_or(BinaryOperator::NotEq),
                    right: right.clone(),
                };
                self.expr_p(&flipped)
            }
            Expr::IsNull(y) => self.expr_p(&Expr::IsNotNull(y.clone())),
            Expr::IsNotNull(y) => self.expr_p(&Expr::IsNull(y.clone())),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.expr_p(&Expr::InList {
                expr: expr.clone(),
                list: list.clone(),
                negated: !negated,
            }),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.expr_p(&Expr::Between {
                expr: expr.clone(),
                negated: !negated,
                low: low.clone(),
                high: high.clone(),
            }),
            Expr::Exists { subquery, negated } => self.expr_p(&Expr::Exists {
                subquery: subquery.clone(),
                negated: !negated,
            }),
            other => {
                let s = self.expr(other, P_TOP, false);
                if self.mysql {
                    (format!("(not({s}))"), P_TOP)
                } else {
                    (format!("{s} = 0"), P_CMP)
                }
            }
        }
    }

    fn value(&self, v: &Value) -> String {
        match v {
            Value::Number(n, _) => n.clone(),
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => quote_str(s),
            Value::Boolean(b) => {
                if self.mysql {
                    if *b { "true".into() } else { "false".into() }
                } else if *b {
                    "1".into()
                } else {
                    "0".into()
                }
            }
            Value::Null => "NULL".into(),
            Value::HexStringLiteral(h) => format!("X'{h}'"),
            other => other.to_string(),
        }
    }
}

fn flatten<'e>(e: &'e Expr, op: &BinaryOperator, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::BinaryOp { left, op: o, right } if o == op => {
            flatten(left, op, out);
            flatten(right, op, out);
        }
        Expr::Nested(n) if matches!(&**n, Expr::BinaryOp { op: o, .. } if o == op) => {
            flatten(n, op, out);
        }
        other => out.push(other),
    }
}

fn empty_query() -> Query {
    Query {
        with: None,
        body: Box::new(SetExpr::Values(sqlparser::ast::Values {
            explicit_row: false,
            value_keyword: false,
            rows: Vec::new(),
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    }
}

/// The stored text of a view: its header and `body` (printed by
/// [`Printer`]).
pub(crate) fn render(h: &Header, definer: &(String, String), mysql: bool, body: &str) -> String {
    let mut out = format!(
        "CREATE ALGORITHM={} DEFINER={}@{} SQL SECURITY {} VIEW {}",
        h.algorithm.unwrap_or(Algorithm::Undefined).word(),
        quote_name(&definer.0),
        quote_name(&definer.1),
        if h.invoker == Some(true) {
            "INVOKER"
        } else {
            "DEFINER"
        },
        quote_name(&h.name),
    );
    if mysql && let Some(c) = &h.columns {
        out.push_str(" (");
        out.push_str(
            &c.iter()
                .map(|c| quote_name(c))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push(')');
    }
    out.push_str(" AS ");
    out.push_str(body);
    match h.check {
        Some(CheckOption::Cascaded) => out.push_str(" WITH CASCADED CHECK OPTION"),
        Some(CheckOption::Local) => out.push_str(" WITH LOCAL CHECK OPTION"),
        None => {}
    }
    out
}

/// A view reference as a derived table: `(select ...) alias (cols)`.
pub(crate) fn as_derived(v: &ViewDef, alias: Option<&TableAlias>) -> TableFactor {
    let name = alias.map_or_else(|| Ident::with_quote('`', &v.name), |a| a.name.clone());
    TableFactor::Derived {
        lateral: false,
        subquery: Box::new(v.query.clone()),
        alias: Some(TableAlias {
            explicit: true,
            at: None,
            name,
            columns: v
                .columns
                .iter()
                .map(|c| TableAliasColumnDef {
                    name: Ident::with_quote('`', c),
                    data_type: None,
                })
                .collect(),
        }),
        sample: None,
    }
}

/// A write through a view: the condition its new rows must meet (`WITH
/// CHECK OPTION`), over the base table read under `qual`.
#[derive(Debug, Clone)]
pub(crate) struct ViewCheck {
    pub cond: Expr,
    pub qual: String,
    /// `db`.`view` as the error names it.
    pub db: String,
    pub view: String,
}

/// The base column expression of each view column (`None`: computed).
fn view_columns(v: &ViewDef, s: &Select) -> Vec<(String, Expr, bool)> {
    v.columns
        .iter()
        .zip(&s.projection)
        .map(|(name, it)| {
            let e = match it {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e.clone(),
                other => Expr::Identifier(Ident::new(other.to_string())),
            };
            let plain = matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
            (name.clone(), e, plain)
        })
        .collect()
}

fn not_updatable(view: &str, what: &str) -> SprigError {
    SprigError::standard(
        1288,
        *b"HY000",
        format!("The target table {view} of the {what} is not updatable"),
    )
}

fn not_insertable(view: &str) -> SprigError {
    SprigError::standard(
        1471,
        *b"HY000",
        format!("The target table {view} of the INSERT is not insertable-into"),
    )
}

fn column_not_updatable(c: &str) -> SprigError {
    SprigError::standard(1348, *b"HY000", format!("Column '{c}' is not updatable"))
}

/// Rewrites a write whose target is an updatable view of one table into the
/// same write on that table: the view's columns become their base
/// expressions, and an `UPDATE` / `DELETE` also meets the view's `WHERE`.
/// Returns the statement unchanged when its target isn't a view.
///
/// # Errors
/// The servers' 1288, 1348, 1471 for a view that can't take the write;
/// `ER_SPRIG_NOT_ON_ENGINE` for a view over a view.
#[allow(clippy::too_many_lines)] // one arm per statement kind
pub(crate) fn rewrite_write(
    cx: &super::plan::BindCtx<'_>,
    stmt: sqlparser::ast::Statement,
    mysql: bool,
) -> Result<(sqlparser::ast::Statement, Option<ViewCheck>)> {
    use sqlparser::ast::{AssignmentTarget, FromTable, Statement, TableObject};
    let target: Option<ObjectName> = match &stmt {
        Statement::Insert(i) => match &i.table {
            TableObject::TableName(n) => Some(n.clone()),
            _ => None,
        },
        Statement::Update(u) if u.table.joins.is_empty() && u.from.is_none() => {
            match &u.table.relation {
                TableFactor::Table { name, .. } => Some(name.clone()),
                _ => None,
            }
        }
        Statement::Delete(d) if d.tables.is_empty() && d.using.is_none() => match &d.from {
            FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f)
                if f.len() == 1 && f[0].joins.is_empty() =>
            {
                match &f[0].relation {
                    TableFactor::Table { name, .. } => Some(name.clone()),
                    _ => None,
                }
            }
            _ => None,
        },
        _ => None,
    };
    let Some(target) = target else {
        return Ok((stmt, None));
    };
    let parts: Vec<String> = target
        .0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect();
    let (db, name) = match parts.as_slice() {
        [t] => match cx.current {
            Some(d) => (d, t.clone()),
            None => return Ok((stmt, None)),
        },
        [d, t] => match super::plan::physical_db(cx.tenant, d).or_else(|| cx.names.db_by_name(d)) {
            Some(d) => (d, t.clone()),
            None => return Ok((stmt, None)),
        },
        _ => return Ok((stmt, None)),
    };
    let Some(v) = cx.names.view(db, &name) else {
        return Ok((stmt, None));
    };
    let db_name = cx.names.db_name(db);
    let v = match &*v {
        Ok(v) => v.clone(),
        Err(_) => return Err(errors::invalid(&db_name, &name)),
    };
    let what = match &stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        _ => "DELETE",
    };
    let refuse = || {
        if what == "INSERT" {
            not_insertable(&v.name)
        } else {
            not_updatable(&v.name, what)
        }
    };
    if !v.updatable || !updatable(&v.query, v.algorithm, mysql) {
        return Err(refuse());
    }
    let SetExpr::Select(vs) = &*v.query.body else {
        return Err(refuse());
    };
    let [from] = vs.from.as_slice() else {
        return Err(refuse());
    };
    if !from.joins.is_empty() || v.query.limit_clause.is_some() {
        return Err(refuse());
    }
    let TableFactor::Table {
        name: base,
        alias: base_alias,
        ..
    } = &from.relation
    else {
        return Err(refuse());
    };
    if let Some(ObjectNamePart::Identifier(t)) = base.0.last()
        && let Some(ObjectNamePart::Identifier(d)) = base.0.first()
        && let Some(bd) = cx.names.db_by_name(&d.value)
        && cx.names.view(bd, &t.value).is_some()
    {
        return Err(super::not_on_engine("a write through a view of a view"));
    }
    let qual = match base_alias {
        Some(a) => a.name.value.clone(),
        None => match base.0.last() {
            Some(ObjectNamePart::Identifier(i)) => i.value.clone(),
            _ => return Err(refuse()),
        },
    };
    let cols = view_columns(&v, vs);
    // What the statement calls the view.
    let alias = match &stmt {
        Statement::Update(u) => match &u.table.relation {
            TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
            _ => v.name.clone(),
        },
        Statement::Delete(d) => match &d.from {
            FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => match &f[0].relation {
                TableFactor::Table { alias: Some(a), .. } => a.name.value.clone(),
                _ => v.name.clone(),
            },
        },
        _ => v.name.clone(),
    };
    let lookup = |c: &str| cols.iter().find(|(n, _, _)| n.eq_ignore_ascii_case(c));
    // The statement's references to view columns, as base expressions.
    let subst = |e: &mut Expr| {
        let _ = sqlparser::ast::visit_expressions_mut(e, |x| {
            let c = match x {
                Expr::Identifier(i) => Some(i.value.clone()),
                Expr::CompoundIdentifier(p)
                    if p.len() == 2 && p[0].value.eq_ignore_ascii_case(&alias) =>
                {
                    Some(p[1].value.clone())
                }
                _ => None,
            };
            if let Some((_, b, _)) = c.as_deref().and_then(lookup) {
                *x = if matches!(b, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                    b.clone()
                } else {
                    Expr::Nested(Box::new(b.clone()))
                };
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
    };
    let base_factor = TableFactor::Table {
        name: base.clone(),
        alias: base_alias.clone(),
        args: None,
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    };
    let base_col = |c: &str| -> Result<Ident> {
        match lookup(c) {
            Some((_, Expr::Identifier(i), true)) => Ok(i.clone()),
            Some((_, Expr::CompoundIdentifier(p), true)) => {
                p.last().cloned().ok_or_else(|| column_not_updatable(c))
            }
            Some(_) => Err(column_not_updatable(c)),
            None => Err(SprigError::standard(
                1054,
                *b"42S22",
                format!("Unknown column '{c}' in 'field list'"),
            )),
        }
    };
    let both = |w: Option<Expr>| -> Option<Expr> {
        match (vs.selection.clone(), w) {
            (Some(a), Some(b)) => Some(Expr::BinaryOp {
                left: Box::new(a),
                op: BinaryOperator::And,
                right: Box::new(b),
            }),
            (a, b) => a.or(b),
        }
    };
    let mut stmt = stmt;
    match &mut stmt {
        Statement::Insert(i) => {
            let computed = cols.iter().find(|(_, _, plain)| !plain);
            if let Some((c, _, _)) = computed.filter(|_| mysql || i.columns.is_empty()) {
                // MySQL: a view with a computed column takes no INSERT
                // naming columns, and names the column otherwise; MariaDB
                // the other way round.
                if mysql {
                    return Err(if i.columns.is_empty() {
                        column_not_updatable(c)
                    } else {
                        not_insertable(&v.name)
                    });
                }
                if i.columns.is_empty() {
                    return Err(not_insertable(&v.name));
                }
            }
            let named: Vec<String> = if i.columns.is_empty() {
                cols.iter().map(|(n, _, _)| n.clone()).collect()
            } else {
                i.columns
                    .iter()
                    .map(|c| c.to_string().trim_matches('`').to_owned())
                    .collect()
            };
            let mut out = Vec::new();
            for c in &named {
                out.push(ObjectName(vec![ObjectNamePart::Identifier(base_col(c)?)]));
            }
            i.columns = out;
            i.table = TableObject::TableName(base.clone());
            for a in &mut i.assignments {
                if let AssignmentTarget::ColumnName(n) = &mut a.target {
                    let c = n.to_string().trim_matches('`').to_owned();
                    *n = ObjectName(vec![ObjectNamePart::Identifier(base_col(&c)?)]);
                }
            }
        }
        Statement::Update(u) => {
            for a in &mut u.assignments {
                if let AssignmentTarget::ColumnName(n) = &mut a.target {
                    let c =
                        n.0.last()
                            .map(|p| p.to_string().trim_matches('`').to_owned())
                            .unwrap_or_default();
                    *n = ObjectName(vec![ObjectNamePart::Identifier(base_col(&c)?)]);
                }
                subst(&mut a.value);
            }
            let mut w = u.selection.take();
            if let Some(w) = &mut w {
                subst(w);
            }
            u.selection = both(w);
            for o in &mut u.order_by {
                subst(&mut o.expr);
            }
            u.table.relation = base_factor;
        }
        Statement::Delete(d) => {
            let mut w = d.selection.take();
            if let Some(w) = &mut w {
                subst(w);
            }
            d.selection = both(w);
            for o in &mut d.order_by {
                subst(&mut o.expr);
            }
            match &mut d.from {
                FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => {
                    f[0].relation = base_factor;
                }
            }
        }
        _ => {}
    }
    let check = match (&v.check, &vs.selection, what) {
        (Some(_), Some(cond), "INSERT" | "UPDATE") => Some(ViewCheck {
            cond: cond.clone(),
            qual,
            db: db_name,
            view: v.name.clone(),
        }),
        _ => None,
    };
    Ok((stmt, check))
}

/// The error of a row a view's `WITH CHECK OPTION` refuses.
pub(crate) fn check_failed(db: &str, view: &str, mysql: bool) -> SprigError {
    SprigError::standard(
        1369,
        if mysql { *b"HY000" } else { *b"44000" },
        if mysql {
            format!("CHECK OPTION failed '{db}.{view}'")
        } else {
            format!("CHECK OPTION failed `{db}`.`{view}`")
        },
    )
}

/// Errors of view statements, as the servers word them.
pub(crate) mod errors {
    use sprig_common::SprigError;

    pub(crate) fn column_counts(mysql: bool) -> SprigError {
        SprigError::standard(
            1353,
            *b"HY000",
            if mysql {
                "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts"
            } else {
                "View's SELECT and view's field list have different column counts"
            },
        )
    }

    pub(crate) fn duplicate_column(name: &str) -> SprigError {
        SprigError::standard(1060, *b"42S21", format!("Duplicate column name '{name}'"))
    }

    pub(crate) fn check_on_non_updatable(db: &str, view: &str, mysql: bool) -> SprigError {
        SprigError::standard(
            1368,
            *b"HY000",
            if mysql {
                format!("CHECK OPTION on non-updatable view '{db}.{view}'")
            } else {
                format!("CHECK OPTION on non-updatable view `{db}`.`{view}`")
            },
        )
    }

    pub(crate) fn variable() -> SprigError {
        SprigError::standard(
            1351,
            *b"HY000",
            "View's SELECT contains a variable or parameter",
        )
    }

    pub(crate) fn invalid(db: &str, view: &str) -> SprigError {
        SprigError::standard(
            1356,
            *b"HY000",
            format!(
                "View '{db}.{view}' references invalid table(s) or column(s) or function(s) or \
                 definer/invoker of view lack rights to use them"
            ),
        )
    }

    pub(crate) fn recursive(db: &str, view: &str) -> SprigError {
        SprigError::standard(
            1462,
            *b"HY000",
            format!("`{db}`.`{view}` contains view recursion"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(_db: &str, t: &str) -> Option<Vec<String>> {
        match t {
            "vt" => Some(vec!["id".into(), "a".into(), "b".into()]),
            "vu" => Some(vec!["k".into(), "id".into(), "n".into()]),
            _ => None,
        }
    }

    fn shown(sql: &str, mysql: bool) -> String {
        let h = parse_header(sql).unwrap();
        let q = parse_query(&h.body).unwrap();
        let names = h.columns.clone().filter(|_| !mysql);
        let mut p = Printer::new(mysql, "rg_a", &h.body, &cols);
        let body = p.query(&q, names.as_deref());
        let definer = h.definer.clone().unwrap_or(("root".into(), "%".into()));
        render(&h, &definer, mysql, &body)
    }

    #[test]
    fn views_print_as_each_server_stores_them() {
        let v1 = "CREATE OR REPLACE VIEW v1 AS SELECT id, a+1 AS a1, b FROM vt WHERE a > 0 WITH CHECK OPTION";
        assert_eq!(
            shown(v1, false),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS select `vt`.`id` AS `id`,`vt`.`a` + 1 AS `a1`,`vt`.`b` AS `b` from `vt` where `vt`.`a` > 0 WITH CASCADED CHECK OPTION"
        );
        assert_eq!(
            shown(v1, true),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v1` AS select `vt`.`id` AS `id`,(`vt`.`a` + 1) AS `a1`,`vt`.`b` AS `b` from `vt` where (`vt`.`a` > 0) WITH CASCADED CHECK OPTION"
        );
        let v2 = "CREATE OR REPLACE VIEW v2 (x, y) AS SELECT id, a FROM vt";
        assert_eq!(
            shown(v2, false),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v2` AS select `vt`.`id` AS `x`,`vt`.`a` AS `y` from `vt`"
        );
        assert_eq!(
            shown(v2, true),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v2` (`x`,`y`) AS select `vt`.`id` AS `id`,`vt`.`a` AS `a` from `vt`"
        );
        let v3 = "CREATE OR REPLACE ALGORITHM=TEMPTABLE SQL SECURITY INVOKER VIEW v3 AS SELECT * FROM vt";
        assert_eq!(
            shown(v3, false),
            "CREATE ALGORITHM=TEMPTABLE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `v3` AS select `vt`.`id` AS `id`,`vt`.`a` AS `a`,`vt`.`b` AS `b` from `vt`"
        );
        let v4 = "CREATE OR REPLACE VIEW v4 AS SELECT vt.b, COUNT(*) c, SUM(u.k) FROM vt JOIN vu u ON u.id = vt.id WHERE vt.a BETWEEN 1 AND 5 OR vt.b LIKE 'x%' GROUP BY vt.b HAVING COUNT(*) > 1 ORDER BY vt.b DESC LIMIT 3";
        assert_eq!(
            shown(v4, false),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v4` AS select `vt`.`b` AS `b`,count(0) AS `c`,sum(`u`.`k`) AS `SUM(u.k)` from (`vt` join `vu` `u` on(`u`.`id` = `vt`.`id`)) where `vt`.`a` between 1 and 5 or `vt`.`b` like 'x%' group by `vt`.`b` having count(0) > 1 order by `vt`.`b` desc limit 3"
        );
        assert_eq!(
            shown(v4, true),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v4` AS select `vt`.`b` AS `b`,count(0) AS `c`,sum(`u`.`k`) AS `SUM(u.k)` from (`vt` join `vu` `u` on((`u`.`id` = `vt`.`id`))) where ((`vt`.`a` between 1 and 5) or (`vt`.`b` like 'x%')) group by `vt`.`b` having (count(0) > 1) order by `vt`.`b` desc limit 3"
        );
        let v5 = "CREATE OR REPLACE VIEW v5 AS SELECT a FROM vt UNION SELECT k FROM vu";
        assert_eq!(
            shown(v5, false),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v5` AS select `vt`.`a` AS `a` from `vt` union select `vu`.`k` AS `k` from `vu`"
        );
        let v6 = "CREATE OR REPLACE VIEW v6 AS SELECT id, (SELECT MAX(k) FROM vu WHERE vu.id = vt.id) m, CONCAT(b,'x'), IF(a IS NULL, 0, a) FROM vt LEFT JOIN (SELECT id AS i2 FROM vu) d ON d.i2 = vt.id WHERE id IN (1,2) AND NOT a = 3";
        assert_eq!(
            shown(v6, false),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v6` AS select `vt`.`id` AS `id`,(select max(`vu`.`k`) from `vu` where `vu`.`id` = `vt`.`id`) AS `m`,concat(`vt`.`b`,'x') AS `CONCAT(b,'x')`,if(`vt`.`a` is null,0,`vt`.`a`) AS `IF(a IS NULL, 0, a)` from (`vt` left join (select `vu`.`id` AS `i2` from `vu`) `d` on(`d`.`i2` = `vt`.`id`)) where `vt`.`id` in (1,2) and `vt`.`a` <> 3"
        );
        assert_eq!(
            shown(v6, true),
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v6` AS select `vt`.`id` AS `id`,(select max(`vu`.`k`) from `vu` where (`vu`.`id` = `vt`.`id`)) AS `m`,concat(`vt`.`b`,'x') AS `CONCAT(b,'x')`,if((`vt`.`a` is null),0,`vt`.`a`) AS `IF(a IS NULL, 0, a)` from (`vt` left join (select `vu`.`id` AS `i2` from `vu`) `d` on((`d`.`i2` = `vt`.`id`))) where ((`vt`.`id` in (1,2)) and (`vt`.`a` <> 3))"
        );
    }

    #[test]
    fn view_definitions_name_the_database() {
        let text = shown("CREATE VIEW v2 (x, y) AS SELECT id, a FROM vt", false);
        let v = load(ObjectId(9), DbId(1), "rg_a", &text).unwrap();
        let d = Printer::new(false, "", &v.body, &cols).query(&v.query, None);
        assert_eq!(
            d,
            "select `rg_a`.`vt`.`id` AS `x`,`rg_a`.`vt`.`a` AS `y` from `rg_a`.`vt`"
        );
    }

    #[test]
    fn a_stored_text_reads_back() {
        let text = shown(
            "CREATE ALGORITHM=MERGE DEFINER='bob'@'localhost' SQL SECURITY INVOKER VIEW v (x) AS SELECT a FROM vt WITH LOCAL CHECK OPTION",
            false,
        );
        let v = load(ObjectId(9), DbId(1), "rg_a", &text).unwrap();
        assert_eq!(v.columns, vec!["x".to_owned()]);
        assert_eq!(v.algorithm, Algorithm::Merge);
        assert_eq!(v.definer, ("bob".into(), "localhost".into()));
        assert!(v.invoker);
        assert_eq!(v.check, Some(CheckOption::Local));
        assert!(v.updatable);
        assert_eq!(
            v.query.to_string(),
            "SELECT `vt`.`a` AS `x` FROM `rg_a`.`vt`"
        );
    }
}
