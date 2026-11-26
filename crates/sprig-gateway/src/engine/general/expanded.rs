//! MySQL 8.4's Note 1003 after `EXPLAIN`: the statement as its optimizer
//! prints it back ("expanded" form). Every column is written
//! `` `db`.`table`.`column` ``, every computed item gets `AS` its name,
//! each operation is parenthesised, inner joins are written `join` with
//! their conditions moved into `where`, function names are lower case,
//! `COUNT(*)` is `count(0)` and `NOT (a = b)` is `(a <> b)`.
//!
//! MySQL prints the statement after its own rewrites (constant tables
//! read in advance, semi-joins, equalities propagated); those are not
//! repeated here, so such statements print their plain form.

use std::fmt::Write as _;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, JoinConstraint,
    JoinOperator, LimitClause, ObjectName, OrderByKind, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, UnaryOperator, Value,
};

use super::names::TableUse;

/// One table of a `FROM`: the name its columns are written under, its
/// database, and its definition when it is a base table.
struct From<'a> {
    qual: String,
    db: Option<String>,
    table: Option<&'a TableUse>,
}

struct Printer<'a> {
    tables: &'a [TableUse],
    current: Option<&'a str>,
    /// The next `select#` number.
    next: usize,
    /// The `FROM` lists in scope, innermost last.
    scopes: Vec<Vec<From<'a>>>,
}

fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn ident_name(i: &sqlparser::ast::ObjectNamePart) -> Option<&Ident> {
    i.as_ident()
}

/// The expanded text of `stmt` (a query, or an `EXPLAIN` of one), with
/// the names of its computed result columns as written (`written`,
/// parallel to its select list). `None`: not a form printed here.
pub(crate) fn expanded(
    stmt: &Statement,
    tables: &[TableUse],
    current: Option<&str>,
    written: &[Option<String>],
) -> Option<String> {
    let query = match stmt {
        Statement::Query(q) => q,
        Statement::Explain { statement, .. } => match &**statement {
            Statement::Query(q) => q,
            _ => return None,
        },
        _ => return None,
    };
    let mut p = Printer {
        tables,
        current,
        next: 1,
        scopes: Vec::new(),
    };
    p.query(query, written)
}

impl<'a> Printer<'a> {
    fn query(&mut self, query: &Query, written: &[Option<String>]) -> Option<String> {
        if query.with.is_some() {
            return None;
        }
        let SetExpr::Select(sel) = &*query.body else {
            return None;
        };
        let mut out = self.select(sel, written)?;
        if let Some(ob) = &query.order_by {
            let OrderByKind::Expressions(list) = &ob.kind else {
                return None;
            };
            let mut items = Vec::new();
            for o in list {
                let mut t = self.expr(&o.expr)?;
                if o.options.sort == Some(sqlparser::ast::OrderBySort::Desc) {
                    t.push_str(" desc");
                }
                items.push(t);
            }
            out.push_str(" order by ");
            out.push_str(&items.join(","));
        }
        match &query.limit_clause {
            None => {}
            Some(LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) if limit_by.is_empty() => match (limit, offset) {
                (Some(l), None) => {
                    let _ = write!(out, " limit {}", self.expr(l)?);
                }
                (Some(l), Some(o)) => {
                    let _ = write!(out, " limit {},{}", self.expr(&o.value)?, self.expr(l)?);
                }
                _ => return None,
            },
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
                let _ = write!(out, " limit {},{}", self.expr(offset)?, self.expr(limit)?);
            }
            Some(_) => return None,
        }
        self.scopes.pop();
        Some(out)
    }

    /// The `FROM` entry of a table factor.
    fn entry_of(&self, f: &TableFactor) -> Option<From<'a>> {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let parts: Vec<&Ident> = name.0.iter().map(ident_name).collect::<Option<_>>()?;
                let (db, t) = match parts.as_slice() {
                    [t] => (self.current.map(str::to_owned), t.value.clone()),
                    [d, t] => (Some(d.value.clone()), t.value.clone()),
                    _ => return None,
                };
                let table = self.tables.iter().find(|u| {
                    u.def.name.eq_ignore_ascii_case(&t)
                        && db
                            .as_deref()
                            .is_some_and(|d| u.db_name.eq_ignore_ascii_case(d))
                });
                let qual = alias.as_ref().map_or(t, |a| a.name.value.clone());
                Some(From { qual, db, table })
            }
            TableFactor::Derived { alias: Some(a), .. } => Some(From {
                qual: a.name.value.clone(),
                db: None,
                table: None,
            }),
            _ => None,
        }
    }

    fn factor_text(&mut self, f: &TableFactor) -> Option<String> {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let e = self.entry_of(f)?;
                let t = name.0.last().and_then(ident_name)?.value.clone();
                let mut s = format!("{}.{}", q(e.db.as_deref()?), q(&t));
                if alias.is_some() {
                    s.push(' ');
                    s.push_str(&q(&e.qual));
                }
                Some(s)
            }
            TableFactor::Derived {
                subquery,
                alias: Some(a),
                ..
            } => {
                let inner = self.query(subquery, &[])?;
                Some(format!("({inner}) {}", q(&a.name.value)))
            }
            _ => None,
        }
    }

    #[allow(clippy::too_many_lines)] // the clauses in order
    fn select(&mut self, sel: &Select, written: &[Option<String>]) -> Option<String> {
        if sel.distinct.is_some() || sel.top.is_some() || sel.into.is_some() {
            return None;
        }
        let n = self.next;
        self.next += 1;
        // The FROM list first: the select list's names resolve in it.
        let mut scope = Vec::new();
        for t in &sel.from {
            scope.push(self.entry_of(&t.relation)?);
            for j in &t.joins {
                scope.push(self.entry_of(&j.relation)?);
            }
        }
        self.scopes.push(scope);
        let mut items = Vec::new();
        for (i, item) in sel.projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(_) => {
                    let scope = self.scopes.last()?;
                    for f in scope {
                        let t = f.table?;
                        for c in t.def.columns.iter().filter(|c| !c.invisible) {
                            items.push(format!(
                                "{}.{}.{} AS {}",
                                q(f.db.as_deref()?),
                                q(&f.qual),
                                q(&c.name),
                                q(&c.name)
                            ));
                        }
                    }
                }
                SelectItem::UnnamedExpr(e) => {
                    let name = match e {
                        Expr::Identifier(i) => Some(self.column_name(None, &i.value)),
                        Expr::CompoundIdentifier(p) if p.len() >= 2 => Some(
                            self.column_name(Some(&p[p.len() - 2].value), &p[p.len() - 1].value),
                        ),
                        // A string constant is named by its value.
                        Expr::Value(v) => match &v.value {
                            Value::SingleQuotedString(t) | Value::DoubleQuotedString(t) => {
                                Some(t.clone())
                            }
                            other => Some(other.to_string()),
                        },
                        _ => written
                            .get(i)
                            .cloned()
                            .flatten()
                            .or_else(|| Some(e.to_string())),
                    }?;
                    items.push(format!("{} AS {}", self.expr(e)?, q(&name)));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    items.push(format!("{} AS {}", self.expr(expr)?, q(&alias.value)));
                }
                _ => return None,
            }
        }
        let mut out = format!("/* select#{n} */ select {}", items.join(","));
        let mut conds = Vec::new();
        if !sel.from.is_empty() {
            let mut from = Vec::new();
            for t in &sel.from {
                let mut s = self.factor_text(&t.relation)?;
                for j in &t.joins {
                    let right = self.factor_text(&j.relation)?;
                    match &j.join_operator {
                        JoinOperator::Join(c)
                        | JoinOperator::Inner(c)
                        | JoinOperator::CrossJoin(c) => {
                            s.push_str(" join ");
                            s.push_str(&right);
                            match c {
                                JoinConstraint::On(e) => conds.push(e.clone()),
                                JoinConstraint::None => {}
                                _ => return None,
                            }
                        }
                        JoinOperator::Left(JoinConstraint::On(e))
                        | JoinOperator::LeftOuter(JoinConstraint::On(e)) => {
                            let _ = write!(s, " left join {right} on({})", self.expr(e)?);
                        }
                        _ => return None,
                    }
                }
                from.push(s);
            }
            out.push_str(" from ");
            out.push_str(&from.join(" join "));
        }
        if let Some(w) = &sel.selection {
            conds.push(w.clone());
        }
        match conds.as_slice() {
            [] => {}
            [c] => {
                let _ = write!(out, " where {}", self.cond(c)?);
            }
            [first, rest @ ..] => {
                let mut acc = self.cond(first)?;
                for c in rest {
                    acc = format!("({acc} and {})", self.cond(c)?);
                }
                let _ = write!(out, " where {acc}");
            }
        }
        if let sqlparser::ast::GroupByExpr::Expressions(list, mods) = &sel.group_by {
            if !mods.is_empty() {
                return None;
            }
            if !list.is_empty() {
                let g: Option<Vec<String>> = list.iter().map(|e| self.expr(e)).collect();
                out.push_str(" group by ");
                out.push_str(&g?.join(","));
            }
        } else {
            return None;
        }
        if let Some(h) = &sel.having {
            let _ = write!(out, " having {}", self.cond(h)?);
        }
        Some(out)
    }

    /// The column of the select list's plain reference, named as its table
    /// names it.
    fn column_name(&self, qual: Option<&str>, name: &str) -> String {
        self.resolve(qual, name)
            .map_or_else(|| name.to_owned(), |(_, _, c)| c)
    }

    /// `(db, qualifier, column)` of a column reference.
    fn resolve(&self, qual: Option<&str>, name: &str) -> Option<(Option<String>, String, String)> {
        for scope in self.scopes.iter().rev() {
            for f in scope {
                if qual.is_some_and(|q| !q.eq_ignore_ascii_case(&f.qual)) {
                    continue;
                }
                match f.table {
                    Some(t) => {
                        if let Some(c) = t.def.column(name) {
                            return Some((
                                f.db.clone(),
                                f.qual.clone(),
                                t.def.columns[c].name.clone(),
                            ));
                        }
                    }
                    None if qual.is_some() => {
                        return Some((None, f.qual.clone(), name.to_owned()));
                    }
                    None => {}
                }
            }
        }
        None
    }

    /// A condition: `NOT (a = b)` as `(a <> b)`, as MySQL folds it.
    fn cond(&mut self, e: &Expr) -> Option<String> {
        if let Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } = e
        {
            let mut inner = &**expr;
            while let Expr::Nested(x) = inner {
                inner = x;
            }
            if let Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } = inner
            {
                return Some(format!("({} <> {})", self.expr(left)?, self.expr(right)?));
            }
        }
        self.expr(e)
    }

    #[allow(clippy::too_many_lines)] // one arm per expression form
    fn expr(&mut self, e: &Expr) -> Option<String> {
        Some(match e {
            Expr::Identifier(i) => self.column(None, &i.value)?,
            Expr::CompoundIdentifier(p) if p.len() >= 2 => {
                self.column(Some(&p[p.len() - 2].value), &p[p.len() - 1].value)?
            }
            Expr::Nested(x) => self.expr(x)?,
            Expr::Value(v) => match &v.value {
                Value::Number(n, _) => n.clone(),
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
                }
                Value::Null => "NULL".into(),
                Value::Boolean(b) => if *b { "true" } else { "false" }.into(),
                _ => return None,
            },
            Expr::BinaryOp { left, op, right } => {
                let op = match op {
                    BinaryOperator::NotEq => "<>".to_owned(),
                    BinaryOperator::And => "and".to_owned(),
                    BinaryOperator::Or => "or".to_owned(),
                    BinaryOperator::Xor => "xor".to_owned(),
                    o => o.to_string().to_ascii_lowercase(),
                };
                format!("({} {op} {})", self.expr(left)?, self.expr(right)?)
            }
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => format!("(not({}))", self.expr(expr)?),
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => format!("-({})", self.expr(expr)?),
            Expr::IsNull(x) => format!("({} is null)", self.expr(x)?),
            Expr::IsNotNull(x) => format!("({} is not null)", self.expr(x)?),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let l: Option<Vec<String>> = list.iter().map(|x| self.expr(x)).collect();
                format!(
                    "({} {}in ({}))",
                    self.expr(expr)?,
                    if *negated { "not " } else { "" },
                    l?.join(",")
                )
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => format!(
                "({} {}between {} and {})",
                self.expr(expr)?,
                if *negated { "not " } else { "" },
                self.expr(low)?,
                self.expr(high)?
            ),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char: None,
                any: false,
            } => format!(
                "({} {}like {})",
                self.expr(expr)?,
                if *negated { "not " } else { "" },
                self.expr(pattern)?
            ),
            Expr::Function(f) => {
                let name = object_name(&f.name)?.to_ascii_lowercase();
                let args = match &f.args {
                    FunctionArguments::None => Vec::new(),
                    FunctionArguments::List(l) => {
                        if l.duplicate_treatment.is_some() || !l.clauses.is_empty() {
                            return None;
                        }
                        let mut v = Vec::new();
                        for a in &l.args {
                            match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => {
                                    v.push(self.expr(x)?);
                                }
                                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                                    if name == "count" =>
                                {
                                    v.push("0".into());
                                }
                                _ => return None,
                            }
                        }
                        v
                    }
                    FunctionArguments::Subquery(_) => return None,
                };
                if f.over.is_some() || f.filter.is_some() || !f.within_group.is_empty() {
                    return None;
                }
                format!("{name}({})", args.join(","))
            }
            Expr::Subquery(sq) => format!("({})", self.query(sq, &[])?),
            _ => return None,
        })
    }

    fn column(&self, qual: Option<&str>, name: &str) -> Option<String> {
        let (db, t, c) = self.resolve(qual, name)?;
        Some(match db {
            Some(d) => format!("{}.{}.{}", q(&d), q(&t), q(&c)),
            None => format!("{}.{}", q(&t), q(&c)),
        })
    }
}

fn object_name(n: &ObjectName) -> Option<String> {
    let parts: Option<Vec<&Ident>> = n.0.iter().map(ident_name).collect();
    Some(
        parts?
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."),
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_statement_prints_as_mysql_prints_it() {
        use sqlparser::parser::Parser;
        let p = |sql: &str| {
            Parser::parse_sql(&sprig_plan::dialect::SprigMySql, sql)
                .expect("parses")
                .remove(0)
        };
        let s = p("EXPLAIN SELECT 1 + 2, 'a', NULL, NOW()");
        assert_eq!(
            super::expanded(
                &s,
                &[],
                Some("rg_a"),
                &[Some("1 + 2".into()), None, None, Some("NOW()".into())]
            )
            .as_deref(),
            Some(
                "/* select#1 */ select (1 + 2) AS `1 + 2`,'a' AS `a`,NULL AS `NULL`,now() AS `NOW()`"
            )
        );
    }
}
