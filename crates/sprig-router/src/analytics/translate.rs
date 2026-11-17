//! The analytic guard: which gathered
//! `SELECT`s DataFusion may answer, as a plan over shard scans.
//!
//! ```text
//! accepted:  SELECT [DISTINCT] item, …
//!            FROM t [AS a] { [INNER | LEFT [OUTER]] JOIN u [AS b] ON … | , u }
//!            [WHERE …] [GROUP BY col, …] [ORDER BY …] [LIMIT n [OFFSET m]]
//!   item:    a column, or COUNT(*), COUNT(col), COUNT(DISTINCT col), SUM(col),
//!            AVG(col), MIN(col), MAX(col), or ROW_NUMBER(), RANK(),
//!            DENSE_RANK(), COUNT(*), COUNT(col), SUM(col)
//!            OVER (PARTITION BY col, … ORDER BY col, …), with an alias
//!   WHERE/ON: AND of conjuncts that each read one table and print exactly
//!            (pushed to that table's shards), or `x.c = y.d` between two tables
//!            (a join key); a LEFT JOIN's ON may filter only its right side
//!   ORDER BY: output columns (alias, position or the column itself), ASC or DESC
//! refused (the statement is gathered instead): everything else — subqueries,
//!   derived tables, CTEs, set operations, HAVING, expressions over columns or
//!   aggregates, literals in the select list, `*`, RIGHT and FULL joins, window
//!   frames, ROLLUP, string or other functions
//! ```
//!
//! The types are checked later, against the shards' column definitions
//! ([`super::keys::Kind`]): this module only reads the statement.

use std::fmt::Write as _;

use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause,
    ObjectNamePart, OrderByKind, OrderBySort, SelectItem, SetExpr, Statement, TableFactor, Value,
    WindowType,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// Why a statement isn't analytic (it is gathered instead).
pub(crate) type Refusal = String;

type R<T> = std::result::Result<T, Refusal>;

/// A table the statement reads, with its columns (lower case, in order).
#[derive(Debug, Clone)]
pub(crate) struct TableCols {
    /// The schema the statement names it in (physical).
    pub schema: String,
    /// The table name, lower case.
    pub name: String,
    /// Its columns, lower case.
    pub columns: Vec<String>,
}

/// One column of one scan.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ColRef {
    /// The scan.
    pub scan: usize,
    /// The column, lower case.
    pub col: String,
}

/// One table instance in `FROM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scan {
    /// Index into the gathered plan's tables.
    pub table: usize,
    /// The alias the statement gives it (or its name), lower case.
    pub alias: String,
    /// Conjuncts pushed to its shards, as printed.
    pub filter: Vec<String>,
}

/// How a scan joins the scans before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinKind {
    /// `JOIN`, `INNER JOIN`, a comma.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
}

/// A scan after the first, joined on key equalities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JoinStep {
    /// The scan joined.
    pub scan: usize,
    /// Inner or left.
    pub kind: JoinKind,
    /// `(earlier scan's column, this scan's column)` pairs.
    pub on: Vec<(ColRef, ColRef)>,
}

/// An aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Agg {
    /// `COUNT(*)`.
    CountStar,
    /// `COUNT(col)`.
    Count(ColRef),
    /// `COUNT(DISTINCT col)`.
    CountDistinct(ColRef),
    /// `SUM(col)`.
    Sum(ColRef),
    /// `AVG(col)`.
    Avg(ColRef),
    /// `MIN(col)`.
    Min(ColRef),
    /// `MAX(col)`.
    Max(ColRef),
}

/// A window function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WinFn {
    /// `ROW_NUMBER()`.
    RowNumber,
    /// `RANK()`.
    Rank,
    /// `DENSE_RANK()`.
    DenseRank,
    /// `COUNT(*)`.
    CountStar,
    /// `COUNT(col)`.
    Count(ColRef),
    /// `SUM(col)`.
    Sum(ColRef),
}

/// A window function with its window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Win {
    /// The function.
    pub func: WinFn,
    /// `PARTITION BY`.
    pub partition: Vec<ColRef>,
    /// `ORDER BY`, with descending flags.
    pub order: Vec<(ColRef, bool)>,
}

/// One output column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Item {
    /// A column's value.
    Col(ColRef),
    /// An aggregate.
    Agg(Agg),
    /// A window function.
    Win(Win),
}

/// A sort key of the result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrderKey {
    /// Output column `i`.
    Item(usize, bool),
    /// A column not in the output (a plain query only).
    Col(ColRef, bool),
}

/// The plan the guard accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Query {
    /// Table instances, in `FROM` order.
    pub scans: Vec<Scan>,
    /// Joins of `scans[1..]`, in order.
    pub joins: Vec<JoinStep>,
    /// `GROUP BY` columns.
    pub group: Vec<ColRef>,
    /// `SELECT DISTINCT`.
    pub distinct: bool,
    /// Output columns.
    pub items: Vec<Item>,
    /// `ORDER BY`.
    pub order: Vec<OrderKey>,
    /// `LIMIT`.
    pub limit: Option<u64>,
    /// `OFFSET`.
    pub offset: u64,
    /// The statement is answered in two steps: this plan computes its
    /// groups and aggregates, and MariaDB finishes it (expressions over
    /// them, `HAVING`, the order and `LIMIT`) over those rows.
    pub finish: Option<Finish>,
}

/// MariaDB's step after the aggregates: the rows the plan computes go into
/// a temporary table made like `inner`'s answer, and `outer` reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Finish {
    /// The statement's own `FROM`, `WHERE` and `GROUP BY` with the plan's
    /// items as its select list (`g<i>` for each `GROUP BY` column, then
    /// `a<i>` for each aggregate): its answer's columns are the temporary
    /// table's.
    pub inner: String,
    /// The temporary table, qualified (in the first table's schema).
    pub table: String,
    /// The statement over that table: its select list, `HAVING` (as
    /// `WHERE`), `ORDER BY` and `LIMIT`, with each aggregate and grouped
    /// column replaced by the table's column.
    pub outer: String,
}

/// The temporary table a finished statement's rows go into.
pub(crate) const FINISH_TABLE: &str = "__sprig_fin";

impl Query {
    /// Whether it aggregates (`GROUP BY` or an aggregate in the output).
    pub(crate) fn aggregated(&self) -> bool {
        !self.group.is_empty() || self.items.iter().any(|i| matches!(i, Item::Agg(_)))
    }

    /// Whether it has window functions.
    pub(crate) fn windowed(&self) -> bool {
        self.items.iter().any(|i| matches!(i, Item::Win(_)))
    }
}

fn refuse<T>(why: impl Into<String>) -> R<T> {
    Err(why.into())
}

fn lc(i: &Ident) -> String {
    i.value.to_ascii_lowercase()
}

fn name_parts(n: &sqlparser::ast::ObjectName) -> R<Vec<String>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Ok(lc(i)),
            ObjectNamePart::Function(_) => refuse("a table function"),
        })
        .collect()
}

struct Cx<'a> {
    tables: &'a [TableCols],
    scans: Vec<Scan>,
}

impl Cx<'_> {
    fn add_scan(&mut self, f: &TableFactor) -> R<usize> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            with_hints,
            version: None,
            partitions,
            json_path: None,
            sample: None,
            index_hints,
            ..
        } = f
        else {
            return refuse("FROM names something other than a table");
        };
        if !with_hints.is_empty() || !partitions.is_empty() || !index_hints.is_empty() {
            return refuse("a table with hints or partitions");
        }
        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
            return refuse("an alias with column names");
        }
        let parts = name_parts(name)?;
        let (schema, tname) = match parts.as_slice() {
            [t] => (None, t.clone()),
            [s, t] => (Some(s.clone()), t.clone()),
            _ => return refuse("a table name of three parts"),
        };
        let found: Vec<usize> = self
            .tables
            .iter()
            .enumerate()
            .filter(|(_, t)| {
                t.name == tname
                    && schema
                        .as_ref()
                        .is_none_or(|s| t.schema.eq_ignore_ascii_case(s))
            })
            .map(|(i, _)| i)
            .collect();
        let [table] = found.as_slice() else {
            return refuse(format!("table {tname} isn't one gathered table"));
        };
        let alias = alias
            .as_ref()
            .map_or_else(|| tname.clone(), |a| lc(&a.name));
        if self.scans.iter().any(|s| s.alias == alias) {
            return refuse(format!("alias {alias} twice"));
        }
        self.scans.push(Scan {
            table: *table,
            alias,
            filter: Vec::new(),
        });
        Ok(self.scans.len() - 1)
    }

    fn has(&self, scan: usize, col: &str) -> bool {
        self.tables[self.scans[scan].table]
            .columns
            .iter()
            .any(|c| c == col)
    }

    /// The column an expression names, or `None` when it isn't one.
    fn colref(&self, e: &Expr) -> R<Option<ColRef>> {
        match e {
            Expr::Nested(x) => self.colref(x),
            Expr::Identifier(i) => {
                let col = lc(i);
                let hits: Vec<usize> = (0..self.scans.len())
                    .filter(|&s| self.has(s, &col))
                    .collect();
                match hits.as_slice() {
                    [s] => Ok(Some(ColRef { scan: *s, col })),
                    [] => refuse(format!("unknown column {col}")),
                    _ => refuse(format!("column {col} is ambiguous")),
                }
            }
            Expr::CompoundIdentifier(p) if p.len() == 2 => {
                let (a, col) = (lc(&p[0]), lc(&p[1]));
                let Some(s) = self.scans.iter().position(|s| s.alias == a) else {
                    return refuse(format!("unknown table {a}"));
                };
                if !self.has(s, &col) {
                    return refuse(format!("unknown column {a}.{col}"));
                }
                Ok(Some(ColRef { scan: s, col }))
            }
            Expr::CompoundIdentifier(_) => refuse("a column name of three parts"),
            _ => Ok(None),
        }
    }

    fn col(&self, e: &Expr) -> R<ColRef> {
        self.colref(e)?
            .map_or_else(|| refuse(format!("{e} isn't a column")), Ok)
    }

    /// The scans a conjunct reads (every column it names).
    fn reads(&self, e: &Expr) -> R<Vec<usize>> {
        let mut out = Vec::new();
        let mut err = None;
        let _ = sqlparser::ast::visit_expressions(e, |x| {
            match self.colref(x) {
                Ok(Some(c)) => {
                    if !out.contains(&c.scan) {
                        out.push(c.scan);
                    }
                }
                Ok(None) => {}
                Err(r) => err = Some(r),
            }
            std::ops::ControlFlow::<()>::Continue(())
        });
        match err {
            Some(r) => Err(r),
            None => Ok(out),
        }
    }
}

fn conjuncts(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        Expr::Nested(x)
            if matches!(
                **x,
                Expr::BinaryOp {
                    op: BinaryOperator::And,
                    ..
                }
            ) =>
        {
            conjuncts(x, out);
        }
        _ => out.push(e.clone()),
    }
}

/// Where one conjunct goes: a scan's filter, or a join key.
enum Placed {
    Filter(usize, String),
    Key(ColRef, ColRef),
}

fn place(cx: &Cx<'_>, e: &Expr) -> R<Placed> {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = e
        && let (Some(a), Some(b)) = (cx.colref(left)?, cx.colref(right)?)
        && a.scan != b.scan
    {
        return Ok(Placed::Key(a, b));
    }
    let reads = cx.reads(e)?;
    match reads.as_slice() {
        [s] if crate::planner::gather::pushable(e) => Ok(Placed::Filter(*s, format!("({e})"))),
        [_] => refuse(format!("a condition that doesn't print exactly: {e}")),
        [] => refuse(format!("a condition on no table: {e}")),
        _ => refuse(format!(
            "a condition across tables that isn't an equality: {e}"
        )),
    }
}

fn literal_u64(e: &Expr) -> R<u64> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => n.parse().map_err(|_| format!("LIMIT {n}")),
            _ => refuse("LIMIT that isn't a number"),
        },
        _ => refuse("LIMIT that isn't a number"),
    }
}

fn func_name(f: &sqlparser::ast::Function) -> R<String> {
    let parts = name_parts(&f.name)?;
    match parts.as_slice() {
        [n] => Ok(n.clone()),
        _ => refuse("a qualified function"),
    }
}

/// The one argument of `f`: `None` for `*`, with the `DISTINCT` flag.
fn one_arg(f: &sqlparser::ast::Function) -> R<(Option<Expr>, bool)> {
    let FunctionArguments::List(l) = &f.args else {
        return refuse("a function without an argument list");
    };
    if !l.clauses.is_empty() {
        return refuse("an aggregate with clauses");
    }
    let distinct = match l.duplicate_treatment {
        None | Some(DuplicateTreatment::All) => false,
        Some(DuplicateTreatment::Distinct) => true,
    };
    match l.args.as_slice() {
        [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)] | [] => Ok((None, distinct)),
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(e))] => Ok((Some(e.clone()), distinct)),
        _ => refuse("a function with several arguments"),
    }
}

fn plain_function(f: &sqlparser::ast::Function) -> R<()> {
    if f.uses_odbc_syntax
        || !matches!(f.parameters, FunctionArguments::None)
        || f.filter.is_some()
        || f.null_treatment.is_some()
        || !f.within_group.is_empty()
    {
        return refuse("a function with modifiers");
    }
    Ok(())
}

fn agg_of(cx: &Cx<'_>, f: &sqlparser::ast::Function) -> R<Agg> {
    plain_function(f)?;
    let name = func_name(f)?;
    let (arg, distinct) = one_arg(f)?;
    let col = |a: Option<Expr>| -> R<ColRef> {
        match a {
            Some(e) => cx.col(&e),
            None => refuse(format!("{name}(*)")),
        }
    };
    Ok(match (name.as_str(), distinct) {
        ("count", false) => match arg {
            None => Agg::CountStar,
            Some(e) => Agg::Count(cx.col(&e)?),
        },
        ("count", true) => Agg::CountDistinct(col(arg)?),
        ("sum", false) => Agg::Sum(col(arg)?),
        ("avg", false) => Agg::Avg(col(arg)?),
        // MIN and MAX ignore DISTINCT.
        ("min", _) => Agg::Min(col(arg)?),
        ("max", _) => Agg::Max(col(arg)?),
        _ => return refuse(format!("aggregate {name}")),
    })
}

fn win_of(cx: &Cx<'_>, f: &sqlparser::ast::Function) -> R<Win> {
    plain_function(f)?;
    let Some(WindowType::WindowSpec(w)) = &f.over else {
        return refuse("a named window");
    };
    if w.window_name.is_some() || w.window_frame.is_some() {
        return refuse("a window frame or name");
    }
    let name = func_name(f)?;
    let (arg, distinct) = one_arg(f)?;
    if distinct {
        return refuse("DISTINCT in a window function");
    }
    let func = match (name.as_str(), arg) {
        ("row_number", None) => WinFn::RowNumber,
        ("rank", None) => WinFn::Rank,
        ("dense_rank", None) => WinFn::DenseRank,
        ("count", None) => WinFn::CountStar,
        ("count", Some(e)) => WinFn::Count(cx.col(&e)?),
        ("sum", Some(e)) => WinFn::Sum(cx.col(&e)?),
        _ => return refuse(format!("window function {name}")),
    };
    if matches!(func, WinFn::RowNumber | WinFn::Rank | WinFn::DenseRank) && w.order_by.is_empty() {
        // MariaDB numbers the rows in an order of its own choosing.
        return refuse(format!("{name}() without ORDER BY"));
    }
    let partition = w
        .partition_by
        .iter()
        .map(|e| cx.col(e))
        .collect::<R<Vec<_>>>()?;
    let order = w
        .order_by
        .iter()
        .map(|o| {
            if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                return refuse("ORDER BY with NULLS or WITH FILL");
            }
            let desc = match o.options.sort {
                None | Some(OrderBySort::Asc) => false,
                Some(OrderBySort::Desc) => true,
                Some(OrderBySort::Using(_)) => return refuse("ORDER BY … USING"),
            };
            Ok((cx.col(&o.expr)?, desc))
        })
        .collect::<R<Vec<_>>>()?;
    Ok(Win {
        func,
        partition,
        order,
    })
}

/// The analytic plan of `sql` over `tables`, or why there is none: the
/// plan alone when it computes every output column, else (an aggregating
/// statement with `HAVING` or expressions over its aggregates) the plan of
/// its groups and aggregates with MariaDB's finishing step ([`Finish`]).
pub(crate) fn translate(sql: &str, tables: &[TableCols]) -> R<Query> {
    match plain(sql, tables) {
        Ok(q) => Ok(q),
        Err(why) => match finishing(sql, tables) {
            Ok(q) => Ok(q),
            Err(None) => Err(why),
            Err(Some(w2)) => Err(format!("{why}; finishing in MariaDB: {w2}")),
        },
    }
}

fn parse_one(sql: &str) -> R<Box<sqlparser::ast::Query>> {
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| format!("parse: {e}"))?;
    let [Statement::Query(q)] = stmts.as_slice() else {
        return refuse("not one SELECT");
    };
    Ok(q.clone())
}

/// The plan of a statement whose every output column the plan computes.
fn plain(sql: &str, tables: &[TableCols]) -> R<Query> {
    let q = parse_one(sql)?;
    if q.with.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return refuse("WITH, FETCH, locking or other clauses");
    }
    let SetExpr::Select(s) = &*q.body else {
        return refuse("a set operation or a parenthesized query");
    };
    if s.into.is_some()
        || s.top.is_some()
        || s.prewhere.is_some()
        || !s.lateral_views.is_empty()
        || !s.connect_by.is_empty()
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || s.having.is_some()
        || !s.named_window.is_empty()
        || s.qualify.is_some()
        || s.value_table_mode.is_some()
        || s.exclude.is_some()
    {
        return refuse("HAVING, INTO, named windows or other clauses");
    }
    if s.select_modifiers.is_some() {
        return refuse("SELECT modifiers");
    }
    let distinct = match &s.distinct {
        None | Some(Distinct::All) => false,
        Some(Distinct::Distinct) => true,
        Some(Distinct::On(_)) => return refuse("DISTINCT ON"),
    };
    let mut cx = Cx {
        tables,
        scans: Vec::new(),
    };
    let (joins, group) = sources(&mut cx, s)?;
    // The output.
    let mut items = Vec::new();
    let mut aliases: Vec<Option<String>> = Vec::new();
    for it in &s.projection {
        let (e, alias) = match it {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(lc(alias))),
            _ => return refuse("* in the select list"),
        };
        let item = match e {
            Expr::Function(f) if f.over.is_some() => Item::Win(win_of(&cx, f)?),
            Expr::Function(f) => Item::Agg(agg_of(&cx, f)?),
            other => match cx.colref(other)? {
                Some(c) => Item::Col(c),
                None => return refuse(format!("an expression in the select list: {other}")),
            },
        };
        items.push(item);
        aliases.push(alias);
    }
    let q2 = Query {
        scans: Vec::new(),
        joins: Vec::new(),
        group: group.clone(),
        distinct,
        items: items.clone(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        finish: None,
    };
    let aggregated = q2.aggregated();
    if aggregated && q2.windowed() {
        return refuse("window functions over aggregates");
    }
    if distinct && (aggregated || q2.windowed()) {
        return refuse("DISTINCT with aggregates or windows");
    }
    if aggregated {
        // Every plain column must be a GROUP BY column (ONLY_FULL_GROUP_BY
        // aside, MariaDB would pick any row's value).
        for it in &items {
            if let Item::Col(c) = it
                && !group.contains(c)
            {
                return refuse(format!("{} isn't grouped", c.col));
            }
        }
    }
    // ORDER BY.
    let mut order = Vec::new();
    if let Some(ob) = &q.order_by {
        if ob.interpolate.is_some() {
            return refuse("INTERPOLATE");
        }
        let OrderByKind::Expressions(es) = &ob.kind else {
            return refuse("ORDER BY ALL");
        };
        for o in es {
            if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                return refuse("ORDER BY with NULLS or WITH FILL");
            }
            let desc = match o.options.sort {
                None | Some(OrderBySort::Asc) => false,
                Some(OrderBySort::Desc) => true,
                Some(OrderBySort::Using(_)) => return refuse("ORDER BY … USING"),
            };
            // A position, an alias, an output column, or (plain queries)
            // any column.
            if let Expr::Value(v) = &o.expr
                && let Value::Number(n, _) = &v.value
            {
                let i: usize = n.parse().map_err(|_| format!("ORDER BY {n}"))?;
                if i == 0 || i > items.len() {
                    return refuse(format!("ORDER BY {i}"));
                }
                order.push(OrderKey::Item(i - 1, desc));
                continue;
            }
            if let Expr::Identifier(id) = &o.expr
                && let Some(i) = aliases.iter().position(|a| a.as_deref() == Some(&lc(id)))
            {
                order.push(OrderKey::Item(i, desc));
                continue;
            }
            let c = cx.col(&o.expr)?;
            if let Some(i) = items.iter().position(|it| *it == Item::Col(c.clone())) {
                order.push(OrderKey::Item(i, desc));
            } else if (aggregated && group.contains(&c)) || (!aggregated && !distinct) {
                order.push(OrderKey::Col(c, desc));
            } else {
                return refuse("ORDER BY a column that isn't in the output");
            }
        }
    }
    // LIMIT.
    let (mut limit, mut offset) = (None, 0);
    if let Some(l) = &q.limit_clause {
        match l {
            LimitClause::LimitOffset {
                limit: lim,
                offset: off,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return refuse("LIMIT BY");
                }
                if let Some(e) = lim {
                    limit = Some(literal_u64(e)?);
                }
                if let Some(o) = off {
                    offset = literal_u64(&o.value)?;
                }
            }
            LimitClause::OffsetCommaLimit {
                offset: off,
                limit: lim,
            } => {
                offset = literal_u64(off)?;
                limit = Some(literal_u64(lim)?);
            }
        }
    }
    if (limit.is_some() || offset > 0) && order.is_empty() {
        // MariaDB returns rows of its own choosing.
        return refuse("LIMIT without ORDER BY");
    }
    Ok(Query {
        scans: cx.scans,
        joins,
        group,
        distinct,
        items,
        order,
        limit,
        offset,
        finish: None,
    })
}

/// Aggregate names the plan computes.
const AGGREGATES: [&str; 5] = ["count", "sum", "avg", "min", "max"];

/// What [`finish_expr`] collects: the aggregates and the group columns an
/// expression uses, each as the temporary table's column.
struct Collected<'c, 'a> {
    cx: &'c Cx<'a>,
    group: &'c [ColRef],
    /// The select list's aliases (`HAVING` may name them).
    aliases: &'c [(String, Expr)],
    aggs: Vec<(Agg, String)>,
}

impl Collected<'_, '_> {
    /// `e` over the temporary table: every aggregate the plan computes is
    /// `a<i>`, every grouped column `g<i>`; with `aliases_inline`, a select
    /// alias is its own expression (for `HAVING`, which runs as `WHERE`).
    fn rewrite(&mut self, e: &Expr, aliases_inline: bool) -> R<Expr> {
        let mut out = e.clone();
        self.walk(&mut out, aliases_inline)?;
        Ok(out)
    }

    fn walk(&mut self, e: &mut Expr, aliases_inline: bool) -> R<()> {
        match e {
            Expr::Function(f) => {
                if f.over.is_some() {
                    return refuse("a window function");
                }
                let name = func_name(f)?;
                if AGGREGATES.contains(&name.as_str()) {
                    let agg = agg_of(self.cx, f)?;
                    let i = if let Some(i) = self.aggs.iter().position(|(a, _)| *a == agg) {
                        i
                    } else {
                        self.aggs.push((agg, f.to_string()));
                        self.aggs.len() - 1
                    };
                    *e = Expr::Identifier(Ident::new(format!("a{i}")));
                    return Ok(());
                }
                // Any other function MariaDB computes; its arguments may
                // hold aggregates and grouped columns.
                if let FunctionArguments::List(l) = &mut f.args {
                    for a in &mut l.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(x),
                            ..
                        } = a
                        {
                            self.walk(x, aliases_inline)?;
                        }
                    }
                    if !l.clauses.is_empty() {
                        return refuse(format!("function {name} with clauses"));
                    }
                }
                if f.filter.is_some() || !f.within_group.is_empty() {
                    return refuse(format!("function {name} with modifiers"));
                }
                Ok(())
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                if let Expr::Identifier(id) = &*e
                    && let Some((_, x)) = self.aliases.iter().find(|(a, _)| *a == lc(id))
                {
                    let column = self.cx.colref(e).ok().flatten();
                    if column.is_some() {
                        return refuse(format!("{} is both a column and an alias", lc(id)));
                    }
                    if aliases_inline {
                        *e = Expr::Nested(Box::new(x.clone()));
                    }
                    return Ok(());
                }
                let c = self.cx.col(e)?;
                let Some(i) = self.group.iter().position(|g| *g == c) else {
                    return refuse(format!("{} isn't grouped", c.col));
                };
                *e = Expr::Identifier(Ident::new(format!("g{i}")));
                Ok(())
            }
            Expr::Value(_) | Expr::TypedString { .. } => Ok(()),
            Expr::BinaryOp { left, right, .. } => {
                self.walk(left, aliases_inline)?;
                self.walk(right, aliases_inline)
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => self.walk(expr, aliases_inline),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.walk(expr, aliases_inline)?;
                self.walk(low, aliases_inline)?;
                self.walk(high, aliases_inline)
            }
            Expr::InList { expr, list, .. } => {
                self.walk(expr, aliases_inline)?;
                for x in list {
                    self.walk(x, aliases_inline)?;
                }
                Ok(())
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                self.walk(expr, aliases_inline)?;
                self.walk(pattern, aliases_inline)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(o) = operand {
                    self.walk(o, aliases_inline)?;
                }
                for c in conditions {
                    self.walk(&mut c.condition, aliases_inline)?;
                    self.walk(&mut c.result, aliases_inline)?;
                }
                if let Some(x) = else_result {
                    self.walk(x, aliases_inline)?;
                }
                Ok(())
            }
            other => refuse(format!(
                "an expression the finishing step doesn't read: {other}"
            )),
        }
    }
}

/// The two-step plan of an aggregating statement ([`Finish`]):
/// `Err(None)` when the statement isn't of that kind (no aggregate or
/// `GROUP BY`), `Err(Some(why))` when it is but can't be finished.
fn finishing(sql: &str, tables: &[TableCols]) -> std::result::Result<Query, Option<Refusal>> {
    let q = parse_one(sql).map_err(|_| None)?;
    let SetExpr::Select(s) = &*q.body else {
        return Err(None);
    };
    let aggregating = !matches!(&s.group_by, GroupByExpr::Expressions(es, _) if es.is_empty())
        || s.having.is_some()
        || s.projection.iter().any(|it| {
            let mut found = false;
            let _ = sqlparser::ast::visit_expressions(it, |x| {
                if let Expr::Function(f) = x
                    && f.over.is_none()
                    && func_name(f).is_ok_and(|n| AGGREGATES.contains(&n.as_str()))
                {
                    found = true;
                }
                std::ops::ControlFlow::<()>::Continue(())
            });
            found
        });
    if !aggregating {
        return Err(None);
    }
    finish_plan(&q, s, tables).map_err(Some)
}

/// The select list [`finish_plan`] prints, then replaces.
const PLACEHOLDER: &str = "__sprig_finish_items__";

#[allow(clippy::too_many_lines)] // the two statements, clause by clause
fn finish_plan(
    q: &sqlparser::ast::Query,
    s: &sqlparser::ast::Select,
    tables: &[TableCols],
) -> R<Query> {
    if q.with.is_some()
        || q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || q.settings.is_some()
        || q.format_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return refuse("WITH, FETCH, locking or other clauses");
    }
    if s.into.is_some()
        || s.top.is_some()
        || s.prewhere.is_some()
        || !s.lateral_views.is_empty()
        || !s.connect_by.is_empty()
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || !s.named_window.is_empty()
        || s.qualify.is_some()
        || s.value_table_mode.is_some()
        || s.exclude.is_some()
        || s.select_modifiers.is_some()
    {
        return refuse("INTO, named windows or other clauses");
    }
    if s.distinct.is_some() {
        return refuse("DISTINCT");
    }
    let mut cx = Cx {
        tables,
        scans: Vec::new(),
    };
    let (joins, group) = sources(&mut cx, s)?;
    let GroupByExpr::Expressions(group_exprs, _) = &s.group_by else {
        return refuse("GROUP BY ALL");
    };
    // The select list's aliases, for HAVING.
    let mut aliases: Vec<(String, Expr)> = Vec::new();
    for it in &s.projection {
        if let SelectItem::ExprWithAlias { expr, alias } = it {
            aliases.push((lc(alias), expr.clone()));
        }
    }
    let mut col = Collected {
        cx: &cx,
        group: &group,
        aliases: &[],
        aggs: Vec::new(),
    };
    // The outer select list: each item rewritten, keeping its alias.
    let mut outer_items: Vec<String> = Vec::new();
    let mut rewritten_aliases: Vec<(String, Expr)> = Vec::new();
    for it in &s.projection {
        let (e, alias) = match it {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => return refuse("* in the select list"),
        };
        let r = col.rewrite(e, false)?;
        if let Some(a) = alias {
            rewritten_aliases.push((lc(a), r.clone()));
        }
        outer_items.push(match alias {
            Some(a) => format!("{r} AS {a}"),
            None => r.to_string(),
        });
    }
    // HAVING runs as WHERE over the table: aliases are their expressions.
    let having = match &s.having {
        Some(h) => {
            let mut hc = Collected {
                cx: &cx,
                group: &group,
                aliases: &rewritten_aliases,
                aggs: std::mem::take(&mut col.aggs),
            };
            let mut inline = h.clone();
            // An alias names the rewritten item; the rest is rewritten.
            let r = hc.rewrite(&inline, true)?;
            inline = r;
            col.aggs = hc.aggs;
            Some(inline.to_string())
        }
        None => None,
    };
    // ORDER BY: positions and aliases stay; the rest is rewritten.
    let mut order: Vec<String> = Vec::new();
    if let Some(ob) = &q.order_by {
        if ob.interpolate.is_some() {
            return refuse("INTERPOLATE");
        }
        let OrderByKind::Expressions(es) = &ob.kind else {
            return refuse("ORDER BY ALL");
        };
        for o in es {
            if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                return refuse("ORDER BY with NULLS or WITH FILL");
            }
            let dir = match o.options.sort {
                None | Some(OrderBySort::Asc) => "",
                Some(OrderBySort::Desc) => " DESC",
                Some(OrderBySort::Using(_)) => return refuse("ORDER BY … USING"),
            };
            let keep = match &o.expr {
                Expr::Value(v) => matches!(v.value, Value::Number(..)),
                Expr::Identifier(id) if aliases.iter().any(|(a, _)| *a == lc(id)) => {
                    if cx.colref(&o.expr).ok().flatten().is_some() {
                        return refuse(format!("{} is both a column and an alias", lc(id)));
                    }
                    true
                }
                _ => false,
            };
            let e = if keep {
                o.expr.to_string()
            } else {
                col.rewrite(&o.expr, false)?.to_string()
            };
            order.push(format!("{e}{dir}"));
        }
    }
    let limit = match &q.limit_clause {
        None => String::new(),
        Some(LimitClause::LimitOffset { limit_by, .. }) if !limit_by.is_empty() => {
            return refuse("LIMIT BY");
        }
        // Printed with its leading space.
        Some(l) => l.to_string(),
    };
    if !limit.is_empty() && order.is_empty() {
        return refuse("LIMIT without ORDER BY");
    }
    if col.aggs.is_empty() && group.is_empty() {
        return refuse("nothing to aggregate");
    }
    // The plan: the group columns, then the aggregates.
    let mut items: Vec<Item> = group.iter().cloned().map(Item::Col).collect();
    items.extend(col.aggs.iter().map(|(a, _)| Item::Agg(a.clone())));
    let mut inner_items: Vec<String> = group_exprs
        .iter()
        .enumerate()
        .map(|(i, e)| format!("{e} AS g{i}"))
        .collect();
    inner_items.extend(
        col.aggs
            .iter()
            .enumerate()
            .map(|(i, (_, text))| format!("{text} AS a{i}")),
    );
    // The statement's own FROM, WHERE and GROUP BY under the new list (a
    // placeholder item, replaced once printed).
    let mut inner = s.clone();
    inner.projection = vec![SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(
        PLACEHOLDER,
    )))];
    inner.having = None;
    let inner_text = inner
        .to_string()
        .replacen(PLACEHOLDER, &inner_items.join(", "), 1);
    let first = cx
        .scans
        .first()
        .map(|sc| tables[sc.table].schema.clone())
        .ok_or_else(|| "no table".to_owned())?;
    let table = format!("`{}`.`{FINISH_TABLE}`", first.replace('`', "``"));
    let mut outer = format!("SELECT {} FROM {table}", outer_items.join(", "));
    if let Some(h) = having {
        let _ = write!(outer, " WHERE {h}");
    }
    if !order.is_empty() {
        let _ = write!(outer, " ORDER BY {}", order.join(", "));
    }
    outer.push_str(&limit);
    Ok(Query {
        scans: cx.scans.clone(),
        joins,
        group,
        distinct: false,
        items,
        order: Vec::new(),
        limit: None,
        offset: 0,
        finish: Some(Finish {
            inner: inner_text,
            table,
            outer,
        }),
    })
}

/// `FROM`, `WHERE` and `GROUP BY`: the scans (into `cx`) with their
/// pushed filters, the joins, and the group columns.
fn sources(cx: &mut Cx<'_>, s: &sqlparser::ast::Select) -> R<(Vec<JoinStep>, Vec<ColRef>)> {
    let mut pending: Vec<(usize, JoinKind, Vec<Expr>)> = Vec::new();
    let mut where_conj: Vec<Expr> = Vec::new();
    for (ti, twj) in s.from.iter().enumerate() {
        let first = cx.add_scan(&twj.relation)?;
        if ti > 0 {
            pending.push((first, JoinKind::Inner, Vec::new()));
        }
        for j in &twj.joins {
            if j.global {
                return refuse("GLOBAL JOIN");
            }
            let (kind, c) = match &j.join_operator {
                JoinOperator::Join(c) | JoinOperator::Inner(c) | JoinOperator::CrossJoin(c) => {
                    (JoinKind::Inner, c)
                }
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
                _ => return refuse("a RIGHT, FULL or other join"),
            };
            let sc = cx.add_scan(&j.relation)?;
            let mut conds = Vec::new();
            match c {
                JoinConstraint::On(e) => conjuncts(e, &mut conds),
                JoinConstraint::None => {}
                JoinConstraint::Using(_) | JoinConstraint::Natural => {
                    return refuse("USING or NATURAL");
                }
            }
            if kind == JoinKind::Left && conds.is_empty() {
                return refuse("LEFT JOIN without ON");
            }
            pending.push((sc, kind, conds));
        }
    }
    if cx.scans.is_empty() {
        return refuse("no table");
    }
    if let Some(w) = &s.selection {
        conjuncts(w, &mut where_conj);
    }
    // Scans on the null side of a LEFT JOIN: WHERE can't filter them before
    // the join.
    let nullable: Vec<usize> = pending
        .iter()
        .filter(|(_, k, _)| *k == JoinKind::Left)
        .map(|(s, _, _)| *s)
        .collect();
    let mut joins: Vec<JoinStep> = pending
        .iter()
        .map(|(s, k, _)| JoinStep {
            scan: *s,
            kind: *k,
            on: Vec::new(),
        })
        .collect();
    // The join step that introduces `scan` (None for the first scan).
    let step_of = |scan: usize| joins_index(&pending, scan);
    for (i, (sc, kind, conds)) in pending.iter().enumerate() {
        for c in conds {
            match place(cx, c)? {
                Placed::Key(a, b) => {
                    let (early, late) = if a.scan == *sc { (b, a) } else { (a, b) };
                    if late.scan != *sc || step_of(early.scan).is_some_and(|j| j >= i) {
                        return refuse("an ON equality that doesn't name the joined table");
                    }
                    joins[i].on.push((early, late));
                }
                Placed::Filter(s, f) => {
                    if *kind == JoinKind::Left && s != *sc {
                        return refuse("a LEFT JOIN's ON filters its left side");
                    }
                    if s != *sc && nullable.contains(&s) {
                        return refuse("an ON condition filters an earlier LEFT JOIN's right side");
                    }
                    if *kind == JoinKind::Inner && s != *sc && step_of(s).is_some_and(|j| j > i) {
                        return refuse("an ON condition on a later table");
                    }
                    cx.scans[s].filter.push(f);
                }
            }
        }
    }
    for c in &where_conj {
        match place(cx, c)? {
            Placed::Key(a, b) => {
                if nullable.contains(&a.scan) || nullable.contains(&b.scan) {
                    return refuse("WHERE compares a LEFT JOIN's right side");
                }
                let (ja, jb) = (step_of(a.scan), step_of(b.scan));
                let (early, late, j) = match (ja, jb) {
                    (None, Some(j)) => (a, b, j),
                    (Some(j), None) => (b, a, j),
                    (Some(x), Some(y)) if x < y => (a, b, y),
                    (Some(x), Some(_)) => (b, a, x),
                    (None, None) => return refuse("an equality inside one table"),
                };
                joins[j].on.push((early, late));
            }
            Placed::Filter(s, f) => {
                if nullable.contains(&s) {
                    return refuse("WHERE filters a LEFT JOIN's right side");
                }
                cx.scans[s].filter.push(f);
            }
        }
    }
    if joins.iter().any(|j| j.on.is_empty()) {
        // A cross product: never an analytic shape worth the copies.
        return refuse("a join without an equality");
    }
    // GROUP BY.
    let group = match &s.group_by {
        GroupByExpr::Expressions(es, mods) => {
            if !mods.is_empty() {
                return refuse("GROUP BY modifiers (ROLLUP)");
            }
            es.iter().map(|e| cx.col(e)).collect::<R<Vec<_>>>()?
        }
        GroupByExpr::All(_) => return refuse("GROUP BY ALL"),
    };
    Ok((joins, group))
}

/// The join step (index into the steps) that introduces `scan`; `None`
/// for the first scan.
fn joins_index(pending: &[(usize, JoinKind, Vec<Expr>)], scan: usize) -> Option<usize> {
    pending.iter().position(|(s, _, _)| *s == scan)
}

/// The columns of `scan` the plan reads, in first-use order.
pub(crate) fn columns_of_scan(q: &Query, scan: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |c: &ColRef| {
        if c.scan == scan && !out.contains(&c.col) {
            out.push(c.col.clone());
        }
    };
    for j in &q.joins {
        for (a, b) in &j.on {
            add(a);
            add(b);
        }
    }
    for c in &q.group {
        add(c);
    }
    for it in &q.items {
        match it {
            Item::Col(c) => add(c),
            Item::Agg(a) => match a {
                Agg::CountStar => {}
                Agg::Count(c)
                | Agg::CountDistinct(c)
                | Agg::Sum(c)
                | Agg::Avg(c)
                | Agg::Min(c)
                | Agg::Max(c) => add(c),
            },
            Item::Win(w) => {
                if let WinFn::Count(c) | WinFn::Sum(c) = &w.func {
                    add(c);
                }
                for c in &w.partition {
                    add(c);
                }
                for (c, _) in &w.order {
                    add(c);
                }
            }
        }
    }
    for o in &q.order {
        if let OrderKey::Col(c, _) = o {
            add(c);
        }
    }
    out
}
