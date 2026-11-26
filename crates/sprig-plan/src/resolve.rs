//! Name resolution: from the parser's tree to query blocks whose
//! expressions are bound to row slots.
//!
//! A block's `FROM` items get consecutive slots in the order they are
//! written; every expression of the block reads those slots, and the
//! planner maps them to positions in the joined row once it has chosen a
//! join order. An aggregate is lifted out of the expression it appears in
//! and read back through a slot of its own, numbered from [`AGG_BASE`].

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::Value;
use sprig_exec::ops::{AggKind, Aggregate, Row, RowSource};
use sprig_exec::{BinOp, CastTarget, ColumnRef, Expr, ExprType, Result, SqlError, Truth, compile};
use sqlparser::ast::{self, BinaryOperator, UnaryOperator, Value as SqlValue};

use crate::catalog::TableDef;
use crate::dependent::{DepKind, DepSpec, Recording};
use crate::exprs::{self, slot_ref, slot_type};
use crate::plan::Cx;

/// The first slot number of window function results.
pub(crate) const WIN_BASE: usize = 1 << 21;
/// The first slot number of scalar subqueries computed per row.
pub(crate) const SUBQ_BASE: usize = 1 << 22;
/// The first slot number of aggregate results.
pub(crate) const AGG_BASE: usize = 1 << 24;
/// Added to a slot of an enclosing block when a subquery reads it.
pub(crate) const OUTER_BASE: usize = 1 << 28;

/// The clause a name appears in, for error texts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Clause {
    Select,
    Where,
    On,
    GroupBy,
    Having,
    OrderBy,
}

impl Clause {
    fn name(self, maria: bool) -> &'static str {
        match (self, maria) {
            (Clause::Select, true) => "SELECT",
            (Clause::Where, true) => "WHERE",
            (Clause::On, true) => "ON",
            (Clause::GroupBy, true) => "GROUP BY",
            (Clause::Having, true) => "HAVING",
            (Clause::OrderBy, true) => "ORDER BY",
            (Clause::Select, false) => "field list",
            (Clause::Where, false) => "where clause",
            (Clause::On, false) => "on clause",
            (Clause::GroupBy, false) => "group statement",
            (Clause::Having, false) => "having clause",
            (Clause::OrderBy, false) => "order clause",
        }
    }
}

pub(crate) fn unknown_column(name: &str, clause: Clause, maria: bool) -> SqlError {
    SqlError::new(
        1054,
        "42S22",
        format!("Unknown column '{name}' in '{}'", clause.name(maria)),
    )
}

fn ambiguous(name: &str, clause: Clause, maria: bool) -> SqlError {
    SqlError::new(
        1052,
        "23000",
        format!("Column '{name}' in {} is ambiguous", clause.name(maria)),
    )
}

pub(crate) fn unsupported(what: &str) -> SqlError {
    SqlError::unsupported(what)
}

/// The rows of a derived table: its query is planned when the block
/// around it is (so that the block's conditions on it can move into it),
/// and run once, when the rows are first read.
pub(crate) struct Derived {
    block: RefCell<Option<Block>>,
    source: RefCell<Option<Box<dyn RowSource>>>,
    rows: RefCell<Option<Rc<Vec<Row>>>>,
    /// The column types.
    pub(crate) types: Vec<ExprType>,
    /// The planner's guess of the row count.
    pub(crate) est_rows: Cell<f64>,
}

impl Derived {
    /// A derived table whose query is already planned.
    pub(crate) fn new(source: Box<dyn RowSource>, est_rows: f64) -> Rc<Self> {
        let types = source.types();
        Rc::new(Self {
            block: RefCell::new(None),
            source: RefCell::new(Some(source)),
            rows: RefCell::new(None),
            types,
            est_rows: Cell::new(est_rows),
        })
    }

    /// A derived table whose query is resolved but not planned yet.
    pub(crate) fn lazy(block: Block, types: Vec<ExprType>) -> Rc<Self> {
        Rc::new(Self {
            block: RefCell::new(Some(block)),
            source: RefCell::new(None),
            rows: RefCell::new(None),
            types,
            est_rows: Cell::new(1000.0),
        })
    }

    /// The query, if it is still to be planned.
    pub(crate) fn take_block(&self) -> Option<Block> {
        self.block.borrow_mut().take()
    }

    /// Hands over the planned query.
    pub(crate) fn set_source(&self, source: Box<dyn RowSource>, est_rows: f64) {
        *self.source.borrow_mut() = Some(source);
        self.est_rows.set(est_rows);
    }

    /// The rows, running the derived query on the first call.
    pub(crate) fn rows(&self, ctx: &mut sprig_exec::Ctx<'_>) -> Result<Rc<Vec<Row>>> {
        if let Some(rows) = self.rows.borrow().as_ref() {
            return Ok(rows.clone());
        }
        let mut out = Vec::new();
        if let Some(mut source) = self.source.borrow_mut().take() {
            while let Some(row) = source.next(ctx)? {
                out.push(row);
            }
        } else {
            return Err(SqlError::new(
                1105,
                "HY000",
                "a derived table was read before it was planned",
            ));
        }
        let rows = Rc::new(out);
        *self.rows.borrow_mut() = Some(rows.clone());
        Ok(rows)
    }
}

/// What a `FROM` item reads.
#[derive(Clone)]
pub(crate) enum RelKind {
    /// A stored table.
    Table(Arc<TableDef>),
    /// The result of a query, held in memory.
    Derived(Rc<Derived>),
    /// `JSON_TABLE(...)`: rows made from a document, again for each row of
    /// the items it reads.
    JsonTable(Rc<crate::jsontable::JtRel>),
}

/// A column a `FROM` item shows under a name.
#[derive(Debug, Clone)]
pub(crate) struct RelCol {
    pub(crate) name: String,
    /// What a reference to the column reads: a slot of the item's row, or
    /// (for a derived table merged into its block) an expression over the
    /// underlying table's slots.
    pub(crate) expr: Expr,
}

/// One `FROM` item of a block.
#[derive(Clone)]
pub(crate) struct Rel {
    /// The name the item is referred to by.
    pub(crate) alias: String,
    /// The name `EXPLAIN` shows: the table's own alias when the item is
    /// a derived table merged into its block.
    pub(crate) shown: String,
    /// The database and table name, for a stored table named directly.
    pub(crate) qualified: Option<(String, String)>,
    pub(crate) kind: RelKind,
    /// The named columns.
    pub(crate) cols: Vec<RelCol>,
    /// The first slot of the item's row.
    pub(crate) base: usize,
    /// How many slots the item's row has.
    pub(crate) width: usize,
    /// The `ON` condition's parts, when the item is the inner side of a
    /// left join.
    pub(crate) outer_on: Option<Vec<Expr>>,
    /// A `FROM` item of the join's other side: the left join keeps that
    /// side's rows, so this item is never read before it.
    pub(crate) outer_dep: Option<usize>,
    /// Conditions that came with the item: the `WHERE` of a derived
    /// table that was merged into this block.
    pub(crate) pending: Vec<Expr>,
    /// Whether `*` leaves the item out (a subquery turned into a join).
    pub(crate) hidden: bool,
    /// The query block number shown by `EXPLAIN` for a derived table.
    pub(crate) derived_id: Option<usize>,
}

/// A column an unqualified name can reach: `rel`'s column `col`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StarCol {
    pub(crate) rel: usize,
    pub(crate) col: usize,
}

/// One aggregate of a block.
#[derive(Debug, Clone)]
pub(crate) struct AggSpec {
    pub(crate) kind: AggKind,
    pub(crate) arg: Option<Expr>,
    pub(crate) distinct: bool,
    pub(crate) separator: Option<Vec<u8>>,
    /// `GROUP_CONCAT(... ORDER BY ...)`: the keys and whether each descends.
    pub(crate) order: Vec<(Expr, bool)>,
    /// `GROUP_CONCAT(... LIMIT [offset,] count)` (MariaDB).
    pub(crate) limit: Option<(u64, u64)>,
    pub(crate) ty: ExprType,
}

/// One select-list item.
#[derive(Debug, Clone)]
pub(crate) struct Item {
    pub(crate) name: String,
    pub(crate) expr: Expr,
    /// Whether the item was written with `AS` (or is a bare column), so
    /// that other clauses can name it.
    pub(crate) alias: Option<String>,
}

/// A subquery predicate at the top level of `WHERE`: rows of the block
/// are kept when the subquery has (or, for `anti`, hasn't) a matching row.
pub(crate) struct SemiJoin {
    /// The subquery's rows.
    pub(crate) inner: Rc<Derived>,
    /// Pairs of an expression of this block and a column of the subquery
    /// that must be equal.
    pub(crate) keys: Vec<(Expr, usize)>,
    /// `NOT EXISTS`.
    pub(crate) anti: bool,
    /// The query block number shown by `EXPLAIN`.
    pub(crate) id: usize,
    /// The subquery, when it is one stored table with conditions: it can
    /// then be probed by key for each outer row.
    pub(crate) simple: Option<SimpleInner>,
}

/// A subquery over one stored table.
pub(crate) struct SimpleInner {
    pub(crate) table: Arc<TableDef>,
    pub(crate) alias: String,
    /// The subquery's select list, over the table's row.
    pub(crate) items: Vec<Expr>,
    /// Its conditions, over the table's row.
    pub(crate) filters: Vec<Expr>,
}

/// A scalar subquery that reads its outer block through equalities: for
/// each outer row, the value of one expression over the matching rows of
/// one table.
pub(crate) struct Apply {
    /// The subquery's table (`None` for a dependent subquery).
    pub(crate) rel: Option<Rel>,
    /// Pairs of an outer expression and an expression over the table
    /// that must be equal.
    pub(crate) keys: Vec<(Expr, Expr)>,
    /// The subquery's own conditions on the table.
    pub(crate) filters: Vec<Expr>,
    /// Its aggregates, when it has any: then it gives exactly one value.
    pub(crate) aggs: Vec<AggSpec>,
    pub(crate) grouped: bool,
    /// The value: over the table's row, or over the aggregates.
    pub(crate) item: Expr,
    /// `ORDER BY ... LIMIT 1`: the value comes from the first matching
    /// row in this order.
    pub(crate) first_by: Vec<(Expr, bool)>,
    pub(crate) ty: ExprType,
    /// The query block number shown by `EXPLAIN`.
    pub(crate) id: usize,
    /// A subquery planned again for each combination of the outer values
    /// in `keys` (their inner sides unused).
    pub(crate) dep: Option<Rc<crate::dependent::DepSpec>>,
}

/// What a window function computes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum WinFn {
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    CumeDist,
    Ntile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    NthValue,
    Agg(AggKind),
}

/// One window function of a block, over the block's rows (after grouping
/// when the block groups).
#[derive(Debug, Clone)]
pub(crate) struct WinSpec {
    pub(crate) func: WinFn,
    pub(crate) args: Vec<Expr>,
    pub(crate) partition: Vec<Expr>,
    pub(crate) order: Vec<(Expr, bool)>,
    pub(crate) frame: sprig_exec::ops::Frame,
    pub(crate) ty: ExprType,
}

impl WinSpec {
    /// Every expression the function reads.
    pub(crate) fn exprs(&self) -> impl Iterator<Item = &Expr> {
        self.args
            .iter()
            .chain(self.partition.iter())
            .chain(self.order.iter().map(|(e, _)| e))
    }

    /// The same function with `f` applied to each expression.
    pub(crate) fn map(&self, f: &dyn Fn(&Expr) -> Expr) -> Self {
        Self {
            args: self.args.iter().map(f).collect(),
            partition: self.partition.iter().map(f).collect(),
            order: self.order.iter().map(|(e, d)| (f(e), *d)).collect(),
            ..self.clone()
        }
    }
}

/// A resolved query block.
pub(crate) struct Block {
    pub(crate) rels: Vec<Rel>,
    /// The conjuncts of `WHERE` and of inner joins' `ON`.
    pub(crate) filters: Vec<Expr>,
    pub(crate) semis: Vec<SemiJoin>,
    /// Scalar subqueries tied to this block: each is computed per row
    /// and read through a slot numbered from [`SUBQ_BASE`].
    pub(crate) applies: Vec<Apply>,
    /// Window functions, read through slots numbered from [`WIN_BASE`].
    pub(crate) windows: Vec<WinSpec>,
    pub(crate) items: Vec<Item>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) aggs: Vec<AggSpec>,
    pub(crate) having: Option<Expr>,
    pub(crate) order_by: Vec<(Expr, bool)>,
    pub(crate) distinct: bool,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: u64,
    /// Whether the block groups its rows (it has `GROUP BY` or an
    /// aggregate).
    pub(crate) grouped: bool,
    /// `GROUP BY ... WITH ROLLUP`.
    pub(crate) rollup: bool,
    /// The block's number in `EXPLAIN`.
    pub(crate) id: usize,
    /// Pairs (outer expression, inner expression) taken out of the
    /// block's `WHERE` when it is a subquery tied to its outer block by
    /// equalities.
    pub(crate) correlated: Vec<(Expr, Expr)>,
}

/// What names can be seen from a place in a block.
pub(crate) struct Scope<'a> {
    pub(crate) rels: &'a [Rel],
    pub(crate) star: &'a [StarCol],
    /// The `FROM` items a qualified name may reach (an `ON` condition
    /// sees only the tables of its own join).
    pub(crate) visible: std::ops::Range<usize>,
    /// The enclosing block's scope, for a subquery.
    pub(crate) outer: Option<&'a Scope<'a>>,
}

impl Scope<'_> {
    fn col_expr(&self, sc: StarCol) -> Expr {
        self.rels[sc.rel].cols[sc.col].expr.clone()
    }

    fn unqualified(&self, name: &str, clause: Clause, maria: bool) -> Result<Option<Expr>> {
        let mut found = None;
        for sc in self.star {
            if self.rels[sc.rel].cols[sc.col]
                .name
                .eq_ignore_ascii_case(name)
            {
                if found.is_some() {
                    return Err(ambiguous(name, clause, maria));
                }
                found = Some(*sc);
            }
        }
        Ok(found.map(|sc| self.col_expr(sc)))
    }

    fn qualified(&self, schema: Option<&str>, table: &str, name: &str) -> Option<Expr> {
        let rel = self.rels.iter().enumerate().find_map(|(i, r)| {
            let hit = self.visible.contains(&i)
                && !r.hidden
                && r.alias.eq_ignore_ascii_case(table)
                && schema.is_none_or(|s| {
                    r.qualified
                        .as_ref()
                        .is_some_and(|(db, _)| db.eq_ignore_ascii_case(s))
                });
            hit.then_some(r)
        })?;
        rel.cols
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .map(|c| c.expr.clone())
    }
}

/// The partition, order and frame of an `OVER` clause, with a named
/// window's parts filled in.
fn window_of(
    over: &ast::WindowType,
    named: &[ast::NamedWindowDefinition],
) -> Result<(
    Vec<ast::Expr>,
    Vec<ast::OrderByExpr>,
    Option<ast::WindowFrame>,
)> {
    fn lookup(
        name: &ast::Ident,
        named: &[ast::NamedWindowDefinition],
        depth: usize,
    ) -> Result<(
        Vec<ast::Expr>,
        Vec<ast::OrderByExpr>,
        Option<ast::WindowFrame>,
    )> {
        let Some(def) = named
            .iter()
            .find(|d| d.0.value.eq_ignore_ascii_case(&name.value))
        else {
            return Err(SqlError::new(
                3579,
                "HY000",
                format!("Window name '{}' is not defined.", name.value),
            ));
        };
        if depth > named.len() {
            return Err(SqlError::new(
                3580,
                "HY000",
                "There is a circularity in the window dependency graph.",
            ));
        }
        match &def.1 {
            ast::NamedWindowExpr::NamedWindow(other) => lookup(other, named, depth + 1),
            ast::NamedWindowExpr::WindowSpec(spec) => spec_of(spec, named, depth + 1),
        }
    }
    fn spec_of(
        spec: &ast::WindowSpec,
        named: &[ast::NamedWindowDefinition],
        depth: usize,
    ) -> Result<(
        Vec<ast::Expr>,
        Vec<ast::OrderByExpr>,
        Option<ast::WindowFrame>,
    )> {
        let (mut p, mut o, mut f) = match &spec.window_name {
            Some(n) => lookup(n, named, depth)?,
            None => (Vec::new(), Vec::new(), None),
        };
        if !spec.partition_by.is_empty() {
            p.clone_from(&spec.partition_by);
        }
        if !spec.order_by.is_empty() {
            o.clone_from(&spec.order_by);
        }
        if spec.window_frame.is_some() {
            f.clone_from(&spec.window_frame);
        }
        Ok((p, o, f))
    }
    match over {
        ast::WindowType::NamedWindow(n) => lookup(n, named, 0),
        ast::WindowType::WindowSpec(spec) => spec_of(spec, named, 0),
    }
}

/// The type of a subquery predicate's value: MariaDB's 32-bit integer,
/// MySQL's 64-bit one, `len` characters wide.
fn truth_type(maria: bool, len: u32, nullable: bool) -> ExprType {
    let t = ExprType {
        prec: 1,
        width: if maria { 4 } else { 0 },
        ..ExprType::int(len, false)
    };
    if nullable { t.nullable() } else { t }
}

/// A scope's identity, for telling the blocks of a statement apart.
fn scope_id(s: &Scope<'_>) -> *const () {
    std::ptr::from_ref(s).cast()
}

/// Shifts an expression of an enclosing block into a subquery's slots.
fn to_outer(e: &Expr) -> Expr {
    exprs::remap(e, &|s| s + OUTER_BASE)
}

/// What an expression is being resolved for.
pub(crate) struct ExprCx<'a, 'b> {
    pub(crate) scope: &'a Scope<'a>,
    pub(crate) clause: Clause,
    /// Where aggregates are collected; `None` where they aren't allowed.
    pub(crate) aggs: Option<&'b mut Vec<AggSpec>>,
    /// The select list, where its aliases can be named.
    pub(crate) items: Option<&'a [Item]>,
    /// Whether aliases win over columns for a bare name at the top.
    pub(crate) alias_first: bool,
    /// Where scalar subqueries tied to the block are collected; `None`
    /// where the clause can't hold one.
    pub(crate) applies: Option<&'b mut Vec<Apply>>,
    /// Where window functions are collected; `None` where they aren't
    /// allowed.
    pub(crate) windows: Option<&'b mut Vec<WinSpec>>,
    /// The block's named windows (`WINDOW w AS (...)`).
    pub(crate) named: &'a [ast::NamedWindowDefinition],
    in_agg: bool,
}

impl<'a, 'b> ExprCx<'a, 'b> {
    pub(crate) fn new(scope: &'a Scope<'a>, clause: Clause) -> Self {
        Self {
            scope,
            clause,
            aggs: None,
            items: None,
            alias_first: false,
            applies: None,
            windows: None,
            named: &[],
            in_agg: false,
        }
    }
}

fn ident_text(parts: &[ast::Ident]) -> String {
    parts
        .iter()
        .map(|i| i.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn object_name(name: &ast::ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|p| match p {
            ast::ObjectNamePart::Identifier(i) => Ok(i.value.clone()),
            ast::ObjectNamePart::Function(_) => Err(unsupported("a computed name")),
        })
        .collect()
}

fn number(text: &str) -> Expr {
    if text.contains(['e', 'E']) {
        Expr::Double(text.to_owned())
    } else if text.contains('.') {
        let t = if text.starts_with('.') {
            format!("0{text}")
        } else {
            text.to_owned()
        };
        Expr::Decimal(t.trim_end_matches('.').to_owned())
    } else {
        Expr::Int(text.to_owned())
    }
}

fn hex_bytes(s: &str) -> Option<Vec<u8>> {
    let s = if s.len() % 2 == 1 {
        format!("0{s}")
    } else {
        s.to_owned()
    };
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok())
        .collect()
}

/// A value as a literal expression of the same family.
pub(crate) fn literal(v: &Value, ty: &ExprType) -> Expr {
    let cast_str = |text: String, target: CastTarget| Expr::Cast(Box::new(Expr::Str(text)), target);
    let signed = |text: String| match text.strip_prefix('-') {
        Some(rest) => (true, rest.to_owned()),
        None => (false, text),
    };
    let fsp = if ty.kind.is_temporal() {
        ty.dec.min(6)
    } else {
        6
    };
    match v {
        Value::Null => Expr::Null,
        Value::Int(i) => Expr::int(*i),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => Expr::Int(u.to_string()),
        Value::Year(y) => Expr::Int(y.to_string()),
        Value::Enum(e) => Expr::Int(e.to_string()),
        Value::Decimal(d) => {
            let (neg, text) = signed(d.to_string());
            let e = if text.contains('.') {
                Expr::Decimal(text)
            } else {
                // A whole number that is a decimal keeps its type.
                Expr::Cast(
                    Box::new(Expr::Int(text)),
                    CastTarget::Decimal(ty.prec.clamp(1, 65), 0),
                )
            };
            if neg { Expr::Neg(Box::new(e)) } else { e }
        }
        Value::Double(_) | Value::Float(_) => {
            let x = match v {
                Value::Double(x) => *x,
                Value::Float(f) => f64::from(*f),
                _ => 0.0,
            };
            let (neg, text) = signed(format!("{x:e}"));
            let e = Expr::Double(text);
            if neg { Expr::Neg(Box::new(e)) } else { e }
        }
        // A JSON value (a subquery's, computed once) stays a JSON value.
        Value::Str(_) | Value::Json(_) if ty.json => Expr::Typed(Box::new((v.clone(), *ty))),
        Value::Str(s) | Value::Json(s) => Expr::Str(String::from_utf8_lossy(s).into_owned()),
        Value::Bytes(b) => Expr::Cast(Box::new(Expr::Hex(b.to_vec())), CastTarget::Binary(None)),
        Value::Date(d) => cast_str(d.to_string(), CastTarget::Date),
        Value::Time(t) => cast_str(t.to_string(), CastTarget::Time(fsp)),
        Value::DateTime(d) => cast_str(d.to_string(), CastTarget::DateTime(fsp)),
        Value::Timestamp(t) => Expr::Int(t.seconds.to_string()),
    }
}

fn cast_target(dt: &ast::DataType) -> Result<CastTarget> {
    use ast::DataType as D;
    let len = |l: &Option<ast::CharacterLength>| match l {
        Some(ast::CharacterLength::IntegerLength { length, .. }) => {
            Some(u32::try_from(*length).unwrap_or(u32::MAX))
        }
        _ => None,
    };
    Ok(match dt {
        D::Signed | D::SignedInteger | D::Int(_) | D::Integer(_) | D::BigInt(_) => {
            CastTarget::Signed
        }
        D::Unsigned | D::UnsignedInteger => CastTarget::Unsigned,
        D::Decimal(info) | D::Dec(info) | D::Numeric(info) => match info {
            ast::ExactNumberInfo::None => CastTarget::Decimal(10, 0),
            ast::ExactNumberInfo::Precision(p) => CastTarget::Decimal((*p).min(65) as u8, 0),
            ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                CastTarget::Decimal((*p).min(65) as u8, (*s).clamp(0, 38) as u8)
            }
        },
        D::Double(_) | D::Real | D::Float(_) => CastTarget::Double,
        D::Char(l) | D::Character(l) | D::Varchar(l) => CastTarget::Char(len(l)),
        D::Binary(l) => CastTarget::Binary(l.map(|n| u32::try_from(n).unwrap_or(u32::MAX))),
        D::Date => CastTarget::Date,
        D::Time(f, _) => CastTarget::Time(f.unwrap_or(0).min(6) as u8),
        D::Datetime(f) => CastTarget::DateTime(f.unwrap_or(0).min(6) as u8),
        D::JSON => CastTarget::Json,
        other => return Err(unsupported(&format!("CAST AS {other}"))),
    })
}

fn agg_kind(name: &str) -> Option<AggKind> {
    Some(match name {
        "COUNT" => AggKind::Count,
        "SUM" => AggKind::Sum,
        "AVG" => AggKind::Avg,
        "MIN" => AggKind::Min,
        "MAX" => AggKind::Max,
        "GROUP_CONCAT" => AggKind::GroupConcat,
        "STD" | "STDDEV" | "STDDEV_POP" => AggKind::StdPop,
        "STDDEV_SAMP" => AggKind::StdSamp,
        "VARIANCE" | "VAR_POP" => AggKind::VarPop,
        "VAR_SAMP" => AggKind::VarSamp,
        "BIT_AND" => AggKind::BitAnd,
        "BIT_OR" => AggKind::BitOr,
        "BIT_XOR" => AggKind::BitXor,
        _ => return None,
    })
}

impl Cx<'_> {
    fn maria(&self) -> bool {
        self.session.persona.is_maria()
    }

    /// The literal a `?` stands for in this execution.
    fn param(&self, text: &str) -> Result<Expr> {
        let index: usize = text
            .trim_start_matches('?')
            .parse()
            .map_err(|_| unsupported("a named parameter"))?;
        let v = self
            .params
            .get(index)
            .ok_or_else(|| SqlError::new(1210, "HY000", "Incorrect arguments to EXECUTE"))?;
        let ty = match v {
            Value::Str(_) => ExprType {
                mb3: self.session.connection_mb3,
                ..ExprType::string(
                    0,
                    self.session.collation_connection,
                    sprig_exec::types::Derivation::Coercible,
                )
            },
            Value::Decimal(d) => ExprType::decimal(65, u32::from(d.scale()), false),
            _ => ExprType::datetime(6),
        };
        Ok(literal(v, &ty))
    }

    /// A string constant with a character set introducer (`_utf8mb4`,
    /// `_utf8mb3`, `_utf8`, `_binary`); other character sets stay refused.
    fn introduced(&self, cs: &str, value: &ast::Expr) -> Result<Expr> {
        use sprig_engine::keys::collation::Collation as C;
        let refused = || unsupported(&format!("the introducer _{cs}"));
        let ast::Expr::Value(v) = value else {
            return Err(refused());
        };
        let bytes: Vec<u8> = match &v.value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                s.clone().into_bytes()
            }
            SqlValue::HexStringLiteral(h) => {
                hex_bytes(h).ok_or_else(|| unsupported("a malformed hex literal"))?
            }
            _ => return Err(refused()),
        };
        let l = cs.to_ascii_lowercase();
        let coll = match (l.as_str(), self.maria()) {
            ("binary", _) => C::Binary,
            ("utf8mb4" | "utf8mb3" | "utf8", true) => C::Utf8mb4Uca1400AiCi,
            ("utf8mb4", false) => C::Utf8mb4Mysql0900AiCi,
            ("utf8mb3" | "utf8", false) => C::Utf8mb4GeneralCi,
            _ => return Err(refused()),
        };
        let mb3 = matches!(l.as_str(), "utf8mb3" | "utf8");
        let coercible = sprig_exec::types::Derivation::Coercible;
        if coll == C::Binary {
            let n = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            return Ok(literal(
                &Value::Bytes(bytes::Bytes::from(bytes)),
                &ExprType::string(n, C::Binary, coercible),
            ));
        }
        let text = String::from_utf8(bytes).map_err(|_| refused())?;
        let n = u32::try_from(text.chars().count()).unwrap_or(u32::MAX);
        let ascii = text.is_ascii();
        let v = Value::Str(bytes::Bytes::from(text.into_bytes()));
        let ty = ExprType::string(n, coll, coercible);
        if mb3 || coll != self.session.collation_connection {
            // Typed, so the value keeps its own collation (not the
            // connection's) and its character set's name.
            return Ok(Expr::Typed(Box::new((v, ExprType { mb3, ascii, ..ty }))));
        }
        Ok(literal(&v, &ty))
    }

    fn name_expr(&mut self, parts: &[ast::Ident], x: &ExprCx<'_, '_>, top: bool) -> Result<Expr> {
        let maria = self.maria();
        let text = ident_text(parts);
        let not_found = || unknown_column(&text, x.clause, maria);
        // A select-list name that stands for two different expressions
        // can't be referred to.
        let alias_of = |name: &str| -> Result<Option<Expr>> {
            let mut found: Option<&Expr> = None;
            for i in x.items.unwrap_or(&[]) {
                if i.alias
                    .as_deref()
                    .is_some_and(|a| a.eq_ignore_ascii_case(name))
                {
                    if found.is_some_and(|f| *f != i.expr) {
                        return Err(ambiguous(name, x.clause, maria));
                    }
                    found = Some(&i.expr);
                }
            }
            Ok(found.cloned())
        };
        match parts {
            [name] => {
                let name = name.value.as_str();
                if x.alias_first && top && !x.in_agg {
                    if let Some(e) = alias_of(name)? {
                        return Ok(e);
                    }
                }
                if let Some(e) = x.scope.unqualified(name, x.clause, maria)? {
                    return Ok(e);
                }
                if !x.in_agg {
                    if let Some(e) = alias_of(name)? {
                        return Ok(e);
                    }
                }
                let mut outer = x.scope.outer;
                let mut seen: Vec<*const ()> = Vec::new();
                while let Some(o) = outer {
                    seen.push(scope_id(o));
                    if let Some(e) = o.unqualified(name, x.clause, maria)? {
                        if let Some(r) = self.recorded(&[name], &e, &seen)? {
                            return Ok(r);
                        }
                        self.outer_refs += 1;
                        return Ok(to_outer(&e));
                    }
                    outer = o.outer;
                }
                if let Some(e) = self.bound_outer.as_ref().and_then(|b| b.find(&[name])) {
                    return Ok(e);
                }
                Err(not_found())
            }
            [table, name] => self.qualified_name(None, &table.value, &name.value, x, &text),
            [schema, table, name] => {
                self.qualified_name(Some(&schema.value), &table.value, &name.value, x, &text)
            }
            _ => Err(not_found()),
        }
    }

    fn qualified_name(
        &mut self,
        schema: Option<&str>,
        table: &str,
        name: &str,
        x: &ExprCx<'_, '_>,
        text: &str,
    ) -> Result<Expr> {
        if let Some(e) = x.scope.qualified(schema, table, name) {
            return Ok(e);
        }
        let mut parts: Vec<&str> = schema.into_iter().collect();
        parts.push(table);
        parts.push(name);
        let mut outer = x.scope.outer;
        let mut seen: Vec<*const ()> = Vec::new();
        while let Some(o) = outer {
            seen.push(scope_id(o));
            if let Some(e) = o.qualified(schema, table, name) {
                if let Some(r) = self.recorded(&parts, &e, &seen)? {
                    return Ok(r);
                }
                self.outer_refs += 1;
                return Ok(to_outer(&e));
            }
            outer = o.outer;
        }
        if let Some(e) = self.bound_outer.as_ref().and_then(|b| b.find(&parts)) {
            return Ok(e);
        }
        Err(unknown_column(text, x.clause, self.maria()))
    }

    fn boxed(&mut self, e: &ast::Expr, x: &mut ExprCx<'_, '_>) -> Result<Box<Expr>> {
        Ok(Box::new(self.expr_in(e, x, false)?))
    }

    /// Resolves an expression.
    pub(crate) fn expr(&mut self, e: &ast::Expr, x: &mut ExprCx<'_, '_>) -> Result<Expr> {
        self.expr_in(e, x, true)
    }

    fn expr_in(&mut self, e: &ast::Expr, x: &mut ExprCx<'_, '_>, top: bool) -> Result<Expr> {
        use ast::Expr as A;
        Ok(match e {
            // `@v`: a user variable, read through the session.
            A::Identifier(_) if user_var(e).is_some() => Expr::Func(
                "SPRIG_USER_VAR".into(),
                vec![Expr::Keyword(user_var(e).unwrap_or_default())],
            ),
            A::Identifier(i) => self.name_expr(std::slice::from_ref(i), x, top)?,
            A::CompoundIdentifier(parts) => self.name_expr(parts, x, top)?,
            A::Nested(inner) => self.expr_in(inner, x, top)?,
            A::Value(v) => match &v.value {
                SqlValue::Number(n, _) => number(n),
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                    Expr::Str(s.clone())
                }
                SqlValue::HexStringLiteral(h) => {
                    Expr::Hex(hex_bytes(h).ok_or_else(|| unsupported("a malformed hex literal"))?)
                }
                SqlValue::Boolean(b) => Expr::Bool(*b),
                SqlValue::Null => Expr::Null,
                SqlValue::Placeholder(p) => self.param(p)?,
                other => return Err(unsupported(&format!("the literal {other}"))),
            },
            // `_cs 'text'`, `_cs X'..'`: a constant in that character set
            // with its default collation, as coercible as a plain string.
            A::Prefixed { prefix, value } if prefix.value.starts_with('_') => {
                self.introduced(&prefix.value[1..], value)?
            }
            A::TypedString(t) => {
                let s = match &t.value.value {
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
                    other => return Err(unsupported(&format!("the literal {other}"))),
                };
                let target = match t.data_type {
                    ast::DataType::Date => CastTarget::Date,
                    ast::DataType::Time(..) => CastTarget::Time(6),
                    ast::DataType::Timestamp(..) | ast::DataType::Datetime(_) => {
                        CastTarget::DateTime(6)
                    }
                    _ => return Err(unsupported("a typed literal")),
                };
                Expr::Cast(Box::new(Expr::Str(s)), target)
            }
            A::UnaryOp { op, expr } => {
                let inner = self.boxed(expr, x)?;
                match op {
                    UnaryOperator::Plus => *inner,
                    UnaryOperator::Minus => Expr::Neg(inner),
                    UnaryOperator::Not | UnaryOperator::BangNot => Expr::Not(inner),
                    UnaryOperator::BitwiseNot => Expr::BitNot(inner),
                    other => return Err(unsupported(&format!("the operator {other}"))),
                }
            }
            A::BinaryOp {
                left,
                op: op @ (BinaryOperator::Eq | BinaryOperator::NotEq),
                right,
            } if matches!((&**left, &**right), (A::Tuple(l), A::Tuple(r))
                if l.len() != r.len()
                    || !l.iter().chain(r.iter()).any(|e| matches!(e, A::Tuple(_)))) =>
            {
                // A row equality of single values, column by column (so the
                // key lookups see each column); nested rows go to the
                // expression engine's row comparison below.
                let (A::Tuple(l), A::Tuple(r)) = (&**left, &**right) else {
                    return Err(unsupported("this row comparison"));
                };
                if l.len() != r.len() || l.is_empty() {
                    return Err(SqlError::new(
                        1241,
                        "21000",
                        format!("Operand should contain {} column(s)", l.len()),
                    ));
                }
                let mut parts = Vec::with_capacity(l.len());
                for (a, b) in l.iter().zip(r) {
                    let a = self.boxed(a, x)?;
                    parts.push(Expr::Binary(BinOp::Eq, a, self.boxed(b, x)?));
                }
                let all = exprs::and_all(parts).unwrap_or(Expr::Bool(true));
                if *op == BinaryOperator::Eq {
                    all
                } else {
                    Expr::Not(Box::new(all))
                }
            }
            // Any other comparison with a row operand, such as keyset
            // pagination's `(created, id) < (?, ?)`: the expression engine's
            // row comparison (`ROW`), with each server's shape errors. In a
            // filter, `(a, ...) < (x, ...)` also gets the implied `a <= x`
            // (true, false or unknown exactly when needed, so the answer is
            // unchanged) for the range analysis of the first column.
            A::BinaryOp {
                left,
                op:
                    op @ (BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Spaceship),
                right,
            } if matches!(&**left, A::Tuple(_)) || matches!(&**right, A::Tuple(_)) => {
                let bop = match op {
                    BinaryOperator::Eq => BinOp::Eq,
                    BinaryOperator::NotEq => BinOp::Ne,
                    BinaryOperator::Lt => BinOp::Lt,
                    BinaryOperator::LtEq => BinOp::Le,
                    BinaryOperator::Gt => BinOp::Gt,
                    BinaryOperator::GtEq => BinOp::Ge,
                    _ => BinOp::NullSafeEq,
                };
                let implied = match (&**left, &**right, bop) {
                    (A::Tuple(l), A::Tuple(r), BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge)
                        if matches!(x.clause, Clause::Where | Clause::On)
                            && l.len() == r.len()
                            && l.len() > 1
                            && matches!(l[0], A::Identifier(_) | A::CompoundIdentifier(_))
                            && matches!(
                                r[0],
                                A::Value(_) | A::Identifier(_) | A::CompoundIdentifier(_)
                            ) =>
                    {
                        let first = match bop {
                            BinOp::Lt | BinOp::Le => BinOp::Le,
                            _ => BinOp::Ge,
                        };
                        Some((first, &l[0], &r[0]))
                    }
                    _ => None,
                };
                let row = Expr::Binary(bop, self.boxed(left, x)?, self.boxed(right, x)?);
                match implied {
                    Some((first, a, b)) => {
                        let a = self.boxed(a, x)?;
                        let first = Expr::Binary(first, a, self.boxed(b, x)?);
                        Expr::Binary(BinOp::And, Box::new(first), Box::new(row))
                    }
                    None => row,
                }
            }
            // A row value: the expression engine's `ROW`, which is only
            // valid as an operand of a comparison or `IN` (1241 elsewhere).
            A::Tuple(items) => {
                let mut row = Vec::with_capacity(items.len());
                for item in items {
                    row.push(self.expr_in(item, x, false)?);
                }
                Expr::Func("ROW".into(), row)
            }
            // `d + INTERVAL n unit`, `INTERVAL n unit + d`, `d - INTERVAL n unit`.
            A::BinaryOp {
                left,
                op: op @ (BinaryOperator::Plus | BinaryOperator::Minus),
                right,
            } if matches!(&**right, A::Interval(_))
                || (*op == BinaryOperator::Plus && matches!(&**left, A::Interval(_))) =>
            {
                let (d, iv) = match (&**left, &**right) {
                    (_, A::Interval(iv)) => (&**left, iv),
                    (A::Interval(iv), _) => (&**right, iv),
                    _ => return Err(syntax_error()),
                };
                let d = self.expr_in(d, x, false)?;
                let (n, unit) = self.interval(iv, x)?;
                let name = if *op == BinaryOperator::Plus {
                    "DATE_ADD"
                } else {
                    "DATE_SUB"
                };
                Expr::Func(name.into(), vec![d, n, unit])
            }
            A::Interval(_) => return Err(syntax_error()),
            A::Extract { field, expr, .. } => {
                let unit = unit_keyword(field)?;
                Expr::Func("EXTRACT".into(), vec![unit, self.expr_in(expr, x, false)?])
            }
            A::MemberOf(m) => Expr::Func(
                "MEMBER OF".into(),
                vec![
                    self.expr_in(&m.value, x, false)?,
                    self.expr_in(&m.array, x, false)?,
                ],
            ),
            // `@v := e`: assigns user variable `v`, as each row reaches it.
            A::BinaryOp {
                left,
                op: BinaryOperator::Assignment,
                right,
            } if user_var(left).is_some() => Expr::Func(
                "SPRIG_SET_USER_VAR".into(),
                vec![
                    Expr::Keyword(user_var(left).unwrap_or_default()),
                    self.expr_in(right, x, false)?,
                ],
            ),
            // `doc->'$.path'` is `JSON_EXTRACT(doc, '$.path')`; `->>` also
            // unquotes.
            A::BinaryOp {
                left,
                op: op @ (BinaryOperator::Arrow | BinaryOperator::LongArrow),
                right,
            } => {
                let e = Expr::Func(
                    "JSON_EXTRACT".into(),
                    vec![
                        self.expr_in(left, x, false)?,
                        self.expr_in(right, x, false)?,
                    ],
                );
                if matches!(op, BinaryOperator::LongArrow) {
                    Expr::Func("JSON_UNQUOTE".into(), vec![e])
                } else {
                    e
                }
            }
            A::BinaryOp { left, op, right } => {
                let op = match op {
                    BinaryOperator::Plus => BinOp::Add,
                    BinaryOperator::Minus => BinOp::Sub,
                    BinaryOperator::Multiply => BinOp::Mul,
                    BinaryOperator::Divide => BinOp::Div,
                    BinaryOperator::Modulo => BinOp::Mod,
                    BinaryOperator::MyIntegerDivide => BinOp::IntDiv,
                    BinaryOperator::Gt => BinOp::Gt,
                    BinaryOperator::Lt => BinOp::Lt,
                    BinaryOperator::GtEq => BinOp::Ge,
                    BinaryOperator::LtEq => BinOp::Le,
                    BinaryOperator::Spaceship => BinOp::NullSafeEq,
                    BinaryOperator::Eq => BinOp::Eq,
                    BinaryOperator::NotEq => BinOp::Ne,
                    BinaryOperator::And => BinOp::And,
                    BinaryOperator::Or | BinaryOperator::StringConcat => BinOp::Or,
                    BinaryOperator::Xor => BinOp::Xor,
                    BinaryOperator::BitwiseOr => BinOp::BitOr,
                    BinaryOperator::BitwiseAnd => BinOp::BitAnd,
                    BinaryOperator::BitwiseXor => BinOp::BitXor,
                    BinaryOperator::PGBitwiseShiftLeft => BinOp::Shl,
                    BinaryOperator::PGBitwiseShiftRight => BinOp::Shr,
                    other => return Err(unsupported(&format!("the operator {other}"))),
                };
                let l = self.boxed(left, x)?;
                let r = self.boxed(right, x)?;
                Expr::Binary(op, l, r)
            }
            A::IsNull(a) => Expr::IsNull(self.boxed(a, x)?, false),
            A::IsNotNull(a) => Expr::IsNull(self.boxed(a, x)?, true),
            A::IsTrue(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::True, false),
            A::IsNotTrue(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::True, true),
            A::IsFalse(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::False, false),
            A::IsNotFalse(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::False, true),
            A::IsUnknown(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::Unknown, false),
            A::IsNotUnknown(a) => Expr::IsTruth(self.boxed(a, x)?, Truth::Unknown, true),
            A::IsNotDistinctFrom(a, b) => {
                let l = self.boxed(a, x)?;
                Expr::Binary(BinOp::NullSafeEq, l, self.boxed(b, x)?)
            }
            A::InList {
                expr,
                list,
                negated,
            } => {
                let a = self.boxed(expr, x)?;
                let mut items = Vec::with_capacity(list.len());
                for item in list {
                    items.push(self.expr_in(item, x, false)?);
                }
                Expr::In(a, items, *negated)
            }
            A::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let a = self.boxed(expr, x)?;
                let lo = self.boxed(low, x)?;
                Expr::Between(a, lo, self.boxed(high, x)?, *negated)
            }
            A::Like {
                negated,
                any: false,
                expr,
                pattern,
                escape_char,
            } => {
                let a = self.boxed(expr, x)?;
                let p = self.boxed(pattern, x)?;
                let esc = match escape_char {
                    Some(c) => Some(self.boxed(c, x)?),
                    None => None,
                };
                Expr::Like(a, p, esc, *negated)
            }
            A::RLike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let a = self.expr_in(expr, x, false)?;
                let p = self.expr_in(pattern, x, false)?;
                let m = Expr::Func("REGEXP".into(), vec![a, p]);
                if *negated { Expr::Not(Box::new(m)) } else { m }
            }
            A::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let operand = match operand {
                    Some(o) => Some(self.boxed(o, x)?),
                    None => None,
                };
                let mut whens = Vec::with_capacity(conditions.len());
                for w in conditions {
                    let c = self.expr_in(&w.condition, x, false)?;
                    whens.push((c, self.expr_in(&w.result, x, false)?));
                }
                let otherwise = match else_result {
                    Some(o) => Some(self.boxed(o, x)?),
                    None => None,
                };
                Expr::Case {
                    operand,
                    whens,
                    otherwise,
                }
            }
            A::Cast {
                kind: ast::CastKind::Cast,
                expr,
                data_type,
                format: None,
            } => Expr::Cast(self.boxed(expr, x)?, cast_target(data_type)?),
            A::Convert {
                is_try: false,
                expr,
                data_type: Some(dt),
                charset: None,
                ..
            } => Expr::Cast(self.boxed(expr, x)?, cast_target(dt)?),
            // `CONVERT(e USING cs)` to a UTF-8 character set (the
            // expression engine's strings) or to `binary`; other character
            // sets stay refused.
            A::Convert {
                is_try: false,
                expr,
                data_type: None,
                charset: Some(cs),
                ..
            } if {
                let n = object_name(cs)?.join(".").to_ascii_lowercase();
                matches!(n.as_str(), "utf8mb4" | "utf8mb3" | "utf8" | "binary")
            } =>
            {
                let cs = object_name(cs)?.join(".");
                if cs.eq_ignore_ascii_case("binary") {
                    Expr::Cast(self.boxed(expr, x)?, CastTarget::Binary(None))
                } else {
                    // The character set's own default collation, not the
                    // connection's.
                    Expr::Func(
                        "CONVERT_USING".into(),
                        vec![self.expr_in(expr, x, false)?, Expr::Keyword(cs)],
                    )
                }
            }
            A::Collate { expr, collation } => {
                let name = object_name(collation)?.join(".");
                let coll =
                    Collation::resolve(&name, self.session.persona.flavor()).ok_or_else(|| {
                        SqlError::new(1273, "HY000", format!("Unknown collation: '{name}'"))
                    })?;
                let l = name.to_ascii_lowercase();
                let mb3 = l.starts_with("utf8mb3_") || l.starts_with("utf8_");
                Expr::Collate(self.boxed(expr, x)?, coll, mb3)
            }
            A::Ceil { expr, .. } => Expr::Func("CEIL".into(), vec![self.expr_in(expr, x, false)?]),
            A::Floor { expr, .. } => {
                Expr::Func("FLOOR".into(), vec![self.expr_in(expr, x, false)?])
            }
            A::Position { expr, r#in } => {
                let sub = self.expr_in(expr, x, false)?;
                Expr::Func("LOCATE".into(), vec![sub, self.expr_in(r#in, x, false)?])
            }
            A::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut args = vec![self.expr_in(expr, x, false)?];
                match (substring_from, substring_for) {
                    (Some(f), None) => args.push(self.expr_in(f, x, false)?),
                    (Some(f), Some(n)) => {
                        args.push(self.expr_in(f, x, false)?);
                        args.push(self.expr_in(n, x, false)?);
                    }
                    (None, Some(n)) => {
                        args.push(Expr::int(1));
                        args.push(self.expr_in(n, x, false)?);
                    }
                    (None, None) => return Err(SqlError::param_count("SUBSTRING")),
                }
                Expr::Func("SUBSTRING".into(), args)
            }
            A::Trim {
                trim_where,
                trim_what,
                expr,
                trim_characters: None,
            } => {
                let s = self.expr_in(expr, x, false)?;
                match trim_what {
                    None => {
                        let name = match trim_where {
                            Some(ast::TrimWhereField::Leading) => "LTRIM",
                            Some(ast::TrimWhereField::Trailing) => "RTRIM",
                            _ => "TRIM",
                        };
                        Expr::Func(name.into(), vec![s])
                    }
                    Some(what) => {
                        let name = match trim_where {
                            Some(ast::TrimWhereField::Leading) => "TRIM_LEADING",
                            Some(ast::TrimWhereField::Trailing) => "TRIM_TRAILING",
                            _ => "TRIM_BOTH",
                        };
                        Expr::Func(name.into(), vec![self.expr_in(what, x, false)?, s])
                    }
                }
            }
            A::Function(f) => self.function(f, x)?,
            A::Subquery(q) => self.scalar_subquery(q, x)?,
            A::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let a = self.boxed(expr, x)?;
                self.in_subquery(a, subquery, *negated, x)?
            }
            A::Exists { subquery, negated } => self.exists(subquery, *negated, x)?,
            other => return Err(unsupported(&other.to_string())),
        })
    }

    fn function(&mut self, f: &ast::Function, x: &mut ExprCx<'_, '_>) -> Result<Expr> {
        let name = object_name(&f.name)?.join(".").to_ascii_uppercase();
        if f.filter.is_some() || !f.within_group.is_empty() {
            return Err(unsupported(&format!("{name} with FILTER or WITHIN GROUP")));
        }
        let (args, distinct, clauses): (&[ast::FunctionArg], bool, &[ast::FunctionArgumentClause]) =
            match &f.args {
                ast::FunctionArguments::None => (&[], false, &[]),
                ast::FunctionArguments::List(l) => (
                    &l.args,
                    l.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct),
                    &l.clauses,
                ),
                ast::FunctionArguments::Subquery(_) => {
                    return Err(unsupported(&format!("{name} of a subquery")));
                }
            };
        if let Some(over) = &f.over {
            return self.window_fn(&name, args, distinct, clauses, over, x);
        }
        if matches!(name.as_str(), "JSON_ARRAYAGG" | "JSON_OBJECTAGG") {
            let e = self.json_agg(&name, args, distinct, clauses)?;
            return self.expr_in(&e, x, false);
        }
        if let Some(kind) = agg_kind(&name) {
            return self.aggregate(&name, kind, args, distinct, clauses, x);
        }
        if name == "JSON_VALUE"
            && !distinct
            && let [ast::FunctionArgumentClause::JsonReturningClause(r)] = clauses
        {
            return self.json_value_returning(args, &r.data_type, x);
        }
        if distinct || !clauses.is_empty() {
            return Err(unsupported(&format!("{name} with these arguments")));
        }
        if matches!(
            name.as_str(),
            "NEXTVAL" | "LASTVAL" | "SETVAL" | "SPRIG_SEQUENCE_STATE"
        ) {
            if self.maria() {
                return self.sequence_call(&name, args, x);
            }
            // MySQL has no sequences: a stored function of that name.
            let written = object_name(&f.name)?.join(".");
            let db = self.catalog.default_schema().unwrap_or_default();
            return Err(SqlError::new(
                1305,
                "42000",
                format!("FUNCTION {db}.{written} does not exist"),
            ));
        }
        if let Some(e) = self.unit_call(&name, args, x)? {
            return Ok(e);
        }
        let mut out = Vec::with_capacity(args.len());
        for a in args {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    out.push(self.expr_in(e, x, false)?);
                }
                _ => return Err(unsupported(&format!("{name} with these arguments"))),
            }
        }
        Ok(Expr::Func(name, out))
    }

    /// `JSON_VALUE(doc, path RETURNING type)`: MySQL's, with the type
    /// passed on as a keyword (`RETURNING SIGNED`, `RETURNING
    /// DECIMAL(5,2)`, ...). MariaDB has no `RETURNING`: a syntax error.
    fn json_value_returning(
        &mut self,
        args: &[ast::FunctionArg],
        dt: &ast::DataType,
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        use ast::DataType as D;
        if self.maria() {
            return Err(SqlError::new(
                1064,
                "42000",
                format!(
                    "You have an error in your SQL syntax; check the manual that corresponds to \
                     your MariaDB server version for the right syntax to use near 'RETURNING {dt})' \
                     at line 1"
                ),
            ));
        }
        let len = |l: &Option<ast::CharacterLength>| match l {
            Some(ast::CharacterLength::IntegerLength { length, .. }) => format!("({length})"),
            _ => String::new(),
        };
        let fsp = |f: &Option<u64>| format!("({})", f.unwrap_or(0).min(6));
        let kw = match dt {
            D::Signed | D::SignedInteger | D::Int(_) | D::Integer(_) | D::BigInt(_) => {
                "SIGNED".to_owned()
            }
            D::Unsigned | D::UnsignedInteger => "UNSIGNED".to_owned(),
            D::Decimal(info) | D::Dec(info) | D::Numeric(info) => match info {
                ast::ExactNumberInfo::None => "DECIMAL".to_owned(),
                ast::ExactNumberInfo::Precision(p) => format!("DECIMAL({p})"),
                ast::ExactNumberInfo::PrecisionAndScale(p, sc) => format!("DECIMAL({p},{sc})"),
            },
            D::Double(_) | D::Real => "DOUBLE".to_owned(),
            D::Float(_) => "FLOAT".to_owned(),
            D::Char(l) | D::Character(l) | D::Varchar(l) => format!("CHAR{}", len(l)),
            D::Binary(l) => match l {
                Some(n) => format!("BINARY({n})"),
                None => "BINARY".to_owned(),
            },
            D::Date => "DATE".to_owned(),
            D::Time(f, _) => format!("TIME{}", fsp(f)),
            D::Datetime(f) => format!("DATETIME{}", fsp(f)),
            D::Custom(n, _) if n.to_string().eq_ignore_ascii_case("YEAR") => "YEAR".to_owned(),
            D::JSON => "JSON".to_owned(),
            other => return Err(unsupported(&format!("JSON_VALUE RETURNING {other}"))),
        };
        let mut out = Vec::with_capacity(3);
        for a in args {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    out.push(self.expr_in(e, x, false)?);
                }
                _ => return Err(unsupported("JSON_VALUE with these arguments")),
            }
        }
        if out.len() != 2 {
            return Err(SqlError::param_count("json_value"));
        }
        out.push(Expr::Keyword(format!("RETURNING {kw}")));
        Ok(Expr::Func("JSON_VALUE".to_owned(), out))
    }

    /// A sequence function (MariaDB): its first argument names the
    /// sequence, passed on as the sequence's identity.
    fn sequence_call(
        &mut self,
        name: &str,
        args: &[ast::FunctionArg],
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        let lower = name.to_ascii_lowercase();
        let (first, rest) = args
            .split_first()
            .ok_or_else(|| SqlError::param_count(&lower))?;
        let named = match first {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => match e {
                ast::Expr::Identifier(i) => Some((None, i.value.clone())),
                ast::Expr::CompoundIdentifier(p) if p.len() == 2 => {
                    Some((Some(p[0].value.clone()), p[1].value.clone()))
                }
                _ => None,
            },
            _ => None,
        };
        let Some((schema, seq)) = named else {
            return Err(SqlError::new(
                1064,
                "42000",
                format!("You have an error in your SQL syntax; {lower} takes a sequence name"),
            ));
        };
        let schema = match schema {
            Some(s) => s,
            None => self
                .catalog
                .default_schema()
                .ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))?,
        };
        let id = self.catalog.sequence(&schema, &seq)?;
        let mut out = vec![Expr::Keyword(id)];
        for a in rest {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    if name == "SPRIG_SEQUENCE_STATE" {
                        if let ast::Expr::Value(v) = e
                            && let ast::Value::Number(n, _) = &v.value
                        {
                            out.push(Expr::Int(n.clone()));
                            continue;
                        }
                        return Err(unsupported("a sequence field"));
                    }
                    out.push(self.expr_in(e, x, false)?);
                }
                _ => return Err(unsupported(&format!("{name} with these arguments"))),
            }
        }
        Ok(Expr::Func(name.to_owned(), out))
    }

    /// A window function call: resolved into the block's window list and
    /// read back through its slot.
    fn window_fn(
        &mut self,
        name: &str,
        args: &[ast::FunctionArg],
        distinct: bool,
        clauses: &[ast::FunctionArgumentClause],
        over: &ast::WindowType,
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        let maria = self.maria();
        let lower = name.to_ascii_lowercase();
        if x.windows.is_none() || x.in_agg {
            return Err(if maria {
                SqlError::new(
                    4015,
                    "HY000",
                    "Window function is allowed only in SELECT list and ORDER BY clause",
                )
            } else {
                SqlError::new(
                    3593,
                    "HY000",
                    format!("You cannot use the window function '{lower}' in this context.'"),
                )
            });
        }
        if distinct || !clauses.is_empty() {
            return Err(unsupported(&format!(
                "{name} with DISTINCT or a clause in a window"
            )));
        }
        let func = match name {
            "ROW_NUMBER" => WinFn::RowNumber,
            "RANK" => WinFn::Rank,
            "DENSE_RANK" => WinFn::DenseRank,
            "PERCENT_RANK" => WinFn::PercentRank,
            "CUME_DIST" => WinFn::CumeDist,
            "NTILE" => WinFn::Ntile,
            "LAG" => WinFn::Lag,
            "LEAD" => WinFn::Lead,
            "FIRST_VALUE" => WinFn::FirstValue,
            "LAST_VALUE" => WinFn::LastValue,
            "NTH_VALUE" => WinFn::NthValue,
            other => match agg_kind(other) {
                Some(
                    k
                    @ (AggKind::Count | AggKind::Sum | AggKind::Avg | AggKind::Min | AggKind::Max),
                ) => WinFn::Agg(k),
                _ => return Err(unsupported(&format!("{name} as a window function"))),
            },
        };
        let (lo, hi) = match func {
            WinFn::RowNumber
            | WinFn::Rank
            | WinFn::DenseRank
            | WinFn::PercentRank
            | WinFn::CumeDist => (0, 0),
            WinFn::Ntile | WinFn::FirstValue | WinFn::LastValue | WinFn::Agg(_) => (1, 1),
            WinFn::Lag | WinFn::Lead => (1, if maria { 2 } else { 3 }),
            WinFn::NthValue => (2, 2),
        };
        // `COUNT(*)`.
        let star = matches!(
            args,
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)]
        );
        if star && func != WinFn::Agg(AggKind::Count) {
            return Err(unsupported(&format!("{name}(*)")));
        }
        if !star && (args.len() < lo || args.len() > hi) {
            return Err(SqlError::param_count(name));
        }
        // The window: a named one, possibly extended.
        let (partition_ast, order_ast, frame_ast) = window_of(over, x.named).map_err(|e| {
            if maria && e.code == 3579 {
                let n = e.message.split('\'').nth(1).unwrap_or_default();
                SqlError::new(
                    4009,
                    "HY000",
                    format!("Window specification with name '{n}' is not defined"),
                )
            } else {
                e
            }
        })?;
        let saved = x.windows.take();
        let resolved = self.window_parts(args, star, &partition_ast, &order_ast, x);
        x.windows = saved;
        let (mut wargs, partition, order) = resolved?;
        let ordered = !order.is_empty();
        if maria
            && !ordered
            && matches!(
                func,
                WinFn::Rank
                    | WinFn::DenseRank
                    | WinFn::PercentRank
                    | WinFn::CumeDist
                    | WinFn::Ntile
                    | WinFn::Lag
                    | WinFn::Lead
            )
        {
            return Err(SqlError::new(
                4018,
                "HY000",
                format!("No order list in window specification for '{lower}'"),
            ));
        }
        let frame = match &frame_ast {
            None => sprig_exec::ops::Frame::default_for(ordered),
            Some(f) => self.frame_of(f)?,
        };
        let count_ty = ExprType::int(21, !maria);
        let ty = match func {
            WinFn::RowNumber | WinFn::Rank | WinFn::DenseRank | WinFn::Ntile => count_ty,
            WinFn::PercentRank | WinFn::CumeDist => {
                if maria {
                    ExprType {
                        dec: 10,
                        len: 17,
                        ..ExprType::double()
                    }
                    .nullable()
                } else {
                    ExprType::double()
                }
            }
            WinFn::Lag | WinFn::Lead | WinFn::FirstValue | WinFn::LastValue | WinFn::NthValue => {
                self.type_of(&wargs[0])?.nullable()
            }
            WinFn::Agg(k) => {
                let arg = match wargs.first() {
                    Some(a) => Some(compile(a, &self.session)?),
                    None => None,
                };
                let kind = if star { AggKind::CountStar } else { k };
                *Aggregate::new(kind, arg, false, &self.session)?.ty()
            }
        };
        let func = if star {
            WinFn::Agg(AggKind::CountStar)
        } else {
            func
        };
        if star {
            wargs.clear();
        }
        let Some(list) = x.windows.as_deref_mut() else {
            return Err(unsupported("a window function here"));
        };
        list.push(WinSpec {
            func,
            args: wargs,
            partition,
            order,
            frame,
            ty,
        });
        Ok(slot_ref(WIN_BASE + list.len() - 1, &lower, &ty))
    }

    /// The arguments, partition and order of a window function.
    fn window_parts(
        &mut self,
        args: &[ast::FunctionArg],
        star: bool,
        partition: &[ast::Expr],
        order: &[ast::OrderByExpr],
        x: &mut ExprCx<'_, '_>,
    ) -> Result<(Vec<Expr>, Vec<Expr>, Vec<(Expr, bool)>)> {
        let mut wargs = Vec::new();
        if !star {
            for a in args {
                match a {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                        wargs.push(self.expr_in(e, x, false)?);
                    }
                    _ => return Err(unsupported("these window function arguments")),
                }
            }
        }
        let mut parts = Vec::with_capacity(partition.len());
        for p in partition {
            parts.push(self.expr_in(p, x, false)?);
        }
        let mut ord = Vec::with_capacity(order.len());
        for o in order {
            if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                return Err(unsupported("this window ORDER BY option"));
            }
            let desc = o.options.sort == Some(ast::OrderBySort::Desc);
            ord.push((self.expr_in(&o.expr, x, false)?, desc));
        }
        Ok((wargs, parts, ord))
    }

    /// A frame clause: `ROWS` with row counts, or `RANGE` with
    /// `UNBOUNDED` and `CURRENT ROW` bounds.
    fn frame_of(&mut self, f: &ast::WindowFrame) -> Result<sprig_exec::ops::Frame> {
        use sprig_exec::ops::FrameBound as B;
        let rows = match f.units {
            ast::WindowFrameUnits::Rows => true,
            ast::WindowFrameUnits::Range => false,
            ast::WindowFrameUnits::Groups => return Err(unsupported("GROUPS frames")),
        };
        let mut bound = |b: &ast::WindowFrameBound| -> Result<B> {
            Ok(match b {
                ast::WindowFrameBound::CurrentRow => B::CurrentRow,
                ast::WindowFrameBound::Preceding(None) => B::UnboundedPreceding,
                ast::WindowFrameBound::Following(None) => B::UnboundedFollowing,
                ast::WindowFrameBound::Preceding(Some(e))
                | ast::WindowFrameBound::Following(Some(e)) => {
                    if !rows {
                        return Err(unsupported("RANGE frames with an offset"));
                    }
                    let scope = Scope {
                        rels: &[],
                        star: &[],
                        visible: 0..0,
                        outer: None,
                    };
                    let n = self.const_u64(e, &scope)?;
                    if matches!(b, ast::WindowFrameBound::Preceding(_)) {
                        B::Preceding(n)
                    } else {
                        B::Following(n)
                    }
                }
            })
        };
        let start = bound(&f.start_bound)?;
        let end = match &f.end_bound {
            Some(e) => bound(e)?,
            None => B::CurrentRow,
        };
        Ok(sprig_exec::ops::Frame { rows, start, end })
    }

    /// `INTERVAL n unit`: the amount and the unit as a keyword.
    fn interval(&mut self, iv: &ast::Interval, x: &mut ExprCx<'_, '_>) -> Result<(Expr, Expr)> {
        if iv.last_field.is_some()
            || iv.leading_precision.is_some()
            || iv.fractional_seconds_precision.is_some()
        {
            return Err(syntax_error());
        }
        let unit = unit_keyword(iv.leading_field.as_ref().ok_or_else(syntax_error)?)?;
        Ok((self.expr_in(&iv.value, x, false)?, unit))
    }

    /// The date and time functions that take an interval or a unit:
    /// `DATE_ADD(d, INTERVAL n unit)` (and `DATE_SUB`, `ADDDATE`, `SUBDATE`),
    /// `TIMESTAMPADD(unit, n, d)`, `TIMESTAMPDIFF(unit, a, b)`. `None` for
    /// any other call.
    fn unit_call(
        &mut self,
        name: &str,
        args: &[ast::FunctionArg],
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Option<Expr>> {
        let plain = |a: &'_ ast::FunctionArg| match a {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        };
        let args: Vec<ast::Expr> = match args.iter().map(plain).collect::<Option<Vec<_>>>() {
            Some(a) => a,
            None => return Ok(None),
        };
        Ok(Some(match name {
            "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => match args.as_slice() {
                [d, ast::Expr::Interval(iv)] => {
                    let d = self.expr_in(d, x, false)?;
                    let (n, unit) = self.interval(iv, x)?;
                    Expr::Func(name.into(), vec![d, n, unit])
                }
                // `ADDDATE(d, n)`: days.
                [d, n] if matches!(name, "ADDDATE" | "SUBDATE") => {
                    let d = self.expr_in(d, x, false)?;
                    let n = self.expr_in(n, x, false)?;
                    Expr::Func(name.into(), vec![d, n, Expr::Keyword("DAY".into())])
                }
                _ => return Err(syntax_error()),
            },
            "TIMESTAMPADD" | "TIMESTAMPDIFF" => match args.as_slice() {
                [u, a, b] => {
                    let unit = match u {
                        ast::Expr::Identifier(i) if i.quote_style.is_none() => {
                            let up = i.value.to_ascii_uppercase();
                            let up = up.strip_prefix("SQL_TSI_").unwrap_or(&up).to_owned();
                            if !TIMESTAMP_UNITS.contains(&up.as_str()) {
                                return Err(syntax_error());
                            }
                            Expr::Keyword(up)
                        }
                        _ => return Err(syntax_error()),
                    };
                    let a = self.expr_in(a, x, false)?;
                    let b = self.expr_in(b, x, false)?;
                    Expr::Func(name.into(), vec![unit, a, b])
                }
                _ => return Err(SqlError::param_count(name)),
            },
            _ => return Ok(None),
        }))
    }

    /// A name an enclosing block answered, while a dependent subquery is
    /// resolved: recorded for the subquery whose block it is, and read as
    /// a typed `NULL` meanwhile. `None` when no dependent subquery is
    /// being resolved across that block. `seen`: the enclosing scopes
    /// walked, the answering one last.
    fn recorded(&mut self, parts: &[&str], e: &Expr, seen: &[*const ()]) -> Result<Option<Expr>> {
        if self.recording.is_empty() {
            return Ok(None);
        }
        let at = seen.last().copied();
        let mut crossed = false;
        let mut owned = false;
        for r in &mut self.recording {
            if !seen.contains(&r.boundary) {
                continue;
            }
            crossed = true;
            if Some(r.boundary) == at {
                owned = true;
                let key: Vec<String> = parts.iter().map(|p| p.to_ascii_lowercase()).collect();
                if !r.uses.iter().any(|(k, _)| *k == key) {
                    r.uses.push((key, e.clone()));
                }
            }
        }
        if !crossed {
            return Ok(None);
        }
        if !owned {
            return Err(unsupported("a subquery that reads a query two levels out"));
        }
        let ty = self.type_of(e)?.nullable();
        Ok(Some(Expr::Typed(Box::new((Value::Null, ty)))))
    }

    /// A subquery that reads its outer block in a way no other path
    /// covers: planned again for each combination of the outer values it
    /// reads (see [`crate::dependent`]).
    fn dependent(
        &mut self,
        q: &ast::Query,
        kind: DepKind,
        lhs: Option<Expr>,
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        let Some(catalog) = self.catalog.shared() else {
            return Err(unsupported("a subquery that reads its outer query here"));
        };
        self.recording.push(Recording {
            boundary: scope_id(x.scope),
            uses: Vec::new(),
        });
        let planned = self.query(q, Some(x.scope), "DEPENDENT SUBQUERY");
        let rec = self.recording.pop();
        let planned = planned?;
        let uses = rec.map(|r| r.uses).unwrap_or_default();
        let one_column = |n: usize| {
            SqlError::new(
                1241,
                "21000",
                format!("Operand should contain {n} column(s)"),
            )
        };
        let width = planned.types.len();
        if width != 1 && !matches!(kind, DepKind::Exists { .. }) {
            return Err(one_column(1));
        }
        let inner_ty = planned.types.first().copied();
        let mut keys: Vec<(Expr, Expr)> = Vec::with_capacity(uses.len() + 1);
        let mut key_types = Vec::with_capacity(uses.len() + 1);
        let mut names = Vec::with_capacity(uses.len());
        for (n, e) in uses {
            key_types.push(self.type_of(&e)?);
            keys.push((e, Expr::Null));
            names.push(n);
        }
        let maria = self.maria();
        let ty = match kind {
            DepKind::Scalar => inner_ty.map_or_else(ExprType::boolean, |t| {
                let mut t = t.nullable();
                // A subquery's string is shown without a fixed scale.
                if t.kind == sprig_exec::Kind::Str {
                    t.dec = self.session.persona.not_fixed_dec();
                }
                t
            }),
            DepKind::Exists { .. } => truth_type(maria, 1, false),
            DepKind::In { negated } => {
                let Some(l) = lhs else {
                    return Err(unsupported("IN without its left side"));
                };
                let lt = self.type_of(&l)?;
                let probe = Expr::In(
                    Box::new(Expr::Typed(Box::new((Value::Null, lt)))),
                    vec![Expr::Typed(Box::new((Value::Null, inner_ty.unwrap_or(lt))))],
                    negated,
                );
                key_types.push(lt);
                keys.push((l, Expr::Null));
                self.type_of(&probe)?;
                truth_type(maria, if maria { 21 } else { 1 }, true)
            }
        };
        let spec = DepSpec {
            query: Rc::new(q.clone()),
            names,
            key_types,
            kind,
            ty,
            inner_ty,
            catalog,
            storage: self.storage.clone(),
            session: self.session.clone(),
            options: self.options.clone(),
            memory: self.memory.clone(),
            ctes: self.ctes.clone(),
            parent: self.bound_outer.clone(),
        };
        let Some(applies) = x.applies.as_deref_mut() else {
            return Err(unsupported("a subquery that reads its outer query here"));
        };
        applies.push(Apply {
            rel: None,
            keys,
            filters: Vec::new(),
            aggs: Vec::new(),
            grouped: false,
            item: Expr::Null,
            first_by: Vec::new(),
            ty,
            id: planned.id,
            dep: Some(Rc::new(spec)),
        });
        Ok(slot_ref(SUBQ_BASE + applies.len() - 1, "subquery", &ty))
    }

    /// `JSON_ARRAYAGG(x)` and `JSON_OBJECTAGG(k, v)` as a `GROUP_CONCAT`
    /// of each row's JSON text: MariaDB's text as it builds it (`[1,null]`,
    /// `{"a":1, "a":2}`, rows with a `NULL` key left out, `DISTINCT` and
    /// `ORDER BY` allowed); on MySQL the document read back, so it prints
    /// and keeps duplicate keys as MySQL does (`[1, null]`, last key wins,
    /// 3158 for a `NULL` key). Like MariaDB's own, the text is bounded by
    /// `group_concat_max_len`.
    fn json_agg(
        &mut self,
        name: &str,
        args: &[ast::FunctionArg],
        distinct: bool,
        clauses: &[ast::FunctionArgumentClause],
    ) -> Result<ast::Expr> {
        let maria = self.maria();
        let mut texts = Vec::with_capacity(args.len());
        for a in args {
            match a {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                    texts.push(e.to_string());
                }
                _ => return Err(unsupported(&format!("{name} with these arguments"))),
            }
        }
        let object = name == "JSON_OBJECTAGG";
        if texts.len() != if object { 2 } else { 1 } {
            return Err(SqlError::new(
                1582,
                "42000",
                format!(
                    "Incorrect parameter count in the call to native function '{}'",
                    name.to_ascii_lowercase()
                ),
            ));
        }
        let mut order = String::new();
        for c in clauses {
            match c {
                ast::FunctionArgumentClause::OrderBy(keys) if maria && !object => {
                    let keys: Vec<String> = keys.iter().map(ToString::to_string).collect();
                    order = format!(" ORDER BY {}", keys.join(", "));
                }
                ast::FunctionArgumentClause::Limit(_) | ast::FunctionArgumentClause::OrderBy(_)
                    if !maria =>
                {
                    return Err(syntax_error());
                }
                _ => return Err(unsupported(&format!("{name} with this clause"))),
            }
        }
        if (distinct && (!maria || object)) || texts.iter().any(|t| t.contains('?')) {
            return Err(if distinct && !maria {
                syntax_error()
            } else {
                unsupported(&format!("{name} with these arguments"))
            });
        }
        let inner = |j: &str| format!("SUBSTRING({j}, 2, CHAR_LENGTH({j}) - 2)");
        let d = if distinct { "DISTINCT " } else { "" };
        let sql = match (object, maria) {
            (false, true) => format!(
                "CONCAT('[', GROUP_CONCAT({d}{}{order} SEPARATOR ','), ']')",
                inner(&format!("JSON_ARRAY({})", texts[0]))
            ),
            (false, false) => format!(
                "JSON_EXTRACT(CONCAT('[', GROUP_CONCAT({} SEPARATOR ','), ']'), '$')",
                inner(&format!("JSON_ARRAY({})", texts[0]))
            ),
            (true, true) => format!(
                "CONCAT('{{', GROUP_CONCAT(IF(({k}) IS NULL, NULL, CONCAT(JSON_QUOTE(CONCAT({k})), ':', {v})) SEPARATOR ', '), '}}')",
                k = texts[0],
                v = inner(&format!("JSON_ARRAY({})", texts[1]))
            ),
            (true, false) => format!(
                "JSON_EXTRACT(CONCAT('{{', GROUP_CONCAT({} SEPARATOR ','), '}}'), '$')",
                inner(&format!("JSON_OBJECT({}, {})", texts[0], texts[1]))
            ),
        };
        sqlparser::parser::Parser::new(&crate::dialect::SprigMySql)
            .try_with_sql(&sql)
            .and_then(|mut p| p.parse_expr())
            .map_err(|_| unsupported(name))
    }

    fn aggregate(
        &mut self,
        name: &str,
        kind: AggKind,
        args: &[ast::FunctionArg],
        distinct: bool,
        clauses: &[ast::FunctionArgumentClause],
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        let invalid = || SqlError::new(1111, "HY000", "Invalid use of group function");
        if x.in_agg || x.aggs.is_none() {
            return Err(invalid());
        }
        let mut separator = None;
        let mut order = Vec::new();
        let mut limit = None;
        for c in clauses {
            match c {
                ast::FunctionArgumentClause::Separator(v) if kind == AggKind::GroupConcat => {
                    match &v.value {
                        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                            separator = Some(s.as_bytes().to_vec());
                        }
                        _ => return Err(unsupported("this SEPARATOR")),
                    }
                }
                ast::FunctionArgumentClause::OrderBy(keys) if kind == AggKind::GroupConcat => {
                    x.in_agg = true;
                    let r = keys
                        .iter()
                        .map(|k| {
                            let desc = k.options.sort == Some(ast::OrderBySort::Desc);
                            self.expr_in(&k.expr, x, false).map(|e| (e, desc))
                        })
                        .collect::<Result<Vec<_>>>();
                    x.in_agg = false;
                    order = r?;
                }
                // MariaDB's `LIMIT [offset,] count`; MySQL has none.
                ast::FunctionArgumentClause::Limit(e)
                    if kind == AggKind::GroupConcat && self.maria() =>
                {
                    let n = |e: &ast::Expr| match e {
                        ast::Expr::Value(v) => match &v.value {
                            SqlValue::Number(n, _) => n.parse::<u64>().ok(),
                            _ => None,
                        },
                        _ => None,
                    };
                    limit = Some(
                        n(e).map(|c| (0, c))
                            .ok_or_else(|| unsupported("this GROUP_CONCAT LIMIT"))?,
                    );
                }
                ast::FunctionArgumentClause::Limit(_) if kind == AggKind::GroupConcat => {
                    return Err(syntax_error());
                }
                _ => return Err(unsupported(&format!("{name} with this clause"))),
            }
        }
        let (kind, arg) = match args {
            // `GROUP_CONCAT(a, b, ...)` concatenates its arguments.
            [_, _, ..] if kind == AggKind::GroupConcat => {
                let mut parts = Vec::with_capacity(args.len());
                x.in_agg = true;
                for a in args {
                    let r = match a {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            self.expr_in(e, x, false)
                        }
                        _ => Err(unsupported(&format!("{name} with these arguments"))),
                    };
                    match r {
                        Ok(e) => parts.push(e),
                        Err(e) => {
                            x.in_agg = false;
                            return Err(e);
                        }
                    }
                }
                x.in_agg = false;
                (kind, Some(Expr::Func("CONCAT".into(), parts)))
            }
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)]
                if kind == AggKind::Count =>
            {
                (AggKind::CountStar, None)
            }
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))] => {
                x.in_agg = true;
                let r = self.expr_in(e, x, false);
                x.in_agg = false;
                (kind, Some(r?))
            }
            _ => return Err(unsupported(&format!("{name} with these arguments"))),
        };
        let compiled = match &arg {
            Some(a) => Some(compile(a, &self.session)?),
            None => None,
        };
        let ty = *Aggregate::new(kind, compiled, distinct, &self.session)?.ty();
        let spec = AggSpec {
            kind,
            arg,
            distinct,
            separator,
            order,
            limit,
            ty,
        };
        let Some(aggs) = x.aggs.as_deref_mut() else {
            return Err(invalid());
        };
        let at = aggs
            .iter()
            .position(|a| {
                a.kind == spec.kind
                    && a.arg == spec.arg
                    && a.distinct == spec.distinct
                    && a.separator == spec.separator
                    && a.order == spec.order
                    && a.limit == spec.limit
            })
            .unwrap_or_else(|| {
                aggs.push(spec);
                aggs.len() - 1
            });
        let ty = aggs[at].ty;
        Ok(slot_ref(AGG_BASE + at, name, &ty))
    }

    /// Runs a subquery that doesn't read its outer block. Gives its rows
    /// (at most `limit`, when given) and column types.
    fn run_subquery(
        &mut self,
        q: &ast::Query,
        x: &ExprCx<'_, '_>,
        limit: Option<usize>,
    ) -> Result<(Vec<Row>, Vec<ExprType>)> {
        let planned = self.query(q, Some(x.scope), "SUBQUERY");
        let mut planned = planned?;
        let types = planned.source.types();
        let mut ctx = sprig_exec::Ctx::new(&self.session);
        let mut rows = Vec::new();
        while let Some(row) = planned.source.next(&mut ctx)? {
            rows.push(row);
            if limit.is_some_and(|n| rows.len() >= n) {
                break;
            }
        }
        Ok((rows, types))
    }

    /// `[NOT] EXISTS` where it can't be a semi-join (under `OR`, in the
    /// select list): one truth value when the subquery doesn't read the
    /// outer block, else a count of the matching rows per outer row.
    fn exists(&mut self, q: &ast::Query, negated: bool, x: &mut ExprCx<'_, '_>) -> Result<Expr> {
        let refs = self.outer_refs;
        match self.exists_shapes(q, negated, x) {
            Err(e) if e.code == 1235 && x.applies.is_some() && !x.in_agg => {
                self.outer_refs = refs;
                self.dependent(q, DepKind::Exists { negated }, None, x)
            }
            r => r,
        }
    }

    fn exists_shapes(
        &mut self,
        q: &ast::Query,
        negated: bool,
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        if let ast::SetExpr::Select(select) = &*q.body {
            if q.with.is_none() && q.fetch.is_none() && x.applies.is_some() && !x.in_agg {
                let id = self.next_id();
                let mut block = self.select_block(
                    select,
                    None,
                    q.limit_clause.as_ref(),
                    Some(x.scope),
                    true,
                    id,
                )?;
                if block.correlated.is_empty() {
                    block.items.clear();
                    block.order_by.clear();
                    let mut planned = self.plan_block(block, "SUBQUERY")?;
                    let mut ctx = sprig_exec::Ctx::new(&self.session);
                    let any = planned.source.next(&mut ctx)?.is_some();
                    return Ok(Expr::Bool(any != negated));
                }
                let simple = block.rels.len() == 1
                    && matches!(block.rels[0].kind, RelKind::Table(_))
                    && block.rels[0].outer_on.is_none()
                    && block.semis.is_empty()
                    && block.applies.is_empty()
                    && !block.grouped
                    && block.limit.is_none_or(|l| l > 0)
                    && block.offset == 0;
                let Some(applies) = x.applies.as_deref_mut().filter(|_| simple) else {
                    return Err(unsupported(
                        "EXISTS of this form that reads its outer query here",
                    ));
                };
                let count = ExprType::bigint(false);
                applies.push(Apply {
                    rel: Some(block.rels.remove(0)),
                    keys: block.correlated,
                    filters: block.filters,
                    aggs: vec![AggSpec {
                        kind: AggKind::CountStar,
                        arg: None,
                        distinct: false,
                        separator: None,
                        order: Vec::new(),
                        limit: None,
                        ty: count,
                    }],
                    grouped: true,
                    item: Expr::binary(
                        if negated { BinOp::Eq } else { BinOp::Gt },
                        slot_ref(AGG_BASE, "count", &count),
                        Expr::int(0),
                    ),
                    first_by: Vec::new(),
                    ty: ExprType::boolean(),
                    id,
                    dep: None,
                });
                return Ok(slot_ref(
                    SUBQ_BASE + applies.len() - 1,
                    "exists",
                    &ExprType::boolean(),
                ));
            }
        }
        let rows = self.run_subquery(q, x, Some(1))?;
        Ok(Expr::Bool(rows.0.is_empty() == negated))
    }

    fn scalar_subquery(&mut self, q: &ast::Query, x: &mut ExprCx<'_, '_>) -> Result<Expr> {
        let refs = self.outer_refs;
        match self.scalar_shapes(q, x) {
            Err(e) if e.code == 1235 && x.applies.is_some() && !x.in_agg => {
                self.outer_refs = refs;
                self.dependent(q, DepKind::Scalar, None, x)
            }
            r => r,
        }
    }

    fn scalar_shapes(&mut self, q: &ast::Query, x: &mut ExprCx<'_, '_>) -> Result<Expr> {
        let one_column = || SqlError::new(1241, "21000", "Operand should contain 1 column(s)");
        let (rows, types) = match &*q.body {
            // One SELECT may be tied to this block by equalities: it is
            // then computed per row of the block.
            ast::SetExpr::Select(select)
                if q.with.is_none() && q.fetch.is_none() && x.applies.is_some() && !x.in_agg =>
            {
                let id = self.next_id();
                let mut block = self.select_block(
                    select,
                    q.order_by.as_ref(),
                    q.limit_clause.as_ref(),
                    Some(x.scope),
                    true,
                    id,
                )?;
                if !block.correlated.is_empty() {
                    let simple = block.rels.len() == 1
                        && matches!(block.rels[0].kind, RelKind::Table(_))
                        && block.rels[0].outer_on.is_none()
                        && block.semis.is_empty()
                        && block.applies.is_empty()
                        && block.group_by.is_empty()
                        && block.having.is_none()
                        && !block.distinct
                        && (block.limit.is_none() || (block.limit == Some(1) && !block.grouped))
                        && block.offset == 0;
                    if !simple {
                        return Err(unsupported(
                            "a scalar subquery of this form that reads its outer query",
                        ));
                    }
                    let first_by = if block.limit == Some(1) {
                        std::mem::take(&mut block.order_by)
                    } else {
                        Vec::new()
                    };
                    let first = block.limit == Some(1);
                    if block.items.len() != 1 {
                        return Err(one_column());
                    }
                    let item = block.items.remove(0).expr;
                    let ty = self.type_of(&item)?.nullable();
                    let Some(applies) = x.applies.as_deref_mut() else {
                        return Err(unsupported("a subquery that reads its outer query here"));
                    };
                    applies.push(Apply {
                        rel: Some(block.rels.remove(0)),
                        keys: block.correlated,
                        filters: block.filters,
                        aggs: block.aggs,
                        grouped: block.grouped,
                        item,
                        first_by: if first && first_by.is_empty() {
                            // LIMIT 1 without an order: any one row; a
                            // constant key keeps the first one read.
                            vec![(Expr::int(0), false)]
                        } else {
                            first_by
                        },
                        ty,
                        id,
                        dep: None,
                    });
                    return Ok(slot_ref(SUBQ_BASE + applies.len() - 1, "subquery", &ty));
                }
                let mut planned = self.plan_block(block, "SUBQUERY")?;
                let types = planned.source.types();
                let mut ctx = sprig_exec::Ctx::new(&self.session);
                let mut rows = Vec::new();
                while let Some(row) = planned.source.next(&mut ctx)? {
                    rows.push(row);
                    if rows.len() >= 2 {
                        break;
                    }
                }
                (rows, types)
            }
            _ => self.run_subquery(q, x, Some(2))?,
        };
        if types.len() != 1 {
            return Err(one_column());
        }
        match rows.as_slice() {
            [] => Ok(Expr::Null),
            [row] => Ok(literal(&row[0], &types[0])),
            _ => Err(SqlError::new(
                1242,
                "21000",
                "Subquery returns more than 1 row",
            )),
        }
    }

    fn in_subquery(
        &mut self,
        a: Box<Expr>,
        q: &ast::Query,
        negated: bool,
        x: &mut ExprCx<'_, '_>,
    ) -> Result<Expr> {
        if q.limit_clause.is_some() || q.fetch.is_some() {
            let server = if self.maria() { "MariaDB" } else { "MySQL" };
            return Err(SqlError::new(
                1235,
                "42000",
                format!(
                    "This version of {server} doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'"
                ),
            ));
        }
        let refs = self.outer_refs;
        let (rows, types) = match self.run_subquery(q, x, None) {
            Ok(r) => r,
            Err(e) if e.code == 1235 && x.applies.is_some() && !x.in_agg => {
                self.outer_refs = refs;
                return self.dependent(q, DepKind::In { negated }, Some(*a), x);
            }
            Err(e) => return Err(e),
        };
        if types.len() != 1 {
            return Err(SqlError::new(
                1241,
                "21000",
                "Operand should contain 1 column(s)",
            ));
        }
        if rows.is_empty() {
            return Ok(Expr::Bool(negated));
        }
        let list = rows.iter().map(|r| literal(&r[0], &types[0])).collect();
        Ok(Expr::In(a, list, negated))
    }
}

/// A reference to column `i` of a relation whose row starts at `base`.
/// MySQL's syntax error, for an interval or a unit where the grammar has
/// none.
fn syntax_error() -> SqlError {
    SqlError::new(
        1064,
        "42000",
        "You have an error in your SQL syntax; check the manual that corresponds to your server version for the right syntax to use",
    )
}

/// The units `TIMESTAMPADD` and `TIMESTAMPDIFF` take.
const TIMESTAMP_UNITS: &[&str] = &[
    "MICROSECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
];

/// The interval unit `f` names, as the keyword argument the date and time
/// functions take; 1064 for a field neither server has.
fn unit_keyword(f: &ast::DateTimeField) -> Result<Expr> {
    use ast::DateTimeField as F;
    let name = match f {
        F::Year => "YEAR",
        F::Month => "MONTH",
        F::Week(None) => "WEEK",
        F::Day => "DAY",
        F::Hour => "HOUR",
        F::Minute => "MINUTE",
        F::Second => "SECOND",
        F::Microsecond => "MICROSECOND",
        F::Quarter => "QUARTER",
        F::Custom(i) if i.quote_style.is_none() && crate::dialect::compound_unit(&i.value) => {
            return Ok(Expr::Keyword(i.value.to_ascii_uppercase()));
        }
        _ => return Err(syntax_error()),
    };
    Ok(Expr::Keyword(name.into()))
}

pub(crate) fn table_col(base: usize, i: usize, c: &crate::catalog::ColumnDef) -> Expr {
    Expr::Column(ColumnRef {
        slot: base + i,
        ty: c.ty,
        nullable: c.nullable,
        name: c.name.clone(),
        mb3: c.mb3,
    })
}

/// The named columns of a derived table's rows.
/// The name of user variable `@v` (lower case, unquoted) when `e` is one.
fn user_var(e: &ast::Expr) -> Option<String> {
    let ast::Expr::Identifier(i) = e else {
        return None;
    };
    let n = i.value.strip_prefix('@')?;
    if n.starts_with('@') || n.is_empty() {
        return None;
    }
    let n = match n.as_bytes()[0] {
        q @ (b'`' | b'\'' | b'"') if n.len() >= 2 && n.as_bytes()[n.len() - 1] == q => {
            &n[1..n.len() - 1]
        }
        _ => n,
    };
    Some(n.to_ascii_lowercase())
}

pub(crate) fn derived_cols(base: usize, names: &[String], types: &[ExprType]) -> Vec<RelCol> {
    names
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (name, ty))| RelCol {
            name: name.clone(),
            expr: Expr::Column(ColumnRef {
                slot: base + i,
                ty: slot_type(ty),
                nullable: ty.nullable,
                name: name.clone(),
                mb3: ty.mb3,
            }),
        })
        .collect()
}
