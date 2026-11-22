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
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::ops::{AggKind, Aggregate, Row, RowSource};
use sprig_exec::{BinOp, CastTarget, ColumnRef, Expr, ExprType, Result, SqlError, Truth, compile};
use sqlparser::ast::{self, BinaryOperator, UnaryOperator, Value as SqlValue};

use crate::catalog::TableDef;
use crate::exprs::{self, slot_ref, slot_type};
use crate::plan::Cx;

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
    /// The subquery's table.
    pub(crate) rel: Rel,
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
        _ => return None,
    })
}

const OTHER_AGGREGATES: &[&str] = &[
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
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
];

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
            Value::Str(_) => ExprType::string(
                0,
                self.session.collation_connection,
                sprig_exec::types::Derivation::Coercible,
            ),
            Value::Decimal(d) => ExprType::decimal(65, u32::from(d.scale()), false),
            _ => ExprType::datetime(6),
        };
        Ok(literal(v, &ty))
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
                while let Some(o) = outer {
                    if let Some(e) = o.unqualified(name, x.clause, maria)? {
                        self.outer_refs += 1;
                        return Ok(to_outer(&e));
                    }
                    outer = o.outer;
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
        let mut outer = x.scope.outer;
        while let Some(o) = outer {
            if let Some(e) = o.qualified(schema, table, name) {
                self.outer_refs += 1;
                return Ok(to_outer(&e));
            }
            outer = o.outer;
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
            } if matches!((&**left, &**right), (A::Tuple(_), A::Tuple(_))) => {
                // A row comparison, column by column.
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
            A::Collate { expr, collation } => {
                let name = object_name(collation)?.join(".");
                let coll =
                    Collation::resolve(&name, self.session.persona.flavor()).ok_or_else(|| {
                        SqlError::new(1273, "HY000", format!("Unknown collation: '{name}'"))
                    })?;
                Expr::Collate(self.boxed(expr, x)?, coll)
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
        if f.over.is_some() {
            return Err(unsupported("window functions"));
        }
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
        if OTHER_AGGREGATES.contains(&name.as_str()) {
            return Err(unsupported(&name));
        }
        if let Some(kind) = agg_kind(&name) {
            return self.aggregate(&name, kind, args, distinct, clauses, x);
        }
        if distinct || !clauses.is_empty() {
            return Err(unsupported(&format!("{name} with these arguments")));
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
                _ => return Err(unsupported(&format!("{name} with this clause"))),
            }
        }
        let (kind, arg) = match args {
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
                    rel: block.rels.remove(0),
                    keys: block.correlated,
                    filters: block.filters,
                    aggs: vec![AggSpec {
                        kind: AggKind::CountStar,
                        arg: None,
                        distinct: false,
                        separator: None,
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
                        rel: block.rels.remove(0),
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
        x: &ExprCx<'_, '_>,
    ) -> Result<Expr> {
        let (rows, types) = self.run_subquery(q, x, None)?;
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
pub(crate) fn table_col(base: usize, i: usize, name: &str, ty: ColumnType, nullable: bool) -> Expr {
    Expr::Column(ColumnRef {
        slot: base + i,
        ty,
        nullable,
        name: name.to_owned(),
    })
}

/// The named columns of a derived table's rows.
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
            }),
        })
        .collect()
}
