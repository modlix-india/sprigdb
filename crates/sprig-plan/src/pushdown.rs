//! Moving a block's conditions into the derived tables it reads.
//!
//! A derived table (or a reference to a common table expression) is
//! resolved with its block but planned only when the block is, so that the
//! block's conditions on its columns can go into its query first:
//!
//! - into a `SELECT` (also one with its own `WITH`): all of them when it
//!   doesn't group, those on grouping columns when it does; none past a
//!   `LIMIT`, into a block with window functions or `WITH ROLLUP`, or
//!   through a column that is computed by a function whose value changes
//!   between calls;
//! - into each `UNION ALL` part whose column compares the same way as the
//!   union's column (other set operations keep their conditions outside);
//! - through a `LEFT JOIN`: a condition becomes a `WHERE` condition of the
//!   part, so on the kept side it restricts that side's rows and keys.
//!
//! The conditions also stay where they were, so moving one never changes
//! the rows; it only lets the part read fewer.
//!
//! Two more steps serve queries that read the same rows several ways (the
//! merged views of inheritance are such queries):
//!
//! - a `NOT EXISTS` / `EXISTS` / `IN` subquery tied by `outer.k = inner.k`
//!   gets the block's `k = value` and `k IN (values)` conditions on
//!   `inner.k` when both sides compare alike;
//! - a common table expression named more than once is computed once per
//!   set of key values pushed into it, every reference with the same
//!   values reading the same rows (both servers also compute such an
//!   expression once).

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sprig_exec::ops::{Row, RowSource, SetKind, SetOp, Union};
use sprig_exec::{BinOp, Ctx, Expr, ExprType, Kind, Result};
use sqlparser::ast;

use crate::exprs;
use crate::plan::{Cx, Planned};
use crate::resolve::{Block, SemiJoin, WIN_BASE, object_name, unsupported};

/// A derived table's query that isn't one `SELECT`, resolved but not yet
/// planned.
pub(crate) enum LazyQuery {
    /// One `SELECT` and the select type `EXPLAIN` shows for it.
    Block(Box<Block>, String),
    /// A set operation (`kind` `None` is `UNION`).
    Set {
        left: Box<LazyQuery>,
        right: Box<LazyQuery>,
        all: bool,
        kind: Option<SetKind>,
    },
    /// `ORDER BY` (columns by position) and `LIMIT` over a set operation.
    Output {
        inner: Box<LazyQuery>,
        order: Vec<(usize, bool)>,
        limit: Option<u64>,
        offset: u64,
    },
}

/// What a resolved query gives: the query, its column names and types, and
/// the `EXPLAIN` number of its first block.
type Resolved = (LazyQuery, Vec<String>, Vec<ExprType>, usize);

/// Functions that give another value on each call, or have an effect.
const UNSTABLE: &[&str] = &[
    "RAND",
    "UUID",
    "UUID_SHORT",
    "SYSDATE",
    "RANDOM_BYTES",
    "NEXTVAL",
    "LASTVAL",
    "SETVAL",
    "SPRIG_SEQUENCE_STATE",
    "SPRIG_USER_VAR",
    "SPRIG_SET_USER_VAR",
    "SLEEP",
    "BENCHMARK",
    "GET_LOCK",
    "RELEASE_LOCK",
    "RELEASE_ALL_LOCKS",
    "IS_FREE_LOCK",
    "IS_USED_LOCK",
    "LAST_INSERT_ID",
    "ROW_COUNT",
    "FOUND_ROWS",
];

/// Whether `e` calls a function that gives another value on each call or
/// has an effect (such an expression is never moved or computed fewer
/// times).
pub(crate) fn unstable(e: &Expr) -> bool {
    if let Expr::Func(name, _) = e
        && UNSTABLE.contains(&name.as_str())
    {
        return true;
    }
    let mut any = false;
    exprs::for_children(e, &mut |x| any |= unstable(x));
    any
}

/// Whether a query, as written, gives the same rows each time it runs in
/// one statement: no function of [`UNSTABLE`], no user variable.
pub(crate) fn pure_query(q: &ast::Query) -> bool {
    let flow = ast::visit_expressions(q, |e: &ast::Expr| {
        let bad = match e {
            ast::Expr::Function(f) => object_name(&f.name).ok().is_some_and(|p| {
                p.last()
                    .is_some_and(|n| UNSTABLE.contains(&n.to_ascii_uppercase().as_str()))
            }),
            ast::Expr::Identifier(i) => i.value.starts_with('@'),
            ast::Expr::BinaryOp {
                op: ast::BinaryOperator::Assignment,
                ..
            } => true,
            _ => false,
        };
        if bad {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    flow.is_continue()
}

/// Whether values of types `a` and `b` compare the same way: a condition
/// over one can be checked on the other.
fn same_class(a: &ExprType, b: &ExprType) -> bool {
    a.kind == b.kind
        && a.unsigned == b.unsigned
        && a.coll == b.coll
        && a.json == b.json
        && a.hex == b.hex
}

/// Whether `col = value` and `col IN (value)` mean the same and an
/// equality between two columns of the type carries it over: the value is
/// a number for a number column, a string in the column's collation for a
/// string one, or a value of the column's own temporal kind.
fn key_compatible(col: &ExprType, k: &ExprType) -> bool {
    if k.hex || col.hex || col.json || k.json {
        return false;
    }
    match (col.kind, k.kind) {
        (Kind::Int | Kind::Decimal, Kind::Int | Kind::Decimal) => true,
        (Kind::Str, Kind::Str) => col.coll == k.coll,
        (Kind::Null | Kind::Str, _) | (_, Kind::Null | Kind::Str) => false,
        (a, b) => a == b,
    }
}

/// A source with types and no rows: a set operation's merged types are
/// worked out on it before its parts are planned.
struct Shape(Vec<ExprType>);

impl RowSource for Shape {
    fn next(&mut self, _ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        self.0.clone()
    }
}

/// Whether a derived table's query can be resolved now and planned later:
/// set operations of `SELECT`s and parenthesized queries, with `ORDER BY`
/// and `LIMIT` (`VALUES` lists and other forms are planned at once).
fn lazy_shape(q: &ast::Query) -> bool {
    fn body(b: &ast::SetExpr) -> bool {
        match b {
            ast::SetExpr::Select(_) => true,
            ast::SetExpr::Query(q) => lazy_shape(q),
            ast::SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                matches!(
                    set_quantifier,
                    ast::SetQuantifier::All
                        | ast::SetQuantifier::Distinct
                        | ast::SetQuantifier::None
                ) && matches!(
                    op,
                    ast::SetOperator::Union
                        | ast::SetOperator::Intersect
                        | ast::SetOperator::Except
                ) && body(left)
                    && body(right)
            }
            _ => false,
        }
    }
    q.fetch.is_none() && q.for_clause.is_none() && q.pipe_operators.is_empty() && body(&q.body)
}

impl Cx<'_> {
    /// A derived table's query that isn't one `SELECT`, resolved now and
    /// planned with the block that reads it; `None` when it is planned at
    /// once instead.
    pub(crate) fn lazy_derived(&mut self, q: &ast::Query) -> Result<Option<Resolved>> {
        if !lazy_shape(q) {
            return Ok(None);
        }
        self.lazy_query(q, "DERIVED").map(Some)
    }

    fn lazy_query(&mut self, q: &ast::Query, select_type: &str) -> Result<Resolved> {
        let mark = self.open_with(q.with.as_ref())?;
        let out = self.lazy_body(q, select_type);
        self.ctes.truncate(mark);
        out
    }

    fn lazy_body(&mut self, q: &ast::Query, select_type: &str) -> Result<Resolved> {
        if let ast::SetExpr::Select(select) = &*q.body {
            let id = self.next_id();
            let block = self.select_block(
                select,
                q.order_by.as_ref(),
                q.limit_clause.as_ref(),
                None,
                false,
                id,
            )?;
            return self.lazy_block(block, select_type, id);
        }
        let (query, names, types, id) = self.lazy_set(&q.body, select_type)?;
        if q.order_by.is_none() && q.limit_clause.is_none() {
            return Ok((query, names, types, id));
        }
        let order = self.output_order(&names, q.order_by.as_ref())?;
        let (limit, offset) = match &q.limit_clause {
            Some(l) => self.limit_clause(l)?,
            None => (None, 0),
        };
        Ok((
            LazyQuery::Output {
                inner: Box::new(query),
                order,
                limit,
                offset,
            },
            names,
            types,
            id,
        ))
    }

    fn lazy_block(&mut self, block: Block, select_type: &str, id: usize) -> Result<Resolved> {
        let names = block.items.iter().map(|i| i.name.clone()).collect();
        let types = block
            .items
            .iter()
            .map(|i| self.type_of(&i.expr))
            .collect::<Result<Vec<_>>>()?;
        Ok((
            LazyQuery::Block(Box::new(block), select_type.to_owned()),
            names,
            types,
            id,
        ))
    }

    fn lazy_set(&mut self, body: &ast::SetExpr, select_type: &str) -> Result<Resolved> {
        match body {
            ast::SetExpr::Select(select) => {
                let id = self.next_id();
                let block = self.select_block(select, None, None, None, false, id)?;
                self.lazy_block(block, select_type, id)
            }
            ast::SetExpr::Query(q) => self.lazy_query(q, select_type),
            ast::SetExpr::SetOperation {
                left,
                op,
                set_quantifier,
                right,
            } => {
                let all = matches!(set_quantifier, ast::SetQuantifier::All);
                let (label, kind) = match op {
                    ast::SetOperator::Intersect => ("INTERSECT", Some(SetKind::Intersect)),
                    ast::SetOperator::Except => ("EXCEPT", Some(SetKind::Except)),
                    _ => ("UNION", None),
                };
                let (l, names, lt, id) = self.lazy_set(left, select_type)?;
                let (r, _, rt, _) = self.lazy_set(right, label)?;
                // The merged types and the errors of merging (1222, 1271)
                // come from the operator itself.
                let types = match kind {
                    None => Union::new(
                        vec![Box::new(Shape(lt)), Box::new(Shape(rt))],
                        all,
                        &self.session,
                        self.memory.clone(),
                    )?
                    .types(),
                    Some(k) => SetOp::new(
                        Box::new(Shape(lt)),
                        Box::new(Shape(rt)),
                        k,
                        all,
                        &self.session,
                        self.memory.clone(),
                    )?
                    .types(),
                };
                Ok((
                    LazyQuery::Set {
                        left: Box::new(l),
                        right: Box::new(r),
                        all,
                        kind,
                    },
                    names,
                    types,
                    id,
                ))
            }
            _ => Err(unsupported("this set operation")),
        }
    }

    /// Plans a resolved query with `push` (conditions over its columns'
    /// positions, whose types are `out`) moved into its parts where that
    /// keeps its rows the same. `part`: the query is a part of a set
    /// operation, so a condition goes in only where the part's column
    /// compares like the operation's.
    pub(crate) fn plan_lazy(
        &mut self,
        query: LazyQuery,
        push: &[Expr],
        out: &[ExprType],
        part: bool,
    ) -> Result<Planned> {
        match query {
            LazyQuery::Block(mut block, select_type) => {
                self.push_into(&mut block, push, part.then_some(out));
                self.plan_block(*block, &select_type)
            }
            LazyQuery::Set {
                left,
                right,
                all,
                kind,
            } => {
                let push: &[Expr] = if all && kind.is_none() { push } else { &[] };
                let l = self.plan_lazy(*left, push, out, true)?;
                let r = self.plan_lazy(*right, push, out, true)?;
                self.combine(l, r, all, kind)
            }
            LazyQuery::Output {
                inner,
                order,
                limit,
                offset,
            } => {
                let push: &[Expr] = if limit.is_none() && offset == 0 {
                    push
                } else {
                    &[]
                };
                let planned = self.plan_lazy(*inner, push, out, part)?;
                self.finish_output(planned, &order, limit, offset)
            }
        }
    }

    /// Adds to `block`'s `WHERE` each condition of `push` (over the
    /// block's select-list positions) that gives the same rows there:
    /// none past a `LIMIT`, into `WITH ROLLUP` or a block with window
    /// functions; through a grouped block only on grouping columns; never
    /// through an aggregate, a window function, a per-row subquery or a
    /// column computed by an unstable function. (A block whose other
    /// columns call `RAND()` still takes the condition: both servers
    /// filter such a block's rows before computing them.) With `out`, a condition
    /// also needs the item's type to compare like `out`'s.
    pub(crate) fn push_into(&self, block: &mut Block, push: &[Expr], out: Option<&[ExprType]>) {
        if push.is_empty()
            || block.limit.is_some()
            || block.offset != 0
            || block.rollup
            || !block.windows.is_empty()
        {
            return;
        }
        for c in push {
            if unstable(c) {
                continue;
            }
            let mut ok = true;
            let pushed = exprs::rewrite(c, &mut |e| match e {
                Expr::Column(col) => {
                    let Some(item) = block.items.get(col.slot).map(|i| &i.expr) else {
                        ok = false;
                        return None;
                    };
                    if (block.grouped && !block.group_by.iter().any(|g| g == item))
                        || exprs::slots(item).iter().any(|s| *s >= WIN_BASE)
                        || unstable(item)
                    {
                        ok = false;
                    }
                    if let Some(out) = out {
                        let same = match (self.type_of(item), out.get(col.slot)) {
                            (Ok(t), Some(o)) => same_class(&t, o),
                            _ => false,
                        };
                        ok &= same;
                    }
                    Some(item.clone())
                }
                _ => None,
            });
            if ok {
                block.filters.push(pushed);
            }
        }
    }

    /// The key values among `conds` (conditions over a derived table's
    /// column positions): per column, the shortest `col = value` or `col
    /// IN (values)` list (values computed, sorted, without repeats), as a
    /// text that identifies them and as conditions. Values must compare
    /// with the column as numbers, strings of its collation or its own
    /// temporal kind.
    pub(crate) fn key_values(&self, conds: &[Expr]) -> (String, Vec<Expr>) {
        let mut best: BTreeMap<usize, (usize, String, Expr)> = BTreeMap::new();
        'conds: for c in conds {
            let (col, list): (&Expr, Vec<&Expr>) = match c {
                Expr::Binary(BinOp::Eq, a, b) => match (&**a, &**b) {
                    (col @ Expr::Column(_), k) | (k, col @ Expr::Column(_))
                        if exprs::is_const(k) =>
                    {
                        (col, vec![k])
                    }
                    _ => continue,
                },
                Expr::In(a, list, false)
                    if matches!(&**a, Expr::Column(_)) && list.iter().all(exprs::is_const) =>
                {
                    (&**a, list.iter().collect())
                }
                _ => continue,
            };
            let Expr::Column(cr) = col else { continue };
            let Ok(col_ty) = self.type_of(col) else {
                continue;
            };
            let mut vals: Vec<(String, Expr)> = Vec::new();
            for k in list {
                let Some((v, ty)) = self.eval_const(k) else {
                    continue 'conds;
                };
                if !key_compatible(&col_ty, &ty) {
                    continue 'conds;
                }
                let fp = format!("{v:?}/{:?}/{:?}/{}", ty.kind, ty.coll, ty.unsigned);
                if !vals.iter().any(|(f, _)| *f == fp) {
                    vals.push((fp, Expr::Typed(Box::new((v, ty)))));
                }
            }
            if vals.is_empty() {
                continue;
            }
            vals.sort_by(|a, b| a.0.cmp(&b.0));
            let n = vals.len();
            let fp = format!(
                "{}:[{}]",
                cr.slot,
                vals.iter()
                    .map(|v| v.0.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let mut list: Vec<Expr> = vals.into_iter().map(|v| v.1).collect();
            let e = if n == 1 {
                Expr::binary(BinOp::Eq, col.clone(), list.remove(0))
            } else {
                Expr::In(Box::new(col.clone()), list, false)
            };
            let better = best
                .get(&cr.slot)
                .is_none_or(|(m, f, _)| (n, &fp) < (*m, f));
            if better {
                best.insert(cr.slot, (n, fp, e));
            }
        }
        let fp = best
            .values()
            .map(|(_, f, _)| f.as_str())
            .collect::<Vec<_>>()
            .join(";");
        (fp, best.into_values().map(|(_, _, e)| e).collect())
    }

    /// Copies the block's `k = value` / `k IN (values)` conditions on an
    /// outer key of `semi` to the subquery's side of the key (when both
    /// sides compare alike), into the subquery's query.
    pub(crate) fn push_semi(&self, semi: &SemiJoin, filters: &[Expr]) {
        let mut conds = Vec::new();
        for (outer_e, col) in &semi.keys {
            let Expr::Column(oc) = outer_e else { continue };
            let Some(inner_ty) = semi.inner.types.get(*col) else {
                continue;
            };
            let Ok(outer_ty) = self.type_of(outer_e) else {
                continue;
            };
            if !same_class(&outer_ty, inner_ty) {
                continue;
            }
            let target = exprs::slot_ref(*col, "k", inner_ty);
            let is_key = |e: &Expr| matches!(e, Expr::Column(c) if c.slot == oc.slot);
            let fits = |k: &Expr| {
                exprs::is_const(k)
                    && !unstable(k)
                    && self.type_of(k).is_ok_and(|t| key_compatible(&outer_ty, &t))
            };
            for f in filters {
                match f {
                    Expr::Binary(BinOp::Eq, a, b) => {
                        let k = if is_key(a) {
                            b
                        } else if is_key(b) {
                            a
                        } else {
                            continue;
                        };
                        if fits(k) {
                            conds.push(Expr::binary(BinOp::Eq, target.clone(), (**k).clone()));
                        }
                    }
                    Expr::In(a, list, false) if is_key(a) && list.iter().all(&fits) => {
                        conds.push(Expr::In(Box::new(target.clone()), list.clone(), false));
                    }
                    _ => {}
                }
            }
        }
        if !conds.is_empty() {
            semi.inner
                .with_block(|block| self.push_into(block, &conds, None));
        }
    }
}
