//! Dependent subqueries: a subquery that reads its outer query in a way
//! the shapes the planner joins or probes don't cover (a condition other
//! than an equality, a join, grouping, a set operation, `IN` of a
//! correlated list, ...). Such a subquery is planned again for each
//! distinct combination of the outer values it reads, with those values
//! bound in place of the outer names, and run; its value (a scalar, an
//! `EXISTS` truth value, an `IN` result) is added to the outer row as a
//! last column, as for the other per-row subqueries.
//!
//! - At plan time the subquery is resolved once with the outer names
//!   recorded ([`Recording`]): each reads as a typed `NULL`, so the
//!   resolution checks names, gives the value's type and fills `EXPLAIN`.
//! - At run time, for each new combination of outer values, the same
//!   statement text is planned with a [`BoundOuter`] that answers the
//!   recorded names with the values (typed as the outer columns), so the
//!   subquery's own keys and ranges see constants.
//! - The value is remembered per combination, as the servers' subquery
//!   cache does.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::rc::Rc;
use std::sync::Arc;

use sprig_engine::row::Value;
use sprig_exec::ops::{Memory, Row, RowSource};
use sprig_exec::{Compiled, Ctx, Expr, ExprType, Result, Session, SqlError, compile};
use sqlparser::ast;

use crate::access::Storage;
use crate::catalog::Catalog;
use crate::plan::{Cx, PlanOptions};

/// Outer names bound to values, for a subquery planned per outer row.
#[derive(Debug, Default)]
pub(crate) struct BoundOuter {
    /// Each name as written (its parts, lower-case) and its value.
    pub(crate) names: Vec<(Vec<String>, Expr)>,
    /// The bindings of the query around the outer one, when that is a
    /// dependent subquery too.
    pub(crate) parent: Option<Rc<BoundOuter>>,
}

impl BoundOuter {
    /// The value bound to the name `parts`.
    pub(crate) fn find(&self, parts: &[&str]) -> Option<Expr> {
        let hit = self.names.iter().find(|(n, _)| {
            n.len() == parts.len() && n.iter().zip(parts).all(|(a, b)| a.eq_ignore_ascii_case(b))
        });
        match hit {
            Some((_, e)) => Some(e.clone()),
            None => self.parent.as_ref().and_then(|p| p.find(parts)),
        }
    }
}

/// The outer names a subquery being resolved for the dependent path reads.
pub(crate) struct Recording {
    /// The scope of the block the subquery is in (by address).
    pub(crate) boundary: *const (),
    /// Each name as written (lower-case parts) and the outer block's
    /// expression for it.
    pub(crate) uses: Vec<(Vec<String>, Expr)>,
}

/// What a dependent subquery gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DepKind {
    /// Its one value (`NULL` without rows; 1242 with more than one).
    Scalar,
    /// `[NOT] EXISTS`.
    Exists { negated: bool },
    /// `lhs [NOT] IN (subquery)`: the last key is `lhs`.
    In { negated: bool },
}

/// Everything a dependent subquery needs to be planned again at run time.
pub(crate) struct DepSpec {
    pub(crate) query: Rc<ast::Query>,
    /// The recorded outer names (lower-case parts), in key order.
    pub(crate) names: Vec<Vec<String>>,
    /// The static type of each key's value.
    pub(crate) key_types: Vec<ExprType>,
    pub(crate) kind: DepKind,
    /// The type of the value added to the row.
    pub(crate) ty: ExprType,
    /// The column type of the subquery's one column (for `IN`).
    pub(crate) inner_ty: Option<ExprType>,
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) session: Session,
    pub(crate) options: PlanOptions,
    pub(crate) memory: Arc<Memory>,
    pub(crate) ctes: Vec<Rc<ast::Cte>>,
    pub(crate) parent: Option<Rc<BoundOuter>>,
}

/// The row source: each outer row with the subquery's value appended.
pub(crate) struct DependentOp {
    pub(crate) outer: Box<dyn RowSource>,
    /// The outer values, over the outer row (for `IN`, `lhs` last).
    pub(crate) keys: Vec<Compiled>,
    pub(crate) spec: Rc<DepSpec>,
    pub(crate) cache: HashMap<String, Value>,
}

impl DependentOp {
    fn value(&self, ctx: &mut Ctx<'_>, keys: &[Value]) -> Result<Value> {
        let spec = &*self.spec;
        let bound = Rc::new(BoundOuter {
            names: spec
                .names
                .iter()
                .zip(keys)
                .zip(&spec.key_types)
                .map(|((n, v), t)| (n.clone(), Expr::Typed(Box::new((v.clone(), *t)))))
                .collect(),
            parent: spec.parent.clone(),
        });
        let mut cx = Cx::runtime(
            &*spec.catalog,
            spec.storage.clone(),
            spec.session.clone(),
            spec.options.clone(),
            spec.memory.clone(),
            spec.ctes.clone(),
            bound,
        );
        let mut planned = cx.query(&spec.query, None, "DEPENDENT SUBQUERY")?;
        let source = &mut planned.source;
        match spec.kind {
            DepKind::Exists { negated } => {
                let any = source.next(ctx)?.is_some();
                Ok(Value::Int(i64::from(any != negated)))
            }
            DepKind::Scalar => {
                let first: Option<Row> = source.next(ctx)?;
                if first.is_some() && source.next(ctx)?.is_some() {
                    return Err(SqlError::new(
                        1242,
                        "21000",
                        "Subquery returns more than 1 row",
                    ));
                }
                Ok(first
                    .and_then(|r| r.into_iter().next())
                    .unwrap_or(Value::Null))
            }
            DepKind::In { negated } => {
                let (lhs, lhs_ty) = match (keys.last(), spec.key_types.last()) {
                    (Some(v), Some(t)) => (v.clone(), *t),
                    _ => return Err(SqlError::unsupported("IN without its left side")),
                };
                let inner_ty = spec.inner_ty.unwrap_or(lhs_ty);
                let mut list = Vec::new();
                while let Some(row) = source.next(ctx)? {
                    let v = row.into_iter().next().unwrap_or(Value::Null);
                    list.push(Expr::Typed(Box::new((v, inner_ty))));
                }
                if list.is_empty() {
                    return Ok(Value::Int(i64::from(negated)));
                }
                let e = Expr::In(
                    Box::new(Expr::Typed(Box::new((lhs, lhs_ty)))),
                    list,
                    negated,
                );
                let c = compile(&e, &spec.session)?;
                c.eval(ctx, &[])
            }
        }
    }
}

impl RowSource for DependentOp {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        let Some(mut row) = self.outer.next(ctx)? else {
            return Ok(None);
        };
        let mut vals = Vec::with_capacity(self.keys.len());
        let mut key = String::new();
        for k in &self.keys {
            let v = k.eval(ctx, &row)?;
            let _ = write!(key, "{v:?}|");
            vals.push(v);
        }
        let v = match self.cache.get(&key) {
            Some(v) => v.clone(),
            None => {
                let v = self.value(ctx, &vals)?;
                self.cache.insert(key, v.clone());
                v
            }
        };
        row.push(v);
        Ok(Some(row))
    }

    fn types(&self) -> Vec<ExprType> {
        let mut t = self.outer.types();
        t.push(self.spec.ty);
        t
    }
}
