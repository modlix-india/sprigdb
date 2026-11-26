//! Common table expressions in scope, and recursive ones (`WITH
//! RECURSIVE`).
//!
//! A common table expression is kept with the place it was defined at:
//! its query sees the expressions defined before it in its `WITH` (and,
//! under `WITH RECURSIVE`, itself), never later ones, so a name that is
//! not visible reads as a stored table.
//!
//! A recursive expression is a `UNION [ALL | DISTINCT]` of query blocks
//! that don't read it (the anchors) and blocks that read it once in their
//! `FROM` (the recursive blocks). Its rows are made when they are first
//! read:
//!
//! - the anchors' rows, converted to the anchors' merged column types
//!   (every column nullable), are the first rows;
//! - each recursive block is then planned again for each pass, with its
//!   reference to the expression bound to the rows the previous pass made
//!   (held in memory), and run; its rows are stored into the columns as
//!   into a temporary table's ([`sprig_exec::ops::CteRows`]);
//! - it ends when a pass makes no new row.
//!
//! Each server's rules apply. MySQL: anchors first, a recursive block
//! reads the expression once and only in its own `FROM`, no aggregate,
//! window function, `GROUP BY` or `DISTINCT` in it, and pass `n` beyond
//! `cte_max_recursion_depth` fails with 3636; a `LIMIT` on the whole
//! expression stops the passes once it has its rows. MariaDB: anchors in
//! any place, the expression read once (also inside a derived table of
//! the block's `FROM`, never on the inner side of an outer join or in an
//! expression's subquery), no aggregate or window function, no `ORDER BY`
//! or `LIMIT` on the whole, and after `max_recursive_iterations` passes the
//! result is cut with warning 1931. With more than one recursive block,
//! MariaDB runs every block on the previous pass's rows; MySQL lets each
//! block read on through the rows made so far, its own included.
//!
//! The rows are held in the statement's memory account: past its limit the
//! statement fails with 1114 (nothing is written to disk).

use std::cell::{Cell, RefCell};
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;

use sprig_engine::row::Value;
use sprig_exec::ops::{CteRows, Memory, Row, RowSource};
use sprig_exec::{Ctx, ExprType, Result, Session, SqlError};
use sqlparser::ast;

use crate::access::Storage;
use crate::catalog::Catalog;
use crate::dependent::BoundOuter;
use crate::plan::{Cx, PlanOptions};
use crate::resolve::{Derived, Rel, RelKind, derived_cols, object_name, unsupported};
use crate::sources::Fixed;

/// A common table expression in scope.
pub(crate) struct CteDef {
    pub(crate) cte: ast::Cte,
    /// How many entries of the planner's list of expressions its query
    /// sees: the ones before it (itself too when it is recursive).
    pub(crate) sees: usize,
    /// It reads itself (under `WITH RECURSIVE`).
    pub(crate) recursive: bool,
    /// A recursive expression's rows once made, shared by every reference
    /// of the statement (when no outer value was bound into them).
    rows: RefCell<Option<Rc<Vec<Row>>>>,
    /// How many references to it the statement's text resolved so far.
    pub(crate) refs: Cell<usize>,
    /// Whether its query gives the same rows each time it runs in one
    /// statement (computed on first use).
    pure: Cell<Option<bool>>,
    /// What the first reference that was planned on its own (not merged)
    /// gave: a later reference is resolved only if it has to be planned.
    pub(crate) shape: RefCell<Option<CteShape>>,
}

/// A common table expression's columns as a derived table, and the
/// `EXPLAIN` number of its query.
#[derive(Clone)]
pub(crate) struct CteShape {
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<ExprType>,
    pub(crate) id: Option<usize>,
}

impl CteDef {
    pub(crate) fn new(cte: ast::Cte, sees: usize, recursive: bool) -> Rc<Self> {
        Rc::new(Self {
            cte,
            sees,
            recursive,
            rows: RefCell::new(None),
            refs: Cell::new(0),
            pure: Cell::new(None),
            shape: RefCell::new(None),
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.cte.alias.name.value
    }

    /// Whether the expression's query gives the same rows each time it
    /// runs within one statement: it calls no function whose value changes
    /// between calls or that has an effect, and reads or sets no user
    /// variable. Such an expression may be computed once for several
    /// references.
    pub(crate) fn pure(&self) -> bool {
        if let Some(p) = self.pure.get() {
            return p;
        }
        let p = crate::pushdown::pure_query(&self.cte.query);
        self.pure.set(Some(p));
        p
    }
}

/// The rows of the previous pass, which a recursive block's reference to
/// its expression reads.
pub(crate) struct Working {
    pub(crate) name: String,
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<ExprType>,
    pub(crate) rows: Rc<Vec<Row>>,
}

/// How many times `node` names the table `name` (a one-part name),
/// anywhere inside it.
fn refs<V: ast::Visit>(node: &V, name: &str) -> usize {
    let mut n = 0;
    let _ = ast::visit_relations(node, |o: &ast::ObjectName| {
        if let Ok(parts) = object_name(o) {
            if let [one] = parts.as_slice() {
                if one.eq_ignore_ascii_case(name) {
                    n += 1;
                }
            }
        }
        ControlFlow::<()>::Continue(())
    });
    n
}

/// Whether a common table expression's query reads itself.
pub(crate) fn reads_itself(cte: &ast::Cte) -> bool {
    refs(&*cte.query, &cte.alias.name.value) > 0
}

/// The references to `name` among a `FROM` clause's own items (nested
/// joins included, derived tables not), and whether one of them is on the
/// inner side of an outer join.
fn from_refs(from: &[ast::TableWithJoins], name: &str, inner: &mut bool) -> usize {
    fn factor(f: &ast::TableFactor, name: &str, optional: bool, inner: &mut bool) -> usize {
        match f {
            ast::TableFactor::Table { name: n, .. } => {
                let hit = object_name(n)
                    .ok()
                    .is_some_and(|p| p.len() == 1 && p[0].eq_ignore_ascii_case(name));
                if hit && optional {
                    *inner = true;
                }
                usize::from(hit)
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => twj(table_with_joins, name, optional, inner),
            _ => 0,
        }
    }
    fn twj(t: &ast::TableWithJoins, name: &str, optional: bool, inner: &mut bool) -> usize {
        let right_joined = t.joins.iter().any(|j| {
            matches!(
                j.join_operator,
                ast::JoinOperator::Right(_) | ast::JoinOperator::RightOuter(_)
            )
        });
        let mut n = factor(&t.relation, name, optional || right_joined, inner);
        for j in &t.joins {
            let opt = matches!(
                j.join_operator,
                ast::JoinOperator::Left(_)
                    | ast::JoinOperator::LeftOuter(_)
                    | ast::JoinOperator::FullOuter(_)
            );
            n += factor(&j.relation, name, optional || opt, inner);
        }
        n
    }
    from.iter().map(|t| twj(t, name, false, inner)).sum()
}

fn mysql_err(code: u16, name: &str) -> SqlError {
    let text = match code {
        3573 => format!("Recursive Common Table Expression '{name}' should contain a UNION"),
        3574 => format!(
            "Recursive Common Table Expression '{name}' should have one or more non-recursive query blocks followed by one or more recursive ones"
        ),
        3575 => format!(
            "Recursive Common Table Expression '{name}' can contain neither aggregation nor window functions in recursive query block"
        ),
        _ => format!(
            "In recursive query block of Recursive Common Table Expression '{name}', the recursive table must be referenced only once, and not in any subquery"
        ),
    };
    SqlError::new(code, "HY000", text)
}

fn no_anchors(name: &str) -> SqlError {
    SqlError::new(
        4005,
        "HY000",
        format!("No anchors for recursive WITH element '{name}'"),
    )
}

fn restrictions(name: &str) -> SqlError {
    SqlError::new(
        4008,
        "HY000",
        format!("Restrictions imposed on recursive definitions are violated for table '{name}'"),
    )
}

fn not_yet(maria: bool, what: &str) -> SqlError {
    let server = if maria { "MariaDB" } else { "MySQL" };
    SqlError::new(
        1235,
        "42000",
        format!("This version of {server} doesn't yet support '{what}'"),
    )
}

/// The columns a common table expression's list names, checked against
/// the query's: MariaDB's 4002, MySQL's 1353.
pub(crate) fn check_column_list(maria: bool, listed: usize, width: usize) -> Result<()> {
    if listed == 0 || listed == width {
        return Ok(());
    }
    Err(if maria {
        SqlError::new(
            4002,
            "HY000",
            "WITH column list and SELECT field list have different column counts",
        )
    } else {
        SqlError::new(
            1353,
            "HY000",
            "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts",
        )
    })
}

/// The parts of a recursive expression, checked.
#[derive(Clone)]
struct Shape {
    anchors: Vec<ast::SetExpr>,
    recursive: Vec<ast::Select>,
    /// `UNION DISTINCT`: a row already made is not made again.
    distinct: bool,
    /// MySQL's `LIMIT` on the whole expression.
    limit: Option<ast::LimitClause>,
}

/// Splits a chain of `UNION`s into its parts, each with whether the
/// `UNION` before it keeps duplicates. Gives `false` when the chain holds
/// another set operation.
fn flatten<'q>(body: &'q ast::SetExpr, out: &mut Vec<(Option<bool>, &'q ast::SetExpr)>) -> bool {
    match body {
        ast::SetExpr::SetOperation {
            left,
            op: ast::SetOperator::Union,
            set_quantifier,
            right,
        } => {
            let ok = flatten(left, out);
            out.push((
                Some(matches!(set_quantifier, ast::SetQuantifier::All)),
                right,
            ));
            ok && !matches!(right.as_ref(), ast::SetExpr::SetOperation { .. })
        }
        ast::SetExpr::SetOperation { .. } => {
            out.push((None, body));
            false
        }
        other => {
            out.push((None, other));
            true
        }
    }
}

fn shape(cte: &ast::Cte, maria: bool) -> Result<Shape> {
    let name = cte.alias.name.value.as_str();
    let q = &*cte.query;
    if q.with.is_some() || q.fetch.is_some() || q.for_clause.is_some() {
        return Err(unsupported("this clause in a recursive WITH element"));
    }
    let mut parts = Vec::new();
    let unions = flatten(&q.body, &mut parts);
    let rec: Vec<bool> = parts.iter().map(|(_, p)| refs(*p, name) > 0).collect();
    let anchors = rec.iter().filter(|r| !**r).count();
    if !unions || parts.len() == 1 {
        if maria {
            if anchors == 0 {
                return Err(no_anchors(name));
            }
            return Err(unsupported(
                "INTERSECT or EXCEPT in a recursive WITH element",
            ));
        }
        return Err(mysql_err(3573, name));
    }
    if anchors == 0 {
        return Err(if maria {
            no_anchors(name)
        } else {
            mysql_err(3574, name)
        });
    }
    if !maria {
        let first_rec = rec.iter().position(|r| *r).unwrap_or(rec.len());
        if rec[first_rec..].iter().any(|r| !*r) {
            return Err(mysql_err(3574, name));
        }
    }
    let ops: Vec<bool> = parts.iter().filter_map(|(op, _)| *op).collect();
    if maria && ops.iter().any(|a| *a) && ops.iter().any(|a| !*a) {
        return Err(not_yet(
            true,
            "mix of ALL and DISTINCT UNION operations in recursive CTE spec",
        ));
    }
    let distinct = parts
        .iter()
        .zip(&rec)
        .any(|((op, _), r)| *r && *op == Some(false));
    if q.order_by.is_some() {
        return Err(not_yet(
            maria,
            if maria {
                "global ORDER_BY/LIMIT in recursive CTE spec"
            } else {
                "ORDER BY over UNION in recursive Common Table Expression"
            },
        ));
    }
    if q.limit_clause.is_some() && maria {
        return Err(not_yet(true, "global ORDER_BY/LIMIT in recursive CTE spec"));
    }
    let mut out = Shape {
        anchors: Vec::new(),
        recursive: Vec::new(),
        distinct,
        limit: q.limit_clause.clone(),
    };
    for ((_, part), r) in parts.iter().zip(&rec) {
        if !*r {
            out.anchors.push((*part).clone());
            continue;
        }
        let select = match part {
            ast::SetExpr::Select(s) => s.as_ref(),
            ast::SetExpr::Query(inner)
                if inner.with.is_none()
                    && inner.order_by.is_none()
                    && inner.limit_clause.is_none()
                    && matches!(inner.body.as_ref(), ast::SetExpr::Select(_)) =>
            {
                let ast::SetExpr::Select(s) = inner.body.as_ref() else {
                    unreachable!("matched above");
                };
                s.as_ref()
            }
            ast::SetExpr::Query(_) if !maria => {
                return Err(not_yet(
                    false,
                    "ORDER BY / LIMIT / SELECT DISTINCT in recursive query block of Common Table Expression",
                ));
            }
            _ => return Err(unsupported("this recursive part of a WITH element")),
        };
        let total = refs(select, name);
        let mut inner = false;
        let top = from_refs(&select.from, name, &mut inner);
        if maria {
            let in_from: usize = select.from.iter().map(|t| refs(t, name)).sum();
            if total != 1 || in_from != 1 || inner {
                return Err(restrictions(name));
            }
        } else {
            if total != 1 || top != 1 {
                return Err(mysql_err(3577, name));
            }
            if select.distinct.is_some() {
                return Err(not_yet(
                    false,
                    "ORDER BY / LIMIT / SELECT DISTINCT in recursive query block of Common Table Expression",
                ));
            }
        }
        out.recursive.push(select.clone());
    }
    Ok(out)
}

/// Everything a recursive expression needs to make its rows while the
/// statement runs.
struct Spec {
    name: String,
    names: Vec<String>,
    types: Vec<ExprType>,
    shape: Shape,
    catalog: Arc<dyn Catalog>,
    storage: Arc<dyn Storage>,
    session: Session,
    params: Vec<Value>,
    options: PlanOptions,
    memory: Arc<Memory>,
    ctes: Vec<Rc<CteDef>>,
    bound: Option<Rc<BoundOuter>>,
    /// Where the rows are kept for the statement's other references.
    keep: Option<Rc<CteDef>>,
}

/// The rows of a recursive expression, made on the first read.
struct RecursiveSource {
    spec: Rc<Spec>,
    rows: Option<Rc<Vec<Row>>>,
    pos: usize,
}

impl RowSource for RecursiveSource {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if self.rows.is_none() {
            let kept = self
                .spec
                .keep
                .as_ref()
                .and_then(|d| d.rows.borrow().clone());
            let rows = match kept {
                Some(r) => r,
                None => {
                    let r = Rc::new(make_rows(&self.spec, ctx)?);
                    if let Some(d) = &self.spec.keep {
                        *d.rows.borrow_mut() = Some(r.clone());
                    }
                    r
                }
            };
            self.rows = Some(rows);
        }
        let row = self.rows.as_ref().and_then(|r| r.get(self.pos).cloned());
        self.pos += 1;
        Ok(row)
    }

    fn types(&self) -> Vec<ExprType> {
        self.spec.types.clone()
    }
}

/// A pass's planner: its own memory account for the pass's operators (the
/// rows made so far are charged to the statement's).
fn pass_cx<'s>(spec: &'s Spec) -> Cx<'s> {
    let memory = match spec.options.memory_limit {
        Some(l) => Memory::with_limit(l),
        None => Memory::unlimited(),
    };
    Cx::detached(
        &*spec.catalog,
        spec.storage.clone(),
        spec.session.clone(),
        &spec.params,
        spec.options.clone(),
        memory,
        spec.ctes.clone(),
        spec.bound.clone(),
    )
}

fn make_rows(spec: &Spec, ctx: &mut Ctx<'_>) -> Result<Vec<Row>> {
    let maria = spec.session.persona.is_maria();
    let mut out = CteRows::new(
        &spec.name,
        spec.names.clone(),
        spec.types.clone(),
        !spec.shape.distinct,
        spec.memory.clone(),
    );
    let (limit, offset) = match &spec.shape.limit {
        Some(l) => {
            let mut cx = pass_cx(spec);
            let (lim, off) = cx.limit_clause(l)?;
            (lim, off)
        }
        None => (None, 0),
    };
    let want = limit.map(|l| l.saturating_add(offset));
    let full = |out: &CteRows| want.is_some_and(|w| out.rows.len() as u64 >= w);
    // The anchors.
    for a in &spec.shape.anchors {
        if full(&out) {
            break;
        }
        let mut cx = pass_cx(spec);
        let mut p = cx.set_expr_part(a)?;
        let from = p.source.types();
        while let Some(r) = p.source.next(ctx)? {
            let at = out.rows.len() as u64 + 1;
            out.add(ctx, r, &from, at)?;
            if full(&out) {
                break;
            }
        }
    }
    // Each recursive block over a stretch of the rows made so far.
    let run = |out: &mut CteRows, ctx: &mut Ctx<'_>, b: usize, from: usize, to: usize| {
        let working = Rc::new(Working {
            name: spec.name.clone(),
            names: spec.names.clone(),
            types: spec.types.clone(),
            rows: Rc::new(out.rows[from..to].to_vec()),
        });
        let mut cx = pass_cx(spec);
        cx.working.push(working);
        let mut p = cx.recursive_part(&spec.shape.recursive[b])?;
        let types = p.source.types();
        let mut made = 0u64;
        while let Some(r) = p.source.next(ctx)? {
            made += 1;
            let at = if maria {
                out.rows.len() as u64 + 1
            } else {
                made
            };
            out.add(ctx, r, &types, at)?;
            if full(out) {
                break;
            }
        }
        Ok::<(), SqlError>(())
    };
    let limit_passes = spec.options.max_recursion;
    if maria {
        let (mut start, mut passes) = (0usize, 0u64);
        loop {
            let end = out.rows.len();
            if start == end || full(&out) {
                break;
            }
            if passes >= limit_passes {
                ctx.warn(
                    1931,
                    format!(
                        "Query execution was interrupted. The query exceeded max_recursive_iterations = {limit_passes}. The query result may be incomplete"
                    ),
                );
                break;
            }
            passes += 1;
            for b in 0..spec.shape.recursive.len() {
                run(&mut out, ctx, b, start, end)?;
            }
            start = end;
        }
    } else {
        // Each block reads on from where it stopped; a row is one pass
        // deeper than the deepest row it was made from.
        let blocks = spec.shape.recursive.len();
        let mut pos = vec![0usize; blocks];
        let mut depth: Vec<u64> = vec![0; out.rows.len()];
        loop {
            let mut any = false;
            for b in 0..blocks {
                while pos[b] < out.rows.len() && !full(&out) {
                    let (from, to) = (pos[b], out.rows.len());
                    let pass = depth[from..to].iter().copied().max().unwrap_or(0) + 1;
                    if pass > limit_passes {
                        return Err(SqlError::new(
                            3636,
                            "HY000",
                            format!(
                                "Recursive query aborted after {pass} iterations. Try increasing @@cte_max_recursion_depth to a larger value."
                            ),
                        ));
                    }
                    pos[b] = to;
                    run(&mut out, ctx, b, from, to)?;
                    let made = out.rows.len() - depth.len();
                    depth.extend(std::iter::repeat_n(pass, made));
                    any |= made > 0;
                }
            }
            if !any || full(&out) {
                break;
            }
        }
    }
    let mut rows = out.rows;
    if want.is_some() || offset > 0 {
        let off = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(rows.len());
        rows.drain(..off);
        if let Some(l) = limit {
            rows.truncate(usize::try_from(l).unwrap_or(usize::MAX));
        }
    }
    Ok(rows)
}

impl<'a> Cx<'a> {
    /// The rows of a `FROM` item that names the rows of the previous pass
    /// of a recursive expression being made.
    pub(crate) fn working_rel(&mut self, name: &str, alias: String) -> Option<Rel> {
        let w = self
            .working
            .iter()
            .rev()
            .find(|w| w.name.eq_ignore_ascii_case(name))?
            .clone();
        let n = w.rows.len();
        let derived = Derived::new(
            Box::new(Fixed::new(w.rows.as_ref().clone(), w.types.clone())),
            n as f64,
        );
        let cols = derived_cols(0, &w.names, &w.types);
        let id = self.next_id();
        Some(Rel {
            shown: alias.clone(),
            alias,
            qualified: None,
            width: cols.len(),
            kind: RelKind::Derived(derived),
            cols,
            base: 0,
            outer_on: None,
            outer_dep: None,
            pending: Vec::new(),
            hidden: false,
            derived_id: Some(id),
        })
    }

    /// A non-recursive part of a recursive expression, planned by itself.
    pub(crate) fn set_expr_part(&mut self, part: &ast::SetExpr) -> Result<crate::plan::Planned> {
        self.set_expr_pub(part, "DERIVED")
    }

    /// A recursive block, checked for what the servers refuse in one
    /// (aggregates, window functions; MySQL also `GROUP BY`), and planned.
    pub(crate) fn recursive_part(&mut self, select: &ast::Select) -> Result<crate::plan::Planned> {
        let maria = self.session.persona.is_maria();
        let name = self
            .working
            .last()
            .map(|w| w.name.clone())
            .unwrap_or_default();
        let id = self.next_id();
        let block = self.select_block(select, None, None, None, false, id)?;
        if !block.aggs.is_empty() || !block.windows.is_empty() {
            return Err(if maria {
                restrictions(&name)
            } else {
                mysql_err(3575, &name)
            });
        }
        if !maria && !block.group_by.is_empty() {
            return Err(mysql_err(3575, &name));
        }
        self.plan_block(block, "UNION")
    }

    /// A reference to a recursive expression: checked and typed now (its
    /// parts planned once, against no rows), its rows made when first
    /// read.
    pub(crate) fn recursive_rel(&mut self, def: &Rc<CteDef>, alias: String) -> Result<Rel> {
        let maria = self.session.persona.is_maria();
        let name = def.name().to_owned();
        let shape = shape(&def.cte, maria)?;
        let outer_refs = self.outer_refs;
        // The anchors give the column types and names.
        let mut anchor_types: Vec<Vec<ExprType>> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut first_id = None;
        for a in &shape.anchors {
            let p = self.set_expr_part(a)?;
            if names.is_empty() {
                names = p.names.clone();
                first_id = Some(p.id);
            }
            if !anchor_types.is_empty() && p.types.len() != names.len() {
                return Err(SqlError::new(
                    1222,
                    "21000",
                    "The used SELECT statements have a different number of columns",
                ));
            }
            anchor_types.push(p.types);
        }
        let width = names.len();
        check_column_list(maria, def.cte.alias.columns.len(), width)?;
        for (i, c) in def.cte.alias.columns.iter().enumerate() {
            names[i] = c.name.value.clone();
        }
        for (i, n) in names.iter().enumerate() {
            if names[..i].iter().any(|m| m.eq_ignore_ascii_case(n)) {
                return Err(SqlError::new(
                    1060,
                    "42S21",
                    format!("Duplicate column name '{n}'"),
                ));
            }
        }
        let mut types = if anchor_types.len() == 1 {
            anchor_types.remove(0)
        } else {
            let parts: Vec<Box<dyn RowSource>> = anchor_types
                .into_iter()
                .map(|t| Box::new(Fixed::new(Vec::new(), t)) as Box<dyn RowSource>)
                .collect();
            sprig_exec::ops::Union::new(parts, true, &self.session, Memory::unlimited())?.types()
        };
        for t in &mut types {
            t.nullable = true;
            t.column = false;
        }
        // The recursive blocks, against no rows: names, shapes, types.
        for r in &shape.recursive {
            self.working.push(Rc::new(Working {
                name: name.clone(),
                names: names.clone(),
                types: types.clone(),
                rows: Rc::new(Vec::new()),
            }));
            let p = self.recursive_part(r);
            self.working.pop();
            if p?.types.len() != width {
                return Err(SqlError::new(
                    1222,
                    "21000",
                    "The used SELECT statements have a different number of columns",
                ));
            }
        }
        let catalog = self
            .catalog
            .shared()
            .ok_or_else(|| unsupported("WITH RECURSIVE here"))?;
        // Rows that read no outer value are made once for the statement.
        let shareable = self.bound_outer.is_none() && self.outer_refs == outer_refs;
        let spec = Spec {
            name: name.clone(),
            names: names.clone(),
            types: types.clone(),
            shape,
            catalog,
            storage: self.storage.clone(),
            session: self.session.clone(),
            params: self.params.to_vec(),
            options: self.options.clone(),
            memory: self.memory.clone(),
            ctes: self.ctes[..def.sees.min(self.ctes.len())].to_vec(),
            bound: self.bound_outer.clone(),
            keep: shareable.then(|| def.clone()),
        };
        let source = RecursiveSource {
            spec: Rc::new(spec),
            rows: None,
            pos: 0,
        };
        let derived = Derived::new(Box::new(source), 1000.0);
        let cols = derived_cols(0, &names, &types);
        Ok(Rel {
            shown: alias.clone(),
            alias,
            qualified: None,
            width: cols.len(),
            kind: RelKind::Derived(derived),
            cols,
            base: 0,
            outer_on: None,
            outer_dep: None,
            pending: Vec::new(),
            hidden: false,
            derived_id: first_id,
        })
    }
}
