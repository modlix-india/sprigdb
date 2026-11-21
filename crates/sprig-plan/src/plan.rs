//! The planner: from a resolved query block to a tree of row operators.
//!
//! For each `FROM` item the single-table access paths are costed (a full
//! scan, a range scan per key the conditions restrict, a scan of a key
//! that holds every column read). A join order is then searched over
//! left-deep trees: each item joins the ones before it by key lookup, by a
//! hash join, or by replaying its rows. Grouping, `DISTINCT`, ordering
//! and `LIMIT` are placed on top, and an index that already gives the
//! wanted order saves the sort.
//!
//! Costs are in units of "one row read in key order from the primary
//! key". The constants are this engine's own, set by reasoning about what
//! each step does in a sorted key-value store; they are not measured yet.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;

use sprig_engine::row::Value;
use sprig_exec::ops::{
    AggKind, Aggregate, Aggregation, Distinct, Filter, HashJoin, InnerSource, JoinKind, Limit,
    Memory, NestedLoopJoin, Project, RowSource, Sort, SortKey, Union,
};
use sprig_exec::{
    BinOp, Compiled, Ctx, Expr, ExprType, Persona, Result, Session, SqlError, compile,
};
use sqlparser::ast;

use crate::access::{Counters, Fetch, KeyRange, Storage, key_holds};
use crate::catalog::{Catalog, KeyId, TableDef};
use crate::explain::{ExplainRow, key_len};
use crate::exprs::{self, slot_ref};
use crate::keyval::{key_usable, lookup_exact};
use crate::range::{KeyCtx, RangeSet};
use crate::resolve::{
    AGG_BASE, AggSpec, Apply, Block, Clause, Derived, Rel, RelKind, SUBQ_BASE, Scope, SemiJoin,
    derived_cols, unknown_column, unsupported,
};
use crate::sources::{
    ApplyAgg, ApplyOp, CachedInner, DerivedSource, Fixed, LookupInner, LookupPart, ScanSource,
    table_types,
};

/// Reading the next row of a primary-key scan.
const ROW: f64 = 1.0;
/// Reading the next entry of a secondary index.
const ENTRY: f64 = 0.6;
/// Fetching a row by primary key from a secondary-index entry.
const FETCH: f64 = 2.5;
/// Positioning in a key: a point lookup or the start of a range.
const SEEK: f64 = 4.0;
/// Starting a scan of a whole table or index: what makes a key lookup
/// win on a table of a few rows.
const SCAN_START: f64 = 8.0;
/// Adding a row to a hash table.
const HASH_BUILD: f64 = 1.5;
/// Probing a hash table with a row.
const HASH_PROBE: f64 = 1.0;
/// One comparison of a sort.
const SORT: f64 = 0.35;
/// Grouping a row through a hash table.
const HASH_GROUP: f64 = 0.4;
/// A plan must be cheaper by more than this to replace an earlier one,
/// so the first plan found wins ties and plans are repeatable.
const EPS: f64 = 1e-9;

/// The most tables one block joins.
const MAX_TABLES: usize = 61;
/// Up to this many tables every left-deep order is considered (with
/// pruning); beyond it the cheapest next table is taken at each step.
const EXHAUSTIVE_TABLES: usize = 7;

/// Choices the caller makes for a statement.
#[derive(Debug, Clone)]
pub struct PlanOptions {
    /// `ONLY_FULL_GROUP_BY`: a column that is neither grouped nor
    /// aggregated, nor decided by the grouped columns, is an error.
    pub only_full_group_by: bool,
    /// Whether equi-joins without a usable key may be hash joins.
    pub hash_join: bool,
    /// Whether an index that gives the `ORDER BY` order may replace a
    /// cheaper access path plus a sort.
    pub order_by_index: bool,
    /// Check every condition on the rows read, also where the key range
    /// read already guarantees it. For tests of the range analyzer.
    pub keep_filters: bool,
    /// The most bytes the statement's sorts and hash tables may hold.
    pub memory_limit: Option<usize>,
}

impl PlanOptions {
    /// The server defaults of a persona.
    #[must_use]
    pub fn for_persona(persona: Persona) -> Self {
        Self {
            only_full_group_by: !persona.is_maria(),
            hash_join: true,
            order_by_index: true,
            keep_filters: false,
            memory_limit: None,
        }
    }
}

/// One table access of a plan and the work it did.
#[derive(Debug, Clone)]
pub struct AccessStat {
    /// The query block's number.
    pub id: usize,
    /// The table's alias.
    pub table: String,
    /// Rows, index entries and positionings counted while the plan runs.
    pub counters: Arc<Counters>,
}

/// One result column.
#[derive(Debug, Clone)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// Its static type.
    pub ty: ExprType,
}

/// A planned statement: pull rows from `source`.
pub struct Plan {
    /// The root operator.
    pub source: Box<dyn RowSource>,
    /// The result columns.
    pub columns: Vec<Column>,
    /// The plan as `EXPLAIN` shows it.
    pub explain: Vec<ExplainRow>,
    /// The table accesses, for rows-examined figures after the run.
    pub accesses: Vec<AccessStat>,
}

impl Plan {
    /// Rows examined so far: the rows every table access handed over.
    #[must_use]
    pub fn rows_examined(&self) -> u64 {
        self.accesses.iter().map(|a| a.counters.rows()).sum()
    }
}

/// Plans statements against a catalog and its storage.
pub struct Planner<'a> {
    /// Where tables are found.
    pub catalog: &'a dyn Catalog,
    /// Where their rows are.
    pub storage: Arc<dyn Storage>,
    /// The session the statement runs in.
    pub session: &'a Session,
    /// The planner's options.
    pub options: PlanOptions,
}

impl<'a> Planner<'a> {
    /// A planner with the persona's default options.
    #[must_use]
    pub fn new(catalog: &'a dyn Catalog, storage: Arc<dyn Storage>, session: &'a Session) -> Self {
        Self {
            catalog,
            storage,
            session,
            options: PlanOptions::for_persona(session.persona),
        }
    }

    /// Plans a `SELECT` (or `EXPLAIN SELECT`) with the values of its `?`
    /// parameters.
    ///
    /// # Errors
    /// The errors the servers raise when they prepare the statement
    /// (unknown or ambiguous names, wrong column counts, illegal mixes of
    /// collations), and 1235 for what the planner doesn't cover yet.
    pub fn plan(&self, statement: &ast::Statement, params: &[Value]) -> Result<Plan> {
        let mut statement = statement.clone();
        // Number the parameters in the order they are written.
        let mut n = 0usize;
        let _ = ast::visit_expressions_mut(&mut statement, |e: &mut ast::Expr| {
            if let ast::Expr::Value(v) = e {
                if let ast::Value::Placeholder(p) = &mut v.value {
                    if p == "?" {
                        *p = format!("?{n}");
                        n += 1;
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
        let mut session = self.session.clone();
        session.fold_constants = true;
        let memory = match self.options.memory_limit {
            Some(limit) => Memory::with_limit(limit),
            None => Memory::unlimited(),
        };
        let mut cx = Cx {
            catalog: self.catalog,
            storage: self.storage.clone(),
            session,
            params,
            options: self.options.clone(),
            memory,
            explain: Vec::new(),
            accesses: Vec::new(),
            ids: 0,
            outer_refs: 0,
            ctes: Vec::new(),
        };
        let (query, explain) = match &statement {
            ast::Statement::Query(q) => (q, false),
            ast::Statement::Explain {
                statement, analyze, ..
            } => match &**statement {
                ast::Statement::Query(q) if !analyze => (q, true),
                _ => return Err(unsupported("EXPLAIN of this statement")),
            },
            _ => return Err(unsupported("this statement")),
        };
        let planned = cx.query(query, None, "SIMPLE")?;
        let mut rows = std::mem::take(&mut cx.explain);
        rows.sort_by_key(|r| r.id);
        if rows.iter().any(|r| r.id != 1) {
            for r in &mut rows {
                if r.select_type == "SIMPLE" {
                    r.select_type = "PRIMARY".into();
                }
            }
        }
        if explain {
            let (source, columns) = crate::explain::as_rows(&rows, &cx.session);
            return Ok(Plan {
                source,
                columns,
                explain: rows,
                accesses: Vec::new(),
            });
        }
        let types = planned.types;
        Ok(Plan {
            source: planned.source,
            columns: planned
                .names
                .into_iter()
                .zip(types)
                .map(|(name, ty)| Column { name, ty })
                .collect(),
            explain: rows,
            accesses: cx.accesses,
        })
    }
}

/// A planned query: its operator tree and what it gives.
pub(crate) struct Planned {
    pub(crate) source: Box<dyn RowSource>,
    pub(crate) names: Vec<String>,
    pub(crate) types: Vec<ExprType>,
    pub(crate) est_rows: f64,
    pub(crate) id: usize,
}

/// The state of planning one statement.
pub(crate) struct Cx<'a> {
    pub(crate) catalog: &'a dyn Catalog,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) session: Session,
    pub(crate) params: &'a [Value],
    pub(crate) options: PlanOptions,
    pub(crate) memory: Arc<Memory>,
    pub(crate) explain: Vec<ExplainRow>,
    pub(crate) accesses: Vec<AccessStat>,
    ids: usize,
    /// How many names resolved into an enclosing block so far.
    pub(crate) outer_refs: usize,
    ctes: Vec<Rc<ast::Cte>>,
}

/// One way to read a table by itself.
#[derive(Debug, Clone)]
struct Path {
    key: KeyId,
    ranges: Vec<KeyRange>,
    /// Whether every entry of the key is read.
    full: bool,
    set: Option<RangeSet>,
    /// Rows read.
    rows: f64,
    cost: f64,
    /// Whether the key's entries hold every column the block reads.
    covering: bool,
    /// The local conditions still to check on the rows read.
    kept: Vec<usize>,
}

impl Path {
    fn const_parts(&self) -> usize {
        self.set.as_ref().map_or(0, |s| s.const_parts)
    }

    fn empty(&self) -> bool {
        self.set.as_ref().is_some_and(RangeSet::is_empty)
    }
}

/// What the planner knows about one `FROM` item.
struct RelInfo {
    /// Conditions over this item alone that its own scan may apply.
    local: Vec<Expr>,
    /// The item's access paths, cheapest first at `best`.
    paths: Vec<Path>,
    best: usize,
    /// The item's row count.
    table_rows: f64,
    /// Rows left after the local conditions.
    out_rows: f64,
    /// The table columns the block reads.
    needed: BTreeSet<usize>,
    /// The items that must be read before this one.
    deps: u64,
    /// Keys the conditions could use, for `EXPLAIN`.
    possible: Vec<String>,
}

/// A key lookup driven by earlier tables' rows.
#[derive(Debug, Clone)]
struct Lookup {
    key: KeyId,
    /// Per bound key part: the expression over earlier tables.
    exprs: Vec<Expr>,
    /// Whether a lookup finds at most one row.
    unique: bool,
    /// Whether the rows a lookup finds are exactly those equal to the
    /// values looked up, so the equalities needn't be checked again.
    exact: bool,
    rows: f64,
    covering: bool,
}

/// How an item joins the ones before it.
#[derive(Debug, Clone)]
enum Method {
    /// The first item: read by one of its paths.
    Scan,
    Lookup(Lookup),
    /// Pairs of (expression over earlier items, expression over this one).
    Hash(Vec<(Expr, Expr)>),
    /// Its rows are read once and replayed for every earlier row.
    Replay,
}

#[derive(Debug, Clone)]
struct Step {
    rel: usize,
    method: Method,
    /// Rows after this step.
    rows: f64,
    /// Cost up to and including this step.
    cost: f64,
}

fn mask_of(rels: &[Rel], e: &Expr) -> u64 {
    let mut m = 0u64;
    for s in exprs::slots(e) {
        if let Some(i) = rels
            .iter()
            .position(|r| s >= r.base && s < r.base + r.width)
        {
            m |= 1 << i;
        }
    }
    m
}

/// Whether `e` can't be true on a row where every column of `rel` is
/// `NULL`: such a condition in `WHERE` turns a left join into an inner
/// join. Conservative: `false` when in doubt.
fn rejects_nulls(e: &Expr, rel: &Rel) -> bool {
    /// What is known of an expression on such a row.
    #[derive(PartialEq, Clone, Copy)]
    enum On {
        Null,
        NotTrue,
        Unknown,
    }
    fn walk(e: &Expr, rel: &Rel) -> On {
        let strict = |parts: &[&Expr]| {
            if parts.iter().any(|p| walk(p, rel) == On::Null) {
                On::Null
            } else {
                On::Unknown
            }
        };
        match e {
            Expr::Column(c) if c.slot >= rel.base && c.slot < rel.base + rel.width => On::Null,
            Expr::Binary(BinOp::And, a, b) => {
                let (a, b) = (walk(a, rel), walk(b, rel));
                if a != On::Unknown || b != On::Unknown {
                    On::NotTrue
                } else {
                    On::Unknown
                }
            }
            Expr::Binary(BinOp::Or, a, b) => {
                let (a, b) = (walk(a, rel), walk(b, rel));
                if a != On::Unknown && b != On::Unknown {
                    On::NotTrue
                } else {
                    On::Unknown
                }
            }
            Expr::Binary(BinOp::NullSafeEq, ..) => On::Unknown,
            Expr::Binary(_, a, b) => strict(&[a, b]),
            Expr::Neg(a)
            | Expr::BitNot(a)
            | Expr::Not(a)
            | Expr::Cast(a, _)
            | Expr::Collate(a, _) => strict(&[a]),
            Expr::Like(a, p, _, _) => strict(&[a, p]),
            Expr::Between(a, _, _, _) | Expr::In(a, _, _) => strict(&[a]),
            Expr::IsNull(a, true) => {
                if walk(a, rel) == On::Null {
                    On::NotTrue
                } else {
                    On::Unknown
                }
            }
            _ => On::Unknown,
        }
    }
    walk(e, rel) != On::Unknown
}

/// A rough share of rows a condition keeps, where no key range says.
fn selectivity(e: &Expr) -> f64 {
    match e {
        Expr::Binary(BinOp::And, a, b) => selectivity(a) * selectivity(b),
        Expr::Binary(BinOp::Or, a, b) => (selectivity(a) + selectivity(b)).min(1.0),
        Expr::Binary(BinOp::Eq | BinOp::NullSafeEq, ..) => 0.1,
        Expr::Binary(BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge, ..)
        | Expr::Between(_, _, _, false) => 0.33,
        Expr::In(_, list, false) => (0.1 * list.len() as f64).min(0.5),
        Expr::IsNull(_, false) => 0.1,
        _ => 1.0,
    }
}

/// Whether a condition reads a scalar subquery computed per row: it can
/// be checked only after the subquery's value is there.
fn reads_subquery(e: &Expr) -> bool {
    exprs::slots(e)
        .iter()
        .any(|s| (SUBQ_BASE..AGG_BASE).contains(s))
}

/// Whether making the rows of a subquery distinct by their own column
/// type merges exactly the values an equality with `outer` treats as
/// equal, so that a join with the distinct rows is the semi-join.
fn dedup_exact(outer: &ExprType, inner: &ExprType) -> bool {
    use sprig_exec::Kind as K;
    match (outer.kind, inner.kind) {
        (K::Int | K::Decimal, K::Int | K::Decimal) => !outer.hex && !inner.hex,
        (K::Double, K::Double) | (K::Date, K::Date) => true,
        (K::DateTime | K::Timestamp, K::DateTime | K::Timestamp) => true,
        (K::Str, K::Str) => outer.coll == inner.coll,
        _ => false,
    }
}

fn sort_cost(rows: f64, top: Option<f64>) -> f64 {
    let n = rows.max(1.0);
    let depth = top.map_or(n, |t| t.min(n)) + 2.0;
    n * depth.log2() * SORT
}

/// The cheapest path of a table once the order it gives is counted: a
/// path that reads in the wanted order saves `unsorted` (the cost of
/// sorting or hashing its rows) and, with `early` (the rows a `LIMIT`
/// needs), stops early. Gives the path and whether to read it backwards,
/// when the cheapest path is one that gives the order.
fn pick_path(
    table: &TableDef,
    info: &RelInfo,
    const_cols: &BTreeSet<usize>,
    want: &[(usize, bool)],
    single: bool,
    later: f64,
    early: Option<f64>,
    unsorted: &dyn Fn(f64) -> f64,
) -> Option<(usize, bool)> {
    let mut best: Option<(usize, Option<bool>, f64)> = None;
    for (p, path) in info.paths.iter().enumerate() {
        let served = if path.empty() {
            Some(false)
        } else {
            Cx::order_served(table, path.key, path.const_parts(), const_cols, want)
        };
        let out = (path
            .kept
            .iter()
            .map(|c| selectivity(&info.local[*c]))
            .product::<f64>()
            * path.rows)
            .max(1.0);
        let cost = match (served, early) {
            (Some(_), Some(l)) if single => path.cost * (l / out).min(1.0) + SEEK,
            (Some(_), _) => path.cost,
            (None, _) => path.cost + unsorted(if single { out } else { later }),
        };
        if best.is_none_or(|(_, _, c)| cost + EPS < c) {
            best = Some((p, served, cost));
        }
    }
    best.and_then(|(p, served, _)| served.map(|rev| (p, rev)))
}

impl<'a> Cx<'a> {
    pub(crate) fn next_id(&mut self) -> usize {
        self.ids += 1;
        self.ids
    }

    pub(crate) fn find_cte(&self, name: &str) -> Option<Rc<ast::Cte>> {
        self.ctes
            .iter()
            .rev()
            .find(|c| c.alias.name.value.eq_ignore_ascii_case(name))
            .cloned()
    }

    pub(crate) fn type_of(&self, e: &Expr) -> Result<ExprType> {
        Ok(*compile(e, &self.session)?.ty())
    }

    /// The value and type of a constant expression, when it can be
    /// computed now.
    fn eval_const(&self, e: &Expr) -> Option<(Value, ExprType)> {
        let c = compile(e, &self.session).ok()?;
        let mut ctx = Ctx::new(&self.session);
        let v = c.eval(&mut ctx, &[]).ok()?;
        Some((v, *c.ty()))
    }

    /// Plans a query: a `SELECT`, a `UNION` of them, or one in
    /// parentheses, with the query's own `ORDER BY` and `LIMIT`.
    pub(crate) fn query(
        &mut self,
        q: &ast::Query,
        outer: Option<&Scope<'_>>,
        select_type: &str,
    ) -> Result<Planned> {
        if q.fetch.is_some() || q.for_clause.is_some() || !q.pipe_operators.is_empty() {
            return Err(unsupported("this query clause"));
        }
        let cte_mark = self.ctes.len();
        if let Some(with) = &q.with {
            if with.recursive {
                return Err(unsupported("WITH RECURSIVE"));
            }
            for cte in &with.cte_tables {
                self.ctes.push(Rc::new(cte.clone()));
            }
        }
        let out = self.query_body(q, outer, select_type);
        self.ctes.truncate(cte_mark);
        out
    }

    fn query_body(
        &mut self,
        q: &ast::Query,
        outer: Option<&Scope<'_>>,
        select_type: &str,
    ) -> Result<Planned> {
        match &*q.body {
            ast::SetExpr::Select(select) => {
                let id = self.next_id();
                let block = self.select_block(
                    select,
                    q.order_by.as_ref(),
                    q.limit_clause.as_ref(),
                    outer,
                    false,
                    id,
                )?;
                self.plan_block(block, select_type)
            }
            body => {
                let planned = self.set_expr(body, outer, select_type)?;
                self.order_output(planned, q.order_by.as_ref(), q.limit_clause.as_ref())
            }
        }
    }

    fn set_expr(
        &mut self,
        body: &ast::SetExpr,
        outer: Option<&Scope<'_>>,
        select_type: &str,
    ) -> Result<Planned> {
        match body {
            ast::SetExpr::Select(select) => {
                let id = self.next_id();
                let block = self.select_block(select, None, None, outer, false, id)?;
                self.plan_block(block, select_type)
            }
            ast::SetExpr::Query(q) => self.query(q, outer, select_type),
            ast::SetExpr::SetOperation {
                left,
                op: ast::SetOperator::Union,
                set_quantifier,
                right,
            } => {
                let all = match set_quantifier {
                    ast::SetQuantifier::All => true,
                    ast::SetQuantifier::Distinct | ast::SetQuantifier::None => false,
                    _ => return Err(unsupported("this UNION form")),
                };
                let l = self.set_expr(left, outer, select_type)?;
                let r = self.set_expr(right, outer, "UNION")?;
                let est_rows = l.est_rows + r.est_rows;
                let source = Union::new(
                    vec![l.source, r.source],
                    all,
                    &self.session,
                    self.memory.clone(),
                )?;
                let types = source.types();
                Ok(Planned {
                    source: Box::new(source),
                    names: l.names,
                    types,
                    est_rows,
                    id: l.id,
                })
            }
            _ => Err(unsupported("this set operation")),
        }
    }

    /// `ORDER BY` and `LIMIT` over a finished result: the keys name its
    /// columns.
    fn order_output(
        &mut self,
        mut planned: Planned,
        order_by: Option<&ast::OrderBy>,
        limit: Option<&ast::LimitClause>,
    ) -> Result<Planned> {
        let maria = self.session.persona.is_maria();
        let empty = Scope {
            rels: &[],
            star: &[],
            visible: 0..0,
            outer: None,
        };
        let mut keys = Vec::new();
        if let Some(ob) = order_by {
            let ast::OrderByKind::Expressions(list) = &ob.kind else {
                return Err(unsupported("ORDER BY ALL"));
            };
            for o in list {
                let desc = o.options.sort == Some(ast::OrderBySort::Desc);
                let at = match &o.expr {
                    ast::Expr::Identifier(i) => planned
                        .names
                        .iter()
                        .position(|n| n.eq_ignore_ascii_case(&i.value))
                        .ok_or_else(|| unknown_column(&i.value, Clause::OrderBy, maria))?,
                    ast::Expr::Value(v) => match &v.value {
                        ast::Value::Number(n, _) => n
                            .parse::<usize>()
                            .ok()
                            .filter(|p| *p >= 1 && *p <= planned.names.len())
                            .map(|p| p - 1)
                            .ok_or_else(|| unknown_column(n, Clause::OrderBy, maria))?,
                        _ => continue,
                    },
                    _ => return Err(unsupported("an expression in a UNION's ORDER BY")),
                };
                let e = slot_ref(at, &planned.names[at], &planned.types[at]);
                keys.push(SortKey {
                    expr: compile(&e, &self.session)?,
                    desc,
                });
            }
        }
        let (mut lim, mut off) = (None, 0u64);
        match limit {
            None => {}
            Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => {
                if let Some(l) = limit {
                    lim = Some(self.limit_value(l, &empty)?);
                }
                if let Some(o) = offset {
                    off = self.limit_value(&o.value, &empty)?;
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                off = self.limit_value(offset, &empty)?;
                lim = Some(self.limit_value(limit, &empty)?);
            }
        }
        if !keys.is_empty() {
            planned.source = match lim {
                Some(l) => Box::new(Sort::top_n(
                    planned.source,
                    keys,
                    usize::try_from(l.saturating_add(off)).unwrap_or(usize::MAX),
                    self.memory.clone(),
                )),
                None => Box::new(Sort::new(planned.source, keys, self.memory.clone())),
            };
        }
        if lim.is_some() || off > 0 {
            planned.source = Box::new(Limit::new(planned.source, off, lim.unwrap_or(u64::MAX)));
            planned.est_rows = planned.est_rows.min(lim.unwrap_or(u64::MAX) as f64);
        }
        Ok(planned)
    }

    fn compile_at(&self, e: &Expr, map: &dyn Fn(usize) -> usize) -> Result<Compiled> {
        compile(&exprs::remap(e, map), &self.session)
    }

    /// The access paths of a stored table under its local conditions.
    fn table_paths(
        &self,
        base: usize,
        table: &Arc<TableDef>,
        local: &[Expr],
        needed: &BTreeSet<usize>,
        possible: &mut Vec<String>,
    ) -> Vec<Path> {
        let n = self.storage.table_rows(table) as f64;
        let all: Vec<usize> = (0..local.len()).collect();
        let mut paths = vec![Path {
            key: KeyId::Primary,
            ranges: vec![KeyRange::all()],
            full: true,
            set: None,
            rows: n,
            cost: SCAN_START + n * ROW,
            covering: false,
            kept: all.clone(),
        }];
        let eval = |e: &Expr| self.eval_const(e);
        for (id, def) in table.keys() {
            let parts = table.ordering_parts(id);
            let held: BTreeSet<usize> = parts
                .iter()
                .filter(|p| key_holds(table, p))
                .map(|p| p.column)
                .collect();
            let covering = id != KeyId::Primary && needed.iter().all(|c| held.contains(c));
            let per_row = match (id, covering) {
                (KeyId::Primary, _) => ROW,
                (_, true) => ENTRY,
                (_, false) => ENTRY + FETCH,
            };
            let ctx = KeyCtx {
                table,
                parts: &parts,
                base,
                eval: &eval,
            };
            let trees: Vec<_> = local.iter().map(|c| ctx.tree(c)).collect();
            let tree = trees.iter().cloned().fold(None, |acc, t| ctx.and(acc, t));
            let set = tree.as_ref().and_then(|t| ctx.ranges(t));
            match set {
                Some(set) => {
                    possible.push(def.name.clone());
                    let rows: f64 = set
                        .ranges
                        .iter()
                        .map(|r| self.storage.rows_in_range(table, id, r) as f64)
                        .sum::<f64>()
                        .min(n);
                    let kept = all
                        .iter()
                        .copied()
                        .filter(|i| {
                            if self.options.keep_filters {
                                return true;
                            }
                            // A condition on one key part is guaranteed by
                            // the ranges when every range binds that part.
                            let exact = trees[*i]
                                .as_ref()
                                .and_then(|t| t.exact_part())
                                .is_some_and(|part| {
                                    parts[part].prefix.is_none()
                                        && !set.cut
                                        && part <= set.min_level
                                });
                            !exact && !set.is_empty()
                        })
                        .collect();
                    paths.push(Path {
                        key: id,
                        cost: set.ranges.len() as f64 * SEEK + rows * per_row,
                        ranges: set.ranges.clone(),
                        full: false,
                        set: Some(set),
                        rows,
                        covering,
                        kept,
                    });
                }
                None if covering => paths.push(Path {
                    key: id,
                    ranges: vec![KeyRange::all()],
                    full: true,
                    set: None,
                    rows: n,
                    cost: SCAN_START + n * ENTRY,
                    covering: true,
                    kept: all.clone(),
                }),
                None => {}
            }
        }
        paths
    }

    /// Whether reading `key` after `const_parts` constant leading parts
    /// gives rows in the wanted order; the answer is whether to read
    /// backwards.
    fn order_served(
        table: &TableDef,
        key: KeyId,
        const_parts: usize,
        const_cols: &BTreeSet<usize>,
        want: &[(usize, bool)],
    ) -> Option<bool> {
        let parts = table.ordering_parts(key);
        let unique_tail = table.primary.is_some();
        let mut i = const_parts;
        let mut reverse: Option<bool> = None;
        for (col, desc) in want {
            if const_cols.contains(col) {
                continue;
            }
            while i < parts.len()
                && parts[i].column != *col
                && const_cols.contains(&parts[i].column)
            {
                i += 1;
            }
            if i >= parts.len() {
                // Every part of the key is used up: the rows are single,
                // so what follows can't reorder them.
                return (unique_tail && !parts.is_empty()).then(|| reverse.unwrap_or(false));
            }
            let p = parts[i];
            if p.column != *col || p.prefix.is_some() {
                return None;
            }
            let rev = *desc != p.desc;
            if reverse.is_some_and(|r| r != rev) {
                return None;
            }
            reverse = Some(rev);
            i += 1;
        }
        Some(reverse.unwrap_or(false))
    }

    /// Plans a resolved block.
    pub(crate) fn plan_block(&mut self, mut block: Block, select_type: &str) -> Result<Planned> {
        let n = block.rels.len();
        if n > MAX_TABLES {
            return Err(SqlError::new(
                1116,
                "HY000",
                format!("Too many tables; SprigDB can only use {MAX_TABLES} tables in a join"),
            ));
        }
        let id = block.id;

        // Conditions without columns are settled now.
        let mut impossible = false;
        let mut filters = Vec::new();
        for f in std::mem::take(&mut block.filters) {
            if exprs::is_const(&f) {
                if let Ok(c) = compile(&f, &self.session) {
                    let mut ctx = Ctx::new(&self.session);
                    match c.eval_bool(&mut ctx, &[]) {
                        Ok(true) => continue,
                        Ok(false) => {
                            impossible = true;
                            continue;
                        }
                        Err(_) => {}
                    }
                }
            }
            filters.push(f);
        }
        // Conditions that read a per-row subquery wait until its value is
        // there; they take no part in choosing access paths or joins.
        let (late, mut filters): (Vec<Expr>, Vec<Expr>) =
            filters.into_iter().partition(reads_subquery);

        // A subquery compared with a key column of a much larger table is
        // read first, made distinct, and joined like a table: the larger
        // table is then reached through its key instead of being scanned.
        let mut k = 0;
        while k < block.semis.len() {
            let semi = &block.semis[k];
            let mut turn = !semi.anti && !semi.keys.is_empty();
            for (outer_e, col) in &semi.keys {
                turn = turn
                    && self
                        .type_of(outer_e)
                        .is_ok_and(|ty| dedup_exact(&ty, &semi.inner.types[*col]));
            }
            // The table the first compared column belongs to, if the
            // column leads one of its keys.
            let outer_rows = semi.keys.first().and_then(|(e, col)| {
                let Expr::Column(c) = e else { return None };
                let rel = block
                    .rels
                    .iter()
                    .find(|r| c.slot >= r.base && c.slot < r.base + r.width)?;
                let RelKind::Table(table) = &rel.kind else {
                    return None;
                };
                let column = c.slot - rel.base;
                let leads = table.keys().any(|(_, def)| {
                    def.parts.first().is_some_and(|p| {
                        p.column == column
                            && key_usable(&table.columns[column].ty, &semi.inner.types[*col])
                    })
                });
                leads.then(|| self.storage.table_rows(table) as f64)
            });
            let inner_rows = match (&semi.simple, turn, outer_rows) {
                (Some(simple), true, Some(_)) => {
                    let mut unused = Vec::new();
                    let paths = self.table_paths(
                        0,
                        &simple.table,
                        &simple.filters,
                        &BTreeSet::new(),
                        &mut unused,
                    );
                    paths
                        .iter()
                        .map(|p| {
                            p.rows
                                * p.kept
                                    .iter()
                                    .map(|c| selectivity(&simple.filters[*c]))
                                    .product::<f64>()
                        })
                        .fold(f64::INFINITY, f64::min)
                }
                _ => f64::INFINITY,
            };
            if !(turn && outer_rows.is_some_and(|n| inner_rows * 5.0 < n)) {
                k += 1;
                continue;
            }
            let semi = block.semis.remove(k);
            let Some(inner) = semi.inner.take_block() else {
                continue;
            };
            let planned = self.plan_block(inner, "MATERIALIZED")?;
            let est = planned.est_rows;
            let derived = Derived::new(
                Box::new(Distinct::new(planned.source, self.memory.clone())),
                est,
            );
            let base = block
                .rels
                .iter()
                .map(|r| r.base + r.width)
                .max()
                .unwrap_or(0);
            let names: Vec<String> = (0..derived.types.len()).map(|i| format!("k{i}")).collect();
            let cols = derived_cols(base, &names, &derived.types);
            for (outer_e, col) in &semi.keys {
                filters.push(Expr::binary(
                    BinOp::Eq,
                    outer_e.clone(),
                    cols[*col].expr.clone(),
                ));
            }
            block.rels.push(Rel {
                shown: format!("<subquery{}>", semi.id),
                alias: format!("<subquery{}>", semi.id),
                qualified: None,
                width: cols.len(),
                kind: RelKind::Derived(derived),
                cols,
                base,
                outer_on: None,
                outer_dep: None,
                pending: Vec::new(),
                hidden: true,
                derived_id: None,
            });
        }
        let n = block.rels.len();

        // A left join whose inner side a WHERE condition needs non-NULL
        // is an inner join.
        loop {
            let mut changed = false;
            for i in 0..n {
                if block.rels[i].outer_on.is_some()
                    && filters.iter().any(|f| rejects_nulls(f, &block.rels[i]))
                {
                    if let Some(on) = block.rels[i].outer_on.take() {
                        filters.extend(on);
                    }
                    block.rels[i].outer_dep = None;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        // A left join's inner side that nothing else reads, and that can
        // match at most one row, isn't read at all.
        let mut eliminated = 0u64;
        for i in 0..n {
            let rel = &block.rels[i];
            let (Some(on), RelKind::Table(table)) = (&rel.outer_on, &rel.kind) else {
                continue;
            };
            let inside = |s: usize| s >= rel.base && s < rel.base + rel.width;
            let mut outside = BTreeSet::new();
            for e in filters
                .iter()
                .chain(late.iter())
                .chain(block.items.iter().map(|i| &i.expr))
                .chain(block.group_by.iter())
                .chain(block.aggs.iter().filter_map(|a| a.arg.as_ref()))
                .chain(block.having.iter())
                .chain(block.order_by.iter().map(|(e, _)| e))
                .chain(
                    block
                        .semis
                        .iter()
                        .flat_map(|s| s.keys.iter().map(|(e, _)| e)),
                )
                .chain(
                    block
                        .applies
                        .iter()
                        .flat_map(|a| a.keys.iter().map(|(e, _)| e)),
                )
                .chain(
                    block
                        .rels
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .filter_map(|(_, r)| r.outer_on.as_ref())
                        .flatten(),
                )
            {
                exprs::collect_slots(e, &mut outside);
            }
            if outside.iter().any(|s| inside(*s)) {
                continue;
            }
            let bound = |col: usize| {
                let ty = &table.columns[col].ty;
                on.iter().any(|c| {
                    let Expr::Binary(BinOp::Eq, a, b) = c else {
                        return false;
                    };
                    let is_col =
                        |e: &Expr| matches!(e, Expr::Column(c) if c.slot == rel.base + col);
                    let free = |e: &Expr| {
                        !exprs::slots(e).iter().any(|s| inside(*s))
                            && self.type_of(e).is_ok_and(|t| key_usable(ty, &t))
                    };
                    (is_col(a) && free(b)) || (is_col(b) && free(a))
                })
            };
            let single = table.keys().any(|(id, def)| {
                (id == KeyId::Primary || table.is_strict_unique(id))
                    && def
                        .parts
                        .iter()
                        .all(|p| p.prefix.is_none() && bound(p.column))
            });
            if single {
                eliminated |= 1 << i;
            }
        }
        let rels = &block.rels;
        let mask = |e: &Expr| mask_of(rels, e);

        // The columns of each item the block reads.
        let mut used = BTreeSet::new();
        for e in filters
            .iter()
            .chain(late.iter())
            .chain(block.items.iter().map(|i| &i.expr))
            .chain(block.group_by.iter())
            .chain(block.aggs.iter().filter_map(|a| a.arg.as_ref()))
            .chain(block.having.iter())
            .chain(block.order_by.iter().map(|(e, _)| e))
            .chain(
                block
                    .semis
                    .iter()
                    .flat_map(|s| s.keys.iter().map(|(e, _)| e)),
            )
            .chain(
                block
                    .applies
                    .iter()
                    .flat_map(|a| a.keys.iter().map(|(e, _)| e)),
            )
            .chain(
                rels.iter()
                    .enumerate()
                    .filter(|(i, _)| eliminated & (1 << i) == 0)
                    .filter_map(|(_, r)| r.outer_on.as_ref())
                    .flatten(),
            )
        {
            exprs::collect_slots(e, &mut used);
        }

        // Each item on its own.
        let mut infos: Vec<RelInfo> = Vec::with_capacity(n);
        for (i, rel) in rels.iter().enumerate() {
            let local: Vec<Expr> = match &rel.outer_on {
                Some(on) => on.iter().filter(|c| mask(c) == 1 << i).cloned().collect(),
                None => filters
                    .iter()
                    .filter(|c| mask(c) == 1 << i)
                    .cloned()
                    .collect(),
            };
            let mut deps = 0u64;
            if let Some(on) = &rel.outer_on {
                for c in on {
                    deps |= mask(c) & !(1 << i);
                }
                if deps == 0 {
                    if let Some(d) = rel.outer_dep {
                        deps = 1 << d;
                    }
                }
            }
            let needed: BTreeSet<usize> = used
                .iter()
                .filter(|s| **s >= rel.base && **s < rel.base + rel.width)
                .map(|s| s - rel.base)
                .collect();
            let mut possible = Vec::new();
            let (paths, table_rows) = match &rel.kind {
                RelKind::Table(t) => (
                    self.table_paths(rel.base, t, &local, &needed, &mut possible),
                    self.storage.table_rows(t) as f64,
                ),
                RelKind::Derived(d) => {
                    self.ready_derived(d, Some((rel, &local)), "DERIVED")?;
                    let rows = d.est_rows.get();
                    (
                        vec![Path {
                            key: KeyId::Primary,
                            ranges: Vec::new(),
                            full: true,
                            set: None,
                            rows,
                            cost: rows * 0.2,
                            covering: false,
                            kept: (0..local.len()).collect(),
                        }],
                        rows,
                    )
                }
            };
            let mut best = 0;
            for (p, path) in paths.iter().enumerate() {
                if path.cost + EPS < paths[best].cost {
                    best = p;
                }
            }
            let out_rows = paths[best]
                .kept
                .iter()
                .map(|c| selectivity(&local[*c]))
                .product::<f64>()
                * paths[best].rows;
            infos.push(RelInfo {
                local,
                paths,
                best,
                table_rows,
                out_rows,
                needed,
                deps,
                possible,
            });
        }

        // The join order.
        let steps = self.join_order(rels, &infos, &filters, eliminated)?;

        // ORDER BY and GROUP BY that a key of the first item can serve.
        let limit_rows = block.limit.map(|l| l.saturating_add(block.offset) as f64);
        let want_order = |list: &[(Expr, bool)]| -> Option<(usize, Vec<(usize, bool)>)> {
            let mut rel = None;
            let mut cols = Vec::new();
            for (e, desc) in list {
                if exprs::is_const(e) {
                    continue;
                }
                let Expr::Column(c) = e else { return None };
                let r = rels
                    .iter()
                    .position(|r| c.slot >= r.base && c.slot < r.base + r.width)?;
                if rel.is_some_and(|x| x != r) {
                    return None;
                }
                rel = Some(r);
                cols.push((c.slot - rels[r].base, *desc));
            }
            rel.map(|r| (r, cols))
        };
        let mut sorted_by_index = false;
        let mut streaming = false;
        let mut reverse = false;
        let mut min_max: Option<Path> = None;
        if let Some(first) = steps.first() {
            let r = first.rel;
            if let RelKind::Table(table) = &rels[r].kind {
                let const_cols: BTreeSet<usize> = infos[r]
                    .local
                    .iter()
                    .filter_map(|c| match c {
                        Expr::Binary(BinOp::Eq, a, b) => match (&**a, &**b) {
                            (Expr::Column(col), k) | (k, Expr::Column(col))
                                if exprs::is_const(k) =>
                            {
                                Some(col.slot - rels[r].base)
                            }
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect();
                let later = steps.last().map_or(1.0, |s| s.rows).max(1.0);
                if block.grouped && !block.group_by.is_empty() && self.options.order_by_index {
                    let list: Vec<(Expr, bool)> =
                        block.group_by.iter().map(|g| (g.clone(), false)).collect();
                    if let Some((wr, want)) = want_order(&list) {
                        if wr == r && !want.is_empty() {
                            let choice = pick_path(
                                table,
                                &infos[r],
                                &const_cols,
                                &want,
                                n == 1,
                                later,
                                None,
                                &|rows| rows * HASH_GROUP,
                            );
                            if let Some((p, rev)) = choice {
                                infos[r].best = p;
                                reverse = rev;
                                streaming = true;
                            }
                        }
                    }
                } else if !block.grouped
                    && !block.distinct
                    && !block.order_by.is_empty()
                    && self.options.order_by_index
                {
                    if let Some((wr, want)) = want_order(&block.order_by) {
                        if wr == r {
                            // With a LIMIT, an index in the wanted order
                            // is a candidate also when no condition uses
                            // it: the scan stops early.
                            if limit_rows.is_some() {
                                let rows = infos[r].table_rows;
                                let kept: Vec<usize> = (0..infos[r].local.len()).collect();
                                for (kid, _) in table.keys() {
                                    if kid != KeyId::Primary
                                        && !infos[r].paths.iter().any(|p| p.key == kid)
                                    {
                                        infos[r].paths.push(Path {
                                            key: kid,
                                            ranges: vec![KeyRange::all()],
                                            full: true,
                                            set: None,
                                            rows,
                                            cost: SCAN_START + rows * (ENTRY + FETCH),
                                            covering: false,
                                            kept: kept.clone(),
                                        });
                                    }
                                }
                            }
                            let choice = pick_path(
                                table,
                                &infos[r],
                                &const_cols,
                                &want,
                                n == 1,
                                later,
                                limit_rows,
                                &|rows| sort_cost(rows, limit_rows),
                            );
                            if let Some((p, rev)) = choice {
                                infos[r].best = p;
                                reverse = rev;
                                sorted_by_index = true;
                            }
                        }
                    }
                }

                // MIN or MAX of a key column, read off the key's edge.
                if n == 1
                    && block.grouped
                    && block.group_by.is_empty()
                    && block.aggs.len() == 1
                    && block.semis.is_empty()
                    && block.applies.is_empty()
                    && late.is_empty()
                    && !impossible
                    && !self.options.keep_filters
                    && filters.len() == infos[r].local.len()
                    && matches!(block.aggs[0].kind, AggKind::Min | AggKind::Max)
                {
                    if let Some(Expr::Column(c)) = &block.aggs[0].arg {
                        let col = c.slot - rels[r].base;
                        let mut local = infos[r].local.clone();
                        local.push(Expr::IsNull(Box::new(Expr::Column(c.clone())), true));
                        let mut unused = Vec::new();
                        let paths = self.table_paths(
                            rels[r].base,
                            table,
                            &local,
                            &infos[r].needed,
                            &mut unused,
                        );
                        for path in paths {
                            let Some(set) = &path.set else { continue };
                            let parts = table.ordering_parts(path.key);
                            let at = set.const_parts.min(parts.len());
                            let on_col = parts.get(at).is_some_and(|p| p.column == col)
                                || parts[..at].iter().any(|p| p.column == col);
                            if path.kept.is_empty() && on_col {
                                let desc = parts.get(at).is_some_and(|p| p.desc);
                                reverse = (block.aggs[0].kind == AggKind::Max) != desc;
                                min_max = Some(path);
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Build the join.
        let mut phys: Vec<Option<usize>> = vec![None; n];
        let mut width = 0usize;
        let mut placed = 0u64;
        let mut applied = vec![false; filters.len()];
        let mut semi_done = vec![false; block.semis.len()];
        let mut source: Option<Box<dyn RowSource>> = None;
        let mut explain_rows: Vec<ExplainRow> = Vec::new();
        let schema_of = |r: usize| -> String {
            rels[r]
                .qualified
                .as_ref()
                .map_or(String::new(), |(s, _)| format!("{s}."))
        };
        if n == 0 {
            source = Some(Box::new(Fixed::new(vec![Vec::new()], Vec::new())));
            explain_rows.push(ExplainRow::message(id, select_type, "No tables used"));
        }
        for step in &steps {
            let r = step.rel;
            let rel = &rels[r];
            let info = &infos[r];
            let outer_join = rel.outer_on.is_some();
            let base = rel.base;
            let local_map = move |s: usize| s - base;
            let counters = Counters::new();
            let mut row = ExplainRow::new(id, select_type, &rel.shown);
            if let Some(d) = rel.derived_id {
                row.table = Some(format!("<derived{d}>"));
            }
            row.possible_keys = (!info.possible.is_empty()).then(|| info.possible.join(","));
            let mut using_where = false;

            // The item's own rows, for every method but a key lookup.
            let by_order = sorted_by_index || streaming;
            let own_scan = |cx: &mut Self,
                            path: &Path,
                            row: &mut ExplainRow,
                            reverse: bool,
                            using_where: &mut bool|
             -> Result<Box<dyn RowSource>> {
                let mut src: Box<dyn RowSource> = match &rel.kind {
                    RelKind::Table(table) => {
                        let fetch = if path.covering {
                            Fetch::KeyOnly
                        } else {
                            Fetch::Row
                        };
                        cx.accesses.push(AccessStat {
                            id,
                            table: rel.alias.clone(),
                            counters: counters.clone(),
                        });
                        crate::explain::describe_path(
                            row,
                            table,
                            path.key,
                            path.full,
                            path.set.as_ref(),
                            path.covering,
                            path.rows,
                            by_order,
                        );
                        Box::new(ScanSource {
                            storage: cx.storage.clone(),
                            table: table.clone(),
                            key: path.key,
                            ranges: path.ranges.clone(),
                            reverse,
                            fetch,
                            counters: counters.clone(),
                            cursor: None,
                        })
                    }
                    RelKind::Derived(d) => {
                        row.access = Some("ALL".into());
                        row.rows = Some(d.est_rows.get().max(1.0) as u64);
                        Box::new(DerivedSource::new(d.clone()))
                    }
                };
                let kept: Vec<Expr> = path.kept.iter().map(|i| info.local[*i].clone()).collect();
                if let Some(cond) = exprs::and_all(kept) {
                    *using_where = true;
                    src = Box::new(Filter::new(src, cx.compile_at(&cond, &local_map)?));
                }
                Ok(src)
            };

            let phys_now = phys.clone();
            let prefix_map = move |s: usize| -> usize {
                for (i, rl) in rels.iter().enumerate() {
                    if s >= rl.base && s < rl.base + rl.width {
                        return phys_now[i].map_or(usize::MAX, |p| p + (s - rl.base));
                    }
                }
                s
            };
            let on_cond = rel.outer_on.clone().and_then(exprs::and_all);
            let kind = if outer_join {
                JoinKind::Left
            } else {
                JoinKind::Inner
            };
            // After this step the item's row starts here.
            let here = width;
            let phys_after = {
                let mut p = phys.clone();
                p[r] = Some(here);
                p
            };
            let joined_map = move |s: usize| -> usize {
                for (i, rl) in rels.iter().enumerate() {
                    if s >= rl.base && s < rl.base + rl.width {
                        return phys_after[i].map_or(usize::MAX, |p| p + (s - rl.base));
                    }
                }
                s
            };
            let best_path = info.paths[info.best].clone();
            source = Some(match (&step.method, source.take()) {
                (Method::Scan, _) | (_, None) => {
                    if let Some(path) = &min_max {
                        let mut src = own_scan(self, path, &mut row, reverse, &mut using_where)?;
                        src = Box::new(Limit::new(src, 0, 1));
                        row = ExplainRow::message(id, select_type, "Select tables optimized away");
                        src
                    } else {
                        own_scan(self, &best_path, &mut row, reverse, &mut using_where)?
                    }
                }
                (Method::Lookup(lookup), Some(outer)) => {
                    let RelKind::Table(table) = &rel.kind else {
                        return Err(unsupported("a key lookup into a derived table"));
                    };
                    let def = table
                        .key(lookup.key)
                        .ok_or_else(|| unsupported("a lookup without a key"))?;
                    let mut parts = Vec::with_capacity(lookup.exprs.len());
                    for (e, part) in lookup.exprs.iter().zip(&def.parts) {
                        parts.push(LookupPart {
                            expr: self.compile_at(e, &prefix_map)?,
                            column: table.columns[part.column].ty,
                            prefix: part.prefix,
                        });
                    }
                    self.accesses.push(AccessStat {
                        id,
                        table: rel.alias.clone(),
                        counters: counters.clone(),
                    });
                    row.access = Some(if lookup.unique { "eq_ref" } else { "ref" }.into());
                    row.key = Some(def.name.clone());
                    if row.possible_keys.is_none() {
                        row.possible_keys = Some(def.name.clone());
                    }
                    row.key_len =
                        Some(key_len(table, &def.parts[..lookup.exprs.len()]).to_string());
                    row.r#ref = Some(
                        lookup
                            .exprs
                            .iter()
                            .map(|e| match e {
                                Expr::Column(c) => {
                                    let owner = rels.iter().position(|x| {
                                        c.slot >= x.base && c.slot < x.base + x.width
                                    });
                                    match owner {
                                        Some(o) => {
                                            format!("{}{}.{}", schema_of(o), rels[o].alias, c.name)
                                        }
                                        None => "func".into(),
                                    }
                                }
                                k if exprs::is_const(k) => "const".into(),
                                _ => "func".into(),
                            })
                            .collect::<Vec<_>>()
                            .join(","),
                    );
                    row.rows = Some(lookup.rows.max(1.0).round() as u64);
                    if lookup.covering {
                        row.extra.push("Using index".into());
                    }
                    let inner: Box<dyn InnerSource> = Box::new(LookupInner {
                        storage: self.storage.clone(),
                        table: table.clone(),
                        key: lookup.key,
                        parts,
                        fetch: if lookup.covering {
                            Fetch::KeyOnly
                        } else {
                            Fetch::Row
                        },
                        counters: counters.clone(),
                        cursor: None,
                    });
                    let on = match &on_cond {
                        Some(c) => Some(self.compile_at(c, &joined_map)?),
                        None => None,
                    };
                    Box::new(NestedLoopJoin::new(outer, inner, on, kind))
                }
                (Method::Hash(keys), Some(outer)) => {
                    let right = own_scan(self, &best_path, &mut row, false, &mut using_where)?;
                    let mut lk = Vec::with_capacity(keys.len());
                    let mut rk = Vec::with_capacity(keys.len());
                    for (l, rr) in keys {
                        lk.push(self.compile_at(l, &prefix_map)?);
                        rk.push(self.compile_at(rr, &local_map)?);
                    }
                    let residual = match &on_cond {
                        Some(c) => Some(self.compile_at(c, &joined_map)?),
                        None => None,
                    };
                    if row.access.as_deref() == Some("ALL") {
                        row.access = Some("hash_ALL".into());
                    } else if let Some(a) = &row.access {
                        row.access = Some(format!("hash_{a}"));
                    }
                    row.extra.push("Using join buffer (hash join)".into());
                    Box::new(HashJoin::new(
                        outer,
                        right,
                        lk,
                        rk,
                        residual,
                        kind,
                        &self.session,
                        self.memory.clone(),
                    )?)
                }
                (Method::Replay, Some(outer)) => {
                    let right = own_scan(self, &best_path, &mut row, false, &mut using_where)?;
                    let on = match &on_cond {
                        Some(c) => Some(self.compile_at(c, &joined_map)?),
                        None => None,
                    };
                    row.extra.push("Using join buffer (replayed rows)".into());
                    Box::new(NestedLoopJoin::new(
                        outer,
                        Box::new(CachedInner::new(right)),
                        on,
                        kind,
                    ))
                }
            });
            phys[r] = Some(here);
            width += rel.width;
            placed |= 1 << r;

            // WHERE conditions that can be checked from here on.
            let phys_here = phys.clone();
            let map_here = move |s: usize| -> usize {
                for (i, rl) in rels.iter().enumerate() {
                    if s >= rl.base && s < rl.base + rl.width {
                        return phys_here[i].map_or(usize::MAX, |p| p + (s - rl.base));
                    }
                }
                s
            };
            let mut now = Vec::new();
            for (i, f) in filters.iter().enumerate() {
                if applied[i] || mask(f) & !placed != 0 || reads_subquery(f) {
                    continue;
                }
                applied[i] = true;
                // The first item's own scan has already checked (or
                // proved) its local conditions.
                let own = !outer_join && mask(f) == 1 << r;
                if own && matches!(step.method, Method::Scan | Method::Hash(_) | Method::Replay) {
                    continue;
                }
                // An exact key lookup has already compared its key parts.
                if let (Method::Lookup(l), RelKind::Table(table), false) =
                    (&step.method, &rel.kind, outer_join)
                {
                    let parts = table.key(l.key).map_or(&[][..], |d| &d.parts[..]);
                    let guaranteed = l.exact
                        && !self.options.keep_filters
                        && l.exprs.iter().zip(parts).any(|(outer_e, part)| {
                            let is_col = |e: &Expr| {
                                matches!(e, Expr::Column(c) if c.slot == rel.base + part.column)
                            };
                            matches!(f, Expr::Binary(BinOp::Eq, a, b)
                                if (is_col(a) && **b == *outer_e) || (is_col(b) && **a == *outer_e))
                        });
                    if guaranteed {
                        continue;
                    }
                }
                now.push(f.clone());
            }
            if let Some(cond) = exprs::and_all(now) {
                using_where = true;
                if let Some(s) = source.take() {
                    source = Some(Box::new(Filter::new(s, self.compile_at(&cond, &map_here)?)));
                }
            }
            if on_cond.is_some() && !matches!(step.method, Method::Scan) {
                using_where = true;
            }
            if using_where {
                row.extra.insert(0, "Using where".into());
            }
            explain_rows.push(row);

            // Subquery predicates whose outer side is complete.
            for (si, semi) in block.semis.iter().enumerate() {
                if semi_done[si] || semi.keys.iter().any(|(e, _)| mask(e) & !placed != 0) {
                    continue;
                }
                semi_done[si] = true;
                if let Some(s) = source.take() {
                    source = Some(self.semi_source(
                        s,
                        semi,
                        &map_here,
                        width,
                        step.rows,
                        id,
                        select_type,
                        &mut explain_rows,
                    )?);
                }
            }
        }
        let mut source = match source {
            Some(s) => s,
            None => Box::new(Fixed::new(vec![Vec::new()], Vec::new())),
        };
        let phys_final = phys.clone();
        let apply_base = width;
        let final_map = move |s: usize| -> usize {
            if (SUBQ_BASE..AGG_BASE).contains(&s) {
                return apply_base + (s - SUBQ_BASE);
            }
            for (i, rl) in rels.iter().enumerate() {
                if s >= rl.base && s < rl.base + rl.width {
                    return phys_final[i].map_or(usize::MAX, |p| p + (s - rl.base));
                }
            }
            s
        };
        // Conditions and subquery predicates over no table at all.
        let rest: Vec<Expr> = filters
            .iter()
            .enumerate()
            .filter(|(i, f)| !applied[*i] && !reads_subquery(f))
            .map(|(_, f)| f.clone())
            .collect();
        if let Some(cond) = exprs::and_all(rest) {
            source = Box::new(Filter::new(source, self.compile_at(&cond, &final_map)?));
        }
        for (si, semi) in block.semis.iter().enumerate() {
            if semi_done[si] {
                continue;
            }
            source = self.semi_source(
                source,
                semi,
                &final_map,
                width,
                steps.last().map_or(1.0, |s| s.rows),
                id,
                select_type,
                &mut explain_rows,
            )?;
        }

        // Scalar subqueries computed per row, each adding a column, and
        // then the conditions that read them.
        for (ai, apply) in block.applies.iter().enumerate() {
            source = self.apply_source(
                source,
                apply,
                &final_map,
                apply_base + ai,
                &mut explain_rows,
            )?;
        }
        if let Some(cond) = exprs::and_all(late.iter().cloned()) {
            source = Box::new(Filter::new(source, self.compile_at(&cond, &final_map)?));
        }
        if impossible {
            source = Box::new(Fixed::new(Vec::new(), source.types()));
            explain_rows = vec![ExplainRow::message(id, select_type, "Impossible WHERE")];
        }
        let mut est_rows = steps.last().map_or(1.0, |s| s.rows);

        // Grouping.
        let mut items: Vec<Expr> = block.items.iter().map(|i| i.expr.clone()).collect();
        let mut order: Vec<(Expr, bool)> = block.order_by.clone();
        let mut exact_types: Vec<Option<ExprType>> = vec![None; items.len()];
        let mut layout_map: Box<dyn Fn(usize) -> usize + '_> = Box::new(final_map);
        let mut temporary = false;
        if block.grouped {
            let groups = block.group_by.clone();
            let mut aggs: Vec<AggSpec> = block.aggs.clone();
            let group_types: Vec<ExprType> = groups
                .iter()
                .map(|g| self.type_of(g))
                .collect::<Result<_>>()?;
            // A column that is neither grouped nor aggregated shows one of
            // its group's values.
            let mut loose: Vec<Expr> = Vec::new();
            let having = block.having.clone();
            {
                let mut find = |e: &Expr| {
                    fn walk(e: &Expr, groups: &[Expr], out: &mut Vec<Expr>) {
                        if groups.iter().any(|g| g == e) {
                            return;
                        }
                        if let Expr::Column(c) = e {
                            if c.slot < AGG_BASE && !out.iter().any(|o| o == e) {
                                out.push(e.clone());
                            }
                            return;
                        }
                        exprs::for_children(e, &mut |x| walk(x, groups, out));
                    }
                    walk(e, &groups, &mut loose);
                };
                items.iter().for_each(&mut find);
                having.iter().for_each(&mut find);
                order.iter().for_each(|(e, _)| find(e));
            }
            if !loose.is_empty() && self.options.only_full_group_by {
                self.check_dependent(rels, &groups, &loose, &items)?;
            }
            let loose_base = aggs.len();
            for e in &loose {
                let arg = compile(e, &self.session)?;
                let ty = *Aggregate::new(AggKind::Min, Some(arg), false, &self.session)?.ty();
                aggs.push(AggSpec {
                    kind: AggKind::Min,
                    arg: Some(e.clone()),
                    distinct: false,
                    separator: None,
                    ty,
                });
            }
            let k = groups.len();
            let agg_types: Vec<ExprType> = aggs.iter().map(|a| a.ty).collect();
            let post = |e: &Expr| -> Expr {
                exprs::rewrite(e, &mut |x| {
                    if let Some(j) = groups.iter().position(|g| g == x) {
                        return Some(slot_ref(j, "group", &group_types[j]));
                    }
                    if let Expr::Column(c) = x {
                        if c.slot >= AGG_BASE {
                            let i = c.slot - AGG_BASE;
                            return Some(slot_ref(k + i, &c.name, &agg_types[i]));
                        }
                        if let Some(i) = loose.iter().position(|l| l == x) {
                            return Some(slot_ref(
                                k + loose_base + i,
                                &c.name,
                                &agg_types[loose_base + i],
                            ));
                        }
                    }
                    None
                })
            };
            let mut group_c = Vec::with_capacity(k);
            for g in &groups {
                group_c.push(self.compile_at(g, &*layout_map)?);
            }
            let mut agg_c = Vec::with_capacity(aggs.len());
            for a in &aggs {
                let arg = match &a.arg {
                    Some(e) => Some(self.compile_at(e, &*layout_map)?),
                    None => None,
                };
                let mut agg = Aggregate::new(a.kind, arg, a.distinct, &self.session)?;
                if let Some(sep) = &a.separator {
                    agg = agg.separator(sep);
                }
                agg_c.push(agg);
            }
            source = if streaming {
                Box::new(Aggregation::streaming(source, group_c, agg_c))
            } else {
                temporary = k > 0;
                Box::new(Aggregation::hash(
                    source,
                    group_c,
                    agg_c,
                    self.memory.clone(),
                ))
            };
            let out_types = source.types();
            for (i, item) in items.iter_mut().enumerate() {
                *item = post(item);
                if let Expr::Column(c) = item {
                    exact_types[i] = out_types.get(c.slot).copied();
                }
            }
            for (e, _) in &mut order {
                *e = post(e);
            }
            if let Some(h) = &having {
                source = Box::new(Filter::new(source, compile(&post(h), &self.session)?));
            }
            layout_map = Box::new(|s| s);
            est_rows = if k == 0 {
                1.0
            } else {
                (est_rows * 0.5).max(1.0)
            };
        }

        // Ordering, DISTINCT, LIMIT and the select list.
        order.retain(|(e, _)| !exprs::is_const(e));
        let names: Vec<String> = block.items.iter().map(|i| i.name.clone()).collect();
        let top = block
            .limit
            .map(|l| usize::try_from(l.saturating_add(block.offset)).unwrap_or(usize::MAX));
        let mut filesort = false;
        if block.distinct {
            let mut proj = Vec::with_capacity(items.len());
            for e in &items {
                proj.push(self.compile_at(e, &*layout_map)?);
            }
            source = Box::new(Project::new(source, proj));
            let types = source.types();
            source = Box::new(Distinct::new(source, self.memory.clone()));
            temporary = true;
            if !order.is_empty() {
                let mut keys = Vec::new();
                for (e, desc) in &order {
                    let Some(at) = items.iter().position(|i| i == e) else {
                        return Err(SqlError::new(
                            3065,
                            "HY000",
                            "Expression #1 of ORDER BY clause is not in SELECT list; this is incompatible with DISTINCT",
                        ));
                    };
                    keys.push(SortKey {
                        expr: compile(&slot_ref(at, &names[at], &types[at]), &self.session)?,
                        desc: *desc,
                    });
                }
                filesort = true;
                source = match top {
                    Some(t) => Box::new(Sort::top_n(source, keys, t, self.memory.clone())),
                    None => Box::new(Sort::new(source, keys, self.memory.clone())),
                };
            }
            if block.limit.is_some() || block.offset > 0 {
                source = Box::new(Limit::new(
                    source,
                    block.offset,
                    block.limit.unwrap_or(u64::MAX),
                ));
            }
        } else {
            if !order.is_empty() && !(sorted_by_index && !block.grouped) {
                let mut keys = Vec::new();
                for (e, desc) in &order {
                    keys.push(SortKey {
                        expr: self.compile_at(e, &*layout_map)?,
                        desc: *desc,
                    });
                }
                filesort = true;
                source = match top {
                    Some(t) => Box::new(Sort::top_n(source, keys, t, self.memory.clone())),
                    None => Box::new(Sort::new(source, keys, self.memory.clone())),
                };
            }
            if block.limit.is_some() || block.offset > 0 {
                source = Box::new(Limit::new(
                    source,
                    block.offset,
                    block.limit.unwrap_or(u64::MAX),
                ));
            }
            let mut proj = Vec::with_capacity(items.len());
            for e in &items {
                proj.push(self.compile_at(e, &*layout_map)?);
            }
            source = Box::new(Project::new(source, proj));
        }
        if let Some(l) = block.limit {
            est_rows = est_rows.min(l as f64);
        }
        if let Some(first) = explain_rows.first_mut() {
            if temporary && first.table.is_some() {
                first.extra.push("Using temporary".into());
            }
            if filesort && first.table.is_some() {
                first.extra.push("Using filesort".into());
            }
        }
        self.explain.extend(explain_rows);
        let mut types = source.types();
        for (t, exact) in types.iter_mut().zip(&exact_types) {
            if let Some(e) = exact {
                *t = *e;
            }
        }
        Ok(Planned {
            source,
            names,
            types,
            est_rows,
            id,
        })
    }

    /// Plans a derived table's query if that is still to do. With
    /// `push`, the outer block's conditions on the derived table's
    /// columns move into the query first: all of them when the query
    /// doesn't group, those on grouping columns when it does, none past
    /// a LIMIT.
    fn ready_derived(
        &mut self,
        d: &Rc<Derived>,
        push: Option<(&Rel, &[Expr])>,
        select_type: &str,
    ) -> Result<()> {
        let Some(mut block) = d.take_block() else {
            return Ok(());
        };
        if let (Some((rel, local)), true) = (push, block.limit.is_none() && block.offset == 0) {
            for c in local {
                if exprs::is_volatile(c) {
                    continue;
                }
                let mut ok = true;
                let pushed = exprs::rewrite(c, &mut |e| match e {
                    Expr::Column(col)
                        if col.slot >= rel.base && col.slot < rel.base + rel.width =>
                    {
                        let item = &block.items[col.slot - rel.base].expr;
                        if block.grouped && !block.group_by.iter().any(|g| g == item) {
                            ok = false;
                        }
                        if exprs::slots(item).iter().any(|s| *s >= SUBQ_BASE) {
                            ok = false;
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
        let planned = self.plan_block(block, select_type)?;
        d.set_source(planned.source, planned.est_rows);
        Ok(())
    }

    /// The key of `table` whose leading parts the given equalities bind
    /// best: per bound part, the position of its equality in `keys`
    /// (pairs of an outer expression and an expression over the table's
    /// row); whether a lookup finds at most one row; the rows per lookup.
    fn bind_key(
        &self,
        table: &TableDef,
        keys: &[(Expr, Expr)],
    ) -> Option<(KeyId, Vec<usize>, bool, f64)> {
        let mut best: Option<(KeyId, Vec<usize>, bool, f64)> = None;
        for (kid, def) in table.keys() {
            let mut bound = Vec::new();
            for part in &def.parts {
                let ty = &table.columns[part.column].ty;
                let hit = keys.iter().position(|(outer_e, inner_e)| {
                    matches!(inner_e, Expr::Column(c) if c.slot == part.column)
                        && self.type_of(outer_e).is_ok_and(|t| key_usable(ty, &t))
                });
                match hit {
                    Some(i) => bound.push(i),
                    None => break,
                }
            }
            if bound.is_empty() {
                continue;
            }
            let unique = bound.len() == def.parts.len()
                && def.unique
                && def.parts.iter().all(|p| p.prefix.is_none());
            let rows = if unique {
                1.0
            } else {
                self.storage
                    .rows_per_key(table, kid, bound.len())
                    .unwrap_or(10.0)
            };
            if best.as_ref().is_none_or(|b| rows + EPS < b.3) {
                best = Some((kid, bound, unique, rows));
            }
        }
        best
    }

    /// A key lookup into `table` for each outer row.
    fn lookup_inner(
        &mut self,
        table: &Arc<TableDef>,
        alias: &str,
        id: usize,
        kid: KeyId,
        outer_exprs: &[&Expr],
        map: &dyn Fn(usize) -> usize,
    ) -> Result<Box<dyn InnerSource>> {
        let def = table
            .key(kid)
            .ok_or_else(|| unsupported("a lookup without a key"))?;
        let mut parts = Vec::with_capacity(outer_exprs.len());
        for (e, part) in outer_exprs.iter().zip(&def.parts) {
            parts.push(LookupPart {
                expr: self.compile_at(e, map)?,
                column: table.columns[part.column].ty,
                prefix: part.prefix,
            });
        }
        let counters = Counters::new();
        self.accesses.push(AccessStat {
            id,
            table: alias.to_owned(),
            counters: counters.clone(),
        });
        Ok(Box::new(LookupInner {
            storage: self.storage.clone(),
            table: table.clone(),
            key: kid,
            parts,
            fetch: Fetch::Row,
            counters,
            cursor: None,
        }))
    }

    /// Applies a subquery predicate to the rows of `source`: by a key
    /// probe per row that stops at the first match when the subquery is
    /// one table with a key on the compared columns and the rows are few,
    /// else by a hash table of the subquery's rows.
    fn semi_source(
        &mut self,
        source: Box<dyn RowSource>,
        semi: &SemiJoin,
        map: &dyn Fn(usize) -> usize,
        width: usize,
        rows_now: f64,
        id: usize,
        select_type: &str,
        explain_rows: &mut Vec<ExplainRow>,
    ) -> Result<Box<dyn RowSource>> {
        let kind = if semi.anti {
            JoinKind::Anti
        } else {
            JoinKind::Semi
        };
        let note = if semi.anti {
            "Not exists"
        } else {
            "FirstMatch"
        };
        if let Some(simple) = &semi.simple {
            let pairs: Vec<(Expr, Expr)> = semi
                .keys
                .iter()
                .map(|(e, col)| (e.clone(), simple.items[*col].clone()))
                .collect();
            let table_rows = self.storage.table_rows(&simple.table) as f64;
            if let Some((kid, bound, unique, rows)) = self.bind_key(&simple.table, &pairs) {
                let probe = rows_now.max(1.0) * (SEEK + rows.min(2.0) * (ENTRY + FETCH));
                let hash = SEEK + table_rows * (ROW + HASH_BUILD) + rows_now * HASH_PROBE;
                if probe < hash && semi.inner.take_block().is_some() {
                    let outer_exprs: Vec<&Expr> = bound.iter().map(|i| &pairs[*i].0).collect();
                    let inner = self.lookup_inner(
                        &simple.table,
                        &simple.alias,
                        id,
                        kid,
                        &outer_exprs,
                        map,
                    )?;
                    let inner_map = move |s: usize| width + s;
                    let mut conds: Vec<Expr> = simple
                        .filters
                        .iter()
                        .map(|f| exprs::remap(f, &inner_map))
                        .collect();
                    for (outer_e, inner_e) in &pairs {
                        conds.push(Expr::binary(
                            BinOp::Eq,
                            exprs::remap(outer_e, map),
                            exprs::remap(inner_e, &inner_map),
                        ));
                    }
                    let on = match exprs::and_all(conds) {
                        Some(c) => Some(compile(&c, &self.session)?),
                        None => None,
                    };
                    let mut row = ExplainRow::new(id, select_type, &simple.alias);
                    row.access = Some(if unique { "eq_ref" } else { "ref" }.into());
                    if let Some(def) = simple.table.key(kid) {
                        row.key = Some(def.name.clone());
                        row.possible_keys = Some(def.name.clone());
                        row.key_len =
                            Some(key_len(&simple.table, &def.parts[..bound.len()]).to_string());
                    }
                    row.r#ref = Some(vec!["func"; bound.len()].join(","));
                    row.rows = Some(rows.max(1.0).round() as u64);
                    row.extra.push("Using where".into());
                    row.extra.push(note.into());
                    explain_rows.push(row);
                    return Ok(Box::new(NestedLoopJoin::new(source, inner, on, kind)));
                }
            }
        }
        self.ready_derived(&semi.inner, None, "MATERIALIZED")?;
        let mut lk = Vec::new();
        let mut rk = Vec::new();
        for (e, col) in &semi.keys {
            lk.push(self.compile_at(e, map)?);
            rk.push(compile(
                &slot_ref(*col, "k", &semi.inner.types[*col]),
                &self.session,
            )?);
        }
        let mut row = ExplainRow::new(id, select_type, &format!("<subquery{}>", semi.id));
        row.access = Some("hash".into());
        row.rows = Some(semi.inner.est_rows.get().max(1.0) as u64);
        row.extra.push(note.into());
        explain_rows.push(row);
        Ok(Box::new(HashJoin::new(
            source,
            Box::new(DerivedSource::new(semi.inner.clone())),
            lk,
            rk,
            None,
            kind,
            &self.session,
            self.memory.clone(),
        )?))
    }

    /// Adds the value of a per-row scalar subquery as a column.
    fn apply_source(
        &mut self,
        source: Box<dyn RowSource>,
        apply: &Apply,
        map: &dyn Fn(usize) -> usize,
        width: usize,
        explain_rows: &mut Vec<ExplainRow>,
    ) -> Result<Box<dyn RowSource>> {
        let RelKind::Table(table) = &apply.rel.kind else {
            return Err(unsupported("a per-row subquery over a derived table"));
        };
        let inner_map = move |s: usize| width + s;
        let mut row = ExplainRow::new(apply.id, "DEPENDENT SUBQUERY", &apply.rel.alias);
        let inner: Box<dyn InnerSource> = match self.bind_key(table, &apply.keys) {
            Some((kid, bound, unique, rows)) => {
                let outer_exprs: Vec<&Expr> = bound.iter().map(|i| &apply.keys[*i].0).collect();
                row.access = Some(if unique { "eq_ref" } else { "ref" }.into());
                if let Some(def) = table.key(kid) {
                    row.key = Some(def.name.clone());
                    row.possible_keys = Some(def.name.clone());
                    row.key_len = Some(key_len(table, &def.parts[..bound.len()]).to_string());
                }
                row.r#ref = Some(vec!["func"; bound.len()].join(","));
                row.rows = Some(rows.max(1.0).round() as u64);
                self.lookup_inner(table, &apply.rel.alias, apply.id, kid, &outer_exprs, map)?
            }
            None => {
                let counters = Counters::new();
                self.accesses.push(AccessStat {
                    id: apply.id,
                    table: apply.rel.alias.clone(),
                    counters: counters.clone(),
                });
                row.access = Some("ALL".into());
                row.rows = Some(self.storage.table_rows(table).max(1));
                Box::new(CachedInner::new(Box::new(ScanSource {
                    storage: self.storage.clone(),
                    table: table.clone(),
                    key: KeyId::Primary,
                    ranges: vec![KeyRange::all()],
                    reverse: false,
                    fetch: Fetch::Row,
                    counters,
                    cursor: None,
                })))
            }
        };
        row.extra.push("Using where".into());
        explain_rows.push(row);
        let mut conds: Vec<Expr> = apply
            .filters
            .iter()
            .map(|f| exprs::remap(f, &inner_map))
            .collect();
        let mut keys = Vec::with_capacity(apply.keys.len());
        for (outer_e, inner_e) in &apply.keys {
            keys.push(self.compile_at(outer_e, map)?);
            conds.push(Expr::binary(
                BinOp::Eq,
                exprs::remap(outer_e, map),
                exprs::remap(inner_e, &inner_map),
            ));
        }
        let on = match exprs::and_all(conds) {
            Some(c) => Some(compile(&c, &self.session)?),
            None => None,
        };
        let mut aggs = Vec::with_capacity(apply.aggs.len());
        for a in &apply.aggs {
            aggs.push(ApplyAgg {
                kind: a.kind,
                arg: match &a.arg {
                    Some(e) => Some(self.compile_at(e, &inner_map)?),
                    None => None,
                },
                distinct: a.distinct,
                separator: a.separator.clone(),
            });
        }
        let item = if apply.grouped {
            let rewritten = exprs::rewrite(&apply.item, &mut |e| match e {
                Expr::Column(c) if c.slot >= AGG_BASE => Some(slot_ref(
                    c.slot - AGG_BASE,
                    &c.name,
                    &apply.aggs[c.slot - AGG_BASE].ty,
                )),
                _ => None,
            });
            if exprs::slots(&rewritten)
                .iter()
                .any(|s| *s >= apply.aggs.len())
            {
                return Err(unsupported(
                    "a column outside the aggregates of a per-row subquery",
                ));
            }
            compile(&rewritten, &self.session)?
        } else {
            self.compile_at(&apply.item, &inner_map)?
        };
        let mut first_by = Vec::with_capacity(apply.first_by.len());
        for (e, desc) in &apply.first_by {
            first_by.push((self.compile_at(e, &inner_map)?, *desc));
        }
        let mut joined_types = source.types();
        joined_types.extend(table_types(table));
        Ok(Box::new(ApplyOp {
            outer: source,
            inner,
            on,
            keys,
            aggs,
            grouped: apply.grouped,
            item,
            first_by,
            ty: apply.ty,
            session: self.session.clone(),
            memory: self.memory.clone(),
            joined_types,
            cache: std::collections::HashMap::new(),
        }))
    }

    /// `ONLY_FULL_GROUP_BY`: every loose column must belong to a table
    /// whose primary key (or a unique key without `NULL`s) is grouped.
    fn check_dependent(
        &self,
        rels: &[Rel],
        groups: &[Expr],
        loose: &[Expr],
        items: &[Expr],
    ) -> Result<()> {
        for l in loose {
            let Expr::Column(c) = l else { continue };
            let owner = rels
                .iter()
                .find(|r| c.slot >= r.base && c.slot < r.base + r.width);
            let decided = owner.is_some_and(|rel| {
                let RelKind::Table(table) = &rel.kind else {
                    return false;
                };
                let grouped = |col: usize| {
                    groups
                        .iter()
                        .any(|g| matches!(g, Expr::Column(gc) if gc.slot == rel.base + col))
                };
                table.keys().any(|(id, def)| {
                    (id == KeyId::Primary || table.is_strict_unique(id))
                        && def
                            .parts
                            .iter()
                            .all(|p| p.prefix.is_none() && grouped(p.column))
                })
            });
            if decided {
                continue;
            }
            let position = items
                .iter()
                .position(|i| exprs::slots(i).contains(&c.slot))
                .map_or(1, |p| p + 1);
            let name = owner.map_or_else(
                || c.name.clone(),
                |r| match &r.qualified {
                    Some((s, _)) => format!("{s}.{}.{}", r.alias, c.name),
                    None => format!("{}.{}", r.alias, c.name),
                },
            );
            return Err(if groups.is_empty() {
                SqlError::new(
                    1140,
                    "42000",
                    format!(
                        "In aggregated query without GROUP BY, expression #{position} of SELECT list contains nonaggregated column '{name}'; this is incompatible with sql_mode=only_full_group_by"
                    ),
                )
            } else {
                SqlError::new(
                    1055,
                    "42000",
                    format!(
                        "Expression #{position} of SELECT list is not in GROUP BY clause and contains nonaggregated column '{name}' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by"
                    ),
                )
            });
        }
        Ok(())
    }

    /// The best way for item `r` to join the items in `placed`.
    fn join_method(
        &self,
        rels: &[Rel],
        infos: &[RelInfo],
        filters: &[Expr],
        placed: u64,
        prefix_rows: f64,
        r: usize,
    ) -> Result<(Method, f64, f64)> {
        let rel = &rels[r];
        let info = &infos[r];
        let me = 1u64 << r;
        let conds: Vec<&Expr> = match &rel.outer_on {
            Some(on) => on.iter().collect(),
            None => filters
                .iter()
                .filter(|f| {
                    let m = mask_of(rels, f);
                    m & me != 0 && m & !(placed | me) == 0
                })
                .collect(),
        };
        // Equalities between this item and the earlier ones.
        let mut eqs: Vec<(Expr, Expr)> = Vec::new();
        for c in &conds {
            if let Expr::Binary(BinOp::Eq, a, b) = c {
                let (ma, mb) = (mask_of(rels, a), mask_of(rels, b));
                if ma == me && mb & !placed == 0 {
                    eqs.push(((**b).clone(), (**a).clone()));
                } else if mb == me && ma & !placed == 0 {
                    eqs.push(((**a).clone(), (**b).clone()));
                }
            }
        }
        let best = &info.paths[info.best];
        let local_sel = if info.table_rows > 0.0 {
            (info.out_rows / info.table_rows).min(1.0)
        } else {
            1.0
        };
        let other_sel: f64 = conds
            .iter()
            .filter(|c| mask_of(rels, c) != me && !matches!(c, Expr::Binary(BinOp::Eq, ..)))
            .map(|c| selectivity(c))
            .product();
        let floor = |fan: f64| {
            if rel.outer_on.is_some() {
                fan.max(1.0)
            } else {
                fan
            }
        };
        let mut options: Vec<(Method, f64, f64)> = Vec::new();

        if let RelKind::Table(table) = &rel.kind {
            for (kid, def) in table.keys() {
                let mut bound = Vec::new();
                let mut driven = false;
                let mut exact = true;
                for part in &def.parts {
                    let col_ty = &table.columns[part.column].ty;
                    let hit = eqs.iter().find(|(outer_e, inner_e)| {
                        matches!(inner_e, Expr::Column(c) if c.slot == rel.base + part.column)
                            && self
                                .type_of(outer_e)
                                .is_ok_and(|ty| key_usable(col_ty, &ty))
                    });
                    match hit {
                        Some((outer_e, _)) => {
                            driven |= mask_of(rels, outer_e) != 0;
                            exact &= part.prefix.is_none()
                                && self
                                    .type_of(outer_e)
                                    .is_ok_and(|ty| lookup_exact(col_ty, &ty));
                            bound.push(outer_e.clone());
                        }
                        None => break,
                    }
                }
                if bound.is_empty() || !driven {
                    continue;
                }
                let whole = bound.len() == def.parts.len();
                let unique = whole
                    && def.unique
                    && (table.is_strict_unique(kid)
                        || def.parts.iter().all(|p| p.prefix.is_none()));
                let rows = if unique {
                    1.0
                } else {
                    self.storage
                        .rows_per_key(table, kid, bound.len())
                        .unwrap_or_else(|| {
                            (info.table_rows / 10f64.powi(bound.len() as i32)).max(1.0)
                        })
                };
                let held: BTreeSet<usize> = table
                    .ordering_parts(kid)
                    .iter()
                    .filter(|p| key_holds(table, p))
                    .map(|p| p.column)
                    .collect();
                let covering =
                    kid != KeyId::Primary && info.needed.iter().all(|c| held.contains(c));
                let per_row = match (kid, covering) {
                    (KeyId::Primary, _) => ROW,
                    (_, true) => ENTRY,
                    (_, false) => ENTRY + FETCH,
                };
                let cost = prefix_rows * (SEEK + rows * per_row);
                let fan = floor(rows * local_sel * other_sel);
                options.push((
                    Method::Lookup(Lookup {
                        key: kid,
                        exprs: bound,
                        unique,
                        exact,
                        rows,
                        covering,
                    }),
                    cost,
                    fan,
                ));
            }
        }
        let driven_eqs: Vec<(Expr, Expr)> = eqs
            .iter()
            .filter(|(o, _)| mask_of(rels, o) != 0)
            .cloned()
            .collect();
        if !driven_eqs.is_empty() && self.options.hash_join {
            let cost = best.cost + info.out_rows * HASH_BUILD + prefix_rows * HASH_PROBE;
            // Rows per probe: by the key's rows-per-value when the join
            // column leads a key, else a tenth of the table.
            let mut per_value = (info.out_rows / 10.0).max(1.0);
            if let (RelKind::Table(table), Some((_, Expr::Column(c)))) =
                (&rel.kind, driven_eqs.first())
            {
                let col = c.slot - rel.base;
                for (kid, def) in table.keys() {
                    if def.parts.first().is_some_and(|p| p.column == col) {
                        if let Some(v) = self.storage.rows_per_key(table, kid, 1) {
                            per_value = v * local_sel;
                        }
                    }
                }
            }
            options.push((
                Method::Hash(driven_eqs),
                cost,
                floor(per_value.min(info.out_rows.max(1.0)) * other_sel),
            ));
        }
        {
            let cost = best.cost + prefix_rows * info.out_rows.max(1.0) * 0.2;
            let joined = if conds.iter().any(|c| mask_of(rels, c) != me) {
                0.33
            } else {
                1.0
            };
            options.push((Method::Replay, cost, floor(info.out_rows * joined)));
        }
        let mut pick = 0;
        for (i, o) in options.iter().enumerate() {
            if o.1 + EPS < options[pick].1 {
                pick = i;
            }
        }
        Ok(options.swap_remove(pick))
    }

    /// Searches the join order: every left-deep order for a few tables,
    /// the cheapest next table at each step for many.
    fn join_order(
        &self,
        rels: &[Rel],
        infos: &[RelInfo],
        filters: &[Expr],
        eliminated: u64,
    ) -> Result<Vec<Step>> {
        let n = rels.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        struct Search<'s, 'a> {
            cx: &'s Cx<'a>,
            rels: &'s [Rel],
            infos: &'s [RelInfo],
            filters: &'s [Expr],
            best: Option<(f64, Vec<Step>)>,
            greedy: bool,
            /// The items left out of the join.
            eliminated: u64,
        }
        impl Search<'_, '_> {
            fn candidates(&self, placed: u64, rows: f64, cost: f64) -> Result<Vec<Step>> {
                let n = self.rels.len();
                let mut out = Vec::new();
                for r in 0..n {
                    if placed & (1 << r) != 0 || self.infos[r].deps & !placed != 0 {
                        continue;
                    }
                    let info = &self.infos[r];
                    let step = if placed == self.eliminated {
                        if self.rels[r].outer_on.is_some() {
                            continue;
                        }
                        Step {
                            rel: r,
                            method: Method::Scan,
                            rows: info.out_rows,
                            cost: info.paths[info.best].cost,
                        }
                    } else {
                        let (method, c, fan) = self.cx.join_method(
                            self.rels,
                            self.infos,
                            self.filters,
                            placed,
                            rows,
                            r,
                        )?;
                        Step {
                            rel: r,
                            method,
                            rows: rows * fan,
                            cost: cost + c + rows * fan * 0.1,
                        }
                    };
                    out.push(step);
                }
                out.sort_by(|a, b| {
                    a.rows
                        .partial_cmp(&b.rows)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then(a.rel.cmp(&b.rel))
                });
                Ok(out)
            }

            fn go(
                &mut self,
                placed: u64,
                rows: f64,
                cost: f64,
                path: &mut Vec<Step>,
            ) -> Result<()> {
                let n = self.rels.len() - self.eliminated.count_ones() as usize;
                if path.len() == n {
                    if self.best.as_ref().is_none_or(|(c, _)| cost + EPS < *c) {
                        self.best = Some((cost, path.clone()));
                    }
                    return Ok(());
                }
                let mut cands = self.candidates(placed, rows, cost)?;
                if self.greedy {
                    cands.sort_by(|a, b| {
                        a.cost
                            .partial_cmp(&b.cost)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    cands.truncate(1);
                }
                for step in cands {
                    if self
                        .best
                        .as_ref()
                        .is_some_and(|(c, _)| step.cost + EPS >= *c)
                    {
                        continue;
                    }
                    let (r, rows2, cost2) = (step.rel, step.rows, step.cost);
                    path.push(step);
                    self.go(placed | (1 << r), rows2, cost2, path)?;
                    path.pop();
                }
                Ok(())
            }
        }
        let mut search = Search {
            cx: self,
            rels,
            infos,
            filters,
            best: None,
            greedy: n > EXHAUSTIVE_TABLES,
            eliminated,
        };
        let mut path = Vec::with_capacity(n);
        search.go(eliminated, 1.0, 0.0, &mut path)?;
        match search.best {
            Some((_, steps)) => Ok(steps),
            None => Err(unsupported("this combination of outer joins")),
        }
    }

    fn limit_value(&mut self, e: &ast::Expr, scope: &Scope<'_>) -> Result<u64> {
        let mut x = crate::resolve::ExprCx::new(scope, Clause::Where);
        let e = self.expr(e, &mut x)?;
        let bad = || {
            SqlError::new(
                1064,
                "42000",
                "You have an error in your SQL syntax near 'LIMIT'",
            )
        };
        match self.eval_const(&e) {
            Some((Value::Int(i), _)) if i >= 0 => Ok(i as u64),
            Some((Value::UInt(u), _)) => Ok(u),
            _ => Err(bad()),
        }
    }
}
