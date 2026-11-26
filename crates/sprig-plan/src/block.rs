//! From a parsed `SELECT` to a resolved query block: the `FROM` items with
//! their joins, and every clause's expressions bound to the block's slots.

use sprig_engine::row::Value;
use sprig_exec::ops::Aggregate;
use sprig_exec::{BinOp, ColumnRef, Expr, Result, SqlError, compile};
use sqlparser::ast::{self, Value as SqlValue};

use crate::exprs;
use crate::plan::Cx;
use crate::resolve::{
    AGG_BASE, AggSpec, Apply, Block, Clause, Derived, ExprCx, Item, OUTER_BASE, Rel, RelCol,
    RelKind, Scope, SemiJoin, SimpleInner, StarCol, WinSpec, derived_cols, object_name, table_col,
    unknown_column, unsupported,
};

/// The `FROM` clause as it is built up.
#[derive(Default)]
struct From {
    rels: Vec<Rel>,
    star: Vec<StarCol>,
    filters: Vec<Expr>,
    next_slot: usize,
    /// Inner joins' `ON` conditions that hold a subquery: resolved with
    /// the block's `WHERE` (an inner join's `ON` filters the same rows),
    /// where a scalar subquery may read the row it is computed for.
    deferred_on: Vec<ast::Expr>,
}

/// Whether `e` holds a subquery.
fn has_subquery(e: &ast::Expr) -> bool {
    let mut found = false;
    let _ = ast::visit_expressions(e, |x: &ast::Expr| {
        if matches!(
            x,
            ast::Expr::Subquery(_) | ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. }
        ) {
            found = true;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    found
}

fn nullable_cols(rel: &mut Rel) {
    for c in &mut rel.cols {
        c.expr = exprs::rewrite(&c.expr, &mut |e| match e {
            Expr::Column(r) => Some(Expr::Column(ColumnRef {
                nullable: true,
                ..r.clone()
            })),
            _ => None,
        });
    }
}

fn split_and<'e>(e: &'e ast::Expr, out: &mut Vec<&'e ast::Expr>) {
    match e {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOperator::And,
            right,
        } => {
            split_and(left, out);
            split_and(right, out);
        }
        ast::Expr::Nested(inner) => split_and(inner, out),
        other => out.push(other),
    }
}

impl Cx<'_> {
    fn add_rel(&mut self, from: &mut From, mut rel: Rel) -> Result<usize> {
        if from
            .rels
            .iter()
            .any(|r| !r.hidden && r.alias.eq_ignore_ascii_case(&rel.alias))
        {
            return Err(SqlError::new(
                1066,
                "42000",
                format!("Not unique table/alias: '{}'", rel.alias),
            ));
        }
        rel.base = from.next_slot;
        let base = rel.base;
        for c in &mut rel.cols {
            c.expr = exprs::remap(&c.expr, &|s| s + base);
        }
        for p in &mut rel.pending {
            *p = exprs::remap(p, &|s| s + base);
        }
        from.next_slot += rel.width;
        from.rels.push(rel);
        Ok(from.rels.len() - 1)
    }

    /// A relation from a table factor, with slots counted from zero (the
    /// caller places it). `merge` allows a derived table to become its
    /// underlying table; `inner` says the relation is the inner side of a
    /// left join.
    fn relation(&mut self, tf: &ast::TableFactor, merge: bool, inner: bool) -> Result<Rel> {
        match tf {
            ast::TableFactor::Table {
                name, alias, args, ..
            } => {
                if args.is_some() {
                    return Err(unsupported("a table function"));
                }
                let parts = object_name(name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                if let [single] = parts.as_slice() {
                    let shown = alias_name.clone().unwrap_or_else(|| single.clone());
                    if let Some(rel) = self.working_rel(single, shown.clone()) {
                        return Ok(rel);
                    }
                    if let Some(def) = self.find_cte(single) {
                        if def.recursive {
                            return self.recursive_rel(&def, shown);
                        }
                        let columns: Vec<String> = def
                            .cte
                            .alias
                            .columns
                            .iter()
                            .map(|c| c.name.value.clone())
                            .collect();
                        // Its query sees only the expressions defined
                        // before it.
                        let later = self.ctes.split_off(def.sees.min(self.ctes.len()));
                        let maria = self.session.persona.is_maria();
                        let rel = self
                            .derived_rel(&def.cte.query, &columns, shown, merge, inner)
                            .and_then(|rel| {
                                let width = rel.cols.len();
                                crate::recursive::check_column_list(maria, columns.len(), width)
                                    .map(|()| rel)
                            });
                        self.ctes.extend(later);
                        return rel;
                    }
                }
                let (schema, table) = match parts.as_slice() {
                    [t] => (
                        self.catalog
                            .default_schema()
                            .ok_or_else(|| SqlError::new(1046, "3D000", "No database selected"))?,
                        t.clone(),
                    ),
                    [s, t] => (s.clone(), t.clone()),
                    _ => return Err(unsupported("a table name of more than two parts")),
                };
                let def = self.catalog.table(&schema, &table).ok_or_else(|| {
                    SqlError::new(
                        1146,
                        "42S02",
                        format!("Table '{schema}.{table}' doesn't exist"),
                    )
                })?;
                let cols = def
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| RelCol {
                        name: c.name.clone(),
                        expr: table_col(0, i, c),
                    })
                    .collect();
                let alias = alias_name.unwrap_or_else(|| table.clone());
                Ok(Rel {
                    shown: alias.clone(),
                    alias,
                    qualified: Some((schema, table)),
                    width: def.columns.len(),
                    kind: RelKind::Table(def),
                    cols,
                    base: 0,
                    outer_on: None,
                    outer_dep: None,
                    pending: Vec::new(),
                    hidden: false,
                    derived_id: None,
                })
            }
            ast::TableFactor::Derived {
                lateral: false,
                subquery,
                alias,
                ..
            } => {
                let Some(alias) = alias else {
                    return Err(SqlError::new(
                        1248,
                        "42000",
                        "Every derived table must have its own alias",
                    ));
                };
                let columns: Vec<String> =
                    alias.columns.iter().map(|c| c.name.value.clone()).collect();
                self.derived_rel(subquery, &columns, alias.name.value.clone(), merge, inner)
            }
            other => Err(unsupported(&format!("{other} in FROM"))),
        }
    }

    /// A derived table (or a reference to a common table expression) as a
    /// relation. A query that only filters and projects one stored table
    /// is merged: the relation is that table, with the query's columns as
    /// names for expressions over it, so the table's keys stay usable.
    /// Anything else is planned on its own and its rows are held.
    fn derived_rel(
        &mut self,
        q: &ast::Query,
        columns: &[String],
        alias: String,
        merge: bool,
        inner: bool,
    ) -> Result<Rel> {
        let rename = |mut names: Vec<String>| -> Result<Vec<String>> {
            for (i, c) in columns.iter().enumerate() {
                if i < names.len() {
                    names[i] = c.clone();
                }
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
            Ok(names)
        };
        let simple = match &*q.body {
            ast::SetExpr::Select(select)
                if q.with.is_none()
                    && q.fetch.is_none()
                    && q.for_clause.is_none()
                    && q.pipe_operators.is_empty() =>
            {
                Some(select)
            }
            _ => None,
        };
        let Some(select) = simple else {
            let planned = self.query(q, None, "DERIVED")?;
            let names = rename(planned.names.clone())?;
            let id = planned.id;
            let derived = Derived::new(planned.source, planned.est_rows);
            let cols = derived_cols(0, &names, &derived.types);
            return Ok(Rel {
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
            });
        };
        let id = self.next_id();
        let mut block = self.select_block(
            select,
            q.order_by.as_ref(),
            q.limit_clause.as_ref(),
            None,
            false,
            id,
        )?;
        let names = rename(block.items.iter().map(|i| i.name.clone()).collect())?;
        let plain = block.rels.len() == 1
            && matches!(block.rels[0].kind, RelKind::Table(_))
            && block.rels[0].outer_on.is_none()
            && block.semis.is_empty()
            && block.applies.is_empty()
            && !block.grouped
            && !block.distinct
            && block.limit.is_none()
            && block.offset == 0
            && block.items.iter().all(|i| !exprs::is_volatile(&i.expr))
            // Under a left join a computed column would be computed on
            // the NULL row too: only plain columns pass through.
            && (!inner || block.items.iter().all(|i| matches!(i.expr, Expr::Column(_))));
        if merge && plain {
            let rel = block.rels.remove(0);
            let mut pending = rel.pending;
            pending.extend(block.filters);
            return Ok(Rel {
                shown: rel.shown,
                alias,
                qualified: None,
                kind: rel.kind,
                width: rel.width,
                cols: names
                    .into_iter()
                    .zip(block.items)
                    .map(|(name, item)| RelCol {
                        name,
                        expr: item.expr,
                    })
                    .collect(),
                base: 0,
                outer_on: None,
                outer_dep: None,
                pending,
                hidden: false,
                derived_id: None,
            });
        }
        let types = block
            .items
            .iter()
            .map(|i| self.type_of(&i.expr))
            .collect::<Result<Vec<_>>>()?;
        let cols = derived_cols(0, &names, &types);
        Ok(Rel {
            shown: alias.clone(),
            alias,
            qualified: None,
            width: cols.len(),
            kind: RelKind::Derived(Derived::lazy(block, types)),
            cols,
            base: 0,
            outer_on: None,
            outer_dep: None,
            pending: Vec::new(),
            hidden: false,
            derived_id: Some(id),
        })
    }

    fn from_clause(
        &mut self,
        tables: &[ast::TableWithJoins],
        outer: Option<&Scope<'_>>,
    ) -> Result<From> {
        // `FROM DUAL` reads no table.
        let dual = |t: &ast::TableWithJoins| {
            t.joins.is_empty()
                && matches!(&t.relation, ast::TableFactor::Table { name, alias: None, args: None, .. }
                    if matches!(name.0.as_slice(), [p] if p.as_ident().is_some_and(|i| i.quote_style.is_none() && i.value.eq_ignore_ascii_case("dual"))))
        };
        let tables = match tables {
            [t] if dual(t) => &[],
            _ => tables,
        };
        let flat;
        let tables = match flatten_nested(tables) {
            Some(f) => {
                flat = f;
                flat.as_slice()
            }
            None => tables,
        };
        let maria = self.session.persona.is_maria();
        let mut from = From::default();
        for twj in tables {
            // A RIGHT JOIN turns the first relation into an inner side
            // after the fact: it is then kept as written.
            let any_right = twj.joins.iter().any(|j| {
                matches!(
                    j.join_operator,
                    ast::JoinOperator::Right(_) | ast::JoinOperator::RightOuter(_)
                )
            });
            // A table function reads the items before it.
            let first = if matches!(twj.relation, ast::TableFactor::JsonTable { .. }) {
                let scope = Scope {
                    rels: &from.rels,
                    star: &from.star,
                    visible: 0..from.rels.len(),
                    outer,
                };
                self.json_table_rel(&twj.relation, &scope)?
            } else {
                self.relation(&twj.relation, !any_right, false)?
            };
            let first = self.add_rel(&mut from, first)?;
            let pending = std::mem::take(&mut from.rels[first].pending);
            from.filters.extend(pending);
            let chain_start = first;
            let cols_of = |from: &From, r: usize| -> Vec<StarCol> {
                (0..from.rels[r].cols.len())
                    .map(|col| StarCol { rel: r, col })
                    .collect()
            };
            let mut chain = cols_of(&from, first);
            for join in &twj.joins {
                use ast::JoinOperator as J;
                let (constraint, left, right) = match &join.join_operator {
                    J::Join(c) | J::Inner(c) | J::CrossJoin(c) | J::StraightJoin(c) => {
                        (c, false, false)
                    }
                    J::Left(c) | J::LeftOuter(c) => (c, true, false),
                    J::Right(c) | J::RightOuter(c) => (c, false, true),
                    other => return Err(unsupported(&format!("{other:?}"))),
                };
                let rel = if matches!(join.relation, ast::TableFactor::JsonTable { .. }) {
                    let star: Vec<StarCol> = from.star.iter().chain(&chain).copied().collect();
                    let scope = Scope {
                        rels: &from.rels,
                        star: &star,
                        visible: 0..from.rels.len(),
                        outer,
                    };
                    self.json_table_rel(&join.relation, &scope)?
                } else {
                    self.relation(&join.relation, true, left)?
                };
                let r = self.add_rel(&mut from, rel)?;
                let pending = std::mem::take(&mut from.rels[r].pending);
                if right && r - chain_start != 1 {
                    return Err(unsupported(
                        "RIGHT JOIN after another join (write it as a LEFT JOIN)",
                    ));
                }
                let right_cols = cols_of(&from, r);
                let mut conds = Vec::new();
                match constraint {
                    ast::JoinConstraint::None => chain.extend(right_cols),
                    ast::JoinConstraint::On(e) if !left && !right && has_subquery(e) => {
                        chain.extend(right_cols);
                        from.deferred_on.push(e.clone());
                    }
                    ast::JoinConstraint::On(e) => {
                        chain.extend(right_cols);
                        let scope = Scope {
                            rels: &from.rels,
                            star: &chain,
                            visible: chain_start..r + 1,
                            outer,
                        };
                        let mut x = ExprCx::new(&scope, Clause::On);
                        let e = self.expr(e, &mut x)?;
                        exprs::conjuncts(e, &mut conds);
                    }
                    ast::JoinConstraint::Using(_) | ast::JoinConstraint::Natural => {
                        let names: Vec<String> = match constraint {
                            ast::JoinConstraint::Using(names) => names
                                .iter()
                                .map(|n| object_name(n).map(|p| p.join(".")))
                                .collect::<Result<_>>()?,
                            _ => {
                                let mut common = Vec::new();
                                for sc in &chain {
                                    let name = &from.rels[sc.rel].cols[sc.col].name;
                                    if from.rels[r]
                                        .cols
                                        .iter()
                                        .any(|c| c.name.eq_ignore_ascii_case(name))
                                        && !common
                                            .iter()
                                            .any(|c: &String| c.eq_ignore_ascii_case(name))
                                    {
                                        common.push(name.clone());
                                    }
                                }
                                common
                            }
                        };
                        let mut left_used = Vec::new();
                        let mut right_used = Vec::new();
                        for name in &names {
                            let missing = || {
                                SqlError::new(
                                    1054,
                                    "42S22",
                                    format!(
                                        "Unknown column '{name}' in '{}'",
                                        if maria { "FROM" } else { "from clause" }
                                    ),
                                )
                            };
                            let mut found = None;
                            for sc in &chain {
                                if from.rels[sc.rel].cols[sc.col]
                                    .name
                                    .eq_ignore_ascii_case(name)
                                {
                                    if found.is_some() {
                                        return Err(SqlError::new(
                                            1052,
                                            "23000",
                                            format!(
                                                "Column '{name}' in {} is ambiguous",
                                                if maria { "FROM" } else { "from clause" }
                                            ),
                                        ));
                                    }
                                    found = Some(*sc);
                                }
                            }
                            let l = found.ok_or_else(missing)?;
                            let rc = from.rels[r]
                                .cols
                                .iter()
                                .position(|c| c.name.eq_ignore_ascii_case(name))
                                .ok_or_else(missing)?;
                            conds.push(Expr::binary(
                                BinOp::Eq,
                                from.rels[l.rel].cols[l.col].expr.clone(),
                                from.rels[r].cols[rc].expr.clone(),
                            ));
                            left_used.push(l);
                            right_used.push(StarCol { rel: r, col: rc });
                        }
                        let left_rest: Vec<StarCol> = chain
                            .iter()
                            .copied()
                            .filter(|sc| !left_used.contains(sc))
                            .collect();
                        let right_rest: Vec<StarCol> = right_cols
                            .iter()
                            .copied()
                            .filter(|sc| !right_used.contains(sc))
                            .collect();
                        chain = if right {
                            right_used
                                .into_iter()
                                .chain(right_rest)
                                .chain(left_rest)
                                .collect()
                        } else {
                            left_used
                                .into_iter()
                                .chain(left_rest)
                                .chain(right_rest)
                                .collect()
                        };
                    }
                }
                if left {
                    // A merged derived table's own conditions belong to
                    // the inner side: they join the ON condition.
                    conds.extend(pending);
                    from.rels[r].outer_on = Some(conds);
                    from.rels[r].outer_dep = Some(r - 1);
                    nullable_cols(&mut from.rels[r]);
                } else if right {
                    from.filters.extend(pending);
                    from.rels[chain_start].outer_on = Some(conds);
                    from.rels[chain_start].outer_dep = Some(r);
                    nullable_cols(&mut from.rels[chain_start]);
                } else {
                    from.filters.extend(pending);
                    from.filters.extend(conds);
                }
            }
            from.star.extend(chain);
        }
        Ok(from)
    }

    pub(crate) fn const_u64(&mut self, e: &ast::Expr, scope: &Scope<'_>) -> Result<u64> {
        let mut x = ExprCx::new(scope, Clause::Where);
        let e = self.expr(e, &mut x)?;
        let bad = || {
            SqlError::new(
                1064,
                "42000",
                "You have an error in your SQL syntax near 'LIMIT'",
            )
        };
        if !exprs::is_const(&e) {
            return Err(bad());
        }
        let c = compile(&e, &self.session)?;
        let mut ctx = sprig_exec::Ctx::new(&self.session);
        match c.eval(&mut ctx, &[])? {
            Value::Int(i) if i >= 0 => Ok(i as u64),
            Value::UInt(u) => Ok(u),
            _ => Err(bad()),
        }
    }

    /// Turns `x IN (SELECT ...)` or `[NOT] EXISTS (SELECT ...)` at the top
    /// level of `WHERE` into a semi-join, when the subquery is one block
    /// tied to this one by equalities only. `None` leaves the predicate to
    /// the general path.
    fn semi_join(&mut self, e: &ast::Expr, scope: &Scope<'_>) -> Result<Option<SemiJoin>> {
        let (subquery, anti, outer_exprs) = match e {
            ast::Expr::InSubquery {
                expr,
                subquery,
                negated: false,
            } => {
                let parts: Vec<&ast::Expr> = match &**expr {
                    ast::Expr::Tuple(items) => items.iter().collect(),
                    other => vec![other],
                };
                let mut out = Vec::new();
                for p in parts {
                    let mut x = ExprCx::new(scope, Clause::Where);
                    out.push(self.expr(p, &mut x)?);
                }
                (subquery, false, out)
            }
            ast::Expr::Exists { subquery, negated } => (subquery, *negated, Vec::new()),
            _ => return Ok(None),
        };
        let exists = outer_exprs.is_empty();
        let ast::SetExpr::Select(select) = &*subquery.body else {
            return Ok(None);
        };
        if subquery.with.is_some() || subquery.limit_clause.is_some() || subquery.fetch.is_some() {
            return Ok(None);
        }
        let id = self.next_id();
        let mut block = self.select_block(select, None, None, Some(scope), true, id)?;
        if !block.correlated.is_empty() && (block.grouped || block.distinct) {
            return Err(unsupported("a grouped subquery that reads its outer query"));
        }
        if exists {
            block.items.clear();
            block.order_by.clear();
        } else if block.items.len() != outer_exprs.len() {
            return Err(SqlError::new(
                1241,
                "21000",
                format!("Operand should contain {} column(s)", outer_exprs.len()),
            ));
        }
        let mut keys: Vec<(Expr, usize)> = outer_exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| (e, i))
            .collect();
        for (outer_e, inner_e) in std::mem::take(&mut block.correlated) {
            keys.push((outer_e, block.items.len()));
            block.items.push(Item {
                name: format!("k{}", block.items.len()),
                expr: inner_e,
                alias: None,
            });
        }
        // One stored table with conditions can be probed by key.
        let simple = (block.rels.len() == 1
            && block.rels[0].outer_on.is_none()
            && block.semis.is_empty()
            && block.applies.is_empty()
            && !block.grouped
            && !block.distinct
            && block.limit.is_none()
            && block.offset == 0)
            .then(|| match &block.rels[0].kind {
                RelKind::Table(table) => Some(SimpleInner {
                    table: table.clone(),
                    alias: block.rels[0].alias.clone(),
                    items: block.items.iter().map(|i| i.expr.clone()).collect(),
                    filters: block.filters.clone(),
                }),
                RelKind::Derived(_) | RelKind::JsonTable(_) => None,
            })
            .flatten();
        let types = block
            .items
            .iter()
            .map(|i| self.type_of(&i.expr))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(SemiJoin {
            inner: Derived::lazy(block, types),
            keys,
            anti,
            id,
            simple,
        }))
    }

    /// Resolves one `SELECT` into a block. `order_by` and `limit` are the
    /// enclosing query's, when the `SELECT` is its whole body. With
    /// `tied`, equalities between this block and the outer one are taken
    /// out of `WHERE` into [`Block::correlated`].
    pub(crate) fn select_block(
        &mut self,
        select: &ast::Select,
        order_by: Option<&ast::OrderBy>,
        limit: Option<&ast::LimitClause>,
        outer: Option<&Scope<'_>>,
        tied: bool,
        id: usize,
    ) -> Result<Block> {
        let maria = self.session.persona.is_maria();
        if select.top.is_some()
            || select.into.is_some()
            || !select.lateral_views.is_empty()
            || select.qualify.is_some()
            || !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
        {
            return Err(unsupported("this SELECT clause"));
        }
        let From {
            rels,
            star,
            mut filters,
            deferred_on,
            ..
        } = self.from_clause(&select.from, outer)?;
        let mut semis = Vec::new();
        let mut correlated = Vec::new();
        let mut applies: Vec<Apply> = Vec::new();
        let mut windows: Vec<WinSpec> = Vec::new();

        // WHERE.
        if let Some(selection) = &select.selection {
            let mut parts = Vec::new();
            split_and(selection, &mut parts);
            for part in parts {
                let scope = Scope {
                    rels: &rels,
                    star: &star,
                    visible: 0..rels.len(),
                    outer,
                };
                // A shape the semi-join doesn't take is left to the
                // expression path (a dependent subquery there).
                let refs = self.outer_refs;
                match self.semi_join(part, &scope) {
                    Ok(Some(semi)) => {
                        semis.push(semi);
                        continue;
                    }
                    Ok(None) => {}
                    Err(e) if e.code == 1235 => self.outer_refs = refs,
                    Err(e) => return Err(e),
                }
                let before = self.outer_refs;
                let mut x = ExprCx::new(&scope, Clause::Where);
                x.applies = Some(&mut applies);
                let e = self.expr(part, &mut x)?;
                if self.outer_refs == before {
                    exprs::conjuncts(e, &mut filters);
                    continue;
                }
                // The conjunct reads the outer block.
                self.outer_refs = before;
                if !tied {
                    return Err(unsupported("a subquery that reads its outer query here"));
                }
                let is_outer = |e: &Expr| {
                    let s = exprs::slots(e);
                    !s.is_empty() && s.iter().all(|x| *x >= OUTER_BASE)
                };
                let is_inner = |e: &Expr| exprs::slots(e).iter().all(|x| *x < OUTER_BASE);
                let down = |e: &Expr| exprs::remap(e, &|s| s - OUTER_BASE);
                match e {
                    Expr::Binary(BinOp::Eq, a, b) if is_outer(&a) && is_inner(&b) => {
                        correlated.push((down(&a), *b));
                    }
                    Expr::Binary(BinOp::Eq, a, b) if is_outer(&b) && is_inner(&a) => {
                        correlated.push((down(&b), *a));
                    }
                    _ => {
                        return Err(unsupported(
                            "a subquery tied to its outer query by more than equalities",
                        ));
                    }
                }
            }
        }
        // Inner joins' ON conditions with a subquery (see `From`).
        for on in &deferred_on {
            let scope = Scope {
                rels: &rels,
                star: &star,
                visible: 0..rels.len(),
                outer,
            };
            let before = self.outer_refs;
            let mut x = ExprCx::new(&scope, Clause::On);
            x.applies = Some(&mut applies);
            let e = self.expr(on, &mut x)?;
            if self.outer_refs != before {
                return Err(unsupported("a subquery that reads its outer query here"));
            }
            exprs::conjuncts(e, &mut filters);
        }
        let refs_before = self.outer_refs;
        let scope = Scope {
            rels: &rels,
            star: &star,
            visible: 0..rels.len(),
            outer,
        };

        // The select list.
        let mut aggs: Vec<AggSpec> = Vec::new();
        let mut items: Vec<Item> = Vec::new();
        for item in &select.projection {
            match item {
                ast::SelectItem::Wildcard(_) => {
                    if rels.iter().all(|r| r.hidden) {
                        return Err(SqlError::new(1096, "HY000", "No tables used"));
                    }
                    for sc in &star {
                        if rels[sc.rel].hidden || invisible(&rels[sc.rel], sc.col) {
                            continue;
                        }
                        let c = &rels[sc.rel].cols[sc.col];
                        items.push(Item {
                            name: c.name.clone(),
                            expr: c.expr.clone(),
                            alias: Some(c.name.clone()),
                        });
                    }
                }
                ast::SelectItem::QualifiedWildcard(kind, _) => {
                    let ast::SelectItemQualifiedWildcardKind::ObjectName(name) = kind else {
                        return Err(unsupported("this wildcard"));
                    };
                    let parts = object_name(name)?;
                    let table = parts.last().cloned().unwrap_or_default();
                    let rel = rels
                        .iter()
                        .find(|r| !r.hidden && r.alias.eq_ignore_ascii_case(&table))
                        .ok_or_else(|| {
                            // MariaDB names the database too.
                            let shown = match (maria, parts.len(), self.catalog.default_schema()) {
                                (true, 1, Some(db)) => format!("{db}.{table}"),
                                (true, _, _) => parts.join("."),
                                _ => table.clone(),
                            };
                            SqlError::new(1051, "42S02", format!("Unknown table '{shown}'"))
                        })?;
                    for (ci, c) in rel.cols.iter().enumerate() {
                        if invisible(rel, ci) {
                            continue;
                        }
                        items.push(Item {
                            name: c.name.clone(),
                            expr: c.expr.clone(),
                            alias: Some(c.name.clone()),
                        });
                    }
                }
                ast::SelectItem::UnnamedExpr(e) => {
                    let mut x = ExprCx::new(&scope, Clause::Select);
                    x.aggs = Some(&mut aggs);
                    x.applies = Some(&mut applies);
                    x.windows = Some(&mut windows);
                    x.named = &select.named_window;
                    let expr = self.expr(e, &mut x)?;
                    let (name, alias) = match e {
                        ast::Expr::Identifier(i) => (i.value.clone(), Some(i.value.clone())),
                        ast::Expr::CompoundIdentifier(p) => {
                            let n = p.last().map(|i| i.value.clone()).unwrap_or_default();
                            (n.clone(), Some(n))
                        }
                        // `_cs 'text'`: named by its text, as a plain string is.
                        ast::Expr::Prefixed { prefix, value } if prefix.value.starts_with('_') => {
                            match &**value {
                                ast::Expr::Value(v) => match &v.value {
                                    SqlValue::SingleQuotedString(t)
                                    | SqlValue::DoubleQuotedString(t) => (t.clone(), None),
                                    _ => (e.to_string(), None),
                                },
                                _ => (e.to_string(), None),
                            }
                        }
                        other => (other.to_string(), None),
                    };
                    items.push(Item { name, expr, alias });
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let mut x = ExprCx::new(&scope, Clause::Select);
                    x.aggs = Some(&mut aggs);
                    x.applies = Some(&mut applies);
                    x.windows = Some(&mut windows);
                    x.named = &select.named_window;
                    let expr = self.expr(expr, &mut x)?;
                    items.push(Item {
                        name: alias.value.clone(),
                        expr,
                        alias: Some(alias.value.clone()),
                    });
                }
                ast::SelectItem::ExprWithAliases { .. } => {
                    return Err(unsupported("several aliases for one expression"));
                }
            }
        }

        let position = |e: &ast::Expr, clause: Clause| -> Result<Option<usize>> {
            let ast::Expr::Value(v) = e else {
                return Ok(None);
            };
            let SqlValue::Number(n, _) = &v.value else {
                return Ok(None);
            };
            match n.parse::<usize>() {
                Ok(p) if p >= 1 && p <= items.len() => Ok(Some(p - 1)),
                _ => Err(unknown_column(n, clause, maria)),
            }
        };

        // GROUP BY.
        let mut group_by = Vec::new();
        let mut rollup = false;
        match &select.group_by {
            ast::GroupByExpr::Expressions(list, modifiers) => {
                match modifiers.as_slice() {
                    [] => {}
                    [ast::GroupByWithModifier::Rollup] if !list.is_empty() => rollup = true,
                    _ => return Err(unsupported("GROUP BY modifiers")),
                }
                for e in list {
                    if let Some(p) = position(e, Clause::GroupBy)? {
                        group_by.push(items[p].expr.clone());
                        continue;
                    }
                    let mut x = ExprCx::new(&scope, Clause::GroupBy);
                    x.items = Some(&items);
                    match self.expr(e, &mut x) {
                        Ok(g) => group_by.push(g),
                        // MySQL names what can't be grouped on.
                        Err(err) if err.code == 1111 && !maria => {
                            return Err(SqlError::new(
                                1056,
                                "42000",
                                format!("Can't group on '{e}'"),
                            ));
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
            ast::GroupByExpr::All(_) => return Err(unsupported("GROUP BY ALL")),
        }
        for g in &group_by {
            if exprs::slots(g)
                .iter()
                .any(|s| *s >= AGG_BASE && *s < OUTER_BASE)
            {
                return Err(SqlError::new(1056, "42000", "Can't group on an aggregate"));
            }
        }

        // HAVING.
        let mut having = None;
        if let Some(h) = &select.having {
            let mut x = ExprCx::new(&scope, Clause::Having);
            x.aggs = Some(&mut aggs);
            x.items = Some(&items);
            x.alias_first = true;
            having = Some(self.expr(h, &mut x)?);
        }

        // ORDER BY.
        let mut order = Vec::new();
        if let Some(ob) = order_by {
            let ast::OrderByKind::Expressions(list) = &ob.kind else {
                return Err(unsupported("ORDER BY ALL"));
            };
            for o in list {
                if o.with_fill.is_some() || o.options.nulls_first.is_some() {
                    return Err(unsupported("this ORDER BY option"));
                }
                let desc = o.options.sort == Some(ast::OrderBySort::Desc);
                if let Some(p) = position(&o.expr, Clause::OrderBy)? {
                    order.push((items[p].expr.clone(), desc));
                    continue;
                }
                let mut x = ExprCx::new(&scope, Clause::OrderBy);
                x.aggs = Some(&mut aggs);
                x.items = Some(&items);
                x.alias_first = true;
                x.windows = Some(&mut windows);
                x.named = &select.named_window;
                order.push((self.expr(&o.expr, &mut x)?, desc));
            }
        }

        // LIMIT.
        let (mut lim, mut off) = (None, 0);
        match limit {
            None => {}
            Some(ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            }) => {
                if !limit_by.is_empty() {
                    return Err(unsupported("LIMIT BY"));
                }
                if let Some(l) = limit {
                    lim = Some(self.const_u64(l, &scope)?);
                }
                if let Some(o) = offset {
                    off = self.const_u64(&o.value, &scope)?;
                }
            }
            Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                off = self.const_u64(offset, &scope)?;
                lim = Some(self.const_u64(limit, &scope)?);
            }
        }

        let distinct = match &select.distinct {
            None | Some(ast::Distinct::All) => false,
            Some(ast::Distinct::Distinct) => true,
            Some(ast::Distinct::On(_)) => return Err(unsupported("DISTINCT ON")),
        };
        let grouped = !group_by.is_empty() || !aggs.is_empty();
        // Without grouping, HAVING is one more filter.
        if !grouped {
            if let Some(h) = having.take() {
                exprs::conjuncts(h, &mut filters);
            }
        }
        // The aggregates' own types were needed while resolving; check
        // them once more now that every clause is known.
        for a in &aggs {
            let arg = match &a.arg {
                Some(e) => Some(compile(e, &self.session)?),
                None => None,
            };
            Aggregate::new(a.kind, arg, a.distinct, &self.session)?;
        }
        if self.outer_refs != refs_before {
            self.outer_refs = refs_before;
            return Err(unsupported("a subquery that reads its outer query here"));
        }
        // MariaDB doesn't order a rollup.
        if rollup && maria && !order.is_empty() {
            return Err(SqlError::new(
                1221,
                "HY000",
                "Incorrect usage of CUBE/ROLLUP and ORDER BY",
            ));
        }
        Ok(Block {
            rels,
            filters,
            semis,
            applies,
            windows,
            items,
            group_by,
            aggs,
            having,
            order_by: order,
            distinct,
            limit: lim,
            offset: off,
            grouped,
            rollup,
            id,
            correlated,
        })
    }
}

/// Whether column `col` of `rel` is a table's invisible column, which `*`
/// leaves out (it is still found by name).
fn invisible(rel: &Rel, col: usize) -> bool {
    if let RelKind::Table(t) = &rel.kind {
        t.columns.get(col).is_some_and(|c| c.invisible)
    } else {
        false
    }
}

/// Whether `op` is an inner join (`JOIN`, `INNER JOIN`, `CROSS JOIN`).
fn inner_join(op: &ast::JoinOperator) -> bool {
    use ast::JoinOperator as J;
    matches!(op, J::Join(_) | J::Inner(_) | J::CrossJoin(_))
}

/// `FROM` items with parenthesized inner joins written out flat: `(a JOIN
/// b ON x) LEFT JOIN c ON y` is `a JOIN b ON x LEFT JOIN c ON y` (the
/// parenthesized joins come first anyway); when every join involved is an
/// inner join, `a JOIN (b JOIN c ON x) ON y` is
/// `a CROSS JOIN b JOIN c ON (x) AND (y)` (the same rows: every condition
/// of inner joins is a condition on their product). `None` when there is
/// no such nesting.
fn flatten_nested(tables: &[ast::TableWithJoins]) -> Option<Vec<ast::TableWithJoins>> {
    use ast::{JoinConstraint, JoinOperator as J, TableFactor};
    let nested = |f: &TableFactor| match f {
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } if table_with_joins
            .joins
            .iter()
            .all(|j| inner_join(&j.join_operator)) =>
        {
            Some((**table_with_joins).clone())
        }
        _ => None,
    };
    // The first item of a join chain may hold any joins: joins associate
    // to the left, so `(a LEFT JOIN b ON x) JOIN c ON y` is
    // `a LEFT JOIN b ON x JOIN c ON y` as written.
    let first_nested = |f: &TableFactor| match f {
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } => Some((**table_with_joins).clone()),
        _ => None,
    };
    let mut changed = false;
    let mut out = Vec::with_capacity(tables.len());
    for twj in tables {
        let all_inner = twj.joins.iter().all(|j| inner_join(&j.join_operator));
        let mut cur = twj.clone();
        // The first item: whatever joins follow, its joins come first
        // (left to right), so they can be written out flat.
        while let Some(inner) = first_nested(&cur.relation) {
            let mut joins = inner.joins;
            joins.extend(cur.joins);
            cur = ast::TableWithJoins {
                relation: inner.relation,
                joins,
            };
            changed = true;
        }
        if all_inner {
            let mut joins = Vec::with_capacity(cur.joins.len());
            for j in cur.joins {
                match nested(&j.relation) {
                    Some(inner) => {
                        let on = match &j.join_operator {
                            J::Join(JoinConstraint::On(e)) | J::Inner(JoinConstraint::On(e)) => {
                                Some(e.clone())
                            }
                            J::Join(JoinConstraint::None)
                            | J::Inner(JoinConstraint::None)
                            | J::CrossJoin(JoinConstraint::None) => None,
                            _ => {
                                joins.push(j);
                                continue;
                            }
                        };
                        changed = true;
                        let mut inner_joins = vec![ast::Join {
                            relation: inner.relation,
                            global: false,
                            join_operator: J::CrossJoin(JoinConstraint::None),
                        }];
                        inner_joins.extend(inner.joins);
                        let n = inner_joins.len();
                        for (k, mut ij) in inner_joins.into_iter().enumerate() {
                            if k + 1 == n
                                && let Some(e) = &on
                            {
                                let cond = match &ij.join_operator {
                                    J::Join(JoinConstraint::On(x))
                                    | J::Inner(JoinConstraint::On(x)) => ast::Expr::BinaryOp {
                                        left: Box::new(ast::Expr::Nested(Box::new(x.clone()))),
                                        op: ast::BinaryOperator::And,
                                        right: Box::new(ast::Expr::Nested(Box::new(e.clone()))),
                                    },
                                    _ => e.clone(),
                                };
                                ij.join_operator = J::Join(JoinConstraint::On(cond));
                            }
                            joins.push(ij);
                        }
                    }
                    None => joins.push(j),
                }
            }
            cur.joins = joins;
        }
        out.push(cur);
    }
    changed.then_some(out)
}
