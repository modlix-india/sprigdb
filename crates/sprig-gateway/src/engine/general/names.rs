//! Reading a general `SELECT` before the planner sees it: which engine
//! tables it names, which of their columns it may read, and where the
//! columns of its result come from.
//!
//! ```text
//! analyze(statement):
//!   database names in their physical form (t<tenant>_d<db>_g<g>_s<s>)
//!     → the logical names, in table names and column qualifiers
//!   every table name that isn't a common table expression's:
//!     an engine table            → a TableUse
//!     a table elsewhere, a view  → 39960 (none of it runs)
//!   every column the text names → marked on each table that could own it:
//!     `c`      on every table of the statement with a column `c`
//!     `q.c`    on every table read under the name `q`
//!     `*`      every column of the tables in that SELECT's FROM
//!     `q.*`    every column of the tables read under `q` there
//!     USING (c), NATURAL: as `c`; every column of that FROM's tables
//! ```
//!
//! The marks are a superset of what the statement reads: SQL has no way
//! to read a column without naming it or a `*` over its table. The
//! privilege check runs on the marks, and storage hands the planner the
//! marked columns only (the others read as `NULL`), so a column that was
//! not checked can't reach a result even if a mark were missed. A name
//! shared by two tables of the statement marks both, which can ask for a
//! privilege the servers wouldn't (only a session with column privileges
//! but no table privilege could tell).
//!
//! **Column types the expression engine doesn't compute with** (`ENUM`,
//! `SET`, `BIT`, `JSON`: it would see their stored numbers or bytes): such
//! a column may only pass through as a plain select-list column of the
//! outermost `SELECT`, where the result encoders of the single-table path
//! print it; named anywhere else, the statement is refused with 39960.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use sprig_common::{Result, SprigError};
use sprig_plan::catalog::{
    Catalog, ColumnDef as PlanColumn, IndexDef, IndexPart, TableDef as PlanTable,
};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, JoinConstraint, JoinOperator,
    ObjectName, ObjectNamePart, Query, Select, SelectItem, SelectItemQualifiedWildcardKind,
    SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue, ValueWithSpan, Visit,
    VisitMut, Visitor, VisitorMut,
};

use super::General;
use crate::engine::not_on_engine;
use crate::engine::plan::{BindCtx, physical_db};
use crate::engine::table::{ColType, TableDef};

/// One engine table a statement reads.
#[derive(Debug)]
pub(crate) struct TableUse {
    /// Its definition.
    pub def: Arc<TableDef>,
    /// The database's logical name.
    pub db_name: String,
    /// The table as the planner sees it.
    pub plan: Arc<PlanTable>,
    /// The columns the statement may read (what the session's privileges
    /// are checked for, and all that storage hands over).
    pub read: Vec<bool>,
    /// The same columns in the order the statement first names them (the
    /// order a missing privilege is reported in).
    pub order: Vec<usize>,
    /// The names the statement reads the table under, in lower case.
    pub aliases: Vec<String>,
    /// The columns a `*` of a select list names: a missing privilege on
    /// one of them denies the whole table (1142), as the servers answer.
    pub starred: Vec<bool>,
}

impl TableUse {
    fn mark(&mut self, c: usize) {
        if !self.read[c] {
            self.read[c] = true;
            self.order.push(c);
        }
    }

    fn mark_all(&mut self, star: bool) {
        for c in 0..self.read.len() {
            self.mark(c);
            self.starred[c] |= star;
        }
    }
}

/// Where a result column comes from: a column of a table, unchanged.
#[derive(Debug, Clone)]
pub(crate) struct Origin {
    /// The table's position in [`General::tables`].
    pub table: usize,
    /// The column's position in the table.
    pub column: usize,
    /// The name the statement reads the table under.
    pub alias: String,
    /// The table is the inner side of an outer join: its columns can be
    /// `NULL` in the result whatever their definition says.
    pub outer: bool,
    /// Placed by a `*` over a join that merges columns (`USING`,
    /// `NATURAL`): the result column must carry the column's name, or no
    /// column of the result is placed (checked against the planner's).
    pub merged: bool,
}

/// What [`analyze`] found.
pub(super) struct Analysis {
    pub(super) stmt: Statement,
    pub(super) tables: Vec<TableUse>,
    pub(super) origins: Vec<Option<Origin>>,
    pub(super) written: Vec<Option<String>>,
    pub(super) params: usize,
}

fn ident_of(p: &ObjectNamePart) -> Option<&Ident> {
    match p {
        ObjectNamePart::Identifier(i) => Some(i),
        ObjectNamePart::Function(_) => None,
    }
}

fn last_name(n: &ObjectName) -> Option<String> {
    n.0.last()
        .and_then(ident_of)
        .map(|i| i.value.to_lowercase())
}

/// Database names in their logical form.
struct Logical<'a, 'b> {
    cx: &'a BindCtx<'b>,
}

impl Logical<'_, '_> {
    fn fix(&self, i: &mut Ident) {
        if let Some(db) = physical_db(self.cx.tenant, &i.value) {
            i.value = self.cx.names.db_name(db);
        }
    }

    fn fix_name(&self, n: &mut ObjectName, with_column: bool) {
        let want = if with_column { 3 } else { 2 };
        if n.0.len() == want
            && let Some(ObjectNamePart::Identifier(i)) = n.0.first_mut()
        {
            self.fix(i);
        }
    }
}

impl VisitorMut for Logical<'_, '_> {
    type Break = ();

    fn pre_visit_relation(&mut self, n: &mut ObjectName) -> ControlFlow<()> {
        self.fix_name(n, false);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        match e {
            Expr::CompoundIdentifier(p) if p.len() == 3 => self.fix(&mut p[0]),
            Expr::QualifiedWildcard(n, _) => self.fix_name(n, false),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
        for item in &mut s.projection {
            if let SelectItem::QualifiedWildcard(
                SelectItemQualifiedWildcardKind::ObjectName(n),
                _,
            ) = item
            {
                self.fix_name(n, false);
            }
        }
        ControlFlow::Continue(())
    }
}

/// A column the text names, or a run of them.
enum Mark {
    /// `[q.]c`; `plain`: as a select-list column of the outermost SELECT.
    Column {
        qual: Option<String>,
        name: String,
        plain: bool,
    },
    /// Every column of these tables (`*`, or a `NATURAL` join's); `plain`
    /// as above; `star`: a select list's `*`.
    All {
        tables: Vec<usize>,
        plain: bool,
        star: bool,
    },
    /// Every column of the tables read under `q`, anywhere.
    AllUnder(String),
}

/// A base table in a `FROM`, with the name it is read under.
struct Factor {
    table: usize,
    alias: String,
    outer: bool,
}

struct Walk<'a, 'b> {
    cx: &'a BindCtx<'b>,
    /// Names of common table expressions, in lower case.
    ctes: HashSet<String>,
    tables: Vec<TableUse>,
    /// The statement names a table that is no engine table.
    elsewhere: bool,
    err: Option<SprigError>,
    marks: Vec<Mark>,
    /// The outermost SELECT, when the statement is one (no set operation).
    top: Option<*const Select>,
    /// Its plain column items.
    plain: HashSet<*const Expr>,
    params: usize,
}

impl Walk<'_, '_> {
    fn fail(&mut self, e: SprigError) {
        if self.err.is_none() {
            self.err = Some(e);
        }
    }

    /// The engine table a name in a `FROM` means, registered; `None` for
    /// a common table expression, and for a table elsewhere (noted).
    fn table_of(&mut self, name: &ObjectName, alias: Option<&Ident>) -> Option<usize> {
        let parts: Option<Vec<&Ident>> = name.0.iter().map(ident_of).collect();
        let (db, table) = match parts.as_deref() {
            // `FROM DUAL` reads no table.
            Some([t]) if crate::engine::plan::is_dual(t) => return None,
            Some([t]) => {
                let db = self.cx.current.filter(|d| self.cx.names.engine_db(*d));
                // A common table expression's name, unless a table of the
                // current database has it too (the reference may be
                // outside the expression's reach: counted, to be safe).
                if self.ctes.contains(&t.value.to_lowercase())
                    && db.is_none_or(|d| self.cx.names.table(d, &t.value).is_none())
                {
                    return None;
                }
                (self.cx.current, &t.value)
            }
            Some([s, t]) => (
                physical_db(self.cx.tenant, &s.value)
                    .or_else(|| self.cx.names.db_by_name(&s.value)),
                &t.value,
            ),
            _ => {
                self.fail(not_on_engine("a table name of more than two parts"));
                return None;
            }
        };
        let Some(db) = db.filter(|d| self.cx.names.engine_db(*d)) else {
            self.elsewhere = true;
            return None;
        };
        let Some(parsed) = self.cx.names.table(db, table) else {
            if self.cx.names.other_relation(db, table) {
                self.fail(not_on_engine("a view or another relation kind"));
            } else {
                self.fail(SprigError::standard(
                    1146,
                    *b"42S02",
                    format!(
                        "Table '{}.{table}' doesn't exist",
                        self.cx.names.db_name(db)
                    ),
                ));
            }
            return None;
        };
        let def = match &*parsed {
            Ok(def) => Arc::clone(def),
            Err(why) => {
                self.fail(not_on_engine(why));
                return None;
            }
        };
        let at = if let Some(at) = self.tables.iter().position(|t| t.def.object == def.object) {
            at
        } else {
            let db_name = self.cx.names.db_name(db);
            self.tables.push(TableUse {
                plan: Arc::new(plan_table(&def, &db_name)),
                read: vec![false; def.columns.len()],
                order: Vec::new(),
                aliases: Vec::new(),
                starred: vec![false; def.columns.len()],
                db_name,
                def,
            });
            self.tables.len() - 1
        };
        let alias = alias.map_or_else(|| table.to_lowercase(), |a| a.value.to_lowercase());
        if !self.tables[at].aliases.contains(&alias) {
            self.tables[at].aliases.push(alias);
        }
        Some(at)
    }

    /// The base tables of a `FROM` item, with the names they are read
    /// under. `derived`: set when the item holds something else (a derived
    /// table, a common table expression, a nested join).
    fn factors(&mut self, f: &TableFactor, outer: bool, out: &mut Vec<Factor>, derived: &mut bool) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                let a = alias.as_ref().map(|a| &a.name);
                match self.table_of(name, a) {
                    Some(table) => out.push(Factor {
                        table,
                        alias: a.map_or_else(
                            || {
                                name.0
                                    .last()
                                    .and_then(ident_of)
                                    .map_or_else(String::new, |i| i.value.clone())
                            },
                            |a| a.value.clone(),
                        ),
                        outer,
                    }),
                    None => *derived = true,
                }
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                *derived = true;
                self.read_from_item(table_with_joins, out, derived, &mut false);
            }
            _ => *derived = true,
        }
    }

    /// The base tables of one comma-separated `FROM` item. `merged`: set
    /// when a join of it has `USING` or is `NATURAL` (its `*` shows the
    /// shared columns once).
    fn read_from_item(
        &mut self,
        t: &TableWithJoins,
        out: &mut Vec<Factor>,
        derived: &mut bool,
        merged: &mut bool,
    ) {
        let first = out.len();
        self.factors(&t.relation, false, out, derived);
        for j in &t.joins {
            let (constraint, inner_side, flips) = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::CrossJoin(c)
                | JoinOperator::StraightJoin(c) => (Some(c), false, false),
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (Some(c), true, false),
                JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (Some(c), false, true),
                JoinOperator::FullOuter(c) => (Some(c), true, true),
                // Not MariaDB's or MySQL's: the planner refuses them. Read
                // as widely as a NATURAL join, to be safe.
                _ => (None, true, true),
            };
            if flips {
                for f in &mut out[first..] {
                    f.outer = true;
                }
            }
            self.factors(&j.relation, inner_side, out, derived);
            match constraint {
                Some(JoinConstraint::Using(cols)) => {
                    *merged = true;
                    for c in cols {
                        if let Some(name) = last_name(c) {
                            self.marks.push(Mark::Column {
                                qual: None,
                                name,
                                plain: false,
                            });
                        }
                    }
                }
                Some(JoinConstraint::Natural) | None => {
                    *merged = true;
                    let tables = out[first..].iter().map(|f| f.table).collect();
                    self.marks.push(Mark::All {
                        tables,
                        plain: false,
                        star: false,
                    });
                }
                Some(JoinConstraint::On(_) | JoinConstraint::None) => {}
            }
        }
    }

    /// The base tables of a SELECT's `FROM`, and whether it holds anything
    /// else or a join that merges columns.
    fn read_from(&mut self, s: &Select) -> (Vec<Factor>, bool) {
        let mut out = Vec::new();
        let (mut derived, mut merged) = (false, false);
        for t in &s.from {
            self.read_from_item(t, &mut out, &mut derived, &mut merged);
        }
        (out, derived || merged)
    }
}

impl Visitor for Walk<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.ctes.insert(c.alias.name.value.to_lowercase());
            }
        }
        if !q.locks.is_empty() || q.fetch.is_some() || q.for_clause.is_some() {
            self.fail(not_on_engine(
                "FETCH, or a locking read with a join, grouping, an expression or a subquery",
            ));
        }
        if matches!(&*q.body, SetExpr::Table(_)) {
            self.fail(not_on_engine("the TABLE statement"));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        let top = self.top.is_some_and(|t| std::ptr::eq(t, s));
        let (factors, _) = self.read_from(s);
        for item in &s.projection {
            match item {
                SelectItem::Wildcard(_) => self.marks.push(Mark::All {
                    tables: factors.iter().map(|f| f.table).collect(),
                    plain: top,
                    star: true,
                }),
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(n),
                    _,
                ) => {
                    let q = last_name(n).unwrap_or_default();
                    self.marks.push(Mark::All {
                        tables: factors
                            .iter()
                            .filter(|f| f.alias.eq_ignore_ascii_case(&q))
                            .map(|f| f.table)
                            .collect(),
                        plain: top,
                        star: true,
                    });
                }
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }
                    if top && matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) =>
                {
                    self.plain.insert(std::ptr::from_ref(e));
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, alias, .. } = f {
            self.table_of(name, alias.as_ref().map(|a| &a.name));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        let plain = self.plain.contains(&std::ptr::from_ref(e));
        match e {
            Expr::Identifier(i) => self.marks.push(Mark::Column {
                qual: None,
                name: i.value.to_lowercase(),
                plain,
            }),
            Expr::CompoundIdentifier(p) => {
                if let [.., q, c] = p.as_slice() {
                    self.marks.push(Mark::Column {
                        qual: Some(q.value.to_lowercase()),
                        name: c.value.to_lowercase(),
                        plain,
                    });
                } else if let [c] = p.as_slice() {
                    self.marks.push(Mark::Column {
                        qual: None,
                        name: c.value.to_lowercase(),
                        plain,
                    });
                }
            }
            Expr::QualifiedWildcard(n, _) => {
                if let Some(q) = last_name(n) {
                    self.marks.push(Mark::AllUnder(q));
                }
            }
            Expr::MatchAgainst { columns, .. } => {
                for c in columns {
                    if let Some(name) = last_name(c) {
                        self.marks.push(Mark::Column {
                            qual: None,
                            name,
                            plain: false,
                        });
                    }
                }
            }
            Expr::Function(f) => {
                if let FunctionArguments::List(l) = &f.args {
                    for a in &l.args {
                        let (FunctionArg::Unnamed(x)
                        | FunctionArg::Named { arg: x, .. }
                        | FunctionArg::ExprNamed { arg: x, .. }) = a;
                        if let FunctionArgExpr::QualifiedWildcard(n) = x
                            && let Some(q) = last_name(n)
                        {
                            self.marks.push(Mark::AllUnder(q));
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, v: &ValueWithSpan) -> ControlFlow<()> {
        if matches!(&v.value, SqlValue::Placeholder(_)) {
            self.params += 1;
        }
        ControlFlow::Continue(())
    }
}

/// Column types whose stored values the expression engine would misread.
pub(crate) fn opaque(ty: ColType) -> Option<&'static str> {
    match ty {
        ColType::Enum { .. } => Some("ENUM"),
        ColType::Set { .. } => Some("SET"),
        ColType::Bit { .. } => Some("BIT"),
        ColType::Json => Some("JSON"),
        _ => None,
    }
}

/// The reserved name of a hidden row number in the planner's tables.
pub(crate) const HIDDEN_ROW: &str = "_sprig_hidden_row";

/// `def` as the planner sees it, in database `db_name`.
pub(crate) fn plan_table(def: &TableDef, db_name: &str) -> PlanTable {
    let part = |column: usize, kc: &sprig_engine::keys::typed::KeyColumn| IndexPart {
        column,
        prefix: kc.prefix,
        desc: kc.desc,
    };
    let mut columns: Vec<PlanColumn> = def
        .columns
        .iter()
        .map(|c| PlanColumn {
            name: c.name.clone(),
            ty: c.ty,
            nullable: c.nullable,
            invisible: c.invisible,
        })
        .collect();
    // A table keyed by a hidden row number shows it as one more invisible
    // column under a reserved name: a write through a join finds its rows
    // by it (`multiwrite`).
    if def.hidden {
        columns.push(PlanColumn {
            name: HIDDEN_ROW.to_owned(),
            ty: crate::engine::table::HIDDEN_TYPE,
            nullable: false,
            invisible: true,
        });
    }
    PlanTable {
        schema: db_name.to_owned(),
        name: def.name.clone(),
        columns,
        // A table keyed by a hidden row number has no primary key the
        // planner could use.
        primary: (!def.hidden).then(|| {
            IndexDef::new(
                &def.pk_name,
                true,
                def.pk
                    .iter()
                    .zip(&def.pk_parts)
                    .map(|(&c, kc)| part(c, kc))
                    .collect(),
            )
        }),
        indexes: def
            .indexes
            .iter()
            .map(|x| {
                IndexDef::new(
                    &x.name,
                    x.unique,
                    x.cols
                        .iter()
                        .zip(&x.parts)
                        .map(|(&c, kc)| part(c, kc))
                        .collect(),
                )
            })
            .collect(),
    }
}

/// The query a statement is: its own, or the one it explains.
fn query_of(stmt: &Statement) -> Option<&Query> {
    match stmt {
        Statement::Query(q) => Some(q),
        Statement::Explain { statement, .. } => match &**statement {
            Statement::Query(q) => Some(q),
            _ => None,
        },
        _ => None,
    }
}

/// Reads `stmt` (a `SELECT`, or an `EXPLAIN` of one): `None` when it names
/// no engine table.
///
/// # Errors
/// `ER_SPRIG_NOT_ON_ENGINE` when it names engine tables and also a table
/// elsewhere or a view, has a locking clause, or computes with a column
/// type the expression engine doesn't have; 1146.
#[allow(clippy::too_many_lines)] // one pass, in the order the checks must run
/// The columns `*` names of table `def`: all but the invisible ones.
fn star_columns(def: &crate::engine::table::TableDef) -> impl Iterator<Item = usize> + '_ {
    (0..def.columns.len()).filter(|&c| !def.columns[c].invisible)
}

#[allow(clippy::too_many_lines)] // one walk, then the marks and the origins
pub(super) fn analyze(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
    tableless: bool,
) -> Result<Option<Analysis>> {
    // The names of computed columns, from the statement as it was parsed
    // (the copy below loses nothing of it, but is read first for its
    // tables).
    let source = stmt;
    let mut stmt = stmt.clone();
    let _ = VisitMut::visit(&mut stmt, &mut Logical { cx });
    let Some(query) = query_of(&stmt) else {
        return Ok(None);
    };
    let top = match &*query.body {
        SetExpr::Select(s) => Some(std::ptr::from_ref::<Select>(s)),
        _ => None,
    };
    let mut w = Walk {
        cx,
        ctes: HashSet::new(),
        tables: Vec::new(),
        elsewhere: false,
        err: None,
        marks: Vec::new(),
        top,
        plain: HashSet::new(),
        params: 0,
    };
    let _ = Visit::visit(&stmt, &mut w);
    // The outermost SELECT's own tables, for where its columns come from
    // (read again after the walk, which registered every table).
    let top_from = match &*query.body {
        SetExpr::Select(s) => Some((w.read_from(s), &**s)),
        _ => None,
    };
    if w.tables.is_empty() && (!tableless || w.elsewhere) {
        // Not the engine's: whatever it names is MariaDB's to answer.
        return Ok(None);
    }
    if let Some(e) = w.err {
        return Err(e);
    }
    if w.elsewhere {
        return Err(not_on_engine(
            "a statement that reads tables of an engine database and of a MariaDB-backed one",
        ));
    }
    // The marks, on the tables. `computed`: named where the expression
    // engine works with the value.
    let mut computed: Vec<Vec<bool>> = w
        .tables
        .iter()
        .map(|t| vec![false; t.def.columns.len()])
        .collect();
    for m in &w.marks {
        match m {
            Mark::Column { qual, name, plain } => {
                for (ti, t) in w.tables.iter_mut().enumerate() {
                    if qual.as_ref().is_some_and(|q| !t.aliases.contains(q)) {
                        continue;
                    }
                    if let Some(c) = t.def.column(name) {
                        t.mark(c);
                        computed[ti][c] |= !plain;
                    }
                }
            }
            Mark::All {
                tables,
                plain,
                star,
            } => {
                for &ti in tables {
                    w.tables[ti].mark_all(*star);
                    if !plain {
                        computed[ti].fill(true);
                    }
                }
            }
            Mark::AllUnder(q) => {
                for (ti, t) in w.tables.iter_mut().enumerate() {
                    if t.aliases.contains(q) {
                        t.mark_all(false);
                        computed[ti].fill(true);
                    }
                }
            }
        }
    }
    // The outermost SELECT as it was parsed: its items' places in the
    // text name the computed columns.
    let parsed = match query_of(source).map(|q| &*q.body) {
        Some(SetExpr::Select(s)) => Some(&**s),
        _ => None,
    };
    let (origins, written) = top_from.map_or_else(
        || (Vec::new(), Vec::new()),
        |((factors, mixed), s)| {
            let name = |i: usize| -> Option<String> {
                let (SelectItem::UnnamedExpr(e), Some(text)) = (parsed?.projection.get(i)?, text)
                else {
                    return None;
                };
                written_name(cx, &w.tables, e, text)
            };
            origins(&w.tables, &factors, mixed, s, &name)
        },
    );
    for (ti, t) in w.tables.iter().enumerate() {
        for (ci, c) in t.def.columns.iter().enumerate() {
            let Some(kind) = opaque(c.ty) else { continue };
            // Passing through is fine where the result's encoders know
            // the column; anything else would show its stored number.
            let passes = origins
                .iter()
                .flatten()
                .any(|o| o.table == ti && o.column == ci);
            if computed[ti][ci] || (t.read[ci] && !passes) {
                return Err(not_on_engine(&format!(
                    "a column of type {kind} (`{}`.`{}`) in a join, grouping, ordering, an \
                     expression, a subquery or a set operation; only a plain select-list \
                     column of that type is served there yet",
                    t.def.name, c.name
                )));
            }
        }
    }
    let params = w.params;
    let tables = w.tables;
    Ok(Some(Analysis {
        stmt,
        tables,
        origins,
        written,
        params,
    }))
}

/// Where each column of the outermost SELECT's result comes from: one
/// entry per result column, or nothing when the count can't be told from
/// the text (a `*` over something that isn't a stored table, or over a
/// join that merges columns).
fn origins(
    tables: &[TableUse],
    factors: &[Factor],
    mixed: bool,
    s: &Select,
    written: &dyn Fn(usize) -> Option<String>,
) -> (Vec<Option<Origin>>, Vec<Option<String>>) {
    let of = |f: &Factor, column: usize| Origin {
        table: f.table,
        column,
        alias: f.alias.clone(),
        outer: f.outer,
        merged: false,
    };
    // A `*` over joins that merge columns, worked out from the text.
    let merged_star = if mixed {
        merged_columns(tables, factors, s)
    } else {
        None
    };
    let none = || (Vec::new(), Vec::new());
    let mut out = Vec::new();
    // The servers' name of each computed column (see `written_name`).
    let mut names: Vec<Option<String>> = Vec::new();
    for (at, item) in s.projection.iter().enumerate() {
        match item {
            SelectItem::Wildcard(_) => {
                if mixed {
                    match &merged_star {
                        Some(cols) => {
                            out.extend(cols.iter().cloned().map(Some));
                            continue;
                        }
                        None => return none(),
                    }
                }
                for f in factors {
                    out.extend(star_columns(&tables[f.table].def).map(|c| Some(of(f, c))));
                }
            }
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), _) => {
                let q = last_name(n).unwrap_or_default();
                let mut hits = factors.iter().filter(|f| f.alias.eq_ignore_ascii_case(&q));
                let (Some(f), None) = (hits.next(), hits.next()) else {
                    return none();
                };
                out.extend(star_columns(&tables[f.table].def).map(|c| Some(of(f, c))));
            }
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                let (qual, name) = match e {
                    Expr::Identifier(i) => (None, &i.value),
                    Expr::CompoundIdentifier(p) => match p.as_slice() {
                        [.., q, c] => (Some(&q.value), &c.value),
                        [c] => (None, &c.value),
                        [] => return none(),
                    },
                    _ => {
                        out.push(None);
                        names.resize(out.len() - 1, None);
                        names.push(if matches!(item, SelectItem::UnnamedExpr(_)) {
                            written(at)
                        } else {
                            None
                        });
                        continue;
                    }
                };
                // A name that could also be a derived table's column, or
                // that two tables have, isn't placed.
                let mut hits = factors.iter().filter(|f| {
                    qual.is_none_or(|q| f.alias.eq_ignore_ascii_case(q))
                        && tables[f.table].def.column(name).is_some()
                });
                out.push(match (hits.next(), hits.next()) {
                    (Some(f), None) if !(mixed && qual.is_none()) => {
                        tables[f.table].def.column(name).map(|c| of(f, c))
                    }
                    // An unqualified name over joins that merge columns:
                    // the one column of that name the merged `*` shows.
                    _ if qual.is_none() => merged_star.as_ref().and_then(|cols| {
                        let mut same = cols.iter().filter(|o| {
                            tables[o.table].def.columns[o.column]
                                .name
                                .eq_ignore_ascii_case(name)
                        });
                        match (same.next(), same.next()) {
                            // Resolved as the servers resolve the name.
                            (Some(o), None) => Some(Origin {
                                merged: false,
                                ..o.clone()
                            }),
                            _ => None,
                        }
                    }),
                    _ => None,
                });
            }
            _ => return none(),
        }
    }
    names.resize(out.len(), None);
    (out, names)
}

/// The columns `*` shows over the outermost `FROM` when its joins merge
/// columns (`USING`, `NATURAL`), each with the table it is read from:
/// per `FROM` item, joined left to right, the shared columns first (in
/// the left side's order), then the left side's others, then the right
/// table's; a `RIGHT JOIN` the other way round (the right table's shared
/// columns and others first), as the servers show it. `None` for anything
/// else in the `FROM` (a derived table, a nested join, a join kind not
/// listed here): the result's columns then aren't placed.
fn merged_columns(tables: &[TableUse], factors: &[Factor], s: &Select) -> Option<Vec<Origin>> {
    let mut out = Vec::new();
    let mut fi = 0;
    let base = |f: &TableFactor| matches!(f, TableFactor::Table { .. });
    let all = |fi: usize, outer: bool| -> Option<Vec<Origin>> {
        let f = factors.get(fi)?;
        Some(
            (0..tables[f.table].def.columns.len())
                .map(|column| Origin {
                    table: f.table,
                    column,
                    alias: f.alias.clone(),
                    outer,
                    merged: true,
                })
                .collect(),
        )
    };
    let name = |o: &Origin| tables[o.table].def.columns[o.column].name.to_lowercase();
    for item in &s.from {
        if !base(&item.relation) {
            return None;
        }
        let mut cur = all(fi, false)?;
        fi += 1;
        for j in &item.joins {
            if !base(&j.relation) {
                return None;
            }
            let (c, left, right) = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::CrossJoin(c)
                | JoinOperator::StraightJoin(c) => (c, false, false),
                JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, true, false),
                JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (c, false, true),
                _ => return None,
            };
            let mut new = all(fi, left)?;
            fi += 1;
            if right {
                for o in &mut cur {
                    o.outer = true;
                }
            }
            let shared: Vec<String> = match c {
                JoinConstraint::On(_) | JoinConstraint::None => Vec::new(),
                JoinConstraint::Using(cols) => cols.iter().filter_map(last_name).collect(),
                JoinConstraint::Natural => cur
                    .iter()
                    .map(name)
                    .filter(|n| new.iter().any(|o| name(o) == *n))
                    .collect(),
            };
            if shared.is_empty() {
                cur.append(&mut new);
                continue;
            }
            // Each shared name once on each side, or the text isn't read.
            let once = |v: &[Origin], n: &str| v.iter().filter(|o| name(o) == n).count() == 1;
            if !shared.iter().all(|n| once(&cur, n) && once(&new, n)) {
                return None;
            }
            let is_shared = |o: &Origin| shared.contains(&name(o));
            // A RIGHT JOIN reads as the right table LEFT JOIN the left
            // side: its shared columns, in its order, lead.
            let (first, second) = if right { (&new, &cur) } else { (&cur, &new) };
            let mut next: Vec<Origin> = first.iter().filter(|o| is_shared(o)).cloned().collect();
            next.extend(first.iter().filter(|o| !is_shared(o)).cloned());
            next.extend(second.iter().filter(|o| !is_shared(o)).cloned());
            cur = next;
        }
        out.append(&mut cur);
    }
    (fi == factors.len()).then_some(out)
}

/// The byte offset of a parser location (line and column, from 1, in
/// characters) in `text`.
fn offset(text: &str, line: u64, column: u64) -> Option<usize> {
    let mut at = 0;
    for (n, l) in text.split_inclusive('\n').enumerate() {
        if n as u64 + 1 == line {
            let chars = usize::try_from(column.checked_sub(1)?).ok()?;
            return Some(at + l.char_indices().nth(chars).map_or(l.len(), |(i, _)| i));
        }
        at += l.len();
    }
    (usize::try_from(line).ok() == Some(text.split_inclusive('\n').count() + 1) && column == 1)
        .then_some(at)
}

/// The name a computed select-list item gets on the servers: its text as
/// the client wrote it (`a+1`, not `a + 1`), a string literal's value.
/// `None` where the planner's own name stands (the text can't be cut out
/// of the statement exactly).
fn written_name(cx: &BindCtx<'_>, tables: &[TableUse], e: &Expr, text: &str) -> Option<String> {
    use sqlparser::ast::Spanned;
    if let Expr::Value(v) = e
        && let SqlValue::SingleQuotedString(lit) | SqlValue::DoubleQuotedString(lit) = &v.value
    {
        return Some(lit.clone());
    }
    // `(1)` is named `1`.
    if let Expr::Nested(inner) = e {
        return written_name(cx, tables, inner, text);
    }
    let span = e.span();
    let from = offset(text, span.start.line, span.start.column)?;
    let mut to = offset(text, span.end.line, span.end.column)?;
    // The cut must read back as the same expression. The parser's place
    // for a call can stop before its closing parenthesis: the cut then
    // grows to the next one, a few times at most.
    let same = |t: &str| {
        sqlparser::parser::Parser::new(&sqlparser::dialect::MySqlDialect {})
            .try_with_sql(t)
            .ok()
            .is_some_and(|mut p| {
                p.parse_expr().is_ok_and(|x| x == *e)
                    && p.peek_token().token == sqlparser::tokenizer::Token::EOF
            })
    };
    let mut cut = None;
    for _ in 0..8 {
        let t = text.get(from..to)?.trim();
        if same(t) {
            cut = Some(t);
            break;
        }
        to += text.get(to..)?.find(')')? + 1;
    }
    let cut = cut?;
    // A database name in its physical form reads as the logical one.
    let mut t = cut.to_owned();
    for u in tables {
        let p = format!("t{}_d{}_g", cx.tenant.0, u.def.db.0);
        while let Some(i) = t.find(&p) {
            let end = t[i..]
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .map_or(t.len(), |e| i + e);
            t.replace_range(i..end, &u.db_name);
        }
    }
    Some(t)
}

/// The planner's catalog for one statement: exactly the tables [`analyze`]
/// resolved, under the names the statement uses.
pub(super) struct PlanCatalog<'a> {
    g: &'a General,
}

impl<'a> PlanCatalog<'a> {
    pub(super) fn new(g: &'a General) -> Self {
        Self { g }
    }
}

impl Catalog for PlanCatalog<'_> {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<PlanTable>> {
        // Database names compare without case here; a statement names a
        // database in one spelling, and the tables were resolved under the
        // tenant's own rule before.
        let mut hits = self.g.tables.iter().filter(|t| {
            t.db_name.eq_ignore_ascii_case(schema) && t.def.name.eq_ignore_ascii_case(name)
        });
        let exact = self
            .g
            .tables
            .iter()
            .find(|t| t.db_name.eq_ignore_ascii_case(schema) && t.def.name == name);
        match (exact, hits.next(), hits.next()) {
            (Some(t), _, _) | (None, Some(t), None) => Some(Arc::clone(&t.plan)),
            _ => None,
        }
    }

    fn default_schema(&self) -> Option<String> {
        self.g.current.clone()
    }
}
