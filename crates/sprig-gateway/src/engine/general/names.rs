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
//! `SET`, `BIT`: it would see their stored numbers or bytes): such
//! a column may only pass through as a plain select-list column of the
//! outermost `SELECT`, where the result encoders of the single-table path
//! print it; named anywhere else, the statement is refused with 39960.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use sprig_common::ids::DbId;
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
    /// A parent tenant's table read through a share ([`Views::foreign`]):
    /// its keyspace's tenant. The share is what allows the read: no
    /// privilege of the session applies to it.
    pub tenant: Option<u64>,
    /// A MongoDB collection read as a table (`engine::collection`).
    pub coll: Option<Arc<crate::engine::collection::CollTable>>,
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
    /// Read through this view (merged): the view's name (the column's
    /// table) and the view's column name.
    pub view: Option<(String, String)>,
}

/// A view a statement reads (expanded into it as a derived table).
#[derive(Debug, Clone)]
pub(crate) struct ViewUse {
    /// The view.
    pub view: Arc<crate::engine::view::ViewDef>,
    /// Its database's logical name.
    pub db_name: String,
    /// The view whose select names this one; `None`: the statement does.
    pub under: Option<usize>,
    /// The tables its select names directly: (database, name as written).
    pub tables: Vec<(DbId, String)>,
}

/// The views of a statement, expanded, and the tables it names itself.
#[derive(Debug, Clone, Default)]
pub(crate) struct Views {
    pub uses: Vec<ViewUse>,
    /// The tables the statement names outside any view.
    pub direct: Vec<(DbId, String)>,
    /// The parent tenants whose shares the statement reads in process
    /// (`__sprig_share_<id>`, see `sprig_inherit::upstream`): each one's
    /// snapshot. Names in a parent's physical schemas resolve in it, and
    /// only inside the share's query.
    pub foreign: Vec<Arc<sprig_catalog::TenantCatalog>>,
}

impl Views {
    /// Whether table `name` of `db` is read with the session's own rights:
    /// named by the statement, or by a view of `SQL SECURITY INVOKER`
    /// that the session's rights reach.
    pub(crate) fn invoker_reads(&self, db: DbId, name: &str) -> bool {
        let same = |(d, n): &(DbId, String)| *d == db && n.eq_ignore_ascii_case(name);
        if self.uses.is_empty() || self.direct.iter().any(same) {
            return true;
        }
        self.uses
            .iter()
            .enumerate()
            .any(|(i, _)| self.uses[i].tables.iter().any(same) && self.principal(i).is_none())
    }

    /// The view (database, name) through which alone the statement reads
    /// table `name` of `db`, when it doesn't name the table itself.
    pub(crate) fn only_through(&self, db: DbId, name: &str) -> Option<(String, String)> {
        let same = |(d, n): &(DbId, String)| *d == db && n.eq_ignore_ascii_case(name);
        if self.direct.iter().any(same) {
            return None;
        }
        let mut i = self.uses.iter().position(|u| u.tables.iter().any(same))?;
        while let Some(up) = self.uses[i].under {
            i = up;
        }
        let u = &self.uses[i];
        Some((u.db_name.clone(), u.view.name.clone()))
    }

    /// Whose rights the select of view use `i` runs with: `None` for the
    /// session's, else the index of the view use whose definer it is.
    pub(crate) fn principal(&self, mut i: usize) -> Option<usize> {
        loop {
            let u = &self.uses[i];
            if !u.view.invoker {
                return Some(i);
            }
            i = u.under?;
        }
    }
}

/// Replaces each view a statement names with a derived table of its
/// select (nested views too).
struct Expand<'a, 'b> {
    cx: &'a BindCtx<'b>,
    ctes: HashSet<String>,
    out: Views,
    /// The expansions being walked: the factor's address, its use.
    stack: Vec<(*const TableFactor, usize)>,
    err: Option<SprigError>,
}

impl Expand<'_, '_> {
    /// A share read in process: `__sprig_share_<id>` in the session's
    /// schema (or, inside a share's query, in a parent's) as the derived
    /// table of the share's query (`None`: not such a name).
    fn share(&mut self, f: &TableFactor) -> Option<Result<TableFactor>> {
        let TableFactor::Table {
            name, alias, args, ..
        } = f
        else {
            return None;
        };
        if args.is_some() {
            return None;
        }
        let parts: Option<Vec<&Ident>> = name.0.iter().map(ident_of).collect();
        let parts = parts?;
        let (schema, table) = match parts.as_slice() {
            [t] => (None, *t),
            [s, t] => (Some(*s), *t),
            _ => return None,
        };
        let id: u64 = table.value.strip_prefix("__sprig_share_")?.parse().ok()?;
        // Whose share view this is: the session's tenant, or a parent whose
        // share's query is being read.
        let tenant = match schema {
            None => self.cx.tenant,
            Some(s)
                if physical_db(self.cx.tenant, &s.value).is_some()
                    || self.cx.names.db_by_name(&s.value).is_some() =>
            {
                self.cx.tenant
            }
            Some(s) => {
                let t = s
                    .value
                    .parse::<sprig_common::names::SchemaName>()
                    .ok()?
                    .tenant();
                if !self.out.foreign.iter().any(|c| c.tenant == t) {
                    return None;
                }
                t
            }
        };
        let Some(sh) = sprig_inherit::upstream::engine_share(tenant, id) else {
            return Some(Err(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{}' doesn't exist", table.value),
            )));
        };
        let parsed = sqlparser::parser::Parser::new(&sprig_plan::dialect::SprigMySql)
            .try_with_sql(&sh.sql)
            .and_then(|mut p| p.parse_query());
        let q = match parsed {
            Ok(q) => q,
            Err(e) => {
                return Some(Err(SprigError::internal(format!(
                    "a share's query doesn't parse: {e}"
                ))));
            }
        };
        if !self
            .out
            .foreign
            .iter()
            .any(|c| c.tenant == sh.parent.tenant)
        {
            self.out.foreign.push(Arc::clone(&sh.parent));
        }
        let name = alias.as_ref().map_or_else(
            || Ident::with_quote('`', table.value.clone()),
            |a| a.name.clone(),
        );
        Some(Ok(TableFactor::Derived {
            lateral: false,
            subquery: q,
            alias: Some(sqlparser::ast::TableAlias {
                explicit: true,
                at: None,
                name,
                columns: Vec::new(),
            }),
            sample: None,
        }))
    }

    fn resolve(&self, name: &ObjectName) -> Option<(DbId, String)> {
        let parts: Option<Vec<&Ident>> = name.0.iter().map(ident_of).collect();
        match parts.as_deref()? {
            [t] => {
                if self.ctes.contains(&t.value.to_lowercase()) {
                    return None;
                }
                Some((self.cx.current?, t.value.clone()))
            }
            [s, t] => Some((
                physical_db(self.cx.tenant, &s.value)
                    .or_else(|| self.cx.names.db_by_name(&s.value))?,
                t.value.clone(),
            )),
            _ => None,
        }
    }
}

impl VisitorMut for Expand<'_, '_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.ctes.insert(c.alias.name.value.to_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
        if let Some(d) = self.share(f) {
            match d {
                Ok(d) => *f = d,
                Err(e) => {
                    self.err = Some(e);
                    return ControlFlow::Break(());
                }
            }
            return ControlFlow::Continue(());
        }
        let TableFactor::Table { name, alias, .. } = f else {
            return ControlFlow::Continue(());
        };
        let Some((db, table)) = self.resolve(name) else {
            return ControlFlow::Continue(());
        };
        let under = self.stack.last().map(|(_, i)| *i);
        if self.cx.names.view(db, &table).is_none()
            && self.cx.names.table(db, &table).is_none()
            && let Some(e) = self.cx.names.sequence(db, &table)
        {
            let db_name = self.cx.names.db_name(db);
            let derived = match &e.def {
                Ok(def) => seq_derived(&db_name, &table, def, alias.as_ref())
                    .ok_or_else(|| not_on_engine("this read of a sequence")),
                Err(why) => Err(not_on_engine(why)),
            };
            match derived {
                Ok(d) => *f = d,
                Err(err) => {
                    self.err = Some(err);
                    return ControlFlow::Break(());
                }
            }
            return ControlFlow::Continue(());
        }
        let Some(v) = self.cx.names.view(db, &table) else {
            match under {
                Some(i) => self.out.uses[i].tables.push((db, table)),
                None => self.out.direct.push((db, table)),
            }
            return ControlFlow::Continue(());
        };
        match under {
            Some(i) => self.out.uses[i].tables.push((db, table.clone())),
            None => self.out.direct.push((db, table.clone())),
        }
        let db_name = self.cx.names.db_name(db);
        let Ok(v) = &*v else {
            self.err = Some(crate::engine::view::errors::invalid(&db_name, &table));
            return ControlFlow::Break(());
        };
        let v = Arc::new(v.clone());
        if self.stack.len() >= 32
            || self.stack.iter().any(|(_, i)| {
                let u = &self.out.uses[*i].view;
                u.db == v.db && u.name.eq_ignore_ascii_case(&v.name)
            })
        {
            self.err = Some(crate::engine::view::errors::recursive(&db_name, &v.name));
            return ControlFlow::Break(());
        }
        // Every table the select names must still exist.
        for (d, t) in crate::engine::view::relations(&v.query) {
            let Some(d) = d.and_then(|d| self.cx.names.db_by_name(&d)) else {
                continue;
            };
            if self.cx.names.table(d, &t).is_none()
                && self.cx.names.view(d, &t).is_none()
                && !self.ctes.contains(&t.to_lowercase())
            {
                self.err = Some(crate::engine::view::errors::invalid(&db_name, &v.name));
                return ControlFlow::Break(());
            }
        }
        let alias = alias.clone();
        *f = crate::engine::view::as_derived(&v, alias.as_ref());
        self.out.uses.push(ViewUse {
            view: v,
            db_name,
            under,
            tables: Vec::new(),
        });
        self.stack
            .push((std::ptr::from_ref(f), self.out.uses.len() - 1));
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, f: &mut TableFactor) -> ControlFlow<()> {
        if self
            .stack
            .last()
            .is_some_and(|(p, _)| std::ptr::eq(*p, std::ptr::from_ref(f)))
        {
            self.stack.pop();
        }
        ControlFlow::Continue(())
    }
}

/// The derived table standing for a read of sequence `db`.`name`.
fn seq_derived(
    db: &str,
    name: &str,
    def: &crate::engine::sequence::SeqDef,
    alias: Option<&sqlparser::ast::TableAlias>,
) -> Option<TableFactor> {
    let sql = crate::engine::sequence::as_select(db, name, def);
    let mut v =
        sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, &sql).ok()?;
    let Some(Statement::Query(q)) = v.pop() else {
        return None;
    };
    let name = alias.map_or_else(|| Ident::with_quote('`', name), |a| a.name.clone());
    Some(TableFactor::Derived {
        lateral: false,
        subquery: q,
        alias: Some(sqlparser::ast::TableAlias {
            explicit: true,
            at: None,
            name,
            columns: Vec::new(),
        }),
        sample: None,
    })
}

/// Expands the views `stmt` names (see [`Expand`]).
///
/// # Errors
/// 1356 for a view whose tables are gone, 1462 for one that reads itself.
pub(crate) fn expand_views(cx: &BindCtx<'_>, stmt: &mut Statement) -> Result<Views> {
    let mut e = Expand {
        cx,
        ctes: HashSet::new(),
        out: Views::default(),
        stack: Vec::new(),
        err: None,
    };
    let _ = VisitMut::visit(stmt, &mut e);
    match e.err {
        Some(err) => Err(err),
        None => Ok(e.out),
    }
}

/// What [`analyze`] found.
pub(super) struct Analysis {
    pub(super) views: Views,
    pub(super) stmt: Statement,
    pub(super) tables: Vec<TableUse>,
    pub(super) origins: Vec<Option<Origin>>,
    pub(super) written: Vec<Option<String>>,
    pub(super) params: usize,
    pub(super) view_named: Vec<Option<(String, String)>>,
    pub(super) sequences: Vec<SeqUse>,
}

/// A sequence a statement's sequence functions name: the database and
/// name as written (the database's logical name), and its identity.
#[derive(Debug, Clone)]
pub(crate) struct SeqUse {
    pub db_name: String,
    pub name: String,
    /// Its identity, or the error a use of the name gets (code, message).
    pub id: std::result::Result<String, (u16, String)>,
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

/// Comparisons of an `ENUM` column with string constants, written as
/// comparisons of its member numbers: `c = 'b'`, `c <> 'b'`, `c <=> 'b'`
/// and `c [NOT] IN ('a', 'b')` (the expression engine holds an `ENUM` as
/// its member number). A constant that is no member (by the column's
/// collation) becomes a number no row holds; `''` is the error value 0.
/// `c IS [NOT] NULL` needs no change. The column references so written
/// are returned: computing with them is safe (see [`Mark::Safe`]).
struct EnumLits<'a> {
    /// The statement's base tables with an `ENUM` column, under each name
    /// they are read by (lower case).
    tables: Vec<(String, Arc<TableDef>)>,
    /// Names a derived table or common table expression gives a column:
    /// an unqualified reference to one of them may not be the table's.
    derived: HashSet<String>,
    backslash: bool,
    safe: HashSet<*const Expr>,
    /// The `FROM` names of each `SELECT` being visited, innermost last.
    scopes: Vec<Scope>,
    /// The statement's common table expressions (lower case).
    ctes: HashSet<String>,
    /// Every query but the outermost one gives its `ENUM` select items as
    /// their member text (see [`Self::member_text`]), so a derived table,
    /// a common table expression or a subquery reads them as the servers
    /// do: as strings.
    texts: bool,
    /// The outermost query.
    top: Option<*const Query>,
    _cx: std::marker::PhantomData<&'a ()>,
}

/// The names one `SELECT`'s `FROM` gives its tables (lower case).
#[derive(Default)]
struct Scope {
    /// A base table's.
    base: HashSet<String>,
    /// A derived table's, a common table expression's, or another row
    /// source's.
    derived: HashSet<String>,
}

impl Scope {
    fn of(s: &Select, ctes: &HashSet<String>) -> Self {
        fn factor(f: &TableFactor, ctes: &HashSet<String>, out: &mut Scope) {
            match f {
                TableFactor::Table { name, alias, .. } => {
                    let last = name
                        .0
                        .last()
                        .and_then(ident_of)
                        .map(|i| i.value.to_lowercase());
                    let cte = name.0.len() == 1 && last.as_ref().is_some_and(|n| ctes.contains(n));
                    let q = alias.as_ref().map(|a| a.name.value.to_lowercase()).or(last);
                    if let Some(q) = q {
                        if cte {
                            out.derived.insert(q);
                        } else {
                            out.base.insert(q);
                        }
                    }
                }
                TableFactor::NestedJoin {
                    table_with_joins, ..
                } => {
                    factor(&table_with_joins.relation, ctes, out);
                    for j in &table_with_joins.joins {
                        factor(&j.relation, ctes, out);
                    }
                }
                TableFactor::Derived { alias: Some(a), .. }
                | TableFactor::JsonTable { alias: Some(a), .. } => {
                    out.derived.insert(a.name.value.to_lowercase());
                }
                _ => {}
            }
        }
        let mut out = Scope::default();
        for t in &s.from {
            factor(&t.relation, ctes, &mut out);
            for j in &t.joins {
                factor(&j.relation, ctes, &mut out);
            }
        }
        out
    }
}

impl EnumLits<'_> {
    /// The members and collation of the `ENUM` column `e` names.
    fn column(&self, e: &Expr) -> Option<(Arc<[bytes::Bytes]>, crate::engine::table::Column)> {
        let (qual, name) = match e {
            Expr::Identifier(i) => (None, i.value.to_lowercase()),
            Expr::CompoundIdentifier(p) if p.len() >= 2 => (
                Some(p[p.len() - 2].value.to_lowercase()),
                p[p.len() - 1].value.to_lowercase(),
            ),
            _ => return None,
        };
        // An unqualified name a base table of the nearest `FROM` has is
        // that table's, whatever a derived table elsewhere in the
        // statement names a column; else it may be a derived table's.
        let local: Option<&HashSet<String>> = match (&qual, self.scopes.last()) {
            (None, Some(sc))
                if self
                    .tables
                    .iter()
                    .any(|(a, d)| sc.base.contains(a) && d.column(&name).is_some()) =>
            {
                Some(&sc.base)
            }
            _ => None,
        };
        if qual.is_none() && local.is_none() && self.derived.contains(&name) {
            return None;
        }
        // A qualifier the nearest `FROM` naming it gives a derived table
        // (whatever base table elsewhere goes by the same name).
        if let Some(q) = &qual {
            for sc in self.scopes.iter().rev() {
                if sc.derived.contains(q) {
                    return None;
                }
                if sc.base.contains(q) {
                    break;
                }
            }
        }
        let mut found: Option<crate::engine::table::Column> = None;
        for (alias, def) in &self.tables {
            if qual.as_ref().is_some_and(|q| q != alias)
                || local.is_some_and(|l| !l.contains(alias))
            {
                continue;
            }
            let Some(c) = def.column(&name) else { continue };
            let col = &def.columns[c];
            if !matches!(col.ty, ColType::Enum { .. }) {
                return None;
            }
            match &found {
                Some(f) if f.members != col.members || f.collation != col.collation => {
                    return None;
                }
                _ => found = Some(col.clone()),
            }
        }
        // A qualified name of no base table is a derived table's.
        found.map(|c| (Arc::clone(&c.members), c))
    }

    /// The member number of string constant `v` (`None`: not a plain
    /// string constant).
    fn number(&self, col: &crate::engine::table::Column, v: &Expr) -> Option<i64> {
        let Expr::Value(ValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        }) = v
        else {
            return None;
        };
        if (self.backslash && s.contains('\\')) || s.contains('\'') {
            return None;
        }
        let text = crate::engine::row::Datum::Str(bytes::Bytes::from(s.clone().into_bytes()));
        let cx = crate::engine::expr::CmpCx {
            collation: col.collation,
            mb3: col.mb3,
            text_right: true,
            ..crate::engine::expr::CmpCx::default()
        };
        let env = crate::engine::assign::Env::STRICT;
        for i in 0..=col.members.len() {
            let m = crate::engine::expr::enum_text(&col.members, u16::try_from(i).ok()?);
            match crate::engine::expr::compare(
                &crate::engine::row::Datum::Str(m),
                &text,
                &cx,
                &env,
                "=",
            ) {
                Ok(Some(std::cmp::Ordering::Equal)) => return i64::try_from(i).ok(),
                Ok(_) => {}
                Err(_) => return None,
            }
        }
        Some(-1)
    }

    /// A set operation's select item that is an `ENUM` column, as its
    /// member text: `ELT(c + 1, '', 'a', 'b')`, named as the column (0 is
    /// the servers' error value `''`, `NULL` stays `NULL`). The servers
    /// give a set operation the members' text; the expression engine
    /// holds the member number. `None`: no such column.
    fn member_text(&mut self, e: &Expr, single: Option<&str>) -> Option<Expr> {
        // An unqualified name in a branch that reads one table is that
        // table's, even where a derived table names a column the same.
        let lookup = match (e, single) {
            (Expr::Identifier(i), Some(t)) => {
                Expr::CompoundIdentifier(vec![Ident::new(t), i.clone()])
            }
            _ => e.clone(),
        };
        let (members, _) = self.column(&lookup)?;
        let mut lits = vec!["''".to_owned()];
        for m in members.iter() {
            let t = std::str::from_utf8(m).ok()?;
            // Read back by the parser, which always takes backslash
            // escapes.
            let t = t.replace('\\', "\\\\").replace('\'', "''");
            lits.push(format!("'{t}'"));
        }
        let text = format!("ELT({e} + 1, {})", lits.join(", "));
        let mut out = sqlparser::parser::Parser::new(&sprig_plan::dialect::SprigMySql)
            .try_with_sql(&text)
            .ok()?
            .parse_expr()
            .ok()?;
        // The column inside: computing its number is what is wanted.
        if let Expr::Function(f) = &mut out
            && let FunctionArguments::List(l) = &mut f.args
            && let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::BinaryOp {
                left, ..
            }))) = l.args.first_mut()
        {
            **left = e.clone();
            self.safe.insert(std::ptr::from_ref(&**left));
            return Some(out);
        }
        None
    }

    /// [`Self::member_text`] for each select item of a set operation's
    /// branches.
    fn set_branches(&mut self, b: &mut SetExpr) {
        match b {
            SetExpr::Select(s) => {
                self.scopes.push(Scope::of(s, &self.ctes));
                let single = match s.from.as_slice() {
                    [
                        TableWithJoins {
                            relation: TableFactor::Table { name, alias, .. },
                            joins,
                        },
                    ] if joins.is_empty() => alias
                        .as_ref()
                        .map(|a| a.name.value.to_lowercase())
                        .or_else(|| {
                            name.0
                                .last()
                                .and_then(ident_of)
                                .map(|i| i.value.to_lowercase())
                        }),
                    _ => None,
                };
                for item in &mut s.projection {
                    match item {
                        SelectItem::UnnamedExpr(e) => {
                            let name = match &*e {
                                Expr::Identifier(i) => Some(i.clone()),
                                Expr::CompoundIdentifier(p) => p.last().cloned(),
                                _ => None,
                            };
                            if let Some(name) = name
                                && let Some(t) = self.member_text(e, single.as_deref())
                            {
                                *item = SelectItem::ExprWithAlias {
                                    expr: t,
                                    alias: name,
                                };
                            }
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            if let Some(t) = self.member_text(expr, single.as_deref()) {
                                *expr = t;
                            }
                        }
                        _ => {}
                    }
                }
                self.scopes.pop();
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.set_branches(left);
                self.set_branches(right);
            }
            SetExpr::Query(q) => self.set_branches(&mut q.body),
            _ => {}
        }
    }

    fn num(n: i64) -> Expr {
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(n.to_string(), false),
            span: sqlparser::tokenizer::Span::empty(),
        })
    }
}

impl VisitorMut for EnumLits<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        let inner = self.top != Some(std::ptr::from_ref::<Query>(q));
        if matches!(&*q.body, SetExpr::SetOperation { .. }) || (self.texts && inner) {
            self.set_branches(&mut q.body);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
        self.scopes.push(Scope::of(s, &self.ctes));
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _s: &mut Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        use sqlparser::ast::BinaryOperator as B;
        match e {
            Expr::BinaryOp {
                left,
                op: B::Eq | B::NotEq | B::Spaceship,
                right,
            } => {
                if let Some((_, col)) = self.column(left)
                    && let Some(n) = self.number(&col, right)
                {
                    **right = Self::num(n);
                    self.safe.insert(std::ptr::from_ref(&**left));
                } else if let Some((_, col)) = self.column(right)
                    && let Some(n) = self.number(&col, left)
                {
                    **left = Self::num(n);
                    self.safe.insert(std::ptr::from_ref(&**right));
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Some((_, col)) = self.column(expr) {
                    let nums: Option<Vec<i64>> =
                        list.iter().map(|v| self.number(&col, v)).collect();
                    if let Some(nums) = nums {
                        *list = nums.into_iter().map(Self::num).collect();
                        self.safe.insert(std::ptr::from_ref(&**expr));
                    }
                }
            }
            Expr::IsNull(x) | Expr::IsNotNull(x) => {
                if self.column(x).is_some() {
                    self.safe.insert(std::ptr::from_ref(&**x));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Runs [`EnumLits`] over `stmt` (after its views are expanded).
#[allow(clippy::too_many_lines)] // the table collection, then the rewrite
fn enum_literals(
    cx: &BindCtx<'_>,
    foreign: &[Arc<sprig_catalog::TenantCatalog>],
    stmt: &mut Statement,
    texts: bool,
) -> HashSet<*const Expr> {
    struct Collect<'a, 'b> {
        cx: &'a BindCtx<'b>,
        foreign: &'a [Arc<sprig_catalog::TenantCatalog>],
        tables: Vec<(String, Arc<TableDef>)>,
        derived: HashSet<String>,
        ctes: HashSet<String>,
    }
    impl Visitor for Collect<'_, '_> {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            if let Some(w) = &q.with {
                for c in &w.cte_tables {
                    self.ctes.insert(c.alias.name.value.to_lowercase());
                    names_of(&c.query, &mut self.derived);
                    for col in &c.alias.columns {
                        self.derived.insert(col.name.value.to_lowercase());
                    }
                }
            }
            ControlFlow::Continue(())
        }
        fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
            match f {
                TableFactor::Derived { subquery, .. } => names_of(subquery, &mut self.derived),
                TableFactor::Table { name, alias, .. } => {
                    let parts: Option<Vec<&Ident>> = name.0.iter().map(ident_of).collect();
                    let (db, t) = match parts.as_deref() {
                        Some([t]) => (self.cx.current, *t),
                        Some([s, t]) => (
                            physical_db(self.cx.tenant, &s.value)
                                .or_else(|| self.cx.names.db_by_name(&s.value)),
                            *t,
                        ),
                        _ => return ControlFlow::Continue(()),
                    };
                    // A parent's table read through a share.
                    let foreign = match (db, parts.as_deref()) {
                        (None, Some([s, _])) => s
                            .value
                            .parse::<sprig_common::names::SchemaName>()
                            .ok()
                            .and_then(|sn| {
                                let c = self.foreign.iter().find(|c| c.tenant == sn.tenant())?;
                                let eng = crate::engine::catalog::of(c)?;
                                let d = sn.db()?;
                                let p = eng
                                    .table(d, &t.value, false)
                                    .or_else(|| eng.table(d, &t.value, true))?;
                                p.as_ref().as_ref().ok().cloned()
                            }),
                        _ => None,
                    };
                    let def = foreign.or_else(|| {
                        db.and_then(|db| self.cx.names.table(db, &t.value))
                            .and_then(|p| p.as_ref().as_ref().ok().cloned())
                    });
                    if let Some(def) = &def
                        && def
                            .columns
                            .iter()
                            .any(|c| matches!(c.ty, ColType::Enum { .. }))
                    {
                        let a = alias.as_ref().map_or_else(
                            || t.value.to_lowercase(),
                            |a| a.name.value.to_lowercase(),
                        );
                        self.tables.push((a, Arc::clone(def)));
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }
    fn names_of(q: &Query, out: &mut HashSet<String>) {
        fn selects<'q>(b: &'q SetExpr, v: &mut Vec<&'q Select>) {
            match b {
                SetExpr::Select(s) => v.push(s),
                SetExpr::SetOperation { left, right, .. } => {
                    selects(left, v);
                    selects(right, v);
                }
                SetExpr::Query(q) => selects(&q.body, v),
                _ => {}
            }
        }
        let mut add = |s: &Select| {
            for item in &s.projection {
                match item {
                    SelectItem::ExprWithAlias { alias, .. } => {
                        out.insert(alias.value.to_lowercase());
                    }
                    SelectItem::UnnamedExpr(e) => {
                        if let Some(n) = match e {
                            Expr::Identifier(i) => Some(&i.value),
                            Expr::CompoundIdentifier(p) => p.last().map(|i| &i.value),
                            _ => None,
                        } {
                            out.insert(n.to_lowercase());
                        }
                    }
                    _ => {}
                }
            }
        };
        let mut v = Vec::new();
        selects(&q.body, &mut v);
        for s in v {
            add(s);
        }
    }
    let mut c = Collect {
        cx,
        foreign,
        tables: Vec::new(),
        derived: HashSet::new(),
        ctes: HashSet::new(),
    };
    let _ = Visit::visit(&*stmt, &mut c);
    if c.tables.is_empty() {
        return HashSet::new();
    }
    let top = query_of(stmt).map(std::ptr::from_ref::<Query>);
    let mut e = EnumLits {
        tables: c.tables,
        derived: c.derived,
        backslash: cx.backslash,
        safe: HashSet::new(),
        scopes: Vec::new(),
        ctes: c.ctes,
        texts,
        top,
        _cx: std::marker::PhantomData,
    };
    let _ = VisitMut::visit(stmt, &mut e);
    e.safe
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
    /// `[q.]c` of an `ENUM` column compared with its member numbers only
    /// ([`EnumLits`]): read, and safe to compute with.
    Safe { qual: Option<String>, name: String },
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
    /// The sequences named by sequence functions.
    sequences: Vec<SeqUse>,
    /// A sequence function was seen.
    seq_calls: bool,
    /// Column references [`EnumLits`] made comparisons of member numbers.
    safe: HashSet<*const Expr>,
    /// [`Views::foreign`].
    foreign: Vec<Arc<sprig_catalog::TenantCatalog>>,
    /// The `FROM` names of each `SELECT` being visited, innermost last.
    scopes: Vec<Scope>,
}

impl Walk<'_, '_> {
    /// Whether qualifier `q` names a derived table or a common table
    /// expression where it is read (its columns are that query's, marked
    /// where that query names them).
    fn derived_qual(&self, q: &str) -> bool {
        for sc in self.scopes.iter().rev() {
            if sc.derived.contains(q) {
                return true;
            }
            if sc.base.contains(q) {
                return false;
            }
        }
        false
    }

    fn fail(&mut self, e: SprigError) {
        if self.err.is_none() {
            self.err = Some(e);
        }
    }

    /// A sequence function's sequence (`args`: the call's arguments).
    fn sequence_of(&mut self, args: &[FunctionArg]) {
        self.seq_calls = true;
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) = args.first() else {
            return;
        };
        let (db, name, written) = match e {
            Expr::Identifier(i) => (self.cx.current, &i.value, i.value.clone()),
            Expr::CompoundIdentifier(p) if p.len() == 2 => (
                physical_db(self.cx.tenant, &p[0].value)
                    .or_else(|| self.cx.names.db_by_name(&p[0].value)),
                &p[1].value,
                format!("{}.{}", p[0].value, p[1].value),
            ),
            _ => return,
        };
        let Some(db) = db else {
            self.fail(SprigError::standard(
                1046,
                *b"3D000",
                "No database selected",
            ));
            return;
        };
        let db_name = self.cx.names.db_name(db);
        // Errors are the planner's to give (MariaDB's port only).
        let id = match self.cx.names.sequence(db, name) {
            Some(e) => Ok(e.object.0.to_string()),
            None if self.cx.names.table(db, name).is_some()
                || self.cx.names.view(db, name).is_some() =>
            {
                Err((4089, format!("'{db_name}.{name}' is not a SEQUENCE")))
            }
            None => Err((4091, format!("Unknown SEQUENCE: '{written}'"))),
        };
        if !self
            .sequences
            .iter()
            .any(|s| s.db_name == db_name && s.name == *name)
        {
            self.sequences.push(SeqUse {
                db_name,
                name: name.clone(),
                id,
            });
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
            Some([s, t]) => {
                let db = physical_db(self.cx.tenant, &s.value)
                    .or_else(|| self.cx.names.db_by_name(&s.value));
                if db.is_none() && self.foreign_schema(&s.value) {
                    return self.foreign_table(&s.value, &t.value, alias);
                }
                (db, &t.value)
            }
            _ => {
                self.fail(not_on_engine("a table name of more than two parts"));
                return None;
            }
        };
        let Some(db) = db.filter(|d| self.cx.names.engine_db(*d)) else {
            self.elsewhere = true;
            return None;
        };
        let (def, coll) = if let Some(parsed) = self.cx.names.table(db, table) {
            match &*parsed {
                Ok(def) => (Arc::clone(def), None),
                Err(why) => {
                    self.fail(not_on_engine(why));
                    return None;
                }
            }
        } else if let Some(c) = (!self.cx.names.other_relation(db, table))
            .then(|| self.cx.names.collection(db, table))
            .flatten()
        {
            // A MongoDB collection of the database of the same name.
            (Arc::clone(&c.def), Some(c))
        } else {
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
        let at = if let Some(at) = self
            .tables
            .iter()
            .position(|t| t.tenant.is_none() && t.def.object == def.object)
        {
            at
        } else {
            let db_name = self.cx.names.db_name(db);
            self.tables.push(TableUse {
                plan: Arc::new(if coll.is_some() {
                    crate::engine::collection::plan_table(&def, &db_name)
                } else {
                    plan_table(&def, &db_name)
                }),
                coll,
                read: vec![false; def.columns.len()],
                order: Vec::new(),
                aliases: Vec::new(),
                starred: vec![false; def.columns.len()],
                db_name,
                def,
                tenant: None,
            });
            self.tables.len() - 1
        };
        let alias = alias.map_or_else(|| table.to_lowercase(), |a| a.value.to_lowercase());
        if !self.tables[at].aliases.contains(&alias) {
            self.tables[at].aliases.push(alias);
        }
        Some(at)
    }

    /// Whether `schema` is a physical schema of a parent tenant whose share
    /// the statement reads ([`Views::foreign`]).
    fn foreign_schema(&self, schema: &str) -> bool {
        schema
            .parse::<sprig_common::names::SchemaName>()
            .is_ok_and(|sn| {
                sn.db().is_some() && self.foreign.iter().any(|c| c.tenant == sn.tenant())
            })
    }

    /// Table `table` of a parent tenant's physical schema `schema` (see
    /// [`Self::foreign_schema`]), registered under the schema's physical
    /// name.
    fn foreign_table(&mut self, schema: &str, table: &str, alias: Option<&Ident>) -> Option<usize> {
        let sn = schema.parse::<sprig_common::names::SchemaName>().ok()?;
        let cat = Arc::clone(self.foreign.iter().find(|c| c.tenant == sn.tenant())?);
        let db = sn.db()?;
        let def = crate::engine::catalog::of(&cat).and_then(|eng| {
            eng.table(db, table, false)
                .or_else(|| eng.table(db, table, true))
                .and_then(|p| p.as_ref().as_ref().ok().cloned())
        });
        let Some(def) = def else {
            self.fail(SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{schema}.{table}' doesn't exist"),
            ));
            return None;
        };
        let tenant = cat.tenant.0;
        let at = if let Some(at) = self
            .tables
            .iter()
            .position(|t| t.tenant == Some(tenant) && t.def.object == def.object)
        {
            at
        } else {
            self.tables.push(TableUse {
                plan: Arc::new(plan_table(&def, schema)),
                read: vec![false; def.columns.len()],
                order: Vec::new(),
                aliases: Vec::new(),
                starred: vec![false; def.columns.len()],
                db_name: schema.to_owned(),
                def,
                tenant: Some(tenant),
                coll: None,
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

    fn post_visit_select(&mut self, _s: &Select) -> ControlFlow<()> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        self.scopes.push(Scope::of(s, &self.ctes));
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
        // A derived table's column: its own query marks what it reads.
        if let Expr::CompoundIdentifier(p) = e
            && let [.., q, _] = p.as_slice()
            && self.derived_qual(&q.value.to_lowercase())
        {
            return ControlFlow::Continue(());
        }
        let plain = self.plain.contains(&std::ptr::from_ref(e));
        if self.safe.contains(&std::ptr::from_ref(e)) {
            let (qual, name) = match e {
                Expr::CompoundIdentifier(p) if p.len() >= 2 => (
                    Some(p[p.len() - 2].value.to_lowercase()),
                    p[p.len() - 1].value.to_lowercase(),
                ),
                Expr::CompoundIdentifier(p) if p.len() == 1 => (None, p[0].value.to_lowercase()),
                Expr::Identifier(i) => (None, i.value.to_lowercase()),
                _ => return ControlFlow::Continue(()),
            };
            self.marks.push(Mark::Safe { qual, name });
            return ControlFlow::Continue(());
        }
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
                if let FunctionArguments::List(l) = &f.args
                    && let [ObjectNamePart::Identifier(n)] = f.name.0.as_slice()
                    && ["NEXTVAL", "LASTVAL", "SETVAL", "SPRIG_SEQUENCE_STATE"]
                        .iter()
                        .any(|s| n.value.eq_ignore_ascii_case(s))
                {
                    self.sequence_of(&l.args);
                }
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
            mb3: c.mb3,
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
            mb3: false,
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
                .ignored(x.ignored)
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

pub(super) fn analyze(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
    tableless: bool,
) -> Result<Option<Analysis>> {
    let mut enum_refused = false;
    match analyze_in(cx, stmt, text, tableless, false, &mut enum_refused) {
        // An `ENUM` column a derived table, a common table expression or
        // a subquery reads and its outer query computes with: read again
        // with those queries giving the member text, as the servers'
        // derived tables do.
        Err(_) if enum_refused => analyze_in(cx, stmt, text, tableless, true, &mut enum_refused),
        r => r,
    }
}

#[allow(clippy::too_many_lines)] // one walk, then the marks and the origins
fn analyze_in(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
    tableless: bool,
    texts: bool,
    enum_refused: &mut bool,
) -> Result<Option<Analysis>> {
    // The names of computed columns, from the statement as it was parsed
    // (the copy below loses nothing of it, but is read first for its
    // tables).
    let source = stmt;
    let mut stmt = stmt.clone();
    let _ = VisitMut::visit(&mut stmt, &mut Logical { cx });
    let views = expand_views(cx, &mut stmt)?;
    let tableless = tableless || !views.uses.is_empty();
    let safe = enum_literals(cx, &views.foreign, &mut stmt, texts);
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
        sequences: Vec::new(),
        seq_calls: false,
        safe,
        foreign: views.foreign.clone(),
        scopes: Vec::new(),
    };
    let _ = Visit::visit(&stmt, &mut w);
    // Sequence functions are the engine's: such a statement needs no table.
    let tableless = tableless || w.seq_calls;
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
    // `other`: named some other way than as a member-number comparison.
    let mut other = computed.clone();
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
                        other[ti][c] = true;
                    }
                }
            }
            Mark::Safe { qual, name } => {
                for t in &mut w.tables {
                    if qual.as_ref().is_some_and(|q| !t.aliases.contains(q)) {
                        continue;
                    }
                    if let Some(c) = t.def.column(name) {
                        t.mark(c);
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
                    other[ti].fill(true);
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
                        other[ti].fill(true);
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
    let simple_view = match &*query.body {
        SetExpr::Select(s) => view_origins(&views, &w.tables, s),
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
            // Compared with member numbers only: the engine computes with
            // those.
            if !other[ti][ci] {
                continue;
            }
            // Passing through is fine where the result's encoders know
            // the column; anything else would show its stored number.
            let passes = origins
                .iter()
                .flatten()
                .any(|o| o.table == ti && o.column == ci);
            if computed[ti][ci] || (t.read[ci] && !passes) {
                *enum_refused = matches!(c.ty, ColType::Enum { .. });
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
    let (origins, view_named) = match simple_view {
        Some((o, n)) if o.len() == origins.len() || origins.is_empty() => (o, n),
        _ => (origins, Vec::new()),
    };
    Ok(Some(Analysis {
        views,
        stmt,
        tables,
        origins,
        written,
        params,
        view_named,
        sequences: w.sequences,
    }))
}

/// The outermost select reads one view and nothing else, a view of one
/// table merged into it: its columns are described as the servers describe
/// a merged view's (the view's name as the table; a plain column keeps its
/// column's definition, a computed one is named after the view).
#[allow(
    clippy::type_complexity,
    clippy::too_many_lines,
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::match_wildcard_for_single_variants
)] // per result column: origin, or (`table`, `org_table`); one case after another
fn view_origins(
    views: &Views,
    tables: &[TableUse],
    s: &Select,
) -> Option<(Vec<Option<Origin>>, Vec<Option<(String, String)>>)> {
    let [t] = s.from.as_slice() else {
        return None;
    };
    if !t.joins.is_empty() {
        return None;
    }
    let TableFactor::Derived {
        alias: Some(a),
        subquery,
        ..
    } = &t.relation
    else {
        return None;
    };
    let alias = a.name.value.clone();
    let mut top = views.uses.iter().filter(|u| u.under.is_none());
    let u = top.next()?;
    if top.next().is_some() || u.view.algorithm == crate::engine::view::Algorithm::Temptable {
        return None;
    }
    let SetExpr::Select(vs) = &*subquery.body else {
        return None;
    };
    if !crate::engine::view::updatable(subquery, u.view.algorithm, false) {
        return None;
    }
    let [vt] = vs.from.as_slice() else {
        return None;
    };
    if !vt.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &vt.relation else {
        return None;
    };
    let base = last_name(name)?;
    let ti = tables
        .iter()
        .position(|x| x.def.name.eq_ignore_ascii_case(&base))?;
    // Per view column: the base column it is, if a plain one.
    let cols: Vec<Option<usize>> = vs
        .projection
        .iter()
        .map(|it| {
            let e = match it {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                _ => return None,
            };
            let c = match e {
                Expr::Identifier(i) => i.value.clone(),
                Expr::CompoundIdentifier(p) => p.last()?.value.clone(),
                _ => return None,
            };
            tables[ti].def.column(&c)
        })
        .collect();
    if cols.len() != u.view.columns.len() {
        return None;
    }
    let mut origins = Vec::new();
    let mut named = Vec::new();
    let mut push = |j: Option<usize>| {
        let Some(j) = j else {
            origins.push(None);
            named.push(None);
            return;
        };
        match cols[j] {
            Some(c) => {
                origins.push(Some(Origin {
                    table: ti,
                    column: c,
                    alias: alias.clone(),
                    outer: false,
                    merged: false,
                    view: Some((u.view.name.clone(), u.view.columns[j].clone())),
                }));
                named.push(None);
            }
            None => {
                origins.push(None);
                named.push(Some((alias.clone(), u.view.name.clone())));
            }
        }
    };
    for it in &s.projection {
        match it {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                (0..cols.len()).for_each(|j| push(Some(j)));
            }
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                let c = match e {
                    Expr::Identifier(i) => Some(i.value.clone()),
                    Expr::CompoundIdentifier(p) if p.len() == 2 => Some(p[1].value.clone()),
                    _ => None,
                };
                push(c.and_then(|c| {
                    u.view
                        .columns
                        .iter()
                        .position(|x| x.eq_ignore_ascii_case(&c))
                }));
            }
            _ => return None,
        }
    }
    Some((origins, named))
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
        view: None,
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
                    view: None,
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
    let to = offset(text, span.end.line, span.end.column)?;
    // The cut must read back as the same expression. The parser's place
    // for a call can stop before its closing parenthesis: the cut then
    // grows to the next one, a few times at most.
    let same = |t: &str| {
        sqlparser::parser::Parser::new(&sprig_plan::dialect::SprigMySql)
            .try_with_sql(t)
            .ok()
            .is_some_and(|mut p| {
                p.parse_expr().is_ok_and(|x| x == *e)
                    && p.peek_token().token == sqlparser::tokenizer::Token::EOF
            })
    };
    // A row value's place starts after its opening parenthesis: the cut
    // then also grows back to the previous one, a few times at most.
    let mut cut = None;
    let mut start = from;
    'grow: for _ in 0..4 {
        let mut end = to;
        for _ in 0..8 {
            let t = text.get(start..end)?.trim();
            if same(t) {
                cut = Some(t);
                break 'grow;
            }
            match text.get(end..).and_then(|r| r.find(')')) {
                Some(i) => end += i + 1,
                None => break,
            }
        }
        match text.get(..start).and_then(|l| l.rfind('(')) {
            Some(i) => start = i,
            None => break,
        }
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

    fn sequence(
        &self,
        schema: &str,
        name: &str,
    ) -> std::result::Result<String, sprig_exec::SqlError> {
        self.g
            .sequences
            .iter()
            .find(|s| s.db_name.eq_ignore_ascii_case(schema) && s.name.eq_ignore_ascii_case(name))
            .map_or_else(
                || Err((4091, format!("Unknown SEQUENCE: '{name}'"))),
                |s| s.id.clone(),
            )
            .map_err(|(code, m)| sprig_exec::SqlError::new(code, "42S02", m))
    }

    fn shared(&self) -> Option<Arc<dyn Catalog>> {
        Some(Arc::new(sprig_plan::catalog::Snapshot {
            tables: self
                .g
                .tables
                .iter()
                .map(|t| (t.db_name.clone(), t.def.name.clone(), Arc::clone(&t.plan)))
                .collect(),
            default: self.g.current.clone(),
        }))
    }
}
