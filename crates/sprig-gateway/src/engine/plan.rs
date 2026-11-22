//! Binding a statement to an engine table, and its plan.
//!
//! The statement is the text the gateway's layers produced: names mapped
//! to physical schemas (`t<tenant>_d<db>_g<group>_s<shard>`), row-level
//! security predicates written in. Binding resolves its one table, checks
//! the shape is in the prototype's subset, and compiles its expressions once.
//!
//! | Statement | Plan |
//! |---|---|
//! | `SELECT cols FROM t WHERE pk = e [AND rest]` | `Point`: one key get, `rest` as a filter (no optimizer) |
//! | the same, with `e` of a type compared with the key in another type | the whole `WHERE` over a scan (`Plan::unkeyed`) |
//! | `SELECT cols FROM t WHERE pk BETWEEN / < / > ...` | `PkRange` |
//! | `SELECT cols FROM t WHERE pk IN (c1, c2, ...)` | `PkIn`: one key get per constant, in key order |
//! | `SELECT cols FROM t WHERE pk1 = e` (a composite key's first columns) | `PkPrefix`: the rows sharing those key parts |
//! | `SELECT cols FROM t WHERE ix = e ...` | `IndexEq` on a secondary index's first column |
//! | `SELECT cols FROM t [WHERE f] [ORDER BY pk] [LIMIT n]` | `Full` scan in key order |
//! | `INSERT INTO t [(cols)] VALUES (...), ...` | rows built and checked |
//! | `UPDATE t SET c = e, ... WHERE ...`, `DELETE FROM t WHERE ...` | the same access paths |
//!
//! Anything else naming an engine table answers `ER_SPRIG_NOT_ON_ENGINE`;
//! a statement naming no engine table is not the engine's ([`bind`] gives
//! `None`) and goes on to MariaDB as before.

use std::sync::Arc;

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_catalog::store::Namespace;
use sprig_common::ids::{DbId, TenantId};
use sprig_common::{Result, SprigError};
use sqlparser::ast::{
    Expr, FromTable, GroupByExpr, Ident, LimitClause, ObjectName, ObjectNamePart, OrderByKind,
    OrderBySort, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableObject,
    TableWithJoins, Value as SqlValue,
};

use super::catalog::EngineCatalog;
use super::expr::{CExpr, Cmp, Compiler};
use super::not_on_engine;
use super::table::{self, TableDef};

/// The tenant's names as binding sees them.
pub(crate) trait Names {
    /// Whether database `db` is engine-backed.
    fn engine_db(&self, db: DbId) -> bool;
    /// The engine table `name` of `db`, if any.
    fn table(&self, db: DbId, name: &str) -> Option<Arc<table::Parsed>>;
    /// Whether `db` has another relation (a view, ...) called `name`.
    fn other_relation(&self, db: DbId, name: &str) -> bool;
    /// The logical name of `db`.
    fn db_name(&self, db: DbId) -> String;
    /// The database with logical name `name`.
    fn db_by_name(&self, name: &str) -> Option<DbId>;
    /// The catalog version.
    fn version(&self) -> u64;
    /// The foreign keys whose parent is table `name` of `db`: each child
    /// table's definition and the key's position among its foreign keys.
    fn referencing(&self, _db: DbId, _name: &str) -> Vec<(Arc<TableDef>, usize)> {
        Vec::new()
    }
}

/// [`Names`] over a catalog snapshot.
pub(crate) struct CatalogNames<'a> {
    /// The snapshot.
    pub cat: &'a TenantCatalog,
    /// Its engine module state.
    pub eng: &'a EngineCatalog,
    /// The tenant's `lower_case_table_names`: database and table names
    /// compare in lower case, or exactly as written (MariaDB on Linux).
    pub lower_case: bool,
}

impl CatalogNames<'_> {
    /// `name` in the form the catalog looks names up by.
    fn key(&self, name: &str) -> String {
        sprig_catalog::ddl::database_key(name, self.lower_case)
    }
}

impl Names for CatalogNames<'_> {
    fn engine_db(&self, db: DbId) -> bool {
        self.eng.dbs.contains(&db)
    }
    fn table(&self, db: DbId, name: &str) -> Option<Arc<table::Parsed>> {
        self.eng.table(db, name, self.lower_case).cloned()
    }
    fn other_relation(&self, db: DbId, name: &str) -> bool {
        self.cat
            .object(db, Namespace::Relation, &self.key(name))
            .is_some()
    }
    fn db_name(&self, db: DbId) -> String {
        self.cat
            .databases
            .get(db)
            .map_or_else(String::new, |d| d.name.clone())
    }
    fn db_by_name(&self, name: &str) -> Option<DbId> {
        self.cat.database(&self.key(name)).map(|d| d.db_id)
    }
    fn version(&self) -> u64 {
        self.cat.version
    }
    fn referencing(&self, db: DbId, name: &str) -> Vec<(Arc<TableDef>, usize)> {
        if self.eng.refs.is_empty() {
            return Vec::new();
        }
        let Some(cands) = self.eng.refs.get(&name.to_lowercase()) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for &(object, i) in cands {
            let Some(Ok(child)) = self.eng.tables.get(&object).map(|p| p.as_ref().clone()) else {
                continue;
            };
            let f = &child.fks[i];
            let same_name = if self.lower_case {
                f.ref_table.eq_ignore_ascii_case(name)
            } else {
                f.ref_table == name
            };
            let ref_db = match &f.ref_db {
                None => Some(child.db),
                Some(n) => self.db_by_name(n),
            };
            if same_name && ref_db == Some(db) {
                out.push((child, i));
            }
        }
        out
    }
}

/// What binding needs to know about the session.
pub(crate) struct BindCtx<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// Its names.
    pub names: &'a dyn Names,
    /// The current database.
    pub current: Option<DbId>,
    /// `\` escapes in literals.
    pub backslash: bool,
}

/// One output column.
#[derive(Debug, Clone)]
pub(crate) struct OutCol {
    /// The table column.
    pub col: usize,
    /// The result name (alias or the name as written).
    pub name: Bytes,
}

/// How rows are found.
#[derive(Debug, Clone)]
pub(crate) enum Access {
    /// One primary key.
    Point(Vec<CExpr>),
    /// A primary-key range (single-column keys): bounds and inclusivity.
    PkRange {
        lo: Option<(CExpr, bool)>,
        hi: Option<(CExpr, bool)>,
    },
    /// Equality on secondary index `ix`'s first column.
    IndexEq { ix: usize, val: CExpr },
    /// A one-column primary key among a list of constants (`pk IN (...)`):
    /// one key get each, in key order.
    PkIn(Vec<CExpr>),
    /// The first columns of a composite primary key by equality: the rows
    /// that share those key parts, in key order.
    PkPrefix(Vec<CExpr>),
    /// Every row, in key order.
    Full,
}

/// What a statement does.
#[derive(Debug, Clone)]
pub(crate) enum Op {
    /// A `SELECT`.
    Select { out: Vec<OutCol> },
    /// An `INSERT ... VALUES`.
    Insert {
        /// The column of each value position.
        cols: Vec<usize>,
        /// The rows' expressions (`None`: `DEFAULT`).
        rows: Vec<Vec<Option<CExpr>>>,
    },
    /// An `UPDATE`.
    Update { sets: Vec<(usize, CExpr)> },
    /// A `DELETE`.
    Delete,
}

/// How a `SELECT` locks its rows (`FOR UPDATE`, `LOCK IN SHARE MODE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowLock {
    /// An exclusive lock (`FOR UPDATE`); otherwise a shared one.
    pub exclusive: bool,
    /// What to do about a row another transaction holds.
    pub wait: sprig_engine::txn::Wait,
}

/// A bound statement.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    /// What it does.
    pub op: Op,
    /// The table.
    pub table: Arc<TableDef>,
    /// The logical database name (result metadata).
    pub db_name: Bytes,
    /// The table as the statement named it (result metadata).
    pub alias: Bytes,
    /// The table's stored name.
    pub org_table: Bytes,
    /// How rows are found (not for `INSERT`).
    pub access: Access,
    /// The rest of `WHERE`.
    pub filter: Option<CExpr>,
    /// The whole `WHERE` of a `Point` plan: what the statement filters a
    /// scan by when a key value can't be made from its constant (a string
    /// key compared with a number compares as numbers, row by row).
    pub unkeyed: Option<CExpr>,
    /// The rows are read from the last key to the first: `ORDER BY pk
    /// DESC`, or `ORDER BY pk` over a key kept descending.
    pub reverse: bool,
    /// `LIMIT`.
    pub limit: Option<u64>,
    /// `OFFSET`.
    pub offset: u64,
    /// `?` placeholders.
    pub params: usize,
    /// The type each `?` takes from its context, if any.
    pub param_types: Vec<Option<table::ColType>>,
    /// Columns read (`SELECT` privilege), by index.
    pub read: Vec<bool>,
    /// Columns written (`INSERT` or `UPDATE` privilege), by index.
    pub written: Vec<bool>,
    /// The catalog version the plan was bound at.
    pub catalog_version: u64,
    /// The statement has an `ORDER BY` (on the primary key).
    pub ordered: bool,
    /// A `SELECT`'s locking clause.
    pub lock: Option<RowLock>,
    /// Row-level security's condition on the new row versions an `INSERT`
    /// or `UPDATE` writes (the policies' `WITH CHECK`, bound to the
    /// session): a row that doesn't meet it fails the statement.
    pub check: Option<CExpr>,
    /// The foreign keys an `INSERT`, `UPDATE` or `DELETE` must keep (none
    /// when the table has none either way).
    pub fks: Option<Arc<super::fk::Graph>>,
}

impl Plan {
    /// Whether the statement returns rows.
    pub(crate) fn is_select(&self) -> bool {
        matches!(self.op, Op::Select { .. })
    }
}

fn ident_parts(n: &ObjectName) -> Option<Vec<&Ident>> {
    n.0.iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

/// The database a physical schema name names, if it is one of the
/// tenant's (`t<tenant>_d<db>_g<group>_s<shard>`).
pub(crate) fn physical_db(tenant: TenantId, schema: &str) -> Option<DbId> {
    let rest = schema.strip_prefix('t')?;
    let (t, rest) = rest.split_once("_d")?;
    if t.parse::<u64>().ok()? != tenant.0 {
        return None;
    }
    let (d, rest) = rest.split_once("_g")?;
    let (g, s) = rest.split_once("_s")?;
    g.parse::<u32>().ok()?;
    s.parse::<u32>().ok()?;
    d.parse::<u32>().ok().map(DbId)
}

/// A resolved table reference.
pub(crate) enum Resolved {
    /// Not an engine database's.
    Elsewhere,
    /// An engine table.
    Engine(Arc<TableDef>, DbId),
}

/// `DUAL`, unquoted: the servers' dummy table, never a table of the
/// database (a table named so must be quoted).
pub(crate) fn is_dual(t: &sqlparser::ast::Ident) -> bool {
    t.quote_style.is_none() && t.value.eq_ignore_ascii_case("dual")
}

pub(crate) fn resolve(cx: &BindCtx<'_>, name: &ObjectName) -> Result<Resolved> {
    let parts = ident_parts(name).ok_or_else(|| not_on_engine("a table name"))?;
    let (db, table) = match parts.as_slice() {
        // `FROM DUAL` reads no table.
        [t] if is_dual(t) => return Ok(Resolved::Elsewhere),
        [t] => (cx.current, &t.value),
        [s, t] => {
            let db = physical_db(cx.tenant, &s.value).or_else(|| cx.names.db_by_name(&s.value));
            (db, &t.value)
        }
        _ => return Err(not_on_engine("a table name")),
    };
    let Some(db) = db.filter(|d| cx.names.engine_db(*d)) else {
        return Ok(Resolved::Elsewhere);
    };
    let Some(p) = cx.names.table(db, table) else {
        if cx.names.other_relation(db, table) {
            return Err(not_on_engine("a view or another relation kind"));
        }
        return Err(SprigError::standard(
            1146,
            *b"42S02",
            format!("Table '{}.{table}' doesn't exist", cx.names.db_name(db)),
        ));
    };
    match &*p {
        Ok(def) => Ok(Resolved::Engine(Arc::clone(def), db)),
        Err(why) => Err(not_on_engine(why)),
    }
}

/// Whether `stmt` names an engine database's table anywhere: in a
/// subquery, a derived table, a common table expression, the source of an
/// `INSERT ... SELECT`, a join. [`bind`] plans only statements on one
/// engine table; any other statement that reads or writes rows and names
/// one must be refused, because MariaDB's copy of the table is empty.
/// A name that is no table of an engine database (a common table
/// expression's own name, a view) doesn't count.
pub(crate) fn names_engine_table(cx: &BindCtx<'_>, stmt: &Statement) -> bool {
    use std::ops::ControlFlow;
    sqlparser::ast::visit_relations(stmt, |name: &ObjectName| {
        let Some(parts) = ident_parts(name) else {
            return ControlFlow::Continue(());
        };
        let (db, table) = match parts.as_slice() {
            [t] => (cx.current, &t.value),
            [s, t] => (
                physical_db(cx.tenant, &s.value).or_else(|| cx.names.db_by_name(&s.value)),
                &t.value,
            ),
            _ => return ControlFlow::Continue(()),
        };
        match db.filter(|d| cx.names.engine_db(*d)) {
            Some(db) if cx.names.table(db, table).is_some() => ControlFlow::Break(()),
            _ => ControlFlow::Continue(()),
        }
    })
    .is_break()
}

/// The one engine table of a `FROM`: its definition, database, name as
/// written and alias.
type EngineFrom<'a> = (Arc<TableDef>, DbId, &'a ObjectName, Option<&'a Ident>);

/// Whether any table factor in `twj` names an engine database's table.
fn single_table<'a>(
    cx: &BindCtx<'_>,
    from: &'a [TableWithJoins],
) -> Result<Option<EngineFrom<'a>>> {
    let mut engine = false;
    let mut elsewhere = false;
    let mut found = None;
    let mut visit = |f: &'a TableFactor| -> Result<()> {
        match f {
            TableFactor::Table { name, alias, .. } => match resolve(cx, name)? {
                Resolved::Engine(def, db) => {
                    engine = true;
                    found = Some((def, db, name, alias.as_ref().map(|a| &a.name)));
                }
                Resolved::Elsewhere => elsewhere = true,
            },
            // Derived tables, functions: not a shape the prototype runs.
            _ => elsewhere = true,
        }
        Ok(())
    };
    for t in from {
        visit(&t.relation)?;
        for j in &t.joins {
            visit(&j.relation)?;
        }
    }
    if !engine {
        return Ok(None);
    }
    if elsewhere || from.len() != 1 || !from[0].joins.is_empty() {
        return Err(not_on_engine("a join"));
    }
    Ok(found)
}

/// The top-level `AND` terms of `e`.
fn conjuncts(e: CExpr, out: &mut Vec<CExpr>) {
    match e {
        CExpr::And(a, b) => {
            conjuncts(*a, out);
            conjuncts(*b, out);
        }
        other => out.push(other),
    }
}

/// Whether `e` reads no column (its value is fixed per execution).
fn constant(e: &CExpr) -> bool {
    match e {
        CExpr::Param(_) | CExpr::Lit(_) | CExpr::Now { .. } => true,
        CExpr::Neg(a) => constant(a),
        CExpr::Arith(_, a, b) => constant(a) && constant(b),
        _ => false,
    }
}

/// Picks the access path from the conjuncts, leaving the rest as a filter;
/// with a `Point`, also the whole `WHERE`.
#[allow(clippy::too_many_lines)] // one block per access path, tried in order
fn access(def: &TableDef, filter: Option<CExpr>) -> (Access, Option<CExpr>, Option<CExpr>) {
    let Some(f) = filter else {
        return (Access::Full, None, None);
    };
    let mut terms = Vec::new();
    conjuncts(f, &mut terms);
    let rebuild = |terms: Vec<CExpr>| {
        terms
            .into_iter()
            .reduce(|a, b| CExpr::And(Box::new(a), Box::new(b)))
    };
    let eq_on = |t: &CExpr, col: usize| -> Option<CExpr> {
        match t {
            CExpr::Cmp(Cmp::Eq, a, b, _) => match (&**a, &**b) {
                (CExpr::Col(c), v) | (v, CExpr::Col(c)) if *c == col && constant(v) => {
                    Some(v.clone())
                }
                _ => None,
            },
            _ => None,
        }
    };
    // The whole primary key by equality.
    let mut pk_vals = Vec::new();
    let mut used = Vec::new();
    for &pc in &def.pk {
        if let Some(i) = terms.iter().position(|t| eq_on(t, pc).is_some()) {
            pk_vals.push(eq_on(&terms[i], pc).unwrap_or(CExpr::Lit(super::row::Datum::Null)));
            used.push(i);
        }
    }
    if pk_vals.len() == def.pk.len() {
        let whole = rebuild(terms.clone());
        // A key on a column prefix finds the rows that share the prefix:
        // the equality itself stays in the filter.
        let prefixed = def.pk_parts.iter().any(|kc| kc.prefix.is_some());
        let rest: Vec<CExpr> = terms
            .into_iter()
            .enumerate()
            .filter(|(i, _)| prefixed || !used.contains(i))
            .map(|(_, t)| t)
            .collect();
        return (Access::Point(pk_vals), rebuild(rest), whole);
    }
    // The first columns of a composite primary key (kept in the filter
    // too): the equalities found above, as far as they go without a gap.
    let leading = def
        .pk
        .iter()
        .take_while(|&&pc| terms.iter().any(|t| eq_on(t, pc).is_some()))
        .count();
    if leading > 0 && def.pk.len() > 1 {
        pk_vals.truncate(leading);
        return (Access::PkPrefix(pk_vals), rebuild(terms), None);
    }
    // A one-column primary key among constants (kept in the filter too).
    if let [pc] = def.pk[..]
        && def.pk_parts[0].prefix.is_none()
    {
        let list = terms.iter().find_map(|t| match t {
            CExpr::In(a, list, false, _)
                if matches!(&**a, CExpr::Col(c) if *c == pc) && list.iter().all(constant) =>
            {
                Some(list.clone())
            }
            _ => None,
        });
        if let Some(list) = list {
            return (Access::PkIn(list), rebuild(terms), None);
        }
    }
    // A range on a one-column primary key (kept in the filter too).
    if let [pc] = def.pk[..] {
        let mut lo: Option<(CExpr, bool)> = None;
        let mut hi: Option<(CExpr, bool)> = None;
        for t in &terms {
            match t {
                CExpr::Between(a, l, h, false, _)
                    if matches!(&**a, CExpr::Col(c) if *c == pc) && constant(l) && constant(h) =>
                {
                    lo = Some(((**l).clone(), true));
                    hi = Some(((**h).clone(), true));
                }
                CExpr::Cmp(op, a, b, _) => {
                    let (op, v) = match (&**a, &**b) {
                        (CExpr::Col(c), v) if *c == pc && constant(v) => (*op, v),
                        (v, CExpr::Col(c)) if *c == pc && constant(v) => (flip(*op), v),
                        _ => continue,
                    };
                    match op {
                        Cmp::Gt => lo = Some((v.clone(), false)),
                        Cmp::Ge => lo = Some((v.clone(), true)),
                        Cmp::Lt => hi = Some((v.clone(), false)),
                        Cmp::Le => hi = Some((v.clone(), true)),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        if lo.is_some() || hi.is_some() {
            return (Access::PkRange { lo, hi }, rebuild(terms), None);
        }
    }
    // Equality on a secondary index's first column (kept in the filter).
    for (ix, x) in def.indexes.iter().enumerate() {
        if let Some(v) = terms.iter().find_map(|t| eq_on(t, x.cols[0])) {
            return (Access::IndexEq { ix, val: v }, rebuild(terms), None);
        }
    }
    (Access::Full, rebuild(terms), None)
}

fn flip(c: Cmp) -> Cmp {
    match c {
        Cmp::Lt => Cmp::Gt,
        Cmp::Le => Cmp::Ge,
        Cmp::Gt => Cmp::Lt,
        Cmp::Ge => Cmp::Le,
        o => o,
    }
}

fn number(e: &Expr) -> Result<u64> {
    match e {
        Expr::Value(v) => match &v.value {
            SqlValue::Number(n, _) => n.parse().map_err(|_| not_on_engine("LIMIT")),
            _ => Err(not_on_engine("LIMIT with a parameter")),
        },
        _ => Err(not_on_engine("LIMIT")),
    }
}

/// Binds `stmt`; `None` when it names no engine table.
///
/// # Errors
/// `ER_SPRIG_NOT_ON_ENGINE` for a statement on an engine table outside
/// the prototype's subset; 1146, 1054.
pub(crate) fn bind(cx: &BindCtx<'_>, stmt: &Statement) -> Result<Option<Plan>> {
    match stmt {
        Statement::Query(q) => bind_query(cx, q),
        Statement::Insert(ins) => bind_insert(cx, ins),
        Statement::Update(u) => {
            let Some((def, db, name, alias)) = single_table(cx, std::slice::from_ref(&u.table))?
            else {
                return Ok(None);
            };
            if u.from.is_some() || !u.order_by.is_empty() || u.returning.is_some() {
                return Err(not_on_engine("this UPDATE form"));
            }
            let mut c = compiler(cx, &def, name, alias);
            let mut sets = Vec::new();
            let mut written = vec![false; def.columns.len()];
            for a in &u.assignments {
                let sqlparser::ast::AssignmentTarget::ColumnName(n) = &a.target else {
                    return Err(not_on_engine("a tuple assignment"));
                };
                let parts = ident_parts(n).ok_or_else(|| not_on_engine("a column"))?;
                let Some(last) = parts.last() else {
                    return Err(not_on_engine("a column"));
                };
                let ci = def
                    .column(&last.value)
                    .ok_or_else(|| super::expr::unknown_column(&last.value, "SET"))?;
                written[ci] = true;
                // `SET col = DEFAULT`: the column's default, whatever kind
                // it is (a generated column is worked out anyway).
                if let Expr::Identifier(i) = &a.value
                    && i.quote_style.is_none()
                    && i.value.eq_ignore_ascii_case("DEFAULT")
                {
                    if let Some(v) = default_of(&def.columns[ci])? {
                        sets.push((ci, v));
                    }
                    continue;
                }
                let v = c.compile(&a.value)?;
                assignable(&def, ci, &v)?;
                c.hint(&v, def.columns[ci].ty);
                sets.push((ci, v));
            }
            let filter = u.selection.as_ref().map(|w| c.compile(w)).transpose()?;
            let limit = u.limit.as_ref().map(number).transpose()?;
            finish(
                cx,
                c,
                Arc::clone(&def),
                db,
                name,
                alias,
                Op::Update { sets },
                filter,
                false,
                limit,
                0,
                written,
            )
        }
        Statement::Delete(d) => {
            let from = match &d.from {
                FromTable::WithFromKeyword(f) | FromTable::WithoutKeyword(f) => f,
            };
            // `DELETE t FROM ...` and `DELETE FROM t USING ...` name their
            // targets by alias: not one table's shape (`multiwrite`).
            if !d.tables.is_empty() || d.using.is_some() {
                return Err(not_on_engine("this DELETE form"));
            }
            let Some((def, db, name, alias)) = single_table(cx, from)? else {
                return Ok(None);
            };
            if !d.tables.is_empty()
                || d.using.is_some()
                || !d.order_by.is_empty()
                || d.returning.is_some()
            {
                return Err(not_on_engine("this DELETE form"));
            }
            let mut c = compiler(cx, &def, name, alias);
            let filter = d.selection.as_ref().map(|w| c.compile(w)).transpose()?;
            let limit = d.limit.as_ref().map(number).transpose()?;
            let n = def.columns.len();
            finish(
                cx,
                c,
                Arc::clone(&def),
                db,
                name,
                alias,
                Op::Delete,
                filter,
                false,
                limit,
                0,
                vec![false; n],
            )
        }
        _ => Ok(None),
    }
}

/// Refuses `SET column = byte string column` for a target that isn't a
/// string: a byte string column's value is text to a number column, while
/// a hexadecimal literal's bytes are a number, and only the literal is
/// told apart once evaluated.
fn assignable(def: &TableDef, target: usize, value: &CExpr) -> Result<()> {
    use table::ColType as T;
    super::expr::now_into(value, &def.columns[target])?;
    let bytes = |c: usize| {
        matches!(
            def.columns[c].ty,
            T::Binary { .. } | T::VarBinary { .. } | T::Blob { .. }
        )
    };
    let text = matches!(
        def.columns[target].ty,
        T::Char { .. } | T::VarChar { .. } | T::Text { .. }
    );
    if let CExpr::Col(src) = value
        && bytes(*src)
        && !bytes(target)
        && !text
    {
        return Err(not_on_engine(
            "a byte string column assigned to a column that isn't a string",
        ));
    }
    Ok(())
}

fn compiler<'a>(
    cx: &BindCtx<'_>,
    def: &'a TableDef,
    name: &ObjectName,
    alias: Option<&Ident>,
) -> Compiler<'a> {
    let mut quals = Vec::new();
    match alias {
        Some(a) => quals.push(a.value.clone()),
        None => {
            if let Some(Some(last)) = ident_parts(name).map(|p| p.last().map(|i| i.value.clone())) {
                quals.push(last);
            }
        }
    }
    Compiler {
        def,
        quals,
        params: 0,
        read: vec![false; def.columns.len()],
        backslash: cx.backslash,
        hints: Vec::new(),
    }
}

/// What `SET col = DEFAULT` assigns: the column's default; `None` for a
/// generated column (worked out anyway).
fn default_of(col: &table::Column) -> Result<Option<CExpr>> {
    if col.generated.is_some() {
        return Ok(None);
    }
    Ok(Some(match &col.default {
        Some(table::ColDefault::Value(d)) => CExpr::Lit(d.clone()),
        Some(table::ColDefault::Expr(d)) => (*d.expr).clone(),
        None if col.nullable => CExpr::Lit(super::row::Datum::Null),
        _ => {
            return Err(not_on_engine(
                "SET column = DEFAULT for this column's kind of default",
            ));
        }
    }))
}

// The binders' common tail: always a plan, kept as their result type.
#[allow(clippy::too_many_arguments, clippy::unnecessary_wraps)]
fn finish(
    cx: &BindCtx<'_>,
    c: Compiler<'_>,
    def: Arc<TableDef>,
    db: DbId,
    name: &ObjectName,
    alias: Option<&Ident>,
    op: Op,
    filter: Option<CExpr>,
    reverse: bool,
    limit: Option<u64>,
    offset: u64,
    written: Vec<bool>,
) -> Result<Option<Plan>> {
    let (access, filter, unkeyed) = if matches!(op, Op::Insert { .. }) {
        (Access::Full, filter, None)
    } else {
        access(&def, filter)
    };
    let table_written = ident_parts(name)
        .and_then(|p| p.last().map(|i| i.value.clone()))
        .unwrap_or_default();
    let alias = alias.map_or_else(|| table_written.clone(), |a| a.value.clone());
    let db_name = cx.names.db_name(db);
    let fks = if matches!(op, Op::Select { .. }) {
        None
    } else {
        super::fk::Graph::build(cx, &def, db)
    };
    Ok(Some(Plan {
        op,
        db_name: Bytes::from(db_name),
        alias: Bytes::from(alias),
        org_table: Bytes::from(def.name.clone()),
        access,
        filter,
        unkeyed,
        reverse,
        limit,
        offset,
        params: c.params,
        param_types: c.hints,
        read: c.read,
        written,
        catalog_version: cx.names.version(),
        ordered: false,
        lock: None,
        check: None,
        fks,
        table: def,
    }))
}

#[allow(clippy::too_many_lines)] // one pass over a SELECT's clauses
fn bind_query(cx: &BindCtx<'_>, q: &Query) -> Result<Option<Plan>> {
    // A query with common table expressions is never one engine table's
    // statement, and the names in its `FROM` may be the expressions' own:
    // the caller refuses it if it names an engine table anywhere
    // ([`names_engine_table`]).
    if q.with.is_some() {
        return Ok(None);
    }
    let SetExpr::Select(s) = &*q.body else {
        // A set operation or VALUES naming engine tables.
        return if mentions_engine(cx, &q.body)? {
            Err(not_on_engine("a set operation"))
        } else {
            Ok(None)
        };
    };
    let Some((def, db, name, alias)) = single_table(cx, &s.from)? else {
        return Ok(None);
    };
    check_plain_select(s)?;
    if q.fetch.is_some() || !q.locks.is_empty() || q.for_clause.is_some() {
        return Err(not_on_engine("FETCH or a locking read"));
    }
    let mut c = compiler(cx, &def, name, alias);
    let mut out = Vec::new();
    for item in &s.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, col) in def.columns.iter().enumerate() {
                    // An invisible column isn't part of `*`.
                    if col.invisible {
                        continue;
                    }
                    c.read[i] = true;
                    out.push(OutCol {
                        col: i,
                        name: Bytes::from(col.name.clone()),
                    });
                }
            }
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                let col = match c.compile(e) {
                    Ok(CExpr::Col(i)) => i,
                    Ok(_) => return Err(not_on_engine("an expression in the select list")),
                    Err(err) if err.protocol().mysql_errno == 1054 => {
                        let n = match e {
                            Expr::Identifier(i) => i.value.clone(),
                            Expr::CompoundIdentifier(p) => p
                                .iter()
                                .map(|i| i.value.as_str())
                                .collect::<Vec<_>>()
                                .join("."),
                            _ => String::new(),
                        };
                        return Err(super::expr::unknown_column(&n, "SELECT"));
                    }
                    Err(err) => return Err(err),
                };
                let name = match (item, e) {
                    (SelectItem::ExprWithAlias { alias, .. }, _) => alias.value.clone(),
                    (_, Expr::Identifier(i)) => i.value.clone(),
                    (_, Expr::CompoundIdentifier(p)) => {
                        p.last().map_or_else(String::new, |i| i.value.clone())
                    }
                    _ => return Err(not_on_engine("a select item")),
                };
                out.push(OutCol {
                    col,
                    name: Bytes::from(name),
                });
            }
            _ => return Err(not_on_engine("a select item")),
        }
    }
    let filter = s.selection.as_ref().map(|w| c.compile(w)).transpose()?;
    let mut reverse = false;
    if let Some(ob) = &q.order_by {
        let OrderByKind::Expressions(es) = &ob.kind else {
            return Err(not_on_engine("ORDER BY ALL"));
        };
        // Only the primary key's order (a one-column key).
        match (es.as_slice(), def.pk.as_slice()) {
            ([o], [pc]) => {
                let col = c.compile(&o.expr)?;
                if !matches!(col, CExpr::Col(i) if i == *pc) || o.options.nulls_first.is_some() {
                    return Err(not_on_engine("ORDER BY other than the primary key"));
                }
                // The key's own direction counts: a `DESC` key holds the
                // largest value first.
                reverse = matches!(o.options.sort, Some(OrderBySort::Desc)) != def.pk_parts[0].desc;
            }
            _ => return Err(not_on_engine("ORDER BY other than the primary key")),
        }
    }
    let (limit, offset) = match &q.limit_clause {
        None => (None, 0),
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if !limit_by.is_empty() {
                return Err(not_on_engine("LIMIT BY"));
            }
            (
                limit.as_ref().map(number).transpose()?,
                offset
                    .as_ref()
                    .map(|o| number(&o.value))
                    .transpose()?
                    .unwrap_or(0),
            )
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            (Some(number(limit)?), number(offset)?)
        }
    };
    let n = def.columns.len();
    let ordered = q.order_by.is_some();
    let plan = finish(
        cx,
        c,
        Arc::clone(&def),
        db,
        name,
        alias,
        Op::Select { out },
        filter,
        reverse,
        limit,
        offset,
        vec![false; n],
    )?;
    Ok(plan.map(|mut p| {
        p.ordered = ordered;
        p
    }))
}

fn check_plain_select(s: &Select) -> Result<()> {
    let grouped =
        !matches!(&s.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty());
    if s.distinct.is_some()
        || s.top.is_some()
        || s.into.is_some()
        || grouped
        || s.having.is_some()
        || s.qualify.is_some()
        || !s.lateral_views.is_empty()
        || s.prewhere.is_some()
        || !s.sort_by.is_empty()
        || !s.named_window.is_empty()
    {
        return Err(not_on_engine("this SELECT form"));
    }
    Ok(())
}

/// Whether a set expression names an engine table anywhere at its top.
fn mentions_engine(cx: &BindCtx<'_>, e: &SetExpr) -> Result<bool> {
    Ok(match e {
        SetExpr::Select(s) => {
            for t in &s.from {
                for f in std::iter::once(&t.relation).chain(t.joins.iter().map(|j| &j.relation)) {
                    if let TableFactor::Table { name, .. } = f
                        && matches!(resolve(cx, name)?, Resolved::Engine(..))
                    {
                        return Ok(true);
                    }
                }
            }
            false
        }
        SetExpr::Query(q) => mentions_engine(cx, &q.body)?,
        SetExpr::SetOperation { left, right, .. } => {
            mentions_engine(cx, left)? || mentions_engine(cx, right)?
        }
        _ => false,
    })
}

#[allow(clippy::too_many_lines)] // the forms, the columns, the rows
fn bind_insert(cx: &BindCtx<'_>, ins: &sqlparser::ast::Insert) -> Result<Option<Plan>> {
    let TableObject::TableName(name) = &ins.table else {
        return Ok(None);
    };
    let (def, db) = match resolve(cx, name)? {
        Resolved::Elsewhere => {
            // `INSERT INTO other SELECT ... FROM engine_table`.
            if let Some(src) = &ins.source
                && mentions_engine(cx, &src.body)?
            {
                return Err(not_on_engine("INSERT ... SELECT"));
            }
            return Ok(None);
        }
        Resolved::Engine(def, db) => (def, db),
    };
    if ins.ignore
        || ins.replace_into
        || ins.on.is_some()
        || ins.returning.is_some()
        || ins.partitioned.is_some()
        || ins.insert_alias.is_some()
    {
        return Err(not_on_engine("this INSERT form"));
    }
    // `INSERT ... SET c = v, ...`: one row of those columns.
    let set_form: Option<(Vec<ObjectName>, Vec<Expr>)> = if ins.assignments.is_empty() {
        None
    } else {
        let mut names = Vec::new();
        let mut row = Vec::new();
        for a in &ins.assignments {
            let sqlparser::ast::AssignmentTarget::ColumnName(n) = &a.target else {
                return Err(not_on_engine("a tuple assignment"));
            };
            names.push(n.clone());
            row.push(a.value.clone());
        }
        Some((names, row))
    };
    let rows_in: Vec<&[Expr]> = match (&set_form, &ins.source) {
        (Some((_, row)), None) => vec![row.as_slice()],
        (None, Some(src)) => {
            let SetExpr::Values(values) = &*src.body else {
                return Err(not_on_engine("INSERT ... SELECT"));
            };
            values.rows.iter().map(|r| r.content.as_slice()).collect()
        }
        _ => return Err(not_on_engine("INSERT without VALUES")),
    };
    let listed: &[ObjectName] = match &set_form {
        Some((names, _)) => names,
        None => &ins.columns,
    };
    let cols: Vec<usize> = if listed.is_empty() {
        // Without a column list, the visible columns in order.
        (0..def.columns.len())
            .filter(|&i| !def.columns[i].invisible)
            .collect()
    } else {
        let mut v = Vec::with_capacity(listed.len());
        for n in listed {
            let parts = ident_parts(n).ok_or_else(|| not_on_engine("a column"))?;
            let Some(last) = parts.last() else {
                return Err(not_on_engine("a column"));
            };
            let ci = def
                .column(&last.value)
                .ok_or_else(|| super::expr::unknown_column(&last.value, "INSERT INTO"))?;
            v.push(ci);
        }
        v
    };
    let mut c = compiler(cx, &def, name, None);
    let mut rows = Vec::with_capacity(rows_in.len());
    for (ri, r) in rows_in.iter().enumerate() {
        if r.len() != cols.len() {
            return Err(SprigError::standard(
                1136,
                *b"21S01",
                format!("Column count doesn't match value count at row {}", ri + 1),
            ));
        }
        let mut row = Vec::with_capacity(cols.len());
        for e in *r {
            if let Expr::Identifier(i) = e
                && i.quote_style.is_none()
                && i.value.eq_ignore_ascii_case("DEFAULT")
            {
                row.push(None);
                continue;
            }
            let x = c.compile(e)?;
            if !constant(&x) {
                return Err(not_on_engine("a column reference in VALUES"));
            }
            super::expr::now_into(&x, &def.columns[cols[row.len()]])?;
            c.hint(&x, def.columns[cols[row.len()]].ty);
            row.push(Some(x));
        }
        rows.push(row);
    }
    let mut written = vec![false; def.columns.len()];
    for &ci in &cols {
        written[ci] = true;
    }
    finish(
        cx,
        c,
        Arc::clone(&def),
        db,
        name,
        None,
        Op::Insert { cols, rows },
        None,
        false,
        None,
        0,
        written,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_schema_names_resolve_to_the_tenants_databases() {
        let t = TenantId(61_306_000_001);
        assert_eq!(physical_db(t, "t61306000001_d3_g4_s5"), Some(DbId(3)));
        assert_eq!(physical_db(t, "t61306000002_d3_g4_s5"), None);
        assert_eq!(physical_db(t, "shop"), None);
    }
}
