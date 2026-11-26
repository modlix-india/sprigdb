//! General `SELECT`s on engine tables: the statements the single-table
//! binder ([`super::plan`]) doesn't take (joins, grouping, `DISTINCT`,
//! ordering off the key, expressions in the select list, set operations,
//! derived tables, common table expressions, subqueries) are planned by
//! `sprig_plan` and run on `sprig_exec`, over the session's engine
//! transaction.
//!
//! ```text
//! statement text (names mapped, row-level security predicates written in)
//!   ├─ plan::bind takes it            → the single-table path, as before
//!   └─ otherwise, a SELECT            → bind (this module):
//!        names::analyze   every table it names is an engine table of the
//!                         tenant, else 39960; the columns it may read
//!        [the seam]       privileges on those tables and columns, and the
//!                         row-level security coverage of each table
//!        run              storage::TxStorage behind sprig_plan's Storage:
//!                         only the bound tables, only the checked columns
//!        results          column definitions and values per port
//! ```
//!
//! - **What a statement may read is fixed before it is planned.** The
//!   planner gets a catalog that holds only the tables [`bind`] resolved,
//!   and storage that serves only those tables and, of their rows, only the
//!   columns the statement names ([`names`]); every other column reads as
//!   `NULL`. The caller checks privileges on exactly that set, so a column
//!   the session wasn't checked for can't reach a result.
//! - **Row-level security** predicates arrive in the statement's text (the
//!   tenant layer's rewrite puts each protected table's predicate on that
//!   table's rows before any join); the seam refuses a statement that reads
//!   a protected table the rewrite didn't cover.
//! - **One synchronous section.** A plan holds `Rc`s and can't cross an
//!   `await`: it is planned, run to its last row and dropped between two
//!   awaits, with the rows collected. A statement that reads many rows
//!   moves off the runtime's worker (`block_in_place`) after its first
//!   stretch.
//! - **Reads are consistent reads** at the transaction's read view. A
//!   locking clause, and any `SELECT` inside a SERIALIZABLE transaction,
//!   read the rows as they are now and lock what they read, as the
//!   single-table path does; such a statement runs off the runtime's
//!   worker, since a lock wait blocks its thread.

pub(crate) mod expanded;
pub(crate) mod names;
pub(crate) mod results;
pub(crate) mod storage;

use std::sync::Arc;
use std::time::Instant;

use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::{Collation, Flavor};
use sprig_engine::txn::{LockMode, TxnOptions, Wait};
use sprig_exec::{Ctx, Persona, Session, SqlError, SqlMode};
use sprig_plan::{PlanOptions, Planner};
use sqlparser::ast::Statement;

use self::names::TableUse;
use self::storage::{Bound, TxStorage};
use super::assign::Env;
use super::keys::Space;
use super::plan::{Access, BindCtx, Op, Plan};
use super::row::Datum;
use super::session::SessionTxn;
use super::store::Tx;
use super::{Engine, not_on_engine};

/// The most bytes one statement's sorts, hash tables and held derived
/// rows may take.
const MEMORY_LIMIT: usize = 256 << 20;

/// Rows a statement pulls on the runtime's worker before the rest of it
/// moves off the worker.
const INLINE_ROWS: u64 = 4096;

/// A bound general `SELECT` (or `EXPLAIN SELECT`).
#[derive(Debug)]
pub(crate) struct General {
    /// The statement, with database names in their logical form.
    pub stmt: Statement,
    /// `EXPLAIN SELECT`: the plan is the answer.
    pub explain: bool,
    /// The `EXPLAIN` is answered with MySQL's Note 1003 on the MySQL port
    /// (its traditional and JSON forms; not the tree the MySQL port draws
    /// from a JSON plan, which `sprig_sql::compat::explain` marks).
    pub explain_note: bool,
    /// `ANALYZE` (MariaDB's; the MySQL port's `EXPLAIN ANALYZE` arrives
    /// as `ANALYZE FORMAT=JSON`): the statement runs, and the plan with
    /// what it measured is the answer; `Some(true)` for the JSON form.
    pub analyze: Option<bool>,
    /// Every table the statement reads, with the columns it may read.
    pub tables: Vec<TableUse>,
    /// Where each result column comes from, when the select list says so
    /// (a plain select of table columns); empty otherwise.
    pub origins: Vec<Option<names::Origin>>,
    /// The name of each computed result column as the client wrote it,
    /// where the select list says (parallel to `origins`).
    pub written: Vec<Option<String>>,
    /// `?` placeholders.
    pub params: usize,
    /// The catalog version it was bound at.
    pub catalog_version: u64,
    /// The current database's logical name.
    pub current: Option<String>,
    /// The locking clause (`FOR UPDATE`, `LOCK IN SHARE MODE`, `FOR
    /// SHARE`): every row read is locked.
    pub lock: Option<crate::engine::plan::RowLock>,
    /// Per table: it is read in the outermost query block's `FROM`.
    pub top: Vec<bool>,
    /// A write's `SELECT` (`multiwrite`): per table, whether its rows are
    /// locked exclusively (a target of the write) or in shared mode;
    /// every table read is locked, subqueries' too.
    pub lock_each: Option<Vec<bool>>,
    /// The views it reads (expanded into `stmt`), and the tables it
    /// names itself: whose rights each table is read with.
    pub views: names::Views,
    /// Per result column computed by a view read alone: (`table`, `org_table`)
    /// as the servers name a merged view's column.
    pub view_named: Vec<Option<(String, String)>>,
    /// The sequences its sequence functions name.
    pub sequences: Vec<names::SeqUse>,
}

impl General {
    /// A single-table plan standing in for the statement where the
    /// prepared-statement record wants one (it is never run): its first
    /// table, nothing selected.
    pub(crate) fn placeholder(&self) -> Option<Plan> {
        let t = self.tables.first()?;
        Some(Plan {
            op: Op::Select { out: Vec::new() },
            table: Arc::clone(&t.def),
            db_name: bytes::Bytes::from(t.db_name.clone()),
            alias: bytes::Bytes::from(t.def.name.clone()),
            org_table: bytes::Bytes::from(t.def.name.clone()),
            access: Access::Full,
            filter: None,
            unkeyed: None,
            reverse: false,
            limit: None,
            offset: 0,
            params: self.params,
            param_types: vec![None; self.params],
            read: vec![false; t.def.columns.len()],
            written: vec![false; t.def.columns.len()],
            catalog_version: self.catalog_version,
            ordered: false,
            lock: None,
            check: None,
            view_check: None,
            fks: None,
        })
    }
}

/// Whether the single-table binder's refusal `e` of `stmt` leaves the
/// statement to the planner: a `SELECT` it answered 39960 for.
pub(crate) fn takes(stmt: &Statement, e: &SprigError) -> bool {
    matches!(stmt, Statement::Query(_)) && e.code() == Some(sprig_common::SprigCode::NotOnEngine)
}

/// Binds `stmt` when it is a `SELECT` (or `EXPLAIN SELECT`) that names
/// engine tables; `None` when it names none, or is another statement.
/// `text`: the text `stmt` was parsed from, when the caller has it (the
/// names of computed columns are cut from it). `locking`: the text carried
/// a locking clause (`FOR UPDATE`, ...).
///
/// # Errors
/// `ER_SPRIG_NOT_ON_ENGINE` for a statement that also names a table of a
/// MariaDB-backed database or a view, a locking read, or a column type the
/// executor can't compute with; 1146.
pub(crate) fn bind(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
    lock: Option<crate::engine::plan::RowLock>,
) -> Result<Option<General>> {
    bind_in(cx, stmt, text, lock, false)
}

/// [`bind`] for a set operation or table value constructor that reads no
/// table (`VALUES ROW(1), ROW(2)`, `SELECT 1 UNION SELECT 2`): the MySQL
/// port plans it here, so it is typed and described as MySQL does, not by
/// the MariaDB backend.
///
/// # Errors
/// As for [`bind`].
/// [`bind`] for the select of a view being defined: also when it reads no
/// table.
///
/// # Errors
/// As for [`bind`].
pub(crate) fn bind_view(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
) -> Result<Option<General>> {
    bind_in(cx, stmt, text, None, true)
}

pub(crate) fn bind_tableless(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
) -> Result<Option<General>> {
    use sqlparser::ast::SetExpr;
    let Statement::Query(q) = stmt else {
        return Ok(None);
    };
    // A set operation or VALUES, alone or as a derived table; or a
    // `JSON_TABLE` (MariaDB, which would run it, types it its way).
    let derived_values = match &*q.body {
        SetExpr::Select(s) => s.from.iter().any(|t| {
            matches!(&t.relation, sqlparser::ast::TableFactor::Derived { subquery, .. }
                if matches!(&*subquery.body, SetExpr::Values(_) | SetExpr::SetOperation { .. }))
                || std::iter::once(&t.relation)
                    .chain(t.joins.iter().map(|j| &j.relation))
                    .any(|f| matches!(f, sqlparser::ast::TableFactor::JsonTable { .. }))
        }),
        _ => false,
    };
    // Or common table expressions alone (`WITH RECURSIVE` number series).
    if !derived_values
        && q.with.is_none()
        && !matches!(&*q.body, SetExpr::Values(_) | SetExpr::SetOperation { .. })
    {
        return Ok(None);
    }
    // Any table at all (`information_schema`'s too) leaves it where it was.
    if names_table(stmt) {
        return Ok(None);
    }
    bind_in(cx, stmt, text, None, true)
}

/// Whether a statement names a table: any name in a `FROM` other than a
/// common table expression the statement defines.
fn names_table(stmt: &Statement) -> bool {
    struct Ctes(std::collections::HashSet<String>);
    impl sqlparser::ast::Visitor for Ctes {
        type Break = ();
        fn pre_visit_query(&mut self, q: &sqlparser::ast::Query) -> std::ops::ControlFlow<()> {
            if let Some(w) = &q.with {
                for c in &w.cte_tables {
                    self.0.insert(c.alias.name.value.to_lowercase());
                }
            }
            std::ops::ControlFlow::Continue(())
        }
    }
    let mut ctes = Ctes(std::collections::HashSet::new());
    let _ = sqlparser::ast::Visit::visit(stmt, &mut ctes);
    let mut found = false;
    let _ = sqlparser::ast::visit_relations(stmt, |n: &sqlparser::ast::ObjectName| {
        let cte = match n.0.as_slice() {
            [sqlparser::ast::ObjectNamePart::Identifier(i)] => {
                ctes.0.contains(&i.value.to_lowercase())
            }
            _ => false,
        };
        if cte {
            return std::ops::ControlFlow::Continue(());
        }
        found = true;
        std::ops::ControlFlow::<()>::Break(())
    });
    found
}

/// [`bind`] for any query that names no table at all (not even
/// `information_schema`'s): constants, functions, set operations. For a
/// tenant whose every database is on the engine, where MariaDB has nothing
/// to answer.
///
/// # Errors
/// As for [`bind`].
pub(crate) fn bind_no_tables(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
) -> Result<Option<General>> {
    // The query, or the query an `EXPLAIN` describes.
    let inner = match stmt {
        Statement::Query(_) => stmt,
        Statement::Explain { statement, .. } if matches!(**statement, Statement::Query(_)) => {
            statement
        }
        _ => return Ok(None),
    };
    if names_table(inner) {
        return Ok(None);
    }
    bind_in(cx, stmt, text, None, true)
}

fn bind_in(
    cx: &BindCtx<'_>,
    stmt: &Statement,
    text: Option<&str>,
    lock: Option<crate::engine::plan::RowLock>,
    tableless: bool,
) -> Result<Option<General>> {
    let mut analyze_json = None;
    let explain = match stmt {
        Statement::Query(_) => false,
        Statement::Explain {
            statement,
            analyze,
            format,
            query_plan,
            estimate,
            options,
            ..
        } if matches!(**statement, Statement::Query(_)) => {
            use sqlparser::ast::{AnalyzeFormat as F, AnalyzeFormatKind as K};
            let planner_form = matches!(
                format,
                None | Some(
                    K::Keyword(F::JSON | F::TRADITIONAL) | K::Assignment(F::JSON | F::TRADITIONAL)
                )
            );
            if *analyze {
                analyze_json = Some(matches!(
                    format,
                    Some(K::Keyword(F::JSON) | K::Assignment(F::JSON))
                ));
            }
            if !planner_form || *query_plan || *estimate || options.is_some() {
                // The tabular and JSON forms are the planner's; the other
                // forms would be answered from MariaDB's empty copy.
                return match names::analyze(cx, stmt, None, false)? {
                    Some(_) => Err(not_on_engine(
                        "EXPLAIN in another form than plain EXPLAIN SELECT, on engine tables",
                    )),
                    None => Ok(None),
                };
            }
            true
        }
        _ => return Ok(None),
    };
    let Some(a) = names::analyze(cx, stmt, text.filter(|_| !explain), tableless)? else {
        return Ok(None);
    };
    if lock.is_some() && explain {
        return Err(not_on_engine("EXPLAIN of a locking read"));
    }
    let top = top_tables(&a.stmt, &a.tables);
    Ok(Some(General {
        stmt: a.stmt,
        explain,
        explain_note: explain
            && analyze_json.is_none()
            && text.is_none_or(|t| !t.contains(sprig_sql::compat::explain::TREE_MARK)),
        analyze: analyze_json,
        tables: a.tables,
        origins: a.origins,
        written: a.written,
        params: a.params,
        catalog_version: cx.names.version(),
        current: cx.current.map(|d| cx.names.db_name(d)),
        top,
        lock,
        lock_each: None,
        views: a.views,
        view_named: a.view_named,
        sequences: a.sequences,
    }))
}

/// Per table of `tables`: whether the outermost query block of `stmt`
/// reads it in its `FROM` (by one of the names it is read under).
fn top_tables(stmt: &Statement, tables: &[TableUse]) -> Vec<bool> {
    use sqlparser::ast::{SetExpr, TableFactor};
    let mut names: Vec<String> = Vec::new();
    let query = match stmt {
        Statement::Query(q) => Some(q),
        _ => None,
    };
    if let Some(q) = query
        && let SetExpr::Select(s) = &*q.body
    {
        let mut add = |f: &TableFactor| {
            if let TableFactor::Table { name, alias, .. } = f {
                if let Some(a) = alias {
                    names.push(a.name.value.to_lowercase());
                }
                if let Some(sqlparser::ast::ObjectNamePart::Identifier(i)) = name.0.last() {
                    names.push(i.value.to_lowercase());
                }
            }
        };
        for t in &s.from {
            add(&t.relation);
            for j in &t.joins {
                add(&j.relation);
            }
        }
    }
    tables
        .iter()
        .map(|t| t.aliases.iter().any(|a| names.contains(a)))
        .collect()
}

/// How a statement's reads lock.
#[derive(Debug, Clone, Copy)]
struct Lock {
    mode: LockMode,
    wait: Wait,
    /// Every table read is locked (SERIALIZABLE); otherwise only the
    /// tables of the outermost query block, as both servers lock for a
    /// locking clause (a subquery's tables are read without locks).
    all: bool,
}

/// A locking clause's lock.
fn lock_of(l: crate::engine::plan::RowLock, all: bool) -> Lock {
    Lock {
        mode: if l.exclusive {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        },
        wait: l.wait,
        all,
    }
}

/// What the session's variables say to the expression engine, and to the
/// planner (`ONLY_FULL_GROUP_BY`).
#[derive(Debug, Clone)]
pub(crate) struct Seed {
    /// The session, without the statement's clock.
    pub session: Session,
    /// `ONLY_FULL_GROUP_BY`.
    pub only_full_group_by: bool,
    /// `WITH RECURSIVE`'s limit: `cte_max_recursion_depth` (3306),
    /// `max_recursive_iterations` (3307).
    pub max_recursion: u64,
}

impl Seed {
    /// From the session's variables as the backend session state tracks
    /// them (`get`), under the port's defaults for those it doesn't know.
    pub(crate) fn from_vars<'v>(
        mysql: bool,
        env: &Env,
        get: impl Fn(&str) -> Option<&'v str>,
    ) -> Self {
        let persona = if mysql {
            Persona::MySql
        } else {
            Persona::MariaDb
        };
        let mut s = Session::new(persona);
        let mut only_full_group_by = mysql;
        if let Some(m) = get("sql_mode") {
            let has = |f: &str| m.split(',').any(|x| x.trim().eq_ignore_ascii_case(f));
            let traditional = has("TRADITIONAL");
            let mut bits = 0;
            let mut set = |on: bool, bit: u32| {
                if on {
                    bits |= bit;
                }
            };
            set(
                has("STRICT_TRANS_TABLES") || has("STRICT_ALL_TABLES") || traditional,
                SqlMode::STRICT,
            );
            set(
                has("ERROR_FOR_DIVISION_BY_ZERO") || traditional,
                SqlMode::ERROR_FOR_DIVISION_BY_ZERO,
            );
            set(
                has("NO_UNSIGNED_SUBTRACTION"),
                SqlMode::NO_UNSIGNED_SUBTRACTION,
            );
            set(has("NO_ZERO_DATE") || traditional, SqlMode::NO_ZERO_DATE);
            set(
                has("NO_ZERO_IN_DATE") || traditional,
                SqlMode::NO_ZERO_IN_DATE,
            );
            set(has("ALLOW_INVALID_DATES"), SqlMode::ALLOW_INVALID_DATES);
            // The MySQL port's backend mode carries TIME_ROUND_FRACTIONAL,
            // which is MySQL's rule always.
            set(
                has("TIME_ROUND_FRACTIONAL") || mysql,
                SqlMode::TIME_ROUND_FRACTIONAL,
            );
            set(has("NO_BACKSLASH_ESCAPES"), SqlMode::NO_BACKSLASH_ESCAPES);
            s.sql_mode = SqlMode(bits);
            only_full_group_by = has("ONLY_FULL_GROUP_BY");
        }
        if let Some(c) = get("collation_connection") {
            // The backend names MySQL's utf8mb4 default by MariaDB's.
            let named = if mysql && c.eq_ignore_ascii_case("utf8mb4_uca1400_ai_ci") {
                Some(Collation::Utf8mb4Mysql0900AiCi)
            } else {
                let flavor = if c.to_ascii_lowercase().contains("_0900_") {
                    Flavor::MySql
                } else {
                    Flavor::MariaDb
                };
                Collation::resolve(c, flavor)
            };
            if let Some(n) = named {
                s.collation_connection = n;
                let l = c.to_ascii_lowercase();
                s.connection_mb3 = l.starts_with("utf8mb3_") || l.starts_with("utf8_");
            }
        }
        let num = |k: &str| get(k).and_then(|v| v.trim().parse::<u64>().ok());
        if let Some(n) = num("div_precision_increment") {
            s.div_precision_increment = u8::try_from(n.min(30)).unwrap_or(4);
        }
        if let Some(n) = num("group_concat_max_len") {
            s.group_concat_max_len = n;
        }
        if let Some(n) = num("max_allowed_packet") {
            s.max_allowed_packet = n;
        }
        if let Some(n) = num("default_week_format") {
            s.default_week_format = u8::try_from(n.min(7)).unwrap_or(0);
        }
        s.time_zone_offset = env.tz.unwrap_or(0);
        // The MySQL port's `SET cte_max_recursion_depth` reaches the
        // session under MariaDB's name for it.
        let max_recursion = (if mysql {
            num("cte_max_recursion_depth").or_else(|| num("max_recursive_iterations"))
        } else {
            num("max_recursive_iterations")
        })
        .unwrap_or(1000);
        Self {
            session: s,
            only_full_group_by,
            max_recursion,
        }
    }

    /// The persona's defaults in UTC (tests).
    #[cfg(test)]
    pub(crate) fn of(mysql: bool) -> Self {
        Self::from_vars(mysql, &Env::STRICT, |_| None)
    }

    /// The session for a statement that starts now.
    fn now(&self) -> Session {
        let mut s = self.session.clone();
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        s.now_seconds = i64::try_from(t.as_secs()).unwrap_or(i64::MAX);
        s.now_micros = t.subsec_micros();
        s
    }
}

/// What a general `SELECT` gave.
#[derive(Debug)]
#[allow(clippy::struct_field_names)] // `rows.rows`: the rows of the answer
pub(crate) struct Rows {
    /// The result columns as the planner typed them.
    pub columns: Vec<sprig_plan::Column>,
    /// The rows.
    pub rows: Vec<Vec<Datum>>,
    /// How many notes and warnings the statement raised.
    pub warnings: u16,
    /// The first of them (as many as the servers list).
    pub conds: Vec<sprig_exec::Warning>,
    /// Rows read from the store.
    pub examined: u64,
    /// Some table was read whole, without a key (`NO_INDEX_USED`).
    pub no_index: bool,
    /// Some table was read whole, by a key or not: what a write reports
    /// as "no index used".
    pub full_scan: bool,
    /// `SQL_CALC_FOUND_ROWS`: the rows found without the `LIMIT`.
    pub found: Option<u64>,
}

/// An executor error as the gateway's.
pub(crate) fn sql_error(e: SqlError) -> SprigError {
    match e.code {
        // What the planner or the expression engine doesn't cover yet: the
        // statement is refused, as every statement outside the engine's
        // reach is. (1305 here is a built-in function it doesn't have;
        // stored functions don't run on the engine.)
        1235 if e.message.starts_with("This version of SprigDB") => not_on_engine(
            e.message
                .strip_prefix("This version of SprigDB doesn't yet support '")
                .and_then(|m| m.strip_suffix('\''))
                .unwrap_or(&e.message),
        ),
        1305 => not_on_engine(&format!("a function it doesn't have ({})", e.message)),
        _ => {
            let mut state = *b"HY000";
            if e.sqlstate.len() == 5 {
                state.copy_from_slice(e.sqlstate.as_bytes());
            }
            SprigError::standard(e.code, state, e.message)
        }
    }
}

/// How a statement runs: its parameters, session and limits.
#[derive(Debug)]
pub(crate) struct RunArgs<'a> {
    /// The tenant.
    pub tenant: u64,
    /// The bound parameters.
    pub params: &'a [Datum],
    /// The session's variables.
    pub seed: &'a Seed,
    /// When the session's statement time limit passes, and its error.
    pub deadline: Option<(Instant, fn() -> SprigError)>,
    /// The session, whose `KILL QUERY` ends a lock wait of the statement.
    pub stop: Option<Arc<crate::session::SessionHandle>>,
}

/// Ends the lock wait of the transaction `h` belongs to, from a task of
/// its own, when the session's `KILL QUERY` comes or the statement's time
/// limit passes: a locking read through the planner waits on its thread,
/// where neither can reach it otherwise. Aborted when the statement ends.
fn interrupt_on_stop(
    h: sprig_engine::txn::InterruptHandle,
    stop: Option<Arc<crate::session::SessionHandle>>,
    deadline: Option<Instant>,
) -> tokio::task::JoinHandle<()> {
    use sprig_engine::txn::Interrupt;
    tokio::spawn(async move {
        let killed = async {
            match &stop {
                Some(s) => s.flags().query_killed().await,
                None => std::future::pending().await,
            }
        };
        let timed = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d.into()).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            () = killed => h.interrupt(Interrupt::Kill),
            () = timed => h.interrupt(Interrupt::StatementTimeout),
        }
    })
}

impl Engine {
    /// The keyspace of each of `g`'s tables, as `tx` (a session's
    /// transaction) holds them or as they are now.
    async fn general_spaces(
        &self,
        tenant: u64,
        g: &General,
        mut tx: Option<&mut Tx>,
    ) -> Result<Vec<Space>> {
        let mut out = Vec::with_capacity(g.tables.len());
        for t in &g.tables {
            // A MongoDB collection has no table keyspace: storage reads its
            // documents (`storage::coll`).
            if t.coll.is_some() {
                out.push(storage::unread_space(&t.def));
                continue;
            }
            let held = tx.as_ref().and_then(|x| x.held(&t.def));
            let sp = if let Some(sp) = held {
                sp
            } else {
                // A parent's table read through a share: its own keyspace.
                let sp = self.space(t.tenant.unwrap_or(tenant), &t.def).await?;
                if let Some(x) = tx.as_mut() {
                    x.hold(&t.def, &sp);
                }
                sp
            };
            // A table from before the row record whose string keys were
            // written under the first collation names: its key order isn't
            // its columns' order, which the planner's ranges assume.
            if sp.first_format
                && (t.def.pk_key(true) != t.def.pk_key(false)
                    || (0..t.def.indexes.len())
                        .any(|ix| t.def.index_key(ix, true) != t.def.index_key(ix, false)))
            {
                return Err(not_on_engine(
                    "a join, grouping or expression on a table in the first row format whose \
                     string keys keep their first collations",
                ));
            }
            out.push(sp);
        }
        Ok(out)
    }

    /// Plans and runs `g` on `tx`, in one synchronous section: the
    /// transaction comes back with the result.
    #[allow(clippy::too_many_lines)] // storage, the run, its stretches
    fn general_exec(
        g: &General,
        spaces: Vec<Space>,
        tx: Option<Tx>,
        a: &RunArgs<'_>,
        lock: Option<Lock>,
    ) -> (Option<Tx>, Result<Rows>) {
        // Without a transaction storage holds no rows: the plan is only
        // described.
        let dry = tx.is_none();
        let bounds: Vec<Bound> = g
            .tables
            .iter()
            .zip(spaces)
            .enumerate()
            .map(|(i, (t, sp))| {
                let l = match (&g.lock_each, lock) {
                    (Some(each), Some(l)) if !dry => {
                        let mode = if each.get(i).copied().unwrap_or(false) {
                            LockMode::Exclusive
                        } else {
                            LockMode::Shared
                        };
                        (l.all || g.top.get(i).copied().unwrap_or(true)).then_some((mode, l.wait))
                    }
                    _ => lock
                        .filter(|l| !dry && (l.all || g.top.get(i).copied().unwrap_or(true)))
                        .map(|l| (l.mode, l.wait)),
                };
                Bound::new(t, sp, l)
            })
            .collect();
        let interrupt = lock
            .filter(|_| !dry)
            .and_then(|_| tx.as_ref().map(Tx::interrupt_handle));
        let storage = TxStorage::new(tx, bounds, a.deadline);
        // A locking read may wait for a lock, blocking its thread: it runs
        // off the runtime's worker from the start.
        let off_worker = lock.is_some()
            && !dry
            && tokio::runtime::Handle::try_current()
                .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
        let catalog = names::PlanCatalog::new(g);
        let session = a.seed.now();
        let run = || -> std::result::Result<Rows, SqlError> {
            let mut planner = Planner::new(&catalog, storage.shared(), &session);
            planner.options = PlanOptions {
                only_full_group_by: a.seed.only_full_group_by,
                memory_limit: Some(MEMORY_LIMIT),
                max_recursion: a.seed.max_recursion,
                ..PlanOptions::for_persona(session.persona)
            };
            let started = std::time::Instant::now();
            let stmt = match (&g.analyze, &g.stmt) {
                (Some(_), Statement::Explain { statement, .. }) => &**statement,
                _ => &g.stmt,
            };
            let mut plan = planner.plan(stmt, a.params)?;
            if !g.explain && !dry {
                crate::autoindex::usage::note_plan(a.tenant, g, &plan.explain);
            }
            let mut ctx = Ctx::with_params(&session, a.params);
            if g.explain_note
                && session.persona == sprig_exec::Persona::MySql
                && let Some(text) =
                    expanded::expanded(&g.stmt, &g.tables, g.current.as_deref(), &g.written)
            {
                ctx.note(1003, text);
            }
            let mut rows = Vec::new();
            let mut done = dry;
            // The first stretch runs where it is; a statement that is
            // still reading after it leaves the runtime's worker to the
            // other sessions.
            while !done && plan.rows_examined() < INLINE_ROWS && (rows.len() as u64) < INLINE_ROWS {
                match plan.source.next(&mut ctx)? {
                    Some(r) => rows.push(r),
                    None => done = true,
                }
            }
            if !done {
                let mut rest = || -> std::result::Result<(), SqlError> {
                    while let Some(r) = plan.source.next(&mut ctx)? {
                        rows.push(r);
                    }
                    Ok(())
                };
                if !off_worker
                    && tokio::runtime::Handle::try_current().is_ok_and(|h| {
                        h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
                    })
                {
                    tokio::task::block_in_place(rest)?;
                } else {
                    rest()?;
                }
            }
            // `ANALYZE`: the rows go; the plan with what it measured is
            // the answer.
            if let Some(json) = g.analyze {
                let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                let steps = sprig_plan::analyzed(&plan.explain, &plan.accesses, micros);
                let (mut src, columns) = sprig_plan::analyze_answer(&steps, micros, &session, json);
                rows.clear();
                while let Some(r) = src.next(&mut ctx)? {
                    rows.push(r);
                }
                plan.columns = columns;
            }
            let no_index = plan
                .explain
                .iter()
                .any(|r| r.access.as_deref() == Some("ALL"));
            Ok(Rows {
                columns: std::mem::take(&mut plan.columns),
                rows,
                warnings: u16::try_from(ctx.warning_count()).unwrap_or(u16::MAX),
                conds: ctx.warnings().to_vec(),
                examined: plan.rows_examined(),
                no_index: no_index && !g.explain,
                full_scan: plan
                    .explain
                    .iter()
                    .any(|r| matches!(r.access.as_deref(), Some("ALL" | "index"))),
                found: plan
                    .found_rows
                    .as_ref()
                    .map(|f| f.load(std::sync::atomic::Ordering::Relaxed)),
            })
        };
        let r = if off_worker {
            let watch =
                interrupt.map(|h| interrupt_on_stop(h, a.stop.clone(), a.deadline.map(|d| d.0)));
            let r = tokio::task::block_in_place(run);
            if let Some(w) = watch {
                w.abort();
            }
            r
        } else {
            run()
        };
        // The plan and the planner are gone: the storage is this
        // function's alone again.
        let (tx, failed) = storage.finish();
        let r = match (failed, r) {
            // The store's or the time limit's own error, not its stand-in.
            (Some(e), _) => Err(e),
            (None, Ok(ran)) => Ok(ran),
            (None, Err(e)) => Err(sql_error(e)),
        };
        (tx, r)
    }

    /// Runs `g` in its own transaction (`opts`, `env`: the session's), and
    /// waits until what it read is durable.
    ///
    /// # Errors
    /// The statement's, or the engine's.
    pub(crate) async fn general_run(
        &self,
        g: &General,
        a: &RunArgs<'_>,
        opts: TxnOptions,
        env: Env,
    ) -> Result<Rows> {
        let spaces = self.general_spaces(a.tenant, g, None).await?;
        let mut tx = self.begin(opts)?;
        tx.set_env(env);
        // An autocommit statement's reads lock only for its locking clause
        // (a SERIALIZABLE autocommit read is a consistent read).
        let lock = g.lock.map(|l| lock_of(l, false));
        let (tx, r) = Self::general_exec(g, spaces, Some(tx), a, lock);
        let Some(tx) = tx else {
            return Err(SprigError::internal(
                "engine: a statement lost its transaction",
            ));
        };
        match r {
            Ok(ran) => {
                tx.finish().await?;
                Ok(ran)
            }
            Err(e) => {
                tx.rollback();
                Err(e)
            }
        }
    }

    /// Runs `g` as one statement of the session's transaction `txn`, and
    /// waits until what it read is durable. On an error the transaction
    /// stays open, as after any failed statement.
    ///
    /// # Errors
    /// As for [`Engine::general_run`]; `ER_SPRIG_NOT_ON_ENGINE` inside a
    /// SERIALIZABLE transaction (its reads lock what they read).
    pub(crate) async fn general_run_in(
        &self,
        g: &General,
        a: &RunArgs<'_>,
        txn: &mut SessionTxn,
    ) -> Result<Rows> {
        let Some(tx) = txn.statement() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        // Every read of a SERIALIZABLE transaction locks what it reads, in
        // shared mode, unless its locking clause asks for more.
        // A write's `SELECT` locks its subqueries' rows too, except at
        // READ COMMITTED (both servers).
        let all = if g.lock_each.is_some() {
            tx.options().isolation != sprig_engine::txn::Isolation::ReadCommitted
        } else {
            tx.shared_reads()
        };
        let lock = g.lock.map(|l| lock_of(l, all)).or_else(|| {
            tx.shared_reads().then_some(Lock {
                mode: LockMode::Shared,
                wait: Wait::Block,
                all: true,
            })
        });
        let spaces = self.general_spaces(a.tenant, g, Some(tx)).await?;
        tx.begin_statement();
        // The plan's storage owns the transaction while it runs: nothing
        // is awaited between taking it and putting it back.
        let Some(owned) = txn.lend() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        let (owned, r) = Self::general_exec(g, spaces, Some(owned), a, lock);
        let Some(owned) = owned else {
            return Err(SprigError::internal(
                "engine: a statement lost its transaction",
            ));
        };
        let tx = txn.restore(owned);
        match r {
            Ok(ran) => tx.gate().await.map(|()| ran),
            Err(e) => {
                tx.rollback_statement();
                Err(e)
            }
        }
    }

    /// The result columns of `g` without running it (a `PREPARE`): planned
    /// over storage that holds no rows, with every parameter standing as
    /// the number 1.
    ///
    /// # Errors
    /// The errors the servers raise when they prepare the statement.
    /// Automatic indexing's what-if planning: plans the bound `SELECT`
    /// `g` with `params` as the tables are, and also with `hypo` added
    /// (a hypothetical index), in a read-only transaction of its own that
    /// takes no locks and is rolled back. Nothing is executed.
    ///
    /// # Errors
    /// The planner's, or the engine's.
    pub(crate) async fn general_whatif(
        &self,
        tenant: u64,
        g: &General,
        params: &[Datum],
        seed: &Seed,
        hypo: Option<&sprig_plan::whatif::Hypothetical>,
    ) -> Result<(
        sprig_plan::whatif::Estimate,
        Option<sprig_plan::whatif::Estimate>,
    )> {
        use sprig_plan::whatif::{Prepared, estimate};
        let spaces = self.general_spaces(tenant, g, None).await?;
        let tx = self.begin(TxnOptions::default())?;
        let bounds: Vec<Bound> = g
            .tables
            .iter()
            .zip(spaces)
            .map(|(t, sp)| Bound::new(t, sp, None))
            .collect();
        let storage = TxStorage::new(Some(tx), bounds, None);
        let catalog = names::PlanCatalog::new(g);
        let session = seed.now();
        let options = PlanOptions {
            only_full_group_by: seed.only_full_group_by,
            memory_limit: Some(MEMORY_LIMIT),
            max_recursion: seed.max_recursion,
            ..PlanOptions::for_persona(session.persona)
        };
        let base = estimate(
            &catalog,
            storage.shared(),
            &session,
            Some(options.clone()),
            &g.stmt,
            params,
        );
        let with = hypo.map(|h| {
            Prepared::new(&catalog, storage.shared(), h)
                .and_then(|p| p.estimate(&session, Some(options.clone()), &g.stmt, params))
        });
        let (tx, failed) = storage.finish();
        if let Some(tx) = tx {
            tx.rollback();
        }
        if let Some(e) = failed {
            return Err(e);
        }
        let base = base.map_err(sql_error)?;
        let with = match with {
            Some(Ok(e)) => Some(e),
            Some(Err(e)) => return Err(sql_error(e)),
            None => None,
        };
        Ok((base, with))
    }

    pub(crate) fn general_describe(g: &General, seed: &Seed) -> Result<Vec<sprig_plan::Column>> {
        let spaces = g
            .tables
            .iter()
            .map(|t| storage::unread_space(&t.def))
            .collect();
        let params = vec![Datum::Int(1); g.params];
        let a = RunArgs {
            tenant: 0,
            params: &params,
            seed,
            deadline: None,
            stop: None,
        };
        Self::general_exec(g, spaces, None, &a, None)
            .1
            .map(|ran| ran.columns)
    }
}

#[cfg(test)]
mod tests;
