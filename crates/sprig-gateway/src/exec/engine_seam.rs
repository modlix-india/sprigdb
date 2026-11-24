//! The executor's hooks into the in-process SQL backend
//! ([`crate::engine`]): where a tenant statement that the name, privilege
//! and row-level security layers already handled would go to MariaDB, a
//! statement on an engine-backed database runs in the gateway instead.
//!
//! | Hook | Called from | Does |
//! |---|---|---|
//! | [`PassSession::engine_text`] | `forward` (text protocol) | transaction control; binds, checks, runs, answers |
//! | [`PassSession::engine_prepare`] | `tenant_prepare` | binds once; `PREPARE_OK` from the plan |
//! | [`PassSession::engine_execute`] | `execute` | the prepared fast path: privileges rechecked, no planning |
//! | [`PassSession::engine_implicit_commit`] | DDL | commits the session's engine transaction first |
//! | [`PassSession::engine_end`] | reset, close, a lost client | rolls it back |
//!
//! Every check of the general path stays: the tenant layers ran before the
//! hook, the execute rechecks privileges as the general execute does
//! (`authz::recheck`, 1615 re-prepares), workload capture fingerprints the
//! client's text, and admission, SLIs and the request span wrap the
//! request outside the executor. The engine adds what MariaDB's own
//! accounts used to check: exact column privileges (1143), and writes to
//! tables whose row-level security binds the session are refused (their
//! checks were MariaDB triggers).
//!
//! **Transactions.** The session owns its transaction on the engine
//! ([`EngineState`]). Which side a transaction belongs to is decided when
//! it starts:
//!
//! ```text
//! BEGIN / START TRANSACTION
//!   ├─ MariaDB already has a transaction open ──► MariaDB's (as before)
//!   ├─ the current database is on the engine ───► the engine's, held here;
//!   │                                             MariaDB hears nothing
//!   └─ otherwise ───────────────────────────────► MariaDB's
//! autocommit = 0: the first statement that reads or writes a table opens
//!   the transaction on that table's side.
//! ```
//!
//! Inside a transaction of one side, a statement on the other side's
//! tables is refused with `ER_SPRIG_NOT_ON_ENGINE`: one transaction can't
//! be atomic across the two. `COMMIT`, `ROLLBACK`, savepoints and
//! `SET TRANSACTION` of an engine transaction are answered here; every
//! OK and end-of-rows packet carries the transaction's status flags, also
//! for the statements MariaDB still answers meanwhile (`SELECT 1`, `SET`).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::authz::{Action, CompiledPrivileges, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_engine::txn::TxnOptions;
use sprig_proto_mysql::command::{ExecuteParams, ParamValue, StmtExecute};
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{Caps, ColumnDef, StatusFlags};
use sqlparser::ast::Statement;
use sqlparser::parser::Parser;

use super::PassSession;
use super::capture::Ran;
use super::stmt::Stmt;
use super::tenant::Step;
use crate::backend::{StmtInfo, StmtOutcome};
use crate::engine::control::{self, Control, Level, Scope, Xa};
use crate::engine::meta::{self, Results};
use crate::engine::plan::{self, BindCtx, CatalogNames, Op, Plan};
use crate::engine::row::Datum;
use crate::engine::session::{self, SessionTxn, Start, XaPhase};
use crate::engine::{self, catalog, expr, locking, not_on_engine};
use crate::pipeline::{
    EndOfRows, ExecCtx, ExecSummary, PreparedMeta, ResultSink, RowFormat, RowValues, StmtRef, Wire,
};

// The diagnostics area of statements answered in the gateway.
pub(super) mod diag;
// Plain statements that skip the gateway's text passes.
mod fast;
// General SELECTs (joins, grouping, subqueries) through the planner.
mod general;
// `SET` of session variables kept in the gateway.
mod routine;
mod setvars;
// Reads of system and user variables, and `SET @x`, kept in the gateway.
mod sessvars;
// `GET_LOCK` and its kin, kept in the gateway.
mod locks;
mod show;
// DDL on the session's temporary tables.
mod temp;
// Multi-table and subquery UPDATE and DELETE.
mod write;

/// What the session's variables say, as the engine path needs it. Worked
/// out once and kept while only prepared engine statements run (they
/// change no variable); any other statement makes it stale.
#[derive(Debug, Clone)]
pub(super) struct SessVars {
    /// The handshake collation the results were worked out for.
    handshake: u16,
    /// A transaction's options: the persona's, with the session's
    /// isolation level, lock wait and snapshot-isolation switch.
    opts: TxnOptions,
    /// `transaction_read_only`.
    read_only: bool,
    /// `autocommit = 0`.
    autocommit_off: bool,
    /// The MySQL port (its wording of errors).
    mysql: bool,
    /// The status flags outside a transaction.
    status: StatusFlags,
    /// The lexer's options.
    lex: sprig_sql::lex::LexOptions,
    /// The current database's logical name.
    current: Option<Arc<str>>,
    /// The results character set.
    results: Results,
    /// The rules for storing values: `sql_mode` and the time zone.
    env: crate::engine::assign::Env,
    /// The session's statement time limit (MariaDB's `max_statement_time`;
    /// MySQL's `max_execution_time`, which is for `SELECT` only).
    time_limit: Option<std::time::Duration>,
    /// What the planner and the expression engine read of the session (a
    /// general `SELECT`'s rules).
    general: crate::engine::general::Seed,
}

/// The session's engine state.
#[derive(Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct EngineState {
    /// The named locks (`GET_LOCK`) the session holds, released when it
    /// ends.
    locks: Option<locks::Owner>,
    /// A named-lock statement's answer is running (its text isn't the
    /// client's).
    lock_answer: bool,
    /// Its transaction on the engine.
    pub(super) txn: SessionTxn,
    /// The session's variables as last read; `None`: read them again.
    vars: Option<SessVars>,
    /// The session's `LAST_INSERT_ID()` from the engine's inserts.
    last_insert_id: u64,
    /// `innodb_lock_wait_timeout` as the session last set it, in seconds
    /// (`None`: the default).
    lock_wait: Option<u64>,
    /// The session set its results character set since login (`SET
    /// NAMES`, `SET character_set_results`): the MySQL port then reports
    /// that character set's collation instead of the handshake's.
    results_set: bool,
    /// A prepared statement was prepared again by itself and its result's
    /// columns are no longer the ones the client was told: the next
    /// answer says so in its status flags, as the servers' does.
    metadata_changed: bool,
    /// A statement is being run again against a newer catalog, after the
    /// engine found its table's definition changed (once per statement).
    retrying: bool,
    /// `lock_wait_timeout` as the session last set it, in seconds
    /// (`None`: the default).
    ddl_wait: Option<u64>,
    /// Text statements bound to one engine table, by text and context.
    text_plans: crate::engine::plancache::TextPlans,
    /// Statement shapes that skip the gateway's text passes (`fast`).
    fast_shapes: crate::engine::plancache::FastShapes,
    /// The shape the statement pipeline found for the text it runs next.
    fast: Option<sprig_sql::compat::fastpath::FastStmt>,
    /// A shape not known yet, met by the running statement: learnt if the
    /// statement reaches the engine unchanged (with the catalog version).
    fast_candidate: Option<(u64, crate::engine::plancache::Shape)>,
    /// The candidate, once the passes left the statement unchanged.
    fast_learn: Option<(u64, crate::engine::plancache::Shape)>,
    /// The diagnostics area's bookkeeping (`SHOW WARNINGS`).
    pub(super) diag: diag::DiagState,
    /// The running statement's text as the client sent it, before the
    /// gateway's text passes.
    pub(super) entry: Option<Bytes>,
    /// 3306: the table a 1062 of the running statement names (`'t.key'`).
    pub(super) dup_table: Option<String>,
    /// The session's last value of each sequence (`LASTVAL`), by object.
    seq_last: Arc<std::sync::Mutex<std::collections::HashMap<u64, i64>>>,
    /// The session's temporary tables on engine databases.
    pub(super) temps: crate::engine::temp::TempTables,
    /// MySQL: the open transaction created (bit 1) or dropped (bit 2)
    /// temporary tables, which its rollback can't undo (1751, 1752).
    pub(super) temp_txn: u8,
    /// The running `CREATE` / `ALTER EVENT`'s schedule, as evaluated
    /// before the catalog's DDL path stores it.
    pub(super) event_eval: Option<crate::engine::routine::event::Eval>,
}

impl EngineState {
    /// A statement that may change session variables or the current
    /// database ran (or runs now): they are read again when next needed.
    pub(super) fn stale(&mut self) {
        self.vars = None;
    }
}

/// What a text is to the engine.
enum Bound {
    /// A statement on one engine table.
    Engine(Box<Plan>),
    /// A `SELECT` on engine tables that the planner runs (a join, a
    /// grouping, a subquery, ...).
    General(Box<crate::engine::general::General>),
    /// Not the engine's; `tables`: it may read or write tables (of
    /// MariaDB-backed databases).
    Elsewhere { tables: bool },
    /// An `UPDATE` or `DELETE` through a join or a subquery.
    Write(Box<crate::engine::multiwrite::MultiWrite>),
    /// `INSERT ... SELECT` into an engine table: the query (the planner's)
    /// runs first, then its rows are inserted.
    InsertSelect(Box<(crate::engine::general::General, sqlparser::ast::Insert)>),
}

/// A prepared statement on an engine table.
#[derive(Debug)]
pub(crate) struct EnginePrepared {
    /// The plan.
    pub plan: Arc<Plan>,
    /// The result's column definitions, for the results character set
    /// in `results` (shared, so an execute copies no definition).
    pub columns: Arc<[ColumnDef]>,
    /// The results character set the columns were built for.
    pub results: Results,
    /// The privileges the plan's columns were last checked with.
    pub checked: Option<Arc<dyn SessionGrants>>,
    /// A general `SELECT` (the planner's): `plan` then only stands in,
    /// and the statement is planned at each execute.
    pub general: Option<Arc<crate::engine::general::General>>,
    /// The session's temporary tables when it was bound (their
    /// generation): a change binds it again.
    pub temp_gen: u64,
}

/// The `AUTO_INCREMENT` value of the last row insert `plan` gives (0 when
/// it gives none): what an `INSERT ... SELECT` that generated no value
/// reports.
fn last_row_id(plan: &Plan) -> u64 {
    let Op::Insert { cols, rows, .. } = &plan.op else {
        return 0;
    };
    let Some(ai) = plan.table.columns.iter().position(|c| c.auto_inc) else {
        return 0;
    };
    let Some(at) = cols.iter().position(|&c| c == ai) else {
        return 0;
    };
    match rows.last().and_then(|r| r.get(at)) {
        Some(Some(crate::engine::expr::CExpr::Lit(Datum::Int(i)))) => {
            u64::try_from(*i).unwrap_or(0)
        }
        Some(Some(crate::engine::expr::CExpr::Lit(Datum::UInt(u)))) => *u,
        _ => 0,
    }
}

/// MySQL's warnings for a rollback of a transaction that created (bit 1)
/// or dropped (bit 2) temporary tables.
fn temp_rollback_notes(bits: u8) -> Vec<crate::tenant::session::LocalWarning> {
    let mut v = Vec::new();
    if bits & 1 != 0 {
        v.push(diag::warning(
            "Warning",
            1751,
            "The creation of some temporary tables could not be rolled back.".into(),
        ));
    }
    if bits & 2 != 0 {
        v.push(diag::warning(
            "Warning",
            1752,
            "Some temporary tables were dropped, but these operations could not be rolled back."
                .into(),
        ));
    }
    v
}

/// Statements that run SQL elsewhere (routines, file loads): refused when
/// they could reach an engine table.
fn opaque(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Call(_)
            | Statement::LoadData { .. }
            | Statement::Merge { .. }
            | Statement::Prepare { .. }
            | Statement::Execute { .. }
    )
}

/// MySQL 8.4 takes a table value constructor only as `VALUES ROW(...)`:
/// where a query's `VALUES` is followed by a bare `(`, the 1064 it
/// answers (near that `(`).
fn bare_values(text: &str) -> Option<SprigError> {
    use sprig_sql::lex::{LexOptions, TokenKind, lex};
    let toks: Vec<_> = lex(text.as_bytes(), &LexOptions::mysql84())
        .ok()?
        .into_iter()
        .filter(|t| !t.kind.is_trivia())
        .collect();
    let b = text.as_bytes();
    let at = toks.windows(2).find(|w| {
        w[0].kind == TokenKind::Word
            && w[0].bytes(b).eq_ignore_ascii_case(b"VALUES")
            && w[1].kind == TokenKind::Punct
            && w[1].bytes(b) == b"("
    })?[1]
        .start as usize;
    let rest = text[at..].trim_end().trim_end_matches(';').trim_end();
    let shown: String = rest.chars().take(80).collect();
    Some(SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             MySQL server version for the right syntax to use near '{shown}' at line 1"
        ),
    ))
}

/// Statements that read or write rows: one of these that names an engine
/// table without being a plan the engine runs is refused. Data definition
/// isn't among them: it goes to the catalog and the database's MariaDB
/// copy, after `engine_ddl_check`.
fn row_kind(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Set(_)
    )
}

/// The first keyword of a statement that reads or writes rows.
fn row_statement(text: &[u8]) -> bool {
    let t = String::from_utf8_lossy(text);
    let w = t
        .trim_start_matches(|c: char| c.is_whitespace() || c == '(')
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    matches!(
        w.as_str(),
        "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "REPLACE"
            | "LOAD"
            | "WITH"
            | "TABLE"
            | "VALUES"
            | "HANDLER"
            | "CALL"
            | "DO"
            // SQL-level prepared statements and compound statements carry
            // statements the binder never sees.
            | "PREPARE"
            | "EXECUTE"
            | "BEGIN"
    )
}

/// Whether `stmt` names a table outside the system schemas (which hold
/// no transactional rows).
fn names_user_table(stmt: &Statement) -> bool {
    use std::ops::ControlFlow;
    sqlparser::ast::visit_relations(stmt, |name: &sqlparser::ast::ObjectName| {
        let system = name.0.len() > 1
            && name.0.first().is_some_and(|p| {
                let s = p.to_string();
                let s = s.trim_matches('`');
                s.eq_ignore_ascii_case("information_schema")
                    || s.eq_ignore_ascii_case("performance_schema")
                    || s.eq_ignore_ascii_case("mysql")
                    || s.eq_ignore_ascii_case("sys")
            });
        if system {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(())
        }
    })
    .is_break()
}

/// 39960 for a statement on MariaDB-backed tables inside a transaction on
/// the engine.
fn other_side_in_engine_txn() -> SprigError {
    not_on_engine(
        "a statement on a table of a MariaDB-backed database inside a transaction on an \
         engine database (one transaction can't span both; COMMIT or ROLLBACK first)",
    )
}

/// 39960 for an engine statement inside a MariaDB transaction.
fn engine_in_other_txn() -> SprigError {
    not_on_engine(
        "a statement on an engine database inside a transaction that began on a \
         MariaDB-backed database (one transaction can't span both; COMMIT or ROLLBACK \
         first, or make the engine database current before BEGIN)",
    )
}

/// The engine's prefix of a database's physical schema names.
fn schema_prefix(tenant: u64, db: DbId) -> String {
    format!("t{tenant}_d{}_", db.0)
}

impl PassSession {
    /// The session's snapshot and current database when its tenant has
    /// engine databases; `None` otherwise (nothing here applies).
    fn engine_scope(&self) -> Option<(Arc<TenantCatalog>, Option<DbId>)> {
        let (cat, current) = self.engine_scope_base()?;
        Some((self.eng.temps.overlay(&cat), current))
    }

    /// `INSERT ... SELECT`: runs the query `x.0` (checked as a `SELECT`)
    /// and gives the insert of its rows into `x.1`'s table.
    async fn insert_select_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        current: Option<DbId>,
        x: (crate::engine::general::General, sqlparser::ast::Insert),
    ) -> Result<(Box<Plan>, bool)> {
        let (g, ins) = x;
        self.general_checks(cat, &g)?;
        let sv = self.engine_vars(cx);
        let rows = self
            .general_run(cat.tenant.0, &g, &[], &sv, Arc::clone(cx.session.handle()))
            .await?;
        let Some(eng) = catalog::of(cat) else {
            return Err(not_on_engine("INSERT ... SELECT"));
        };
        let names = CatalogNames {
            cat,
            eng,
            lower_case: self
                .tenant
                .as_deref()
                .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names),
        };
        let bcx = BindCtx {
            tenant: cat.tenant,
            names: &names,
            current,
            backslash: !self.lex_opts().0.no_backslash_escapes,
        };
        match plan::bind_insert_rows(&bcx, &ins, &rows.rows, rows.columns.len())? {
            Some(p) => Ok((Box::new(p), rows.no_index)),
            None => Err(not_on_engine("INSERT ... SELECT")),
        }
    }

    /// The temporary tables' generation prepared statement `stmt` was
    /// bound at.
    fn engine_temp_gen_of(&mut self, stmt: StmtRef) -> Option<u64> {
        self.stmts
            .get_mut(stmt)
            .ok()?
            .engine
            .as_ref()
            .map(|e| e.temp_gen)
    }

    /// [`Self::engine_scope`] without the session's temporary tables: the
    /// catalog's own definitions, for DDL the catalog runs.
    fn engine_scope_base(&self) -> Option<(Arc<TenantCatalog>, Option<DbId>)> {
        let ts = self.tenant.as_deref()?;
        let cat = Arc::clone(ts.catalog()?);
        catalog::of(&cat)?;
        let current = self
            .current_logical()
            .and_then(|n| cat.database(&database_key(&n, ts.lower_case_table_names())))
            .map(|d| d.db_id);
        Some((cat, current))
    }

    /// The session's catalog snapshot when its tenant has engine databases
    /// (the prepared path needs no current database).
    fn engine_cat(&self) -> Option<Arc<TenantCatalog>> {
        let cat = self.tenant.as_deref()?.catalog()?;
        catalog::of(cat)?;
        Some(self.eng.temps.overlay(cat))
    }

    /// Whether MariaDB holds a transaction of this session (its own
    /// connection's).
    fn backend_in_txn(&self) -> bool {
        self.bs.as_ref().is_some_and(|b| b.pins().in_transaction())
    }

    /// The session's variables as the engine path needs them, read again
    /// only after a statement that could have changed them.
    fn engine_vars(&mut self, cx: &ExecCtx<'_>) -> SessVars {
        let mut v = self.engine_vars_read(cx);
        // The session's own values, as of this statement.
        v.env.last_insert_id = self.eng.last_insert_id;
        let info = &mut v.general.session.info;
        info.last_insert_id = self.eng.last_insert_id;
        info.database = self.current_logical();
        info.connection_id = u64::from(cx.session.id.0);
        if let Some(ts) = self.tenant.as_deref() {
            info.user = format!("{}@{}", ts.principal.user, ts.principal.client_ip);
            info.current_user = format!("{}@{}", ts.principal.user, ts.principal.host);
        }
        info.version = self.settings.persona_version(self.persona);
        // MariaDB's sequences, read through the session's catalog.
        info.sequences = None;
        if self.persona != sprig_proto_mysql::Persona::MySql84
            && let Some(cat) = self.engine_cat()
            && catalog::of(&cat).is_some_and(|c| !c.seq_by_object.is_empty())
            && let Ok(eng) = engine::engine()
        {
            info.sequences = Some(Arc::new(crate::engine::sequence::SessionSequences {
                eng,
                cat,
                last: Arc::clone(&self.eng.seq_last),
            }));
        }
        v
    }

    fn engine_vars_read(&mut self, cx: &ExecCtx<'_>) -> SessVars {
        let handshake = cx.proto.collation;
        if let Some(v) = &self.eng.vars
            && v.handshake == handshake
        {
            return v.clone();
        }
        let persona = self.persona;
        let lex = self.lex_opts().0;
        let current = self.current_logical().map(Arc::<str>::from);
        let lock_wait = self.eng.lock_wait;
        let results_set = self.eng.results_set;
        let v = {
            let bs = self.bs(cx);
            let sv = bs.vars();
            let get = |k: &str| sv.vars.get(k).map(String::as_str);
            let off =
                |v: Option<&str>| v.is_some_and(|v| v.eq_ignore_ascii_case("off") || v == "0");
            let on = |v: Option<&str>| v.is_some_and(|v| v.eq_ignore_ascii_case("on") || v == "1");
            let mut opts = match persona {
                sprig_proto_mysql::Persona::MySql84 => TxnOptions::mysql(),
                sprig_proto_mysql::Persona::MariaDb => TxnOptions::mariadb(),
            };
            if let Some(l) = get("transaction_isolation")
                .or_else(|| get("tx_isolation"))
                .and_then(Level::from_value)
            {
                opts.isolation = l.engine();
            }
            if let Some(si) = get("innodb_snapshot_isolation") {
                opts.snapshot_isolation = on(Some(si));
            }
            if let Some(secs) = lock_wait {
                opts = opts.with_lock_wait_seconds(secs);
            }
            let autocommit_off = off(get("autocommit"));
            let mut status = StatusFlags::empty();
            if !autocommit_off {
                status |= StatusFlags::AUTOCOMMIT;
            }
            if lex.no_backslash_escapes {
                status |= StatusFlags::NO_BACKSLASH_ESCAPES;
            }
            SessVars {
                handshake,
                opts,
                read_only: on(get("transaction_read_only")) || on(get("tx_read_only")),
                autocommit_off,
                mysql: persona == sprig_proto_mysql::Persona::MySql84,
                status,
                lex,
                current,
                results: meta::session_results(
                    persona,
                    handshake,
                    get("character_set_results"),
                    get("collation_connection"),
                    results_set,
                    sv.login_charset.is_some(),
                ),
                env: crate::engine::assign::Env {
                    no_fk_checks: off(get("foreign_key_checks")),
                    ..crate::engine::assign::Env::from_vars(
                        persona == sprig_proto_mysql::Persona::MySql84,
                        get("sql_mode"),
                        get("time_zone"),
                    )
                },
                time_limit: match persona {
                    // Seconds, with a fraction; 0 is no limit.
                    sprig_proto_mysql::Persona::MariaDb => get("max_statement_time")
                        .and_then(|v| v.trim().parse::<f64>().ok())
                        .filter(|s| *s > 0.0 && s.is_finite())
                        .map(std::time::Duration::from_secs_f64),
                    // Milliseconds; 0 is no limit.
                    sprig_proto_mysql::Persona::MySql84 => get("max_execution_time")
                        .and_then(|v| v.trim().parse::<u64>().ok())
                        .filter(|ms| *ms > 0)
                        .map(std::time::Duration::from_millis),
                },
                general: crate::engine::general::Seed::from_vars(
                    persona == sprig_proto_mysql::Persona::MySql84,
                    &crate::engine::assign::Env::from_vars(
                        persona == sprig_proto_mysql::Persona::MySql84,
                        get("sql_mode"),
                        get("time_zone"),
                    ),
                    get,
                ),
            }
        };
        self.eng.vars = Some(v.clone());
        v
    }

    /// The lexer's options as the engine path last read them, while they
    /// are still current (no statement that could change `sql_mode` ran
    /// since): a run of prepared statements asks for them at every
    /// execute.
    pub(super) fn engine_lex(&self) -> Option<sprig_sql::lex::LexOptions> {
        self.eng.vars.as_ref().map(|v| v.lex)
    }

    /// The status flags of an answer the engine path makes: the session's,
    /// with the engine transaction's.
    fn engine_status(&self, sv: &SessVars) -> StatusFlags {
        sv.status | self.engine_txn_flags()
    }

    /// The status flags an open engine transaction adds to every answer.
    pub(super) fn engine_txn_flags(&self) -> StatusFlags {
        if !self.eng.txn.reported() {
            return StatusFlags::empty();
        }
        if self.eng.txn.read_only() {
            StatusFlags::IN_TRANS | StatusFlags::IN_TRANS_READONLY
        } else {
            StatusFlags::IN_TRANS
        }
    }

    /// Whether the tenant's logical database `name` is engine-backed (the
    /// Redis front end asks, for the database holding its Redis databases).
    pub(super) fn engine_database_named(&self, name: &str) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return false;
        };
        let Some(cat) = Self::session_catalog(ts) else {
            return false;
        };
        catalog::of(&cat).is_some_and(|eng| {
            cat.database(&database_key(name, ts.lower_case_table_names()))
                .is_some_and(|d| eng.dbs.contains(&d.db_id))
        })
    }

    /// Whether the tenant's catalog (loaded) has no database `name`.
    /// `false` when it can't tell (no tenant session, no catalog yet).
    pub(super) fn unknown_database(&self, name: &str) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return false;
        };
        let Some(cat) = Self::session_catalog(ts) else {
            return false;
        };
        cat.database(&database_key(name, ts.lower_case_table_names()))
            .is_none()
    }

    /// The session's catalog; before the session has loaded one (a MongoDB
    /// or Redis connection's first command), the tenant's cached one.
    pub(super) fn session_catalog(
        ts: &crate::tenant::session::TenantSession,
    ) -> Option<Arc<TenantCatalog>> {
        ts.catalog()
            .cloned()
            .or_else(|| ts.services().cache.cached(ts.tenant))
    }

    /// Binds `text`: a plan when it is one engine table's statement.
    #[allow(clippy::too_many_lines)] // one pass over the binders, in order
    fn engine_bind(
        &self,
        cat: &TenantCatalog,
        current: Option<DbId>,
        text: &[u8],
    ) -> Result<Bound> {
        let Some(eng) = catalog::of(cat) else {
            return Ok(Bound::Elsewhere { tables: true });
        };
        let tenant = cat.tenant;
        let mentions = || {
            current.is_some_and(|c| eng.dbs.contains(&c))
                || eng.dbs.iter().any(|d| {
                    let p = schema_prefix(tenant.0, *d);
                    text.windows(p.len()).any(|w| w == p.as_bytes())
                })
        };
        // A `SELECT`'s locking clause is read here; the parser gets the
        // statement without it.
        let whole = std::str::from_utf8(text).ok();
        let (plain, locking) = match whole.and_then(locking::split) {
            Some((rest, clause)) => (Some(rest), Some(clause)),
            None => (whole, None),
        };
        let parsed = plain
            .and_then(|t| Parser::parse_sql(&sprig_plan::dialect::SprigMySql, t).ok())
            .filter(|v| v.len() == 1);
        let names = CatalogNames {
            cat,
            eng,
            lower_case: self
                .tenant
                .as_deref()
                .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names),
        };
        let cx = BindCtx {
            tenant,
            names: &names,
            current,
            backslash: !self.lex_opts().0.no_backslash_escapes,
        };
        // A write through joins or subqueries (`UPDATE a, b SET ...`
        // isn't parsed as one statement).
        let write = |cx: &BindCtx<'_>| -> Result<Option<Bound>> {
            let Some(t) = whole else {
                return Ok(None);
            };
            let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
            Ok(
                crate::engine::multiwrite::bind(cx, t, self.lex_opts().0, mysql)?
                    .map(|w| Bound::Write(Box::new(w))),
            )
        };
        let Some(stmt) = parsed.and_then(|mut v| v.pop()) else {
            if row_statement(text)
                && mentions()
                && let Some(b) = write(&cx)?
            {
                return Ok(b);
            }
            // Unparsed: a statement that may touch rows of an engine
            // database is refused, never sent to its empty MariaDB copy.
            return if row_statement(text) && mentions() {
                Err(not_on_engine("a statement the engine can't parse"))
            } else {
                Ok(Bound::Elsewhere {
                    tables: row_statement(text),
                })
            };
        };
        // A write through a view: the same write on its table.
        let (stmt, view_check) = crate::engine::view::rewrite_write(
            &cx,
            stmt,
            self.persona == sprig_proto_mysql::Persona::MySql84,
        )?;
        if self.persona == sprig_proto_mysql::Persona::MySql84
            && matches!(stmt, Statement::Query(_))
            && let Some(e) = whole.and_then(bare_values)
        {
            return Err(e);
        }
        if opaque(&stmt) {
            return if mentions() {
                Err(not_on_engine(
                    "CALL, LOAD DATA, MERGE, or PREPARE and EXECUTE as statements",
                ))
            } else {
                Ok(Bound::Elsewhere { tables: true })
            };
        }
        match plan::bind(&cx, &stmt) {
            Ok(Some(mut bound)) => {
                if let Some(vc) = &view_check {
                    let mut c = expr::Compiler {
                        def: &bound.table,
                        quals: vec![vc.qual.clone()],
                        params: 0,
                        read: vec![false; bound.table.columns.len()],
                        backslash: cx.backslash,
                        hints: Vec::new(),
                    };
                    let compiled = c.compile(&vc.cond).map_err(|_| {
                        not_on_engine("a view's CHECK OPTION condition the engine can't evaluate")
                    })?;
                    bound.view_check = Some((
                        compiled,
                        vc.db.clone(),
                        vc.view.clone(),
                        self.persona == sprig_proto_mysql::Persona::MySql84,
                    ));
                }
                if let Some(clause) = locking {
                    bound.lock = Some(self.engine_locking(&bound, &clause, whole.unwrap_or(""))?);
                }
                return Ok(Bound::Engine(Box::new(bound)));
            }
            Ok(None) => {}
            // A SELECT outside the single-table binder's shapes: the
            // planner's, below.
            Err(e) if crate::engine::general::takes(&stmt, &e) => {}
            // `INSERT ... SELECT`: the query is the planner's.
            Err(e)
                if matches!(&stmt, Statement::Insert(ins) if plan::insert_select(ins))
                    && e.code() == Some(sprig_common::SprigCode::NotOnEngine) =>
            {
                let Statement::Insert(ins) = stmt else {
                    return Err(e);
                };
                let Some(src) = ins.source.clone() else {
                    return Err(e);
                };
                let query = Statement::Query(src);
                return match crate::engine::general::bind_view(&cx, &query, None)? {
                    Some(g) => Ok(Bound::InsertSelect(Box::new((g, ins)))),
                    None => Err(e),
                };
            }
            // An UPDATE or DELETE outside them: a join, a subquery, ORDER BY.
            Err(e)
                if matches!(stmt, Statement::Update(_) | Statement::Delete(_))
                    && e.code() == Some(sprig_common::SprigCode::NotOnEngine) =>
            {
                return match write(&cx)? {
                    Some(b) => Ok(b),
                    None => Err(e),
                };
            }
            Err(e) => return Err(e),
        }
        let general_lock = match &locking {
            Some(clause) => Some(self.engine_general_locking(clause, whole.unwrap_or(""))?),
            None => None,
        };
        if let Some(g) = crate::engine::general::bind(&cx, &stmt, plain, general_lock)? {
            return Ok(Bound::General(Box::new(g)));
        }
        // The MySQL port: a set operation or `VALUES` without tables is
        // planned here (MariaDB, which would run it, types it its way).
        if self.persona == sprig_proto_mysql::Persona::MySql84
            && locking.is_none()
            && let Ok(Some(g)) = crate::engine::general::bind_tableless(&cx, &stmt, plain)
        {
            return Ok(Bound::General(Box::new(g)));
        }
        // A tenant with only engine databases: a query without tables is
        // planned here too (MariaDB has nothing of the tenant's).
        // (A variable read the gateway couldn't answer stays out: the
        // planner would take it for a column.)
        if locking.is_none()
            && !sessvars::reads_vars(text, self.lex_opts().0)
            && sessvars::all_engine(cat)
            && let Ok(Some(g)) = crate::engine::general::bind_no_tables(&cx, &stmt, plain)
        {
            return Ok(Bound::General(Box::new(g)));
        }
        // Not one engine table's statement. If it still names an engine
        // table (in a subquery, a derived table, next to a MariaDB table),
        // MariaDB would answer from the table's empty copy: refuse it.
        if row_kind(&stmt) && plan::names_engine_table(&cx, &stmt) {
            return Err(not_on_engine(
                "an engine table in a subquery, a derived table, or a statement on another table",
            ));
        }
        Ok(Bound::Elsewhere {
            tables: row_kind(&stmt) && names_user_table(&stmt),
        })
    }

    /// The client's text `l` planned, when it starts with `VALUES` and
    /// reads no table.
    fn tableless_values(
        &self,
        cat: &TenantCatalog,
        current: Option<DbId>,
        l: &[u8],
    ) -> Option<crate::engine::general::General> {
        let t = std::str::from_utf8(l).ok()?.trim();
        let t = t.strip_suffix(';').unwrap_or(t);
        if !t.get(..6).is_some_and(|w| w.eq_ignore_ascii_case("VALUES")) || bare_values(t).is_some()
        {
            return None;
        }
        let eng = catalog::of(cat)?;
        let stmt = Parser::parse_sql(&sprig_plan::dialect::SprigMySql, t)
            .ok()?
            .pop()?;
        let names = CatalogNames {
            cat,
            eng,
            lower_case: self
                .tenant
                .as_deref()
                .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names),
        };
        let cx = BindCtx {
            tenant: cat.tenant,
            names: &names,
            current,
            backslash: !self.lex_opts().0.no_backslash_escapes,
        };
        crate::engine::general::bind_tableless(&cx, &stmt, Some(t)).ok()?
    }

    /// Row-level security on a write: the condition the new row versions
    /// of `plan` (an `INSERT` or `UPDATE`) must meet, compiled for the
    /// engine: the session's `WITH CHECK` policies with its values bound,
    /// which MariaDB's triggers check on a MariaDB-backed table. `None`
    /// when the table's policies don't bind the session. (The rows an
    /// `UPDATE` or `DELETE` may touch are already narrowed by the `USING`
    /// policies the rewrite put into its `WHERE`.)
    ///
    /// # Errors
    /// The policy's (39364, 39366), or `ER_SPRIG_NOT_ON_ENGINE` for a
    /// condition the engine can't evaluate (a subquery, a function it
    /// doesn't have).
    async fn engine_row_check(
        &mut self,
        cx: &ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        plan: &Plan,
    ) -> Result<Option<expr::CExpr>> {
        use sprig_auth::authz::rls::cmd;
        let bit = match plan.op {
            Op::Insert { .. } => cmd::INSERT,
            Op::Update { .. } => cmd::UPDATE,
            Op::Select { .. } | Op::Delete => return Ok(None),
        };
        if crate::tenant::rls::is_exempt(cx.session.id) {
            return Ok(None);
        }
        let lex = self.lex_opts().0;
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let p = crate::tenant::authz::privileges(ts, cat);
        let Some(rls) = p.row_policies() else {
            return Ok(None);
        };
        if !rls.tables.contains_key(&plan.table.object) {
            return Ok(None);
        }
        let mut values = crate::tenant::rls::SessionValues::of(ts, cat);
        Box::pin(crate::tenant::rls::bind_tree(ts, cat, &rls, &mut values)).await?;
        let alias = String::from_utf8_lossy(&plan.alias).into_owned();
        // The resolver isn't `Send`: it ends before anything is awaited.
        let text = {
            let none = std::collections::HashSet::new();
            let temp = |_: Option<&str>, _: &str| false;
            let r = crate::tenant::rls::Resolve::new(
                cat,
                &rls,
                None,
                ts.lower_case_table_names(),
                &temp,
                &values,
                lex,
                &none,
            );
            let db = String::from_utf8_lossy(&plan.db_name);
            let table = String::from_utf8_lossy(&plan.org_table);
            let Some(k) = sprig_sql::rls::Resolver::protected(&r, Some(&db), &table) else {
                return Ok(None);
            };
            r.new_row_check(k, bit, &sprig_sql::rls::quote(&alias))?
        };
        let parsed = Parser::new(&sprig_plan::dialect::SprigMySql)
            .try_with_sql(&text)
            .and_then(|mut p| p.parse_expr())
            .map_err(|_| {
                not_on_engine("a row-level security WITH CHECK condition the engine can't read")
            })?;
        let mut c = expr::Compiler {
            def: &plan.table,
            quals: vec![alias],
            params: 0,
            read: vec![false; plan.table.columns.len()],
            backslash: !lex.no_backslash_escapes,
            hints: Vec::new(),
        };
        let compiled = c.compile(&parsed).map_err(|_| {
            not_on_engine(
                "a write to a table whose row-level security WITH CHECK condition the engine \
                 can't evaluate (a subquery, or a function it doesn't have)",
            )
        })?;
        Ok(Some(compiled))
    }

    /// The lock a `SELECT`'s locking clause asks for; the clause's parts
    /// the session's port doesn't have are its syntax errors, as each
    /// server reports them (MariaDB has no `FOR SHARE` and no `OF`; MySQL
    /// has no `WAIT n`).
    fn engine_locking(
        &self,
        plan: &Plan,
        clause: &locking::Clause,
        text: &str,
    ) -> Result<plan::RowLock> {
        if !plan.is_select() {
            return Err(not_on_engine("a locking clause on this statement"));
        }
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let near = |at: usize| {
            let rest = text.get(at..).unwrap_or("").trim_end();
            let rest = rest.trim_end_matches(';').trim_end();
            let shown: String = rest.chars().take(80).collect();
            SprigError::standard(
                1064,
                *b"42000",
                format!(
                    "You have an error in your SQL syntax; check the manual that corresponds \
                     to your {} server version for the right syntax to use near '{shown}' at \
                     line 1",
                    if mysql { "MySQL" } else { "MariaDB" }
                ),
            )
        };
        if mysql {
            // The MySQL port's own rewrite already turned `FOR SHARE [OF
            // t]` into `LOCK IN SHARE MODE` (MariaDB's spelling), so a
            // wait option after that is MySQL's `FOR SHARE NOWAIT`; only
            // `WAIT n` can't be MySQL's.
            if let Some(at) = clause.at.wait
                && clause.wait_n
            {
                return Err(near(at));
            }
            // `OF` names the statement's one table, by its alias.
            if let Some(n) = clause
                .of
                .iter()
                .find(|n| !n.as_bytes().eq_ignore_ascii_case(&plan.alias))
            {
                return Err(SprigError::standard(
                    3568,
                    *b"HY000",
                    format!("Unresolved table name `{n}` in locking clause."),
                ));
            }
        } else {
            if clause.form == locking::Form::ForShare {
                return Err(near(clause.at.lock_word));
            }
            if let Some(at) = clause.at.of {
                return Err(near(at));
            }
        }
        Ok(plan::RowLock {
            exclusive: clause.exclusive(),
            wait: clause.wait,
        })
    }

    /// The lock a general `SELECT`'s locking clause asks for, with the
    /// same syntax errors per port as [`Self::engine_locking`]; `OF` (a
    /// subset of the tables) isn't served for a general `SELECT`.
    fn engine_general_locking(
        &self,
        clause: &locking::Clause,
        text: &str,
    ) -> Result<plan::RowLock> {
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let near = |at: usize| {
            let rest = text.get(at..).unwrap_or("").trim_end();
            let rest = rest.trim_end_matches(';').trim_end();
            let shown: String = rest.chars().take(80).collect();
            SprigError::standard(
                1064,
                *b"42000",
                format!(
                    "You have an error in your SQL syntax; check the manual that corresponds \
                     to your {} server version for the right syntax to use near '{shown}' at \
                     line 1",
                    if mysql { "MySQL" } else { "MariaDB" }
                ),
            )
        };
        if mysql {
            if let Some(at) = clause.at.wait
                && clause.wait_n
            {
                return Err(near(at));
            }
            if !clause.of.is_empty() {
                return Err(not_on_engine(
                    "a locking clause with OF on a join, grouping, an expression or a subquery",
                ));
            }
        } else {
            if clause.form == locking::Form::ForShare {
                return Err(near(clause.at.lock_word));
            }
            if let Some(at) = clause.at.of {
                return Err(near(at));
            }
        }
        Ok(plan::RowLock {
            exclusive: clause.exclusive(),
            wait: clause.wait,
        })
    }

    /// The column definitions of a `SELECT` plan.
    fn engine_columns(plan: &Plan, results: Results) -> Vec<ColumnDef> {
        let Op::Select { out } = &plan.op else {
            return Vec::new();
        };
        out.iter()
            .map(|oc| {
                let org = Bytes::from(plan.table.columns[oc.col].name.clone());
                meta::column_def(
                    &plan.db_name,
                    &plan.alias,
                    &plan.table,
                    &plan.org_table,
                    oc.col,
                    &oc.name,
                    &org,
                    results,
                )
            })
            .collect()
    }

    /// What MariaDB's per-user account and triggers checked: exact column
    /// privileges, and row-level security on writes. `checked`: the
    /// privileges last checked, skipped when they are the same.
    fn engine_checks(
        &self,
        cat: &TenantCatalog,
        plan: &Plan,
        checked: Option<&Arc<dyn SessionGrants>>,
    ) -> Result<Option<Arc<dyn SessionGrants>>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let p = crate::tenant::authz::privileges(ts, cat);
        if checked.is_some_and(|c| Arc::ptr_eq(c, &p)) {
            return Ok(None);
        }
        let def = &plan.table;
        // An INSERT or UPDATE of a table whose row-level security binds
        // the session carries the policies' `WITH CHECK`
        // (`engine_row_check`). A prepared one is bound again (1615) when
        // the session's privileges changed since: its condition may have,
        // and one bound without a condition never runs on a bound table.
        if matches!(plan.op, Op::Insert { .. } | Op::Update { .. })
            && ((checked.is_some() && plan.check.is_some())
                || (plan.check.is_none()
                    && p.row_policies()
                        .is_some_and(|r| r.tables.contains_key(&def.object))))
        {
            return Err(SprigError::standard(
                1615,
                *b"HY000",
                "Prepared statement needs to be re-prepared",
            ));
        }
        if p.unrestricted() {
            return Ok(Some(p));
        }
        let Some(cp) = p.as_any().downcast_ref::<CompiledPrivileges>() else {
            // Another authorizer: its coarse check already ran; the exact
            // column rules are the built-in model's.
            return Ok(Some(p));
        };
        let db_key = database_key(
            &String::from_utf8_lossy(&plan.db_name),
            ts.lower_case_table_names(),
        );
        let target = Target::Object {
            db_key: &db_key,
            db: def.db,
            object: def.object,
        };
        let table_level = cp.actions_on(target);
        let client_host = ts.principal.client_ip.to_string();
        let need = |action: Action, cols: &[bool]| -> Result<()> {
            if table_level.has(action) {
                return Ok(());
            }
            for (i, used) in cols.iter().enumerate() {
                if !used {
                    continue;
                }
                let name = &def.columns[i].name;
                let ok = cp
                    .columns
                    .get(&(def.object, name.to_lowercase()))
                    .is_some_and(|a| a.has(action));
                if !ok {
                    return Err(SprigError::standard(
                        1143,
                        *b"42000",
                        format!(
                            "{} command denied to user '{}'@'{}' for column '{}' in table '{}'",
                            action.command(),
                            ts.principal.user,
                            client_host,
                            name,
                            def.name
                        ),
                    ));
                }
            }
            Ok(())
        };
        match &plan.op {
            Op::Select { .. } | Op::Delete => need(Action::Select, &plan.read)?,
            Op::Insert { .. } => need(Action::Insert, &plan.written)?,
            Op::Update { .. } => {
                need(Action::Update, &plan.written)?;
                need(Action::Select, &plan.read)?;
            }
        }
        Ok(Some(p))
    }

    /// Runs `plan`: as a statement of the session's engine transaction
    /// (opened now when `autocommit` is off), or in its own.
    async fn engine_run(
        &mut self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        sv: &SessVars,
    ) -> Result<engine::exec::Outcome> {
        // The session's sequences, for `NEXTVAL(s)` among the values.
        let seqs = sv.general.session.info.sequences.clone();
        crate::engine::sequence::CURRENT
            .scope(
                seqs,
                self.engine_run_in(tenant, plan, params, found_rows, sv),
            )
            .await
    }

    async fn engine_run_in(
        &mut self,
        tenant: u64,
        plan: &Plan,
        params: &[Datum],
        found_rows: bool,
        sv: &SessVars,
    ) -> Result<engine::exec::Outcome> {
        // MariaDB holds the session's transaction: an engine statement
        // can't join it.
        if self.backend_in_txn() {
            return Err(engine_in_other_txn());
        }
        let e = engine::engine()?;
        // An XA branch takes statements only while it is active.
        if let Some(ph) = self.eng.txn.xa_phase()
            && ph != XaPhase::Active
        {
            return Err(Self::xa_refusal(ph, sv.mysql));
        }
        // The session's statement time limit: when it passes, the
        // statement is dropped where it stands (a wait for a lock ends),
        // undone, and answered with the persona's error. The transaction
        // stays open with its locks.
        let limit = sv.time_limit.filter(|_| !sv.mysql || plan.is_select());
        let timed_out = || {
            if sv.mysql {
                sprig_engine::txn::errors::statement_timeout_mysql()
            } else {
                sprig_engine::txn::errors::statement_timeout_mariadb()
            }
        };
        if !self.eng.txn.is_open() {
            if !sv.autocommit_off {
                if sv.read_only && !plan.is_select() {
                    return Err(session::read_only_transaction(sv.mysql));
                }
                let run = e.run_env(tenant, plan, params, found_rows, sv.opts, sv.env);
                return match limit {
                    None => run.await,
                    // Past the limit its own transaction goes with it.
                    Some(d) => tokio::time::timeout(d, run)
                        .await
                        .unwrap_or_else(|_| Err(timed_out())),
                };
            }
            // `autocommit = 0`: the statement opens the transaction.
            self.eng
                .txn
                .begin(e, sv.opts, sv.read_only, Start::default())?;
        }
        if self.eng.txn.read_only() && !plan.is_select() {
            return Err(session::read_only_transaction(sv.mysql));
        }
        let Some(tx) = self.eng.txn.statement() else {
            return Err(SprigError::internal("engine: no open transaction"));
        };
        // The session's `sql_mode` and time zone as they are now.
        tx.set_env(sv.env);
        let run = e.run_in(tenant, plan, params, found_rows, tx);
        let r = match limit {
            None => run.await,
            Some(d) => {
                if let Ok(r) = tokio::time::timeout(d, run).await {
                    r
                } else {
                    self.eng.txn.abandon_statement();
                    Err(timed_out())
                }
            }
        };
        self.eng.txn.statement_done();
        r
    }

    /// A prepared `SET autocommit = ?` that MariaDB is about to run, inside
    /// a transaction on the engine: turning autocommit on (from off)
    /// commits that transaction first, as the text form does.
    ///
    /// # Errors
    /// The commit's; 1399 inside an XA branch.
    pub(super) async fn engine_autocommit_param(
        &mut self,
        cx: &ExecCtx<'_>,
        sql: &[u8],
        params: &ExecuteParams,
    ) -> Result<()> {
        let Some(i) = control::autocommit_param(sql) else {
            return Ok(());
        };
        if !self.engine_vars(cx).autocommit_off {
            return Ok(());
        }
        let value = params.params.iter().filter(|p| p.name.is_empty()).nth(i);
        let off = match value.map(|p| &p.value) {
            Some(ParamValue::Value(v)) => match expr::param_datum(v) {
                Ok(Datum::Int(0) | Datum::UInt(0)) => true,
                Ok(Datum::Str(s)) => {
                    let s = String::from_utf8_lossy(&s).trim().to_ascii_uppercase();
                    matches!(s.as_str(), "0" | "OFF" | "FALSE")
                }
                _ => false,
            },
            _ => false,
        };
        if off {
            return Ok(());
        }
        self.engine_implicit_commit().await
    }

    /// Commits the session's engine transaction before a statement that
    /// commits implicitly (DDL, `BEGIN`, `SET autocommit = 1`, `LOCK
    /// TABLES`).
    ///
    /// # Errors
    /// The commit's: the transaction is gone either way.
    pub(super) async fn engine_implicit_commit(&mut self) -> Result<()> {
        self.eng.temp_txn = 0;
        // An XA branch can't be committed on the side: 1399, as the servers
        // answer DDL, `BEGIN` and `LOCK TABLES` inside one.
        if let Some(ph) = self.eng.txn.xa_phase() {
            return Err(session::xa_rmfail(ph.name()));
        }
        if !self.eng.txn.is_open() {
            return Ok(());
        }
        self.eng.txn.commit().await.map(|_| ())
    }

    /// The refusal of a statement on tables while the session holds an XA
    /// branch without an open transaction (prepared, or rolled back): such
    /// a branch takes no statement on tables, wherever they are.
    fn xa_held_refusal(&self) -> Option<SprigError> {
        let ph = self.eng.txn.xa_phase()?;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        Some(Self::xa_refusal(ph, mysql))
    }

    /// What a statement that reads or writes tables gets in an XA branch
    /// that is past `XA END`: 1399 with the state; MySQL answers 1614 in a
    /// branch a deadlock rolled back.
    fn xa_refusal(ph: XaPhase, mysql: bool) -> SprigError {
        if mysql && ph == XaPhase::RollbackOnly {
            session::xa_rbdeadlock()
        } else {
            session::xa_rmfail(ph.name())
        }
    }

    /// A statement MariaDB prepared is about to run: refused inside a
    /// transaction on the engine when it reads or writes tables (one
    /// transaction can't span both sides).
    ///
    /// # Errors
    /// `ER_SPRIG_NOT_ON_ENGINE`.
    pub(super) fn engine_other_side(&self, sql: &[u8]) -> Result<()> {
        if !self.eng.txn.is_open() {
            return Ok(());
        }
        let parsed = std::str::from_utf8(sql)
            .ok()
            .and_then(|t| Parser::parse_sql(&sprig_plan::dialect::SprigMySql, t).ok())
            .filter(|v| v.len() == 1)
            .and_then(|mut v| v.pop());
        let tables = match parsed {
            Some(stmt) => opaque(&stmt) || (row_kind(&stmt) && names_user_table(&stmt)),
            None => row_statement(sql),
        };
        if tables {
            return Err(other_side_in_engine_txn());
        }
        Ok(())
    }

    /// Whether DDL on `target` commits the session's open engine
    /// transaction first: all of it but temporary tables', as on the
    /// servers.
    pub(super) fn engine_ddl_commits(&self, target: &sprig_sql::names::DdlTarget) -> bool {
        (self.eng.txn.is_open() || self.eng.txn.xa_phase().is_some()) && target.is_catalog()
    }

    /// The session's engine transaction ends without a commit (a reset, a
    /// change of user, the session's end): rolled back, its locks
    /// released, and the next-transaction characteristics forgotten.
    pub(super) fn engine_end(&mut self) {
        self.eng.txn.rollback();
        self.eng.temp_txn = 0;
        // The session's temporary tables go with it.
        if !self.eng.temps.is_empty() {
            self.eng.temps.clear();
            self.eng.text_plans = crate::engine::plancache::TextPlans::default();
        }
        self.eng.txn.xa_leave();
        self.eng.txn.clear_next();
        self.eng.lock_wait = None;
        self.eng.ddl_wait = None;
        self.eng.results_set = false;
        self.eng.diag.restart();
        self.eng.stale();
    }

    /// The running request was dropped (a killed query): the statement's
    /// writes are undone; the transaction stays open, as on the servers.
    pub(super) fn engine_abandon_statement(&mut self) {
        self.eng.txn.abandon_statement();
    }

    /// An OK for a statement the engine path answers itself.
    async fn engine_ok(
        &mut self,
        cx: &ExecCtx<'_>,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        out.ok(OkPacket::new(status)).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    /// A transaction-control statement: `Some` when the session's engine
    /// transaction (or the lack of any transaction on an engine database)
    /// answers it; `None` when it is MariaDB's, as before.
    #[allow(clippy::too_many_lines)] // one arm per statement
    async fn engine_control(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &TenantCatalog,
        current: Option<DbId>,
        ctl: Control,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let open = self.eng.txn.is_open();
        let backend_txn = self.backend_in_txn();
        let commit = matches!(ctl, Control::Commit { .. });
        let mut rollback_notes = Vec::new();
        // Outside any transaction, the current database's side answers.
        let here = open
            || (!backend_txn
                && current.is_some_and(|c| catalog::of(cat).is_some_and(|e| e.dbs.contains(&c))));
        macro_rules! fail {
            ($e:expr) => {
                return self.fail(out, &$e).await.map(Some)
            };
        }
        // Inside an XA branch only the XA statements end the transaction,
        // and past `XA END` nothing else touches it.
        let xa = self.eng.txn.xa_phase();
        if let Some(ph) = xa {
            let refused = match &ctl {
                Control::Begin { .. }
                | Control::Commit { .. }
                | Control::Rollback { .. }
                | Control::LockTables(_) => true,
                Control::Savepoint(_) | Control::RollbackTo(_) | Control::Release(_) => {
                    ph != XaPhase::Active
                }
                // Turning autocommit on would commit.
                Control::Set {
                    autocommit: Some(v),
                    ..
                } => *v != control::Switch::Off && self.engine_vars(cx).autocommit_off,
                _ => false,
            };
            if refused {
                fail!(session::xa_rmfail(ph.name()));
            }
        }
        match ctl {
            Control::Begin {
                read_only,
                snapshot,
            } => {
                // A new transaction commits the open one first.
                if let Err(e) = self.engine_implicit_commit().await {
                    fail!(e);
                }
                let here = !backend_txn
                    && current
                        .is_some_and(|c| catalog::of(cat).is_some_and(|e| e.dbs.contains(&c)));
                if !here {
                    return Ok(None);
                }
                let sv = self.engine_vars(cx);
                let start = Start {
                    read_only,
                    snapshot,
                };
                let r = engine::engine()
                    .and_then(|e| self.eng.txn.begin(e, sv.opts, sv.read_only, start));
                if let Err(e) = r {
                    fail!(e);
                }
            }
            Control::Commit { chain, release } | Control::Rollback { chain, release } => {
                if !here {
                    return Ok(None);
                }
                let temp_txn = std::mem::take(&mut self.eng.temp_txn);
                if !commit && temp_txn != 0 {
                    rollback_notes = temp_rollback_notes(temp_txn);
                }
                let ended = if commit {
                    match self.eng.txn.commit().await {
                        Ok(x) => x,
                        Err(e) => fail!(e),
                    }
                } else {
                    self.eng.txn.rollback()
                };
                if chain == Some(true) {
                    let sv = self.engine_vars(cx);
                    let r = engine::engine()
                        .and_then(|e| self.eng.txn.chain(e, ended, sv.opts, sv.read_only));
                    if let Err(e) = r {
                        fail!(e);
                    }
                }
                // `RELEASE`: the OK, then the connection closes.
                if release == Some(true) {
                    let step = self.engine_ok(cx, more, out).await?;
                    out.flush().await?;
                    cx.session.flags().kill_conn();
                    return Ok(Some(step));
                }
            }
            Control::Savepoint(name) => {
                if !here {
                    return Ok(None);
                }
                if !open {
                    // Outside a transaction a savepoint ends with its
                    // statement; with `autocommit = 0` it opens the
                    // transaction it belongs to.
                    let sv = self.engine_vars(cx);
                    if sv.autocommit_off {
                        let r = engine::engine()
                            .and_then(|e| self.eng.txn.begin_quiet(e, sv.opts, sv.read_only));
                        if let Err(e) = r {
                            fail!(e);
                        }
                    }
                }
                self.eng.txn.savepoint(&name);
            }
            Control::RollbackTo(name) => {
                if !here {
                    return Ok(None);
                }
                let r = if open {
                    engine::engine().and_then(|e| self.eng.txn.rollback_to(e, &name))
                } else {
                    Err(session::no_savepoint(&name))
                };
                if let Err(e) = r {
                    fail!(e);
                }
            }
            Control::Release(name) => {
                if !here {
                    return Ok(None);
                }
                if let Err(e) = self.eng.txn.release(&name) {
                    fail!(e);
                }
            }
            Control::SetTransaction {
                scope: Scope::Next,
                isolation,
                read_only,
            } => {
                if !here {
                    return Ok(None);
                }
                if let Err(e) = self.eng.txn.set_next(isolation, read_only) {
                    fail!(e);
                }
            }
            // The session's and the tenant's defaults are variables
            // MariaDB keeps and reports back; table locks are MariaDB's.
            Control::SetTransaction { .. } | Control::UnlockTables => return Ok(None),
            Control::Set {
                autocommit,
                lock_wait,
                results_charset,
            } => {
                if results_charset {
                    self.eng.results_set = true;
                }
                // Turning autocommit on (from off) commits; so does a value
                // that can't be read here.
                if let Some(v) = autocommit
                    && open
                    && v != control::Switch::Off
                    && self.engine_vars(cx).autocommit_off
                    && let Err(e) = self.engine_implicit_commit().await
                {
                    fail!(e);
                }
                // MariaDB's session tracking reports that this variable
                // changed, not its value: the engine keeps the value.
                match lock_wait {
                    Some(control::LockWait::Seconds(n)) => self.eng.lock_wait = Some(n),
                    Some(control::LockWait::Default) => self.eng.lock_wait = None,
                    Some(control::LockWait::Unknown) | None => {}
                }
                return Ok(None);
            }
            Control::Xa(x) => {
                let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
                let on_engine = !backend_txn
                    && current
                        .is_some_and(|c| catalog::of(cat).is_some_and(|e| e.dbs.contains(&c)));
                // With no branch and no transaction here, the statement is
                // the current database's side's, as `BEGIN` is.
                if xa.is_none() && !open && !on_engine {
                    return Ok(None);
                }
                let e = match engine::engine() {
                    Ok(e) => e,
                    Err(err) => fail!(err),
                };
                let tenant = cat.tenant.0;
                match x {
                    // A form this layer doesn't read: with no branch and
                    // no transaction here, MariaDB answers it (a syntax
                    // error, as a rule).
                    Xa::Other if xa.is_none() && !open => return Ok(None),
                    Xa::Other | Xa::RecoverWith => fail!(not_on_engine(
                        "this form of XA statement on an engine database"
                    )),
                    Xa::Recover => {
                        return self
                            .engine_xa_recover(cx, e, tenant, more, out)
                            .await
                            .map(Some);
                    }
                    Xa::Start { xid, join } => {
                        if let Some(ph) = xa {
                            fail!(session::xa_rmfail(ph.name()));
                        }
                        if join || (xid.gtrid.is_empty() && !mysql) {
                            fail!(session::xa_inval());
                        }
                        if open {
                            fail!(session::xa_outside());
                        }
                        let key = xid.engine_key(tenant);
                        if session::xa_taken(e, &key) {
                            fail!(session::xa_dupid());
                        }
                        let sv = self.engine_vars(cx);
                        if let Err(err) = self.eng.txn.xa_start(e, sv.opts, sv.read_only, xid, key)
                        {
                            fail!(err);
                        }
                    }
                    Xa::End { xid, suspend } => match xa {
                        None => fail!(session::xa_rmfail("NON-EXISTING")),
                        Some(_) if !self.eng.txn.xa_is(&xid) => fail!(session::xa_nota()),
                        Some(XaPhase::Active) => {
                            if suspend {
                                fail!(session::xa_inval());
                            }
                            self.eng.txn.xa_end();
                        }
                        Some(ph) => fail!(Self::xa_refusal(ph, mysql)),
                    },
                    Xa::Prepare(xid) => match xa {
                        None => fail!(session::xa_rmfail("NON-EXISTING")),
                        Some(XaPhase::Idle) => {
                            if !self.eng.txn.xa_is(&xid) {
                                fail!(session::xa_nota());
                            }
                            // MariaDB keeps the prepared branch with its
                            // session; on MySQL the session is free of it.
                            if let Err(err) = self.eng.txn.xa_prepare(!mysql).await {
                                tracing::warn!(error = %err, "XA PREPARE failed; the branch was rolled back");
                                fail!(SprigError::standard(
                                    1402,
                                    *b"XA100",
                                    "XA_RBROLLBACK: Transaction branch was rolled back",
                                ));
                            }
                        }
                        Some(ph) => fail!(session::xa_rmfail(ph.name())),
                    },
                    Xa::Commit { xid, one_phase } => {
                        if let Err(err) = self
                            .engine_xa_finish(cx, e, tenant, &xid, true, one_phase)
                            .await
                        {
                            fail!(err);
                        }
                    }
                    Xa::Rollback(xid) => {
                        if let Err(err) = self
                            .engine_xa_finish(cx, e, tenant, &xid, false, false)
                            .await
                        {
                            fail!(err);
                        }
                    }
                }
            }
            Control::LockTables(tables) => {
                // A table lock on an engine table isn't built: refused, so
                // nobody works on in the belief of holding one. (Tables of
                // MariaDB-backed databases are locked there, as before.)
                let lctn = self
                    .tenant
                    .as_deref()
                    .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
                let eng = catalog::of(cat);
                let on_engine = |db: Option<&str>| match db {
                    None => current.is_some_and(|c| eng.is_some_and(|e| e.dbs.contains(&c))),
                    Some(d) => eng.is_some_and(|e| {
                        cat.database(&database_key(d, lctn))
                            .is_some_and(|x| e.dbs.contains(&x.db_id))
                            || e.dbs
                                .iter()
                                .any(|x| d.starts_with(&schema_prefix(cat.tenant.0, *x)))
                    }),
                };
                if tables.iter().any(|(db, _)| on_engine(db.as_deref())) {
                    fail!(not_on_engine(
                        "LOCK TABLES on a table of an engine database (use a transaction: \
                         START TRANSACTION, or mysqldump --single-transaction)"
                    ));
                }
                if let Err(e) = self.engine_implicit_commit().await {
                    fail!(e);
                }
                return Ok(None);
            }
        }
        if !rollback_notes.is_empty() {
            let sv = self.engine_vars(cx);
            let mut status = self.engine_status(&sv);
            if more {
                status |= StatusFlags::MORE_RESULTS_EXISTS;
            }
            let mut ok = OkPacket::new(status);
            ok.warnings = u16::try_from(rollback_notes.len()).unwrap_or(u16::MAX);
            self.diag_set(rollback_notes, 0);
            out.ok(ok).await?;
            return Ok(Some(Step {
                errored: false,
                status: Some(status),
            }));
        }
        self.engine_ok(cx, more, out).await.map(Some)
    }

    /// `XA COMMIT` (`commit`) or `XA ROLLBACK` of branch `xid`: the
    /// session's own, or a prepared one no session holds.
    async fn engine_xa_finish(
        &mut self,
        cx: &ExecCtx<'_>,
        e: &'static engine::Engine,
        tenant: u64,
        xid: &control::Xid,
        commit: bool,
        one_phase: bool,
    ) -> Result<()> {
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let Some(ph) = self.eng.txn.xa_phase() else {
            // Not this session's: a session inside a transaction of its
            // own can't finish one (MariaDB counts `autocommit = 0` as
            // that).
            if self.eng.txn.is_open() || (!mysql && self.engine_vars(cx).autocommit_off) {
                return Err(session::xa_outside());
            }
            return session::xa_finish(e, &xid.engine_key(tenant), commit).await;
        };
        if !self.eng.txn.xa_is(xid) {
            return Err(if mysql {
                session::xa_rmfail(ph.name())
            } else {
                session::xa_outside()
            });
        }
        match ph {
            XaPhase::Active => Err(session::xa_rmfail(ph.name())),
            // From the idle state only `ONE PHASE` commits.
            XaPhase::Idle if commit && !one_phase => Err(session::xa_rmfail(ph.name())),
            XaPhase::Prepared if commit && one_phase => Err(session::xa_inval()),
            XaPhase::RollbackOnly if commit => {
                // The branch is gone already; the commit says so.
                self.eng.txn.xa_finish_own(e, false).await?;
                Err(session::xa_rbdeadlock())
            }
            XaPhase::Idle | XaPhase::Prepared | XaPhase::RollbackOnly => {
                self.eng.txn.xa_finish_own(e, commit).await
            }
        }
    }

    /// `XA RECOVER`: the tenant's prepared branches, as each server lists
    /// them (`formatID`, `gtrid_length`, `bqual_length`, `data`).
    async fn engine_xa_recover(
        &mut self,
        cx: &ExecCtx<'_>,
        e: &engine::Engine,
        tenant: u64,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        use sprig_proto_mysql::resultset::column_type;
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let number = |name: &'static [u8]| ColumnDef {
            name: Bytes::from_static(name),
            charset: 63,
            length: if mysql { 12 } else { 11 },
            col_type: column_type::LONGLONG,
            flags: 0x0081,
            ..ColumnDef::default()
        };
        let data = if mysql {
            ColumnDef {
                name: Bytes::from_static(b"data"),
                charset: 45,
                length: 1032,
                col_type: column_type::VAR_STRING,
                flags: 0x0001,
                decimals: 31,
                ..ColumnDef::default()
            }
        } else {
            ColumnDef {
                name: Bytes::from_static(b"data"),
                charset: 63,
                length: 128,
                col_type: column_type::VAR_STRING,
                flags: 0x0081,
                decimals: 39,
                ..ColumnDef::default()
            }
        };
        let columns = [
            number(b"formatID"),
            number(b"gtrid_length"),
            number(b"bqual_length"),
            data,
        ];
        out.begin_rows(&columns, RowFormat::Text).await?;
        for xid in session::xa_recover(e, tenant) {
            let format = xid.format.to_string();
            let gtrid_len = xid.gtrid.len().to_string();
            let bqual_len = xid.bqual.len().to_string();
            let mut data = xid.gtrid.clone();
            data.extend_from_slice(&xid.bqual);
            let row: [Option<&[u8]>; 4] = [
                Some(format.as_bytes()),
                Some(gtrid_len.as_bytes()),
                Some(bqual_len.as_bytes()),
                Some(&data),
            ];
            out.row(RowValues::Text(&row)).await?;
        }
        let sv = self.engine_vars(cx);
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        out.end_rows(EndOfRows {
            status,
            warnings: 0,
        })
        .await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    /// Sends a plan's outcome: rows, or an OK. `sv`: the session's
    /// variables the statement ran under (its results character set and
    /// time zone shape the values).
    #[allow(clippy::too_many_arguments)]
    async fn engine_answer(
        &mut self,
        plan: &Plan,
        columns: &[ColumnDef],
        o: &engine::exec::Outcome,
        format: RowFormat,
        status: StatusFlags,
        sv: &SessVars,
        out: &mut dyn ResultSink,
    ) -> Result<()> {
        self.engine_diag(&o.conds);
        if !plan.is_select() {
            // A write that read the whole table says so, as the servers do.
            let mut ok = OkPacket::new(if o.no_index {
                status | StatusFlags::NO_INDEX_USED
            } else {
                status
            });
            ok.affected_rows = o.affected;
            ok.last_insert_id = if o.last_insert_id == 0 {
                o.reported_id
            } else {
                o.last_insert_id
            };
            if o.last_insert_id != 0 {
                self.eng.last_insert_id = o.last_insert_id;
            }
            ok.warnings = o.warnings();
            if let Some(i) = &o.info {
                ok.info = Bytes::from(i.clone());
            }
            return out.ok(ok).await;
        }
        out.begin_rows(columns, format).await?;
        let Op::Select { out: cols } = &plan.op else {
            return Ok(());
        };
        match format {
            RowFormat::Binary => {
                let mut vals = Vec::with_capacity(cols.len());
                for r in &o.rows {
                    vals.clear();
                    for (d, oc) in r.iter().zip(cols) {
                        let c = &plan.table.columns[oc.col];
                        vals.push(meta::binary(c, d, &sv.env, sv.results));
                    }
                    out.row(RowValues::Binary(&vals)).await?;
                }
            }
            RowFormat::Text => {
                let mut texts: Vec<Option<Bytes>> = Vec::with_capacity(cols.len());
                for r in &o.rows {
                    texts.clear();
                    texts.extend(r.iter().zip(cols).map(|(d, oc)| {
                        meta::text_in(&plan.table.columns[oc.col], d, &sv.env, sv.results)
                    }));
                    let refs: Vec<Option<&[u8]>> = texts.iter().map(|t| t.as_deref()).collect();
                    out.row(RowValues::Text(&refs)).await?;
                }
            }
        }
        let mut s = status;
        if o.no_index {
            s |= StatusFlags::NO_INDEX_USED;
        }
        out.end_rows(EndOfRows {
            status: s,
            warnings: o.warnings(),
        })
        .await
    }

    /// The statement's conditions become the session's diagnostics area.
    fn engine_diag(&mut self, conds: &[crate::engine::assign::Condition]) {
        let (area, extra) = diag::kept(conds.iter().map(|c| {
            let level = match c.level {
                crate::engine::assign::Level::Note => "Note",
                crate::engine::assign::Level::Warning => "Warning",
            };
            diag::warning(level, c.code, c.message.clone())
        }));
        self.diag_set(area, extra);
    }

    /// The text-protocol hook: `Some` when the statement was the engine's
    /// (answered, or refused with its error).
    pub(super) async fn engine_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        sql: &Bytes,
        logical: Option<&[u8]>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some((cat, current)) = self.engine_scope() else {
            return Ok(None);
        };
        // Once per tenant and process: what dropped tables left behind
        // (a process stopped while it deleted them) goes.
        if let Ok(e) = engine::engine() {
            e.sweep_once(&cat);
        }
        // The session's wait for definition changes of engine tables
        // (MariaDB keeps its own copy of the variable).
        if let Some(w) = control::metadata_wait(sql) {
            self.eng.ddl_wait = match w {
                control::LockWait::Seconds(n) => Some(n),
                _ => None,
            };
        }
        let r = self
            .engine_text_in(cx, &cat, current, sql, logical, more, format, out)
            .await;
        if matches!(r, Ok(None)) {
            // MariaDB runs it: it may change a variable or the database.
            self.eng.stale();
        }
        r
    }

    /// [`Self::engine_text`] for a tenant with engine databases.
    // One pass: control statements, what isn't the engine's, then the plan.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn engine_text_in(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        current: Option<DbId>,
        sql: &Bytes,
        logical: Option<&[u8]>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        // Calls of stored functions (`routine`): run first, then the
        // statement with their values.
        if let Some(step) =
            Box::pin(self.engine_sql_functions(cx, cat, logical, more, format, out)).await?
        {
            return Ok(Some(step));
        }
        // An `INSERT`, `UPDATE` or `DELETE` that fires SQL triggers: run
        // row by row (`routine::trigger`).
        if let Some(step) =
            Box::pin(self.engine_sql_triggers(cx, cat, logical, sql, more, out)).await?
        {
            return Ok(Some(step));
        }
        // A `SET` of session variables the gateway keeps.
        if let Some(step) = Box::pin(self.engine_set(cx, cat, sql, more, out)).await? {
            return Ok(Some(step));
        }
        // `SET @x = ...`, and reads of variables, kept in the gateway.
        if let Some(step) = Box::pin(self.engine_user_set(cx, cat, current, sql, more, out)).await?
        {
            return Ok(Some(step));
        }
        // The client's own text (the name layer scopes lock names for
        // MariaDB), unless this is the lock's own answer.
        let client = self.eng.entry.clone().unwrap_or_else(|| sql.clone());
        if !self.eng.lock_answer
            && let Some(step) =
                Box::pin(self.engine_named_lock(cx, cat, current, &client, more, format, out))
                    .await?
        {
            return Ok(Some(step));
        }
        if let Some(step) = Box::pin(self.engine_do(cx, cat, current, sql, more, out)).await? {
            return Ok(Some(step));
        }
        if let Some(text) = self.engine_var_reads(cx, cat, sql) {
            return Box::pin(
                self.engine_text_in(cx, cat, current, &text, logical, more, format, out),
            )
            .await;
        }
        if let Some(ctl) = control::parse(sql) {
            return Box::pin(self.engine_control(cx, cat, current, ctl, more, out)).await;
        }
        // The MySQL port's `SHOW COLUMNS` and `SHOW INDEX`, as queries over
        // `information_schema` with a probe of the table: the engine's
        // `information_schema` answers them (`engine_show`), not the binder,
        // which would refuse the probe next to MariaDB's views.
        if let Ok(t) = std::str::from_utf8(sql)
            && let Some(p) = show::strip_probe(t)
            && crate::engine::infoschema::reads_tables(&p.rest)
        {
            return Ok(None);
        }
        // The MySQL port: a statement starting with `VALUES ROW` reaches
        // here as the union of one-row SELECTs MariaDB needs; the client's
        // text is planned instead (it reads no table), so it is described
        // as MySQL describes a table value constructor.
        let entry = self.eng.entry.clone();
        if self.persona == sprig_proto_mysql::Persona::MySql84
            && let Some(l) = entry.as_deref()
            && let Some(g) = self.tableless_values(cat, current, l)
        {
            return Box::pin(
                self.general_text(cx, cat, current, g, sql, logical, more, format, out),
            )
            .await
            .map(Some);
        }
        // A text sent before in the same context reuses its plan.
        let lex = self.lex_opts().0;
        // `INSERT ... SELECT`: whether its query read a table without a key.
        let mut from_select: Option<bool> = None;
        let bound = if let Some(p) = self.eng.text_plans.get(cat.version, sql, current, lex) {
            Ok(Bound::Engine(Box::new((*p).clone())))
        } else {
            let b = self.engine_bind(cat, current, sql);
            if let Ok(Bound::Engine(p)) = &b {
                self.eng.text_plans.put(cat.version, sql, current, lex, p);
            }
            b
        };
        let plan = match bound {
            Ok(Bound::Engine(p)) => p,
            Ok(Bound::General(g)) => {
                // A statement without tables that the planner can't plan
                // (a function it doesn't have) goes to MariaDB as before.
                if g.tables.is_empty() {
                    let sv = self.engine_vars(cx);
                    if let Err(e) = crate::engine::Engine::general_describe(&g, &sv.general)
                        && e.code() == Some(sprig_common::SprigCode::NotOnEngine)
                    {
                        return Ok(None);
                    }
                }
                return Box::pin(
                    self.general_text(cx, cat, current, *g, sql, logical, more, format, out),
                )
                .await
                .map(Some);
            }
            Ok(Bound::Write(w)) => {
                return Box::pin(self.write_text(cx, cat, *w, logical, more, out))
                    .await
                    .map(Some);
            }
            Ok(Bound::InsertSelect(x)) => {
                match Box::pin(self.insert_select_rows(cx, cat, current, *x)).await {
                    Ok((p, no_index)) => {
                        from_select = Some(no_index);
                        p
                    }
                    Err(e) => return self.fail(out, &e).await.map(Some),
                }
            }
            Ok(Bound::Elsewhere { tables }) => {
                if !self.eng.txn.is_open() {
                    let Some(e) = self.xa_held_refusal().filter(|_| tables) else {
                        return Ok(None);
                    };
                    return self.fail(out, &e).await.map(Some);
                }
                // Inside a transaction on the engine: MariaDB's tables
                // can't join it, and a transaction statement this layer
                // didn't read must not reach MariaDB alone.
                let effects = StmtInfo::classify(sql).effects;
                let control = effects.iter().any(|e| {
                    matches!(
                        e,
                        crate::backend::Effect::Begin
                            | crate::backend::Effect::Commit
                            | crate::backend::Effect::Rollback
                            | crate::backend::Effect::Savepoint
                            | crate::backend::Effect::XaStart
                            | crate::backend::Effect::XaEnd
                    )
                });
                if control {
                    let e = not_on_engine(
                        "a transaction statement in a form the engine doesn't read, inside a \
                         transaction on an engine database",
                    );
                    return self.fail(out, &e).await.map(Some);
                }
                if effects.iter().any(|e| {
                    matches!(
                        e,
                        crate::backend::Effect::ImplicitCommit | crate::backend::Effect::LockTables
                    )
                }) {
                    if let Err(e) = self.engine_implicit_commit().await {
                        return self.fail(out, &e).await.map(Some);
                    }
                    return Ok(None);
                }
                if tables {
                    return self.fail(out, &other_side_in_engine_txn()).await.map(Some);
                }
                return Ok(None);
            }
            Err(e) => return self.fail(out, &e).await.map(Some),
        };
        let mut plan = plan;
        match self.engine_row_check(cx, cat, &plan).await {
            Ok(c) => plan.check = c,
            Err(e) => return self.fail(out, &e).await.map(Some),
        }
        if let Err(e) = self.engine_checks(cat, &plan, None) {
            return self.fail(out, &e).await.map(Some);
        }
        let sv = self.engine_vars(cx);
        let columns = Self::engine_columns(&plan, sv.results);
        let pending = logical.and_then(|l| self.wl_start(l));
        let tenant = cat.tenant.0;
        let found_rows = cx.proto.has(Caps::FOUND_ROWS);
        let mut r = self.engine_run(tenant, &plan, &[], found_rows, &sv).await;
        // `INSERT ... SELECT` reports its rows however many there were,
        // and the last row's id when it generated none.
        if let Some(no_index) = from_select {
            match &mut r {
                Ok(o) => {
                    if o.info.is_none() {
                        o.info = Some(format!(
                            "Records: {}  Duplicates: 0  Warnings: {}",
                            o.affected,
                            crate::engine::exec::warning_count(&o.conds)
                        ));
                    }
                    o.no_index |= no_index;
                    if o.last_insert_id == 0 {
                        o.reported_id = last_row_id(&plan);
                    }
                }
                // MySQL reports a string too long for its column as
                // truncated when the rows come from a query.
                Err(e)
                    if sv.mysql
                        && matches!(
                            e.kind,
                            sprig_common::ErrorKind::Standard { errno: 1406, .. }
                        ) =>
                {
                    *e = SprigError::standard(
                        1265,
                        *b"01000",
                        e.message.replacen("Data too long", "Data truncated", 1),
                    );
                }
                Err(_) => {}
            }
        }
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let step = match r {
            Ok(o) => {
                self.engine_answer(&plan, &columns, &o, format, status, &sv, out)
                    .await?;
                self.fast_learnt(cat.version, plan.table.object);
                if let Some(l) = logical {
                    let oc = outcome_of(&o, &plan, status, &self.target.cluster);
                    let ran = Ran {
                        outcome: oc.as_ref(),
                        errored: false,
                        params: &[],
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                Step {
                    errored: false,
                    status: Some(status),
                }
            }
            Err(e) => {
                if let Some(l) = logical {
                    let ran = Ran {
                        errored: true,
                        ..Ran::default()
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                // The table's definition changed while the statement
                // waited for the change: outside a transaction it is read
                // again against the new catalog and run once more, as the
                // servers run a statement that waited for a metadata lock.
                if e.protocol().mysql_errno == 1412
                    && !self.eng.retrying
                    && !self.eng.txn.is_open()
                    && let Some(newer) = self
                        .engine_newer_catalog(plan.table.object, plan.table.version)
                        .await
                {
                    self.eng.retrying = true;
                    let r = Box::pin(
                        self.engine_text_in(cx, &newer, current, sql, logical, more, format, out),
                    )
                    .await;
                    self.eng.retrying = false;
                    return r;
                }
                let mut step = self.fail(out, &e).await?;
                // The transaction's state after the error (the engine may
                // have rolled it back).
                step.status = Some(status);
                step
            }
        };
        Ok(Some(step))
    }

    /// The session's catalog once it holds a definition of table `object`
    /// newer than `version` (another session changed the table a moment
    /// ago); `None` when none arrives within two seconds.
    async fn engine_newer_catalog(
        &mut self,
        object: sprig_common::ids::ObjectId,
        version: u64,
    ) -> Option<Arc<TenantCatalog>> {
        for _ in 0..40 {
            let cat = self.tenant.as_deref_mut()?.refresh().await.ok()?;
            let newer = catalog::of(&cat)
                .and_then(|e| e.tables.get(&object))
                .is_some_and(|p| matches!(p.as_ref(), Ok(d) if d.version > version));
            if newer {
                self.eng.stale();
                return Some(cat);
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        None
    }

    /// The prepare hook: `Some` (a `PREPARE_OK` summary) when the
    /// statement is the engine's.
    #[allow(clippy::too_many_lines)] // one arm per kind of bound statement
    pub(super) async fn engine_prepare(
        &mut self,
        cx: &mut ExecCtx<'_>,
        rewritten: &Bytes,
        logical: &Bytes,
        client: &Bytes,
        recheck: Option<crate::tenant::authz::Recheck>,
    ) -> Result<Option<ExecSummary>> {
        let Some((cat, current)) = self.engine_scope() else {
            return Ok(None);
        };
        // Transaction control is the session's: kept as text and answered
        // at each execute, by the engine's transaction or MariaDB's.
        if !rewritten.contains(&b'?')
            && let Some(ctl) = control::parse(rewritten)
        {
            match ctl {
                // MySQL 8.4 prepares `COMMIT` and `ROLLBACK` only, and no
                // XA statement.
                Control::Begin { .. }
                | Control::Savepoint(_)
                | Control::RollbackTo(_)
                | Control::Release(_)
                | Control::Xa(_)
                    if self.persona == sprig_proto_mysql::Persona::MySql84 =>
                {
                    return Err(SprigError::standard(
                        1295,
                        *b"HY000",
                        "This command is not supported in the prepared statement protocol yet",
                    ));
                }
                // Kept as text and answered at each execute.
                Control::Begin { .. }
                | Control::Commit { .. }
                | Control::Rollback { .. }
                | Control::Savepoint(_)
                | Control::RollbackTo(_)
                | Control::Release(_)
                | Control::SetTransaction {
                    scope: Scope::Next, ..
                }
                | Control::Set { .. }
                | Control::Xa(_)
                | Control::LockTables(_) => {
                    return Ok(Some(self.prepared_deferred(logical.clone(), Vec::new())));
                }
                // MariaDB's, as before.
                Control::SetTransaction { .. } | Control::UnlockTables => {}
            }
        }
        let plan = match self.engine_bind(&cat, current, rewritten)? {
            Bound::Engine(plan) => plan,
            Bound::General(g) => {
                return Box::pin(
                    self.general_prepare(cx, &cat, *g, rewritten, logical, client, recheck),
                )
                .await
                .map(Some);
            }
            Bound::Elsewhere { .. } => return Ok(None),
            Bound::InsertSelect(_) => {
                return Err(not_on_engine("INSERT ... SELECT as a prepared statement"));
            }
            Bound::Write(w) => {
                let results = self.engine_vars(cx).results;
                let types = write_param_types(self.persona, &w, rewritten);
                return Ok(Some(self.write_prepare(client, results, &types)));
            }
        };
        let mut plan = *plan;
        plan.check = self.engine_row_check(cx, &cat, &plan).await?;
        let checked = self.engine_checks(&cat, &plan, None)?;
        let results = self.engine_vars(cx).results;
        let persona = self.persona;
        let param_types = plan.param_types.clone();
        let columns = Self::engine_columns(&plan, results);
        let params = u16::try_from(plan.params).map_err(|_| {
            SprigError::standard(
                1390,
                *b"HY000",
                "Prepared statement contains too many placeholders",
            )
        })?;
        let mut st = Stmt::engine(
            rewritten.clone(),
            StmtInfo::classify(rewritten),
            params,
            columns.clone(),
        );
        st.logical = Some(logical.clone());
        st.recheck = recheck;
        st.client = Some(client.clone());
        st.engine = Some(Box::new(EnginePrepared {
            plan: Arc::new(plan),
            columns: columns.clone().into(),
            results,
            checked,
            general: None,
            temp_gen: self.eng.temps.generation(),
        }));
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        Ok(Some(ExecSummary {
            status: None,
            prepared: Some(PreparedMeta {
                stmt,
                params: param_types
                    .iter()
                    .map(|t| meta::param_def(persona, *t, results))
                    .collect(),
                columns,
                warnings: 0,
            }),
            db: None,
        }))
    }

    /// The execute hook (the prepared fast path): `Some` when `stmt` is an
    /// engine statement. No planning: the plan from the prepare runs as it
    /// is while the catalog and the table's definition stay the same.
    ///
    /// The execute's conditions (or its error) become the session's
    /// diagnostics area; one that isn't the engine's leaves it to the
    /// backend connection.
    pub(super) async fn engine_execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        raw: &StmtExecute,
        params: &ExecuteParams,
        out: &mut dyn ResultSink,
    ) -> Result<Option<ExecSummary>> {
        let engine = self.stmts.get_mut(stmt).is_ok_and(|s| s.engine.is_some());
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.warnings = None;
            self.eng.diag.bypass(!engine);
        }
        let r = Box::pin(self.engine_execute_in(cx, stmt, raw, params, out)).await;
        if engine && let Err(e) = &r {
            self.diag_set(self.diag_error(e), 0);
        }
        r
    }

    /// [`Self::engine_execute`] without the diagnostics.
    #[allow(clippy::too_many_lines)] // the checks, then one plan
    async fn engine_execute_in(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        raw: &StmtExecute,
        params: &ExecuteParams,
        out: &mut dyn ResultSink,
    ) -> Result<Option<ExecSummary>> {
        let persona = self.persona;
        let (plan, recheck, logical) = {
            let s = self.stmts.get_mut(stmt)?;
            let Some(e) = s.engine.as_ref() else {
                return Ok(None);
            };
            // A general SELECT is planned now, with this execute's values.
            if let Some(g) = e.general.clone() {
                let (recheck, logical) = (s.recheck.clone(), s.logical.clone());
                return Box::pin(
                    self.general_execute(cx, stmt, g, recheck, logical, raw, params, out),
                )
                .await;
            }
            (Arc::clone(&e.plan), s.recheck.clone(), s.logical.clone())
        };
        // A table with SQL triggers: its changes run row by row, from text
        // only (`routine::trigger`).
        if let Some(l) = &logical
            && self.engine_prepared_fires(l)
        {
            // The bound values as literals, then the text path.
            let values: Vec<String> = self
                .stmts
                .get_mut(stmt)?
                .params_for_backend(params, persona)?
                .iter()
                .map(crate::exec::search::param_literal)
                .collect();
            let text = std::str::from_utf8(l)
                .ok()
                .and_then(|t| sprig_search::sqlscan::interpolate(t, &values))
                .map(Bytes::from)
                .ok_or_else(|| SprigError::internal("a prepared write's parameters"))?;
            let step = Box::pin(self.tenant_text(
                cx,
                &text,
                &crate::exec::QueryAttrs::default(),
                false,
                RowFormat::Binary,
                out,
            ))
            .await?;
            return Ok(Some(ExecSummary {
                status: step.status,
                prepared: None,
                db: None,
            }));
        }
        // Privileges, as the general execute checks them.
        if let Some(rc) = &recheck
            && let Some(ts) = self.tenant.as_deref_mut()
        {
            ts.refresh().await?;
            match crate::tenant::authz::recheck(ts, persona, rc) {
                Err(e) if e.protocol().mysql_errno == 1615 => {
                    if !Box::pin(self.reprepare_engine(cx, stmt)).await? {
                        return Err(e);
                    }
                    return Box::pin(self.engine_execute(cx, stmt, raw, params, out)).await;
                }
                r => r?,
            }
        }
        let Some(cat) = self.engine_cat() else {
            return Err(not_on_engine("the database is no longer on the engine"));
        };
        // The table changed (a new index, a dropped table): bound again.
        // (The table's current definition is looked at, not cloned, and
        // only when the catalog moved on.)
        let same = self.engine_temp_gen_of(stmt) == Some(self.eng.temps.generation())
            && (cat.version == plan.catalog_version
                || catalog::of(&cat)
                    .and_then(|e| e.tables.get(&plan.table.object))
                    .is_some_and(|p| {
                        matches!(&**p, Ok(d) if d.version == plan.table.version
                        && d.indexes.len() == plan.table.indexes.len())
                    }));
        let plan = if same {
            plan
        } else {
            if !Box::pin(self.reprepare_engine(cx, stmt)).await? {
                return Err(SprigError::standard(
                    1615,
                    *b"HY000",
                    "Prepared statement needs to be re-prepared",
                ));
            }
            let s = self.stmts.get_mut(stmt)?;
            match s.engine.as_ref() {
                Some(e) => Arc::clone(&e.plan),
                None => return Ok(None),
            }
        };
        let checked = {
            let s = self.stmts.get_mut(stmt)?;
            s.engine.as_ref().and_then(|e| e.checked.clone())
        };
        match self.engine_checks(&cat, &plan, checked.as_ref()) {
            Ok(Some(c)) => {
                let s = self.stmts.get_mut(stmt)?;
                if let Some(e) = s.engine.as_mut() {
                    e.checked = Some(c);
                }
            }
            Ok(None) => {}
            // Its row-level security condition must be bound again.
            Err(e) if e.protocol().mysql_errno == 1615 => {
                if !Box::pin(self.reprepare_engine(cx, stmt)).await? {
                    return Err(e);
                }
                return Box::pin(self.engine_execute(cx, stmt, raw, params, out)).await;
            }
            Err(e) => return Err(e),
        }
        // Parameters, long data folded in.
        let mut vals: Vec<Datum> = Vec::with_capacity(plan.params);
        {
            let s = self.stmts.get_mut(stmt)?;
            for (i, p) in params
                .params
                .iter()
                .filter(|p| p.name.is_empty())
                .enumerate()
            {
                vals.push(match &p.value {
                    ParamValue::Value(v) => expr::param_datum(v)?,
                    ParamValue::LongData => {
                        let k = u16::try_from(i).unwrap_or(u16::MAX);
                        Datum::Str(
                            s.long_data
                                .remove(&k)
                                .map(bytes::BytesMut::freeze)
                                .unwrap_or_default(),
                        )
                    }
                });
            }
            s.clear_long_data();
            if params.new_params_bound {
                s.types = Some(params.params.iter().map(|p| p.param_type).collect());
            }
        }
        // The columns for the session's results character set now.
        let sv = self.engine_vars(cx);
        let results = sv.results;
        let columns = {
            let s = self.stmts.get_mut(stmt)?;
            match s.engine.as_mut() {
                Some(e) if e.results == results => Arc::clone(&e.columns),
                Some(e) => {
                    e.columns = Self::engine_columns(&plan, results).into();
                    e.results = results;
                    Arc::clone(&e.columns)
                }
                None => Arc::from(Vec::new()),
            }
        };
        let mut pending = match &logical {
            Some(l) => self.wl_start_stmt_known(stmt, l, sv.current.as_deref(), sv.lex),
            None => None,
        };
        // The engine's prepared executions are gathered per session for the
        // recorder.
        if let Some(p) = pending.as_mut() {
            p.batch();
        }
        let found_rows = cx.proto.has(Caps::FOUND_ROWS);
        let r = self
            .engine_run(cat.tenant.0, &plan, &vals, found_rows, &sv)
            .await;
        // Once per statement, after it: the engine may have ended the
        // transaction.
        let status = self.engine_status(&sv);
        // The first answer after the statement was prepared again with
        // other result columns says so.
        let answered = if std::mem::take(&mut self.eng.metadata_changed) {
            status | StatusFlags::METADATA_CHANGED
        } else {
            status
        };
        match r {
            Ok(o) => {
                self.engine_answer(&plan, &columns, &o, RowFormat::Binary, answered, &sv, out)
                    .await?;
                if let Some(l) = &logical {
                    let oc = outcome_of(&o, &plan, status, &self.target.cluster);
                    let ran = Ran {
                        outcome: oc.as_ref(),
                        errored: false,
                        params: &[],
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                Ok(Some(ExecSummary {
                    status: Some(status),
                    prepared: None,
                    db: None,
                }))
            }
            Err(e) => {
                if let Some(l) = &logical {
                    let ran = Ran {
                        errored: true,
                        ..Ran::default()
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                // The table's definition changed while the statement
                // waited for the change: prepared and run again, once.
                if e.protocol().mysql_errno == 1412
                    && !self.eng.retrying
                    && !self.eng.txn.is_open()
                    && self
                        .engine_newer_catalog(plan.table.object, plan.table.version)
                        .await
                        .is_some()
                {
                    self.eng.retrying = true;
                    let r = Box::pin(self.engine_execute(cx, stmt, raw, params, out)).await;
                    self.eng.retrying = false;
                    return r;
                }
                // The error is the answer; the summary carries the
                // session's status after it (a deadlock or a snapshot
                // conflict ended the transaction), so what the connection
                // reports next (`COM_PING`) is right.
                self.fail(out, &e).await?;
                Ok(Some(ExecSummary {
                    status: Some(status),
                    prepared: None,
                    db: None,
                }))
            }
        }
    }

    /// Binds statement `stmt`'s text again (its table changed, or its
    /// row-level security predicates must be bound again): prepared again
    /// from the client's text when that was kept, as a general
    /// re-prepare does. `false` when the result's shape changed.
    async fn reprepare_engine(&mut self, cx: &mut ExecCtx<'_>, stmt: StmtRef) -> Result<bool> {
        let (client, params) = {
            let s = self.stmts.get_mut(stmt)?;
            (s.client.clone(), s.params)
        };
        let Some(client) = client else {
            return Ok(false);
        };
        let summary = Box::pin(self.tenant_prepare(cx, client)).await?;
        let Some(meta) = summary.prepared else {
            return Ok(false);
        };
        let Some(new) = self.stmts.remove(meta.stmt) else {
            return Ok(false);
        };
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .decrement(1.0);
        // The parameters must be the ones the client prepared; the result's
        // columns may have changed with the table (`SELECT *` after `ALTER
        // TABLE ... ADD COLUMN`): the answer carries their definitions, as
        // the servers' answers do.
        if new.engine.is_none() || new.params != params {
            return Ok(false);
        }
        let s = self.stmts.get_mut(stmt)?;
        let changed = s.columns != new.columns;
        s.columns = new.columns;
        s.sql = new.sql;
        s.info = new.info;
        s.recheck = new.recheck;
        s.logical = new.logical;
        s.engine = new.engine;
        s.fp = None;
        self.eng.metadata_changed |= changed;
        Ok(true)
    }
}

/// The outcome workload capture records; `cluster`: the session's.
fn outcome_of(
    o: &engine::exec::Outcome,
    plan: &Plan,
    status: StatusFlags,
    cluster: &sprig_common::ids::ClusterId,
) -> Option<StmtOutcome> {
    Some(StmtOutcome {
        status,
        warnings: o.warnings(),
        affected_rows: o.affected,
        last_insert_id: o.last_insert_id,
        error: None,
        node: crate::backend::cluster::NodeKey {
            cluster: cluster.clone(),
            node: sprig_common::ids::NodeId::new("engine").ok()?,
        },
        route: crate::backend::RouteKind::Writer,
        pinned: false,
        rows: if plan.is_select() {
            o.rows.len() as u64
        } else {
            0
        },
        examined: Some(o.examined),
    })
}

/// MySQL's answer to sequence DDL: a syntax error at `SEQUENCE`.
fn no_sequences(text: &[u8]) -> SprigError {
    let t = String::from_utf8_lossy(text);
    let near = t
        .to_ascii_uppercase()
        .find("SEQUENCE")
        .and_then(|i| t.get(i..))
        .unwrap_or("SEQUENCE")
        .to_owned();
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{near}' at line 1"
        ),
    )
}

impl PassSession {
    /// DDL on an engine database that the engine can't follow is refused
    /// before the catalog runs it: an `ALTER TABLE` that also renames the
    /// table, `RENAME TABLE` between an engine database and a
    /// MariaDB-backed one, `CREATE TABLE ... SELECT` (it would put rows
    /// into the MariaDB copy), temporary tables, and views and triggers
    /// (MariaDB would evaluate them over the empty copy). A `CREATE TABLE`
    /// (`text`: the client's statement) with a column type or collation
    /// the engine doesn't serve, or a table element it can't keep, is
    /// refused too, so the table is never created half-usable. The other
    /// changes of a table (`ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`,
    /// `TRUNCATE`, `DROP`) run with the engine taking part
    /// ([`Self::engine_ddl_hook`]).
    pub(super) fn engine_ddl_check(
        &self,
        target: &sprig_sql::names::DdlTarget,
        text: &[u8],
    ) -> Result<()> {
        use sprig_sql::names::{AlterOp, DdlKind, DdlName, DdlTarget};
        let Some((cat, _)) = self.engine_scope_base() else {
            return Ok(());
        };
        let Some(eng) = catalog::of(&cat) else {
            return Ok(());
        };
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        let on_engine = |n: &DdlName| {
            n.db.as_deref()
                .and_then(|d| cat.database(&database_key(d, lctn)))
                .is_some_and(|d| eng.dbs.contains(&d.db_id))
        };
        let refuse = |what: &str| Err(not_on_engine(what));
        match target {
            DdlTarget::CreateObject {
                kind,
                name,
                as_select,
                parent,
                temporary,
                ..
            } => {
                if !on_engine(name) && !parent.as_ref().is_some_and(on_engine) {
                    return Ok(());
                }
                if *temporary {
                    return refuse("a temporary table in an engine database");
                }
                if *kind == DdlKind::Sequence && self.persona == sprig_proto_mysql::Persona::MySql84
                {
                    return Err(no_sequences(text));
                }
                match kind {
                    DdlKind::Table if *as_select => refuse("CREATE TABLE ... SELECT"),
                    DdlKind::Table if on_engine(name) => {
                        match crate::engine::table::check_create(&String::from_utf8_lossy(text)) {
                            Ok(()) => Ok(()),
                            Err(why) => refuse(&why),
                        }
                    }
                    DdlKind::Table
                    | DdlKind::View
                    | DdlKind::Sequence
                    | DdlKind::Procedure
                    | DdlKind::Function
                    | DdlKind::Trigger
                    | DdlKind::Event => Ok(()),
                }
            }
            DdlTarget::AlterObject {
                kind: DdlKind::Sequence,
                ..
            }
            | DdlTarget::DropObjects {
                kind: DdlKind::Sequence,
                ..
            } if self.persona == sprig_proto_mysql::Persona::MySql84 => Err(no_sequences(text)),
            // `ALTER SEQUENCE` of a name that is none: MariaDB's 4091.
            DdlTarget::AlterObject {
                kind: DdlKind::Sequence,
                name,
                if_exists: false,
                ..
            } if on_engine(name)
                && name.db.as_deref().is_some_and(|d| {
                    cat.database(&database_key(d, lctn))
                        .is_some_and(|row| eng.sequence(row.db_id, &name.name, lctn).is_none())
                }) =>
            {
                Err(SprigError::standard(
                    4091,
                    *b"42S02",
                    format!("Unknown SEQUENCE: '{}'", name.name),
                ))
            }
            DdlTarget::AlterObject {
                kind: DdlKind::Table,
                name,
                op,
                ..
            } if on_engine(name) => match op {
                AlterOp::CreateIndex { .. }
                | AlterOp::DropIndex { .. }
                | AlterOp::Change { rename_to: None } => Ok(()),
                AlterOp::Change { .. } => refuse(
                    "ALTER TABLE that renames the table among other changes (rename it in a statement of its own)",
                ),
            },
            // A table keeps its rows where they are: it can move between
            // engine databases, not between the engine and MariaDB.
            DdlTarget::RenameTables { pairs, .. }
                if pairs.iter().any(|(a, b)| on_engine(a) != on_engine(b)) =>
            {
                refuse("RENAME TABLE between an engine database and a MariaDB-backed one")
            }
            _ => Ok(()),
        }
    }

    /// The engine's part of DDL statement `target`, when it changes,
    /// empties or drops tables of an engine database: the executor the
    /// catalog's DDL path runs the statement through
    /// (`crate::engine::ddlexec`).
    #[allow(clippy::match_same_arms, clippy::too_many_lines)] // one arm per object kind
    pub(super) fn engine_ddl_hook(
        &mut self,
        cx: &ExecCtx<'_>,
        target: &sprig_sql::names::DdlTarget,
    ) -> Option<crate::engine::ddlexec::Hook> {
        use sprig_sql::names::{DdlKind, DdlName, DdlTarget};
        let (cat, _) = self.engine_scope_base()?;
        let ts = self.tenant.as_deref()?;
        let tenant = ts.tenant;
        let lctn = ts.lower_case_table_names();
        let account = (ts.principal.user.clone(), ts.principal.host.clone());
        let db_of = |d: &str| {
            let eng = catalog::of(&cat)?;
            cat.database(&database_key(d, lctn))
                .filter(|x| eng.dbs.contains(&x.db_id))
                .map(|_| d.to_owned())
        };
        let of_name = |n: &DdlName| n.db.as_deref().and_then(db_of);
        let db = match target {
            DdlTarget::AlterObject {
                kind: DdlKind::Table,
                name,
                ..
            }
            | DdlTarget::Truncate { name }
            | DdlTarget::CreateObject {
                kind: DdlKind::Table,
                name,
                temporary: false,
                ..
            } => of_name(name),
            DdlTarget::CreateObject {
                kind: DdlKind::View,
                name,
                ..
            }
            | DdlTarget::AlterObject {
                kind: DdlKind::View,
                name,
                ..
            } => of_name(name),
            DdlTarget::DropObjects {
                kind: DdlKind::View | DdlKind::Sequence,
                names,
                ..
            } => names.iter().find_map(of_name),
            DdlTarget::CreateObject {
                kind: DdlKind::Sequence,
                name,
                ..
            }
            | DdlTarget::AlterObject {
                kind: DdlKind::Sequence,
                name,
                ..
            } => of_name(name),
            DdlTarget::RenameTables { pairs, .. } => pairs.iter().find_map(|(a, _)| of_name(a)),
            // Stored procedures and functions (`crate::engine::routine`).
            DdlTarget::CreateObject {
                kind: DdlKind::Procedure | DdlKind::Function | DdlKind::Trigger | DdlKind::Event,
                name,
                ..
            }
            | DdlTarget::AlterObject {
                kind: DdlKind::Procedure | DdlKind::Function | DdlKind::Event,
                name,
                ..
            } => of_name(name),
            DdlTarget::DropObjects {
                kind: DdlKind::Procedure | DdlKind::Function | DdlKind::Trigger | DdlKind::Event,
                names,
                ..
            } => names.iter().find_map(of_name),
            DdlTarget::DropObjects {
                kind: DdlKind::Table,
                names,
                temporary: false,
                ..
            } => names.iter().find_map(of_name),
            DdlTarget::DropDatabase { name, .. } => db_of(name),
            _ => None,
        }?;
        let creates = matches!(
            target,
            DdlTarget::CreateObject {
                or_replace: false,
                ..
            }
        );
        let eng = engine::engine().ok()?;
        let routine = self.routine_ddl_session(cx, target);
        let sv = self.engine_vars(cx);
        // `lock_wait_timeout`: how long a definition change waits for the
        // statements and transactions using its table.
        let default = if sv.mysql { 31_536_000 } else { 86_400 };
        let wait = self.eng.ddl_wait.unwrap_or(default);
        let hook = crate::engine::ddlexec::Hook::new(
            eng,
            tenant,
            cat,
            lctn,
            crate::engine::ddl::Session {
                env: sv.env,
                wait: std::time::Duration::from_secs(wait),
                db,
                conn: cx.session.id.0,
                account,
            },
            creates,
        );
        Some(match routine {
            Some(r) => hook.with_routine(r),
            None => hook,
        })
    }

    /// The catalog answered a DDL statement the engine took part in
    /// (`published`: the catalog version of its change, when it made one):
    /// a published change is applied, another is abandoned. Returns what
    /// the engine reports for the statement.
    pub(super) async fn engine_ddl_done(
        &mut self,
        hook: Option<crate::engine::ddlexec::Hook>,
        failed: bool,
        published: Option<u64>,
    ) -> crate::engine::ddlexec::Report {
        use crate::engine::ddlexec::Outcome;
        let Some(hook) = hook else {
            return crate::engine::ddlexec::Report::default();
        };
        // The session reads the new definitions from now on.
        self.eng.stale();
        let cat = match (published, self.tenant.as_deref_mut()) {
            (Some(v), Some(ts)) => {
                ts.changed(Some(v));
                ts.refresh().await.ok()
            }
            _ => None,
        };
        let outcome = match (&cat, failed) {
            (Some(c), _) => Outcome::Published(c),
            (None, true) => Outcome::Failed,
            (None, false) => Outcome::Unknown,
        };
        hook.done(outcome).await
    }
}

/// A sink that adds an open engine transaction's status flags to the
/// answers MariaDB makes while it is open (`SELECT 1`, `SET`, `SHOW`):
/// the client's driver follows the transaction by these flags.
pub(super) struct TxnSink<'a> {
    inner: &'a mut dyn ResultSink,
    flags: StatusFlags,
}

impl<'a> TxnSink<'a> {
    /// A sink over `inner` adding `flags`.
    pub(super) fn new(inner: &'a mut dyn ResultSink, flags: StatusFlags) -> Self {
        Self { inner, flags }
    }
}

#[async_trait]
impl ResultSink for TxnSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }

    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }

    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }

    async fn cursor_opened(&mut self, columns: &[ColumnDef], mut end: EndOfRows) -> Result<()> {
        end.status |= self.flags;
        self.inner.cursor_opened(columns, end).await
    }

    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }

    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }

    fn try_raw_row(&mut self, payload: Bytes) -> std::result::Result<Result<()>, Bytes> {
        self.inner.try_raw_row(payload)
    }

    async fn end_rows(&mut self, mut end: EndOfRows) -> Result<()> {
        end.status |= self.flags;
        self.inner.end_rows(end).await
    }

    async fn ok(&mut self, mut ok: OkPacket) -> Result<()> {
        ok.status |= self.flags;
        self.inner.ok(ok).await
    }

    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.inner.error(err).await
    }

    async fn column_list(&mut self, columns: &[ColumnDef], mut end: EndOfRows) -> Result<()> {
        end.status |= self.flags;
        self.inner.column_list(columns, end).await
    }

    async fn local_infile_request(&mut self, file_name: &[u8]) -> Result<()> {
        self.inner.local_infile_request(file_name).await
    }

    async fn local_infile_read(&mut self) -> Result<Bytes> {
        self.inner.local_infile_read().await
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.flush().await
    }

    fn finished(&self) -> bool {
        self.inner.finished()
    }
}

/// The context types of a multi-table write's `?` parameters (see
/// `engine::paramtype`), over every table the write reads; MySQL's port
/// only (MariaDB types none).
fn write_param_types(
    persona: sprig_proto_mysql::Persona,
    w: &crate::engine::multiwrite::MultiWrite,
    rewritten: &Bytes,
) -> Vec<Option<crate::engine::table::ColType>> {
    if persona != sprig_proto_mysql::Persona::MySql84 {
        return Vec::new();
    }
    let Ok(text) = std::str::from_utf8(rewritten) else {
        return Vec::new();
    };
    let Some(stmt) = sqlparser::parser::Parser::parse_sql(&sprig_plan::dialect::SprigMySql, text)
        .ok()
        .and_then(|mut v| v.pop())
    else {
        return Vec::new();
    };
    let named: Vec<crate::engine::paramtype::Named<'_>> = w
        .select
        .tables
        .iter()
        .map(|t| crate::engine::paramtype::Named {
            aliases: &t.aliases,
            def: &t.def,
        })
        .collect();
    crate::engine::paramtype::infer(&stmt, &named)
}
