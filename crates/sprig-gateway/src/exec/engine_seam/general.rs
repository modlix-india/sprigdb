//! The seam's part for general `SELECT`s on engine tables
//! ([`crate::engine::general`]): the statements the single-table binder
//! doesn't take, checked here and run through the planner.
//!
//! ```text
//! text:     engine_bind → Bound::General → general_text
//! prepare:  engine_bind → Bound::General → general_prepare (described, not run)
//! execute:  engine_execute sees the prepared record's statement → general_execute
//!
//! every path, before a row is read:
//!   general_checks:
//!     row-level security   each table whose policies bind the session must be
//!                          one the statement's rewrite covered, else refused
//!     privileges           SELECT on each table, or on every column of it the
//!                          statement may read (1143); none at all: 1142
//!   a prepared statement is bound and checked again from the client's text
//!   when the catalog or the session's privileges changed since
//! ```

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use sprig_auth::authz::account_sql::table_denied;
use sprig_auth::authz::{Action, CompiledPrivileges, SessionGrants, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::DbId;
use sprig_common::{Result, SprigError};
use sprig_proto_mysql::command::{ExecuteParams, ParamValue, StmtExecute};
use sprig_proto_mysql::{Caps, StatusFlags};

use super::{EnginePrepared, SessVars, diag, engine_in_other_txn};
use crate::backend::{StmtInfo, StmtOutcome};
use crate::engine::general::results::{self, Layout};
use crate::engine::general::{General, Rows, RunArgs};
use crate::engine::meta;
use crate::engine::row::Datum;
use crate::engine::session::{Start, XaPhase};
use crate::engine::{self, catalog, expr, not_on_engine};
use crate::exec::PassSession;
use crate::exec::capture::Ran;
use crate::exec::stmt::Stmt;
use crate::exec::tenant::Step;
use crate::pipeline::{
    EndOfRows, ExecCtx, ExecSummary, PreparedMeta, ResultSink, RowFormat, RowValues, StmtRef,
};

/// 1615: the statement is prepared again from the client's text.
fn needs_reprepare() -> SprigError {
    SprigError::standard(
        1615,
        *b"HY000",
        "Prepared statement needs to be re-prepared",
    )
}

/// Whether `g`'s tables are still the ones `cat` holds.
fn current(cat: &TenantCatalog, g: &General) -> bool {
    if cat.version == g.catalog_version {
        return true;
    }
    let Some(eng) = catalog::of(cat) else {
        return false;
    };
    g.tables.iter().all(|t| {
        matches!(
            eng.tables.get(&t.def.object).map(AsRef::as_ref),
            Some(Ok(d)) if d.version == t.def.version && d.indexes.len() == t.def.indexes.len()
        )
    })
}

impl PassSession {
    /// What must hold before a general `SELECT` reads a row: every table
    /// whose row-level security binds the session is covered by the
    /// statement's rewrite (`covered`: the tables the rewrite of this
    /// statement covered), and the session may read every table, and
    /// every column the statement may read. Returns the privileges checked.
    ///
    /// # Errors
    /// 1142, 1143; `ER_SPRIG_NOT_ON_ENGINE` for a protected table the
    /// rewrite didn't cover.
    pub(super) fn general_checks(
        &self,
        cat: &TenantCatalog,
        g: &General,
    ) -> Result<Option<Arc<dyn SessionGrants>>> {
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        let p = crate::tenant::authz::privileges(ts, cat);
        // Row-level security. The rewrite put each bound table's predicate
        // into the text and listed the table; a bound table that isn't
        // listed would be read unfiltered.
        let mut bound: Vec<sprig_common::ids::ObjectId> = Vec::new();
        if let Some(rls) = p.row_policies() {
            bound.extend(
                g.tables
                    .iter()
                    .map(|t| t.def.object)
                    .filter(|o| rls.tables.contains_key(o)),
            );
            for t in &g.tables {
                if rls.tables.contains_key(&t.def.object) && !ts.rls.covered.contains(&t.def.object)
                {
                    return Err(not_on_engine(&format!(
                        "a statement that reads table '{}', whose rows are filtered for this \
                         session, in a way its row-level security can't be applied to",
                        t.def.name
                    )));
                }
            }
        }
        if p.unrestricted() {
            return Ok(Some(p));
        }
        let Some(cp) = p.as_any().downcast_ref::<CompiledPrivileges>() else {
            // Another authorizer: its check of each table already ran; the
            // exact column rules are the built-in model's.
            return Ok(Some(p));
        };
        let client_host = ts.principal.client_ip.to_string();
        for t in &g.tables {
            let db_key = database_key(&t.db_name, ts.lower_case_table_names());
            let target = Target::Object {
                db_key: &db_key,
                db: t.def.db,
                object: t.def.object,
            };
            if cp.actions_on(target).has(Action::Select) {
                continue;
            }
            let may = |c: usize| {
                cp.columns
                    .get(&(t.def.object, t.def.columns[c].name.to_lowercase()))
                    .is_some_and(|a| a.has(Action::Select))
            };
            // No privilege on the table at all: the table is denied, as
            // the servers deny it before they look at columns.
            let first_missing = t.order.iter().copied().find(|c| !may(*c));
            // No privilege on the table at all, a `*` over a column the
            // session may not read (the servers deny the table then), or a
            // table whose rows the session's row-level security filters:
            // its predicate's columns are in the text but aren't the
            // client's to be told about, so the table is denied as a whole.
            if !(0..t.def.columns.len()).any(may)
                || first_missing.is_some_and(|c| t.starred[c] || bound.contains(&t.def.object))
            {
                return Err(table_denied(
                    self.persona,
                    Action::Select.command(),
                    &ts.principal.user,
                    &client_host,
                    &t.db_name,
                    &t.def.name,
                ));
            }
            if let Some(c) = first_missing {
                return Err(SprigError::standard(
                    1143,
                    *b"42000",
                    format!(
                        "{} command denied to user '{}'@'{}' for column '{}' in table '{}'",
                        Action::Select.command(),
                        ts.principal.user,
                        client_host,
                        t.def.columns[c].name,
                        t.def.name
                    ),
                ));
            }
        }
        Ok(Some(p))
    }

    /// Runs `g`: as a statement of the session's engine transaction
    /// (opened now when `autocommit` is off), or in its own.
    async fn general_run(
        &mut self,
        tenant: u64,
        g: &General,
        params: &[Datum],
        sv: &SessVars,
        stop: std::sync::Arc<crate::session::SessionHandle>,
    ) -> Result<Rows> {
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
        // The session's statement time limit: storage checks it as the
        // statement reads.
        let timed_out: fn() -> SprigError = if sv.mysql {
            sprig_engine::txn::errors::statement_timeout_mysql
        } else {
            sprig_engine::txn::errors::statement_timeout_mariadb
        };
        let a = RunArgs {
            tenant,
            params,
            seed: &sv.general,
            deadline: sv.time_limit.map(|d| (Instant::now() + d, timed_out)),
            stop: Some(stop),
        };
        if !self.eng.txn.is_open() {
            if !sv.autocommit_off {
                return e.general_run(g, &a, sv.opts, sv.env).await;
            }
            // `autocommit = 0`: the statement opens the transaction.
            self.eng
                .txn
                .begin(e, sv.opts, sv.read_only, Start::default())?;
        }
        // The session's `sql_mode` and time zone as they are now. The
        // statement itself starts in `general_run_in`: starting it here
        // too marked the previous statement as still running, and the
        // second start undid that statement's writes.
        if let Some(tx) = self.eng.txn.open_tx() {
            tx.set_env(sv.env);
        }
        let r = e.general_run_in(g, &a, &mut self.eng.txn).await;
        self.eng.txn.statement_done();
        r
    }

    /// Sends a general `SELECT`'s rows.
    async fn general_answer(
        &mut self,
        g: &General,
        rows: &Rows,
        format: RowFormat,
        status: StatusFlags,
        sv: &SessVars,
        out: &mut dyn ResultSink,
    ) -> Result<()> {
        // The statement's conditions become the diagnostics area; one
        // that read no table and raised nothing leaves it as the
        // statement's kind says (`diag`).
        if rows.warnings > 0 || !g.tables.is_empty() {
            let (area, extra) = diag::kept(
                rows.conds
                    .iter()
                    .map(|w| diag::warning(w.level.name(), w.code, w.message.clone())),
            );
            let beyond = usize::from(rows.warnings).saturating_sub(area.len());
            self.diag_set(area, extra.max(beyond));
        }
        let Layout { shapes, defs } = results::layout(g, &rows.columns, sv.results);
        let mut s = status;
        if rows.no_index || results::set_operation_scans(&g.stmt) {
            s |= StatusFlags::NO_INDEX_USED;
        }
        match format {
            RowFormat::Binary => {
                // Every value is made before anything is sent: one that
                // doesn't fit its column's announced type fails the
                // statement instead of breaking the row stream.
                let mut all = Vec::with_capacity(rows.rows.len());
                for r in &rows.rows {
                    let mut vals = Vec::with_capacity(defs.len());
                    for ((v, shape), def) in r.iter().zip(&shapes).zip(&defs) {
                        vals.push(results::binary(g, shape, def, v, &sv.env, sv.results)?);
                    }
                    all.push(vals);
                }
                out.begin_rows(&defs, format).await?;
                for vals in &all {
                    out.row(RowValues::Binary(vals)).await?;
                }
            }
            RowFormat::Text => {
                out.begin_rows(&defs, format).await?;
                let mut texts: Vec<Option<Bytes>> = Vec::with_capacity(defs.len());
                for r in &rows.rows {
                    texts.clear();
                    texts.extend(
                        r.iter()
                            .zip(&shapes)
                            .map(|(v, shape)| results::text(g, shape, v, &sv.env, sv.results)),
                    );
                    let refs: Vec<Option<&[u8]>> = texts.iter().map(|t| t.as_deref()).collect();
                    out.row(RowValues::Text(&refs)).await?;
                }
            }
        }
        out.end_rows(EndOfRows {
            status: s,
            warnings: rows.warnings,
        })
        .await
    }

    /// The outcome workload capture records.
    fn general_outcome(&self, rows: &Rows, status: StatusFlags) -> Option<StmtOutcome> {
        Some(StmtOutcome {
            status,
            warnings: rows.warnings,
            affected_rows: 0,
            last_insert_id: 0,
            error: None,
            node: crate::backend::cluster::NodeKey {
                cluster: self.target.cluster.clone(),
                node: sprig_common::ids::NodeId::new("engine").ok()?,
            },
            route: crate::backend::RouteKind::Writer,
            pinned: false,
            rows: rows.rows.len() as u64,
            examined: Some(rows.examined),
        })
    }

    /// The text-protocol path of a general `SELECT`: checked, run,
    /// answered (or refused with its error).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn general_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        current_db: Option<DbId>,
        g: General,
        sql: &Bytes,
        logical: Option<&[u8]>,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        if let Err(e) = self.general_checks(cat, &g) {
            return self.fail(out, &e).await;
        }
        let sv = self.engine_vars(cx);
        let pending = logical.and_then(|l| self.wl_start(l));
        let r = self
            .general_run(
                cat.tenant.0,
                &g,
                &[],
                &sv,
                std::sync::Arc::clone(cx.session.handle()),
            )
            .await;
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match r {
            Ok(rows) => {
                self.general_answer(&g, &rows, format, status, &sv, out)
                    .await?;
                if let Some(l) = logical {
                    let oc = self.general_outcome(&rows, status);
                    let ran = Ran {
                        outcome: oc.as_ref(),
                        errored: false,
                        params: &[],
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                if let Some(l) = logical {
                    let ran = Ran {
                        errored: true,
                        ..Ran::default()
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                // A table's definition changed while the statement waited
                // for the change: outside a transaction it is read again
                // against the new catalog and run once more.
                if e.protocol().mysql_errno == 1412
                    && !self.eng.retrying
                    && !self.eng.txn.is_open()
                    && let Some(t) = g.tables.first()
                    && let Some(newer) =
                        self.engine_newer_catalog(t.def.object, t.def.version).await
                {
                    self.eng.retrying = true;
                    let r =
                        Box::pin(self.engine_text_in(
                            cx, &newer, current_db, sql, logical, more, format, out,
                        ))
                        .await;
                    self.eng.retrying = false;
                    return r.map(|s| {
                        s.unwrap_or(Step {
                            errored: true,
                            status: Some(status),
                        })
                    });
                }
                let mut step = self.fail(out, &e).await?;
                // The transaction's state after the error.
                step.status = Some(status);
                Ok(step)
            }
        }
    }

    /// The prepare path of a general `SELECT`: checked and described; it
    /// is planned at each execute, with that execute's parameter values.
    /// (`async` like the seam's other prepare paths it sits beside.)
    #[allow(clippy::too_many_arguments, clippy::unused_async)]
    pub(super) async fn general_prepare(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        g: General,
        rewritten: &Bytes,
        logical: &Bytes,
        client: &Bytes,
        recheck: Option<crate::tenant::authz::Recheck>,
    ) -> Result<ExecSummary> {
        let checked = self.general_checks(cat, &g)?;
        let sv = self.engine_vars(cx);
        let described = engine::Engine::general_describe(&g, &sv.general)?;
        let Layout { defs, .. } = results::layout(&g, &described, sv.results);
        let params = u16::try_from(g.params).map_err(|_| {
            SprigError::standard(
                1390,
                *b"HY000",
                "Prepared statement contains too many placeholders",
            )
        })?;
        let Some(plan) = g.placeholder() else {
            return Err(SprigError::internal("engine: a statement without a table"));
        };
        let persona = self.persona;
        // MySQL types each `?` from its context.
        let param_types = if persona == sprig_proto_mysql::Persona::MySql84 {
            let named: Vec<engine::paramtype::Named<'_>> = g
                .tables
                .iter()
                .map(|t| engine::paramtype::Named {
                    aliases: &t.aliases,
                    def: &t.def,
                })
                .collect();
            engine::paramtype::infer(&g.stmt, &named)
        } else {
            Vec::new()
        };
        let mut st = Stmt::engine(
            rewritten.clone(),
            StmtInfo::classify(rewritten),
            params,
            defs.clone(),
        );
        st.logical = Some(logical.clone());
        st.recheck = recheck;
        st.client = Some(client.clone());
        st.engine = Some(Box::new(EnginePrepared {
            plan: Arc::new(plan),
            columns: defs.clone().into(),
            results: sv.results,
            checked,
            general: Some(Arc::new(g)),
        }));
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        Ok(ExecSummary {
            status: None,
            prepared: Some(PreparedMeta {
                stmt,
                params: (0..usize::from(params))
                    .map(|i| {
                        meta::param_def(persona, param_types.get(i).copied().flatten(), sv.results)
                    })
                    .collect(),
                columns: defs,
                warnings: 0,
            }),
            db: None,
        })
    }

    /// The execute path of a prepared general `SELECT`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn general_execute(
        &mut self,
        cx: &mut ExecCtx<'_>,
        stmt: StmtRef,
        g: Arc<General>,
        recheck: Option<crate::tenant::authz::Recheck>,
        logical: Option<Bytes>,
        raw: &StmtExecute,
        params: &ExecuteParams,
        out: &mut dyn ResultSink,
    ) -> Result<Option<ExecSummary>> {
        let persona = self.persona;
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
        // A table changed, or the session's privileges did (and with them,
        // perhaps, the row-level security predicates in the text): the
        // statement is bound and checked again from the client's text.
        let checked = {
            let s = self.stmts.get_mut(stmt)?;
            s.engine.as_ref().and_then(|e| e.checked.clone())
        };
        let now = self
            .tenant
            .as_deref()
            .map(|ts| crate::tenant::authz::privileges(ts, &cat));
        let same_privileges = match (&now, &checked) {
            // The same compiled privileges; or everything allowed then and
            // now (an internal principal's are made anew at each call).
            (Some(a), Some(b)) => Arc::ptr_eq(a, b) || (a.unrestricted() && b.unrestricted()),
            (None, None) => true,
            _ => false,
        };
        if !same_privileges || !current(&cat, &g) {
            if !Box::pin(self.reprepare_engine(cx, stmt)).await? {
                return Err(needs_reprepare());
            }
            return Box::pin(self.engine_execute(cx, stmt, raw, params, out)).await;
        }
        // Parameters, long data folded in.
        let mut vals: Vec<Datum> = Vec::with_capacity(g.params);
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
        let sv = self.engine_vars(cx);
        let pending = match &logical {
            Some(l) => self.wl_start_stmt_known(stmt, l, sv.current.as_deref(), sv.lex),
            None => None,
        };
        let r = self
            .general_run(
                cat.tenant.0,
                &g,
                &vals,
                &sv,
                std::sync::Arc::clone(cx.session.handle()),
            )
            .await;
        // Once per statement, after it: the engine may have ended the
        // transaction.
        let status = self.engine_status(&sv);
        let answered = if std::mem::take(&mut self.eng.metadata_changed) {
            status | StatusFlags::METADATA_CHANGED
        } else {
            status
        };
        let _ = cx.proto.has(Caps::FOUND_ROWS);
        match r {
            Ok(rows) => {
                self.general_answer(&g, &rows, RowFormat::Binary, answered, &sv, out)
                    .await?;
                if let Some(l) = &logical {
                    let oc = self.general_outcome(&rows, status);
                    let ran = Ran {
                        outcome: oc.as_ref(),
                        errored: false,
                        params: &[],
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
            }
            Err(e) => {
                if let Some(l) = &logical {
                    let ran = Ran {
                        errored: true,
                        ..Ran::default()
                    };
                    self.wl_finish(cx, pending, l, &ran);
                }
                if e.protocol().mysql_errno == 1412
                    && !self.eng.retrying
                    && !self.eng.txn.is_open()
                    && let Some(t) = g.tables.first()
                    && self
                        .engine_newer_catalog(t.def.object, t.def.version)
                        .await
                        .is_some()
                {
                    self.eng.retrying = true;
                    let r = Box::pin(self.engine_execute(cx, stmt, raw, params, out)).await;
                    self.eng.retrying = false;
                    return r;
                }
                self.fail(out, &e).await?;
            }
        }
        Ok(Some(ExecSummary {
            status: Some(status),
            prepared: None,
            db: None,
        }))
    }
}
