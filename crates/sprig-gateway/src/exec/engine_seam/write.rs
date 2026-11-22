//! Multi-table and subquery `UPDATE`/`DELETE` on engine tables
//! ([`crate::engine::multiwrite`]): checked, run in the session's
//! transaction (or one of its own), answered.
//!
//! ```text
//! text:     engine_bind → Bound::Write → write_text
//! prepare:  engine_bind → Bound::Write → kept as text, run with its
//!           values written in at each execute
//! ```

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use sprig_catalog::TenantCatalog;
use sprig_common::Result;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::SessVars;
use super::engine_in_other_txn;
use crate::engine::exec::Outcome;
use crate::engine::general::RunArgs;
use crate::engine::meta::{self, Results};
use crate::engine::multiwrite::MultiWrite;
use crate::engine::session::{self, SessionTxn, Start, XaPhase};
use crate::engine::{self};
use crate::exec::PassSession;
use crate::exec::capture::Ran;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ExecSummary, PreparedMeta, ResultSink};

impl PassSession {
    /// Runs `w`: as a statement of the session's engine transaction
    /// (opened now when `autocommit` is off), or in a transaction of its
    /// own, committed when it succeeds.
    async fn write_run(
        &mut self,
        tenant: u64,
        w: &MultiWrite,
        sv: &SessVars,
        found_rows: bool,
        stop: Arc<crate::session::SessionHandle>,
    ) -> Result<Outcome> {
        if self.backend_in_txn() {
            return Err(engine_in_other_txn());
        }
        let eng = engine::engine()?;
        if let Some(ph) = self.eng.txn.xa_phase()
            && ph != XaPhase::Active
        {
            return Err(Self::xa_refusal(ph, sv.mysql));
        }
        let timed_out: fn() -> sprig_common::SprigError = if sv.mysql {
            sprig_engine::txn::errors::statement_timeout_mysql
        } else {
            sprig_engine::txn::errors::statement_timeout_mariadb
        };
        // MySQL's statement time limit is for `SELECT` only.
        let limit = sv.time_limit.filter(|_| !sv.mysql);
        let args = RunArgs {
            tenant,
            params: &[],
            seed: &sv.general,
            deadline: limit.map(|d| (Instant::now() + d, timed_out)),
            stop: Some(stop),
        };
        if !self.eng.txn.is_open() {
            if !sv.autocommit_off {
                if sv.read_only {
                    return Err(session::read_only_transaction(sv.mysql));
                }
                // Its own transaction: committed with the statement.
                let mut own = SessionTxn::default();
                own.begin(eng, sv.opts, false, Start::default())?;
                if let Some(tx) = own.open_tx() {
                    tx.set_env(sv.env);
                }
                let r = eng.multi_write_in(w, &args, &mut own, found_rows).await;
                own.statement_done();
                return match r {
                    Ok(o) => own.commit().await.map(|_| o),
                    Err(err) => {
                        own.rollback();
                        Err(err)
                    }
                };
            }
            self.eng
                .txn
                .begin(eng, sv.opts, sv.read_only, Start::default())?;
        }
        if self.eng.txn.read_only() {
            return Err(session::read_only_transaction(sv.mysql));
        }
        if let Some(tx) = self.eng.txn.open_tx() {
            tx.set_env(sv.env);
        }
        let r = eng
            .multi_write_in(w, &args, &mut self.eng.txn, found_rows)
            .await;
        self.eng.txn.statement_done();
        r
    }

    /// The text-protocol path of a multi-table or subquery write: checked,
    /// run, answered (or refused with its error).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        mut w: MultiWrite,
        logical: Option<&[u8]>,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        // Privileges: what the statement reads, and each target's write
        // (with its row-security check).
        if let Err(e) = self.general_checks(cat, &w.select) {
            return self.fail(out, &e).await;
        }
        for i in 0..w.targets.len() {
            match self.engine_row_check(cx, cat, &w.targets[i].plan).await {
                Ok(c) => w.targets[i].plan.check = c,
                Err(e) => return self.fail(out, &e).await,
            }
            if let Err(e) = self.engine_checks(cat, &w.targets[i].plan, None) {
                return self.fail(out, &e).await;
            }
        }
        let sv = self.engine_vars(cx);
        let pending = logical.and_then(|l| self.wl_start(l));
        let found_rows = cx.proto.has(sprig_proto_mysql::Caps::FOUND_ROWS);
        let r = self
            .write_run(
                cat.tenant.0,
                &w,
                &sv,
                found_rows,
                Arc::clone(cx.session.handle()),
            )
            .await;
        let mut status = self.engine_status(&sv);
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        match r {
            Ok(o) => {
                self.engine_diag(&o.conds);
                // A table read whole says so, as the servers do.
                let mut ok = OkPacket::new(if o.no_index {
                    status | StatusFlags::NO_INDEX_USED
                } else {
                    status
                });
                ok.affected_rows = o.affected;
                ok.warnings = o.warnings();
                if let Some(i) = &o.info {
                    ok.info = Bytes::from(i.clone());
                }
                out.ok(ok).await?;
                if let Some(l) = logical {
                    let ran = Ran {
                        outcome: None,
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
                let mut step = self.fail(out, &e).await?;
                step.status = Some(status);
                Ok(step)
            }
        }
    }

    /// A multi-table or subquery write prepared: kept as the client's
    /// text, run at each execute with the bound values written in.
    pub(super) fn write_prepare(
        &mut self,
        client: &Bytes,
        results: Results,
        types: &[Option<crate::engine::table::ColType>],
    ) -> ExecSummary {
        let (lex, _) = self.lex_opts();
        let n = sprig_sql::lex::lex(client, &lex)
            .map(|t| {
                t.iter()
                    .filter(|t| t.kind == sprig_sql::lex::TokenKind::Param)
                    .count()
            })
            .unwrap_or(0);
        let n = u16::try_from(n).unwrap_or(u16::MAX);
        let mut st = crate::exec::stmt::Stmt::deferred(client.clone(), Vec::new());
        st.params = n;
        st.deferred_params = true;
        st.client = Some(client.clone());
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        ExecSummary {
            status: None,
            prepared: Some(PreparedMeta {
                stmt,
                params: (0..usize::from(n))
                    .map(|i| {
                        meta::param_def(self.persona, types.get(i).copied().flatten(), results)
                    })
                    .collect(),
                columns: Vec::new(),
                warnings: 0,
            }),
            db: None,
        }
    }
}
