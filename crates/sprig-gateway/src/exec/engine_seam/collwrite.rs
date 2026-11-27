//! SQL writes to MongoDB collections ([`crate::engine::collwrite`]):
//! checked, run in the session's transaction (or one of its own), answered.
//!
//! ```text
//! text:     engine_bind → Bound::CollWrite → coll_write_text
//! prepare:  engine_bind → Bound::CollWrite → kept as text, run with its
//!           values written in at each execute (as multi-table writes)
//! ```
//!
//! Privileges: a collection has no SQL grants of its own, so the write
//! needs its privilege (`INSERT`, `UPDATE`, `DELETE`; `REPLACE` both
//! `INSERT` and `DELETE`) on the collection's database or wider, and its
//! reads need `SELECT` as any query's do.
//!
//! `doc.sql_writes = off` (tenant, database or the collection's object
//! scope) makes collections read-only to SQL: such a write answers 39661.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use sprig_auth::authz::account_sql::table_denied;
use sprig_auth::authz::{CompiledPrivileges, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::Result;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::SessVars;
use super::engine_in_other_txn;
use crate::engine::collwrite::CollWrite;
use crate::engine::exec::Outcome;
use crate::engine::general::RunArgs;
use crate::engine::session::{self, SessionTxn, Start, XaPhase};
use crate::engine::{self};
use crate::exec::PassSession;
use crate::exec::capture::Ran;
use crate::exec::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink};

/// `doc.sql_writes`: whether SQL may write MongoDB collections.
pub(super) static SQL_WRITES: sprig_settings::Setting<std::borrow::Cow<'static, str>> =
    sprig_settings::Setting {
        key: "doc.sql_writes",
        default: std::borrow::Cow::Borrowed("on"),
        scopes: sprig_settings::Scopes::DATA,
        hard: sprig_settings::HardBounds::one_of(&["on", "off"]),
        apply: sprig_settings::Apply::Live,
        operator_only: false,
        owner: 31,
        doc: "SQL writes to MongoDB collections: off makes them read-only to SQL (39661)",
    };
sprig_settings::register!(SQL_WRITES);

/// 39661: `doc.sql_writes` is `off` where the collection is.
fn collection_read_only(db: &str, coll: &str) -> sprig_common::SprigError {
    sprig_common::SprigError::new(
        sprig_common::SprigCode::CollectionReadOnly,
        format!("MongoDB collection '{db}.{coll}' is read-only to SQL here (doc.sql_writes = off)"),
    )
}

impl PassSession {
    /// Whether `doc.sql_writes` lets SQL write `w`'s collection: the value
    /// at the collection's object, its database or the tenant.
    fn sql_writes_on(&self, cat: &TenantCatalog, w: &CollWrite) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return true;
        };
        let svc = ts.services();
        let ctx = cat
            .setting_ctx(&svc.dc)
            .with_db(w.db)
            .with_object(w.coll.def.object);
        svc.settings.get(&SQL_WRITES, &ctx, Some(&cat.settings)) != "off"
    }

    /// The write's privileges: its reads as a query's, its write on the
    /// collection's database.
    fn coll_write_checks(&self, cat: &TenantCatalog, w: &CollWrite) -> Result<()> {
        self.general_checks(cat, &w.select)?;
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(());
        };
        let p = crate::tenant::authz::privileges(ts, cat);
        if p.unrestricted() {
            return Ok(());
        }
        let Some(cp) = p.as_any().downcast_ref::<CompiledPrivileges>() else {
            return Ok(());
        };
        let db_name = &w.coll.db;
        let db_key = database_key(db_name, ts.lower_case_table_names());
        let target = Target::Object {
            db_key: &db_key,
            db: w.db,
            object: w.coll.def.object,
        };
        let held = cp.actions_on(target);
        for a in w.actions() {
            if !held.has(*a) {
                return Err(table_denied(
                    self.persona,
                    a.command(),
                    &ts.principal.user,
                    &ts.principal.client_ip.to_string(),
                    db_name,
                    &w.coll.name,
                ));
            }
        }
        Ok(())
    }

    /// Runs `w`: as a statement of the session's engine transaction
    /// (opened now when `autocommit` is off), or in a transaction of its
    /// own, committed when it succeeds.
    async fn coll_write_run(
        &mut self,
        tenant: u64,
        w: &CollWrite,
        sv: &SessVars,
        found_rows: bool,
        stop: Arc<crate::session::SessionHandle>,
    ) -> Result<Outcome> {
        if self.backend_in_txn() {
            return Err(engine_in_other_txn());
        }
        let eng = engine::engine()?;
        // Row-level security: the document store checks each new document
        // against the session's policies on the collection (`WITH CHECK`).
        let guard = <Self as crate::pipeline::SessionExec>::row_guard(self);
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
                let r = sprig_doc::docstore::guard::scope(
                    guard,
                    eng.coll_write_in(w, &args, &mut own, found_rows),
                )
                .await;
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
        let r = sprig_doc::docstore::guard::scope(
            guard,
            eng.coll_write_in(w, &args, &mut self.eng.txn, found_rows),
        )
        .await;
        self.eng.txn.statement_done();
        r
    }

    /// The text-protocol path of an SQL write of a MongoDB collection:
    /// checked, run, answered (or refused with its error).
    pub(super) async fn coll_write_text(
        &mut self,
        cx: &mut ExecCtx<'_>,
        cat: &Arc<TenantCatalog>,
        w: CollWrite,
        logical: Option<&[u8]>,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        if let Err(e) = self.coll_write_checks(cat, &w) {
            return self.fail(out, &e).await;
        }
        if !self.sql_writes_on(cat, &w) {
            let e = collection_read_only(&w.coll.db, &w.coll.name);
            return self.fail(out, &e).await;
        }
        // A table written beside the collection runs without its row
        // triggers here: one that has some is refused.
        let triggered = crate::engine::catalog::of(cat).is_some_and(|eng| {
            w.tables.iter().any(|t| {
                eng.triggers
                    .values()
                    .any(|g| g.table == t.plan.table.object)
            })
        });
        if triggered {
            let e = crate::engine::not_on_engine(
                "a table with triggers written in one statement with a MongoDB collection",
            );
            return self.fail(out, &e).await;
        }
        let sv = self.engine_vars(cx);
        let pending = logical.and_then(|l| self.wl_start(l));
        let found_rows = cx.proto.has(sprig_proto_mysql::Caps::FOUND_ROWS);
        let r = self
            .coll_write_run(
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
}

impl PassSession {
    /// `SessionExec::lend_engine_txn`: a MongoDB transaction's engine
    /// transaction becomes the session's open transaction for its SQL
    /// statements on tables, and is taken back afterwards.
    pub(crate) fn engine_txn_loan(
        &mut self,
        txn: Option<sprig_engine::txn::Txn>,
    ) -> Option<sprig_engine::txn::Txn> {
        let Some(t) = txn else {
            return self
                .eng
                .txn
                .take_lent()
                .map(crate::engine::store::Tx::into_lent);
        };
        let Ok(eng) = engine::engine() else {
            return Some(t);
        };
        if self.backend_in_txn() {
            return Some(t);
        }
        self.eng
            .txn
            .adopt_lent(crate::engine::store::Tx::lent(eng, t))
            .map(crate::engine::store::Tx::into_lent)
    }
}
