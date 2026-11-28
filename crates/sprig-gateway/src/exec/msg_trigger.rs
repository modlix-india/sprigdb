//! Triggers on queues in a tenant session.
//!
//! **Declaring.** `CREATE [OR REPLACE] TRIGGER g ON QUEUE [db.]q`,
//! `LANGUAGE javascript | lua | kirun AS ...`: the queue must exist and the
//! deploying user needs `SELECT` and `DELETE` on the queue's vhost and
//! `CREATE ROUTINE` on the trigger's database.
//! [`sprig_logic::messages::deploy`] records it, and the index entry that
//! makes the controller start its job is written. `ON STREAM` is
//! [`super::stream_trigger`]'s (streams are stored on the engine).
//!
//! **Running.** A worker sends each run to a gateway over the peer service
//! ([`crate::triggers`]), which opens a session as the trigger's definer
//! and runs one marked statement in it, `/* sprig message trigger <nonce>
//! */ <RunRequest as JSON>`. The nonce is this process's own, so no client
//! can send the statement. [`PassSession::tenant_msg_trigger`] answers it
//! with one row, the [`RunReply`] as JSON:
//!
//! ```text
//! queue q:
//!   mq_get(q, 1, lease) as the definer           -- none: Empty
//!   START TRANSACTION; hold the delivery; DELETE its row (mq_ack's path)
//!   the version with ctx.message
//!   COMMIT; the owner settles it
//!   failure: ROLLBACK; the delivery returns as a failed one (its delivery
//!   count rises), or, at logic.trigger_max_attempts failures, a
//!   logic_dead_letters row and a reject (the queue's dead-letter exchange
//!   gets it when there is one)
//! dead letter d replayed:
//!   START TRANSACTION; mark d in the replay group (marked: Moved)
//!   the version with the dead letter's event; COMMIT
//! ```

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::declare::{Declaration, SourceKind};
use sprig_logic::engine::bridge::DbOp;
use sprig_logic::engine::kirun::Host;
use sprig_logic::engine::outbound::HttpOutbound;
use sprig_logic::messages::{self, MsgIndex, RunOutcome, RunReply, RunRequest};
use sprig_proto_mysql::StatusFlags;
use tokio::sync::{mpsc, oneshot};

use super::PassSession;
use super::logic::{ChannelDb, DbRequest};
use super::tenant::Step;
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};

/// A trigger run's `db`: the session's, keeping the code's log lines
/// (`console.log`, `ctx.log`) for the run log (`logic_log`), at most
/// [`sprig_logic::engine::engine::MAX_LOG_LINES`] a run.
struct LoggedDb {
    db: ChannelDb,
    lines: Arc<std::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl sprig_logic::engine::bridge::DbBridge for LoggedDb {
    async fn exec(&self, op: DbOp) -> Result<sprig_logic::engine::bridge::Reply> {
        self.db.exec(op).await
    }

    fn log(&self, line: &str) {
        let mut v = self
            .lines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if v.len() < sprig_logic::engine::engine::MAX_LOG_LINES {
            v.push(line.to_owned());
        }
    }
}

/// Where a compiled trigger keeps its log lines.
const LOG_VAR: &str = sprig_logic::compiler::plsql::LOG_VAR;

/// The marked statement's start.
pub(crate) const MARK: &str = "/* sprig message trigger ";

/// How long a trigger's rollout is cached.
const ROLLOUT_TTL: Duration = Duration::from_secs(5);

type RolloutCache = std::collections::HashMap<
    (u64, u64),
    (std::time::Instant, Option<sprig_logic::rollout::Rollout>),
>;

/// Triggers' rollouts by tenant and module (`None`: none), read at most
/// every [`ROLLOUT_TTL`].
static ROLLOUTS: std::sync::LazyLock<std::sync::Mutex<RolloutCache>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Forgets a trigger's cached rollout (after `ALTER TRIGGER ... ROLLOUT`
/// through this gateway).
pub(super) fn forget_rollout(tenant: u64, module: u64) {
    if let Ok(mut m) = ROLLOUTS.lock() {
        m.remove(&(tenant, module));
    }
}

/// This process's nonce: only statements carrying it are runs.
#[must_use]
pub(crate) fn nonce() -> &'static str {
    static N: OnceLock<String> = OnceLock::new();
    N.get_or_init(|| {
        format!(
            "{}{}",
            uuid::Uuid::now_v7().simple(),
            uuid::Uuid::now_v7().simple()
        )
    })
}

/// The marked statement of `req`.
///
/// # Errors
/// Internal when `req` doesn't serialize.
pub(crate) fn statement(req: &RunRequest) -> Result<String> {
    let j = serde_json::to_string(req)
        .map_err(|e| SprigError::internal(format!("a trigger run: {e}")))?;
    Ok(format!("{MARK}{} */ {j}", nonce()))
}

fn denied(ts: &crate::tenant::session::TenantSession, what: &str, db: &str, t: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{what} command denied to user '{}'@'{}' for table `{db}`.`{t}`",
            ts.principal.user, ts.principal.host
        ),
    )
}

impl PassSession {
    /// `CREATE TRIGGER ... ON STREAM | ON QUEUE` (see the module docs).
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_message(
        &mut self,
        cx: &mut ExecCtx<'_>,
        decl: Declaration,
    ) -> Result<Vec<String>> {
        let _ = cx;
        let src = decl
            .source
            .clone()
            .ok_or_else(|| SprigError::internal("no source"))?;
        // Streams are stored on the engine (`exec::stream_trigger`).
        if src.kind == SourceKind::Stream {
            return Box::pin(self.logic_deploy_stream_trigger(decl)).await;
        }
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = decl
            .db
            .clone()
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let cat = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let row = cat
            .database(&database_key(&db, lctn))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let tenant_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let grants = crate::tenant::authz::privileges(ts, &cat);
        let allows = |a: sprig_auth::authz::Action, db_name: &str, name: &str| {
            let db_key = database_key(db_name, lctn);
            let name_key = object_key(ObjectKind::Table, name, lctn);
            grants.unrestricted()
                || grants.allows(
                    a,
                    sprig_auth::authz::Target::Named {
                        db_key: &db_key,
                        name_key: &name_key,
                    },
                )
                || grants.allows(a, sprig_auth::authz::Target::Database { db_key: &db_key })
        };
        // Code: CREATE ROUTINE on the trigger's database.
        {
            let db_key = database_key(&db, lctn);
            if !grants.unrestricted()
                && !grants.allows(
                    sprig_auth::authz::Action::CreateRoutine,
                    sprig_auth::authz::Target::Database { db_key: &db_key },
                )
            {
                return Err(SprigError::standard(
                    1044,
                    *b"42000",
                    format!(
                        "Access denied for user '{}' to database '{db}'",
                        ts.principal.user
                    ),
                ));
            }
        }
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let ep = svc.locator.writer(tenant).await?;
        let source_db = src.db.clone().unwrap_or_else(|| db.clone());
        let (source_db_row, object_id) = match src.kind {
            SourceKind::Stream => {
                return Err(SprigError::internal("a stream trigger's deploy came here"));
            }
            SourceKind::Queue => {
                let r = cat
                    .database(&database_key(&source_db, lctn))
                    .cloned()
                    .ok_or_else(|| {
                        SprigError::standard(
                            1049,
                            *b"42000",
                            format!("Unknown database '{source_db}'"),
                        )
                    })?;
                if !allows(sprig_auth::authz::Action::Select, &source_db, &src.name)
                    || !allows(sprig_auth::authz::Action::Delete, &source_db, &src.name)
                {
                    return Err(denied(ts, "SELECT and DELETE", &source_db, &src.name));
                }
                // Durable queues are the catalog's (the snapshot's `mq`
                // module).
                let id: Option<u64> = cat
                    .ext
                    .get::<sprig_queues::catalog::MqState>(sprig_queues::catalog::MODULE)
                    .and_then(|m| m.dbs.get(&r.db_id.0))
                    .and_then(|t| t.queues.get(&src.name))
                    .map(|q| q.id);
                let id = id.ok_or_else(|| {
                    SprigError::new(
                        SprigCode::LogicNotFound,
                        format!(
                            "no durable queue '{}' in vhost {source_db} (triggers read durable queues)",
                            src.name
                        ),
                    )
                })?;
                (r, id)
            }
        };
        // The compiled tier's routines go on every shard of the trigger's
        // database, through `sprig_ctl`, with the definer as `DEFINER`.
        let streams = sprig_logic::service::StreamList::new();
        let template = ts.schema_of(Some(&db));
        let shard_list: Vec<(String, sprig_common::ids::ClusterId)> = {
            match template {
                Some(t) => vec![(t, self.target.cluster.clone())],
                None => Vec::new(),
            }
        };
        let single_shard = shard_list.len() == 1;
        let mut shards = super::logic_bound::CtlShards {
            list: shard_list,
            conns: std::collections::HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let account = sprig_auth::authz::backend::account(tenant, user);
        let compile = (!shards.list.is_empty()).then(|| sprig_logic::messages::MsgCompile {
            shards: &mut shards,
            single_shard,
            definer: format!("`{}`@`%`", account.replace('`', "``")),
            db_name: &row.name,
            streams,
            max_loop_iterations: 100_000,
            compile_max_cost: 100_000,
        });
        // Taken after the chain's loads above and given back before the
        // fan-in attach below, which take catalog connections of their own
        // from the same pool (4 per cluster).
        let mut conn = svc.records.begin(tenant).await?;
        let deployed = sprig_logic::messages::deploy(
            &decl,
            sprig_logic::messages::MsgTarget {
                catalog: &mut *conn,
                catalog_schema: ep.store.schema(),
                catalog_version: cat.version,
                db_id: row.db_id.0,
                user_id: user.0,
                tenant_name: &tenant_name,
                source_db: (source_db_row.db_id.0, &source_db_row.name),
                object_id,
                fanin_declarer: 0,
                compile,
            },
        )
        .await?;
        conn.commit().await?;
        shards.release();
        let notes = deployed.notes();
        messages::put_index(
            svc.locator.dc_store().as_ref(),
            &MsgIndex {
                tenant,
                binding: deployed.binding_id,
                kind: src.kind,
            },
        )
        .await?;
        tracing::info!(%tenant, binding = deployed.binding_id, kind = src.kind.as_str(), name = %decl.name, "trigger on a stream or queue deployed");
        Ok(notes)
    }

    /// The marked statement of a run (see the module docs): `None` for any
    /// other statement.
    pub(super) async fn tenant_msg_trigger(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if !text.starts_with(MARK.as_bytes()) || self.tenant.is_none() {
            return Ok(None);
        }
        let s = String::from_utf8_lossy(&text[MARK.len()..]).into_owned();
        let parsed = s
            .split_once(" */ ")
            .filter(|(n, _)| *n == nonce())
            .and_then(|(_, j)| serde_json::from_str::<RunRequest>(j).ok());
        let Some(req) = parsed else {
            let e = SprigError::standard(
                1064,
                *b"42000",
                "You have an error in your SQL syntax: a trigger run isn't a client statement",
            );
            return Ok(Some(self.fail(out, &e).await?));
        };
        let reply = if let Some(rp) = &req.replay {
            // A dead letter's replay.
            self.run_replay(cx, &req, rp).await
        } else if super::change_trigger::is_change_source(&req.source) {
            // An after-commit trigger on an engine table.
            self.run_change_trigger(cx, &req).await
        } else if req.source.kind == SourceKind::Stream {
            // A trigger on an engine stream.
            self.run_stream_trigger(cx, &req).await
        } else {
            self.run_queue_trigger(cx, &req).await
        };
        // An inherited trigger lacking a privilege waits
        // for the grant (39242) instead of failing its records.
        let reply = match self.tenant.as_deref() {
            Some(ts) => {
                crate::inherited::triggers::on_run(
                    ts.services(),
                    ts.tenant,
                    &ts.principal.user,
                    req.source.binding_id,
                    reply,
                )
                .await
            }
            None => reply,
        };
        if !matches!(
            reply.outcome,
            RunOutcome::Ok | RunOutcome::Empty | RunOutcome::Moved
        ) {
            tracing::debug!(binding = req.source.binding_id, code = reply.code, message = %reply.message, "trigger run failed");
        }
        let body = serde_json::to_string(&reply).unwrap_or_default();
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let rows = sprig_types::Rows {
            columns: vec![crate::pipeline::text_column("reply", cx.proto.collation)],
            rows: vec![vec![Some(body.into_bytes())]],
        };
        super::types::send_rows(out, &rows, RowFormat::Text, status).await?;
        Ok(Some(Step {
            errored: false,
            status: Some(status),
        }))
    }

    /// One statement through this session; its error returned.
    pub(super) async fn trigger_stmt(&mut self, cx: &mut ExecCtx<'_>, sql: &str) -> Result<()> {
        self.run_checked(cx, sql, false).await.map(|_| ())
    }

    pub(super) async fn rollback_quietly(&mut self, cx: &mut ExecCtx<'_>) {
        if let Err(e) = self.trigger_stmt(cx, "ROLLBACK").await {
            tracing::debug!(error = %e, "trigger run: ROLLBACK failed");
        }
    }

    /// Loads the version and runs it in this session with `event` as
    /// `ctx.record` or `ctx.message`: the version's `db` calls go through
    /// this session, in its transaction.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn run_version(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        field: &str,
        event: Value,
    ) -> (Result<()>, Vec<String>) {
        let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
        let r = self
            .run_version_logged(cx, req, field, event, Arc::clone(&lines))
            .await;
        let logs = std::mem::take(
            &mut *lines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (r, logs)
    }

    /// [`Self::run_version`], keeping the code's log lines in `lines`.
    #[allow(clippy::too_many_lines)]
    async fn run_version_logged(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        field: &str,
        event: Value,
        lines: Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Result<()> {
        let (svc, tenant, cat, user) = {
            let ts = self
                .tenant
                .as_deref_mut()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let cat = match ts.catalog() {
                Some(c) => Arc::clone(c),
                None => ts.refresh().await?,
            };
            (
                Arc::clone(ts.services()),
                ts.tenant,
                cat,
                ts.principal.user.clone(),
            )
        };
        let v = if let Some(v) =
            sprig_logic::invoke::cached(tenant.0, req.source.module_id, req.version)
        {
            v
        } else {
            let mut conn = svc.records.begin(tenant).await?;
            sprig_logic::invoke::load_any(&mut *conn, tenant.0, req.source.module_id, req.version)
                .await?
        };
        // A compiled version runs in MariaDB; a record it hands back (39876)
        // runs below, in the engine tier.
        if v.tier == "mariadb"
            && let Some(entry) = messages::CompiledEntry::of(&v.signature)
            && let Some(db) = cat.databases.get(sprig_common::ids::DbId(v.db_id))
        {
            let db = db.name.clone();
            if self
                .run_compiled(cx, req, &entry, &db, &event, &lines)
                .await?
            {
                return Ok(());
            }
        }
        let program = v.program.as_ref().ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicEngineUnavailable,
                format!("{}: the {} runtime isn't built", v.name, v.runtime),
            )
        })?;
        let db_row = cat.databases.get(sprig_common::ids::DbId(v.db_id)).cloned();
        let mut sctx = cat.setting_ctx(&svc.dc);
        if let Some(r) = &db_row {
            sctx = sctx.with_db(r.db_id);
        }
        let (mut limits, policy) =
            sprig_logic::settings::limits(&svc.settings, &sctx, Some(&cat.settings));
        limits.wall = Duration::from_millis(req.budget_ms.max(1));
        limits.wall_setting = "logic.trigger_budget_ms";
        let mut ctx = json!({
            "user": user,
            "tenant": tenant.0,
            "database": db_row.as_ref().map(|r| r.name.clone()),
            "function": v.name.clone(),
            "version": v.version,
            "trigger": {"binding": req.source.binding_id, "kind": req.source.kind.as_str()},
        });
        ctx[field] = event.clone();
        let params: Vec<String> = match program {
            sprig_logic::engine::Program::Kirun(p) => p.params().to_vec(),
            _ => Vec::new(),
        };
        let args = if super::change_trigger::is_change_source(&req.source) {
            super::change_trigger::args(&v.runtime, &params, &v.signature, &event, &mut ctx)
        } else {
            sprig_logic::messages::args_for(&v.runtime, &params, &event, &ctx)
        };
        let wire = Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let (tx, mut rx) = mpsc::channel::<DbRequest>(1);
        let host = Host {
            db: Arc::new(LoggedDb {
                db: ChannelDb(tx),
                lines,
            }),
            outbound: Arc::new(HttpOutbound::new(policy)),
            default_db: db_row.map(|r| r.name.clone()),
            info: ctx,
        };
        let run = program.run(args, host, limits);
        tokio::pin!(run);
        // A statement the definer may not run fails the
        // run with that error, whatever the code did with it, so an
        // inherited trigger can wait for the grant (39242).
        let mut denied: Option<SprigError> = None;
        let outcome = loop {
            tokio::select! {
                biased;
                r = &mut run => break r,
                Some((op, reply)) = rx.recv() => {
                    let op: DbOp = op;
                    let r: oneshot::Sender<Result<sprig_logic::engine::bridge::Reply>> = reply;
                    let x = self.engine_db(cx, op, wire).await;
                    if let Err(e) = &x
                        && crate::inherited::triggers::is_privilege_error(e)
                    {
                        denied.get_or_insert_with(|| SprigError {
                            kind: e.kind,
                            message: e.message.clone(),
                            retryable: e.retryable,
                            source: None,
                        });
                    }
                    let _ = r.send(x);
                }
            }
        };
        if let Some(e) = denied {
            return Err(e);
        }
        let outcome = outcome?;
        // An `error` or `reject` event fails the run, as for after-commit
        // triggers (39865).
        if matches!(outcome.event.as_str(), "error" | "reject") {
            let msg = outcome
                .result
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(|| outcome.result.to_string(), str::to_owned);
            return Err(SprigError::new(
                SprigCode::LogicUserError,
                format!("{}: {msg}", outcome.event),
            ));
        }
        Ok(())
    }

    /// The trigger's rollout, if it has one (cached).
    async fn trigger_rollout(&mut self, module: u64) -> Option<sprig_logic::rollout::Rollout> {
        let ts = self.tenant.as_deref()?;
        let tenant = ts.tenant.0;
        if let Ok(m) = ROLLOUTS.lock()
            && let Some((at, r)) = m.get(&(tenant, module))
            && at.elapsed() < ROLLOUT_TTL
        {
            return r.clone();
        }
        let svc = Arc::clone(ts.services());
        let mut conn = svc.records.begin(ts.tenant).await.ok()?;
        let r = sprig_logic::rollout::get(&mut *conn, module).await.ok()?;
        if let Ok(mut m) = ROLLOUTS.lock() {
            m.insert((tenant, module), (std::time::Instant::now(), r.clone()));
        }
        r
    }

    /// The version a run of `event` takes: the worker's (the active one),
    /// or, in a rollout, the candidate when the event's key falls in its
    /// share ([`sprig_logic::rollout::key_picks_candidate`]). With the
    /// module when it is in a rollout, so the run is counted.
    pub(super) async fn pick_version(
        &mut self,
        req: &RunRequest,
        event: &Value,
        message_id: Option<&str>,
    ) -> (RunRequest, Option<u64>) {
        let module = req.source.module_id;
        let Some(r) = self.trigger_rollout(module).await else {
            return (req.clone(), None);
        };
        if r.active_version != req.version {
            return (req.clone(), None);
        }
        let key = messages::rollout_key(event, message_id);
        let mut picked = req.clone();
        if sprig_logic::rollout::key_picks_candidate(r.spec(), key.as_deref(), module) {
            picked.version = r.candidate_version;
        }
        (picked, Some(module))
    }

    /// Counts a run of a trigger in a rollout (the automatic rollback
    /// reads the numbers).
    pub(super) async fn count_run(
        &mut self,
        cx: &mut ExecCtx<'_>,
        counted: Option<u64>,
        req: &RunRequest,
        started: std::time::Instant,
        reply: &RunReply,
    ) {
        if let Some(module) = counted {
            let failed =
                matches!(reply.outcome, RunOutcome::Failed | RunOutcome::Dead) && !reply.transient;
            self.rollout_count(cx, module, req.version, started.elapsed(), failed)
                .await;
        }
    }

    /// A compiled trigger version, in this session's transaction: its
    /// entry procedure with the event's fields, after a savepoint, under
    /// `logic.trigger_budget_ms`. `Ok(false)` when the code handed the
    /// record back (39876): the savepoint is rolled back and the caller
    /// runs it in the engine tier.
    async fn run_compiled(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        entry: &messages::CompiledEntry,
        db: &str,
        event: &Value,
        lines: &Arc<std::sync::Mutex<Vec<String>>>,
    ) -> Result<bool> {
        const SAVEPOINT: &str = "__sprig_compiled";
        self.trigger_stmt(cx, &format!("SAVEPOINT {SAVEPOINT}"))
            .await?;
        let call = entry.call(db, req.source.module_id, req.version, event);
        let budget = req.budget_ms.max(1);
        let timed = format!(
            "SET STATEMENT max_statement_time = {} FOR {call}",
            Duration::from_millis(budget).as_secs_f64()
        );
        // The entry procedure's name is reserved (`__sprig_`): only the
        // gateway's own statements may name it.
        if let Err(e) = self.run_checked(cx, &timed, true).await.map(|_| ()) {
            let n = messages::error_number(&e);
            if n == messages::ENGINE_RERUN {
                tracing::debug!(binding = req.source.binding_id, reason = %e.message, "compiled trigger: this record runs in the engine tier");
                self.trigger_stmt(cx, &format!("ROLLBACK TO SAVEPOINT {SAVEPOINT}"))
                    .await?;
                return Ok(false);
            }
            // The lines written before the error, as the engine tier
            // keeps a failed run's: the entry keeps them in the variable as
            // it goes, emptied when it starts.
            if entry.log
                && let Ok(mut got) = self
                    .run_checked(cx, &format!("SELECT {LOG_VAR}"), false)
                    .await
                && let Some(Some(l)) = got.rows.pop().and_then(|r| r.into_iter().next())
            {
                let logged: Vec<String> = serde_json::from_slice(&l).unwrap_or_default();
                let mut v = lines
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                v.extend(logged);
                v.truncate(sprig_logic::engine::engine::MAX_LOG_LINES);
            }
            if matches!(n, 1969 | 3024) {
                return Err(SprigError::new(
                    SprigCode::LogicBudgetExceeded,
                    format!("the run ran past logic.trigger_budget_ms = {budget} ms"),
                ));
            }
            return Err(e);
        }
        let read = if entry.log {
            "SELECT @__sprig_e, @__sprig_r, @__sprig_l"
        } else {
            "SELECT @__sprig_e, @__sprig_r"
        };
        let mut got = self.run_checked(cx, read, false).await?;
        let row = got.rows.pop().unwrap_or_default();
        let text = |i: usize| {
            row.get(i)
                .cloned()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
        };
        if let Some(l) = text(2) {
            let logged: Vec<String> = serde_json::from_str(&l).unwrap_or_default();
            let mut v = lines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            v.extend(logged);
            v.truncate(sprig_logic::engine::engine::MAX_LOG_LINES);
        }
        let ev = text(0).unwrap_or_else(|| "output".to_owned());
        // An `error` or `reject` event fails the run, as in the engine
        // tier (39865).
        if matches!(ev.as_str(), "error" | "reject") {
            let result: Value = text(1)
                .and_then(|r| serde_json::from_str(&r).ok())
                .unwrap_or(Value::Null);
            let msg = result
                .get("message")
                .and_then(Value::as_str)
                .map_or_else(|| result.to_string(), str::to_owned);
            return Err(SprigError::new(
                SprigCode::LogicUserError,
                format!("{ev}: {msg}"),
            ));
        }
        Ok(true)
    }

    /// A dead letter's replay: its event with the active version, in
    /// a transaction that moves its marker in the replay group from 0 to 1
    /// (39877 when a repeat finds it moved: `Moved`), outside the trigger's
    /// own position.
    async fn run_replay(
        &mut self,
        cx: &mut ExecCtx<'_>,
        req: &RunRequest,
        rp: &messages::ReplayRun,
    ) -> RunReply {
        if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
            return RunReply::failed(&e);
        }
        match self
            .replay_mark(&messages::replay_group(req.source.binding_id), rp.dl_id)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                self.rollback_quietly(cx).await;
                return RunReply::of(RunOutcome::Moved);
            }
            Err(e) => {
                self.rollback_quietly(cx).await;
                return RunReply::failed(&e);
            }
        }
        let arg = match req.source.kind {
            _ if super::change_trigger::is_change_source(&req.source) => "trigger",
            SourceKind::Stream => "record",
            SourceKind::Queue => "message",
        };
        let (ran, logs) = self.run_version(cx, req, arg, rp.event.clone()).await;
        let reply = match ran {
            Ok(()) => match self.trigger_stmt(cx, "COMMIT").await {
                Ok(()) => RunReply::of(RunOutcome::Ok),
                Err(e) => {
                    self.rollback_quietly(cx).await;
                    RunReply::failed(&e)
                }
            },
            Err(e) => {
                self.rollback_quietly(cx).await;
                RunReply::failed(&e)
            }
        };
        RunReply { logs, ..reply }
    }

    /// A held message's deletes as plain writes of the session's engine
    /// transaction: 1, or 0 when the message is no longer stored.
    fn engine_ack_in_txn(&mut self, t: &sprig_queues::token::TokenData) -> Result<u64> {
        let store = super::queues::engine_msg_store(t.tenant, t.db)?;
        let Some(ops) = store
            .txn_ack(t.id.queue, &t.msg_id)
            .map_err(|e| SprigError::internal(e.to_string()))?
        else {
            return Ok(0);
        };
        let tx = self.eng.txn.open_tx().ok_or_else(|| {
            SprigError::internal("a queue trigger's transaction isn't on the engine")
        })?;
        for op in ops {
            super::queues::plain_write(tx.engine_txn(), op)?;
        }
        Ok(1)
    }

    /// The next message of a queue (see the module docs).
    #[allow(clippy::too_many_lines)]
    async fn run_queue_trigger(&mut self, cx: &mut ExecCtx<'_>, req: &RunRequest) -> RunReply {
        let (engine, principal, tenant) = {
            let Some(ts) = self.tenant.as_deref() else {
                return RunReply::failed(&SprigError::internal("no tenant"));
            };
            let Some(engine) = ts.services().mq.get().cloned() else {
                let mut e = SprigError::new(
                    SprigCode::LogicEngineUnavailable,
                    "the queue engine isn't running on this gateway",
                );
                e.retryable = true;
                return RunReply::failed(&e);
            };
            let p = &ts.principal;
            let principal = Arc::new(sprig_proto_amqp::auth::Principal {
                tenant_id: ts.tenant.0,
                tenant: p.tenant_name.clone().unwrap_or_default(),
                user: p.user.clone(),
                user_id: p.user_id.map(|u| u64::from(u.0)),
                handle: Some(Arc::new(p.clone())),
            });
            (engine, principal, ts.tenant)
        };
        let mq_err = |e: &sprig_proto_amqp::error::MqError| {
            let mut s = SprigError::standard(
                u16::try_from(e.code.number()).unwrap_or(39250),
                *b"HY000",
                format!("{}: {}", e.code.name(), e.text),
            );
            s.retryable = e.code == sprig_proto_amqp::error::MqCode::OwnerMoved;
            s
        };
        let session = u64::from(cx.session.id.0);
        // Opening the vhost or taking a message isn't the code's failure:
        // transient (the job ends by itself when the source is gone).
        let infra = |e: &sprig_proto_amqp::error::MqError| {
            let mut r = RunReply::failed(&mq_err(e));
            r.transient = true;
            r
        };
        let v = match engine
            .sql_vhost(principal, &req.source.db_name, session)
            .await
        {
            Ok(v) => v,
            Err(e) => return infra(&e),
        };
        // The trigger reads the queue of its own database: one with the
        // name in a database made since is another queue.
        if v.vhost().db_id != req.source.db_id {
            return RunReply::of(RunOutcome::Empty);
        }
        let ds = match v.get(&req.source.name, 1, req.lease_ms.max(1000)).await {
            Ok(d) => d,
            Err(e) => return infra(&e),
        };
        let Some(d) = ds.into_iter().next() else {
            return RunReply::of(RunOutcome::Empty);
        };
        let token = d
            .annotations
            .get("x-sprig-delivery")
            .and_then(|t| t.as_str().map(std::borrow::Cow::into_owned))
            .unwrap_or_default();
        let t = match engine.verify_token(&token) {
            Ok(t) => t,
            Err(e) => return RunReply::failed(&mq_err(&e)),
        };
        let attempts = d.delivery_count;
        let message_id = t.msg_id.iter().fold(String::new(), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        });
        let props: Value =
            serde_json::from_str(&sprig_queues::sql::properties_json(&d)).unwrap_or(json!({}));
        let body = sprig_queues::sql::body_bytes(&d);
        let event = json!({
            "queue": req.source.name,
            "vhost": req.source.db_name,
            "exchange": d.message.exchange,
            "routingKey": d.message.routing_key,
            "body": String::from_utf8_lossy(&body),
            "properties": props,
            "redelivered": d.redelivered,
            "deliveryCount": d.delivery_count,
        });
        let db = v.db().to_owned();
        let with_id = |mut r: RunReply| {
            r.message_id = Some(message_id.clone());
            r.attempts = attempts;
            r
        };
        // The code's log lines, also when the run fails.
        let mut code_logs = Vec::new();
        let failure = if let Err(e) = self.trigger_stmt(cx, "START TRANSACTION").await {
            e
        } else {
            {
                // mq_ack's path: hold, then the row's delete in this
                // transaction.
                if let Err(e) = v.hold(&t).await {
                    self.rollback_quietly(cx).await;
                    return with_id(RunReply::of(RunOutcome::Moved)).with_note(&mq_err(&e));
                }
                let deleted = if self.mq_on_engine(&db) {
                    // Messages on the engine: the message's deletes are
                    // plain writes of the session's engine transaction,
                    // applied with its commit.
                    match self.engine_ack_in_txn(&t) {
                        Ok(n) => n,
                        Err(e) => {
                            self.rollback_quietly(cx).await;
                            v.release(&t, false);
                            return with_id(RunReply::failed(&e));
                        }
                    }
                } else {
                    match self
                        .run_checked(cx, &sprig_queues::sql::ack_sql(&db, &t), true)
                        .await
                    {
                        Ok(c) => c.affected.unwrap_or(0),
                        Err(e) => {
                            self.rollback_quietly(cx).await;
                            v.release(&t, false);
                            return with_id(RunReply::failed(&e));
                        }
                    }
                };
                if deleted == 0 {
                    self.rollback_quietly(cx).await;
                    v.release(&t, false);
                    return with_id(RunReply::of(RunOutcome::Moved));
                }
                // In a rollout, the message's key picks the version.
                let (picked, counted) = self.pick_version(req, &event, Some(&message_id)).await;
                let started = std::time::Instant::now();
                let (ran, logs) = self
                    .run_version(cx, &picked, "message", event.clone())
                    .await;
                code_logs.clone_from(&logs);
                let ok = RunReply::of(RunOutcome::Ok);
                let failed = RunReply::of(RunOutcome::Failed);
                let counted_ok = ran.is_ok();
                self.count_run(
                    cx,
                    counted,
                    &picked,
                    started,
                    if counted_ok { &ok } else { &failed },
                )
                .await;
                match ran.map(|()| logs) {
                    Ok(logs) => match self.trigger_stmt(cx, "COMMIT").await {
                        Ok(()) => {
                            v.release(&t, true);
                            return with_id(RunReply {
                                logs,
                                ..RunReply::of(RunOutcome::Ok)
                            });
                        }
                        Err(e) => {
                            self.rollback_quietly(cx).await;
                            v.release(&t, false);
                            e
                        }
                    },
                    Err(e) => {
                        self.rollback_quietly(cx).await;
                        v.release(&t, false);
                        e
                    }
                }
            }
        };
        let mut reply = with_id(RunReply {
            logs: code_logs,
            ..RunReply::failed(&failure)
        });
        // An inherited trigger whose sprig_inherited lost a
        // privilege waits for the grant (39242, marked by `on_run`): its
        // message goes back without counting, never to the dead letters.
        if self
            .tenant
            .as_deref()
            .is_some_and(|ts| ts.principal.user == crate::inherited::RUN_AS_USER)
            && crate::inherited::triggers::is_privilege_error(&failure)
        {
            reply.transient = true;
        }
        if reply.transient {
            // Not the code's fault: back as it was, without counting.
            v.nack(&t, true);
            return reply;
        }
        if attempts.saturating_add(1) < req.max_attempts.max(1) {
            v.fail(&t);
            return reply;
        }
        // The last attempt: a dead letter in the trigger's table first (so
        // nothing is lost when the queue has no dead-letter exchange), then
        // a reject, which the queue dead-letters through its exchange when
        // it has one.
        let written = async {
            let mut conn = {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                ts.services().records.begin(tenant).await?
            };
            let store = sprig_logic::LogicStore::default();
            store
                .put_dead_letter(
                    &mut *conn,
                    req.source.binding_id,
                    req.version,
                    &format!("m{message_id}"),
                    token.as_bytes(),
                    &event,
                    attempts.saturating_add(1),
                    reply.code,
                    &reply.message,
                )
                .await?;
            let _ = store
                .log(
                    &mut *conn,
                    req.source.module_id,
                    Some(req.version),
                    "error",
                    "trigger",
                    &format!(
                        "queue {} message {message_id}: dead letter after {} attempts: {} {}",
                        req.source.name,
                        attempts.saturating_add(1),
                        reply.code,
                        reply.message
                    ),
                )
                .await;
            conn.commit().await
        }
        .await;
        match written {
            Ok(()) => {
                v.nack(&t, false);
                reply.outcome = RunOutcome::Dead;
            }
            Err(e) => {
                // Try again later; the delivery count still says it's due.
                tracing::warn!(error = %e, binding = req.source.binding_id, "trigger dead letter not written");
                v.nack(&t, true);
            }
        }
        reply
    }
}

/// A note on a reply (the reason a run didn't happen).
trait WithNote {
    fn with_note(self, e: &SprigError) -> Self;
}

impl WithNote for RunReply {
    fn with_note(mut self, e: &SprigError) -> Self {
        self.message.clone_from(&e.message);
        self
    }
}
