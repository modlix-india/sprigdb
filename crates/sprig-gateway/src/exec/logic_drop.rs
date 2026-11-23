//! `DROP FUNCTION | PROCEDURE | VALIDATION | TRIGGER | SCHEDULE` of logic
//! modules in a tenant session (`sprig_logic::drop`).
//!
//! ```text
//! the module of that kind and name (none: a FUNCTION, PROCEDURE or
//!   TRIGGER is ordinary DDL; a VALIDATION or SCHEDULE is 39860, or a note
//!   with IF EXISTS)
//! ALTER ROUTINE (functions, procedures, schedules) or TRIGGER (validations,
//!   triggers) on the database, or CREATE ROUTINE
//! functions and procedures: dependents → 39875, or CASCADE drops the
//!   schedules that call it
//! every shard (sprig_ctl): the routines, packages and MariaDB triggers of
//!   every version; a validation's CHECK through the DDL path's ALTER TABLE
//! the catalog: dropped, versions retired, rollout ended, index entries
//! ```
//!
//! `ALTER TRIGGER [db.]name DISABLE | ENABLE | ROLLOUT ...` of a trigger on a stream,
//! a queue or a table (after-commit), with the rights `DROP TRIGGER` needs:
//!
//! ```text
//! on a stream or a queue:
//!   DISABLE: the module disabled, its index entry removed; the running job
//!     ends at its next check of the module (5 s), and the stream's consumer
//!     group, or the queue's messages, keep the position
//!   ENABLE: the module active, the index entry back: a job starts there
//! after-commit:
//!   DISABLE: the module disabled, its feed subscription paused; the running
//!     job ends at its next check of the module (5 s), and the controller
//!     starts none while the subscription is paused; the checkpoint stays
//!   ENABLE: the module active, the subscription active: a job resumes at
//!     the checkpoint, so changes committed meanwhile run then, each once
//!     (the markers stop a change the old job ran after its last
//!     acknowledgement)
//! ```
//!
//! An inherited trigger's instance is 39862 (`ALTER INHERITED TRIGGER`
//! owns those).
//!
//! `SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name` answers what
//! the drop would find, as rows (`Kind`, `Name`, `Cascade`, `Detail`), so
//! clients needn't read 39875's text.

use std::collections::HashMap;
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::catalog::ModuleRow;
use sprig_logic::declare::Kind;
use sprig_logic::deploy::ShardSessions;
use sprig_logic::drop::{AlterTrigger, DropStmt, ShowDependents};
use sprig_logic::messages::{self, MsgIndex};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::logic_bound::CtlShards;
use super::tenant::{Step, scrub_typed};
use crate::pipeline::{ExecCtx, ResultSink};
use crate::tenant::session::LocalWarning;

/// How long after an `ENABLE` its index entry is checked again: past a
/// running job's next check of the module (every 5 s).
const REENABLE_RECHECK: std::time::Duration = std::time::Duration::from_secs(7);

impl PassSession {
    /// A `DROP` of a logic module; `None` when it names no logic module
    /// and is ordinary DDL.
    pub(super) async fn tenant_logic_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: Result<DropStmt>,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let r = match d {
            Ok(d) => Box::pin(self.logic_drop(cx, &d)).await,
            Err(e) => Err(e),
        };
        if let Some(ts) = self.tenant.as_deref() {
            super::logic_rollout::invalidate(ts.tenant.0);
        }
        self.answer_notes(r, status, out).await
    }

    /// `ALTER TRIGGER [db.]name DISABLE | ENABLE` of a trigger on a stream
    /// or a queue: an OK with notes, or the error.
    pub(super) async fn tenant_logic_toggle(
        &mut self,
        a: &AlterTrigger,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let r = Box::pin(self.logic_toggle(a)).await.map(Some);
        self.answer_notes(r, status, out).await
    }

    /// An OK carrying `notes` as notes (1105), or the error; `Ok(None)`
    /// passes the statement on.
    async fn answer_notes(
        &mut self,
        r: Result<Option<Vec<String>>>,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        match r {
            Ok(None) => Ok(None),
            Ok(Some(notes)) => {
                let n = notes.len();
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.warnings = Some(
                        notes
                            .into_iter()
                            .map(|m| LocalWarning {
                                level: "Note",
                                code: 1105,
                                message: m,
                            })
                            .collect(),
                    );
                }
                let mut ok = OkPacket::new(status);
                ok.warnings = u16::try_from(n).unwrap_or(u16::MAX);
                out.ok(ok).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => {
                let shown = SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                };
                out.error(&scrub_typed(shown)).await?;
                Ok(Some(Step {
                    errored: true,
                    status: None,
                }))
            }
        }
    }

    #[allow(clippy::too_many_lines)] // lookup, rights, dependents, shards, catalog
    async fn logic_drop(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: &DropStmt,
    ) -> Result<Option<Vec<String>>> {
        let ordinary = matches!(d.kind, Kind::Function | Kind::Procedure | Kind::Trigger);
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let Some(db) = d.db.clone().or(current) else {
            return if ordinary {
                Ok(None)
            } else {
                Err(SprigError::standard(
                    1046,
                    *b"3D000",
                    "No database selected",
                ))
            };
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let Some(row) = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
        else {
            return if ordinary {
                Ok(None)
            } else {
                Err(SprigError::standard(
                    1049,
                    *b"42000",
                    format!("Unknown database '{db}'"),
                ))
            };
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let store = sprig_logic::LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let module = match store
            .module(&mut *conn, row.db_id.0, d.kind.as_str(), &d.name)
            .await
        {
            Ok(m) => m,
            Err(e) => return Err(e),
        };
        let Some(m) = module else {
            if ordinary {
                return Ok(None);
            }
            if d.if_exists {
                return Ok(Some(vec![format!(
                    "{} {} doesn't exist",
                    d.kind.as_str(),
                    d.name
                )]));
            }
            return Err(SprigError::new(
                SprigCode::LogicNotFound,
                format!("no {} {} in {}", d.kind.as_str(), d.name, row.name),
            ));
        };
        // Dropping code needs the right that created it.
        {
            use sprig_auth::authz::{Action, Target};
            let grants = crate::tenant::authz::privileges(ts, &catalog);
            let db_key = sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
            let on = Target::Database { db_key: &db_key };
            let ok = grants.unrestricted()
                || grants.allows(Action::AlterRoutine, on)
                || grants.allows(Action::CreateRoutine, on)
                || (matches!(d.kind, Kind::Validation | Kind::Trigger)
                    && grants.allows(Action::Trigger, on));
            if !ok {
                return Err(SprigError::standard(
                    1044,
                    *b"42000",
                    format!(
                        "Access denied for user '{}' to database '{}'",
                        ts.principal.user, row.name
                    ),
                ));
            }
        }
        let shard_list = super::logic::shard_list(ts, &self.target.cluster, &row.name, &row)?;
        let mut shards = CtlShards {
            list: shard_list,
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let mut notes = Vec::new();
        // What uses a function or procedure.
        let mut also: Vec<ModuleRow> = Vec::new();
        if matches!(d.kind, Kind::Function | Kind::Procedure) {
            let mut deps =
                sprig_logic::drop::dependents(&mut *conn, &store, row.db_id.0, &row.name, &m)
                    .await?;
            deps.sinks = sinks_using(ts.catalog(), row.db_id.0, &row.name, &m.name);
            if let Some(e) = deps.refusal(d.kind, &d.name, d.cascade) {
                return Err(e);
            }
            for (id, name) in &deps.schedules {
                if let Some(s) = store.module_by_id(&mut *conn, *id).await? {
                    also.push(s);
                    notes.push(format!("schedule {name} dropped with it (CASCADE)"));
                }
            }
            for (_, col) in &deps.columns {
                notes.push(format!(
                    "computed column {col} keeps its own expression; its link to {} is removed",
                    d.name
                ));
            }
            for c in &deps.callers {
                notes.push(format!(
                    "{c} calls {}: such calls now fail with 39860",
                    d.name
                ));
            }
        }
        // A validation's CHECK constraints go through the DDL path's ALTER
        // TABLE, as the deploy added them.
        let binding = store.binding(&mut *conn, m.module_id).await?;
        let versions: Vec<u64> = store
            .versions(&mut *conn, m.module_id)
            .await?
            .iter()
            .map(|v| v.version)
            .collect();
        if d.kind == Kind::Validation
            && let Some(b) = &binding
            && b.enforcement.as_deref() == Some("check")
            && let Some(obj) = b.object_id
        {
            // The table's name, from the session's catalog snapshot.
            let table: Option<String> = ts.catalog().and_then(|c| {
                c.objects
                    .get(sprig_common::ids::ObjectId(obj))
                    .map(|o| o.name.clone())
            });
            if let Some(t) = table {
                let names: Vec<String> = versions
                    .iter()
                    .map(|v| sprig_logic::enforce::check_name(b.binding_id, *v))
                    .collect();
                let alter = sprig_logic::enforce::check_alter(&row.name, &t, None, &names);
                // The ALTER is a catalog change, which takes connections
                // from the same pool: this one goes back meanwhile (it
                // holds no transaction).
                drop(conn);
                self.drop_alter(cx, &alter).await?;
                conn = svc.records.begin(tenant).await?;
            }
        }
        // Every shard: routines, packages, MariaDB triggers.
        let mut objects = sprig_logic::drop::routine_objects(&m, &versions);
        if let Some(b) = &binding
            && b.enforcement.as_deref() == Some("trigger")
        {
            let events: Vec<String> = ["insert", "update", "delete"]
                .iter()
                .map(|e| (*e).to_owned())
                .collect();
            for v in &versions {
                for (n, _) in sprig_logic::enforce::trigger_names(b.binding_id, *v, &events) {
                    objects.insert(0, ("TRIGGER".to_owned(), n));
                }
            }
        }
        for s in &also {
            let vs: Vec<u64> = store
                .versions(&mut *conn, s.module_id)
                .await?
                .iter()
                .map(|v| v.version)
                .collect();
            objects.splice(0..0, sprig_logic::drop::routine_objects(s, &vs));
        }
        for schema in shards.schemas() {
            let sess = shards.session(&schema).await?;
            sprig_logic::deploy::drop_objects(sess, &schema, &objects).await?;
        }
        let dc = svc.locator.dc_store();
        for s in &also {
            sprig_logic::drop::module_dropped(&mut *conn, &store, &dc, tenant, s, catalog.version)
                .await?;
        }
        sprig_logic::drop::module_dropped(&mut *conn, &store, &dc, tenant, &m, catalog.version)
            .await?;
        conn.commit().await?;
        // An after-commit trigger on an engine table holds old row versions
        // in the node's change log; its drop lets them go.
        if m.kind == "trigger" {
            super::change_trigger::release_module(&svc, tenant, m.module_id).await;
        }
        notes.insert(
            0,
            format!(
                "{} {} dropped ({} version{})",
                d.kind.as_str(),
                d.name,
                versions.len(),
                if versions.len() == 1 { "" } else { "s" }
            ),
        );
        Ok(Some(notes))
    }

    /// Disables or enables a trigger on a stream or a queue (see the
    /// module docs). The same rights as `DROP TRIGGER`.
    #[allow(clippy::too_many_lines)] // the checks, then one arm per kind of trigger
    async fn logic_toggle(&mut self, a: &AlterTrigger) -> Result<Vec<String>> {
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("a tenant statement without a tenant"));
        };
        let Some(db) = a.db.clone().or(current) else {
            return Err(SprigError::standard(
                1046,
                *b"3D000",
                "No database selected",
            ));
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let Some(row) = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
        else {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let store = sprig_logic::LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let module = match store
            .module(&mut *conn, row.db_id.0, Kind::Trigger.as_str(), &a.name)
            .await
        {
            Ok(m) => m,
            Err(e) => return Err(e),
        };
        let not_found = || {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no trigger {} in {}", a.name, row.name),
            )
        };
        let Some(m) = module else {
            return Err(not_found());
        };
        may_alter_trigger(ts, &catalog, &row.name)?;
        let who = format!("{}@{}", ts.principal.user, ts.principal.host);
        if !matches!(m.state.as_str(), "active" | "disabled") {
            return Err(not_found());
        }
        // `ROLLOUT TO n | PROMOTE | ABORT`: the trigger's rollout by key
        // (a stream record's or queue message's, an after-commit row's).
        if let Some(op) = &a.rollout {
            let at = (tenant.0, m.module_id);
            let r = alter_rollout(&mut *conn, at, a, op, catalog.version, &who).await?;
            conn.commit().await?;
            return Ok(r);
        }
        let binding = store.binding(&mut *conn, m.module_id).await?;
        if let Some(b) = binding.as_ref().filter(|b| b.timing == "after_commit") {
            let dc = svc.locator.dc_store();
            let changed = after_commit_toggle(&mut *conn, &store, &dc, tenant, b, &m, a).await?;
            let state = if a.enable { "enabled" } else { "disabled" };
            let note = if changed {
                if let Err(e) = store
                    .log(
                        &mut *conn,
                        m.module_id,
                        m.active_version,
                        "info",
                        "deploy",
                        &format!("{state} by {who}"),
                    )
                    .await
                {
                    tracing::warn!(error = %e, "logic_log line not written");
                }
                format!(
                    "after-commit trigger {} {state}{}",
                    a.name,
                    if a.enable {
                        "; it resumes from where it stopped, and changes committed meanwhile run now"
                    } else {
                        "; its job ends within 5 s and its position is kept"
                    }
                )
            } else {
                format!("trigger {} was already {state}", a.name)
            };
            conn.commit().await?;
            tracing::info!(%tenant, binding = b.binding_id, trigger = %a.name, state, "after-commit trigger altered");
            return Ok(vec![note]);
        }
        let (b, src) = message_source(&mut *conn, &store, &catalog, a, m.module_id).await?;
        let changed = store
            .set_module_state(&mut *conn, m.module_id, a.enable)
            .await?;
        let index = MsgIndex {
            tenant,
            binding: b.binding_id,
            kind: src.kind,
        };
        conn.commit().await?;
        set_indexed(&svc, tenant, m.module_id, index, a.enable).await?;
        let state = if a.enable { "enabled" } else { "disabled" };
        let note = if changed {
            let mut conn = svc.records.begin(tenant).await?;
            if let Err(e) = store
                .log(
                    &mut *conn,
                    m.module_id,
                    m.active_version,
                    "info",
                    "deploy",
                    &format!("{state} by {who}"),
                )
                .await
            {
                tracing::warn!(error = %e, "logic_log line not written");
            }
            conn.commit().await?;
            toggled_note(a, &src, state)
        } else {
            format!("trigger {} was already {state}", a.name)
        };
        tracing::info!(%tenant, binding = b.binding_id, trigger = %a.name, state, "trigger on a stream or queue altered");
        Ok(vec![note])
    }

    /// `SHOW DEPENDENTS OF FUNCTION | PROCEDURE [db.]name`: what
    /// `DROP ... RESTRICT` would refuse with 39875, one row each, with what
    /// `CASCADE` does to it. The same module and the same rights as the
    /// drop (`ALTER ROUTINE` or `CREATE ROUTINE` on the database).
    pub(super) async fn tenant_logic_dependents(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: Result<ShowDependents>,
        more: bool,
        format: crate::pipeline::RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        let r = match d {
            Ok(d) => Box::pin(self.logic_dependents(&d)).await,
            Err(e) => Err(e),
        };
        let rows = match r {
            Ok(rows) => rows,
            Err(e) => return self.fail(out, &e).await,
        };
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let rows = sprig_types::Rows {
            columns: sprig_logic::drop::DEPENDENTS_COLUMNS
                .iter()
                .map(|n| crate::pipeline::text_column(n, cx.proto.collation))
                .collect(),
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|c| Some(c.into_bytes())).collect())
                .collect(),
        };
        super::types::send_rows(out, &rows, format, status).await?;
        Ok(Step {
            errored: false,
            status: Some(status),
        })
    }

    async fn logic_dependents(&mut self, d: &ShowDependents) -> Result<Vec<[String; 4]>> {
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db =
            d.db.clone()
                .or(current)
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let row = catalog
            .database(&db)
            .or_else(|| catalog.database(&db.to_ascii_lowercase()))
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
            })?;
        {
            use sprig_auth::authz::{Action, Target};
            let grants = crate::tenant::authz::privileges(ts, &catalog);
            let db_key = sprig_catalog::ddl::database_key(&row.name, ts.lower_case_table_names());
            let on = Target::Database { db_key: &db_key };
            if !(grants.unrestricted()
                || grants.allows(Action::AlterRoutine, on)
                || grants.allows(Action::CreateRoutine, on))
            {
                return Err(SprigError::standard(
                    1044,
                    *b"42000",
                    format!(
                        "Access denied for user '{}' to database '{}'",
                        ts.principal.user, row.name
                    ),
                ));
            }
        }
        let svc = Arc::clone(ts.services());
        let store = sprig_logic::LogicStore::default();
        let mut conn = svc.records.begin(ts.tenant).await?;
        let module = match store
            .module(&mut *conn, row.db_id.0, d.kind.as_str(), &d.name)
            .await
        {
            Ok(m) => m,
            Err(e) => return Err(e),
        };
        let m = module.ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no {} {} in {}", d.kind.as_str(), d.name, row.name),
            )
        })?;
        let mut deps =
            sprig_logic::drop::dependents(&mut *conn, &store, row.db_id.0, &row.name, &m).await?;
        deps.sinks = sinks_using(ts.catalog(), row.db_id.0, &row.name, &m.name);
        Ok(deps.rows())
    }

    /// Runs an `ALTER TABLE` of the drop through the session, as a
    /// client's (the DDL path).
    async fn drop_alter(&mut self, cx: &mut ExecCtx<'_>, alter: &str) -> Result<()> {
        let wire = crate::pipeline::Wire {
            persona: self.persona,
            caps: cx.proto.caps,
        };
        let r = self
            .engine_db(
                cx,
                sprig_logic::engine::bridge::DbOp {
                    shape: sprig_logic::engine::bridge::Shape::Exec,
                    sql: alter.to_owned(),
                },
                wire,
            )
            .await;
        r.map(|_| ())
    }
}

/// The index entry of a trigger on a stream or a queue after `ALTER TRIGGER
/// ... ENABLE` (written, so a job starts from the kept position) or
/// `ALTER TRIGGER ... ROLLOUT ...` of a trigger on a stream or queue: the
/// rollout's next stage, promotion or end (`sprig_logic::messages`).
/// `(tenant, module)` names the trigger.
async fn alter_rollout(
    conn: &mut dyn sprig_catalog::RecordTx,
    (tenant, module_id): (u64, u64),
    a: &AlterTrigger,
    op: &std::result::Result<sprig_logic::rollout::RolloutOp, String>,
    catalog_version: u64,
    who: &str,
) -> Result<Vec<String>> {
    let op = op
        .clone()
        .map_err(|msg| SprigError::new(SprigCode::LogicSyntax, msg))?;
    let note = sprig_logic::messages::rollout_change(
        conn,
        "",
        module_id,
        &a.name,
        op,
        catalog_version,
        who,
    )
    .await?;
    super::msg_trigger::forget_rollout(tenant, module_id);
    Ok(vec![note])
}

/// The note of a trigger disabled or enabled.
fn toggled_note(a: &AlterTrigger, src: &sprig_logic::messages::SourceRow, state: &str) -> String {
    format!(
        "trigger {} on {} {}.{} {state}{}",
        a.name,
        src.kind.as_str(),
        src.db_name,
        src.name,
        if a.enable {
            "; it resumes from where it stopped"
        } else {
            "; its job ends within 5 s and its position is kept"
        }
    )
}

/// `DISABLE` (removed; the running job ends at its next check of the
/// module, within 5 s, and its consumer group or queue keeps the position).
async fn set_indexed(
    svc: &Arc<crate::tenant::TenantServices>,
    tenant: sprig_common::ids::TenantId,
    module_id: u64,
    index: MsgIndex,
    enable: bool,
) -> Result<()> {
    let dc = svc.locator.dc_store();
    if !enable {
        return messages::delete_index(dc.as_ref(), index.tenant, index.binding).await;
    }
    if !messages::indexed(dc.as_ref(), index.tenant, index.binding).await? {
        messages::put_index(dc.as_ref(), &index).await?;
    }
    // A job that read the module as disabled just before this may still
    // remove the entry as it ends: put it back once that job's last check
    // has passed.
    let (locator, records) = (Arc::clone(&svc.locator), Arc::clone(&svc.records));
    tokio::spawn(async move {
        tokio::time::sleep(REENABLE_RECHECK).await;
        let Ok(mut c) = records.begin(tenant).await else {
            return;
        };
        let active = sprig_logic::LogicStore::default()
            .module_by_id(&mut *c, module_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|m| m.state == "active");
        drop(c);
        let dc = locator.dc_store();
        if active
            && matches!(
                messages::indexed(dc.as_ref(), index.tenant, index.binding).await,
                Ok(false)
            )
        {
            let _ = messages::put_index(dc.as_ref(), &index).await;
        }
    });
    Ok(())
}

/// `ALTER TRIGGER ... DISABLE | ENABLE` of an after-commit trigger: the
/// module's state and its feed subscription's (`logic:b<binding>`), in the
/// order that never leaves a job running for a disabled module: `DISABLE`
/// pauses the subscription first (the controller starts no new job), then
/// disables the module (the running job ends at its next check);
/// `ENABLE` makes the module active first, then the subscription (a job
/// starts at the checkpoint). Whether the module's state changed.
async fn after_commit_toggle(
    conn: &mut dyn sprig_catalog::RecordTx,
    store: &sprig_logic::LogicStore,
    dc: &Arc<dyn sprig_meta::MetaStore>,
    tenant: sprig_common::ids::TenantId,
    b: &sprig_logic::catalog::BindingRow,
    m: &ModuleRow,
    a: &AlterTrigger,
) -> Result<bool> {
    let subs = sprig_feed::subs::Subscriptions::new(Arc::clone(dc));
    let name = sprig_logic::triggers::subscription_name(b.binding_id);
    if a.enable {
        let changed = store.set_module_state(conn, m.module_id, true).await?;
        subs.set_state(tenant, &name, "active").await?;
        Ok(changed)
    } else {
        subs.set_state(tenant, &name, "paused").await?;
        store.set_module_state(conn, m.module_id, false).await
    }
}

/// `ALTER TRIGGER`'s rights: those `DROP TRIGGER` needs (`ALTER ROUTINE`,
/// `CREATE ROUTINE` or `TRIGGER` on the database), else 1044.
fn may_alter_trigger(
    ts: &crate::tenant::session::TenantSession,
    catalog: &sprig_catalog::TenantCatalog,
    db: &str,
) -> Result<()> {
    use sprig_auth::authz::{Action, Target};
    let grants = crate::tenant::authz::privileges(ts, catalog);
    let db_key = sprig_catalog::ddl::database_key(db, ts.lower_case_table_names());
    let on = Target::Database { db_key: &db_key };
    if grants.unrestricted()
        || grants.allows(Action::AlterRoutine, on)
        || grants.allows(Action::CreateRoutine, on)
        || grants.allows(Action::Trigger, on)
    {
        return Ok(());
    }
    Err(SprigError::standard(
        1044,
        *b"42000",
        format!(
            "Access denied for user '{}' to database '{db}'",
            ts.principal.user
        ),
    ))
}

/// The binding and source row of trigger module `module_id`, which must be
/// on a stream or a queue (39862 otherwise; after-commit triggers are
/// handled before) and not an inherited trigger's instance (its
/// definition's overrides would undo the change: 39862).
async fn message_source(
    conn: &mut dyn sprig_catalog::RecordTx,
    store: &sprig_logic::LogicStore,
    catalog: &sprig_catalog::TenantCatalog,
    a: &AlterTrigger,
    module_id: u64,
) -> Result<(sprig_logic::catalog::BindingRow, messages::SourceRow)> {
    let binding = store.binding(conn, module_id).await?;
    let timing = binding.as_ref().map(|b| b.timing.clone());
    let Some(b) = binding.filter(|b| b.timing == "on_message") else {
        // A trigger that runs inside its statement (BEFORE a write, on a
        // read), or one without a binding.
        let what = match timing.as_deref() {
            Some("before_write" | "on_write") => "runs inside the statements that write its table",
            Some("on_read") => "runs inside the statements that read its table",
            _ => "isn't bound to a table, a stream or a queue",
        };
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "trigger {} {what}: ALTER TRIGGER ... DISABLE | ENABLE applies only to \
                 after-commit triggers and triggers on a stream or a queue; use DROP TRIGGER \
                 and create it again instead",
                a.name
            ),
        ));
    };
    let Some(src) = messages::source(conn, b.binding_id).await? else {
        return Err(SprigError::new(
            SprigCode::LogicNotFound,
            format!("no trigger {}", a.name),
        ));
    };
    let inherited = sprig_auth::cache::TenantAuth::of(catalog)
        .and_then(|au| {
            au.accounts(crate::inherited::RUN_AS_USER)
                .first()
                .map(|u| u.row.user_id)
        })
        .is_some_and(|u| u.0 == src.definer_user_id);
    if inherited {
        return Err(SprigError::new(
            SprigCode::LogicUnsupported,
            format!(
                "trigger {} is an inherited trigger's instance: use ALTER INHERITED TRIGGER {} {}",
                a.name,
                a.name,
                if a.enable { "ENABLE" } else { "DISABLE" }
            ),
        ));
    }
    Ok((b, src))
}

/// Sinks of database `db_id` whose `TRANSFORM` is function `name` (by its
/// name or `<db_name>.<name>`), from the catalog snapshot's sinks.
fn sinks_using(
    cat: Option<&Arc<sprig_catalog::TenantCatalog>>,
    db_id: u32,
    db_name: &str,
    name: &str,
) -> Vec<String> {
    let qualified = format!("{db_name}.{name}");
    let mut v: Vec<String> = cat
        .and_then(|c| {
            c.ext
                .get::<sprig_sinks::store::SinksState>(sprig_sinks::store::MODULE)
        })
        .map(|st| {
            st.sinks
                .values()
                .filter(|s| {
                    s.db_id == db_id
                        && s.transform
                            .as_deref()
                            .is_some_and(|t| t == name || t == qualified)
                })
                .map(|s| s.name.clone())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}
