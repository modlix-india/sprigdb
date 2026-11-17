//! Validations, after-commit triggers, schedules and computed fields in a
//! tenant session.
//!
//! ```text
//! CREATE VALIDATION | TRIGGER ... AFTER COMMIT | SCHEDULE:
//!   privileges: TRIGGER on the table (validations, triggers), CREATE
//!     ROUTINE on the database (schedules), checked here
//!   shards: the DDL layout (the table's group, or every shard of the
//!     database); one sprig_ctl connection per shard, every routine and
//!     trigger written with DEFINER = the user's backend account and the
//!     definer checked afterwards; all or nothing (sprig_logic::service)
//!   a CHECK validation: ALTER TABLE ... ADD CONSTRAINT through this
//!     session's own DDL path (the DDL engine), as if the client sent it
//!   trigger: feed subscription logic:b<binding> registered at the cluster's
//!     binlog position now; schedule: logic/schedules/<tenant>/<module>
//! ALTER TABLE t ADD c TYPE AS (f(args)), f a logic function: f's body as
//!   one expression replaces the call, and the statement runs as ordinary
//!   DDL (a generated column through the DDL engine)
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState};
use sprig_common::ids::ClusterId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic::catalog::BindingRow;
use sprig_logic::declare::{Declaration, Kind};
use sprig_logic::dependent::Rebind;
use sprig_logic::deploy::{ConnSession, Session, ShardSessions, conn_err};
use sprig_logic::service::{self, Bind, Target};
use sprig_logic::{LogicStore, enforce};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;

use super::PassSession;
use super::tenant::{Step, scrub_typed};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat, Wire};
use crate::tenant::session::LocalWarning;

/// `logic.max_loop_iterations`'s default.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
/// `logic.compile_max_cost`'s default (validations: `logic.validation_max_cost`).
const COMPILE_MAX_COST: u64 = 100_000;

/// How stale a worker's view of a trigger binding's active version can be
/// (`sprig_logic::triggers::maria::CatalogVersions` reads it again after
/// two seconds), with a margin: a column change waits this long between
/// an after-commit trigger's new version and the `ALTER TABLE`.
const TRIGGER_VERSION_STALE: std::time::Duration = std::time::Duration::from_millis(2500);

fn bq(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// One `sprig_ctl` connection that several [`Session`]s of one deploy
/// share: the routines' creator, the reader of the columns and the shard
/// session of its cluster. A deploy runs one statement at a time, so they
/// never wait for each other; with a connection each, one deploy held up
/// to three of a cluster's four and waited for more while it held them.
#[derive(Clone)]
pub(super) struct SharedCtl(Arc<tokio::sync::Mutex<ConnSession>>);

impl SharedCtl {
    pub(super) fn new(conn: mysql_async::Conn) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(ConnSession(conn))))
    }
}

#[async_trait]
impl Session for SharedCtl {
    async fn exec(&mut self, sql: &str) -> Result<()> {
        self.0.lock().await.exec(sql).await
    }

    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        self.0.lock().await.rows(sql).await
    }
}

/// `sprig_ctl` connections to every shard a deploy reaches: one per
/// cluster, taken together in cluster order the first time one is needed
/// (two deploys over the same clusters can't each hold one and wait for
/// the other's), and given back with [`CtlShards::release`] before work
/// that takes connections of its own.
pub(super) struct CtlShards {
    pub(super) list: Vec<(String, ClusterId)>,
    pub(super) conns: HashMap<String, SharedCtl>,
    pub(super) locator: Arc<crate::tenant::locator::GatewayLocator>,
}

#[async_trait]
impl ShardSessions for CtlShards {
    fn schemas(&self) -> Vec<String> {
        self.list.iter().map(|(s, _)| s.clone()).collect()
    }

    async fn session<'a>(&'a mut self, schema: &str) -> Result<&'a mut (dyn Session + 'static)> {
        // One connection per cluster, not per shard schema: every routine
        // names its schema, and a gateway's `sprig_ctl` pool is small
        // (four), so a deploy over many shards of one cluster must not
        // hold one connection each (it would wait forever).
        let cluster = self
            .list
            .iter()
            .find(|(s, _)| s == schema)
            .map(|(_, c)| c.clone())
            .ok_or_else(|| {
                SprigError::internal(format!("{schema} isn't a shard of this deploy"))
            })?;
        let key = cluster.to_string();
        if !self.conns.contains_key(&key) {
            self.take_all().await?;
        }
        self.conns
            .get_mut(&key)
            .map(|c| c as &mut (dyn Session + 'static))
            .ok_or_else(|| SprigError::internal("no shard session"))
    }
}

impl CtlShards {
    /// A connection to every cluster of the list not held yet, in cluster
    /// order.
    async fn take_all(&mut self) -> Result<()> {
        let mut clusters: Vec<ClusterId> = self.list.iter().map(|(_, c)| c.clone()).collect();
        clusters.sort_by_key(ToString::to_string);
        clusters.dedup();
        for cluster in clusters {
            let key = cluster.to_string();
            if self.conns.contains_key(&key) {
                continue;
            }
            let pool = self.locator.ctl_pool(&cluster).await?;
            let conn = pool.get_conn_bounded().await.map_err(conn_err)?;
            self.conns.insert(key, SharedCtl::new(conn));
        }
        Ok(())
    }

    /// The connection to `cluster` (one of the list's), to share with the
    /// deploy's other sessions.
    pub(super) async fn shared(&mut self, cluster: &ClusterId) -> Result<SharedCtl> {
        let key = cluster.to_string();
        if !self.conns.contains_key(&key) {
            self.take_all().await?;
        }
        self.conns
            .get(&key)
            .cloned()
            .ok_or_else(|| SprigError::internal(format!("{cluster} isn't a shard's cluster")))
    }

    /// Gives every connection back; the next session takes them again.
    pub(super) fn release(&mut self) {
        self.conns.clear();
    }

    /// `@@gtid_binlog_pos` of the first shard's cluster.
    async fn position(&mut self) -> Result<(ClusterId, String)> {
        let (schema, cluster) = self
            .list
            .first()
            .cloned()
            .ok_or_else(|| SprigError::internal("no shard"))?;
        let s = self.session(&schema).await?;
        let pos = s
            .rows("SELECT @@gtid_binlog_pos")
            .await?
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next().flatten());
        Ok((cluster, pos.unwrap_or_default()))
    }
}

fn now_ms() -> u64 {
    sprig_logic::triggers::maria::now_ms()
}

fn denied(action: &str, user: &str, what: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!("{action} command denied to user '{user}' for table '{what}'"),
    )
}

/// A sink that keeps one statement's outcome (the `CHECK`'s `ALTER`).
struct Outcome {
    wire: Wire,
    error: Option<SprigError>,
    done: bool,
}

#[async_trait]
impl ResultSink for Outcome {
    fn wire(&self) -> Wire {
        self.wire
    }
    async fn begin_rows(
        &mut self,
        _c: &[sprig_proto_mysql::ColumnDef],
        _f: RowFormat,
    ) -> Result<()> {
        Ok(())
    }
    async fn row(&mut self, _v: crate::pipeline::RowValues<'_>) -> Result<()> {
        Ok(())
    }
    async fn raw_row(&mut self, _p: Bytes) -> Result<()> {
        Ok(())
    }
    async fn end_rows(&mut self, _e: crate::pipeline::EndOfRows) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn ok(&mut self, _ok: OkPacket) -> Result<()> {
        self.done = true;
        Ok(())
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        self.error = Some(SprigError {
            kind: err.kind,
            message: err.message.clone(),
            retryable: err.retryable,
            source: None,
        });
        self.done = true;
        Ok(())
    }
    async fn column_list(
        &mut self,
        _c: &[sprig_proto_mysql::ColumnDef],
        _e: crate::pipeline::EndOfRows,
    ) -> Result<()> {
        Ok(())
    }
    async fn local_infile_request(&mut self, _f: &[u8]) -> Result<()> {
        Err(SprigError::internal("no LOAD DATA here"))
    }
    async fn local_infile_read(&mut self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.done
    }
}

/// A bound deploy between its steps ([`PassSession::logic_prepare_bound`],
/// then an `ALTER TABLE` when it has one, then
/// [`PassSession::logic_finish_bound`]).
struct BoundDeploy {
    pending: service::Pending,
    shards: CtlShards,
    ep: sprig_catalog::store::CatalogEndpoint,
    catalog_version: u64,
    svc: Arc<crate::tenant::TenantServices>,
    tenant: sprig_common::ids::TenantId,
    first: String,
    kind: Kind,
    name: String,
}

impl BoundDeploy {
    /// Undoes the new version (its routines and triggers dropped on every
    /// shard, the version `failed`); the old one stays active.
    async fn abort(&mut self) {
        if let Ok(mut conn) = self.ep.pool.get_conn_bounded().await {
            service::abort(
                &mut conn,
                self.ep.store.schema(),
                &self.pending,
                &mut self.shards,
            )
            .await;
        }
    }
}

impl PassSession {
    /// Answers a bound declaration's outcome: OK with its notes, or the
    /// error.
    async fn answer(
        &mut self,
        r: Result<Vec<String>>,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Step> {
        match r {
            Ok(notes) => {
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
                Ok(Step {
                    errored: false,
                    status: Some(status),
                })
            }
            Err(e) => {
                let shown = SprigError {
                    kind: e.kind,
                    message: e.message.clone(),
                    retryable: e.retryable,
                    source: None,
                };
                out.error(&scrub_typed(shown)).await?;
                Ok(Step {
                    errored: true,
                    status: None,
                })
            }
        }
    }

    /// `CREATE VALIDATION | TRIGGER ... AFTER COMMIT | SCHEDULE` (see the
    /// module docs). Returns the client's notes.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn logic_deploy_bound(
        &mut self,
        cx: &mut ExecCtx<'_>,
        decl: Declaration,
    ) -> Result<Vec<String>> {
        self.logic_deploy_bound_as(cx, decl, None).await
    }

    /// [`Self::logic_deploy_bound`]; with `owner`, SprigDB deploys it for
    /// that user (the module's owner, its `DEFINER`): a validation or
    /// trigger recompiled after its table's columns changed type. Rights
    /// aren't checked then.
    async fn logic_deploy_bound_as(
        &mut self,
        cx: &mut ExecCtx<'_>,
        decl: Declaration,
        owner: Option<sprig_common::ids::UserId>,
    ) -> Result<Vec<String>> {
        let mut d = self.logic_prepare_bound(&decl, owner, None).await?;
        if let Some(alter) = d.pending.alter.clone() {
            self.logic_bound_alter(cx, &mut d, &alter).await?;
        }
        Self::logic_finish_bound(d).await
    }

    /// A bound deploy's first step: the checks, then the new version's
    /// routines (and a validation's `BEFORE` triggers) on every shard, next
    /// to the old version's (`sprig_logic::service::deploy_bound`). With
    /// `columns`, it is compiled for those columns rather than the table's
    /// columns now (an `ALTER TABLE` deployed with it).
    #[allow(clippy::too_many_lines)]
    async fn logic_prepare_bound(
        &mut self,
        decl: &Declaration,
        owner: Option<sprig_common::ids::UserId>,
        columns: Option<&sprig_logic::compiler::StaticColumns>,
    ) -> Result<BoundDeploy> {
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Err(SprigError::internal("no tenant"));
        };
        let db = decl
            .table
            .as_ref()
            .and_then(|t| t.db.clone())
            .or_else(|| decl.db.clone())
            .or(current)
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(&db, lctn);
        let row = catalog.database(&db_key).cloned().ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })?;
        let user = match owner {
            Some(u) => u,
            None => ts
                .principal
                .user_id
                .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?,
        };
        let user_name = ts.principal.user.clone();
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        // SprigDB's own redeploy for the owner checks no rights.
        let unchecked = owner.is_some() || grants.unrestricted();
        let table = if let Some(t) = &decl.table {
            {
                let o = catalog
                    .object(
                        row.db_id,
                        Namespace::Relation,
                        &object_key(ObjectKind::Table, &t.name, lctn),
                    )
                    .filter(|o| o.state == ObjectState::Active)
                    .cloned()
                    .ok_or_else(|| {
                        SprigError::standard(
                            1146,
                            *b"42S02",
                            format!("Table '{db}.{}' doesn't exist", t.name),
                        )
                    })?;
                if !unchecked
                    && !grants.allows(
                        Action::Trigger,
                        AuthzTarget::Object {
                            db_key: &db_key,
                            db: row.db_id,
                            object: o.object_id,
                        },
                    )
                {
                    return Err(denied("TRIGGER", &user_name, &t.name));
                }
                // An after-commit trigger's code gets the changed rows'
                // images, as its definer: it needs SELECT on the table (as
                // MySQL asks of a trigger reading OLD and NEW). When policies
                // on the table bind the definer, the worker checks each
                // event's rows against them (refused before). Validations
                // run in the writer's own statement and see only its row, so
                // TRIGGER stays enough for them.
                if decl.kind == Kind::Trigger && !unchecked {
                    let on = AuthzTarget::Object {
                        db_key: &db_key,
                        db: row.db_id,
                        object: o.object_id,
                    };
                    if !grants.allows(Action::Select, on) {
                        return Err(denied("SELECT", &user_name, &t.name));
                    }
                }
                Some((t.name.clone(), o.object_id.0))
            }
        } else {
            {
                if !unchecked
                    && !grants.allows(
                        Action::CreateRoutine,
                        AuthzTarget::Database { db_key: &db_key },
                    )
                {
                    return Err(SprigError::standard(
                        1044,
                        *b"42000",
                        format!("Access denied for user '{user_name}' to database '{db}'"),
                    ));
                }
                None
            }
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let streams = super::logic::logic_streams(ts, &catalog, row.db_id);
        // The DDL layout's shards: the table's group, or every shard of the
        // database; one shard (the template) for unsharded databases.
        let template = ts.schema_of(Some(&db));
        let layout = super::router::ddl_layout(ts);
        let shard_list: Vec<(String, ClusterId)> = {
            let from_layout = match (&layout, &template) {
                (Some(l), Some(t)) => l(t, table.as_ref().map(|(n, _)| n.as_str())),
                (Some(l), None) => {
                    let first = sprig_catalog::ddl::shard_schemas(tenant, &row)
                        .first()
                        .map(ToString::to_string);
                    first.and_then(|f| l(&f, table.as_ref().map(|(n, _)| n.as_str())))
                }
                _ => None,
            };
            if let Some(v) = from_layout.filter(|v| !v.is_empty()) {
                v
            } else {
                {
                    let t = template.clone().ok_or_else(|| {
                        SprigError::new(
                            SprigCode::LogicUnsupported,
                            format!("database {db} has no shard this gateway can find"),
                        )
                    })?;
                    vec![(t, self.target.cluster.clone())]
                }
            }
        };
        let single_shard = shard_list.len() == 1;
        let mut shards = CtlShards {
            list: shard_list,
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let ep = svc.locator.writer(tenant).await?;
        let mut conn = ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let account = sprig_auth::authz::backend::account(tenant, user);
        let definer = format!("`{}`@`%`", account.replace('`', "``"));
        let first = shards.list[0].0.clone();
        // A validation's wall budget, which its `BEFORE` triggers pass to
        // the entry procedure: `logic.function_budget_ms` for the database.
        let budget_ms = svc.settings.get(
            &sprig_logic::settings::FUNCTION_BUDGET_MS,
            &catalog.setting_ctx(&svc.dc).with_db(row.db_id),
            Some(&catalog.settings),
        );
        let pending = service::deploy_bound(
            decl,
            Target {
                catalog: &mut conn,
                catalog_schema: ep.store.schema(),
                catalog_version: catalog.version,
                db_id: row.db_id.0,
                shard_schema: &first,
                single_shard,
                user_id: user.0,
                budget_ms,
                max_loop_iterations: MAX_LOOP_ITERATIONS,
                compile_max_cost: COMPILE_MAX_COST,
            },
            &Bind {
                db_name: &row.name,
                table: table.as_ref().map(|(n, o)| (n.as_str(), *o)),
                definer,
                now_ms: now_ms(),
                streams,
                columns,
            },
            &mut shards,
        )
        .await?;
        Ok(BoundDeploy {
            pending,
            shards,
            ep,
            catalog_version: catalog.version,
            svc,
            tenant,
            first,
            kind: decl.kind,
            name: decl.name.clone(),
        })
    }

    /// Runs `alter` (the version's `ALTER TABLE`: its `CHECK`, or the old
    /// `CHECK` dropped) through this session's `ALTER TABLE` path, as if
    /// the client sent it; on an error the new version is undone
    /// ([`service::abort`]).
    async fn logic_bound_alter(
        &mut self,
        cx: &mut ExecCtx<'_>,
        d: &mut BoundDeploy,
        alter: &str,
    ) -> Result<()> {
        // The ALTER is a catalog change of its own, which takes
        // connections from the same pool (4 per cluster), and the shards'
        // `sprig_ctl` ones go back too (the ALTER's DDL takes more from
        // their pool); the finish takes them again after the catalog's,
        // in that order.
        d.shards.release();
        let err = self.run_alter(cx, alter).await;
        if let Some(e) = err {
            d.abort().await;
            return Err(sprig_logic::triggers::map_check_error(e));
        }
        Ok(())
    }

    /// Runs `alter` through this session's `ALTER TABLE` path (the DDL
    /// engine), as if the client sent it: its error, if any.
    async fn run_alter(&mut self, cx: &mut ExecCtx<'_>, alter: &str) -> Option<SprigError> {
        let mut sink = Outcome {
            wire: Wire {
                persona: self.persona,
                caps: cx.proto.caps,
            },
            error: None,
            done: false,
        };
        let text = Bytes::from(alter.to_owned().into_bytes());
        let attrs = QueryAttrs::default();
        let r =
            Box::pin(self.tenant_text(cx, &text, &attrs, false, RowFormat::Text, &mut sink)).await;
        match r {
            Err(e) => Some(e),
            Ok(_) => sink.error.take(),
        }
    }

    /// A bound deploy's last step: the version becomes active, the old
    /// version's triggers go ([`service::finish`]), and a new trigger's
    /// feed subscription or a schedule's index is written. The client's
    /// notes.
    async fn logic_finish_bound(mut d: BoundDeploy) -> Result<Vec<String>> {
        let mut conn = d.ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        service::finish(
            &mut conn,
            d.ep.store.schema(),
            d.catalog_version,
            &d.pending,
            &mut d.shards,
        )
        .await?;
        let BoundDeploy {
            pending,
            mut shards,
            svc,
            tenant,
            first,
            kind,
            name,
            ..
        } = d;
        let mut notes = pending.notes();
        match kind {
            Kind::Trigger => {
                if let Some(b) = &pending.binding {
                    let (cluster, pos) = shards.position().await?;
                    let fresh = sprig_logic::triggers::maria::register(
                        svc.locator.dc_store(),
                        tenant,
                        b.binding_id,
                        pending.module_id,
                        cluster.clone(),
                        &pos,
                    )
                    .await?;
                    if fresh {
                        notes.push(format!("subscribed from {cluster} at {pos}"));
                    }
                }
            }
            Kind::Schedule => {
                if let Some(s) = &pending.schedule {
                    sprig_logic::schedule::put_index(
                        svc.locator.dc_store().as_ref(),
                        &sprig_logic::schedule::ScheduleIndex {
                            tenant,
                            module_id: pending.module_id,
                            schema: first.clone(),
                            next_fire_ms: s.next_fire_ms,
                        },
                    )
                    .await?;
                }
            }
            _ => {}
        }
        tracing::info!(module = pending.module_id, version = pending.version, kind = kind.as_str(), name = %name, "logic version deployed");
        Ok(notes)
    }

    /// `ALTER TABLE t` changing existing columns under a compiled
    /// validation or after-commit trigger on `t`
    /// ([`sprig_logic::dependent`]): refused (39481) when the code would
    /// no longer compile (a column it reads is dropped or renamed); when a
    /// column it reads only changes type, the statement runs here and each
    /// such validation or trigger is then recompiled and deployed again as
    /// a new version, for its owner. `None`: the statement goes on.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn tenant_column_change(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        // The statement this check runs itself, below.
        if self.column_change_running {
            return Ok(None);
        }
        let sql = String::from_utf8_lossy(text).into_owned();
        let Some(change) = sprig_logic::dependent::parse(&sql) else {
            return Ok(None);
        };
        let current = self.current_logical();
        let cluster = self.target.cluster.clone();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let Some(db) = change.db.clone().or(current) else {
            return Ok(None);
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let Some(row) = catalog.database(&database_key(&db, lctn)).cloned() else {
            return Ok(None);
        };
        let Some(obj) = catalog
            .object(
                row.db_id,
                Namespace::Relation,
                &object_key(ObjectKind::Table, &change.table, lctn),
            )
            .map(|o| o.object_id.0)
        else {
            return Ok(None);
        };
        let Some(template) = ts.schema_of(Some(&db)) else {
            return Ok(None);
        };
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let Ok(ep) = svc.locator.writer(tenant).await else {
            return Ok(None);
        };
        let Ok(mut conn) = ep.pool.get_conn_bounded().await else {
            return Ok(None);
        };
        let store = LogicStore::new(ep.store.schema());
        // No logic catalog yet: nothing is bound.
        let Ok(modules) = store.modules(&mut conn, Some(row.db_id.0)).await else {
            return Ok(None);
        };
        let mut bound = Vec::new();
        for m in modules {
            if !matches!(m.kind.as_str(), "validation" | "trigger") || m.state != "active" {
                continue;
            }
            let Some(v) = m.active_version else {
                continue;
            };
            let Ok(Some(b)) = store.binding(&mut conn, m.module_id).await else {
                continue;
            };
            if b.object_id != Some(obj)
                || !matches!(b.timing.as_str(), "before_write" | "after_commit")
            {
                continue;
            }
            let source = store.source(&mut conn, m.module_id, v).await?;
            bound.push((m, source, b));
        }
        drop(conn);
        if bound.is_empty() {
            return Ok(None);
        }
        // Today's columns, read as a deploy reads them.
        let mut shards = CtlShards {
            list: vec![(template.clone(), cluster.clone())],
            conns: HashMap::new(),
            locator: Arc::clone(&svc.locator),
        };
        let before = {
            let mut session = shards.shared(&cluster).await?;
            service::columns(&mut session, &template).await
        };
        shards.release();
        let before = before?;
        let after = sprig_logic::dependent::predict(&before, &change.table, &change.ops);
        let mut why = Vec::new();
        let mut recompile = Vec::new();
        for (m, source, b) in bound {
            let runtime = sprig_logic::compiler::Runtime::parse(&m.runtime)
                .unwrap_or(sprig_logic::compiler::Runtime::Kirun);
            match sprig_logic::dependent::rebind(
                runtime,
                &m.name,
                &source,
                &change.table,
                &before,
                &after,
            ) {
                Rebind::Same => {}
                Rebind::Refuse(w) => why.push(format!("{} {}: {w}", m.kind, m.name)),
                Rebind::Recompile(w) => recompile.push((m, source, b, runtime, w)),
            }
        }
        if !why.is_empty() {
            let e = SprigError::new(
                SprigCode::DdlUnsupported,
                format!(
                    "ALTER TABLE {} would change columns compiled logic reads: {}; drop it, \
                     change the table, and create it again",
                    change.table,
                    why.join("; ")
                ),
            );
            return self.answer(Err(e), status, out).await.map(Some);
        }
        if recompile.is_empty() {
            return Ok(None);
        }
        // One DDL job: each new version first, compiled for the columns
        // the statement leaves, next to the old one (a validation's BEFORE
        // triggers run both, which is stricter, never weaker); a CHECK
        // swap rides in the statement itself; an after-commit trigger's
        // new version is active before the statement (an event before it
        // reaches the new types exactly, the old column's values as the
        // statement converts them); then the statement; then the old
        // validations' triggers go. A write after the statement is never
        // judged only by a version compiled for the old types.
        let mut decls = Vec::new();
        for (m, source, b, runtime, w) in recompile {
            let kind = if m.kind == "validation" {
                Kind::Validation
            } else {
                Kind::Trigger
            };
            let decl = Declaration {
                kind,
                or_replace: true,
                db: Some(db.clone()),
                name: m.name.clone(),
                runtime,
                definer_security: m.security == "definer",
                deterministic: false,
                exposure: Vec::new(),
                tier: match m.tier_pref.as_str() {
                    "mariadb" => sprig_logic::declare::TierPref::Mariadb,
                    "engine" => sprig_logic::declare::TierPref::Engine,
                    _ => sprig_logic::declare::TierPref::Auto,
                },
                body: source,
                table: Some(sprig_logic::declare::TableRef {
                    db: Some(db.clone()),
                    name: change.table.clone(),
                }),
                source: None,
                events: b
                    .events
                    .split(',')
                    .filter(|e| !e.is_empty())
                    .map(str::to_owned)
                    .collect(),
                when_fields: b.when_fields.clone(),
                schedule: None,
                params: Vec::new(),
                returns: None,
                rollout: None,
                rollout_key: None,
            };
            decls.push((decl, sprig_common::ids::UserId(m.created_by), m.kind, w));
        }
        // 1. The new versions' routines and triggers on every shard.
        let mut prepared: Vec<(BoundDeploy, String, String)> = Vec::new();
        for (decl, owner, kind, w) in &decls {
            match Box::pin(self.logic_prepare_bound(decl, Some(*owner), Some(&after))).await {
                Ok(d) => prepared.push((d, kind.clone(), w.clone())),
                Err(e) => {
                    for (d, ..) in &mut prepared {
                        d.abort().await;
                    }
                    let e = SprigError::new(
                        SprigCode::DdlUnsupported,
                        format!(
                            "ALTER TABLE {} would change columns compiled logic reads, and {kind} \
                             {} couldn't be compiled again for them ({}); nothing changed",
                            change.table, decl.name, e.message
                        ),
                    );
                    return self.answer(Err(e), status, out).await.map(Some);
                }
            }
        }
        // 2. The statement, with every CHECK swap in it.
        let prefix = format!("ALTER TABLE {}.{} ", bq(&db), bq(&change.table));
        let mut statement = sql.trim().trim_end_matches(';').trim_end().to_owned();
        for (d, ..) in &prepared {
            if let Some(a) = &d.pending.alter {
                match a.strip_prefix(&prefix) {
                    Some(clauses) => {
                        statement.push_str(", ");
                        statement.push_str(clauses);
                    }
                    None => {
                        return Err(SprigError::internal(format!(
                            "a validation's ALTER TABLE names another table: {a}"
                        )));
                    }
                }
            }
        }
        // 3. After-commit triggers switch first; the worker reads a
        // binding's active version at most this stale.
        let mut switched = Vec::new();
        let mut rest = Vec::new();
        for (i, (d, kind, w)) in prepared.into_iter().enumerate() {
            if d.kind == Kind::Trigger {
                let name = d.name.clone();
                switched.push((Self::logic_finish_bound(d).await, kind, w, name, i));
            } else {
                rest.push((d, kind, w));
            }
        }
        if !switched.is_empty() {
            tokio::time::sleep(TRIGGER_VERSION_STALE).await;
        }
        for (d, ..) in &mut rest {
            d.shards.release();
        }
        self.column_change_running = true;
        let err = self.run_alter(cx, &statement).await;
        self.column_change_running = false;
        if let Some(e) = err {
            // Nothing changed: the new validations go; a trigger already
            // switched is deployed again for the columns as they are.
            for (d, ..) in &mut rest {
                d.abort().await;
            }
            for (r, kind, _, name, i) in &switched {
                if r.is_err() {
                    continue;
                }
                let (decl, owner, ..) = &decls[*i];
                tracing::info!(%kind, %name, table = %change.table, "a column change failed after its trigger switched: deploying it again for the columns as they are");
                if let Err(x) =
                    Box::pin(self.logic_deploy_bound_as(cx, decl.clone(), Some(*owner))).await
                {
                    tracing::warn!(%kind, %name, error = %x, "a trigger couldn't be deployed again after a failed column change");
                }
            }
            let e = sprig_logic::triggers::map_check_error(e);
            return self.answer(Err(e), status, out).await.map(Some);
        }
        // 4. The validations' new versions become active; their old
        // triggers go.
        let mut notes = Vec::new();
        let finished = {
            let mut v = Vec::new();
            for (d, kind, w) in rest {
                let name = d.name.clone();
                v.push((Self::logic_finish_bound(d).await, kind, w, name));
            }
            v
        };
        for (r, kind, w, name) in switched
            .into_iter()
            .map(|(r, k, w, n, _)| (r, k, w, n))
            .chain(finished)
        {
            match r {
                Ok(_) => {
                    tracing::info!(kind = %kind, name = %name, table = %change.table, "recompiled for its table's new column types");
                    notes.push(format!(
                        "{kind} {name} was recompiled for the new column types and deployed as a \
                         new version ({w})"
                    ));
                }
                Err(e) => {
                    tracing::warn!(kind = %kind, name = %name, table = %change.table, error = %e, "activating a recompiled version failed");
                    notes.push(format!(
                        "the table changed, but activating the recompiled {kind} {name} failed \
                         ({}); its previous version still runs: deploy it again",
                        e.message
                    ));
                }
            }
        }
        if let Some(ts) = self.tenant.as_deref() {
            super::logic_rollout::invalidate(ts.tenant.0);
        }
        self.answer(Ok(notes), status, out).await.map(Some)
    }

    /// `ALTER TABLE t ADD c TYPE AS (f(args))` where `f` is a logic
    /// function (see the module docs). `None`: not a computed field, the
    /// statement goes on as ordinary DDL.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub(super) async fn tenant_computed(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let sql = String::from_utf8_lossy(text).into_owned();
        let Some(call) = enforce::parse_computed(&sql) else {
            return Ok(None);
        };
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        if ts.principal.must_change_password {
            return Ok(None);
        }
        let Some(db) = call
            .function_db
            .clone()
            .or_else(|| call.db.clone())
            .or(current)
        else {
            return Ok(None);
        };
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let Some(row) = catalog.database(&database_key(&db, lctn)).cloned() else {
            return Ok(None);
        };
        let table_db = call.db.clone().unwrap_or_else(|| db.clone());
        let table_obj = catalog
            .database(&database_key(&table_db, lctn))
            .and_then(|d| {
                catalog.object(
                    d.db_id,
                    Namespace::Relation,
                    &object_key(ObjectKind::Table, &call.table, lctn),
                )
            })
            .map(|o| o.object_id.0);
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let Ok(ep) = svc.locator.writer(tenant).await else {
            return Ok(None);
        };
        let Ok(mut conn) = ep.pool.get_conn_bounded().await else {
            return Ok(None);
        };
        let store = LogicStore::new(ep.store.schema());
        let mut module = None;
        for kind in ["function", "procedure"] {
            match store
                .module(&mut conn, row.db_id.0, kind, &call.function)
                .await
            {
                Ok(Some(m)) => {
                    module = Some(m);
                    break;
                }
                Ok(None) => {}
                // No logic catalog yet: not a logic function.
                Err(_) => return Ok(None),
            }
        }
        let Some(module) = module else {
            return Ok(None);
        };
        let r: Result<String> = async {
            let v = module.active_version.ok_or_else(|| {
                SprigError::new(
                    SprigCode::LogicNotFound,
                    format!("{} has no active version", module.name),
                )
            })?;
            let source = store.source(&mut conn, module.module_id, v).await?;
            // The version's signature: JavaScript compiles only with its
            // declared parameter types.
            let (sig, _) = store
                .version_signature(&mut conn, module.module_id, v)
                .await?;
            let expr =
                service::computed_column(&module.name, &module.runtime, &source, &sig, &call)?;
            Ok(enforce::rewrite_computed(&sql, &call, &expr))
        }
        .await;
        // The DDL below takes catalog connections from the same pool.
        drop(conn);
        let rewritten = match r {
            Ok(t) => t,
            Err(e) => return self.answer(Err(e), status, out).await.map(Some),
        };
        tracing::info!(function = %call.function, column = %call.column, "computed field as a generated column");
        let step = Box::pin(self.tenant_text(
            cx,
            &Bytes::from(rewritten.into_bytes()),
            attrs,
            more,
            format,
            out,
        ))
        .await?;
        if !step.errored {
            let b = BindingRow {
                binding_id: 0,
                module_id: module.module_id,
                object_id: table_obj,
                column_name: Some(call.column.clone()),
                timing: if call.stored { "on_write" } else { "on_read" }.to_owned(),
                events: String::new(),
                when_fields: Vec::new(),
                enforcement: Some("generated".to_owned()),
                state: "active".to_owned(),
            };
            let put = async {
                let mut conn = ep.pool.get_conn_bounded().await.map_err(|e| {
                    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable")
                        .with_source(e)
                })?;
                store.put_binding(&mut conn, &b).await
            };
            if let Err(e) = put.await {
                tracing::warn!(error = %e, "computed field's binding not recorded");
            }
        }
        Ok(Some(step))
    }
}

/// A validation's `CHECK` failure as 39863. A constraint
/// this gateway hasn't seen yet loads the tenant's `CHECK` messages in the
/// background, so later failures show the validation's own message.
pub(super) fn map_validation(
    ts: &crate::tenant::session::TenantSession,
    e: SprigError,
) -> SprigError {
    let mapped = sprig_logic::triggers::map_check_error(e);
    if mapped.code() == Some(SprigCode::LogicValidationFailed)
        && mapped
            .message
            .starts_with("rejected by a validation (constraint")
    {
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        tokio::spawn(async move {
            let Ok(ep) = svc.locator.writer(tenant).await else {
                return;
            };
            let Ok(mut c) = ep.pool.get_conn_bounded().await else {
                return;
            };
            if let Err(e) = sprig_logic::triggers::load_checks(&mut c, ep.store.schema()).await {
                tracing::debug!(error = %e, "validation messages not loaded");
            }
        });
    }
    mapped
}

/// Loads the tenant's validation messages once per process (and again
/// after a failure named a constraint this process didn't know).
pub(super) fn warm_validations(ts: &crate::tenant::session::TenantSession) {
    if !sprig_logic::triggers::checks_wanted(ts.tenant.0) {
        return;
    }
    let svc = Arc::clone(ts.services());
    let tenant = ts.tenant;
    tokio::spawn(async move {
        let Ok(ep) = svc.locator.writer(tenant).await else {
            return;
        };
        let Ok(mut c) = ep.pool.get_conn_bounded().await else {
            return;
        };
        // No logic catalog yet is fine: nothing to load.
        let _ = sprig_logic::triggers::load_checks(&mut c, ep.store.schema()).await;
    });
}
