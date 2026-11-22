//! Event streams in a tenant session: stream DDL, publishing and the SQL
//! reads.
//!
//! Before the name layer's plan, each statement of a tenant text goes to
//! [`sprig_streams::sql::handle`] with the tenant's streams from the
//! session's catalog snapshot. Stream DDL runs here: physical tables on the
//! shards as `sprig_ctl` (like logic deploys), the catalog row in
//! one versioned change, and the stream's feed subscription. A publish or a
//! read becomes a statement on the outbox or the log, which runs through
//! this same session ([`PassSession::tenant_text`]) in internal mode, as
//! typed-table plans do: logical names, the authorization checks, the
//! session's connection and transaction, so a publish commits or rolls back with the
//! client's other writes.
//!
//! After `ALTER TABLE t SPRIG SET (stream.change_topic = ...)` succeeds,
//! [`PassSession::streams_after_settings`] creates or drops `t.changes`.

mod select;

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use sprig_auth::authz::{Action, Target};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::pool_wait::Bounded as _;
use sprig_catalog::rows::SettingsTx;
use sprig_catalog::store::{Namespace, ObjectKind, ObjectState, SettingRow, SettingScope};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, TenantCatalog};
use sprig_common::ids::{ClusterId, DbId, ObjectId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_proto_mysql::{ColumnDef, StatusFlags};
use sprig_streams::sql::{
    Ctx, Handled, Host, PhysSlot, Privilege, StmtSettings, StreamsChange, TableInfo,
};
use sprig_streams::{ShardSlot, StreamDef, StreamsState};

use super::PassSession;
use super::tenant::Step;
use crate::pipeline::{EndOfRows, ExecCtx, ResultSink, RowFormat, RowValues, Wire};

const MODULE: &str = sprig_streams::MODULE;

fn slot_of(schema: &str, cluster: ClusterId) -> Option<PhysSlot> {
    match schema.parse::<SchemaName>().ok()? {
        SchemaName::Shard { group, shard, .. } => Some(PhysSlot {
            slot: ShardSlot { group, shard },
            schema: schema.to_owned(),
            cluster,
        }),
        _ => None,
    }
}

/// The stream layer's view of a tenant session.
struct GwHost<'s> {
    sess: &'s mut PassSession,
    cat: Arc<TenantCatalog>,
    migrated: bool,
}

impl GwHost<'_> {
    fn ts(&self) -> Option<&crate::tenant::session::TenantSession> {
        self.sess.tenant.as_deref()
    }

    fn lctn(&self) -> bool {
        self.ts()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names)
    }

    fn db_name(&self, db: DbId) -> Option<String> {
        self.cat.databases.get(db).map(|d| d.name.clone())
    }

    fn action(p: Privilege) -> Action {
        match p {
            Privilege::Select => Action::Select,
            Privilege::Insert => Action::Insert,
            Privilege::Create => Action::Create,
            Privilege::Alter => Action::Alter,
            Privilege::Drop => Action::Drop,
        }
    }

    async fn ctl(&self, cluster: &ClusterId) -> Result<mysql_async::Conn> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let pool = ts.services().locator.ctl_pool(cluster).await?;
        pool.get_conn_bounded()
            .await
            .map_err(sprig_streams::maria::map_err)
    }
}

#[async_trait]
impl Host for GwHost<'_> {
    fn tenant(&self) -> TenantId {
        self.cat.tenant
    }

    fn db_id(&self, name: &str) -> Option<DbId> {
        self.cat
            .database(&database_key(name, self.lctn()))
            .map(|d| d.db_id)
    }

    fn table(&self, db: DbId, name: &str) -> Option<TableInfo> {
        let o = self
            .cat
            .object(
                db,
                Namespace::Relation,
                &object_key(ObjectKind::Table, name, self.lctn()),
            )
            .filter(|o| o.state == ObjectState::Active && o.kind == ObjectKind::Table)?;
        let ts = self.ts()?;
        let template = self.template(db)?;
        let from_layout = super::router::ddl_layout(ts)
            .and_then(|l| l(&template.schema, Some(&o.name)))
            .filter(|v| !v.is_empty());
        let slots = match from_layout {
            Some(v) => v.into_iter().filter_map(|(s, c)| slot_of(&s, c)).collect(),
            None => vec![template],
        };
        Some(TableInfo {
            object: o.object_id,
            name: o.name.clone(),
            slots,
        })
    }

    fn template(&self, db: DbId) -> Option<PhysSlot> {
        let name = self.db_name(db)?;
        let schema = self.ts()?.schema_of(Some(&name))?;
        slot_of(&schema, self.sess.target.cluster.clone())
    }

    fn allowed(&self, p: Privilege, db_name: &str, object: Option<ObjectId>, name: &str) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let grants = crate::tenant::authz::privileges(ts, &self.cat);
        if grants.unrestricted() {
            return true;
        }
        let lctn = self.lctn();
        let db_key = database_key(db_name, lctn);
        let a = Self::action(p);
        if let (Some(o), Some(db)) = (object, self.db_id(db_name)) {
            return grants.allows(
                a,
                Target::Object {
                    db_key: &db_key,
                    db,
                    object: o,
                },
            );
        }
        let name_key = object_key(ObjectKind::Table, name, lctn);
        grants.allows(
            a,
            Target::Named {
                db_key: &db_key,
                name_key: &name_key,
            },
        ) || grants.allows(a, Target::Database { db_key: &db_key })
    }

    fn row_filtered(&self, db_name: &str, object: ObjectId) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let Some(db) = self.db_id(db_name) else {
            return false;
        };
        let db_key = database_key(db_name, self.lctn());
        crate::tenant::authz::privileges(ts, &self.cat)
            .row_filter(
                Target::Object {
                    db_key: &db_key,
                    db,
                    object,
                },
                Action::Select,
            )
            .is_some()
    }

    async fn change_view(&mut self, def: &StreamDef) -> Result<()> {
        // The change topic's row-level security filter view.
        Box::pin(self.sess.deploy_change_view(def)).await
    }

    async fn change_filter(
        &mut self,
        db_name: &str,
        def: &StreamDef,
    ) -> Result<Option<sprig_streams::read::RecordFilter>> {
        // The records whose images this session's
        // policies show.
        let (Some(object), Some(db)) = (def.source_object, self.db_id(db_name)) else {
            return Err(crate::tenant::rls_streams::bound(
                def.source_name.as_deref().unwrap_or_default(),
                &def.name,
            ));
        };
        let obj = self
            .cat
            .objects
            .get(object)
            .cloned()
            .ok_or_else(|| SprigError::internal("a change topic's table is gone"))?;
        let slot = self
            .table(db, &obj.name)
            .and_then(|t| t.slots.into_iter().next())
            .ok_or_else(|| SprigError::internal("a change topic's table has no shard"))?;
        let (lex, _) = self.sess.lex_opts();
        let current = self.sess.current_logical();
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        let columns = crate::tenant::rls_streams::filter_columns(
            ts.services(),
            self.cat.tenant.0,
            &obj,
            &slot.cluster,
            &slot.schema,
        )
        .await?;
        crate::tenant::rls_streams::record_filter(
            ts,
            &self.cat,
            db_name,
            object,
            &columns,
            lex,
            current.as_deref(),
        )
        .await
    }

    fn settings(&self, db: DbId) -> StmtSettings {
        use sprig_streams::settings as s;
        let Some(ts) = self.ts() else {
            return StmtSettings {
                partitions_default: 12,
                max_message_bytes: s::MAX_MESSAGE_BYTES.default,
            };
        };
        let svc = ts.services();
        let ctx = self.cat.setting_ctx(&svc.dc).with_db(db);
        let get = |k: &'static sprig_settings::Setting<u64>| {
            svc.settings.get(k, &ctx, Some(&self.cat.settings))
        };
        StmtSettings {
            partitions_default: u32::try_from(get(&s::PARTITIONS_DEFAULT)).unwrap_or(12),
            max_message_bytes: get(&s::MAX_MESSAGE_BYTES),
        }
    }

    fn stream_max_bytes(&self, def: &StreamDef) -> u64 {
        use sprig_streams::settings as s;
        let Some(ts) = self.ts() else {
            return s::MAX_MESSAGE_BYTES.default;
        };
        let svc = ts.services();
        // The stream's own scope (`ALTER STREAM ... SET`), then its
        // database's and up, as Kafka's produce reads it.
        let ctx = self
            .cat
            .object_ctx(&svc.dc, ObjectId(def.id.0))
            .with_db(def.db);
        svc.settings
            .get(&s::MAX_MESSAGE_BYTES, &ctx, Some(&self.cat.settings))
    }

    fn change_topic_on(&self, _db: DbId, object: ObjectId) -> bool {
        let Some(ts) = self.ts() else {
            return false;
        };
        let svc = ts.services();
        let ctx = self.cat.object_ctx(&svc.dc, object);
        svc.settings.get(
            &sprig_streams::settings::CHANGE_TOPIC,
            &ctx,
            Some(&self.cat.settings),
        )
    }

    fn user(&self) -> (String, String) {
        self.ts().map_or_else(Default::default, |t| {
            (t.principal.user.clone(), t.principal.client_ip.to_string())
        })
    }

    async fn ddl(&mut self, slot: &PhysSlot, statements: &[String]) -> Result<()> {
        let mut c = self.ctl(&slot.cluster).await?;
        for s in statements {
            c.query_drop(s.as_str())
                .await
                .map_err(sprig_streams::maria::map_err)?;
        }
        Ok(())
    }

    async fn position(&mut self, slot: &PhysSlot) -> Result<String> {
        let mut c = self.ctl(&slot.cluster).await?;
        let p: Option<String> = c
            .query_first("SELECT @@gtid_binlog_pos")
            .await
            .map_err(sprig_streams::maria::map_err)?;
        Ok(p.unwrap_or_default())
    }

    async fn next_id(&mut self) -> Result<u64> {
        let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
        Ok(ts
            .services()
            .catalog
            .backend()
            .next_object_id(self.cat.tenant)
            .await?
            .0)
    }

    async fn commit(&mut self, change: &StreamsChange) -> Result<()> {
        let tenant = self.cat.tenant;
        let (svc, set_by) = {
            let ts = self.ts().ok_or_else(|| SprigError::internal("no tenant"))?;
            (
                Arc::clone(ts.services()),
                ts.principal
                    .user_id
                    .map_or_else(|| "system".to_owned(), |u| format!("user:{}", u.0)),
            )
        };
        if !self.migrated {
            let ep = svc.locator.writer(tenant).await?;
            let mut conn = ep.pool.get_conn_bounded().await.map_err(|e| {
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
            })?;
            sprig_streams::store::migrate(&mut conn, ep.store.schema()).await?;
            self.migrated = true;
        }
        let mut c = svc.catalog.change(tenant, VersionKind::Catalog).await?;
        let version = c.version();
        let applied = {
            let tx = c.tx();
            let retention = |def: &StreamDef, h: u64| SettingRow {
                scope: SettingScope::Object(ObjectId(def.id.0)),
                key: sprig_streams::settings::RETENTION_HOURS.key.to_owned(),
                value: Some(serde_json::json!(h)),
                bounds: None,
                set_by: set_by.clone(),
                set_at: SystemTime::now(),
                version,
            };
            async {
                // Stream definitions stay in the catalog schema (the MariaDB
                // stream path's table); their settings are catalog records.
                match change {
                    StreamsChange::Insert(def, h) => {
                        let (streams, q) = stream_sql(tx).await?;
                        streams.insert(q, def).await?;
                        if let Some(h) = h {
                            SettingsTx::put_setting(tx, &retention(def, *h)).await?;
                        }
                    }
                    StreamsChange::Update(def, changes) => {
                        let (streams, q) = stream_sql(tx).await?;
                        streams.update(q, def).await?;
                        // A change topic's settings are its table's.
                        let scope = SettingScope::Object(ObjectId(
                            def.source_object.map_or(def.id.0, |o| o.0),
                        ));
                        for c in changes {
                            match c.value {
                                Some(v) => {
                                    SettingsTx::put_setting(
                                        tx,
                                        &SettingRow {
                                            scope,
                                            key: c.key.to_owned(),
                                            value: Some(serde_json::json!(v)),
                                            bounds: None,
                                            set_by: set_by.clone(),
                                            set_at: SystemTime::now(),
                                            version,
                                        },
                                    )
                                    .await?;
                                }
                                None => {
                                    SettingsTx::reset_setting(
                                        tx,
                                        scope,
                                        c.key,
                                        true,
                                        false,
                                        &set_by,
                                        SystemTime::now(),
                                        version,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                    StreamsChange::Delete(def) => {
                        let (streams, q) = stream_sql(tx).await?;
                        streams.delete(q, def.id).await?;
                    }
                }
                Ok::<_, SprigError>(())
            }
            .await
        };
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        if let Some(ts) = self.sess.tenant.as_deref_mut() {
            ts.changed(Some(v));
            let fresh = ts.refresh().await?;
            self.cat = fresh;
        }
        Ok(())
    }

    async fn subscribe(&mut self, def: &StreamDef) -> Result<()> {
        let dc = self
            .ts()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .services()
            .locator
            .dc_store();
        sprig_streams::runtime::subscribe(dc, self.cat.tenant, def).await
    }

    async fn unsubscribe(&mut self, def: &StreamDef) -> Result<()> {
        let dc = self
            .ts()
            .ok_or_else(|| SprigError::internal("no tenant"))?
            .services()
            .locator
            .dc_store();
        sprig_streams::runtime::unsubscribe(dc, self.cat.tenant, def.id).await
    }

    async fn lags(&mut self) -> std::collections::HashMap<sprig_streams::StreamId, u64> {
        let Some(ts) = self.ts() else {
            return std::collections::HashMap::new();
        };
        let dc = ts.services().locator.dc_store();
        // A read error leaves the column NULL: SHOW STREAMS still answers.
        sprig_streams::runtime::instance_lag::of_tenant(dc.as_ref(), self.cat.tenant)
            .await
            .unwrap_or_else(|e| {
                tracing::debug!(error = %e, "stream sequencers' lag not read");
                std::collections::HashMap::new()
            })
    }
}

/// A bound value of a prepared publish as a literal: text as
/// `_utf8mb4 X'..'` (the engine tier's form), other bytes as `X'..'`, so
/// no `sql_mode` changes them and the stream layer measures them exactly.
/// Numbers, times and `NULL` as for a prepared search.
pub(super) fn param_literal(p: &sprig_proto_mysql::command::BoundParam) -> String {
    use sprig_proto_mysql::Value;
    use sprig_proto_mysql::command::ParamValue;
    use std::fmt::Write as _;
    let ParamValue::Value(Value::Bytes(b)) = &p.value else {
        return super::search::param_literal(p);
    };
    let mut h = String::with_capacity(b.len() * 2 + 12);
    if std::str::from_utf8(b).is_ok() {
        h.push_str("_utf8mb4 ");
    }
    h.push_str("X'");
    for x in b {
        let _ = write!(h, "{x:02X}");
    }
    h.push('\'');
    h
}

/// The tenant's streams at `cat`.
fn state_of(cat: &TenantCatalog) -> (StreamsState, bool) {
    match cat.ext.get::<StreamsState>(MODULE) {
        Some(s) => (s.clone(), s.migrated),
        None => (StreamsState::default(), false),
    }
}

impl PassSession {
    fn streams_status(&self, more: bool) -> StatusFlags {
        let s = self.status();
        if more {
            s | StatusFlags::MORE_RESULTS_EXISTS
        } else {
            s
        }
    }

    /// The streams step of [`PassSession::tenant_text`]: `None` when the
    /// statement isn't the stream layer's.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn tenant_streams(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        if let Some(step) = self
            .engine_streams(cx, text, more, format, cat, out)
            .await?
        {
            return Ok(Some(step));
        }
        let Some(ts) = self.tenant.as_deref() else {
            return Ok(None);
        };
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let (state, migrated) = state_of(cat);
        if !sprig_streams::grammar::maybe_stream(text, !state.is_empty()) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let lctn = ts.lower_case_table_names();
        let current = self.current_logical();
        let (lex, _) = self.lex_opts();
        let mysql = self.persona == sprig_proto_mysql::Persona::MySql84;
        let handled = {
            let ctx = Ctx {
                state: &state,
                lex,
                lctn,
                current: current.as_deref(),
                mysql,
            };
            let mut host = GwHost {
                sess: self,
                cat: Arc::clone(cat),
                migrated,
            };
            sprig_streams::sql::handle(sql, &ctx, &mut host).await
        };
        let status = self.streams_status(more);
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match handled {
            Ok(Handled::NotStream) => Ok(None),
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Handled::Ok { affected }) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = affected;
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Handled::Rows { columns, rows }) => {
                let cols: Vec<ColumnDef> = columns
                    .iter()
                    .map(|c| crate::pipeline::text_column(c, cx.proto.collation))
                    .collect();
                let rows = sprig_types::Rows {
                    columns: cols,
                    rows: rows
                        .into_iter()
                        .map(|r| r.into_iter().map(|v| v.map(String::into_bytes)).collect())
                        .collect(),
                };
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
            Ok(Handled::Publish(p)) => {
                let r = Box::pin(self.publish_rows(cx, &p, &lex)).await;
                let status = self.streams_status(more);
                match r {
                    Ok(affected) => {
                        let mut ok = OkPacket::new(status);
                        ok.affected_rows = affected;
                        out.ok(ok).await?;
                        Ok(Some(Step {
                            errored: false,
                            status: Some(status),
                        }))
                    }
                    Err(e) => Ok(Some(self.fail(out, &e).await?)),
                }
            }
            Ok(Handled::Forward { sql, compact, .. }) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = true;
                }
                let text = Bytes::from(sql.into_bytes());
                let r = if compact {
                    let mut sink = CompactSink { inner: out };
                    Box::pin(self.tenant_text(cx, &text, attrs, more, format, &mut sink)).await
                } else {
                    Box::pin(self.tenant_text(cx, &text, attrs, more, format, out)).await
                };
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.types.internal = false;
                }
                r.map(Some)
            }
        }
    }

    /// A publish whose sizes the text doesn't tell: its rows evaluated in
    /// this session under the user's rights, each record measured, then
    /// written to the outbox as literals, in the session's transaction (one
    /// of its own when the rows take more than one statement). The rows
    /// affected.
    async fn publish_rows(
        &mut self,
        cx: &mut ExecCtx<'_>,
        p: &sprig_streams::sql::PublishRows,
        lex: &sprig_sql::lex::LexOptions,
    ) -> Result<u64> {
        let query = super::inherit_msg::rows_query(&p.source, lex)?;
        let rows = self.run_checked(cx, &query, false).await?.rows;
        let inserts = p.inserts(&rows)?;
        let own = inserts.len() > 1 && !self.offsets_in_transaction();
        if own {
            self.run_checked(cx, "START TRANSACTION", true).await?;
        }
        let mut affected = 0;
        for sql in &inserts {
            match self.run_checked(cx, sql, true).await {
                Ok(c) => affected += c.affected.unwrap_or(0),
                Err(e) => {
                    if own {
                        let _ = self.run_checked(cx, "ROLLBACK", true).await;
                    }
                    if p.compact && e.message.contains("sprig_sc_needs_key") {
                        return Err(SprigError::new(
                            SprigCode::StreamCompactNeedsKey,
                            "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted stream",
                        ));
                    }
                    return Err(e);
                }
            }
        }
        if own {
            self.run_checked(cx, "COMMIT", true).await?;
        }
        Ok(affected)
    }

    /// Whether `sql` publishes into one of the tenant's streams (an
    /// `INSERT` into a stream, which MariaDB itself can't prepare: the
    /// stream is no table of its own). Never for the gateway's own
    /// statements or a session that must change its password.
    pub(super) fn stream_publish(&self, sql: &[u8]) -> bool {
        let Some(ts) = self.tenant.as_deref() else {
            return false;
        };
        if ts.types.internal || ts.principal.must_change_password {
            return false;
        }
        let Some(cat) = ts.catalog().map(Arc::clone) else {
            return false;
        };
        let (state, _) = state_of(&cat);
        let Ok(text) = std::str::from_utf8(sql) else {
            return false;
        };
        let lctn = ts.lower_case_table_names();
        let current = self.current_logical();
        let (lex, _) = self.lex_opts();
        let ctx = Ctx {
            state: &state,
            lex,
            lctn,
            current: current.as_deref(),
            mysql: false,
        };
        let db_id = |n: &str| cat.database(&database_key(n, lctn)).map(|d| d.db_id);
        sprig_streams::sql::publishes(text, &ctx, &db_id)
    }

    /// `COM_STMT_PREPARE` of a publish into one of the tenant's streams:
    /// kept as text and run at each execute with the bound values written
    /// in as literals, so the stream layer measures every record as it
    /// does for a text statement. `None` for any other statement.
    pub(super) fn streams_prepare(
        &mut self,
        sql: &Bytes,
        charset: u16,
    ) -> Option<crate::pipeline::ExecSummary> {
        if !self.stream_publish(sql) {
            return None;
        }
        let (lex, _) = self.lex_opts();
        let params = sprig_sql::lex::lex(sql, &lex)
            .map(|t| {
                t.iter()
                    .filter(|t| t.kind == sprig_sql::lex::TokenKind::Param)
                    .count()
            })
            .unwrap_or(0);
        let n = u16::try_from(params).ok()?;
        let mut st = super::stmt::Stmt::deferred(sql.clone(), Vec::new());
        st.params = n;
        st.deferred_params = true;
        st.stream_params = true;
        st.client = Some(sql.clone());
        let stmt = self.stmts.insert(st);
        metrics::gauge!("sprig_mysql_prepared_statements", "persona" => self.persona.as_str())
            .increment(1.0);
        Some(crate::pipeline::ExecSummary {
            status: None,
            prepared: Some(crate::pipeline::PreparedMeta {
                stmt,
                params: (0..n)
                    .map(|_| crate::pipeline::text_column("?", charset))
                    .collect(),
                columns: Vec::new(),
                warnings: 0,
            }),
            db: None,
        })
    }

    /// After a successful `ALTER TABLE t SPRIG SET | RESET (...)` naming
    /// `stream.change_topic`: creates or drops `t.changes` to match. An
    /// error is the statement's (the setting stays).
    pub(super) async fn streams_after_settings(&mut self, text: &[u8]) -> Result<()> {
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(());
        };
        let (lex, _) = self.lex_opts();
        let Some(target) = sprig_streams::sql::change_topic_target(sql, &lex) else {
            return Ok(());
        };
        let current = self.current_logical();
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(());
        };
        let lctn = ts.lower_case_table_names();
        let cat = ts.refresh().await?;
        let (state, migrated) = state_of(&cat);
        let ctx = Ctx {
            state: &state,
            lex,
            lctn,
            current: current.as_deref(),
            mysql: false,
        };
        let mut host = GwHost {
            sess: self,
            cat,
            migrated,
        };
        sprig_streams::sql::reconcile_change_topic(&target, &ctx, &mut host)
            .await
            .map(|_| ())
    }
}

/// A publish to a compacted stream: MariaDB's `CHECK` failure on a keyless
/// record (4025, `sprig_sc_needs_key`) becomes 39955.
struct CompactSink<'a> {
    inner: &'a mut dyn ResultSink,
}

#[async_trait]
impl ResultSink for CompactSink<'_> {
    fn wire(&self) -> Wire {
        self.inner.wire()
    }
    async fn begin_rows(&mut self, columns: &[ColumnDef], format: RowFormat) -> Result<()> {
        self.inner.begin_rows(columns, format).await
    }
    async fn begin_rows_raw(&mut self, defs: &[Bytes], format: RowFormat) -> Result<()> {
        self.inner.begin_rows_raw(defs, format).await
    }
    async fn cursor_opened(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
        self.inner.cursor_opened(columns, end).await
    }
    async fn row(&mut self, values: RowValues<'_>) -> Result<()> {
        self.inner.row(values).await
    }
    async fn raw_row(&mut self, payload: Bytes) -> Result<()> {
        self.inner.raw_row(payload).await
    }
    async fn end_rows(&mut self, end: EndOfRows) -> Result<()> {
        self.inner.end_rows(end).await
    }
    async fn ok(&mut self, ok: OkPacket) -> Result<()> {
        self.inner.ok(ok).await
    }
    async fn error(&mut self, err: &SprigError) -> Result<()> {
        if err.message.contains("sprig_sc_needs_key") {
            let e = SprigError::new(
                SprigCode::StreamCompactNeedsKey,
                "ER_SPRIG_STREAM_COMPACT_NEEDS_KEY: a record without a key on a compacted stream",
            );
            return self.inner.error(&e).await;
        }
        self.inner.error(err).await
    }
    async fn column_list(&mut self, columns: &[ColumnDef], end: EndOfRows) -> Result<()> {
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

/// What the engine streams' statements ask of the session.
struct EngineStreamHost<'s> {
    sess: &'s PassSession,
    cat: &'s TenantCatalog,
    tenant: u64,
}

impl sprig_streams::seg::sql::SqlHost for EngineStreamHost<'_> {
    fn tenant(&self) -> u64 {
        self.tenant
    }

    fn current_db(&self) -> Option<String> {
        self.sess.current_logical()
    }

    fn on_engine(&self, db: &str) -> bool {
        crate::kafka::engine::tenants().has(self.tenant) || self.sess.engine_database_named(db)
    }

    fn allowed(&self, db: &str, stream: &str, need: sprig_streams::seg::kafka::Need) -> bool {
        use sprig_streams::seg::kafka::Need;
        let Some(ts) = self.sess.tenant.as_deref() else {
            return false;
        };
        let grants = crate::tenant::authz::privileges(ts, self.cat);
        if grants.unrestricted() {
            return true;
        }
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(db, lctn);
        let a = match need {
            Need::Read => Action::Select,
            Need::Write => Action::Insert,
            Need::Create => Action::Create,
            Need::Alter => Action::Alter,
            Need::Drop => Action::Drop,
        };
        if !stream.is_empty() {
            let name_key = object_key(ObjectKind::Table, stream, lctn);
            if grants.allows(
                a,
                Target::Named {
                    db_key: &db_key,
                    name_key: &name_key,
                },
            ) {
                return true;
            }
        }
        grants.allows(a, Target::Database { db_key: &db_key })
    }

    fn user(&self) -> (String, String) {
        self.sess
            .tenant
            .as_deref()
            .map_or_else(Default::default, |t| {
                (t.principal.user.clone(), t.principal.client_ip.to_string())
            })
    }

    fn in_transaction(&self) -> bool {
        self.sess.offsets_in_transaction()
    }

    fn engine_transaction(&self) -> bool {
        self.sess.eng.txn.is_open()
    }
}

/// Whether `text` can only be a stream statement (`CREATE | ALTER | DROP
/// STREAM`, `SHOW STREAMS`); an `INSERT`, `DROP DATABASE` or a read may as
/// well be about tables.
fn surely_stream_statement(text: &[u8], lex: sprig_sql::lex::LexOptions) -> bool {
    use sprig_streams::grammar::Stmt;
    matches!(
        sprig_streams::grammar::classify(text, &lex),
        Ok(Stmt::Create(_) | Stmt::Alter(_) | Stmt::Drop { .. } | Stmt::Show { .. })
    )
}

impl PassSession {
    /// The stream statements of engine databases (streams stored on the
    /// node's engine): `None` when the statement isn't one, or the tenant
    /// has no engine database.
    #[allow(clippy::too_many_lines)] // one branch per kind of answer
    async fn engine_streams(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        more: bool,
        format: RowFormat,
        cat: &Arc<TenantCatalog>,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        use sprig_streams::seg::sql::SqlHost as _;
        if self
            .tenant
            .as_deref()
            .is_none_or(|ts| ts.types.internal || ts.principal.must_change_password)
        {
            return Ok(None);
        }
        let tenant = cat.tenant.0;
        let any = crate::kafka::engine::tenants().has(tenant)
            || crate::engine::catalog::of(cat).is_some_and(|e| !e.dbs.is_empty());
        if !any || !sprig_streams::grammar::maybe_stream(text, true) {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let (lex, _) = self.lex_opts();
        let mut selected: Option<select::Answer> = None;
        let planned = {
            let host = EngineStreamHost {
                sess: self,
                cat,
                tenant,
            };
            let on_any = |q: &str| host.on_engine(q);
            // Cheap first: only open the stream log for a statement that
            // names an engine database (or runs in one).
            let names_engine = host.current_db().is_some_and(|d| on_any(d.as_str()))
                || sprig_streams::grammar::identifiers(text, &lex)
                    .iter()
                    .any(|w| on_any(w.as_str()));
            if !names_engine {
                return Ok(None);
            }
            let Some(eng) = crate::kafka::engine::streams() else {
                // The stream log failed to open: only statements that are
                // surely about streams fail; the rest (tables, documents,
                // anything else on the engine database) go on.
                if !surely_stream_statement(text, lex) {
                    return Ok(None);
                }
                return Ok(Some(
                    self.fail(
                        out,
                        &SprigError::internal(
                            "streams of engine databases are stored on the SprigDB engine, which \
                             failed to start on this node; see the node's log",
                        ),
                    )
                    .await?,
                ));
            };
            match sprig_streams::seg::sql::plan(&eng, sql, &lex, &host) {
                Ok(Some(p)) => Ok(Some((eng, p))),
                // A query may read streams.
                Ok(None) => {
                    let persona = if self.persona == sprig_proto_mysql::Persona::MySql84 {
                        sprig_exec::Persona::MySql
                    } else {
                        sprig_exec::Persona::MariaDb
                    };
                    select::run(&eng, &host, sql, lex, persona, cx.proto.collation).map(|a| {
                        selected = a;
                        None
                    })
                }
                Err(e) => Err(e),
            }
        };
        if let Some((columns, rows)) = selected.take() {
            if format == RowFormat::Binary {
                return Ok(Some(
                    self.fail(
                        out,
                        &SprigError::standard(
                            1235,
                            *b"42000",
                            "This version of SprigDB doesn't yet support 'a prepared SELECT that \
                             reads a stream of an engine database'",
                        ),
                    )
                    .await?,
                ));
            }
            let status = self.streams_status(more);
            let rows = sprig_types::Rows { columns, rows };
            super::types::send_rows(out, &rows, format, status).await?;
            return Ok(Some(Step {
                errored: false,
                status: Some(status),
            }));
        }
        let (eng, plan) = match planned {
            Ok(None) => return Ok(None),
            Ok(Some(x)) => x,
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        let status = self.streams_status(more);
        let step = Step {
            errored: false,
            status: Some(status),
        };
        // A publish inside the session's engine transaction joins it.
        let in_engine_txn = matches!(plan, sprig_streams::seg::sql::Plan::Publish { .. })
            && self.offsets_in_transaction();
        let ran = if in_engine_txn {
            match self.eng.txn.open_tx() {
                Some(tx) => {
                    sprig_streams::seg::sql::publish_in(&eng, tenant, plan, tx.engine_txn()).await
                }
                None => Err(SprigError::internal(
                    "stream publish: the session's engine transaction ended",
                )),
            }
        } else {
            sprig_streams::seg::sql::run(&eng, tenant, plan).await
        };
        match ran {
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(sprig_streams::seg::sql::Answer::Continue) => Ok(None),
            Ok(sprig_streams::seg::sql::Answer::Ok(affected)) => {
                let mut ok = OkPacket::new(status);
                ok.affected_rows = affected;
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(sprig_streams::seg::sql::Answer::Rows { columns, rows }) => {
                let cols: Vec<ColumnDef> = columns
                    .iter()
                    .map(|c| crate::pipeline::text_column(c, cx.proto.collation))
                    .collect();
                let rows = sprig_types::Rows {
                    columns: cols,
                    rows: rows
                        .into_iter()
                        .map(|r| r.into_iter().map(|v| v.map(String::into_bytes)).collect())
                        .collect(),
                };
                super::types::send_rows(out, &rows, format, status).await?;
                Ok(Some(step))
            }
        }
    }
}

/// The stream definitions' store and the catalog schema's MariaDB
/// transaction of the change `tx`.
async fn stream_sql(
    tx: &mut sprig_catalog::version::MariaTx,
) -> Result<(
    sprig_streams::StreamsStore,
    &mut mysql_async::Transaction<'static>,
)> {
    let (store, q) = tx.sql().await?;
    Ok((sprig_streams::StreamsStore::new(store.schema()), q))
}

#[cfg(test)]
mod engine_stream_tests {
    use super::surely_stream_statement;

    #[test]
    fn only_stream_statements_fail_when_the_stream_log_is_down() {
        let lex = sprig_sql::lex::LexOptions::mariadb();
        for s in [
            "CREATE STREAM s PARTITIONS 2",
            "ALTER STREAM db.s PARTITIONS 4",
            "DROP STREAM IF EXISTS s",
            "SHOW STREAMS FROM db",
        ] {
            assert!(surely_stream_statement(s.as_bytes(), lex), "{s}");
        }
        for s in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "CREATE TABLE t (a INT)",
            "DROP DATABASE db",
            "DROP TABLE t",
            "BEGIN",
            "CREATE STREAM",
        ] {
            assert!(!surely_stream_statement(s.as_bytes(), lex), "{s}");
        }
    }
}
