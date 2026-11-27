//! Inherited streams (`FOR EACH TENANT`) over the engine's stream log.
//!
//! ```text
//! CREATE STREAM s FOR EACH TENANT [scope] [PARTITIONS n] ...
//!     a definition: the CREATE STREAM text without the clause, kept as the
//!     instances' template
//! a member's INSERT INTO s ...
//!     its instance (an ordinary stream of the member, in the definition's
//!     database, made from the template) is made first when it has none
//! INSERT INTO s FOR TENANT t ...            into t's instance (made if needed)
//! INSERT INTO s FOR ALL TENANTS [WITH INSTANCE] [UNDER u] ...
//!     into every member's instance in scope below the publisher (WITH
//!     INSTANCE: only existing instances), one member after another
//! INSERT INTO s IN TENANT a ...
//!     into ancestor a's ordinary stream, with a's consent
//!     (stream.descendant_publish), keyed by the publisher's name
//! ```
//!
//! The server stamps `x-sprig-tenant` (the member) and
//! `x-sprig-origin-tenant` (the publisher) on every record. A `FOR TENANT`
//! or `IN TENANT` publish inside an engine transaction joins it (a
//! transaction across units); outside one, and for each member of a
//! broadcast, the append is its own transaction on the member's unit.

use std::sync::Arc;

use serde_json::json;
use sprig_auth::authz::Action;
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::grammar::{self, InsertTarget, Target};
use sprig_inherit::msg::settings as s;
use sprig_inherit::msg::{Chain, DefKind, DefState, Definition, MsgState};
use sprig_proto_kafka::broker::data::ProduceRecord;
use sprig_proto_kafka::records::Header;
use sprig_sql::lex::LexOptions;
use sprig_streams::seg::kafka::{EngineStreams, Need};
use sprig_streams::seg::sql::{self as ssql, SqlHost};

use super::{Reply, allows, check_quota, denied, next_object_id, scope_of};
use crate::exec::PassSession;
use crate::inherited::{self, InstanceIndex, MsgWrite};
use crate::tenant::TenantServices;

/// The member's side of an instance: the system makes it and appends to
/// it (the publisher's own rights were checked).
struct MemberHost {
    tenant: u64,
    db: String,
}

impl SqlHost for MemberHost {
    fn tenant(&self) -> u64 {
        self.tenant
    }
    fn current_db(&self) -> Option<String> {
        Some(self.db.clone())
    }
    fn on_engine(&self, _db: &str) -> bool {
        true
    }
    fn allowed(&self, _db: &str, _stream: &str, _need: Need) -> bool {
        true
    }
    fn user(&self) -> (String, String) {
        ("sprig_inherit".into(), "localhost".into())
    }
    fn in_transaction(&self) -> bool {
        false
    }
}

fn streams() -> Result<Arc<EngineStreams>> {
    crate::kafka::engine::streams().ok_or_else(|| {
        SprigError::internal(
            "streams are stored on the SprigDB engine, which failed to start on this node; see \
             the node's log",
        )
    })
}

fn no_definition(db: &str, name: &str) -> SprigError {
    SprigError::standard(
        1146,
        *b"42S02",
        format!("Table '{db}.{name}' doesn't exist (no FOR EACH TENANT stream of that name)"),
    )
}

/// Member `member`'s instance of stream definition `def`, made from the
/// definition's template when it has none (its database too). `true`
/// when it was made now.
///
/// # Errors
/// The catalog's, the stream log's.
pub(crate) async fn ensure_instance(
    svc: &Arc<TenantServices>,
    eng: &EngineStreams,
    member: TenantId,
    def: &Definition,
    lex: &LexOptions,
) -> Result<bool> {
    let made = make_instance(svc, eng, member, def, lex).await?;
    if made {
        join_fan_ins(svc, eng, member, def).await?;
    }
    // Its inherited triggers, before a record goes in (they start at the
    // instance's end).
    let mcat = inherited::catalog(svc, member).await?;
    let chain = inherited::chain(svc, &mcat).await?;
    if made || inherited::triggers::pending_for(&chain, def, &inherited::state_of(&mcat)) {
        inherited::triggers::ensure_for_stream_instance(svc, &chain, def).await?;
    }
    Ok(made)
}

/// A new instance joins its definition's `FAN IN` triggers, read from its
/// first record.
async fn join_fan_ins(
    svc: &Arc<TenantServices>,
    eng: &EngineStreams,
    member: TenantId,
    def: &Definition,
) -> Result<()> {
    use sprig_logic::messages;
    let dc = svc.locator.dc_store();
    let triggers = messages::fanin_triggers(dc.as_ref(), (def.declarer.0, def.object_id)).await?;
    if triggers.is_empty() {
        return Ok(());
    }
    let full = format!("{}.{}", def.db_name, def.name);
    let Some(topic) = eng.topic(member.0, &full)? else {
        return Ok(());
    };
    for t in triggers {
        messages::put_fanin_member(
            dc.as_ref(),
            &messages::FanInMember {
                tenant: t.tenant,
                binding: t.binding,
                member,
                stream: topic.stream_id,
            },
        )
        .await?;
    }
    Ok(())
}

/// The instance itself: made from the template when missing.
async fn make_instance(
    svc: &Arc<TenantServices>,
    eng: &EngineStreams,
    member: TenantId,
    def: &Definition,
    lex: &LexOptions,
) -> Result<bool> {
    let full = format!("{}.{}", def.db_name, def.name);
    if eng.topic(member.0, &full)?.is_some() {
        return Ok(false);
    }
    inherited::ensure_database(svc, member, &def.db_name).await?;
    let sql = def
        .definition
        .get("create")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SprigError::internal("an inherited stream's definition has no CREATE STREAM text")
        })?;
    let host = MemberHost {
        tenant: member.0,
        db: def.db_name.clone(),
    };
    let plan = ssql::plan(eng, sql, lex, &host)?
        .ok_or_else(|| SprigError::internal("an inherited stream's template isn't a stream"))?;
    if let Err(e) = ssql::run(eng, member.0, plan).await {
        // Made meanwhile by another session: that one is the instance.
        if eng.topic(member.0, &full)?.is_none() {
            return Err(e);
        }
        return Ok(false);
    }
    if let Some(rec) = eng.topic(member.0, &full)? {
        inherited::put_index(
            svc,
            &InstanceIndex {
                tenant: member,
                def: def.key(),
                kind: DefKind::Stream,
                instance_id: rec.stream_id,
                db: def.db_name.clone(),
                name: def.name.clone(),
            },
        )
        .await?;
    }
    tracing::info!(tenant = %member, def = %def.key(), stream = %full, "inherited stream instance made");
    Ok(true)
}

impl PassSession {
    /// `CREATE STREAM s FOR EACH TENANT [scope] ...`: a definition of this
    /// tenant.
    pub(super) async fn define_stream(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let rest = f.strip(sql);
        let syntax = || {
            SprigError::standard(
                1064,
                *b"42000",
                "expected CREATE STREAM name FOR EACH TENANT [scope] [PARTITIONS n] ...",
            )
        };
        let Ok(sprig_streams::grammar::Stmt::Create(c)) =
            sprig_streams::grammar::classify(rest.as_bytes(), lex)
        else {
            return Err(syntax());
        };
        let q = c.name.clone().ok_or_else(syntax)?;
        if !f.grants.is_empty() {
            return Err(SprigError::standard(
                1064,
                *b"42000",
                "GRANTS belongs to triggers and sinks",
            ));
        }
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let lctn = ts.lower_case_table_names();
        let db =
            q.db.clone()
                .or_else(|| self.current_logical())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        if cat.database(&database_key(&db, lctn)).is_none() {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        }
        if !allows(ts, cat, Action::Create, &db, &q.name) {
            return Err(denied(ts, "CREATE", &q.name));
        }
        // The template is checked as a member would run it.
        let eng = streams()?;
        let probe = MemberHost {
            tenant: cat.tenant.0,
            db: db.clone(),
        };
        ssql::plan(&eng, &rest, lex, &probe)?.ok_or_else(syntax)?;
        if own.def(&db, DefKind::Stream, &q.name).is_some() {
            if c.if_not_exists {
                return Ok(Reply::Ok(0, Vec::new()));
            }
            return Err(SprigError::standard(
                1050,
                *b"42S01",
                format!("stream '{db}.{}' is already defined", q.name),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        check_quota(svc, cat, own)?;
        let def = Definition {
            declarer: cat.tenant,
            object_id: next_object_id(svc, cat.tenant).await?,
            kind: DefKind::Stream,
            db_name: db.clone(),
            name: q.name.clone(),
            scope,
            definition: json!({ "create": rest.trim() }),
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: ts.principal.user_id.map_or(0, |u| u.0),
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), stream = %q.name, "inherited stream defined");
        Ok(Reply::Ok(
            0,
            vec![format!(
                "stream {db}.{} is defined for every tenant in scope; each member's instance is \
                 made by the first publish into it",
                q.name
            )],
        ))
    }

    /// A member's plain `INSERT INTO s ...` naming an inherited stream it
    /// has no instance of yet: the instance is made, and the publish goes
    /// on as any other. Nothing for other statements.
    pub(super) async fn member_instance(
        &self,
        svc: &Arc<TenantServices>,
        cat: &TenantCatalog,
        chain: &Chain,
        sql: &str,
        lex: &LexOptions,
    ) -> Result<()> {
        let Ok(sprig_streams::grammar::Stmt::Insert(i)) =
            sprig_streams::grammar::classify(sql.as_bytes(), lex)
        else {
            return Ok(());
        };
        let Some(db) = i.target.db.clone().or_else(|| self.current_logical()) else {
            return Ok(());
        };
        let Some(def) = chain.resolve(&db, DefKind::Stream, &i.target.name) else {
            return Ok(());
        };
        if def.state != DefState::Active {
            return Ok(());
        }
        let Some(eng) = crate::kafka::engine::streams() else {
            return Ok(());
        };
        // A table of that name is the member's own (and no instance is
        // needed for it).
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(crate::tenant::session::TenantSession::lower_case_table_names);
        if let Some(d) = cat.database(&database_key(&db, lctn))
            && crate::engine::catalog::of(cat)
                .is_some_and(|e| e.table(d.db_id, &i.target.name, lctn).is_some())
        {
            return Ok(());
        }
        ensure_instance(svc, &eng, cat.tenant, &def, lex).await?;
        Ok(())
    }

    /// `INSERT INTO s FOR TENANT t | FOR ALL TENANTS ... | IN TENANT t`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // the targets, then each append
    pub(super) async fn publish_inherited(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        sql: &str,
        lex: &LexOptions,
        it: &InsertTarget,
    ) -> Result<Reply> {
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = it
            .db
            .clone()
            .or_else(|| self.current_logical())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        if let Target::In(a) = &it.target {
            return self.publish_up(svc, cat, sql, lex, it, a, db).await;
        }
        let def = own
            .def(&db, DefKind::Stream, &it.name)
            .cloned()
            .or_else(|| chain.resolve(&db, DefKind::Stream, &it.name))
            .ok_or_else(|| no_definition(&db, &it.name))?;
        if def.state != DefState::Active {
            return Err(inherited::dropping(format!("stream {db}.{}", it.name)));
        }
        if !allows(ts, cat, Action::Insert, &db, &it.name) {
            return Err(denied(ts, "INSERT", &it.name));
        }
        let eng = streams()?;
        let me = cat.tenant;
        let targets: Vec<TenantId> = match &it.target {
            Target::Tenant(name) => {
                let t = inherited::tenant_by_name(svc, name)
                    .await?
                    .ok_or_else(|| inherited::target_err(format!("no tenant named '{name}'")))?;
                let entry = inherited::dir_entry(svc, t)
                    .await?
                    .ok_or_else(|| inherited::target_err(format!("no tenant named '{name}'")))?;
                let path = entry.path_ids();
                let anc: Vec<TenantId> = path
                    .split_last()
                    .map(|(_, a)| a.iter().map(|x| TenantId(*x)).collect())
                    .unwrap_or_default();
                if !(t == me || anc.contains(&me)) || !def.scope.reaches(def.declarer, t, &anc) {
                    return Err(inherited::target_err(format!(
                        "tenant '{name}' isn't a member of stream {db}.{} below this tenant",
                        it.name
                    )));
                }
                vec![t]
            }
            Target::All {
                with_instance,
                under,
            } => {
                let under = match under {
                    Some(n) => Some(
                        inherited::tenant_by_name(svc, n)
                            .await?
                            .ok_or_else(|| inherited::target_err(format!("no tenant named '{n}'")))?
                            .0,
                    ),
                    None => None,
                };
                let form = inherited::broadcast::Form {
                    with_instance: *with_instance,
                    under,
                };
                return self
                    .broadcast(svc, &eng, &def, me, &it.strip(sql), lex, form)
                    .await;
            }
            Target::In(_) => Vec::new(),
        };
        let body = it.strip(sql);
        let origin = self.own_tenant_name(svc, me).await?;
        // One member's publish joins the session's engine transaction; a
        // broadcast's appends are each their own.
        let join = matches!(it.target, Target::Tenant(_)) && self.eng.txn.is_open();
        let mut affected = 0u64;
        for t in &targets {
            ensure_instance(svc, &eng, *t, &def, lex).await?;
            let host = MemberHost {
                tenant: t.0,
                db: db.clone(),
            };
            let plan = ssql::plan(&eng, &body, lex, &host)?.ok_or_else(|| {
                SprigError::internal("a publish into an inherited stream isn't a stream publish")
            })?;
            let to = match &it.target {
                Target::Tenant(n) => n.clone(),
                _ => inherited::tenant_name(svc, *t).await?,
            };
            let plan = restamp(&eng, t.0, plan, |r| {
                stamp(r, TENANT_HEADER, &to);
                stamp(r, ORIGIN_HEADER, &origin);
                Ok(())
            })?;
            if let ssql::Answer::Ok(n) = self.append(&eng, t.0, plan, join).await? {
                affected += n;
            }
        }
        Ok(Reply::Ok(affected, Vec::new()))
    }

    /// `FOR ALL TENANTS`: the records go into the definition's broadcast
    /// log (in the session's engine transaction when one is open), and the
    /// fan-out task delivers them to every member ([`super::bcast`]).
    #[allow(clippy::too_many_arguments)]
    async fn broadcast(
        &mut self,
        svc: &Arc<TenantServices>,
        eng: &Arc<EngineStreams>,
        def: &Definition,
        me: TenantId,
        body: &str,
        lex: &LexOptions,
        form: inherited::broadcast::Form,
    ) -> Result<Reply> {
        let sql = def
            .definition
            .get("create")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                SprigError::internal("an inherited stream's definition has no CREATE STREAM text")
            })?;
        let host = MemberHost {
            tenant: me.0,
            db: def.db_name.clone(),
        };
        let Some(ssql::Plan::Create { rec, .. }) = ssql::plan(eng, sql, lex, &host)? else {
            return Err(SprigError::internal(
                "an inherited stream's template isn't a stream",
            ));
        };
        let records = ssql::publish_records(eng, body, lex, &rec)?;
        let origin = self.own_tenant_name(svc, me).await?;
        let n = records.len() as u64;
        let seq = if self.eng.txn.is_open() {
            let tx = self.eng.txn.open_tx().ok_or_else(|| {
                SprigError::internal("stream publish: the session's engine transaction ended")
            })?;
            super::bcast::publish(
                svc,
                eng,
                def,
                me,
                &origin,
                form,
                &records,
                Some(tx.engine_txn()),
            )
            .await?
        } else {
            super::bcast::publish(svc, eng, def, me, &origin, form, &records, None).await?
        };
        Ok(Reply::Ok(
            n,
            vec![format!(
                "broadcast {}.{seq}: every member in scope gets it from the broadcast log, in \
                 order, once",
                def.object_id
            )],
        ))
    }

    /// The name of the session's own tenant.
    async fn own_tenant_name(&self, svc: &TenantServices, me: TenantId) -> Result<String> {
        match self
            .tenant
            .as_deref()
            .and_then(|t| t.principal.tenant_name.clone())
        {
            Some(n) => Ok(n),
            None => inherited::tenant_name(svc, me).await,
        }
    }

    /// Appends a planned publish into `tenant`'s stream: inside the
    /// session's engine transaction (`join`, a transaction across units
    /// when `tenant` isn't the session's), else on its own.
    async fn append(
        &mut self,
        eng: &EngineStreams,
        tenant: u64,
        plan: ssql::Plan,
        join: bool,
    ) -> Result<ssql::Answer> {
        if join {
            let tx = self.eng.txn.open_tx().ok_or_else(|| {
                SprigError::internal("stream publish: the session's engine transaction ended")
            })?;
            return ssql::publish_in(eng, tenant, plan, tx.engine_txn()).await;
        }
        ssql::run(eng, tenant, plan).await
    }

    /// `INSERT INTO s IN TENANT a ...`: into ancestor `a`'s ordinary stream
    /// (one platform stream for every client). Only with the owner's
    /// consent (`stream.descendant_publish = on` where the stream is), and
    /// under `stream.descendant_key = origin` keyed by the publisher's own
    /// tenant name (its name when the record has no key; 39957 for another
    /// key). Anything but an ancestor, or no consent, answers 1142 (never a
    /// hint that a tenant exists). The record carries
    /// `x-sprig-origin-tenant`; inside an engine transaction the append
    /// joins it.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn publish_up(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        sql: &str,
        lex: &LexOptions,
        it: &InsertTarget,
        anc_name: &str,
        db: String,
    ) -> Result<Reply> {
        let me = cat.tenant;
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let refused = denied(ts, "INSERT", &it.name);
        let anc = inherited::tenant_by_name(svc, anc_name).await?;
        let eng = streams()?;
        let body = it.strip(sql);
        let host_db = db.clone();
        let join = self.eng.txn.is_open();
        // Its own stream: an ordinary publish.
        if anc == Some(me) {
            if !allows(ts, cat, Action::Insert, &db, &it.name) {
                return Err(refused);
            }
            let plan = ssql::plan(
                &eng,
                &body,
                lex,
                &MemberHost {
                    tenant: me.0,
                    db: host_db,
                },
            )?
            .ok_or_else(|| SprigError::internal("a publish isn't a stream publish"))?;
            return match self.append(&eng, me.0, plan, join).await? {
                ssql::Answer::Ok(n) => Ok(Reply::Ok(n, Vec::new())),
                _ => Ok(Reply::Ok(0, Vec::new())),
            };
        }
        let Some(anc) = anc else {
            return Err(refused);
        };
        let ancestors = inherited::dir_entry(svc, me)
            .await?
            .map(|e| e.path_ids())
            .unwrap_or_default();
        if !ancestors
            .split_last()
            .is_some_and(|(_, up)| up.contains(&anc.0))
        {
            return Err(refused);
        }
        // The owner's consent, where the stream is.
        let acat = inherited::catalog(svc, anc).await?;
        let Some(adb) = acat
            .databases
            .iter()
            .find(|d| d.name.eq_ignore_ascii_case(&db))
        else {
            return Err(refused);
        };
        let ctx = acat.setting_ctx(&svc.dc).with_db(adb.db_id);
        let consent = svc
            .settings
            .get(&s::STREAM_DESCENDANT_PUBLISH, &ctx, Some(&acat.settings));
        if !consent.eq_ignore_ascii_case("on") {
            return Err(refused);
        }
        let any_key = svc
            .settings
            .get(&s::STREAM_DESCENDANT_KEY, &ctx, Some(&acat.settings))
            .eq_ignore_ascii_case("any");
        let origin = self.own_tenant_name(svc, me).await?;
        let plan = ssql::plan(
            &eng,
            &body,
            lex,
            &MemberHost {
                tenant: anc.0,
                db: host_db,
            },
        )?
        .ok_or_else(|| SprigError::internal("a publish isn't a stream publish"))?;
        let plan = restamp(&eng, anc.0, plan, |r| {
            if !any_key {
                match r.key.as_deref() {
                    None => r.key = Some(origin.clone().into_bytes()),
                    Some(k) if k == origin.as_bytes() => {}
                    Some(k) => {
                        return Err(SprigError::new(
                            SprigCode::StreamOriginKey,
                            format!(
                                "ER_SPRIG_STREAM_ORIGIN_KEY: a record tenant '{origin}' publishes \
                                 into tenant '{anc_name}''s stream {db}.{} is keyed by its tenant \
                                 name (stream.descendant_key = origin), not '{}'",
                                it.name,
                                String::from_utf8_lossy(k)
                            ),
                        ));
                    }
                }
            }
            stamp(r, ORIGIN_HEADER, &origin);
            Ok(())
        })?;
        let n = match self.append(&eng, anc.0, plan, join).await? {
            ssql::Answer::Ok(n) => n,
            _ => 0,
        };
        tracing::debug!(tenant = %me, into = %anc, stream = %it.name, records = n, "published into an ancestor's stream");
        Ok(Reply::Ok(n, Vec::new()))
    }
}

/// The member a record was published for (server-stamped).
pub(super) const TENANT_HEADER: &str = "x-sprig-tenant";
/// The tenant that published it (server-stamped).
pub(super) const ORIGIN_HEADER: &str = "x-sprig-origin-tenant";

/// Sets header `key` of a record to `value`, replacing any the client sent.
pub(super) fn stamp(r: &mut ProduceRecord, key: &str, value: &str) {
    r.headers.retain(|h| !h.key.eq_ignore_ascii_case(key));
    r.headers.push(Header {
        key: key.to_owned(),
        value: Some(value.as_bytes().to_vec()),
    });
}

/// A publish plan with `f` applied to each record; a record given a key
/// here goes to that key's partition.
fn restamp(
    eng: &EngineStreams,
    tenant: u64,
    plan: ssql::Plan,
    mut f: impl FnMut(&mut ProduceRecord) -> Result<()>,
) -> Result<ssql::Plan> {
    let ssql::Plan::Publish { stream_id, batches } = plan else {
        return Ok(plan);
    };
    let parts = eng
        .topics(tenant)?
        .into_iter()
        .find(|t| t.stream_id == stream_id)
        .map_or(1, |t| u32::try_from(t.partitions.max(1)).unwrap_or(1));
    let mut by_part: std::collections::BTreeMap<i32, Vec<ProduceRecord>> =
        std::collections::BTreeMap::new();
    for (p, recs) in batches {
        for mut r in recs {
            let keyed = r.key.is_some();
            f(&mut r)?;
            let p = match r.key.as_deref() {
                Some(k) if !keyed => {
                    i32::try_from(sprig_streams::partition::for_key(k, parts).0).unwrap_or(0)
                }
                _ => p,
            };
            by_part.entry(p).or_default().push(r);
        }
    }
    Ok(ssql::Plan::Publish {
        stream_id,
        batches: by_part.into_iter().collect(),
    })
}
