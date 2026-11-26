//! Inherited messaging objects (`FOR EACH TENANT`) in a tenant session.
//! Runs before the stream layer ([`PassSession::tenant_streams`]) and the
//! logic layer's declarations:
//!
//! ```text
//! CREATE EXCHANGE | QUEUE ... FOR EACH TENANT [scope] ...   a definition
//! BIND ... FOR EACH TENANT | UNBIND ...                     bindings
//! CREATE [OR REPLACE] TRIGGER g ON QUEUE q FOR EACH TENANT [scope]
//!   [GRANTS (...)] LANGUAGE ... AS ...            a trigger definition; existing
//!                                                 instances get it in the background
//! DROP QUEUE | EXCHANGE | TRIGGER ... [IMMEDIATE]  drops it
//! ALTER INHERITED kind name ...      overrides (nearest level wins)
//! SHOW INHERITED DEFINITIONS | INSTANCES | OVERRIDES [FOR kind name]
//! ```
//!
//! `CREATE STREAM ... FOR EACH TENANT`, and publishes `FOR TENANT`, `IN
//! TENANT` or `FOR ALL TENANTS`, answer 1235: streams are stored on the
//! engine, whose inherited instances aren't built yet.

// Lexing options are passed by reference, as the lexer takes them.
#![allow(clippy::trivially_copy_pass_by_ref)]

use std::sync::Arc;

use bytes::Bytes;
use serde_json::{Value, json};
use sprig_auth::authz::{Action, Target};
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::ObjectKind;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::grammar::{self, AlterOp, For, ShowInherited, Stmt};
use sprig_inherit::msg::{
    AppliesTo, Cause, Chain, DefKey, DefKind, DefState, Definition, MsgState, Override, Scope,
    TriggerSpec, What,
};
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::command::QueryAttrs;
use sprig_proto_mysql::packets::OkPacket;
use sprig_sql::lex::{LexOptions, TokenKind, lex};
use sprig_streams::publish::Publish;

use super::PassSession;
use super::tenant::Step;
use crate::inherited::{self, MsgWrite, target_err, triggers};
use crate::pipeline::{ExecCtx, ResultSink, RowFormat};
use crate::tenant::TenantServices;
use crate::tenant::session::{LocalWarning, TenantSession};

/// What a statement answers.
enum Reply {
    /// OK, with notes.
    Ok(u64, Vec<String>),
    /// A result set.
    Rows(Vec<&'static str>, Vec<Vec<Option<String>>>),
}

/// Inherited streams: their instances, publishes and fan-in were built for
/// streams kept in MariaDB.
fn streams_not_built() -> SprigError {
    not_built(
        "FOR EACH TENANT on a stream: streams are stored on the SprigDB engine, whose inherited \
         instances aren't built yet",
    )
}

fn not_built(what: &str) -> SprigError {
    SprigError::standard(
        1235,
        *b"42000",
        format!("This version of SprigDB doesn't yet support {what}"),
    )
}

pub(super) fn denied(ts: &TenantSession, cmd: &str, name: &str) -> SprigError {
    SprigError::standard(
        1142,
        *b"42000",
        format!(
            "{cmd} command denied to user '{}'@'{}' for table '{name}'",
            ts.principal.user, ts.principal.client_ip
        ),
    )
}

/// Whether the session may do `a` on `db.name`: by name, or on the
/// database.
pub(super) fn allows(
    ts: &TenantSession,
    cat: &TenantCatalog,
    a: Action,
    db: &str,
    name: &str,
) -> bool {
    let grants = crate::tenant::authz::privileges(ts, cat);
    if grants.unrestricted() {
        return true;
    }
    let lctn = ts.lower_case_table_names();
    let db_key = database_key(db, lctn);
    let name_key = object_key(ObjectKind::Table, name, lctn);
    grants.allows(
        a,
        Target::Named {
            db_key: &db_key,
            name_key: &name_key,
        },
    ) || grants.allows(a, Target::Database { db_key: &db_key })
}

pub(super) fn tenant_admin(ts: &TenantSession, cat: &TenantCatalog) -> bool {
    let grants = crate::tenant::authz::privileges(ts, cat);
    grants.unrestricted() || grants.allows(Action::TenantAdmin, Target::Tenant)
}

fn first_word(sql: &str, lex_opts: &LexOptions) -> Option<String> {
    let toks = lex(sql.as_bytes(), lex_opts).ok()?;
    let t = toks.iter().find(|t| !t.kind.is_trivia())?;
    (t.kind == TokenKind::Word).then(|| sql[t.range()].to_ascii_lowercase())
}

impl PassSession {
    /// The inherited-messaging step of [`PassSession::tenant_text`]: `None`
    /// when the statement isn't about inherited messaging objects.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn tenant_inherit_msg(
        &mut self,
        cx: &mut ExecCtx<'_>,
        text: &Bytes,
        _attrs: &QueryAttrs,
        more: bool,
        format: RowFormat,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        if ts.types.internal || ts.principal.must_change_password {
            return Ok(None);
        }
        let Ok(sql) = std::str::from_utf8(text) else {
            return Ok(None);
        };
        let (lex_opts, _) = self.lex_opts();
        let Some(head) = first_word(sql, &lex_opts) else {
            return Ok(None);
        };
        if !matches!(
            head.as_str(),
            "insert" | "create" | "drop" | "alter" | "show" | "select" | "bind" | "unbind"
        ) {
            return Ok(None);
        }
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        // The session's own last change included (a CREATE DATABASE just
        // before).
        let cat = ts.refresh().await?;
        let kw = grammar::maybe(text);
        let own = inherited::state_of(&cat);
        if !kw && own.is_empty() && cat.ancestry.is_empty() {
            return Ok(None);
        }
        let svc = Arc::clone(ts.services());
        let chain = match inherited::chain(&svc, &cat).await {
            Ok(c) => c,
            Err(e) if !kw => {
                tracing::debug!(error = %e, "inherited definitions unreadable; statement goes on");
                return Ok(None);
            }
            Err(e) => return Ok(Some(self.fail(out, &e).await?)),
        };
        if !kw && !inherited::has_defs(&chain) && own.instances.is_empty() {
            return Ok(None);
        }
        let r = Box::pin(self.inherit_msg(sql, &head, &lex_opts, &svc, &cat, &chain, &own)).await;
        let mut status = self.status();
        if more {
            status |= StatusFlags::MORE_RESULTS_EXISTS;
        }
        let step = Step {
            errored: false,
            status: Some(status),
        };
        match r {
            Ok(None) => Ok(None),
            Err(e) => Ok(Some(self.fail(out, &e).await?)),
            Ok(Some(Reply::Ok(n, notes))) => {
                if !notes.is_empty()
                    && let Some(ts) = self.tenant.as_deref_mut()
                {
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
                ok.affected_rows = n;
                ok.warnings = self
                    .tenant
                    .as_deref()
                    .and_then(|t| t.warnings.as_ref())
                    .map_or(0, |w| u16::try_from(w.len()).unwrap_or(u16::MAX));
                out.ok(ok).await?;
                Ok(Some(step))
            }
            Ok(Some(Reply::Rows(columns, rows))) => {
                let cols = columns
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

    #[allow(clippy::too_many_arguments)]
    async fn inherit_msg(
        &mut self,
        sql: &str,
        head: &str,
        lex_opts: &LexOptions,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
    ) -> Result<Option<Reply>> {
        if let Some(s) = grammar::statement(sql, lex_opts)? {
            return match s {
                Stmt::Show(s) => self.inherit_show(svc, cat, chain, own, &s).await.map(Some),
                Stmt::Alter(a) => Box::pin(self.inherit_alter(svc, cat, chain, own, &a))
                    .await
                    .map(Some),
            };
        }
        match head {
            "create" => {
                let Some(f) = grammar::for_each(sql, lex_opts)? else {
                    return self.inherit_shadow_check(sql, lex_opts, cat, chain);
                };
                match grammar::ddl_kind(sql, lex_opts) {
                    Some((DefKind::Stream, false)) => Err(streams_not_built()),
                    Some((DefKind::Trigger, false)) => self
                        .define_trigger(svc, cat, chain, own, sql, lex_opts, &f)
                        .await
                        .map(Some),
                    Some((DefKind::Exchange | DefKind::Queue, false)) => self
                        .define_mq(svc, cat, own, sql, lex_opts, &f)
                        .await
                        .map(Some),
                    Some((k, _)) => Err(not_built(&format!(
                        "FOR EACH TENANT on a {} (it works on streams, exchanges, queues, \
                         bindings and triggers)",
                        k.as_str()
                    ))),
                    None => Err(SprigError::standard(
                        1064,
                        *b"42000",
                        "FOR EACH TENANT follows the name of a stream, exchange, queue, binding, trigger, connection or sink",
                    )),
                }
            }
            "drop" | "alter" => self.inherit_ddl(svc, cat, own, sql, lex_opts).await,
            "bind" => match grammar::for_each(sql, lex_opts)? {
                Some(f) => self
                    .define_mq(svc, cat, own, sql, lex_opts, &f)
                    .await
                    .map(Some),
                None => Ok(None),
            },
            "unbind" => self.unbind_def(svc, cat, own, sql, lex_opts).await,
            "insert" => match grammar::insert_target(sql, lex_opts)? {
                // `INSERT ... FOR TENANT | IN TENANT | FOR ALL TENANTS`
                // publish into inherited streams.
                Some(_) => Err(streams_not_built()),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }
}

impl PassSession {
    /// `CREATE TABLE | STREAM | TRIGGER name` in a member whose chain has a
    /// definition of the name (39897); else nothing.
    fn inherit_shadow_check(
        &self,
        sql: &str,
        lex_opts: &LexOptions,
        cat: &TenantCatalog,
        chain: &Chain,
    ) -> Result<Option<Reply>> {
        let Some((kind, false)) = grammar::ddl_kind(sql, lex_opts) else {
            return Ok(None);
        };
        if kind != DefKind::Stream {
            return Ok(None);
        }
        let Ok(sprig_streams::grammar::Stmt::Create(c)) =
            sprig_streams::grammar::classify(sql.as_bytes(), lex_opts)
        else {
            return Ok(None);
        };
        let Some(n) = c.name else { return Ok(None) };
        let Some(db) = n.db.clone().or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        if let Some(d) = chain.resolve(&db, DefKind::Stream, &n.name)
            && d.declarer != cat.tenant
        {
            return Err(inherited::not_overridable(format!(
                "stream {db}.{} is inherited from tenant {}; a member can't declare its own of \
                 that name",
                n.name, d.declarer
            )));
        }
        Ok(None)
    }

    /// A catalog change of this session's own tenant: read it at once.
    pub(super) async fn after_own_change(&mut self, v: u64) -> Result<()> {
        if let Some(ts) = self.tenant.as_deref_mut() {
            ts.changed(Some(v));
            ts.refresh().await?;
        }
        Ok(())
    }

    /// `CREATE [OR REPLACE] TRIGGER g ON QUEUE q FOR EACH TENANT ...`.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn define_trigger(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        sql: &str,
        _lex_opts: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let text = f.strip(sql).trim().trim_end_matches(';').to_owned();
        let decl = sprig_logic::declare::parse(&text)?;
        let Some(src) = decl.source.clone() else {
            return Err(not_built(
                "FOR EACH TENANT on an after-commit trigger (inherited triggers read streams)",
            ));
        };
        if src.kind != sprig_logic::declare::SourceKind::Queue {
            return Err(not_built(
                "FOR EACH TENANT on a trigger on a stream: it reads a FOR EACH TENANT stream, \
                 and inherited streams aren't built on the SprigDB engine yet",
            ));
        }
        let src_kind = DefKind::Queue;
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db = decl
            .db
            .clone()
            .or_else(|| current.clone())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let src_db = src
            .db
            .clone()
            .or_else(|| current.clone())
            .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        // The source: a stream or queue definition of this tenant, or one
        // reaching it.
        let source = own
            .def(&src_db, src_kind, &src.name)
            .cloned()
            .or_else(|| chain.resolve(&src_db, src_kind, &src.name))
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::StreamNotFound,
                    format!(
                        "ER_SPRIG_STREAM_NOT_FOUND: a FOR EACH TENANT trigger reads a FOR EACH \
                         TENANT {}; {src_db}.{} isn't one here",
                        src_kind.as_str(),
                        src.name
                    ),
                )
            })?;
        // The trigger checks: code in the trigger's database, reading the
        // stream; GRANTS are down-tree grants in every member.
        let lctn = ts.lower_case_table_names();
        let grants = crate::tenant::authz::privileges(ts, cat);
        let db_key = database_key(&db, lctn);
        if !grants.unrestricted()
            && !grants.allows(Action::CreateRoutine, Target::Database { db_key: &db_key })
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
        if !allows(ts, cat, Action::Select, &src_db, &src.name) {
            return Err(denied(ts, "SELECT", &src.name));
        }
        if !f.grants.is_empty() && !tenant_admin(ts, cat) {
            return Err(SprigError::standard(
                1227,
                *b"42000",
                "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN privilege(s) \
                 for this operation (GRANTS gives every member's sprig_inherited these rights)",
            ));
        }
        for g in &f.grants {
            triggers::actions_of(g)?;
        }
        sprig_logic::messages::check(&decl, src.kind).await?;
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        let existing = own.def(&db, DefKind::Trigger, &decl.name).cloned();
        if existing.is_some() && !decl.or_replace {
            return Err(SprigError::standard(
                1304,
                *b"42000",
                format!("TRIGGER {} already exists", decl.name),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        if existing.is_none() {
            check_quota(svc, cat, own)?;
        }
        let spec = TriggerSpec {
            text,
            source_kind: src_kind.as_str().into(),
            source_db: src_db.clone(),
            source_name: src.name.clone(),
            db: db.clone(),
        };
        let now = sprig_inherit::msg::store::now_ms();
        let def = match &existing {
            Some(e) => Definition {
                scope,
                definition: serde_json::to_value(&spec).unwrap_or_default(),
                grants: (!f.grants.is_empty())
                    .then(|| serde_json::to_value(&f.grants).unwrap_or_default()),
                def_version: e.def_version + 1,
                ..(**e).clone()
            },
            None => Definition {
                declarer: cat.tenant,
                object_id: next_object_id(svc, cat.tenant).await?,
                kind: DefKind::Trigger,
                db_name: db.clone(),
                name: decl.name.clone(),
                scope,
                definition: serde_json::to_value(&spec).unwrap_or_default(),
                grants: (!f.grants.is_empty())
                    .then(|| serde_json::to_value(&f.grants).unwrap_or_default()),
                def_version: 1,
                state: DefState::Active,
                drop_after_ms: None,
                created_by: user,
                created_at_ms: now,
                changed_version: 0,
            },
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        // Existing instances of the source get the trigger now, in the
        // background; members without one get it with their instance.
        let n = if src_kind == DefKind::Queue {
            mq_rollout(svc, source.declarer, &src_db, None).await?
        } else {
            rollout(svc, source.key()).await?
        };
        tracing::info!(tenant = %cat.tenant, def = %def.key(), name = %def.name, version = def.def_version, instances = n, "inherited trigger defined");
        Ok(Reply::Ok(
            0,
            vec![format!(
                "trigger {db}.{} (version {}) on {} {src_db}.{} runs in every member as its \
                 sprig_inherited; rolling out to {n} existing instance(s)",
                decl.name,
                def.def_version,
                src_kind.as_str(),
                src.name
            )],
        ))
    }

    /// `ALTER STREAM`, `DROP STREAM`, `DROP TRIGGER` naming a definition
    /// (or, in a member, an instance: 39897).
    #[allow(clippy::too_many_lines)]
    async fn inherit_ddl(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Option<Reply>> {
        let Some((kind, drop)) = grammar::ddl_kind(sql, lex_opts) else {
            return Ok(None);
        };
        let toks: Vec<_> = lex(sql.as_bytes(), lex_opts)
            .map_err(|_| SprigError::internal("lex"))?
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        let word = |i: usize| toks.get(i).map(|t| sql[t.range()].to_owned());
        let ident = |i: usize| {
            toks.get(i).map(|t| {
                String::from_utf8_lossy(&sprig_sql::lex::ident_value(t.bytes(sql.as_bytes())))
                    .into_owned()
            })
        };
        // ALTER|DROP KIND [IF EXISTS] [db.]name [IMMEDIATE | CASCADE]...
        let mut i = 2;
        let mut if_exists = false;
        if word(i).is_some_and(|w| w.eq_ignore_ascii_case("if")) {
            if_exists = true;
            i += 2;
        }
        let Some(first) = ident(i) else {
            return Ok(None);
        };
        let (db, name, next) = if word(i + 1).as_deref() == Some(".") {
            (Some(first), ident(i + 2).unwrap_or_default(), i + 3)
        } else {
            (None, first, i + 1)
        };
        let Some(db) = db.or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let rest: Vec<String> = (next..toks.len())
            .filter_map(|j| word(j).map(|w| w.to_ascii_lowercase()))
            .collect();
        // A member's instance: only ALTER INHERITED changes it. Not the
        // declarer's own instance of an `INCLUDING SELF` definition:
        // there the statement names the definition, which
        // the declarer could otherwise never drop (nor, by the 39895
        // guard, its database).
        if own.def(&db, kind, &name).is_none()
            && let Some(i) = own.instances.values().find(|i| {
                i.kind == kind
                    && i.state != sprig_inherit::msg::InstanceState::Detached
                    && phys_name(i.physical.as_ref()) == Some((&db, &name))
            })
        {
            let _ = i;
            return Err(inherited::not_overridable(format!(
                "{} {db}.{name} is inherited; use ALTER INHERITED {} {db}.{name} ... (settings, \
                 DISABLE, ENABLE, RESET, DETACH)",
                kind.as_str(),
                kind.as_str().to_ascii_uppercase()
            )));
        }
        let Some(def) = own.def(&db, kind, &name).cloned() else {
            // No SQL statement drops an ordinary exchange or queue yet: a
            // missing definition with IF EXISTS is a note, as MySQL answers
            // a missing table (1051 as a note).
            if drop && if_exists && matches!(kind, DefKind::Exchange | DefKind::Queue) {
                return Ok(Some(Reply::Ok(
                    0,
                    vec![format!("{} {db}.{name} doesn't exist", kind.as_str())],
                )));
            }
            return Ok(None);
        };
        let ts = self
            .tenant
            .as_deref()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        if drop {
            if !allows(ts, cat, Action::Drop, &db, &name) {
                return Err(denied(ts, "DROP", &name));
            }
            return self
                .drop_def(
                    svc,
                    cat,
                    own,
                    &def,
                    rest.iter().any(|w| w == "immediate"),
                    rest.iter().any(|w| w == "cascade"),
                )
                .await
                .map(Some);
        }
        // `ALTER STREAM` of a stream definition: none can be made any more.
        if kind == DefKind::Stream {
            return Err(streams_not_built());
        }
        Ok(None)
    }

    /// `DROP STREAM | TRIGGER` of a definition. A stream: refused while trigger definitions read it
    /// (unless `CASCADE`); with `IMMEDIATE` every instance goes now, else
    /// the definition is `dropping` until `inherit.drop_grace_hours`.
    /// A trigger: its instances' jobs stop and it goes now (it holds no
    /// data).
    #[allow(clippy::too_many_lines)]
    async fn drop_def(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        def: &Arc<Definition>,
        immediate: bool,
        cascade: bool,
    ) -> Result<Reply> {
        let mut writes_defs: Vec<Definition> = Vec::new();
        let mut deletes: Vec<u64> = Vec::new();
        let mut removed = 0usize;
        if def.kind == DefKind::Trigger {
            removed += drop_trigger_instances(svc, def.key()).await?;
            deletes.push(def.object_id);
        } else {
            {
                let readers: Vec<Arc<Definition>> = own
                    .defs
                    .values()
                    .filter(|g| g.kind == DefKind::Trigger)
                    .filter(|g| {
                        serde_json::from_value::<TriggerSpec>(g.definition.clone()).is_ok_and(|s| {
                            s.source_db.eq_ignore_ascii_case(&def.db_name)
                                && s.source_name == def.name
                        })
                    })
                    .cloned()
                    .collect();
                if !readers.is_empty() && !cascade {
                    return Err(SprigError::new(
                        SprigCode::InheritedObjectInUse,
                        format!(
                            "ER_SPRIG_INHERITED_OBJECT_IN_USE: trigger definition(s) {} read \
                             stream {}.{}; drop them first, or DROP ... CASCADE",
                            readers
                                .iter()
                                .map(|g| g.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            def.db_name,
                            def.name
                        ),
                    ));
                }
                for g in &readers {
                    removed += drop_trigger_instances(svc, g.key()).await?;
                    deletes.push(g.object_id);
                }
                // Binding definitions joining an exchange or queue go with it
                // (CASCADE).
                let binds: Vec<Arc<Definition>> = own
                    .defs
                    .values()
                    .filter(|b| b.kind == DefKind::Binding && def.kind != DefKind::Stream)
                    .filter(|b| {
                        serde_json::from_value::<sprig_inherit::msg::BindingSpec>(
                            b.definition.clone(),
                        )
                        .is_ok_and(|s| {
                            b.db_name.eq_ignore_ascii_case(&def.db_name)
                                && (s.source == def.name
                                    || (s.dest == def.name && s.dest_kind == def.kind.as_str()))
                        })
                    })
                    .cloned()
                    .collect();
                if !binds.is_empty() && !cascade {
                    return Err(SprigError::new(
                        SprigCode::InheritedObjectInUse,
                        format!(
                            "ER_SPRIG_INHERITED_OBJECT_IN_USE: binding definition(s) {} join {} \
                             {}.{}; UNBIND them first, or DROP ... CASCADE",
                            binds
                                .iter()
                                .map(|b| b.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            def.kind.as_str(),
                            def.db_name,
                            def.name
                        ),
                    ));
                }
                for b in &binds {
                    deletes.push(b.object_id);
                    if let Ok(spec) = serde_json::from_value::<sprig_inherit::msg::BindingSpec>(
                        b.definition.clone(),
                    ) {
                        mq_rollout(svc, cat.tenant, &def.db_name, Some(&spec)).await?;
                    }
                }
                if immediate {
                    let ts = self
                        .tenant
                        .as_deref()
                        .ok_or_else(|| SprigError::internal("no tenant"))?;
                    if !tenant_admin(ts, cat) {
                        return Err(SprigError::standard(
                            1227,
                            *b"42000",
                            "Access denied; you need (at least one of) the SPRIG_TENANT_ADMIN \
                             privilege(s) for this operation (DROP ... IMMEDIATE)",
                        ));
                    }
                    // Every instance, those stuck in 'creating' included.
                    removed += inherited::lifecycle::remove_all(svc, def).await?;
                    metrics::counter!("sprig_inherit_drops_total", "how" => "immediate")
                        .increment(1);
                    deletes.push(def.object_id);
                } else {
                    let grace = svc.settings.get(
                        &sprig_inherit::msg::settings::DROP_GRACE_HOURS,
                        &cat.setting_ctx(&svc.dc),
                        Some(&cat.settings),
                    );
                    writes_defs.push(Definition {
                        state: DefState::Dropping,
                        drop_after_ms: Some(
                            sprig_inherit::msg::store::now_ms() + grace.saturating_mul(3_600_000),
                        ),
                        def_version: def.def_version + 1,
                        ..(**def).clone()
                    });
                }
            }
        }
        let mut writes: Vec<MsgWrite<'_>> = writes_defs.iter().map(MsgWrite::Def).collect();
        writes.extend(deletes.iter().map(|o| MsgWrite::DeleteDef(*o)));
        let v = inherited::commit(svc, cat.tenant, &writes).await?;
        self.after_own_change(v).await?;
        // The grace's end: a gateway's pass removes the instances then.
        for d in &writes_defs {
            if let Some(at) = d.drop_after_ms {
                inherited::lifecycle::list_dropping(svc, d.key(), at).await?;
            }
        }
        tracing::info!(tenant = %cat.tenant, def = %def.key(), immediate, removed, "inherited definition dropped");
        Ok(Reply::Ok(
            0,
            vec![if writes_defs.is_empty() {
                format!(
                    "{} {}.{} dropped; {removed} instance(s) removed",
                    def.kind.as_str(),
                    def.db_name,
                    def.name
                )
            } else {
                format!(
                    "{} {}.{} is dropping: publishes stop, consumers drain, and members may \
                     DETACH their instance until the grace ends",
                    def.kind.as_str(),
                    def.db_name,
                    def.name
                )
            }],
        ))
    }
}

/// `{"db": .., "name": ..}` of an instance row.
fn phys_name(p: Option<&Value>) -> Option<(&str, &str)> {
    let p = p?;
    Some((p.get("db")?.as_str()?, p.get("name")?.as_str()?))
}

/// The scope of a `FOR EACH TENANT` clause in `declarer`.
pub(super) async fn scope_of(
    svc: &TenantServices,
    declarer: TenantId,
    f: &grammar::ForEach,
) -> Result<Scope> {
    let under = match &f.under {
        None => None,
        Some(n) => {
            let id = inherited::tenant_by_name(svc, n)
                .await?
                .ok_or_else(|| target_err(format!("UNDER {n}: no descendant of that name")))?;
            let e = inherited::dir_entry(svc, id)
                .await?
                .ok_or_else(|| target_err(format!("UNDER {n}: no descendant of that name")))?;
            let path = e.path_ids();
            if id == declarer || !path.contains(&declarer.0) {
                return Err(target_err(format!("UNDER {n}: no descendant of that name")));
            }
            Some(id.0)
        }
    };
    Ok(Scope {
        depth: f.depth,
        under,
        including_self: f.including_self,
    })
}

pub(super) fn check_quota(svc: &TenantServices, cat: &TenantCatalog, own: &MsgState) -> Result<()> {
    let max = svc.settings.get(
        &sprig_inherit::msg::settings::MAX_DEFINITIONS_PER_TENANT,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    );
    let have = own.defs.len() as u64;
    if have >= max {
        return Err(SprigError::new(
            SprigCode::InheritInstanceQuota,
            format!(
                "ER_SPRIG_INHERIT_INSTANCE_QUOTA: this tenant declares {have} FOR EACH TENANT \
                 definitions (inherit.max_definitions_per_tenant = {max})"
            ),
        ));
    }
    Ok(())
}

pub(super) async fn next_object_id(svc: &TenantServices, t: TenantId) -> Result<u64> {
    Ok(svc.catalog.backend().next_object_id(t).await?.0)
}

/// Every member holding an instance of stream definition `def` re-applies
/// its derived state (settings, triggers, sinks) now, in the background;
/// for a queue definition, its sinks. The number of instances.
pub(super) async fn rollout(svc: &Arc<TenantServices>, def: DefKey) -> Result<usize> {
    // A stream's derived state, or a queue's sinks.
    let n = crate::inherited::remote::rollout_here(svc, def).await?;
    // Members in other DCs: their DC's gateways do the same.
    let svc = Arc::clone(svc);
    tokio::spawn(async move {
        crate::inherited::remote::in_other_dcs(
            &svc,
            crate::inherited::remote::MemberOp::Rollout,
            def,
        )
        .await;
    });
    Ok(n)
}

/// Stops and forgets every member's instance of trigger definition `g`:
/// this DC's members, then those other DCs index (each DC's gateways stop
/// their own). The number of instances.
async fn drop_trigger_instances(svc: &Arc<TenantServices>, g: DefKey) -> Result<usize> {
    let here = crate::inherited::remote::drop_trigger_here(svc, g).await?;
    let there = crate::inherited::remote::in_other_dcs(
        svc,
        crate::inherited::remote::MemberOp::DropTrigger,
        g,
    )
    .await;
    Ok(here + usize::try_from(there).unwrap_or(0))
}

impl PassSession {
    /// `ALTER INHERITED ...`: overrides, the nearest level wins.
    #[allow(clippy::too_many_lines)]
    async fn inherit_alter(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        a: &grammar::AlterInherited,
    ) -> Result<Reply> {
        let db =
            a.db.clone()
                .or_else(|| self.current_logical())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let me = cat.tenant;
        let def = own
            .def(&db, a.kind, &a.name)
            .cloned()
            .or_else(|| chain.resolve(&db, a.kind, &a.name))
            .ok_or_else(|| {
                target_err(format!(
                    "no inherited {} {db}.{} reaches this tenant",
                    a.kind.as_str(),
                    a.name
                ))
            })?;
        let (target, applies) = match &a.target {
            For::Own => (me, AppliesTo::Own),
            For::Subtree(None) => (me, AppliesTo::Subtree),
            For::Tenant(n) | For::Subtree(Some(n)) => {
                let id = inherited::tenant_by_name(svc, n)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let e = inherited::dir_entry(svc, id)
                    .await?
                    .ok_or_else(|| target_err(format!("no descendant named {n}")))?;
                let below = e.path_ids().contains(&me.0);
                let ok = match a.target {
                    For::Tenant(_) => below && id != me,
                    _ => below,
                };
                if !ok {
                    return Err(target_err(format!("no descendant named {n}")));
                }
                (
                    id,
                    if matches!(a.target, For::Tenant(_)) {
                        AppliesTo::Own
                    } else {
                        AppliesTo::Subtree
                    },
                )
            }
        };
        if def.declarer == me && target == me {
            return Err(inherited::not_overridable(format!(
                "this tenant declares {} {db}.{}: change the definition itself (ALTER STREAM, \
                 CREATE OR REPLACE TRIGGER), or override it FOR TENANT t | FOR SUBTREE t",
                a.kind.as_str(),
                a.name
            )));
        }
        {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            let ok = if matches!(a.target, For::Own) {
                allows(ts, cat, Action::Alter, &db, &a.name)
            } else {
                tenant_admin(ts, cat)
            };
            if !ok {
                return Err(denied(ts, "ALTER", &a.name));
            }
        }
        let user = self
            .tenant
            .as_deref()
            .and_then(|t| t.principal.user_id)
            .map_or(0, |u| u.0);
        let existing = |what: What| {
            own.overrides
                .iter()
                .find(|o| {
                    o.def == def.key()
                        && o.target == target
                        && o.applies_to == applies
                        && o.what == what
                })
                .cloned()
        };
        let mk = |what: What, value: Value| Override {
            made_in: me,
            def: def.key(),
            target,
            applies_to: applies,
            what,
            value,
            set_by: user,
            set_at_ms: sprig_inherit::msg::store::now_ms(),
        };
        let mut puts: Vec<Override> = Vec::new();
        let mut deletes: Vec<Option<What>> = Vec::new();
        let what_done: String;
        match &a.op {
            AlterOp::Set(kvs) if a.kind == DefKind::Queue => {
                let mut m = existing(What::Settings)
                    .and_then(|o| o.value.as_object().cloned())
                    .unwrap_or_default();
                for (k, v) in kvs {
                    let k = k.replace('_', "-");
                    if !sprig_inherit::msg::model::QUEUE_OVERRIDABLE.contains(&k.as_str()) {
                        return Err(inherited::not_overridable(format!(
                            "{k} belongs to the definition; a member may set {}",
                            sprig_inherit::msg::model::QUEUE_OVERRIDABLE.join(", ")
                        )));
                    }
                    let val = if v.eq_ignore_ascii_case("null") {
                        Value::Null
                    } else {
                        v.parse::<i64>().map_or_else(|_| json!(v), |n| json!(n))
                    };
                    m.insert(k, val);
                }
                puts.push(mk(What::Settings, Value::Object(m)));
                what_done = "queue arguments".into();
            }
            AlterOp::Set(_) => {
                return Err(not_built(&format!(
                    "SET on an inherited {} yet (queues take SET; DISABLE, ENABLE, REPLACE and \
                     RESET are built)",
                    a.kind.as_str()
                )));
            }
            AlterOp::Reset(keys) if keys.is_empty() => {
                deletes.push(None);
                what_done = "every override".into();
            }
            AlterOp::Reset(keys) => {
                let mut m = existing(What::Settings)
                    .and_then(|o| o.value.as_object().cloned())
                    .unwrap_or_default();
                for k in keys {
                    m.remove(k);
                }
                if m.is_empty() {
                    deletes.push(Some(What::Settings));
                } else {
                    puts.push(mk(What::Settings, Value::Object(m)));
                }
                what_done = format!("settings {}", keys.join(", "));
            }
            AlterOp::Disable | AlterOp::Enable => {
                if !matches!(a.kind, DefKind::Trigger | DefKind::Binding) {
                    return Err(inherited::not_overridable(format!(
                        "an inherited {} can't be disabled (triggers, sinks and bindings can)",
                        a.kind.as_str()
                    )));
                }
                let on = matches!(a.op, AlterOp::Enable);
                puts.push(mk(
                    What::State,
                    json!(if on { "enabled" } else { "disabled" }),
                ));
                what_done = if on {
                    "enabled".into()
                } else {
                    "disabled".into()
                };
            }
            AlterOp::Replace(body) => {
                if a.kind != DefKind::Trigger {
                    return Err(inherited::not_overridable(
                        "only a trigger's body can be replaced",
                    ));
                }
                let spec: TriggerSpec =
                    serde_json::from_value(def.definition.clone()).unwrap_or_default();
                let queue = spec.source_kind == "queue";
                let full = format!(
                    "CREATE TRIGGER {} ON {} {}.{} {body}",
                    sprig_common::names::quote(&def.name),
                    if queue { "QUEUE" } else { "STREAM" },
                    sprig_common::names::quote(&spec.source_db),
                    sprig_common::names::quote(&spec.source_name),
                );
                let decl = sprig_logic::declare::parse(&full)?;
                sprig_logic::messages::check(
                    &decl,
                    if queue {
                        sprig_logic::declare::SourceKind::Queue
                    } else {
                        sprig_logic::declare::SourceKind::Stream
                    },
                )
                .await?;
                puts.push(mk(
                    What::Body,
                    json!({"text": full, "at": sprig_inherit::msg::store::now_ms()}),
                ));
                what_done = "body replaced".into();
            }
            AlterOp::Detach => {
                if !matches!(a.target, For::Own) {
                    return Err(inherited::not_overridable(
                        "DETACH is the member's own choice: run it in the member",
                    ));
                }
                let stopped = inherited::lifecycle::detach(svc, me, &def).await?;
                let v = cat.version + 1;
                self.after_own_change(v).await.ok();
                return Ok(Reply::Ok(
                    0,
                    vec![format!(
                        "{} {db}.{} is now this tenant's own, with its data; {stopped} \
                         inherited trigger(s) on it stopped",
                        a.kind.as_str(),
                        a.name
                    )],
                ));
            }
        }
        let mut writes: Vec<MsgWrite<'_>> = puts.iter().map(MsgWrite::Override).collect();
        for w in &deletes {
            writes.push(MsgWrite::DeleteOverrides {
                def: def.key(),
                target,
                applies_to: applies,
                what: *w,
            });
        }
        let v = inherited::commit(svc, me, &writes).await?;
        self.after_own_change(v).await?;
        // Members with an instance apply it now: this DC's, then those
        // other DCs index.
        let mut reached =
            inherited::remote::apply_override_here(svc, chain, &def, target, applies).await?;
        reached += usize::try_from(
            inherited::remote::in_other_dcs(
                svc,
                inherited::remote::MemberOp::Override {
                    by: me,
                    version: v,
                    target,
                    subtree: applies == AppliesTo::Subtree,
                },
                def.key(),
            )
            .await,
        )
        .unwrap_or(0);
        Ok(Reply::Ok(
            0,
            vec![format!(
                "{} {db}.{}: {what_done} for {}; applied to {reached} instance(s)",
                a.kind.as_str(),
                a.name,
                match applies {
                    AppliesTo::Own => format!("tenant {target}"),
                    AppliesTo::Subtree => format!("the subtree of tenant {target}"),
                }
            )],
        ))
    }

    /// `SHOW INHERITED ...`.
    #[allow(clippy::too_many_lines)]
    async fn inherit_show(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        chain: &Chain,
        own: &MsgState,
        s: &ShowInherited,
    ) -> Result<Reply> {
        let me = cat.tenant;
        match s {
            ShowInherited::Definitions => {
                let mut rows = Vec::new();
                for d in own.defs.values() {
                    let counted =
                        matches!(d.kind, DefKind::Stream | DefKind::Exchange | DefKind::Queue);
                    let mut n = inherited::instances_of(svc, d.key())
                        .await
                        .map_or(0, |v| v.len());
                    // Members homed in other DCs: their DCs count them.
                    if counted {
                        n += usize::try_from(
                            inherited::remote::in_other_dcs(
                                svc,
                                inherited::remote::MemberOp::Count,
                                d.key(),
                            )
                            .await,
                        )
                        .unwrap_or(0);
                    }
                    rows.push(vec![
                        Some(d.kind.as_str().to_owned()),
                        Some(d.db_name.clone()),
                        Some(d.name.clone()),
                        Some(serde_json::to_string(&d.scope).unwrap_or_default()),
                        Some(d.def_version.to_string()),
                        Some(d.state.as_str().to_owned()),
                        Some(if counted { n.to_string() } else { "-".into() }),
                    ]);
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Database",
                        "Name",
                        "Scope",
                        "Version",
                        "State",
                        "Instances",
                    ],
                    rows,
                ))
            }
            ShowInherited::Instances => {
                // Instance rows aren't versioned: read them, not the
                // snapshot.
                let live: std::collections::BTreeMap<DefKey, sprig_inherit::msg::Instance> =
                    sprig_inherit::msg::MsgStore::new(Arc::clone(&svc.records), me)
                        .instances()
                        .await?
                        .into_iter()
                        .map(|i| (i.def, i))
                        .collect();
                let _ = own;
                let mut rows = Vec::new();
                for kind in [
                    DefKind::Stream,
                    DefKind::Exchange,
                    DefKind::Queue,
                    DefKind::Binding,
                    DefKind::Trigger,
                    DefKind::Connection,
                    DefKind::Sink,
                ] {
                    for d in chain.reaching(kind) {
                        let i = live.get(&d.key());
                        let state =
                            if matches!(kind, DefKind::Trigger | DefKind::Binding | DefKind::Sink)
                                && chain.disabled(d.key())
                            {
                                "disabled".to_owned()
                            } else {
                                i.map_or_else(
                                    || {
                                        if matches!(kind, DefKind::Binding | DefKind::Connection) {
                                            "inherited".to_owned()
                                        } else {
                                            "none".to_owned()
                                        }
                                    },
                                    |i| {
                                        if i.physical
                                            .as_ref()
                                            .is_some_and(|p| p.get("blocked").is_some())
                                        {
                                            "blocked".to_owned()
                                        } else if let Some(p) = i.physical.as_ref().filter(|p| {
                                            p.get("waiting_config").is_some()
                                                || p.get("error").is_some()
                                        }) {
                                            // An inherited sink waiting for its
                                            // member's configuration (39241), or not deployable.
                                            if p.get("waiting_config").is_some() {
                                                "waiting_config".to_owned()
                                            } else {
                                                "error".to_owned()
                                            }
                                        } else {
                                            i.state.as_str().to_owned()
                                        }
                                    },
                                )
                            };
                        let from = if d.declarer == me {
                            "self".to_owned()
                        } else {
                            inherited::tenant_name(svc, d.declarer)
                                .await
                                .unwrap_or_else(|_| d.declarer.to_string())
                        };
                        rows.push(vec![
                            Some(kind.as_str().to_owned()),
                            Some(d.db_name.clone()),
                            Some(d.name.clone()),
                            Some(from),
                            Some(d.def_version.to_string()),
                            Some(state),
                            i.map(|i| i.instance_id.to_string()),
                            i.and_then(|i| i.physical.as_ref())
                                .and_then(|p| p.get("binding"))
                                .map(Value::to_string),
                        ]);
                    }
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Database",
                        "Name",
                        "Inherited_from",
                        "Version",
                        "Instance",
                        "Instance_id",
                        "Binding",
                    ],
                    rows,
                ))
            }
            ShowInherited::Broadcasts(kind, db, name) => {
                let db = db
                    .clone()
                    .or_else(|| self.current_logical())
                    .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
                let def = own
                    .def(&db, *kind, name)
                    .cloned()
                    .or_else(|| {
                        inherited::mq::declared_in(chain, &db)
                            .into_iter()
                            .chain(chain.reaching(DefKind::Stream))
                            .find(|d| d.kind == *kind && d.name == *name)
                    })
                    .ok_or_else(|| {
                        target_err(format!("no definition {} {db}.{name}", kind.as_str()))
                    })?;
                let rows = inherited::broadcast::show(svc, &def).await?;
                Ok(Reply::Rows(
                    vec![
                        "Broadcast",
                        "Publisher",
                        "Committed_ms",
                        "Form",
                        "Under",
                        "State",
                        "Delivered",
                        "Skipped",
                        "Error",
                        "Pending",
                    ],
                    rows,
                ))
            }
            ShowInherited::Overrides(filter) => {
                let anc = chain.ancestry();
                let mut rows = Vec::new();
                for l in &chain.levels {
                    let Some(st) = &l.state else { continue };
                    for o in &st.overrides {
                        let applies_here = o.target == me
                            || (o.applies_to == AppliesTo::Subtree && anc.contains(&o.target));
                        let mine = o.made_in == me;
                        if !applies_here && !mine {
                            continue;
                        }
                        let Some(d) = chain
                            .def(o.def)
                            .or_else(|| own.def_by_id(o.def.object).cloned())
                        else {
                            continue;
                        };
                        if let Some((k, db, n)) = filter
                            && (d.kind != *k
                                || &d.name != n
                                || db
                                    .as_deref()
                                    .is_some_and(|x| !x.eq_ignore_ascii_case(&d.db_name)))
                        {
                            continue;
                        }
                        rows.push(vec![
                            Some(d.kind.as_str().to_owned()),
                            Some(format!("{}.{}", d.db_name, d.name)),
                            Some(o.what.as_str().to_owned()),
                            Some(if o.what == What::Body {
                                "(replaced body)".to_owned()
                            } else {
                                o.value.to_string()
                            }),
                            Some(o.made_in.to_string()),
                            Some(o.target.to_string()),
                            Some(o.applies_to.as_str().to_owned()),
                        ]);
                    }
                }
                Ok(Reply::Rows(
                    vec![
                        "Kind",
                        "Name",
                        "What",
                        "Value",
                        "Set_in",
                        "For",
                        "Applies_to",
                    ],
                    rows,
                ))
            }
        }
    }
}

impl PassSession {
    /// Writes `recs` for `dest` into this tenant's cross-tenant outbox,
    /// through this session (in its transaction): in database `db` when the
    /// tenant has it, else the session's current one. The commit hook
    /// forwards them ([`inherited::outbox`]).
    async fn xo_publish(
        &mut self,
        cx: &mut ExecCtx<'_>,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        db: &str,
        dest: &inherited::outbox::Dest,
        recs: Vec<Publish>,
    ) -> Result<()> {
        let me = cat.tenant;
        let lctn = self
            .tenant
            .as_deref()
            .is_some_and(TenantSession::lower_case_table_names);
        let current = self.current_logical();
        let row = cat
            .database(&database_key(db, lctn))
            .or_else(|| {
                current
                    .as_deref()
                    .and_then(|c| cat.database(&database_key(c, lctn)))
            })
            .cloned()
            .ok_or_else(|| {
                SprigError::standard(
                    1046,
                    *b"3D000",
                    format!(
                        "No database selected: a publish into another tenant inside a \
                         transaction goes through an outbox in one of this tenant's \
                         databases ('{db}' or the current one)"
                    ),
                )
            })?;
        let schema = sprig_catalog::ddl::shard_schemas(me, &row)
            .first()
            .map(ToString::to_string)
            .ok_or_else(|| SprigError::internal("a database without a shard"))?;
        let cluster = svc.locator.cluster(me).await?;
        inherited::outbox::ensure_table(svc, me, &schema, &cluster).await?;
        let rows: Vec<([u8; 16], inherited::outbox::Dest, Publish)> = recs
            .into_iter()
            .map(|r| (inherited::outbox::new_id(), dest.clone(), r))
            .collect();
        let ids = rows.iter().map(|(i, _, _)| *i).collect();
        let sql = inherited::outbox::insert_sql(&row.name, &rows);
        self.run_checked(cx, &sql, true).await?;
        inherited::outbox::note(u64::from(cx.session.id.0), svc, me, &schema, ids);
        Ok(())
    }
}

impl PassSession {
    /// `CREATE EXCHANGE | QUEUE ... FOR EACH TENANT`, `BIND QUEUE |
    /// EXCHANGE ... FOR EACH TENANT`: definitions in this tenant; members make
    /// their instances on first use.
    #[allow(clippy::too_many_lines)]
    async fn define_mq(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
        f: &grammar::ForEach,
    ) -> Result<Reply> {
        let rest = f.strip(sql);
        let decl = grammar::mq_decl(&rest, lex_opts)?.ok_or_else(|| {
            SprigError::standard(
                1064,
                *b"42000",
                "expected CREATE EXCHANGE, CREATE QUEUE or BIND ... FOR EACH TENANT",
            )
        })?;
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
        let current = self.current_logical();
        let db_of = |d: &Option<String>| {
            d.clone()
                .or_else(|| current.clone())
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))
        };
        let (kind, db, name, definition, if_not_exists) = match &decl {
            grammar::MqDecl::Exchange {
                if_not_exists,
                db,
                name,
                kind,
                internal,
                arguments,
            } => {
                if sprig_queues::topology::ExKind::parse(kind).is_none() {
                    return Err(SprigError::standard(
                        1064,
                        *b"42000",
                        format!("unknown exchange type '{kind}'"),
                    ));
                }
                let spec = sprig_inherit::msg::ExchangeSpec {
                    kind: kind.clone(),
                    durable: true,
                    internal: *internal,
                    arguments: arguments.iter().cloned().collect(),
                };
                (
                    DefKind::Exchange,
                    db_of(db)?,
                    name.clone(),
                    serde_json::to_value(&spec).unwrap_or_default(),
                    *if_not_exists,
                )
            }
            grammar::MqDecl::Queue {
                if_not_exists,
                db,
                name,
                arguments,
            } => {
                if name.is_empty() || name.starts_with("amq.") {
                    return Err(SprigError::standard(
                        1103,
                        *b"42000",
                        format!("Incorrect queue name '{name}'"),
                    ));
                }
                if let Some((_, t)) = arguments.iter().find(|(k, _)| k == "x-queue-type")
                    && !matches!(t.as_str(), Some("classic" | "quorum"))
                {
                    return Err(not_built(&format!(
                        "x-queue-type {t} on a FOR EACH TENANT queue (classic and quorum are \
                         served)"
                    )));
                }
                let spec = sprig_inherit::msg::QueueSpec {
                    arguments: arguments.iter().cloned().collect(),
                };
                (
                    DefKind::Queue,
                    db_of(db)?,
                    name.clone(),
                    serde_json::to_value(&spec).unwrap_or_default(),
                    *if_not_exists,
                )
            }
            grammar::MqDecl::Bind {
                bind: true,
                db,
                dest_kind,
                dest,
                source,
                key,
                arguments,
            } => {
                let db = db_of(db)?;
                // Both ends are definitions of this tenant (a binding
                // between two inherited objects is inherited).
                let dk = if dest_kind == "exchange" {
                    DefKind::Exchange
                } else {
                    DefKind::Queue
                };
                if own.def(&db, DefKind::Exchange, source).is_none()
                    || own.def(&db, dk, dest).is_none()
                {
                    return Err(SprigError::standard(
                        1146,
                        *b"42S02",
                        format!(
                            "a FOR EACH TENANT binding joins two FOR EACH TENANT definitions of \
                             this tenant in {db}; exchange '{source}' or {dest_kind} '{dest}' \
                             isn't one"
                        ),
                    ));
                }
                let spec = sprig_inherit::msg::BindingSpec {
                    source: source.clone(),
                    dest: dest.clone(),
                    dest_kind: dest_kind.clone(),
                    key: key.clone(),
                    arguments: arguments.iter().cloned().collect(),
                };
                let name = spec.def_name();
                (
                    DefKind::Binding,
                    db,
                    name,
                    serde_json::to_value(&spec).unwrap_or_default(),
                    true,
                )
            }
            grammar::MqDecl::Bind { bind: false, .. } => {
                return Err(SprigError::standard(
                    1064,
                    *b"42000",
                    "UNBIND takes no FOR EACH TENANT: it drops the binding definition",
                ));
            }
        };
        if cat.database(&database_key(&db, lctn)).is_none() {
            return Err(SprigError::standard(
                1049,
                *b"42000",
                format!("Unknown database '{db}'"),
            ));
        }
        let check_name = if kind == DefKind::Binding {
            definition
                .get("dest")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        } else {
            name.clone()
        };
        if !allows(ts, cat, Action::Create, &db, &check_name) {
            return Err(denied(ts, "CREATE", &check_name));
        }
        let user = ts.principal.user_id.map_or(0, |u| u.0);
        if own.def(&db, kind, &name).is_some() {
            if if_not_exists {
                return Ok(Reply::Ok(0, Vec::new()));
            }
            return Err(SprigError::standard(
                1050,
                *b"42S01",
                format!("{} '{db}.{name}' is already defined", kind.as_str()),
            ));
        }
        let scope = scope_of(svc, cat.tenant, f).await?;
        check_quota(svc, cat, own)?;
        let def = Definition {
            declarer: cat.tenant,
            object_id: next_object_id(svc, cat.tenant).await?,
            kind,
            db_name: db.clone(),
            name: name.clone(),
            scope,
            definition,
            grants: None,
            def_version: 1,
            state: DefState::Active,
            drop_after_ms: None,
            created_by: user,
            created_at_ms: sprig_inherit::msg::store::now_ms(),
            changed_version: 0,
        };
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::Def(&def)]).await?;
        self.after_own_change(v).await?;
        // Members that already made this vhost's instances get it now, in
        // the background.
        let n = mq_rollout(svc, cat.tenant, &db, None).await?;
        tracing::info!(tenant = %cat.tenant, def = %def.key(), kind = kind.as_str(), name = %name, "inherited {} defined", kind.as_str());
        Ok(Reply::Ok(
            0,
            vec![format!(
                "{} {db}.{name} is defined for every tenant in scope; each member's instance is \
                 made on its first use of the vhost ({n} member(s) updated now)",
                kind.as_str()
            )],
        ))
    }

    /// `UNBIND QUEUE | EXCHANGE d FROM EXCHANGE s [KEY 'k']` naming a binding
    /// definition of this tenant: it goes, and so does each member's
    /// binding (a binding holds no data).
    async fn unbind_def(
        &mut self,
        svc: &Arc<TenantServices>,
        cat: &Arc<TenantCatalog>,
        own: &MsgState,
        sql: &str,
        lex_opts: &LexOptions,
    ) -> Result<Option<Reply>> {
        let Some(grammar::MqDecl::Bind {
            bind: false,
            db,
            dest_kind,
            dest,
            source,
            key,
            arguments,
        }) = grammar::mq_decl(sql, lex_opts)?
        else {
            return Ok(None);
        };
        let Some(db) = db.or_else(|| self.current_logical()) else {
            return Ok(None);
        };
        let spec = sprig_inherit::msg::BindingSpec {
            source,
            dest,
            dest_kind,
            key,
            arguments: arguments.into_iter().collect(),
        };
        let Some(def) = own.def(&db, DefKind::Binding, &spec.def_name()).cloned() else {
            return Ok(None);
        };
        {
            let ts = self
                .tenant
                .as_deref()
                .ok_or_else(|| SprigError::internal("no tenant"))?;
            if !allows(ts, cat, Action::Drop, &db, &spec.dest) {
                return Err(denied(ts, "DROP", &spec.dest));
            }
        }
        let v = inherited::commit(svc, cat.tenant, &[MsgWrite::DeleteDef(def.object_id)]).await?;
        self.after_own_change(v).await?;
        let n = mq_rollout(svc, cat.tenant, &db, Some(&spec)).await?;
        Ok(Some(Reply::Ok(
            0,
            vec![format!(
                "binding definition {db}.{} dropped; removed in {n} member(s)",
                def.name
            )],
        )))
    }

    /// A `sprig.mq_publish` in this session naming an inherited exchange
    /// (or, through the default exchange, an inherited queue) of vhost
    /// database `db` (publishing by target tenant):
    /// `Some(routed)` when it was handled here (a targeted publish, or a
    /// refusal), `None` to go on with the ordinary path, with `m` stamped
    /// when it goes into this tenant's own instance.
    #[allow(clippy::too_many_lines)]
    pub(super) async fn inherit_mq_publish(
        &mut self,
        cx: &mut ExecCtx<'_>,
        m: &mut sprig_proto_amqp::message::Message,
        mandatory: bool,
        vhost: &str,
    ) -> Result<Option<u32>> {
        let Some(ts) = self.tenant.as_deref_mut() else {
            return Ok(None);
        };
        let cat = ts.refresh().await?;
        let own = inherited::state_of(&cat);
        if own.is_empty() && cat.ancestry.is_empty() {
            return Ok(None);
        }
        let svc = Arc::clone(ts.services());
        let my_name = ts.principal.tenant_name.clone().unwrap_or_default();
        let db = sprig_proto_amqp::mem::default_database(
            vhost,
            &svc.settings.get(
                &crate::amqp::DEFAULT_VHOST_DATABASE,
                &cat.setting_ctx(&svc.dc),
                Some(&cat.settings),
            ),
        );
        let chain = inherited::chain(&svc, &cat).await?;
        let Some((kind, name)) = inherited::mq::named_def(&chain, &db, &m.exchange, &m.routing_key)
        else {
            return Ok(None);
        };
        let me = cat.tenant;
        let mine = inherited::mq::defs_in(&chain, &db)
            .iter()
            .any(|d| d.kind == kind && d.name == name);
        let target = inherited::mq::target_of(m);
        match target {
            Some((t, key)) if t == "*" => {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                if !allows(ts, &cat, Action::Insert, &db, &name) {
                    return Err(denied(ts, "INSERT", &name));
                }
                let def = inherited::mq::declared_in(&chain, &db)
                    .into_iter()
                    .find(|d| d.kind == kind && d.name == name)
                    .ok_or_else(|| target_err(format!("no definition {db}.{name}")))?;
                let rec = Publish {
                    key: Some(key.into_bytes()),
                    value: Some(m.body.to_vec()),
                    headers: Some(inherited::mq::xo_headers(m)),
                    partition: None,
                    ts_ms: None,
                };
                let form = inherited::broadcast::Form::default();
                if self.offsets_in_transaction() {
                    let bytes = rec.value.as_ref().map_or(0, Vec::len);
                    inherited::broadcast::check_limits(&svc, &def, me, bytes, form).await?;
                    let dest = inherited::outbox::Dest::Broadcast {
                        def_tenant: def.declarer.0,
                        def_object: def.object_id,
                        publisher: me.0,
                        form,
                    };
                    let current = self.current_logical().unwrap_or_else(|| db.clone());
                    self.xo_publish(cx, &svc, &cat, &current, &dest, vec![rec])
                        .await?;
                } else {
                    inherited::broadcast::publish_now(&svc, &def, me, form, &rec).await?;
                }
                Ok(Some(1))
            }
            Some((t, key)) if t != my_name && t != me.0.to_string() => {
                let ts = self
                    .tenant
                    .as_deref()
                    .ok_or_else(|| SprigError::internal("no tenant"))?;
                if !allows(ts, &cat, Action::Insert, &db, &m.exchange) {
                    return Err(denied(ts, "INSERT", &m.exchange));
                }
                let (tid, tname, tchain) =
                    inherited::mq::resolve_target(&svc, me, &t, &db, kind, &name).await?;
                let stamped = inherited::mq::stamp(m, &key, &tname, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                if !self.offsets_in_transaction() {
                    return Box::pin(inherited::mq::publish_into(
                        &svc, &tchain, &db, stamped, mandatory,
                    ))
                    .await
                    .map(Some);
                }
                // Inside a transaction: through this tenant's cross-tenant
                // outbox; forwarded into the member's queue outbox once
                // the transaction commits.
                let strict = svc.settings.get(
                    &sprig_inherit::msg::settings::CROSS_TENANT_MESSAGING_STRICT,
                    &cat.setting_ctx(&svc.dc),
                    Some(&cat.settings),
                ) == "on";
                if strict {
                    inherited::check_residency(&svc, tid, me, SprigCode::InheritResidency).await?;
                }
                let rec = Publish {
                    key: Some(stamped.routing_key.clone().into_bytes()),
                    value: Some(stamped.body.to_vec()),
                    headers: Some(inherited::mq::xo_headers(&stamped)),
                    partition: None,
                    ts_ms: None,
                };
                let dest = inherited::outbox::Dest::Exchange {
                    tenant: tid.0,
                    db: db.clone(),
                    exchange: stamped.exchange.clone(),
                };
                let current = self.current_logical().unwrap_or_else(|| db.clone());
                self.xo_publish(cx, &svc, &cat, &current, &dest, vec![rec])
                    .await?;
                Ok(Some(1))
            }
            Some((_, key)) if mine => {
                Box::pin(inherited::mq::ensure_vhost(
                    &svc,
                    &chain,
                    &db,
                    Cause::Publish,
                ))
                .await?;
                *m = inherited::mq::stamp(m, &key, &my_name, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                Ok(None)
            }
            None if mine => {
                Box::pin(inherited::mq::ensure_vhost(
                    &svc,
                    &chain,
                    &db,
                    Cause::Publish,
                ))
                .await?;
                let key = m.routing_key.clone();
                *m = inherited::mq::stamp(m, &key, &my_name, &my_name)
                    .map_err(|e| SprigError::standard(3140, *b"22032", e.text))?;
                Ok(None)
            }
            _ => Err(target_err(format!(
                "{} {db}.{name} is a FOR EACH TENANT definition: name the member with the \
                 x-sprig-tenant header or a routing key '@tenant:key'",
                kind.as_str()
            ))),
        }
    }
}

/// Re-applies database `db`'s inherited exchanges, queues and bindings in
/// every member of `declarer`'s definitions that has instances there, in
/// the background (a new definition, an override); with `unbind`, also
/// removes that binding in each first. Members homed in other DCs: a
/// gateway of each other DC does the same for the members its DC indexes.
/// The number of members in this DC.
async fn mq_rollout(
    svc: &Arc<TenantServices>,
    declarer: TenantId,
    db: &str,
    unbind: Option<&sprig_inherit::msg::BindingSpec>,
) -> Result<usize> {
    let n = inherited::mq::rollout_here(svc, declarer, db, unbind.cloned()).await?;
    let p = inherited::remote::MqRolloutPayload {
        db: db.to_owned(),
        unbind: unbind.cloned(),
    };
    let svc = Arc::clone(svc);
    tokio::spawn(async move {
        inherited::remote::mq_rollout_in_other_dcs(&svc, declarer, &p).await;
    });
    Ok(n)
}
