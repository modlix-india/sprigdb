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
//! ```
//!
//! Each member's append is its own transaction on the member's unit: a
//! publish from an ancestor isn't part of the ancestor's transaction, and
//! a broadcast that fails part way leaves the members before the failure
//! with the record.

use std::sync::Arc;

use serde_json::json;
use sprig_auth::authz::Action;
use sprig_catalog::TenantCatalog;
use sprig_catalog::ddl::database_key;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_inherit::msg::grammar::{self, InsertTarget, Target};
use sprig_inherit::msg::{Chain, DefKind, DefState, Definition, MsgState};
use sprig_sql::lex::LexOptions;
use sprig_streams::seg::kafka::{EngineStreams, Need};
use sprig_streams::seg::sql::{self as ssql, SqlHost};

use super::{Reply, allows, check_quota, denied, next_object_id, not_built, scope_of};
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
        if let Target::In(_) = it.target {
            return Err(not_built(
                "IN TENANT publishes into an ancestor's stream on the engine",
            ));
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
                let all = inherited::broadcast::members(
                    svc,
                    &def,
                    me,
                    form,
                    sprig_inherit::msg::store::now_ms(),
                )
                .await?;
                let full = format!("{db}.{}", it.name);
                let mut out = Vec::with_capacity(all.len());
                for t in all {
                    if *with_instance && eng.topic(t.0, &full)?.is_none() {
                        continue;
                    }
                    out.push(t);
                }
                out
            }
            Target::In(_) => Vec::new(),
        };
        let body = it.strip(sql);
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
            if let ssql::Answer::Ok(n) = ssql::run(&eng, t.0, plan).await? {
                affected += n;
            }
        }
        let notes = if matches!(it.target, Target::All { .. }) {
            vec![format!("published to {} member(s)", targets.len())]
        } else {
            Vec::new()
        };
        Ok(Reply::Ok(affected, notes))
    }
}
