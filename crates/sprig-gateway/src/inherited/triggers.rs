//! Inherited triggers, run as the member's `sprig_inherited` principal:
//!
//! ```text
//! ensure_for_queue(T, queue definition Q, T's instance of Q):
//!   for each trigger definition G reaching T whose source is Q:
//!     disabled for T (ALTER INHERITED TRIGGER ... DISABLE, nearest level):
//!       its job's index entry is removed (the position stays); next
//!     no instance yet:
//!       T's sprig_inherited (created locked, no password anyone knows) gets
//!       G's GRANTS, once: what T's administrators revoke later stays revoked
//!       deploy G's body in T as a trigger of Q's instance, definer
//!       sprig_inherited, module database = Q's database in T; the index
//!       entry starts its job
//!     an instance whose body is behind (G changed, or the nearest body
//!     override changed): deploy the new body as a new version (OR REPLACE
//!       keeps the binding and the position); the job picks it up within 5 s
//!     enabled again: the index entry comes back
//! ```
//!
//! `ensure_for_stream` does the same for a stream instance (an engine
//! stream of T); a trigger new to the instance starts at each partition's
//! high watermark, and `sprig_inherited` also gets SELECT on the database.
//!
//! Every run is then an ordinary trigger run of T: in a gateway, as T's
//! `sprig_inherited`, once per message, in the engine tier.

use std::sync::Arc;

use serde_json::{Value, json};
use sprig_auth::admin::NewUser;
use sprig_auth::authz::privilege::{Action, ActionSet, PrivName, parse_privilege};
use sprig_auth::authz::store::{AuthzStore, Grantee, PrivTarget};
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{Namespace, ObjectKind};
use sprig_catalog::version::VersionKind;
use sprig_catalog::{CatalogLocator, ChangeOpts, TenantCatalog};
use sprig_common::ids::{TenantId, UserId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::store::MsgStore;
use sprig_inherit::msg::{
    Cause, Chain, DefKind, DefState, Definition, GrantItem, Instance, InstanceState, TriggerSpec,
    What, model::grant_items,
};
use sprig_logic::declare::SourceKind;
use sprig_logic::messages::{self, MsgIndex};

use super::{RUN_AS_USER, catalog};
use crate::tenant::TenantServices;

/// The trigger definitions reaching the member whose source is the queue
/// definition `queue`.
#[must_use]
pub fn on_queue(chain: &Chain, queue: &Definition) -> Vec<Arc<Definition>> {
    on_source(chain, queue)
}

/// The trigger definitions reaching the member whose source is the stream
/// or queue definition `source`.
fn on_source(chain: &Chain, source: &Definition) -> Vec<Arc<Definition>> {
    chain
        .reaching(DefKind::Trigger)
        .into_iter()
        .filter(|g| g.state == DefState::Active)
        .filter(|g| {
            let Ok(spec) = serde_json::from_value::<TriggerSpec>(g.definition.clone()) else {
                return false;
            };
            spec.source_kind == source.kind.as_str()
                && chain
                    .resolve(&spec.source_db, source.kind, &spec.source_name)
                    .is_some_and(|q| q.key() == source.key())
        })
        .collect()
}

/// The inherited triggers of the member's stream instance
/// (`stream_def`'s instance, stream `stream_id` of database `db`), as
/// [`ensure_for_queue`] does for queues. A trigger new to the instance
/// starts at each partition's high watermark (all of a new instance).
///
/// # Errors
/// The catalog's and the deploy's.
pub async fn ensure_for_stream(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    stream_def: &Definition,
    db: (u32, &str),
    stream_id: u64,
) -> Result<bool> {
    let triggers = on_source(chain, stream_def);
    if triggers.is_empty() {
        return Ok(true);
    }
    ensure_for(
        svc,
        chain,
        triggers,
        Source {
            tenant: chain.member(),
            db_id: db.0,
            db_name: db.1,
            object_id: stream_id,
            name: &stream_def.name,
            kind: SourceKind::Stream,
        },
    )
    .await
}

/// Whether a trigger definition reaching the member reads `source` and
/// isn't deployed on the member's instance yet (or is behind).
#[must_use]
pub fn pending_for(chain: &Chain, source: &Definition, st: &sprig_inherit::msg::MsgState) -> bool {
    on_source(chain, source).iter().any(|g| {
        if chain.disabled(g.key()) {
            return false;
        }
        let (_, mark) = body_of(chain, g);
        !st.instances.get(&g.key()).is_some_and(|i| {
            i.state == InstanceState::Active
                && i.physical
                    .as_ref()
                    .and_then(|p| p.get("applied"))
                    .and_then(Value::as_str)
                    == Some(mark.as_str())
        })
    })
}

/// [`ensure_for_stream`] for the chain's member's instance of `stream_def`,
/// when it has one.
///
/// # Errors
/// The catalog's, the stream log's and the deploy's.
pub async fn ensure_for_stream_instance(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    stream_def: &Definition,
) -> Result<bool> {
    let t = chain.member();
    let Some(streams) = crate::kafka::engine::streams() else {
        return Ok(true);
    };
    let full = format!("{}.{}", stream_def.db_name, stream_def.name);
    let Some(topic) = streams.topic(t.0, &full)? else {
        return Ok(true);
    };
    let cat = catalog(svc, t).await?;
    let Some(db) = cat
        .databases
        .iter()
        .find(|d| d.name.eq_ignore_ascii_case(&stream_def.db_name))
        .cloned()
    else {
        return Ok(true);
    };
    ensure_for_stream(
        svc,
        chain,
        stream_def,
        (db.db_id.0, &db.name),
        topic.stream_id,
    )
    .await
}

/// A new stream trigger's positions: each partition's high watermark, in
/// the trigger's own consumer group (kept when it has some already).
async fn start_positions(t: TenantId, binding: u64, stream_id: u64) -> Result<()> {
    let Some(streams) = crate::kafka::engine::streams() else {
        return Ok(());
    };
    let Some(topic) = streams.topic_by_id(t.0, stream_id)? else {
        return Ok(());
    };
    let group = messages::sprig_streams_group(binding);
    if streams
        .records()
        .offsets(t.0, &group, None)?
        .into_iter()
        .any(|o| o.stream_id == stream_id)
    {
        return Ok(());
    }
    let at: Vec<sprig_proto_kafka::group::CommittedOffset> = (0..topic.partitions.max(0))
        .map(|p| sprig_proto_kafka::group::CommittedOffset {
            stream_id,
            partition: p,
            offset: streams
                .log()
                .bounds(sprig_streams::seg::Key {
                    tenant: t.0,
                    stream: stream_id,
                    partition: u32::try_from(p).unwrap_or(0),
                })
                .high_watermark,
            leader_epoch: None,
            metadata: None,
        })
        .collect();
    streams
        .records()
        .commit_offsets(t.0, &group, &at, sprig_streams::seg::now_ms())
        .await
}

/// What an inherited trigger reads in the member: a queue instance (an
/// ordinary queue of the member).
#[derive(Clone, Copy)]
pub struct Source<'a> {
    /// The member.
    pub tenant: TenantId,
    /// The source's database.
    pub db_id: u32,
    /// Its name.
    pub db_name: &'a str,
    /// The stream's or queue's id.
    pub object_id: u64,
    /// Its name.
    pub name: &'a str,
    /// The source's kind (a queue).
    pub kind: SourceKind,
}

/// The body text the member's instance of `g` runs, and a mark of where
/// it came from (the definition's version, or the override's time).
fn body_of(chain: &Chain, g: &Definition) -> (String, String) {
    let spec: TriggerSpec = serde_json::from_value(g.definition.clone()).unwrap_or_default();
    match chain.effective(g.key(), What::Body, None) {
        Some((v, at)) => {
            let text = v
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or(&spec.text)
                .to_owned();
            let set = v.get("at").cloned().unwrap_or(Value::Null);
            (text, format!("override:{at}:{set}"))
        }
        None => (spec.text, format!("def:{}", g.def_version)),
    }
}

/// The inherited triggers of the member's queue instance (`queue_def`'s
/// instance, queue `queue_id` of database `db`); see the module docs.
/// Whether every trigger is settled: `false` when another deploy (the
/// rollout, another gateway, another session's publish) is still making
/// one, so the caller doesn't record the instance as up to date and its
/// next use looks again.
///
/// # Errors
/// The catalog's and the deploy's.
pub async fn ensure_for_queue(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    queue_def: &Definition,
    db: (u32, &str),
    queue_id: u64,
) -> Result<bool> {
    let triggers = on_queue(chain, queue_def);
    if triggers.is_empty() {
        return Ok(true);
    }
    ensure_for(
        svc,
        chain,
        triggers,
        Source {
            tenant: chain.member(),
            db_id: db.0,
            db_name: db.1,
            object_id: queue_id,
            name: &queue_def.name,
            kind: SourceKind::Queue,
        },
    )
    .await
}

#[allow(clippy::too_many_lines)]
async fn ensure_for(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    triggers: Vec<Arc<Definition>>,
    src: Source<'_>,
) -> Result<bool> {
    let t = src.tenant;
    let cat = catalog(svc, t).await?;
    let st = super::state_of(&cat);
    let mstore = MsgStore::new(Arc::clone(&svc.writers), t);
    let dc = svc.locator.dc_store();
    let mut settled = true;
    for g in triggers {
        let key = g.key();
        let disabled = chain.disabled(key);
        let row = match st.instances.get(&key) {
            Some(i) => Some((**i).clone()),
            None => mstore.instance(key).await?,
        };
        let binding = row
            .as_ref()
            .and_then(|i| i.physical.as_ref())
            .and_then(|p| p.get("binding"))
            .and_then(Value::as_u64);
        let module = row
            .as_ref()
            .and_then(|i| i.physical.as_ref())
            .and_then(|p| p.get("module"))
            .and_then(Value::as_u64);
        if disabled {
            // The job ends at its next check (a module that isn't active),
            // keeping its position; ENABLE resumes there.
            if let (Some(b), Some(m)) = (binding, module) {
                set_module_state(&svc.writers, t, m, false).await?;
                messages::delete_index(dc.as_ref(), t, b).await?;
            }
            continue;
        }
        let (text, mark) = body_of(chain, &g);
        let current = row
            .as_ref()
            .and_then(|i| i.physical.as_ref())
            .and_then(|p| p.get("applied"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let (Some(i), Some(b)) = (&row, binding)
            && i.state == InstanceState::Active
            && current.as_deref() == Some(mark.as_str())
        {
            // Up to date; make sure it runs (ENABLE after a DISABLE).
            if let Some(m) = module {
                set_module_state(&svc.writers, t, m, true).await?;
            }
            if !messages::indexed(dc.as_ref(), t, b).await? {
                messages::put_index(
                    dc.as_ref(),
                    &MsgIndex {
                        tenant: t,
                        binding: b,
                        kind: src.kind,
                    },
                )
                .await?;
            }
            continue;
        }
        // A new instance, or a new body.
        let _instance_id = if let Some(i) = &row {
            if i.state == InstanceState::Creating
                && !mstore.take_over(key, CLAIM_ABANDONED_MS).await?
            {
                // Another deploy claimed it and is making it now (the
                // rollout, or a publish on another connection or gateway).
                // Deploying it here too would race that one for the
                // module's version and fail with 39869.
                settled = false;
                continue;
            }
            i.instance_id
        } else {
            let id = svc.catalog.backend().next_object_id(t).await?.0;
            let claimed = mstore
                .claim(&Instance {
                    def: key,
                    instance_id: id,
                    kind: DefKind::Trigger,
                    def_version: g.def_version,
                    definition: g.definition.clone(),
                    state: InstanceState::Creating,
                    cause: Cause::Declare,
                    physical: None,
                    created_at_ms: 0,
                })
                .await?;
            if !claimed {
                // Another gateway is deploying it.
                settled = false;
                continue;
            }
            id
        };
        // A queue trigger takes its messages as sprig_inherited: it reads
        // them and deletes their rows in the vhost's shard (an
        // acknowledgement needs DELETE on the database), granted with
        // GRANTS.
        let extra: Vec<GrantItem> = if src.kind == SourceKind::Queue {
            vec![GrantItem {
                privileges: vec!["SELECT".into(), "DELETE".into()],
                db: src.db_name.to_owned(),
                name: "*".to_owned(),
            }]
        } else {
            Vec::new()
        };
        // A stream trigger reads the instance as sprig_inherited.
        let extra: Vec<GrantItem> = if src.kind == SourceKind::Stream {
            vec![GrantItem {
                privileges: vec!["SELECT".into()],
                db: src.db_name.to_owned(),
                name: "*".to_owned(),
            }]
        } else {
            extra
        };
        let run_as = ensure_run_as(svc, t, &g, binding.is_none(), &extra).await?;
        let deployed = match deploy(svc, t, &g, &text, src, run_as).await {
            Ok(d) => d,
            Err(e) if e.code() == Some(SprigCode::LogicVersionConflict) => {
                // A new body deployed by two at once: the other one took
                // the version and finishes the instance.
                tracing::debug!(tenant = %t, def = %key, "inherited trigger being deployed by another session");
                settled = false;
                continue;
            }
            Err(e) => return Err(e),
        };
        let physical = json!({
            "binding": deployed.binding_id,
            "module": deployed.module_id,
            "version": deployed.version,
            "applied": mark,
            "source": src.kind.as_str(),
            "stream": src.object_id,
        });
        if src.kind == SourceKind::Stream {
            start_positions(t, deployed.binding_id, src.object_id).await?;
        }
        set_module_state(&svc.writers, t, deployed.module_id, true).await?;
        mstore
            .set_instance(
                key,
                InstanceState::Active,
                g.def_version,
                &g.definition,
                Some(&physical),
            )
            .await?;
        messages::put_index(
            dc.as_ref(),
            &MsgIndex {
                tenant: t,
                binding: deployed.binding_id,
                kind: src.kind,
            },
        )
        .await?;
        metrics::counter!("sprig_inherit_instance_creations_total", "kind" => "trigger", "cause" => "declare")
            .increment(u64::from(row.is_none()));
        tracing::info!(tenant = %t, def = %key, binding = deployed.binding_id, version = deployed.version, source = src.kind.as_str(), "inherited trigger deployed");
    }
    Ok(settled)
}

/// A `creating` instance row nobody finished for this long was left by a
/// gateway that stopped while deploying it; the next use takes it over.
const CLAIM_ABANDONED_MS: u64 = 30_000;

/// Sets a module's state (`active` or `disabled`); the trigger job of a
/// module that isn't active ends at its next check.
async fn set_module_state(
    records: &Arc<dyn sprig_catalog::RecordStore>,
    t: TenantId,
    module: u64,
    enabled: bool,
) -> Result<()> {
    let mut tx = records.begin(t).await?;
    sprig_logic::LogicStore::default()
        .set_module_state(&mut *tx, module, enabled)
        .await?;
    tx.commit().await
}

/// Deploys `text` in `t` as the trigger of `src`, as `run_as`.
async fn deploy(
    svc: &Arc<TenantServices>,
    t: TenantId,
    g: &Definition,
    text: &str,
    src: Source<'_>,
    run_as: UserId,
) -> Result<messages::Deployed> {
    let mut decl = sprig_logic::declare::parse(text)?;
    decl.or_replace = true;
    if src.kind == SourceKind::Stream {
        // Stream triggers run on the engine tier.
        decl.tier = sprig_logic::declare::TierPref::Engine;
    }
    decl.db = Some(src.db_name.to_owned());
    if let Some(s) = decl.source.as_mut() {
        s.db = Some(src.db_name.to_owned());
        src.name.clone_into(&mut s.name);
    }
    let cat = catalog(svc, t).await?;
    let db_row = cat
        .databases
        .get(sprig_common::ids::DbId(src.db_id))
        .cloned()
        .ok_or_else(|| SprigError::internal("an instance without its database"))?;
    let ep = svc.locator.writer(t).await?;
    let mut conn = svc.writers.begin(t).await?;
    // A member's own trigger of the name shadows the inherited one.
    let store = sprig_logic::LogicStore::default();
    if let Some(m) = store
        .module(&mut *conn, db_row.db_id.0, "trigger", &decl.name)
        .await?
        && m.created_by != run_as.0
    {
        return Err(super::not_overridable(format!(
            "tenant {t} has its own trigger {}.{}; it shadows the inherited one from tenant {}",
            db_row.name, decl.name, g.declarer
        )));
    }
    let tname = super::tenant_name(svc, t).await?;
    let deployed = messages::deploy(
        &decl,
        messages::MsgTarget {
            catalog: &mut *conn,
            catalog_schema: ep.store.schema(),
            catalog_version: cat.version,
            db_id: db_row.db_id.0,
            user_id: run_as.0,
            tenant_name: &tname,
            source_db: (db_row.db_id.0, &db_row.name),
            object_id: src.object_id,
            fanin_declarer: 0,
            compile: None,
        },
    )
    .await?;
    conn.commit().await?;
    Ok(deployed)
}

/// The actions of a `GRANTS` item's privilege names.
///
/// # Errors
/// 1064 for a name that isn't a privilege, or `ALL`.
pub fn actions_of(item: &GrantItem) -> Result<ActionSet> {
    let mut set = ActionSet::EMPTY;
    for p in &item.privileges {
        match parse_privilege(p) {
            PrivName::Action(a) if a != Action::GrantOption => {
                set = set.union(ActionSet::of(&[a]));
            }
            _ => {
                return Err(SprigError::standard(
                    1064,
                    *b"42000",
                    format!(
                        "GRANTS: '{p}' isn't a privilege an inherited trigger can hold (name each \
                         privilege; ALL, USAGE and GRANT OPTION aren't allowed)"
                    ),
                ));
            }
        }
    }
    Ok(set)
}

/// `t`'s `sprig_inherited`: created when missing (locked: nobody can log
/// in as it); with `grant`, `g`'s `GRANTS` are added to it. Its backend
/// account follows.
///
/// # Errors
/// The catalog's.
pub async fn ensure_run_as(
    svc: &Arc<TenantServices>,
    t: TenantId,
    g: &Definition,
    grant: bool,
    extra: &[GrantItem],
) -> Result<UserId> {
    let mut cat = catalog(svc, t).await?;
    let find = |c: &TenantCatalog| {
        sprig_auth::cache::TenantAuth::of(c)
            .and_then(|a| a.accounts(RUN_AS_USER).first().map(|u| u.row.user_id))
    };
    let user = if let Some(u) = find(&cat) {
        u
    } else {
        let mut pw = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut pw)
            .map_err(|_| SprigError::internal("no randomness"))?;
        let mut new = NewUser::new(RUN_AS_USER, hex(&pw).as_bytes());
        new.account_locked = true;
        new.created_by_tenant = g.declarer.0;
        match sprig_auth::admin::create_user(&svc.auth, t, &new, &*svc.clock).await {
            Ok(c) => {
                cat = wait_auth(svc, t, c.auth_version).await?;
                c.user_id
            }
            Err(e) => {
                // Created meanwhile by another gateway.
                cat = catalog(svc, t).await?;
                find(&cat).ok_or(e)?
            }
        }
    };
    let mut items = grant_items(g.grants.as_ref());
    items.extend_from_slice(extra);
    if grant && !items.is_empty() {
        let lctn = svc.settings.get(
            &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
            &cat.setting_ctx(&svc.dc),
            Some(&cat.settings),
        ) == 1;
        let mut change = svc
            .auth
            .change_with(t, VersionKind::Auth, ChangeOpts::default())
            .await?;
        let r = {
            let q = change.tx().records();
            let store = AuthzStore::new();
            async {
                for it in &items {
                    let actions = actions_of(it)?;
                    let db_key = database_key(&it.db, lctn);
                    let target = if it.name == "*" {
                        PrivTarget::Database(db_key)
                    } else {
                        let name_key = object_key(ObjectKind::Table, &it.name, lctn);
                        match cat.database(&db_key).and_then(|d| {
                            cat.object(d.db_id, Namespace::Relation, &name_key)
                                .map(|o| (d.db_id, o.object_id))
                        }) {
                            Some((db, object)) => PrivTarget::Object { db, object },
                            None => PrivTarget::Named {
                                db_key,
                                name_key,
                                column: None,
                            },
                        }
                    };
                    store
                        .grant(q, Grantee::User(user), &target, actions, None)
                        .await?;
                }
                Ok::<_, SprigError>(())
            }
            .await
        };
        if let Err(e) = r {
            let _ = change.rollback().await;
            return Err(e);
        }
        let v = change.commit().await?;
        cat = wait_auth(svc, t, v).await?;
    }
    // Its backend account on the member's cluster.
    let cluster = svc.locator.cluster(t).await?;
    if let Err(e) = svc.authz.ensure(svc, &cat, &cluster).await {
        tracing::warn!(tenant = %t, error = %e, "sprig_inherited's backend grants not applied yet");
    }
    Ok(user)
}

/// Takes back `SELECT, DELETE ON db.*` from member `t`'s `sprig_inherited`:
/// the database grant inherited queue sinks had before they were granted
/// their queue alone. Kept while an inherited trigger of `chain` is declared
/// in `db` (a queue trigger acknowledges inside its transaction, which needs
/// the database's `DELETE`). True when a grant was taken back.
///
/// # Errors
/// The catalog's.
pub async fn revoke_database_grant(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    t: TenantId,
    db: &str,
) -> Result<bool> {
    if chain
        .reaching(DefKind::Trigger)
        .iter()
        .any(|d| d.db_name.eq_ignore_ascii_case(db))
    {
        return Ok(false);
    }
    let cat = catalog(svc, t).await?;
    let Some(user) = sprig_auth::cache::TenantAuth::of(&cat)
        .and_then(|a| a.accounts(RUN_AS_USER).first().map(|u| u.row.user_id))
    else {
        return Ok(false);
    };
    let lctn = svc.settings.get(
        &sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1;
    let actions = actions_of(&GrantItem {
        privileges: vec!["SELECT".into(), "DELETE".into()],
        db: db.to_owned(),
        name: "*".to_owned(),
    })?;
    let mut change = svc
        .auth
        .change_with(t, VersionKind::Auth, ChangeOpts::default())
        .await?;
    let before = {
        let q = change.tx().records();
        AuthzStore::new()
            .revoke(
                q,
                Grantee::User(user),
                &PrivTarget::Database(database_key(db, lctn)),
                actions,
            )
            .await
    };
    let before = match before {
        Ok(b) => b,
        Err(e) => {
            let _ = change.rollback().await;
            return Err(e);
        }
    };
    if before == ActionSet::EMPTY {
        let _ = change.rollback().await;
        return Ok(false);
    }
    let v = change.commit().await?;
    let cat = wait_auth(svc, t, v).await?;
    let cluster = svc.locator.cluster(t).await?;
    if let Err(e) = svc.authz.ensure(svc, &cat, &cluster).await {
        tracing::warn!(tenant = %t, error = %e, "sprig_inherited's backend grants not applied yet");
    }
    tracing::info!(tenant = %t, db, "inherited queue sink: the database grant taken back (its queue's grant stays)");
    Ok(true)
}

async fn wait_auth(svc: &TenantServices, t: TenantId, v: u64) -> Result<Arc<TenantCatalog>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let c = catalog(svc, t).await?;
        if c.auth_version >= v || std::time::Instant::now() > deadline {
            return Ok(c);
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// Whether `e` is a statement the session's user may not run (MySQL's
/// 1142, 1143, 1044, 1227, 1370).
#[must_use]
pub fn is_privilege_error(e: &SprigError) -> bool {
    matches!(
        sprig_logic::messages::error_number(e),
        1142 | 1143 | 1044 | 1227 | 1370
    )
}

fn blocked_set() -> &'static std::sync::Mutex<std::collections::HashSet<(TenantId, u64)>> {
    static M: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<(TenantId, u64)>>> =
        std::sync::OnceLock::new();
    M.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

/// A run of an inherited trigger (its definer is `sprig_inherited`) that
/// failed for a missing privilege becomes transient with 39242, so the record
/// waits (retried with backoff, never dead-lettered) until the grant returns;
/// the instance is marked `blocked` meanwhile, and unmarked by its next good
/// run.
pub async fn on_run(
    svc: &Arc<TenantServices>,
    t: TenantId,
    user: &str,
    binding: u64,
    mut reply: messages::RunReply,
) -> messages::RunReply {
    use messages::RunOutcome;
    if user != RUN_AS_USER {
        return reply;
    }
    let privilege = reply.outcome == RunOutcome::Failed
        && matches!(reply.code, 1142 | 1143 | 1044 | 1227 | 1370);
    let key = (t, binding);
    let was = blocked_set()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(&key);
    if privilege {
        reply.message = format!(
            "ER_SPRIG_INHERIT_RUN_AS_PRIVILEGE: this tenant's sprig_inherited lacks a privilege \
             the definition's GRANTS asked for; the trigger waits until it is granted again \
             ({} {})",
            reply.code, reply.message
        );
        reply.code = u32::from(SprigCode::InheritRunAsPrivilege.number());
        reply.transient = true;
        if !was {
            blocked_set()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key);
            tracing::warn!(tenant = %t, binding, "inherited trigger blocked (39242): a privilege was revoked");
            metrics::gauge!("sprig_inherit_blocked_instances").increment(1.0);
            mark_blocked(svc, t, binding, Some(&reply.message)).await;
        }
    } else if was && matches!(reply.outcome, RunOutcome::Ok | RunOutcome::Moved) {
        blocked_set()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        tracing::info!(tenant = %t, binding, "inherited trigger unblocked");
        metrics::gauge!("sprig_inherit_blocked_instances").decrement(1.0);
        mark_blocked(svc, t, binding, None).await;
    }
    reply
}

/// Sets (or clears) `blocked` in the instance row of the trigger whose
/// binding is `binding`.
async fn mark_blocked(svc: &TenantServices, t: TenantId, binding: u64, why: Option<&str>) {
    let r = MsgStore::new(Arc::clone(&svc.writers), t)
        .mark_blocked(binding, why)
        .await;
    if let Err(e) = r {
        tracing::debug!(tenant = %t, binding, error = %e, "blocked state not recorded");
    }
}
