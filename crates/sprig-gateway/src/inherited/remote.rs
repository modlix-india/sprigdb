//! `FOR EACH TENANT` changes reaching members in other data centres.
//!
//! ```text
//! each DC indexes the instances its own gateways made (inherit-instances/
//!   and sinks/ in its tier), so a declarer's gateway finds only its DC's
//! DROP SINK      this DC's members' instances first (sinks::drop_listed),
//!                then a ready gateway of every other registered DC drops
//!                the instances its DC indexes the same way
//!                (GatewayPeer.InheritedMembers "drop_sink"), before the
//!                definition goes
//! rollout        (CREATE or ALTER SINK FOR EACH TENANT and the other
//!                derived-state changes): this DC's members re-apply their
//!                state, and every other DC is asked to do the same
//!                ("rollout"), in the background
//! DROP TRIGGER   (and a stream's trigger readers going with it): this
//!                DC's instances stop, then every other DC's
//!                ("drop_trigger"), before the definition goes
//! override       ALTER ... FOR TENANT t | FOR SUBTREE t: the members it
//!                applies to here re-apply it, then every other DC's
//!                ("override", with the overriding tenant's catalog version)
//! SHOW INHERITED the instance counts add every other DC's ("count")
//! AMQP vhost     a new or dropped exchange, queue or binding definition,
//!                or an override of one: this DC's members re-apply their
//!                vhost (or drop the binding), then every other DC's do
//!                ("mq_rollout", the database and binding in the call)
//! broadcast      the fan-out delivers to a member homed in another DC
//!                through a gateway of that DC ("broadcast", the record in
//!                the call), so its instance and index stay in its home
//! a DC that can't be reached: logged and counted; its gateways' orphan
//!   sweep (orphan_sinks) drops what a dropped definition left there, a
//!   member's next publish applies a new definition, an override reaches
//!   it at its next use, and a broadcast goes to the retry list
//! ```
//!
//! Gateways of another DC are found in that DC's tier: through the one
//! etcd (single-etcd mode), or through its controllers' relay (two tiers),
//! as for the login hand-off.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sprig_api::proto::internal::{InheritedMembersReply, InheritedMembersRequest};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::{
    AppliesTo, BindingSpec, Cause, Chain, DefKey, DefKind, Definition, TriggerSpec,
};

use crate::tenant::TenantServices;

/// What a declarer's change asks of another DC's members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberOp {
    /// Drop the members' instances of a sink definition.
    DropSink,
    /// Re-apply the derived state (sinks, triggers, settings) of the
    /// members holding an instance of a stream or queue definition.
    Rollout,
    /// Stop and forget the members' instances of a trigger definition.
    DropTrigger,
    /// An override made in tenant `by` (its catalog at `version` or
    /// later) for `target`, alone or with its subtree.
    Override {
        /// The tenant that made it.
        by: TenantId,
        /// Its catalog version with the override.
        version: u64,
        /// For whom.
        target: TenantId,
        /// FOR SUBTREE.
        subtree: bool,
    },
    /// Count the instances only.
    Count,
}

impl MemberOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::DropSink => "drop_sink",
            Self::Rollout => "rollout",
            Self::DropTrigger => "drop_trigger",
            Self::Override { .. } => "override",
            Self::Count => "count",
        }
    }
}

/// `payload` of an override: who made it, and at which version.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct OverridePayload {
    by: u64,
    version: u64,
}

/// `payload` of a broadcast: the logged record.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BroadcastPayload {
    /// The broadcast id (16 bytes).
    pub bid: Vec<u8>,
    /// Who published it.
    pub publisher: u64,
    /// The record's key.
    pub key: Option<Vec<u8>>,
    /// Its value.
    pub value: Option<Vec<u8>>,
    /// Its headers, JSON text.
    pub headers: Option<String>,
    /// `WITH INSTANCE`: only a member whose instance exists.
    pub with_instance: bool,
}

/// `payload` of an AMQP vhost re-apply: the vhost's database, and the
/// binding to remove for an `UNBIND`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MqRolloutPayload {
    /// The vhost's database.
    pub db: String,
    /// Only this binding goes (an `UNBIND` of a binding definition).
    pub unbind: Option<BindingSpec>,
}

/// The registered DCs other than this one that aren't retired or
/// witnesses.
fn other_dcs(svc: &TenantServices) -> Vec<DcId> {
    crate::dc::registry()
        .map(crate::dc::DcRegistry::full_dcs)
        .unwrap_or_default()
        .into_iter()
        .filter(|d| *d != svc.dc)
        .collect()
}

fn failed(op: &str) {
    metrics::counter!("sprig_inherit_remote_calls_total", "op" => op.to_owned(), "outcome" => "failed")
        .increment(1);
}

/// `req` on a ready gateway of `dc`, each in turn until one answers: its
/// reply; `None` when the DC has no ready gateway.
///
/// # Errors
/// The last gateway's error when none answers; the store's.
async fn call_dc(
    svc: &TenantServices,
    dc: &DcId,
    req: &InheritedMembersRequest,
) -> Result<Option<InheritedMembersReply>> {
    let Some(peers) = svc.peers.get() else {
        return Ok(None);
    };
    let gateways = peers.gateways_in(dc).await?;
    let mut last = None;
    for rec in &gateways {
        match peers.inherited_members(rec, req.clone()).await {
            Ok(r) => {
                tracing::info!(%dc, gateway = %rec.gateway_id, op = %req.op, tenant = req.tenant_id, object = req.object_id, instances = r.instances, "FOR EACH TENANT change applied in another DC");
                metrics::counter!("sprig_inherit_remote_calls_total", "op" => req.op.clone(), "outcome" => "ok").increment(1);
                return Ok(Some(r));
            }
            Err(e) => {
                tracing::warn!(%dc, gateway = %rec.gateway_id, op = %req.op, error = %e, "a gateway of another DC didn't apply a FOR EACH TENANT change; trying the next");
                last = Some(e);
            }
        }
    }
    match last {
        Some(e) => Err(e),
        None => Ok(None),
    }
}

/// Asks a ready gateway of every other DC to apply `op` to definition
/// `def` for the members it indexes. The member instances they reached
/// (for [`MemberOp::Count`], have); a DC that can't be reached is logged
/// and counted (`sprig_inherit_remote_calls_total{outcome="failed"}`).
pub async fn in_other_dcs(svc: &TenantServices, op: MemberOp, def: DefKey) -> u64 {
    if svc.peers.get().is_none() {
        return 0;
    }
    let (catalog_version, target_tenant_id, subtree, payload) = match op {
        MemberOp::Override {
            by,
            version,
            target,
            subtree,
        } => (
            0,
            target.0,
            subtree,
            serde_json::to_vec(&OverridePayload { by: by.0, version }).unwrap_or_default(),
        ),
        // The declarer's catalog as this gateway's session left it: the
        // other DC reads at least that version, not what its watch has
        // delivered.
        _ => (
            svc.cache.cached(def.tenant).map_or(0, |c| c.version),
            0,
            false,
            Vec::new(),
        ),
    };
    let req = InheritedMembersRequest {
        op: op.as_str().to_owned(),
        tenant_id: def.tenant.0,
        object_id: def.object,
        catalog_version,
        target_tenant_id,
        subtree,
        payload,
    };
    fan_out(svc, &req).await
}

/// Asks a ready gateway of every other DC to re-apply vhost `p.db` of
/// `declarer`'s definitions (or remove binding `p.unbind`) for the members
/// it indexes, as [`in_other_dcs`]. The members they reached.
pub async fn mq_rollout_in_other_dcs(
    svc: &TenantServices,
    declarer: TenantId,
    p: &MqRolloutPayload,
) -> u64 {
    if svc.peers.get().is_none() {
        return 0;
    }
    let req = InheritedMembersRequest {
        op: MQ_ROLLOUT.to_owned(),
        tenant_id: declarer.0,
        object_id: 0,
        catalog_version: svc.cache.cached(declarer).map_or(0, |c| c.version),
        target_tenant_id: 0,
        subtree: false,
        payload: serde_json::to_vec(p).unwrap_or_default(),
    };
    fan_out(svc, &req).await
}

/// The `op` of an AMQP vhost re-apply.
const MQ_ROLLOUT: &str = "mq_rollout";

/// `req` on a ready gateway of every other DC; the instances they reached.
async fn fan_out(svc: &TenantServices, req: &InheritedMembersRequest) -> u64 {
    let def = DefKey {
        tenant: TenantId(req.tenant_id),
        object: req.object_id,
    };
    let mut n = 0;
    for dc in other_dcs(svc) {
        match call_dc(svc, &dc, req).await {
            Ok(Some(r)) => n += r.instances,
            Ok(None) => {
                tracing::debug!(%dc, %def, "no ready gateway in the DC: none of its members to reach");
            }
            Err(e) => {
                tracing::warn!(%dc, %def, op = %req.op, error = %e, "no gateway of the DC applied the FOR EACH TENANT change; its sweep or the members' next use catches up");
                failed(&req.op);
            }
        }
    }
    n
}

/// Delivers a broadcast of `def` to member `member`, homed in `dc`,
/// through a gateway there. `true` when delivered, `false` when skipped
/// (`WITH INSTANCE` and no instance).
///
/// # Errors
/// 39788 when no gateway of `dc` answers (the fan-out retries it); the
/// member's errors there.
pub async fn broadcast_in(
    svc: &TenantServices,
    dc: &DcId,
    def: DefKey,
    member: TenantId,
    payload: &BroadcastPayload,
) -> Result<bool> {
    let req = InheritedMembersRequest {
        op: "broadcast".to_owned(),
        tenant_id: def.tenant.0,
        object_id: def.object,
        catalog_version: svc.cache.cached(def.tenant).map_or(0, |c| c.version),
        target_tenant_id: member.0,
        subtree: false,
        payload: serde_json::to_vec(payload).map_err(|e| SprigError::internal(e.to_string()))?,
    };
    match call_dc(svc, dc, &req).await {
        Ok(Some(r)) => Ok(r.instances > 0),
        Ok(None) => Err(SprigError::new(
            SprigCode::ForwardFailed,
            format!(
                "ER_SPRIG_FORWARD_FAILED: no ready gateway in {dc} to deliver a broadcast to tenant {member}"
            ),
        )),
        Err(e) => {
            failed("broadcast");
            Err(e)
        }
    }
}

/// The declarer's definition `object` of kind `kind`, from its own rows.
fn definition(chain: &sprig_inherit::msg::Chain, key: DefKey) -> Option<Arc<Definition>> {
    chain
        .levels
        .iter()
        .filter(|l| l.tenant == key.tenant)
        .filter_map(|l| l.state.as_ref())
        .flat_map(|s| s.defs.values())
        .find(|d| d.key() == key)
        .cloned()
}

/// Serves `GatewayPeer.InheritedMembers`: applies the request's change to
/// the members this DC indexes.
///
/// # Errors
/// 39792 for an unknown op or a malformed payload; the catalog's and the
/// store's.
pub async fn serve(
    svc: &Arc<TenantServices>,
    req: &InheritedMembersRequest,
) -> Result<InheritedMembersReply> {
    let bad = |why: String| SprigError::new(SprigCode::AdminRequestInvalid, why);
    let key = DefKey {
        tenant: TenantId(req.tenant_id),
        object: req.object_id,
    };
    // The declarer's change, which the caller's commit just made: read at
    // least its version (the chain then reads this snapshot from the cache).
    if req.catalog_version > 0 {
        svc.cache
            .get(
                key.tenant,
                sprig_catalog::cache::Freshness::Cached {
                    min_version: req.catalog_version,
                },
            )
            .await?;
    }
    let instances = match req.op.as_str() {
        "drop_sink" => {
            let cat = super::catalog(svc, key.tenant).await?;
            let chain = super::chain(svc, &cat).await?;
            // Gone already (another gateway's drop committed): nothing to
            // drop by it; the sweep takes what is left.
            let Some(d) = definition(&chain, key).filter(|d| d.kind == DefKind::Sink) else {
                return Ok(InheritedMembersReply { instances: 0 });
            };
            // The members now; the drops in the background, so the
            // declarer's call returns within the peer timeout.
            let members = super::sinks::members_to_drop(svc, &chain, &d).await?;
            let n = members.len();
            let svc = Arc::clone(svc);
            tokio::spawn(async move {
                match super::sinks::drop_listed(&svc, &d, members).await {
                    Ok(n) => {
                        tracing::info!(def = %d.key(), instances = n, "inherited sink dropped in this DC for the declarer's gateway in another");
                    }
                    Err(e) => {
                        tracing::warn!(def = %d.key(), error = %e, "inherited sink instances not dropped; the orphan sweep takes them");
                    }
                }
            });
            n
        }
        "rollout" => rollout_here(svc, key).await?,
        MQ_ROLLOUT => {
            let p: MqRolloutPayload = serde_json::from_slice(&req.payload)
                .map_err(|e| bad(format!("mq_rollout payload: {e}")))?;
            super::mq::rollout_here(svc, key.tenant, &p.db, p.unbind).await?
        }
        "drop_trigger" => drop_trigger_here(svc, key).await?,
        "count" => super::instances_of(svc, key).await?.len(),
        "override" => {
            let p: OverridePayload = serde_json::from_slice(&req.payload)
                .map_err(|e| bad(format!("override payload: {e}")))?;
            let by = TenantId(p.by);
            let cat = svc
                .cache
                .get(
                    by,
                    sprig_catalog::cache::Freshness::Cached {
                        min_version: p.version,
                    },
                )
                .await?;
            let chain = super::chain(svc, &cat).await?;
            let Some(d) = definition(&chain, key) else {
                return Ok(InheritedMembersReply { instances: 0 });
            };
            apply_override_here(
                svc,
                &chain,
                &d,
                TenantId(req.target_tenant_id),
                if req.subtree {
                    AppliesTo::Subtree
                } else {
                    AppliesTo::Own
                },
            )
            .await?
        }
        "broadcast" => {
            let p: BroadcastPayload = serde_json::from_slice(&req.payload)
                .map_err(|e| bad(format!("broadcast payload: {e}")))?;
            let cat = super::catalog(svc, key.tenant).await?;
            let Some(d) = super::state_of(&cat).def_by_id(key.object).cloned() else {
                return Err(super::target_err(format!("definition {key} is gone")));
            };
            usize::from(
                super::broadcast::deliver_here(svc, &d, TenantId(req.target_tenant_id), &p).await?,
            )
        }
        other => return Err(bad(format!("unknown member op {other:?}"))),
    };
    Ok(InheritedMembersReply {
        instances: u64::try_from(instances).unwrap_or(0),
    })
}

/// Stops and forgets the instance of trigger definition `g` of every
/// member this DC indexes: its job's index entry and its instance row (the
/// member's module keeps its versions for the record). The number of
/// instances.
///
/// # Errors
/// The store's and the members' catalogs'.
pub async fn drop_trigger_here(svc: &Arc<TenantServices>, g: DefKey) -> Result<usize> {
    use serde_json::Value;

    // Trigger instances live with the members' stream instances.
    let prefix = svc.locator.dc_store().tier().key("inherit-instances/")?;
    let page = svc
        .locator
        .dc_store()
        .list(
            &prefix,
            sprig_meta::ListOpts {
                limit: 1_000_000,
                ..sprig_meta::ListOpts::default()
            },
        )
        .await?;
    let tenants: std::collections::BTreeSet<TenantId> = page
        .kvs
        .iter()
        .filter_map(|kv| {
            sprig_meta::Record::<super::InstanceIndex>::decode(&kv.value)
                .ok()
                .map(|r| r.body.tenant)
        })
        .collect();
    let mut n = 0;
    for t in tenants {
        // A member deleted since has nothing left to stop.
        if super::dir_entry(svc, t).await?.is_none() {
            continue;
        }
        let store = sprig_inherit::msg::MsgStore::new(Arc::clone(&svc.writers), t);
        if let Some(i) = store.instance(g).await? {
            if let Some(b) = i
                .physical
                .as_ref()
                .and_then(|p| p.get("binding"))
                .and_then(Value::as_u64)
            {
                sprig_logic::messages::delete_index(svc.locator.dc_store().as_ref(), t, b).await?;
                // The job ends at its next check; its module stays, marked.
                let module = i
                    .physical
                    .as_ref()
                    .and_then(|p| p.get("module"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let mut tx = svc.writers.begin(t).await?;
                if sprig_logic::LogicStore::default()
                    .mark_dropped(&mut *tx, module, 0)
                    .await
                    .is_ok()
                {
                    let _ = tx.commit().await;
                }
            }
            store.delete_instance(g).await?;
            n += 1;
        }
    }
    Ok(n)
}

/// The members this DC indexes that an override of `def`, made by
/// `chain`'s tenant for `target` (alone, or with its subtree), applies to
/// re-apply it now: exchanges, queues, bindings and triggers on queues
/// through the members' vhosts. The instances reached.
///
/// # Errors
/// The members' catalogs' and the store's.
pub async fn apply_override_here(
    svc: &Arc<TenantServices>,
    chain: &Chain,
    def: &Definition,
    target: TenantId,
    applies: AppliesTo,
) -> Result<usize> {
    let mut reached = 0;
    let hits = |t: TenantId| async move {
        if t == target {
            return Ok::<_, SprigError>(true);
        }
        Ok(applies == AppliesTo::Subtree
            && super::dir_entry(svc, t)
                .await?
                .is_some_and(|d| d.path_ids().contains(&target.0)))
    };
    let mq_db = match def.kind {
        DefKind::Exchange | DefKind::Queue | DefKind::Binding => Some(def.db_name.clone()),
        DefKind::Trigger => {
            let spec: TriggerSpec =
                serde_json::from_value(def.definition.clone()).unwrap_or_default();
            (spec.source_kind == "queue").then_some(spec.source_db)
        }
        _ => None,
    };
    if let Some(mdb) = mq_db {
        let mut members = std::collections::BTreeSet::new();
        for d in super::mq::declared_in(chain, &mdb)
            .into_iter()
            .chain(super::mq::defs_in(chain, &mdb))
        {
            for e in super::instances_of(svc, d.key()).await? {
                members.insert(e.tenant);
            }
        }
        for t in members {
            if !hits(t).await? {
                continue;
            }
            super::mq::forget_applied(t, Some(&mdb));
            let tcat = super::catalog(svc, t).await?;
            let tchain = super::chain(svc, &tcat).await?;
            if Box::pin(super::mq::ensure_vhost(svc, &tchain, &mdb, Cause::Declare)).await? {
                reached += 1;
            }
        }
    }
    Ok(reached)
}

/// Every member this DC indexes with an instance of stream or queue
/// definition `def` re-applies its derived state, in the background. The
/// number of instances.
///
/// # Errors
/// The store's.
pub async fn rollout_here(svc: &Arc<TenantServices>, def: DefKey) -> Result<usize> {
    let entries = super::instances_of(svc, def).await?;
    let n = entries.len();
    let svc = Arc::clone(svc);
    tokio::spawn(async move {
        for e in entries {
            let r = Box::pin(super::sinks::reapply_source(&svc, e.tenant, def))
                .await
                .and_then(|reached| {
                    if reached {
                        Ok(())
                    } else {
                        Err(SprigError::internal("the definition is gone"))
                    }
                });
            if let Err(err) = r {
                tracing::warn!(tenant = %e.tenant, def = %def, error = %err, "inherited instance not updated; its next use retries");
            }
        }
    });
    Ok(n)
}
