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
//! a DC that can't be reached: logged and counted; its gateways' orphan
//!   sweep (orphan_sinks) drops what a dropped definition left there, and a
//!   member's next publish applies a new definition
//! ```
//!
//! Gateways of another DC are found in that DC's tier of the one etcd
//! (single-etcd mode), as for the login hand-off.

use std::sync::Arc;

use sprig_api::proto::internal::{InheritedMembersReply, InheritedMembersRequest};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_inherit::msg::{DefKey, DefKind, Definition};

use crate::tenant::TenantServices;

/// What a declarer's change asks of another DC's members.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberOp {
    /// Drop the members' instances of a sink definition.
    DropSink,
    /// Re-apply the derived state (sinks, triggers, settings) of the
    /// members holding an instance of a stream or queue definition.
    Rollout,
}

impl MemberOp {
    fn as_str(self) -> &'static str {
        match self {
            Self::DropSink => "drop_sink",
            Self::Rollout => "rollout",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "drop_sink" => Some(Self::DropSink),
            "rollout" => Some(Self::Rollout),
            _ => None,
        }
    }
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

/// Asks a ready gateway of every other DC to apply `op` to definition
/// `def` for the members it indexes. The member instances they reached;
/// a DC that can't be reached is logged and counted
/// (`sprig_inherit_remote_calls_total{outcome="failed"}`).
pub async fn in_other_dcs(svc: &TenantServices, op: MemberOp, def: DefKey) -> u64 {
    let Some(peers) = svc.peers.get() else {
        return 0;
    };
    // The declarer's catalog as this gateway's session left it: the other
    // DC reads at least that version, not what its watch has delivered.
    let req = InheritedMembersRequest {
        op: op.as_str().to_owned(),
        tenant_id: def.tenant.0,
        object_id: def.object,
        catalog_version: svc.cache.cached(def.tenant).map_or(0, |c| c.version),
    };
    let mut n = 0;
    for dc in other_dcs(svc) {
        let gateways = match peers.gateways_in(&dc).await {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(%dc, %def, op = op.as_str(), error = %e, "can't list the DC's gateways for a FOR EACH TENANT change");
                metrics::counter!("sprig_inherit_remote_calls_total", "op" => op.as_str(), "outcome" => "failed").increment(1);
                continue;
            }
        };
        if gateways.is_empty() {
            tracing::debug!(%dc, %def, "no ready gateway in the DC: none of its members to reach");
            continue;
        }
        let mut done = false;
        for rec in &gateways {
            match peers.inherited_members(rec, req.clone()).await {
                Ok(r) => {
                    tracing::info!(%dc, gateway = %rec.gateway_id, %def, op = op.as_str(), instances = r.instances, "FOR EACH TENANT change applied in another DC");
                    metrics::counter!("sprig_inherit_remote_calls_total", "op" => op.as_str(), "outcome" => "ok").increment(1);
                    n += r.instances;
                    done = true;
                    break;
                }
                Err(e) => {
                    tracing::warn!(%dc, gateway = %rec.gateway_id, %def, op = op.as_str(), error = %e, "a gateway of another DC didn't apply a FOR EACH TENANT change; trying the next");
                }
            }
        }
        if !done {
            tracing::warn!(%dc, %def, op = op.as_str(), "no gateway of the DC applied the FOR EACH TENANT change; its orphan sweep or the members' next use catches up");
            metrics::counter!("sprig_inherit_remote_calls_total", "op" => op.as_str(), "outcome" => "failed").increment(1);
        }
    }
    n
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
/// 39792 for an unknown op; the catalog's and the store's.
pub async fn serve(
    svc: &Arc<TenantServices>,
    req: &InheritedMembersRequest,
) -> Result<InheritedMembersReply> {
    let op = MemberOp::parse(&req.op).ok_or_else(|| {
        SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!("unknown member op {:?}", req.op),
        )
    })?;
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
    let instances = match op {
        MemberOp::DropSink => {
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
        MemberOp::Rollout => rollout_here(svc, key).await?,
    };
    Ok(InheritedMembersReply {
        instances: u64::try_from(instances).unwrap_or(0),
    })
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
