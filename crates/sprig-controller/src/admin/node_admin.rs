//! `sprigctl node`: the DC's engine nodes (`GET /admin/v1/nodes`) and
//! their operator states (`POST /admin/v1/nodes/{id}:drain|:undrain|
//! :remove|:restore`).
//!
//! A state change is all the controller does: each node follows the
//! registry and acts on its own record (a draining node hands its
//! leaderships to other voters; a removed node's units are re-replicated by
//! their leaders at once, as a dead node's are after
//! `replication.dead_node_minutes`).

use std::collections::BTreeMap;

use serde::Serialize;
use sprig_catalog::units::{self, NODE_ACTIVE, NODE_DEAD, NODE_DRAINING, NODE_REMOVED};
use sprig_common::Result;
use sprig_meta::{ListOpts, Record, Versioned};

use super::{AdminService, invalid};

/// One node as `sprigctl node list` shows it.
#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    /// Its Raft id.
    pub id: u64,
    /// Its gateway's id.
    pub gateway_id: String,
    /// `active`, `draining`, `dead` or `removed`.
    pub state: String,
    /// Why the state last changed.
    pub reason: String,
    /// When the state last changed (Unix milliseconds).
    pub since_ms: u64,
    /// Its binary's version (its registry record, else its gateway's).
    pub version: String,
    /// Its failure domain.
    pub failure_domain: String,
    /// Its gateway is registered and ready now.
    pub ready: bool,
    /// Its gateway's HTTP endpoint (`http://host:port`), when registered.
    pub http: Option<String>,
    /// Units whose descriptor lists it as a voter.
    pub units: usize,
    /// Units whose descriptor names it home.
    pub homes: usize,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Gw {
    gateway_id: String,
    state: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    addr: String,
    #[serde(default)]
    listeners: BTreeMap<String, u16>,
}

impl Versioned for Gw {
    const V: u32 = 1;
}

impl AdminService {
    /// Every engine node of this DC with its state, readiness and units.
    ///
    /// # Errors
    /// The store's.
    pub async fn list_nodes(&self) -> Result<Vec<NodeView>> {
        let dc = self.stores.dc.as_ref();
        let nodes = units::nodes(dc).await?;
        let prefix = dc.tier().key("gateways/")?;
        let gws: BTreeMap<String, Gw> = dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?
            .kvs
            .iter()
            .filter_map(|kv| Record::<Gw>::decode(&kv.value).ok())
            .map(|r| (r.body.gateway_id.clone(), r.body))
            .collect();
        let mut counts: BTreeMap<u64, (usize, usize)> = BTreeMap::new();
        for d in units::list(dc).await? {
            for v in &d.voters {
                counts.entry(*v).or_default().0 += 1;
            }
            counts.entry(d.home).or_default().1 += 1;
        }
        let mut out: Vec<NodeView> = nodes
            .into_iter()
            .map(|n| {
                let gw = gws.get(&n.gateway_id);
                let (units, homes) = counts.get(&n.id).copied().unwrap_or_default();
                NodeView {
                    id: n.id,
                    ready: gw.is_some_and(|g| g.state == "ready"),
                    http: gw.and_then(|g| {
                        g.listeners
                            .get("http")
                            .map(|p| format!("http://{}:{p}", g.addr))
                    }),
                    version: if n.version.is_empty() {
                        gw.map(|g| g.version.clone()).unwrap_or_default()
                    } else {
                        n.version.clone()
                    },
                    gateway_id: n.gateway_id,
                    state: n.state,
                    reason: n.reason,
                    since_ms: n.since_ms,
                    failure_domain: n.failure_domain,
                    units,
                    homes,
                }
            })
            .collect();
        out.sort_by_key(|n| n.id);
        Ok(out)
    }

    /// `POST /admin/v1/nodes/{id}:{method}`: `drain` (active → draining),
    /// `undrain` (draining → active), `remove` (any → removed: its units
    /// are re-replicated at once), `restore` (removed or dead → active).
    ///
    /// # Errors
    /// 39XXX invalid for an unknown method or node; the store's.
    pub async fn node_method(&self, id: u64, method: &str, reason: &str) -> Result<NodeView> {
        let (from, to): (&[&str], &str) = match method {
            "drain" => (&[NODE_ACTIVE], NODE_DRAINING),
            "undrain" => (&[NODE_DRAINING], NODE_ACTIVE),
            "remove" => (&[NODE_ACTIVE, NODE_DRAINING, NODE_DEAD], NODE_REMOVED),
            "restore" => (&[NODE_REMOVED, NODE_DEAD], NODE_ACTIVE),
            m => {
                return Err(invalid(format!(
                    "POST /admin/v1/nodes/{id}:{m}: expected :drain, :undrain, :remove or :restore"
                )));
            }
        };
        let reason = if reason.is_empty() {
            format!("sprigctl node {method}")
        } else {
            reason.to_owned()
        };
        let Some(rec) =
            units::set_node_state(self.stores.dc.as_ref(), id, from, to, &reason).await?
        else {
            return Err(invalid(format!("node {id} has no registry record")));
        };
        if rec.state != to {
            return Err(invalid(format!(
                "node {id} is {}; {method} needs it {}",
                rec.state,
                from.join(" or ")
            )));
        }
        tracing::info!(event = "decision", component = "controller", kind = "node.state",
            node = id, state = %to, reason = %reason, "an operator changed an engine node's state");
        self.list_nodes()
            .await?
            .into_iter()
            .find(|n| n.id == id)
            .ok_or_else(|| invalid(format!("node {id} has no registry record")))
    }

    /// The DC's cluster version (`null` before the first finalize).
    ///
    /// # Errors
    /// The store's.
    pub async fn cluster_version(
        &self,
    ) -> Result<Option<sprig_catalog::cluster_version::ClusterVersion>> {
        sprig_catalog::cluster_version::read(self.stores.dc.as_ref()).await
    }

    /// Raises the cluster version to `version` once every node runs it.
    ///
    /// # Errors
    /// Invalid naming the nodes still older; the store's.
    pub async fn finalize_version(
        &self,
        version: &str,
    ) -> Result<sprig_catalog::cluster_version::ClusterVersion> {
        let v =
            sprig_catalog::cluster_version::finalize(self.stores.dc.as_ref(), version, "sprigctl")
                .await?;
        tracing::info!(event = "decision", component = "controller", kind = "cluster.version",
            version = %v.version, "the cluster version was raised");
        Ok(v)
    }
}
