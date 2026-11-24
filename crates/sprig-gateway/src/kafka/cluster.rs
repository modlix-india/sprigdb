//! Brokers and coordinator leases.
//!
//! - **Brokers** are the DC's ready gateways that run the `kafka` listener,
//!   from their registry records, read at most once a second.
//!   The broker id is the gateway's slot: unique among live gateways of a
//!   DC, and a small integer as Kafka needs. The host is the gateway's
//!   own `[gateway] kafka_advertise_host` from its record when set (one
//!   public name per VM), else, for this gateway,
//!   `kafka.advertised_host` when set, else the gateway's advertised
//!   address.
//! - **Coordinator leases** are etcd keys
//!   `/sprig/dc/<dc_id>/stream-groups/<tenant_id>/<group>` bound to one
//!   lease of this gateway (10 s, kept alive every 3 s). A key is created
//!   only if missing, so one gateway coordinates a group at a time. When
//!   the lease is lost, every group of this gateway is dropped (its members
//!   get `NOT_COORDINATOR`) and a later request takes a new lease.
//! - **Replicated engines.** A tenant's streams and groups commit only on
//!   its unit's Raft leader, so Metadata names the gateway leading it
//!   (found by the `raft_node` of the gateway records) as every
//!   partition's leader and the controller, FindCoordinator names it for
//!   every group, and a gateway that doesn't lead gives its coordinator
//!   leases up (the leader takes a group over at once). Writes that land
//!   elsewhere get `NOT_LEADER_OR_FOLLOWER` / `NOT_CONTROLLER` /
//!   `NOT_COORDINATOR`, and the client asks Metadata again.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sprig_common::CancellationToken;
use sprig_common::ids::DcId;
use sprig_meta::{Cmp, Consistency, Key, LeaseId, MetaStore, Op, OpResult, Txn};
use sprig_proto_kafka::broker::{BrokerNode, Cluster};
use sprig_proto_kafka::group::{GroupKey, LeaseState, Leases, StoreError};
use sprig_proto_kafka::topics::Principal;
use sprig_settings::{LiveSettings, SettingCtx};

use crate::registry::{self, GatewayState, Registrar};

/// The coordinator lease's time to live.
const LEASE_TTL: Duration = Duration::from_secs(10);
/// How often it is kept alive.
const KEEPALIVE: Duration = Duration::from_secs(3);
/// How long a broker list is reused.
const BROKERS_TTL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
struct LeaseValue {
    v: u32,
    gateway_id: String,
    node: i32,
}

#[derive(Default)]
struct Held {
    lease: Option<LeaseId>,
    epoch: u64,
    keys: HashSet<GroupKey>,
}

/// This gateway as a Kafka broker, and its coordinator leases.
pub struct GatewayCluster {
    store: Arc<dyn MetaStore>,
    registrar: Registrar,
    live: LiveSettings,
    dc: DcId,
    held: tokio::sync::Mutex<Held>,
    /// Serializes granting the coordinator lease; `held` is never kept
    /// across an etcd call (a slow etcd answer would hold every group's
    /// heartbeat, join and timer behind one group's lease request).
    granting: tokio::sync::Mutex<()>,
    brokers: Mutex<Option<Brokers>>,
}

/// The broker list as last read, with each replicated gateway's Raft id.
#[derive(Clone)]
struct Brokers {
    at: Instant,
    nodes: Vec<BrokerNode>,
    /// (Raft id, broker id) of the ready gateways whose engine replicates.
    raft: Vec<(u64, i32)>,
}

/// Where a tenant's unit is led, for Kafka routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lead {
    /// No gateway alone (a one-node engine, or a unit not started yet).
    Any,
    /// This gateway.
    Here,
    /// Another gateway: its broker id.
    Broker(i32),
    /// Not known now (an election), or led by a gateway that isn't ready.
    Unknown,
}

impl std::fmt::Debug for GatewayCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayCluster")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// Escapes a group id into one etcd key segment: bytes outside
/// `[A-Za-z0-9._-]` become `%XX`.
#[must_use]
pub fn key_segment(group: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(group.len());
    for b in group.bytes() {
        if b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn store_err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

impl GatewayCluster {
    /// The cluster of the gateway `registrar` registers, over the DC tier.
    #[must_use]
    pub fn new(
        store: Arc<dyn MetaStore>,
        registrar: Registrar,
        live: LiveSettings,
        dc: DcId,
    ) -> Self {
        Self {
            store,
            registrar,
            live,
            dc,
            held: tokio::sync::Mutex::new(Held::default()),
            granting: tokio::sync::Mutex::new(()),
            brokers: Mutex::new(None),
        }
    }

    fn key(&self, k: &GroupKey) -> Result<Key, StoreError> {
        self.store
            .tier()
            .key(&format!(
                "stream-groups/{}/{}",
                k.tenant,
                key_segment(&k.group)
            ))
            .map_err(store_err)
    }

    fn value(&self) -> Vec<u8> {
        let r = self.registrar.record();
        serde_json::to_vec(&LeaseValue {
            v: 1,
            gateway_id: r.gateway_id,
            node: i32::from(r.slot),
        })
        .unwrap_or_default()
    }

    fn node_of(raw: &[u8]) -> Option<i32> {
        serde_json::from_slice::<LeaseValue>(raw)
            .ok()
            .map(|v| v.node)
    }

    /// Keeps the coordinator lease alive until `stop`; a lost lease drops
    /// every group key this gateway held.
    pub async fn keep_leases(self: Arc<Self>, stop: CancellationToken) {
        loop {
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(KEEPALIVE) => {}
            }
            let lease = self.held.lock().await.lease;
            let Some(lease) = lease else { continue };
            // An error counts as a loss: the lease may expire before etcd
            // answers again, and another gateway may then coordinate.
            if !matches!(self.store.keep_alive(lease).await, Ok(Some(_))) {
                let mut h = self.held.lock().await;
                if h.lease == Some(lease) {
                    tracing::warn!("kafka coordinator lease lost: groups move to other gateways");
                    h.lease = None;
                    h.epoch += 1;
                    h.keys.clear();
                }
            }
        }
        let lease = self.held.lock().await.lease.take();
        if let Some(l) = lease {
            let _ = self.store.revoke_lease(l).await;
        }
    }
}

#[async_trait]
impl Cluster for GatewayCluster {
    fn node_id(&self) -> i32 {
        i32::from(self.registrar.record().slot)
    }

    fn cluster_id(&self) -> String {
        format!("sprig-{}", self.dc)
    }

    async fn brokers(&self, _p: &Principal) -> Vec<BrokerNode> {
        self.broker_list().await.nodes
    }

    async fn tenant_leader(&self, p: &Principal) -> Option<i32> {
        match self.lead_of(p.tenant_id).await {
            Lead::Here => Some(self.node_id()),
            Lead::Broker(b) => Some(b),
            Lead::Any | Lead::Unknown => None,
        }
    }

    async fn partition_leader(&self, p: &Principal, stream_id: u64, partition: i32) -> Option<i32> {
        // With partition groups a partition commits through its group's
        // unit, whose leader may be another gateway than the tenant's.
        let unit = super::engine::streams()
            .filter(|s| s.log().groups() > 0)
            .map_or(p.tenant_id, |s| {
                s.log().unit_of_part(sprig_streams::seg::part::PartKey {
                    tenant: p.tenant_id,
                    stream: stream_id,
                    partition: u32::try_from(partition).unwrap_or(0),
                })
            });
        match self.lead_of_unit(unit).await {
            Lead::Here => Some(self.node_id()),
            Lead::Broker(b) => Some(b),
            Lead::Any | Lead::Unknown => None,
        }
    }
}

impl GatewayCluster {
    /// Where `tenant`'s unit is led: on replicated engines a tenant's
    /// streams and consumer groups commit only on its unit's leader.
    async fn lead_of(&self, tenant: u64) -> Lead {
        self.lead_of_unit(tenant).await
    }

    /// Where engine unit `unit` is led (a tenant's, or a partition
    /// group's).
    async fn lead_of_unit(&self, unit: u64) -> Lead {
        match crate::node_engine::unit_leader(unit) {
            crate::node_engine::UnitLeader::Any => Lead::Any,
            crate::node_engine::UnitLeader::Here => Lead::Here,
            crate::node_engine::UnitLeader::Unknown => Lead::Unknown,
            crate::node_engine::UnitLeader::Node(id) => self
                .broker_list()
                .await
                .raft
                .iter()
                .find(|(r, _)| *r == id)
                .map_or(Lead::Unknown, |(_, b)| Lead::Broker(*b)),
        }
    }

    async fn broker_list(&self) -> Brokers {
        if let Some(b) = self
            .brokers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && b.at.elapsed() < BROKERS_TTL
        {
            return b.clone();
        }
        let advertised = self
            .live
            .get(&super::ADVERTISED_HOST, &SettingCtx::process(&self.dc));
        let records = match registry::list(self.store.as_ref()).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "kafka: gateway records unreadable");
                Vec::new()
            }
        };
        let me = self.registrar.record();
        let ready: Vec<_> = records
            .into_iter()
            .filter(|r| r.state == GatewayState::Ready)
            .collect();
        let raft = ready
            .iter()
            .filter(|r| r.listeners.contains_key("kafka"))
            .filter_map(|r| Some((r.raft_node?, i32::from(r.slot))))
            .collect();
        let mut out: Vec<BrokerNode> = ready
            .into_iter()
            .filter_map(|r| {
                let port = *r.listeners.get("kafka")?;
                // Its own public name first (`[gateway]
                // kafka_advertise_host`, one per VM), then the DC's
                // `kafka.advertised_host` for this gateway, then its address.
                let host = if let Some(h) = r.kafka_host.as_ref().filter(|h| !h.is_empty()) {
                    h.clone()
                } else if r.gateway_id == me.gateway_id && !advertised.is_empty() {
                    advertised.to_string()
                } else {
                    r.addr.clone()
                };
                Some(BrokerNode {
                    node_id: i32::from(r.slot),
                    host,
                    port: i32::from(port),
                    rack: Some(self.dc.to_string()),
                })
            })
            .collect();
        out.sort_by_key(|b| b.node_id);
        let b = Brokers {
            at: Instant::now(),
            nodes: out,
            raft,
        };
        *self.brokers.lock().unwrap_or_else(PoisonError::into_inner) = Some(b.clone());
        b
    }
}

#[async_trait]
impl Leases for GatewayCluster {
    async fn acquire(&self, k: &GroupKey) -> Result<LeaseState, StoreError> {
        let key = self.key(k)?;
        // On replicated engines a group's rows commit only on the tenant
        // unit's leader: elsewhere the group isn't coordinated (members are
        // sent to the leader), and the leader takes the group over from a
        // gateway that no longer leads.
        let lead = self.lead_of(k.tenant).await;
        match lead {
            Lead::Any | Lead::Here => {}
            Lead::Broker(b) => {
                self.release(k).await;
                return Ok(LeaseState::Elsewhere(b));
            }
            Lead::Unknown => {
                self.release(k).await;
                return Ok(LeaseState::Elsewhere(-1));
            }
        }
        let current = {
            let h = self.held.lock().await;
            if h.keys.contains(k) {
                return Ok(LeaseState::Held { epoch: h.epoch });
            }
            h.lease.map(|l| (l, h.epoch))
        };
        let (lease, epoch) = if let Some(c) = current {
            c
        } else {
            let _one = self.granting.lock().await;
            let again = {
                let h = self.held.lock().await;
                h.lease.map(|l| (l, h.epoch))
            };
            if let Some(c) = again {
                c
            } else {
                let l = self.store.grant_lease(LEASE_TTL).await.map_err(store_err)?;
                let mut h = self.held.lock().await;
                h.lease = Some(l);
                h.epoch += 1;
                (l, h.epoch)
            }
        };
        let put = Op::Put {
            key: key.clone(),
            value: self.value(),
            lease: Some(lease),
        };
        let r = self
            .store
            .txn(Txn {
                when: vec![Cmp::Missing(key.clone())],
                then: vec![put.clone()],
                otherwise: vec![Op::Get(key.clone())],
            })
            .await
            .map_err(store_err)?;
        let mut won = r.succeeded;
        let mut node = -1;
        if !won && let Some(OpResult::Get(Some(kv))) = r.results.first() {
            node = Self::node_of(&kv.value).unwrap_or(-1);
            // Our own broker id under another lease: an earlier incarnation
            // of this gateway (a restart) or a lease this gateway lost. Its
            // key would otherwise answer NOT_COORDINATOR to every member,
            // pointed back here, until its lease ran out (up to 10 s).
            // The tenant's unit is led here: a holder elsewhere can't
            // commit the group's rows any more, so take the group over.
            if (node == self.node_id() || lead == Lead::Here) && kv.lease != Some(lease) {
                let r = self
                    .store
                    .txn(Txn {
                        when: vec![Cmp::ModRev(key.clone(), kv.mod_revision)],
                        then: vec![put],
                        otherwise: Vec::new(),
                    })
                    .await
                    .map_err(store_err)?;
                won = r.succeeded;
            }
        }
        if won {
            let mut h = self.held.lock().await;
            if h.epoch != epoch || h.lease != Some(lease) {
                // The lease was lost while the key was written.
                return Err(StoreError("coordinator lease lost".into()));
            }
            h.keys.insert(k.clone());
            return Ok(LeaseState::Held { epoch });
        }
        Ok(LeaseState::Elsewhere(node))
    }

    async fn holder(&self, k: &GroupKey) -> Result<Option<i32>, StoreError> {
        let key = self.key(k)?;
        let kv = self
            .store
            .get(&key, Consistency::Serializable)
            .await
            .map_err(store_err)?;
        Ok(kv.and_then(|kv| Self::node_of(&kv.value)))
    }

    async fn confirm(&self, k: &GroupKey, epoch: u64) -> bool {
        if matches!(
            self.lead_of(k.tenant).await,
            Lead::Broker(_) | Lead::Unknown
        ) {
            return false;
        }
        let h = self.held.lock().await;
        h.epoch == epoch && h.keys.contains(k)
    }

    async fn release(&self, k: &GroupKey) {
        let Ok(key) = self.key(k) else { return };
        if !self.held.lock().await.keys.remove(k) {
            return;
        }
        // Until the key is gone, an acquire of it is refused (the key
        // exists) and the member retries: never two coordinators.
        let _ = self
            .store
            .txn(Txn {
                when: Vec::new(),
                then: vec![Op::Delete(key)],
                otherwise: Vec::new(),
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_ids_become_key_segments() {
        assert_eq!(key_segment("billing"), "billing");
        assert_eq!(key_segment("a/b c"), "a%2Fb%20c");
        assert_eq!(key_segment("connect-sink.1_x"), "connect-sink.1_x");
    }
}
