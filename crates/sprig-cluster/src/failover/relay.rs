//! Sprig 2PC's relay on Galera clusters: which node's agent relays a
//! `sprig_2pc` tenant's connections, shared by the agents that run the
//! relay, the gateways that connect through it and the controller's
//! resolver.
//!
//! ```text
//! relays/<cluster>         set once by the first agent that starts a relay for the
//!                          cluster, never lease-bound: from then on gateways send
//!                          relayed tenants' connections to this cluster only
//!                          through the relay writer, never straight to a node
//! leases/relay/<cluster>   the relay writer: the agent holding it, under its own
//!                          etcd lease. Created only when missing. The holder
//!                          recovers every held branch on its node before it sets
//!                          ready; gateways connect only to a ready holder
//! ```
//!
//! One relay writer per cluster (not per shard, as Galera's writer set
//! picks): a held branch's rows are fenced only on the node that holds
//! them, so every connection of the tenant to the cluster must pass that
//! node's relay.
//!
//! **Relay hosts.** Each relay agent adds the address its node sees its
//! connections come from to `relays/<cluster>`'s `hosts`. A relayed
//! tenant's backend accounts on the cluster log in only from those
//! addresses: its accounts at `'%'` are locked (they stay, as the definers
//! of its stored code), and the same accounts at each relay host carry the
//! logins. Only the ready relay writer admits anyone, so a login from a
//! relay host is a login through the fence.

use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_common::ids::ClusterId;
use sprig_meta::{Key, Tier, Versioned};

/// `relays/<cluster>`: the cluster's tenants that choose the relay path
/// reach it only through its relay writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayCluster {
    /// The agent that first started a relay for the cluster.
    pub first_agent: String,
    /// When, in milliseconds since the epoch.
    #[serde(default)]
    pub since_ms: u64,
    /// Every relay agent of the cluster, by agent id: the host its node
    /// sees the agent's connections come from (MariaDB's account host).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub hosts: std::collections::BTreeMap<String, String>,
}

impl RelayCluster {
    /// The distinct relay hosts, sorted.
    #[must_use]
    pub fn relay_hosts(&self) -> Vec<String> {
        let set: std::collections::BTreeSet<&String> = self.hosts.values().collect();
        set.into_iter().cloned().collect()
    }
}

impl Versioned for RelayCluster {
    const V: u32 = 1;
}

/// `leases/relay/<cluster>`: the relay writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayLease {
    /// The writer node.
    pub node_id: String,
    /// The agent holding the lease.
    pub agent: String,
    /// Where gateways connect (`host:port`).
    pub relay_addr: String,
    /// The agent's internal service (`https://host:port` with `[tls]`),
    /// where gateways make the holder calls.
    pub internal_url: String,
    /// Raised each time another agent takes the lease.
    #[serde(default)]
    pub epoch: u64,
    /// The holder's recovery of every held branch on its node is done:
    /// gateways may connect.
    #[serde(default)]
    pub ready: bool,
}

impl Versioned for RelayLease {
    const V: u32 = 1;
}

/// `relays/<cluster>` in `tier`.
///
/// # Errors
/// Internal for an id that isn't a key segment.
pub fn relay_cluster_key(tier: &Tier, cluster: &ClusterId) -> Result<Key> {
    tier.key(&format!("relays/{cluster}"))
}

/// `leases/relay/<cluster>` in `tier`.
///
/// # Errors
/// Internal for an id that isn't a key segment.
pub fn relay_lease_key(tier: &Tier, cluster: &ClusterId) -> Result<Key> {
    tier.key(&format!("leases/relay/{cluster}"))
}

/// What a gateway knows of a cluster's relay.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RelayState {
    /// No relay ever ran for the cluster: connections go straight to its
    /// nodes.
    #[default]
    None,
    /// The relay writer is ready.
    Ready(RelayLease),
    /// The cluster has relays, but none may take connections now (the
    /// lease is free, or its holder is still recovering): relayed tenants
    /// wait.
    Unavailable(String),
}

/// The state from what the store holds.
#[must_use]
pub fn relay_state(marker: Option<&RelayCluster>, lease: Option<&RelayLease>) -> RelayState {
    match (marker, lease) {
        (_, Some(l)) if l.ready => RelayState::Ready(l.clone()),
        (_, Some(l)) => RelayState::Unavailable(format!(
            "the relay on node {} is recovering held branches",
            l.node_id
        )),
        (Some(_), None) => RelayState::Unavailable("no agent holds the relay lease".to_owned()),
        (None, None) => RelayState::None,
    }
}

/// Reads the relay state of `cluster` from the DC tier.
///
/// # Errors
/// The store's errors.
pub async fn read_relay_state(
    store: &dyn sprig_meta::MetaStore,
    cluster: &ClusterId,
) -> Result<RelayState> {
    Ok(read_relay(store, cluster).await?.0)
}

/// Reads the relay state of `cluster` and its relay hosts
/// ([`RelayCluster::relay_hosts`]) from the DC tier.
///
/// # Errors
/// The store's errors.
pub async fn read_relay(
    store: &dyn sprig_meta::MetaStore,
    cluster: &ClusterId,
) -> Result<(RelayState, Vec<String>)> {
    use sprig_meta::{Consistency, Record};
    let marker = match store
        .get(
            &relay_cluster_key(store.tier(), cluster)?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => Some(Record::<RelayCluster>::decode(&kv.value)?.body),
        None => None,
    };
    let lease = match store
        .get(
            &relay_lease_key(store.tier(), cluster)?,
            Consistency::Serializable,
        )
        .await?
    {
        Some(kv) => Some(Record::<RelayLease>::decode(&kv.value)?.body),
        None => None,
    };
    let hosts = marker
        .as_ref()
        .map(RelayCluster::relay_hosts)
        .unwrap_or_default();
    Ok((relay_state(marker.as_ref(), lease.as_ref()), hosts))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(ready: bool) -> RelayLease {
        RelayLease {
            node_id: "n2".into(),
            agent: "agent-n2".into(),
            relay_addr: "10.0.0.2:7404".into(),
            internal_url: "https://10.0.0.2:7403".into(),
            epoch: 3,
            ready,
        }
    }

    #[test]
    fn gateways_go_through_a_ready_relay_only() {
        let m = RelayCluster {
            first_agent: "agent-n1".into(),
            since_ms: 1,
            hosts: std::collections::BTreeMap::new(),
        };
        assert_eq!(relay_state(None, None), RelayState::None);
        assert!(matches!(
            relay_state(Some(&m), None),
            RelayState::Unavailable(_)
        ));
        assert!(matches!(
            relay_state(Some(&m), Some(&lease(false))),
            RelayState::Unavailable(_)
        ));
        assert_eq!(
            relay_state(Some(&m), Some(&lease(true))),
            RelayState::Ready(lease(true))
        );
    }

    #[test]
    fn records_decode_with_defaults() {
        let l: RelayLease = serde_json::from_str(
            r#"{"node_id":"n1","agent":"a","relay_addr":"h:1","internal_url":"http://h:2"}"#,
        )
        .expect("decode");
        assert_eq!((l.epoch, l.ready), (0, false));
        // A marker an older agent wrote has no hosts.
        let m: RelayCluster =
            serde_json::from_str(r#"{"first_agent":"a","since_ms":5}"#).expect("decode");
        assert!(m.relay_hosts().is_empty());
    }

    #[test]
    fn relay_hosts_are_distinct_and_sorted() {
        let mut m = RelayCluster {
            first_agent: "a1".into(),
            since_ms: 0,
            hosts: std::collections::BTreeMap::new(),
        };
        m.hosts.insert("a3".into(), "10.0.0.9".into());
        m.hosts.insert("a1".into(), "10.0.0.2".into());
        m.hosts.insert("a2".into(), "10.0.0.9".into());
        assert_eq!(m.relay_hosts(), vec!["10.0.0.2", "10.0.0.9"]);
        let text = serde_json::to_string(&m).expect("encode");
        assert_eq!(
            serde_json::from_str::<RelayCluster>(&text).expect("decode"),
            m
        );
    }
}
