//! Bolt reads from copies.
//!
//! A `neo4j://` driver (its `HELLO` carries a routing context) asks `ROUTE`
//! for a routing table and sends each transaction where the table says:
//! write transactions to a writer, read transactions (`mode: "r"`) to a
//! reader. Here the table names the node leading the tenant's unit as the
//! writer and the nodes holding its other replicas (voters and read-only
//! learners) as readers, so reads cost the leader nothing.
//!
//! - A routing connection is served on any node holding a replica of the
//!   tenant's unit (others hand it to the leader at login, as every other
//!   connection), and stays open when leadership moves.
//! - On a node that doesn't lead the unit, a read transaction reads the
//!   local copy at most `reads.nearest_max_lag_ms` old and never older than
//!   the session's bookmarks; a write transaction gets
//!   `Neo.ClientError.Cluster.NotALeader`, so the driver refreshes its
//!   routing table and sends it to the new writer.
//! - **Bookmarks** carry the hybrid logical clock timestamp of the
//!   session's last commit: a bookmark passed to a reader makes its reads
//!   at least that fresh.

use std::sync::Arc;

use sprig_engine::reads::ReadPolicy;
use sprig_proto_bolt::PackValue;
use sprig_proto_bolt::server::BoltFailure;

use crate::node_engine::{self, UnitLeader};
use crate::reader::{ReadFrom, Reader};

/// A routing session's read state.
pub(crate) fn reader(tenant: u64) -> Arc<Reader> {
    Arc::new(Reader::new(ReadFrom::Nearest(None), tenant))
}

/// Whether this node leads `tenant`'s unit (or units aren't replicated).
pub(crate) fn leads(tenant: u64) -> bool {
    match node_engine::tenant_leader(tenant) {
        UnitLeader::Here | UnitLeader::Any => true,
        UnitLeader::Node(n) => node_engine::raft_id_from_env() == Some(n),
        UnitLeader::Unknown => false,
    }
}

/// `mode: "r"` in a `BEGIN` or `RUN` extra.
pub(crate) fn read_mode(extra: &PackValue) -> bool {
    extra.get("mode").and_then(PackValue::as_str) == Some("r")
}

/// The failure a write transaction gets on a node that doesn't lead.
pub(crate) fn not_a_leader() -> BoltFailure {
    BoltFailure::new(
        "Neo.ClientError.Cluster.NotALeader",
        "No write operations are allowed on this database. This is a read only member of the cluster.",
    )
}

/// The rules a transaction of a routing session reads under on this node:
/// `None` where it reads as usual (a session that doesn't route, or this
/// node leads the unit).
///
/// # Errors
/// [`not_a_leader`] for a write transaction on a node that doesn't lead.
pub(crate) fn policy(
    reader: Option<&Arc<Reader>>,
    tenant: u64,
    read: bool,
) -> std::result::Result<Option<ReadPolicy>, BoltFailure> {
    let Some(r) = reader else {
        return Ok(None);
    };
    if leads(tenant) {
        return Ok(None);
    }
    if !read {
        return Err(not_a_leader());
    }
    Ok(Some(r.policy(None)))
}

/// `ROUTE`'s servers for `tenant` when nodes advertise their own Bolt
/// addresses: the leader's address as the writer (`fallback` when it isn't
/// known), the other replicas' as readers (the writer's when there are
/// none), and `fallback` as the router.
pub(crate) async fn route_servers(tenant: u64, fallback: &str) -> Vec<(&'static str, Vec<String>)> {
    let single = || {
        vec![
            ("ROUTE", vec![fallback.to_owned()]),
            ("READ", vec![fallback.to_owned()]),
            ("WRITE", vec![fallback.to_owned()]),
        ]
    };
    if !crate::forward::enabled() {
        return single();
    }
    let Some(host) = crate::forward::host() else {
        return single();
    };
    let Some(d) = node_engine::unit_directory().and_then(|d| d.get(tenant)) else {
        return single();
    };
    let Ok(recs) = host.peers.gateways().await else {
        return single();
    };
    let addr_of = |node: u64| {
        recs.iter()
            .find(|r| r.raft_node == Some(node))
            .and_then(|r| Some(format!("{}:{}", r.addr, r.listeners.get("bolt")?)))
    };
    let leader = match node_engine::tenant_leader(tenant) {
        UnitLeader::Node(n) => Some(n),
        UnitLeader::Here => node_engine::raft_id_from_env(),
        UnitLeader::Any | UnitLeader::Unknown => None,
    };
    let writer = leader
        .and_then(addr_of)
        .unwrap_or_else(|| fallback.to_owned());
    let mut readers: Vec<String> = d
        .learners
        .iter()
        .chain(d.voters.iter())
        .filter(|n| Some(**n) != leader)
        .filter_map(|n| addr_of(*n))
        .collect();
    readers.sort();
    readers.dedup();
    if readers.is_empty() {
        readers.push(writer.clone());
    }
    vec![
        ("ROUTE", vec![fallback.to_owned()]),
        ("READ", readers),
        ("WRITE", vec![writer]),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_routing_sessions_off_the_leader_are_held_to_reads() {
        assert!(read_mode(&PackValue::map([("mode", PackValue::str("r"))])));
        assert!(!read_mode(&PackValue::map([("db", PackValue::str("x"))])));
        // Not a routing session: as usual.
        assert!(matches!(policy(None, 7, false), Ok(None)));
        // No replication here: this node leads.
        let r = reader(7);
        assert!(matches!(policy(Some(&r), 7, false), Ok(None)));
    }
}
