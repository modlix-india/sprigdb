//! Nodes added after this one: their Raft addresses, from the node
//! registry, and a copy of unit 0 for them.
//!
//! ```text
//! every 2 s:
//!   each node record with a Raft address (`raft_addr`) this node's
//!   transport doesn't have yet → the transport learns it (so this node
//!   reaches a node that wasn't in SPRIGDB_ENGINE_RAFT_PEERS when it
//!   started)
//!   on the leader of unit 0 (data no tenant owns, on every node): one
//!   active node that holds no copy of it joins it as a learner
//! ```
//!
//! A new node starts with `SPRIGDB_ENGINE_RAFT_PEERS` listing every node
//! (itself too) and `SPRIGDB_ENGINE_RAFT_JOIN=true`, so it starts no unit of
//! its own; it writes its record with its Raft address, the others learn
//! it here, and from then on placement, re-replication and the balancer
//! can give it units.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use sprig_catalog::units::NODE_ACTIVE;

use super::nodes;
use crate::node_engine;

/// Addresses handed to the transport, by node id.
static KNOWN: Mutex<Option<HashMap<u64, SocketAddr>>> = Mutex::new(None);

pub(super) fn start(stop: sprig_common::CancellationToken) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            learn();
            let _ = tokio::task::spawn_blocking(unit_zero_copies).await;
        }
    });
}

/// The node records' Raft addresses this node's transport hasn't got:
/// `(id, address)` pairs, by id.
#[must_use]
pub fn new_addresses<S: std::hash::BuildHasher>(
    me: u64,
    records: &[(u64, String)],
    known: &HashMap<u64, SocketAddr, S>,
) -> Vec<(u64, SocketAddr)> {
    let mut out: Vec<(u64, SocketAddr)> = records
        .iter()
        .filter(|(id, _)| *id != me && *id != 0)
        .filter_map(|(id, a)| a.trim().parse::<SocketAddr>().ok().map(|a| (*id, a)))
        .filter(|(id, a)| known.get(id) != Some(a))
        .collect();
    out.sort_unstable();
    out
}

fn learn() {
    let Some(dir) = nodes::directory() else {
        return;
    };
    let me = nodes::me();
    let records: Vec<(u64, String)> = dir
        .all()
        .into_values()
        .filter(|n| !n.raft_addr.is_empty())
        .map(|n| (n.id, n.raft_addr))
        .collect();
    let mut g = KNOWN.lock().unwrap_or_else(PoisonError::into_inner);
    let known = g.get_or_insert_with(|| node_engine::raft_peers().into_iter().collect());
    for (id, addr) in new_addresses(me, &records, known) {
        if node_engine::learn_raft_peer(id, addr) {
            tracing::info!(node = id, raft = %addr, "raft: learned a node's address from the node registry");
            known.insert(id, addr);
        }
    }
}

/// On unit 0's leader: one active node with no copy of unit 0 joins it as
/// a learner (it then reads tenant 0's records locally). Returns whether
/// it asked.
fn unit_zero_copies() -> bool {
    use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
    let (Some(raft), Some(dir)) = (node_engine::engine_raft(), nodes::directory()) else {
        return false;
    };
    if !node_engine::raft_placed() {
        return false;
    }
    let node = raft.node();
    let Some(st) = node.status(0) else {
        return false;
    };
    if !st.is_leader || !st.voters_outgoing.is_empty() {
        return false;
    }
    let known = KNOWN
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .unwrap_or_default();
    let Some(new) = dir
        .all()
        .into_values()
        .filter(|n| n.state == NODE_ACTIVE && n.placement)
        .filter(|n| known.contains_key(&n.id) || n.id == node.id())
        .map(|n| n.id)
        .find(|id| !st.voters.contains(id) && !st.learners.contains(id))
    else {
        return false;
    };
    let mut c = ConfChangeSingle::default();
    c.set_change_type(ConfChangeType::AddLearnerNode);
    c.node_id = new;
    let mut cc = ConfChangeV2::default();
    cc.mut_changes().push(c);
    match node.change_membership(0, cc) {
        Ok(_) => {
            tracing::info!(
                node = new,
                "unit 0: a node added after the cluster started gets a copy (learner)"
            );
            true
        }
        Err(e) => {
            tracing::debug!(node = new, error = %e, "unit 0: learner not added now");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_new_or_changed_addresses_are_learned() {
        let a = |s: &str| s.parse::<SocketAddr>().unwrap();
        let known = HashMap::from([(1, a("127.0.0.1:1")), (2, a("127.0.0.1:2"))]);
        let records = vec![
            (1, "127.0.0.1:1".to_owned()),
            (2, "127.0.0.1:22".to_owned()),
            (3, "127.0.0.1:3".to_owned()),
            (4, "not an address".to_owned()),
            (5, "127.0.0.1:5".to_owned()),
        ];
        assert_eq!(
            new_addresses(5, &records, &known),
            vec![(2, a("127.0.0.1:22")), (3, a("127.0.0.1:3"))]
        );
    }
}
