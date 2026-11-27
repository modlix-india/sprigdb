//! Extra read-only copies of a tenant's unit: non-voting learners listed in
//! its unit descriptor (`learners`), which receive every change and serve
//! reader sessions ([`crate::reader`]) as voters do, without slowing
//! commits.
//!
//! The descriptor says which nodes hold them; the unit's leader makes the
//! group's configuration match: it adds each listed learner that the group
//! lacks (the learner node creates its replica when the leader's first
//! message arrives and catches up from a snapshot and the log) and removes
//! learners the descriptor no longer lists. Voters are never changed here.

use std::sync::Arc;
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_raft::{ConfChangeSingle, ConfChangeType, ConfChangeV2};

/// How often the leader compares its units' learners with the descriptors.
const EVERY: Duration = Duration::from_secs(1);

/// Keeps the learners of the units this node leads equal to their
/// descriptors' until `stop`.
pub async fn follow(dir: Arc<sprig_catalog::units::UnitDirectory>, stop: CancellationToken) {
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(EVERY) => {}
        }
        let dir = Arc::clone(&dir);
        let _ = tokio::task::spawn_blocking(move || reconcile(&dir)).await;
        publish_read_counts();
    }
}

/// Reads served on this node's copies, by how they were let through
/// (`local`: at once; `waited`: after a short wait; `barrier`: after the
/// leader confirmed its commits).
fn publish_read_counts() {
    let (local, waited, barrier) = sprig_engine::reads::stats().get();
    for (served, n) in [("local", local), ("waited", waited), ("barrier", barrier)] {
        metrics::counter!("sprig_unit_follower_reads_total", "served" => served).absolute(n);
    }
}

/// One pass: at most one change per unit (a change waits for the previous
/// one). Returns how many changes it proposed.
fn reconcile(dir: &sprig_catalog::units::UnitDirectory) -> usize {
    let (Some(node), Ok(engine)) = (
        crate::node_engine::raft_node(),
        crate::node_engine::engine(),
    ) else {
        return 0;
    };
    let mut n = 0;
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 || engine.leader_epoch(sprig_engine::UnitId(g)) == 0 {
            continue;
        }
        let Some(d) = dir.get(g) else { continue };
        if d.unit != g {
            continue;
        }
        let Some(st) = node.status(g) else { continue };
        let single = |t: ConfChangeType, id: u64| {
            let mut c = ConfChangeSingle::default();
            c.set_change_type(t);
            c.node_id = id;
            c
        };
        let add = d
            .learners
            .iter()
            .find(|l| !st.learners.contains(l) && !st.voters.contains(l))
            .map(|l| single(ConfChangeType::AddLearnerNode, *l));
        let remove = st
            .learners
            .iter()
            .find(|l| !d.learners.contains(l))
            .map(|l| single(ConfChangeType::RemoveNode, *l));
        let Some(change) = add.or(remove) else {
            continue;
        };
        let what = (change.get_change_type(), change.node_id);
        let mut cc = ConfChangeV2::default();
        cc.mut_changes().push(change);
        match node.change_membership(g, cc) {
            Ok(_) => {
                n += 1;
                tracing::info!(unit = g, change = ?what.0, node = what.1, "read copies: changing the unit's learners to match its descriptor");
            }
            Err(e) => {
                tracing::debug!(unit = g, error = %e, "read copies: learner change not proposed now");
            }
        }
    }
    n
}
