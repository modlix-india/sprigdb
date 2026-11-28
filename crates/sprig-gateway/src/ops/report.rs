//! The node's unit and replication gauges, every 5 s:
//!
//! | Metric | Meaning |
//! |---|---|
//! | `sprig_node_units` | engine units this node holds a replica of |
//! | `sprig_node_leaders` | units it leads |
//! | `sprig_node_units_under_replicated` | units it leads with a voter on a lost node, a voter its log can't reach, or fewer than three voters where the cluster has three nodes |
//! | `sprig_node_units_leaderless` | units it holds with no leader known |
//! | `sprig_node_apply_queue_entries` | committed but not yet applied entries, summed over its units |
//! | `sprig_node_replication_lag_entries` | the largest gap, over the units it leads, between the commit index and a voter's matched index |
//! | `sprig_node_state` | 1 for this node's registry state (`state` label) |
//! | `sprig_node_snapshot_bytes_sent_total` | unit snapshot bytes this node sent in chunks |
//! | `sprig_node_snapshot_bytes_received_total` | unit snapshot bytes this node received in chunks |
//! | `sprig_node_snapshots_built_total` | unit snapshots this node built off its Raft driver thread |
//! | `sprig_node_snapshot_restore_failures_total` | snapshots this node received and could not restore (each refused and asked for again) |
//!
//! The same loop applies `replication.snapshot_mb_per_second` to the node's
//! Raft driver.
//!
//! Per-unit series are not exported (tens of thousands of units per node);
//! `sprigctl unit list` shows any one unit.

use std::time::Duration;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

use super::nodes;
use crate::node_engine;

/// `replication.snapshot_mb_per_second`: how fast this node sends unit
/// snapshots (new copies, lagging ones) over all its transfers.
pub static SNAPSHOT_MB_PER_SECOND: Setting<u64> = Setting {
    key: "replication.snapshot_mb_per_second",
    default: 160,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 100_000),
    apply: Apply::Live,
    operator_only: true,
    owner: 61,
    doc: "MB per second a node sends unit snapshots at, over all its transfers (0: no limit)",
};
sprig_settings::register!(SNAPSHOT_MB_PER_SECOND);

/// Applies `replication.snapshot_mb_per_second` to the Raft node and
/// exports its snapshot counters.
#[allow(clippy::cast_precision_loss)]
fn snapshots() {
    let Some(raft) = node_engine::engine_raft() else {
        return;
    };
    let node = raft.node();
    let mb = super::setting(&SNAPSHOT_MB_PER_SECOND);
    node.set_snapshot_rate(mb.saturating_mul(1 << 20));
    if let Some(dir) = nodes::directory() {
        node.set_whole_snapshot_peers(whole_snapshot_peers(&dir.all()));
    }
    let s = node.stats();
    metrics::counter!("sprig_node_snapshot_bytes_sent_total").absolute(s.snapshot_bytes_sent);
    metrics::counter!("sprig_node_snapshot_bytes_received_total")
        .absolute(s.snapshot_bytes_received);
    metrics::counter!("sprig_node_snapshots_built_total").absolute(s.snapshots_built);
    metrics::counter!("sprig_node_snapshot_restore_failures_total")
        .absolute(s.snapshot_restore_failures);
}

/// The nodes a leader here sends snapshots whole, inside their message:
/// those whose record says their build can't take chunks (a rolling
/// upgrade from a build before chunked snapshots). A node with no record
/// known here yet gets chunks: a node writes its record when it starts.
fn whole_snapshot_peers(
    all: &std::collections::BTreeMap<u64, sprig_catalog::units::NodeRecord>,
) -> Vec<u64> {
    all.values()
        .filter(|n| !n.is_lost() && !n.takes_snapshot_chunks())
        .map(|n| n.id)
        .collect()
}

/// One count of the node's units.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct UnitCounts {
    /// Units held here.
    pub units: u64,
    /// Units led here.
    pub leaders: u64,
    /// Units led here short of copies.
    pub under_replicated: u64,
    /// Units held here with no leader known.
    pub leaderless: u64,
    /// Committed minus applied, summed.
    pub apply_queue: u64,
    /// The largest leader-to-voter gap over units led here.
    pub max_lag: u64,
}

/// Counts this node's units now.
#[must_use]
pub fn count() -> UnitCounts {
    let Some(raft) = node_engine::engine_raft() else {
        return UnitCounts::default();
    };
    let node = raft.node();
    let mut c = UnitCounts::default();
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 {
            continue;
        }
        let Some(st) = node.status(g) else { continue };
        c.units += 1;
        c.apply_queue += st.committed.saturating_sub(st.applied);
        if st.leader.is_none() {
            c.leaderless += 1;
        }
        if !st.is_leader {
            continue;
        }
        c.leaders += 1;
        let mut short = st.voters.len() < 3 && g != 0;
        for v in &st.voters {
            if *v == node.id() {
                continue;
            }
            let m = node.matched(g, *v).unwrap_or(0);
            let lag = st.committed.saturating_sub(m);
            c.max_lag = c.max_lag.max(lag);
            if nodes::is_lost(*v) {
                short = true;
            }
        }
        if short {
            c.under_replicated += 1;
        }
    }
    c
}

#[allow(clippy::cast_precision_loss)]
pub(super) fn start(stop: sprig_common::CancellationToken) {
    tokio::spawn(async move {
        let mut last_state = String::new();
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
            let c = tokio::task::spawn_blocking(count).await.unwrap_or_default();
            snapshots();
            metrics::gauge!("sprig_node_units").set(c.units as f64);
            metrics::gauge!("sprig_node_leaders").set(c.leaders as f64);
            metrics::gauge!("sprig_node_units_under_replicated").set(c.under_replicated as f64);
            metrics::gauge!("sprig_node_units_leaderless").set(c.leaderless as f64);
            metrics::gauge!("sprig_node_apply_queue_entries").set(c.apply_queue as f64);
            metrics::gauge!("sprig_node_replication_lag_entries").set(c.max_lag as f64);
            let state = nodes::state_of(nodes::me()).unwrap_or_default();
            if state != last_state {
                if !last_state.is_empty() {
                    metrics::gauge!("sprig_node_state", "state" => last_state.clone()).set(0.0);
                }
                if !state.is_empty() {
                    metrics::gauge!("sprig_node_state", "state" => state.clone()).set(1.0);
                }
                last_state = state;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sprig_catalog::units::NodeRecord;

    fn node(id: u64, state: &str, features: &[&str]) -> NodeRecord {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "gateway_id": format!("gw{id}"),
            "addr": "127.0.0.1",
            "state": state,
            "raft_features": features,
        }))
        .expect("a node record")
    }

    /// Nodes whose record names no chunked snapshots (an older build, or a
    /// record written before the feature list existed) get snapshots whole;
    /// lost nodes get none at all.
    #[test]
    fn nodes_of_an_older_build_get_snapshots_whole() {
        let chunks = sprig_catalog::units::RAFT_SNAPSHOT_CHUNKS;
        let all: BTreeMap<u64, NodeRecord> = [
            node(1, "active", &[chunks]),
            node(2, "active", &[]),
            node(3, "draining", &["something-else"]),
            node(4, "dead", &[]),
        ]
        .into_iter()
        .map(|n| (n.id, n))
        .collect();
        assert_eq!(super::whole_snapshot_peers(&all), vec![2, 3]);
        // The field round-trips; an old record without it reads as none.
        let old: NodeRecord = serde_json::from_value(serde_json::json!({
            "id": 5, "gateway_id": "gw5", "addr": "a", "state": "active"
        }))
        .expect("an old record");
        assert!(!old.takes_snapshot_chunks());
        let back: NodeRecord =
            serde_json::from_str(&serde_json::to_string(&all[&1]).expect("json")).expect("back");
        assert!(back.takes_snapshot_chunks());
    }
}
