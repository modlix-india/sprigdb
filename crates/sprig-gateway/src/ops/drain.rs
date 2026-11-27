//! Planned leader moves: a node that is draining (its record says so, its
//! own drain route was called, or its clock is out of bounds) hands every
//! unit it leads to another voter.
//!
//! ```text
//! every 200 ms while shedding, for each unit led here:
//!   to := the voter on an active node, other than this one, whose log
//!         matches the most (ties by id)
//!   Raft transfer to it (at most once a second per unit); the leader's
//!   lease ends, the target campaigns at once, proposals pause briefly
//! sessions of the unit's tenant close at their next safe point (39502,
//! the leadership watcher of routing), and clients reconnect through any
//! node
//! ```
//!
//! [`drain_local`] is the route's and a graceful stop's entry: it marks the
//! node `draining` in the registry, waits until it leads nothing or the
//! deadline passes, and reports what it did. `undrain` puts it back to
//! `active`; the preferred-leader thread then brings its home units' leadership back.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_catalog::units::{NODE_ACTIVE, NODE_DRAINING};

use super::nodes;
use crate::node_engine;

/// The node-local Admin API route that drains this node.
pub const DRAIN_PATH: &str = "/admin/v1/engine/drain";
/// The node-local Admin API route that undrains this node.
pub const UNDRAIN_PATH: &str = "/admin/v1/engine/undrain";

static ASKED: Mutex<Option<HashMap<u64, Instant>>> = Mutex::new(None);

pub(super) fn start(stop: sprig_common::CancellationToken) {
    std::thread::Builder::new()
        .name("sprig-ops-drain".into())
        .spawn(move || {
            while !stop.is_cancelled() {
                std::thread::sleep(Duration::from_millis(200));
                if nodes::should_shed() {
                    shed_once();
                }
            }
        })
        .ok();
}

/// Units this node leads now.
#[must_use]
pub fn leading() -> usize {
    let Some(raft) = node_engine::engine_raft() else {
        return 0;
    };
    let node = raft.node();
    node.groups()
        .into_iter()
        .filter(|g| *g < sprig_engine::NODE_UNIT.0)
        .filter(|g| node.status(*g).is_some_and(|s| s.is_leader))
        .count()
}

/// One round of transfers away from this node. Returns how many it asked.
pub fn shed_once() -> usize {
    let Some(raft) = node_engine::engine_raft() else {
        return 0;
    };
    let node = raft.node();
    let me = node.id();
    let reason = nodes::shed_reason();
    let mut asked = ASKED.lock().unwrap_or_else(PoisonError::into_inner);
    let asked = asked.get_or_insert_with(HashMap::new);
    let now = Instant::now();
    asked.retain(|_, t| now.duration_since(*t) < Duration::from_secs(10));
    let mut n = 0;
    for g in node.groups() {
        if g >= sprig_engine::NODE_UNIT.0 {
            continue;
        }
        let Some(st) = node.status(g) else { continue };
        if !st.is_leader
            || asked
                .get(&g)
                .is_some_and(|t| now.duration_since(*t) < Duration::from_secs(1))
        {
            continue;
        }
        let to = st
            .voters
            .iter()
            .copied()
            .filter(|v| *v != me && nodes::takes_leadership(*v) && !nodes::is_lost(*v))
            .max_by_key(|v| (node.matched(g, *v).unwrap_or(0), std::cmp::Reverse(*v)));
        let Some(to) = to else { continue };
        asked.insert(g, now);
        match node.transfer_leader(g, to) {
            Ok(()) => {
                n += 1;
                metrics::counter!("sprig_node_leader_transfers_total", "reason" => reason)
                    .increment(1);
            }
            Err(e) => tracing::debug!(unit = g, to, error = %e, "drain: transfer refused"),
        }
    }
    if n > 0 {
        tracing::info!(
            asked = n,
            reason,
            "handing units' leadership to other voters"
        );
    }
    n
}

/// What a drain did.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DrainReport {
    /// This node's Raft id.
    pub node: u64,
    /// Units it led when the drain started.
    pub leading_before: usize,
    /// Units it still leads.
    pub leading_now: usize,
    /// How long the drain took, in milliseconds.
    pub took_ms: u64,
    /// Whether it leads nothing now.
    pub drained: bool,
}

/// Drains this node: marks it `draining` in the registry (kept across a
/// restart), hands away every unit it leads, and waits until it leads none
/// or `deadline` passes.
///
/// # Errors
/// The node doesn't replicate, or the registry can't be written.
pub async fn drain_local(
    dc: &dyn sprig_meta::MetaStore,
    deadline: Duration,
    reason: &str,
) -> sprig_common::Result<DrainReport> {
    let me = nodes::me();
    if me == 0 || node_engine::engine_raft().is_none() {
        return Err(sprig_common::SprigError::new(
            sprig_common::SprigCode::AdminRequestInvalid,
            "this gateway's engine doesn't replicate",
        ));
    }
    let started = Instant::now();
    let leading_before = tokio::task::spawn_blocking(leading).await.unwrap_or(0);
    nodes::set_local_drain(true);
    let reason = if reason.is_empty() { "drain" } else { reason };
    if let Some(r) =
        sprig_catalog::units::set_node_state(dc, me, &[NODE_ACTIVE], NODE_DRAINING, reason).await?
        && let Some(d) = nodes::directory()
    {
        d.put(r);
    }
    let end = started + deadline;
    let mut now_leading = leading_before;
    while Instant::now() < end {
        now_leading = tokio::task::spawn_blocking(|| {
            shed_once();
            leading()
        })
        .await
        .unwrap_or(0);
        if now_leading == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    tracing::info!(
        node = me,
        leading_before,
        leading_now = now_leading,
        took_ms,
        "drain finished"
    );
    Ok(DrainReport {
        node: me,
        leading_before,
        leading_now: now_leading,
        took_ms,
        drained: now_leading == 0,
    })
}

/// Undrains this node: `active` again in the registry; its home units'
/// leadership comes back through the preferred-leader thread.
///
/// # Errors
/// The registry can't be written.
pub async fn undrain_local(dc: &dyn sprig_meta::MetaStore) -> sprig_common::Result<String> {
    let me = nodes::me();
    nodes::set_local_drain(false);
    if me == 0 {
        return Ok(NODE_ACTIVE.to_owned());
    }
    let r = sprig_catalog::units::set_node_state(dc, me, &[NODE_DRAINING], NODE_ACTIVE, "undrain")
        .await?;
    if let (Some(r), Some(d)) = (r.clone(), nodes::directory()) {
        d.put(r);
    }
    Ok(r.map_or_else(|| NODE_ACTIVE.to_owned(), |r| r.state))
}

/// At a graceful stop: hands away every unit this node leads, waiting until
/// it leads none or `deadline` passes. Unlike [`drain_local`] it doesn't
/// mark the node `draining` in the registry, so the node is `active` again
/// when it restarts.
pub async fn shed_for_stop(deadline: Duration) {
    if node_engine::engine_raft().is_none() {
        return;
    }
    nodes::set_local_drain(true);
    let started = Instant::now();
    let before = tokio::task::spawn_blocking(leading).await.unwrap_or(0);
    if before == 0 {
        return;
    }
    let mut now_leading = before;
    while started.elapsed() < deadline {
        now_leading = tokio::task::spawn_blocking(|| {
            shed_once();
            leading()
        })
        .await
        .unwrap_or(0);
        if now_leading == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tracing::info!(
        leading_before = before,
        leading_now = now_leading,
        took_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "stopping: handed the units this node led to other voters"
    );
}
