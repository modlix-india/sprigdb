//! The DC's node records as this node follows them (`nodes/`), and who may
//! lead units now.
//!
//! A node takes leaderships when its record is `active` (or it has none
//! yet: a cluster from before node states) and, for this node itself, while
//! its clock is within bounds. Draining, dead and removed nodes are
//! nobody's preferred leader, and a draining node hands its leaderships
//! away ([`super::drain`]).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use sprig_catalog::units::{NODE_ACTIVE, NodeDirectory, NodeRecord};

static DIR: OnceLock<Arc<NodeDirectory>> = OnceLock::new();
static ME: AtomicU64 = AtomicU64::new(0);
/// This node's clock is out of bounds ([`super::clock`]): lead nothing.
static CLOCK_BAD: AtomicBool = AtomicBool::new(false);
/// A drain asked for on this node's own route, before (or without) its
/// record saying so.
static LOCAL_DRAIN: AtomicBool = AtomicBool::new(false);

/// The node records this node follows (empty until [`start`]).
#[must_use]
pub fn directory() -> Option<Arc<NodeDirectory>> {
    DIR.get().cloned()
}

/// This node's Raft id (0 before [`start`]).
#[must_use]
pub fn me() -> u64 {
    ME.load(Ordering::Relaxed)
}

pub(super) fn start(
    id: u64,
    dc: Arc<dyn sprig_meta::MetaStore>,
    stop: sprig_common::CancellationToken,
) {
    ME.store(id, Ordering::Relaxed);
    let dir = Arc::clone(DIR.get_or_init(NodeDirectory::new));
    tokio::spawn(dir.follow(dc, stop));
}

/// Records this node's own record as stored at start (before the watch
/// delivers it).
pub(crate) fn note_own(r: NodeRecord) {
    if let Some(d) = DIR.get() {
        d.put(r);
    }
}

/// Node `id`'s state as followed here (`None`: no record known).
#[must_use]
pub fn state_of(id: u64) -> Option<String> {
    DIR.get().and_then(|d| d.state(id))
}

/// Whether node `id` may lead units now: `active` (or no record known),
/// and for this node, a clock within bounds and no local drain.
#[must_use]
pub fn takes_leadership(id: u64) -> bool {
    if id == me() && (CLOCK_BAD.load(Ordering::Relaxed) || LOCAL_DRAIN.load(Ordering::Relaxed)) {
        return false;
    }
    state_of(id).is_none_or(|s| s == NODE_ACTIVE)
}

/// Whether node `id` is lost (`dead` or `removed`): its replicas are
/// replaced elsewhere.
#[must_use]
pub fn is_lost(id: u64) -> bool {
    DIR.get()
        .and_then(|d| d.get(id))
        .is_some_and(|n| n.is_lost())
}

/// Whether this node should hand away the units it leads now.
#[must_use]
pub fn should_shed() -> bool {
    let me = me();
    me != 0
        && (CLOCK_BAD.load(Ordering::Relaxed)
            || LOCAL_DRAIN.load(Ordering::Relaxed)
            || state_of(me).is_some_and(|s| s != NODE_ACTIVE))
}

/// Why this node sheds its leaderships (for logs and metrics).
#[must_use]
pub fn shed_reason() -> &'static str {
    if CLOCK_BAD.load(Ordering::Relaxed) {
        "clock"
    } else {
        "drain"
    }
}

/// Whether this node's clock is out of bounds now.
#[must_use]
pub fn clock_bad() -> bool {
    CLOCK_BAD.load(Ordering::Relaxed)
}

pub(super) fn set_clock_bad(bad: bool) -> bool {
    CLOCK_BAD.swap(bad, Ordering::Relaxed) != bad
}

pub(super) fn set_local_drain(on: bool) {
    LOCAL_DRAIN.store(on, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_catalog::units::NODE_DRAINING;

    #[test]
    fn unknown_nodes_lead_and_lost_ones_do_not() {
        // Before any record is known, every node may lead.
        assert!(takes_leadership(42));
        let dir = Arc::clone(DIR.get_or_init(NodeDirectory::new));
        let rec = |id: u64, state: &str| NodeRecord {
            id,
            gateway_id: format!("gw-{id}"),
            addr: "127.0.0.1".into(),
            failure_domain: String::new(),
            placement: true,
            state: state.into(),
            version: String::new(),
            since_ms: 0,
            reason: String::new(),
        };
        dir.put(rec(91, NODE_ACTIVE));
        dir.put(rec(92, NODE_DRAINING));
        dir.put(rec(93, sprig_catalog::units::NODE_DEAD));
        assert!(takes_leadership(91));
        assert!(!takes_leadership(92));
        assert!(!takes_leadership(93));
        assert!(is_lost(93));
        assert!(!is_lost(92));
        assert!(!is_lost(94));
    }
}
