//! Operating a replicated node: what each engine node does by itself so a
//! cluster of them keeps every unit at three copies, moves leaderships away
//! before a planned stop, and watches its clock.
//!
//! - [`nodes`]: the DC's node records (`nodes/<raft id>`) as this node
//!   follows them, and who may lead units now;
//! - [`replicate`]: on each unit's leader, a voter on a lost node (`dead`
//!   or `removed`) is replaced on another node; replicas a node no longer
//!   holds by its unit's descriptor leave it;
//! - [`moves`]: on each unit's leader, a planned replica move written
//!   into the unit's descriptor (by the balancer or an operator) is
//!   carried out;
//! - [`peers`]: nodes added later are learned from the node registry;
//! - [`drain`]: a draining node hands every unit it leads to another
//!   voter (`POST /admin/v1/engine/drain`, also at the start of a
//!   graceful stop);
//! - [`clock`]: the clock offset to the other nodes, and stepping down
//!   from leadership when it is out of bounds;
//! - [`report`]: the node's unit and replication gauges.
//!
//! All of it runs only on a node whose engine replicates with units placed
//! per tenant; on a one-node engine [`start`] does nothing.

pub mod clock;
pub mod drain;
pub mod moves;
pub mod nodes;
pub mod peers;
pub mod replicate;
pub mod report;
pub mod version;

use std::sync::{Arc, OnceLock};

use sprig_settings::{LiveSettings, SettingCtx};

/// Overrides the version this node reports (testing rolling upgrades with
/// two builds of one version).
pub const VERSION_VAR: &str = "SPRIGDB_ENGINE_REPORT_VERSION";

/// The version this node reports in its registry record: its build's,
/// unless [`VERSION_VAR`] names another.
#[must_use]
pub fn node_version() -> String {
    std::env::var(VERSION_VAR)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned())
}

static LIVE: OnceLock<LiveSettings> = OnceLock::new();
static DC: OnceLock<Arc<dyn sprig_meta::MetaStore>> = OnceLock::new();

/// This DC's tier, once [`start`] ran (a replicated node).
pub(crate) fn dc() -> Option<Arc<dyn sprig_meta::MetaStore>> {
    DC.get().cloned()
}

/// Gives the node's loops the settings they read (`replication.*`).
pub fn set_settings(live: &LiveSettings) {
    let _ = LIVE.set(live.clone());
}

/// A process-scope setting, or its default before [`set_settings`].
pub(crate) fn setting<T: sprig_settings::SettingValue + Copy>(
    s: &'static sprig_settings::Setting<T>,
) -> T {
    LIVE.get().map_or_else(
        || s.default,
        |live| {
            let dc = live.stores().dc_id().clone();
            live.get(s, &SettingCtx::process(&dc))
        },
    )
}

/// Starts the node's operation loops for Raft node `id`, until `stop`:
/// following the node registry, re-replication, stale replicas, drain,
/// the clock watch and the gauges. Call once, on a replicated node.
pub fn start(id: u64, dc: &Arc<dyn sprig_meta::MetaStore>, stop: sprig_common::CancellationToken) {
    let _ = DC.set(Arc::clone(dc));
    nodes::start(id, Arc::clone(dc), stop.clone());
    replicate::start(dc, stop.clone());
    moves::start(dc, stop.clone());
    peers::start(stop.clone());
    drain::start(stop.clone());
    clock::start(Arc::clone(dc), stop.clone());
    report::start(stop);
}
