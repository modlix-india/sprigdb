//! The two `auto_index.*` keys still read: the controller's job scheduler
//! sizes its `build` slot pool by [`MAX_BUILDS_PER_CLUSTER`] (search index
//! builds use it), and the sharding advisor's lookup advice reads
//! [`LOOKUP_SCATTER_PCT`].

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// The key of `auto_index.max_builds_per_cluster`.
pub const MAX_BUILDS_PER_CLUSTER_KEY: &str = "auto_index.max_builds_per_cluster";

/// `auto_index.max_builds_per_cluster`: concurrent index
/// builds per cluster, the `build` slot pool of the controller's job
/// scheduler.
pub static MAX_BUILDS_PER_CLUSTER: Setting<u32> = Setting {
    key: MAX_BUILDS_PER_CLUSTER_KEY,
    default: 1,
    scopes: Scopes::ALL,
    hard: HardBounds::range(1, 64),
    apply: Apply::Live,
    operator_only: true,
    owner: 13,
    doc: "Concurrent index builds per cluster",
};
sprig_settings::register!(MAX_BUILDS_PER_CLUSTER);

/// `auto_index.lookup_scatter_pct`: the scatter share above which the
/// sharding advisor proposes a lookup index.
pub static LOOKUP_SCATTER_PCT: Setting<f64> = Setting {
    key: "auto_index.lookup_scatter_pct",
    default: 80.0,
    scopes: Scopes::ALL,
    hard: HardBounds {
        min: Some(0.0),
        max: Some(100.0),
        allowed: None,
    },
    apply: Apply::Live,
    operator_only: false,
    owner: 13,
    doc: "Scatter share for a lookup index",
};
sprig_settings::register!(LOOKUP_SCATTER_PCT);

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn both_keys_are_in_the_global_registry() {
        let reg = Registry::global().unwrap();
        for k in [MAX_BUILDS_PER_CLUSTER_KEY, LOOKUP_SCATTER_PCT.key] {
            assert_eq!(reg.get(k).unwrap().owner(), 13);
        }
        assert_eq!(MAX_BUILDS_PER_CLUSTER.default, 1);
    }
}
