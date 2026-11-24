//! The `auto_index.*` keys, and [`AutoIndexSettings`], their values for
//! one tenant as the loop reads them.
//!
//! Two keys are older than the engine loop and other code still reads
//! them: the controller's job scheduler sizes its `build` slot pool by
//! [`MAX_BUILDS_PER_CLUSTER`] (search index builds use it), and the
//! sharding advisor's lookup advice reads [`LOOKUP_SCATTER_PCT`].

use std::borrow::Cow;

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

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $hard:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: Scopes::ALL,
            hard: $hard,
            apply: Apply::Live,
            operator_only: false,
            owner: 13,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

const fn pct() -> HardBounds {
    HardBounds {
        min: Some(0.0),
        max: Some(100.0),
        allowed: None,
    }
}

const fn non_negative() -> HardBounds {
    HardBounds {
        min: Some(0.0),
        max: None,
        allowed: None,
    }
}

key!(
    /// `auto_index.enabled`: whether the loop evaluates the tenant at all.
    ENABLED: bool = "auto_index.enabled", true, HardBounds::NONE,
    "Evaluate the workload and build or recommend indexes automatically"
);
key!(
    /// `auto_index.mode`: `auto` builds; `recommend` sends each index to
    /// the approvals inbox and builds it once approved.
    MODE: Cow<'static, str> = "auto_index.mode", Cow::Borrowed("auto"),
    HardBounds::one_of(&["auto", "recommend"]),
    "auto: build indexes; recommend: ask for approval first"
);
key!(
    /// `auto_index.max_new_per_day`: the daily build budget per tenant.
    MAX_NEW_PER_DAY: u32 = "auto_index.max_new_per_day", 5, HardBounds::range(0, 1000),
    "Automatic indexes built per tenant per 24 hours"
);
key!(
    /// `auto_index.window_hours`.
    WINDOW_HOURS: u32 = "auto_index.window_hours", 24, HardBounds::range(1, 720),
    "Workload window the triggers read"
);
key!(
    /// `auto_index.min_runs`.
    MIN_RUNS: u32 = "auto_index.min_runs", 50, HardBounds::range(1, 1_000_000),
    "Runs of a query pattern needed in the window"
);
key!(
    /// `auto_index.p95_threshold_ms`.
    P95_THRESHOLD_MS: f64 = "auto_index.p95_threshold_ms", 100.0, non_negative(),
    "A pattern slower than this at p95 qualifies"
);
key!(
    /// `auto_index.top_n`.
    TOP_N: u32 = "auto_index.top_n", 10, HardBounds::range(0, 1000),
    "Patterns among the top N by total time qualify when they read too many rows"
);
key!(
    /// `auto_index.min_examined_per_returned`.
    MIN_EXAMINED_PER_RETURNED: f64 = "auto_index.min_examined_per_returned", 100.0, non_negative(),
    "Rows examined per row returned that make a top pattern qualify"
);
key!(
    /// `auto_index.cooldown_hours`: before a query pattern is decided again.
    COOLDOWN_HOURS: u32 = "auto_index.cooldown_hours", 24, HardBounds::range(0, 8760),
    "Hours before a query pattern is decided again"
);
key!(
    /// `auto_index.observe_days`.
    OBSERVE_DAYS: u32 = "auto_index.observe_days", 7, HardBounds::range(1, 365),
    "Days an automatic index is watched; unused ones are dropped"
);
key!(
    /// `auto_index.min_improvement_pct`.
    MIN_IMPROVEMENT_PCT: f64 = "auto_index.min_improvement_pct", 30.0, pct(),
    "Improvement a pattern needs, in validation and after the build"
);
key!(
    /// `auto_index.max_regression_pct`.
    MAX_REGRESSION_PCT: f64 = "auto_index.max_regression_pct", 10.0, pct(),
    "Largest slowdown of any replayed statement"
);
key!(
    /// `auto_index.max_per_table`.
    MAX_PER_TABLE: u32 = "auto_index.max_per_table", 5, HardBounds::range(0, 64),
    "Automatic indexes per table"
);
key!(
    /// `auto_index.recreate_block_days`.
    RECREATE_BLOCK_DAYS: u32 = "auto_index.recreate_block_days", 14, HardBounds::range(0, 365),
    "Days a dropped or rejected definition can't be proposed again"
);
key!(
    /// `auto_index.eval_interval_minutes`.
    EVAL_INTERVAL_MINUTES: u32 = "auto_index.eval_interval_minutes", 60, HardBounds::range(1, 10_080),
    "Minutes between evaluations of a tenant"
);
key!(
    /// `auto_index.replay_samples`.
    REPLAY_SAMPLES: u32 = "auto_index.replay_samples", 20, HardBounds::range(1, 1000),
    "Recent statements of a pattern planned in validation"
);
key!(
    /// `auto_index.min_net_ms_per_hour`.
    MIN_NET_MS_PER_HOUR: f64 = "auto_index.min_net_ms_per_hour", 100.0, non_negative(),
    "Smallest estimated time saved per hour, net of write cost"
);
key!(
    /// `auto_index.maintenance_cost_ms`.
    MAINTENANCE_COST_MS: f64 = "auto_index.maintenance_cost_ms", 0.05, non_negative(),
    "Estimated cost of one index entry change"
);
key!(
    /// `auto_index.build_batch_rows`: rows per backfill transaction.
    BUILD_BATCH_ROWS: u32 = "auto_index.build_batch_rows", 1024, HardBounds::range(1, 100_000),
    "Rows per backfill batch of an online build"
);
key!(
    /// `auto_index.build_pause_ms`: the build's throttle between batches.
    BUILD_PAUSE_MS: u32 = "auto_index.build_pause_ms", 5, HardBounds::range(0, 60_000),
    "Pause between backfill batches of an online build"
);
key!(
    /// `auto_index.recommend_ttl_days`.
    RECOMMEND_TTL_DAYS: u32 = "auto_index.recommend_ttl_days", 14, HardBounds::range(1, 365),
    "Days a recommendation waits for approval"
);

/// Whether the loop builds or asks first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Build.
    Auto,
    /// Send to the approvals inbox; build once approved.
    Recommend,
}

/// The loop's settings for one tenant.
#[derive(Debug, Clone, PartialEq)]
pub struct AutoIndexSettings {
    /// `auto_index.enabled`.
    pub enabled: bool,
    /// `auto_index.mode`.
    pub mode: Mode,
    /// `auto_index.max_new_per_day`.
    pub max_new_per_day: u32,
    /// `auto_index.window_hours`.
    pub window_hours: u32,
    /// `auto_index.min_runs`.
    pub min_runs: u32,
    /// `auto_index.p95_threshold_ms`.
    pub p95_threshold_ms: f64,
    /// `auto_index.top_n`.
    pub top_n: u32,
    /// `auto_index.min_examined_per_returned`.
    pub min_examined_per_returned: f64,
    /// `auto_index.cooldown_hours`.
    pub cooldown_hours: u32,
    /// `auto_index.observe_days`.
    pub observe_days: u32,
    /// `auto_index.min_improvement_pct`.
    pub min_improvement_pct: f64,
    /// `auto_index.max_regression_pct`.
    pub max_regression_pct: f64,
    /// `auto_index.max_per_table`.
    pub max_per_table: u32,
    /// `auto_index.recreate_block_days`.
    pub recreate_block_days: u32,
    /// `auto_index.eval_interval_minutes`.
    pub eval_interval_minutes: u32,
    /// `auto_index.replay_samples`.
    pub replay_samples: u32,
    /// `auto_index.min_net_ms_per_hour`.
    pub min_net_ms_per_hour: f64,
    /// `auto_index.maintenance_cost_ms`.
    pub maintenance_cost_ms: f64,
    /// `auto_index.build_batch_rows`.
    pub build_batch_rows: u32,
    /// `auto_index.build_pause_ms`.
    pub build_pause_ms: u32,
    /// `auto_index.recommend_ttl_days`.
    pub recommend_ttl_days: u32,
}

impl Default for AutoIndexSettings {
    fn default() -> Self {
        Self {
            enabled: ENABLED.default,
            mode: Mode::Auto,
            max_new_per_day: MAX_NEW_PER_DAY.default,
            window_hours: WINDOW_HOURS.default,
            min_runs: MIN_RUNS.default,
            p95_threshold_ms: P95_THRESHOLD_MS.default,
            top_n: TOP_N.default,
            min_examined_per_returned: MIN_EXAMINED_PER_RETURNED.default,
            cooldown_hours: COOLDOWN_HOURS.default,
            observe_days: OBSERVE_DAYS.default,
            min_improvement_pct: MIN_IMPROVEMENT_PCT.default,
            max_regression_pct: MAX_REGRESSION_PCT.default,
            max_per_table: MAX_PER_TABLE.default,
            recreate_block_days: RECREATE_BLOCK_DAYS.default,
            eval_interval_minutes: EVAL_INTERVAL_MINUTES.default,
            replay_samples: REPLAY_SAMPLES.default,
            min_net_ms_per_hour: MIN_NET_MS_PER_HOUR.default,
            maintenance_cost_ms: MAINTENANCE_COST_MS.default,
            build_batch_rows: BUILD_BATCH_ROWS.default,
            build_pause_ms: BUILD_PAUSE_MS.default,
            recommend_ttl_days: RECOMMEND_TTL_DAYS.default,
        }
    }
}

impl AutoIndexSettings {
    /// Reads every key through `get` (a resolver at the tenant's scope):
    /// `get_bool`, `get_u32`, `get_f64` and `get_text` for each type.
    pub fn read(
        get_bool: impl Fn(&'static Setting<bool>) -> bool,
        get_u32: impl Fn(&'static Setting<u32>) -> u32,
        get_f64: impl Fn(&'static Setting<f64>) -> f64,
        get_text: impl Fn(&'static Setting<Cow<'static, str>>) -> String,
    ) -> Self {
        Self {
            enabled: get_bool(&ENABLED),
            mode: if get_text(&MODE) == "recommend" {
                Mode::Recommend
            } else {
                Mode::Auto
            },
            max_new_per_day: get_u32(&MAX_NEW_PER_DAY),
            window_hours: get_u32(&WINDOW_HOURS),
            min_runs: get_u32(&MIN_RUNS),
            p95_threshold_ms: get_f64(&P95_THRESHOLD_MS),
            top_n: get_u32(&TOP_N),
            min_examined_per_returned: get_f64(&MIN_EXAMINED_PER_RETURNED),
            cooldown_hours: get_u32(&COOLDOWN_HOURS),
            observe_days: get_u32(&OBSERVE_DAYS),
            min_improvement_pct: get_f64(&MIN_IMPROVEMENT_PCT),
            max_regression_pct: get_f64(&MAX_REGRESSION_PCT),
            max_per_table: get_u32(&MAX_PER_TABLE),
            recreate_block_days: get_u32(&RECREATE_BLOCK_DAYS),
            eval_interval_minutes: get_u32(&EVAL_INTERVAL_MINUTES),
            replay_samples: get_u32(&REPLAY_SAMPLES),
            min_net_ms_per_hour: get_f64(&MIN_NET_MS_PER_HOUR),
            maintenance_cost_ms: get_f64(&MAINTENANCE_COST_MS),
            build_batch_rows: get_u32(&BUILD_BATCH_ROWS),
            build_pause_ms: get_u32(&BUILD_PAUSE_MS),
            recommend_ttl_days: get_u32(&RECOMMEND_TTL_DAYS),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn every_key_is_in_the_global_registry() {
        let reg = Registry::global().unwrap();
        for k in [
            MAX_BUILDS_PER_CLUSTER_KEY,
            LOOKUP_SCATTER_PCT.key,
            ENABLED.key,
            MODE.key,
            MAX_NEW_PER_DAY.key,
            MIN_RUNS.key,
            OBSERVE_DAYS.key,
            COOLDOWN_HOURS.key,
            BUILD_BATCH_ROWS.key,
        ] {
            assert_eq!(reg.get(k).unwrap().owner(), 13, "{k}");
        }
        assert_eq!(MAX_BUILDS_PER_CLUSTER.default, 1);
        let d = AutoIndexSettings::default();
        assert!(d.enabled);
        assert_eq!(d.mode, Mode::Auto);
        assert_eq!(d.observe_days, 7);
        assert_eq!(d.cooldown_hours, 24);
    }

    #[test]
    fn read_resolves_every_key() {
        let s = AutoIndexSettings::read(
            |_| false,
            |k| {
                if k.key == MAX_NEW_PER_DAY.key {
                    2
                } else {
                    k.default
                }
            },
            |k| k.default,
            |_| "recommend".into(),
        );
        assert!(!s.enabled);
        assert_eq!(s.mode, Mode::Recommend);
        assert_eq!(s.max_new_per_day, 2);
        assert_eq!(s.min_runs, 50);
    }
}
