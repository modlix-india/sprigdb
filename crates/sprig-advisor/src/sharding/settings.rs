//! The `advisor.*` keys and [`AdvisorSettings`], a typed snapshot for one
//! tenant. Every key accepts the tenant and global scopes; `advisor.mode`
//! also dc.

use std::borrow::Cow;
use std::time::Duration;

use sprig_settings::{Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx, TenantSettings};

/// Tenant and global.
const TENANT_GLOBAL: Scopes = Scopes(0b10_0100);
/// Tenant, dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident: $t:ty = $key:literal, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<$t> = Setting {
            key: $key,
            default: $default,
            scopes: TENANT_GLOBAL,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: true,
            owner: 16,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

/// `advisor.mode`: `on`, `recommend` or `off`.
pub static MODE: Setting<Cow<'static, str>> = Setting {
    key: "advisor.mode",
    default: Cow::Borrowed("on"),
    scopes: TENANT_DC,
    hard: HardBounds::one_of(&["on", "approve", "recommend", "off"]),
    apply: Apply::Live,
    operator_only: true,
    owner: 16,
    doc: "Sharding advisor: on, approve (held for approval), recommend or off",
};
sprig_settings::register!(MODE);

key! {
    /// `advisor.window_days`.
    WINDOW_DAYS: f64 = "advisor.window_days", 7.0, 0, 365, "Statistics window"
}
key! {
    /// `advisor.min_observe_hours`.
    MIN_OBSERVE_HOURS: f64 = "advisor.min_observe_hours", 24.0, 0, 8760, "History needed for a design"
}
key! {
    /// `advisor.min_units`.
    MIN_UNITS: u64 = "advisor.min_units", 10_000, 0, 1_000_000_000, "Sampled units needed"
}
key! {
    /// `advisor.min_confidence`.
    MIN_CONFIDENCE: f64 = "advisor.min_confidence", 0.9, 0, 1, "Confidence needed to apply a first design"
}
key! {
    /// `advisor.max_wait_hours`.
    MAX_WAIT_HOURS: f64 = "advisor.max_wait_hours", 72.0, 0, 8760, "Then apply the best design anyway"
}
key! {
    /// `advisor.txn_weight`.
    TXN_WEIGHT: f64 = "advisor.txn_weight", 2.0, 0, 100, "Weight of a write transaction"
}
key! {
    /// `advisor.lookup_penalty`.
    LOOKUP_PENALTY: f64 = "advisor.lookup_penalty", 0.1, 0, 1, "Credit lost per lookup a statement needs"
}
key! {
    /// `advisor.min_ndv_per_shard`.
    MIN_NDV_PER_SHARD: u64 = "advisor.min_ndv_per_shard", 100, 0, 1_000_000_000, "Distinct values needed per expected shard"
}
key! {
    /// `advisor.max_top_value_pct`.
    MAX_TOP_VALUE_PCT: u32 = "advisor.max_top_value_pct", 25, 1, 100, "Largest share one value may hold"
}
key! {
    /// `advisor.max_moves`.
    MAX_MOVES: u32 = "advisor.max_moves", 200, 0, 100_000, "Search steps per run"
}

key! {
    /// `advisor.lookup_min_runs`: equality reads of one column (`=`, `IN`)
    /// in the window before the advisor proposes a global lookup index on
    /// it (chapter 19).
    LOOKUP_MIN_RUNS: u64 = "advisor.lookup_min_runs", 100, 1, 1_000_000_000, "Reads of a column before a lookup index is proposed"
}
key! {
    /// `advisor.lookup_window_minutes`: the workload window lookup
    /// proposals read.
    LOOKUP_WINDOW_MINUTES: u64 = "advisor.lookup_window_minutes", 60, 1, 10_080, "Workload window of lookup proposals"
}
key! {
    /// `advisor.lookup_eval_minutes`: how often a tenant's workload is read
    /// for lookup proposals (and approved ones are built).
    LOOKUP_EVAL_MINUTES: u64 = "advisor.lookup_eval_minutes", 5, 1, 1440, "Period of the lookup proposals"
}

/// The advisor's lookup proposals for one tenant (chapter 19):
/// `advisor.lookup_*` and `auto_index.lookup_scatter_pct`.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LookupAdvice {
    /// `advisor.lookup_min_runs`.
    pub min_runs: u64,
    /// `auto_index.lookup_scatter_pct`: the share of those reads that
    /// went to every shard, in percent.
    pub scatter_pct: f64,
    /// `advisor.lookup_window_minutes`.
    pub window: Duration,
    /// `advisor.lookup_eval_minutes`.
    pub every: Duration,
}

impl Default for LookupAdvice {
    fn default() -> Self {
        Self {
            min_runs: LOOKUP_MIN_RUNS.default,
            scatter_pct: crate::autoindex::settings::LOOKUP_SCATTER_PCT.default,
            window: Duration::from_secs(LOOKUP_WINDOW_MINUTES.default * 60),
            every: Duration::from_secs(LOOKUP_EVAL_MINUTES.default * 60),
        }
    }
}

impl LookupAdvice {
    /// Resolves the keys in `ctx` over the etcd scopes and, when given,
    /// the tenant's own rows.
    #[must_use]
    pub fn resolve_with(
        s: &MemSettings<'_>,
        rows: Option<&TenantSettings>,
        ctx: &SettingCtx<'_>,
    ) -> Self {
        Self {
            min_runs: layered(s, rows, &LOOKUP_MIN_RUNS, ctx).max(1),
            scatter_pct: layered(
                s,
                rows,
                &crate::autoindex::settings::LOOKUP_SCATTER_PCT,
                ctx,
            )
            .clamp(0.0, 100.0),
            window: Duration::from_secs(
                layered(s, rows, &LOOKUP_WINDOW_MINUTES, ctx)
                    .max(1)
                    .saturating_mul(60),
            ),
            every: Duration::from_secs(
                layered(s, rows, &LOOKUP_EVAL_MINUTES, ctx)
                    .max(1)
                    .saturating_mul(60),
            ),
        }
    }

    /// Whether `runs` equality reads of which `scattered` went to every
    /// shard ask for a lookup.
    #[must_use]
    pub fn qualifies(&self, runs: u64, scattered: u64) -> bool {
        #[allow(clippy::cast_precision_loss)] // run counts
        let share = scattered as f64 * 100.0;
        #[allow(clippy::cast_precision_loss)]
        let need = runs as f64 * self.scatter_pct;
        runs >= self.min_runs && share >= need
    }
}

/// How the advisor acts on a design.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvisorMode {
    /// Runs and applies.
    On,
    /// Runs; `apply` becomes `recommend`.
    Recommend,
    /// Runs; `apply` waits for an approval, then applies.
    Approve,
    /// No runs: a first split uses the structural design.
    Off,
}

/// The keys for one tenant, resolved.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdvisorSettings {
    /// `advisor.mode`.
    pub mode: AdvisorMode,
    /// `window_days`.
    pub window: Duration,
    /// `min_observe_hours`.
    pub min_observe: Duration,
    /// `min_units`.
    pub min_units: u64,
    /// `min_confidence`.
    pub min_confidence: f64,
    /// `max_wait_hours`.
    pub max_wait: Duration,
    /// `txn_weight`.
    pub txn_weight: f64,
    /// `lookup_penalty`.
    pub lookup_penalty: f64,
    /// `min_ndv_per_shard`.
    pub min_ndv_per_shard: u64,
    /// `max_top_value_pct` as a ratio.
    pub max_top_value: f64,
    /// `max_moves`.
    pub max_moves: u32,
}

/// One key over the etcd scopes and, when given, a tenant's own rows.
fn layered<T: sprig_settings::SettingValue>(
    s: &MemSettings<'_>,
    rows: Option<&TenantSettings>,
    k: &Setting<T>,
    ctx: &SettingCtx<'_>,
) -> T {
    match rows {
        Some(t) => t
            .resolve(s, k.key, ctx)
            .ok()
            .and_then(|r| T::from_value(&r.value))
            .unwrap_or_else(|| k.default.clone()),
        None => s.get(k, ctx),
    }
}

fn hours(h: f64) -> Duration {
    Duration::from_secs_f64((h * 3600.0).max(0.0))
}

impl AdvisorSettings {
    /// Resolves the keys in `ctx` (a context with the tenant set).
    #[must_use]
    pub fn resolve(s: &MemSettings<'_>, ctx: &SettingCtx<'_>) -> Self {
        Self::resolve_with(s, None, ctx)
    }

    /// [`Self::resolve`] with the tenant's own catalog rows (tenant,
    /// database, ancestors) layered over the etcd scopes:
    /// what a gateway would resolve.
    #[must_use]
    pub fn resolve_with(
        s: &MemSettings<'_>,
        rows: Option<&TenantSettings>,
        ctx: &SettingCtx<'_>,
    ) -> Self {
        Self {
            mode: match layered(s, rows, &MODE, ctx).as_ref() {
                "recommend" => AdvisorMode::Recommend,
                "approve" => AdvisorMode::Approve,
                "off" => AdvisorMode::Off,
                _ => AdvisorMode::On,
            },
            window: hours(layered(s, rows, &WINDOW_DAYS, ctx) * 24.0),
            min_observe: hours(layered(s, rows, &MIN_OBSERVE_HOURS, ctx)),
            min_units: layered(s, rows, &MIN_UNITS, ctx),
            min_confidence: layered(s, rows, &MIN_CONFIDENCE, ctx),
            max_wait: hours(layered(s, rows, &MAX_WAIT_HOURS, ctx)),
            txn_weight: layered(s, rows, &TXN_WEIGHT, ctx),
            lookup_penalty: layered(s, rows, &LOOKUP_PENALTY, ctx),
            min_ndv_per_shard: layered(s, rows, &MIN_NDV_PER_SHARD, ctx),
            max_top_value: f64::from(layered(s, rows, &MAX_TOP_VALUE_PCT, ctx)) / 100.0,
            max_moves: layered(s, rows, &MAX_MOVES, ctx),
        }
    }
}

impl Default for AdvisorSettings {
    fn default() -> Self {
        Self {
            mode: AdvisorMode::On,
            window: hours(WINDOW_DAYS.default * 24.0),
            min_observe: hours(MIN_OBSERVE_HOURS.default),
            min_units: MIN_UNITS.default,
            min_confidence: MIN_CONFIDENCE.default,
            max_wait: hours(MAX_WAIT_HOURS.default),
            txn_weight: TXN_WEIGHT.default,
            lookup_penalty: LOOKUP_PENALTY.default,
            min_ndv_per_shard: MIN_NDV_PER_SHARD.default,
            max_top_value: f64::from(MAX_TOP_VALUE_PCT.default) / 100.0,
            max_moves: MAX_MOVES.default,
        }
    }
}

#[cfg(test)]
mod lookup_tests {
    #![allow(clippy::unwrap_used)]

    use sprig_settings::{Registry, Value};

    use super::*;

    #[test]
    fn lookup_thresholds_are_registered_and_judge_scatter() {
        let reg = Registry::global().unwrap();
        for (k, d) in [
            ("advisor.lookup_min_runs", 100),
            ("advisor.lookup_window_minutes", 60),
            ("advisor.lookup_eval_minutes", 5),
        ] {
            let def = reg.get(k).unwrap_or_else(|| panic!("{k} isn't registered"));
            assert_eq!(def.default_value(), Value::Int(d), "{k}");
        }
        let a = LookupAdvice::default();
        assert_eq!(a.window, Duration::from_secs(3600));
        assert_eq!(a.every, Duration::from_secs(300));
        assert!((a.scatter_pct - 80.0).abs() < f64::EPSILON);
        assert!(a.qualifies(120, 100));
        assert!(!a.qualifies(120, 90), "75% scattered");
        assert!(!a.qualifies(99, 99), "too few runs");
        let strict = LookupAdvice {
            min_runs: 10,
            scatter_pct: 100.0,
            ..a
        };
        assert!(strict.qualifies(10, 10) && !strict.qualifies(10, 9));
    }
}
