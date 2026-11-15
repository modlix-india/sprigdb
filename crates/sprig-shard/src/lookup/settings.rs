//! The `lookup.*` keys (chapter 19) and [`LookupSettings`], what lookup
//! writers and lookup jobs read for one tenant. The gateway (DDL, `SPRIG
//! ... LOOKUP INDEX`, the router's claims) and the controller (recovery,
//! the advisor's builds) resolve the same keys.
//!
//! Scopes follow chapter 19's table: `lookup.lock_wait_seconds` takes every
//! data scope (its object is the owner table, resolved per claim); the job
//! keys take tenant, dc and global, as does `lookup.orphan_scan_hours`.

use std::time::Duration;

use sprig_settings::{
    Apply, HardBounds, MemSettings, Scopes, Setting, SettingCtx, SettingValue, TenantSettings,
};

/// Tenant (and ancestors), dc and global.
const TENANT_DC: Scopes = Scopes(0b11_0100);

macro_rules! key {
    ($(#[$doc:meta])* $name:ident = $key:literal, $scopes:expr, $default:expr, $min:expr, $max:expr, $text:literal) => {
        $(#[$doc])*
        pub static $name: Setting<u64> = Setting {
            key: $key,
            default: $default,
            scopes: $scopes,
            hard: HardBounds::range($min, $max),
            apply: Apply::Live,
            operator_only: false,
            owner: 19,
            doc: $text,
        };
        sprig_settings::register!($name);
    };
}

key! {
    /// `lookup.lock_wait_seconds`: the lock wait of claims and owner checks
    /// (the Pre sessions' `innodb_lock_wait_timeout`).
    LOCK_WAIT_SECONDS = "lookup.lock_wait_seconds", Scopes::DATA, 5, 1, 3600,
    "Lock wait for claims and owner checks"
}
key! {
    /// `lookup.build_lease_seconds`: a lookup job that hasn't touched its
    /// lookup for this long has stopped; the controller takes it over.
    BUILD_LEASE_SECONDS = "lookup.build_lease_seconds", TENANT_DC, 30, 5, 3600,
    "A lookup build untouched this long is taken over"
}
key! {
    /// `lookup.backfill_chunk_rows`: owner rows per back-fill and
    /// verification chunk.
    BACKFILL_CHUNK_ROWS = "lookup.backfill_chunk_rows", TENANT_DC, 1000, 10, 100_000,
    "Owner rows per back-fill and verification chunk"
}
key! {
    /// `lookup.publish_timeout_ms`: how long a lookup state change waits for
    /// every live gateway to acknowledge it; gateways that haven't by then
    /// are fenced (their registration lease is revoked) and the job waits
    /// out their lease.
    PUBLISH_TIMEOUT_MS = "lookup.publish_timeout_ms", TENANT_DC, 10_000, 100, 600_000,
    "Wait for every gateway to acknowledge a lookup state change"
}

key! {
    /// `lookup.orphan_scan_hours`: hours between orphan scans of each public
    /// lookup, which the controller's sharding loop schedules; 0 turns the
    /// scans off.
    ORPHAN_SCAN_HOURS = "lookup.orphan_scan_hours", TENANT_DC, 24, 0, 8760,
    "Hours between orphan scans of each lookup (0: off)"
}

/// The keys for one tenant, resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupSettings {
    /// `lookup.lock_wait_seconds`.
    pub lock_wait_seconds: u64,
    /// `lookup.build_lease_seconds`.
    pub build_lease: Duration,
    /// `lookup.backfill_chunk_rows`.
    pub chunk_rows: usize,
    /// `lookup.publish_timeout_ms`.
    pub publish_timeout: Duration,
    /// `lookup.orphan_scan_hours`; `None` when 0 (no scans).
    pub orphan_scan_every: Option<Duration>,
}

impl Default for LookupSettings {
    fn default() -> Self {
        Self {
            lock_wait_seconds: LOCK_WAIT_SECONDS.default,
            build_lease: Duration::from_secs(BUILD_LEASE_SECONDS.default),
            chunk_rows: to_usize(BACKFILL_CHUNK_ROWS.default),
            publish_timeout: Duration::from_millis(PUBLISH_TIMEOUT_MS.default),
            orphan_scan_every: hours(ORPHAN_SCAN_HOURS.default),
        }
    }
}

/// `n` hours, `None` for 0.
fn hours(n: u64) -> Option<Duration> {
    (n > 0).then(|| Duration::from_secs(n.saturating_mul(3600)))
}

fn to_usize(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// One key over the etcd scopes and, when given, a tenant's own rows.
fn layered<T: SettingValue>(
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

impl LookupSettings {
    /// Resolves the keys with `get` (the gateway's live settings with the
    /// tenant's rows) for `ctx`.
    pub fn resolve(
        ctx: &SettingCtx<'_>,
        mut get: impl FnMut(&'static Setting<u64>, &SettingCtx<'_>) -> u64,
    ) -> Self {
        Self {
            lock_wait_seconds: get(&LOCK_WAIT_SECONDS, ctx).max(1),
            build_lease: Duration::from_secs(get(&BUILD_LEASE_SECONDS, ctx).max(1)),
            chunk_rows: to_usize(get(&BACKFILL_CHUNK_ROWS, ctx)).max(1),
            publish_timeout: Duration::from_millis(get(&PUBLISH_TIMEOUT_MS, ctx).max(1)),
            orphan_scan_every: hours(get(&ORPHAN_SCAN_HOURS, ctx)),
        }
    }

    /// `lookup.lock_wait_seconds` alone, over a snapshot of the etcd scopes
    /// and, when given, the tenant's rows, for `ctx` (an owner table's
    /// scope): what the controller's orphan scans use.
    #[must_use]
    pub fn lock_wait_with(
        s: &MemSettings<'_>,
        rows: Option<&TenantSettings>,
        ctx: &SettingCtx<'_>,
    ) -> u64 {
        layered(s, rows, &LOCK_WAIT_SECONDS, ctx).max(1)
    }

    /// [`Self::resolve`] over a snapshot of the etcd scopes and, when given,
    /// the tenant's own catalog rows: what the controller resolves.
    #[must_use]
    pub fn resolve_with(
        s: &MemSettings<'_>,
        rows: Option<&TenantSettings>,
        ctx: &SettingCtx<'_>,
    ) -> Self {
        Self::resolve(ctx, |k, c| layered(s, rows, k, c))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_settings::Registry;

    use super::*;

    #[test]
    fn keys_are_registered_with_the_books_defaults_and_scopes() {
        let reg = Registry::global().unwrap();
        for (k, d) in [
            ("lookup.lock_wait_seconds", 5),
            ("lookup.build_lease_seconds", 30),
            ("lookup.backfill_chunk_rows", 1000),
            ("lookup.publish_timeout_ms", 10_000),
            ("lookup.orphan_scan_hours", 24),
        ] {
            let s = reg.get(k).unwrap_or_else(|| panic!("{k} isn't registered"));
            assert_eq!(s.default_value(), sprig_settings::Value::Int(d), "{k}");
        }
        let d = LookupSettings::default();
        assert_eq!(d.build_lease, Duration::from_secs(30));
        assert_eq!(d.chunk_rows, 1000);
        // Every data scope for the lock wait (its object is the owner
        // table); tenant, dc and global for the jobs' keys.
        assert_eq!(LOCK_WAIT_SECONDS.scopes, Scopes::DATA);
        assert_eq!(BUILD_LEASE_SECONDS.scopes, TENANT_DC);
        // Resolved through a resolver: values pass, a zero never stalls.
        let dc = sprig_common::ids::DcId::new("dc-a").unwrap();
        let r = LookupSettings::resolve(&SettingCtx::process(&dc), |k, _| {
            if k.key == "lookup.backfill_chunk_rows" {
                250
            } else {
                0
            }
        });
        assert_eq!(r.chunk_rows, 250);
        assert_eq!(r.lock_wait_seconds, 1);
        assert_eq!(r.build_lease, Duration::from_secs(1));
        // 0 hours: no orphan scans; the default is a day.
        assert_eq!(r.orphan_scan_every, None);
        assert_eq!(d.orphan_scan_every, Some(Duration::from_secs(24 * 3600)));
        assert_eq!(ORPHAN_SCAN_HOURS.scopes, TENANT_DC);
    }
}
