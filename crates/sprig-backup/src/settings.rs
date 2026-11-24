//! The `backup.*` settings.

use std::borrow::Cow;

use sprig_settings::{Apply, HardBounds, Scopes, Setting};

/// The chapter that owns the keys (operations: backups).
const OWNER: u8 = 47;

/// `backup.enabled`.
pub static ENABLED: Setting<bool> = Setting {
    key: "backup.enabled",
    default: true,
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Back up the tenant's engine data: a daily backup and the continuous change-log archive",
};
sprig_settings::register!(ENABLED);

/// `backup.pitr_days`.
pub static PITR_DAYS: Setting<u64> = Setting {
    key: "backup.pitr_days",
    default: 7,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 3650),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Any moment in this many past days can be restored",
};
sprig_settings::register!(PITR_DAYS);

/// `backup.daily_keep_days`.
pub static DAILY_KEEP_DAYS: Setting<u64> = Setting {
    key: "backup.daily_keep_days",
    default: 30,
    scopes: Scopes::TREE,
    hard: HardBounds::range(0, 3650),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Daily backups are kept this many days",
};
sprig_settings::register!(DAILY_KEEP_DAYS);

/// `backup.location`.
pub static LOCATION: Setting<Cow<'static, str>> = Setting {
    key: "backup.location",
    default: Cow::Borrowed(""),
    scopes: Scopes::TREE,
    hard: HardBounds::NONE,
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "Where backups go: an absolute path, file:///path, or s3://bucket/prefix?endpoint=URL&region=R (empty: <engine dir>/backups)",
};
sprig_settings::register!(LOCATION);

/// `backup.daily_hour`.
pub static DAILY_HOUR: Setting<u64> = Setting {
    key: "backup.daily_hour",
    default: 2,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 23),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "The hour (UTC) after which each day's backups are taken",
};
sprig_settings::register!(DAILY_HOUR);

/// `backup.rate_mb_per_sec`.
pub static RATE_MB_PER_SEC: Setting<u64> = Setting {
    key: "backup.rate_mb_per_sec",
    default: 32,
    scopes: Scopes::PROCESS,
    hard: HardBounds::range(0, 100_000),
    apply: Apply::Live,
    operator_only: true,
    owner: OWNER,
    doc: "How fast a backup reads the engine, MiB per second, so live work keeps its latency (0: no limit)",
};
sprig_settings::register!(RATE_MB_PER_SEC);
