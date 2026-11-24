//! Backups of the node's engine tenants ([`sprig_backup`]): opened with
//! the engine, they archive every tenant unit's commit log continuously
//! and take each tenant's daily backup (the `backup.*` settings), and the
//! node-local Admin API routes under [`ROUTE`] serve `sprigctl tenant
//! backup|backups|restore|backup-check`.
//!
//! - `SPRIGDB_BACKUP=off` leaves the service closed.
//! - `SPRIGDB_BACKUP_KEY_FILE`: the per-installation key (default
//!   `<engine dir>/backup.key`, created on first use). Keep a copy off the
//!   node: backups can't be read without it.
//! - `SPRIGDB_BACKUP_LOCATION`: where backups go when a tenant's
//!   `backup.location` is empty (default `<engine dir>/backups`).
//!
//! Routes (POST, JSON bodies, the Admin API's authorization):
//! `:take {tenant}`, `:list {tenant}`, `:restore {tenant, into, to_ms?,
//! replace?}`, `:verify {tenant, to_ms?}`, `:archive {}`,
//! `:retention {tenant}`.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::{Value, json};
use sprig_backup::service::{Policy, ServiceConfig};
use sprig_backup::settings::{
    DAILY_HOUR, DAILY_KEEP_DAYS, ENABLED, LOCATION, PITR_DAYS, RATE_MB_PER_SEC,
};
use sprig_backup::{BackupService, KeyFile};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_engine::Engine;
use sprig_settings::{LiveSettings, SettingCtx};

/// The variable that turns backups off.
pub const OFF_VAR: &str = "SPRIGDB_BACKUP";
/// The variable naming the key file.
pub const KEY_VAR: &str = "SPRIGDB_BACKUP_KEY_FILE";
/// The variable naming the default location.
pub const LOCATION_VAR: &str = "SPRIGDB_BACKUP_LOCATION";
/// The node-local Admin API routes' prefix.
pub const ROUTE: &str = "/admin/v1/engine/backups";

static SERVICE: OnceLock<Option<BackupService>> = OnceLock::new();
static LIVE: OnceLock<LiveSettings> = OnceLock::new();

/// Gives the service the settings it reads tenants' `backup.*` keys from
/// (the tenant registry's start calls it).
pub(crate) fn set_settings(live: &LiveSettings) {
    let _ = LIVE.set(live.clone());
}

fn policy(tenant: u64) -> Policy {
    let Some(live) = LIVE.get() else {
        return Policy::default();
    };
    let dc = live.stores().dc_id().clone();
    let ctx = SettingCtx::process(&dc).for_tenant(TenantId(tenant), &[]);
    Policy {
        enabled: live.get(&ENABLED, &ctx),
        pitr_days: live.get(&PITR_DAYS, &ctx),
        daily_keep_days: live.get(&DAILY_KEEP_DAYS, &ctx),
        location: live.get(&LOCATION, &ctx).into_owned(),
    }
}

fn rate_mb_per_sec() -> u64 {
    LIVE.get().map_or(RATE_MB_PER_SEC.default, |live| {
        let dc = live.stores().dc_id().clone();
        live.get(&RATE_MB_PER_SEC, &SettingCtx::process(&dc))
    })
}

fn daily_hour() -> u64 {
    LIVE.get().map_or(DAILY_HOUR.default, |live| {
        let dc = live.stores().dc_id().clone();
        live.get(&DAILY_HOUR, &SettingCtx::process(&dc))
    })
}

/// Opens the backup service on the node's engine, once (the engine's
/// initialisation calls it).
pub(crate) fn open_on(e: &Engine) {
    SERVICE.get_or_init(|| {
        if std::env::var(OFF_VAR).is_ok_and(|v| v.eq_ignore_ascii_case("off")) {
            // An archive an earlier run kept must not hold the log now.
            if let Err(err) = e.clear_log_floor(sprig_backup::archive::FLOOR) {
                tracing::warn!(error = %err, "backups off: clearing the archive's log floor failed");
            }
            tracing::info!("backups off ({OFF_VAR}=off)");
            return None;
        }
        let key_path = std::env::var(KEY_VAR)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map_or_else(|| e.data_dir().join("backup.key"), PathBuf::from);
        let keys = match KeyFile::open_or_create(&key_path) {
            Ok(k) => k,
            Err(err) => {
                tracing::error!(error = %err, "backups not started");
                return None;
            }
        };
        let default_location = std::env::var(LOCATION_VAR)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| e.data_dir().join("backups").display().to_string());
        let cfg = ServiceConfig {
            work_dir: e.data_dir().join("backup-work"),
            default_location,
            tick: Duration::from_secs(5),
            archive: sprig_backup::archive::ArchiveConfig::default(),
            policy: Arc::new(policy),
            daily_hour: Arc::new(daily_hour),
            rate_mb_per_sec: Arc::new(rate_mb_per_sec),
        };
        match BackupService::open(e.clone(), cfg, Arc::new(keys)) {
            Ok(s) => {
                if e.is_replicated() {
                    tracing::info!("backups: the archive runs on one-node engines only; on demand backups still work");
                }
                drop(s.spawn());
                tracing::info!(key = %key_path.display(), "backups open");
                Some(s)
            }
            Err(err) => {
                tracing::error!(error = %err, "backups not started");
                None
            }
        }
    });
}

/// The node's backup service, once the engine and the service are open.
///
/// # Errors
/// The engine isn't open here, or backups are off.
pub fn service() -> Result<BackupService> {
    crate::node_engine::engine()?;
    SERVICE.get().cloned().flatten().ok_or_else(|| {
        SprigError::new(
            SprigCode::AdminRequestInvalid,
            format!(
                "backups are off on this node ({OFF_VAR}=off, or the key file failed; see the log)"
            ),
        )
    })
}

fn bad(m: String) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

fn to_json<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).unwrap_or_default()
}

/// Serves one route under [`ROUTE`] (blocking: run it off the async
/// runtime).
///
/// # Errors
/// An unknown route, a malformed body, or the operation's own error.
pub fn handle(path: &str, body: &[u8]) -> Result<Value> {
    let action = path
        .strip_prefix(ROUTE)
        .ok_or_else(|| bad(format!("no route {path}")))?;
    let body: Value = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(body).map_err(|e| bad(format!("the body isn't JSON: {e}")))?
    };
    let num = |k: &str| body.get(k).and_then(Value::as_u64);
    let tenant = || num("tenant").ok_or_else(|| bad("'tenant' (a tenant id) is required".into()));
    let svc = service()?;
    Ok(match action {
        ":take" => to_json(&svc.backup_now(tenant()?)?),
        ":list" => json!({ "backups": to_json(&svc.list(tenant()?)?) }),
        ":restore" => {
            let into = num("into")
                .ok_or_else(|| bad("'into' (the target tenant id) is required".into()))?;
            let replace = body
                .get("replace")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            to_json(&svc.restore(tenant()?, num("to_ms"), into, replace)?)
        }
        ":verify" => to_json(&svc.verify(tenant()?, num("to_ms"))?),
        ":archive" => {
            let p = svc.archive_pass(true)?;
            json!({"segments": p.segments, "entries": p.entries, "bytes": p.bytes, "failed": p.failed})
        }
        ":retention" => to_json(&svc.retention_now(tenant()?, sprig_backup::now_ms())?),
        _ => return Err(bad(format!("no route {path}"))),
    })
}
