//! The node's backup service: archive passes every few seconds, each
//! tenant's daily backup after `backup.daily_hour` (UTC) followed by its
//! retention, and the operations `sprigctl` asks for (backup now, list,
//! restore, verify).
//!
//! Operations that read or change a tenant's backups run one at a time
//! (retention must not remove the chunks of a backup still being written);
//! the archive runs beside them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use sprig_common::{Result, SprigError};
use sprig_engine::{Engine, UnitId};

use crate::archive::{ArchiveConfig, Archiver, PassReport, is_tenant_unit};
use crate::backup::{self, BackupManifest, BackupReport};
use crate::crypt::KeyProvider;
use crate::now_ms;
use crate::restore::{self, RestoreReport};
use crate::retention::{self, RetentionReport};
use crate::target::{BackupTarget, open_location};

const DAY_MS: u64 = 86_400_000;

/// A tenant's backup settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// `backup.enabled`.
    pub enabled: bool,
    /// `backup.pitr_days`.
    pub pitr_days: u64,
    /// `backup.daily_keep_days`.
    pub daily_keep_days: u64,
    /// `backup.location` (empty: the service's default).
    pub location: String,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: true,
            pitr_days: 7,
            daily_keep_days: 30,
            location: String::new(),
        }
    }
}

/// Reads a tenant's policy (the settings chain, in the gateway).
pub type PolicyFn = Arc<dyn Fn(u64) -> Policy + Send + Sync>;

/// How the service runs.
#[derive(Clone)]
pub struct ServiceConfig {
    /// Scratch engines, staged chunks and restore markers.
    pub work_dir: PathBuf,
    /// The location used when a tenant's `backup.location` is empty.
    pub default_location: String,
    /// How often the background loop runs.
    pub tick: Duration,
    /// When archive segments close.
    pub archive: ArchiveConfig,
    /// Each tenant's settings.
    pub policy: PolicyFn,
    /// `backup.daily_hour` (UTC).
    pub daily_hour: Arc<dyn Fn() -> u64 + Send + Sync>,
    /// `backup.rate_mb_per_sec`: how fast a backup may read the store
    /// (0: no limit).
    pub rate_mb_per_sec: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl std::fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceConfig")
            .field("work_dir", &self.work_dir)
            .field("default_location", &self.default_location)
            .field("tick", &self.tick)
            .field("archive", &self.archive)
            .finish_non_exhaustive()
    }
}

struct Inner {
    engine: Engine,
    cfg: ServiceConfig,
    keys: Arc<dyn KeyProvider>,
    archiver: Archiver,
    targets: Mutex<HashMap<String, Arc<dyn BackupTarget>>>,
    ops: Mutex<()>,
    /// The UTC day each tenant's daily backup was last seen taken.
    daily: Mutex<HashMap<u64, u64>>,
    stop: AtomicBool,
}

/// The backup service of one node's engine. Cheap to clone.
#[derive(Clone)]
pub struct BackupService {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for BackupService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupService")
            .field("cfg", &self.inner.cfg)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl BackupService {
    /// Opens the service: finishes what a crash interrupted (a restore
    /// half-way is undone; scratch directories go).
    ///
    /// # Errors
    /// The work directory can't be created or read, or the store failed.
    pub fn open(
        engine: Engine,
        cfg: ServiceConfig,
        keys: Arc<dyn KeyProvider>,
    ) -> Result<BackupService> {
        std::fs::create_dir_all(&cfg.work_dir).map_err(|e| {
            SprigError::internal(format!("backup: create {}: {e}", cfg.work_dir.display()))
        })?;
        let undone = restore::recover(&engine, &cfg.work_dir)?;
        if !undone.is_empty() {
            tracing::warn!(?undone, "restores that stopped half-way were undone");
        }
        Ok(BackupService {
            inner: Arc::new(Inner {
                archiver: Archiver::new(cfg.archive.clone()),
                engine,
                cfg,
                keys,
                targets: Mutex::new(HashMap::new()),
                ops: Mutex::new(()),
                daily: Mutex::new(HashMap::new()),
                stop: AtomicBool::new(false),
            }),
        })
    }

    /// The engine it backs up.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// A tenant's policy.
    #[must_use]
    pub fn policy(&self, tenant: u64) -> Policy {
        (self.inner.cfg.policy)(tenant)
    }

    /// The storage `tenant`'s backups go to (whether or not they're on).
    ///
    /// # Errors
    /// Its location can't be opened.
    pub fn target(&self, tenant: u64) -> Result<Arc<dyn BackupTarget>> {
        let p = self.policy(tenant);
        let loc = if p.location.trim().is_empty() {
            self.inner.cfg.default_location.clone()
        } else {
            p.location
        };
        let mut t = lock(&self.inner.targets);
        if let Some(x) = t.get(&loc) {
            return Ok(Arc::clone(x));
        }
        let x = open_location(&loc)?;
        t.insert(loc, Arc::clone(&x));
        Ok(x)
    }

    fn enabled_target(&self, tenant: u64) -> Result<Option<Arc<dyn BackupTarget>>> {
        if self.policy(tenant).enabled {
            self.target(tenant).map(Some)
        } else {
            Ok(None)
        }
    }

    /// One archive pass (`force`: upload everything applied now).
    ///
    /// # Errors
    /// See [`Archiver::pass`].
    pub fn archive_pass(&self, force: bool) -> Result<PassReport> {
        let i = &self.inner;
        i.archiver.pass(
            &i.engine,
            &|t| self.enabled_target(t),
            i.keys.as_ref(),
            force,
        )
    }

    /// Takes a backup of `tenant` now.
    ///
    /// # Errors
    /// The engine or the storage failed.
    pub fn backup_now(&self, tenant: u64) -> Result<BackupReport> {
        let _g = lock(&self.inner.ops);
        self.backup_locked(tenant)
    }

    fn backup_locked(&self, tenant: u64) -> Result<BackupReport> {
        let i = &self.inner;
        let target = self.target(tenant)?;
        // Hold the log from here on before the export, so the archive
        // continues from the backup's index without a gap.
        if self.policy(tenant).enabled && !i.engine.is_replicated() {
            let unit = Engine::unit_of_tenant(tenant);
            i.archiver.hold(&i.engine, &|u| u == unit, &|_| false)?;
        }
        let key = i.keys.key_for(tenant)?;
        let rate = (i.cfg.rate_mb_per_sec)().saturating_mul(1 << 20);
        backup::take(
            &i.engine,
            target.as_ref(),
            &key,
            tenant,
            &i.cfg.work_dir,
            rate,
        )
    }

    /// The backups of `tenant`, oldest first.
    ///
    /// # Errors
    /// The storage failed.
    pub fn list(&self, tenant: u64) -> Result<Vec<BackupManifest>> {
        let target = self.target(tenant)?;
        backup::list(target.as_ref(), &self.inner.keys.key_for(tenant)?, tenant)
    }

    /// Restores `tenant` as of `to_ms` (newest archived state when `None`)
    /// into tenant `into`, which must hold nothing on the engine unless
    /// `replace` is set (see [`restore::restore_into`]); then takes `into`'s first backup, so its own archive has a base.
    ///
    /// # Errors
    /// See [`restore::restore_into`].
    pub fn restore(
        &self,
        tenant: u64,
        to_ms: Option<u64>,
        into: u64,
        replace: bool,
    ) -> Result<RestoreReport> {
        // Recent moments need what the archive hasn't uploaded yet.
        if self.recent(to_ms)
            && let Err(e) = self.archive_pass(true)
        {
            tracing::warn!(error = %e, "archive pass before a restore failed");
        }
        let _g = lock(&self.inner.ops);
        let i = &self.inner;
        let target = self.target(tenant)?;
        let key = i.keys.key_for(tenant)?;
        let r = restore::restore_into(
            &i.engine,
            target.as_ref(),
            &key,
            tenant,
            into,
            to_ms,
            replace,
            &i.cfg.work_dir,
        )?;
        if self.policy(into).enabled
            && let Err(e) = self.backup_locked(into)
        {
            tracing::warn!(tenant = into, error = %e, "the restored tenant's first backup failed; the daily run retries");
        }
        Ok(r)
    }

    /// Whether a moment is recent enough that the archive may not hold it
    /// yet.
    fn recent(&self, to_ms: Option<u64>) -> bool {
        let lag = u64::try_from(self.inner.cfg.archive.close_after.as_millis()).unwrap_or(u64::MAX);
        to_ms.is_none_or(|t| t.saturating_add(lag.saturating_mul(2)) >= now_ms())
    }

    /// A restore check into a scratch engine: the checksum `tenant` had
    /// as of `to_ms`.
    ///
    /// # Errors
    /// See [`restore::verify`].
    pub fn verify(&self, tenant: u64, to_ms: Option<u64>) -> Result<RestoreReport> {
        if self.recent(to_ms)
            && let Err(e) = self.archive_pass(true)
        {
            tracing::warn!(error = %e, "archive pass before a restore check failed");
        }
        let _g = lock(&self.inner.ops);
        let i = &self.inner;
        let target = self.target(tenant)?;
        let key = i.keys.key_for(tenant)?;
        restore::verify(target.as_ref(), &key, tenant, to_ms, &i.cfg.work_dir)
    }

    /// Applies retention to `tenant` now.
    ///
    /// # Errors
    /// The storage failed.
    pub fn retention_now(&self, tenant: u64, now: u64) -> Result<RetentionReport> {
        let _g = lock(&self.inner.ops);
        self.retention_locked(tenant, now)
    }

    fn retention_locked(&self, tenant: u64, now: u64) -> Result<RetentionReport> {
        let p = self.policy(tenant);
        let target = self.target(tenant)?;
        let key = self.inner.keys.key_for(tenant)?;
        retention::apply(
            target.as_ref(),
            &key,
            tenant,
            now,
            p.pitr_days,
            p.daily_keep_days,
        )
    }

    /// The daily run at `now`: after the daily hour, each enabled tenant
    /// without a backup taken today gets one, then its retention. Returns
    /// the tenants backed up.
    pub fn daily(&self, now: u64) -> Vec<u64> {
        let day = now / DAY_MS;
        let hour = (now % DAY_MS) / 3_600_000;
        if hour < (self.inner.cfg.daily_hour)() {
            return Vec::new();
        }
        let mut done = Vec::new();
        let tenants: Vec<u64> = self
            .inner
            .engine
            .units()
            .into_iter()
            .map(|(u, _)| u)
            .filter(|u| is_tenant_unit(*u))
            .map(|UnitId(t)| t)
            .collect();
        for t in tenants {
            if self.inner.stop.load(Ordering::Relaxed) {
                break;
            }
            if lock(&self.inner.daily).get(&t) == Some(&day) || !self.policy(t).enabled {
                continue;
            }
            // A tenant whose keys are all gone (deleted) needs no more
            // backups; the ones it has stay until removed.
            if !has_keys(&self.inner.engine, t) {
                lock(&self.inner.daily).insert(t, day);
                continue;
            }
            let _g = lock(&self.inner.ops);
            let taken_today = self
                .list(t)
                .map(|l| l.last().is_some_and(|m| m.taken_ms / DAY_MS == day));
            match taken_today {
                Ok(true) => {}
                Ok(false) => match self.backup_locked(t) {
                    Ok(_) => done.push(t),
                    Err(e) => {
                        tracing::warn!(tenant = t, error = %e, "daily backup failed; retrying next pass");
                        continue;
                    }
                },
                Err(e) => {
                    tracing::warn!(tenant = t, error = %e, "listing backups failed; retrying next pass");
                    continue;
                }
            }
            if let Err(e) = self.retention_locked(t, now) {
                tracing::warn!(tenant = t, error = %e, "backup retention failed");
            }
            lock(&self.inner.daily).insert(t, day);
        }
        done
    }

    /// Starts the background loop on its own thread.
    #[must_use]
    pub fn spawn(&self) -> std::thread::JoinHandle<()> {
        // The archive on a thread of its own: a long (paced) daily backup
        // must not keep a new tenant's log unheld.
        let me = self.clone();
        let archive = std::thread::Builder::new()
            .name("sprig-backup-archive".into())
            .spawn(move || {
                while !me.inner.stop.load(Ordering::Relaxed) {
                    if let Err(e) = me.archive_pass(false) {
                        tracing::debug!(error = %e, "archive pass skipped");
                    }
                    std::thread::park_timeout(me.inner.cfg.tick);
                }
            });
        if let Err(e) = archive {
            tracing::error!(error = %e, "the archive thread didn't start");
        }
        let me = self.clone();
        std::thread::Builder::new()
            .name("sprig-backup".into())
            .spawn(move || {
                while !me.inner.stop.load(Ordering::Relaxed) {
                    me.daily(now_ms());
                    std::thread::park_timeout(me.inner.cfg.tick);
                }
            })
            .unwrap_or_else(|_| std::thread::spawn(|| {}))
    }

    /// Stops the background loop (it ends after its current step).
    pub fn stop(&self) {
        self.inner.stop.store(true, Ordering::Relaxed);
    }
}

/// Whether `tenant`'s range holds any key on `engine`.
fn has_keys(engine: &Engine, tenant: u64) -> bool {
    use sprig_engine::store::{Cf, IterOpts};
    let r = sprig_engine::keys::tenant_range(tenant);
    let mut it = engine.store().iter(Cf::Data, IterOpts::range(&r));
    it.seek(&r.start);
    it.valid()
}
