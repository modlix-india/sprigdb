//! The worker's live settings.
//!
//! A worker mirrors its DC's `settings/` and `cluster-settings/` and the
//! global `settings/` ([`LiveSettings`]), as gateways and controllers do,
//! and every job resolves its keys from the current snapshot when it uses
//! them, never once at the start:
//!
//! - mover jobs: `mover.*` for the job's tenant and source cluster, at the
//!   claim and then every second while the job runs (chunks, batches,
//!   ticks, cutover attempts, the retention's end);
//! - trigger jobs: `logic.trigger_*` at each failure (lanes when the job
//!   starts), from the trigger's table, its database and its tenant over
//!   the etcd scopes ([`LogicScope`]);
//! - the key-value sweeper: `kv.*` at each pass;
//! - stream sequencers and sinks: `stream.*` and `sink.*` from the tenant
//!   catalog's rows over the global value (no longer the default).
//!
//! A change therefore reaches running jobs within the mirror's watch round
//! trip plus one second, inside the 2 s target for settings changes.
//!
//! Before this, the worker resolved nothing: every job ran on the
//! registry's defaults, so a `mover.source_retain_hours`
//! set at dc scope never reached a move, even after a restart.

use std::sync::Arc;
use std::time::Duration;

use sprig_common::ids::{DbId, DcId, ObjectId, TenantId};
use sprig_common::{CancellationToken, Result};
use sprig_logic::triggers::{LiveRunOpts, RunOpts};
use sprig_meta::MetaStore;
use sprig_settings::{
    LiveSettings, MemSettings, Registry, Setting, SettingCtx, SettingValue, SettingsStores,
    Snapshot,
};

/// Where a trigger's or a scheduled run's `logic.*` keys resolve: its
/// tenant, and the database and table it belongs to when known. A value
/// set on the table wins over the database's, which wins over the
/// tenant's, as for every data key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicScope {
    /// The tenant.
    pub tenant: TenantId,
    /// The module's database (a stream's or queue's database for a
    /// trigger on one).
    pub db: Option<DbId>,
    /// The table an after-commit trigger is bound to.
    pub object: Option<ObjectId>,
}

impl LogicScope {
    /// `tenant`'s database `db`.
    #[must_use]
    pub const fn db(tenant: TenantId, db: u32) -> Self {
        Self {
            tenant,
            db: Some(DbId(db)),
            object: None,
        }
    }

    /// The same scope narrowed to table `object` (0 or `None`: none).
    #[must_use]
    pub const fn with_object(self, object: Option<u64>) -> Self {
        Self {
            object: match object {
                Some(o) if o != 0 => Some(ObjectId(o)),
                _ => None,
            },
            ..self
        }
    }
}

impl From<TenantId> for LogicScope {
    fn from(tenant: TenantId) -> Self {
        Self {
            tenant,
            db: None,
            object: None,
        }
    }
}

/// The worker's settings: the mirrors (absent in unit tests: defaults) and
/// its DC.
#[derive(Clone)]
pub struct WorkerSettings {
    live: Option<LiveSettings>,
    dc: DcId,
    defaults: Snapshot,
    /// Tenants' own catalog rows.
    scopes: Option<Arc<sprig_controller::tenant_scopes::TenantScopes>>,
}

impl std::fmt::Debug for WorkerSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerSettings")
            .field("dc", &self.dc)
            .field("live", &self.live.is_some())
            .finish_non_exhaustive()
    }
}

impl WorkerSettings {
    /// Starts mirroring `global` and `dc_store` and waits for the first
    /// snapshot; `None` if `shutdown` came first.
    ///
    /// # Errors
    /// Internal if the registry is invalid (a bug) or a store isn't the
    /// tier it should be.
    pub async fn start(
        global: Arc<dyn MetaStore>,
        dc_store: Arc<dyn MetaStore>,
        dc: DcId,
        shutdown: &CancellationToken,
    ) -> Result<Option<Self>> {
        let registry = Registry::global()?;
        let live = LiveSettings::start(registry, SettingsStores::new(global, dc_store)?)?;
        tokio::select! {
            () = shutdown.cancelled() => return Ok(None),
            () = live.ready() => {}
        }
        Ok(Some(Self {
            live: Some(live),
            dc,
            defaults: Arc::new(MemSettings::new(registry)),
            scopes: None,
        }))
    }

    /// The registry's defaults only (tests, tools).
    ///
    /// # Errors
    /// Internal if the registry is invalid (a bug).
    pub fn defaults(dc: DcId) -> Result<Self> {
        Ok(Self {
            live: None,
            dc,
            defaults: Arc::new(MemSettings::new(Registry::global()?)),
            scopes: None,
        })
    }

    /// Settings from `live` in `dc` (tests that drive the mirrors).
    ///
    /// # Errors
    /// Internal if the registry is invalid (a bug).
    pub fn from_live(live: LiveSettings, dc: DcId) -> Result<Self> {
        Ok(Self {
            live: Some(live),
            dc,
            defaults: Arc::new(MemSettings::new(Registry::global()?)),
            scopes: None,
        })
    }

    /// Jobs of one tenant also read its own catalog rows (tenant,
    /// database, object and ancestors' scopes).
    #[must_use]
    pub fn with_tenant_scopes(
        mut self,
        s: Arc<sprig_controller::tenant_scopes::TenantScopes>,
    ) -> Self {
        self.scopes = Some(s);
        self
    }

    /// The tenant-scope cache, when set.
    #[must_use]
    pub fn tenant_scopes(&self) -> Option<&Arc<sprig_controller::tenant_scopes::TenantScopes>> {
        self.scopes.as_ref()
    }

    /// This worker's DC.
    #[must_use]
    pub fn dc(&self) -> &DcId {
        &self.dc
    }

    /// The mirrors, when running.
    #[must_use]
    pub fn live(&self) -> Option<&LiveSettings> {
        self.live.as_ref()
    }

    /// The current rows (etcd scopes).
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.live
            .as_ref()
            .map_or_else(|| Arc::clone(&self.defaults), LiveSettings::snapshot)
    }

    /// `setting` at the process scopes (dc, global).
    #[must_use]
    pub fn get<T: SettingValue>(&self, setting: &Setting<T>) -> T {
        self.snapshot().get(setting, &SettingCtx::process(&self.dc))
    }

    /// The trigger runner's settings at the global scope (tenant values in
    /// catalogs aren't read by workers), each read again at every failure
    /// through [`RunOpts::live`]. `SPRIG_LOGIC_TRIGGER_MAX_ATTEMPTS`,
    /// `SPRIG_LOGIC_TRIGGER_RETRY_INITIAL_MS` and
    /// `SPRIG_LOGIC_TRIGGER_RETRY_MAX_MS` still override them for tests.
    #[must_use]
    pub fn run_opts(&self) -> RunOpts {
        let me = self.clone();
        let mut o = me.resolve_run_opts(None);
        o.live = Some(LiveRunOpts(Arc::new(move || me.resolve_run_opts(None))));
        o
    }

    /// [`Self::run_opts`] for one trigger: its tenant's own catalog rows
    /// (table, database, tenant, ancestors) over the etcd scopes, read
    /// again at every failure.
    #[must_use]
    pub fn run_opts_for(&self, scope: impl Into<LogicScope>) -> RunOpts {
        let scope = scope.into();
        let me = self.clone();
        let mut o = me.resolve_run_opts(Some(scope));
        o.live = Some(LiveRunOpts(Arc::new(move || {
            me.resolve_run_opts(Some(scope))
        })));
        o
    }

    /// `setting` for `tenant`: its own catalog rows over the etcd scopes,
    /// as cached now (a missing copy is read in the background).
    #[must_use]
    pub fn tenant_get<T: SettingValue>(&self, tenant: TenantId, setting: &Setting<T>) -> T {
        self.scoped_get(tenant.into(), setting)
    }

    /// `setting` in `scope`: the table's, the database's and the
    /// tenant's own rows over the etcd scopes.
    #[must_use]
    pub fn scoped_get<T: SettingValue>(&self, scope: LogicScope, setting: &Setting<T>) -> T {
        let rows = self.scopes.as_ref().and_then(|s| s.cached(scope.tenant));
        let mut ctx = SettingCtx::process(&self.dc).for_tenant(scope.tenant, &[]);
        if let Some(db) = scope.db {
            ctx = ctx.with_db(db);
        }
        if let Some(o) = scope.object {
            ctx = ctx.with_object(o);
        }
        sprig_controller::tenant_scopes::get(&self.snapshot(), rows.as_deref(), setting, &ctx)
    }

    fn resolve_run_opts(&self, scope: Option<LogicScope>) -> RunOpts {
        use sprig_logic::settings as ls;
        let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
        let get = |s: &Setting<u64>| match scope {
            Some(sc) => self.scoped_get(sc, s),
            None => self.get(s),
        };
        let lanes = get(&ls::TRIGGER_LANES);
        let attempts = env("SPRIG_LOGIC_TRIGGER_MAX_ATTEMPTS")
            .unwrap_or_else(|| get(&ls::TRIGGER_MAX_ATTEMPTS));
        let initial = env("SPRIG_LOGIC_TRIGGER_RETRY_INITIAL_MS")
            .unwrap_or_else(|| get(&ls::TRIGGER_RETRY_INITIAL_MS));
        let max = env("SPRIG_LOGIC_TRIGGER_RETRY_MAX_MS")
            .unwrap_or_else(|| get(&ls::TRIGGER_RETRY_MAX_MS));
        RunOpts {
            lanes: usize::try_from(lanes).unwrap_or(8).max(1),
            max_attempts: u32::try_from(attempts).unwrap_or(u32::MAX).max(1),
            retry_initial: Duration::from_millis(initial),
            retry_max: Duration::from_millis(max),
            ..RunOpts::default()
        }
    }

    /// `logic.trigger_budget_ms` at the global scope.
    #[must_use]
    pub fn trigger_budget(&self) -> Duration {
        Duration::from_millis(self.get(&sprig_logic::settings::TRIGGER_BUDGET_MS))
    }

    /// `logic.trigger_budget_ms` for one trigger (its tenant's own rows
    /// too, as [`Self::run_opts_for`]).
    #[must_use]
    pub fn trigger_budget_for(&self, scope: impl Into<LogicScope>) -> Duration {
        Duration::from_millis(
            self.scoped_get(scope.into(), &sprig_logic::settings::TRIGGER_BUDGET_MS),
        )
    }

    /// `logic.job_budget_ms` at the global scope.
    #[must_use]
    pub fn job_budget(&self) -> Duration {
        Duration::from_millis(self.get(&sprig_logic::settings::JOB_BUDGET_MS))
    }

    /// `logic.job_budget_ms` for one scheduled module (its tenant's own
    /// rows too, as [`Self::trigger_budget_for`]).
    #[must_use]
    pub fn job_budget_for(&self, scope: impl Into<LogicScope>) -> Duration {
        Duration::from_millis(self.scoped_get(scope.into(), &sprig_logic::settings::JOB_BUDGET_MS))
    }
}

#[cfg(test)]
mod tests;
