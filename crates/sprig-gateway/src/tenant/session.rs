//! One tenant session's catalog state: the pinned tenant, the snapshot
//! statements read, the [`NameMap`](sprig_sql::names::NameMap) built from
//! it, the session's last own catalog change (read your own DDL), its
//! temporary tables, the warnings the gateway raised for its last
//! statement, and the live settings it follows.
//!
//! **Freshness.** The first statement (or `USE` at login) asks the cache
//! for [`Freshness::Latest`]; every later one for `Cached { min_version:
//! last_change }`, which returns the newest cached snapshot at once and
//! waits only when the session's own change isn't loaded yet. The tenant
//! is pinned for the session's life, so its snapshots reload as soon as its
//! directory entry moves: another session's change (DDL, `SPRIG SET`)
//! reaches this one within one load.
//!
//! **The short path.** Between full checks a statement keeps the session's
//! snapshot after one atomic read that says no load of the tenant finished
//! ([`sprig_catalog::cache::CatalogFollow`]), as long as the session's own
//! last change is in it and the last full check is younger than
//! [`FRESH_RECHECK`] (5 ms). A snapshot another session's change loaded is
//! seen at the next statement, as before; what only the full check looks
//! at (the gateway's lease, a newer version the directory announced but
//! nobody loaded yet, the accounts' reconciliation, the shard maps) is
//! seen at most 5 ms later than before.
//!
//! **Live settings.** A resolved-setting watch
//! ([`Settings::watch_resolved`]) follows `gateway.long_data_max_bytes`
//! for the tenant: a `SPRIG SET` at tenant scope (or a wider one) applies
//! to running sessions as soon as the cache loads it. Statement-level keys
//! (`sql.local_infile`) are resolved per statement against the snapshot.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_auth::{Principal, TenantAuth};
use sprig_catalog::ddl::LOWER_CASE_TABLE_NAMES;
use sprig_catalog::{Freshness, TenantCatalog, TenantPin};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigError};
use sprig_settings::{OwnedCtx, Settings};
use sprig_sql::names::{MemoryNameMap, NameMap, Purpose, SystemSchema, Visibility};
use tokio::task::JoinHandle;

use super::TenantServices;
use super::names::name_map;
use crate::exec::settings::LONG_DATA_MAX_BYTES;

/// A warning or note the gateway raised itself (a DDL note, a clamp
/// warning of `SPRIG SET`), answered by the next `SHOW WARNINGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWarning {
    /// `Note` or `Warning`.
    pub level: &'static str,
    /// The MySQL code.
    pub code: u16,
    /// The message, with logical names.
    pub message: String,
}

/// The per-session half of tenant sessions.
pub struct TenantSession {
    svc: Arc<TenantServices>,
    /// Who logged in.
    pub principal: Principal,
    /// The tenant.
    pub tenant: TenantId,
    _pin: TenantPin,
    cat: Option<Arc<TenantCatalog>>,
    map: Arc<MemoryNameMap>,
    /// The catalog version of the session's last own change.
    pub last_change: u64,
    /// The session's temporary tables: (logical database, lower-case name).
    pub temp: BTreeSet<(Option<String>, String)>,
    /// What the gateway raised for the last statement, if it answered it.
    pub warnings: Option<Vec<LocalWarning>>,
    /// The current logical database before the backend session exists.
    pub current: Option<String>,
    /// `SET @sprig_show_auto_indexes = 1`: automatic indexes shown in
    /// `SHOW INDEX`, `SHOW CREATE TABLE` and `STATISTICS`, and droppable.
    /// Kept by the gateway, never sent to MariaDB.
    pub show_auto_indexes: bool,
    /// Active roles and the compiled privileges.
    pub authz: super::authz::SessionAuthz,
    /// The type layer's session state.
    pub types: crate::exec::types::TypesSession,
    /// The session's space.
    pub inherit: crate::exec::inherit::InheritSession,
    /// The record-hierarchy locks its transaction holds.
    pub hier_locks: crate::exec::inherit::HierLocks,
    /// Row-level security (claims, the covered tables).
    pub rls: super::rls::RlsSession,
    long_data_max: Arc<AtomicU64>,
    watch: Option<JoinHandle<()>>,
    /// What the backend accounts were last found reconciled for, and when
    /// (`refresh`).
    ensured: Option<(EnsureKey, std::time::Instant)>,
    /// Whether the tenant's cached catalog changed since the last full
    /// check, and when that check ran (`refresh`'s short path).
    follow: Option<sprig_catalog::cache::CatalogFollow>,
    checked_at: Option<std::time::Instant>,
    /// `lower_case_table_names` with the catalog and settings snapshots it
    /// was resolved against.
    lctn_memo: std::sync::Mutex<Option<(Arc<TenantCatalog>, sprig_settings::Snapshot, bool)>>,
    /// `aliases` once the provisioning account's name is known.
    aliases_memo:
        std::sync::OnceLock<(sprig_common::ids::ClusterId, super::aliases::AccountAliases)>,
}

/// How long a session trusts its last check that the tenant's backend
/// accounts are reconciled, while nothing it can see changed.
const ENSURE_AGAIN: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a session keeps its catalog snapshot without the full check
/// while the cache reports no new load of its tenant (`refresh`): the
/// lease, the directory's newer versions, the accounts' reconciliation and
/// the shard maps are looked at again at least this often.
pub const FRESH_RECHECK: std::time::Duration = std::time::Duration::from_millis(5);

/// Whether a session may keep the snapshot of `version` for its next
/// statement without the full check: nothing new was loaded for its tenant
/// (`changed` false), its own last change (`last_change`) is in the
/// snapshot, the last full check reconciled the accounts (`ensured`) and
/// is younger than [`FRESH_RECHECK`] (`age`).
#[must_use]
pub fn keeps_snapshot(
    version: u64,
    last_change: u64,
    changed: bool,
    ensured: bool,
    age: Option<std::time::Duration>,
) -> bool {
    !changed && ensured && version >= last_change && age.is_some_and(|a| a < FRESH_RECHECK)
}

/// What a check of the backend accounts depends on that a session can see:
/// the catalog snapshot, the etcd settings and the tenant's shard maps.
struct EnsureKey {
    catalog: Arc<TenantCatalog>,
    etcd: sprig_settings::Snapshot,
    maps: (sprig_common::ids::DcId, u64),
}

impl std::fmt::Debug for TenantSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantSession")
            .field("tenant", &self.tenant)
            .field("user", &self.principal.user)
            .field("version", &self.cat.as_ref().map(|c| c.version))
            .field("last_change", &self.last_change)
            .finish_non_exhaustive()
    }
}

impl Drop for TenantSession {
    fn drop(&mut self) {
        if let Some(w) = self.watch.take() {
            w.abort();
        }
    }
}

impl TenantSession {
    /// The session of `principal`, which must be a catalog login.
    ///
    /// # Errors
    /// `Internal` for a principal without a tenant (an accounts-file login).
    pub fn new(svc: Arc<TenantServices>, principal: Principal) -> Result<Self> {
        let tenant = principal
            .tenant
            .ok_or_else(|| SprigError::internal("a tenant session without a tenant"))?;
        let pin = svc.cache.pin(tenant);
        Ok(Self {
            map: Arc::new(MemoryNameMap::new(tenant)),
            svc,
            principal,
            tenant,
            _pin: pin,
            cat: None,
            last_change: 0,
            temp: BTreeSet::new(),
            warnings: None,
            current: None,
            show_auto_indexes: false,
            authz: super::authz::SessionAuthz::default(),
            types: crate::exec::types::TypesSession::default(),
            inherit: crate::exec::inherit::InheritSession::default(),
            hier_locks: crate::exec::inherit::HierLocks::default(),
            rls: super::rls::RlsSession::default(),
            long_data_max: Arc::new(AtomicU64::new(LONG_DATA_MAX_BYTES.default)),
            watch: None,
            ensured: None,
            follow: None,
            checked_at: None,
            lctn_memo: std::sync::Mutex::new(None),
            aliases_memo: std::sync::OnceLock::new(),
        })
    }

    /// The services.
    #[must_use]
    pub fn services(&self) -> &Arc<TenantServices> {
        &self.svc
    }

    /// The snapshot statements read now: `Latest` the first time, then
    /// `Cached { min_version: last_change }`.
    ///
    /// # Errors
    /// The cache's (39070 catalog unavailable, 39072, 39073).
    pub async fn refresh(&mut self) -> Result<Arc<TenantCatalog>> {
        // The short path: every statement asks, and nothing changed.
        if let (Some(cat), Some(f)) = (&self.cat, &self.follow) {
            let age = self
                .checked_at
                .map(|t| sprig_common::time::poll::now().saturating_duration_since(t));
            if keeps_snapshot(
                cat.version,
                self.last_change,
                f.changed(),
                self.ensured.is_some(),
                age,
            ) {
                return Ok(Arc::clone(cat));
            }
        }
        // Loads finished from here on count as changes at the next
        // statement.
        let mut follow = self
            .follow
            .take()
            .or_else(|| self.svc.cache.follow(self.tenant));
        if let Some(f) = follow.as_mut() {
            f.mark_seen();
        }
        let r = self.refresh_full().await;
        if r.is_ok() {
            self.follow = follow.or_else(|| self.svc.cache.follow(self.tenant));
            self.checked_at = Some(sprig_common::time::poll::now());
        }
        r
    }

    /// The full check behind [`Self::refresh`].
    async fn refresh_full(&mut self) -> Result<Arc<TenantCatalog>> {
        let fresh = match &self.cat {
            None => Freshness::Latest,
            Some(_) => Freshness::Cached {
                min_version: self.last_change,
            },
        };
        if self.cat.is_none() {
            // Learn the provisioning account's name, which output hides.
            let _ = self
                .svc
                .locator
                .ctl_pool(&self.principal.backend.cluster)
                .await;
        }
        // This gateway's acknowledgement lease lapsed (it was paused, or
        // fenced by a lookup job): a lookup state change may have gone by
        // without it, so the tenant's catalog comes from the writer first.
        if let Some(lapse) = crate::mongo::publish::stale(self.tenant) {
            self.reload_after_lapse(lapse).await;
        }
        let cat = self.svc.cache.get(self.tenant, fresh).await?;
        self.install(&cat);
        // The tenant's per-user backend accounts follow the
        // snapshot before this session uses them. Checked again when the
        // snapshot, the settings or the tenant's maps changed, and at least
        // every `ENSURE_AGAIN` (where the accounts log in from moves with
        // the relays, which none of those show).
        let maps = self.svc.locator.maps_generation(self.tenant);
        let live = self.svc.settings.live();
        if self.ensured.as_ref().is_some_and(|(k, at)| {
            Arc::ptr_eq(&k.catalog, &cat)
                && k.maps == maps
                && live.with_snapshot(|e| Arc::ptr_eq(&k.etcd, e))
                && sprig_common::time::poll::now().saturating_duration_since(*at) < ENSURE_AGAIN
        }) {
            return Ok(cat);
        }
        let key = EnsureKey {
            catalog: Arc::clone(&cat),
            etcd: live.snapshot(),
            maps,
        };
        let r = self
            .svc
            .authz
            .ensure(&self.svc, &cat, &self.principal.backend.cluster)
            .await;
        self.ensured = r.is_ok().then(|| (key, std::time::Instant::now()));
        if let Err(e) = r {
            // A tenant being deleted: every statement says so.
            if e.code() == Some(sprig_common::SprigCode::TenantNotReady) {
                tracing::debug!(tenant = %self.tenant, error = %e, "backend grants not reconciled");
            } else {
                tracing::warn!(tenant = %self.tenant, error = %e, "backend grants not reconciled");
            }
        }
        Ok(cat)
    }

    /// A writer load of the tenant's catalog after the gateway's lease
    /// lapsed (`mongo::publish::stale`); a load another session asked for
    /// within the last second counts once it is done.
    async fn reload_after_lapse(&mut self, lapse: u64) {
        for _ in 0..4 {
            match self.svc.cache.reload_after_miss(self.tenant).await {
                Ok(Some(_)) => {
                    crate::mongo::publish::reloaded(self.tenant, lapse);
                    tracing::info!(tenant = %self.tenant, lapse, "catalog reloaded after the gateway's lease lapsed");
                    return;
                }
                Ok(None) => {
                    if crate::mongo::publish::stale(self.tenant).is_none() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                }
                Err(e) => {
                    tracing::warn!(tenant = %self.tenant, error = %e, "catalog reload after a lapsed lease failed");
                    return;
                }
            }
        }
    }

    /// After a name miss: one writer load (at most once a second per
    /// tenant). `true` if a newer snapshot arrived, so the statement is
    /// worth planning again.
    ///
    /// # Errors
    /// The cache's.
    pub async fn reload_after_miss(&mut self) -> Result<bool> {
        let before = self.cat.as_ref().map_or(0, |c| c.version);
        match self.svc.cache.reload_after_miss(self.tenant).await? {
            Some(cat) if cat.version > before => {
                self.install(&cat);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn install(&mut self, cat: &Arc<TenantCatalog>) {
        let same = self.cat.as_ref().is_some_and(|c| c.version == cat.version);
        if !same {
            self.map = Arc::new(name_map(cat));
        }
        // An auth change moves only the auth version.
        if !same
            || self
                .cat
                .as_ref()
                .is_some_and(|c| c.auth_version != cat.auth_version)
        {
            self.cat = Some(Arc::clone(cat));
        }
        if self.watch.is_none() {
            self.start_watch(cat);
        }
    }

    fn start_watch(&mut self, cat: &Arc<TenantCatalog>) {
        let settings: Settings = self.svc.settings.clone();
        let ctx = OwnedCtx::from_ctx(&cat.setting_ctx(&self.svc.dc));
        let w = settings.watch_resolved(
            LONG_DATA_MAX_BYTES.key,
            ctx,
            Some(Arc::clone(&cat.settings)),
        );
        let mut w = match w {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, error = %e, "no live watch of gateway.long_data_max_bytes");
                return;
            }
        };
        let cell = Arc::clone(&self.long_data_max);
        let value = |r: &sprig_settings::Resolved| {
            <u64 as sprig_settings::SettingValue>::from_value(&r.value)
                .unwrap_or(LONG_DATA_MAX_BYTES.default)
        };
        cell.store(value(w.current()), Ordering::Relaxed);
        self.watch = Some(tokio::spawn(async move {
            while let Ok(r) = w.changed().await {
                let v = value(&r);
                tracing::debug!(
                    key = LONG_DATA_MAX_BYTES.key,
                    value = v,
                    "live setting applied to a session"
                );
                cell.store(v, Ordering::Relaxed);
            }
        }));
    }

    /// The snapshot last installed (after [`Self::refresh`]).
    #[must_use]
    pub fn catalog(&self) -> Option<&Arc<TenantCatalog>> {
        self.cat.as_ref()
    }

    /// The tenant's names at the installed snapshot.
    #[must_use]
    pub fn map(&self) -> Arc<MemoryNameMap> {
        Arc::clone(&self.map)
    }

    /// The schema a logical database's session statements run in (a
    /// system schema keeps its name); `None` for an unknown name.
    #[must_use]
    pub fn schema_of(&self, logical: Option<&str>) -> Option<String> {
        let cur = logical?;
        if let Some(s) = SystemSchema::parse(cur) {
            return Some(s.as_str().to_owned());
        }
        self.map
            .physical(cur, Purpose::Session)
            .ok()
            .and_then(|s| s.single())
            .map(|s| s.to_string())
    }

    /// [`Self::aliases`] without a copy once the provisioning account's
    /// name is known (it doesn't change): every execute asks.
    #[must_use]
    pub fn aliases_cached(&self) -> std::borrow::Cow<'_, super::aliases::AccountAliases> {
        let cluster = &self.principal.backend.cluster;
        if let Some((c, a)) = self.aliases_memo.get() {
            if c == cluster {
                return std::borrow::Cow::Borrowed(a);
            }
            // The session follows its tenant to another cluster.
            return std::borrow::Cow::Owned(self.aliases());
        }
        let a = self.aliases();
        if self.svc.locator.ctl_user(cluster).is_some() {
            let (_, a) = self.aliases_memo.get_or_init(|| (cluster.clone(), a));
            return std::borrow::Cow::Borrowed(a);
        }
        std::borrow::Cow::Owned(a)
    }

    /// Backend accounts this session's output must not show (see
    /// [`super::aliases`]).
    #[must_use]
    pub fn aliases(&self) -> super::aliases::AccountAliases {
        let mut backend = vec![self.principal.backend.user.clone()];
        if let Some(u) = self.svc.locator.ctl_user(&self.principal.backend.cluster)
            && !backend.contains(&u)
        {
            backend.push(u);
        }
        super::aliases::AccountAliases {
            backend,
            user: self.principal.user.clone(),
        }
    }

    /// The snapshot's auth module (account names in output).
    #[must_use]
    pub fn auth(&self) -> Option<&TenantAuth> {
        self.cat.as_deref().and_then(TenantAuth::of)
    }

    /// `names.lower_case_table_names` for the tenant.
    #[must_use]
    pub fn lower_case_table_names(&self) -> bool {
        let Some(cat) = &self.cat else { return false };
        // Asked several times per statement: resolved again only for
        // another catalog snapshot or settings snapshot.
        self.svc.settings.live().with_snapshot(|etcd| {
            let mut memo = self
                .lctn_memo
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((c, e, v)) = memo.as_ref()
                && Arc::ptr_eq(c, cat)
                && Arc::ptr_eq(e, etcd)
            {
                return *v;
            }
            let v = self.svc.settings.get(
                &LOWER_CASE_TABLE_NAMES,
                &cat.setting_ctx(&self.svc.dc),
                Some(&cat.settings),
            ) == 1;
            *memo = Some((Arc::clone(cat), Arc::clone(etcd), v));
            v
        })
    }

    /// `catalog.ddl_wait_ms` for the tenant: how long its DDL waits for a
    /// target another DDL holds. `None` outside a tenant's catalog.
    #[must_use]
    pub fn ddl_wait(&self) -> Option<std::time::Duration> {
        let cat = self.cat.as_ref()?;
        Some(std::time::Duration::from_millis(self.svc.settings.get(
            &sprig_catalog::ddl::DDL_WAIT_MS,
            &cat.setting_ctx(&self.svc.dc),
            Some(&cat.settings),
        )))
    }

    /// `gateway.long_data_max_bytes` as the live watch last saw it.
    #[must_use]
    pub fn long_data_max(&self) -> usize {
        usize::try_from(self.long_data_max.load(Ordering::Relaxed)).unwrap_or(usize::MAX)
    }

    /// Records a catalog change of this session: its next statements read
    /// at least this version.
    pub fn changed(&mut self, version: Option<u64>) {
        if let Some(v) = version {
            self.last_change = self.last_change.max(v);
        }
    }

    /// Whether `name` (in logical database `db`) is one of the session's
    /// temporary tables.
    #[must_use]
    pub fn is_temp(&self, db: Option<&str>, name: &str) -> bool {
        self.temp
            .contains(&(db.map(str::to_owned), name.to_ascii_lowercase()))
    }

    /// `COM_RESET_CONNECTION` or `COM_CHANGE_USER`: temporary tables and
    /// gateway warnings go.
    pub fn reset(&mut self) {
        self.temp.clear();
        self.warnings = None;
        self.show_auto_indexes = false;
        self.inherit = crate::exec::inherit::InheritSession::default();
        // The backend's transaction ended with the reset.
        self.hier_locks = crate::exec::inherit::HierLocks::default();
    }

    /// What the session may see of internal objects: the persona's
    /// defaults and `@sprig_show_auto_indexes`.
    #[must_use]
    pub fn visibility(&self, persona: sprig_proto_mysql::packets::Persona) -> Visibility {
        Visibility {
            auto_indexes: self.show_auto_indexes,
            ..Visibility::for_persona(persona)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{FRESH_RECHECK, keeps_snapshot};

    #[test]
    fn a_snapshot_is_kept_only_while_nothing_it_depends_on_moved() {
        let young = Some(Duration::from_millis(1));
        assert!(keeps_snapshot(10, 10, false, true, young));
        // A finished load of the tenant (another session's DDL, a
        // directory move, an eviction): the full check.
        assert!(!keeps_snapshot(10, 10, true, true, young));
        // The session's own change isn't in the snapshot yet: the full
        // check waits for it.
        assert!(!keeps_snapshot(10, 11, false, true, young));
        // The accounts weren't found reconciled: checked every time.
        assert!(!keeps_snapshot(10, 10, false, false, young));
        // At most every FRESH_RECHECK, the full check whatever else.
        assert!(!keeps_snapshot(10, 10, false, true, Some(FRESH_RECHECK)));
        assert!(!keeps_snapshot(10, 10, false, true, None));
    }
}
