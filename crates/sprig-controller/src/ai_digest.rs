//! Tenant digests on the DC leader (`ai.digest`).
//!
//! ```text
//! every CHECK_INTERVAL, per active tenant homed here:
//!   ai.enabled off, or ai.digest = off -> nothing
//!   the last whole day (daily) or week (weekly, Monday to Monday), UTC
//!   a digest for it exists -> nothing
//!   facts from the tenant's catalog (schema versions, the workload store,
//!     automatic indexes, the approvals inbox) and table sizes from every
//!     cluster that may hold the tenant's schemas (information_schema.TABLES)
//!   sprig_ai::digest::build: values stripped unless ai.share_data, worded by
//!     ai.model.data (else ai.model.dba) within the tenant's budgets, stored in
//!     ai_digests with its conversation; facts alone when it can't be worded
//!   ai.digest_email -> emailed to the tenant's approvals subscribers
//!   one ai.digest audit event on the tenant's stream
//! ```
//!
//! Only names, counts and sizes are read; no table row is.

use sprig_catalog::pool_wait::Bounded as _;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use serde_json::json;
use sprig_ai::Role;
use sprig_ai::audit::AuditStore;
use sprig_ai::digest::{self, Cadence, CatalogDigests, DigestStore, Facts, Volume};
use sprig_ai::settings::{Lookup, TurnSettings};
use sprig_catalog::CatalogLocator;
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_common::audit::AuditStream;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_settings::{LiveSettings, MemSettings, Registry, Setting, Value as SettingValue};

use crate::ai_incidents::IncidentEnv;
use crate::approvals::notify::Notifier;
use crate::catalogs::{ClusterPools, RegistryPools};
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, DirectoryEntry, TenantState};

/// How often each tenant is looked at.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(3600);

/// The digest loop (module docs).
pub struct DigestLoop {
    dir: Directory,
    locator: Arc<dyn CatalogLocator>,
    dc: DcId,
    live: Option<LiveSettings>,
    env: Arc<dyn IncidentEnv>,
    pools: RegistryPools,
    notifier: Option<Arc<Notifier>>,
}

impl std::fmt::Debug for DigestLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DigestLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// A tenant's settings, resolved from its catalog and etcd.
struct TenantLookup<'a> {
    cat: TenantCatalog,
    etcd: &'a MemSettings<'static>,
    dc: &'a DcId,
}

impl TenantLookup<'_> {
    fn value(&self, key: &str) -> Option<SettingValue> {
        self.cat
            .settings
            .resolve(self.etcd, key, &self.cat.setting_ctx(self.dc))
            .ok()
            .map(|r| r.value)
    }
}

impl Lookup for TenantLookup<'_> {
    fn flag(&self, s: &'static Setting<bool>) -> bool {
        match self.value(s.key) {
            Some(SettingValue::Bool(b)) => b,
            _ => s.default,
        }
    }
    fn number(&self, s: &'static Setting<u64>) -> u64 {
        match self.value(s.key) {
            Some(SettingValue::Int(n)) => u64::try_from(n).unwrap_or(s.default),
            _ => s.default,
        }
    }
    fn text(&self, s: &'static Setting<std::borrow::Cow<'static, str>>) -> String {
        match self.value(s.key) {
            Some(SettingValue::Str(t)) => t,
            _ => s.default.to_string(),
        }
    }
}

fn unavailable(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        "the tenant's catalog is unreachable",
    )
    .with_source(e)
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(0)
}

impl DigestLoop {
    /// The loop over the tenants homed in `dc`; `env` opens the models,
    /// `pools` reaches the clusters for table sizes, `notifier` emails.
    #[must_use]
    pub fn new(
        dir: Directory,
        locator: Arc<dyn CatalogLocator>,
        dc: DcId,
        live: Option<LiveSettings>,
        env: Arc<dyn IncidentEnv>,
        pools: RegistryPools,
        notifier: Option<Arc<Notifier>>,
    ) -> Self {
        Self {
            dir,
            locator,
            dc,
            live,
            env,
            pools,
            notifier,
        }
    }

    fn ours(&self, e: &DirectoryEntry) -> bool {
        e.home_dc == self.dc && e.state == TenantState::Active
    }

    fn registry(&self) -> Result<&'static Registry> {
        match &self.live {
            Some(l) => Ok(l.registry()),
            None => Registry::global(),
        }
    }

    /// Approximate rows and bytes of every table of `tenant`, summed over
    /// its shards, from every cluster that may hold them.
    async fn volumes(&self, tenant: TenantId, dbs: &BTreeMap<u32, String>) -> Result<Vec<Volume>> {
        let mut sum: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
        let pattern = format!("t{}\\_d%", tenant.0);
        for cluster in self.pools.sweepable().await? {
            let pool = match self.pools.writer(&cluster).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(error = %e, %cluster, "a digest skipped a cluster");
                    continue;
                }
            };
            let mut c = pool.get_conn_bounded().await.map_err(unavailable)?;
            let rows: Vec<(String, String, Option<u64>, Option<u64>)> = c
                .exec(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_ROWS, DATA_LENGTH + INDEX_LENGTH \
                     FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE ? \
                     AND TABLE_TYPE = 'BASE TABLE'",
                    (pattern.as_str(),),
                )
                .await
                .map_err(unavailable)?;
            for (schema, table, n, bytes) in rows {
                let Ok(SchemaName::Shard { db, .. } | SchemaName::Reference { db, .. }) =
                    schema.parse::<SchemaName>()
                else {
                    continue;
                };
                let Some(name) = dbs.get(&db.0) else {
                    continue;
                };
                // SprigDB's own tables in a shard schema aren't the tenant's.
                if table.starts_with("__sprig") || table.starts_with("sprig_") {
                    continue;
                }
                let e = sum.entry((name.clone(), table)).or_default();
                e.0 += n.unwrap_or(0);
                e.1 += bytes.unwrap_or(0);
            }
        }
        Ok(sum
            .into_iter()
            .map(|((db, table), (rows, bytes))| Volume {
                db,
                table,
                rows,
                bytes,
                rows_before: None,
                bytes_before: None,
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl Reconciler for DigestLoop {
    type Key = TenantId;

    fn name(&self) -> &'static str {
        "ai_digest"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        vec![WatchSpec::global("tenants/")]
    }

    fn keys_for(&self, ev: &MetaEvent) -> Vec<TenantId> {
        self.dir.tenant_of(&ev.key).into_iter().collect()
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<TenantId>> {
        Ok(self
            .dir
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| self.ours(e))
            .map(|e| e.tenant_id)
            .collect())
    }

    #[allow(clippy::too_many_lines)] // the steps in the module docs' order
    async fn reconcile(&self, _cx: &LoopCx, key: &TenantId) -> Result<Outcome> {
        let Some(entry) = self.dir.get(*key).await? else {
            return Ok(Outcome::Done);
        };
        if !self.ours(entry.body()) {
            return Ok(Outcome::Done);
        }
        let ep = self.locator.writer(*key).await?;
        let schema = ep.store.schema().to_owned();
        // The catalog's core rows (settings among them) are engine records.
        let snap = sprig_catalog::store::rec::CoreReader::new(crate::catalog_records(), *key)
            .snapshot()
            .await?;
        let reg = self.registry()?;
        let empty = MemSettings::new(reg);
        let held = self.live.as_ref().map(LiveSettings::snapshot);
        let etcd = held.as_deref().unwrap_or(&empty);
        let look = TenantLookup {
            cat: TenantCatalog::build(reg, *key, Loaded::core(snap)),
            etcd,
            dc: &self.dc,
        };
        // Off by default: both the assistants and the digest must be on.
        let Some(cadence) = Cadence::parse(&look.text(&sprig_ai::settings::DIGEST)) else {
            return Ok(Outcome::RequeueAfter(CHECK_INTERVAL));
        };
        if !look.flag(&sprig_ai::settings::ENABLED) {
            return Ok(Outcome::RequeueAfter(CHECK_INTERVAL));
        }
        let now = now_ms();
        let (from, to) = cadence.period(now);
        let store = CatalogDigests::new(ep.pool.clone(), schema.clone());
        if store.exists().await? && store.for_period(cadence, to).await?.is_some() {
            return Ok(Outcome::RequeueAfter(CHECK_INTERVAL));
        }
        let mut facts = Facts::new(cadence, from, to);
        {
            let mut c = ep.pool.get_conn_bounded().await.map_err(unavailable)?;
            digest::gather::catalog(&mut c, &schema, &mut facts, now).await?;
            let dbs = digest::gather::databases(&mut c, &schema).await?;
            drop(c);
            match self.volumes(*key, &dbs).await {
                Ok(v) => facts.volumes = v,
                Err(e) => facts.missing.push(format!("table sizes ({})", e.message)),
            }
        }
        let settings = TurnSettings::read(&look, Role::Data);
        // The data assistant's model, else the DBA's.
        let model = if settings.model.trim().is_empty() {
            let dba = look.text(&sprig_ai::settings::MODEL_DBA);
            self.env.model(Role::Dba, &dba).await
        } else {
            self.env.model(Role::Data, &settings.model).await
        };
        let audit: Arc<dyn AuditStore> =
            Arc::new(sprig_ai::CatalogAudit::new(ep.pool.clone(), schema.clone()));
        let (d, new) = match digest::build(&store, audit, model, &settings, *key, facts).await {
            Ok(r) => r,
            Err(e) => {
                sprig_ai::metrics::digest("error");
                return Err(e);
            }
        };
        if new {
            let mut emailed = "off";
            if look.flag(&sprig_ai::settings::DIGEST_EMAIL) {
                emailed = match &self.notifier {
                    Some(n) => {
                        let subject = format!(
                            "[SprigDB] {} digest, {} to {}",
                            if cadence == Cadence::Weekly {
                                "Weekly"
                            } else {
                                "Daily"
                            },
                            digest::date(d.from_ms),
                            digest::date(d.to_ms - 1)
                        );
                        let id = format!("ai-digest.{}.{}@sprigdb", key.0, d.id);
                        n.ai_digest(*key, &subject, &d.body(), &id).await
                    }
                    None => "skipped",
                };
                sprig_ai::metrics::digest_email(emailed);
                if emailed == "sent"
                    && let Err(e) = store.emailed(d.id).await
                {
                    tracing::debug!(error = %e, "a digest's email time wasn't kept");
                }
            }
            sprig_common::audit::record(
                AuditStream::Tenant(*key),
                "ai.digest",
                json!({
                    "digest": d.id,
                    "cadence": cadence.as_str(),
                    "status": d.status.as_str(),
                    "conversation": d.conversation,
                    "email": emailed,
                }),
            );
            tracing::info!(tenant = %key, digest = d.id, status = d.status.as_str(), "AI digest built");
        }
        Ok(Outcome::RequeueAfter(CHECK_INTERVAL))
    }
}
