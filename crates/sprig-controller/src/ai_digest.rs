//! Tenant digests on the DC leader (`ai.digest`).
//!
//! ```text
//! every CHECK_INTERVAL, per active tenant homed here:
//!   ai.enabled off, or ai.digest = off -> nothing
//!   the last whole day (daily) or week (weekly, Monday to Monday), UTC
//!   a digest for it exists -> nothing
//!   facts from the tenant's catalog records (schema versions, the workload
//!     store, the approvals inbox); table sizes aren't read (noted as missing)
//!   sprig_ai::digest::build: values stripped unless ai.share_data, worded by
//!     ai.model.data (else ai.model.dba) within the tenant's budgets, stored in
//!     the catalog records with its conversation; facts alone when it can't be worded
//!   ai.digest_email -> emailed to the tenant's approvals subscribers
//!   one ai.digest audit event on the tenant's stream
//! ```
//!
//! Only names, counts and sizes are read; no table row is.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use sprig_ai::Role;
use sprig_ai::audit::AuditStore;
use sprig_ai::audit::rec::RecAudit;
use sprig_ai::digest::{
    self, Cadence, DigestStore, Facts, RecDigests, SchemaChange, Statement, Waiting, Workload,
};
use sprig_ai::settings::{Lookup, TurnSettings};
use sprig_catalog::RecordStore;
use sprig_catalog::cache::{Loaded, TenantCatalog};
use sprig_catalog::store::DbState;
use sprig_catalog::store::rec::CoreRecords;
use sprig_common::Result;
use sprig_common::audit::AuditStream;
use sprig_common::ids::{DcId, TenantId};
use sprig_settings::{LiveSettings, MemSettings, Registry, Setting, Value as SettingValue};

use crate::ai_incidents::IncidentEnv;
use crate::approvals::notify::Notifier;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, DirectoryEntry, TenantState};

/// How often each tenant is looked at.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(3600);

/// The digest loop (module docs).
pub struct DigestLoop {
    dir: Directory,
    records: Arc<dyn RecordStore>,
    dc: DcId,
    live: Option<LiveSettings>,
    env: Arc<dyn IncidentEnv>,
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
    /// The loop over the tenants homed in `dc`, whose catalog records are
    /// `records`; `env` opens the models, `notifier` emails.
    #[must_use]
    pub fn new(
        dir: Directory,
        records: Arc<dyn RecordStore>,
        dc: DcId,
        live: Option<LiveSettings>,
        env: Arc<dyn IncidentEnv>,
        notifier: Option<Arc<Notifier>>,
    ) -> Self {
        Self {
            dir,
            records,
            dc,
            live,
            env,
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
}

/// `ms` (since the epoch) as a time.
fn time_of(ms: i64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + Duration::from_millis(u64::try_from(ms.max(0)).unwrap_or(0))
}

fn ms_of(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Fills `facts` from `tenant`'s catalog records: schema changes in the
/// period (the object and the statement's verb, never its text), the
/// workload store's totals and slowest statements, and the
/// recommendations waiting (automatic index proposals among them). Only names, counts and
/// times are read. Table sizes aren't: they are noted as missing.
///
/// # Errors
/// The records' (39070 when the engine node can't be reached).
#[allow(clippy::cast_precision_loss)] // counters far below 2^52
#[allow(clippy::too_many_lines)] // one step per part of the facts
pub async fn gather(
    records: &dyn RecordStore,
    tenant: TenantId,
    dbs: &BTreeMap<u32, String>,
    objects: &BTreeMap<u64, (u32, String, String)>,
    facts: &mut Facts,
    now_ms: i64,
) -> Result<()> {
    use sprig_workload::store::WorkloadTables;
    let mut tx = records.begin(tenant).await?;
    // Schema changes in the period.
    let mut changes = Vec::new();
    for (id, (db, name, kind)) in objects {
        let Some(db) = dbs.get(db) else { continue };
        for v in CoreRecords::schema_versions(&mut *tx, sprig_common::ids::ObjectId(*id)).await? {
            let at = ms_of(v.applied_at);
            if at >= facts.from_ms && at < facts.to_ms {
                changes.push(SchemaChange {
                    db: db.clone(),
                    object: name.clone(),
                    kind: kind.clone(),
                    verb: v
                        .ddl
                        .split_whitespace()
                        .next()
                        .unwrap_or("CHANGE")
                        .to_ascii_uppercase(),
                    at_ms: at,
                });
            }
        }
    }
    changes.sort_by_key(|c| c.at_ms);
    facts.schema_change_count = changes.len() as u64;
    changes.truncate(digest::MAX_CHANGES);
    facts.schema_changes = changes;
    // The workload store: totals for the period and the one before, and
    // the statements that took the most time.
    let len = facts.to_ms - facts.from_ms;
    let now = WorkloadTables::window(
        &mut *tx,
        tenant,
        time_of(facts.from_ms),
        time_of(facts.to_ms),
    )
    .await?;
    let before = WorkloadTables::window(
        &mut *tx,
        tenant,
        time_of(facts.from_ms - len),
        time_of(facts.from_ms),
    )
    .await?;
    let sum = |w: &[sprig_workload::window::FpWindowStats]| Workload {
        statements: w.iter().map(|s| s.stats.exec_count).sum(),
        errors: w.iter().map(|s| s.stats.error_count).sum(),
        total_ms: w.iter().map(|s| s.stats.total_us as f64).sum::<f64>() / 1000.0,
    };
    if now.is_empty() && before.is_empty() {
        facts
            .missing
            .push("the workload (no statements recorded yet)".to_owned());
    } else {
        facts.workload = sum(&now);
        facts.workload_before = Some(sum(&before)).filter(|w| w.statements > 0);
        let mut top: Vec<&sprig_workload::window::FpWindowStats> = now.iter().collect();
        top.sort_by_key(|s| std::cmp::Reverse(s.stats.total_us));
        for s in top.into_iter().take(digest::SLOWEST) {
            let text: String = s.text.split_whitespace().collect::<Vec<_>>().join(" ");
            facts.slowest.push(Statement {
                fingerprint: format!("{:016x}", s.id.0),
                db: s
                    .shape
                    .tables
                    .iter()
                    .find_map(|t| t.object_id)
                    .and_then(|o| objects.get(&o.0))
                    .and_then(|(d, _, _)| dbs.get(d).cloned()),
                text: text.chars().take(digest::MAX_STATEMENT).collect(),
                calls: s.stats.exec_count,
                errors: s.stats.error_count,
                total_ms: s.stats.total_us as f64 / 1000.0,
                max_ms: s.stats.max_us as f64 / 1000.0,
            });
        }
    }
    // Recommendations waiting for review: kind, target and age only.
    let recs = sprig_approvals::catalog::RecRecords::new(tenant)
        .list(&mut *tx, &sprig_approvals::Filter::default())
        .await?;
    let mut waiting: Vec<Waiting> = Vec::new();
    for r in &recs {
        let state = r.state.as_str();
        if matches!(state, "open" | "snoozed" | "approved") {
            waiting.push(Waiting {
                kind: r.kind.as_str().to_owned(),
                target: r.target.clone(),
                state: state.to_owned(),
                age_hours: ((now_ms * 1000 - r.created_at_us).max(0) as f64) / 3.6e9,
            });
        }
    }
    waiting.sort_by(|a, b| b.age_hours.total_cmp(&a.age_hours));
    facts.waiting_count = waiting.len() as u64;
    waiting.truncate(digest::MAX_WAITING);
    facts.waiting = waiting;
    drop(tx);
    facts
        .missing
        .push("table sizes (not read for engine tables yet)".to_owned());
    Ok(())
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
        // The catalog's core rows (settings among them) are engine records.
        let snap = sprig_catalog::store::rec::CoreReader::new(Arc::clone(&self.records), *key)
            .snapshot()
            .await?;
        let dbs: BTreeMap<u32, String> = snap
            .databases
            .iter()
            .filter(|d| d.state == DbState::Active)
            .map(|d| (d.db_id.0, d.name.clone()))
            .collect();
        let objects: BTreeMap<u64, (u32, String, String)> = snap
            .objects
            .iter()
            .map(|o| {
                (
                    o.object_id.0,
                    (o.db_id.0, o.name.clone(), o.kind.as_str().to_owned()),
                )
            })
            .collect();
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
        let store = RecDigests::new(Arc::clone(&self.records), *key);
        if store.for_period(cadence, to).await?.is_some() {
            return Ok(Outcome::RequeueAfter(CHECK_INTERVAL));
        }
        let mut facts = Facts::new(cadence, from, to);
        gather(&*self.records, *key, &dbs, &objects, &mut facts, now).await?;
        let settings = TurnSettings::read(&look, Role::Data);
        // The data assistant's model, else the DBA's.
        let model = if settings.model.trim().is_empty() {
            let dba = look.text(&sprig_ai::settings::MODEL_DBA);
            self.env.model(Role::Dba, &dba).await
        } else {
            self.env.model(Role::Data, &settings.model).await
        };
        let audit: Arc<dyn AuditStore> =
            Arc::new(RecAudit::tenant(Arc::clone(&self.records), *key));
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
