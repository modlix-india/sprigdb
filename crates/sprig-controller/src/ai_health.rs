//! The administration agent's health checks, metric history, incident
//! write-ups, upgrade readiness and decision log, on the DC leader, and
//! their Admin API routes.
//!
//! ```text
//! every SAMPLE_INTERVAL, while ai.admin.enabled (global):
//!   sample: shard sizes against their split size, cluster fullness,
//!     replica lag and health, gateway sessions, backup times, waiting
//!     data moves -> the metric history (ai/metrics/, flushed every 15 min)
//!   every ai.admin.check_interval: the rules over the history -> findings
//!     (ai/health/<key>); a finding whose item still waits -> the item is
//!     refreshed; a new one (or one quiet for a day, or worse) -> the admin
//!     model words it (search_docs, metrics_trend, decision_log) -> an
//!     ai_health_warning item; without a model, the rule's own words
//!     at most MAX_PER_PASS a pass and ai.admin.max_advice_hour an hour
//!   a cluster or DC failover, or a failed data move, 15 minutes old and
//!     less than a day -> its write-up (ai/writeups/<id>) -> an
//!     ai_incident_advice item
//! ```
//!
//! | Route | Does |
//! |---|---|
//! | `GET /admin/v1/ai/metrics?name=&label=k=v&days=` | A series of the metric history |
//! | `GET /admin/v1/ai/health` | Findings, newest first |
//! | `POST /admin/v1/ai/health:check` | Runs the rules now and returns what they find (nothing is filed) |
//! | `GET /admin/v1/ai/decisions?subject=&days=` | The DC's decision log: failovers, data moves, incidents, findings and write-ups |
//! | `GET /admin/v1/ai/writeups[/{id}]` | Write-ups |
//! | `POST /admin/v1/ai/writeups/{id}:draft` | Drafts (again) the write-up of an event id or incident fingerprint |
//! | `POST /admin/v1/ai:upgradeCheck` | `{to, catalog?, explain?}`: the upgrade readiness report |

use sprig_catalog::pool_wait::Bounded as _;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use axum::routing::{get, post};
use mysql_async::prelude::Queryable as _;
use serde::Deserialize;
use serde_json::{Value, json};
use sprig_ai::admin::history::{DAY_MS, HOUR_MS, SAMPLE_INTERVAL, time_text};
use sprig_ai::admin::upgrade::{self, Catalog, SetKey, SettingInfo, Usage};
use sprig_ai::admin::writeup::{self, Event, Evidence, Seen, WriteupRecord, WriteupStore};
use sprig_ai::admin::{
    self, Finding, FindingRecord, FindingStore, History, Limits, Sample, TrendQuery,
};
use sprig_ai::audit::AuditStore;
use sprig_ai::egress::NameKind;
use sprig_ai::incidents::advise::Model;
use sprig_ai::incidents::{Advisor, IncidentStore, Names, Writing, intake};
use sprig_ai::settings::{AdminSettings, TurnSettings};
use sprig_ai::tools::{Source, ToolBody, ToolHost, ToolOutput, ToolSpec};
use sprig_ai::{MemoryAudit, Role};
use sprig_approvals::Kind;
use sprig_approvals::gate::Approvals;
use sprig_approvals::model::{RecId, State as RecState};
use sprig_catalog::CatalogLocator;
use sprig_cluster::health::ClusterHealth;
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore};
use sprig_settings::{LiveSettings, ScopeKind, SettingCtx};

use crate::admin::http::respond;
use crate::admin::invalid;
use crate::ai_incidents::{Advisory, IncidentEnv, Knobs, MAX_ADVICE, raise_advisory};
use crate::catalogs::{ClusterPools, RegistryPools};
use crate::monitor::ClusterMonitor;
use crate::operators::http::principal;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::{Directory, TenantState};

/// Warnings and write-ups sent to the model in one pass at most.
pub const MAX_PER_PASS: usize = 3;
/// Decision entries one answer holds at most.
pub const MAX_ENTRIES: usize = 50;
/// Tenants whose workload an upgrade check reads at most.
pub const MAX_USAGE_TENANTS: usize = 200;
/// Words a decision-log subject may hold that say nothing about it.
const STOP_WORDS: &[&str] = &[
    "why",
    "did",
    "does",
    "the",
    "and",
    "for",
    "what",
    "when",
    "how",
    "was",
    "were",
    "recent",
    "all",
    "last",
    "this",
    "that",
    "sprigdb",
    "happen",
    "happened",
    "decide",
    "decided",
    "decision",
    "decisions",
    "log",
    "about",
    "explain",
    "yesterday",
    "today",
    "night",
];
/// A shard size older than this isn't sampled.
const SHARD_SIZE_FRESH_MS: u64 = 6 * HOUR_MS;
/// The loop's one key.
const KEY: &str = "health";

fn now_ms() -> u64 {
    intake::now_ms()
}

// ---- shard sizes, from the sharding loop ----

/// The largest shard of each table group against its split size, as the
/// sharding loop last measured them.
#[derive(Debug, Clone, Default)]
pub struct ShardSizes(Arc<Mutex<SizeMap>>);

/// `(tenant, group)` to `(ratio, measured at ms)`.
type SizeMap = BTreeMap<(u64, u32), (f64, u64)>;

impl ShardSizes {
    /// Records `tenant`'s group `group`: its largest shard and its split
    /// size, in bytes.
    pub fn record(&self, tenant: TenantId, group: u32, largest: f64, split: f64) {
        if split <= 0.0 || !largest.is_finite() {
            return;
        }
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((tenant.0, group), (largest / split, now_ms()));
    }

    /// The fresh ratios as samples.
    #[must_use]
    pub fn samples(&self, now_ms: u64) -> Vec<Sample> {
        let mut m = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        m.retain(|_, (_, at)| now_ms.saturating_sub(*at) < SHARD_SIZE_FRESH_MS);
        m.iter()
            .map(|((t, g), (r, _))| {
                Sample::new(
                    "sprig_shard_split_ratio",
                    &[("tenant", &format!("t{t}")), ("group", &g.to_string())],
                    *r,
                )
            })
            .collect()
    }
}

// ---- what the loop needs from the controller ----

/// One entry of the DC's decision log, as operators see it.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// Its id, to cite.
    pub id: String,
    /// When, ms (`None`: a standing state).
    pub at_ms: Option<u64>,
    /// `cluster.failover`, `dc.failover`, `move.<kind>`, `incident`,
    /// `health.<rule>`, `writeup`.
    pub kind: String,
    /// What it is about: ids only.
    pub subject: String,
    /// What was decided and why, with the numbers.
    pub summary: String,
}

impl Entry {
    fn json(&self) -> Value {
        json!({
            "id": self.id,
            "at": self.at_ms.map(time_text),
            "kind": self.kind,
            "subject": self.subject,
            "summary": self.summary,
        })
    }

    /// How many of `words` it mentions.
    fn matches(&self, words: &[String]) -> usize {
        let hay = format!(
            "{} {} {} {}",
            self.id, self.kind, self.subject, self.summary
        )
        .to_lowercase();
        words.iter().filter(|w| hay.contains(w.as_str())).count()
    }
}

/// The controller's side of health checks, write-ups and upgrade checks.
#[async_trait::async_trait]
pub trait HealthEnv: Send + Sync + 'static {
    /// One sample of every series.
    async fn sample(&self, now_ms: u64) -> Result<Vec<Sample>>;
    /// What the rules need besides the series.
    async fn limits(&self) -> Limits;
    /// Failovers and failed data moves at or after `since_ms`.
    async fn events(&self, since_ms: u64) -> Result<Vec<Event>>;
    /// Decision entries of failovers and data moves at or after `since_ms`.
    async fn entries(&self, since_ms: u64) -> Result<Vec<Entry>>;
    /// How this deployment is used.
    async fn usage(&self) -> Result<Usage>;
    /// The catalog of `version`'s docs pack, when one can be found.
    async fn target(&self, version: &str) -> Result<Option<Catalog>>;
}

/// This build's settings and error codes.
#[must_use]
pub fn current_catalog() -> Catalog {
    let mut c = Catalog {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        ..Catalog::default()
    };
    if let Ok(reg) = sprig_settings::Registry::global() {
        for s in sprig_docs::reference::settings_from_registry(reg) {
            c.settings.insert(
                s.key,
                SettingInfo {
                    kind: s.kind,
                    default: s.default,
                    scopes: s.scopes,
                    apply: s.apply,
                    doc: s.doc,
                },
            );
        }
    }
    for code in SprigCode::ALL {
        c.codes
            .insert(u32::from(code.number()), code.name().to_owned());
    }
    c
}

/// Unix ms of an RFC 3339 UTC time (`2026-11-14T09:02:00Z`, fractions
/// allowed).
#[must_use]
#[allow(clippy::many_single_char_names)] // the parts of a date
pub fn rfc3339_ms(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |a: usize, z: usize| s.get(a..z)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0, 4)?, n(5, 7)?, n(8, 10)?);
    let (h, mi, se) = (n(11, 13)?, n(14, 16)?, n(17, 19)?);
    let frac_ms = match s.get(19..20) {
        Some(".") => {
            let digits: String = s[20..].chars().take_while(char::is_ascii_digit).collect();
            let mut ms = digits.chars().take(3).collect::<String>();
            while ms.len() < 3 {
                ms.push('0');
            }
            ms.parse::<i64>().ok()?
        }
        _ => 0,
    };
    // Days from civil (Howard Hinnant's algorithm).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let ms = ((days * 24 + h) * 60 + mi) * 60_000 + se * 1000 + frac_ms;
    u64::try_from(ms).ok()
}

/// [`HealthEnv`] over the controller's stores and loops.
pub struct ControllerHealth {
    dc: DcId,
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    registry: ClusterRegistry,
    monitor: Arc<ClusterMonitor>,
    balancer: Arc<crate::balancer_loop::BalancerLoop>,
    sizes: ShardSizes,
    live: Option<(LiveSettings, DcId)>,
    pools: RegistryPools,
    clock: Arc<dyn sprig_common::time::Clock>,
}

impl std::fmt::Debug for ControllerHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerHealth")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// What [`ControllerHealth`] reads from.
pub struct HealthSources {
    /// This DC.
    pub dc: DcId,
    /// Its tier.
    pub dc_store: Arc<dyn MetaStore>,
    /// The global tier.
    pub global: Arc<dyn MetaStore>,
    /// Its clusters.
    pub registry: ClusterRegistry,
    /// Their probes.
    pub monitor: Arc<ClusterMonitor>,
    /// The balancer (cluster fullness).
    pub balancer: Arc<crate::balancer_loop::BalancerLoop>,
    /// Shard sizes from the sharding loop.
    pub sizes: ShardSizes,
    /// The live settings.
    pub live: Option<(LiveSettings, DcId)>,
    /// Tenant catalogs (unused since the workload store moved to catalog
    /// records; kept so callers build the sources as before).
    pub locator: Arc<dyn CatalogLocator>,
    /// Cluster connections (the docs index on the system cluster).
    pub pools: RegistryPools,
    /// The clock.
    pub clock: Arc<dyn sprig_common::time::Clock>,
}

impl ControllerHealth {
    /// The environment over `s`.
    #[must_use]
    pub fn new(s: HealthSources) -> Self {
        Self {
            dc: s.dc,
            dc_store: s.dc_store,
            global: s.global,
            registry: s.registry,
            monitor: s.monitor,
            balancer: s.balancer,
            sizes: s.sizes,
            live: s.live,
            pools: s.pools,
            clock: s.clock,
        }
    }

    async fn list_values(store: &dyn MetaStore, prefix: &str) -> Result<Vec<(String, Value)>> {
        let key = store.tier().key(prefix)?;
        let mut out = Vec::new();
        let mut opts = ListOpts::default();
        loop {
            let page = store.list(&key, opts.clone()).await?;
            for kv in &page.kvs {
                if let Ok(v) = serde_json::from_slice::<Value>(&kv.value) {
                    let id = kv.key.as_str().rsplit('/').next().unwrap_or("").to_owned();
                    out.push((id, v));
                }
            }
            if !page.more {
                return Ok(out);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }

    fn cluster_failovers(v: &Value) -> Option<crate::failover::cluster::ClusterFailoverRecord> {
        serde_json::from_value(v.clone())
            .ok()
            .or_else(|| serde_json::from_value(v.get("body")?.clone()).ok())
    }

    fn dc_failover(v: &Value) -> Option<crate::failover::record::FailoverRecord> {
        serde_json::from_value(v.clone())
            .ok()
            .or_else(|| serde_json::from_value(v.get("body")?.clone()).ok())
    }

    fn failover_event(c: &str, e: &crate::failover::cluster::FailoverEvent) -> Event {
        let at = e.at_unix_ms;
        let mut facts = vec![
            (
                Some(at.saturating_sub(e.rto_ms)),
                format!(
                    "the writer {} of cluster {c} stopped answering (sources: {})",
                    if e.from.is_empty() { "?" } else { &e.from },
                    if e.sources.is_empty() {
                        "probes".to_owned()
                    } else {
                        e.sources.join(", ")
                    }
                ),
            ),
            (
                Some(at.saturating_sub(e.rto_ms).saturating_add(e.detect_ms)),
                format!(
                    "SprigDB decided to fail over after {} ms of failed probes",
                    e.detect_ms
                ),
            ),
            (
                Some(at),
                format!(
                    "{} {} became the writer (writer epoch {}); recovery took {} ms from the first failed probe{}",
                    if e.kind == "galera_bootstrap" {
                        "bootstrapped node"
                    } else {
                        "promoted replica"
                    },
                    e.to,
                    e.writer_epoch,
                    e.rto_ms,
                    e.loss_estimate_ms
                        .map(|l| format!("; expected loss up to {l} ms of writes"))
                        .unwrap_or_default()
                ),
            ),
        ];
        if let Some(f) = &e.former {
            let fate = serde_json::to_value(f)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            let errant = e
                .errant
                .as_ref()
                .map(|r| format!(" with {} diverged transactions", r.count))
                .unwrap_or_default();
            facts.push((None, format!("the former writer is {fate}{errant}")));
        }
        if e.recovery_error.is_some() {
            facts.push((
                None,
                "its recovery job failed; an operator retries it with sprigctl cluster extract|rebuild"
                    .to_owned(),
            ));
        }
        Event {
            id: format!("cf-{}-{}", cluster_tag(c), e.writer_epoch),
            kind: "cluster_failover".to_owned(),
            at_ms: at,
            cluster: Some(c.to_owned()),
            facts,
        }
    }
}

/// Eight hex digits standing for cluster `c` in event ids, which reach
/// the provider: the id itself never names the cluster.
#[must_use]
pub fn cluster_tag(c: &str) -> String {
    sprig_ai::admin::history::series_id(c, &BTreeMap::new())[..8].to_owned()
}

fn health_value(h: ClusterHealth) -> f64 {
    match h {
        ClusterHealth::Healthy => 0.0,
        ClusterHealth::Degraded => 1.0,
        ClusterHealth::NoWriter => 2.0,
        ClusterHealth::Unknown => 3.0,
    }
}

#[async_trait::async_trait]
impl HealthEnv for ControllerHealth {
    #[allow(clippy::cast_precision_loss)]
    async fn sample(&self, now_ms: u64) -> Result<Vec<Sample>> {
        let mut out = self.sizes.samples(now_ms);
        for c in self.registry.list().await? {
            let r = c.body();
            if !matches!(
                r.admin_state,
                AdminState::Active | AdminState::Draining | AdminState::Drained
            ) {
                continue;
            }
            let id = r.cluster_id.to_string();
            if let Some(h) = &r.health {
                out.push(Sample::new(
                    "sprig_cluster_health_state",
                    &[("cluster", &id)],
                    health_value(h.state),
                ));
            }
            if let Some(rep) = self.monitor.last_report(&r.cluster_id).await
                && let Some(lag) = rep.nodes.iter().filter_map(|n| n.lag_ms).max()
            {
                out.push(Sample::new(
                    "sprig_backend_replica_lag_seconds",
                    &[("cluster", &id)],
                    lag as f64 / 1000.0,
                ));
            }
        }
        for (cluster, dimension, v) in self.balancer.fullness().await {
            out.push(Sample::new(
                "sprig_balancer_cluster_full_ratio",
                &[("cluster", &cluster), ("dimension", &dimension)],
                v,
            ));
        }
        for (id, v) in Self::list_values(&*self.dc_store, "gateways/").await? {
            let n = v
                .pointer("/load/connections")
                .or_else(|| v.pointer("/body/load/connections"))
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            out.push(Sample::new(
                "sprig_gateway_connections",
                &[("gateway", &id)],
                n,
            ));
        }
        for b in crate::backups::read(&*self.dc_store).await? {
            out.push(Sample::new(
                "sprig_backup_last_success_timestamp_seconds",
                &[("cluster", &b.cluster), ("kind", &b.kind)],
                b.finished_at,
            ));
        }
        let jobs =
            sprig_mover::record::JobStore::new(Arc::clone(&self.dc_store), Arc::clone(&self.clock))
                .all()
                .await?;
        let waiting: Vec<u64> = jobs
            .iter()
            .filter(|(j, _)| j.state == sprig_mover::api::JobState::Planned)
            .map(|(j, _)| j.created_ms)
            .collect();
        out.push(Sample::new(
            "sprig_mover_jobs_waiting",
            &[],
            waiting.len() as f64,
        ));
        let oldest = waiting
            .iter()
            .min()
            .map_or(0, |c| now_ms.saturating_sub(*c));
        out.push(Sample::new(
            "sprig_mover_oldest_waiting_seconds",
            &[],
            oldest as f64 / 1000.0,
        ));
        Ok(out)
    }

    #[allow(clippy::cast_precision_loss)]
    async fn limits(&self) -> Limits {
        let max = self.live.as_ref().and_then(|(l, dc)| {
            l.resolve("gateway.max_connections", &SettingCtx::process(dc))
                .ok()
                .and_then(|r| match r.value {
                    sprig_settings::Value::Int(i) => Some(i as f64),
                    sprig_settings::Value::Float(f) => Some(f),
                    _ => None,
                })
        });
        Limits {
            max_connections: max.unwrap_or(Limits::default().max_connections),
        }
    }

    async fn events(&self, since_ms: u64) -> Result<Vec<Event>> {
        let mut out = Vec::new();
        for (c, v) in Self::list_values(&*self.dc_store, "cluster-failover/").await? {
            if let Some(r) = Self::cluster_failovers(&v) {
                for e in r.history.iter().filter(|e| e.at_unix_ms >= since_ms) {
                    out.push(Self::failover_event(&c, e));
                }
            }
        }
        for (_, v) in Self::list_values(&*self.global, "failovers/").await? {
            let Some(r) = Self::dc_failover(&v) else {
                continue;
            };
            let Some(at) = rfc3339_ms(&r.lease_lost_at) else {
                continue;
            };
            if at < since_ms
                || r.dc_id != self.dc && !r.tenants.iter().any(|t| t.to.as_ref() == Some(&self.dc))
            {
                continue;
            }
            let state = serde_json::to_value(r.state)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            let promoted = r.tenants.iter().filter(|t| t.outcome == "promoted").count();
            let mut facts = vec![
                (Some(at), format!("DC {} lost its lease", r.dc_id)),
                (
                    None,
                    format!(
                        "{} observer votes; the failover ended {state}; {promoted} of {} tenants promoted elsewhere",
                        r.votes.len(),
                        r.tenants.len()
                    ),
                ),
            ];
            if let Some(m) = r.tenants.iter().filter_map(|t| t.lag_ms).max() {
                facts.push((None, format!("the largest standby lag was {m} ms")));
            }
            if r.reason.is_some() {
                facts.push((
                    None,
                    "the record holds a reason (sprigctl dc failover show)".to_owned(),
                ));
            }
            out.push(Event {
                id: format!("df-{}", writeup::safe_id(&r.failover_id)),
                kind: "dc_failover".to_owned(),
                at_ms: at,
                cluster: None,
                facts,
            });
        }
        let jobs =
            sprig_mover::record::JobStore::new(Arc::clone(&self.dc_store), Arc::clone(&self.clock))
                .all()
                .await?;
        for (j, _) in jobs {
            if j.state != sprig_mover::api::JobState::RolledBack
                || j.error.is_none()
                || j.updated_ms < since_ms
            {
                continue;
            }
            out.push(Event {
                id: format!("mv-{}", j.job_id.0.simple()),
                kind: "move_failed".to_owned(),
                at_ms: j.updated_ms,
                cluster: None,
                facts: vec![
                    (
                        Some(j.created_ms),
                        format!(
                            "a {} job for tenant t{} was created{}",
                            j.kind.as_str(),
                            j.tenant_id.0,
                            j.decision_id
                                .map(|d| format!(" (the tenant's decision {d})"))
                                .unwrap_or_default()
                        ),
                    ),
                    (
                        Some(j.updated_ms),
                        "it failed and was rolled back; the source stayed authoritative (the error is in sprigctl job show)"
                            .to_owned(),
                    ),
                ],
            });
        }
        out.sort_by_key(|e| e.at_ms);
        Ok(out)
    }

    async fn entries(&self, since_ms: u64) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        for (c, v) in Self::list_values(&*self.dc_store, "cluster-failover/").await? {
            let Some(r) = Self::cluster_failovers(&v) else {
                continue;
            };
            for e in r.history.iter().filter(|e| e.at_unix_ms >= since_ms) {
                let ev = Self::failover_event(&c, e);
                out.push(Entry {
                    id: ev.id.clone(),
                    at_ms: Some(e.at_unix_ms),
                    kind: format!("cluster.failover.{}", e.kind),
                    subject: format!("cluster {c}"),
                    summary: ev
                        .facts
                        .iter()
                        .map(|(_, f)| f.as_str())
                        .collect::<Vec<_>>()
                        .join("; "),
                });
            }
            if r.state != "healthy" {
                out.push(Entry {
                    id: format!("cf-{}-state", cluster_tag(&c)),
                    at_ms: None,
                    kind: "cluster.failover.state".to_owned(),
                    subject: format!("cluster {c}"),
                    summary: format!(
                        "the failover loop holds the cluster {}{}",
                        r.state,
                        r.reason
                            .as_deref()
                            .map(|x| format!(": {x}"))
                            .unwrap_or_default()
                    ),
                });
            }
        }
        for e in self.events(since_ms).await? {
            if e.kind == "cluster_failover" {
                continue;
            }
            out.push(Entry {
                id: e.id.clone(),
                at_ms: Some(e.at_ms),
                kind: e.kind.replace('_', "."),
                subject: e.cluster.clone().unwrap_or_else(|| self.dc.to_string()),
                summary: e
                    .facts
                    .iter()
                    .map(|(_, f)| f.as_str())
                    .collect::<Vec<_>>()
                    .join("; "),
            });
        }
        let jobs =
            sprig_mover::record::JobStore::new(Arc::clone(&self.dc_store), Arc::clone(&self.clock))
                .all()
                .await?;
        for (j, _) in jobs {
            if j.updated_ms < since_ms || j.state == sprig_mover::api::JobState::RolledBack {
                continue;
            }
            out.push(Entry {
                id: format!("mv-{}", j.job_id.0.simple()),
                at_ms: Some(j.updated_ms),
                kind: format!("move.{}", j.kind.as_str()),
                subject: format!("tenant t{}", j.tenant_id.0),
                summary: format!(
                    "a {} job, {} (created {}){}",
                    j.kind.as_str(),
                    j.state.as_str(),
                    time_text(j.created_ms),
                    j.decision_id
                        .map(|d| format!("; the tenant's decision {d} says why"))
                        .unwrap_or_default()
                ),
            });
        }
        Ok(out)
    }

    async fn usage(&self) -> Result<Usage> {
        let mut u = Usage::default();
        if let Some((live, dc)) = &self.live {
            let snap = live.snapshot();
            let ctx = SettingCtx::process(dc);
            for d in live.registry().iter() {
                if let Ok(r) = snap.resolve(d.key(), &ctx) {
                    let scope = match r.source {
                        ScopeKind::Global => "global",
                        ScopeKind::Dc => "dc",
                        ScopeKind::Cluster => "cluster",
                        _ => continue,
                    };
                    u.settings_set.push(SetKey {
                        key: d.key().to_owned(),
                        scope: scope.to_owned(),
                    });
                }
            }
        }
        let tenants: Vec<TenantId> = Directory::new(Arc::clone(&self.global))
            .list()
            .await?
            .iter()
            .map(crate::tree::StoredEntry::body)
            .filter(|e| e.home_dc == self.dc && e.state == TenantState::Active)
            .map(|e| e.tenant_id)
            .collect();
        for (i, t) in tenants.iter().enumerate() {
            if i >= MAX_USAGE_TENANTS {
                u.tenants_skipped += 1;
                continue;
            }
            let read = async {
                let mut rec = crate::catalog_records().begin(*t).await?;
                let rows =
                    sprig_workload::store::WorkloadTables::statement_counts(&mut *rec).await?;
                Ok::<_, SprigError>(rows)
            };
            match tokio::time::timeout(Duration::from_secs(5), read).await {
                Ok(Ok(rows)) => {
                    u.tenants_read += 1;
                    for (fe, kind, st, n) in rows {
                        *u.statements
                            .entry(format!("{fe} {kind} {}", st.to_uppercase()))
                            .or_default() += n;
                        if kind != "sql" {
                            *u.features.entry(kind).or_default() += n;
                        }
                    }
                }
                _ => u.tenants_skipped += 1,
            }
        }
        Ok(u)
    }

    async fn target(&self, version: &str) -> Result<Option<Catalog>> {
        // A pack file next to the binary or next to docs.pack_path.
        let name = sprig_docs::settings::pack_file_name(version);
        let mut paths = Vec::new();
        if let Some(p) = sprig_docs::settings::next_to_binary(&name) {
            paths.push(p);
        }
        if let Some((l, dc)) = &self.live {
            let set = l.get(&sprig_docs::settings::PACK_PATH, &SettingCtx::process(dc));
            if let Some(dir) = std::path::Path::new(set.trim()).parent()
                && !set.trim().is_empty()
            {
                paths.push(dir.join(&name));
            }
        }
        for p in paths {
            if let Ok(bytes) = std::fs::read(&p) {
                let pack = sprig_docs::pack::Pack::from_bytes(&bytes).map_err(|e| {
                    SprigError::new(SprigCode::ConfigInvalid, format!("{}: {e}", p.display()))
                })?;
                return Ok(Some(catalog_of_pack(&pack)));
            }
        }
        // The pack loaded into the system cluster's docs index.
        let Some(cluster) = self.registry.system_cluster().await? else {
            return Ok(None);
        };
        let pool = self.pools.writer(&cluster).await?;
        let mut conn = pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "the system cluster is unreachable",
            )
            .with_source(e)
        })?;
        let texts: Vec<String> = conn
            .exec(
                format!(
                    "SELECT text FROM {} WHERE version = ? AND page IN (?, ?)",
                    sprig_docs::table::qualified(sprig_docs::table::CHUNKS)
                ),
                (version, upgrade::SETTINGS_PAGE, upgrade::ERRORS_PAGE),
            )
            .await
            .unwrap_or_default();
        if texts.is_empty() {
            return Ok(None);
        }
        let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
        Ok(Some(Catalog::from_reference(version, &refs)))
    }
}

/// The catalog of a docs pack file's bytes (`sprigctl ai upgrade-check
/// --pack`).
///
/// # Errors
/// `ConfigInvalid` for bytes that aren't a pack.
pub fn catalog_of_pack_bytes(bytes: &[u8]) -> Result<Catalog> {
    let pack = sprig_docs::pack::Pack::from_bytes(bytes)
        .map_err(|e| SprigError::new(SprigCode::ConfigInvalid, format!("the docs pack: {e}")))?;
    Ok(catalog_of_pack(&pack))
}

/// The catalog of a docs pack: its settings and error reference pages.
#[must_use]
pub fn catalog_of_pack(pack: &sprig_docs::pack::Pack) -> Catalog {
    let texts: Vec<&str> = pack
        .chunks
        .iter()
        .filter(|c| c.page == upgrade::SETTINGS_PAGE || c.page == upgrade::ERRORS_PAGE)
        .map(|c| c.text.as_str())
        .collect();
    Catalog::from_reference(&pack.version, &texts)
}

// ---- the tools the agent's turns run ----

/// The parts the loop, its routes and its tool host share.
pub struct Parts {
    /// The metric history.
    pub history: Arc<History>,
    /// Findings.
    pub findings: FindingStore,
    /// Write-ups.
    pub writeups: WriteupStore,
    /// Incidents of the error intake.
    pub incidents: IncidentStore,
    /// The controller's side.
    pub health: Arc<dyn HealthEnv>,
}

impl Parts {
    /// The decision log's entries about `subject` over the last `days`,
    /// newest first; with nothing matching, the newest entries and a note.
    ///
    /// # Errors
    /// The stores'.
    pub async fn decisions(&self, subject: &str, days: u64, now_ms: u64) -> Result<Value> {
        let since = now_ms.saturating_sub(days.clamp(1, 90) * DAY_MS);
        let mut all = self.health.entries(since).await?;
        for r in self.incidents.list().await? {
            if r.last_seen_ms < since {
                continue;
            }
            all.push(Entry {
                id: format!("inc-{}", r.fingerprint),
                at_ms: Some(r.last_seen_ms),
                kind: "incident".to_owned(),
                subject: format!("{} {} in {} {}", r.code, r.code_name, r.component, r.span),
                summary: format!(
                    "{} errors since {}; status {}{}{}",
                    r.total,
                    time_text(r.first_seen_ms),
                    serde_json::to_value(r.status)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_owned))
                        .unwrap_or_default(),
                    r.severity
                        .as_deref()
                        .map(|s| format!(", severity {s}"))
                        .unwrap_or_default(),
                    r.summary
                        .as_deref()
                        .map(|s| format!(": {s}"))
                        .unwrap_or_default()
                ),
            });
        }
        for (_, (f, _)) in self.findings.list().await? {
            if f.last_ms < since {
                continue;
            }
            all.push(Entry {
                id: format!("health-{}", f.finding.key),
                at_ms: Some(f.last_ms),
                kind: format!("health.{}", f.finding.rule),
                subject: f.finding.series.clone(),
                summary: format!(
                    "{} (severity {}, first found {}, {})",
                    f.finding.text,
                    f.finding.severity,
                    time_text(f.first_ms),
                    f.status
                ),
            });
        }
        for w in self.writeups.list().await? {
            if w.at_ms < since {
                continue;
            }
            let first = w.text.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
            all.push(Entry {
                id: format!("wu-{}", w.id),
                at_ms: Some(w.at_ms),
                kind: "writeup".to_owned(),
                subject: format!("{} {}", w.kind, w.id),
                summary: format!(
                    "{}: {}",
                    w.status,
                    sprig_ai::incidents::brief::clip(first, 300)
                ),
            });
        }
        all.sort_by(|a, b| b.at_ms.unwrap_or(now_ms).cmp(&a.at_ms.unwrap_or(now_ms)));
        let words: Vec<String> = subject
            .to_lowercase()
            .split(|c: char| c.is_whitespace() || matches!(c, ',' | '?' | '.' | '!' | '\'' | '"'))
            .filter(|w| w.len() >= 3 && !STOP_WORDS.contains(w))
            .map(str::to_owned)
            .collect();
        // The entries matching most words first; any match counts.
        let mut scored: Vec<(usize, &Entry)> = all
            .iter()
            .map(|e| (e.matches(&words), e))
            .filter(|(n, _)| *n > 0 || words.is_empty())
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0));
        let hits: Vec<&Entry> = scored.into_iter().map(|(_, e)| e).collect();
        let (list, note) = if hits.is_empty() && !words.is_empty() {
            (
                all.iter().take(MAX_ENTRIES).collect::<Vec<_>>(),
                format!(
                    "nothing matched {subject:?}; the newest entries of the last {days} days follow"
                ),
            )
        } else {
            (hits.into_iter().take(MAX_ENTRIES).collect(), String::new())
        };
        Ok(json!({
            "entries": list.iter().map(|e| e.json()).collect::<Vec<_>>(),
            "total": all.len(),
            "note": note,
            "days": days,
        }))
    }
}

/// The read tools of the agent's own turns: `search_docs` (the operator
/// pages), `metrics_trend` (the history) and `decision_log` (the DC's).
pub struct AdminHost {
    parts: Arc<Parts>,
    docs: Arc<dyn ToolHost>,
}

impl std::fmt::Debug for AdminHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminHost").finish_non_exhaustive()
    }
}

impl AdminHost {
    /// A host over `parts`, searching docs with `docs`.
    #[must_use]
    pub fn new(parts: Arc<Parts>, docs: Arc<dyn ToolHost>) -> Self {
        Self { parts, docs }
    }
}

#[async_trait::async_trait]
impl ToolHost for AdminHost {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        let now = now_ms();
        match spec.name {
            "search_docs" => self.docs.call(spec, input).await,
            "metrics_trend" => {
                let q = TrendQuery::from_input(input);
                let v = self.parts.history.trend(&q, now).await?;
                Ok(ToolOutput {
                    body: ToolBody::Json(v),
                    sources: vec![Source {
                        kind: "metric".into(),
                        title: q.name.clone(),
                        reference: format!("metrics_trend {}", q.name),
                    }],
                })
            }
            "decision_log" => {
                let subject = input["subject"].as_str().unwrap_or("");
                let days = input["days"].as_u64().unwrap_or(30);
                let v = self.parts.decisions(subject, days, now).await?;
                let sources = v["entries"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .take(10)
                    .map(|e| Source {
                        kind: "decision".into(),
                        title: e["kind"].as_str().unwrap_or("").to_owned(),
                        reference: e["id"].as_str().unwrap_or("").to_owned(),
                    })
                    .collect();
                Ok(ToolOutput {
                    body: ToolBody::Json(v),
                    sources,
                })
            }
            other => Err(invalid(format!(
                "{other} isn't available to the administration agent's own turns"
            ))),
        }
    }
}

// ---- the loop ----

#[derive(Debug, Default)]
struct HealthState {
    last_check_ms: u64,
    last_prune_ms: u64,
    warned: VecDeque<u64>,
    written: VecDeque<u64>,
    budget_until_ms: u64,
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HealthReport {
    /// Samples taken.
    pub sampled: usize,
    /// Whether the rules ran.
    pub checked: bool,
    /// Findings of this check.
    pub findings: Vec<Finding>,
    /// Items filed (health warnings and write-ups).
    pub filed: Vec<String>,
    /// Items refreshed.
    pub refreshed: usize,
    /// Write-ups drafted.
    pub writeups: Vec<String>,
    /// Held, by reason.
    pub held: BTreeMap<String, usize>,
}

/// The health loop (module docs).
pub struct HealthLoop {
    dc: DcId,
    knobs: Knobs,
    hub: Arc<Approvals>,
    env: Arc<dyn IncidentEnv>,
    parts: Arc<Parts>,
    state: tokio::sync::Mutex<HealthState>,
}

impl std::fmt::Debug for HealthLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

fn within_hour(q: &mut VecDeque<u64>, now_ms: u64) -> u64 {
    while q
        .front()
        .is_some_and(|t| now_ms.saturating_sub(*t) >= HOUR_MS)
    {
        q.pop_front();
    }
    q.len() as u64
}

fn next_midnight(now_ms: u64) -> u64 {
    (now_ms / DAY_MS + 1) * DAY_MS
}

/// Why a model can't be used now, or the model.
enum ModelOr {
    Model(Model),
    Not(&'static str),
}

impl HealthLoop {
    /// The loop of `dc` over its tier `dc_store`, filing through `hub`.
    #[must_use]
    pub fn new(
        dc: DcId,
        dc_store: &Arc<dyn MetaStore>,
        knobs: Knobs,
        hub: Arc<Approvals>,
        env: Arc<dyn IncidentEnv>,
        health: Arc<dyn HealthEnv>,
    ) -> Self {
        Self {
            dc,
            knobs,
            hub,
            env,
            parts: Arc::new(Parts {
                history: Arc::new(History::new(Arc::clone(dc_store))),
                findings: FindingStore::new(Arc::clone(dc_store)),
                writeups: WriteupStore::new(Arc::clone(dc_store)),
                incidents: IncidentStore::new(Arc::clone(dc_store)),
                health,
            }),
            state: tokio::sync::Mutex::new(HealthState::default()),
        }
    }

    /// The shared parts.
    #[must_use]
    pub fn parts(&self) -> Arc<Parts> {
        Arc::clone(&self.parts)
    }

    fn admin(&self) -> AdminSettings {
        self.knobs.with(AdminSettings::read)
    }

    async fn model(&self, st: &HealthState, now_ms: u64) -> ModelOr {
        let set = self.knobs.with(|l| TurnSettings::read(l, Role::Admin));
        if set.model.trim().is_empty() {
            return ModelOr::Not("ai.model.admin is empty");
        }
        if now_ms < st.budget_until_ms {
            return ModelOr::Not("a daily token budget is spent until 00:00 UTC");
        }
        match self.env.model(Role::Admin, &set.model).await {
            Ok((provider, model)) => ModelOr::Model(Model {
                provider,
                model,
                settings: set,
            }),
            Err(_) => ModelOr::Not("ai.model.admin names no usable provider"),
        }
    }

    async fn advisor(&self) -> Advisor {
        let (audit, stored) = match self.env.audit().await {
            Ok(a) => (a, true),
            Err(e) => {
                tracing::warn!(error = %e, "the administration agent runs unstored");
                (
                    Arc::new(MemoryAudit::system()) as Arc<dyn AuditStore>,
                    false,
                )
            }
        };
        let host = Arc::new(AdminHost::new(Arc::clone(&self.parts), self.env.docs()));
        Advisor::new(audit, host, stored)
    }

    async fn live_item(&self, id: Option<&str>) -> bool {
        let Some(id) = id.and_then(|i| RecId::parse(i).ok()) else {
            return false;
        };
        self.hub
            .get(&id)
            .await
            .is_ok_and(|r| matches!(r.state, RecState::Open | RecState::Snoozed))
    }

    /// One pass at `now_ms`.
    ///
    /// # Errors
    /// The stores'.
    pub async fn pass(&self, now_ms: u64) -> Result<HealthReport> {
        let mut rep = HealthReport::default();
        let knobs = self.admin();
        if !knobs.enabled {
            return Ok(rep);
        }
        match self.parts.health.sample(now_ms).await {
            Ok(s) => {
                rep.sampled = s.len();
                self.parts.history.record(&s, now_ms);
                sprig_ai::metrics::history_sample("ok");
            }
            Err(e) => {
                tracing::debug!(error = %e, "metric history: sampling failed");
                sprig_ai::metrics::history_sample("error");
            }
        }
        if self.parts.history.flush_due(now_ms)
            && let Err(e) = self.parts.history.flush(now_ms).await
        {
            tracing::debug!(error = %e, "metric history: flush failed");
        }
        let mut st = self.state.lock().await;
        if now_ms.saturating_sub(st.last_prune_ms) >= DAY_MS {
            st.last_prune_ms = now_ms;
            if let Err(e) = self.parts.history.prune(now_ms).await {
                tracing::debug!(error = %e, "metric history: prune failed");
            }
        }
        let every = u64::try_from(knobs.check_interval.as_millis()).unwrap_or(u64::MAX);
        if st.last_check_ms == 0 || now_ms.saturating_sub(st.last_check_ms) >= every {
            st.last_check_ms = now_ms;
            rep.checked = true;
            self.check(&mut st, &knobs, now_ms, &mut rep).await?;
        }
        self.writeups(&mut st, &knobs, now_ms, &mut rep).await?;
        Ok(rep)
    }

    /// The rules over the history now (nothing filed).
    ///
    /// # Errors
    /// The stores'.
    pub async fn findings_now(&self, now_ms: u64) -> Result<Vec<Finding>> {
        let series = self.parts.history.series(now_ms).await?;
        let limits = self.parts.health.limits().await;
        Ok(sprig_ai::admin::rules::evaluate(&series, &limits, now_ms))
    }

    async fn check(
        &self,
        st: &mut HealthState,
        knobs: &AdminSettings,
        now_ms: u64,
        rep: &mut HealthReport,
    ) -> Result<()> {
        let found = self.findings_now(now_ms).await?;
        let existing = self.parts.findings.list().await?;
        let mut due: Vec<(FindingRecord, Option<u64>)> = Vec::new();
        for f in &found {
            sprig_ai::metrics::health_finding(&f.rule);
            let (mut r, rev) = match existing.get(&f.key) {
                Some((r, rev)) => (r.clone(), Some(*rev)),
                None => (FindingRecord::new(f.clone(), now_ms), None),
            };
            if rev.is_some() {
                r.finding = f.clone();
                r.last_ms = now_ms;
                r.seen += 1;
            }
            let live = self.live_item(r.advice_id.as_deref()).await;
            if live {
                // Still waiting: refresh its numbers, send nothing.
                if let Err(e) = self.raise(&r, now_ms).await {
                    tracing::debug!(error = %e, "health warning not refreshed");
                }
                rep.refreshed += 1;
                self.parts.findings.put(&r, rev).await?;
            } else if r.due(false, now_ms) {
                due.push((r, rev));
            } else {
                self.parts.findings.put(&r, rev).await?;
            }
        }
        // Findings gone for a quiet day are forgotten: a return is new.
        let quiet = u64::try_from(admin::QUIET.as_millis()).unwrap_or(u64::MAX);
        for (k, (r, _)) in &existing {
            if !found.iter().any(|f| &f.key == k) && now_ms.saturating_sub(r.last_ms) >= quiet {
                self.parts.findings.delete(k).await?;
            }
        }
        rep.findings = found;
        let mut sent = 0;
        for (mut r, rev) in due {
            if sent >= MAX_PER_PASS || within_hour(&mut st.warned, now_ms) >= knobs.max_advice_hour
            {
                "held".clone_into(&mut r.status);
                r.reason = Some("advice_cap".to_owned());
                *rep.held.entry("advice_cap".to_owned()).or_default() += 1;
                sprig_ai::metrics::incident_held("advice_cap");
                self.parts.findings.put(&r, rev).await?;
                continue;
            }
            sent += 1;
            st.warned.push_back(now_ms);
            self.warn(st, &mut r, now_ms).await;
            match self.raise(&r, now_ms).await {
                Ok(id) => {
                    r.advice_id = Some(id.clone());
                    "warned".clone_into(&mut r.status);
                    r.reason = None;
                    r.warned_ms = Some(now_ms);
                    r.warned_severity = Some(r.finding.severity.clone());
                    rep.filed.push(id);
                }
                Err(e) => {
                    "failed".clone_into(&mut r.status);
                    r.reason = Some(e.message.clone());
                }
            }
            if !self.parts.findings.put(&r, rev).await? {
                // Another writer moved it on; this check's outcome wins.
                let cur = self.parts.findings.get(&r.finding.key).await?;
                self.parts.findings.put(&r, cur.map(|(_, v)| v)).await?;
            }
        }
        Ok(())
    }

    /// Words `r`'s warning: the model, else the rule's own text.
    #[allow(clippy::many_single_char_names)] // r, f, m, t: record, finding, model, text
    async fn warn(&self, st: &mut HealthState, r: &mut FindingRecord, now_ms: u64) {
        let f = r.finding.clone();
        let text = match self.model(st, now_ms).await {
            ModelOr::Not(why) => {
                sprig_ai::metrics::health_warning("rules");
                admin::plain_warning(&f, why)
            }
            ModelOr::Model(m) => {
                let (topology, names) = self.env.topology().await.unwrap_or_default();
                let w = Writing {
                    title: format!("Health warning {}: {}", f.key, f.rule),
                    text: admin::health_brief(&f, &topology, now_ms),
                    rules: admin::HEALTH_RULES,
                    tools: admin::agent_tools(),
                    names: &names,
                    restore: f.labels.values().cloned().collect(),
                };
                match self.advisor().await.write(&w, &m).await {
                    Ok(out) => {
                        sprig_ai::metrics::health_warning("model");
                        r.intake = Some(out.intake.clone());
                        r.sent.clone_from(&out.sent);
                        r.conversation = out.conversation.map(|c| c.0);
                        let mut t = out.text.clone();
                        if !out.sources.is_empty() {
                            t.push_str("\n\nDocs searched: ");
                            t.push_str(
                                &out.sources
                                    .iter()
                                    .filter(|s| s.kind == "docs")
                                    .map(|s| format!("{} ({})", s.title, s.reference))
                                    .collect::<Vec<_>>()
                                    .join("; "),
                            );
                        }
                        t
                    }
                    Err(e) => {
                        if e.code() == Some(SprigCode::AiBudgetExceeded) {
                            st.budget_until_ms = next_midnight(now_ms);
                        }
                        sprig_ai::metrics::health_warning("rules");
                        admin::plain_warning(
                            &f,
                            &format!(
                                "the model failed ({})",
                                e.code().map_or(0, SprigCode::number)
                            ),
                        )
                    }
                }
            }
        };
        r.warning = Some(sprig_ai::incidents::brief::clip(&text, MAX_ADVICE));
    }

    async fn raise(&self, r: &FindingRecord, _now_ms: u64) -> Result<String> {
        let f = &r.finding;
        raise_advisory(
            &self.hub,
            &self.dc,
            Advisory {
                kind: Kind::AiHealthWarning,
                target: format!("health/{}", f.key),
                text: format!(
                    "Found by the health rules: {}\n\n{}\n\nAdvice only: nothing is changed. What was sent: sprigctl ai health show {}",
                    f.text,
                    r.warning.as_deref().unwrap_or(""),
                    f.key
                ),
                machine: json!({"finding": f.key}),
                evidence: json!({
                    "rule": f.rule,
                    "series": f.series,
                    "severity": f.severity,
                    "value": f.value,
                    "days_to_threshold": f.days,
                    "first_found": time_text(r.first_ms),
                    "checks": r.seen,
                    "conversation": r.conversation.map(|c| format!("a{c}")),
                    "sent_sha256": r.sent,
                }),
                score: f64::from(admin::severity_rank(&f.severity)),
                summary: sprig_ai::incidents::brief::clip(&f.text, 300),
            },
        )
        .await
    }

    async fn writeups(
        &self,
        st: &mut HealthState,
        knobs: &AdminSettings,
        now_ms: u64,
        rep: &mut HealthReport,
    ) -> Result<()> {
        let since = now_ms.saturating_sub(writeup::MAX_AGE_MS);
        let events = self.parts.health.events(since).await?;
        let mut sent = 0;
        for e in events {
            if e.at_ms + writeup::SETTLE_MS > now_ms
                || self.parts.writeups.get(&e.id).await?.is_some()
            {
                continue;
            }
            if sent >= MAX_PER_PASS || within_hour(&mut st.written, now_ms) >= knobs.max_advice_hour
            {
                *rep.held.entry("advice_cap".to_owned()).or_default() += 1;
                break;
            }
            sent += 1;
            st.written.push_back(now_ms);
            let w = self.draft(st, &e, now_ms).await?;
            if let Some(id) = &w.advice_id {
                rep.filed.push(id.clone());
            }
            rep.writeups.push(w.id);
        }
        Ok(())
    }

    /// The evidence of `e`.
    async fn evidence(&self, e: &Event, now_ms: u64) -> Result<Evidence> {
        let (from, to) = writeup::window(e.at_ms);
        let incidents = self
            .parts
            .incidents
            .list()
            .await?
            .into_iter()
            .filter(|r| r.last_seen_ms >= from && r.first_seen_ms <= to)
            .take(20)
            .map(|r| Seen {
                first_ms: r.first_seen_ms,
                last_ms: r.last_seen_ms,
                text: format!(
                    "{} ({}) in {} {}: {} in all",
                    r.code,
                    if r.code_name.is_empty() {
                        "a MariaDB error"
                    } else {
                        &r.code_name
                    },
                    r.component,
                    r.span,
                    r.total
                ),
            })
            .collect();
        let series = self
            .parts
            .history
            .series(now_ms)
            .await?
            .into_iter()
            .filter(|s| match &e.cluster {
                Some(c) => s.labels.get("cluster") == Some(c),
                None => s.labels.is_empty(),
            })
            .take(12)
            .collect();
        Ok(Evidence {
            event: e.clone(),
            incidents,
            series,
        })
    }

    /// Drafts and files `e`'s write-up.
    #[allow(clippy::too_many_lines)] // the evidence, the turn, the item, the record
    async fn draft(&self, st: &mut HealthState, e: &Event, now_ms: u64) -> Result<WriteupRecord> {
        let ev = self.evidence(e, now_ms).await?;
        let mut w = WriteupRecord {
            id: e.id.clone(),
            kind: e.kind.clone(),
            at_ms: e.at_ms,
            status: "held".to_owned(),
            intake: String::new(),
            text: String::new(),
            sources: Vec::new(),
            advice_id: None,
            conversation: None,
            sent: Vec::new(),
            error: None,
            updated_ms: now_ms,
        };
        let m = match self.model(st, now_ms).await {
            ModelOr::Model(m) => m,
            ModelOr::Not(why) => {
                w.error = Some(why.to_owned());
                w.text = writeup::brief(&ev);
                sprig_ai::metrics::writeup("held");
                self.parts.writeups.put(&w).await?;
                return Ok(w);
            }
        };
        let (topology, names) = self.env.topology().await.unwrap_or_default();
        let mut text = writeup::brief(&ev);
        text.push('\n');
        text.push_str(&topology.summary());
        let wr = Writing {
            title: format!("Write-up {}", e.id),
            text,
            rules: writeup::RULES,
            tools: admin::agent_tools(),
            names: &names,
            restore: e.cluster.iter().cloned().collect(),
        };
        match self.advisor().await.write(&wr, &m).await {
            Ok(out) => {
                w.text = out.text;
                w.intake = out.intake;
                w.sent = out.sent;
                w.conversation = out.conversation.map(|c| c.0);
                w.sources = out
                    .sources
                    .iter()
                    .map(|s| format!("{} ({})", s.title, s.reference))
                    .collect();
                "drafted".clone_into(&mut w.status);
                let first = w
                    .text
                    .lines()
                    .map(|l| l.replace("**Summary:**", "").replace('*', ""))
                    .find(|l| !l.trim().is_empty())
                    .unwrap_or_default();
                match raise_advisory(
                    &self.hub,
                    &self.dc,
                    Advisory {
                        kind: Kind::AiIncidentAdvice,
                        target: format!("writeup/{}", e.id),
                        text: format!(
                            "{}\n\nWrite-up only: nothing is changed. What was sent: sprigctl ai incident writeup {}",
                            sprig_ai::incidents::brief::clip(&w.text, MAX_ADVICE),
                            e.id
                        ),
                        machine: json!({"writeup": e.id}),
                        evidence: json!({
                            "event": e.id,
                            "kind": e.kind,
                            "at": time_text(e.at_ms),
                            "conversation": w.conversation.map(|c| format!("a{c}")),
                            "sent_sha256": w.sent,
                        }),
                        score: 1.0,
                        summary: format!(
                            "Write-up of {} {}: {}",
                            e.kind.replace('_', " "),
                            e.id,
                            sprig_ai::incidents::brief::clip(first.trim(), 200)
                        ),
                    },
                )
                .await
                {
                    Ok(id) => w.advice_id = Some(id),
                    Err(err) => w.error = Some(err.message.clone()),
                }
                sprig_ai::metrics::writeup("drafted");
            }
            Err(err) => {
                if err.code() == Some(SprigCode::AiBudgetExceeded) {
                    st.budget_until_ms = next_midnight(now_ms);
                }
                "failed".clone_into(&mut w.status);
                w.error = Some(err.message.clone());
                w.text = writeup::brief(&ev);
                sprig_ai::metrics::writeup("failed");
            }
        }
        self.parts.writeups.put(&w).await?;
        Ok(w)
    }

    /// Drafts (again) the write-up of event `id` (from the last 90 days) or
    /// of incident fingerprint `id`.
    ///
    /// # Errors
    /// 39792 for an unknown id; the stores'.
    pub async fn draft_now(&self, id: &str, now_ms: u64) -> Result<WriteupRecord> {
        let event = if id.len() == 12 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            let (r, _) = self
                .parts
                .incidents
                .get(id)
                .await?
                .ok_or_else(|| invalid(format!("there is no incident {id}")))?;
            let mut facts = vec![
                (
                    Some(r.first_seen_ms),
                    format!(
                        "error {} ({}) in {} {} first seen",
                        r.code, r.code_name, r.component, r.span
                    ),
                ),
                (
                    Some(r.last_seen_ms),
                    format!("last seen; {} in all", r.total),
                ),
            ];
            if let Some(at) = r.escalated_ms {
                facts.push((
                    Some(at),
                    format!(
                        "sent to triage and advice: verdict {}{}",
                        r.verdict.as_deref().unwrap_or("none"),
                        r.severity
                            .as_deref()
                            .map(|s| format!(", severity {s}"))
                            .unwrap_or_default()
                    ),
                ));
            }
            if let Some(s) = &r.summary {
                facts.push((None, format!("the advice said: {s}")));
            }
            Event {
                id: format!("inc-{id}"),
                kind: "incident".to_owned(),
                at_ms: r.last_seen_ms,
                cluster: None,
                facts,
            }
        } else {
            self.parts
                .health
                .events(now_ms.saturating_sub(90 * DAY_MS))
                .await?
                .into_iter()
                .find(|e| e.id == id)
                .ok_or_else(|| {
                    invalid(format!(
                        "there is no event {id} in the last 90 days (ids look like cf-<cluster>-<epoch>, df-<failover>, mv-<job>, or an incident fingerprint)"
                    ))
                })?
        };
        let mut st = self.state.lock().await;
        self.draft(&mut st, &event, now_ms).await
    }

    /// The upgrade readiness report for `req`.
    ///
    /// # Errors
    /// 39792 when no catalog of the target version can be found; the
    /// stores'.
    pub async fn upgrade_check(&self, req: UpgradeRequest, now_ms: u64) -> Result<Value> {
        let r = self.upgrade_inner(req, now_ms).await;
        sprig_ai::metrics::upgrade_check(if r.is_ok() { "ok" } else { "error" });
        r
    }

    #[allow(clippy::many_single_char_names)] // d, v, m, w: diff, answer, model, writing
    async fn upgrade_inner(&self, req: UpgradeRequest, now_ms: u64) -> Result<Value> {
        let to = req.to.trim().to_owned();
        if to.is_empty() || to.len() > 32 {
            return Err(invalid("upgrade check: give the target version in to"));
        }
        let running = current_catalog();
        let target = match req.catalog {
            Some(mut c) => {
                c.version.clone_from(&to);
                c
            }
            None => self.parts.health.target(&to).await?.ok_or_else(|| {
                invalid(format!(
                    "no docs pack of version {to} was found (next to the controller's binary, next to docs.pack_path, or loaded in the docs index); pass it with sprigctl ai upgrade-check --to {to} --pack <file>"
                ))
            })?,
        };
        if target.settings.is_empty() {
            return Err(invalid(format!(
                "the docs pack of {to} has no settings reference page to compare with"
            )));
        }
        let usage = self.parts.health.usage().await?;
        let d = upgrade::diff(&running, &target);
        let report = upgrade::report(&d, &usage);
        let mut v = json!({
            "from": report.from,
            "to": report.to,
            "summary": report.text().lines().next().unwrap_or(""),
            "items": report.items,
            "added_by_group": report.added_by_group,
            "codes_added": report.codes_added,
            "usage": {
                "settings_set": usage.settings_set,
                "statements": usage.statements,
                "features": usage.features,
                "tenants_read": usage.tenants_read,
                "tenants_skipped": usage.tenants_skipped,
            },
        });
        if req.explain {
            let st = self.state.lock().await;
            match self.model(&st, now_ms).await {
                ModelOr::Not(why) => v["explanation_unavailable"] = json!(why),
                ModelOr::Model(m) => {
                    drop(st);
                    let (_, names) = self.env.topology().await.unwrap_or_default();
                    let kinds: Vec<String> = usage
                        .statements
                        .iter()
                        .take(30)
                        .map(|(k, n)| format!("{k} {n}"))
                        .collect();
                    let text = format!(
                        "{}\nUsage: {} settings set away from their defaults; statement kinds: {}.",
                        report.text(),
                        usage.settings_set.len(),
                        kinds.join(", ")
                    );
                    let w = Writing {
                        title: format!("Upgrade check to {to}"),
                        text,
                        rules: upgrade::RULES,
                        tools: admin::agent_tools(),
                        names: &names,
                        restore: Vec::new(),
                    };
                    match self.advisor().await.write(&w, &m).await {
                        Ok(out) => {
                            v["explanation"] = json!(out.text);
                            v["sent_sha256"] = json!(out.sent);
                            v["conversation"] =
                                json!(out.conversation.map(|c| format!("a{}", c.0)));
                        }
                        Err(e) => v["explanation_unavailable"] = json!(e.message),
                    }
                }
            }
        }
        Ok(v)
    }
}

/// `POST /admin/v1/ai:upgradeCheck`'s body.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpgradeRequest {
    /// The target version.
    #[serde(default)]
    pub to: String,
    /// The target's catalog, when the caller read its docs pack.
    #[serde(default)]
    pub catalog: Option<Catalog>,
    /// Ask the admin model to explain the report.
    #[serde(default)]
    pub explain: bool,
}

#[async_trait::async_trait]
impl Reconciler for HealthLoop {
    type Key = String;

    fn name(&self) -> &'static str {
        "ai_health"
    }

    fn tier(&self) -> LoopTier {
        LoopTier::DcLeader
    }

    fn watches(&self) -> Vec<WatchSpec> {
        Vec::new()
    }

    fn keys_for(&self, _ev: &MetaEvent) -> Vec<String> {
        vec![KEY.to_owned()]
    }

    async fn all_keys(&self, _cx: &LoopCx) -> Result<Vec<String>> {
        Ok(vec![KEY.to_owned()])
    }

    async fn reconcile(&self, cx: &LoopCx, _key: &String) -> Result<Outcome> {
        cx.leader()?;
        self.pass(now_ms()).await?;
        Ok(Outcome::RequeueAfter(SAMPLE_INTERVAL))
    }
}

// ---- the routes ----

fn query(uri: &Uri) -> Vec<(String, String)> {
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (pct_decode(k), pct_decode(v)))
        .collect()
}

/// `s` with `+` and `%xx` decoded.
#[must_use]
pub fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                match u8::from_str_radix(s.get(i + 1..i + 3).unwrap_or(""), 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 2;
                    }
                    Err(_) => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The routes.
pub fn router(l: Arc<HealthLoop>) -> Router {
    Router::new()
        .route("/admin/v1/ai/metrics", get(metrics))
        .route("/admin/v1/ai/health", get(health_list))
        .route("/admin/v1/ai/health:check", post(health_check))
        .route("/admin/v1/ai/health/{key}", get(health_show))
        .route("/admin/v1/ai/decisions", get(decisions))
        .route("/admin/v1/ai/writeups", get(writeup_list))
        .route(
            "/admin/v1/ai/writeups/{id}",
            get(writeup_show).post(writeup_post),
        )
        .route("/admin/v1/ai:upgradeCheck", post(upgrade_post))
        .with_state(l)
}

async fn metrics(State(l): State<Arc<HealthLoop>>, headers: HeaderMap, uri: Uri) -> Response {
    respond(
        async {
            principal(&headers)?;
            let q = query(&uri);
            let get = |k: &str| q.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone());
            let tq = TrendQuery {
                name: get("name").unwrap_or_default(),
                labels: q
                    .iter()
                    .filter(|(k, _)| k == "label")
                    .filter_map(|(_, v)| v.split_once('='))
                    .map(|(a, b)| (a.to_owned(), b.to_owned()))
                    .collect(),
                days: get("days").and_then(|d| d.parse().ok()).unwrap_or(7),
            };
            l.parts.history.trend(&tq, now_ms()).await
        }
        .await,
    )
}

async fn health_list(State(l): State<Arc<HealthLoop>>, headers: HeaderMap) -> Response {
    respond(
        async {
            principal(&headers)?;
            let mut all: Vec<FindingRecord> = l
                .parts
                .findings
                .list()
                .await?
                .into_values()
                .map(|(r, _)| r)
                .collect();
            all.sort_by(|a, b| b.last_ms.cmp(&a.last_ms));
            let rows: Vec<Value> = all
                .iter()
                .map(|r| {
                    json!({
                        "key": r.finding.key, "rule": r.finding.rule,
                        "severity": r.finding.severity, "text": r.finding.text,
                        "status": r.status, "reason": r.reason,
                        "first_found": time_text(r.first_ms), "last_found": time_text(r.last_ms),
                        "checks": r.seen, "advice_id": r.advice_id,
                    })
                })
                .collect();
            Ok(json!({"findings": rows}))
        }
        .await,
    )
}

async fn health_show(
    State(l): State<Arc<HealthLoop>>,
    headers: HeaderMap,
    UrlPath(key): UrlPath<String>,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            let (r, _) = l
                .parts
                .findings
                .get(&key)
                .await?
                .ok_or_else(|| invalid(format!("there is no finding {key}")))?;
            Ok(json!({"finding": r}))
        }
        .await,
    )
}

async fn health_check(State(l): State<Arc<HealthLoop>>, headers: HeaderMap) -> Response {
    respond(
        async {
            principal(&headers)?;
            let found = l.findings_now(now_ms()).await?;
            Ok(json!({"findings": found, "filed": false}))
        }
        .await,
    )
}

async fn decisions(State(l): State<Arc<HealthLoop>>, headers: HeaderMap, uri: Uri) -> Response {
    respond(
        async {
            principal(&headers)?;
            let q = query(&uri);
            let get = |k: &str| q.iter().find(|(a, _)| a == k).map(|(_, v)| v.clone());
            let days = get("days").and_then(|d| d.parse().ok()).unwrap_or(30);
            l.parts
                .decisions(&get("subject").unwrap_or_default(), days, now_ms())
                .await
        }
        .await,
    )
}

async fn writeup_list(State(l): State<Arc<HealthLoop>>, headers: HeaderMap) -> Response {
    respond(
        async {
            principal(&headers)?;
            let rows: Vec<Value> = l
                .parts
                .writeups
                .list()
                .await?
                .iter()
                .map(|w| {
                    json!({"id": w.id, "kind": w.kind, "at": time_text(w.at_ms),
                           "status": w.status, "advice_id": w.advice_id, "error": w.error})
                })
                .collect();
            Ok(json!({"writeups": rows}))
        }
        .await,
    )
}

async fn writeup_show(
    State(l): State<Arc<HealthLoop>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            let w = l
                .parts
                .writeups
                .get(&id)
                .await?
                .ok_or_else(|| invalid(format!("there is no write-up {id}; draft one with sprigctl ai incident writeup {id} --draft")))?;
            Ok(json!({"writeup": w}))
        }
        .await,
    )
}

async fn writeup_post(
    State(l): State<Arc<HealthLoop>>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            let Some(id) = id.strip_suffix(":draft") else {
                return Err(invalid("the method is :draft"));
            };
            let w = l.draft_now(id, now_ms()).await?;
            Ok(json!({"writeup": w}))
        }
        .await,
    )
}

async fn upgrade_post(
    State(l): State<Arc<HealthLoop>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            let req: UpgradeRequest =
                serde_json::from_slice(&body).map_err(|e| invalid(format!("request body: {e}")))?;
            l.upgrade_check(req, now_ms()).await
        }
        .await,
    )
}

/// Registers every id a write-up or warning could hold with `names`, as
/// hosts (cluster, gateway ids).
pub fn add_labels(names: &mut Names, labels: &BTreeMap<String, String>) {
    for (k, v) in labels {
        if matches!(k.as_str(), "cluster" | "gateway" | "node") {
            names.add(NameKind::Host, v);
        }
    }
}

#[cfg(test)]
mod tests;
