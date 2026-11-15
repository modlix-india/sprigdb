//! The administration agent's incident intake on the DC leader, and the
//! Admin API's incident routes.
//!
//! ```text
//! every PASS_INTERVAL, while ai.admin.enabled (global):
//!   new errors := Merger over the processes' ai/intake/ records
//!   per (code, component, span): ai/incidents/<fingerprint> += counts;
//!     new, critical or rising (and not cooling down) -> pending;
//!     its advice still waiting -> the item's counts are refreshed
//!   pending, at most MAX_PER_PASS, critical first then the busiest:
//!     no ai.model.admin -> held (no_model); a budget spent -> held until 00:00 UTC
//!     over ai.admin.max_advice_hour or ai.admin.max_triage_hour -> held
//!     Advisor: triage, then advice -> an ai_incident_advice item
//!       (dc level, advisory, never applied; emailed like any new item)
//!   pending for a day -> dropped (held: expired)
//! ```
//!
//! | Route | Does |
//! |---|---|
//! | `GET /admin/v1/ai/incidents` | This DC's incidents, newest first |
//! | `GET /admin/v1/ai/incidents/{fingerprint}?payloads=true` | One incident, with its conversation; `payloads=true` adds the exact payloads sent |
//!
//! Approving an advice item only acknowledges it ([`Acknowledge`]):
//! nothing is ever applied.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::{HeaderMap, Uri};
use axum::response::Response;
use axum::routing::get;
use serde_json::{Value, json};
use sprig_ai::audit::AuditStore;
use sprig_ai::egress::NameKind;
use sprig_ai::incidents::advise::Model;
use sprig_ai::incidents::{
    Advisor, Case, ClusterShape, INCIDENTS, IncidentRecord, IncidentStore, Merger, Names, Status,
    Topology, Why, intake,
};
use sprig_ai::keys::AiKeyring;
use sprig_ai::providers::ProviderStore;
use sprig_ai::settings::{AdminSettings, Lookup, TurnSettings};
use sprig_ai::tools::{Source, ToolBody, ToolHost, ToolOutput, ToolSpec};
use sprig_ai::{MemoryAudit, Provider, Role, SystemAudit};
use sprig_approvals::gate::Approvals;
use sprig_approvals::model::{
    Action, Level, Mode, Proposal, RecId, Recommendation, State as RecState, Subject,
};
use sprig_approvals::{Kind, Policy, Verdict};
use sprig_cluster::record::AdminState;
use sprig_cluster::registry::ClusterRegistry;
use sprig_common::ids::DcId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{ListOpts, MetaStore};
use sprig_settings::{LiveSettings, SettingCtx};

use crate::admin::http::respond;
use crate::admin::invalid;
use crate::approvals::Applier;
use crate::catalogs::{ClusterPools, RegistryPools};
use crate::operators::http::principal;
use crate::runtime::{LoopCx, LoopTier, MetaEvent, Outcome, Reconciler, WatchSpec};
use crate::tree::Directory;

/// How often the intake runs.
pub const PASS_INTERVAL: Duration = Duration::from_secs(60);
/// Incidents sent to triage or advice in one pass at most.
pub const MAX_PER_PASS: usize = 3;
/// A leader that didn't run a pass for this long starts its counts over.
const STALE_PASS: Duration = Duration::from_secs(180);
/// Advice text kept in an item and in the incident.
pub const MAX_ADVICE: usize = 8 * 1024;
/// Tenant names registered with the scrubber at most.
const MAX_TENANT_NAMES: usize = 100_000;
/// The loop's one key.
const KEY: &str = "intake";
const HOUR_MS: u64 = 3_600_000;

// ---- what the loop needs from the controller ----

/// Providers, the system store, the topology and the docs search.
#[async_trait::async_trait]
pub trait IncidentEnv: Send + Sync + 'static {
    /// `ai.model.<role>`'s provider and model.
    async fn model(&self, role: Role, setting: &str) -> Result<(Arc<dyn Provider>, String)>;
    /// The system store opened for the incident intake.
    async fn audit(&self) -> Result<Arc<dyn AuditStore>>;
    /// The topology summary and every name that could appear in an
    /// intake.
    async fn topology(&self) -> Result<(Topology, Names)>;
    /// Runs `search_docs` (operator pages).
    fn docs(&self) -> Arc<dyn ToolHost>;
}

/// [`IncidentEnv`] over the controller's stores.
pub struct ControllerEnv {
    dc: DcId,
    dc_store: Arc<dyn MetaStore>,
    global: Arc<dyn MetaStore>,
    providers: ProviderStore,
    keys: Arc<dyn AiKeyring>,
    registry: ClusterRegistry,
    pools: RegistryPools,
    docs: Arc<DocsHost>,
}

impl std::fmt::Debug for ControllerEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerEnv")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// No master key file: only providers without a key open.
struct NoKeys;

impl AiKeyring for NoKeys {
    fn current(&self, _owner: sprig_common::ids::TenantId) -> Option<(u32, [u8; 32])> {
        None
    }
    fn key(&self, _owner: sprig_common::ids::TenantId, _version: u32) -> Option<[u8; 32]> {
        None
    }
}

impl ControllerEnv {
    /// The environment of `dc`'s controller; provider keys open with the
    /// master key file at `key_file`.
    ///
    /// # Errors
    /// The provider store's (a store that isn't the global tier); a key
    /// file that doesn't read.
    pub fn new(
        dc: DcId,
        dc_store: Arc<dyn MetaStore>,
        global: Arc<dyn MetaStore>,
        key_file: Option<&std::path::Path>,
        pools: RegistryPools,
        live: Option<(LiveSettings, DcId)>,
    ) -> Result<Self> {
        let keys: Arc<dyn AiKeyring> = match key_file {
            Some(p) => Arc::new(sprig_workload::keyring::MasterKeyring::from_file(p)?),
            None => Arc::new(NoKeys),
        };
        Ok(Self {
            providers: ProviderStore::new(Arc::clone(&global))?,
            registry: ClusterRegistry::new(Arc::clone(&global), Arc::clone(&dc_store))?,
            dc,
            dc_store,
            global,
            keys,
            pools,
            docs: Arc::new(DocsHost::new(live)),
        })
    }

    async fn ids(&self, prefix: &str, field: &str, names: &mut Names) -> Result<u32> {
        let key = self.dc_store.tier().key(prefix)?;
        let mut n = 0u32;
        let mut opts = ListOpts::default();
        loop {
            let page = self.dc_store.list(&key, opts.clone()).await?;
            for kv in &page.kvs {
                n += 1;
                let id = kv.key.as_str().rsplit('/').next().unwrap_or_default();
                names.add(NameKind::Host, id);
                if let Ok(v) = serde_json::from_slice::<Value>(&kv.value) {
                    for f in [field, "addr", "kafka_host"] {
                        if let Some(s) = v.get(f).and_then(Value::as_str) {
                            names.add(NameKind::Host, s);
                        }
                    }
                }
            }
            if !page.more {
                return Ok(n);
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
}

#[async_trait::async_trait]
impl IncidentEnv for ControllerEnv {
    async fn model(&self, role: Role, setting: &str) -> Result<(Arc<dyn Provider>, String)> {
        self.providers.for_role(role, setting, &*self.keys).await
    }

    async fn audit(&self) -> Result<Arc<dyn AuditStore>> {
        let cluster = self.registry.system_cluster().await?.ok_or_else(|| {
            SprigError::new(
                SprigCode::CatalogUnavailable,
                "administration conversations: the data center has no cluster in service yet",
            )
        })?;
        let pool = self.pools.writer(&cluster).await?;
        Ok(Arc::new(SystemAudit::new(pool, INCIDENTS)))
    }

    async fn topology(&self) -> Result<(Topology, Names)> {
        let mut names = Names::new();
        let mut t = Topology::default();
        let dcs = crate::dc::list(&*self.global).await.unwrap_or_default();
        t.dcs = u32::try_from(dcs.len()).unwrap_or(u32::MAX);
        names.add(NameKind::Host, self.dc.as_str());
        for d in &dcs {
            if let Ok(v) = serde_json::to_value(d) {
                for f in ["dc_id", "name", "region"] {
                    if let Some(s) = v.get(f).and_then(Value::as_str) {
                        names.add(NameKind::Host, s);
                    }
                }
            }
        }
        for c in self.registry.list().await? {
            let r = c.body();
            names.add(NameKind::Host, r.cluster_id.as_str());
            names.add(NameKind::Host, &r.name);
            for n in &r.nodes {
                names.add(NameKind::Host, &n.node.to_string());
                names.add(NameKind::Address, &n.addr);
                if let Some(z) = &n.zone {
                    names.add(NameKind::Host, z);
                }
            }
            for id in r.node_identities.values() {
                names.add(NameKind::Host, id.split('#').next().unwrap_or_default());
            }
            if matches!(
                r.admin_state,
                AdminState::Active | AdminState::Draining | AdminState::Drained
            ) {
                t.clusters.push(ClusterShape {
                    kind: r.topology.as_str().to_owned(),
                    nodes: u32::try_from(r.nodes.len()).unwrap_or(u32::MAX),
                    server: r
                        .profile
                        .as_ref()
                        .and_then(|p| p.server.version.as_ref())
                        .map(|v| format!("{}.{}.{}", v.major, v.minor, v.patch)),
                });
            }
        }
        t.gateways = self.ids("gateways/", "gateway_id", &mut names).await?;
        t.workers = self.ids("workers/", "worker_id", &mut names).await?;
        self.ids(intake::PREFIX, "process", &mut names).await?;
        if let Ok(all) = Directory::new(Arc::clone(&self.global)).list().await {
            for e in all.iter().take(MAX_TENANT_NAMES) {
                names.add(NameKind::Tenant, &e.body().name);
            }
        }
        Ok((t, names))
    }

    fn docs(&self) -> Arc<dyn ToolHost> {
        Arc::clone(&self.docs) as Arc<dyn ToolHost>
    }
}

// ---- the docs search ----

/// The docs pack's operator pages, searched in process (keyword ranking).
pub struct DocsHost {
    live: Option<(LiveSettings, DcId)>,
    path: Option<PathBuf>,
    index: Mutex<Option<(PathBuf, Arc<DocsIndex>)>>,
}

impl std::fmt::Debug for DocsHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocsHost").finish_non_exhaustive()
    }
}

/// The operator pages of a pack and their keyword index.
pub struct DocsIndex {
    chunks: Vec<sprig_docs::pack::Chunk>,
    bm25: sprig_docs::search::Bm25,
}

impl DocsIndex {
    /// The operator chunks of `pack`.
    #[must_use]
    pub fn of(pack: sprig_docs::pack::Pack) -> Self {
        let chunks: Vec<_> = pack
            .chunks
            .into_iter()
            .filter(|c| c.audience.iter().any(|a| a == "operator"))
            .collect();
        let bm25 = sprig_docs::search::Bm25::for_chunks(&chunks);
        Self { chunks, bm25 }
    }

    /// The best `limit` chunks for `query`, at most two per page.
    #[must_use]
    pub fn search(&self, query: &str, limit: usize) -> Vec<&sprig_docs::pack::Chunk> {
        let hits = self.bm25.search(query, sprig_docs::search::CANDIDATES);
        sprig_docs::search::cap_per_page(
            &hits,
            |i| self.chunks[i].page.as_str(),
            sprig_docs::search::MAX_PER_PAGE,
            limit,
        )
        .into_iter()
        .map(|(i, _)| &self.chunks[i])
        .collect()
    }
}

/// Docs hits returned.
const DOCS_HITS: usize = 5;
/// Characters of a hit kept.
const DOCS_TEXT: usize = 1500;

impl DocsHost {
    /// The pack named by `docs.pack_path` (else the one next to the
    /// binary).
    #[must_use]
    pub fn new(live: Option<(LiveSettings, DcId)>) -> Self {
        Self {
            live,
            path: None,
            index: Mutex::new(None),
        }
    }

    /// The pack at `path` (tests).
    #[must_use]
    pub fn at(path: PathBuf) -> Self {
        Self {
            live: None,
            path: Some(path),
            index: Mutex::new(None),
        }
    }

    fn pack_path(&self) -> Option<PathBuf> {
        if let Some(p) = &self.path {
            return Some(p.clone());
        }
        let set = match &self.live {
            Some((l, dc)) => l.get(&sprig_docs::settings::PACK_PATH, &SettingCtx::process(dc)),
            None => sprig_docs::settings::PACK_PATH.default.clone(),
        };
        if set.trim().is_empty() {
            sprig_docs::settings::next_to_binary(&sprig_docs::settings::pack_file_name(env!(
                "CARGO_PKG_VERSION"
            )))
        } else {
            Some(PathBuf::from(set.trim()))
        }
    }

    fn index(&self) -> Result<Arc<DocsIndex>> {
        let path = self.pack_path().ok_or_else(|| {
            SprigError::new(SprigCode::ConfigInvalid, "the docs pack's path is unknown")
        })?;
        if let Some((p, ix)) = &*self.index.lock().unwrap_or_else(PoisonError::into_inner)
            && *p == path
        {
            return Ok(Arc::clone(ix));
        }
        let bytes = std::fs::read(&path).map_err(|e| {
            SprigError::new(
                SprigCode::ConfigInvalid,
                format!("the docs pack can't be read ({e}); docs search is unavailable"),
            )
        })?;
        let pack = sprig_docs::pack::Pack::from_bytes(&bytes).map_err(|e| {
            SprigError::new(SprigCode::ConfigInvalid, format!("the docs pack: {e}"))
        })?;
        let ix = Arc::new(DocsIndex::of(pack));
        *self.index.lock().unwrap_or_else(PoisonError::into_inner) = Some((path, Arc::clone(&ix)));
        Ok(ix)
    }
}

fn clip(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_owned();
    }
    let mut out: String = s.chars().take(n).collect();
    out.push('…');
    out
}

#[async_trait::async_trait]
impl ToolHost for DocsHost {
    async fn call(&self, spec: &ToolSpec, input: &Value) -> Result<ToolOutput> {
        if spec.name != "search_docs" {
            return Err(invalid(format!(
                "{} isn't available to incident advice",
                spec.name
            )));
        }
        let query = input
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let ix = self.index()?;
        let mut sources = Vec::new();
        let hits: Vec<Value> = ix
            .search(&query, DOCS_HITS)
            .into_iter()
            .map(|c| {
                let title = if c.heading_path.is_empty() {
                    c.title.clone()
                } else {
                    format!("{} › {}", c.title, c.heading_path)
                };
                sources.push(Source {
                    kind: "docs".into(),
                    title: title.clone(),
                    reference: c.page.clone(),
                });
                json!({"page": c.page, "title": title, "text": clip(&c.text, DOCS_TEXT)})
            })
            .collect();
        Ok(ToolOutput {
            body: ToolBody::Json(json!({"version": env!("CARGO_PKG_VERSION"), "hits": hits})),
            sources,
        })
    }
}

// ---- the loop ----

/// Where the `ai.*` values come from.
#[derive(Clone)]
pub enum Knobs {
    /// The live settings, at the global (process) scope.
    Live(LiveSettings, DcId),
    /// Fixed values (tests).
    Fixed(Arc<dyn Lookup + Send + Sync>),
}

impl std::fmt::Debug for Knobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Knobs")
    }
}

impl Knobs {
    pub(crate) fn with<R>(&self, f: impl FnOnce(&dyn Lookup) -> R) -> R {
        match self {
            Knobs::Live(l, dc) => f(&sprig_ai::settings::Live {
                snap: l.snapshot(),
                ctx: SettingCtx::process(dc),
            }),
            Knobs::Fixed(l) => f(&**l),
        }
    }
}

#[derive(Debug)]
struct LoopState {
    merger: Merger,
    last_pass_ms: u64,
    advice: VecDeque<u64>,
    triage: VecDeque<u64>,
    budget_until_ms: u64,
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Fingerprints with new errors.
    pub counted: usize,
    /// Sent to triage or advice.
    pub escalated: usize,
    /// Advice items filed.
    pub advised: Vec<String>,
    /// Held, by reason.
    pub held: BTreeMap<String, usize>,
}

/// The intake loop (module docs).
pub struct IncidentLoop {
    dc: DcId,
    store: IncidentStore,
    dc_store: Arc<dyn MetaStore>,
    knobs: Knobs,
    hub: Arc<Approvals>,
    env: Arc<dyn IncidentEnv>,
    state: tokio::sync::Mutex<LoopState>,
}

impl std::fmt::Debug for IncidentLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncidentLoop")
            .field("dc", &self.dc)
            .finish_non_exhaustive()
    }
}

/// The next 00:00 UTC after `now_ms`.
fn next_midnight(now_ms: u64) -> u64 {
    (now_ms / 86_400_000 + 1) * 86_400_000
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

impl IncidentLoop {
    /// The loop of `dc` over its tier `dc_store`, filing advice through
    /// `hub`.
    #[must_use]
    pub fn new(
        dc: DcId,
        dc_store: Arc<dyn MetaStore>,
        knobs: Knobs,
        hub: Arc<Approvals>,
        env: Arc<dyn IncidentEnv>,
    ) -> Self {
        Self {
            store: IncidentStore::new(Arc::clone(&dc_store)),
            dc,
            dc_store,
            knobs,
            hub,
            env,
            state: tokio::sync::Mutex::new(LoopState {
                merger: Merger::new(intake::now_ms()),
                last_pass_ms: 0,
                advice: VecDeque::new(),
                triage: VecDeque::new(),
                budget_until_ms: 0,
            }),
        }
    }

    fn admin(&self) -> AdminSettings {
        self.knobs.with(AdminSettings::read)
    }

    /// Whether the advice item `id` is still waiting (open or snoozed),
    /// and whether it is live at all.
    async fn item(&self, id: &str) -> Option<Recommendation> {
        let id = RecId::parse(id).ok()?;
        self.hub.get(&id).await.ok()
    }

    /// One pass at `now_ms`.
    ///
    /// # Errors
    /// The stores'.
    #[allow(clippy::too_many_lines)] // the steps of the module docs
    pub async fn pass(&self, now_ms: u64) -> Result<PassReport> {
        let mut report = PassReport::default();
        let knobs = self.admin();
        let mut st = self.state.lock().await;
        if !knobs.enabled {
            st.last_pass_ms = 0;
            return Ok(report);
        }
        let stale = u64::try_from(STALE_PASS.as_millis()).unwrap_or(u64::MAX);
        if st.last_pass_ms == 0 || now_ms.saturating_sub(st.last_pass_ms) > stale {
            // Newly leading (or turned on): what processes counted before
            // is a baseline, not new.
            st.merger = Merger::new(now_ms);
        }
        st.last_pass_ms = now_ms;
        let records = intake::list(&*self.dc_store).await?;
        let deltas = st.merger.merge(&records);
        report.counted = deltas.len();
        for ((code, component, span), d) in &deltas {
            let fp = sprig_ai::incidents::fingerprint(*code, component, span);
            for _ in 0..3 {
                let (mut rec, rev) = match self.store.get(&fp).await? {
                    Some((r, rev)) => (r, Some(rev)),
                    None => (IncidentRecord::new(*code, component, span, now_ms), None),
                };
                rec.add(d, now_ms);
                let item = match &rec.advice_id {
                    Some(id) => self.item(id).await,
                    None => None,
                };
                let open = item.as_ref().is_some_and(|i| i.state.is_live());
                if let Some(why) = rec.goes_further(rev.is_none(), now_ms, open) {
                    rec.pend(why, now_ms);
                }
                if let Some(i) = item
                    && matches!(i.state, RecState::Open | RecState::Snoozed)
                {
                    // Repeats only refresh the item's counts.
                    if let Err(e) = self.raise(&rec, now_ms).await {
                        tracing::debug!(error = %e, "incident advice not refreshed");
                    }
                }
                if self.store.put(&rec, rev).await? {
                    break;
                }
            }
        }

        let mut pending: Vec<IncidentRecord> = self
            .store
            .list()
            .await?
            .into_iter()
            .filter(|r| r.status == Status::Pending)
            .collect();
        pending.sort_by_key(|r| {
            (
                r.why != Some(Why::Critical),
                std::cmp::Reverse(r.last_hour(now_ms)),
            )
        });
        let (admin_set, triage_set) = self.knobs.with(|l| {
            (
                TurnSettings::read(l, Role::Admin),
                TurnSettings::read(l, Role::Triage),
            )
        });
        let held = |r: &mut IncidentRecord, why: &'static str, report: &mut PassReport| {
            r.held = Some(why.to_owned());
            sprig_ai::metrics::incident_held(why);
            *report.held.entry(why.to_owned()).or_default() += 1;
        };
        let mut sent = 0usize;
        for mut rec in pending {
            let Some((_, rev)) = self.store.get(&rec.fingerprint).await? else {
                continue;
            };
            if rec.stale_pending(now_ms) {
                rec.status = Status::Counting;
                rec.pending_since_ms = None;
                held(&mut rec, "expired", &mut report);
                self.store.put(&rec, Some(rev)).await?;
                continue;
            }
            let why = rec.why.unwrap_or(Why::New);
            let reason = if admin_set.model.trim().is_empty() {
                Some("no_model")
            } else if now_ms < st.budget_until_ms {
                Some("budget")
            } else if within_hour(&mut st.advice, now_ms) >= knobs.max_advice_hour {
                Some("advice_cap")
            } else if why != Why::Critical
                && within_hour(&mut st.triage, now_ms) >= knobs.max_triage_hour
            {
                Some("triage_cap")
            } else if sent >= MAX_PER_PASS {
                None
            } else {
                Some("")
            };
            match reason {
                None => continue,
                Some("") => {}
                Some(r) => {
                    if rec.held.as_deref() != Some(r) {
                        held(&mut rec, r, &mut report);
                        self.store.put(&rec, Some(rev)).await?;
                    }
                    continue;
                }
            }
            sent += 1;
            report.escalated += 1;
            if why != Why::Critical {
                st.triage.push_back(now_ms);
            }
            match self
                .escalate(&mut rec, why, &admin_set, &triage_set, now_ms)
                .await
            {
                Ok(Some(id)) => {
                    st.advice.push_back(now_ms);
                    report.advised.push(id);
                }
                Ok(None) => {}
                Err(e) if e.code() == Some(SprigCode::AiBudgetExceeded) => {
                    st.budget_until_ms = next_midnight(now_ms);
                    held(&mut rec, "budget", &mut report);
                }
                Err(e) if e.code() == Some(SprigCode::AiNotConfigured) => {
                    held(&mut rec, "no_model", &mut report);
                }
                Err(e) => {
                    tracing::warn!(fingerprint = %rec.fingerprint, error = %e, "incident advice failed");
                    rec.status = Status::Failed;
                    rec.escalated_ms = Some(now_ms);
                    rec.error = Some(e.message.clone());
                    held(&mut rec, "error", &mut report);
                }
            }
            rec.updated_ms = now_ms;
            // The record's counts may have moved on; keep this pass's
            // outcome over the newest copy.
            let mut tries = 0;
            let mut rev = Some(rev);
            loop {
                if self.store.put(&rec, rev).await? || tries == 2 {
                    break;
                }
                tries += 1;
                if let Some((cur, r)) = self.store.get(&rec.fingerprint).await? {
                    let outcome = rec.clone();
                    rec = cur;
                    copy_outcome(&mut rec, &outcome);
                    rev = Some(r);
                }
            }
        }
        Ok(report)
    }

    /// Triage and advice for `rec`; the item's id when advice was filed.
    async fn escalate(
        &self,
        rec: &mut IncidentRecord,
        why: Why,
        admin_set: &TurnSettings,
        triage_set: &TurnSettings,
        now_ms: u64,
    ) -> Result<Option<String>> {
        let (provider, model) = self.env.model(Role::Admin, &admin_set.model).await?;
        let admin = Model {
            provider,
            model,
            settings: admin_set.clone(),
        };
        let triage = if triage_set.model.trim().is_empty() {
            None
        } else {
            let (provider, model) = self.env.model(Role::Triage, &triage_set.model).await?;
            Some(Model {
                provider,
                model,
                settings: triage_set.clone(),
            })
        };
        let (audit, stored) = match self.env.audit().await {
            Ok(a) => (a, true),
            Err(e) => {
                tracing::warn!(error = %e, "incident advice runs unstored");
                (
                    Arc::new(MemoryAudit::system()) as Arc<dyn AuditStore>,
                    false,
                )
            }
        };
        let (topology, names) = self.env.topology().await?;
        let advisor = Advisor::new(audit, self.env.docs(), stored);
        let case = Case {
            incident: rec,
            why,
            topology: &topology,
            names: &names,
            now_ms,
        };
        let rep = advisor.advise(&case, triage.as_ref(), &admin).await?;
        rec.escalated_ms = Some(now_ms);
        rec.pending_since_ms = None;
        rec.held = None;
        rec.error = None;
        rec.intake = Some(rep.intake.clone());
        rec.sent.clone_from(&rep.sent);
        rec.conversation = rep.conversation.map(|c| c.0);
        rec.verdict = Some(rep.verdict.map_or("critical", |v| v.as_str()).to_owned());
        let Some(adv) = rep.advice else {
            rec.status = Status::Noise;
            return Ok(None);
        };
        let name = if rec.code_name.is_empty() {
            rec.code.to_string()
        } else {
            rec.code_name.clone()
        };
        rec.summary = Some(format!(
            "{name} in {} {}: {}",
            rec.component, rec.span, adv.cause
        ));
        let mut text = sprig_ai::incidents::brief::clip(&adv.text, MAX_ADVICE);
        if !rep.sources.is_empty() {
            let docs: Vec<String> = rep
                .sources
                .iter()
                .map(|s| format!("{} ({})", s.title, s.reference))
                .collect();
            text.push_str("\n\nDocs searched: ");
            text.push_str(&docs.join("; "));
        }
        rec.advice = Some(text);
        rec.severity = Some(adv.severity.clone());
        rec.status = Status::Advised;
        let id = self.raise(rec, now_ms).await?;
        rec.advice_id = Some(id.clone());
        sprig_ai::metrics::incident(&adv.severity);
        Ok(Some(id))
    }

    /// Files or refreshes `rec`'s advice item; its id.
    async fn raise(&self, rec: &IncidentRecord, now_ms: u64) -> Result<String> {
        #[allow(clippy::cast_precision_loss)]
        let last = rec.last_hour(now_ms) as f64;
        raise_advisory(
            &self.hub,
            &self.dc,
            Advisory {
                kind: Kind::AiIncidentAdvice,
                target: format!("incident/{}", rec.fingerprint),
                text: format!(
                    "{}\n\nAdvice only: nothing is changed. What was sent: sprigctl ai incident show {} --payload",
                    rec.advice.as_deref().unwrap_or(""),
                    rec.fingerprint
                ),
                machine: json!({"fingerprint": rec.fingerprint}),
                evidence: json!({
                    "fingerprint": rec.fingerprint,
                    "code": rec.code,
                    "code_name": rec.code_name,
                    "component": rec.component,
                    "span": rec.span,
                    "last_hour": rec.last_hour(now_ms),
                    "baseline_per_hour": rec.baseline(now_ms),
                    "total": rec.total,
                    "versions": rec.versions,
                    "severity": rec.severity,
                    "conversation": rec.conversation.map(|c| format!("a{c}")),
                    "sent_sha256": rec.sent,
                }),
                score: last,
                summary: rec.summary.clone().unwrap_or_default(),
            },
        )
        .await
    }
}

/// An advisory item of the administration agent.
#[derive(Debug, Clone)]
pub struct Advisory {
    /// `ai_incident_advice` or `ai_health_warning`.
    pub kind: Kind,
    /// What it is about (`incident/<fingerprint>`, `health/<key>`,
    /// `writeup/<id>`): the same target refreshes the same item.
    pub target: String,
    /// The whole text.
    pub text: String,
    /// What a machine needs to find the record again.
    pub machine: Value,
    /// Counts and ids, never names.
    pub evidence: Value,
    /// Ranks items in the inbox.
    pub score: f64,
    /// One line.
    pub summary: String,
}

/// Files `a` as a dc-level advisory item of `dc` (advice only, no timers),
/// or refreshes the live item with the same kind and target; its id.
///
/// # Errors
/// The approvals hub's; internal when it answers anything but advice.
pub async fn raise_advisory(hub: &Approvals, dc: &DcId, a: Advisory) -> Result<String> {
    let p = Proposal {
        kind: a.kind,
        subject: Subject {
            dc: Some(dc.clone()),
            ..Subject::default()
        },
        target: a.target,
        action: Action {
            text: a.text,
            machine: a.machine,
        },
        evidence: a.evidence,
        score: a.score,
        summary: a.summary,
        emergency: false,
    };
    let policy = Policy::fixed(Mode::Recommend, Level::Dc);
    match hub.check(&p, &policy, &[]).await? {
        Verdict::Advice(id) | Verdict::Suppressed(id) | Verdict::Wait(id) => Ok(id.to_string()),
        v => Err(SprigError::internal(format!(
            "advisory item: the approvals gate answered {v:?}"
        ))),
    }
}

/// Copies what an escalation decided onto a newer copy of the record.
fn copy_outcome(to: &mut IncidentRecord, from: &IncidentRecord) {
    to.status = from.status;
    to.why = from.why;
    to.pending_since_ms = from.pending_since_ms;
    to.held.clone_from(&from.held);
    to.escalated_ms = from.escalated_ms;
    to.verdict.clone_from(&from.verdict);
    to.severity.clone_from(&from.severity);
    to.summary.clone_from(&from.summary);
    to.advice.clone_from(&from.advice);
    to.advice_id.clone_from(&from.advice_id);
    to.conversation = from.conversation;
    to.sent.clone_from(&from.sent);
    to.intake.clone_from(&from.intake);
    to.error.clone_from(&from.error);
}

#[async_trait::async_trait]
impl Reconciler for IncidentLoop {
    type Key = String;

    fn name(&self) -> &'static str {
        "ai_incidents"
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
        self.pass(intake::now_ms()).await?;
        Ok(Outcome::RequeueAfter(PASS_INTERVAL))
    }
}

/// Approving an advice item acknowledges it: the approvals loop marks it
/// applied with nothing changed.
#[derive(Debug, Default)]
pub struct Acknowledge;

#[async_trait::async_trait]
impl Applier for Acknowledge {
    async fn revalidate(&self, _rec: &Recommendation) -> Result<Option<String>> {
        Ok(None)
    }
    async fn apply(&self, _rec: &Recommendation) -> Result<()> {
        Ok(())
    }
    async fn progress(&self, _rec: &Recommendation) -> Result<Option<(bool, String)>> {
        Ok(Some((
            true,
            "acknowledged: advice changes nothing by itself".into(),
        )))
    }
}

// ---- the routes ----

/// The incident routes' state.
pub struct IncidentsApi {
    store: IncidentStore,
    env: Arc<dyn IncidentEnv>,
}

impl std::fmt::Debug for IncidentsApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncidentsApi").finish_non_exhaustive()
    }
}

impl IncidentsApi {
    /// Incidents in `dc_store`, conversations through `env`.
    #[must_use]
    pub fn new(dc_store: Arc<dyn MetaStore>, env: Arc<dyn IncidentEnv>) -> Self {
        Self {
            store: IncidentStore::new(dc_store),
            env,
        }
    }

    /// Every incident, newest first, without its intake and advice text.
    ///
    /// # Errors
    /// The store's.
    pub async fn list(&self) -> Result<Value> {
        let all = self.store.list().await?;
        let rows: Vec<Value> = all
            .iter()
            .map(|r| {
                json!({
                    "fingerprint": r.fingerprint,
                    "code": r.code,
                    "code_name": r.code_name,
                    "component": r.component,
                    "span": r.span,
                    "status": r.status,
                    "total": r.total,
                    "last_hour": r.last_hour(intake::now_ms()),
                    "first_seen_ms": r.first_seen_ms,
                    "last_seen_ms": r.last_seen_ms,
                    "verdict": r.verdict,
                    "severity": r.severity,
                    "summary": r.summary,
                    "advice_id": r.advice_id,
                    "held": r.held,
                })
            })
            .collect();
        Ok(json!({"incidents": rows}))
    }

    /// One incident; with `payloads`, its conversation's messages
    /// including the exact payloads sent.
    ///
    /// # Errors
    /// 39792 for an unknown fingerprint; the store's.
    pub async fn show(&self, fp: &str, payloads: bool) -> Result<Value> {
        if fp.is_empty() || !fp.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid(format!("{fp:?} is not an incident fingerprint")));
        }
        let (rec, _) = self
            .store
            .get(fp)
            .await?
            .ok_or_else(|| invalid(format!("there is no incident {fp}")))?;
        let mut v = json!({
            "incident": rec,
            "last_hour": rec.last_hour(intake::now_ms()),
            "baseline_per_hour": rec.baseline(intake::now_ms()),
        });
        if let Some(c) = rec.conversation {
            v["conversation"] = match self.conversation(c, payloads).await {
                Ok(m) => json!({"id": format!("a{c}"), "messages": m}),
                Err(e) => json!({"id": format!("a{c}"), "unavailable": e.message}),
            };
        }
        Ok(v)
    }

    async fn conversation(&self, id: u64, payloads: bool) -> Result<Vec<Value>> {
        let audit = self.env.audit().await?;
        Ok(audit
            .messages(sprig_ai::ConvId(id))
            .await?
            .into_iter()
            .map(|m| {
                let mut body = m.body;
                if !payloads && let Some(o) = body.as_object_mut() {
                    o.remove("payload");
                }
                json!({
                    "id": format!("a{}", m.id),
                    "kind": m.kind.as_str(),
                    "body": body,
                    "sent_sha256": m.sent_hash.map(|h| sprig_ai::provider::hex(&h)),
                    "created_ms": m.created_ms,
                })
            })
            .collect())
    }
}

/// The routes.
pub fn router(api: Arc<IncidentsApi>) -> Router {
    Router::new()
        .route("/admin/v1/ai/incidents", get(list))
        .route("/admin/v1/ai/incidents/{fingerprint}", get(show))
        .with_state(api)
}

async fn list(State(api): State<Arc<IncidentsApi>>, headers: HeaderMap) -> Response {
    respond(
        async {
            principal(&headers)?;
            api.list().await
        }
        .await,
    )
}

async fn show(
    State(api): State<Arc<IncidentsApi>>,
    headers: HeaderMap,
    UrlPath(fp): UrlPath<String>,
    uri: Uri,
) -> Response {
    respond(
        async {
            principal(&headers)?;
            let payloads = uri
                .query()
                .unwrap_or("")
                .split('&')
                .any(|kv| matches!(kv, "payloads=true" | "payloads=1" | "payloads=yes"));
            api.show(&fp, payloads).await
        }
        .await,
    )
}

#[cfg(test)]
mod tests;
