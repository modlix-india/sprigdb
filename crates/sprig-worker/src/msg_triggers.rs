//! Triggers on queues, on the job runtime: `logic_queue_trigger`, one per
//! binding; `logic.trigger_lanes` loops of [`sprig_logic::msgrun::run_queue`],
//! each asking a gateway to run the queue's next message. A job reports
//! progress every second (its lease: a lost job stops), picks up a new
//! version within 5 s, and ends, removing its index entry, when the
//! trigger's queue (or the binding) is gone.
//!
//! **Gateways.** Runs go over the peer service (`sprig.internal.logic.v1`
//! on each gateway's `peer` port, mutual TLS with `[tls]`, plaintext on
//! loopback without it) to a ready gateway of this DC, the same one for a
//! binding while it answers (rendezvous on the binding), the next one when
//! it doesn't.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mysql_async::Pool;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sprig_api::proto::logic_peer::RunTriggerRequest;
use sprig_api::proto::logic_peer::logic_peer_client::LogicPeerClient;
use sprig_catalog::CatalogLocator;
use sprig_catalog::pool_wait::Bounded as _;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_controller::catalogs::{ClusterPools, ShardMapLocator};
use sprig_controller::dispatch::JobClass;
use sprig_logic::LogicStore;
use sprig_logic::declare::SourceKind;
use sprig_logic::messages::{self, RunReply, RunRequest, SourceRow};
use sprig_logic::msgrun::{self, Gateway, MsgStats, Template};
use sprig_logic::triggers::RunOpts;
use sprig_meta::{ListOpts, MetaStore, Record, Versioned};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// sprig-queues' durable queues (`m/mq/queues/<id>` catalog records).
const MQ_QUEUES: sprig_catalog::RowTable = sprig_catalog::RowTable::new("mq", "queues");

use crate::runtime::{JobCtx, JobHandler};
use crate::settings::LogicScope;

/// How long one run may take before the peer call gives up (the run's
/// own budget is `logic.trigger_budget_ms`; this is the transport's).
const CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// How often a job looks for a new version or a dropped source.
const RELOAD_EVERY: Duration = Duration::from_secs(5);

/// The parts of a gateway's registration record a worker needs
/// (`sprig_gateway::registry::GatewayRecord`).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GwRecord {
    gateway_id: String,
    addr: String,
    #[serde(default)]
    listeners: BTreeMap<String, u16>,
    state: String,
}

impl Versioned for GwRecord {
    const V: u32 = 1;
}

/// The process identity for the peer service's mutual TLS.
#[derive(Clone)]
pub struct PeerPems {
    /// The internal CA.
    pub ca: Vec<u8>,
    /// This process's chain.
    pub cert: Vec<u8>,
    /// Its key.
    pub key: Vec<u8>,
}

impl PeerPems {
    /// Reads `[tls]`.
    ///
    /// # Errors
    /// 39998 for an unreadable file.
    pub fn from_config(cfg: &sprig_common::config::TlsConfig) -> Result<Self> {
        let read = |p: &std::path::Path| {
            std::fs::read(p).map_err(|e| {
                SprigError::new(
                    SprigCode::ConfigInvalid,
                    format!("[tls] {}: {e}", p.display()),
                )
            })
        };
        Ok(Self {
            ca: read(&cfg.ca)?,
            cert: read(&cfg.cert)?,
            key: read(&cfg.key)?,
        })
    }
}

/// Clients of this DC's gateways' `LogicPeer` service.
pub struct Gateways {
    dc: Arc<dyn MetaStore>,
    tls: Option<PeerPems>,
    channels: Mutex<HashMap<String, Channel>>,
    listed: Mutex<Option<(Instant, Vec<GwRecord>)>>,
}

impl std::fmt::Debug for Gateways {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateways")
            .field("mtls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

fn unreachable_gw(m: impl Into<String>) -> SprigError {
    let mut e = SprigError::new(SprigCode::ForwardFailed, m);
    e.retryable = true;
    e
}

impl Gateways {
    /// Gateways registered in `dc`, reached with `tls` (`None`: plaintext).
    #[must_use]
    pub fn new(dc: Arc<dyn MetaStore>, tls: Option<PeerPems>) -> Self {
        Self {
            dc,
            tls,
            channels: Mutex::new(HashMap::new()),
            listed: Mutex::new(None),
        }
    }

    /// Ready gateways with a peer port (the list is kept for 2 s).
    async fn ready(&self) -> Result<Vec<GwRecord>> {
        if let Some((at, v)) = self
            .listed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            && at.elapsed() < Duration::from_secs(2)
        {
            return Ok(v.clone());
        }
        let prefix = self.dc.tier().key("gateways/")?;
        let page = self
            .dc
            .list(
                &prefix,
                ListOpts {
                    limit: 10_000,
                    ..ListOpts::default()
                },
            )
            .await?;
        let v: Vec<GwRecord> = page
            .kvs
            .iter()
            .filter_map(|kv| Record::<GwRecord>::decode(&kv.value).ok())
            .map(|r| r.body)
            .filter(|g| g.state == "ready" && g.listeners.contains_key("peer"))
            .collect();
        *self.listed.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((Instant::now(), v.clone()));
        Ok(v)
    }

    fn client(&self, g: &GwRecord) -> Result<LogicPeerClient<Channel>> {
        self.channel(g).map(LogicPeerClient::new)
    }

    /// One `SearchSeed.SeedPage` on the first
    /// gateway, in rendezvous order for `seed`, that answers; the reply's
    /// JSON.
    ///
    /// # Errors
    /// 39788 (retryable) when none does.
    pub async fn seed_page(&self, seed: u64, request_json: String) -> Result<String> {
        use sprig_api::proto::internal::SeedPageRequest;
        use sprig_api::proto::internal::search_seed_client::SearchSeedClient;
        let mut gws = self.ready().await?;
        if gws.is_empty() {
            return Err(unreachable_gw(
                "no ready gateway with a peer port in this DC",
            ));
        }
        gws.sort_by_key(|g| std::cmp::Reverse(rendezvous(seed, &g.gateway_id)));
        let mut last = None;
        for g in &gws {
            let ch = match self.channel(g) {
                Ok(c) => c,
                Err(e) => {
                    last = Some(e);
                    continue;
                }
            };
            match SearchSeedClient::new(ch)
                .max_decoding_message_size(256 << 20)
                .seed_page(SeedPageRequest {
                    request_json: request_json.clone(),
                })
                .await
            {
                Ok(r) => return Ok(r.into_inner().reply_json),
                Err(s) => {
                    last = Some(unreachable_gw(format!(
                        "gateway {}: {}",
                        g.gateway_id,
                        s.message()
                    )));
                    *self.listed.lock().unwrap_or_else(PoisonError::into_inner) = None;
                }
            }
        }
        Err(last.unwrap_or_else(|| unreachable_gw("no gateway answered")))
    }

    fn channel(&self, g: &GwRecord) -> Result<Channel> {
        let port = g.listeners.get("peer").copied().unwrap_or(7403);
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}:{port}", g.addr);
        let mut channels = self.channels.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(c) = channels.get(&uri) {
            return Ok(c.clone());
        }
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| unreachable_gw(format!("peer address {uri}: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(CALL_TIMEOUT);
        if let Some(t) = &self.tls {
            let cfg = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(&t.ca))
                .identity(Identity::from_pem(&t.cert, &t.key))
                .domain_name(g.addr.clone());
            ep = ep
                .tls_config(cfg)
                .map_err(|e| unreachable_gw(format!("peer TLS: {e}")))?;
        }
        let ch = ep.connect_lazy();
        channels.insert(uri, ch.clone());
        Ok(ch)
    }

    /// One run on the first gateway, in rendezvous order for `seed`, that
    /// answers.
    ///
    /// # Errors
    /// 39788 (retryable) when none does.
    pub async fn run(&self, seed: u64, req: &RunRequest) -> Result<RunReply> {
        let mut gws = self.ready().await?;
        if gws.is_empty() {
            return Err(unreachable_gw(
                "no ready gateway with a peer port in this DC",
            ));
        }
        gws.sort_by_key(|g| std::cmp::Reverse(rendezvous(seed, &g.gateway_id)));
        let body = serde_json::to_string(req)
            .map_err(|e| SprigError::internal(format!("a trigger run: {e}")))?;
        let mut last = None;
        for g in &gws {
            let mut c = match self.client(g) {
                Ok(c) => c,
                Err(e) => {
                    last = Some(e);
                    continue;
                }
            };
            match c
                .run_trigger(RunTriggerRequest {
                    request_json: body.clone(),
                })
                .await
            {
                Ok(r) => {
                    return serde_json::from_str(&r.get_ref().reply_json)
                        .map_err(|e| SprigError::internal(format!("a trigger reply: {e}")));
                }
                Err(s) => {
                    last = Some(unreachable_gw(format!(
                        "gateway {}: {}",
                        g.gateway_id,
                        s.message()
                    )));
                    // The list may be stale.
                    *self.listed.lock().unwrap_or_else(PoisonError::into_inner) = None;
                }
            }
        }
        Err(last.unwrap_or_else(|| unreachable_gw("no gateway answered")))
    }
}

// Sink tasks' statements as their owners (queue sources,
// the script `db` handle), through the same gateways.
#[async_trait]
impl sprig_sinks::peer::SinkPeer for Gateways {
    async fn sql(
        &self,
        seed: u64,
        req: &sprig_sinks::peer::SqlRequest,
    ) -> Result<sprig_sinks::peer::SqlReply> {
        let mut gws = self.ready().await?;
        if gws.is_empty() {
            return Err(unreachable_gw(
                "no ready gateway with a peer port in this DC",
            ));
        }
        gws.sort_by_key(|g| std::cmp::Reverse(rendezvous(seed, &g.gateway_id)));
        let body = serde_json::to_string(req)
            .map_err(|e| SprigError::internal(format!("a sink request: {e}")))?;
        let mut last = None;
        for g in &gws {
            let mut c = match self.client(g) {
                Ok(c) => c,
                Err(e) => {
                    last = Some(e);
                    continue;
                }
            };
            match c
                .sink_sql(sprig_api::proto::logic_peer::SinkSqlRequest {
                    request_json: body.clone(),
                })
                .await
            {
                Ok(r) => {
                    return serde_json::from_str(&r.get_ref().reply_json)
                        .map_err(|e| SprigError::internal(format!("a sink reply: {e}")));
                }
                Err(s) => {
                    last = Some(unreachable_gw(format!(
                        "gateway {}: {}",
                        g.gateway_id,
                        s.message()
                    )));
                    *self.listed.lock().unwrap_or_else(PoisonError::into_inner) = None;
                }
            }
        }
        Err(last.unwrap_or_else(|| unreachable_gw("no gateway answered")))
    }
}

/// Highest-random-weight choice of a gateway for a binding.
fn rendezvous(seed: u64, id: &str) -> u64 {
    // FNV-1a over the seed and the id.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in seed.to_le_bytes().iter().chain(id.as_bytes()) {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// The runners' gateway: [`Gateways`] for one binding, and its run log.
struct GwClient {
    gws: Arc<Gateways>,
    seed: u64,
    log: RunLog,
    /// When "no gateway answers" was last logged.
    unreachable_noted: Mutex<Option<Instant>>,
}

/// How often a trigger whose runs reach no gateway says so.
const UNREACHABLE_NOTE_EVERY: Duration = Duration::from_secs(30);

/// Whether a note last made at `last` is due again at `now`; records `now`
/// when it is.
fn note_due(last: &mut Option<Instant>, now: Instant, every: Duration) -> bool {
    if last.is_some_and(|t| now.saturating_duration_since(t) < every) {
        return false;
    }
    *last = Some(now);
    true
}

/// Where a trigger's run log goes: `logic_log` in its tenant's catalog.
#[derive(Clone)]
struct RunLog {
    catalog: Pool,
    schema: String,
    module_id: u64,
    version: Arc<AtomicU64>,
}

impl RunLog {
    async fn write(&self, level: &str, message: &str) {
        let Ok(mut c) = self.catalog.get_conn_bounded().await else {
            return;
        };
        let _ = LogicStore::new(&self.schema)
            .log(
                &mut c,
                self.module_id,
                Some(self.version.load(Ordering::Relaxed)),
                level,
                "trigger",
                message,
            )
            .await;
    }
}

#[async_trait]
impl Gateway for GwClient {
    async fn run(&self, req: &RunRequest) -> Result<RunReply> {
        let r = self.gws.run(self.seed, req).await;
        // Runs that reach no gateway are retried without end; say so in
        // the worker's log and the trigger's run log, at most every 30 s,
        // instead of waiting silently (a gateway whose peer service is off
        // looks like this).
        if let Err(e) = &r
            && e.code() == Some(SprigCode::ForwardFailed)
            && note_due(
                &mut self
                    .unreachable_noted
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner),
                Instant::now(),
                UNREACHABLE_NOTE_EVERY,
            )
        {
            tracing::warn!(binding = self.seed, error = %e, "trigger runs reach no gateway; retrying");
            self.log
                .write(
                    "warn",
                    &format!("runs wait: no gateway answered ({})", e.message),
                )
                .await;
        }
        r
    }

    async fn note(&self, level: &str, message: &str) {
        self.log.write(level, message).await;
    }
}

/// What the message-trigger jobs share.
#[derive(Clone)]
pub struct MsgEnv {
    /// This DC's tier (the index).
    pub dc: Arc<dyn MetaStore>,
    /// Tenant catalogs.
    pub locator: Arc<ShardMapLocator>,
    /// `sprig_ctl` pools per cluster.
    pub pools: Arc<dyn ClusterPools>,
    /// The gateways runs go to.
    pub gateways: Arc<Gateways>,
    /// Retries, attempts, lanes (following the worker's live settings).
    pub opts: RunOpts,
    /// The worker's live settings: `logic.trigger_budget_ms` when a
    /// trigger job starts.
    pub settings: crate::settings::WorkerSettings,
}

impl std::fmt::Debug for MsgEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MsgEnv").finish_non_exhaustive()
    }
}

/// The `end` a job that stopped for `why` reports.
fn end_label(why: Unloaded) -> &'static str {
    match why {
        Unloaded::Gone => "gone",
        Unloaded::Disabled => "disabled",
    }
}

/// Why a job's binding didn't load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unloaded {
    /// The binding, its module, source or database is gone.
    Gone,
    /// The trigger is disabled (`ALTER TRIGGER ... DISABLE`).
    Disabled,
}

/// A job's binding as the catalog has it now.
struct Loaded {
    source: SourceRow,
    version: u64,
    catalog: Pool,
    schema: String,
}

fn spec_u64(spec: &Value, k: &str) -> std::result::Result<u64, String> {
    spec[k]
        .as_u64()
        .ok_or_else(|| format!("spec.{k} is missing"))
}

impl MsgEnv {
    /// Ends a job whose binding didn't load. A gone binding's index entry
    /// is removed. A disabled trigger's is left alone: `DISABLE` removed it
    /// already, and an `ENABLE` since may have put it back, which the
    /// controller then follows with a new job at once.
    async fn end_unloaded(&self, tenant: TenantId, binding: u64, why: Unloaded) {
        if why == Unloaded::Gone {
            let _ = messages::delete_index(self.dc.as_ref(), tenant, binding).await;
        }
    }

    /// A job's binding at its start, or (after [`Self::end_unloaded`])
    /// the job's end to report.
    async fn load_or_end(
        &self,
        tenant: TenantId,
        binding: u64,
    ) -> std::result::Result<std::result::Result<Loaded, Value>, String> {
        match self
            .load_or_why(tenant, binding)
            .await
            .map_err(|e| e.to_string())?
        {
            Ok(l) => Ok(Ok(l)),
            Err(why) => {
                self.end_unloaded(tenant, binding, why).await;
                Ok(Err(json!({"end": end_label(why), "binding": binding})))
            }
        }
    }

    /// The binding, or why it can't run now.
    async fn load_or_why(
        &self,
        tenant: TenantId,
        binding: u64,
    ) -> Result<std::result::Result<Loaded, Unloaded>> {
        let ep = self.locator.writer(tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn_bounded().await.map_err(|e| {
            SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
        })?;
        let Some(source) = messages::source(&mut c, &schema, binding).await? else {
            return Ok(Err(Unloaded::Gone));
        };
        let store = LogicStore::new(&schema);
        let b = store.binding_by_id(&mut c, binding).await?;
        if b.as_ref().is_none_or(|b| b.state != "active") {
            return Ok(Err(Unloaded::Gone));
        }
        let Some(m) = store.module_by_id(&mut c, source.module_id).await? else {
            return Ok(Err(Unloaded::Gone));
        };
        if m.state == "disabled" {
            return Ok(Err(Unloaded::Disabled));
        }
        if m.state != "active" {
            return Ok(Err(Unloaded::Gone));
        }
        let Some(version) = m.active_version else {
            return Ok(Err(Unloaded::Gone));
        };
        // The source's database (a dropped database's streams and queues
        // are gone, even when a new one has its name).
        let db_state =
            sprig_catalog::store::rec::CoreReader::new(crate::sinks::catalog_records(), tenant)
                .database(sprig_common::ids::DbId(source.db_id))
                .await?
                .map(|d| d.state);
        if db_state != Some(sprig_catalog::store::DbState::Active) {
            return Ok(Err(Unloaded::Gone));
        }
        // The source itself (a fan-in trigger's: its definition).
        let alive = match source.kind {
            // Triggers on streams ran only on streams kept in MariaDB.
            SourceKind::Stream => false,
            SourceKind::Queue => {
                // Durable queues are catalog records on the engine node.
                drop(c);
                let mut rec = crate::sinks::catalog_records().begin(tenant).await?;
                let q: Result<Option<Value>> = MQ_QUEUES
                    .get(&mut *rec, &sprig_catalog::Pk::new().u64(source.object_id))
                    .await;
                let _ = rec.rollback().await;
                q?.is_some_and(|q| {
                    q["state"] == "active" && q["db_id"].as_u64() == Some(u64::from(source.db_id))
                })
            }
        };
        if !alive {
            return Ok(Err(Unloaded::Gone));
        }
        Ok(Ok(Loaded {
            source,
            version,
            catalog: ep.pool.clone(),
            schema,
        }))
    }

    /// The stream's or queue's database's and the tenant's own
    /// `logic.trigger_*` rows over the etcd scopes, as after-commit
    /// triggers read them; the worker-wide options when the worker reads
    /// no tenant rows (tests).
    fn opts_for(&self, tenant: TenantId, l: &Loaded) -> RunOpts {
        if self.settings.tenant_scopes().is_some() {
            self.settings
                .run_opts_for(LogicScope::db(tenant, l.source.db_id))
        } else {
            self.opts.clone()
        }
    }

    /// The runs' fixed request, its budget followed at each run. A job
    /// calls it after [`crate::settings::WorkerSettings::refresh_tenant`],
    /// so its first runs see a budget set just before the trigger was
    /// created.
    fn template(&self, tenant: TenantId, l: &Loaded) -> Template {
        let scope = LogicScope::db(tenant, l.source.db_id);
        let budget = self.settings.trigger_budget_for(scope);
        Template {
            req: RunRequest {
                tenant: tenant.0,
                source: l.source.clone(),
                version: l.version,
                record: None,
                max_attempts: self.opts_for(tenant, l).current().max_attempts,
                lease_ms: 30_000.max(u64::try_from(budget.as_millis()).unwrap_or(30_000) + 5_000),
                budget_ms: u64::try_from(budget.as_millis()).unwrap_or(30_000),
                instance_tenant: 0,
                instance_stream: 0,
                replay: None,
            },
            version: Arc::new(AtomicU64::new(l.version)),
            budget: Some(self.settings.live_trigger_budget(scope)),
        }
    }

    /// Supervises a running trigger: progress every second, a new version
    /// or a dropped source every [`RELOAD_EVERY`]. Returns `true` when the
    /// trigger is gone (its index entry is then removed).
    #[allow(clippy::too_many_arguments)]
    async fn supervise(
        &self,
        cx: &JobCtx,
        tenant: TenantId,
        binding: u64,
        t: &Template,
        stats: &MsgStats,
        stop: &sprig_common::CancellationToken,
        gw: &Arc<dyn Gateway>,
    ) -> bool {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let mut last_reload = Instant::now();
        loop {
            tokio::select! {
                () = stop.cancelled() => return false,
                _ = tick.tick() => {}
            }
            let mut p = stats.json();
            p["binding"] = json!(binding);
            p["version"] = json!(t.version.load(Ordering::Relaxed));
            match cx.progress(p).await {
                Ok(true) => {}
                Ok(false) => {
                    stop.cancel();
                    return false;
                }
                Err(e) => tracing::warn!(error = %e, "trigger job progress not written"),
            }
            if last_reload.elapsed() >= RELOAD_EVERY {
                last_reload = Instant::now();
                match self.load_or_why(tenant, binding).await {
                    Ok(Ok(l)) => {
                        let old = t.version.swap(l.version, Ordering::Relaxed);
                        if old != l.version {
                            tracing::info!(%tenant, binding, version = l.version, "trigger: new version");
                        }
                        // Dead letters marked for replay.
                        if let Err(e) = replay_due(&l, t, gw.as_ref()).await {
                            tracing::warn!(%tenant, binding, error = %e, "trigger: dead-letter replay stopped; retried");
                        }
                    }
                    Ok(Err(why)) => {
                        let what = match why {
                            Unloaded::Gone => "trigger's binding or source is gone; its job ends",
                            Unloaded::Disabled => "trigger disabled; its job ends",
                        };
                        tracing::info!(%tenant, binding, "{what}");
                        self.end_unloaded(tenant, binding, why).await;
                        stop.cancel();
                        // Only a gone binding is "gone" (a fan-in trigger's
                        // record is removed then); a disabled one just stops.
                        return why == Unloaded::Gone;
                    }
                    Err(e) => tracing::debug!(error = %e, "trigger: catalog unreadable"),
                }
            }
        }
    }
}

/// Runs the binding's dead letters marked `replaying`, oldest
/// first, with the active version, each once (its marker in
/// [`messages::replay_group`]); `replayed` when it ran (or had); when one
/// fails, it (one more attempt) and every later one go back to `dead`, so
/// nothing runs out of order. Returns how many ran.
async fn replay_due(l: &Loaded, t: &Template, gw: &dyn Gateway) -> Result<u64> {
    // A catalog connection per step: none is held across a run, which
    // takes the gateway's time and the trigger's budget.
    let store = LogicStore::new(&l.schema);
    let rows = {
        let mut c = l.catalog.get_conn_bounded().await.map_err(db_err)?;
        store
            .dead_letters(&mut c, Some(l.source.binding_id), Some("replaying"), 100)
            .await?
    };
    if rows.is_empty() {
        return Ok(0);
    }
    let mut n = 0;
    for r in rows {
        let mut req = t.req.clone();
        req.version = t.version.load(Ordering::Relaxed);
        req.record = None;
        req.replay = Some(messages::ReplayRun {
            dl_id: r.dl_id,
            event: r.event.clone(),
        });
        let reply = gw.run(&req).await?;
        let mut c = l.catalog.get_conn_bounded().await.map_err(db_err)?;
        match reply.outcome {
            messages::RunOutcome::Ok | messages::RunOutcome::Moved => {
                store
                    .set_dead_letters(
                        &mut c,
                        Some(l.source.binding_id),
                        &[r.dl_id],
                        "replaying",
                        "replayed",
                    )
                    .await?;
                let _ = store
                    .log(
                        &mut c,
                        l.source.module_id,
                        Some(req.version),
                        "info",
                        "trigger",
                        &format!("dead letter {} ({}) replayed", r.dl_id, r.event_key),
                    )
                    .await;
                n += 1;
            }
            _ => {
                store
                    .put_dead_letter(
                        &mut c,
                        l.source.binding_id,
                        req.version,
                        &r.event_key,
                        b"",
                        &r.event,
                        1,
                        reply.code,
                        &reply.message,
                    )
                    .await?;
                // In order: nothing after it runs; the later ones go back
                // to `dead` with it, for the next replay.
                store
                    .set_dead_letters(&mut c, Some(l.source.binding_id), &[], "replaying", "dead")
                    .await?;
                tracing::warn!(
                    binding = l.source.binding_id,
                    dead_letter = r.dl_id,
                    code = reply.code,
                    "a replayed dead letter failed again; the replay stops there"
                );
                break;
            }
        }
    }
    if n > 0 {
        tracing::info!(
            binding = l.source.binding_id,
            replayed = n,
            "trigger dead letters replayed"
        );
    }
    Ok(n)
}

fn db_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

/// The `logic_queue_trigger` job.
pub struct QueueTrigger(pub MsgEnv);

#[async_trait]
impl JobHandler for QueueTrigger {
    fn kind(&self) -> &'static str {
        messages::QUEUE_JOB_KIND
    }

    fn class(&self) -> JobClass {
        JobClass::Consumer
    }

    async fn run(&self, cx: &JobCtx) -> std::result::Result<Value, String> {
        let env = &self.0;
        let tenant = TenantId(spec_u64(&cx.spec, "tenant")?);
        let binding = spec_u64(&cx.spec, "binding")?;
        let l = match env.load_or_end(tenant, binding).await? {
            Ok(l) => l,
            Err(end) => return Ok(end),
        };
        env.settings.refresh_tenant(tenant).await;
        let t = env.template(tenant, &l);
        let log = RunLog {
            catalog: l.catalog.clone(),
            schema: l.schema.clone(),
            module_id: l.source.module_id,
            version: Arc::clone(&t.version),
        };
        log.write(
            "info",
            &format!(
                "trigger on queue {} of vhost {} started (job {}, {} lanes)",
                l.source.name,
                l.source.db_name,
                cx.job_id,
                env.opts_for(tenant, &l).lanes
            ),
        )
        .await;
        let gw: Arc<dyn Gateway> = Arc::new(GwClient {
            gws: Arc::clone(&env.gateways),
            seed: binding,
            log,
            unreachable_noted: Mutex::new(None),
        });
        let stats = Arc::new(MsgStats::default());
        let stop = cx.cancel.child_token();
        tracing::info!(%tenant, binding, queue = %l.source.name, lanes = env.opts_for(tenant, &l).lanes, "trigger on a queue started");
        let runner = msgrun::run_queue(
            Arc::clone(&gw),
            t.clone(),
            env.opts_for(tenant, &l),
            Arc::clone(&stats),
            stop.clone(),
        );
        tokio::pin!(runner);
        let sup = env.supervise(cx, tenant, binding, &t, &stats, &stop, &gw);
        tokio::pin!(sup);
        let gone = tokio::select! {
            () = &mut runner => {
                stop.cancel();
                false
            }
            gone = &mut sup => {
                stop.cancel();
                runner.await;
                gone
            }
        };
        let mut out = stats.json();
        out["binding"] = json!(binding);
        if gone {
            out["end"] = json!("gone");
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendezvous_is_stable_and_spreads() {
        let a = rendezvous(7, "gw-1");
        assert_eq!(a, rendezvous(7, "gw-1"));
        let firsts: std::collections::HashSet<&str> = (0..64u64)
            .map(|b| {
                ["gw-1", "gw-2", "gw-3"]
                    .into_iter()
                    .max_by_key(|g| rendezvous(b, g))
                    .unwrap_or("gw-1")
            })
            .collect();
        assert_eq!(firsts.len(), 3, "bindings spread over the gateways");
    }

    #[test]
    fn gateway_records_parse() {
        let raw = br#"{"v":1,"gateway_id":"g1","slot":3,"version":"0.1","addr":"127.0.0.1","listeners":{"peer":7403,"mysql":3306},"state":"ready","load":{"connections":0,"active":0}}"#;
        let r = Record::<GwRecord>::decode(raw).unwrap();
        assert_eq!(r.body.listeners.get("peer"), Some(&7403));
        assert_eq!(r.body.state, "ready");
    }

    #[test]
    fn a_trigger_reaching_no_gateway_says_so_at_most_every_30_s() {
        let t0 = Instant::now();
        let mut last = None;
        assert!(note_due(&mut last, t0, UNREACHABLE_NOTE_EVERY));
        assert!(!note_due(
            &mut last,
            t0 + Duration::from_secs(29),
            UNREACHABLE_NOTE_EVERY
        ));
        assert!(note_due(
            &mut last,
            t0 + Duration::from_secs(30),
            UNREACHABLE_NOTE_EVERY
        ));
        assert!(!note_due(
            &mut last,
            t0 + Duration::from_secs(31),
            UNREACHABLE_NOTE_EVERY
        ));
    }
}
