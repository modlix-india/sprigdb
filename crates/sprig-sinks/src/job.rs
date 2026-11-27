//! One sink task in a worker (its record pipeline and task placement):
//! the `sink_task` job body.
//!
//! ```text
//! run_sink_task(tenant, sink, task):
//!   def := sinks row; gone, dropping or failed → end
//!   epoch := claim sink_tasks(sink, task) (bumped on every claim)
//!   open the connections: tenant credentials opened from sink_secrets with
//!     the tenant's sink key; platform credentials from files, after the grant
//!   destinations := sink.destination_allowlist (platform: the operator's hosts)
//!   target := webhook | kafka | s3 | database | script
//!   source := the stream's log from consumer group __sprig_sink_<id>
//!   runtime::run_task(...) until cancelled, fenced, dropped, or the
//!     definition changes (the controller starts the job again)
//! ```
//!
//! The job runtime's lease decides which worker runs a task; the epoch in
//! `sink_tasks` fences one that lost it (every settle checks it).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::MetaStore;

use crate::filter::SinkPrepare;
use crate::model::{
    ConnKind, ConnRef, Guarantee, SinkDef, SinkId, SinkState, SourceSpec, TargetKind, TaskEpoch,
    TaskNo, TaskState, err,
};
use crate::policy::{Destinations, Residency, Unpinned, parse_allowlist};
use crate::runtime::{
    Cancel, Control, DeadInfo, DeadLetters, Directive, Event, RunSettings, Target, TaskEnd,
    TaskInfo, TaskParts, run_task,
};
use crate::secrets::SinkKeyring;
use crate::store::SinksStore;
use crate::target::{Conn, MemSecrets, Opts, TargetCtx};

/// What a sink task needs from its worker.
#[async_trait]
pub trait SinkEnv: Send + Sync {
    /// The tenant catalog's pool and schema.
    async fn catalog(&self, tenant: TenantId) -> Result<(Pool, String)>;
    /// The engine's catalog records (sinks, tasks, dead letters).
    fn records(&self) -> Arc<dyn sprig_catalog::records::RecordStore>;
    /// A pool (as `sprig_ctl`) on the tenant's cluster, for its shards.
    async fn shard_pool(&self, tenant: TenantId) -> Result<Pool>;
    /// The DC tier.
    fn dc(&self) -> Arc<dyn MetaStore>;
    /// The global tier (platform connections).
    fn global(&self) -> Option<Arc<dyn MetaStore>>;
    /// The sink credential keyring.
    fn keyring(&self) -> Option<Arc<dyn SinkKeyring>>;
    /// The tenant's residency policy.
    fn residency(&self, _tenant: TenantId) -> Box<dyn Residency> {
        Box::new(Unpinned)
    }
    /// The tenant's residency policy as the directory holds it now (its
    /// own and its ancestors' policies); by default [`SinkEnv::residency`].
    ///
    /// # Errors
    /// The directory's.
    async fn residency_of(&self, tenant: TenantId) -> Result<Box<dyn Residency>> {
        Ok(self.residency(tenant))
    }
    /// `sink.platform_secret_dir`.
    fn secret_dir(&self) -> PathBuf {
        PathBuf::from("/etc/sprigdb/platform-secrets")
    }
    /// This worker's id.
    fn worker_id(&self) -> String;
    /// The worker's live settings (etcd scopes) and its DC: global
    /// `sink.*` values under the catalog's rows. `None`: the defaults.
    fn settings(&self) -> Option<(sprig_settings::Snapshot, sprig_common::ids::DcId)> {
        None
    }
    /// The DC's gateways, for statements as the sink's owner (queue
    /// sources, the script `db` handle). `None`: none here.
    fn peer(&self) -> Option<Arc<dyn crate::peer::SinkPeer>> {
        None
    }
    /// Whether row-level security policies on any of tables `objects` now bind
    /// user `owner` (the sink would export rows its owner can't see). By
    /// default `false`: workers without the tenant catalog rely on the
    /// gateway's pause.
    ///
    /// # Errors
    /// The catalog's.
    async fn row_policies_bind(
        &self,
        _tenant: TenantId,
        _owner: u32,
        _objects: &[u64],
    ) -> Result<bool> {
        Ok(false)
    }
    /// Why (39229) the sink's owner `owner` can no longer read `sources`
    /// (dropped, locked, or `SELECT` lost), or `None` while it can. By default
    /// `None`: workers without the tenant catalog leave it to the gateway's
    /// check at grant changes.
    ///
    /// # Errors
    /// The catalog's.
    async fn owner_lost(
        &self,
        _tenant: TenantId,
        _owner: u32,
        _sources: &[OwnerRead],
    ) -> Result<Option<String>> {
        Ok(None)
    }
}

/// The start of the pause reason row-level security gives a sink: a
/// running task stops at once, not after its buffered records.
pub const RLS_PAUSE: &str = "ER_SPRIG_RLS_UNSUPPORTED";

/// The start of the pause reason when the sink's owner can no longer read
/// its source: the sink resumes by itself once the owner can again.
pub const OWNER_PAUSE: &str = "39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST";

/// What a sink reads, for its owner's `SELECT` (39229): by database and
/// name, or a table by object id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerRead {
    /// A table, stream or queue of database `db` (a change topic `t.changes`
    /// reads as table `t`).
    Named {
        /// The database.
        db: u32,
        /// The name.
        name: String,
    },
    /// A table (`CHANGES OF t`).
    Object(u64),
    /// A fan-in sink's stream or queue, by database name and name.
    InDatabase {
        /// The database's name.
        db: String,
        /// The name.
        name: String,
    },
    /// `SPRIG_FAN_IN`, which a fan-in sink's owner needs besides `SELECT`.
    FanIn,
}

/// The sources `def`'s owner must be able to read. A fan-in sink's owner needs
/// `SPRIG_FAN_IN` and `SELECT` on the definition's name (the members'
/// instances are read under those).
#[must_use]
pub fn owner_reads(def: &SinkDef) -> Vec<OwnerRead> {
    match &def.source {
        SourceSpec::Stream { name, .. } => vec![OwnerRead::Named {
            db: def.db_id,
            name: name.strip_suffix(".changes").unwrap_or(name).to_owned(),
        }],
        SourceSpec::Queue { name, .. } => vec![OwnerRead::Named {
            db: def.db_id,
            name: name.clone(),
        }],
        SourceSpec::Changes { objects } => objects
            .iter()
            .map(|(id, _)| OwnerRead::Object(*id))
            .collect(),
        SourceSpec::FanIn { db, name, .. } | SourceSpec::FanInQueue { db, name, .. } => vec![
            OwnerRead::FanIn,
            OwnerRead::InDatabase {
                db: db.clone(),
                name: name.clone(),
            },
        ],
    }
}

/// The pause reason of an owner that lost access.
#[must_use]
pub fn owner_pause_reason(why: &str) -> String {
    format!("{OWNER_PAUSE}: {why}; the sink resumes once the owner can read its source again")
}

/// Who a sink's statements run as on a gateway: its owner, in its
/// tenant (by name) and database.
///
/// # Errors
/// The directory's (the tenant's name).
async fn run_as(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> Result<crate::peer::RunAs> {
    let global = env.global().ok_or_else(|| {
        SprigError::new(
            SprigCode::SinkOptionInvalid,
            "this worker has no global tier to name the sink's tenant",
        )
    })?;
    Ok(crate::peer::RunAs {
        tenant: tenant.0,
        tenant_name: crate::peer::tenant_name(global.as_ref(), tenant.0).await?,
        user_id: def.owner_user_id,
        database: opts.get("__db_name").unwrap_or("").to_owned(),
    })
}

/// The script `db` handle of a script sink in this worker: reads through
/// a gateway as the sink's owner. `None` for other targets
/// or without gateways.
async fn script_db(
    env: &dyn SinkEnv,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> Result<Option<Arc<dyn crate::target::script::ReadOnlyDb>>> {
    if def.target.kind() != TargetKind::Script {
        return Ok(None);
    }
    let Some(peer) = env.peer() else {
        return Ok(None);
    };
    let who = run_as(env, tenant, def, opts).await?;
    Ok(Some(Arc::new(crate::peer::PeerReadOnly::new(peer, who))))
}

/// `opts` with every `sink.*` key set at the global scope of `etcd` (in
/// `dc`) added where the catalog and the sink's own options don't set it:
/// object > database > tenant > `WITH` > global > default.
#[must_use]
pub fn over_global(
    mut opts: Opts,
    etcd: &sprig_settings::MemSettings<'_>,
    dc: &sprig_common::ids::DcId,
) -> Opts {
    let ctx = sprig_settings::SettingCtx::process(dc);
    for def in etcd.registry().iter() {
        let Some(short) = def.key().strip_prefix("sink.") else {
            continue;
        };
        if opts.0.contains_key(short) {
            continue;
        }
        let Ok(r) = etcd.resolve(def.key(), &ctx) else {
            continue;
        };
        if r.source == sprig_settings::ScopeKind::Default {
            continue;
        }
        let text = match r.value {
            sprig_settings::Value::Bool(b) => if b { "on" } else { "off" }.to_owned(),
            sprig_settings::Value::Int(i) => i.to_string(),
            sprig_settings::Value::Float(f) => f.to_string(),
            sprig_settings::Value::Str(t) => t,
        };
        opts.0.insert(short.to_owned(), text);
    }
    opts
}

/// The partitions task `task_no` reads, of a stream with `partitions`
/// partitions at `per_task` partitions a task (one under `EXACTLY ONCE`).
#[must_use]
pub fn task_partitions(partitions: u32, per_task: u32, task_no: u32) -> Vec<u32> {
    let k = per_task.max(1);
    (task_no * k..((task_no + 1) * k).min(partitions)).collect()
}

/// How many tasks a sink has.
#[must_use]
pub fn task_count(def: &SinkDef, per_task: u32) -> u32 {
    match &def.source {
        SourceSpec::Stream { partitions, .. } => {
            let k = if def.guarantee == Guarantee::ExactlyOnce {
                1
            } else {
                per_task.max(1)
            };
            partitions.div_ceil(k).max(1)
        }
        _ => 1,
    }
}

fn unquote(v: &str) -> String {
    serde_json::from_str::<Value>(v)
        .ok()
        .and_then(|j| match j {
            Value::String(s) => Some(s),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(if b { "on".into() } else { "off".into() }),
            _ => None,
        })
        .unwrap_or_else(|| v.trim().to_owned())
}

/// `sink.*` from the catalog's settings rows (tenant < database < the
/// sink's own scope), then the sink's `WITH (...)` values, keys without
/// `sink.`.
///
/// # Errors
/// The backend's.
pub async fn resolve_opts<Q: Queryable + Send>(
    q: &mut Q,
    catalog: &str,
    def: &SinkDef,
) -> Result<Opts> {
    let rows: Vec<(String, String, Option<String>)> = match q
        .exec(
            format!(
                "SELECT scope_kind, setting_key, CAST(value AS CHAR) FROM {}.settings \
                 WHERE setting_key LIKE 'sink.%' AND ((scope_kind = 'object' AND scope_id = ?) \
                 OR (scope_kind = 'database' AND scope_id = ?) OR scope_kind = 'tenant')",
                sprig_common::names::quote(catalog)
            ),
            (def.id.0, def.db_id),
        )
        .await
    {
        Ok(r) => r,
        // A tenant created on the engine has no MariaDB catalog schema:
        // no settings rows there.
        Err(mysql_async::Error::Server(e)) if matches!(e.code, 1049 | 1146) => Vec::new(),
        Err(e) => {
            return Err(
                SprigError::new(SprigCode::CatalogUnavailable, "catalog unavailable")
                    .with_source(e),
            );
        }
    };
    let rank = |k: &str| match k {
        "object" => 0,
        "database" => 1,
        _ => 2,
    };
    let mut best: BTreeMap<String, (u8, String)> = BTreeMap::new();
    for (kind, key, value) in rows {
        let Some(v) = value else { continue };
        let r = rank(&kind);
        let k = key.strip_prefix("sink.").unwrap_or(&key).to_owned();
        if best.get(&k).is_none_or(|(br, _)| r < *br) {
            best.insert(k, (r, unquote(&v)));
        }
    }
    let mut m: BTreeMap<String, String> = best.into_iter().map(|(k, (_, v))| (k, v)).collect();
    for (k, v) in &def.options {
        // A sink's own `WITH` decides only what one sink may decide: never
        // the destination rules or another key that is the operator's or
        // the tenant's as a whole.
        if crate::settings::with_may_set(k) {
            m.insert(k.clone(), v.clone());
        }
    }
    Ok(Opts(m))
}

/// The runtime settings from resolved options.
///
/// # Errors
/// 39218 for a bad number.
pub fn run_settings(o: &Opts, def: &SinkDef) -> Result<RunSettings> {
    let ms = |k: &str, d: u64| o.u64(k, d).map(Duration::from_millis);
    Ok(RunSettings {
        lanes: u16::try_from(o.u64("lanes", 8)?).unwrap_or(8),
        batch_max_records: usize::try_from(o.u64("batch_max_records", 500)?).unwrap_or(500),
        batch_max_bytes: o.u64("batch_max_bytes", 4 << 20)?,
        batch_max_wait: ms("batch_max_wait_ms", 1000)?,
        max_inflight_bytes: o.u64("max_inflight_bytes", 64 << 20)?,
        retry_initial: ms("retry_initial_ms", 500)?,
        retry_max: ms("retry_max_ms", 60_000)?,
        record_max_attempts: u32::try_from(o.u64("record_max_attempts", 10)?).unwrap_or(10),
        pause_after: Duration::from_secs(o.u64("pause_after_minutes", 15)? * 60),
        auto_resume: o.flag("auto_resume", true),
        probe_interval: Duration::from_secs(o.u64("probe_interval_seconds", 60)?),
        delivery_timeout: ms("http_timeout_ms", 30_000)?.max(Duration::from_secs(1))
            + Duration::from_secs(5),
        tenant_bytes_per_sec: o.u64("tenant_bytes_per_sec", 0)?,
        settle_every: Duration::from_millis(200),
        poll_every: Duration::from_secs(1),
        exactly_once: def.guarantee == Guarantee::ExactlyOnce,
    })
}

/// Dead letters into the sink's dead-letter records.
struct CatalogDead {
    store: SinksStore,
    sink: SinkId,
}

#[async_trait]
impl DeadLetters for CatalogDead {
    async fn write(&self, rec: &crate::model::SinkRecord, info: &DeadInfo) -> Result<()> {
        self.store.insert_dead(self.sink, rec, info).await
    }
}

/// Dead letters published to an engine stream (`DEAD LETTER TO STREAM`)
/// through a gateway as the sink's owner: the record's key, value and
/// headers, plus `sprig.dl.reason`, `sprig.dl.class`, `sprig.dl.attempts`,
/// `sprig.dl.sink` and `sprig.dl.position`.
struct StreamDead {
    peer: Arc<dyn crate::peer::SinkPeer>,
    run_as: crate::peer::RunAs,
    db: String,
    stream: String,
    sink: SinkId,
}

#[async_trait]
impl DeadLetters for StreamDead {
    async fn write(&self, rec: &crate::model::SinkRecord, info: &DeadInfo) -> Result<()> {
        use base64::Engine as _;
        let b64 = |b: &[u8]| base64::engine::general_purpose::STANDARD.encode(b);
        let mut headers: Vec<(String, String)> = rec
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
            .collect();
        headers.push(("sprig.dl.reason".into(), info.reason.clone()));
        headers.push(("sprig.dl.class".into(), info.class.to_owned()));
        headers.push(("sprig.dl.attempts".into(), info.attempts.to_string()));
        headers.push(("sprig.dl.sink".into(), self.sink.0.to_string()));
        headers.push(("sprig.dl.position".into(), rec.pos.text()));
        // A change record's value is its envelope; its key the row's key.
        let value = rec
            .value
            .clone()
            .or_else(|| rec.after.as_ref().map(|a| a.to_string().into_bytes()));
        let op = crate::peer::SqlOp::StreamPublish {
            db: self.db.clone(),
            stream: self.stream.clone(),
            key: rec.key.as_deref().map(b64),
            value: value.as_deref().map(b64),
            headers,
        };
        self.peer
            .sql(self.sink.0, &self.run_as.request(op))
            .await?
            .ok()?;
        Ok(())
    }
}

/// Where a task's dead letters go: the stream `DEAD LETTER TO STREAM`
/// names, else the sink's dead-letter records.
async fn dead_letters(
    env: &Arc<dyn SinkEnv>,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
    store: &SinksStore,
    sink: SinkId,
) -> Result<Arc<dyn DeadLetters>> {
    if let Some(crate::model::DeadLetter::Stream { name, .. }) = &def.dead_letter {
        let peer = env.peer().ok_or_else(|| {
            err(
                SprigCode::SinkOptionInvalid,
                "DEAD LETTER TO STREAM needs the DC's gateways, and this worker reaches none",
            )
        })?;
        let who = run_as(env.as_ref(), tenant, def, opts).await?;
        return Ok(Arc::new(StreamDead {
            peer,
            db: who.database.clone(),
            run_as: who,
            stream: name.clone(),
            sink,
        }));
    }
    Ok(Arc::new(CatalogDead {
        store: store.clone(),
        sink,
    }))
}

/// The catalog side of a task: directives, state reports, the epoch.
struct CatalogControl {
    store: SinksStore,
    sink: SinkId,
    task: TaskNo,
    epoch: TaskEpoch,
    definition_ver: u64,
    delivered: AtomicU64,
    /// A change sink's owner and tables, checked before each batch (every
    /// poll).
    rls: Option<(Arc<dyn SinkEnv>, TenantId, u32, Vec<u64>)>,
    /// A grouped fan-in task's check of its sink's task count (see
    /// [`Regroup`]).
    regroup: Option<Regroup>,
    /// The owner's access to the source (39229), checked at most every
    /// second before a batch.
    owner: Option<OwnerCheck>,
}

/// A task's check of its sink owner's access (see
/// [`SinkEnv::owner_lost`]).
struct OwnerCheck {
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    owner: u32,
    sources: Vec<OwnerRead>,
    next: std::sync::Mutex<Option<std::time::Instant>>,
}

impl OwnerCheck {
    fn new(env: &Arc<dyn SinkEnv>, tenant: TenantId, def: &SinkDef) -> Option<Self> {
        let sources = owner_reads(def);
        (!sources.is_empty()).then(|| Self {
            env: Arc::clone(env),
            tenant,
            owner: def.owner_user_id,
            sources,
            next: std::sync::Mutex::new(None),
        })
    }

    /// Why the owner lost access, checked now when `now` or once a second
    /// has passed since the last check.
    async fn lost(&self, now: bool) -> Result<Option<String>> {
        let t = std::time::Instant::now();
        {
            let mut n = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !now && n.is_some_and(|at| t < at) {
                return Ok(None);
            }
            *n = Some(t + Duration::from_secs(1));
        }
        self.env
            .owner_lost(self.tenant, self.owner, &self.sources)
            .await
    }
}

/// A fan-in task started as one of `of` tasks: it stops once the sink's
/// index entry has another count (the controller regrouped the sink), so
/// the new tasks start after it ends. The entry is read at most every 5 s.
struct Regroup {
    dc: Arc<dyn sprig_meta::MetaStore>,
    tenant: u64,
    sink: u64,
    of: u32,
    next: std::sync::Mutex<Option<std::time::Instant>>,
}

impl Regroup {
    /// Whether the task should stop now (see the type's docs).
    async fn moved(&self) -> bool {
        let now = std::time::Instant::now();
        {
            let mut n = self
                .next
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if n.is_some_and(|t| now < t) {
                return false;
            }
            *n = Some(now + Duration::from_secs(5));
        }
        match crate::meta::index_get(&self.dc, self.tenant, self.sink).await {
            Ok(Some(ix)) => ix.tasks != self.of,
            // Gone: the catalog state stops the task.
            Ok(None) | Err(_) => false,
        }
    }
}

/// The directive that stops a task, from its sink's catalog state: gone,
/// redefined, failed, dropping, or paused by row-level security (a
/// running task stops at once, its buffered records undelivered).
fn stop_directive(
    state: Option<&(SinkState, Option<String>, u64)>,
    definition_ver: u64,
) -> Option<Directive> {
    match state {
        None => Some(Directive::Stop),
        Some((_, _, v)) if *v != definition_ver => Some(Directive::Stop),
        Some((SinkState::Failed | SinkState::Dropping, _, _)) => Some(Directive::Stop),
        Some((SinkState::Paused, r, _))
            if r.as_deref().is_some_and(|r| r.starts_with(RLS_PAUSE)) =>
        {
            Some(Directive::Stop)
        }
        _ => None,
    }
}

impl CatalogControl {
    /// Whether policies now bind the sink's owner on its tables; if
    /// so the sink is paused (state `paused`, reason 39366), as the
    /// gateway's policy statements do.
    async fn rls_bound(&self) -> Result<bool> {
        let Some((env, tenant, owner, objects)) = &self.rls else {
            return Ok(false);
        };
        if !env.row_policies_bind(*tenant, *owner, objects).await? {
            return Ok(false);
        }
        self.store
            .set_state(
                self.sink,
                SinkState::Paused,
                Some(&format!(
                    "{RLS_PAUSE}: row-level security on a source table binds the sink's owner; \
                     the sink would export rows the owner can't see"
                )),
            )
            .await?;
        tracing::warn!(tenant = %tenant, sink = self.sink.0, "sink paused by its worker: its owner is bound by row-level security");
        Ok(true)
    }
}

impl CatalogControl {
    /// A sink paused (39229) because its owner lost access resumes once
    /// the owner can read its source again
    /// (`Some(Run)`), else stays paused (`Some(Stop)`: the controller
    /// starts the task again a few seconds later, which checks again). A
    /// running sink whose owner lost access is paused (`Some(Stop)`).
    async fn owner_directive(
        &self,
        state: Option<&(SinkState, Option<String>, u64)>,
    ) -> Result<Option<Directive>> {
        let Some(check) = &self.owner else {
            return Ok(None);
        };
        let paused = matches!(
            state,
            Some((SinkState::Paused, Some(r), v))
                if r.starts_with(OWNER_PAUSE) && *v == self.definition_ver
        );
        if paused {
            if check.lost(true).await?.is_some() {
                // Still paused: shown wherever its task is started again.
                metrics::gauge!("sprig_sink_paused", "reason" => "owner_privilege").set(1.0);
                return Ok(Some(Directive::Stop));
            }
            self.store
                .set_state(self.sink, SinkState::Running, None)
                .await?;
            metrics::gauge!("sprig_sink_paused", "reason" => "owner_privilege").set(0.0);
            tracing::info!(tenant = %check.tenant, sink = self.sink.0, "sink resumed: its owner can read its source again");
            return Ok(Some(Directive::Run));
        }
        if !matches!(state, Some((SinkState::Running | SinkState::Failing, _, _))) {
            return Ok(None);
        }
        let Some(why) = check.lost(false).await? else {
            return Ok(None);
        };
        let reason = owner_pause_reason(&why);
        self.store
            .set_state(self.sink, SinkState::Paused, Some(&reason))
            .await?;
        self.store
            .report_task(
                self.sink,
                self.task,
                self.epoch,
                TaskState::Paused,
                Some(&reason),
                0,
            )
            .await?;
        // `reason="owner_privilege"`, so operators can alert on 39229
        // pauses apart from the failures ones (SprigSinkPaused).
        metrics::gauge!("sprig_sink_paused", "reason" => "owner_privilege").set(1.0);
        tracing::warn!(tenant = %check.tenant, sink = self.sink.0, reason = %why, code = 39229, "SprigSinkPaused: the sink's owner lost access to its source");
        Ok(Some(Directive::Stop))
    }
}

#[async_trait]
impl Control for CatalogControl {
    async fn poll(&self) -> Result<Directive> {
        let state = self.store.state_of(self.sink).await?;
        // The owner's access (39229), before anything runs.
        if let Some(d) = self.owner_directive(state.as_ref()).await? {
            return Ok(d);
        }
        if let Some(d) = stop_directive(state.as_ref(), self.definition_ver) {
            return Ok(d);
        }
        // Row-level security now binds the owner (checked each poll,
        // so before each batch is cut).
        if self.rls_bound().await? {
            return Ok(Directive::Stop);
        }
        if let Some(g) = &self.regroup
            && g.moved().await
        {
            tracing::info!(
                sink = self.sink.0,
                task = self.task.0,
                of = g.of,
                "fan-in sink regrouped: task stops"
            );
            return Ok(Directive::Stop);
        }
        Ok(match state {
            Some((SinkState::Paused, r, _))
                if r.as_deref().is_some_and(|r| r.starts_with("user")) =>
            {
                Directive::Pause
            }
            _ => Directive::Run,
        })
    }

    async fn report(&self, ev: Event) -> Result<()> {
        match ev {
            Event::Delivered(n) => {
                // At most every few seconds (the runtime batches reports).
                let total = self.delivered.fetch_add(n, Ordering::Relaxed) + n;
                let _ = total;
                self.store
                    .report_task(
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Running,
                        None,
                        n,
                    )
                    .await?;
            }
            Event::Failing(why) => {
                self.store
                    .set_state(self.sink, SinkState::Failing, Some(&why))
                    .await?;
                self.store
                    .report_task(
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Retrying,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
            Event::PausedByFailure(why) => {
                tracing::warn!(sink = self.sink.0, reason = %why, "SprigSinkPaused: paused after failures (39222)");
                self.store
                    .set_state(
                        self.sink,
                        SinkState::Paused,
                        Some(&format!("failures (39222): {why}")),
                    )
                    .await?;
                self.store
                    .report_task(
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Paused,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
            Event::Recovered => {
                self.store
                    .set_state(self.sink, SinkState::Running, None)
                    .await?;
                self.store
                    .report_task(
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Running,
                        None,
                        0,
                    )
                    .await?;
            }
            Event::SourceLost(why) => {
                self.store
                    .set_state(self.sink, SinkState::Failed, Some(&format!("39223: {why}")))
                    .await?;
                self.store
                    .report_task(
                        self.sink,
                        self.task,
                        self.epoch,
                        TaskState::Failed,
                        Some(&why),
                        0,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn check_epoch(&self) -> Result<()> {
        match self.store.task_epoch(self.sink, self.task).await? {
            Some(e) if e == self.epoch => Ok(()),
            _ => Err(err(
                SprigCode::SinkFenced,
                format!(
                    "task {} of sink {} has a newer epoch",
                    self.task.0, self.sink.0
                ),
            )),
        }
    }
}

/// Opens one connection: options, credentials, destination rules (also
/// approval webhooks through a tenant's connection).
///
/// # Errors
/// 39210 for a connection that is gone; 39216 for credentials that don't
/// open; 39232 for a platform connection not granted to the tenant.
pub async fn open_conn(
    env: &dyn SinkEnv,
    tenant: TenantId,
    store: &SinksStore,
    r: &ConnRef,
    opts: &Opts,
) -> Result<Conn> {
    let allow = parse_allowlist(opts.get("destination_allowlist").unwrap_or("[]"))?;
    let metadata_allowed = opts.get("platform_identity") == Some("full");
    let internal = sprig_common::outbound::InternalAllow::parse_or_none(
        opts.get("internal_destinations").unwrap_or("[]"),
    );
    match r {
        ConnRef::Tenant { id, name } => {
            let def = store.connection(*id).await?.ok_or_else(|| {
                err(
                    SprigCode::SinkNotFound,
                    format!("connection {name} is gone"),
                )
            })?;
            let mut secrets = MemSecrets::default();
            let sealed = store
                .sealed_secrets(*id, opts.u64("credential_previous_hours", 24)?)
                .await?;
            if !sealed.is_empty() {
                let keys = env.keyring().ok_or_else(|| {
                    err(
                        SprigCode::SinkCredentialsInvalid,
                        "this worker has no sink credential key file",
                    )
                })?;
                for (n, v, kv, s, previous) in sealed {
                    let value = crate::secrets::open(keys.as_ref(), tenant, id.0, &n, v, kv, &s)?;
                    if previous {
                        secrets.set_previous(&n, value);
                    } else {
                        secrets.set(&n, value);
                    }
                }
            }
            Ok(Conn {
                name: def.name.clone(),
                kind: def.kind,
                options: def.options.clone(),
                secrets: Arc::new(secrets),
                dest: Arc::new(Destinations {
                    allow,
                    residency: env.residency_of(tenant).await?,
                    platform_hosts: None,
                    metadata_allowed,
                    internal,
                }),
            })
        }
        ConnRef::Platform { name } => {
            let global = env.global().ok_or_else(|| {
                err(
                    SprigCode::SinkPlatformConnectionNotGranted,
                    "no global tier here",
                )
            })?;
            let p = crate::meta::platform_get(&global, name)
                .await?
                .ok_or_else(|| {
                    err(
                        SprigCode::SinkNotFound,
                        format!("no platform connection {name}"),
                    )
                })?;
            if !crate::meta::platform_granted(&global, name, tenant.0, &[]).await? {
                metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "not_granted")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkPlatformConnectionNotGranted,
                    format!("platform connection {name} isn't granted to this tenant"),
                ));
            }
            let dir = env.secret_dir();
            let mut secrets = MemSecrets::default();
            for (n, reference) in &p.secrets {
                match crate::meta::read_secret_file(&dir, reference) {
                    Ok(v) => secrets.set(n, v),
                    Err(e) => {
                        metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "secret_unavailable")
                            .increment(1);
                        tracing::error!(connection = %name, error = %e, "platform secret unavailable (39234)");
                        return Err(e);
                    }
                }
            }
            for (n, reference) in &p.previous {
                if let Ok(v) = crate::meta::read_secret_file(&dir, reference) {
                    secrets.set_previous(n, v);
                }
            }
            metrics::counter!("sprig_sink_platform_connection_opens_total", "result" => "ok")
                .increment(1);
            let mut options = p.options.clone();
            options.insert("__platform_tags".into(), Value::Object(p.tags.clone()));
            Ok(Conn {
                name: p.name.clone(),
                kind: p.kind,
                options,
                secrets: Arc::new(secrets),
                dest: Arc::new(Destinations {
                    allow: Vec::new(),
                    residency: env.residency_of(tenant).await?,
                    platform_hosts: Some(p.hosts.clone()),
                    metadata_allowed,
                    internal,
                }),
            })
        }
    }
}

/// The sink's `TRANSFORM` function, compiled at task start (its active
/// version then; a later deploy reaches the next task start).
///
/// # Errors
/// 39219 for a function that is gone or can't be a transform.
pub(crate) async fn load_transform(
    records: &dyn sprig_catalog::records::RecordStore,
    tenant: TenantId,
    def: &SinkDef,
    opts: &Opts,
) -> Result<Option<Arc<crate::transform::Transform>>> {
    let Some(f) = &def.transform else {
        return Ok(None);
    };
    let mut c = records.begin(tenant).await?;
    let budget = Duration::from_millis(opts.u64("transform_budget_ms", 50)?);
    // `transform_version = n` pins a version (0 or absent: the active
    // one, followed at each task start); `EXACTLY ONCE` needs it
    // `DETERMINISTIC`.
    let version = opts.u64("transform_version", 0)?;
    crate::transform::Transform::load_at(
        &mut *c,
        def.db_id,
        f,
        (version > 0).then_some(version),
        def.guarantee == Guarantee::ExactlyOnce,
        budget,
    )
    .await
    .map(Some)
}

/// Builds the target of a sink.
///
/// # Errors
/// The target's.
pub async fn build_target(ctx: &TargetCtx) -> Result<Box<dyn Target>> {
    build_target_with(ctx, None).await
}

/// [`build_target`] with a script sink's `db` handle.
///
/// # Errors
/// The target's.
pub async fn build_target_with(
    ctx: &TargetCtx,
    db: Option<Arc<dyn crate::target::script::ReadOnlyDb>>,
) -> Result<Box<dyn Target>> {
    match ctx.sink.target.kind() {
        TargetKind::Webhook => crate::target::webhook::build(ctx),
        TargetKind::Kafka => crate::target::kafka::build(ctx),
        TargetKind::S3 => crate::target::s3::build(ctx),
        TargetKind::Database => crate::target::db::build(ctx).await,
        TargetKind::Script => crate::target::script::build(ctx, db),
    }
}

/// Runs task `task_no` of sink `sink` until it ends.
///
/// # Errors
/// What stops the task for good (the job then fails and the controller
/// starts it again).
pub async fn run_sink_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    cancel: Cancel,
) -> Result<TaskEnd> {
    run_sink_task_of(env, tenant, sink, task_no, 1, cancel).await
}

/// [`run_sink_task`] for task `task_no` of `of`: a fan-in
/// sink's task reads the member instances [`crate::meta::fanin_task_of`]
/// gives it, and stops when the sink's index entry has another count.
///
/// # Errors
/// As [`run_sink_task`].
#[allow(clippy::too_many_lines)]
pub async fn run_sink_task_of(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    of: u32,
    cancel: Cancel,
) -> Result<TaskEnd> {
    // The tenant's MariaDB catalog schema, when it has one: its
    // `sink.*` settings rows. A tenant created on the engine has none;
    // its sinks run on their own options and the global settings.
    let schema = match env.catalog(tenant).await {
        Ok((pool, schema)) => pool.get_conn().await.ok().map(|c| (c, schema)),
        Err(_) => None,
    };
    let store = SinksStore::new(env.records(), tenant);
    let Some(def) = store.sink(sink).await? else {
        return Ok(TaskEnd::Stopped);
    };
    if matches!(def.state, SinkState::Failed | SinkState::Dropping) {
        return Ok(TaskEnd::Stopped);
    }
    let def = Arc::new(def);
    // `sink.tenant_bytes_per_sec` is one budget across workers.
    crate::runtime::ensure_quota_sharing(env.dc(), env.worker_id());
    let (opts, c) = match schema {
        Some((mut c, schema)) => (resolve_opts(&mut c, &schema, &def).await?, Some(c)),
        None => (Opts(def.options.clone().into_iter().collect()), None),
    };
    let opts = match env.settings() {
        Some((etcd, dc)) => over_global(opts, &etcd, &dc),
        None => opts,
    };
    crate::policy::check_enabled(
        opts.flag("enabled", true),
        opts.get("allowed_kinds")
            .unwrap_or("webhook,kafka,s3,database,script"),
        def.target.kind(),
    )?;
    match &def.source {
        SourceSpec::FanInQueue { .. } => {
            run_fan_in_task(env, tenant, sink, task_no, of, cancel, c, store, def, opts).await
        }
        SourceSpec::Queue { .. } => {
            run_queue_task(env, tenant, sink, task_no, cancel, c, store, def, opts).await
        }
        SourceSpec::Changes { .. } => {
            run_changes_task(env, tenant, sink, task_no, cancel, c, store, def, opts).await
        }
        SourceSpec::Stream { .. } => {
            run_stream_task(env, tenant, sink, task_no, of, cancel, c, store, def, opts).await
        }
        other @ SourceSpec::FanIn { .. } => Err(err(
            SprigCode::SinkOptionInvalid,
            format!("{} sources don't run in workers yet", other.kind()),
        )),
    }
}
/// A queue sink's `ordering` option: `queue` (strict queue order) or
/// `key` (the default: one routing key's messages in order, lanes
/// in parallel).
///
/// # Errors
/// 39218 for another value.
pub fn queue_ordering(opts: &Opts) -> Result<bool> {
    match opts.get("ordering").map(str::to_ascii_lowercase).as_deref() {
        None | Some("key" | "") => Ok(false),
        Some("queue") => Ok(true),
        Some(o) => Err(crate::model::bad_option(format!(
            "ordering = '{o}' isn't known for a queue sink: use 'queue' or 'key'"
        ))),
    }
}

/// A fan-in sink's task (one task reads every member instance in reach,
/// [`EnvFanIn`]), or a `CHANGES OF` sink's ([`EnvChanges`]).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_fan_in_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    of: u32,
    cancel: Cancel,
    c: Option<mysql_async::Conn>,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    // A fan-in queue task takes its members' messages under a lease
    // tag, as a queue task does, and gives back a dead
    // predecessor's in each member it opens.
    let me = env.worker_id();
    let (my_lease, release) = if matches!(def.source, SourceSpec::FanInQueue { .. }) {
        let my_lease = worker_lease(env.dc().as_ref(), &me).await;
        let prev = store.task_claim(sink, task_no).await?;
        let prev_reg = match &prev {
            Some((_, Some(w), _)) => Some(worker_lease(env.dc().as_ref(), w).await),
            _ => None,
        };
        (
            my_lease,
            dead_lease_tag(sink, task_no, prev.as_ref(), prev_reg),
        )
    } else {
        (None, None)
    };
    let epoch = store
        .claim_task(
            sink,
            task_no,
            &json!({
                "source": def.source.kind().as_str(),
                "multi": def.source.name(),
                "of": of.max(1),
                "worker_lease": my_lease,
            }),
            &me,
        )
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    let refresh = Duration::from_secs(opts.u64("fanin_refresh_seconds", 30)?.max(1));
    let source = match &def.source {
        SourceSpec::FanInQueue {
            def_tenant,
            def_object,
            under,
            db,
            name,
        } => {
            let peer = env.peer().ok_or_else(|| {
                err(
                    SprigCode::SinkOptionInvalid,
                    "queue sources need the DC's gateways, and this worker reaches none",
                )
            })?;
            crate::source::FanInSource::open(
                Box::new(EnvFanInQueue {
                    env: Arc::clone(&env),
                    peer,
                    run_as: run_as(env.as_ref(), tenant, &def, &opts).await?,
                    tenant: tenant.0,
                    def: (*def_tenant, *def_object),
                    under: *under,
                    db: db.clone(),
                    queue: name.clone(),
                    lease_ms: opts.u64("queue_lease_ms", 300_000)?.max(1000),
                    seed: sink.0,
                    task_group: (task_no.0, of.max(1)),
                    tag: lease_tag(sink, task_no, epoch),
                    release: release.clone(),
                }),
                refresh,
            )
            .await?
        }
        _ => return Ok(TaskEnd::Stopped),
    };
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(env.records().as_ref(), tenant, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        // A `CHANGES OF` sink is filtered for its owner (EnvChanges)
        // instead of paused when its tables' policies bind the owner.
        rls: None,
        regroup: match &def.source {
            SourceSpec::FanInQueue { .. } => Some(Regroup {
                dc: env.dc(),
                tenant: tenant.0,
                sink: sink.0,
                of: of.max(1),
                next: std::sync::Mutex::new(None),
            }),
            _ => None,
        },
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = Arc::new(CatalogDead {
        store: store.clone(),
        sink,
    });
    let reject_via_source =
        matches!(def.source, SourceSpec::FanInQueue { .. }) && def.dead_letter.is_none();
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, members = source.members().len(), "fan-in sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            reject_via_source,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "fan-in sink task ended");
    end
}

/// Lists and opens a fan-in queue sink's member instances: the DC's
/// `inherit-instances/` index of
/// the queue definition, members below the sink's tenant (and `UNDER t`),
/// members denying remote access left out; each read through a gateway
/// as the sink's owner (`FanInGet`).
struct EnvFanInQueue {
    env: Arc<dyn SinkEnv>,
    peer: Arc<dyn crate::peer::SinkPeer>,
    run_as: crate::peer::RunAs,
    tenant: u64,
    def: (u64, u64),
    under: Option<u64>,
    db: String,
    queue: String,
    lease_ms: u64,
    seed: u64,
    /// This task's group: (task, of).
    task_group: (u32, u32),
    /// This task's lease tag, and a dead predecessor's to give back in
    /// each member.
    tag: String,
    release: Option<String>,
}

#[async_trait]
impl crate::source::FanInOpener for EnvFanInQueue {
    async fn members(&self) -> Result<Vec<(crate::source::FanInMember, Vec<u32>)>> {
        let dc = self.env.dc();
        let prefix = dc.tier().key("inherit-instances/")?;
        let page = dc
            .list(
                &prefix,
                sprig_meta::ListOpts {
                    limit: 1_000_000,
                    ..sprig_meta::ListOpts::default()
                },
            )
            .await?;
        let Some(global) = self.env.global() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for kv in &page.kvs {
            let Ok(v) = serde_json::from_slice::<Value>(&kv.value) else {
                continue;
            };
            let def = (
                v.pointer("/def/tenant").and_then(Value::as_u64),
                v.pointer("/def/object").and_then(Value::as_u64),
            );
            if def != (Some(self.def.0), Some(self.def.1))
                || v.get("kind").and_then(Value::as_str) != Some("queue")
            {
                continue;
            }
            let (Some(member), Some(queue)) = (
                v.get("tenant").and_then(Value::as_u64),
                v.get("instance_id").and_then(Value::as_u64),
            ) else {
                continue;
            };
            if crate::meta::fanin_task_of(member, self.task_group.1) != self.task_group.0 {
                continue;
            }
            let Some(e) = global
                .get(
                    &global.tier().key(&format!("tenants/{member}"))?,
                    sprig_meta::Consistency::Serializable,
                )
                .await?
                .and_then(|kv| serde_json::from_slice::<Value>(&kv.value).ok())
            else {
                continue;
            };
            let path: Vec<u64> = e
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("")
                .split('/')
                .filter_map(|x| x.parse().ok())
                .collect();
            if member == self.tenant || !path.contains(&self.tenant) {
                continue;
            }
            if self.under.is_some_and(|u| !path.contains(&u)) {
                continue;
            }
            if e.pointer("/residency/deny_remote_access")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                metrics::counter!("sprig_inherit_fanin_skipped_total", "reason" => "residency")
                    .increment(1);
                continue;
            }
            let name = e
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            out.push((
                crate::source::FanInMember {
                    tenant: member,
                    name,
                    stream: queue,
                },
                Vec::new(),
            ));
        }
        Ok(out)
    }

    async fn open(
        &self,
        m: &crate::source::FanInMember,
        _partitions: Vec<u32>,
    ) -> Result<Box<dyn crate::runtime::SourceReader>> {
        Ok(Box::new(
            crate::source::QueueSource::new(
                Arc::clone(&self.peer),
                self.run_as.clone(),
                crate::source::QueueAt::Member {
                    def: self.def,
                    member: m.tenant,
                    db: self.db.clone(),
                },
                self.queue.clone(),
                self.lease_ms,
                self.seed ^ m.tenant,
            )
            .with_lease_tag(self.tag.clone(), self.release.clone()),
        ))
    }
}

/// The lease tag a queue sink's task takes its messages under:
/// `mq_get`'s fifth argument, one per task claim.
#[must_use]
pub fn lease_tag(sink: SinkId, task_no: TaskNo, epoch: TaskEpoch) -> String {
    format!("sink/{}/{}/{}", sink.0, task_no.0, epoch.0)
}

/// The lease of worker `worker`'s registration (`workers/<id>` in the DC
/// tier), which identifies the process now running under that id; `None`
/// when it isn't registered. An error reads as `Some(-1)`: never taken
/// for a process that is gone.
async fn worker_lease(dc: &dyn MetaStore, worker: &str) -> Option<i64> {
    let Ok(key) = dc.tier().key(&format!("workers/{worker}")) else {
        return Some(-1);
    };
    match dc.get(&key, sprig_meta::Consistency::Linearizable).await {
        Ok(kv) => kv.map(|kv| kv.lease.map_or(0, |l| l.0)),
        Err(e) => {
            tracing::debug!(worker, error = %e, "can't read a worker's registration");
            Some(-1)
        }
    }
}

/// The lease tag of a queue task's predecessor whose worker process is
/// gone, so the new task gives its messages back at once:
/// the predecessor recorded its worker's registration lease when it
/// claimed the task, and that worker's registration is now missing
/// (`reg` is `Some(None)`) or bound to another lease (a new process under
/// the same id). `None` when there is no predecessor, it predates lease
/// tags, or its process may still run: its messages then return after
/// their lease, as before.
#[must_use]
pub fn dead_lease_tag(
    sink: SinkId,
    task_no: TaskNo,
    prev: Option<&(TaskEpoch, Option<String>, Value)>,
    reg: Option<Option<i64>>,
) -> Option<String> {
    let (epoch, _, part) = prev?;
    let had = part.get("worker_lease").and_then(Value::as_i64)?;
    let now = reg?;
    if had <= 0 || now == Some(had) || now == Some(-1) {
        return None;
    }
    Some(lease_tag(sink, task_no, *epoch))
}

/// A queue sink's task: the sink's own durable queue,
/// consumed through a gateway as its owner (see `source::QueueSource`).
#[allow(clippy::too_many_arguments)]
async fn run_queue_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    cancel: Cancel,
    c: Option<mysql_async::Conn>,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    let SourceSpec::Queue { id, name } = &def.source else {
        return Ok(TaskEnd::Stopped);
    };
    let peer = env.peer().ok_or_else(|| {
        err(
            SprigCode::SinkOptionInvalid,
            "queue sources need the DC's gateways, and this worker reaches none",
        )
    })?;
    let who = run_as(env.as_ref(), tenant, &def, &opts).await?;
    // The task's predecessor, and whether its worker process
    // is gone (its etcd registration expired, or a new process took the
    // worker's id): its leased messages are then given back at once.
    let me = env.worker_id();
    let my_lease = worker_lease(env.dc().as_ref(), &me).await;
    let prev = store.task_claim(sink, task_no).await?;
    let prev_reg = match &prev {
        Some((_, Some(w), _)) => Some(worker_lease(env.dc().as_ref(), w).await),
        _ => None,
    };
    let release = dead_lease_tag(sink, task_no, prev.as_ref(), prev_reg);
    let epoch = store
        .claim_task(
            sink,
            task_no,
            &json!({
                "source": "queue",
                "queue": name,
                "worker_lease": my_lease,
            }),
            &me,
        )
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    let source = crate::source::QueueSource::new(
        peer,
        who.clone(),
        crate::source::QueueAt::Own {
            id: *id,
            db: who.database.clone(),
        },
        name.clone(),
        opts.u64("queue_lease_ms", 300_000)?.max(1000),
        sink.0,
    );
    // `ordering = queue`: queue order, one lane, and earlier
    // tasks' leases waited out before reading on.
    let queue_order = queue_ordering(&opts)?;
    if queue_order {
        settings.lanes = 1;
    }
    let source = source
        .with_queue_order(queue_order)
        .with_lease_tag(lease_tag(sink, task_no, epoch), release);
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(env.records().as_ref(), tenant, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        rls: None,
        regroup: None,
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = dead_letters(&env, tenant, &def, &opts, &store, sink).await?;
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, queue = %name, "queue sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            // No DEAD LETTER TO: the queue's own dead-lettering (a reject
            // without requeue).
            reject_via_source: def.dead_letter.is_none(),
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "queue sink task ended");
    end
}

/// A `CHANGES OF` sink's task (one per sink): its engine tables' changes
/// read through a gateway's change log as the sink's owner
/// ([`crate::change_source`]), resuming after the position its row
/// saved.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_changes_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    cancel: Cancel,
    c: Option<mysql_async::Conn>,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    let SourceSpec::Changes { objects } = &def.source else {
        return Ok(TaskEnd::Stopped);
    };
    let peer = env.peer().ok_or_else(|| {
        err(
            SprigCode::SinkOptionInvalid,
            "change sources need the DC's gateways, and this worker reaches none",
        )
    })?;
    let who = run_as(env.as_ref(), tenant, &def, &opts).await?;
    let me = env.worker_id();
    let saved = store
        .task_claim(sink, task_no)
        .await?
        .and_then(|(_, _, part)| crate::change_source::saved_token(&part));
    let part = match &saved {
        Some(t) => json!({"source": "changes", "token": t}),
        None => json!({"source": "changes"}),
    };
    let epoch = store.claim_task(sink, task_no, &part, &me).await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    // Without a saved position: START AT EARLIEST reads from the oldest
    // change the log holds, a time from that time, and LATEST (the
    // default) from the sink's creation, so a task that restarts before
    // saving a position loses nothing.
    let from_ms = match def.start_at {
        crate::model::StartAt::Earliest => 0,
        crate::model::StartAt::Timestamp { ms } => ms.max(1),
        crate::model::StartAt::Latest | crate::model::StartAt::Snapshot => store
            .created_ms(sink)
            .await?
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            })
            .max(1),
    };
    let source = crate::change_source::ChangeSource::new(
        peer,
        who,
        objects.iter().map(|(o, _)| *o).collect(),
        saved,
        from_ms,
        sink.0,
        format!("{}/{}", sink.0, task_no.0),
        Some(crate::change_source::Saver {
            store: store.clone(),
            sink,
            task_no,
            epoch,
        }),
    );
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(env.records().as_ref(), tenant, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        rls: None,
        regroup: None,
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = dead_letters(&env, tenant, &def, &opts, &store, sink).await?;
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, tables = objects.len(), "change sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            reject_via_source: false,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "change sink task ended");
    end
}

/// A `FROM STREAM` sink's task: its share of the engine stream's
/// partitions (of `of` tasks) read through a gateway as the sink's owner
/// ([`crate::stream_source`]), each resuming at the offset its row saved.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_stream_task(
    env: Arc<dyn SinkEnv>,
    tenant: TenantId,
    sink: SinkId,
    task_no: TaskNo,
    of: u32,
    cancel: Cancel,
    c: Option<mysql_async::Conn>,
    store: SinksStore,
    def: Arc<SinkDef>,
    opts: Opts,
) -> Result<TaskEnd> {
    let SourceSpec::Stream {
        id,
        name,
        partitions,
    } = &def.source
    else {
        return Ok(TaskEnd::Stopped);
    };
    let peer = env.peer().ok_or_else(|| {
        err(
            SprigCode::SinkOptionInvalid,
            "stream sources need the DC's gateways, and this worker reaches none",
        )
    })?;
    let who = run_as(env.as_ref(), tenant, &def, &opts).await?;
    // The tasks share the partitions evenly, in order.
    let share = partitions.div_ceil(of.max(1)).max(1);
    let parts = task_partitions(*partitions, share, task_no.0);
    let me = env.worker_id();
    let saved = store
        .task_claim(sink, task_no)
        .await?
        .map(|(_, _, part)| crate::stream_source::saved_offsets(&part))
        .unwrap_or_default();
    let epoch = store
        .claim_task(sink, task_no, &crate::stream_source::part_of(&saved), &me)
        .await?;
    let mut conns = Vec::new();
    for r in &def.connections {
        conns.push(open_conn(env.as_ref(), tenant, &store, r, &opts).await?);
    }
    drop(c);
    let info = TaskInfo {
        tenant: tenant.0,
        sink: sink.0,
        task_no: task_no.0,
        epoch,
    };
    let (conn, extra) = if def.target.kind() == TargetKind::Script {
        (None, conns)
    } else {
        let mut it = conns.into_iter();
        (it.next(), it.collect())
    };
    let ctx = TargetCtx {
        task: info.clone(),
        sink: Arc::clone(&def),
        conn,
        extra,
        opts: opts.clone(),
        db_name: opts.get("__db_name").unwrap_or("").to_owned(),
    };
    let mut settings = run_settings(&opts, &def)?;
    if def.target.kind() == TargetKind::S3 {
        crate::target::s3::run_settings_override(&opts, &mut settings)?;
    }
    let db = script_db(env.as_ref(), tenant, &def, &opts).await?;
    let target = build_target_with(&ctx, db).await?;
    // Without a saved offset: START AT EARLIEST reads from the partition's
    // start, a time from that time, and LATEST (the default) from the
    // sink's creation, so a task that restarts before saving loses
    // nothing.
    let from_ms = match def.start_at {
        crate::model::StartAt::Earliest => 0,
        crate::model::StartAt::Timestamp { ms } => ms.max(1),
        crate::model::StartAt::Latest | crate::model::StartAt::Snapshot => store
            .created_ms(sink)
            .await?
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            })
            .max(1),
    };
    let source = crate::stream_source::StreamSource::new(
        peer,
        who.clone(),
        who.database.clone(),
        name.clone(),
        *id,
        &parts,
        &saved,
        from_ms,
        sink.0,
        Some(crate::change_source::Saver {
            store: store.clone(),
            sink,
            task_no,
            epoch,
        }),
    );
    let prepare = SinkPrepare {
        condition: def
            .filter_sql
            .as_deref()
            .map(crate::filter::parse)
            .transpose()?,
        projection: def.projection.clone(),
        transform: load_transform(env.records().as_ref(), tenant, &def, &opts).await?,
    };
    let control = Arc::new(CatalogControl {
        store: store.clone(),
        sink,
        task: task_no,
        epoch,
        definition_ver: def.definition_ver,
        delivered: AtomicU64::new(0),
        rls: None,
        regroup: None,
        owner: OwnerCheck::new(&env, tenant, &def),
    });
    let dead = dead_letters(&env, tenant, &def, &opts, &store, sink).await?;
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, stream = %name, partitions = ?parts, "stream sink task started");
    metrics::gauge!("sprig_sink_tasks", "state" => "running").increment(1.0);
    let end = run_task(
        TaskParts {
            info,
            source: Box::new(source),
            target,
            dead,
            control,
            prepare: Arc::new(prepare),
            settings,
            seed: None,
            reject_via_source: false,
        },
        cancel,
    )
    .await;
    metrics::gauge!("sprig_sink_tasks", "state" => "running").decrement(1.0);
    tracing::info!(tenant = %tenant, sink = sink.0, task = task_no.0, end = ?end, "stream sink task ended");
    end
}

/// Kinds of connection each target needs (for messages).
#[must_use]
pub fn needs(kind: TargetKind) -> &'static [ConnKind] {
    match kind {
        TargetKind::Webhook => &[ConnKind::Webhook],
        TargetKind::Kafka => &[ConnKind::Kafka],
        TargetKind::S3 => &[ConnKind::S3],
        TargetKind::Database => &[ConnKind::Mysql, ConnKind::Postgresql],
        TargetKind::Script => &[],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_reads_name_each_source() {
        let mut d =
            crate::probe::stand_in(4, "s", crate::model::TargetSpec::Webhook { path: None });
        d.source = SourceSpec::Stream {
            id: 9,
            name: "orders.changes".into(),
            partitions: 1,
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Named {
                db: 4,
                name: "orders".into()
            }]
        );
        d.source = SourceSpec::Queue {
            id: 3,
            name: "jobs".into(),
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Named {
                db: 4,
                name: "jobs".into()
            }]
        );
        d.source = SourceSpec::Changes {
            objects: vec![(7, "a".into()), (8, "b".into())],
        };
        assert_eq!(
            owner_reads(&d),
            vec![OwnerRead::Object(7), OwnerRead::Object(8)]
        );
        // A fan-in sink: SPRIG_FAN_IN and SELECT on the definition's name.
        d.source = SourceSpec::FanIn {
            def_tenant: 1,
            def_object: 5,
            db: "app".into(),
            name: "events".into(),
            partitions: 1,
            under: None,
        };
        assert_eq!(
            owner_reads(&d),
            vec![
                OwnerRead::FanIn,
                OwnerRead::InDatabase {
                    db: "app".into(),
                    name: "events".into()
                }
            ]
        );
        assert!(owner_pause_reason("x").starts_with("39229 ER_SPRIG_SINK_OWNER_PRIVILEGE_LOST: x"));
    }

    #[test]
    fn a_sink_paused_by_row_level_security_stops_its_task() {
        let st = |s: SinkState, r: Option<&str>| (s, r.map(str::to_owned), 3u64);
        // A sink paused by row-level security stops its running task
        // (it used to keep delivering, `Run`).
        assert_eq!(
            stop_directive(
                Some(&st(
                    SinkState::Paused,
                    Some(
                        "ER_SPRIG_RLS_UNSUPPORTED: row-level security on 'docs' binds the sink's owner"
                    )
                )),
                3
            ),
            Some(Directive::Stop)
        );
        assert_eq!(
            stop_directive(Some(&st(SinkState::Paused, Some("user"))), 3),
            None
        );
        assert_eq!(stop_directive(Some(&st(SinkState::Running, None)), 3), None);
        assert_eq!(
            stop_directive(Some(&st(SinkState::Running, None)), 4),
            Some(Directive::Stop)
        );
        assert_eq!(stop_directive(None, 3), Some(Directive::Stop));
    }

    #[test]
    fn tasks_split_partitions() {
        assert_eq!(task_partitions(12, 4, 0), vec![0, 1, 2, 3]);
        assert_eq!(task_partitions(10, 4, 2), vec![8, 9]);
        assert!(task_partitions(4, 4, 1).is_empty());
        assert_eq!(unquote("\"[\\\"a\\\"]\""), "[\"a\"]");
        assert_eq!(unquote("12"), "12");
    }

    #[test]
    fn queue_orderings() {
        // `ordering` of queue sinks.
        let o = |v: &str| {
            Opts(
                [("ordering".to_owned(), v.to_owned())]
                    .into_iter()
                    .collect(),
            )
        };
        assert!(queue_ordering(&o("queue")).unwrap());
        assert!(!queue_ordering(&o("key")).unwrap());
        assert!(!queue_ordering(&Opts::default()).unwrap());
        assert!(queue_ordering(&o("partition")).is_err());
    }

    #[test]
    fn a_dead_predecessors_lease_tag() {
        // Give back a predecessor's leases only when its
        // worker process is gone.
        let (s, t) = (SinkId(7), TaskNo(0));
        let prev = |lease: Value| {
            (
                TaskEpoch(3),
                Some("wkr-1".to_owned()),
                json!({"source": "queue", "queue": "q", "worker_lease": lease}),
            )
        };
        let p = prev(json!(11));
        assert_eq!(lease_tag(s, t, TaskEpoch(3)), "sink/7/0/3");
        // Registration gone, or a new process under the same id.
        assert_eq!(
            dead_lease_tag(s, t, Some(&p), Some(None)).as_deref(),
            Some("sink/7/0/3")
        );
        assert_eq!(
            dead_lease_tag(s, t, Some(&p), Some(Some(12))).as_deref(),
            Some("sink/7/0/3")
        );
        // The same process still runs, or etcd couldn't say.
        assert_eq!(dead_lease_tag(s, t, Some(&p), Some(Some(11))), None);
        assert_eq!(dead_lease_tag(s, t, Some(&p), Some(Some(-1))), None);
        assert_eq!(dead_lease_tag(s, t, Some(&p), None), None);
        // No predecessor, or one from before lease tags.
        assert_eq!(dead_lease_tag(s, t, None, Some(None)), None);
        let old = (
            TaskEpoch(3),
            Some("wkr-1".to_owned()),
            json!({"source": "queue"}),
        );
        assert_eq!(dead_lease_tag(s, t, Some(&old), Some(None)), None);
        assert_eq!(
            dead_lease_tag(s, t, Some(&prev(Value::Null)), Some(None)),
            None
        );
    }
}
