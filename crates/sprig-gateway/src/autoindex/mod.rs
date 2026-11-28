//! Automatic indexing on the engine: the loop, run by the gateway process
//! that hosts the tenants' engine.
//!
//! ```text
//! spawn(services, keyring):  one task per process
//!   tenants: every tenant whose engine unit this node leads (crate::led;
//!     every tenant on an engine that doesn't replicate) and that has
//!     engine databases, its catalog loaded here on demand
//!   every minute: each tenant due (auto_index.eval_interval_minutes since
//!     its last pass) gets a pass: sprig_advisor::autoindex::pass::run over
//!     GatewayEnv, one tenant at a time (so one build per tenant at a
//!     time, and one per node)
//! GatewayEnv:
//!   window     the workload records (WorkloadTables::window)
//!   table      the engine definition, distinct counts from a sample, kept
//!              in m/autoindex/stats and reused for 6 hours (or until the
//!              definition changes), across restarts
//!   replay     replay::statements + replay::replay_one (what-if plans)
//!   build/drop ddl::build / ddl::drop (the catalog's DDL path)
//!   reads      usage::reads (plans that read the index since start),
//!              usage::last_read_us
//!   auto_indexes  every sprig_auto_* index in the tenant's engine catalog
//!              (the pass's reconciler)
//!   propose    the approvals inbox (kind index.create, mode approve)
//!   paused     the tenant's unit has no known leader (an election or a
//!              failover is running)
//! every minute, besides the passes:
//!   usage      tenants whose automatic indexes were read: pass::note_usage
//!              writes the counts into the records (and once more when the
//!              loop stops), so a restart forgets at most a minute of them
//!   requests   tenants with index records: pass::apply_requests (an
//!              operator's drop, pin, unpin or restore, written by the
//!              controller's Admin API)
//! ```
//!
//! `SPRIGDB_AUTOINDEX_TICK_MS` shortens the minute between checks (tests
//! compress time with it and `auto_index.eval_interval_minutes = 1`).

// Sample counts become estimates; the env keeps `&self` methods its trait
// shape asks for; leader states are listed where they matter.
#![allow(
    clippy::cast_precision_loss,
    clippy::unused_self,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match_else,
    clippy::format_collect
)]

pub(crate) mod ddl;
pub(crate) mod replay;
pub(crate) mod usage;

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sprig_advisor::autoindex::candidates::TableInfo;
use sprig_advisor::autoindex::model::{BuildReport, IndexRecord, IndexSpec, KeyCol, is_auto_name};
use sprig_advisor::autoindex::pass::{self, Approval, Env};
use sprig_advisor::autoindex::policy::Replay;
use sprig_advisor::autoindex::settings::{AutoIndexSettings, EVAL_INTERVAL_MINUTES};
use sprig_advisor::autoindex::store::AutoIndexStore;
use sprig_approvals::model::{
    Action, Kind, Level, Mode as ApMode, Proposal, RecId, State, Subject,
};
use sprig_approvals::{Approvals, LocatorStores, Policy, Verdict};
use sprig_catalog::records::RecordTx;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::time::SystemClock;
use sprig_common::{CancellationToken, Result, SprigError};
use sprig_workload::keyring::TenantKeyring;
use sprig_workload::store::WorkloadTables;
use sprig_workload::window::FpWindowStats;

use crate::engine::catalog;
use crate::node_engine::{self, UnitLeader};
use crate::tenant::TenantServices;

/// The variable that shortens the check interval, in ms.
pub(crate) const TICK_VAR: &str = "SPRIGDB_AUTOINDEX_TICK_MS";

fn to_us(t: SystemTime) -> u64 {
    u64::try_from(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_micros()).unwrap_or(u64::MAX)
}

fn of_us(us: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us)
}

/// The loop's settings for the tenant whose catalog is `cat`.
pub(crate) fn settings_of(svc: &TenantServices, cat: &TenantCatalog) -> AutoIndexSettings {
    let ctx = cat.setting_ctx(&svc.dc);
    let s = &svc.settings;
    AutoIndexSettings::read(
        |k| s.get(k, &ctx, Some(&cat.settings)),
        |k| s.get(k, &ctx, Some(&cat.settings)),
        |k| s.get(k, &ctx, Some(&cat.settings)),
        |k| s.get(k, &ctx, Some(&cat.settings)).into_owned(),
    )
}

/// The node side of one tenant's pass.
pub(crate) struct GatewayEnv {
    svc: Arc<TenantServices>,
    keyring: Arc<dyn TenantKeyring>,
    approvals: Arc<Approvals>,
    tenant: TenantId,
    cat: Arc<TenantCatalog>,
    settings: AutoIndexSettings,
    /// Replayed statements per pattern, for this pass.
    texts: Mutex<HashMap<u64, Vec<String>>>,
}

impl GatewayEnv {
    async fn new(
        svc: &Arc<TenantServices>,
        keyring: &Arc<dyn TenantKeyring>,
        approvals: &Arc<Approvals>,
        tenant: TenantId,
    ) -> Result<Self> {
        let cat = svc.cache.get(tenant, Freshness::Latest).await?;
        let settings = settings_of(svc, &cat);
        Ok(Self {
            svc: Arc::clone(svc),
            keyring: Arc::clone(keyring),
            approvals: Arc::clone(approvals),
            tenant,
            cat,
            settings,
            texts: Mutex::new(HashMap::new()),
        })
    }

    fn object(&self, spec: &IndexSpec) -> Option<u64> {
        ddl::table_def(&self.svc, &self.cat, &spec.db, &spec.table).map(|d| d.object.0)
    }

    fn wait(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[async_trait::async_trait]
impl Env for GatewayEnv {
    fn now_us(&self) -> u64 {
        to_us(SystemTime::now())
    }

    fn settings(&self) -> AutoIndexSettings {
        self.settings.clone()
    }

    fn paused(&self) -> Option<String> {
        match node_engine::tenant_leader(self.tenant.0) {
            UnitLeader::Any | UnitLeader::Here => None,
            UnitLeader::Node(n) => Some(format!("the tenant's unit is led by node {n}")),
            _ => Some("the tenant's unit has no leader now (election or failover)".into()),
        }
    }

    async fn window(&self, from_us: u64, to_us: u64) -> Result<Vec<FpWindowStats>> {
        let mut rec = self.svc.records.begin(self.tenant).await?;
        let w =
            WorkloadTables::window(&mut *rec, self.tenant, of_us(from_us), of_us(to_us)).await?;
        rec.commit().await?;
        Ok(w)
    }

    async fn table(&self, db: &str, table: &str) -> Result<Option<TableInfo>> {
        let cat = self.svc.cache.get(self.tenant, Freshness::Latest).await?;
        let Some(def) = ddl::table_def(&self.svc, &cat, db, table) else {
            return Ok(None);
        };
        // The figures kept in the records while fresh, else a sample (kept
        // for the next passes, and across a restart).
        let key = format!("{db}.{table}").to_lowercase();
        let cached = {
            let mut tx = self.svc.records.begin(self.tenant).await?;
            let st = AutoIndexStore::stats(&mut *tx, &key).await?;
            tx.commit().await?;
            st
        };
        let (info, taken) =
            replay::table_info(self.tenant, db, &def, cached.as_ref(), self.now_us()).await?;
        if let Some(st) = taken {
            let mut tx = self.svc.records.begin(self.tenant).await?;
            AutoIndexStore::put_stats(&mut *tx, &st).await?;
            tx.commit().await?;
        }
        Ok(Some(info))
    }

    async fn replay(
        &self,
        fp: &FpWindowStats,
        spec: &IndexSpec,
        max: usize,
    ) -> Result<Vec<Replay>> {
        let cached = self
            .texts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&fp.id.0)
            .cloned();
        let texts = match cached {
            Some(t) => t,
            None => {
                let t = replay::statements(&self.svc, &*self.keyring, self.tenant, fp, max).await?;
                self.texts
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(fp.id.0, t.clone());
                t
            }
        };
        let db = fp
            .shape
            .tables
            .iter()
            .find(|t| !t.derived)
            .and_then(|t| t.db.clone())
            .unwrap_or_else(|| spec.db.clone());
        let mut out = Vec::new();
        for sql in texts.iter().take(max) {
            if let Some(r) =
                replay::replay_one(&self.svc, self.tenant, &self.cat, &db, sql, spec).await?
            {
                out.push(r);
            }
        }
        Ok(out)
    }

    async fn build(&self, spec: &IndexSpec, s: &AutoIndexSettings) -> Result<BuildReport> {
        let started = Instant::now();
        let r = ddl::build(
            &self.svc,
            self.tenant,
            spec,
            s.build_batch_rows as usize,
            Duration::from_millis(u64::from(s.build_pause_ms)),
            self.wait(),
        )
        .await;
        metrics::histogram!("sprig_advisor_autoindex_build_seconds")
            .record(started.elapsed().as_secs_f64());
        r
    }

    async fn drop_index(&self, spec: &IndexSpec, name: &str) -> Result<()> {
        ddl::drop(&self.svc, self.tenant, spec, name, self.wait()).await
    }

    fn reads(&self, spec: &IndexSpec, name: &str) -> u64 {
        self.object(spec)
            .map_or(0, |o| usage::reads(self.tenant.0, o, name))
    }

    fn last_read_us(&self, spec: &IndexSpec, name: &str) -> Option<u64> {
        usage::last_read_us(self.tenant.0, self.object(spec)?, name)
    }

    async fn auto_indexes(&self) -> Result<Vec<(IndexSpec, String)>> {
        let cat = self.svc.cache.get(self.tenant, Freshness::Latest).await?;
        let Some(eng) = catalog::of(&cat) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for p in eng.tables.values() {
            let Ok(td) = p.as_ref().as_ref() else {
                continue;
            };
            let Some(db) = cat.databases.get(td.db) else {
                continue;
            };
            for x in td.indexes.iter().filter(|x| is_auto_name(&x.name)) {
                let cols = x
                    .cols
                    .iter()
                    .zip(&x.parts)
                    .filter_map(|(&c, kc)| {
                        Some(KeyCol {
                            column: td.columns.get(c)?.name.clone(),
                            desc: kc.desc,
                        })
                    })
                    .collect();
                let spec = IndexSpec {
                    db: db.name.clone(),
                    table: td.name.clone(),
                    cols,
                };
                out.push((spec, x.name.clone()));
            }
        }
        out.sort_by(|a, b| a.1.cmp(&b.1));
        Ok(out)
    }

    async fn propose(&self, rec: &IndexRecord) -> Result<String> {
        let mut subject = Subject::tenant(self.tenant, self.svc.dc.clone());
        subject.db_name = Some(rec.spec.db.clone());
        subject.object_name = Some(rec.spec.table.clone());
        let p = Proposal {
            kind: Kind::IndexCreate,
            subject,
            target: format!("t{}/{}/{}", self.tenant.0, rec.spec.table_key(), rec.name),
            action: Action {
                text: format!(
                    "ALTER TABLE `{}`.`{}` {}",
                    rec.spec.db,
                    rec.spec.table,
                    rec.spec.add_clause()
                ),
                machine: serde_json::json!({ "index": rec.name, "def": rec.spec.canonical() }),
            },
            evidence: serde_json::json!({
                "fingerprints": rec.serves,
                "rows_examined_before": rec.est_before,
                "rows_examined_after": rec.est_after,
                "saved_ms_per_hour": rec.benefit_ms_per_hour,
            }),
            score: rec.benefit_ms_per_hour,
            summary: format!(
                "index {} on {}.{}: rows examined {:.0} -> {:.0}",
                rec.spec.canonical(),
                rec.spec.db,
                rec.spec.table,
                rec.est_before,
                rec.est_after
            ),
            emergency: false,
        };
        let policy = Policy::fixed(ApMode::Approve, Level::Table);
        match self.approvals.check(&p, &policy, &[self.tenant]).await? {
            Verdict::Wait(id) | Verdict::Advice(id) | Verdict::Suppressed(id) => Ok(id.to_string()),
            Verdict::Proceed(_) => Ok("approved".into()),
            Verdict::Off => Err(SprigError::internal("approvals are off for index.create")),
        }
    }

    async fn approval(&self, id: &str) -> Result<Approval> {
        if id == "approved" {
            return Ok(Approval::Approved);
        }
        let rec = self.approvals.get(&RecId::parse(id)?).await?;
        Ok(match rec.state {
            State::Approved | State::Applying | State::Applied => Approval::Approved,
            State::Open | State::Snoozed => Approval::Pending,
            _ => Approval::Rejected,
        })
    }

    async fn records(&self) -> Result<Box<dyn RecordTx>> {
        self.svc.records.begin(self.tenant).await
    }
}

/// Writes the reads counted in memory into the index records of every
/// tenant read since the last call (`pass::note_usage`).
async fn flush_usage(
    svc: &Arc<TenantServices>,
    keyring: &Arc<dyn TenantKeyring>,
    approvals: &Arc<Approvals>,
) {
    for t in usage::take_dirty() {
        if matches!(node_engine::tenant_leader(t), UnitLeader::Node(_)) {
            continue;
        }
        let r = match GatewayEnv::new(svc, keyring, approvals, TenantId(t)).await {
            Ok(env) => pass::note_usage(&env).await,
            Err(e) => Err(e),
        };
        if let Err(e) = r {
            tracing::debug!(tenant = t, error = %e, "automatic indexing: usage not written");
        }
    }
}

/// Applies operators' pending requests for every tenant in `known`
/// (`pass::apply_requests`).
async fn apply_requests(
    svc: &Arc<TenantServices>,
    keyring: &Arc<dyn TenantKeyring>,
    approvals: &Arc<Approvals>,
    known: &BTreeSet<u64>,
) {
    for &t in known {
        if matches!(node_engine::tenant_leader(t), UnitLeader::Node(_)) {
            continue;
        }
        let Ok(env) = GatewayEnv::new(svc, keyring, approvals, TenantId(t)).await else {
            continue;
        };
        match pass::apply_requests(&env).await {
            Ok(done) => {
                for a in done {
                    tracing::info!(tenant = t, index = %a.index, action = a.action.name(),
                        outcome = %a.outcome, "automatic indexing: operator request");
                }
            }
            Err(e) => {
                tracing::warn!(tenant = t, error = %e, "automatic indexing: operator requests failed");
            }
        }
    }
}

/// Starts the loop; it ends with `stop`.
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    keyring: Arc<dyn TenantKeyring>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let tick = std::env::var(TICK_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(60), Duration::from_millis);
    let approvals = Arc::new(Approvals::new(
        Arc::new(LocatorStores::new(Arc::clone(&svc.records))),
        Arc::new(SystemClock),
    ));
    let mut led = crate::led::Led::new(&svc);
    tokio::spawn(async move {
        // Tenants with index records: the ones an operator's request can
        // name.
        let mut known: BTreeSet<u64> = BTreeSet::new();
        let mut last: HashMap<u64, Instant> = HashMap::new();
        let mut timer = tokio::time::interval(tick);
        loop {
            tokio::select! {
                () = stop.cancelled() => {
                    // The reads counted since the last minute.
                    let flush = flush_usage(&svc, &keyring, &approvals);
                    let _ = tokio::time::timeout(Duration::from_secs(5), flush).await;
                    return;
                }
                _ = led.changed() => continue,
                _ = timer.tick() => {}
            }
            flush_usage(&svc, &keyring, &approvals).await;
            apply_requests(&svc, &keyring, &approvals, &known).await;
            let cats = led
                .catalogs(&svc, |cat| {
                    catalog::of(cat).is_some_and(|e| !e.dbs.is_empty())
                })
                .await;
            let led_now: BTreeSet<u64> = cats.iter().map(|c| c.tenant.0).collect();
            last.retain(|t, _| led_now.contains(t));
            for cat in cats {
                let t = cat.tenant.0;
                let tenant = TenantId(t);
                let every = Duration::from_secs(
                    60 * u64::from(svc.settings.get(
                        &EVAL_INTERVAL_MINUTES,
                        &cat.setting_ctx(&svc.dc),
                        Some(&cat.settings),
                    )),
                );
                // A shortened tick shortens the interval in the same ratio.
                let every = every.mul_f64(tick.as_secs_f64() / 60.0);
                if last.get(&t).is_some_and(|at| at.elapsed() < every) {
                    continue;
                }
                last.insert(t, Instant::now());
                let env = match GatewayEnv::new(&svc, &keyring, &approvals, tenant).await {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::debug!(tenant = t, error = %e, "automatic indexing: no pass");
                        continue;
                    }
                };
                metrics::counter!("sprig_advisor_autoindex_runs_total").increment(1);
                match pass::run(&env).await {
                    Ok(r) => {
                        if r.records > 0 {
                            known.insert(t);
                        }
                        if !(r.built.is_empty() && r.dropped.is_empty() && r.recommended.is_empty())
                        {
                            tracing::info!(tenant = t, built = ?r.built, dropped = ?r.dropped,
                                recommended = ?r.recommended, "automatic indexing pass");
                        }
                        metrics::counter!("sprig_advisor_autoindex_candidates_total")
                            .increment(r.candidates as u64);
                        for (_, why) in &r.dropped {
                            metrics::counter!("sprig_advisor_autoindex_drops_total", "reason" => why.clone())
                                .increment(1);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(tenant = t, error = %e, "automatic indexing pass failed");
                    }
                }
            }
        }
    })
}
