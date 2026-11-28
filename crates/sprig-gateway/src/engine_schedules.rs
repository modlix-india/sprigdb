//! The runner of logic schedules (`CREATE SCHEDULE`) on engine databases.
//!
//! ```text
//! spawn(services, login, engine): one task per process, beside the SQL
//! events scheduler
//!   every tick (1 s; SPRIGDB_SQL_EVENTS_TICK_MS), for each tenant whose
//!   unit this node leads (crate::led; every tenant on an engine that
//!   doesn't replicate), whether or not a client of it reached this
//!   process:
//!     the tenant's schedules of engine databases, read from the logic
//!     records again every 10 s, or at once when its cached catalog's
//!     version moves or the catalog load feed lagged, with each one's next
//!     fire time (the catalog, for the databases' names, is loaded only
//!     for a tenant that has schedules)
//!     each one due (next fire time passed, or a run of it live here):
//!       one record transaction: sprig_logic::schedule::tick, the
//!       compare-and-set of the controller's loop (a run another process
//!       started and this one doesn't know is cleared as an error)
//!       a fire: a run, in its own task, in a fresh session as the
//!       schedule's definer: CALL `db`.`target`(args), under
//!       logic.job_budget_ms; then, in a second record transaction, the
//!       run's end (ok, error or budget) and a log line
//! ```
//!
//! The compare-and-set makes one run per fire across leaders: a process
//! that lost the lead finds the slot moved. Runs missed while no process
//! led the tenant are passed over as misfired when later than
//! `logic.schedule_misfire_seconds` (300 s), as the controller's loop does
//! for MariaDB databases. The target is called by name, so it may be a
//! logic procedure or an SQL stored procedure of the database.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::CancellationToken;
use sprig_common::ids::{JobId, TenantId};
use sprig_logic::LogicStore;
use sprig_logic::catalog::ScheduleRow;
use sprig_logic::schedule::Tick;
use tokio::sync::mpsc;

use crate::http::sql::{Caller, SqlCall, SqlEngine, collect};
use crate::tenant::TenantServices;

/// Late fires within this still run (`logic.schedule_misfire_seconds`'s
/// default, as the controller's loop uses it).
const MISFIRE: Duration = Duration::from_secs(300);
/// How long a tenant's list of schedules, as read, stands.
const FRESH: Duration = Duration::from_secs(10);

/// One schedule of an engine database, as last read.
#[derive(Debug, Clone)]
struct Sched {
    module_id: u64,
    name: String,
    db_id: sprig_common::ids::DbId,
    db: String,
    definer: u32,
    version: u64,
    next_fire_ms: u64,
}

/// A tenant's schedules, as last read.
struct Known {
    at: Instant,
    catalog_version: u64,
    list: Vec<Sched>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The runs live in this process (job ids, hex).
type Running = Arc<Mutex<HashSet<String>>>;

/// Starts the runner; it ends with `stop`.
#[allow(clippy::too_many_lines)] // the loop: tenants, their schedules, each tick
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let tick = std::env::var(crate::sql_events::TICK_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(1), Duration::from_millis);
    let mut led = crate::led::Led::new(&svc);
    tokio::spawn(async move {
        let mut known: HashMap<u64, Known> = HashMap::new();
        let running: Running = Arc::new(Mutex::new(HashSet::new()));
        let mut timer = tokio::time::interval(tick);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = led.changed() => continue,
                _ = timer.tick() => {}
            }
            if led.take_lagged() {
                known.clear();
            }
            let tenants = led.tenants(&svc).await;
            let led_now: HashSet<u64> = tenants.iter().copied().collect();
            known.retain(|t, _| led_now.contains(t));
            for t in tenants {
                let cached = svc.cache.cached(TenantId(t)).map(|c| c.version);
                let stale = known.get(&t).is_none_or(|k| {
                    k.at.elapsed() >= FRESH || cached.is_some_and(|v| v != k.catalog_version)
                });
                if stale {
                    match read(&svc, TenantId(t)).await {
                        Ok((catalog_version, list)) => {
                            known.insert(
                                t,
                                Known {
                                    at: Instant::now(),
                                    catalog_version: if catalog_version > 0 {
                                        catalog_version
                                    } else {
                                        cached.unwrap_or(0)
                                    },
                                    list,
                                },
                            );
                        }
                        Err(e) => {
                            tracing::warn!(tenant = t, error = %e, "schedules not read");
                            continue;
                        }
                    }
                }
                let Some(k) = known.get_mut(&t) else { continue };
                let now = now_ms();
                for s in &mut k.list {
                    if s.next_fire_ms > now {
                        continue;
                    }
                    let fired = fire(&svc, TenantId(t), s, now, &running).await;
                    match fired {
                        Ok((Tick::Fire(job), next)) => {
                            if let Some(n) = next {
                                s.next_fire_ms = n;
                            }
                            let run = Run {
                                svc: Arc::clone(&svc),
                                login: Arc::clone(&login),
                                engine: Arc::clone(&engine),
                                tenant: TenantId(t),
                                sched: s.clone(),
                                job,
                                running: Arc::clone(&running),
                            };
                            tokio::spawn(run.run());
                        }
                        Ok((tick, next)) => {
                            if matches!(tick, Tick::Skipped | Tick::Misfired) {
                                metrics::counter!("sprig_logic_schedule_runs_total", "outcome" => if tick == Tick::Skipped { "skipped" } else { "misfired" }).increment(1);
                                tracing::info!(tenant = t, schedule = %s.name, ?tick, "schedule passed");
                            }
                            // A live run keeps it due: its end is seen at once.
                            s.next_fire_ms = match (tick, next) {
                                (Tick::Running, _) => now + 1000,
                                (_, Some(n)) => n,
                                (_, None) => u64::MAX,
                            };
                        }
                        Err(e) => {
                            tracing::warn!(tenant = t, schedule = %s.name, error = %e, "schedule tick failed");
                            s.next_fire_ms = now + 5000;
                        }
                    }
                }
            }
        }
    })
}

/// The tenant's live schedules of engine databases, with the catalog
/// version they were matched against (0 when the tenant has no schedule:
/// its catalog isn't loaded for nothing).
async fn read(svc: &TenantServices, tenant: TenantId) -> sprig_common::Result<(u64, Vec<Sched>)> {
    let store = LogicStore::default();
    let mut c = svc.records.begin(tenant).await?;
    let mut found = Vec::new();
    for m in store.modules(&mut *c, None).await? {
        let Some(version) = m.active_version else {
            continue;
        };
        if m.kind != "schedule" || m.state != "active" {
            continue;
        }
        let Some(row) = store.schedule(&mut *c, m.module_id).await? else {
            continue;
        };
        found.push((m, version, row.next_fire_ms));
    }
    c.commit().await?;
    if found.is_empty() {
        return Ok((0, Vec::new()));
    }
    let cat = svc
        .cache
        .get(tenant, sprig_catalog::Freshness::Latest)
        .await?;
    let Some(dbs) = crate::engine::catalog::of(&cat).map(|e| e.dbs.clone()) else {
        return Ok((cat.version, Vec::new()));
    };
    let mut out = Vec::new();
    for (m, version, next_fire_ms) in found {
        let db = sprig_common::ids::DbId(m.db_id);
        if !dbs.contains(&db) {
            continue;
        }
        let Some(db_name) = cat.databases.get(db).map(|d| d.name.clone()) else {
            continue;
        };
        out.push(Sched {
            module_id: m.module_id,
            name: m.name.clone(),
            db_id: db,
            db: db_name,
            definer: m.created_by,
            version,
            next_fire_ms,
        });
    }
    Ok((cat.version, out))
}

/// One tick of `s` in a record transaction.
async fn fire(
    svc: &TenantServices,
    tenant: TenantId,
    s: &Sched,
    now: u64,
    running: &Running,
) -> sprig_common::Result<(Tick, Option<u64>)> {
    let store = LogicStore::default();
    let mut tx = svc.records.begin(tenant).await?;
    let live = running
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let r = sprig_logic::schedule::tick(&mut *tx, &store, s.module_id, now, MISFIRE, |hex| {
        live.contains(hex)
    })
    .await?;
    tx.commit().await?;
    if let Tick::Fire(job) = &r.0 {
        running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(job.to_string());
    }
    Ok(r)
}

/// One run of a schedule.
struct Run {
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    tenant: TenantId,
    sched: Sched,
    job: JobId,
    running: Running,
}

impl Run {
    async fn run(self) {
        let started = Instant::now();
        let (outcome, error) = match self.call().await {
            Ok(()) => ("ok", None),
            Err(e) if e.message == BUDGET => ("budget", Some(e.message.clone())),
            Err(e) => ("error", Some(e.to_string())),
        };
        metrics::counter!("sprig_logic_schedule_runs_total", "outcome" => outcome).increment(1);
        if let Some(e) = &error {
            tracing::warn!(tenant = %self.tenant, db = %self.sched.db, schedule = %self.sched.name, error = %e, "schedule run failed");
        }
        if let Err(e) = self.end(outcome, error.as_deref(), started).await {
            tracing::warn!(tenant = %self.tenant, schedule = %self.sched.name, error = %e, "schedule run's end not recorded");
        }
        self.running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.job.to_string());
    }

    /// The run's end, and a log line, in the logic records.
    async fn end(
        &self,
        outcome: &str,
        error: Option<&str>,
        started: Instant,
    ) -> sprig_common::Result<()> {
        let store = LogicStore::default();
        // On the unit's leader: the run may end after the leadership moved.
        let mut tx = self.svc.writers.begin(self.tenant).await?;
        let id = self.job.to_bytes();
        store
            .run_ended(&mut *tx, self.sched.module_id, &id, outcome)
            .await?;
        let version = store
            .module_by_id(&mut *tx, self.sched.module_id)
            .await?
            .and_then(|m| m.active_version);
        let msg = match error {
            None => format!(
                "run {} of version {} ok in {} ms",
                self.job,
                version.unwrap_or(0),
                started.elapsed().as_millis()
            ),
            Some(e) => format!(
                "run {} of version {} failed: {e}",
                self.job,
                version.unwrap_or(0)
            ),
        };
        let _ = store
            .log(
                &mut *tx,
                self.sched.module_id,
                version,
                if error.is_some() { "error" } else { "info" },
                "schedule",
                &msg,
            )
            .await;
        tx.commit().await
    }

    /// `CALL target(args)` as the definer, under `logic.job_budget_ms`.
    async fn call(&self) -> sprig_common::Result<()> {
        let cat = self
            .svc
            .cache
            .get(self.tenant, sprig_catalog::Freshness::Latest)
            .await?;
        let (target, args, budget) = {
            let store = LogicStore::default();
            let mut c = self.svc.records.begin(self.tenant).await?;
            let row: ScheduleRow = store
                .schedule(&mut *c, self.sched.module_id)
                .await?
                .ok_or_else(|| sprig_common::SprigError::internal("the schedule is gone"))?;
            let spec = store
                .source(&mut *c, self.sched.module_id, self.sched.version)
                .await
                .ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok());
            c.commit().await?;
            let target = spec
                .as_ref()
                .and_then(|v| v["target"]["name"].as_str().map(str::to_owned))
                .ok_or_else(|| {
                    sprig_common::SprigError::internal("the schedule's target isn't recorded")
                })?;
            let args: Vec<serde_json::Value> = serde_json::from_str(&row.args).unwrap_or_default();
            let sctx = cat.setting_ctx(&self.svc.dc).with_db(self.sched.db_id);
            let budget = self
                .svc
                .settings
                .get(
                    &sprig_logic::settings::JOB_BUDGET_MS,
                    &sctx,
                    Some(&cat.settings),
                )
                .max(1);
            (target, args, Duration::from_millis(budget))
        };
        let name = crate::inherited::tenant_name(&self.svc, self.tenant).await?;
        let principal =
            crate::triggers::principal_of(&self.login, self.tenant.0, &name, self.sched.definer)
                .await?;
        let caller = Caller {
            principal,
            peer: SocketAddr::from(([127, 0, 0, 1], 0)),
            tls: None,
            listener: "schedule",
            generation: 0,
            authorization: None,
            client_dc: None,
        };
        let q = |s: &str| format!("`{}`", s.replace('`', "``"));
        let list: Vec<String> = args.iter().map(literal).collect();
        let sql = format!(
            "CALL {}.{}({})",
            q(&self.sched.db),
            q(&target),
            list.join(", ")
        );
        let (tx, rx) = mpsc::channel(4);
        let engine = Arc::clone(&self.engine);
        let call = SqlCall {
            database: self.sched.db.clone(),
            sql,
            multi: false,
        };
        let task = tokio::spawn(async move { engine.run(&caller, call, None, tx).await });
        let r = tokio::time::timeout(budget, collect(rx, 1 << 20, None)).await;
        let Ok(r) = r else {
            // The session goes with its task: its transaction rolls back.
            task.abort();
            return Err(sprig_common::SprigError::internal(BUDGET));
        };
        let _ = task.await;
        r.map(|_| ())
    }
}

/// The error text of a run stopped by `logic.job_budget_ms`.
const BUDGET: &str = "the run passed logic.job_budget_ms";

/// A SQL literal of a schedule's JSON argument.
fn literal(v: &serde_json::Value) -> String {
    let s = |t: &str| format!("'{}'", t.replace('\\', "\\\\").replace('\'', "''"));
    match v {
        serde_json::Value::Null => "NULL".to_owned(),
        serde_json::Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_owned(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(t) => s(t),
        other => s(&other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn arguments_are_literals() {
        use serde_json::json;
        assert_eq!(super::literal(&json!(null)), "NULL");
        assert_eq!(super::literal(&json!(3)), "3");
        assert_eq!(super::literal(&json!("it's")), "'it''s'");
        assert_eq!(super::literal(&json!(true)), "TRUE");
    }
}
