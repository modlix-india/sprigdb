//! The scheduler of SQL events on engine databases.
//!
//! ```text
//! spawn(services, login, engine): one task per process
//!   every tick (1 s; SPRIGDB_SQL_EVENTS_TICK_MS), for each tenant whose
//!   catalog this process has loaded and whose unit is led here (or has
//!   no single leader):
//!     each ENABLED event whose latest run time is after its last run:
//!       a run, in its own task: a fresh session as the event's definer
//!       (as the logic peer service opens one), and in it the marked
//!       statement `/* sprig sql event <nonce> */ {"db":..,"name":..}`,
//!       which runs the body as a procedure with no parameters
//!     an event with no runs left (a one-time event after its run, a
//!     repeating one after ENDS): DROP EVENT, or ALTER EVENT ... DISABLE
//!     with ON COMPLETION PRESERVE, in a session as its definer
//! ```
//!
//! Runs missed while no process led the tenant (or before it was loaded
//! here) are skipped, as the servers skip runs missed while their
//! scheduler was off. A run's errors go to this process's log with the
//! event's name; no client sees them.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sprig_common::CancellationToken;
use sprig_common::ids::{DbId, TenantId};
use tokio::sync::mpsc;

use crate::engine::routine::event::{self, EvDef};
use crate::engine::routine::parse::EvStatus;
use crate::http::sql::{Caller, SqlCall, SqlEngine, collect};
use crate::node_engine::{self, UnitLeader};
use crate::tenant::TenantServices;

/// The tick, in milliseconds (tests shorten it).
pub(crate) const TICK_VAR: &str = "SPRIGDB_SQL_EVENTS_TICK_MS";
/// The marked statement's start.
pub(crate) const MARK: &str = "/* sprig sql event ";
/// How far back a run first seen may still be: an event created with no
/// `STARTS` is due at once.
const GRACE: i64 = 2;

/// The marked statement's request.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RunEvent {
    /// The event's database (logical name).
    pub db: String,
    /// The event.
    pub name: String,
}

/// The marked statement of a run of `db`.`name`.
pub(crate) fn statement(db: &str, name: &str) -> String {
    let j = serde_json::to_string(&RunEvent {
        db: db.to_owned(),
        name: name.to_owned(),
    })
    .unwrap_or_default();
    format!("{MARK}{} */ {j}", crate::exec::msg_trigger::nonce())
}

/// The request of a marked statement, when `text` is one (`Err`: marked,
/// but without this process's nonce or unreadable).
pub(crate) fn request(text: &[u8]) -> Option<Result<RunEvent, String>> {
    let rest = text.strip_prefix(MARK.as_bytes())?;
    let s = String::from_utf8_lossy(rest);
    Some(
        s.split_once(" */ ")
            .filter(|(n, _)| *n == crate::exec::msg_trigger::nonce())
            .and_then(|(_, j)| serde_json::from_str(j).ok())
            .ok_or_else(|| "not an event run of this process".to_owned()),
    )
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// What the scheduler keeps per event: (tenant, database, lower-case name).
type Key = (u64, DbId, String);

/// Starts the scheduler; it ends with `stop`.
pub(crate) fn spawn(
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let tick = std::env::var(TICK_VAR)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(Duration::from_secs(1), Duration::from_millis);
    let mut loads = svc.cache.loads();
    tokio::spawn(async move {
        let mut tenants: BTreeSet<u64> = BTreeSet::new();
        // The last run time of each event (or where catching up starts).
        let mut last: HashMap<Key, i64> = HashMap::new();
        // Events whose completion was sent (until the catalog drops them
        // or shows them disabled).
        let mut completing: HashSet<Key> = HashSet::new();
        let mut timer = tokio::time::interval(tick);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                l = loads.recv() => {
                    if let Ok(l) = l {
                        tenants.insert(l.tenant.0);
                    }
                    continue;
                }
                _ = timer.tick() => {}
            }
            let now = unix_now();
            let mut seen: HashSet<Key> = HashSet::new();
            for &t in &tenants {
                if !matches!(
                    node_engine::tenant_leader(t),
                    UnitLeader::Here | UnitLeader::Any
                ) {
                    continue;
                }
                let Some(cat) = svc.cache.cached(TenantId(t)) else {
                    continue;
                };
                let Some(eng) = crate::engine::catalog::of(&cat) else {
                    continue;
                };
                for ((db, lname), ev) in &eng.events {
                    let key = (t, *db, lname.clone());
                    seen.insert(key.clone());
                    let Ok(def) = &ev.def else { continue };
                    let Some(db_name) = cat.databases.get(*db).map(|d| d.name.clone()) else {
                        continue;
                    };
                    // A one-time event not run yet runs even when overdue.
                    let start = if def.at.is_some() { i64::MIN } else { now - GRACE };
                    let from = *last.entry(key.clone()).or_insert(start);
                    let runs = event::due(def, from, now);
                    if let Some(at) = runs {
                        last.insert(key.clone(), at);
                    }
                    let ran_once = def.at.as_ref().is_none_or(|a| runs.is_some() || from >= a.1);
                    let done =
                        def.status == EvStatus::Enabled && event::finished(def, now) && ran_once;
                    let complete = done && completing.insert(key.clone());
                    if runs.is_none() && !complete {
                        continue;
                    }
                    let job = Job {
                        svc: Arc::clone(&svc),
                        login: Arc::clone(&login),
                        engine: Arc::clone(&engine),
                        tenant: TenantId(t),
                        definer: ev.cx.definer.clone(),
                        db: db_name,
                        name: ev.name.clone(),
                        def: def.clone(),
                    };
                    tokio::spawn(job.run(runs.is_some(), complete));
                }
            }
            last.retain(|k, _| seen.contains(k) || !tenants.contains(&k.0));
            completing.retain(|k| seen.contains(k));
        }
    })
}

/// One run (and maybe the completion) of an event.
struct Job {
    svc: Arc<TenantServices>,
    login: Arc<sprig_auth::LoginService>,
    engine: Arc<SqlEngine>,
    tenant: TenantId,
    definer: (String, String),
    db: String,
    name: String,
    def: EvDef,
}

impl Job {
    async fn run(self, body: bool, complete: bool) {
        let caller = match self.caller().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, db = %self.db, event = %self.name,
                    error = %e, "event not run: its definer can't run it");
                return;
            }
        };
        if body {
            metrics::counter!("sprig_sql_event_runs_total").increment(1);
            if let Err(e) = self.statement(&caller, statement(&self.db, &self.name)).await {
                metrics::counter!("sprig_sql_event_failures_total").increment(1);
                tracing::warn!(tenant = %self.tenant, db = %self.db, event = %self.name,
                    error = %e, "event run failed");
            }
        }
        if complete {
            let q = |s: &str| format!("`{}`", s.replace('`', "``"));
            let sql = if self.def.preserve {
                format!("ALTER EVENT {}.{} DISABLE", q(&self.db), q(&self.name))
            } else {
                format!("DROP EVENT IF EXISTS {}.{}", q(&self.db), q(&self.name))
            };
            if let Err(e) = self.statement(&caller, sql).await {
                tracing::warn!(tenant = %self.tenant, db = %self.db, event = %self.name,
                    error = %e, "event completion failed");
            }
        }
    }

    /// A caller for the event's definer.
    async fn caller(&self) -> sprig_common::Result<Caller> {
        let cat = self.svc.cache.cached(self.tenant).ok_or_else(|| {
            sprig_common::SprigError::internal("the tenant's catalog isn't loaded")
        })?;
        let (u, h) = &self.definer;
        let uid = sprig_auth::TenantAuth::of(&cat)
            .and_then(|auth| {
                auth.accounts(u)
                    .iter()
                    .find(|a| a.row.host.eq_ignore_ascii_case(h))
                    .map(|a| a.row.user_id)
            })
            .ok_or_else(|| {
                sprig_common::SprigError::standard(
                    1449,
                    *b"HY000",
                    format!("The user specified as a definer ('{u}'@'{h}') does not exist"),
                )
            })?;
        let name = crate::inherited::tenant_name(&self.svc, self.tenant).await?;
        let principal =
            crate::triggers::principal_of(&self.login, self.tenant.0, &name, uid.0).await?;
        Ok(Caller {
            principal,
            peer: SocketAddr::from(([127, 0, 0, 1], 0)),
            tls: None,
            listener: "event",
            generation: 0,
            authorization: None,
            client_dc: None,
        })
    }

    /// Runs `sql` in a fresh session for `caller`.
    async fn statement(&self, caller: &Caller, sql: String) -> sprig_common::Result<()> {
        let (tx, rx) = mpsc::channel(4);
        let engine = Arc::clone(&self.engine);
        let caller = caller.clone();
        let call = SqlCall {
            database: self.db.clone(),
            sql,
            multi: false,
        };
        let task = tokio::spawn(async move { engine.run(&caller, call, None, tx).await });
        let r = collect(rx, 1 << 20, None).await;
        let _ = task.await;
        r.map(|_| ())
    }
}
