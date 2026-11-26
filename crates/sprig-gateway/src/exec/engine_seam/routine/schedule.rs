//! `CREATE [OR REPLACE] SCHEDULE` on an engine database: the logic
//! records only (module, version, schedule row); the gateway's runner
//! (`crate::engine_schedules`) fires it on the tenant's leader.
//!
//! ```text
//! the cron text and zone checked (39873), its first fire time computed
//! CREATE ROUTINE on the database
//! the target: a logic procedure or function of the database, or one of
//!   its SQL stored procedures (39860 otherwise); another database's is
//!   refused as for MariaDB databases
//! one record transaction: the module (kind schedule, definer security,
//!   engine tier), a version whose source is the schedule's clauses, the
//!   schedule row (cron, zone, target, arguments, overlap, next fire
//!   time; a replaced schedule keeps its run state), the version active
//! ```
//!
//! `ALTER SCHEDULE [db.]name PAUSE | RESUME` (any database: both runners
//! read the same record): `PAUSE` sets the record's `paused`, so no later
//! tick fires it (a run already started finishes); `RESUME` clears it and
//! moves the next fire time to the first one after now, so the time spent
//! paused fires nothing (no misfire or late run). The user needs `ALTER
//! ROUTINE` or `CREATE ROUTINE` on the database.

use std::sync::Arc;

use serde_json::json;
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_common::SprigCode;
use sprig_logic::LogicStore;
use sprig_logic::catalog::{ModuleRow, NewVersion, ScheduleRow};
use sprig_logic::declare::{Declaration, TierPref};

use super::{PassSession, Result, SprigError, database_key};
use crate::exec::tenant::{Step, scrub_typed};
use crate::pipeline::ResultSink;
use crate::tenant::session::LocalWarning;
use sprig_proto_mysql::StatusFlags;
use sprig_proto_mysql::packets::OkPacket;

/// `ALTER SCHEDULE [db.]name PAUSE | RESUME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::exec) struct AlterSchedule {
    db: Option<String>,
    name: String,
    pause: bool,
}

/// The statement's words: blanks split them, backticks quote.
fn words(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = sql.trim().trim_end_matches(';').chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '`' if quoted && chars.peek() == Some(&'`') => {
                chars.next();
                cur.push('`');
            }
            '`' => quoted = !quoted,
            c if !quoted && c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            '.' if !quoted => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                out.push(".".to_owned());
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Parses `ALTER SCHEDULE [db.]name PAUSE | RESUME`; `None` for any other
/// statement.
pub(in crate::exec) fn parse_alter_schedule(sql: &[u8]) -> Option<AlterSchedule> {
    let head = sql.get(..sql.len().min(512))?;
    let head = std::str::from_utf8(head).ok()?;
    let w = words(head);
    if w.len() < 4 || !w[0].eq_ignore_ascii_case("alter") || !w[1].eq_ignore_ascii_case("schedule")
    {
        return None;
    }
    let (db, name, rest) = if w.len() == 6 && w[3] == "." {
        (Some(w[2].clone()), w[4].clone(), &w[5])
    } else if w.len() == 4 && w[2] != "." {
        (None, w[2].clone(), &w[3])
    } else {
        return None;
    };
    let pause = match rest.to_ascii_lowercase().as_str() {
        "pause" => true,
        "resume" => false,
        _ => return None,
    };
    (!name.is_empty() && db.as_deref().is_none_or(|d| !d.is_empty())).then_some(AlterSchedule {
        db,
        name,
        pause,
    })
}

impl PassSession {
    /// Deploys schedule `decl` of engine database `db` (see the module
    /// docs); the notes are the deploy's warnings.
    #[allow(clippy::too_many_lines)] // the checks, then one record transaction
    pub(in crate::exec) async fn logic_deploy_schedule(
        &mut self,
        decl: Declaration,
        db: &str,
    ) -> Result<Vec<String>> {
        if decl.tier == TierPref::Mariadb {
            return Err(SprigError::new(
                SprigCode::LogicTierUnavailable,
                format!(
                    "SPRIG_TIER = 'mariadb': database {db} is stored on the engine, where code \
                     runs on the engine tier"
                ),
            ));
        }
        let spec = decl
            .schedule
            .clone()
            .ok_or_else(|| SprigError::internal("a schedule without its clauses"))?;
        let cron = sprig_logic::cron::Cron::parse(&spec.cron, &spec.time_zone)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let next_ms = cron.next_after_ms(now).ok_or_else(|| {
            SprigError::new(
                SprigCode::LogicScheduleInvalid,
                format!("{} never fires", spec.cron),
            )
        })?;
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(db, lctn);
        let row = catalog.database(&db_key).cloned().ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })?;
        if spec
            .target
            .db
            .as_deref()
            .is_some_and(|x| database_key(x, lctn) != db_key)
        {
            return Err(SprigError::new(
                SprigCode::LogicUnsupported,
                "a schedule calls a procedure of its own database",
            ));
        }
        let user = ts
            .principal
            .user_id
            .ok_or_else(|| SprigError::internal("a logic deploy without a user"))?;
        let user_name = ts.principal.user.clone();
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        if !grants.unrestricted()
            && !grants.allows(
                Action::CreateRoutine,
                AuthzTarget::Database { db_key: &db_key },
            )
        {
            return Err(SprigError::standard(
                1044,
                *b"42000",
                format!("Access denied for user '{user_name}' to database '{db}'"),
            ));
        }
        // An SQL stored procedure of the database may be the target too.
        let sql_procedure = crate::engine::catalog::of(&catalog).is_some_and(|e| {
            e.routines.keys().any(|(d, k, n)| {
                *d == row.db_id
                    && *k == crate::engine::routine::Kind::Procedure
                    && n.eq_ignore_ascii_case(&spec.target.name)
            })
        });
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let store = LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let mut target = None;
        for kind in ["procedure", "function"] {
            if let Some(m) = store
                .module(&mut *conn, row.db_id.0, kind, &spec.target.name)
                .await?
            {
                target = Some(m);
                break;
            }
        }
        let target_id = match (&target, sql_procedure) {
            (Some(m), _) if m.active_version.is_some() => m.module_id,
            (Some(_), false) => {
                return Err(SprigError::new(
                    SprigCode::LogicNotFound,
                    format!("{} has no active version", spec.target.name),
                ));
            }
            (_, true) => 0,
            (None, false) => {
                return Err(SprigError::new(
                    SprigCode::LogicNotFound,
                    format!("no procedure {} in {}", spec.target.name, row.name),
                ));
            }
        };
        let kind = decl.kind.as_str();
        let existing = store
            .module(&mut *conn, row.db_id.0, kind, &decl.name)
            .await?;
        if existing.is_some() && !decl.or_replace {
            return Err(SprigError::standard(
                1304,
                *b"42000",
                format!("SCHEDULE {} already exists", decl.name),
            ));
        }
        let (module, exists) = match existing {
            Some(mut m) => {
                "definer".clone_into(&mut m.security);
                TierPref::Engine.as_str().clone_into(&mut m.tier_pref);
                (m, true)
            }
            None => (
                ModuleRow {
                    module_id: sprig_catalog::store::rec::CoreRecords::next_ids(
                        &mut *conn,
                        sprig_catalog::store::Counter::Object,
                        1,
                    )
                    .await?,
                    db_id: row.db_id.0,
                    name: decl.name.clone(),
                    kind: kind.to_owned(),
                    runtime: "sql".to_owned(),
                    security: "definer".to_owned(),
                    tier_pref: TierPref::Engine.as_str().to_owned(),
                    active_version: None,
                    state: "active".to_owned(),
                    created_by: user.0,
                },
                false,
            ),
        };
        if exists {
            store
                .update_options(&mut *conn, &module, catalog.version)
                .await?;
        } else {
            store
                .create_module(&mut *conn, &module, catalog.version)
                .await?;
        }
        let version = store.next_version(&mut *conn, module.module_id).await?;
        let detail = "schedules of engine databases run from the gateway on the tenant's leader";
        store
            .add_version(
                &mut *conn,
                module.module_id,
                version,
                &NewVersion {
                    source: &serde_json::to_string(&spec).unwrap_or_default(),
                    signature: json!({"params": [], "events": [], "target": target_id}),
                    catalog_version: catalog.version,
                    tier: "engine",
                    placement: json!({
                        "tier": "engine",
                        "reasons": [{"code": "schedule.engine", "at": "source", "detail": detail}],
                        "route": null,
                        "objects": [spec.target.name.clone()],
                        "cost": 0,
                    }),
                    plsql: None,
                    created_by: user.0,
                },
            )
            .await?;
        store
            .put_schedule(
                &mut *conn,
                &ScheduleRow {
                    module_id: module.module_id,
                    cron: spec.cron.clone(),
                    time_zone: spec.time_zone.clone(),
                    target_module_id: target_id,
                    args: serde_json::Value::from(spec.args.clone()).to_string(),
                    overlap: if spec.queue_one { "queue_one" } else { "skip" }.to_owned(),
                    paused: false,
                    next_fire_ms: next_ms,
                    last_fire_ms: None,
                    last_outcome: None,
                    running_job_id: None,
                    queued: false,
                },
            )
            .await?;
        store
            .activate(&mut *conn, module.module_id, version, catalog.version)
            .await?;
        conn.commit().await?;
        let (utc, local) = cron.texts_ms(next_ms);
        tracing::info!(%tenant, module = module.module_id, version, name = %decl.name, "schedule on an engine database deployed");
        Ok(vec![format!(
            "schedule {} version {version}: next run {utc} UTC ({local} in {})",
            decl.name, spec.time_zone
        )])
    }

    /// `ALTER SCHEDULE [db.]name PAUSE | RESUME` (see the module docs): an
    /// OK with a note, or the error; `None` for any other statement.
    pub(in crate::exec) async fn tenant_alter_schedule(
        &mut self,
        text: &[u8],
        status: StatusFlags,
        out: &mut dyn ResultSink,
    ) -> Result<Option<Step>> {
        let Some(a) = parse_alter_schedule(text) else {
            return Ok(None);
        };
        match self.logic_alter_schedule(&a).await {
            Ok(note) => {
                if let Some(ts) = self.tenant.as_deref_mut() {
                    ts.warnings = Some(vec![LocalWarning {
                        level: "Note",
                        code: 1105,
                        message: note,
                    }]);
                }
                let mut ok = OkPacket::new(status);
                ok.warnings = 1;
                out.ok(ok).await?;
                Ok(Some(Step {
                    errored: false,
                    status: Some(status),
                }))
            }
            Err(e) => {
                out.error(&scrub_typed(e)).await?;
                Ok(Some(Step {
                    errored: true,
                    status: None,
                }))
            }
        }
    }

    #[allow(clippy::too_many_lines)] // the checks, then one record transaction
    async fn logic_alter_schedule(&mut self, a: &AlterSchedule) -> Result<String> {
        let current = self.current_logical();
        let ts = self
            .tenant
            .as_deref_mut()
            .ok_or_else(|| SprigError::internal("no tenant"))?;
        let db =
            a.db.clone()
                .or(current)
                .ok_or_else(|| SprigError::standard(1046, *b"3D000", "No database selected"))?;
        let catalog = match ts.catalog() {
            Some(c) => Arc::clone(c),
            None => ts.refresh().await?,
        };
        let lctn = ts.lower_case_table_names();
        let db_key = database_key(&db, lctn);
        let row = catalog.database(&db_key).cloned().ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{db}'"))
        })?;
        let grants = crate::tenant::authz::privileges(ts, &catalog);
        let on = AuthzTarget::Database { db_key: &db_key };
        if !grants.unrestricted()
            && !grants.allows(Action::AlterRoutine, on)
            && !grants.allows(Action::CreateRoutine, on)
        {
            return Err(SprigError::standard(
                1044,
                *b"42000",
                format!(
                    "Access denied for user '{}' to database '{}'",
                    ts.principal.user, row.name
                ),
            ));
        }
        let who = format!("{}@{}", ts.principal.user, ts.principal.host);
        let svc = Arc::clone(ts.services());
        let tenant = ts.tenant;
        let store = LogicStore::default();
        let mut conn = svc.records.begin(tenant).await?;
        let not_found = || {
            SprigError::new(
                SprigCode::LogicNotFound,
                format!("no schedule {} in {}", a.name, row.name),
            )
        };
        let m = store
            .module(&mut *conn, row.db_id.0, "schedule", &a.name)
            .await?
            .filter(|m| m.state == "active")
            .ok_or_else(not_found)?;
        let mut s = store
            .schedule(&mut *conn, m.module_id)
            .await?
            .ok_or_else(not_found)?;
        let state = if a.pause { "paused" } else { "resumed" };
        if s.paused == a.pause {
            let _ = conn.rollback().await;
            return Ok(format!(
                "schedule {} was already {}",
                a.name,
                if a.pause { "paused" } else { "running" }
            ));
        }
        let cron = sprig_logic::cron::Cron::parse(&s.cron, &s.time_zone)?;
        s.paused = a.pause;
        if !a.pause {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
            s.next_fire_ms = cron.next_after_ms(now).ok_or_else(|| {
                SprigError::new(
                    SprigCode::LogicScheduleInvalid,
                    format!("{} never fires", s.cron),
                )
            })?;
        }
        store.put_schedule(&mut *conn, &s).await?;
        if let Err(e) = store
            .log(
                &mut *conn,
                m.module_id,
                m.active_version,
                "info",
                "deploy",
                &format!("{state} by {who}"),
            )
            .await
        {
            tracing::warn!(error = %e, "logic_log line not written");
        }
        conn.commit().await?;
        tracing::info!(%tenant, module = m.module_id, name = %a.name, state, "schedule altered");
        Ok(if a.pause {
            format!(
                "schedule {} paused; a run already started finishes, and nothing fires until \
                 ALTER SCHEDULE ... RESUME",
                a.name
            )
        } else {
            let (utc, local) = cron.texts_ms(s.next_fire_ms);
            format!(
                "schedule {} resumed: next run {utc} UTC ({local} in {})",
                a.name, s.time_zone
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alter_schedule_parses() {
        let p = |s: &str| parse_alter_schedule(s.as_bytes());
        assert_eq!(
            p("ALTER SCHEDULE nightly PAUSE"),
            Some(AlterSchedule {
                db: None,
                name: "nightly".into(),
                pause: true
            })
        );
        assert_eq!(
            p("alter schedule `my db`.`night``ly` resume;"),
            Some(AlterSchedule {
                db: Some("my db".into()),
                name: "night`ly".into(),
                pause: false
            })
        );
        assert_eq!(
            p("ALTER SCHEDULE shop . s PAUSE"),
            Some(AlterSchedule {
                db: Some("shop".into()),
                name: "s".into(),
                pause: true
            })
        );
        assert_eq!(p("ALTER SCHEDULE s STOP"), None);
        assert_eq!(p("ALTER SCHEDULE s PAUSE NOW"), None);
        assert_eq!(p("ALTER EVENT s DISABLE"), None);
        assert_eq!(p("ALTER SCHEDULE .s PAUSE"), None);
    }
}
