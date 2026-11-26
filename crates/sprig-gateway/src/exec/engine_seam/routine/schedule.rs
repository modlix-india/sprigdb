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

use std::sync::Arc;

use serde_json::json;
use sprig_auth::authz::{Action, Target as AuthzTarget};
use sprig_common::SprigCode;
use sprig_logic::LogicStore;
use sprig_logic::catalog::{ModuleRow, NewVersion, ScheduleRow};
use sprig_logic::declare::{Declaration, TierPref};

use super::{PassSession, Result, SprigError, database_key};

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
}
