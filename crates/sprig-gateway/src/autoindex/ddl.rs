//! Index changes the loop makes, through the catalog's DDL path exactly as
//! a user's `ALTER TABLE` goes (the engine stages, the catalog publishes,
//! the engine applies), and the online build on top of it:
//!
//! ```text
//! build(spec):
//!   absent       the table has no such index
//!   delete_only  } one published version: ALTER TABLE t ADD INDEX
//!   write_only   }   sprig_auto_x (...) IGNORED, staged without entries
//!                    (the online-build mode); every write from then on
//!                    keeps the index, no plan reads it. One node runs every
//!                    statement on one version (the change waits for the
//!                    table's users), so a delete-only version of its own
//!                    would protect nothing; with replicas it becomes one.
//!   backfill     Engine::backfill_index: a batch of rows per transaction,
//!                rows read with shared locks, auto_index.build_pause_ms
//!                between batches; the table is never held
//!   public       ALTER TABLE t ALTER INDEX sprig_auto_x NOT IGNORED: a
//!                metadata-only change; plans may read it
//! drop(name):    ALTER TABLE t DROP INDEX sprig_auto_x (only names
//!                starting sprig_auto_; never another origin's index)
//! ```
//!
//! A build cut short (a crash, a definition change mid-backfill) leaves an
//! `IGNORED` index that writes keep; the next build finds it and starts
//! the backfill again (entries are written idempotently).

use std::sync::Arc;
use std::time::{Duration, Instant};

use sprig_advisor::autoindex::model::{BuildReport, IndexSpec, is_auto_name};
use sprig_catalog::ddl::{
    DdlExecutor, DdlRequest, DdlStatement, LOWER_CASE_TABLE_NAMES, MariaExecutor, ObjectName,
    PhysicalDdl, PhysicalObject, SchemaInfo, database_key,
};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::TenantId;
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigError};

use crate::engine::assign::Env;
use crate::engine::ddlexec::{Hook, Outcome};
use crate::engine::table::TableDef;
use crate::engine::{self, catalog};
use crate::tenant::TenantServices;

/// The inner executor when the cluster's control pool can't be had: an
/// engine table's `ALTER TABLE` needs nothing from MariaDB.
struct NoBackend;

#[async_trait::async_trait]
impl DdlExecutor for NoBackend {
    async fn execute(&self, _stmt: &PhysicalDdl) -> Result<()> {
        Err(SprigError::internal(
            "automatic indexing: no backend for this statement",
        ))
    }
    async fn schema(&self, _schema: &SchemaName) -> Result<Option<SchemaInfo>> {
        Ok(None)
    }
    async fn show_create(
        &self,
        _schema: &SchemaName,
        _kind: ObjectKind,
        _name: &str,
    ) -> Result<Option<String>> {
        Ok(None)
    }
    async fn objects(&self, _schema: &SchemaName) -> Result<Vec<PhysicalObject>> {
        Ok(Vec::new())
    }
    async fn tenant_schemas(&self, _tenant: TenantId) -> Result<Vec<SchemaName>> {
        Ok(Vec::new())
    }
}

/// `names.lower_case_table_names` of the tenant whose catalog is `cat`.
pub(crate) fn lctn(svc: &TenantServices, cat: &TenantCatalog) -> bool {
    svc.settings.get(
        &LOWER_CASE_TABLE_NAMES,
        &cat.setting_ctx(&svc.dc),
        Some(&cat.settings),
    ) == 1
}

/// The engine definition of table `table` of database `db`, as the newest
/// catalog has it.
pub(crate) fn table_def(
    svc: &TenantServices,
    cat: &TenantCatalog,
    db: &str,
    table: &str,
) -> Option<Arc<TableDef>> {
    let lower = lctn(svc, cat);
    let d = cat.database(&database_key(db, lower))?;
    let eng = catalog::of(cat)?;
    eng.table(d.db_id, table, lower)?
        .as_ref()
        .as_ref()
        .ok()
        .cloned()
}

fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Runs `ALTER TABLE db.table <clause>` for the tenant through the
/// catalog's DDL path; `online`: in the online-build mode.
async fn alter(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    db: &str,
    table: &str,
    clause: &str,
    wait: Duration,
    online: bool,
) -> Result<()> {
    let eng = engine::engine()?;
    let cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let lower = lctn(svc, &cat);
    let sql = format!("ALTER TABLE {} {clause}", q(table));
    let logical = format!("ALTER TABLE {}.{} {clause}", q(db), q(table));
    let hook = Hook::new(
        eng,
        tenant,
        Arc::clone(&cat),
        lower,
        engine::ddl::Session {
            env: Env::STRICT,
            wait,
            db: db.to_owned(),
            conn: 0,
            account: ("sprig_autoindex".into(), "localhost".into()),
        },
        false,
    )
    .with_services(Arc::clone(svc));
    let req = DdlRequest {
        tenant,
        statement: DdlStatement::AlterObject {
            kind: ObjectKind::Table,
            name: ObjectName::qualified(db, table),
            if_exists: false,
            rename_to: None,
        },
        sql: &sql,
        logical_sql: &logical,
        current_db: Some(db),
        user: None,
        lower_case_table_names: lower,
        wait: Some(wait),
    };
    let maria = match svc.locator.cluster(tenant).await {
        Ok(c) => svc.locator.ctl_pool(&c).await.ok().map(MariaExecutor::new),
        Err(_) => None,
    };
    let run = async {
        match &maria {
            Some(m) => svc.ddl.execute(&req, &hook.over(m)).await,
            None => svc.ddl.execute(&req, &hook.over(&NoBackend)).await,
        }
    };
    let result = if online {
        engine::ddl::ONLINE_BUILD.scope((), run).await
    } else {
        run.await
    };
    let published = result.as_ref().ok().and_then(|o| o.version);
    let fresh = match published {
        Some(_) => svc.cache.get(tenant, Freshness::Latest).await.ok(),
        None => None,
    };
    let outcome = match (&fresh, result.is_err()) {
        (Some(c), _) => Outcome::Published(c),
        (None, true) => Outcome::Failed,
        (None, false) => Outcome::Unknown,
    };
    let _ = hook.done(outcome).await;
    result.map(|_| ())
}

/// Builds `spec` online as an automatic index; `batch` rows per backfill
/// transaction, `pause` between batches, `wait` for the table's users at
/// each definition change.
///
/// # Errors
/// The DDL path's (1205 when the table's users don't finish in time), the
/// backfill's.
pub(crate) async fn build(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    spec: &IndexSpec,
    batch: usize,
    pause: Duration,
    wait: Duration,
) -> Result<BuildReport> {
    let started = Instant::now();
    let name = spec.name();
    let mut report = BuildReport {
        states: vec!["absent".into()],
        ..BuildReport::default()
    };
    let current = |cat: &TenantCatalog| table_def(svc, cat, &spec.db, &spec.table);
    let cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let Some(def) = current(&cat) else {
        return Err(SprigError::internal(format!(
            "automatic indexing: table {}.{} is gone",
            spec.db, spec.table
        )));
    };
    let existing = def
        .indexes
        .iter()
        .find(|x| x.name.eq_ignore_ascii_case(&name))
        .map(|x| x.ignored);
    match existing {
        // Built and public already (a record lost its last write).
        Some(false) => {
            report.states = vec!["public".into()];
            report.ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            return Ok(report);
        }
        // A build cut short: its index is there, written by every write.
        Some(true) => {}
        None => {
            let clause = format!("{} IGNORED", spec.add_clause());
            alter(svc, tenant, &spec.db, &spec.table, &clause, wait, true).await?;
        }
    }
    report.states.push("delete_only".into());
    report.states.push("write_only".into());
    let cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let Some(def) = current(&cat) else {
        return Err(SprigError::internal("automatic indexing: the table went"));
    };
    let eng = engine::engine()?;
    let cache = svc.cache.clone();
    let (db, table) = (spec.db.clone(), spec.table.clone());
    let svc2 = Arc::clone(svc);
    let version_now = move || {
        let cat = cache.cached(tenant)?;
        table_def(&svc2, &cat, &db, &table).map(|d| d.version)
    };
    let (rows, batches) = eng
        .backfill_index(tenant.0, &def, &name, batch, pause, &version_now)
        .await?;
    report.states.push("backfill".into());
    report.rows = rows;
    report.batches = batches;
    let clause = format!("ALTER INDEX {} NOT IGNORED", q(&name));
    alter(svc, tenant, &spec.db, &spec.table, &clause, wait, false).await?;
    report.states.push("public".into());
    report.ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    Ok(report)
}

/// Drops automatic index `name` of `spec`'s table.
///
/// # Errors
/// 39050-like refusal for a name that isn't an automatic index's; the
/// DDL path's.
pub(crate) async fn drop(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    spec: &IndexSpec,
    name: &str,
    wait: Duration,
) -> Result<()> {
    if !is_auto_name(name) {
        return Err(SprigError::internal(format!(
            "automatic indexing never drops {name}: not an automatic index"
        )));
    }
    let cat = svc.cache.get(tenant, Freshness::Latest).await?;
    let present = table_def(svc, &cat, &spec.db, &spec.table)
        .is_some_and(|d| d.indexes.iter().any(|x| x.name.eq_ignore_ascii_case(name)));
    if !present {
        return Ok(());
    }
    let clause = format!("DROP INDEX {}", q(name));
    alter(svc, tenant, &spec.db, &spec.table, &clause, wait, false).await
}
