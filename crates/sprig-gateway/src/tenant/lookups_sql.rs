//! Global lookup indexes in SQL (chapter 19): the gateway's side of
//! `sprig_shard::lookup::admin`.
//!
//! - **`SPRIG SHOW LOOKUP INDEXES [FROM t]`** (also `SHOW SPRIG LOOKUP
//!   INDEXES`) lists the tenant's lookup indexes the session can see, read
//!   fresh from the catalog: table, key name, column, unique, origin, state,
//!   conflicts, id and last change. `information_schema.SPRIG_LOOKUP_INDEXES`
//!   is the same rows as a view.
//! - **`SPRIG REBUILD LOOKUP INDEX k ON t`** (needs `ALTER` on `t`) rebuilds
//!   `t`'s lookup `k`: routing stops, every row is checked and repaired,
//!   orphans go, routing resumes. When `k` is a declared unique key (of
//!   one column or several, without the distribution key) without a
//!   lookup (a table the advisor sharded), it builds one, which makes the
//!   key unique across shards (1062 if it isn't).
//! - **`SPRIG DROP LOOKUP INDEX k ON t`** (needs `ALTER`) drops only the
//!   lookup; the key stays unique within each shard.
//!
//! `ALTER TABLE ... ADD UNIQUE`, `CREATE UNIQUE INDEX` and `DROP INDEX` on
//! sharded tables go through the DDL path (`exec::router::lookup_ddl`),
//! which uses [`GwHost`] and [`build_for_key`] from here. `DROP TABLE` and
//! `DROP DATABASE` drop the lookups of the tables they remove
//! ([`owners_dropped`], [`drop_owned`]).
//!
//! A unique lookup whose build fails because its values repeat across
//! shards raises an `advisor.lookup` approvals item naming the table, the
//! key and up to 10 of the values, through the controller so that it
//! notifies ([`raise_duplicates`]); approving it
//! once the values are fixed builds the lookup (the controller's sharding
//! loop applies it).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_catalog::ddl::{database_key, object_key};
use sprig_catalog::store::{DbState, Namespace, ObjectKind, ObjectState};
use sprig_catalog::{CatalogLocator, Target, TenantCatalog, VersionKind};
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Tier, TierView};
use sprig_proto_mysql::ColumnDef;
use sprig_shard::ShardMapStore;
use sprig_shard::lookup::admin::{
    Admin, DuplicateReport, LookupHost, NewLookup, lookup_keys, owner_shape,
};
use sprig_shard::lookup::direct::{BuildReport, Pools};
use sprig_shard::lookup::settings::LookupSettings;
use sprig_shard::lookup::{LookupCatalog, LookupOrigin, LookupRecord};
use sprig_sql::names::LocalAnswer;

use super::TenantServices;
use super::session::TenantSession;
use crate::pipeline::text_column;

/// How long a publish waits for every gateway to load the new catalog
/// version when acknowledgements can't be used (each checks the
/// directory's version at most once a second).
pub const PUBLISH_SETTLE: Duration = Duration::from_millis(2500);

/// Rows a back-fill chunk reads (`lookup.backfill_chunk_rows`' default).
pub const CHUNK_ROWS: usize = 1000;

/// The `lookup.*` keys for `tenant`: with its cached catalog's rows when
/// this gateway has it.
#[must_use]
pub fn settings(svc: &TenantServices, tenant: TenantId) -> LookupSettings {
    if let Some(cat) = svc.cache.cached(tenant) {
        let ctx = cat.setting_ctx(&svc.dc);
        LookupSettings::resolve(&ctx, |k, c| svc.settings.get(k, c, Some(&cat.settings)))
    } else {
        let ctx = sprig_settings::SettingCtx::process(&svc.dc).for_tenant(tenant, &[]);
        LookupSettings::resolve(&ctx, |k, c| svc.settings.get(k, c, None))
    }
}

/// `lookup.lock_wait_seconds` for claims and owner checks on `owner`'s
/// lookups, at the owner table's scope (chapter 19), with `cat`'s rows.
#[must_use]
pub fn lock_wait_in(svc: &TenantServices, cat: &TenantCatalog, owner: ObjectId) -> u64 {
    let ctx = cat.object_ctx(&svc.dc, owner);
    svc.settings
        .get(
            &sprig_shard::lookup::settings::LOCK_WAIT_SECONDS,
            &ctx,
            Some(&cat.settings),
        )
        .max(1)
}

/// [`lock_wait_in`] with `tenant`'s cached catalog; without it, the
/// tenant's value from the etcd scopes.
#[must_use]
pub fn lock_wait_seconds(svc: &TenantServices, tenant: TenantId, owner: ObjectId) -> u64 {
    match svc.cache.cached(tenant) {
        Some(cat) => lock_wait_in(svc, &cat, owner),
        None => settings(svc, tenant).lock_wait_seconds,
    }
}

/// The lookup jobs of `tenant`, run by this gateway.
fn admin<'a>(
    svc: &TenantServices,
    tenant: TenantId,
    store: &'a ShardMapStore,
    host: &'a GwHost,
) -> Admin<'a> {
    let s = settings(svc, tenant);
    Admin {
        tenant,
        maps: store,
        host,
        chunk: s.chunk_rows,
        lease: s.build_lease,
    }
}

fn catalog_err(e: mysql_async::Error) -> SprigError {
    SprigError::new(
        SprigCode::CatalogUnavailable,
        format!("lookup indexes: the catalog's cluster: {e}"),
    )
    .with_source(e)
}

/// The tenant's home DC's shard maps.
#[must_use]
pub fn maps(svc: &TenantServices, tenant: TenantId) -> ShardMapStore {
    let home = svc.locator.home_of(tenant);
    let dc = svc.locator.dc_store();
    if home == svc.dc {
        ShardMapStore::new(dc)
    } else {
        ShardMapStore::new(TierView::over(&dc, Tier::Dc(home)))
    }
}

/// [`LookupHost`] for a gateway: the tenant's catalog through its writer
/// endpoint, clusters through the `sprig_ctl` pools, and a publish that is
/// an empty versioned catalog change followed by every gateway's
/// acknowledgement ([`publish_wait`]).
pub struct GwHost {
    /// The services.
    pub svc: Arc<TenantServices>,
    /// The tenant.
    pub tenant: TenantId,
    /// Building for `ADD UNIQUE`: a failed build drops the shards' new key
    /// again, so a duplicates item asks for the statement to be run again.
    pub ddl: bool,
}

impl std::fmt::Debug for GwHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GwHost")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl LookupHost for GwHost {
    async fn catalog(&self) -> Result<Conn> {
        let ep = self.svc.locator.writer(self.tenant).await?;
        let schema = ep.store.schema().to_owned();
        let mut c = ep.pool.get_conn().await.map_err(catalog_err)?;
        sprig_shard::groups::migrate(&mut c, &schema).await?;
        c.query_drop(format!("USE {}", sprig_common::names::quote(&schema)))
            .await
            .map_err(catalog_err)?;
        Ok(c)
    }

    async fn pools(&self, clusters: &[ClusterId]) -> Result<Pools> {
        let mut m = HashMap::new();
        for c in clusters {
            m.insert(c.clone(), self.svc.locator.ctl_pool(c).await?);
        }
        Ok(Arc::new(m))
    }

    async fn publish(&self) -> Result<()> {
        let v = self
            .svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?
            .commit()
            .await?;
        publish_wait(&self.svc, self.tenant, v).await;
        Ok(())
    }

    fn lock_wait_seconds(&self, _db: DbId, owner: ObjectId) -> u64 {
        lock_wait_seconds(&self.svc, self.tenant, owner)
    }

    async fn duplicates(&self, report: &DuplicateReport) {
        if let Err(e) = raise_duplicates(&self.svc, self.tenant, report, self.ddl).await {
            tracing::warn!(tenant = %self.tenant, table = %report.table, key = %report.key,
                error = %e, "the approvals item for a lookup's duplicates wasn't raised");
        }
    }
}

/// Raises the `advisor.lookup` item that reports a failed build's
/// duplicates (target `t<tenant>/o<owner>/lookup/<key>/duplicates`), held
/// for an approval with no timers: approving it after the values are
/// fixed builds the lookup. A live item for the same target is refreshed
/// with the new evidence instead of a second one.
///
/// The item is raised through the controller's approval gate
/// (`/admin/v1/recommendations:gate`), whose hooks notify, as for the
/// controller's own items. When the controller can't be reached (or
/// refuses), the gateway writes the item into the tenant's catalog itself:
/// it is there for people, but no notification goes out.
///
/// # Errors
/// The catalog's.
pub async fn raise_duplicates(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    r: &DuplicateReport,
    ddl: bool,
) -> Result<()> {
    use sprig_approvals::store::RecStore as _;
    use sprig_approvals::{Action, Kind, Level, Mode, Policy, Proposal, Subject};
    let db_name = svc
        .cache
        .cached(tenant)
        .and_then(|c| c.databases.get(r.db).map(|d| d.name.clone()))
        .unwrap_or_else(|| format!("d{}", r.db.0));
    let mut subject = Subject::tenant(tenant, svc.dc.clone());
    subject.db = Some(r.db);
    subject.db_name = Some(db_name.clone());
    subject.object = Some(r.owner);
    subject.object_name = Some(r.table.clone());
    #[allow(clippy::cast_precision_loss)] // a count of values
    let score = r.found.values as f64;
    // After `ADD UNIQUE` the shards' key was dropped again: the statement
    // is what to run once the values are fixed.
    let (text, mut machine) = if ddl {
        (r.ddl_text(&db_name), r.machine())
    } else {
        (r.action_text(&db_name), r.machine())
    };
    machine["ddl"] = serde_json::Value::Bool(ddl);
    let summary = r.summary(&db_name, ddl);
    if let Some(admin) = crate::tenant::rec_sql::admin() {
        // The gate prefixes `t<tenant>/`.
        let target = r.target(tenant);
        let target = target
            .strip_prefix(&format!("t{tenant}/"))
            .unwrap_or(&target);
        let body = serde_json::json!({
            "op": "check",
            "kind": Kind::AdvisorLookup.as_str(),
            "tenant": tenant.0,
            "db": db_name,
            "target": target,
            "action": text,
            "machine": machine,
            "evidence": r.evidence(),
            "summary": summary,
            "score": score,
            "object": r.owner.0,
            "object_name": r.table,
        });
        match admin
            .forward_internal(
                &hyper::Method::POST,
                "/admin/v1/recommendations:gate",
                Bytes::from(body.to_string()),
            )
            .await
        {
            Ok(reply) if reply.status < 300 => {
                let v: serde_json::Value = serde_json::from_slice(&reply.body).unwrap_or_default();
                tracing::info!(tenant = %tenant, table = %r.table, key = %r.key,
                    values = r.found.values, why = %v.get("why").and_then(serde_json::Value::as_str).unwrap_or(""),
                    "approvals item raised for a lookup's duplicates, through the controller");
                return Ok(());
            }
            Ok(reply) => {
                tracing::warn!(tenant = %tenant, status = reply.status,
                    body = %String::from_utf8_lossy(&reply.body),
                    "the controller refused a lookup's duplicates item; writing it without a notification");
            }
            Err(e) => {
                tracing::warn!(tenant = %tenant, error = %e,
                    "the controller is unreachable; writing a lookup's duplicates item without a notification");
            }
        }
    }
    let p = Proposal {
        kind: Kind::AdvisorLookup,
        subject,
        target: r.target(tenant),
        action: Action { text, machine },
        evidence: r.evidence(),
        score,
        summary,
        emergency: false,
    };
    let locator: Arc<dyn sprig_catalog::CatalogLocator> = svc.locator.clone();
    let recs = sprig_approvals::catalog::CatalogRecStore::new(locator, tenant);
    let now = sprig_approvals::model::wall_micros(svc.clock.wall());
    recs.raise(&p, &Policy::fixed(Mode::Approve, Level::Database), &[], now)
        .await?;
    tracing::info!(tenant = %tenant, table = %r.table, key = %r.key, values = r.found.values,
        "approvals item raised for a lookup's duplicates");
    Ok(())
}

/// Waits until every gateway of the tenant's home DC has acknowledged
/// catalog version `v` (`sprig_shard::lookup::publish`), fencing those that
/// don't within `lookup.publish_timeout_ms`; when the acknowledgements
/// can't be used at all, a fixed [`PUBLISH_SETTLE`] instead.
pub async fn publish_wait(svc: &Arc<TenantServices>, tenant: TenantId, v: u64) {
    let timeout = settings(svc, tenant).publish_timeout;
    let store = Arc::clone(maps(svc, tenant).store());
    match sprig_shard::lookup::publish::wait_for_gateways(&store, tenant, v, timeout).await {
        Ok(o) if o.fenced.is_empty() => {
            tracing::debug!(tenant = %tenant, version = v, gateways = o.gateways,
                waited_ms = o.waited.as_millis(), "lookup index change acknowledged");
        }
        Ok(o) => {
            tracing::warn!(tenant = %tenant, version = v, fenced = ?o.fenced,
                waited_ms = o.waited.as_millis(), "lookup index change: gateways fenced");
        }
        Err(e) => {
            tracing::warn!(tenant = %tenant, version = v, error = %e,
                "lookup index change: acknowledgements unavailable; a fixed wait instead");
            tokio::time::sleep(PUBLISH_SETTLE).await;
        }
    }
}

/// A table's group membership as a lookup needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Membership {
    /// Its group.
    pub group: GroupId,
    /// Its database.
    pub db: DbId,
    /// `columns`, `parent`, ...
    pub routing: String,
    /// The key columns of a `columns` member, lowercase.
    pub key: Vec<String>,
    /// `group_members.rk_state`.
    pub rk_state: String,
}

/// `object`'s `group_members` row in a sharded group, if any.
///
/// # Errors
/// The catalog's.
pub async fn membership(c: &mut Conn, object: ObjectId) -> Result<Option<Membership>> {
    let row: Option<(u32, u32, String, Option<String>, String)> = c
        .exec_first(
            "SELECT m.group_id, m.db_id, CAST(m.routing AS CHAR), CAST(m.key_columns AS CHAR), \
             CAST(m.rk_state AS CHAR) FROM group_members m \
             JOIN table_groups g ON g.group_id = m.group_id \
             WHERE m.object_id = ? AND g.kind = 'sharded'",
            (object.0,),
        )
        .await
        .map_err(catalog_err)?;
    Ok(row.map(|(g, db, routing, key, rk)| Membership {
        group: GroupId(g),
        db: DbId(db),
        routing,
        key: key
            .and_then(|k| serde_json::from_str::<Vec<String>>(&k).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.to_ascii_lowercase())
            .collect(),
        rk_state: rk,
    }))
}

fn refused(why: &str) -> SprigError {
    SprigError::new(
        SprigCode::StatementNotDistributable,
        format!("statement not distributable (lookup): {why}"),
    )
}

/// Builds the unique lookup of `table`'s unique key `key` on `columns`
/// (one or several): what `ADD UNIQUE` and `SPRIG REBUILD LOOKUP INDEX`
/// run.
///
/// # Errors
/// 39420 for a member that follows its parent or hasn't filled
/// `_sprig_rk`; 1062 when the values repeat across shards; the lookup
/// jobs' errors.
pub async fn build_for_key(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    object: ObjectId,
    table: &str,
    key: &str,
    columns: &[String],
    ddl: bool,
) -> Result<BuildReport> {
    let host = GwHost {
        svc: Arc::clone(svc),
        tenant,
        ddl,
    };
    let mut cat = host.catalog().await?;
    let m = membership(&mut cat, object)
        .await?
        .ok_or_else(|| refused(&format!("{table} isn't sharded")))?;
    if m.routing != "columns" {
        return Err(refused(&format!(
            "{table} follows its parent's rows; a lookup index can't name them"
        )));
    }
    if m.rk_state != "ready" {
        return Err(refused(&format!(
            "{table}'s routing hashes are still being filled ({}); try again when its group is ready",
            m.rk_state
        )));
    }
    drop(cat);
    let store = maps(svc, tenant);
    let (owner_map, _) = store.load(tenant, m.group).await?;
    let (l, report) = admin(svc, tenant, &store, &host)
        .create(&NewLookup {
            db: m.db,
            owner: object,
            owner_table: table.to_owned(),
            owner_map: Arc::new(owner_map),
            columns: columns.iter().map(|c| c.to_ascii_lowercase()).collect(),
            key: Some(key.to_owned()),
            origin: LookupOrigin::UniqueKey,
        })
        .await?;
    tracing::info!(
        tenant = %tenant, table, lookup = %l.index_name, rows = report.rows,
        inserted = report.inserted, repaired = report.repaired, "lookup index built"
    );
    Ok(report)
}

/// Drops `table`'s lookup `name`; `Ok(false)` when it has none.
///
/// # Errors
/// The lookup jobs' errors.
pub async fn drop_for_key(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    object: ObjectId,
    name: &str,
) -> Result<bool> {
    let host = GwHost {
        svc: Arc::clone(svc),
        tenant,
        ddl: false,
    };
    let store = maps(svc, tenant);
    admin(svc, tenant, &store, &host).drop(object, name).await
}

/// The tables with lookups among what a `DROP TABLE` or `DROP DATABASE`
/// removes, from the session's snapshot (read before the statement runs).
#[must_use]
pub fn owners_dropped(
    ts: &TenantSession,
    dropped: &[sprig_auth::authz::authorizer::Dropped],
) -> Vec<ObjectId> {
    use sprig_auth::authz::authorizer::Dropped;
    let Some(cat) = ts.catalog() else {
        return Vec::new();
    };
    let Some(state) = cat
        .ext
        .get::<crate::exec::router::layout::ShardingState>(sprig_shard::groups::MODULE)
    else {
        return Vec::new();
    };
    let mut out: Vec<ObjectId> = state
        .lookups
        .iter()
        .map(|l| ObjectId(l.object))
        .filter(|o| {
            dropped.iter().any(|d| match d {
                Dropped::Object { object, .. } => object == o,
                Dropped::Database { db, .. } => cat.objects.get(*o).is_some_and(|x| x.db_id == *db),
            })
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Drops every lookup of `owners`, tables a `DROP` removed
/// ([`Admin::drop_owned`]). Returns their names.
///
/// # Errors
/// The lookup jobs' errors; a crash or failure here is finished by the
/// controller's recovery, which drops lookups whose table is gone.
pub async fn drop_owned(
    svc: &Arc<TenantServices>,
    tenant: TenantId,
    owners: &[ObjectId],
) -> Result<Vec<String>> {
    if owners.is_empty() {
        return Ok(Vec::new());
    }
    let host = GwHost {
        svc: Arc::clone(svc),
        tenant,
        ddl: false,
    };
    let store = maps(svc, tenant);
    admin(svc, tenant, &store, &host).drop_owned(owners).await
}

/// The tenant's lookup rows, fresh from the catalog (none before the
/// `sharding` module or the table exist).
///
/// # Errors
/// The catalog's.
pub async fn records(svc: &Arc<TenantServices>, tenant: TenantId) -> Result<Vec<LookupRecord>> {
    let ep = svc.locator.writer(tenant).await?;
    let schema = ep.store.schema().to_owned();
    let mut c = ep.pool.get_conn().await.map_err(catalog_err)?;
    let has: Option<u64> = c
        .exec_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'lookup_indexes'",
            (schema.as_str(),),
        )
        .await
        .map_err(catalog_err)?;
    if has.unwrap_or(0) == 0 {
        return Ok(Vec::new());
    }
    c.query_drop(format!("USE {}", sprig_common::names::quote(&schema)))
        .await
        .map_err(catalog_err)?;
    LookupCatalog.rows(&mut c).await
}

/// A lookup's owner and table, as the grants need them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupOwner {
    /// The owner's database.
    pub db: DbId,
    /// The owner's name, lowercase.
    pub table: String,
    /// The lookup table's name.
    pub physical: String,
}

/// The database a shard or reference schema of `tenant` belongs to
/// (`t<tenant>_d<db>_...`).
fn schema_db(tenant: TenantId, schema: &str) -> Option<DbId> {
    let rest = schema.strip_prefix(&format!("t{}_d", tenant.0))?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() || !rest[digits.len()..].starts_with('_') {
        return None;
    }
    digits.parse().ok().map(DbId)
}

/// Grants on the lookup tables of the tables accounts hold table or
/// column grants on (Q50): a user granted only some tables reaches their
/// lookups the way a database-wide grant does, and no others.
///
/// - `SELECT` on the owner gives `SELECT` on its lookup tables (routing
///   reads them);
/// - `INSERT`, `UPDATE` or `DELETE` on the owner gives `SELECT`, `INSERT`,
///   `UPDATE` and `DELETE` on them (claims insert, read `FOR UPDATE` and
///   take orphans over; releases delete).
///
/// `places` maps `(database, lookup table)` to the schemas of this cluster
/// holding it (`t<tenant>_d<db>_g<lookup group>_s<n>`). Accounts without a
/// grant on an owner get nothing; database-wide grants already cover the
/// lookup schemas, which share the database's pattern. Clients never name
/// lookup tables (`__sprig_` names are refused outside the gateway's own
/// statements), so the grant reaches only the gateway's claims and reads.
pub fn lookup_grants(
    want: &mut sprig_auth::authz::backend::BackendState,
    tenant: TenantId,
    owners: &[LookupOwner],
    places: &std::collections::BTreeMap<(DbId, String), Vec<String>>,
) {
    use sprig_auth::authz::backend::BackendTarget;
    use sprig_auth::authz::{Action, ActionSet};
    let read = ActionSet::of(&[Action::Select]);
    let write = ActionSet::of(&[Action::Insert, Action::Update, Action::Delete]);
    let all = read.union(write);
    for grants in want.values_mut() {
        let mut add: std::collections::BTreeMap<BackendTarget, ActionSet> =
            std::collections::BTreeMap::new();
        for (t, a) in grants.iter() {
            let (BackendTarget::Table(schema, table) | BackendTarget::Column(schema, table, _)) = t
            else {
                continue;
            };
            if table.starts_with("__sprig_") {
                continue;
            }
            let Some(db) = schema_db(tenant, schema) else {
                continue;
            };
            let need = if !a.intersect(write).is_empty() {
                all
            } else if a.has(Action::Select) {
                read
            } else {
                continue;
            };
            for o in owners
                .iter()
                .filter(|o| o.db == db && o.table.eq_ignore_ascii_case(table))
            {
                for s in places.get(&(db, o.physical.clone())).into_iter().flatten() {
                    let e = add
                        .entry(BackendTarget::Table(s.clone(), o.physical.clone()))
                        .or_default();
                    *e = e.union(need);
                }
            }
        }
        for (t, a) in add {
            let e = grants.entry(t).or_default();
            *e = e.union(a);
        }
    }
}

/// [`lookup_grants`] for `cat`'s lookups on `cluster`: the lookup tables
/// there are read as `sprig_ctl` from `information_schema`, only when some
/// account holds a table or column grant on an owner.
///
/// # Errors
/// The cluster's.
pub async fn add_lookup_grants(
    svc: &TenantServices,
    cat: &TenantCatalog,
    cluster: &ClusterId,
    want: &mut sprig_auth::authz::backend::BackendState,
) -> Result<()> {
    use sprig_auth::authz::backend::BackendTarget;
    let Some(state) = cat
        .ext
        .get::<crate::exec::router::layout::ShardingState>(sprig_shard::groups::MODULE)
    else {
        return Ok(());
    };
    let owners: Vec<LookupOwner> = state
        .lookups
        .iter()
        .filter_map(|l| {
            let o = cat.objects.get(ObjectId(l.object))?;
            Some(LookupOwner {
                db: o.db_id,
                table: o.name.to_ascii_lowercase(),
                physical: l.physical.clone(),
            })
        })
        .collect();
    let named = want.values().flat_map(|g| g.keys()).any(|t| match t {
        BackendTarget::Table(s, n) | BackendTarget::Column(s, n, _) => schema_db(cat.tenant, s)
            .is_some_and(|db| {
                owners
                    .iter()
                    .any(|o| o.db == db && o.table.eq_ignore_ascii_case(n))
            }),
        _ => false,
    });
    if !named {
        return Ok(());
    }
    let pool = svc.locator.ctl_pool(cluster).await?;
    let mut conn = pool.get_conn().await.map_err(|e| {
        SprigError::new(
            SprigCode::BackendUnavailable,
            format!("sprig_ctl on {cluster}: {e}"),
        )
    })?;
    let groups: std::collections::BTreeSet<u32> = state.lookups.iter().map(|l| l.group).collect();
    let mut schemas = Vec::new();
    for g in groups {
        schemas.extend(
            sprig_catalog::infoschema::schemas_like(
                &mut conn,
                &format!("t{}\\_d%\\_g{g}\\_s%", cat.tenant.0),
            )
            .await?,
        );
    }
    let physicals = owners
        .iter()
        .map(|o| sprig_catalog::infoschema::literal(&o.physical))
        .collect::<Vec<_>>()
        .join(", ");
    let rows: Vec<(String, String)> = sprig_catalog::infoschema::read_per_schema(
        &mut conn,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES",
        "TABLE_SCHEMA",
        &schemas,
        &format!(" AND TABLE_NAME IN ({physicals})"),
    )
    .await?;
    let mut places: std::collections::BTreeMap<(DbId, String), Vec<String>> =
        std::collections::BTreeMap::new();
    for (schema, table) in rows {
        if let Some(db) = schema_db(cat.tenant, &schema) {
            places.entry((db, table)).or_default().push(schema);
        }
    }
    lookup_grants(want, cat.tenant, &owners, &places);
    Ok(())
}

/// A parsed lookup statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupStmt {
    /// `SPRIG SHOW LOOKUP INDEXES [FROM [db.]t]`.
    Show {
        /// The table's database, if named.
        db: Option<String>,
        /// The table, if named.
        table: Option<String>,
    },
    /// `SPRIG REBUILD LOOKUP INDEX k ON [db.]t`.
    Rebuild {
        /// The key.
        name: String,
        /// The table's database, if named.
        db: Option<String>,
        /// The table.
        table: String,
    },
    /// `SPRIG DROP LOOKUP INDEX k ON [db.]t`.
    Drop {
        /// The key.
        name: String,
        /// The table's database, if named.
        db: Option<String>,
        /// The table.
        table: String,
    },
}

/// Words and quoted names of `text`, with `.` as its own token; `None` for
/// anything else (strings, comments, other punctuation).
fn words(text: &str) -> Option<Vec<(String, bool)>> {
    let b = text.trim().trim_end_matches(';').trim();
    let mut out = Vec::new();
    let mut it = b.chars().peekable();
    while let Some(&ch) = it.peek() {
        if ch.is_whitespace() {
            it.next();
        } else if ch == '.' {
            it.next();
            out.push((".".to_owned(), false));
        } else if ch == '`' {
            it.next();
            let mut s = String::new();
            loop {
                match it.next()? {
                    '`' if it.peek() == Some(&'`') => {
                        it.next();
                        s.push('`');
                    }
                    '`' => break,
                    c => s.push(c),
                }
            }
            out.push((s, true));
        } else if ch.is_alphanumeric() || ch == '_' || ch == '$' {
            let mut s = String::new();
            while let Some(&c) = it.peek() {
                if c.is_alphanumeric() || c == '_' || c == '$' {
                    s.push(c);
                    it.next();
                } else {
                    break;
                }
            }
            out.push((s, false));
        } else {
            return None;
        }
    }
    Some(out)
}

/// [`parse`] for a statement's bytes, after a cheap look at its first
/// word (every statement passes here).
#[must_use]
pub fn parse_bytes(text: &[u8]) -> Option<LookupStmt> {
    let t = text.trim_ascii_start();
    let first = t.get(..5)?;
    if !(first.eq_ignore_ascii_case(b"SPRIG") || first[..4].eq_ignore_ascii_case(b"SHOW")) {
        return None;
    }
    if !t.windows(6).any(|w| w.eq_ignore_ascii_case(b"LOOKUP")) {
        return None;
    }
    parse(std::str::from_utf8(t).ok()?)
}

/// `text` as a lookup statement; `None` for any other statement.
#[must_use]
pub fn parse(text: &str) -> Option<LookupStmt> {
    let w = words(text)?;
    let kw = |i: usize, k: &str| {
        w.get(i)
            .is_some_and(|(s, q)| !q && s.eq_ignore_ascii_case(k))
    };
    // `[db.]t` from token i: (db, table, next index).
    let name_at = |i: usize| -> Option<(Option<String>, String, usize)> {
        let (a, _) = w.get(i)?;
        if a == "." {
            return None;
        }
        if w.get(i + 1).is_some_and(|(s, q)| !q && s == ".") {
            let (b, _) = w.get(i + 2)?;
            Some((Some(a.clone()), b.clone(), i + 3))
        } else {
            Some((None, a.clone(), i + 1))
        }
    };
    let show_at = if kw(0, "SPRIG") && kw(1, "SHOW") || kw(0, "SHOW") && kw(1, "SPRIG") {
        Some(2)
    } else {
        None
    };
    if let Some(i) = show_at {
        if !(kw(i, "LOOKUP") && (kw(i + 1, "INDEXES") || kw(i + 1, "INDEX"))) {
            return None;
        }
        let i = i + 2;
        if i == w.len() {
            return Some(LookupStmt::Show {
                db: None,
                table: None,
            });
        }
        if kw(i, "FROM") || kw(i, "IN") {
            let (db, table, j) = name_at(i + 1)?;
            return (j == w.len()).then_some(LookupStmt::Show {
                db,
                table: Some(table),
            });
        }
        return None;
    }
    if !kw(0, "SPRIG") || !kw(2, "LOOKUP") || !kw(3, "INDEX") {
        return None;
    }
    let (name, _) = w.get(4)?.clone();
    if !kw(5, "ON") {
        return None;
    }
    let (db, table, j) = name_at(6)?;
    if j != w.len() {
        return None;
    }
    if kw(1, "REBUILD") {
        Some(LookupStmt::Rebuild { name, db, table })
    } else if kw(1, "DROP") {
        Some(LookupStmt::Drop { name, db, table })
    } else {
        None
    }
}

/// The columns of `SPRIG SHOW LOOKUP INDEXES`, as
/// `information_schema.SPRIG_LOOKUP_INDEXES` names them.
pub const VIEW_COLUMNS: [&str; 10] = [
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "INDEX_NAME",
    "COLUMN_NAME",
    "IS_UNIQUE",
    "ORIGIN",
    "STATE",
    "CONFLICTS",
    "LOOKUP_ID",
    "UPDATED_AT",
];

/// `SPRIG SHOW LOOKUP INDEXES`' columns.
#[must_use]
pub fn columns(charset: u16) -> Vec<ColumnDef> {
    VIEW_COLUMNS
        .iter()
        .map(|n| text_column(n, charset))
        .collect()
}

/// What a lookup statement answers.
#[derive(Debug)]
pub enum LookupAnswer {
    /// OK with this many rows (owner rows a build read).
    Ok {
        /// Rows affected.
        affected: u64,
    },
    /// `SHOW`.
    Rows(LocalAnswer),
}

fn no_database() -> SprigError {
    SprigError::standard(1046, *b"3D000", "No database selected")
}

/// `db.t` as the session names it: its database and table rows.
fn resolve(
    ts: &TenantSession,
    cat: &TenantCatalog,
    db: Option<&str>,
    table: &str,
) -> Result<(String, ObjectId, String)> {
    let lctn = ts.lower_case_table_names();
    let dbn = db.ok_or_else(no_database)?;
    let d = cat
        .database(&database_key(dbn, lctn))
        .filter(|d| d.state == DbState::Active)
        .ok_or_else(|| {
            SprigError::standard(1049, *b"42000", format!("Unknown database '{dbn}'"))
        })?;
    let o = cat
        .object(
            d.db_id,
            Namespace::Relation,
            &object_key(ObjectKind::Table, table, lctn),
        )
        .filter(|o| o.state == ObjectState::Active)
        .ok_or_else(|| {
            SprigError::standard(
                1146,
                *b"42S02",
                format!("Table '{dbn}.{table}' doesn't exist"),
            )
        })?;
    Ok((d.name.clone(), o.object_id, o.name.clone()))
}

fn need_alter(ts: &TenantSession, db: &str, object: ObjectId, table: &str) -> Result<()> {
    super::authz::settings_check(
        ts,
        Target::Object(object),
        false,
        false,
        false,
        Some(db),
        Some(table),
        sprig_proto_mysql::Persona::MariaDb,
    )
}

/// The rows the session may see, as text: one per lookup.
async fn visible_rows(
    ts: &TenantSession,
    cat: &TenantCatalog,
    only: Option<ObjectId>,
) -> Result<Vec<Vec<Option<Bytes>>>> {
    let svc = Arc::clone(ts.services());
    let text = |s: &str| Some(Bytes::copy_from_slice(s.as_bytes()));
    let mut out = Vec::new();
    for r in records(&svc, ts.tenant).await? {
        if only.is_some_and(|o| o != r.owner) {
            continue;
        }
        let Some(obj) = cat.objects.get(r.owner) else {
            continue;
        };
        let Some(d) = cat.databases.get(obj.db_id) else {
            continue;
        };
        if !super::authz::settings_visible(ts, cat, &d.name, d.db_id, Some(obj.object_id)) {
            continue;
        }
        out.push(vec![
            text(&d.name),
            text(&obj.name),
            text(&r.index_name),
            text(&r.columns.join(",")),
            text(if r.unique { "YES" } else { "NO" }),
            text(r.origin.as_str()),
            text(r.state.as_str()),
            text(&r.conflicts.to_string()),
            text(&r.id.0.to_string()),
            text(&r.updated_at),
        ]);
    }
    Ok(out)
}

/// Runs a lookup statement (module docs).
///
/// # Errors
/// 1046, 1049, 1146, 1142, 1091, 1062, 39420 and the lookup jobs' errors.
pub async fn execute(
    ts: &TenantSession,
    cat: &Arc<TenantCatalog>,
    current: Option<&str>,
    stmt: &LookupStmt,
    charset: u16,
) -> Result<LookupAnswer> {
    let svc = Arc::clone(ts.services());
    match stmt {
        LookupStmt::Show { db, table } => {
            let only = match table {
                Some(t) => Some(resolve(ts, cat, db.as_deref().or(current), t)?.1),
                None => None,
            };
            Ok(LookupAnswer::Rows(LocalAnswer {
                columns: columns(charset),
                rows: visible_rows(ts, cat, only).await?,
            }))
        }
        LookupStmt::Drop { name, db, table } => {
            let (dbn, object, tname) = resolve(ts, cat, db.as_deref().or(current), table)?;
            need_alter(ts, &dbn, object, &tname)?;
            if !drop_for_key(&svc, ts.tenant, object, name).await? {
                return Err(SprigError::standard(
                    1091,
                    *b"42000",
                    format!("Can't DROP LOOKUP INDEX `{name}`; check that it exists"),
                ));
            }
            Ok(LookupAnswer::Ok { affected: 0 })
        }
        LookupStmt::Rebuild { name, db, table } => {
            let (dbn, object, tname) = resolve(ts, cat, db.as_deref().or(current), table)?;
            need_alter(ts, &dbn, object, &tname)?;
            let host = GwHost {
                svc: Arc::clone(&svc),
                tenant: ts.tenant,
                ddl: false,
            };
            let store = maps(&svc, ts.tenant);
            let admin = admin(&svc, ts.tenant, &store, &host);
            let mut c = host.catalog().await?;
            if admin.find(&mut c, object, name).await?.is_some() {
                drop(c);
                let r = admin.rebuild(object, &tname, name).await?;
                return Ok(LookupAnswer::Ok { affected: r.rows });
            }
            // A declared unique key without a lookup: build one.
            let m = membership(&mut c, object)
                .await?
                .ok_or_else(|| refused(&format!("{tname} isn't sharded")))?;
            drop(c);
            let (owner_map, _) = store.load(ts.tenant, m.group).await?;
            let r0 = owner_map
                .ranges
                .first()
                .ok_or_else(|| SprigError::internal("a group without shards"))?;
            let schema = sprig_shard::shard_schema(ts.tenant, m.db, m.group, r0.shard);
            let pool = svc.locator.ctl_pool(&r0.cluster).await?;
            let mut sc = pool.get_conn().await.map_err(catalog_err)?;
            let shape = owner_shape(&mut sc, &schema, &tname).await?;
            let Some((key, cols)) = lookup_keys(&shape, &m.key)
                .into_iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
            else {
                return Err(SprigError::standard(
                    1176,
                    *b"42000",
                    format!(
                        "Key '{name}' doesn't exist in table '{tname}' as a unique key without \
                         the distribution key"
                    ),
                ));
            };
            let r = build_for_key(&svc, ts.tenant, object, &tname, &key, &cols, false).await?;
            Ok(LookupAnswer::Ok { affected: r.rows })
        }
    }
}

/// `information_schema.SPRIG_LOOKUP_INDEXES` as a derived table of literal
/// rows.
///
/// # Errors
/// The catalog's.
pub async fn view_sql(ts: &TenantSession, cat: &Arc<TenantCatalog>, nbe: bool) -> Result<String> {
    let rows = visible_rows(ts, cat, None).await?;
    let lit = |v: &Option<Bytes>| match v {
        Some(b) => sprig_sql::names::rewrite::sql_string(&String::from_utf8_lossy(b), nbe),
        None => "NULL".to_owned(),
    };
    let mut sql = String::from("(");
    if rows.is_empty() {
        let cols: Vec<String> = VIEW_COLUMNS
            .iter()
            .map(|n| format!("CAST(NULL AS CHAR) AS `{n}`"))
            .collect();
        let _ = write!(sql, "SELECT {} FROM DUAL WHERE 0", cols.join(", "));
    }
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(" UNION ALL ");
        }
        sql.push_str("SELECT ");
        for (j, (v, n)) in row.iter().zip(VIEW_COLUMNS).enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            if i == 0 {
                let _ = write!(sql, "CAST({} AS CHAR) AS `{n}`", lit(v));
            } else {
                sql.push_str(&lit(v));
            }
        }
    }
    sql.push(')');
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_level_grants_reach_their_lookups_only() {
        use sprig_auth::authz::backend::{BackendState, BackendTarget};
        use sprig_auth::authz::{Action, ActionSet};
        let t = TenantId(9);
        let owners = vec![
            LookupOwner {
                db: DbId(1),
                table: "customers".into(),
                physical: "__sprig_lk_customers__uk_email".into(),
            },
            LookupOwner {
                db: DbId(1),
                table: "secrets".into(),
                physical: "__sprig_lk_secrets__uk_code".into(),
            },
        ];
        let mut places = std::collections::BTreeMap::new();
        for o in &owners {
            places.insert(
                (DbId(1), o.physical.clone()),
                vec!["t9_d1_g7_s1".to_owned(), "t9_d1_g7_s2".to_owned()],
            );
        }
        let sel = ActionSet::of(&[Action::Select]);
        let ins = ActionSet::of(&[Action::Insert]);
        let mut want = BackendState::new();
        // u1 may read customers; u2 may insert into it (one column);
        // u3 holds a database-wide grant only; u4 reads another
        // database's customers.
        want.entry("u1".into()).or_default().insert(
            BackendTarget::Table("t9_d1_g3_s1".into(), "Customers".into()),
            sel,
        );
        want.entry("u2".into()).or_default().insert(
            BackendTarget::Column("t9_d1_g3_s2".into(), "customers".into(), "email".into()),
            ins,
        );
        want.entry("u3".into())
            .or_default()
            .insert(BackendTarget::Db("t9\\_d1\\_%".into()), sel.union(ins));
        want.entry("u4".into()).or_default().insert(
            BackendTarget::Table("t9_d2_g3_s1".into(), "customers".into()),
            sel,
        );
        lookup_grants(&mut want, t, &owners, &places);
        let lk = |s: &str| BackendTarget::Table(s.into(), "__sprig_lk_customers__uk_email".into());
        assert_eq!(want["u1"].get(&lk("t9_d1_g7_s1")).copied(), Some(sel));
        assert_eq!(want["u1"].get(&lk("t9_d1_g7_s2")).copied(), Some(sel));
        let rw = ActionSet::of(&[
            Action::Select,
            Action::Insert,
            Action::Update,
            Action::Delete,
        ]);
        assert_eq!(want["u2"].get(&lk("t9_d1_g7_s1")).copied(), Some(rw));
        // Nothing on a table they have no grant on, nothing for the
        // database-wide account, nothing across databases.
        for u in ["u1", "u2"] {
            assert!(
                !want[u]
                    .keys()
                    .any(|k| matches!(k, BackendTarget::Table(_, n) if n.contains("secrets"))),
                "{u}"
            );
        }
        assert_eq!(want["u3"].len(), 1);
        assert_eq!(want["u4"].len(), 1);
        assert_eq!(schema_db(t, "t9_d12_g7_s1"), Some(DbId(12)));
        assert_eq!(schema_db(t, "t91_d1_g7_s1"), None);
        assert_eq!(schema_db(t, "t9_d1x"), None);
    }

    #[test]
    fn statements_parse() {
        assert_eq!(
            parse("SPRIG SHOW LOOKUP INDEXES"),
            Some(LookupStmt::Show {
                db: None,
                table: None
            })
        );
        assert_eq!(
            parse("show sprig lookup indexes from `shop`.customers;"),
            Some(LookupStmt::Show {
                db: Some("shop".into()),
                table: Some("customers".into())
            })
        );
        assert_eq!(
            parse("SPRIG REBUILD LOOKUP INDEX uk_email ON customers"),
            Some(LookupStmt::Rebuild {
                name: "uk_email".into(),
                db: None,
                table: "customers".into()
            })
        );
        assert_eq!(
            parse("SPRIG DROP LOOKUP INDEX `uk``x` ON d.t"),
            Some(LookupStmt::Drop {
                name: "uk`x".into(),
                db: Some("d".into()),
                table: "t".into()
            })
        );
        for other in [
            "SPRIG SHOW PINS",
            "SHOW INDEX FROM t",
            "SPRIG REBUILD LOOKUP INDEX k ON t extra",
            "SPRIG DROP LOOKUP INDEX k",
            "SELECT 'SPRIG SHOW LOOKUP INDEXES'",
        ] {
            assert_eq!(parse(other), None, "{other}");
        }
    }
}
