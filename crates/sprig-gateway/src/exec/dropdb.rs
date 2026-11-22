//! What goes with a dropped database besides its schemas: its sinks and
//! connections and its logic modules, so none stays `running` or `active`
//! in the catalog and no job keeps running for it; a member's
//! inherited instances in it, created again at their next use; and its
//! table groups' shards (sharded and reference groups, whose schemas may
//! be on other clusters than the database's default shards). Its streams
//! went first (in [`super::streams`]).
//!
//! **Table groups.** [`DbDrop::prepare`] reads, before the statement runs,
//! every group with members in the database and where its shards are (the
//! group's map); a group with a mover job before its cutover committed (a
//! split, merge, move or re-key in flight), or a range held by a cutover,
//! refuses the drop with 39071, so no shard is left behind half moved. [`DbDrop::after`] drops each of those shard schemas
//! on its cluster, removes the database's members from `group_members`,
//! and retires a group left without members (`state = 'retired'`, its name
//! freed with a `#<id>` suffix) and deletes its map. A failure there is
//! logged and answered as a warning with the drop's OK.
//!
//! [`DbDrop::before`] notes the database's id and sinks before the
//! statement runs; [`DbDrop::after`] runs once `DROP DATABASE` has
//! published its catalog version. A failure there is logged and the
//! client still gets the drop's OK: the database is gone either way, and
//! the jobs also end on their own checks (a trigger whose module or
//! database is gone).

use std::sync::Arc;

use mysql_async::prelude::Queryable;
use sprig_catalog::CatalogLocator;
use sprig_catalog::ddl::database_key;
use sprig_catalog::pool_wait::Bounded as _;
use sprig_catalog::version::VersionKind;
use sprig_common::ids::{ClusterId, DbId, GroupId, TenantId};
use sprig_common::names::SchemaName;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_sinks::store::SinksState;

use crate::tenant::TenantServices;
use crate::tenant::session::TenantSession;

/// A `DROP DATABASE` about to run (see the module docs).
pub(super) struct DbDrop {
    tenant: TenantId,
    db: DbId,
    name: String,
    lctn: bool,
    sinks: Option<SinksState>,
    /// The database's table groups (not its default group) and their
    /// shards, read by [`Self::prepare`].
    groups: Vec<(GroupId, Vec<(String, ClusterId)>)>,
}

/// What [`DbDrop::after`] did besides the drop itself.
pub(super) struct AfterDrop {
    /// The catalog version the sink changes published, if any.
    pub(super) version: Option<u64>,
    /// What couldn't be cleaned up, for the client's warnings.
    pub(super) warnings: Vec<String>,
}

fn catalog_err(e: impl std::error::Error + Send + Sync + 'static) -> SprigError {
    SprigError::new(SprigCode::CatalogUnavailable, "catalog unreachable").with_source(e)
}

fn missing_table(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1146)
}

impl DbDrop {
    /// The database `name` of the session's tenant, if it exists.
    pub(super) fn before(ts: &TenantSession, name: &str, lctn: bool) -> Option<Self> {
        let cat = ts.catalog()?;
        let db = cat.database(&database_key(name, lctn))?.db_id;
        Some(Self {
            tenant: cat.tenant,
            db,
            name: name.to_owned(),
            lctn,
            sinks: cat
                .ext
                .get::<SinksState>(sprig_sinks::store::MODULE)
                .filter(|s| s.migrated)
                .cloned(),
            groups: Vec::new(),
        })
    }

    /// Reads the database's table groups and their shards (see the module
    /// docs).
    ///
    /// # Errors
    /// 39071 when a mover job holds one of the groups; the catalog's and
    /// the map store's errors.
    pub(super) async fn prepare(&mut self, svc: &Arc<TenantServices>) -> Result<()> {
        let ep = svc.locator.writer(self.tenant).await?;
        let cat = sprig_common::names::quote(ep.store.schema());
        let mut c = ep.pool.get_conn_bounded().await.map_err(catalog_err)?;
        let groups: Vec<u32> = match c
            .exec(
                format!(
                    "SELECT DISTINCT m.group_id FROM {cat}.group_members m \
                     JOIN {cat}.table_groups g ON g.group_id = m.group_id \
                     WHERE m.db_id = ? AND g.kind <> 'default' AND g.state <> 'retired' \
                     ORDER BY m.group_id"
                ),
                (self.db.0,),
            )
            .await
        {
            Ok(v) => v,
            // A tenant that never had a table group has no such tables.
            Err(e) if missing_table(&e) => return Ok(()),
            Err(e) => return Err(catalog_err(e)),
        };
        drop(c);
        let maps = crate::tenant::lookups_sql::maps(svc, self.tenant);
        for g in groups {
            let group = GroupId(g);
            let map = match maps.load(self.tenant, group).await {
                Ok((m, _)) => m,
                Err(e) if e.code() == Some(SprigCode::ShardMapUnavailable) => {
                    self.groups.push((group, Vec::new()));
                    continue;
                }
                Err(e) => return Err(e),
            };
            if let Some(r) = map
                .ranges
                .iter()
                .find(|r| r.state == sprig_shard::shardmap::RangeState::Held)
            {
                return Err(self.busy(g, r.job_id.as_deref().unwrap_or("a cutover")));
            }
            let mut shards: Vec<(String, ClusterId)> = Vec::new();
            for r in &map.ranges {
                let schema = SchemaName::shard(self.tenant, self.db, group, r.shard).to_string();
                if !shards.iter().any(|(s, _)| s == &schema) {
                    shards.push((schema, r.cluster.clone()));
                }
            }
            self.groups.push((group, shards));
        }
        if !self.groups.is_empty() {
            self.no_running_job(&maps).await?;
        }
        Ok(())
    }

    /// 39071 for group `g`, held by `what`.
    fn busy(&self, g: u32, what: &str) -> SprigError {
        SprigError::new(
            SprigCode::CatalogConflict,
            format!(
                "database '{}' can't be dropped now: its table group {g} is being split, merged, \
                 moved or re-keyed ({what}); retry when the job ends",
                self.name
            ),
        )
    }

    /// Refuses (39071) while a mover job of one of the groups hasn't
    /// committed its cutover: dropping its source now would fail it, and
    /// it could create its targets after the drop. A committed job only
    /// retains and then drops its sources, which aren't in the map.
    async fn no_running_job(&self, maps: &sprig_shard::ShardMapStore) -> Result<()> {
        let store = maps.store();
        let prefix = store.tier().key("mover/jobs/")?;
        let page = store
            .list(
                &prefix,
                sprig_meta::ListOpts {
                    limit: 10_000,
                    ..sprig_meta::ListOpts::default()
                },
            )
            .await?;
        for kv in &page.kvs {
            let Ok(j) = serde_json::from_slice::<serde_json::Value>(&kv.value) else {
                continue;
            };
            let state = j["state"].as_str().unwrap_or_default();
            let settled = matches!(
                state,
                "post_cutover" | "retaining" | "cleaning" | "done" | "rolled_back"
            );
            let group = j["spec"]["routing"]["group"].as_u64();
            if j["tenant_id"].as_u64() == Some(self.tenant.0)
                && !settled
                && let Some(g) = group.and_then(|g| u32::try_from(g).ok())
                && self.groups.iter().any(|(x, _)| x.0 == g)
            {
                let id = j["job_id"].as_str().unwrap_or("?");
                return Err(self.busy(g, &format!("job {id}, {state}")));
            }
        }
        Ok(())
    }

    /// Drops the database's sinks, connections and logic modules after
    /// the drop published `version`. Returns the catalog version the sink
    /// changes published, if any.
    pub(super) async fn after(self, svc: &Arc<TenantServices>, version: u64) -> AfterDrop {
        let mut warnings = Vec::new();
        if let Err(e) = self.drop_groups(svc).await {
            tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                "DROP DATABASE: the database's table groups were not all dropped");
            warnings.push(format!(
                "the shards of database '{}' in its table groups were not all dropped ({}); \
                 the catalog reconciler reports what is left",
                self.name, e.message
            ));
        }
        let sinks = match self.drop_sinks(svc).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                    "DROP DATABASE: the database's sinks were not all dropped");
                None
            }
        };
        if let Err(e) = self.drop_logic(svc, version).await {
            tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                "DROP DATABASE: the database's logic modules were not all dropped");
        }
        // A member's inherited instances in the database.
        if let Err(e) = Box::pin(crate::inherited::lifecycle::database_dropped(
            svc,
            self.tenant,
            &self.name,
            self.lctn,
        ))
        .await
        {
            tracing::warn!(tenant = %self.tenant, db = %self.name, error = %e,
                "DROP DATABASE: the database's inherited instances were not all dropped");
        }
        AfterDrop {
            version: sinks,
            warnings,
        }
    }

    /// The table groups' part of the drop (see the module docs).
    async fn drop_groups(&self, svc: &Arc<TenantServices>) -> Result<()> {
        if self.groups.is_empty() {
            return Ok(());
        }
        // The shard schemas, on their clusters, one connection each.
        let mut dropped = Vec::new();
        let mut clusters: Vec<&ClusterId> = self
            .groups
            .iter()
            .flat_map(|(_, s)| s.iter().map(|(_, c)| c))
            .collect();
        clusters.sort_by_key(ToString::to_string);
        clusters.dedup();
        for cluster in clusters {
            let pool = svc.locator.ctl_pool(cluster).await?;
            let mut c = pool.get_conn_bounded().await.map_err(catalog_err)?;
            for (schema, _) in self
                .groups
                .iter()
                .flat_map(|(_, s)| s.iter())
                .filter(|(_, c)| c == cluster)
            {
                c.query_drop(format!(
                    "DROP DATABASE IF EXISTS {}",
                    sprig_common::names::quote(schema)
                ))
                .await
                .map_err(catalog_err)?;
                dropped.push(format!("{schema}@{cluster}"));
            }
        }
        // The catalog: the database's members, then groups left empty.
        let ep = svc.locator.writer(self.tenant).await?;
        let cat = sprig_common::names::quote(ep.store.schema());
        let mut c = ep.pool.get_conn_bounded().await.map_err(catalog_err)?;
        let maps = crate::tenant::lookups_sql::maps(svc, self.tenant);
        let mut retired = Vec::new();
        for (group, _) in &self.groups {
            c.exec_drop(
                format!("DELETE FROM {cat}.group_members WHERE db_id = ? AND group_id = ?"),
                (self.db.0, group.0),
            )
            .await
            .map_err(catalog_err)?;
            let left: Option<u64> = c
                .exec_first(
                    format!("SELECT COUNT(*) FROM {cat}.group_members WHERE group_id = ?"),
                    (group.0,),
                )
                .await
                .map_err(catalog_err)?;
            if left.unwrap_or(0) > 0 {
                continue;
            }
            c.exec_drop(
                format!(
                    "UPDATE {cat}.table_groups SET state = 'retired', \
                     name = CONCAT(LEFT(name, 50), '#', group_id), updated_at = UTC_TIMESTAMP(6), \
                     row_version = row_version + 1 WHERE group_id = ? AND state <> 'retired'"
                ),
                (group.0,),
            )
            .await
            .map_err(catalog_err)?;
            maps.delete(self.tenant, *group).await?;
            retired.push(group.0);
        }
        tracing::info!(tenant = %self.tenant, db = %self.name, schemas = ?dropped, retired_groups = ?retired,
            "DROP DATABASE: the database's table groups dropped");
        Ok(())
    }

    async fn drop_sinks(&self, svc: &TenantServices) -> Result<Option<u64>> {
        let Some(st) = &self.sinks else {
            return Ok(None);
        };
        let (sinks, connections) = sprig_sinks::dropdb::database_plan(st, self.db.0);
        if sinks.is_empty() && connections.is_empty() {
            return Ok(None);
        }
        // Tasks end when their index entry and catalog row are gone, as
        // after `DROP SINK`.
        let dc = svc.locator.dc_store();
        for s in &sinks {
            sprig_sinks::meta::index_delete(&dc, self.tenant.0, s.id.0).await?;
        }
        let names: Vec<String> = sinks
            .iter()
            .map(|s| s.name.clone())
            .chain(connections.iter().map(|c| format!("connection {}", c.name)))
            .collect();
        let changes = sprig_sinks::dropdb::database_changes(sinks, connections);
        let mut c = svc
            .catalog
            .change(self.tenant, VersionKind::Catalog)
            .await?;
        let mut applied = Ok(());
        for ch in &changes {
            applied = sprig_sinks::sql::apply(c.tx(), ch, None, self.tenant, 0).await;
            if applied.is_err() {
                break;
            }
        }
        if let Err(e) = applied {
            let _ = c.rollback().await;
            return Err(e);
        }
        let v = c.commit().await?;
        let store = sprig_sinks::store::SinksStore::new(Arc::clone(&svc.records), self.tenant);
        for ch in &changes {
            sprig_sinks::sql::after_commit(&store, ch).await?;
        }
        tracing::info!(tenant = %self.tenant, db = %self.name, dropped = ?names,
            "DROP DATABASE: the database's sinks and connections dropped");
        Ok(Some(v))
    }

    async fn drop_logic(&self, svc: &TenantServices, version: u64) -> Result<()> {
        let ep = svc.locator.writer(self.tenant).await?;
        let mut c = ep.pool.get_conn_bounded().await.map_err(|e| {
            sprig_common::SprigError::new(
                sprig_common::SprigCode::CatalogUnavailable,
                "catalog unreachable",
            )
            .with_source(e)
        })?;
        sprig_logic::dropdb::database_dropped(
            &mut c,
            ep.store.schema(),
            &svc.locator.dc_store(),
            self.tenant,
            self.db.0,
            version,
        )
        .await?;
        Ok(())
    }
}
