//! Lookup indexes as jobs: the tenant's lookup group, creating a lookup on
//! an owner's columns (with its back-fill), rebuilding, dropping, and
//! resuming a job a crash stopped. The gateway's DDL path (`ADD UNIQUE`,
//! `CREATE UNIQUE INDEX`, `DROP INDEX`, `SPRIG ... LOOKUP INDEX`) and the
//! controller's advisor and recovery all run these, so a lookup is built
//! the same way whoever asks for it.
//!
//! ```text
//! create(owner, columns):
//!   the tenant's lookup group (kind `lookup`, `__sprig_lookups`): found, or
//!     made with one shard per cluster of the owner's group
//!   the owner's column types and primary key from a shard's
//!     information_schema; a `lookup_indexes` row in `building`
//!   direct::build: tables, write_only (publish), back-fill, verify, public
//!     (publish); a real duplicate drops it and fails with 1062
//! rebuild(L): public → backfilling (routing stops; writers still claim),
//!   publish, back-fill and verify (stale routing hashes are repaired),
//!   orphan scan, public
//! drop(L): dropping, publish, tables, row, publish
//! resume(): every lookup in building, write_only, backfilling or
//!   dropping that no job touched for the lease (`lookup.build_lease_seconds`)
//!   is taken over (one taker wins): building is rolled back (no writer
//!   used it), write_only and backfilling are built on, dropping is
//!   finished; a lookup whose owner table is gone is dropped
//! drop_owned(owners), after DROP TABLE or DROP DATABASE: every lookup of
//!   those tables goes dropping (one publish), its tables and row go, publish
//! a build that fails on values repeating across shards tells the host
//!   (LookupHost::duplicates), which raises an approvals item
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use sprig_common::ids::{ClusterId, DbId, GroupId, ObjectId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};

use super::direct::{self, BuildReport, Duplicates, Pools};
use super::{
    LookupCatalog, LookupColumn, LookupId, LookupIndex, LookupOrigin, LookupPlacement,
    LookupRecord, LookupState, MapPlacement, propose_auto_lookup,
};
use crate::groups::db_err;
use crate::names::shard_schema;
use crate::shardmap::{ShardMap, ShardMapStore};

/// A job that hasn't touched its lookup for this long has stopped (its
/// process died); another may take it over. Builds touch it every chunk.
/// The default of `lookup.build_lease_seconds` ([`Admin::lease`]).
pub const BUILD_LEASE: Duration = Duration::from_secs(30);

/// The end of an approvals item's target that reports a failed build's
/// duplicates: `t<tenant>/o<owner>/lookup/<key>/duplicates`.
pub const DUPLICATES_TARGET_SUFFIX: &str = "/duplicates";

/// A unique lookup whose build failed because its values repeat across
/// shards: what the host tells people ([`LookupHost::duplicates`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateReport {
    /// The owner's database.
    pub db: DbId,
    /// The owner table.
    pub owner: ObjectId,
    /// Its name.
    pub table: String,
    /// The key's name.
    pub key: String,
    /// The key's columns.
    pub columns: Vec<String>,
    /// What the verification found.
    pub found: Duplicates,
}

impl DuplicateReport {
    /// The approvals item's target.
    #[must_use]
    pub fn target(&self, tenant: TenantId) -> String {
        format!(
            "t{tenant}/o{}/lookup/{}{DUPLICATES_TARGET_SUFFIX}",
            self.owner.0, self.key
        )
    }

    /// The statement that builds the lookup once the values are fixed.
    #[must_use]
    pub fn action_text(&self, db_name: &str) -> String {
        let q = |s: &str| format!("`{}`", s.replace('`', "``"));
        format!(
            "SPRIG REBUILD LOOKUP INDEX {} ON {}.{}",
            q(&self.key),
            q(db_name),
            q(&self.table)
        )
    }

    /// The `ALTER TABLE` that adds the key (and builds its lookup) again,
    /// for a failure of `ADD UNIQUE`, which dropped the shards' key.
    #[must_use]
    pub fn ddl_text(&self, db_name: &str) -> String {
        let q = |s: &str| format!("`{}`", s.replace('`', "``"));
        format!(
            "ALTER TABLE {}.{} ADD UNIQUE KEY {} ({})",
            q(db_name),
            q(&self.table),
            q(&self.key),
            self.columns
                .iter()
                .map(|c| q(c))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// The machine form of the action: what to build.
    #[must_use]
    pub fn machine(&self) -> serde_json::Value {
        serde_json::json!({
            "object": self.owner.0,
            "db": self.db.0,
            "key": self.key,
            "columns": self.columns,
            "duplicates": true,
        })
    }

    /// The evidence: how many values and rows repeat, and a sample.
    #[must_use]
    pub fn evidence(&self) -> serde_json::Value {
        serde_json::json!({
            "duplicate_values": self.found.values,
            "duplicate_rows": self.found.rows,
            "sample": self.found.sample,
        })
    }

    /// One line for people; `ddl` when the shards' key was dropped again
    /// (a failed `ADD UNIQUE`), so the statement must run again.
    #[must_use]
    pub fn summary(&self, db_name: &str, ddl: bool) -> String {
        format!(
            "unique key {} on {}.{} ({}) isn't unique across shards: {} values repeat in {} \
             rows, for example {}; fix them, then {}",
            self.key,
            db_name,
            self.table,
            self.columns.join(", "),
            self.found.values,
            self.found.rows,
            self.found
                .sample
                .iter()
                .map(|v| format!("'{v}'"))
                .collect::<Vec<_>>()
                .join(", "),
            if ddl {
                "run the ALTER TABLE again (it builds the global lookup index)"
            } else {
                "approve to build its global lookup index"
            }
        )
    }
}

/// The name of the tenant's lookup group in `table_groups`.
pub const LOOKUP_GROUP_NAME: &str = "__sprig_lookups";

/// What a lookup job needs from whoever runs it.
#[async_trait::async_trait]
pub trait LookupHost: Send + Sync {
    /// A connection to the tenant's catalog cluster with the catalog
    /// schema (`t<tenant>_sprig`) as its default database.
    ///
    /// # Errors
    /// The catalog is unreachable.
    async fn catalog(&self) -> Result<Conn>;

    /// Connection pools able to create tables, by cluster, for at least
    /// `clusters`.
    ///
    /// # Errors
    /// A cluster is unknown or unreachable.
    async fn pools(&self, clusters: &[ClusterId]) -> Result<Pools>;

    /// Raises the tenant's catalog version and returns once every gateway
    /// can have loaded it.
    ///
    /// # Errors
    /// The catalog's.
    async fn publish(&self) -> Result<()>;

    /// A unique lookup's build failed because its values repeat across
    /// shards: tell people (an approvals item). Errors are the host's to
    /// log; the default does nothing.
    async fn duplicates(&self, report: &DuplicateReport) {
        let _ = report;
    }
}

/// A table's shape as a lookup needs it, from one shard's
/// `information_schema`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OwnerShape {
    /// Columns by lowercase name: their full type (with the collation of
    /// strings) and key class; columns that can't be key parts are absent.
    pub columns: BTreeMap<String, LookupColumn>,
    /// The primary key's columns, lowercase, in order.
    pub primary: Vec<String>,
    /// Unique keys other than the primary key: name and lowercase columns
    /// (keys over column prefixes left out).
    pub unique: Vec<(String, Vec<String>)>,
    /// Every index name, lowercase.
    pub indexes: BTreeSet<String>,
}

/// Reads `schema.table`'s shape.
///
/// # Errors
/// MariaDB's errors; 1146 when the table doesn't exist.
pub async fn owner_shape(c: &mut Conn, schema: &str, table: &str) -> Result<OwnerShape> {
    let cols: Vec<(String, String, String, Option<String>)> = c
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLLATION_NAME \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (schema, table),
        )
        .await
        .map_err(db_err)?;
    if cols.is_empty() {
        return Err(SprigError::standard(
            1146,
            *b"42S02",
            format!("Table '{schema}.{table}' doesn't exist"),
        ));
    }
    let mut out = OwnerShape::default();
    for (name, data_type, column_type, collation) in cols {
        let Some(class) = crate::pins::column_class(&data_type, collation.as_deref()) else {
            continue;
        };
        if !matches!(
            class,
            crate::key::KeyClass::Numeric
                | crate::key::KeyClass::String { .. }
                | crate::key::KeyClass::Binary
        ) {
            continue;
        }
        let sql_type = match &collation {
            Some(collation_name) if matches!(class, crate::key::KeyClass::String { .. }) => {
                format!("{column_type} COLLATE {collation_name}")
            }
            _ => column_type,
        };
        let lower = name.to_ascii_lowercase();
        out.columns.insert(
            lower.clone(),
            LookupColumn {
                name: lower,
                sql_type,
                class,
            },
        );
    }
    let stats: Vec<(String, i64, String, Option<i64>)> = c
        .exec(
            "SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SUB_PART \
             FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (schema, table),
        )
        .await
        .map_err(db_err)?;
    let mut keys: BTreeMap<String, (bool, bool, Vec<String>)> = BTreeMap::new();
    for (index, non_unique, column, sub_part) in stats {
        out.indexes.insert(index.to_ascii_lowercase());
        let e = keys
            .entry(index)
            .or_insert((non_unique == 0, false, Vec::new()));
        e.1 |= sub_part.is_some();
        e.2.push(column.to_ascii_lowercase());
    }
    for (name, (unique, prefix, cols)) in keys {
        if name == "PRIMARY" {
            out.primary = cols;
        } else if unique && !prefix {
            out.unique.push((name, cols));
        }
    }
    Ok(out)
}

/// What a new lookup is on.
#[derive(Debug, Clone)]
pub struct NewLookup {
    /// The owner's database.
    pub db: DbId,
    /// The owner table.
    pub owner: ObjectId,
    /// Its logical name (as in its shard schemas).
    pub owner_table: String,
    /// The owner group's map.
    pub owner_map: Arc<ShardMap>,
    /// The value columns.
    pub columns: Vec<String>,
    /// The key's name for a unique lookup; `None` for an automatic one
    /// (`sprig_auto_<8 hex>`, non-unique).
    pub key: Option<String>,
    /// Why it exists (`unique_key` or `auto`).
    pub origin: LookupOrigin,
}

fn exists_already(name: &str) -> SprigError {
    SprigError::standard(1061, *b"42000", format!("Duplicate key name '{name}'"))
}

fn no_lookup(name: &str, table: &str) -> SprigError {
    SprigError::standard(
        1091,
        *b"42000",
        format!("Can't DROP INDEX `{name}`; table {table} has no lookup index of that name"),
    )
}

/// Lookup jobs for one tenant.
pub struct Admin<'a> {
    /// The tenant.
    pub tenant: TenantId,
    /// Its home DC's shard maps.
    pub maps: &'a ShardMapStore,
    /// The catalog, pools and publishing.
    pub host: &'a dyn LookupHost,
    /// Back-fill chunk rows (`lookup.backfill_chunk_rows`).
    pub chunk: usize,
    /// A job untouched this long has stopped (`lookup.build_lease_seconds`).
    pub lease: Duration,
}

impl std::fmt::Debug for Admin<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admin")
            .field("tenant", &self.tenant)
            .field("chunk", &self.chunk)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

fn clusters_of(maps: &[&ShardMap]) -> Vec<ClusterId> {
    let mut v: Vec<ClusterId> = maps
        .iter()
        .flat_map(|m| m.ranges.iter().map(|r| r.cluster.clone()))
        .collect();
    v.sort();
    v.dedup();
    v
}

impl Admin<'_> {
    /// The tenant's lookup group's map: found, or made with one shard per
    /// cluster `owner_map` uses (at least one), each schema created when a
    /// lookup's table is.
    ///
    /// # Errors
    /// Catalog and etcd errors.
    pub async fn lookup_group(
        &self,
        cat: &mut Conn,
        owner_map: &ShardMap,
    ) -> Result<Arc<ShardMap>> {
        for _ in 0..5 {
            let found: Option<u32> = cat
                .query_first(
                    "SELECT group_id FROM table_groups WHERE kind = 'lookup' \
                     AND state = 'active' ORDER BY group_id LIMIT 1",
                )
                .await
                .map_err(db_err)?;
            let g = match found {
                Some(g) => GroupId(g),
                None => match self.new_group(cat).await {
                    Ok(g) => g,
                    // Another job made it meanwhile (`uk_name`): read it.
                    Err(e) if super::errno(&e) == Some(1062) => continue,
                    Err(e) => return Err(e),
                },
            };
            if let Ok((m, _)) = self.maps.load(self.tenant, g).await {
                return Ok(Arc::new(m));
            }
            let clusters = clusters_of(&[owner_map]);
            let n = u32::try_from(clusters.len()).unwrap_or(1).max(1);
            let m = ShardMap::presplit(self.tenant, g, 0, n, &clusters)?;
            // A concurrent creator's map wins; read it back.
            let _ = self.maps.create(&m).await;
            let (m, _) = self.maps.load(self.tenant, g).await?;
            return Ok(Arc::new(m));
        }
        Err(SprigError::internal("the lookup group couldn't be made"))
    }

    /// A new `lookup` group row, its id from `sprig.next_group_id` (kept
    /// past every group in use, as the advisor's apply keeps it).
    async fn new_group(&self, cat: &mut Conn) -> Result<GroupId> {
        cat.query_drop(
            "INSERT INTO settings (scope_kind, scope_id, setting_key, value, bounds, set_by, \
             set_at, version) VALUES ('tenant', 0, 'sprig.next_group_id', '1', NULL, 'system', \
             UTC_TIMESTAMP(6), 0) ON DUPLICATE KEY UPDATE setting_key = setting_key",
        )
        .await
        .map_err(db_err)?;
        let floor: Option<u64> = cat
            .query_first(
                "SELECT GREATEST(COALESCE((SELECT MAX(default_group_id) FROM `databases`), 0), \
                 COALESCE((SELECT MAX(group_id) FROM table_groups), 0)) + 1",
            )
            .await
            .map_err(db_err)?;
        cat.exec_drop(
            "UPDATE settings SET value = LAST_INSERT_ID(GREATEST(CAST(value AS UNSIGNED), ?) + 1), \
             set_at = UTC_TIMESTAMP(6) WHERE scope_kind = 'tenant' AND scope_id = 0 \
             AND setting_key = 'sprig.next_group_id'",
            (floor.unwrap_or(1),),
        )
        .await
        .map_err(db_err)?;
        let next: Option<u64> = cat
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .map_err(db_err)?;
        let id = GroupId(
            u32::try_from(next.unwrap_or(2).saturating_sub(1))
                .map_err(|_| SprigError::internal("group ids exhausted"))?,
        );
        cat.exec_drop(
            "INSERT INTO table_groups (group_id, kind, name, key_spec, key_spec_digest, state, \
             origin, created_at, updated_at) VALUES (?, 'lookup', ?, NULL, NULL, 'active', \
             'structural', UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))",
            (id.0, LOOKUP_GROUP_NAME),
        )
        .await
        .map_err(db_err)?;
        Ok(id)
    }

    async fn shape_of(
        &self,
        pools: &Pools,
        db: DbId,
        owner_map: &ShardMap,
        table: &str,
    ) -> Result<OwnerShape> {
        let r = owner_map
            .ranges
            .first()
            .ok_or_else(|| SprigError::internal("an owner group without shards"))?;
        let schema = shard_schema(self.tenant, db, owner_map.group, r.shard);
        let pool = pools.get(&r.cluster).ok_or_else(|| {
            SprigError::new(
                SprigCode::BackendUnavailable,
                format!("no pool for cluster {}", r.cluster),
            )
        })?;
        let mut c = pool.get_conn().await.map_err(db_err)?;
        owner_shape(&mut c, &schema, table).await
    }

    /// Creates and builds a lookup on `n`'s columns. A unique lookup is
    /// named after its key; an automatic one comes from
    /// [`propose_auto_lookup`].
    ///
    /// # Errors
    /// 1061 when the table has a lookup of that name already; 1072 for a
    /// column the table lacks; 39420 for a column a lookup can't hold or a
    /// table without a primary key; 1062 when the values already repeat
    /// across shards (the lookup is dropped); MariaDB's and etcd's errors.
    pub async fn create(&self, n: &NewLookup) -> Result<(LookupIndex, BuildReport)> {
        let mut cat = self.host.catalog().await?;
        let lmap = self.lookup_group(&mut cat, &n.owner_map).await?;
        let pools = self
            .host
            .pools(&clusters_of(&[&n.owner_map, &lmap]))
            .await?;
        let shape = self
            .shape_of(&pools, n.db, &n.owner_map, &n.owner_table)
            .await?;
        let column = |c: &str| -> Result<LookupColumn> {
            let lc = c.to_ascii_lowercase();
            shape.columns.get(&lc).cloned().ok_or_else(|| {
                SprigError::new(
                    SprigCode::StatementNotDistributable,
                    format!(
                        "statement not distributable (lookup): column {c} of {} can't be a \
                         lookup index's value (or doesn't exist)",
                        n.owner_table
                    ),
                )
            })
        };
        let columns = n
            .columns
            .iter()
            .map(|c| column(c))
            .collect::<Result<Vec<_>>>()?;
        if shape.primary.is_empty() {
            return Err(SprigError::new(
                SprigCode::StatementNotDistributable,
                format!(
                    "statement not distributable (lookup): {} has no primary key a lookup \
                     index could name its rows by",
                    n.owner_table
                ),
            ));
        }
        let owner_pk = shape
            .primary
            .iter()
            .map(|c| column(c))
            .collect::<Result<Vec<_>>>()?;
        let mut tries = 0;
        let mut l = loop {
            let id = LookupCatalog.next_id(&mut cat).await?;
            let mut l = propose_auto_lookup(
                id,
                n.owner,
                &n.owner_table,
                columns.clone(),
                owner_pk.clone(),
                lmap.group,
                None,
            );
            if let Some(k) = &n.key {
                l = l.bound_to_key(k);
                l.origin = n.origin;
            }
            let taken: Option<u64> = cat
                .exec_first(
                    "SELECT lookup_id FROM lookup_indexes WHERE object_id = ? AND index_name = ?",
                    (n.owner.0, &l.index_name),
                )
                .await
                .map_err(db_err)?;
            if taken.is_some() {
                return Err(exists_already(&l.index_name));
            }
            match LookupCatalog.insert(&mut cat, &l).await {
                Ok(()) => break l,
                Err(e) if super::errno(&e) == Some(1062) && tries < 5 => tries += 1,
                Err(e) => return Err(e),
            }
        };
        let placement = MapPlacement {
            tenant: self.tenant,
            db: n.db,
            lookup_map: lmap,
            owner_map: Arc::clone(&n.owner_map),
        };
        let host = self.host;
        let built = direct::build(&pools, &mut cat, &placement, &mut l, self.chunk, || {
            host.publish()
        })
        .await;
        match built {
            Ok(report) => Ok((l, report)),
            Err(e) => {
                self.report_duplicates(n.db, &l, &e).await;
                Err(e)
            }
        }
    }

    /// Tells the host when `e` is a build's failure on values that repeat
    /// across shards.
    async fn report_duplicates(&self, db: DbId, l: &LookupIndex, e: &SprigError) {
        let Some(found) = Duplicates::of(e) else {
            return;
        };
        tracing::warn!(
            tenant = %self.tenant, table = %l.owner_table, lookup = %l.index_name,
            values = found.values, rows = found.rows, "lookup index build: values repeat across shards"
        );
        self.host
            .duplicates(&DuplicateReport {
                db,
                owner: l.owner,
                table: l.owner_table.clone(),
                key: l.index_name.clone(),
                columns: l.columns.iter().map(|c| c.name.clone()).collect(),
                found: found.clone(),
            })
            .await;
    }

    /// The lookup a catalog row describes, with its placement: the owner's
    /// name, database and group from the catalog, the column types from a
    /// shard.
    ///
    /// # Errors
    /// Catalog, etcd and MariaDB errors.
    pub async fn load(
        &self,
        cat: &mut Conn,
        rec: &LookupRecord,
    ) -> Result<(LookupIndex, MapPlacement, Pools)> {
        let owner: Option<(String, u32, u32)> = cat
            .exec_first(
                "SELECT o.name, o.db_id, m.group_id FROM objects o \
                 JOIN group_members m ON m.object_id = o.object_id WHERE o.object_id = ?",
                (rec.owner.0,),
            )
            .await
            .map_err(db_err)?;
        let Some((table, db, group)) = owner else {
            return Err(SprigError::internal(format!(
                "lookup index {} names object {}, which is no group member",
                rec.index_name, rec.owner.0
            )));
        };
        let (owner_map, _) = self.maps.load(self.tenant, GroupId(group)).await?;
        let (lookup_map, _) = self.maps.load(self.tenant, rec.group).await?;
        let pools = self
            .host
            .pools(&clusters_of(&[&owner_map, &lookup_map]))
            .await?;
        let shape = self.shape_of(&pools, DbId(db), &owner_map, &table).await?;
        let col = |c: &str| {
            shape
                .columns
                .get(&c.to_ascii_lowercase())
                .cloned()
                .ok_or_else(|| SprigError::internal(format!("{table}.{c}: no such key column")))
        };
        let l = LookupIndex {
            id: rec.id,
            owner: rec.owner,
            owner_table: table.clone(),
            index_name: rec.index_name.clone(),
            columns: rec
                .columns
                .iter()
                .map(|c| col(c))
                .collect::<Result<Vec<_>>>()?,
            owner_pk: shape
                .primary
                .iter()
                .map(|c| col(c))
                .collect::<Result<Vec<_>>>()?,
            unique: rec.unique,
            origin: rec.origin,
            group: rec.group,
            state: rec.state,
            auto_index_id: rec.auto_index_id,
        };
        let placement = MapPlacement {
            tenant: self.tenant,
            db: DbId(db),
            lookup_map: Arc::new(lookup_map),
            owner_map: Arc::new(owner_map),
        };
        Ok((l, placement, pools))
    }

    /// The catalog row of `owner`'s lookup `name` (case-insensitive).
    ///
    /// # Errors
    /// The catalog's.
    pub async fn find(
        &self,
        cat: &mut Conn,
        owner: ObjectId,
        name: &str,
    ) -> Result<Option<LookupRecord>> {
        Ok(LookupCatalog
            .rows(cat)
            .await?
            .into_iter()
            .find(|r| r.owner == owner && r.index_name.eq_ignore_ascii_case(name)))
    }

    /// Rebuilds a lookup: routing stops, every owner row is checked and
    /// repaired (missing rows, old routing hashes), orphans go, then it
    /// routes again.
    ///
    /// # Errors
    /// 1091 without such a lookup; 1062 if the values repeat across shards
    /// now (the lookup is dropped); catalog, etcd and MariaDB errors.
    pub async fn rebuild(&self, owner: ObjectId, table: &str, name: &str) -> Result<BuildReport> {
        let mut cat = self.host.catalog().await?;
        let rec = self
            .find(&mut cat, owner, name)
            .await?
            .ok_or_else(|| no_lookup(name, table))?;
        let (mut l, placement, pools) = self.load(&mut cat, &rec).await?;
        if l.state == LookupState::Public {
            if !LookupCatalog
                .set_state(
                    &mut cat,
                    l.id,
                    LookupState::Public,
                    LookupState::Backfilling,
                )
                .await?
            {
                return Err(SprigError::new(
                    SprigCode::LookupUnavailable,
                    format!("lookup index {name} changed state meanwhile"),
                ));
            }
            l.state = LookupState::Backfilling;
            self.host.publish().await?;
        } else if LookupCatalog
            .take_over(&mut cat, l.id, self.lease.as_secs())
            .await?
        {
            // A stopped build: carry on from its state.
        } else {
            return Err(SprigError::new(
                SprigCode::LookupUnavailable,
                format!(
                    "lookup index {name} is {} and a job is working on it",
                    l.state.as_str()
                ),
            ));
        }
        if l.state == LookupState::Dropping {
            direct::drop_with(&pools, &mut cat, &placement, &mut l, &mut || {
                self.host.publish()
            })
            .await?;
            return Err(no_lookup(name, table));
        }
        let host = self.host;
        let built = direct::build(&pools, &mut cat, &placement, &mut l, self.chunk, || {
            host.publish()
        })
        .await;
        let report = match built {
            Ok(r) => r,
            Err(e) => {
                self.report_duplicates(placement.db, &l, &e).await;
                return Err(e);
            }
        };
        let removed = direct::orphan_scan(&pools, &placement, &l, 5).await?;
        tracing::info!(
            tenant = %self.tenant, lookup = %l.index_name, rows = report.rows,
            repaired = report.repaired, orphans = removed, "lookup index rebuilt"
        );
        Ok(report)
    }

    /// Drops `owner`'s lookup `name`; `Ok(false)` when it has none.
    ///
    /// # Errors
    /// Catalog, etcd and MariaDB errors.
    pub async fn drop(&self, owner: ObjectId, name: &str) -> Result<bool> {
        let mut cat = self.host.catalog().await?;
        let Some(rec) = self.find(&mut cat, owner, name).await? else {
            return Ok(false);
        };
        self.drop_record(&mut cat, &rec).await?;
        Ok(true)
    }

    async fn drop_record(&self, cat: &mut Conn, rec: &LookupRecord) -> Result<()> {
        cat.exec_drop(
            "UPDATE lookup_indexes SET state = 'dropping', updated_at = UTC_TIMESTAMP(6) \
             WHERE lookup_id = ?",
            (rec.id.0,),
        )
        .await
        .map_err(db_err)?;
        self.host.publish().await?;
        self.drop_tables(rec).await?;
        cat.exec_drop(
            "DELETE FROM lookup_indexes WHERE lookup_id = ?",
            (rec.id.0,),
        )
        .await
        .map_err(db_err)?;
        self.host.publish().await
    }

    /// Drops every lookup of the tables in `owners` (after `DROP TABLE` or
    /// `DROP DATABASE` removed them): all go `dropping` in one change and
    /// one publish, then their tables and rows go, then one more publish.
    /// A crash in between leaves them `dropping` (or public with their
    /// owner gone), which [`Admin::resume`] finishes. Returns the dropped
    /// lookups' names.
    ///
    /// # Errors
    /// Catalog, etcd and MariaDB errors.
    pub async fn drop_owned(&self, owners: &[ObjectId]) -> Result<Vec<String>> {
        if owners.is_empty() {
            return Ok(Vec::new());
        }
        let mut cat = self.host.catalog().await?;
        if !has_lookup_table(&mut cat).await? {
            return Ok(Vec::new());
        }
        let recs: Vec<LookupRecord> = LookupCatalog
            .rows(&mut cat)
            .await?
            .into_iter()
            .filter(|r| owners.contains(&r.owner))
            .collect();
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = recs
            .iter()
            .map(|r| r.id.0.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        cat.query_drop(format!(
            "UPDATE lookup_indexes SET state = 'dropping', updated_at = UTC_TIMESTAMP(6) \
             WHERE lookup_id IN ({ids})"
        ))
        .await
        .map_err(db_err)?;
        self.host.publish().await?;
        for rec in &recs {
            self.drop_tables(rec).await?;
            cat.exec_drop(
                "DELETE FROM lookup_indexes WHERE lookup_id = ?",
                (rec.id.0,),
            )
            .await
            .map_err(db_err)?;
        }
        self.host.publish().await?;
        Ok(recs.into_iter().map(|r| r.index_name).collect())
    }

    /// `rec`'s table in every schema of the lookup group that holds it.
    async fn drop_tables(&self, rec: &LookupRecord) -> Result<()> {
        let (lookup_map, _) = self.maps.load(self.tenant, rec.group).await?;
        let pools = self.host.pools(&clusters_of(&[&lookup_map])).await?;
        // The placement's owner map is unused by a drop.
        let lookup_map = Arc::new(lookup_map);
        let placement = MapPlacement {
            tenant: self.tenant,
            db: DbId(0),
            lookup_map: Arc::clone(&lookup_map),
            owner_map: Arc::clone(&lookup_map),
        };
        // Every database's schemas of the lookup group hold the table under
        // its name: drop it in each that exists.
        let prefix = format!("t{}\\_d%\\_g{}\\_s%", self.tenant.0, rec.group.0);
        for t in placement.lookup_shards() {
            let pool = pools.get(&t.cluster).ok_or_else(|| {
                SprigError::new(
                    SprigCode::BackendUnavailable,
                    format!("no pool for cluster {}", t.cluster),
                )
            })?;
            let mut c = pool.get_conn().await.map_err(db_err)?;
            let schemas: Vec<String> = c
                .exec(
                    "SELECT TABLE_SCHEMA FROM information_schema.TABLES \
                     WHERE TABLE_SCHEMA LIKE ? AND TABLE_NAME = ?",
                    (&prefix, &rec.physical_name),
                )
                .await
                .map_err(db_err)?;
            for s in schemas {
                c.query_drop(format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    sprig_common::names::quote(&s),
                    sprig_common::names::quote(&rec.physical_name)
                ))
                .await
                .map_err(db_err)?;
            }
        }
        Ok(())
    }

    /// Takes over every lookup job that stopped (no touch for
    /// [`BUILD_LEASE`]) and finishes it: a `building` lookup is rolled back
    /// (no writer used it yet), `write_only` and `backfilling` are built
    /// on, `dropping` is finished. Returns what it did, per lookup.
    ///
    /// # Errors
    /// The catalog's; a failing job is reported in the result instead.
    pub async fn resume(&self) -> Result<Vec<(LookupId, String)>> {
        let mut cat = self.host.catalog().await?;
        if !has_lookup_table(&mut cat).await? {
            return Ok(Vec::new());
        }
        let mut done = Vec::new();
        let rows = LookupCatalog.rows(&mut cat).await?;
        let live = live_owners(&mut cat, &rows).await?;
        let lease = self.lease.as_secs();
        for rec in rows {
            // An owner `DROP TABLE` (or `DROP DATABASE`) removed while its
            // gateway died before the lookup went with it.
            let gone = !live.contains(&rec.owner);
            if (rec.state == LookupState::Public && !gone) || rec.idle_secs < lease {
                continue;
            }
            if !LookupCatalog.take_over(&mut cat, rec.id, lease).await? {
                continue;
            }
            tracing::info!(
                tenant = %self.tenant, lookup = %rec.index_name, state = rec.state.as_str(),
                idle_secs = rec.idle_secs, owner_gone = gone, "taking over a stopped lookup job"
            );
            let outcome = match rec.state {
                _ if gone => self
                    .drop_record(&mut cat, &rec)
                    .await
                    .map(|()| "dropped (its table is gone)".to_owned()),
                LookupState::Building | LookupState::Dropping => {
                    self.drop_record(&mut cat, &rec).await.map(|()| {
                        if rec.state == LookupState::Building {
                            "rolled back (stopped while building)".to_owned()
                        } else {
                            "dropped (a stopped drop finished)".to_owned()
                        }
                    })
                }
                LookupState::WriteOnly | LookupState::Backfilling => {
                    let r = async {
                        let (mut l, placement, pools) = self.load(&mut cat, &rec).await?;
                        let host = self.host;
                        let built =
                            direct::build(&pools, &mut cat, &placement, &mut l, self.chunk, || {
                                host.publish()
                            })
                            .await;
                        if let Err(e) = &built {
                            self.report_duplicates(placement.db, &l, e).await;
                        }
                        built
                    }
                    .await;
                    r.map(|rep| {
                        format!(
                            "built on: {} rows, {} inserted, {} repaired; public",
                            rep.rows, rep.inserted, rep.repaired
                        )
                    })
                }
                LookupState::Public => continue,
            };
            let text = match outcome {
                Ok(t) => t,
                Err(e) => format!("failed: {e}"),
            };
            tracing::info!(
                tenant = %self.tenant, lookup = %rec.index_name, outcome = %text,
                "stopped lookup job taken over"
            );
            done.push((rec.id, text));
        }
        Ok(done)
    }
}

/// Whether the catalog (the connection's database) has `lookup_indexes`.
async fn has_lookup_table(cat: &mut Conn) -> Result<bool> {
    let has: Option<u64> = cat
        .query_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'lookup_indexes'",
        )
        .await
        .map_err(db_err)?;
    Ok(has.unwrap_or(0) > 0)
}

/// The owners of `rows` the catalog still has (an `objects` row that
/// isn't `dropped`).
async fn live_owners(cat: &mut Conn, rows: &[LookupRecord]) -> Result<BTreeSet<ObjectId>> {
    let ids: BTreeSet<u64> = rows.iter().map(|r| r.owner.0).collect();
    if ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let list = ids
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let live: Vec<u64> = cat
        .query(format!(
            "SELECT object_id FROM objects WHERE object_id IN ({list}) AND state <> 'dropped'"
        ))
        .await
        .map_err(db_err)?;
    Ok(live.into_iter().map(ObjectId).collect())
}

/// The lookups of each owner, by owner (for statements listing them).
#[must_use]
pub fn by_owner(rows: &[LookupRecord]) -> HashMap<ObjectId, Vec<&LookupRecord>> {
    let mut m: HashMap<ObjectId, Vec<&LookupRecord>> = HashMap::new();
    for r in rows {
        m.entry(r.owner).or_default().push(r);
    }
    m
}

/// The unique keys of `shape` (of one column or several) that don't hold
/// `key` (the distribution key): what a unique lookup can make unique
/// across shards. Every column must be one a lookup can hold.
#[must_use]
pub fn lookup_keys(shape: &OwnerShape, key: &[String]) -> Vec<(String, Vec<String>)> {
    shape
        .unique
        .iter()
        .filter(|(_, cols)| !cols.iter().any(|c| key.contains(c)))
        .filter(|(_, cols)| !cols.is_empty() && cols.iter().all(|c| shape.columns.contains_key(c)))
        .map(|(name, cols)| (name.clone(), cols.clone()))
        .collect()
}
