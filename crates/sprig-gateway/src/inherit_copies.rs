//! Materialized copies of inherited tables on engine databases, kept by the
//! gateway that hosts the tenants' engine (what the copy worker's
//! maintainer does on MariaDB databases, [`sprig_inherit::copy`]).
//!
//! ```text
//! pass(family = tree root, db, table; keys K or all), after the orphan pass:
//!   the family's copies: each materialized edge of db.table (a share to a
//!   space inside a tenant, or the edge from the parent tenant), parents
//!   first (sprig_inherit::copy::load_family)
//!   states: rows of each child tenant's catalog records (m/inherit/copies);
//!   a state whose edge is gone is removed with its copy rows, and the copy
//!   table once no copy of the object is left
//!   each edge, in order:
//!     not serving at the object's and share's current versions → seed:
//!       the copy table __sprig_ih_<object> in the child's database (made,
//!       or made again when the child's columns changed), state seeding,
//!       the source's whole view read (copy::source_select), upserted live,
//!       the keys the copy held that the source doesn't tombstoned, state
//!       serving; the edges below it then refresh in full
//!     serving → refresh: the source's view of K (or all) upserted live, the
//!       keys of K (or held) it no longer has tombstoned
//! ```
//!
//! Writes go through the engine in process as the system
//! ([`crate::exec::inherit_tree::ctl_query`]), each upsert guarded by its
//! stamp (`_sprig_src_v` = process epoch << 32 | unix seconds): a row
//! written by a later process (or later in this one) is never overwritten
//! by an older write. Reads use a copy only while its state is serving at
//! the current versions, so a copy being seeded is never read half-filled.
//! Copies have no change streams of their own on engine databases.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sprig_catalog::ddl::{DdlStatement, ObjectName, database_key};
use sprig_catalog::store::ObjectKind;
use sprig_catalog::{Freshness, TenantCatalog};
use sprig_common::ids::{ObjectId, TenantId};
use sprig_common::{Result, SprigError};
use sprig_inherit::RawRows;
use sprig_inherit::copy::{
    CopyEdge, CopyEnv, CopyKey, CopyStates, Family, KeyLit, copy_ddl, copy_table, key_text,
    load_family, source_select, tombstone_sql, upsert_sql,
};
use sprig_inherit::view::q;

use crate::exec::inherit_tree::{
    CopyRow, copy_infos, ctl_query, engine_copy_states, engine_ddl, set_engine_copy_state,
};
use crate::tenant::TenantServices;

/// Rows per upsert statement.
const CHUNK: usize = 500;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    /// Copies seeded.
    pub seeds: u64,
    /// Rows written live.
    pub upserts: u64,
    /// Tombstones written.
    pub tombstones: u64,
}

/// The family's tenants and catalogs for [`load_family`]: the tenants
/// holding the table, read from the cache now.
struct Env {
    svc: Arc<TenantServices>,
    tenants: Vec<TenantId>,
}

#[async_trait::async_trait]
impl CopyEnv for Env {
    async fn family(&self, _root: TenantId) -> Result<Vec<TenantId>> {
        Ok(self.tenants.clone())
    }

    async fn catalog(&self, tenant: TenantId) -> Result<Arc<TenantCatalog>> {
        self.svc.cache.get(tenant, Freshness::Latest).await
    }

    async fn conn(&self, _tenant: TenantId) -> Result<mysql_async::Conn> {
        Err(SprigError::internal(
            "copies on engine databases have no MariaDB connection",
        ))
    }

    fn now_ms(&self) -> u64 {
        now_ms()
    }
}

/// The copy pass of one family (module docs). `tenants`: the tenants
/// holding the table, parents first; `keys`: the keys whose rows changed
/// somewhere in the family (`None`: all of them); `epoch`: this process's
/// part of the guard stamps.
///
/// # Errors
/// The catalog's, the records' and the engine's; the pass runs again on
/// the next change (or in full after a failure).
#[allow(clippy::too_many_arguments)] // the family's parts, as the caller holds them
pub(crate) async fn pass(
    svc: &Arc<TenantServices>,
    root: TenantId,
    tenants: Vec<TenantId>,
    db: &str,
    table: &str,
    lctn: bool,
    keys: Option<&[KeyLit]>,
    epoch: u64,
) -> Result<Counts> {
    let env = Env {
        svc: Arc::clone(svc),
        tenants,
    };
    let mut family = load_family(&env, root).await?;
    family
        .edges
        .retain(|e| e.db_name.eq_ignore_ascii_case(db) && e.table.eq_ignore_ascii_case(table));
    let mut p = Pass {
        svc: Arc::clone(svc),
        lctn,
        epoch,
        copies: CopyStates::default(),
        rows: BTreeMap::new(),
        counts: Counts::default(),
    };
    p.load_states(&family, db, table).await?;
    if family.edges.is_empty() {
        return Ok(p.counts);
    }
    // Tenants whose view a seed changed: the copies made from it refresh
    // in full.
    let mut reseeded: HashSet<TenantId> = HashSet::new();
    let edges = family.edges.clone();
    for e in &edges {
        let below_seed = reseeded
            .iter()
            .any(|t| family.at_or_above(*t, e.source_tenant));
        if p.copies
            .ready(e.key, e.object_version, e.share_version)
            .is_none()
        {
            p.seed(&mut family, e).await?;
            reseeded.insert(e.key.tenant);
        } else if below_seed {
            p.refresh(&family, e, None).await?;
        } else {
            p.refresh(&family, e, keys).await?;
        }
    }
    Ok(p.counts)
}

/// One pass's state.
struct Pass {
    svc: Arc<TenantServices>,
    lctn: bool,
    epoch: u64,
    /// The copies' states as the read path sees them.
    copies: CopyStates,
    /// The state rows, as stored.
    rows: BTreeMap<CopyKey, CopyRow>,
    counts: Counts,
}

impl Pass {
    fn stamp(&self) -> u64 {
        (self.epoch << 32) | ((now_ms() / 1000) & 0xFFFF_FFFF)
    }

    /// Reads the states of the family's copies of `db.table`, and removes
    /// those whose edge is gone (with their rows, and the table once no
    /// copy of its object is left).
    async fn load_states(&mut self, family: &Family, db: &str, table: &str) -> Result<()> {
        let live: BTreeSet<CopyKey> = family.edges.iter().map(|e| e.key).collect();
        for t in &family.tenants {
            let Some(cat) = family.cats.get(t) else {
                continue;
            };
            let Some(st) = sprig_inherit::plan::state_of(cat) else {
                continue;
            };
            let Some(d) = cat.database(&database_key(db, self.lctn)) else {
                continue;
            };
            let Some(o) = st.object_named(d.db_id, table, self.lctn) else {
                continue;
            };
            let rows = engine_copy_states(&self.svc, *t).await?;
            let mut kept = 0;
            let mut gone = Vec::new();
            for (k, row) in rows {
                if k.object != o.object_id {
                    continue;
                }
                if live.contains(&k) {
                    kept += 1;
                    self.rows.insert(k, row);
                } else {
                    gone.push(k);
                }
            }
            if gone.is_empty() {
                continue;
            }
            let Ok(schema) = sprig_inherit::plan::physical_schema(cat, &d.name, self.lctn) else {
                continue;
            };
            let ctable = format!("{}.{}", q(&schema), q(&copy_table(o.object_id)));
            for k in &gone {
                let _ = ctl_query(
                    &self.svc,
                    *t,
                    &format!("DELETE FROM {ctable} WHERE `_sprig_share` = {}", k.share),
                    self.lctn,
                )
                .await;
                set_engine_copy_state(&self.svc, *k, None).await?;
                tracing::info!(tenant = %t, object = %k.object, share = k.share, "copy of a share that is gone removed");
            }
            if kept == 0 {
                self.drop_table(*t, &d.name, &schema, o.object_id).await?;
            }
        }
        self.copies = copy_infos(self.rows.iter().map(|(k, r)| (*k, r.clone())).collect());
        Ok(())
    }

    /// Drops copy table `object` of tenant `t`'s database `db`.
    async fn drop_table(
        &self,
        t: TenantId,
        db: &str,
        schema: &str,
        object: ObjectId,
    ) -> Result<()> {
        let name = copy_table(object);
        let sql = format!("DROP TABLE IF EXISTS {}.{}", q(schema), q(&name));
        let logical = format!("DROP TABLE IF EXISTS {}.{}", q(db), q(&name));
        engine_ddl(
            &self.svc,
            t,
            db,
            DdlStatement::DropObjects {
                kind: ObjectKind::Table,
                names: vec![ObjectName {
                    db: Some(db.to_owned()),
                    name,
                }],
                if_exists: true,
            },
            &sql,
            &logical,
            self.lctn,
        )
        .await
    }

    async fn write_state(&mut self, e: &CopyEdge, serving: bool, seeded: bool) -> Result<()> {
        let now = now_ms();
        let row = CopyRow {
            serving,
            object_version: e.object_version,
            share_version: e.share_version,
            epoch: self.epoch,
            seeded_ms: if seeded {
                now
            } else {
                self.rows.get(&e.key).map_or(now, |r| r.seeded_ms)
            },
            caught_up_ms: now,
            source_tenant: e.source_tenant.0,
        };
        set_engine_copy_state(&self.svc, e.key, Some(&row)).await?;
        self.rows.insert(e.key, row);
        self.copies = copy_infos(self.rows.iter().map(|(k, r)| (*k, r.clone())).collect());
        Ok(())
    }

    /// Makes sure `e`'s copy table exists with the child's columns: made
    /// when missing, made again (the object's other copies started over)
    /// when the child's columns changed.
    async fn ensure_table(&mut self, family: &mut Family, e: &CopyEdge) -> Result<()> {
        let t = e.key.tenant;
        let cat = self.svc.cache.get(t, Freshness::Latest).await?;
        let d = cat
            .database(&database_key(&e.db_name, self.lctn))
            .ok_or_else(|| SprigError::internal("the copy's database is gone"))?;
        let child = crate::engine::show::stored_create(&cat, d.db_id, &e.table)
            .ok_or_else(|| SprigError::internal("the child's table isn't the engine's"))?;
        let mut cols = Vec::with_capacity(e.columns.len());
        for (n, _) in &e.columns {
            let (ty, collation) = sprig_inherit::adopt::column_type(&child, n)
                .ok_or_else(|| SprigError::internal(format!("the child has no column {n}")))?;
            cols.push((n.clone(), ty, collation));
        }
        let name = copy_table(e.key.object);
        if let Some(have) = crate::engine::show::stored_create(&cat, d.db_id, &name) {
            let same = cols.iter().all(|(n, ty, _)| {
                sprig_inherit::adopt::column_type(&have, n)
                    .is_some_and(|(t2, _)| t2.eq_ignore_ascii_case(ty))
            });
            if same {
                return Ok(());
            }
            // The child's columns changed (names, or types its parent
            // widened): the object's copies start over.
            self.drop_table(t, &d.name, &e.child_schema, e.key.object)
                .await?;
            let others: Vec<CopyKey> = self
                .rows
                .keys()
                .filter(|k| k.tenant == t && k.object == e.key.object)
                .copied()
                .collect();
            for k in others {
                set_engine_copy_state(&self.svc, k, None).await?;
                self.rows.remove(&k);
            }
            self.copies = copy_infos(self.rows.iter().map(|(k, r)| (*k, r.clone())).collect());
        }
        let opts = sprig_inherit::adopt::charset_options(&child).unwrap_or_default();
        let with_opts = |schema: &str| {
            let ddl = copy_ddl(schema, e.key.object, &e.child_key, &cols);
            if opts.is_empty() {
                ddl
            } else {
                format!("{ddl} {opts}")
            }
        };
        engine_ddl(
            &self.svc,
            t,
            &d.name,
            DdlStatement::CreateObject {
                kind: ObjectKind::Table,
                name: ObjectName {
                    db: Some(d.name.clone()),
                    name: name.clone(),
                },
                if_not_exists: true,
                or_replace: false,
                parent: None,
            },
            &with_opts(&e.child_schema),
            &with_opts(&d.name),
            self.lctn,
        )
        .await?;
        // The child's catalog changed: the family reads it again.
        let fresh = self.svc.cache.get(t, Freshness::Latest).await?;
        family.cats.insert(t, fresh);
        tracing::info!(tenant = %t, table = %e.table, copy = %name, "copy table made");
        Ok(())
    }

    /// Makes the engine know the share queries the source's view reads.
    fn source_lineage(&self, family: &Family, e: &CopyEdge) -> Result<()> {
        let Some(cat) = family.cats.get(&e.source_tenant) else {
            return Ok(());
        };
        let mut lineage: Vec<Arc<TenantCatalog>> = family
            .ancestors
            .get(&e.source_tenant)
            .cloned()
            .unwrap_or_default();
        if lineage.is_empty() {
            return Ok(());
        }
        lineage.push(Arc::clone(cat));
        sprig_inherit::upstream::engine_lineage(&lineage, &e.db_name, &e.table, self.lctn)
    }

    /// The source rows of `e` for `keys` (all when `None`), as text.
    async fn source(
        &mut self,
        family: &Family,
        e: &CopyEdge,
        keys: Option<&[KeyLit]>,
    ) -> Result<RawRows> {
        self.source_lineage(family, e)?;
        let sql = source_select(family, &self.copies, e, keys)?;
        ctl_query(&self.svc, e.source_tenant, &sql, self.lctn).await
    }

    /// The keys `e`'s copy holds live.
    async fn held(&self, e: &CopyEdge) -> Result<BTreeSet<KeyLit>> {
        let rows = ctl_query(
            &self.svc,
            e.key.tenant,
            &format!(
                "SELECT {} FROM {}.{} WHERE `_sprig_share` = {} AND `_sprig_live`",
                e.child_key
                    .iter()
                    .map(|k| q(k))
                    .collect::<Vec<_>>()
                    .join(", "),
                q(&e.child_schema),
                q(&copy_table(e.key.object)),
                e.key.share
            ),
            self.lctn,
        )
        .await?;
        Ok(rows.iter().map(|r| lits_key(r)).collect())
    }

    /// Fills `e`'s copy from the source's whole view (module docs).
    async fn seed(&mut self, family: &mut Family, e: &CopyEdge) -> Result<()> {
        self.ensure_table(family, e).await?;
        self.write_state(e, false, true).await?;
        let rows = self.source(family, e, None).await?;
        let present: BTreeSet<KeyLit> = rows.iter().map(|r| key_of(e, r)).collect();
        let gone: Vec<KeyLit> = self
            .held(e)
            .await?
            .into_iter()
            .filter(|k| !present.contains(k))
            .collect();
        self.write(e, &rows, &gone).await?;
        self.write_state(e, true, true).await?;
        self.counts.seeds += 1;
        metrics::counter!("sprig_inherit_copy_seeds_total").increment(1);
        tracing::info!(tenant = %e.key.tenant, object = %e.key.object, share = e.key.share, rows = rows.len(), "copy seeded");
        Ok(())
    }

    /// Brings `e`'s copy of `keys` (all when `None`) up to its source.
    async fn refresh(
        &mut self,
        family: &Family,
        e: &CopyEdge,
        keys: Option<&[KeyLit]>,
    ) -> Result<()> {
        if let Some(ks) = keys {
            for part in ks.chunks(CHUNK) {
                let rows = self.source(family, e, Some(part)).await?;
                let present: BTreeSet<KeyLit> = rows.iter().map(|r| key_of(e, r)).collect();
                let gone: Vec<KeyLit> = part
                    .iter()
                    .filter(|k| !present.contains(*k))
                    .cloned()
                    .collect();
                self.write(e, &rows, &gone).await?;
            }
        } else {
            let rows = self.source(family, e, None).await?;
            let present: BTreeSet<KeyLit> = rows.iter().map(|r| key_of(e, r)).collect();
            let gone: Vec<KeyLit> = self
                .held(e)
                .await?
                .into_iter()
                .filter(|k| !present.contains(k))
                .collect();
            self.write(e, &rows, &gone).await?;
        }
        self.write_state(e, true, false).await
    }

    /// Writes `rows` live and `gone` as tombstones into `e`'s copy.
    async fn write(&mut self, e: &CopyEdge, rows: &RawRows, gone: &[KeyLit]) -> Result<()> {
        let v = self.stamp();
        let table = format!("{}.{}", q(&e.child_schema), q(&copy_table(e.key.object)));
        for chunk in rows.chunks(CHUNK) {
            let lits: Vec<Vec<String>> = chunk
                .iter()
                .map(|r| r.iter().map(|v| literal(v.as_deref())).collect())
                .collect();
            ctl_query(
                &self.svc,
                e.key.tenant,
                &upsert_sql(e, &table, &lits, v),
                self.lctn,
            )
            .await?;
            self.counts.upserts += chunk.len() as u64;
        }
        for chunk in gone.chunks(CHUNK) {
            ctl_query(
                &self.svc,
                e.key.tenant,
                &tombstone_sql(e, &table, chunk, v),
                self.lctn,
            )
            .await?;
            self.counts.tombstones += chunk.len() as u64;
        }
        Ok(())
    }
}

/// A value read as text, as an SQL literal: its text quoted, or a hex
/// literal for bytes that aren't UTF-8 (a binary column's).
fn literal(v: Option<&[u8]>) -> String {
    use std::fmt::Write as _;
    let Some(b) = v else {
        return "NULL".to_owned();
    };
    if let Ok(s) = std::str::from_utf8(b) {
        return sprig_inherit::view::lit(s);
    }
    let mut out = String::with_capacity(b.len() * 2 + 3);
    out.push_str("X'");
    for x in b {
        let _ = write!(out, "{x:02X}");
    }
    out.push('\'');
    out
}

/// A row of key values read as text, as [`KeyLit`].
fn lits_key(row: &[Option<Vec<u8>>]) -> KeyLit {
    KeyLit(
        row.iter()
            .map(|v| key_text(v.as_deref().map(String::from_utf8_lossy).as_deref()))
            .collect(),
    )
}

/// The key of a source row of `e` (the child's columns, as text).
fn key_of(e: &CopyEdge, row: &[Option<Vec<u8>>]) -> KeyLit {
    KeyLit(
        e.child_key
            .iter()
            .filter_map(|k| {
                e.columns
                    .iter()
                    .position(|(c, _)| c.eq_ignore_ascii_case(k))
            })
            .map(|i| {
                key_text(
                    row.get(i)
                        .and_then(Option::as_deref)
                        .map(String::from_utf8_lossy)
                        .as_deref(),
                )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_read_as_text_become_literals() {
        assert_eq!(literal(None), "NULL");
        assert_eq!(literal(Some(b"it's")), sprig_inherit::view::lit("it's"));
        assert_eq!(literal(Some(&[0xff, 0x00])), "X'FF00'");
        assert_eq!(
            lits_key(&[Some(b"5".to_vec()), None]).0,
            vec!["'5'", "NULL"]
        );
    }
}
