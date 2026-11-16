//! The orphanage sweep: rows of a parent member written without their
//! parent (`foreign_key_checks = 0`) wait on the shard that holds routing
//! hash 0, with `_sprig_rk = 0`. A parent's insert, and an `UPDATE` of its
//! referenced column, take them to the parent's shard in the same write.
//! This sweep adopts the rows those writes miss (a parent that came by
//! another path, a parent inserted on another connection at the same
//! moment, a parent whose value wasn't a literal), and clears the key's
//! `orphanage` flag once no row waits.
//!
//! ```text
//! orphanage_tick(tenant), after the lookup tick:
//!   keys: foreign_keys rows of parent members (one column) whose orphanage
//!     is present, or was cleared in the last 10 minutes
//!   per key, skipped while a range of its group is held by the mover:
//!     s0 := the shard holding hash 0; on each shard S of s0's cluster,
//!     in transactions of at most 500 parent values:
//!       vals := waiting rows' keys whose parent S holds (FOR UPDATE)
//!       S = s0: UPDATE … JOIN: _sprig_rk := the parent's
//!       else:   INSERT INTO S.child … SELECT … JOIN; DELETE … JOIN on s0;
//!               the two counts differ → ROLLBACK (tried again next tick)
//!     waiting := COUNT(*) on s0 with _sprig_rk = 0
//!     present, none waiting → orphanage := none, publish, wait for every
//!       gateway; rows waiting again (a gateway that hadn't seen it) →
//!       present again, publish
//!     cleared, rows waiting → present again, publish
//! ```
//!
//! Shards on another cluster than s0 are left out: moving rows between
//! clusters needs the rows read and written by the controller. Their rows
//! keep waiting (readable, never joined with their parent) and the flag
//! stays present.

use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use mysql_async::{Conn, TxOpts};
use sprig_common::ids::{ClusterId, DbId, GroupId, ShardId, TenantId};
use sprig_common::names::{SchemaName, quote};
use sprig_common::{Result, SprigError};
use sprig_shard::lookup::admin::LookupHost as _;
use sprig_shard::shard_schema;
use sprig_shard::shardmap::RangeState;

use super::ShardingLoop;
use super::lookups::CtlHost;

/// Parent values adopted per transaction.
const BATCH: usize = 500;
/// Transactions per key and shard in one tick.
const MAX_BATCHES: usize = 20;
/// How long a cleared key is still checked for rows waiting.
const RECHECK: Duration = Duration::from_secs(600);
/// Lock waits of the sweep's statements, in seconds.
const LOCK_WAIT: u64 = 5;

/// One key's sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct OrphanageReport {
    /// The constraint.
    pub(super) fk: String,
    /// Rows adopted.
    pub(super) adopted: u64,
    /// Rows still waiting.
    pub(super) waiting: u64,
    /// The flag was cleared (and stayed so).
    pub(super) cleared: bool,
    /// The flag was set again (rows were waiting after a clear).
    pub(super) reopened: bool,
}

/// A key the sweep looks at.
#[derive(Debug, Clone)]
struct Key {
    fk_id: u64,
    name: String,
    present: bool,
    group: GroupId,
    db: DbId,
    child: String,
    column: String,
    parent: String,
    parent_column: String,
}

fn db_err(what: &str) -> impl Fn(mysql_async::Error) -> SprigError + '_ {
    move |e| SprigError::internal(format!("orphanage sweep: {what}: {e}"))
}

/// A waiting row's key as SQL text: numbers as they read back, anything
/// else as a hex literal of its bytes (it then matches the row's own
/// value exactly).
fn sql_value(v: &[u8], numeric: bool) -> Option<String> {
    if numeric {
        let s = std::str::from_utf8(v).ok()?;
        let ok = !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'));
        return ok.then(|| s.to_owned());
    }
    let mut out = String::with_capacity(v.len() * 2 + 3);
    out.push_str("X'");
    for b in v {
        out.push_str(&format!("{b:02X}"));
    }
    out.push('\'');
    Some(out)
}

const NUMERIC: [&str; 11] = [
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "bigint",
    "decimal",
    "float",
    "double",
    "year",
    "bit",
    "numeric",
];

impl ShardingLoop {
    /// The orphanage sweep of `tenant` (module docs).
    ///
    /// # Errors
    /// Catalog, etcd and MariaDB errors; the next tick tries again.
    pub(super) async fn orphanage_tick(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        publish_timeout: Duration,
    ) -> Result<Vec<OrphanageReport>> {
        let keys = self.orphanage_keys(tenant, home).await?;
        let mut out = Vec::new();
        for k in keys {
            match self.sweep_key(tenant, home, publish_timeout, &k).await {
                Ok(Some(r)) => {
                    if r.adopted > 0 || r.cleared || r.reopened {
                        tracing::info!(tenant = %tenant, fk = %r.fk, adopted = r.adopted,
                            waiting = r.waiting, cleared = r.cleared, reopened = r.reopened,
                            "orphanage sweep");
                    }
                    out.push(r);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(tenant = %tenant, fk = %k.name, error = %e,
                        "orphanage sweep of a key failed");
                }
            }
        }
        Ok(out)
    }

    async fn orphanage_keys(&self, tenant: TenantId, home: &ClusterId) -> Result<Vec<Key>> {
        let cat = quote(&SchemaName::catalog(tenant).to_string());
        let mut c = self.conn(home).await?;
        type Row = (
            u64,
            String,
            Option<String>,
            Option<String>,
            String,
            u32,
            u32,
            String,
            String,
        );
        let rows: Vec<Row> = c
            .exec(
                format!(
                    "SELECT f.fk_id, f.name, CAST(f.child_columns AS CHAR), \
                     CAST(f.parent_columns AS CHAR), f.orphanage, m.group_id, m.db_id, oc.name, \
                     op.name FROM {cat}.foreign_keys f \
                     JOIN {cat}.group_members m ON m.object_id = f.child_object_id \
                       AND m.routing = 'parent' \
                     JOIN {cat}.objects oc ON oc.object_id = f.child_object_id \
                     JOIN {cat}.objects op ON op.object_id = f.parent_object_id \
                     WHERE f.orphanage = 'present' \
                        OR f.updated_at > UTC_TIMESTAMP(6) - INTERVAL ? SECOND \
                     ORDER BY f.fk_id"
                ),
                (RECHECK.as_secs(),),
            )
            .await
            .map_err(db_err("foreign keys"))?;
        let one = |s: Option<String>| -> Option<String> {
            let v: Vec<String> = serde_json::from_str(s.as_deref()?).ok()?;
            match v.as_slice() {
                [c] => Some(c.clone()),
                _ => None,
            }
        };
        Ok(rows
            .into_iter()
            .filter_map(|(fk_id, name, cc, pc, orphanage, g, db, child, parent)| {
                Some(Key {
                    fk_id,
                    name,
                    present: orphanage == "present",
                    group: GroupId(g),
                    db: DbId(db),
                    child,
                    column: one(cc)?,
                    parent,
                    parent_column: one(pc)?,
                })
            })
            .collect())
    }

    #[allow(clippy::too_many_lines)] // the flow in the module docs
    async fn sweep_key(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        publish_timeout: Duration,
        k: &Key,
    ) -> Result<Option<OrphanageReport>> {
        let (map, _) = self.maps.load(tenant, k.group).await?;
        if map.ranges.iter().any(|r| r.state != RangeState::Serving) {
            tracing::debug!(tenant = %tenant, fk = %k.name, "orphanage sweep waits for the mover");
            return Ok(None);
        }
        let Some(first) = map.ranges.first() else {
            return Ok(None);
        };
        let s0 = first.shard;
        let cluster = first.cluster.clone();
        let mut shards: Vec<ShardId> = Vec::new();
        for r in &map.ranges {
            if r.cluster == cluster && !shards.contains(&r.shard) {
                shards.push(r.shard);
            }
        }
        let schema = |s: ShardId| quote(&shard_schema(tenant, k.db, k.group, s));
        let child0 = format!("{}.{}", schema(s0), quote(&k.child));
        let mut c = self.conn(&cluster).await?;
        let mut rep = OrphanageReport {
            fk: k.name.clone(),
            ..OrphanageReport::default()
        };
        if count_waiting(&mut c, &child0).await? > 0 {
            let (columns, numeric) = self.child_columns(&mut c, tenant, k, s0).await?;
            for s in &shards {
                let parent = format!("{}.{}", schema(*s), quote(&k.parent));
                let into = (*s != s0).then(|| format!("{}.{}", schema(*s), quote(&k.child)));
                for _ in 0..MAX_BATCHES {
                    let n = adopt_batch(
                        &mut c,
                        &child0,
                        &parent,
                        into.as_deref(),
                        (&k.column, &k.parent_column),
                        (&columns, numeric),
                    )
                    .await?;
                    rep.adopted += n.moved;
                    if n.values < BATCH {
                        break;
                    }
                }
            }
        }
        rep.waiting = count_waiting(&mut c, &child0).await?;
        // The flag follows what waits.
        if rep.waiting == 0 && k.present {
            self.set_orphanage(tenant, home, k.fk_id, false, publish_timeout)
                .await?;
            rep.waiting = count_waiting(&mut c, &child0).await?;
            if rep.waiting > 0 {
                // A gateway wrote a row before it saw the change.
                self.set_orphanage(tenant, home, k.fk_id, true, publish_timeout)
                    .await?;
                rep.reopened = true;
            } else {
                rep.cleared = true;
            }
        } else if rep.waiting > 0 && !k.present {
            self.set_orphanage(tenant, home, k.fk_id, true, publish_timeout)
                .await?;
            rep.reopened = true;
        }
        Ok(Some(rep))
    }

    /// The child's columns an adopted row carries (visible, not
    /// generated), and whether its foreign-key column is numeric.
    async fn child_columns(
        &self,
        c: &mut Conn,
        tenant: TenantId,
        k: &Key,
        s0: ShardId,
    ) -> Result<(Vec<String>, bool)> {
        let rows: Vec<(String, String)> = c
            .exec(
                "SELECT COLUMN_NAME, LOWER(DATA_TYPE) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND EXTRA NOT LIKE '%INVISIBLE%' \
                 AND IS_GENERATED = 'NEVER' ORDER BY ORDINAL_POSITION",
                (shard_schema(tenant, k.db, k.group, s0), k.child.as_str()),
            )
            .await
            .map_err(db_err("columns"))?;
        let numeric = rows
            .iter()
            .any(|(n, t)| n.eq_ignore_ascii_case(&k.column) && NUMERIC.contains(&t.as_str()));
        Ok((rows.into_iter().map(|(n, _)| n).collect(), numeric))
    }

    /// Sets the key's `orphanage` and publishes the catalog change, waiting
    /// until every gateway has it (as lookup changes do).
    async fn set_orphanage(
        &self,
        tenant: TenantId,
        home: &ClusterId,
        fk_id: u64,
        present: bool,
        publish_timeout: Duration,
    ) -> Result<()> {
        let host = CtlHost {
            l: self,
            tenant,
            home: home.clone(),
            publish_timeout,
        };
        let mut cat = host.catalog().await?;
        cat.exec_drop(
            "UPDATE foreign_keys SET orphanage = ?, updated_at = UTC_TIMESTAMP(6) \
             WHERE fk_id = ?",
            (if present { "present" } else { "none" }, fk_id),
        )
        .await
        .map_err(db_err("flag"))?;
        drop(cat);
        host.publish().await
    }
}

/// Rows waiting in `child0` (the hash-0 shard's child table).
async fn count_waiting(c: &mut Conn, child0: &str) -> Result<u64> {
    let n: Option<u64> = c
        .query_first(format!(
            "SELECT COUNT(*) FROM {child0} WHERE {} = 0",
            quote(sprig_shard::rk::RK_COLUMN)
        ))
        .await
        .map_err(db_err("count"))?;
    Ok(n.unwrap_or(0))
}

/// What one adoption transaction did.
struct Batch {
    /// Parent values found.
    values: usize,
    /// Rows moved.
    moved: u64,
}

/// One transaction of the sweep on one shard (module docs): `into` is the
/// shard's child table, `None` on the hash-0 shard itself.
async fn adopt_batch(
    c: &mut Conn,
    child0: &str,
    parent: &str,
    into: Option<&str>,
    (column, parent_column): (&str, &str),
    (columns, numeric): (&[String], bool),
) -> Result<Batch> {
    let rk = quote(sprig_shard::rk::RK_COLUMN);
    let (fk, pc) = (quote(column), quote(parent_column));
    let join = format!(
        "FROM {child0} c JOIN {parent} p ON p.{pc} = c.{fk} WHERE c.{rk} = 0 AND p.{rk} IS NOT NULL"
    );
    let stmt =
        |sql: String| format!("SET STATEMENT innodb_lock_wait_timeout = {LOCK_WAIT} FOR {sql}");
    let mut opts = TxOpts::default();
    opts.with_isolation_level(mysql_async::IsolationLevel::RepeatableRead);
    let mut tx = c.start_transaction(opts).await.map_err(db_err("begin"))?;
    let found: Vec<Option<Vec<u8>>> = tx
        .query(stmt(format!(
            "SELECT DISTINCT c.{fk} {join} LIMIT {BATCH} FOR UPDATE"
        )))
        .await
        .map_err(db_err("waiting rows"))?;
    let values: Vec<String> = found
        .iter()
        .flatten()
        .filter_map(|v| sql_value(v, numeric))
        .collect();
    if values.is_empty() {
        tx.rollback().await.map_err(db_err("rollback"))?;
        return Ok(Batch {
            values: found.len(),
            moved: 0,
        });
    }
    let only = format!(" AND c.{fk} IN ({})", values.join(", "));
    let moved = match into {
        None => {
            tx.query_drop(stmt(format!(
                "UPDATE {child0} c JOIN {parent} p ON p.{pc} = c.{fk} SET c.{rk} = p.{rk} \
                 WHERE c.{rk} = 0 AND p.{rk} IS NOT NULL{only}"
            )))
            .await
            .map_err(db_err("adopt in place"))?;
            tx.affected_rows()
        }
        Some(into) if !columns.is_empty() => {
            let list = columns.iter().map(|x| quote(x)).collect::<Vec<_>>();
            let from_c = list
                .iter()
                .map(|x| format!("c.{x}"))
                .collect::<Vec<_>>()
                .join(", ");
            tx.query_drop(stmt(format!(
                "INSERT INTO {into} ({}, {rk}) SELECT {from_c}, p.{rk} {join}{only}",
                list.join(", ")
            )))
            .await
            .map_err(db_err("copy"))?;
            let copied = tx.affected_rows();
            tx.query_drop(stmt(format!("DELETE c {join}{only}")))
                .await
                .map_err(db_err("delete"))?;
            let deleted = tx.affected_rows();
            if copied != deleted {
                tx.rollback().await.map_err(db_err("rollback"))?;
                return Err(SprigError::internal(format!(
                    "orphanage sweep: {copied} rows copied to {into} but {deleted} deleted; \
                     rolled back (is the parent's key unique?)"
                )));
            }
            copied
        }
        Some(_) => 0,
    };
    tx.commit().await.map_err(db_err("commit"))?;
    Ok(Batch {
        values: found.len(),
        moved,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiting_keys_become_sql_values() {
        assert_eq!(sql_value(b"42", true).as_deref(), Some("42"));
        assert_eq!(sql_value(b"-1.5e3", true).as_deref(), Some("-1.5e3"));
        assert_eq!(sql_value(b"1; DROP", true), None);
        assert_eq!(sql_value(b"a'b", false).as_deref(), Some("X'612762'"));
        assert_eq!(sql_value(b"", false).as_deref(), Some("X''"));
    }
}
