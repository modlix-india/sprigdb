//! A deleted tenant's cleanup.
//!
//! ```text
//! cleanup(T), state deleting, every step idempotent (a crash resumes):
//! 1. mover jobs of T in this DC: cancel those before their cutover; end the
//!    retention of retaining ones now, flagged purge_tenant; wait (the loop
//!    looks again every 2 s) until every one is done or rolled_back
//! 2. hooks.dropping; refuse (39072) while the catalog lists live databases;
//!    read the catalog's database ids
//! 3. on every cluster of the DC but rejected and retired ones (default
//!    naming only): drop T's schemas whose database the catalog listed,
//!    T's accounts and roles, T's __sprig_names rows and the rows of T's
//!    finished jobs, all by exact names (sprig_cluster::purge); a cluster
//!    whose t<T>_sprig carries another tenant's mark is left alone
//! 4. drop the catalog (unless another tenant's mark is on it)
//! 5. delete T's finished job records, then T's keys in the DC tier:
//!    shardmaps/T/, pins/T, promotions/T, feed/{ownership,subs,checkpoints}/T/,
//!    logic/schedules/T/, mover/shard-locks/T/, inherit-instances/T/,
//!    inherit-xo/T/  (each prefix ends in "/")
//! 6. delete the directory entry and its name
//! ```

use std::collections::BTreeSet;

use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use sprig_catalog::store::DbState;
use sprig_catalog::store::rec::{CoreReader, CoreRecords};
use sprig_common::ids::{DbId, JobId, TenantId};
use sprig_common::names::quote;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_meta::{Cmp, Key, ListOpts, MetaStore, Op, Revision, Txn};
use sprig_mover::{JobRecord, JobState, JobStore, MetaMover, Mover};

/// Most deletes in one transaction.
const DELETE_BATCH: usize = 64;

/// What [`settle_jobs`] found.
#[derive(Debug, Default)]
pub(crate) struct Settled {
    /// Jobs still running (cancelled or told to finish).
    pub open: Vec<JobId>,
    /// Finished jobs, with their records' revisions.
    pub finished: Vec<(JobRecord, Revision)>,
}

/// Brings `tenant`'s mover jobs in `jobs`' tier to an end: a job before
/// its cutover is cancelled (it rolls back, dropping its copies); a
/// retaining one's retention ends now and its cleaning purges the tenant
/// on the source ([`JobStore::end_retention_for_deletion`]); committed jobs
/// past that finish on their own.
///
/// # Errors
/// The store's errors.
pub(crate) async fn settle_jobs(jobs: &JobStore, tenant: TenantId) -> Result<Settled> {
    let mover = MetaMover::new(jobs.clone());
    let mut out = Settled::default();
    for (rec, rev) in jobs.all().await? {
        if rec.tenant_id != tenant {
            continue;
        }
        if rec.state.is_terminal() {
            out.finished.push((rec, rev));
            continue;
        }
        out.open.push(rec.job_id);
        if rec.state == JobState::Retaining {
            if jobs.end_retention_for_deletion(&rec, rev).await? {
                tracing::info!(tenant = %tenant, job = %rec.job_id, "retention ended: the tenant is deleted");
            }
        } else if !rec.state.is_committed() && rec.cancel.is_none() {
            match mover
                .cancel(&rec.job_id, "the tenant is being deleted")
                .await
            {
                Ok(()) => {
                    tracing::info!(tenant = %tenant, job = %rec.job_id, "job cancelled: the tenant is deleted");
                }
                Err(e) => {
                    tracing::warn!(tenant = %tenant, job = %rec.job_id, error = %e, "cancelling the deleted tenant's job failed; retried");
                }
            }
        }
    }
    Ok(out)
}

/// The DC tier's key prefixes holding only `tenant`'s keys. Each ends in
/// `/`, so tenant 3's never covers tenant 30's.
#[must_use]
pub(crate) fn meta_prefixes(tenant: TenantId) -> Vec<String> {
    [
        "shardmaps",
        "feed/ownership",
        "feed/subs",
        "feed/checkpoints",
        "logic/schedules",
        "mover/shard-locks",
        // The tenant's instances of inherited messaging
        // objects, and its cross-tenant outbox schemas.
        "inherit-instances",
        "inherit-xo",
        // Where its engine unit lives: gone last, after its records.
        "units",
    ]
    .iter()
    .map(|p| format!("{p}/{tenant}/"))
    .collect()
}

/// The DC tier's single keys of `tenant`.
#[must_use]
pub(crate) fn meta_keys(tenant: TenantId) -> Vec<String> {
    vec![format!("pins/{tenant}"), format!("promotions/{tenant}")]
}

/// Deletes `tenant`'s keys in `store`'s tier ([`meta_prefixes`],
/// [`meta_keys`]) in transactions guarded by `guard`. Returns how many
/// were deleted.
///
/// # Errors
/// 39504 when `guard` fails; the store's errors.
pub(crate) async fn purge_meta(
    store: &dyn MetaStore,
    tenant: TenantId,
    guard: &[Cmp],
) -> Result<usize> {
    let mut keys: Vec<Key> = Vec::new();
    for rel in meta_prefixes(tenant) {
        let prefix = store.tier().key(&rel)?;
        let mut opts = ListOpts::default();
        loop {
            let page = store.list(&prefix, opts.clone()).await?;
            keys.extend(
                page.kvs
                    .iter()
                    .filter(|kv| kv.key.as_str().starts_with(prefix.as_str()))
                    .map(|kv| kv.key.clone()),
            );
            if !page.more {
                break;
            }
            opts.at = Some(page.revision);
            opts.after = page.kvs.last().map(|kv| kv.key.clone());
        }
    }
    for rel in meta_keys(tenant) {
        keys.push(store.tier().key(&rel)?);
    }
    let mut n = 0;
    for batch in keys.chunks(DELETE_BATCH) {
        let r = store
            .txn(Txn {
                when: guard.to_vec(),
                then: batch.iter().cloned().map(Op::Delete).collect(),
                otherwise: Vec::new(),
            })
            .await?;
        if !r.succeeded {
            return Err(SprigError::new(
                SprigCode::NotLeader,
                "this controller no longer leads the DC",
            ));
        }
        n += r
            .results
            .iter()
            .filter(|x| matches!(x, sprig_meta::OpResult::Delete(true)))
            .count();
    }
    Ok(n)
}

/// How many of the tenant's databases aren't dropped (its catalog records,
/// through the engine node).
///
/// # Errors
/// 39070 when the engine node can't be reached.
pub(crate) async fn live_databases(tenant: TenantId) -> Result<u64> {
    CoreReader::new(crate::catalog_records(), tenant)
        .live_databases()
        .await
}

/// Every database id the tenant's catalog lists, in any state: its
/// catalog records, and the MariaDB catalog schema `schema` (from before
/// the catalog moved onto records; none if the table or the schema is
/// gone).
///
/// # Errors
/// A backend error.
pub(crate) async fn catalog_db_ids(
    tenant: TenantId,
    pool: &Pool,
    schema: &str,
) -> Result<BTreeSet<DbId>> {
    let mut out: BTreeSet<DbId> = CoreReader::new(crate::catalog_records(), tenant)
        .databases()
        .await?
        .into_iter()
        .map(|d| d.db_id)
        .collect();
    out.extend(sql_db_ids(pool, schema).await?);
    Ok(out)
}

async fn sql_db_ids(pool: &Pool, schema: &str) -> Result<BTreeSet<DbId>> {
    let mut conn = crate::catalogs::acquire(pool, "tenant cleanup").await?;
    let r: std::result::Result<Vec<u32>, _> = conn
        .query(format!("SELECT db_id FROM {}.`databases`", quote(schema)))
        .await;
    match r {
        Ok(ids) => Ok(ids.into_iter().map(DbId).collect()),
        Err(mysql_async::Error::Server(e)) if matches!(e.code, 1049 | 1146) => Ok(BTreeSet::new()),
        Err(e) => Err(SprigError::new(
            SprigCode::ClusterUnavailable,
            format!("tenant cleanup: read {schema}.databases: {e}"),
        )),
    }
}

/// Marks every database of the tenant's catalog dropped (the last resort
/// for a deleted tenant, whose catalog goes next and whose schemas are
/// dropped by id), in one record transaction and without a version (no
/// session reads a deleted tenant). Returns how many rows changed.
///
/// # Errors
/// 39070 when the engine node can't be reached.
pub(crate) async fn mark_databases_dropped(tenant: TenantId) -> Result<u64> {
    let records = crate::catalog_records();
    let mut tx = records.begin(tenant).await?;
    let r = async {
        let mut n = 0;
        for d in CoreRecords::databases_since(&mut *tx, None).await? {
            if d.state != DbState::Dropped {
                CoreRecords::set_database_state(
                    &mut *tx,
                    d.db_id,
                    DbState::Dropped,
                    d.changed_version,
                )
                .await?;
                n += 1;
            }
        }
        Ok::<u64, SprigError>(n)
    }
    .await;
    match r {
        Ok(n) => {
            tx.commit().await?;
            Ok(n)
        }
        Err(e) => {
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}

/// Deletes every catalog record of a deleted tenant (core rows, auth and
/// modules) on the engine node, a batch per record transaction.
///
/// # Errors
/// 39070 when the engine node can't be reached.
pub(crate) async fn delete_catalog_records(tenant: TenantId) -> Result<u64> {
    let records = crate::catalog_records();
    let mut n = 0;
    loop {
        let mut tx = records.begin(tenant).await?;
        let batch = match tx.scan(b"", &[0xFF], 1_000).await {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(e);
            }
        };
        if batch.is_empty() {
            let _ = tx.rollback().await;
            return Ok(n);
        }
        for (k, _) in &batch {
            if let Err(e) = tx.delete(k).await {
                let _ = tx.rollback().await;
                return Err(e);
            }
        }
        tx.commit().await?;
        n += batch.len() as u64;
    }
}

#[cfg(test)]
mod tests;
