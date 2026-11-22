//! The workload store over tenant catalogs: `WorkloadStore` for its
//! readers (automatic indexing's trigger evaluation and validation), and
//! the rollup and retention run for one tenant.

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use sprig_catalog::RecordStore;
use sprig_common::Result;
use sprig_common::ids::TenantId;

use super::{MaintenanceReport, Retention, WorkloadTables};
use crate::fingerprint::FingerprintId;
use crate::keyring::TenantKeyring;
use crate::samples::{OpenedSample, SealedSample, open};
use crate::window::FpWindowStats;

/// The `WorkloadStore`: what the advisor, placement and
/// `sprigctl explain query` read.
#[async_trait]
pub trait WorkloadStore: Send + Sync {
    /// Every fingerprint of `tenant` with runs in buckets starting in
    /// `[from, to)`, merged over the window (sums, maxima, merged
    /// histograms, so [`FpWindowStats::p95_ms`] is the window's p95, and
    /// the union of scan sessions). Samples are not filled in.
    ///
    /// # Errors
    /// The catalog's errors.
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>>;

    /// Up to `max` sealed samples of a fingerprint, newest first. They
    /// stay sealed: only the replay harness opens them
    /// ([`WorkloadStore::open_samples`]).
    ///
    /// # Errors
    /// The catalog's errors.
    async fn samples(
        &self,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>>;

    /// The replay harness's decrypt API: up to `max` samples of a
    /// fingerprint, opened in memory with `keyring`. Samples that don't
    /// open are skipped (and counted in
    /// `sprig_workload_samples_unopened_total`). Never log what this
    /// returns.
    ///
    /// # Errors
    /// The catalog's errors.
    async fn open_samples(
        &self,
        keyring: &dyn TenantKeyring,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<OpenedSample>> {
        let sealed = self.samples(tenant, fp, max).await?;
        Ok(sealed
            .iter()
            .filter_map(|s| match open(keyring, tenant, s) {
                Ok(o) => Some(o),
                Err(e) => {
                    metrics::counter!("sprig_workload_samples_unopened_total").increment(1);
                    tracing::debug!(fingerprint = %fp, sample = s.sample_id, error = %e,
                        "a workload sample didn't open; skipped");
                    None
                }
            })
            .collect())
    }
}

/// The workload store over the tenants' catalog records.
#[derive(Clone)]
pub struct CatalogWorkloadStore {
    records: Arc<dyn RecordStore>,
}

impl std::fmt::Debug for CatalogWorkloadStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogWorkloadStore")
            .finish_non_exhaustive()
    }
}

impl CatalogWorkloadStore {
    /// Over the tenants' catalog records in `records`.
    #[must_use]
    pub fn new(records: Arc<dyn RecordStore>) -> Self {
        Self { records }
    }
}

#[async_trait]
impl WorkloadStore for CatalogWorkloadStore {
    async fn window(
        &self,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>> {
        let mut tx = self.records.begin(tenant).await?;
        WorkloadTables::window(&mut *tx, tenant, from, to).await
    }

    async fn samples(
        &self,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>> {
        let mut tx = self.records.begin(tenant).await?;
        WorkloadTables::samples(&mut *tx, fp, max).await
    }
}

/// The rollup and retention run for one tenant (module docs).
///
/// # Errors
/// The record store's.
pub async fn maintain(
    records: &dyn RecordStore,
    tenant: TenantId,
    now: SystemTime,
    r: &Retention,
) -> Result<MaintenanceReport> {
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut rep = WorkloadTables::rollup(records, tenant, secs, r).await?;
    let ret = WorkloadTables::retain(records, tenant, secs, r).await?;
    rep.expired_stats = ret.expired_stats;
    rep.expired_samples = ret.expired_samples;
    rep.expired_fingerprints = ret.expired_fingerprints;
    Ok(rep)
}
