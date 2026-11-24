//! Tenant backups and point-in-time restore on the storage engine (one
//! node).
//!
//! - [`backup`]: a tenant's unit exported from one point of its commit
//!   log, cut into content-named chunks; unchanged chunks are referenced,
//!   not copied. The manifest is written last.
//! - [`archive`]: every tenant unit's commit-log entries, uploaded in
//!   closed segments under the engine log floor `backup`.
//! - [`restore`]: the newest backup at or before a moment plus the
//!   archived entries up to it, rebuilt in a scratch engine, then ingested
//!   into a new tenant in one step.
//! - [`retention`]: the point-in-time window and the daily backups kept.
//! - [`target`]: where it all goes (a directory, or S3-compatible storage
//!   via [`s3`]); [`crypt`]: everything is sealed with the tenant's key.
//! - [`service`]: the node's backup service: the archive passes, the
//!   daily schedule and retention, and the operations behind `sprigctl`.
//!
//! Objects, per tenant `t`:
//! ```text
//! t<t>/backups/<taken ms, 20 digits>.json     manifest (sealed JSON)
//! t<t>/chunks/<keyed hash>                    chunk (sealed)
//! t<t>/log/u<unit, 20 digits>/<first>-<last>.seg   archived entries (sealed)
//! ```

pub mod archive;
pub mod backup;
pub mod crypt;
pub mod payload;
pub mod restore;
pub mod retention;
pub mod s3;
pub mod service;
pub mod settings;
pub mod target;

pub use backup::{BackupManifest, BackupReport};
pub use crypt::{DataKey, KeyFile, KeyProvider};
pub use restore::RestoreReport;
pub use service::{BackupService, ServiceConfig};
pub use target::{BackupTarget, LocalDir, open_location};

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The object of a backup's manifest.
#[must_use]
pub fn manifest_name(tenant: u64, taken_ms: u64) -> String {
    format!("t{tenant}/backups/{taken_ms:020}.json")
}

/// The object of a chunk.
#[must_use]
pub fn chunk_name(tenant: u64, name: &str) -> String {
    format!("t{tenant}/chunks/{name}")
}

/// The object of an archived segment.
#[must_use]
pub fn segment_name(tenant: u64, unit: u64, first: u64, last: u64) -> String {
    format!("t{tenant}/log/u{unit:020}/{first:020}-{last:020}.seg")
}
