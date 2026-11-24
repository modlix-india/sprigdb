//! Retention: what a tenant's backup storage keeps.
//!
//! - **Point-in-time window** (`backup.pitr_days`): any moment in the last
//!   `pitr_days` can be restored. That needs the newest backup taken at or
//!   before the window's start (the window's base; the oldest backup when
//!   none is that old) and every archived entry after the base's index.
//! - **Daily backups** (`backup.daily_keep_days`): backups younger than
//!   this stay, restorable to their own moment.
//!
//! Everything else goes: older manifests, archived segments wholly at or
//! below the base's index, and chunks no kept manifest references (left
//! by deleted backups, or by a backup that stopped before its manifest).
//! The newest backup is always kept. Callers must not run a backup of the
//! same tenant meanwhile: its chunks aren't referenced until its manifest
//! is written.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sprig_common::Result;

use crate::archive::segments;
use crate::backup::{list, stored_chunks};
use crate::chunk_name;
use crate::crypt::DataKey;
use crate::target::BackupTarget;

const DAY_MS: u64 = 86_400_000;

/// What a retention run removed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionReport {
    /// Backups removed (their `taken_ms`).
    pub backups: Vec<u64>,
    /// Archived segments removed.
    pub segments: u64,
    /// Chunks removed.
    pub chunks: u64,
    /// The base of the point-in-time window (its `taken_ms`), if any.
    pub base: Option<u64>,
}

/// Applies retention to `tenant` as of `now_ms`.
///
/// # Errors
/// The storage failed, or a manifest doesn't open.
pub fn apply(
    target: &dyn BackupTarget,
    key: &DataKey,
    tenant: u64,
    now_ms: u64,
    pitr_days: u64,
    daily_keep_days: u64,
) -> Result<RetentionReport> {
    let mut report = RetentionReport::default();
    let all = list(target, key, tenant)?;
    let Some(newest) = all.last().map(|m| m.taken_ms) else {
        return Ok(report);
    };
    let pitr_start = now_ms.saturating_sub(pitr_days.saturating_mul(DAY_MS));
    let keep_start = now_ms.saturating_sub(daily_keep_days.saturating_mul(DAY_MS));
    // By the state's timestamp, as restores choose.
    let pitr_ts = crate::restore::ts_of_ms(pitr_start);
    let base = all
        .iter()
        .rev()
        .find(|m| m.ts <= pitr_ts)
        .or_else(|| all.first())
        .cloned();
    report.base = base.as_ref().map(|b| b.taken_ms);
    let base_ms = report.base.unwrap_or(newest);
    let (kept, gone): (Vec<_>, Vec<_>) = all
        .into_iter()
        .partition(|m| m.taken_ms >= keep_start || m.taken_ms == base_ms || m.taken_ms == newest);
    for m in &gone {
        target.delete(&crate::manifest_name(tenant, m.taken_ms))?;
        report.backups.push(m.taken_ms);
    }
    // Segments the base no longer needs.
    if let Some(b) = &base {
        for u in &b.units {
            for (_, last, name) in segments(target, tenant, u.unit)? {
                if last <= u.index {
                    target.delete(&name)?;
                    report.segments += 1;
                }
            }
        }
    }
    // Chunks no kept backup references.
    let used: BTreeSet<&str> = kept
        .iter()
        .flat_map(|m| m.units.iter())
        .flat_map(|u| u.chunks.iter().chain(u.parts.iter()))
        .map(String::as_str)
        .collect();
    for c in stored_chunks(target, tenant)? {
        if !used.contains(c.as_str()) {
            target.delete(&chunk_name(tenant, &c))?;
            report.chunks += 1;
        }
    }
    Ok(report)
}
