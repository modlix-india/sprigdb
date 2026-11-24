//! The continuous archive: every tenant unit's commit-log entries, in
//! closed segments uploaded as they close.
//!
//! The archive keeps the commit log's entries themselves, not the change
//! log's records: the change log keeps only primary records and metadata,
//! while a restore must rebuild everything the store holds (secondary
//! indexes, counters, queue messages). Change records can still be derived
//! from archived entries, so the archive also serves old feed history.
//!
//! The archiver holds a log floor named [`FLOOR`] on each unit: the engine
//! keeps every entry above it. A pass reads a unit's applied entries above
//! its floor, writes them as one segment
//! `t<tenant>/log/u<unit>/<first>-<last>.seg` (sealed), and only then
//! raises the floor to the segment's last index. A crash between the
//! upload and the floor leaves a segment the next pass writes again with
//! the same first index and possibly a later last one; a restore reads
//! overlapping segments by index, so duplicates are harmless.
//!
//! A segment closes when its oldest entry is older than `close_after`
//! (how much a lost node can lose at most, once backups leave it), or
//! when it reaches `segment_bytes` or `segment_entries`.
//!
//! Segment body: `magic "SPRBKL1\0" | unit u64 | (index u64 | ts u64 |
//! len u32 | payload)*`; `ts` is the entry's commit timestamp
//! ([`crate::payload::entry_ts`]).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sprig_common::{Result, SprigError};
use sprig_engine::log::LogEntry;
use sprig_engine::{Engine, NODE_UNIT, UnitId, keys};

use crate::crypt::KeyProvider;
use crate::payload::entry_ts;
use crate::segment_name;
use crate::target::BackupTarget;

/// The engine log floor the archiver holds.
pub const FLOOR: &str = "backup";
const MAGIC: &[u8; 8] = b"SPRBKL1\0";

/// When segments close.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// A segment closes once its oldest entry waited this long.
    pub close_after: Duration,
    /// ... or once it holds this many bytes of payloads.
    pub segment_bytes: usize,
    /// ... or this many entries.
    pub segment_entries: usize,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            close_after: Duration::from_secs(30),
            segment_bytes: 16 << 20,
            segment_entries: 50_000,
        }
    }
}

/// One archived entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Archived {
    /// Its index in the unit's log.
    pub index: u64,
    /// Its commit timestamp (0: no versioned writes).
    pub ts: u64,
    /// Its payload.
    pub data: Vec<u8>,
}

/// Encodes a segment body.
#[must_use]
pub fn encode_segment(unit: u64, entries: &[LogEntry]) -> Vec<u8> {
    let n: usize = entries.iter().map(|e| e.data.len() + 20).sum();
    let mut out = Vec::with_capacity(16 + n);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&unit.to_be_bytes());
    for e in entries {
        out.extend_from_slice(&e.index.to_be_bytes());
        out.extend_from_slice(&entry_ts(&e.data).to_be_bytes());
        out.extend_from_slice(
            &u32::try_from(e.data.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        out.extend_from_slice(&e.data);
    }
    out
}

/// Decodes a segment body written for `unit`.
///
/// # Errors
/// It's malformed or for another unit.
pub fn decode_segment(unit: u64, b: &[u8]) -> Result<Vec<Archived>> {
    let bad = || SprigError::internal(format!("archive: a segment of unit {unit} is malformed"));
    if b.len() < 16 || &b[..8] != MAGIC {
        return Err(bad());
    }
    if u64::from_be_bytes(b[8..16].try_into().map_err(|_| bad())?) != unit {
        return Err(SprigError::internal(format!(
            "archive: a segment under unit {unit} belongs to another unit"
        )));
    }
    let mut out = Vec::new();
    let mut at = 16usize;
    while at < b.len() {
        let h = b.get(at..at + 20).ok_or_else(bad)?;
        let index = u64::from_be_bytes(h[..8].try_into().map_err(|_| bad())?);
        let ts = u64::from_be_bytes(h[8..16].try_into().map_err(|_| bad())?);
        let len = u32::from_be_bytes(h[16..20].try_into().map_err(|_| bad())?) as usize;
        at += 20;
        let data = b.get(at..at + len).ok_or_else(bad)?.to_vec();
        at += len;
        out.push(Archived { index, ts, data });
    }
    Ok(out)
}

/// What one archiver pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Segments uploaded.
    pub segments: u64,
    /// Entries in them.
    pub entries: u64,
    /// Payload bytes in them.
    pub bytes: u64,
    /// Units whose upload failed (they're retried next pass).
    pub failed: Vec<(u64, String)>,
}

/// Archives tenant units' log entries.
pub struct Archiver {
    cfg: ArchiveConfig,
    /// When each unit's oldest unarchived entry was first seen.
    waiting: Mutex<HashMap<UnitId, Instant>>,
    /// Held by a pass.
    running: Mutex<()>,
}

impl std::fmt::Debug for Archiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archiver")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// Whether a unit is a tenant's (the node unit and the reserved tenant's
/// are not archived).
#[must_use]
pub fn is_tenant_unit(u: UnitId) -> bool {
    u != NODE_UNIT && u.0 != keys::META_TENANT
}

impl Archiver {
    /// An archiver.
    #[must_use]
    pub fn new(cfg: ArchiveConfig) -> Archiver {
        Archiver {
            cfg,
            waiting: Mutex::new(HashMap::new()),
            running: Mutex::new(()),
        }
    }

    /// Holds the log for each tenant unit `wanted` accepts that isn't held
    /// yet, from the oldest entry the log still has: the archive of a unit
    /// starts there. A unit `released` names (backups off) has its floor
    /// moved to its applied index, so the log isn't kept for it.
    ///
    /// # Errors
    /// The engine failed to store the floors.
    pub fn hold(
        &self,
        engine: &Engine,
        wanted: &dyn Fn(UnitId) -> bool,
        released: &dyn Fn(UnitId) -> bool,
    ) -> Result<()> {
        let mut set = Vec::new();
        for (u, applied) in engine.units() {
            if !is_tenant_unit(u) {
                continue;
            }
            let floor = engine.log_floor_of(u, FLOOR);
            if wanted(u) {
                if floor.is_none() {
                    let first = engine.log_first_index_of(u).unwrap_or(applied + 1);
                    set.push((u, first.saturating_sub(1)));
                }
            } else if released(u) && floor.is_some_and(|f| f < applied) {
                // Only a tenant whose backups are off; a tenant whose
                // storage failed to open keeps its floor.
                set.push((u, applied));
            }
        }
        engine.set_log_floors(FLOOR, &set)
    }

    /// One pass: closes and uploads the segments that are due. With
    /// `force`, everything applied is uploaded now. `target_for` gives a
    /// tenant's storage, or `None` when its backups are off.
    ///
    /// # Errors
    /// The engine is replicated (the archive runs on one node), or holding
    /// the log failed. A unit's upload failure is reported, not returned.
    pub fn pass(
        &self,
        engine: &Engine,
        target_for: &dyn Fn(u64) -> Result<Option<Arc<dyn BackupTarget>>>,
        keys: &dyn KeyProvider,
        force: bool,
    ) -> Result<PassReport> {
        if engine.is_replicated() {
            return Err(SprigError::internal(
                "archive: the change-log archive runs on a one-node engine for now",
            ));
        }
        // One pass at a time: two could move a floor back.
        let _one = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut report = PassReport::default();
        let mut targets: HashMap<UnitId, Arc<dyn BackupTarget>> = HashMap::new();
        let mut off: std::collections::HashSet<UnitId> = std::collections::HashSet::new();
        for (u, _) in engine.units() {
            if !is_tenant_unit(u) {
                continue;
            }
            match target_for(u.0) {
                Ok(Some(t)) => {
                    targets.insert(u, t);
                }
                Ok(None) => {
                    off.insert(u);
                }
                Err(e) => report.failed.push((u.0, e.to_string())),
            }
        }
        self.hold(engine, &|u| targets.contains_key(&u), &|u| off.contains(&u))?;
        for (u, applied) in engine.units() {
            // Only what the log has synced: an entry applied but not yet
            // durable could vanish in a power loss after it was archived.
            let applied = applied.min(engine.consensus().durable_index(u));
            let Some(target) = targets.get(&u) else {
                continue;
            };
            let Some(floor) = engine.log_floor_of(u, FLOOR) else {
                continue;
            };
            if floor >= applied {
                self.waiting
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&u);
                continue;
            }
            let due = force || {
                let mut w = self
                    .waiting
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let since = *w.entry(u).or_insert_with(Instant::now);
                since.elapsed() >= self.cfg.close_after
                    || applied - floor >= self.cfg.segment_entries as u64
            };
            if !due {
                continue;
            }
            match self.archive_unit(engine, target.as_ref(), keys, u, floor, applied) {
                Ok((segs, n, bytes)) => {
                    report.segments += segs;
                    report.entries += n;
                    report.bytes += bytes;
                    self.waiting
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&u);
                }
                Err(e) => {
                    tracing::warn!(unit = u.0, error = %e, "archiving a unit's log failed; retrying next pass");
                    report.failed.push((u.0, e.to_string()));
                }
            }
        }
        Ok(report)
    }

    /// Uploads `floor+1 ..= applied` of `u` in segments, raising the floor
    /// after each.
    fn archive_unit(
        &self,
        engine: &Engine,
        target: &dyn BackupTarget,
        keys: &dyn KeyProvider,
        u: UnitId,
        mut floor: u64,
        applied: u64,
    ) -> Result<(u64, u64, u64)> {
        let key = keys.key_for(u.0)?;
        let (mut segs, mut n, mut bytes) = (0u64, 0u64, 0u64);
        'segments: while floor < applied {
            // Read in steps, cutting the segment at its size limits.
            let mut seg: Vec<LogEntry> = Vec::new();
            let mut seg_bytes = 0usize;
            let mut next = floor + 1;
            'fill: while next <= applied {
                let to = (next + 1024).min(applied + 1);
                let first_held = engine.log_first_index_of(u);
                if seg.is_empty() && first_held.is_some_and(|f| f > next) {
                    // The log was trimmed past the floor (it was set too
                    // late, or by an older binary): the archive has a gap
                    // here, which restores refuse to cross. Go on from the
                    // oldest entry held.
                    let f = first_held.unwrap_or(next);
                    tracing::warn!(
                        unit = u.0,
                        from = next,
                        to = f - 1,
                        "archive gap: the log no longer holds these entries"
                    );
                    engine.set_log_floors(FLOOR, &[(u, f - 1)])?;
                    floor = f - 1;
                    next = f;
                    continue;
                }
                let got = engine.log().entries(u, next, to)?;
                if got.is_empty() {
                    break;
                }
                for e in got {
                    if e.index != next {
                        return Err(SprigError::internal(format!(
                            "archive: unit {} log skips from {next} to {}",
                            u.0, e.index
                        )));
                    }
                    seg_bytes += e.data.len();
                    seg.push(e);
                    next += 1;
                    if seg_bytes >= self.cfg.segment_bytes || seg.len() >= self.cfg.segment_entries
                    {
                        break 'fill;
                    }
                }
            }
            if seg.is_empty() && floor >= applied {
                break 'segments;
            }
            let (Some(first), Some(last)) =
                (seg.first().map(|e| e.index), seg.last().map(|e| e.index))
            else {
                return Err(SprigError::internal(format!(
                    "archive: unit {} log no longer holds entry {} (the archive has a gap)",
                    u.0,
                    floor + 1
                )));
            };
            let name = segment_name(u.0, u.0, first, last);
            let body = encode_segment(u.0, &seg);
            target.put(&name, &key.seal(&name, &body)?)?;
            engine.set_log_floors(FLOOR, &[(u, last)])?;
            segs += 1;
            n += seg.len() as u64;
            bytes += seg_bytes as u64;
            floor = last;
        }
        Ok((segs, n, bytes))
    }
}

/// The archived segments of `unit` of `tenant`: (first, last, name), by
/// first index.
///
/// # Errors
/// The storage failed.
pub fn segments(
    target: &dyn BackupTarget,
    tenant: u64,
    unit: u64,
) -> Result<Vec<(u64, u64, String)>> {
    let prefix = format!("t{tenant}/log/u{unit:020}/");
    let mut out: Vec<(u64, u64, String)> = target
        .list(&prefix)?
        .into_iter()
        .filter_map(|n| {
            let base = n.strip_prefix(&prefix)?.strip_suffix(".seg")?;
            let (a, b) = base.split_once('-')?;
            Some((a.parse().ok()?, b.parse().ok()?, n.clone()))
        })
        .collect();
    out.sort();
    Ok(out)
}
