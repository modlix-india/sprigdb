//! The workload store of the tenant catalog, as the engine's catalog
//! records (`workload` module), with the writes the flusher makes, the
//! rollup and retention job, and the readers automatic indexing, the
//! lookup advisor and Studio use.
//!
//! ```text
//! m/workload/fps/<fingerprint u64>                              FpRecord (JSON)
//! m/workload/stats/<bucket s u64><fp u64><gateway u32><part u16> stats (binary, below)
//! m/workload/samples/<fp u64><sample u64>                       sample (JSON head + payload)
//! m/workload/sexp/<expires s u64><fp u64><sample u64>           (empty: the expiry index)
//! ```
//!
//! The rows are read on demand and never bump a catalog version: every
//! write is a short engine transaction of its own (a flush, a batch of a
//! rollup or of a retention run). Stats are keyed by their bucket first,
//! so a window, the rollup's closed buckets and retention are key ranges;
//! samples have an expiry index for the same reason.
//!
//! **Times.** `bucket_start` is UTC seconds aligned to the bucket; the
//! other times are microseconds since the epoch.
//!
//! **Rollup** ([`WorkloadTables::rollup`]), `workload.rollup_delay_minutes`
//! after a bucket closes: each fingerprint's rows of that bucket (every
//! gateway and part) become one row with `gateway_id = 0, part = 0`;
//! counters summed, maxima kept, histograms and session sets merged; the
//! merge and the deletes are one transaction per batch of 500 keys, so a
//! rerun is idempotent.
//!
//! **Day rows** *(choice)*: rolled-up hours older than [`DAY_AFTER_DAYS`]
//! (7) are merged into one row per UTC day, stored with `bucket_start` at
//! midnight, `gateway_id = 0` and `part =` [`DAY_PART`] (65535), so a
//! 30-day window reads at most 7 × 24 + 23 rows per fingerprint. Readers
//! sum whatever rows fall in the window; a window that starts inside a day
//! older than 7 days gets the whole day.
//!
//! **Retention** ([`WorkloadTables::retain`]): stats older than
//! `workload.retention_days` (a key range), expired samples (a key range
//! of the expiry index), samples past
//! `workload.max_samples_per_fingerprint` per fingerprint (oldest first),
//! and fingerprints not seen within the retention.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sprig_catalog::records::{Pk, RecordStore, RecordTx, RowTable, prefix_end, scan_all};
use sprig_catalog::{Migration, Module};
use sprig_common::ids::{DbId, TenantId};
use sprig_common::{Result, SprigError};

use crate::fingerprint::{Caller, FingerprintId, FpKind, kind_of_byte};
use crate::histogram::Histogram;
use crate::recorder::{FlushRow, NewFingerprint, SessionSet, Stats};
use crate::samples::SealedSample;
use crate::shape::{SHAPE_VERSION, Shape};
use crate::window::FpWindowStats;

/// The module name in the catalog's format records.
pub const MODULE: &str = "workload";

/// Fingerprints by id.
pub const FPS: RowTable = RowTable::new(MODULE, "fps");
/// Stats by bucket, fingerprint, gateway and part.
pub const STATS: RowTable = RowTable::new(MODULE, "stats");
/// Samples by fingerprint and id.
pub const SAMPLES: RowTable = RowTable::new(MODULE, "samples");
/// Samples by expiry.
pub const SEXP: RowTable = RowTable::new(MODULE, "sexp");

/// `workload_fingerprints`: the MariaDB table this module kept before
/// its rows became catalog records (kept for readers that still name it).
pub const DDL_FINGERPRINTS: &str = "CREATE TABLE IF NOT EXISTS workload_fingerprints (
  fingerprint_id  BIGINT UNSIGNED NOT NULL,
  kind            ENUM('sql','mongo','redis','kv','vector','search','analytics','api',
                       'logic','txn') NOT NULL,
  front_end       ENUM('mysql','mariadb','mongo','redis','http','grpc','logic',
                       'internal') NOT NULL,
  caller_fn_id    BIGINT UNSIGNED NULL,
  db_id           INT UNSIGNED    NOT NULL,
  stmt_type       VARCHAR(16)     NOT NULL,
  normalized_text MEDIUMTEXT      NOT NULL,
  shape           JSON            NOT NULL,
  object_ids      JSON            NOT NULL,
  shape_version   SMALLINT UNSIGNED NOT NULL,
  flags           SET('truncated','collision','other','unparsed','static') NOT NULL
                  DEFAULT '',
  first_seen      DATETIME(6)     NOT NULL,
  last_seen       DATETIME(6)     NOT NULL,
  PRIMARY KEY (fingerprint_id),
  KEY by_last_seen (last_seen)
) ENGINE=InnoDB";

/// `workload_stats`.
pub const DDL_STATS: &str = "CREATE TABLE IF NOT EXISTS workload_stats (
  fingerprint_id   BIGINT UNSIGNED  NOT NULL,
  bucket_start     DATETIME         NOT NULL,
  gateway_id       INT UNSIGNED     NOT NULL,
  part             SMALLINT UNSIGNED NOT NULL,
  exec_count       BIGINT UNSIGNED  NOT NULL,
  error_count      BIGINT UNSIGNED  NOT NULL,
  total_us         BIGINT UNSIGNED  NOT NULL,
  max_us           BIGINT UNSIGNED  NOT NULL,
  latency_hist     VARBINARY(512)   NOT NULL,
  rows_returned    BIGINT UNSIGNED  NOT NULL,
  rows_affected    BIGINT UNSIGNED  NOT NULL,
  examined_samples BIGINT UNSIGNED  NOT NULL,
  rows_examined    BIGINT UNSIGNED  NOT NULL,
  returned_sampled BIGINT UNSIGNED  NOT NULL,
  shards_sum       BIGINT UNSIGNED  NOT NULL,
  shards_max       INT UNSIGNED     NOT NULL,
  scatter_count    BIGINT UNSIGNED  NOT NULL,
  cross_cluster    BIGINT UNSIGNED  NOT NULL,
  in_list_max      INT UNSIGNED     NOT NULL,
  scan_runs        BIGINT UNSIGNED  NOT NULL,
  scan_sessions    VARBINARY(64)    NOT NULL,
  PRIMARY KEY (fingerprint_id, bucket_start, gateway_id, part),
  KEY by_bucket (bucket_start)
) ENGINE=InnoDB";

/// `workload_samples`.
pub const DDL_SAMPLES: &str = "CREATE TABLE IF NOT EXISTS workload_samples (
  fingerprint_id BIGINT UNSIGNED NOT NULL,
  sample_id      BIGINT UNSIGNED NOT NULL,
  captured_at    DATETIME(6)     NOT NULL,
  latency_us     BIGINT UNSIGNED NOT NULL,
  rows_examined  BIGINT UNSIGNED NULL,
  rows_returned  BIGINT UNSIGNED NOT NULL,
  key_version    INT UNSIGNED    NOT NULL,
  nonce          BINARY(12)      NOT NULL,
  payload        MEDIUMBLOB      NOT NULL,
  expires_at     DATETIME        NOT NULL,
  PRIMARY KEY (fingerprint_id, sample_id),
  KEY by_expiry (expires_at)
) ENGINE=InnoDB";

/// The module's MariaDB migrations: none, its rows are engine records.
pub static WORKLOAD_MIGRATIONS: &[Migration] = &[];

/// The module, for [`Migrator::new`](sprig_catalog::Migrator::new) and the
/// catalog's `BootstrapHooks.modules`.
pub const WORKLOAD_MODULE: Module = Module {
    name: MODULE,
    migrations: WORKLOAD_MIGRATIONS,
};

/// Keys per rollup transaction, and fingerprints per flush chunk.
pub const BATCH: usize = 500;

/// `part` of a day row.
pub const DAY_PART: u16 = u16::MAX;

/// Rolled-up hours older than this many days merge into day rows.
pub const DAY_AFTER_DAYS: u64 = 7;

const DAY: u64 = 86_400;

/// How far back of the previous run's cutoff a rollup run looks for rows
/// to merge *(choice)*: runs come every few minutes, so older buckets were
/// merged by earlier runs (a tenant's first run reads from the start); a
/// row a gateway flushed later than that for an older bucket stays a
/// separate row (readers sum every row of a window, so only compaction is
/// lost) until retention deletes it.
const ROLLUP_LOOKBACK: u64 = DAY;

/// Where each rollup pass got to: `m/workload/mark/hour|day` (u64 seconds).
pub const MARKS: RowTable = RowTable::new(MODULE, "mark");

async fn mark(store: &dyn RecordStore, tenant: TenantId, which: &str) -> Result<Option<u64>> {
    let mut tx = store.begin(tenant).await?;
    MARKS
        .get::<u64>(&mut *tx, &Pk::new().bytes(which.as_bytes()))
        .await
}

async fn set_mark(store: &dyn RecordStore, tenant: TenantId, which: &str, at: u64) -> Result<()> {
    let mut tx = store.begin(tenant).await?;
    MARKS
        .put(&mut *tx, &Pk::new().bytes(which.as_bytes()), &at)
        .await?;
    tx.commit().await
}

/// Stats rows read per rollup batch.
const MERGE_READ: usize = 20_000;

/// Retention deletes per transaction.
const PURGE_BATCH: usize = 10_000;

fn micros(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn from_micros(us: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(u64::try_from(us).unwrap_or(0))
}

fn secs(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn stmt_type(shape: &Shape) -> String {
    serde_json::to_value(shape.stmt_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_ascii_uppercase))
        .unwrap_or_default()
}

fn kind_of(s: &str) -> Option<FpKind> {
    (1..=10).filter_map(kind_of_byte).find(|k| k.as_str() == s)
}

fn bad(what: &str) -> SprigError {
    SprigError::internal(format!("workload record {what} doesn't decode"))
}

/// A fingerprint: what `workload_fingerprints` held.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FpRecord {
    /// `sql`, `mongo`, ...
    pub kind: String,
    /// `mysql`, `mariadb`, ...
    pub front_end: String,
    /// The logic function, if any.
    pub caller_fn_id: Option<u64>,
    /// The logical database part of the id.
    pub db_id: u32,
    /// `SELECT`, `UPDATE`, ...
    pub stmt_type: String,
    /// The normalized text.
    pub normalized_text: String,
    /// The shape (JSON).
    pub shape: String,
    /// Objects the shape names.
    pub object_ids: Vec<u64>,
    /// The shape's version.
    pub shape_version: u16,
    /// Flags (`truncated,collision,...`).
    pub flags: String,
    /// First seen, µs.
    pub first_seen_us: i64,
    /// Last seen, µs.
    pub last_seen_us: i64,
}

/// A stored sample besides its key and payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SampleHead {
    captured_at_us: i64,
    latency_us: u64,
    rows_examined: Option<u64>,
    rows_returned: u64,
    key_version: u32,
    nonce: [u8; 12],
    expires_at_s: u64,
}

fn encode_sample(s: &SealedSample) -> Result<Vec<u8>> {
    let head = SampleHead {
        captured_at_us: micros(s.captured_at),
        latency_us: s.latency_us,
        rows_examined: s.rows_examined,
        rows_returned: s.rows_returned,
        key_version: s.key_version,
        nonce: s.nonce,
        expires_at_s: secs(s.expires_at),
    };
    let json = serde_json::to_vec(&head).map_err(|e| SprigError::internal(e.to_string()))?;
    let mut out = Vec::with_capacity(4 + json.len() + s.payload.len());
    out.extend_from_slice(&u32::try_from(json.len()).unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&json);
    out.extend_from_slice(&s.payload);
    Ok(out)
}

fn decode_sample(fp: FingerprintId, sample_id: u64, b: &[u8]) -> Result<SealedSample> {
    let n = b
        .get(..4)
        .and_then(|x| <[u8; 4]>::try_from(x).ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| bad("sample"))? as usize;
    let json = b.get(4..4 + n).ok_or_else(|| bad("sample"))?;
    let head: SampleHead = serde_json::from_slice(json).map_err(|_| bad("sample"))?;
    Ok(SealedSample {
        fingerprint: fp,
        sample_id,
        captured_at: from_micros(head.captured_at_us),
        latency_us: head.latency_us,
        rows_examined: head.rows_examined,
        rows_returned: head.rows_returned,
        key_version: head.key_version,
        nonce: head.nonce,
        payload: b[4 + n..].to_vec(),
        expires_at: UNIX_EPOCH + Duration::from_secs(head.expires_at_s),
    })
}

/// Stats as a record value: the 15 counters (u64, big-endian), then the
/// histogram and the session set, each with a u16 length.
fn encode_stats(s: &Stats) -> Vec<u8> {
    let hist = s.latency_hist.encode();
    let sessions = s.scan_sessions.encode();
    let mut out = Vec::with_capacity(124 + hist.len() + sessions.len());
    for v in [
        s.exec_count,
        s.error_count,
        s.total_us,
        s.max_us,
        s.rows_returned,
        s.rows_affected,
        s.examined_samples,
        s.rows_examined,
        s.returned_sampled,
        s.shards_sum,
        u64::from(s.shards_max),
        s.scatter_count,
        s.cross_cluster,
        u64::from(s.in_list_max),
        s.scan_runs,
    ] {
        out.extend_from_slice(&v.to_be_bytes());
    }
    for part in [&hist, &sessions] {
        out.extend_from_slice(&u16::try_from(part.len()).unwrap_or(0).to_be_bytes());
        out.extend_from_slice(part);
    }
    out
}

fn decode_stats(b: &[u8]) -> Result<Stats> {
    let u = |i: usize| -> Result<u64> {
        b.get(i * 8..i * 8 + 8)
            .and_then(|x| <[u8; 8]>::try_from(x).ok())
            .map(u64::from_be_bytes)
            .ok_or_else(|| bad("stats"))
    };
    let mut at = 120;
    let mut part = || -> Result<&[u8]> {
        let n = b
            .get(at..at + 2)
            .and_then(|x| <[u8; 2]>::try_from(x).ok())
            .map(u16::from_be_bytes)
            .ok_or_else(|| bad("stats"))? as usize;
        let p = b.get(at + 2..at + 2 + n).ok_or_else(|| bad("stats"))?;
        at += 2 + n;
        Ok(p)
    };
    let hist = part()?;
    let latency_hist = Histogram::decode(hist).ok_or_else(|| bad("stats histogram"))?;
    let scan_sessions = SessionSet::decode(part()?);
    Ok(Stats {
        exec_count: u(0)?,
        error_count: u(1)?,
        total_us: u(2)?,
        max_us: u(3)?,
        latency_hist,
        rows_returned: u(4)?,
        rows_affected: u(5)?,
        examined_samples: u(6)?,
        rows_examined: u(7)?,
        returned_sampled: u(8)?,
        shards_sum: u(9)?,
        shards_max: u32::try_from(u(10)?).unwrap_or(u32::MAX),
        scatter_count: u(11)?,
        cross_cluster: u(12)?,
        in_list_max: u32::try_from(u(13)?).unwrap_or(u32::MAX),
        scan_runs: u(14)?,
        scan_sessions,
    })
}

/// A stats key: (bucket start, fingerprint, gateway, part).
type StatKey = (u64, u64, u32, u16);

fn stat_pk(k: StatKey) -> Pk {
    Pk::new().u64(k.0).u64(k.1).u32(k.2).raw(&k.3.to_be_bytes())
}

fn stat_key(rel: &[u8]) -> Option<StatKey> {
    let u64_at = |i: usize| {
        rel.get(i..i + 8)
            .and_then(|x| <[u8; 8]>::try_from(x).ok())
            .map(u64::from_be_bytes)
    };
    Some((
        u64_at(0)?,
        u64_at(8)?,
        rel.get(16..20)
            .and_then(|x| <[u8; 4]>::try_from(x).ok())
            .map(u32::from_be_bytes)?,
        rel.get(20..22)
            .and_then(|x| <[u8; 2]>::try_from(x).ok())
            .map(u16::from_be_bytes)?,
    ))
}

/// The stats records of buckets in `[from, to)`: `(key, raw key, stats)`.
async fn stats_in(
    rec: &mut dyn RecordTx,
    from: u64,
    to: u64,
    limit: Option<usize>,
) -> Result<Vec<(StatKey, Vec<u8>, Stats)>> {
    let base = STATS.prefix();
    let lo = STATS.key(&Pk::new().u64(from));
    let hi = STATS.key(&Pk::new().u64(to));
    let entries = match limit {
        Some(n) => rec.scan(&lo, &hi, n).await?,
        None => scan_all(rec, &lo, &hi).await?,
    };
    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        let key = stat_key(&k[base.len()..]).ok_or_else(|| bad("stats key"))?;
        out.push((key, k, decode_stats(&v)?));
    }
    Ok(out)
}

/// One fingerprint as the store has it.
#[derive(Debug, Clone, PartialEq)]
pub struct FingerprintRow {
    /// The id.
    pub id: FingerprintId,
    /// The kind.
    pub kind: FpKind,
    /// The logic function, if any.
    pub caller_fn_id: Option<u64>,
    /// The logical database part of the id.
    pub db_id: DbId,
    /// `SELECT`, `UPDATE`, ...
    pub stmt_type: String,
    /// The normalized text.
    pub text: Arc<str>,
    /// The shape.
    pub shape: Arc<Shape>,
    /// Flags (`truncated`, `collision`, ...), as stored.
    pub flags: String,
    /// First seen.
    pub first_seen: SystemTime,
    /// Last seen.
    pub last_seen: SystemTime,
}

/// What a rollup or retention run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    /// (fingerprint, bucket) pairs merged into one row.
    pub merged: u64,
    /// Partial rows deleted by the merge.
    pub merged_rows: u64,
    /// (fingerprint, day) pairs merged into day rows.
    pub days: u64,
    /// Stats rows past retention deleted.
    pub expired_stats: u64,
    /// Samples deleted (expired or over the cap).
    pub expired_samples: u64,
    /// Fingerprints not seen within the retention, deleted.
    pub expired_fingerprints: u64,
}

/// The retention settings of one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    /// `workload.bucket_minutes`.
    pub bucket_minutes: u32,
    /// `workload.rollup_delay_minutes`.
    pub rollup_delay_minutes: u32,
    /// `workload.retention_days`.
    pub retention_days: u32,
    /// `workload.max_samples_per_fingerprint`.
    pub max_samples_per_fingerprint: u32,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            bucket_minutes: 60,
            rollup_delay_minutes: 10,
            retention_days: 30,
            max_samples_per_fingerprint: 50,
        }
    }
}

/// The workload records of one tenant, in the caller's transaction (the
/// rollup and retention take the store and run their own batches).
#[derive(Debug, Clone, Copy, Default)]
pub struct WorkloadTables;

impl WorkloadTables {
    // --- the flusher's writes --------------------------------------------

    /// First sightings. A fingerprint already there whose `shape_version`
    /// is older than [`SHAPE_VERSION`] gets this build's shape,
    /// `object_ids` and `stmt_type` (a fingerprint first recorded with
    /// shape version 1 gains its `JSON_PATH` predicates' `func` and `cmp`,
    /// so automatic indexing can propose field columns for it). Rows of
    /// the same or a newer version, and `first_seen`, `last_seen` and
    /// `flags`, are left alone. Returns 1 per row inserted, 2 per row
    /// rewritten (as MariaDB's affected rows did).
    ///
    /// # Errors
    /// The record store's.
    pub async fn insert_fingerprints(
        rec: &mut dyn RecordTx,
        rows: &[NewFingerprint],
    ) -> Result<u64> {
        let mut n = 0;
        for r in rows {
            let m = &r.meta;
            let pk = Pk::new().u64(m.id.0);
            let objects: Vec<u64> = m.shape.object_ids().iter().map(|o| o.0).collect();
            if let Some(mut old) = FPS.get_for_update::<FpRecord>(rec, &pk).await? {
                if old.shape_version < SHAPE_VERSION {
                    old.stmt_type = stmt_type(&m.shape);
                    old.shape = m.shape.to_json();
                    old.object_ids = objects;
                    old.shape_version = SHAPE_VERSION;
                    FPS.put(rec, &pk, &old).await?;
                    n += 2;
                }
            } else {
                {
                    let at = micros(r.first_seen);
                    let row = FpRecord {
                        kind: m.kind.as_str().to_owned(),
                        front_end: m.front_end.as_str().to_owned(),
                        caller_fn_id: m.caller.map(|c| c.function_id),
                        db_id: m.db_id.0,
                        stmt_type: stmt_type(&m.shape),
                        normalized_text: m.text.to_string(),
                        shape: m.shape.to_json(),
                        object_ids: objects,
                        shape_version: SHAPE_VERSION,
                        flags: m.flags.to_set(),
                        first_seen_us: at,
                        last_seen_us: at,
                    };
                    FPS.put(rec, &pk, &row).await?;
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// One gateway's cumulative values, written under its `(gateway_id,
    /// part)` key (replacing what it wrote before), then `last_seen` of
    /// the fingerprints written. Idempotent.
    ///
    /// # Errors
    /// The record store's.
    pub async fn replace_stats(
        rec: &mut dyn RecordTx,
        gateway_id: u32,
        rows: &[FlushRow],
    ) -> Result<()> {
        let mut latest: BTreeMap<u64, SystemTime> = BTreeMap::new();
        for r in rows {
            let k = STATS.key(&stat_pk((
                r.bucket_start,
                r.fingerprint.0,
                gateway_id,
                r.part,
            )));
            rec.put(&k, &encode_stats(&r.stats)).await?;
            let e = latest.entry(r.fingerprint.0).or_insert(r.last_seen);
            *e = (*e).max(r.last_seen);
        }
        for (id, t) in latest {
            let pk = Pk::new().u64(id);
            if let Some(mut f) = FPS.get_for_update::<FpRecord>(rec, &pk).await?
                && micros(t) > f.last_seen_us
            {
                f.last_seen_us = micros(t);
                FPS.put(rec, &pk, &f).await?;
            }
        }
        Ok(())
    }

    /// Sealed samples, by `(fingerprint_id, sample_id)`, so a reservoir
    /// slot that changed overwrites its record.
    ///
    /// # Errors
    /// The record store's.
    pub async fn replace_samples(rec: &mut dyn RecordTx, rows: &[SealedSample]) -> Result<()> {
        for s in rows {
            let pk = Pk::new().u64(s.fingerprint.0).u64(s.sample_id);
            if let Some(old) = rec.get_for_update(&SAMPLES.key(&pk)).await? {
                let old = decode_sample(s.fingerprint, s.sample_id, &old)?;
                SEXP.delete(
                    rec,
                    &Pk::new()
                        .u64(secs(old.expires_at))
                        .u64(s.fingerprint.0)
                        .u64(s.sample_id),
                )
                .await?;
            }
            rec.put(&SAMPLES.key(&pk), &encode_sample(s)?).await?;
            SEXP.put(
                rec,
                &Pk::new()
                    .u64(secs(s.expires_at))
                    .u64(s.fingerprint.0)
                    .u64(s.sample_id),
                &(),
            )
            .await?;
        }
        Ok(())
    }

    // --- readers ------------------------------------------------------------

    /// The fingerprints with these ids (missing ones are left out).
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn fingerprints(
        rec: &mut dyn RecordTx,
        ids: &[FingerprintId],
    ) -> Result<Vec<FingerprintRow>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(f) = FPS.get::<FpRecord>(rec, &Pk::new().u64(id.0)).await? else {
                continue;
            };
            out.push(FingerprintRow {
                id: *id,
                kind: kind_of(&f.kind).ok_or_else(|| bad("fingerprint kind"))?,
                caller_fn_id: f.caller_fn_id,
                db_id: DbId(f.db_id),
                stmt_type: f.stmt_type,
                text: Arc::from(f.normalized_text),
                shape: Arc::new(
                    Shape::from_json(&f.shape)
                        .map_err(|e| SprigError::internal(format!("workload shape: {e}")))?,
                ),
                flags: f.flags,
                first_seen: from_micros(f.first_seen_us),
                last_seen: from_micros(f.last_seen_us),
            });
        }
        Ok(out)
    }

    /// Every stats row of buckets starting in `[from, to)` (seconds since
    /// the epoch), merged per fingerprint.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn merged_stats(
        rec: &mut dyn RecordTx,
        from: u64,
        to: u64,
    ) -> Result<BTreeMap<u64, Stats>> {
        let mut out: BTreeMap<u64, Stats> = BTreeMap::new();
        for ((_, fp, _, _), _, s) in stats_in(rec, from, to, None).await? {
            out.entry(fp).or_default().merge(&s);
        }
        Ok(out)
    }

    /// One fingerprint's stats per bucket in `[from, to)` (seconds since the
    /// epoch), merged over gateways and parts, oldest first: Studio's
    /// latency series (`Workload.Series`).
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn series(
        rec: &mut dyn RecordTx,
        fingerprint: FingerprintId,
        from: u64,
        to: u64,
    ) -> Result<Vec<(u64, Stats)>> {
        let mut out: BTreeMap<u64, Stats> = BTreeMap::new();
        for ((b, fp, _, _), _, s) in stats_in(rec, from, to, None).await? {
            if fp == fingerprint.0 {
                out.entry(b).or_default().merge(&s);
            }
        }
        Ok(out.into_iter().collect())
    }

    /// `WorkloadStore::window` over one tenant: every fingerprint with
    /// runs in buckets starting in `[from, to)`, its counters merged
    /// (sums, maxima, histograms and session sets), with its text and
    /// shape. Samples are not included (see [`WorkloadTables::samples`]).
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn window(
        rec: &mut dyn RecordTx,
        tenant: TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Result<Vec<FpWindowStats>> {
        let merged = Self::merged_stats(rec, secs(from), secs(to)).await?;
        let ids: Vec<FingerprintId> = merged
            .keys()
            .map(|i| sprig_common::ids::Fingerprint(*i))
            .collect();
        let metas: HashMap<u64, FingerprintRow> = Self::fingerprints(rec, &ids)
            .await?
            .into_iter()
            .map(|f| (f.id.0, f))
            .collect();
        Ok(merged
            .into_iter()
            .filter_map(|(id, stats)| {
                // Stats of a fingerprint deleted by retention: skipped.
                let f = metas.get(&id)?;
                Some(FpWindowStats {
                    tenant,
                    id: f.id,
                    kind: f.kind,
                    text: Arc::clone(&f.text),
                    shape: Arc::clone(&f.shape),
                    caller: f.caller_fn_id.map(|function_id| Caller {
                        function_id,
                        version: 0,
                    }),
                    stats,
                    samples: Vec::new(),
                })
            })
            .collect())
    }

    /// Up to `max` sealed samples of a fingerprint, newest first, not
    /// expired.
    ///
    /// # Errors
    /// The record store's.
    pub async fn samples(
        rec: &mut dyn RecordTx,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<SealedSample>> {
        let now = SystemTime::now();
        let base = SAMPLES.prefix();
        let lo = SAMPLES.key(&Pk::new().u64(fp.0));
        let mut out = Vec::new();
        for (k, v) in scan_all(rec, &lo, &prefix_end(&lo)).await? {
            let id = k
                .get(base.len() + 8..base.len() + 16)
                .and_then(|x| <[u8; 8]>::try_from(x).ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| bad("sample key"))?;
            let s = decode_sample(fp, id, &v)?;
            if s.expires_at > now {
                out.push(s);
            }
        }
        out.sort_by(|a, b| b.captured_at.cmp(&a.captured_at));
        out.truncate(max);
        Ok(out)
    }

    /// The replay harness's decrypt API: up to `max` samples of `fp`,
    /// opened with `keyring` in memory. Samples that don't open (a key
    /// version retired, a damaged record) are skipped and counted in
    /// `sprig_workload_samples_unopened_total`; their values never leave
    /// this call.
    ///
    /// # Errors
    /// The record store's.
    pub async fn open_samples(
        rec: &mut dyn RecordTx,
        keyring: &dyn crate::keyring::TenantKeyring,
        tenant: TenantId,
        fp: FingerprintId,
        max: usize,
    ) -> Result<Vec<crate::samples::OpenedSample>> {
        let sealed = Self::samples(rec, fp, max).await?;
        let mut out = Vec::with_capacity(sealed.len());
        for s in &sealed {
            match crate::samples::open(keyring, tenant, s) {
                Ok(o) => out.push(o),
                Err(e) => {
                    metrics::counter!("sprig_workload_samples_unopened_total").increment(1);
                    tracing::debug!(fingerprint = %fp, sample = s.sample_id, error = %e,
                        "a workload sample didn't open; skipped");
                }
            }
        }
        Ok(out)
    }

    /// How many fingerprints there are per front end, kind and statement
    /// type: `(front end, kind, statement type, count)`.
    ///
    /// # Errors
    /// The record store's.
    pub async fn statement_counts(
        rec: &mut dyn RecordTx,
    ) -> Result<Vec<(String, String, String, u64)>> {
        let mut out: BTreeMap<(String, String, String), u64> = BTreeMap::new();
        for f in FPS.rows::<FpRecord>(rec, &Pk::new()).await? {
            *out.entry((f.front_end, f.kind, f.stmt_type)).or_default() += 1;
        }
        Ok(out.into_iter().map(|((a, b, c), n)| (a, b, c, n)).collect())
    }

    /// The totals of the buckets that start within `window` seconds of
    /// the newest one and not before `since`: (executions, total µs, rows
    /// changed, the oldest such bucket's start); `None` without any.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn recent_sums(
        rec: &mut dyn RecordTx,
        since: u64,
        window: u64,
    ) -> Result<Option<(u64, u64, u64, u64)>> {
        let rows = stats_in(rec, since, u64::MAX, None).await?;
        let Some(newest) = rows.iter().map(|(k, _, _)| k.0).max() else {
            return Ok(None);
        };
        let from = newest.saturating_sub(window);
        let (mut n, mut us, mut changed, mut oldest) = (0u64, 0u64, 0u64, u64::MAX);
        for (k, _, s) in rows.iter().filter(|(k, _, _)| k.0 >= from) {
            n += s.exec_count;
            us += s.total_us;
            changed += s.rows_affected;
            oldest = oldest.min(k.0);
        }
        Ok(Some((n, us, changed, oldest)))
    }

    /// Every fingerprint of kind `kind` with its stats merged over every
    /// row the store keeps.
    ///
    /// # Errors
    /// The record store's; `Internal` for a malformed record.
    pub async fn totals(
        rec: &mut dyn RecordTx,
        kind: FpKind,
    ) -> Result<Vec<(FingerprintRow, Stats)>> {
        let merged = Self::merged_stats(rec, 0, u64::MAX).await?;
        let ids: Vec<FingerprintId> = merged
            .keys()
            .map(|i| sprig_common::ids::Fingerprint(*i))
            .collect();
        let mut out = Vec::new();
        for f in Self::fingerprints(rec, &ids).await? {
            if f.kind == kind
                && let Some(s) = merged.get(&f.id.0)
            {
                out.push((f, s.clone()));
            }
        }
        Ok(out)
    }

    // --- rollup and retention ---------------------------------------------

    /// Merges the partial rows of closed buckets (module docs), in batches
    /// of [`BATCH`] keys, one transaction each. `now` is seconds since the
    /// epoch.
    ///
    /// # Errors
    /// The record store's; a failed batch is rolled back and the rerun
    /// picks it up again.
    pub async fn rollup(
        store: &dyn RecordStore,
        tenant: TenantId,
        now: u64,
        r: &Retention,
    ) -> Result<MaintenanceReport> {
        let bucket = u64::from(r.bucket_minutes.max(1)) * 60;
        let delay = u64::from(r.rollup_delay_minutes) * 60;
        // A bucket starting before the cutoff has ended at least `delay` ago.
        let cutoff = now.saturating_sub(bucket + delay);
        let mut rep = MaintenanceReport::default();
        let mut from = mark(store, tenant, "hour")
            .await?
            .map_or(0, |m| m.min(cutoff).saturating_sub(ROLLUP_LOOKBACK));
        loop {
            let mut tx = store.begin(tenant).await?;
            let (keys, rows, next) = Self::merge_batch(&mut *tx, from, cutoff, false).await?;
            tx.commit().await?;
            rep.merged += keys;
            rep.merged_rows += rows;
            match next {
                Some(b) => from = b,
                None => break,
            }
        }
        set_mark(store, tenant, "hour", cutoff).await?;
        let day_cutoff = now.saturating_sub(DAY_AFTER_DAYS * DAY);
        let day_cutoff = day_cutoff - day_cutoff % DAY;
        let mut from = mark(store, tenant, "day")
            .await?
            .map_or(0, |m| m.min(day_cutoff).saturating_sub(ROLLUP_LOOKBACK));
        loop {
            let mut tx = store.begin(tenant).await?;
            let (keys, _, next) = Self::merge_batch(&mut *tx, from, day_cutoff, true).await?;
            tx.commit().await?;
            rep.days += keys;
            match next {
                Some(b) => from = b,
                None => break,
            }
        }
        set_mark(store, tenant, "day", day_cutoff).await?;
        Ok(rep)
    }

    /// One batch of the rollup: the rows of buckets in `[from, cutoff)`
    /// that need merging (partial rows; with `day`, merged hour rows), up
    /// to [`BATCH`] (fingerprint, bucket) keys, merged into their target
    /// rows. Returns (keys merged, rows merged, where the next batch
    /// starts or `None` when done).
    async fn merge_batch(
        rec: &mut dyn RecordTx,
        from: u64,
        cutoff: u64,
        day: bool,
    ) -> Result<(u64, u64, Option<u64>)> {
        // Whole buckets per batch, from a bounded read: a bucket's rows
        // are contiguous in key order, so the last bucket the read reached
        // is left for the next batch unless it is the only one.
        let rows = stats_in(rec, from, cutoff, Some(MERGE_READ)).await?;
        let full = rows.len() == MERGE_READ;
        let wanted = |k: &StatKey| {
            if day {
                k.2 == 0 && k.3 == 0
            } else {
                k.3 != DAY_PART && (k.2 != 0 || k.3 != 0)
            }
        };
        let last_bucket = rows.last().map(|(k, _, _)| k.0);
        let first_bucket = rows.first().map(|(k, _, _)| k.0);
        let mut next = None;
        let mut take: std::collections::BTreeSet<(u64, u64)> = std::collections::BTreeSet::new();
        for (k, _, _) in &rows {
            if full && Some(k.0) == last_bucket && last_bucket != first_bucket {
                next = last_bucket;
                break;
            }
            if !wanted(k) {
                continue;
            }
            if !take.contains(&(k.0, k.1)) && take.len() == BATCH {
                next = Some(k.0);
                break;
            }
            take.insert((k.0, k.1));
        }
        if next.is_none() && full {
            // One bucket filled the read: the next batch starts after it.
            next = last_bucket.map(|b| b + 1);
        }
        // Whole buckets only: a bucket the key limit cut goes to the next
        // batch.
        if let Some(b) = next {
            take.retain(|(bk, _)| *bk < b);
            if take.is_empty() {
                // The first bucket alone holds more keys than a batch: all of
                // its keys in the read go now (progress is always made).
                take = rows
                    .iter()
                    .filter(|(k, _, _)| k.0 == b && wanted(k))
                    .map(|(k, _, _)| (k.0, k.1))
                    .collect();
                next = Some(b + 1);
            }
        }
        let mut merged: BTreeMap<(u64, u64), Stats> = BTreeMap::new();
        let mut n_rows = 0u64;
        for (k, raw, _) in &rows {
            if !wanted(k) || !take.contains(&(k.0, k.1)) {
                continue;
            }
            // Read again under its lock (a late flush may rewrite a partial
            // row): what is merged is what is deleted.
            let Some(v) = rec.get_for_update(raw).await? else {
                continue;
            };
            let s = decode_stats(&v)?;
            let target_bucket = if day { k.0 - k.0 % DAY } else { k.0 };
            merged.entry((target_bucket, k.1)).or_default().merge(&s);
            rec.delete(raw).await?;
            n_rows += 1;
        }
        let part = if day { DAY_PART } else { 0 };
        for ((b, fp), mut s) in merged {
            let target = STATS.key(&stat_pk((b, fp, 0, part)));
            // A row already at the target (an earlier run's merge, or a day
            // row) joins the merge.
            if let Some(old) = rec.get_for_update(&target).await? {
                s.merge(&decode_stats(&old)?);
            }
            rec.put(&target, &encode_stats(&s)).await?;
        }
        Ok((take.len() as u64, n_rows, next))
    }

    /// Deletes what retention says (module docs), a batch per
    /// transaction. `now` is seconds since the epoch.
    ///
    /// # Errors
    /// The record store's.
    pub async fn retain(
        store: &dyn RecordStore,
        tenant: TenantId,
        now: u64,
        r: &Retention,
    ) -> Result<MaintenanceReport> {
        let mut rep = MaintenanceReport::default();
        let horizon = now.saturating_sub(u64::from(r.retention_days) * DAY);
        // Stats before the horizon: a key range.
        loop {
            let mut tx = store.begin(tenant).await?;
            let rows = stats_in(&mut *tx, 0, horizon, Some(PURGE_BATCH)).await?;
            for (_, raw, _) in &rows {
                tx.delete(raw).await?;
            }
            tx.commit().await?;
            rep.expired_stats += rows.len() as u64;
            if rows.len() < PURGE_BATCH {
                break;
            }
        }
        // Expired samples: a key range of the expiry index.
        loop {
            let mut tx = store.begin(tenant).await?;
            let base = SEXP.prefix();
            let hi = SEXP.key(&Pk::new().u64(now));
            let entries = tx.scan(&base, &hi, PURGE_BATCH).await?;
            for (k, _) in &entries {
                let rel = &k[base.len()..];
                if let Some(sample) = rel.get(8..24) {
                    tx.delete(&SAMPLES.key(&Pk(sample.to_vec()))).await?;
                }
                tx.delete(k).await?;
            }
            tx.commit().await?;
            rep.expired_samples += entries.len() as u64;
            if entries.len() < PURGE_BATCH {
                break;
            }
        }
        // Samples past the cap per fingerprint, oldest first.
        {
            let mut tx = store.begin(tenant).await?;
            let base = SAMPLES.prefix();
            let mut by_fp: BTreeMap<u64, Vec<SealedSample>> = BTreeMap::new();
            for (k, v) in scan_all(&mut *tx, &base, &prefix_end(&base)).await? {
                let rel = &k[base.len()..];
                let (Some(fp), Some(id)) = (
                    rel.get(..8)
                        .and_then(|x| <[u8; 8]>::try_from(x).ok())
                        .map(u64::from_be_bytes),
                    rel.get(8..16)
                        .and_then(|x| <[u8; 8]>::try_from(x).ok())
                        .map(u64::from_be_bytes),
                ) else {
                    continue;
                };
                by_fp.entry(fp).or_default().push(decode_sample(
                    sprig_common::ids::Fingerprint(fp),
                    id,
                    &v,
                )?);
            }
            let cap = r.max_samples_per_fingerprint as usize;
            for (fp, mut v) in by_fp {
                if v.len() <= cap {
                    continue;
                }
                v.sort_by(|a, b| {
                    b.captured_at
                        .cmp(&a.captured_at)
                        .then(a.sample_id.cmp(&b.sample_id))
                });
                for s in &v[cap..] {
                    tx.delete(&SAMPLES.key(&Pk::new().u64(fp).u64(s.sample_id)))
                        .await?;
                    SEXP.delete(
                        &mut *tx,
                        &Pk::new().u64(secs(s.expires_at)).u64(fp).u64(s.sample_id),
                    )
                    .await?;
                    rep.expired_samples += 1;
                }
            }
            tx.commit().await?;
        }
        // Fingerprints not seen within the retention.
        {
            let mut tx = store.begin(tenant).await?;
            let cut = i64::try_from(horizon.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            for (pk, f) in FPS.scan::<FpRecord>(&mut *tx, &Pk::new()).await? {
                if f.last_seen_us >= cut {
                    continue;
                }
                // Read again under its lock: a flush may have just seen it.
                if FPS
                    .get_for_update::<FpRecord>(&mut *tx, &pk)
                    .await?
                    .is_some_and(|f| f.last_seen_us < cut)
                {
                    FPS.delete(&mut *tx, &pk).await?;
                    rep.expired_fingerprints += 1;
                }
            }
            tx.commit().await?;
        }
        Ok(rep)
    }
}

mod catalog;

pub use catalog::{CatalogWorkloadStore, WorkloadStore, maintain};

#[cfg(test)]
mod tests;
