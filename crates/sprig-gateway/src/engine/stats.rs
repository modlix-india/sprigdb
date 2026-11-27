//! Planner statistics of engine tables: kept with each table, collected by
//! `ANALYZE TABLE` and again on their own when enough rows changed, and read
//! by the general planner's estimates (`general::storage`).
//!
//! ```text
//! record   the table's META key "stats" (beside its index and layout
//!          records), sprig_plan::stats::TableStats in its stored form:
//!          rows, per column NULL share, distinct values and a histogram,
//!          per index distinct values of each leading run of parts
//!          (PRIMARY for the primary key). It is written in its own
//!          transaction, goes with the table when it is dropped, and is
//!          no row change (change readers skip META keys).
//! counters per (tenant, object), in hashed buckets: row writes (Tx put,
//!          insert, delete of a row key) and net rows added. A collection
//!          notes the counters it started at; changes since are the
//!          difference (a bucket two tables share over-counts: a refresh
//!          comes early, never late).
//! cache    the record as last read, per (tenant, object), read again from
//!          the statement's transaction after TTL (another node may have
//!          collected); a collection here replaces it at once.
//! refresh  automatic, in the background, one collection at a time per
//!          process: a table without statistics once it holds AUTO_MIN_ROWS
//!          rows, a table whose changes since its collection reach
//!          REFRESH_SHARE of its rows (at least REFRESH_MIN), and a table
//!          whose statistics count rows its first entries don't have
//!          (emptied by TRUNCATE). Not retried for RETRY after it started.
//! switch   SPRIGDB_ENGINE_STATS=off: plans ignore the statistics and none
//!          are collected on their own; ANALYZE TABLE still collects.
//! ```

// Counts and shares are estimates: the casts between counts and floats
// lose nothing that matters.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]

pub(crate) mod admin;
pub(crate) mod collect;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_engine::keys::{KeyPrefix, kind};
use sprig_plan::stats::TableStats;

use super::row::Datum;
use super::table::TableDef;

/// The environment switch: `off` stops plans reading the statistics and
/// automatic collection.
pub(crate) const SWITCH: &str = "SPRIGDB_ENGINE_STATS";
/// Share of a table's rows whose changes make its statistics stale.
pub(crate) const REFRESH_SHARE: f64 = 0.10;
/// The fewest changes that make statistics stale.
pub(crate) const REFRESH_MIN: u64 = 200;
/// Rows a table without statistics reaches before they are collected.
pub(crate) const AUTO_MIN_ROWS: u64 = 1_000;
/// How long a read of the record is reused.
const TTL: Duration = Duration::from_secs(20);
/// How long an automatic collection of a table isn't asked again.
const RETRY: Duration = Duration::from_secs(60);
/// The name the primary key's figures are kept under.
pub(crate) const PRIMARY: &str = "PRIMARY";

/// Whether plans use the statistics (and they are collected on their own).
pub(crate) fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var(SWITCH).is_ok_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "off" | "0" | "false"
            )
        })
    })
}

/// The key of a table's statistics record.
pub(crate) fn record_key(tenant: u64, object: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(32);
    KeyPrefix {
        tenant,
        shard: 0,
        object,
        kind: kind::META,
        index: 0,
    }
    .encode_into(&mut k);
    k.extend_from_slice(b"stats");
    k
}

// Write counters.

const BUCKETS: usize = 1 << 13;

struct Counters {
    changes: Vec<AtomicU64>,
    net: Vec<AtomicI64>,
    /// When counting started (µs since the epoch): every write since is
    /// counted.
    since_us: u64,
}

static COUNTERS: LazyLock<Counters> = LazyLock::new(|| Counters {
    changes: (0..BUCKETS).map(|_| AtomicU64::new(0)).collect(),
    net: (0..BUCKETS).map(|_| AtomicI64::new(0)).collect(),
    since_us: collect::now_us(),
});

/// The counters a record's changes are counted from: zero when it was
/// collected before counting started here (every write since is after
/// it), else the counters now.
fn base_for(tenant: u64, object: u32, st: Option<&TableStats>) -> (u64, i64) {
    match st {
        Some(s) if s.at_us < COUNTERS.since_us => (0, 0),
        _ => counters(tenant, object),
    }
}

fn bucket(tenant: u64, object: u32) -> usize {
    let mut h = tenant ^ (u64::from(object) << 32 | u64::from(object));
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    (h as usize) & (BUCKETS - 1)
}

/// A write of `key` by a transaction: a row of a table (a primary-kind
/// key) counts as one change, and `delta` rows more (1 inserted, -1
/// deleted, 0 changed). Any other key is not counted.
pub(crate) fn note_write(key: &[u8], delta: i64) {
    const KIND_AT: usize = 16;
    if key.get(KIND_AT) != Some(&kind::PRIMARY) {
        return;
    }
    let (Some(t), Some(o)) = (key.get(..8), key.get(12..16)) else {
        return;
    };
    let tenant = u64::from_be_bytes(t.try_into().unwrap_or_default());
    let object = u32::from_be_bytes(o.try_into().unwrap_or_default());
    let b = bucket(tenant, object);
    COUNTERS.changes[b].fetch_add(1, Ordering::Relaxed);
    if delta != 0 {
        COUNTERS.net[b].fetch_add(delta, Ordering::Relaxed);
    }
}

/// The counters of a table now: (changes, net rows added).
pub(crate) fn counters(tenant: u64, object: u32) -> (u64, i64) {
    let b = bucket(tenant, object);
    (
        COUNTERS.changes[b].load(Ordering::Relaxed),
        COUNTERS.net[b].load(Ordering::Relaxed),
    )
}

// The cache.

struct Entry {
    stats: Option<Arc<TableStats>>,
    read_at: Instant,
    /// The counters when the statistics were collected or first read here.
    base: (u64, i64),
    /// The table's first entries hold fewer rows than the statistics say.
    emptied: Option<u64>,
    /// When an automatic collection was last asked for.
    asked: Option<Instant>,
}

static CACHE: LazyLock<Mutex<HashMap<(u64, u32), Entry>>> = LazyLock::new(Mutex::default);

/// One automatic collection at a time per process.
static BUSY: AtomicBool = AtomicBool::new(false);

/// What the planner's estimates read of one table.
#[derive(Clone)]
pub(crate) struct Known {
    /// The statistics.
    pub stats: Arc<TableStats>,
    /// The rows the table holds now, as well as known: the statistics'
    /// count and the rows added since.
    pub rows: u64,
}

/// The statistics of a table for a statement's estimates. `read` reads a
/// key at the statement's read view; `probe` counts the table's first rows
/// up to its argument. `None` when there are none (or the switch is off);
/// an automatic collection is asked for when they are stale.
pub(crate) fn known(
    tenant: u64,
    def: &Arc<TableDef>,
    object: u32,
    read: &dyn Fn(&[u8]) -> Option<Bytes>,
    probe: &dyn Fn(usize) -> usize,
) -> Option<Known> {
    if !enabled() {
        return None;
    }
    let now = Instant::now();
    let key = (tenant, object);
    let fresh = {
        let cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        cache
            .get(&key)
            .filter(|e| now.duration_since(e.read_at) < TTL)
            .map(|e| (e.stats.clone(), e.base, e.emptied))
    };
    let (stats, base, emptied) = if let Some(f) = fresh {
        f
    } else {
        let stats = read(&record_key(tenant, object))
            .and_then(|raw| TableStats::decode(&raw))
            .map(Arc::new);
        // Statistics of many rows over a table whose first entries are
        // fewer: emptied since (TRUNCATE), or most rows deleted.
        let emptied = stats.as_ref().and_then(|s| {
            (s.rows >= 2 * PROBE as u64)
                .then(|| probe(PROBE) as u64)
                .filter(|n| *n < PROBE as u64)
        });
        let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        let e = cache.entry(key).or_insert_with(|| Entry {
            stats: None,
            read_at: now,
            base: counters(tenant, object),
            emptied: None,
            asked: None,
        });
        // A record collected elsewhere (or first seen here).
        if e.stats.as_ref().map(|s| (s.at_us, s.rows)) != stats.as_ref().map(|s| (s.at_us, s.rows))
        {
            e.base = base_for(tenant, object, stats.as_deref());
        }
        e.stats.clone_from(&stats);
        e.read_at = now;
        e.emptied = emptied;
        (stats, e.base, emptied)
    };
    let stats = stats?;
    let (writes, added) = counters(tenant, object);
    let rows = match emptied {
        Some(n) => n,
        None => (stats.rows as i64 + (added - base.1)).max(0) as u64,
    };
    let since = writes.saturating_sub(base.0);
    if emptied.is_some() || stats.version != def.version || stale(stats.rows, since) {
        ask(tenant, def);
    }
    Some(Known { stats, rows })
}

/// Whether statistics taken over `rows` rows are stale after `changed`
/// row changes.
pub(crate) fn stale(rows: u64, changed: u64) -> bool {
    changed as f64 >= (rows as f64 * REFRESH_SHARE).max(REFRESH_MIN as f64)
}

/// Rows counted to check that a table still holds what its statistics say.
const PROBE: usize = 24;

/// A table without statistics was counted at `rows`: its statistics are
/// asked for once it is large enough.
pub(crate) fn counted(tenant: u64, def: &Arc<TableDef>, rows: u64) {
    if enabled() && rows >= AUTO_MIN_ROWS {
        ask(tenant, def);
    }
}

/// Asks for an automatic collection of `def`'s statistics, unless one was
/// asked for lately or another runs.
fn ask(tenant: u64, def: &Arc<TableDef>) {
    let Ok(object) = u32::try_from(def.object.0) else {
        return;
    };
    let now = Instant::now();
    {
        let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        let e = cache.entry((tenant, object)).or_insert_with(|| Entry {
            stats: None,
            read_at: now.checked_sub(TTL).unwrap_or(now),
            base: counters(tenant, object),
            emptied: None,
            asked: None,
        });
        if e.asked.is_some_and(|t| now.duration_since(t) < RETRY) {
            return;
        }
        if BUSY.swap(true, Ordering::AcqRel) {
            return;
        }
        e.asked = Some(now);
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        BUSY.store(false, Ordering::Release);
        return;
    };
    let def = Arc::clone(def);
    rt.spawn(async move {
        let r = collect::analyze(tenant, &def, &collect::Ask::default()).await;
        BUSY.store(false, Ordering::Release);
        if let Err(e) = r {
            tracing::debug!(
                table = %def.name,
                error = %e,
                "automatic statistics collection failed"
            );
        }
    });
}

/// A collection of `object`'s statistics finished: the cache holds them
/// from now, with changes counted from `base` (the counters when it
/// started).
pub(crate) fn collected(tenant: u64, object: u32, stats: Arc<TableStats>, base: (u64, i64)) {
    let now = Instant::now();
    let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
    let e = cache.entry((tenant, object)).or_insert_with(|| Entry {
        stats: None,
        read_at: now,
        base,
        emptied: None,
        asked: None,
    });
    e.stats = Some(stats);
    e.read_at = now;
    e.base = base;
    e.emptied = None;
}

/// Share of a table's rows above which reading an index's entries and
/// fetching each row costs more than reading the table: an entry and its
/// row cost about three rows read in key order.
const SCAN_SHARE: f64 = 0.3;

/// Whether the rows whose column `col` equals `v` are better read by
/// scanning the table than through an index on `col`: the table's
/// statistics (as this process holds them; nothing is read) say most rows
/// hold `v`. The simple plans' index lookups ask it with each value.
pub(crate) fn scan_cheaper(
    tenant: u64,
    object: u32,
    def: &TableDef,
    col: usize,
    v: &Datum,
) -> bool {
    if !enabled() {
        return false;
    }
    let Some(st) = cached(tenant, object) else {
        load_later(tenant, object);
        return false;
    };
    let Some(c) = def.columns.get(col) else {
        return false;
    };
    let point = sprig_plan::access::KeyRange::point(vec![v.clone()]);
    st.rows >= PROBE as u64
        && st
            .share(&c.name, &c.ty, &[point])
            .is_some_and(|s| s >= SCAN_SHARE)
}

/// Reads `object`'s statistics record into the cache in the background,
/// at most once per [`RETRY`]: simple plans read nothing themselves.
fn load_later(tenant: u64, object: u32) {
    let now = Instant::now();
    {
        let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(e) = cache.get_mut(&(tenant, object)) {
            if e.stats.is_some() || e.asked.is_some_and(|t| now.duration_since(t) < RETRY) {
                return;
            }
            e.asked = Some(now);
        } else {
            cache.insert(
                (tenant, object),
                Entry {
                    stats: None,
                    read_at: now,
                    base: counters(tenant, object),
                    emptied: None,
                    asked: Some(now),
                },
            );
        }
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return;
    };
    rt.spawn(async move {
        let Ok(eng) = super::engine() else { return };
        if let Ok(Some(st)) = collect::stored(eng, tenant, object) {
            let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(e) = cache.get_mut(&(tenant, object))
                && e.stats.is_none()
            {
                e.base = base_for(tenant, object, Some(&st));
                e.stats = Some(Arc::new(st));
                e.read_at = Instant::now();
            }
        }
    });
}

/// The statistics of `object` as this process last read or collected them
/// (`information_schema` and automatic indexing read them without a
/// statement).
pub(crate) fn cached(tenant: u64, object: u32) -> Option<Arc<TableStats>> {
    CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&(tenant, object))
        .and_then(|e| e.stats.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_key(tenant: u64, object: u32) -> Vec<u8> {
        let mut k = Vec::new();
        KeyPrefix {
            tenant,
            shard: 0,
            object,
            kind: kind::PRIMARY,
            index: 0,
        }
        .encode_into(&mut k);
        k.extend_from_slice(b"\x80\x00\x00\x01");
        k
    }

    #[test]
    fn only_row_writes_count() {
        let (t, o) = (9_123_456_789, 77);
        let before = counters(t, o);
        note_write(&row_key(t, o), 1);
        note_write(&row_key(t, o), 0);
        note_write(&row_key(t, o), -1);
        note_write(&row_key(t, o), 1);
        let mut meta = row_key(t, o);
        meta[16] = kind::META;
        note_write(&meta, 1);
        note_write(b"short", 1);
        let after = counters(t, o);
        assert_eq!(after.0 - before.0, 4);
        assert_eq!(after.1 - before.1, 1);
        assert!(!stale(30_000, 2_999));
        assert!(stale(30_000, 3_000));
        assert!(!stale(100, 199));
        assert!(stale(100, 200));
        let k = record_key(t, o);
        assert_eq!(k[16], kind::META);
        assert!(k.ends_with(b"stats"));
    }
}
