//! Collecting a table's statistics.
//!
//! ```text
//! rows      one pass over the table's rows at a read view: every row
//!           counted, the primary key's leading runs counted (a key of
//!           several parts), and a uniform sample of SAMPLE rows kept
//!           (reservoir sampling: a row is decoded only when it enters)
//! indexes   one pass over each secondary index's entries: distinct values
//!           of each leading run of its parts, from the key bytes (no row
//!           read); past INDEX_CAP entries the counts are scaled
//! columns   from the sample: NULL share, distinct values, a histogram
//!           (sprig_plan::stats::ColumnSample) of DEFAULT_BUCKETS buckets,
//!           or as many as UPDATE HISTOGRAM asked; a column whose
//!           histogram was dropped keeps none, and one asked with MANUAL
//!           UPDATE keeps its histogram until it is named again
//! stored    in one small transaction of its own (super::record_key)
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::keys::typed::{self, KeyColumn};
use sprig_engine::txn::TxnOptions;
use sprig_plan::stats::{ColumnSample, ColumnStats, DEFAULT_BUCKETS, IndexStats, TableStats};

use super::super::keys;
use super::super::row::{self, Datum};
use super::super::table::TableDef;
use super::super::{Engine, Pace};

/// Rows a collection keeps for the column figures.
pub(crate) const SAMPLE: usize = 20_000;
/// Entries of one index read before its counts are scaled.
const INDEX_CAP: u64 = 4_000_000;
/// Entries read per store call.
const BATCH: usize = 1024;

/// What a statement asked of a collection besides the usual.
#[derive(Debug, Clone, Default)]
pub(crate) struct Ask {
    /// `UPDATE HISTOGRAM ON ...`: columns whose histograms are made now,
    /// with the buckets asked (0: the default) and `MANUAL UPDATE`.
    pub histograms: Vec<(String, u16, bool)>,
}

impl Ask {
    fn named(&self, column: &str) -> Option<(u16, bool)> {
        self.histograms
            .iter()
            .find(|(n, _, _)| n.eq_ignore_ascii_case(column))
            .map(|(_, b, m)| (*b, *m))
    }
}

pub(crate) fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// A small, fast generator for the sample (xorshift64*).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d) % n.max(1)
    }
}

/// Distinct values of each leading run of `parts` over keys read in key
/// order: each key's tail (after the prefix) is compared with the last
/// one's.
struct Runs {
    parts: Vec<KeyColumn>,
    last: Vec<u8>,
    ends: Vec<usize>,
    distinct: Vec<u64>,
    seen: u64,
}

impl Runs {
    fn new(parts: &[KeyColumn]) -> Self {
        Self {
            parts: parts.to_vec(),
            last: Vec::new(),
            ends: Vec::new(),
            distinct: vec![0; parts.len()],
            seen: 0,
        }
    }

    fn add(&mut self, tail: &[u8]) {
        self.seen += 1;
        let mut ends = Vec::with_capacity(self.parts.len());
        let mut rest = tail;
        for kc in &self.parts {
            match typed::decode_column(rest, kc) {
                Ok((_, r)) => rest = r,
                Err(_) => return,
            }
            ends.push(tail.len() - rest.len());
        }
        let first = (0..self.parts.len())
            .find(|&j| {
                self.ends
                    .get(j)
                    .is_none_or(|&e| self.last[..e] != tail[..ends[j]])
            })
            .unwrap_or(self.parts.len());
        for d in &mut self.distinct[first..] {
            *d += 1;
        }
        self.last.clear();
        self.last
            .extend_from_slice(&tail[..ends.last().copied().unwrap_or(0)]);
        self.ends = ends;
    }

    /// The counts, scaled to `total` entries when fewer were read.
    fn finish(self, total: u64) -> Vec<f64> {
        let seen = self.seen;
        self.distinct
            .into_iter()
            .map(|d| {
                if seen == 0 || seen >= total || d * 10 < seen {
                    d as f64
                } else {
                    d as f64 * total as f64 / seen as f64
                }
            })
            .collect()
    }
}

/// The statistics record of `object` as stored, read at a fresh view.
pub(crate) fn stored(eng: &Engine, tenant: u64, object: u32) -> Result<Option<TableStats>> {
    let mut tx = eng.begin(TxnOptions::default())?;
    let raw = tx.get(&super::record_key(tenant, object))?;
    tx.rollback();
    Ok(raw.and_then(|r| TableStats::decode(&r)))
}

/// Writes the statistics record of `object`.
async fn store(eng: &Engine, tenant: u64, object: u32, st: &TableStats) -> Result<()> {
    let key = Bytes::from(super::record_key(tenant, object));
    let value = Bytes::from(st.encode());
    let mut last = None;
    // Two collections of one table at once: the later one writes again.
    for _ in 0..3 {
        let mut tx = eng.begin(TxnOptions::default())?;
        match tx.put(key.clone(), value.clone()).await {
            Ok(()) => match tx.finish().await {
                Ok(()) => return Ok(()),
                Err(e) => last = Some(e),
            },
            Err(e) => {
                tx.rollback();
                last = Some(e);
            }
        }
    }
    Err(last.unwrap_or_else(|| SprigError::internal("statistics were not stored")))
}

/// Collects `def`'s statistics, stores them, and makes them the ones plans
/// read.
///
/// # Errors
/// The store's; the engine isn't available.
pub(crate) async fn analyze(tenant: u64, def: &TableDef, ask: &Ask) -> Result<Arc<TableStats>> {
    analyze_on(super::super::engine()?, tenant, def, ask).await
}

/// [`analyze`] on the engine `eng`.
///
/// # Errors
/// The store's.
#[allow(clippy::too_many_lines)] // the passes in order: rows, indexes, columns
pub(crate) async fn analyze_on(
    eng: &Engine,
    tenant: u64,
    def: &TableDef,
    ask: &Ask,
) -> Result<Arc<TableStats>> {
    let sp = eng.space(tenant, def).await?;
    let object = sp.object;
    let base = super::counters(tenant, object);
    let prev = stored(eng, tenant, object)?;
    let ff = sp.first_format;

    // The rows.
    let mut tx = eng.begin(TxnOptions::default())?;
    let p = sp.rows();
    let end = keys::prefix_end(&p);
    let mut lo = p.clone();
    let mut rows = 0u64;
    let mut sample: Vec<Vec<Datum>> = Vec::new();
    let mut rng = Rng::new(now_us() ^ (u64::from(object) << 20) ^ tenant);
    let mut pk_runs = (!def.hidden && def.pk.len() > 1).then(|| Runs::new(def.pk_key(ff)));
    let mut pace = Pace::default();
    loop {
        let batch = tx.scan(&lo, &end, false, BATCH)?;
        for (k, v) in &batch {
            rows += 1;
            if let Some(r) = pk_runs.as_mut()
                && let Some(tail) = k.get(p.len()..)
            {
                r.add(tail);
            }
            if sample.len() < SAMPLE {
                sample.push(row::decode(def, &sp, k, v)?);
            } else {
                let j = rng.below(rows);
                if let Some(slot) = sample.get_mut(usize::try_from(j).unwrap_or(usize::MAX)) {
                    *slot = row::decode(def, &sp, k, v)?;
                }
            }
        }
        pace.add(batch.len()).await;
        match batch.last() {
            Some((k, _)) if batch.len() == BATCH => lo = keys::prefix_end(k),
            _ => break,
        }
    }

    // The indexes.
    let mut indexes = Vec::with_capacity(def.indexes.len() + 1);
    if !def.hidden {
        indexes.push(IndexStats {
            name: super::PRIMARY.to_owned(),
            distinct: match pk_runs {
                Some(r) => r.finish(rows),
                None => vec![rows as f64],
            },
        });
    }
    for (ix, x) in def.indexes.iter().enumerate() {
        let prefix = sp.index(def, ix);
        let end = keys::prefix_end(&prefix);
        let mut lo = prefix.clone();
        let mut runs = Runs::new(def.index_key(ix, ff));
        loop {
            let batch = tx.scan(&lo, &end, false, BATCH)?;
            for (k, _) in &batch {
                if let Some(tail) = k.get(prefix.len()..) {
                    runs.add(tail);
                }
            }
            pace.add(batch.len()).await;
            match batch.last() {
                Some((k, _)) if batch.len() == BATCH && runs.seen < INDEX_CAP => {
                    lo = keys::prefix_end(k);
                }
                _ => break,
            }
        }
        indexes.push(IndexStats {
            name: x.name.clone(),
            distinct: runs.finish(rows),
        });
    }
    tx.rollback();

    // The columns.
    let now = now_us();
    let mut columns = Vec::with_capacity(def.columns.len());
    for (i, c) in def.columns.iter().enumerate() {
        let before = prev.as_ref().and_then(|s| s.column(&c.name));
        let named = ask.named(&c.name);
        let dropped = named.is_none() && before.is_some_and(|b| b.dropped);
        let buckets = match named {
            Some((b, _)) => b,
            None => before.map_or(0, |b| b.buckets),
        };
        let manual = match named {
            Some((_, m)) => m,
            None => before.is_some_and(|b| b.manual),
        };
        let mut s = ColumnSample::new(&c.ty);
        for r in &sample {
            s.add(r.get(i).unwrap_or(&Datum::Null));
        }
        let want = if buckets == 0 {
            DEFAULT_BUCKETS
        } else {
            usize::from(buckets)
        };
        let mut cs: ColumnStats = s.finish(&c.name, rows, want, !dropped);
        cs.buckets = buckets;
        cs.manual = manual;
        cs.dropped = dropped;
        cs.requested = named.is_some() || before.is_some_and(|b| b.requested && !b.dropped);
        cs.at_us = now;
        // A histogram kept by hand stays as it was until it is named.
        if named.is_none()
            && manual
            && let Some(b) = before
            && b.histogram.is_some()
        {
            cs.histogram.clone_from(&b.histogram);
            cs.at_us = b.at_us;
        }
        columns.push(cs);
    }
    let st = TableStats {
        rows,
        sampled: sample.len() as u64,
        version: def.version,
        at_us: now,
        columns,
        indexes,
    };
    store(eng, tenant, object, &st).await?;
    let st = Arc::new(st);
    super::collected(tenant, object, Arc::clone(&st), base);
    Ok(st)
}

/// `DROP HISTOGRAM ON ...`: the histograms of `names` go (and stay gone
/// until asked for again). Per name, whether it had one asked for.
///
/// # Errors
/// The store's.
pub(crate) async fn drop_histograms(
    tenant: u64,
    def: &TableDef,
    names: &[String],
) -> Result<Vec<bool>> {
    let eng = super::super::engine()?;
    let sp = eng.space(tenant, def).await?;
    let object = sp.object;
    let Some(mut st) = stored(eng, tenant, object)? else {
        return Ok(vec![false; names.len()]);
    };
    let mut had = Vec::with_capacity(names.len());
    for n in names {
        let c = st
            .columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(n));
        match c {
            Some(c) => {
                had.push(c.requested && !c.dropped);
                c.histogram = None;
                c.dropped = true;
                c.requested = false;
                c.manual = false;
                c.buckets = 0;
            }
            None => had.push(false),
        }
    }
    store(eng, tenant, object, &st).await?;
    let base = super::counters(tenant, object);
    super::collected(tenant, object, Arc::new(st), base);
    Ok(had)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_engine::row::ColumnType;

    #[test]
    fn leading_runs_count_distinct_prefixes() {
        let kc = KeyColumn::new(ColumnType::int(false));
        let key = |a: i64, b: i64| {
            let mut out = Vec::new();
            typed::encode_column(&mut out, &kc, &Datum::Int(a)).unwrap_or_default();
            typed::encode_column(&mut out, &kc, &Datum::Int(b)).unwrap_or_default();
            out
        };
        let mut r = Runs::new(&[kc, kc]);
        for (a, b) in [(1, 1), (1, 2), (1, 2), (2, 1), (3, 1), (3, 5)] {
            r.add(&key(a, b));
        }
        assert_eq!(r.distinct, vec![3, 5]);
        assert_eq!(r.finish(6), vec![3.0, 5.0]);
    }

    #[test]
    fn the_sample_index_stays_in_range() {
        let mut g = Rng::new(42);
        for n in 1..1000 {
            assert!(g.below(n) < n);
        }
    }
}
