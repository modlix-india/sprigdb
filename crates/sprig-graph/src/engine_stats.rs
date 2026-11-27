//! Statistics of a graph on the engine, for the pattern planner's
//! estimates (never for answers).
//!
//! - **Counters:** nodes per label set, and relationships per (source
//!   label set, type, destination label set). Labels, types and average
//!   degrees are sums over them.
//! - **Value histograms:** per online index, over its entries (exact
//!   counts); per (label, property key), over a sample of the label's
//!   nodes, with the share of nodes that have the key. Buckets are
//!   equi-height over the ordered keys of [`crate::ordkey`], so they serve
//!   equality and ranges (an equality-only index's histogram serves
//!   equality only).
//!
//! A pass ([`EngineGraph::analyze`]) reads the label-scan entries, the
//! out-adjacency (with one node read per source to learn its label set),
//! every index and a stride sample of each label set's nodes, and stores
//! the result as one record (`g | M | 4`). Between passes, each graph
//! transaction counts what it writes ([`Delta`]) and a commit adds that to
//! the counters in memory; the histograms wait for the next pass. A pass
//! runs in the background when the changes since the last one reach a
//! tenth of the graph's elements (at least 1,000), and when a non-empty
//! graph opens without statistics. `SPRIGDB_GRAPH_STATS=off` turns use and
//! passes off.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sprig_common::Result;
use sprig_engine::keys::{KeyRange, kind};
use sprig_engine::txn::{Dir as ScanDir, ReadMode, Txn};

use super::{EngineGraph, k, poisoned};
use crate::ast::{Dir, Entity};
use crate::engine_keys::{OUT, adj_parts, be32, be64};
use crate::ordkey::{self, RangePred};
use crate::store::PropPred;

/// Buckets of a histogram.
const BUCKETS: usize = 64;
/// Nodes sampled per label set for property histograms.
const SAMPLE_PER_SET: u64 = 2000;
/// Keys read per engine call during a pass.
const PAGE: usize = 4096;
/// Distinct keys held while a histogram is built, before neighbours merge.
const BUILD_CAP: usize = 65_536;
/// The `M | n` family of the statistics record.
const STATS_FAMILY: u16 = 4;
/// The `M | n` family of committed transactions' counter deltas.
const DELTA_FAMILY: u16 = 5;
/// Stored deltas past which opening a graph folds them into the record.
pub const FOLD_AT: usize = 1000;
/// Deltas folded or deleted per transaction.
const FOLD_PAGE: usize = 1000;

/// Whether statistics are kept and used (`SPRIGDB_GRAPH_STATS`, on unless
/// `off`).
#[must_use]
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("SPRIGDB_GRAPH_STATS").map_or(true, |v| !v.eq_ignore_ascii_case("off"))
    })
}

/// One histogram bucket: values up to `upper` (inclusive), after the
/// previous bucket's.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Bucket {
    /// The highest key in the bucket (hex in the record).
    #[serde(with = "hex_bytes")]
    pub upper: Vec<u8>,
    /// Rows (entries, or sampled nodes weighted) in the bucket.
    pub rows: f64,
    /// Distinct keys in the bucket.
    pub distinct: f64,
}

/// An equi-height histogram over ordered keys.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Hist {
    /// Buckets in key order.
    pub buckets: Vec<Bucket>,
    /// Whether the keys are [`crate::ordkey`] keys (ranges can be
    /// estimated).
    pub ordered: bool,
}

impl Hist {
    /// Rows in all.
    #[must_use]
    pub fn rows(&self) -> f64 {
        self.buckets.iter().map(|b| b.rows).sum()
    }

    /// Distinct keys in all.
    #[must_use]
    pub fn distinct(&self) -> f64 {
        self.buckets.iter().map(|b| b.distinct).sum()
    }

    /// The share of rows whose key is `key`.
    #[must_use]
    pub fn eq_share(&self, key: Option<&[u8]>) -> f64 {
        let total = self.rows();
        if total <= 0.0 {
            return 0.0;
        }
        let per_value = |b: &Bucket| b.rows / b.distinct.max(1.0);
        match key.and_then(|k| self.buckets.iter().find(|b| b.upper.as_slice() >= k)) {
            Some(b) if b.upper.as_slice() == key.unwrap_or_default() && b.distinct <= 1.0 => {
                b.rows / total
            }
            Some(b) => per_value(b) / total,
            // Past the last bucket (or no key): an average value.
            None => (total / self.distinct().max(1.0)) / total * 0.5,
        }
    }

    /// The share of rows with keys in `[start, end)`.
    #[must_use]
    pub fn range_share(&self, start: &[u8], end: &[u8]) -> f64 {
        let total = self.rows();
        if total <= 0.0 {
            return 0.0;
        }
        let mut rows = 0.0;
        let mut lower: &[u8] = &[];
        for b in &self.buckets {
            let up = b.upper.as_slice();
            let all_in = lower >= start && up < end;
            let none = up < start || lower >= end;
            if all_in {
                rows += b.rows;
            } else if !none {
                // A bucket the range cuts: half of it (one value for a
                // bucket of one).
                rows += if b.distinct <= 1.0 {
                    if up >= start && up < end { b.rows } else { 0.0 }
                } else {
                    b.rows / 2.0
                };
            }
            lower = up;
        }
        (rows / total).clamp(0.0, 1.0)
    }
}

/// A histogram under construction: keys arrive in order.
#[derive(Default)]
struct Builder {
    parts: Vec<Bucket>,
}

impl Builder {
    fn push(&mut self, key: &[u8], rows: f64) {
        if let Some(last) = self.parts.last_mut()
            && last.upper == key
        {
            last.rows += rows;
            return;
        }
        self.parts.push(Bucket {
            upper: key.to_vec(),
            rows,
            distinct: 1.0,
        });
        if self.parts.len() >= BUILD_CAP {
            // Merge neighbours: keeps the order, halves the memory.
            let old = std::mem::take(&mut self.parts);
            let mut it = old.into_iter();
            while let Some(a) = it.next() {
                match it.next() {
                    Some(b) => self.parts.push(Bucket {
                        upper: b.upper,
                        rows: a.rows + b.rows,
                        distinct: a.distinct + b.distinct,
                    }),
                    None => self.parts.push(a),
                }
            }
        }
    }

    fn finish(self, ordered: bool) -> Hist {
        let total: f64 = self.parts.iter().map(|b| b.rows).sum();
        #[allow(clippy::cast_precision_loss)]
        let height = total / BUCKETS as f64;
        let mut out: Vec<Bucket> = Vec::new();
        let mut cur: Option<Bucket> = None;
        for p in self.parts {
            // A heavy key gets a bucket of its own.
            if p.rows >= height {
                if let Some(c) = cur.take() {
                    out.push(c);
                }
                out.push(p);
                continue;
            }
            let c = cur.get_or_insert_with(Bucket::default);
            c.upper = p.upper;
            c.rows += p.rows;
            c.distinct += p.distinct;
            if c.rows >= height {
                out.extend(cur.take());
            }
        }
        out.extend(cur);
        Hist {
            buckets: out,
            ordered,
        }
    }
}

/// What the statistics know about one property of one label.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct PropStat {
    /// The share of the label's nodes that have the property.
    pub share: f64,
    /// Values of the sampled nodes that have it.
    pub hist: Hist,
}

/// A graph's statistics.
#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    /// Nodes per label set.
    pub sets: HashMap<u32, f64>,
    /// Relationships per (source label set, type, destination label set).
    pub triples: HashMap<(u32, u32, u32), f64>,
    /// Per index id: its entries.
    pub indexes: HashMap<u32, Hist>,
    /// Per (label, property key): sampled values.
    pub props: HashMap<(u32, String), PropStat>,
    /// Nodes and relationships when the last pass ran.
    pub elements_at_pass: f64,
    /// The counters have followed every commit since the graph was new
    /// (passes refresh only the histograms then).
    pub exact: bool,
}

/// A (label, key)'s sampled nodes: their weight, and each value's key
/// with its weight.
type Sampled = (f64, Vec<(Vec<u8>, f64)>);

/// The stored form.
#[derive(Serialize, Deserialize, Default)]
struct Record {
    sets: Vec<(u32, f64)>,
    triples: Vec<(u32, u32, u32, f64)>,
    indexes: Vec<(u32, Hist)>,
    props: Vec<(u32, String, PropStat)>,
    elements: f64,
    /// Elements changed since the pass, from deltas folded into the record.
    #[serde(default)]
    changes: u64,
    /// [`GraphStats::exact`].
    #[serde(default)]
    exact: bool,
}

impl GraphStats {
    fn to_record(&self) -> Record {
        let mut r = Record {
            sets: self.sets.iter().map(|(a, b)| (*a, *b)).collect(),
            triples: self
                .triples
                .iter()
                .map(|((a, b, c), n)| (*a, *b, *c, *n))
                .collect(),
            indexes: self.indexes.iter().map(|(a, h)| (*a, h.clone())).collect(),
            props: self
                .props
                .iter()
                .map(|((l, k2), p)| (*l, k2.clone(), p.clone()))
                .collect(),
            elements: self.elements_at_pass,
            changes: 0,
            exact: self.exact,
        };
        r.sets.sort_by_key(|x| x.0);
        r.triples.sort_by_key(|x| (x.0, x.1, x.2));
        r.indexes.sort_by_key(|x| x.0);
        r.props.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        r
    }

    fn from_record(r: Record) -> Self {
        Self {
            sets: r.sets.into_iter().collect(),
            triples: r
                .triples
                .into_iter()
                .map(|(a, b, c, n)| ((a, b, c), n))
                .collect(),
            indexes: r.indexes.into_iter().collect(),
            props: r.props.into_iter().map(|(l, k2, p)| ((l, k2), p)).collect(),
            elements_at_pass: r.elements,
            exact: r.exact,
        }
    }

    /// Nodes and relationships the counters hold.
    #[must_use]
    pub fn elements(&self) -> f64 {
        self.sets.values().sum::<f64>() + self.triples.values().sum::<f64>()
    }

    #[allow(clippy::cast_precision_loss)] // counts far below 2^52
    fn add(&mut self, d: &Delta) {
        for (s, n) in &d.sets {
            let e = self.sets.entry(*s).or_default();
            *e = (*e + *n as f64).max(0.0);
        }
        for (t, n) in &d.triples {
            let e = self.triples.entry(*t).or_default();
            *e = (*e + *n as f64).max(0.0);
        }
    }
}

/// What one transaction changed in the counters.
#[derive(Debug, Default, Clone)]
pub struct Delta {
    /// Nodes per label set.
    pub sets: HashMap<u32, i64>,
    /// Relationships per (source label set, type, destination label set).
    pub triples: HashMap<(u32, u32, u32), i64>,
    /// Elements written (created, changed or deleted).
    pub changes: u64,
}

impl Delta {
    /// A node of label set `ls` came (`n` 1) or went (-1).
    pub fn node(&mut self, ls: u32, n: i64) {
        *self.sets.entry(ls).or_default() += n;
        self.changes += 1;
    }

    /// A relationship came or went.
    pub fn rel(&mut self, src_ls: u32, rtype: u32, dst_ls: u32, n: i64) {
        *self.triples.entry((src_ls, rtype, dst_ls)).or_default() += n;
        self.changes += 1;
    }

    /// Whether nothing changed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changes == 0
    }

    /// The stored form: counts of label sets and triples, then each
    /// `(label set, n)` and `(source set, type, destination set, n)`, then
    /// the changes; integers big-endian.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.sets.len() * 12 + self.triples.len() * 20);
        out.extend_from_slice(&u32::try_from(self.sets.len()).unwrap_or(0).to_be_bytes());
        out.extend_from_slice(&u32::try_from(self.triples.len()).unwrap_or(0).to_be_bytes());
        for (s, n) in &self.sets {
            out.extend_from_slice(&s.to_be_bytes());
            out.extend_from_slice(&n.to_be_bytes());
        }
        for ((a, t, b), n) in &self.triples {
            out.extend_from_slice(&a.to_be_bytes());
            out.extend_from_slice(&t.to_be_bytes());
            out.extend_from_slice(&b.to_be_bytes());
            out.extend_from_slice(&n.to_be_bytes());
        }
        out.extend_from_slice(&self.changes.to_be_bytes());
        out
    }

    /// Reads [`Delta::encode`]'s form (`None`: not one).
    #[must_use]
    pub fn decode(b: &[u8]) -> Option<Self> {
        let mut at = 0usize;
        let mut take = |n: usize| -> Option<&[u8]> {
            let s = b.get(at..at + n)?;
            at += n;
            Some(s)
        };
        let u32_of = |s: &[u8]| s.try_into().ok().map(u32::from_be_bytes);
        let i64_of = |s: &[u8]| s.try_into().ok().map(i64::from_be_bytes);
        let ns = u32_of(take(4)?)?;
        let nt = u32_of(take(4)?)?;
        let mut d = Delta::default();
        for _ in 0..ns {
            let s = u32_of(take(4)?)?;
            let n = i64_of(take(8)?)?;
            *d.sets.entry(s).or_default() += n;
        }
        for _ in 0..nt {
            let a = u32_of(take(4)?)?;
            let t = u32_of(take(4)?)?;
            let c = u32_of(take(4)?)?;
            let n = i64_of(take(8)?)?;
            *d.triples.entry((a, t, c)).or_default() += n;
        }
        d.changes = take(8)?.try_into().ok().map(u64::from_be_bytes)?;
        Some(d)
    }

    /// Adds `o` to this.
    pub fn merge(&mut self, o: &Delta) {
        for (s, n) in &o.sets {
            *self.sets.entry(*s).or_default() += n;
        }
        for (t, n) in &o.triples {
            *self.triples.entry(*t).or_default() += n;
        }
        self.changes += o.changes;
    }
}

mod hex_bytes {
    use std::fmt::Write as _;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(b: &[u8], s: S) -> Result<S::Ok, S::Error> {
        let mut t = String::with_capacity(b.len() * 2);
        for x in b {
            let _ = write!(t, "{x:02x}");
        }
        s.serialize_str(&t)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let t = String::deserialize(d)?;
        (0..t.len() / 2)
            .map(|i| u8::from_str_radix(&t[2 * i..2 * i + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .map_err(serde::de::Error::custom)
    }
}

/// Reads a prefix of keys page by page, each page in a fresh transaction,
/// calling `f` with each key and value.
fn each_key(
    g: &EngineGraph,
    prefix: &[u8],
    mut f: impl FnMut(&mut Txn, &[u8], &Bytes) -> Result<()>,
) -> Result<()> {
    let end = k::end_of(prefix);
    let mut from = prefix.to_vec();
    loop {
        let mut t = Txn::begin_on(Arc::clone(&g.path), g.opts);
        let rows = match t.scan(
            &KeyRange::new(from.as_slice(), end.as_slice()),
            ScanDir::Forward,
            PAGE,
            ReadMode::Snapshot,
        ) {
            Ok(r) => r,
            Err(e) => {
                t.rollback();
                return Err(e);
            }
        };
        let r: Result<()> = rows.iter().try_for_each(|(key, v)| f(&mut t, key, v));
        t.rollback();
        r?;
        let Some((last, _)) = rows.last() else {
            return Ok(());
        };
        if rows.len() < PAGE {
            return Ok(());
        }
        from.clone_from(last);
        from.push(0);
    }
}

impl EngineGraph {
    /// The statistics key.
    fn stats_key(&self) -> Vec<u8> {
        self.space.prefix(0, kind::META, STATS_FAMILY)
    }

    /// Loads the stored statistics (at open), with the counters'
    /// deltas committed since added, and the changes since the last pass.
    /// More than [`FOLD_AT`] stored deltas are folded into the record.
    pub(super) fn load_stats(&self) -> Result<Option<(GraphStats, u64)>> {
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        let v = t.read_current(&self.stats_key());
        t.rollback();
        let Some(rec) = v?.and_then(|b| serde_json::from_slice::<Record>(&b).ok()) else {
            return Ok(None);
        };
        let mut changes = rec.changes;
        let mut st = GraphStats::from_record(rec);
        let deltas = self.stored_deltas(usize::MAX)?;
        let mut sum = Delta::default();
        for (_, d) in &deltas {
            sum.merge(d);
        }
        self.bump_delta_seq(deltas.last().map(|(k, _)| k.as_slice()));
        st.add(&sum);
        changes += sum.changes;
        if deltas.len() > FOLD_AT {
            self.fold_deltas()?;
        }
        Ok(Some((st, changes)))
    }

    /// Stores empty statistics for a new graph (its counters are exact
    /// from the start).
    pub(super) fn store_empty_stats(&self) -> Result<()> {
        let body = serde_json::to_vec(&Self::empty_stats().to_record())
            .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        if let Err(e) = t.put(self.stats_key(), body) {
            t.rollback();
            return Err(e);
        }
        t.commit()?;
        Ok(())
    }

    /// A new graph's statistics: empty, and exact from now on.
    pub(super) fn empty_stats() -> GraphStats {
        GraphStats {
            exact: true,
            ..GraphStats::default()
        }
    }

    /// The prefix of the stored deltas.
    fn delta_prefix(&self) -> Vec<u8> {
        self.space.prefix(0, kind::META, DELTA_FAMILY)
    }

    /// The first `limit` stored deltas, in sequence order, with their keys.
    pub(super) fn stored_deltas(&self, limit: usize) -> Result<Vec<(Vec<u8>, Delta)>> {
        let p = self.delta_prefix();
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        let rows = t.scan_current(&KeyRange::prefix(&p), ScanDir::Forward, limit);
        t.rollback();
        Ok(rows?
            .into_iter()
            .filter_map(|(k2, v)| Delta::decode(&v).map(|d| (k2.to_vec(), d)))
            .collect())
    }

    /// Keeps the delta sequence above the stored key `last`.
    fn bump_delta_seq(&self, last: Option<&[u8]>) {
        let p = self.delta_prefix().len();
        if let Some(n) = last.and_then(|k2| be64(k2, p).ok()) {
            self.delta_seq.fetch_max(n + 1, Ordering::AcqRel);
        }
    }

    /// Writes `d` as a stored delta in `t` (the committing transaction),
    /// when statistics are on and it changed something.
    ///
    /// # Errors
    /// The engine's.
    pub(super) fn stage_delta(&self, t: &mut Txn, d: &Delta) -> Result<()> {
        if !enabled() || d.is_empty() || self.dropped.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut key = self.delta_prefix();
        key.extend_from_slice(&self.delta_seq.fetch_add(1, Ordering::AcqRel).to_be_bytes());
        t.put(key, d.encode())
    }

    /// Folds the stored deltas into the statistics record, a page per
    /// transaction: each adds the page to the record as stored and deletes
    /// the page's deltas, so the result is exact whatever commits
    /// meanwhile. Waits for (and then holds off) a pass.
    ///
    /// # Errors
    /// The engine's.
    pub fn fold_deltas(&self) -> Result<()> {
        while self.collecting.swap(true, Ordering::AcqRel) {
            std::thread::yield_now();
        }
        let r = self.fold_pages();
        self.collecting.store(false, Ordering::Release);
        r
    }

    fn fold_pages(&self) -> Result<()> {
        loop {
            let page = self.stored_deltas(FOLD_PAGE)?;
            if page.is_empty() {
                return Ok(());
            }
            let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
            let r: Result<()> = (|| {
                let Some(mut rec) = t
                    .read_current(&self.stats_key())?
                    .and_then(|b| serde_json::from_slice::<Record>(&b).ok())
                else {
                    // No record to fold into: the next pass counts them.
                    return Ok(());
                };
                let mut sum = Delta::default();
                for (key, d) in &page {
                    sum.merge(d);
                    t.delete(key.clone())?;
                }
                rec.changes += sum.changes;
                let changes = rec.changes;
                let mut st = GraphStats::from_record(rec);
                st.add(&sum);
                let mut rec = st.to_record();
                rec.changes = changes;
                let body = serde_json::to_vec(&rec)
                    .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
                t.put(self.stats_key(), body)?;
                Ok(())
            })();
            if let Err(e) = r {
                t.rollback();
                return Err(e);
            }
            t.commit()?;
            if page.len() < FOLD_PAGE {
                return Ok(());
            }
        }
    }

    /// Whether the graph has no node.
    pub(super) fn is_empty(&self) -> Result<bool> {
        let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
        let mut empty = true;
        for sh in self.all_shards() {
            let p = self.space.prefix(sh, kind::PRIMARY, k::NODE);
            if !t
                .scan_current(&KeyRange::prefix(&p), ScanDir::Forward, 1)?
                .is_empty()
            {
                empty = false;
                break;
            }
        }
        t.rollback();
        Ok(empty)
    }

    /// One statistics pass over the graph (see the module notes).
    ///
    /// # Errors
    /// The engine's.
    pub fn collect_stats(&self) -> Result<GraphStats> {
        let mut st = GraphStats::default();
        let label_scan_len = self.space.prefix(1, kind::LABELS, k::LABEL_SCAN).len();
        for sh in self.all_shards() {
            // Label sets.
            let p = self.space.prefix(sh, kind::LABELS, k::LABEL_SCAN);
            each_key(self, &p, |_, key, _| {
                let ls = be32(key, label_scan_len)?;
                *st.sets.entry(ls).or_default() += 1.0;
                Ok(())
            })?;
            // Relationships, from the out-adjacency: the source's label
            // set comes from its node, once per source.
            let p = self.space.prefix(sh, kind::ADJACENCY, OUT);
            let mut last: Option<(u64, u32)> = None;
            each_key(self, &p, |t, key, v| {
                let (src, etype, _, _) = adj_parts(key)?;
                let src_ls = match last {
                    Some((s, ls)) if s == src => ls,
                    _ => {
                        let ls = match t.get_snapshot(&self.space.node(sh, src))? {
                            Some(nv) => be32(&nv, 0)?,
                            None => 0,
                        };
                        last = Some((src, ls));
                        ls
                    }
                };
                let dst_ls = be32(v, 0)?;
                *st.triples.entry((src_ls, etype, dst_ls)).or_default() += 1.0;
                Ok(())
            })?;
        }
        // Indexes: their entries in key order (shard by shard; a graph on
        // several shards merges them in a map first).
        for def in self.defs() {
            if def.state != "online" {
                continue;
            }
            let n = u16::try_from(def.id).unwrap_or(u16::MAX);
            let ordered = self.is_ordered(def.id);
            let mut prefixes = Vec::new();
            if def.unique {
                prefixes.push((self.space.prefix(0, kind::UNIQUE, n), false));
            } else {
                for sh in self.all_shards() {
                    prefixes.push((self.space.prefix(sh, kind::SECONDARY, n), true));
                }
            }
            let mut b = Builder::default();
            if prefixes.len() == 1 {
                let (p, with_id) = &prefixes[0];
                each_key(self, p, |_, key, _| {
                    let end = if *with_id { key.len() - 8 } else { key.len() };
                    b.push(&key[p.len()..end], 1.0);
                    Ok(())
                })?;
            } else {
                let mut m: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
                for (p, with_id) in &prefixes {
                    each_key(self, p, |_, key, _| {
                        let end = if *with_id { key.len() - 8 } else { key.len() };
                        *m.entry(key[p.len()..end].to_vec()).or_default() += 1.0;
                        Ok(())
                    })?;
                }
                for (key, rows) in m {
                    b.push(&key, rows);
                }
            }
            st.indexes.insert(def.id, b.finish(ordered));
        }
        // Properties: a stride sample of each label set's nodes.
        let sets: Vec<(u32, f64)> = st.sets.iter().map(|(a, b)| (*a, *b)).collect();
        let mut samples: HashMap<(u32, String), Sampled> = HashMap::new();
        let mut label_rows: HashMap<u32, f64> = HashMap::new();
        for (ls, count) in sets {
            let labels = self.labels_of(ls);
            if labels.is_empty() || count <= 0.0 {
                continue;
            }
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let step = ((count as u64) / SAMPLE_PER_SET).max(1);
            let mut seen = 0u64;
            let mut picked: Vec<(u32, u64)> = Vec::new();
            for sh in self.all_shards() {
                let p = self.space.label_prefix(sh, ls);
                each_key(self, &p, |_, key, _| {
                    if seen.is_multiple_of(step) {
                        picked.push((sh, be64(key, key.len() - 8)?));
                    }
                    seen += 1;
                    Ok(())
                })?;
            }
            if picked.is_empty() {
                continue;
            }
            for l in &labels {
                *label_rows.entry(*l).or_default() += count;
            }
            // Each sampled node stands for this many of its label set.
            #[allow(clippy::cast_precision_loss)]
            let weight = count / picked.len() as f64;
            let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
            let r: Result<()> = picked.iter().try_for_each(|(sh, id)| {
                let Some(v) = t.get_snapshot(&self.space.node(*sh, *id))? else {
                    return Ok(());
                };
                let (_, rec) = k::split_node(&v)?;
                let (props, types) = self.decode(rec, None)?;
                let Some(obj) = props.as_object() else {
                    return Ok(());
                };
                for key in obj.keys() {
                    let Some(val) = ordkey::stored_value(&props, types.as_ref(), key) else {
                        continue;
                    };
                    let okey = ordkey::value_key(&val);
                    for l in &labels {
                        let e = samples.entry((*l, key.clone())).or_default();
                        e.0 += weight;
                        if let Some(kk) = &okey {
                            e.1.push((kk.clone(), weight));
                        }
                    }
                }
                Ok(())
            });
            t.rollback();
            r?;
        }
        for ((l, key), (present, mut vals)) in samples {
            let total = label_rows.get(&l).copied().unwrap_or(0.0);
            vals.sort_by(|a, b| a.0.cmp(&b.0));
            let mut b = Builder::default();
            for (kk, w) in &vals {
                b.push(kk, *w);
            }
            let mut hist = b.finish(true);
            // A sample that saw (almost) every value once: the population
            // has about as many distinct values as rows.
            let n = vals.len();
            let mut d = 0usize;
            for (i, (kk, _)) in vals.iter().enumerate() {
                if i == 0 || vals[i - 1].0 != *kk {
                    d += 1;
                }
            }
            #[allow(clippy::cast_precision_loss)]
            if n > 0 && d as f64 >= 0.9 * n as f64 && present > n as f64 {
                let scale = present / n as f64;
                for bk in &mut hist.buckets {
                    bk.distinct *= scale;
                }
            }
            st.props.insert(
                (l, key),
                PropStat {
                    share: if total > 0.0 {
                        (present / total).min(1.0)
                    } else {
                        0.0
                    },
                    hist,
                },
            );
        }
        st.elements_at_pass = st.elements();
        Ok(st)
    }

    /// Runs a statistics pass now and installs it (`CALL
    /// sprig.graph.analyze()`).
    ///
    /// # Errors
    /// The engine's.
    pub fn analyze(&self) -> Result<()> {
        while self.collecting.swap(true, Ordering::AcqRel) {
            std::thread::yield_now();
        }
        let r = self.analyze_held();
        self.collecting.store(false, Ordering::Release);
        r
    }

    /// [`EngineGraph::analyze`] with the pass flag held. Exact counters
    /// ([`GraphStats::exact`]) stay as the commits keep them: the pass
    /// refreshes the histograms, and the stored deltas are folded into the
    /// record. Otherwise the pass's counts replace the counters and the
    /// deltas committed before it started are deleted (a delta committed
    /// while it runs may be counted twice until the next pass).
    fn analyze_held(&self) -> Result<()> {
        let before = self.changes.load(Ordering::Acquire);
        let exact = self
            .stats
            .read()
            .unwrap_or_else(poisoned)
            .as_ref()
            .is_some_and(|s| s.exact);
        let seen = if exact {
            Vec::new()
        } else {
            self.stored_deltas(usize::MAX)?
        };
        let mut st = self.collect_stats()?;
        if !self.dropped.load(Ordering::Acquire) {
            let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
            let r: Result<()> = (|| {
                let mut rec = st.to_record();
                if exact {
                    // The counters as stored (the deltas add to them).
                    if let Some(old) = t
                        .read_current(&self.stats_key())?
                        .and_then(|b| serde_json::from_slice::<Record>(&b).ok())
                    {
                        rec.sets = old.sets;
                        rec.triples = old.triples;
                    }
                    rec.exact = true;
                }
                let body = serde_json::to_vec(&rec)
                    .map_err(|e| sprig_common::SprigError::internal(e.to_string()))?;
                t.put(self.stats_key(), body)?;
                for (key, _) in seen.iter().take(FOLD_PAGE) {
                    t.delete(key.clone())?;
                }
                Ok(())
            })();
            if let Err(e) = r {
                t.rollback();
                return Err(e);
            }
            t.commit()?;
            for page in seen.chunks(FOLD_PAGE).skip(1) {
                let mut t = Txn::begin_on(Arc::clone(&self.path), self.opts);
                for (key, _) in page {
                    if let Err(e) = t.delete(key.clone()) {
                        t.rollback();
                        return Err(e);
                    }
                }
                t.commit()?;
            }
            if exact {
                self.fold_pages()?;
            }
        }
        {
            let mut s = self.stats.write().unwrap_or_else(poisoned);
            if exact && let Some(cur) = s.as_ref() {
                st.sets.clone_from(&cur.sets);
                st.triples.clone_from(&cur.triples);
                st.exact = true;
            }
            *s = Some(st);
        }
        self.changes.fetch_sub(before, Ordering::AcqRel);
        Ok(())
    }

    /// Adds a committed transaction's changes, and starts a pass in the
    /// background when enough changed.
    pub(super) fn apply_delta(&self, d: &Delta) {
        if !enabled() || d.is_empty() {
            return;
        }
        let changes = self.changes.fetch_add(d.changes, Ordering::AcqRel) + d.changes;
        let elements = {
            let mut s = self.stats.write().unwrap_or_else(poisoned);
            match s.as_mut() {
                Some(st) => {
                    st.add(d);
                    Some(st.elements_at_pass)
                }
                None => None,
            }
        };
        #[allow(clippy::cast_precision_loss)]
        let due = match elements {
            Some(e) => changes as f64 >= (e / 10.0).max(1000.0),
            None => changes >= 1000,
        };
        if due {
            self.analyze_in_background();
        }
    }

    /// Starts a pass on another thread unless one is running.
    pub(super) fn analyze_in_background(&self) {
        if self.collecting.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(me) = self.me.upgrade() else {
            self.collecting.store(false, Ordering::Release);
            return;
        };
        let job = move || {
            if let Err(e) = me.analyze_held() {
                tracing::warn!(error = %e, graph = me.graph_id, "graph statistics pass failed");
            }
            me.collecting.store(false, Ordering::Release);
        };
        match tokio::runtime::Handle::try_current() {
            Ok(h) => {
                h.spawn_blocking(job);
            }
            Err(_) => {
                std::thread::spawn(job);
            }
        }
    }

    /// The statistics now (`None`: none yet, or switched off).
    #[must_use]
    pub fn stats(&self) -> Option<GraphStats> {
        if !enabled() {
            return None;
        }
        self.stats.read().unwrap_or_else(poisoned).clone()
    }

    fn with_stats<T>(&self, f: impl FnOnce(&GraphStats) -> Option<T>) -> Option<T> {
        if !enabled() {
            return None;
        }
        let s = self.stats.read().unwrap_or_else(poisoned);
        s.as_ref().and_then(f)
    }

    /// Estimated nodes with `label` (`None`: all nodes).
    pub(super) fn est_nodes(&self, label: Option<u32>) -> Option<f64> {
        let d = self.dict();
        self.with_stats(|st| {
            Some(
                st.sets
                    .iter()
                    .filter(|(s, _)| label.is_none_or(|l| d.set_has(**s, l)))
                    .map(|(_, n)| *n)
                    .sum(),
            )
        })
    }

    /// Estimated relationships of `types` (empty: any) between nodes with
    /// `near` and `far` labels (`None`: any), going `dir` from the near
    /// side.
    pub(super) fn est_rels(
        &self,
        near: Option<u32>,
        types: &[u32],
        dir: Dir,
        far: Option<u32>,
    ) -> Option<f64> {
        let d = self.dict();
        let has = |set: u32, l: Option<u32>| l.is_none_or(|l| d.set_has(set, l));
        self.with_stats(|st| {
            let mut n = 0.0;
            for ((s, t, e), c) in &st.triples {
                if !types.is_empty() && !types.contains(t) {
                    continue;
                }
                let out = has(*s, near) && has(*e, far);
                let inn = has(*e, near) && has(*s, far);
                n += match dir {
                    Dir::Out => f64::from(u8::from(out)) * c,
                    Dir::In => f64::from(u8::from(inn)) * c,
                    Dir::Both => (f64::from(u8::from(out)) + f64::from(u8::from(inn))) * c,
                };
            }
            Some(n)
        })
    }

    /// The estimated share of `label`'s nodes whose `key` passes `pred`.
    pub(super) fn est_prop(&self, label: Option<u32>, key: &str, pred: &PropPred) -> Option<f64> {
        let label = label?;
        let defs = self.indexes.read().unwrap_or_else(poisoned);
        self.with_stats(|st| {
            // An online index on the label whose first property is `key`.
            let idx = defs.iter().find(|d| {
                d.entity == Entity::Node
                    && d.token_id == label
                    && d.state == "online"
                    && d.props.first().is_some_and(|p| p == key)
            });
            let label_rows = {
                let d = self.dict();
                st.sets
                    .iter()
                    .filter(|(s, _)| d.set_has(**s, label))
                    .map(|(_, n)| *n)
                    .sum::<f64>()
            };
            let (hist, share) = match idx.and_then(|i| st.indexes.get(&i.id).map(|h| (i, h))) {
                Some((i, h))
                    if i.props.len() == 1 && (h.ordered || matches!(pred, PropPred::Eq(_))) =>
                {
                    (
                        h,
                        if label_rows > 0.0 {
                            (h.rows() / label_rows).min(1.0)
                        } else {
                            0.0
                        },
                    )
                }
                _ => {
                    let p = st.props.get(&(label, key.to_owned()))?;
                    (&p.hist, p.share)
                }
            };
            Some(match pred {
                PropPred::Present => share,
                PropPred::Eq(vals) => {
                    let mut s = 0.0;
                    for v in vals {
                        let kk = if hist.ordered {
                            ordkey::value_key(v)
                        } else {
                            None
                        };
                        s += hist.eq_share(kk.as_deref());
                    }
                    (share * s).min(1.0)
                }
                PropPred::Range(r) => match r.key_range() {
                    Some((a, b)) if hist.ordered => share * hist.range_share(&a, &b),
                    Some(_) => share * 0.3,
                    None => 0.0,
                },
            })
        })
    }
}

/// Exposed for the planner's tests: the range predicate's estimate on a
/// histogram.
#[must_use]
pub fn range_share(h: &Hist, r: &RangePred) -> Option<f64> {
    r.key_range().map(|(a, b)| h.range_share(&a, &b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histograms_estimate_equality_and_ranges() {
        let mut b = Builder::default();
        // 1..=1000 once each, and 7 a thousand times more.
        let mut keys: Vec<(Vec<u8>, f64)> = (1..=1000)
            .map(|i| {
                (
                    ordkey::value_key(&crate::value::Value::Int(i)).unwrap(),
                    1.0,
                )
            })
            .collect();
        keys[6].1 += 1000.0;
        for (kk, w) in &keys {
            b.push(kk, *w);
        }
        let h = b.finish(true);
        assert!((h.rows() - 2000.0).abs() < 1e-6);
        let seven = ordkey::value_key(&crate::value::Value::Int(7)).unwrap();
        assert!(h.eq_share(Some(&seven)) > 0.45);
        let other = ordkey::value_key(&crate::value::Value::Int(500)).unwrap();
        assert!(h.eq_share(Some(&other)) < 0.01);
        let r = RangePred {
            lo: Some(ordkey::Bound {
                value: crate::value::Value::Int(501),
                inclusive: true,
            }),
            ..RangePred::default()
        };
        let s = range_share(&h, &r).unwrap();
        assert!((0.2..0.3).contains(&s), "{s}");
    }

    #[test]
    fn records_round_trip() {
        let mut st = GraphStats::default();
        st.sets.insert(1, 10.0);
        st.triples.insert((1, 2, 1), 30.0);
        st.indexes.insert(
            3,
            Hist {
                buckets: vec![Bucket {
                    upper: vec![0, 1, 0xFF],
                    rows: 3.0,
                    distinct: 2.0,
                }],
                ordered: true,
            },
        );
        let body = serde_json::to_vec(&st.to_record()).unwrap();
        let back = GraphStats::from_record(serde_json::from_slice(&body).unwrap());
        assert_eq!(back.sets, st.sets);
        assert_eq!(back.triples, st.triples);
        assert_eq!(back.indexes, st.indexes);
    }
}
