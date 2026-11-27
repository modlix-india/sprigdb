//! Planner statistics: what storage keeps about a table's values so the
//! planner can tell a value held by most rows from one held by a few.
//!
//! ```text
//! TableStats   rows, when and from how many sampled rows, and per
//!   ColumnStats  NULL share, distinct values, a histogram
//!   IndexStats   distinct values of each leading run of the key's parts
//! Histogram    buckets in the column's key order, over its non-NULL values:
//!   lo, hi     the bucket's first and last value as key bytes (the
//!              engine's memcomparable encoding; strings by collation
//!              weight, cut to KEY_CHARS characters)
//!   cum        share of non-NULL values up to and including hi
//!   ndv        distinct values in the bucket (1: a frequent value alone)
//! ```
//!
//! **Building** ([`ColumnSample`]): the values of a sample of rows. With
//! no more distinct values than buckets, every value has a bucket of its
//! own. Otherwise a value held by at least a bucket's share of rows gets a
//! bucket of its own, and the others fill buckets of about equal row
//! counts; a value never spans two buckets. Distinct counts beyond the
//! sample are estimated from how many sampled values were seen once
//! (Haas and Stokes' first-order jackknife).
//!
//! **Estimates** ([`TableStats::share`]): the share of rows in a set of
//! ranges of one column: NULLs from the NULL share; a value inside a
//! bucket of its own has that bucket's share; a value inside a wider bucket
//! the bucket's share over its distinct values; a range adds the buckets it
//! covers and the part of each bucket it cuts, by linear interpolation of
//! the key bytes after the bucket's common prefix.
//!
//! **Stored form** ([`TableStats::encode`]): a compact binary record
//! (magic, then varints, little-endian `f64`s and length-prefixed bytes).

use std::ops::Bound;

use sprig_engine::keys::typed::{KeyColumn, encode_column};
use sprig_engine::row::{ColumnType, Value};

use crate::access::KeyRange;

/// Buckets of a histogram nobody asked a size for (MySQL's default).
pub const DEFAULT_BUCKETS: usize = 100;
/// The most buckets a histogram may have (MySQL's limit).
pub const MAX_BUCKETS: usize = 1024;
/// Characters (or bytes, for binary strings) of a string value a
/// histogram keeps.
pub const KEY_CHARS: u32 = 32;

const MAGIC: &[u8; 4] = b"PST1";

/// How a column's values are encoded in its histogram; `None` for a type
/// that has no key encoding (`JSON`, geometry).
#[must_use]
pub fn hist_column(ty: &ColumnType) -> Option<KeyColumn> {
    let kc = if is_string(ty) {
        KeyColumn::new(*ty).prefix(KEY_CHARS)
    } else {
        KeyColumn::new(*ty)
    };
    if keyable(ty) {
        return Some(kc);
    }
    let probe = sample_value(ty)?;
    let mut out = Vec::new();
    encode_column(&mut out, &kc, &probe).ok()?;
    Some(kc)
}

fn is_string(ty: &ColumnType) -> bool {
    use ColumnType as T;
    matches!(
        ty,
        T::Char { .. }
            | T::VarChar { .. }
            | T::Text { .. }
            | T::Binary { .. }
            | T::VarBinary { .. }
            | T::Blob { .. }
    )
}

/// A value of the type, to check the type has a key encoding (the types
/// [`keyable`] lists have one).
fn sample_value(ty: &ColumnType) -> Option<Value> {
    use ColumnType as T;
    Some(match ty {
        T::Int { unsigned: true, .. } => Value::UInt(0),
        T::Int { .. } => Value::Int(0),
        T::Bit { .. } => Value::Bit(0),
        T::Double => Value::Double(0.0),
        T::Float => Value::Float(0.0),
        T::Year => Value::Year(2000),
        T::Char { .. } | T::VarChar { .. } | T::Text { .. } => {
            Value::Str(bytes::Bytes::from_static(b"a"))
        }
        T::Binary { bytes: n } => Value::Bytes(bytes::Bytes::from(vec![0u8; usize::from(*n)])),
        T::VarBinary { .. } | T::Blob { .. } => Value::Bytes(bytes::Bytes::from_static(b"a")),
        _ => return None,
    })
}

fn keyable(ty: &ColumnType) -> bool {
    use ColumnType as T;
    matches!(
        ty,
        T::Decimal { .. }
            | T::Date
            | T::Time { .. }
            | T::DateTime { .. }
            | T::Timestamp { .. }
            | T::Enum { .. }
            | T::Set { .. }
    )
}

/// The key bytes of `v` in a histogram of a column encoded as `kc`;
/// `None` for `NULL` or a value the encoding can't take.
#[must_use]
pub fn hist_key(kc: &KeyColumn, v: &Value) -> Option<Vec<u8>> {
    if v.is_null() {
        return None;
    }
    let mut out = Vec::new();
    encode_column(&mut out, kc, v).ok()?;
    Some(out)
}

/// One bucket of a histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    /// The bucket's first value, as key bytes.
    pub lo: Vec<u8>,
    /// The bucket's last value, as key bytes.
    pub hi: Vec<u8>,
    /// The share of non-`NULL` values up to and including `hi`.
    pub cum: f64,
    /// Distinct values in the bucket.
    pub ndv: f64,
}

/// A histogram of one column's non-`NULL` values, in key order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Histogram {
    /// The buckets; the last one's `cum` is 1.
    pub buckets: Vec<Bucket>,
}

impl Histogram {
    /// A histogram of `sorted` (key bytes, in key order, one per sampled
    /// non-`NULL` value) with at most about `buckets` buckets; each
    /// bucket's distinct count is multiplied by `ndv_scale` (the estimated
    /// distinct values of the column over those of the sample).
    #[must_use]
    pub fn build(sorted: &[Vec<u8>], buckets: usize, ndv_scale: f64) -> Self {
        let n = sorted.len();
        if n == 0 {
            return Self::default();
        }
        let buckets = buckets.clamp(1, MAX_BUCKETS);
        let mut runs: Vec<(&[u8], usize)> = Vec::new();
        for v in sorted {
            match runs.last_mut() {
                Some((last, c)) if *last == v.as_slice() => *c += 1,
                _ => runs.push((v, 1)),
            }
        }
        let total = n as f64;
        let mut out: Vec<Bucket> = Vec::new();
        let mut done = 0usize;
        let mut push = |out: &mut Vec<Bucket>, lo: &[u8], hi: &[u8], count: usize, ndv: usize| {
            done += count;
            let scale = if ndv > 1 { ndv_scale.max(1.0) } else { 1.0 };
            out.push(Bucket {
                lo: lo.to_vec(),
                hi: hi.to_vec(),
                cum: done as f64 / total,
                ndv: ndv as f64 * scale,
            });
        };
        if runs.len() <= buckets {
            for (v, c) in &runs {
                push(&mut out, v, v, *c, 1);
            }
        } else {
            let mut left_rows = n;
            let mut left_buckets = buckets;
            // The bucket being filled: first value, last value, rows, values.
            let mut cur: Option<(&[u8], &[u8], usize, usize)> = None;
            for (v, c) in &runs {
                let target = left_rows as f64 / left_buckets.max(1) as f64;
                if *c as f64 >= target && left_buckets > 1 {
                    if let Some((lo, hi, rows, ndv)) = cur.take() {
                        push(&mut out, lo, hi, rows, ndv);
                        left_rows -= rows;
                        left_buckets = left_buckets.saturating_sub(1).max(1);
                    }
                    push(&mut out, v, v, *c, 1);
                    left_rows -= c;
                    left_buckets = left_buckets.saturating_sub(1).max(1);
                    continue;
                }
                let next = match cur.take() {
                    Some((lo, _, rows, ndv)) => (lo, *v, rows + c, ndv + 1),
                    None => (*v, *v, *c, 1),
                };
                let target = left_rows as f64 / left_buckets.max(1) as f64;
                if next.2 as f64 >= target {
                    push(&mut out, next.0, next.1, next.2, next.3);
                    left_rows -= next.2;
                    left_buckets = left_buckets.saturating_sub(1).max(1);
                } else {
                    cur = Some(next);
                }
            }
            if let Some((lo, hi, rows, ndv)) = cur {
                push(&mut out, lo, hi, rows, ndv);
            }
        }
        if let Some(last) = out.last_mut() {
            last.cum = 1.0;
        }
        Self { buckets: out }
    }

    /// The bucket that `x` lies in or before: its position, and whether
    /// `x` lies inside it (`lo <= x <= hi`). `None` past the last bucket.
    fn find(&self, x: &[u8]) -> Option<(usize, bool)> {
        let i = self.buckets.partition_point(|b| b.hi.as_slice() < x);
        let b = self.buckets.get(i)?;
        Some((i, b.lo.as_slice() <= x))
    }

    fn cum_before(&self, i: usize) -> f64 {
        if i == 0 { 0.0 } else { self.buckets[i - 1].cum }
    }

    /// The share of non-`NULL` values equal to `x`.
    #[must_use]
    pub fn eq(&self, x: &[u8]) -> f64 {
        match self.find(x) {
            Some((i, true)) => {
                let b = &self.buckets[i];
                (b.cum - self.cum_before(i)) / b.ndv.max(1.0)
            }
            _ => 0.0,
        }
    }

    /// The share of non-`NULL` values at most `x`.
    #[must_use]
    pub fn le(&self, x: &[u8]) -> f64 {
        match self.find(x) {
            None => 1.0,
            Some((i, false)) => self.cum_before(i),
            Some((i, true)) => {
                let b = &self.buckets[i];
                if b.hi.as_slice() == x || b.ndv <= 1.0 {
                    return b.cum;
                }
                let before = self.cum_before(i);
                let width = b.cum - before;
                // Linear between the bucket's ends, plus this value's own
                // rows (the interpolation reaches only its start).
                let at = width * position(&b.lo, &b.hi, x) + width / b.ndv.max(1.0);
                (before + at).min(b.cum)
            }
        }
    }

    /// The share of non-`NULL` values below `x`.
    #[must_use]
    pub fn lt(&self, x: &[u8]) -> f64 {
        let floor = match self.find(x) {
            Some((i, _)) => self.cum_before(i),
            None => 1.0,
        };
        (self.le(x) - self.eq(x)).max(floor.min(self.le(x)))
    }
}

/// Where `x` lies between `lo` and `hi` (`lo <= x <= hi`), from 0 to 1:
/// the eight bytes after the common prefix of `lo` and `hi`, read as a
/// number.
fn position(lo: &[u8], hi: &[u8], x: &[u8]) -> f64 {
    let common = lo.iter().zip(hi).take_while(|(a, b)| a == b).count();
    let num = |k: &[u8]| -> f64 {
        let mut v = [0u8; 8];
        for (i, b) in k.iter().skip(common).take(8).enumerate() {
            v[i] = *b;
        }
        u64::from_be_bytes(v) as f64
    };
    let (a, b, c) = (num(lo), num(hi), num(x));
    if b <= a {
        return 0.5;
    }
    ((c - a) / (b - a)).clamp(0.0, 1.0)
}

/// What is known of one column.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ColumnStats {
    /// The column's name.
    pub name: String,
    /// The share of rows whose value is `NULL`.
    pub nulls: f64,
    /// Distinct non-`NULL` values, estimated.
    pub distinct: f64,
    /// The values' histogram; `None` when the type has none, or it was
    /// dropped.
    pub histogram: Option<Histogram>,
    /// Buckets asked for (`UPDATE HISTOGRAM ... WITH n BUCKETS`); 0: the
    /// default.
    pub buckets: u16,
    /// `DROP HISTOGRAM` removed the histogram: collections keep none until
    /// one is asked for again.
    pub dropped: bool,
    /// The histogram was asked for by name (`UPDATE HISTOGRAM`), so it is
    /// shown as the servers show theirs.
    pub requested: bool,
    /// `MANUAL UPDATE`: a collection that wasn't asked for this column by
    /// name keeps the histogram as it is.
    pub manual: bool,
    /// When the histogram was made, µs since the epoch.
    pub at_us: u64,
}

/// What is known of one index.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct IndexStats {
    /// The index's name (`PRIMARY` for the primary key).
    pub name: String,
    /// Distinct values of the first 1, 2, ... parts.
    pub distinct: Vec<f64>,
}

/// What is known of one table.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableStats {
    /// Rows when the statistics were taken.
    pub rows: u64,
    /// Rows the column figures were taken from.
    pub sampled: u64,
    /// The definition version the statistics were taken under.
    pub version: u64,
    /// When, µs since the epoch.
    pub at_us: u64,
    /// Per column, by name.
    pub columns: Vec<ColumnStats>,
    /// Per index, by name.
    pub indexes: Vec<IndexStats>,
}

impl TableStats {
    /// The figures of column `name` (names compare without case).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnStats> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// The figures of index `name` (names compare without case).
    #[must_use]
    pub fn index(&self, name: &str) -> Option<&IndexStats> {
        self.indexes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Rows per value of the first `parts` parts of index `name`.
    #[must_use]
    pub fn rows_per_key(&self, name: &str, parts: usize) -> Option<f64> {
        let d = *self.index(name)?.distinct.get(parts.checked_sub(1)?)?;
        Some((self.rows as f64 / d.max(1.0)).max(1.0))
    }

    /// The share of the table's rows whose value of column `col` (of type
    /// `ty`) lies in `ranges`: ranges of a key made of that column alone,
    /// ascending (`NULL` first). `None` when nothing is known of the
    /// column, or a range can't be estimated from what is.
    #[must_use]
    pub fn share(&self, col: &str, ty: &ColumnType, ranges: &[KeyRange]) -> Option<f64> {
        let c = self.column(col)?;
        let kc = hist_column(ty);
        let mut total = 0.0;
        for r in ranges {
            total += range_share(c, kc.as_ref(), r)?;
        }
        Some(total.clamp(0.0, 1.0))
    }
}

/// A bound's value on a one-column key.
#[derive(Clone, Copy)]
enum At<'a> {
    /// Unbounded.
    Open,
    /// The `NULL` value.
    Null,
    /// A value.
    Value(&'a Value),
}

fn bound_value(b: &Bound<Vec<Value>>) -> At<'_> {
    match b {
        Bound::Unbounded => At::Open,
        Bound::Included(v) | Bound::Excluded(v) => match v.first() {
            None => At::Open,
            Some(x) if x.is_null() => At::Null,
            Some(x) => At::Value(x),
        },
    }
}

fn range_share(c: &ColumnStats, kc: Option<&KeyColumn>, r: &KeyRange) -> Option<f64> {
    let inclusive = |b: &Bound<Vec<Value>>| matches!(b, Bound::Included(_) | Bound::Unbounded);
    let (lo, hi) = (bound_value(&r.start), bound_value(&r.end));
    // NULLs sort first: the range holds them when it starts at or before
    // NULL and ends at or after it.
    let starts_before_values = matches!(lo, At::Open | At::Null);
    let holds_null = match (lo, hi) {
        (At::Open | At::Null, At::Null) => inclusive(&r.start) && inclusive(&r.end),
        (At::Open, _) => true,
        (At::Null, _) => inclusive(&r.start),
        _ => false,
    };
    let null_part = if holds_null { c.nulls } else { 0.0 };
    // The non-NULL part.
    let values = if matches!(hi, At::Null) {
        0.0
    } else {
        let point = match (lo, hi) {
            (At::Value(a), At::Value(b)) => a == b && inclusive(&r.start) && inclusive(&r.end),
            _ => false,
        };
        match (&c.histogram, kc) {
            (Some(h), Some(kc)) if !h.buckets.is_empty() => {
                let key = |v: &Value| hist_key(kc, v);
                if point {
                    let At::Value(v) = lo else { return None };
                    h.eq(&key(v)?)
                } else {
                    let from = match lo {
                        At::Open | At::Null => 0.0,
                        At::Value(v) => {
                            let k = key(v)?;
                            if inclusive(&r.start) {
                                h.lt(&k)
                            } else {
                                h.le(&k)
                            }
                        }
                    };
                    let to = match hi {
                        At::Open => 1.0,
                        At::Null => 0.0,
                        At::Value(v) => {
                            let k = key(v)?;
                            if inclusive(&r.end) {
                                h.le(&k)
                            } else {
                                h.lt(&k)
                            }
                        }
                    };
                    (to - from).max(0.0)
                }
            }
            // No histogram: a value is one of the distinct ones; a range
            // over values isn't known.
            _ if point && c.distinct > 0.0 => 1.0 / c.distinct,
            _ if starts_before_values && matches!(hi, At::Open) => 1.0,
            _ => return None,
        }
    };
    Some(null_part + (1.0 - c.nulls) * values)
}

/// Distinct values of a column from a sample: `n` sampled values, `d` of
/// them distinct, `f1` seen exactly once, out of `total` values in all.
#[must_use]
pub fn estimate_distinct(n: u64, d: u64, f1: u64, total: u64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    if n >= total {
        return d as f64;
    }
    let (n, d, f1, total) = (n as f64, d as f64, f1 as f64, total as f64);
    let est = n * d / (n - f1 + f1 * n / total);
    est.clamp(d, total)
}

/// The values of one column over a sample of rows.
#[derive(Debug, Clone)]
pub struct ColumnSample {
    kc: Option<KeyColumn>,
    keys: Vec<Vec<u8>>,
    nulls: u64,
    rows: u64,
    /// Values the encoding refused (counted as distinct, unordered).
    odd: u64,
}

impl ColumnSample {
    /// An empty sample of a column of type `ty`.
    #[must_use]
    pub fn new(ty: &ColumnType) -> Self {
        Self {
            kc: hist_column(ty),
            keys: Vec::new(),
            nulls: 0,
            rows: 0,
            odd: 0,
        }
    }

    /// Whether the column's values can have a histogram.
    #[must_use]
    pub fn has_order(&self) -> bool {
        self.kc.is_some()
    }

    /// One more sampled row's value.
    pub fn add(&mut self, v: &Value) {
        self.rows += 1;
        if v.is_null() {
            self.nulls += 1;
            return;
        }
        match self.kc.as_ref().and_then(|kc| hist_key(kc, v)) {
            Some(k) => self.keys.push(k),
            None => self.odd += 1,
        }
    }

    /// The column's figures for a table of `total` rows: a histogram of at
    /// most `buckets` buckets unless `histogram` is false.
    #[must_use]
    pub fn finish(
        mut self,
        name: &str,
        total: u64,
        buckets: usize,
        histogram: bool,
    ) -> ColumnStats {
        self.keys.sort_unstable();
        let n = self.keys.len() as u64;
        let mut d = 0u64;
        let mut f1 = 0u64;
        let mut i = 0;
        while i < self.keys.len() {
            let mut j = i + 1;
            while j < self.keys.len() && self.keys[j] == self.keys[i] {
                j += 1;
            }
            d += 1;
            if j - i == 1 {
                f1 += 1;
            }
            i = j;
        }
        let nulls = if self.rows == 0 {
            0.0
        } else {
            self.nulls as f64 / self.rows as f64
        };
        let non_null_total = ((total as f64) * (1.0 - nulls)).round() as u64;
        let distinct = estimate_distinct(n, d, f1, non_null_total.max(n)) + self.odd as f64;
        let histogram = (histogram && self.kc.is_some() && n > 0).then(|| {
            let scale = if d == 0 { 1.0 } else { distinct / d as f64 };
            Histogram::build(&self.keys, buckets, scale)
        });
        ColumnStats {
            name: name.to_owned(),
            nulls,
            distinct,
            histogram,
            ..ColumnStats::default()
        }
    }
}

// The stored form.

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

fn put_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0;
        loop {
            let (&b, rest) = self.0.split_first()?;
            self.0 = rest;
            v |= u64::from(b & 0x7f).checked_shl(shift)?;
            if b & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let n = usize::try_from(self.varint()?).ok()?;
        let b = self.0.get(..n)?;
        self.0 = &self.0[n..];
        Some(b)
    }

    fn f64(&mut self) -> Option<f64> {
        let b = self.0.get(..8)?;
        self.0 = &self.0[8..];
        Some(f64::from_le_bytes(b.try_into().ok()?))
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?.to_vec()).ok()
    }
}

impl TableStats {
    /// The stored form.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256);
        out.extend_from_slice(MAGIC);
        put_varint(&mut out, self.rows);
        put_varint(&mut out, self.sampled);
        put_varint(&mut out, self.version);
        put_varint(&mut out, self.at_us);
        put_varint(&mut out, self.columns.len() as u64);
        for c in &self.columns {
            put_bytes(&mut out, c.name.as_bytes());
            put_f64(&mut out, c.nulls);
            put_f64(&mut out, c.distinct);
            put_varint(&mut out, u64::from(c.buckets));
            let flags = u64::from(c.dropped)
                | (u64::from(c.requested) << 1)
                | (u64::from(c.manual) << 2)
                | (u64::from(c.histogram.is_some()) << 3);
            put_varint(&mut out, flags);
            put_varint(&mut out, c.at_us);
            if let Some(h) = &c.histogram {
                put_varint(&mut out, h.buckets.len() as u64);
                for b in &h.buckets {
                    put_bytes(&mut out, &b.lo);
                    put_bytes(&mut out, &b.hi);
                    put_f64(&mut out, b.cum);
                    put_f64(&mut out, b.ndv);
                }
            }
        }
        put_varint(&mut out, self.indexes.len() as u64);
        for x in &self.indexes {
            put_bytes(&mut out, x.name.as_bytes());
            put_varint(&mut out, x.distinct.len() as u64);
            for d in &x.distinct {
                put_f64(&mut out, *d);
            }
        }
        out
    }

    /// Reads the stored form; `None` when it isn't one.
    #[must_use]
    pub fn decode(raw: &[u8]) -> Option<Self> {
        let mut r = Reader(raw.strip_prefix(&MAGIC[..])?);
        let rows = r.varint()?;
        let sampled = r.varint()?;
        let version = r.varint()?;
        let at_us = r.varint()?;
        let ncols = r.varint()?;
        let mut columns = Vec::new();
        for _ in 0..ncols {
            let name = r.string()?;
            let nulls = r.f64()?;
            let distinct = r.f64()?;
            let buckets = u16::try_from(r.varint()?).ok()?;
            let flags = r.varint()?;
            let col_at = r.varint()?;
            let histogram = if flags & 8 != 0 {
                let nb = r.varint()?;
                let mut buckets = Vec::new();
                for _ in 0..nb {
                    buckets.push(Bucket {
                        lo: r.bytes()?.to_vec(),
                        hi: r.bytes()?.to_vec(),
                        cum: r.f64()?,
                        ndv: r.f64()?,
                    });
                }
                Some(Histogram { buckets })
            } else {
                None
            };
            columns.push(ColumnStats {
                name,
                nulls,
                distinct,
                histogram,
                buckets,
                dropped: flags & 1 != 0,
                requested: flags & 2 != 0,
                manual: flags & 4 != 0,
                at_us: col_at,
            });
        }
        let nidx = r.varint()?;
        let mut indexes = Vec::new();
        for _ in 0..nidx {
            let name = r.string()?;
            let n = r.varint()?;
            let mut distinct = Vec::new();
            for _ in 0..n {
                distinct.push(r.f64()?);
            }
            indexes.push(IndexStats { name, distinct });
        }
        r.0.is_empty().then_some(Self {
            rows,
            sampled,
            version,
            at_us,
            columns,
            indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn int_sample(values: &[Option<i64>]) -> ColumnStats {
        let mut s = ColumnSample::new(&ColumnType::int(false));
        for v in values {
            s.add(&v.map_or(Value::Null, Value::Int));
        }
        s.finish("c", values.len() as u64, DEFAULT_BUCKETS, true)
    }

    fn table(c: ColumnStats, rows: u64) -> TableStats {
        TableStats {
            rows,
            sampled: rows,
            columns: vec![c],
            ..TableStats::default()
        }
    }

    fn point(v: i64) -> KeyRange {
        KeyRange::point(vec![Value::Int(v)])
    }

    #[test]
    fn a_frequent_value_and_a_rare_one() {
        // 990 rows of 1, ten of 2..=11.
        let mut vals: Vec<Option<i64>> = vec![Some(1); 990];
        vals.extend((2..=11).map(Some));
        let st = table(int_sample(&vals), 1000);
        let ty = ColumnType::int(false);
        let common = st.share("c", &ty, &[point(1)]).unwrap();
        let rare = st.share("c", &ty, &[point(5)]).unwrap();
        let absent = st.share("c", &ty, &[point(500)]).unwrap();
        assert!((common - 0.99).abs() < 1e-9, "{common}");
        assert!((rare - 0.001).abs() < 1e-9, "{rare}");
        assert!(absent < 1e-9, "{absent}");
    }

    #[test]
    fn many_values_make_equal_height_buckets_and_ranges_interpolate() {
        // 0..10000, a NULL every tenth row.
        let vals: Vec<Option<i64>> = (0..10_000).map(|i| (i % 10 != 0).then_some(i)).collect();
        let st = table(int_sample(&vals), 10_000);
        let c = st.column("c").unwrap();
        assert!((c.nulls - 0.1).abs() < 1e-9);
        assert!((c.distinct - 9000.0).abs() < 1.0);
        let h = c.histogram.as_ref().unwrap();
        assert!(
            h.buckets.len() <= DEFAULT_BUCKETS + 1,
            "{}",
            h.buckets.len()
        );
        let ty = ColumnType::int(false);
        // c < 2500: about a quarter of the non-NULL values.
        let r = KeyRange {
            start: Bound::Excluded(vec![Value::Null]),
            end: Bound::Excluded(vec![Value::Int(2500)]),
        };
        let s = st.share("c", &ty, &[r]).unwrap();
        assert!((s - 0.9 * 0.25).abs() < 0.01, "{s}");
        // c IS NULL.
        let s = st
            .share("c", &ty, &[KeyRange::point(vec![Value::Null])])
            .unwrap();
        assert!((s - 0.1).abs() < 1e-9, "{s}");
        // One value of many.
        let s = st.share("c", &ty, &[point(1234)]).unwrap();
        assert!((s - 0.9 / 9000.0).abs() < 1e-5, "{s}");
        // BETWEEN 1000 AND 1999 and everything.
        let r = KeyRange {
            start: Bound::Included(vec![Value::Int(1000)]),
            end: Bound::Included(vec![Value::Int(1999)]),
        };
        let s = st.share("c", &ty, &[r]).unwrap();
        assert!((s - 0.09).abs() < 0.005, "{s}");
        let s = st.share("c", &ty, &[KeyRange::all()]).unwrap();
        assert!((s - 1.0).abs() < 1e-9, "{s}");
    }

    #[test]
    fn distinct_counts_beyond_a_sample() {
        // Every value seen once in a 1% sample: about as many values as rows.
        assert!(estimate_distinct(1000, 1000, 1000, 100_000) > 50_000.0);
        // Ten values, each seen a hundred times: ten.
        assert!((estimate_distinct(1000, 10, 0, 100_000) - 10.0).abs() < 1e-9);
        assert!((estimate_distinct(10, 3, 1, 10) - 3.0).abs() < 1e-9);
    }

    #[test]
    fn strings_compare_by_collation() {
        let ty = ColumnType::VarChar {
            chars: 20,
            collation: sprig_engine::keys::collation::Collation::from_name("utf8mb4_general_ci")
                .unwrap(),
        };
        let mut s = ColumnSample::new(&ty);
        for i in 0..200 {
            let v = if i < 150 { "Alpha" } else { "beta" };
            s.add(&Value::Str(bytes::Bytes::from(v.to_owned())));
        }
        let st = table(s.finish("c", 200, DEFAULT_BUCKETS, true), 200);
        let p = |v: &str| KeyRange::point(vec![Value::Str(bytes::Bytes::from(v.to_owned()))]);
        let a = st.share("c", &ty, &[p("alpha")]).unwrap();
        assert!((a - 0.75).abs() < 1e-9, "{a}");
    }

    #[test]
    fn stored_form_round_trips() {
        let mut vals: Vec<Option<i64>> = (0..500).map(Some).collect();
        vals.push(None);
        let mut st = table(int_sample(&vals), 501);
        st.indexes.push(IndexStats {
            name: "k".into(),
            distinct: vec![7.0, 501.0],
        });
        st.columns[0].buckets = 12;
        st.columns[0].manual = true;
        st.version = 9;
        let raw = st.encode();
        assert_eq!(TableStats::decode(&raw), Some(st.clone()));
        assert_eq!(TableStats::decode(&raw[..raw.len() - 1]), None);
        assert!((st.rows_per_key("K", 1).unwrap() - 501.0 / 7.0).abs() < 1e-9);
    }
}
