//! A MongoDB collection under the planner's storage
//! ([`crate::engine::collection`]): its documents read at the statement's
//! read view, in the same engine transaction as the tables.
//!
//! ```text
//! full scan (KeyId::Primary)   the documents in stored _id order, in batches
//! the _id key (KeyId::Index(0))
//!   one value                  the candidate keys and classes of its text,
//!                              each document checked against the text
//!   any other range            every document, those in range, in _id order
//! ```
//!
//! A numeric condition on an indexed field (`collection::PathIndex`, keys
//! `KeyId::Index(1)` on) reads the index's entries: the numbers in the
//! range, widened by a relative 1e-9 (SQL compares them as doubles), and
//! every entry that is no number (SQL turns a string, a date's text or a
//! boolean into a number too); a multikey index (an array's text converts
//! to 0) is read whole. Each document is read once and the planner checks
//! the condition again.
//!
//! A locking read (`FOR UPDATE`, `LOCK IN SHARE MODE`, any read of a
//! SERIALIZABLE transaction, the `SELECT` half of an SQL write) reads each
//! document as it is now and locks its key, as the store's own writers lock
//! it: a candidate key of `_id` equality is locked even when no document has
//! it (its place, at REPEATABLE READ), a scanned range locks every document
//! read and the stretch between them. `SKIP LOCKED` skips a locked document.

use std::collections::VecDeque;
use std::ops::Bound as RangeBound;
use std::sync::{Arc, OnceLock, PoisonError};
use std::time::Instant;

use bytes::Bytes;
use sprig_doc::docstore::layout::{self as doclayout, Space as DocSpace};
use sprig_exec::SqlError;
use sprig_exec::ops::Row;
use sprig_plan::access::{Counters, KeyRange, RowCursor};
use sprig_plan::catalog::KeyId;

use super::{Bound, COUNT_CAP, Inner, LARGE_TABLE, SCAN_BATCH, STATS_TTL, internal, stats};
use crate::engine::collection::{self, CollTable, ID, row_id};
use crate::engine::row::Datum;

/// One collection a statement reads.
pub(in crate::engine::general) struct CollBound {
    pub(super) table: Arc<CollTable>,
    /// Its keyspace, read from its record at the statement's view on first
    /// use (`None`: the collection is gone).
    space: OnceLock<Option<DocSpace>>,
}

impl CollBound {
    pub(super) fn new(table: Arc<CollTable>) -> Self {
        Self {
            table,
            space: OnceLock::new(),
        }
    }
}

/// The collection's keyspace; `None` when it no longer exists or storage
/// holds no rows.
fn space(inner: &Inner, c: &CollBound) -> Result<Option<DocSpace>, SqlError> {
    if let Some(s) = c.space.get() {
        return Ok(*s);
    }
    let got = inner.with(|tx| collection::space_of(&c.table, |k| tx.get(k)))?;
    let s = got.flatten();
    let _ = c.space.set(s);
    Ok(s)
}

/// Up to `want` documents of `[lo, hi)` from `lo` (or back from `hi`):
/// plain reads at the read view, or, for a locking read, locked and read as
/// they are now.
fn read_batch(
    inner: &Inner,
    b: &Bound,
    lo: &[u8],
    hi: &[u8],
    reverse: bool,
    want: usize,
) -> Result<Option<Vec<(Bytes, Bytes)>>, SqlError> {
    let lock = b.lock;
    inner.with(|tx| match lock {
        Some((mode, how)) => tx.scan_lock_blocking(lo, hi, reverse, want, mode, how, true),
        None => tx.scan(lo, hi, reverse, want),
    })
}

/// The documents the collection holds, counted up to a cap and kept a
/// short while.
pub(super) fn table_rows(inner: &Inner, b: &Bound, c: &CollBound) -> u64 {
    let Ok(Some(sp)) = space(inner, c) else {
        return 0;
    };
    let key = (sp.tenant, sp.object, u16::MAX);
    let now = Instant::now();
    if let Some((n, at)) = stats()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .rows
        .get(&key)
        && now.duration_since(*at) < STATS_TTL
    {
        return *n;
    }
    let p = sp.docs();
    let n = inner.count(&p, &doclayout::after_prefix(&p), COUNT_CAP);
    let n = if n >= COUNT_CAP {
        LARGE_TABLE
    } else {
        n as u64
    };
    let _ = b;
    stats()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .rows
        .insert(key, (n, now));
    n
}

/// Entries of `key` in `range`: one per value of `_id`; for a field's
/// index, its entries the range reads (counted up to a cap).
pub(super) fn rows_in_range(
    inner: &Inner,
    b: &Bound,
    c: &CollBound,
    key: KeyId,
    range: &KeyRange,
) -> u64 {
    if let Some(p) = path_of(c, key) {
        let Ok(Some(sp)) = space(inner, c) else {
            return 0;
        };
        let multikey = is_multikey(inner, &sp, p);
        return path_ranges(&sp, p, std::slice::from_ref(range), multikey)
            .iter()
            .map(|(lo, hi)| inner.count(lo, hi, COUNT_CAP / 10) as u64)
            .sum();
    }
    match key {
        KeyId::Index(_) if range.is_point(1) => 1,
        KeyId::Index(_) => (table_rows(inner, b, c) / 3).max(1),
        KeyId::Primary => table_rows(inner, b, c),
    }
}

/// Rows per value of the `_id` key: one (a field's index: unknown).
pub(super) fn rows_per_key(key: KeyId) -> Option<f64> {
    match key {
        KeyId::Index(0) => Some(1.0),
        KeyId::Index(_) | KeyId::Primary => None,
    }
}

/// The field index planner key `key` stands for, if it is one.
fn path_of(c: &CollBound, key: KeyId) -> Option<&collection::PathIndex> {
    match key {
        KeyId::Index(i) if i > 0 => c.table.paths.get(i - 1),
        _ => None,
    }
}

/// Whether the index has seen an array (its multikey fact, at the read
/// view).
fn is_multikey(inner: &Inner, sp: &DocSpace, p: &collection::PathIndex) -> bool {
    !matches!(inner.with(|tx| tx.get(&sp.multikey(p.id))), Ok(Some(None)))
}

/// A bound's number (`NULL`: below every number).
fn bound_number(v: &[Datum]) -> Option<f64> {
    #[allow(clippy::cast_precision_loss)] // a bound, widened anyway
    match v.first()? {
        Datum::Double(f) => Some(*f),
        Datum::Float(f) => Some(f64::from(*f)),
        Datum::Int(i) => Some(*i as f64),
        Datum::UInt(u) => Some(*u as f64),
        Datum::Null => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// The index's byte ranges a key range of the planner reads (see the
/// module notes): `[start, end)` each.
fn path_ranges(
    sp: &DocSpace,
    p: &collection::PathIndex,
    ranges: &[KeyRange],
    multikey: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    use bson::Bson;
    let base = sp.index(p.id, p.unique);
    let end = doclayout::after_prefix(&base);
    let at = |class: u8| {
        let mut k = base.clone();
        k.push(class);
        k
    };
    let num = sprig_doc::keyenc::class::NUMBER;
    let whole = vec![(base.clone(), end.clone())];
    if multikey {
        return whole;
    }
    // Everything that is no number, then each range's numbers.
    let mut out = vec![(base.clone(), at(num)), (at(num + 1), end)];
    for r in ranges {
        let lo = match &r.start {
            RangeBound::Unbounded => Some(f64::NEG_INFINITY),
            RangeBound::Included(v) | RangeBound::Excluded(v) => bound_number(v),
        };
        let hi = match &r.end {
            RangeBound::Unbounded => Some(f64::INFINITY),
            RangeBound::Included(v) | RangeBound::Excluded(v) => bound_number(v),
        };
        let (Some(lo), Some(hi)) = (lo, hi) else {
            return whole;
        };
        if lo.is_nan() || hi.is_nan() {
            return whole;
        }
        let widen = |x: f64| x.abs() * 1e-9 + f64::MIN_POSITIVE;
        let start = if lo.is_finite() {
            let mut k = base.clone();
            k.extend(sprig_doc::keyenc::key(&Bson::Double(lo - widen(lo))));
            k
        } else {
            at(num)
        };
        let stop = if hi.is_finite() {
            let mut k = base.clone();
            k.extend(sprig_doc::keyenc::key(&Bson::Double(hi + widen(hi))));
            doclayout::after_prefix(&k)
        } else if hi == f64::NEG_INFINITY {
            // Only `NULL`: no number.
            continue;
        } else {
            at(num + 1)
        };
        if start < stop {
            out.push((start, stop));
        }
    }
    out
}

/// The documents a field index's ranges name, each once.
fn path_scan(
    inner: &Inner,
    b: &Bound,
    sp: &DocSpace,
    p: &collection::PathIndex,
    ranges: &[KeyRange],
    counters: &Counters,
) -> Result<VecDeque<Row>, SqlError> {
    let multikey = is_multikey(inner, sp, p);
    let mut ids: Vec<Vec<u8>> = Vec::new();
    for (lo, hi) in path_ranges(sp, p, ranges, multikey) {
        counters.add_seek();
        let mut from = lo;
        loop {
            inner.check_time()?;
            let batch = inner
                .with(|tx| tx.scan(&from, &hi, false, SCAN_BATCH))?
                .unwrap_or_default();
            let full = batch.len() == SCAN_BATCH;
            for (_, id) in &batch {
                counters.add_entry();
                ids.push(id.to_vec());
            }
            match batch.last() {
                Some((k, _)) if full => from = crate::engine::keys::prefix_end(k),
                _ => break,
            }
        }
    }
    ids.sort();
    ids.dedup();
    let mut out = VecDeque::new();
    for id in ids {
        let key = sp.doc(&id);
        let lock = b.lock;
        let got = inner.with(|tx| match lock {
            Some((mode, wait)) => Ok(tx
                .lock_read_blocking(&Bytes::from(key.clone()), mode, wait)?
                .flatten()),
            None => tx.get(&key),
        })?;
        if let Some(Some(v)) = got {
            let mut r = id_and_rest(inner, b, &v)?;
            b.mask(&mut r);
            counters.add_row();
            out.push_back(r);
        }
    }
    Ok(out)
}

/// The row of a stored document, as the statement may read it.
fn doc_row(inner: &Inner, b: &Bound, value: &[u8]) -> Result<Row, SqlError> {
    let mut r = collection::row(value, &b.read).map_err(|e| {
        inner.fail(sprig_common::SprigError::internal(format!(
            "engine: MongoDB collection: {e}"
        )))
    })?;
    b.mask(&mut r);
    Ok(r)
}

/// A cursor over `key`'s `ranges` of the collection.
pub(super) fn scan(
    inner: &Arc<Inner>,
    at: usize,
    key: KeyId,
    ranges: &[KeyRange],
    reverse: bool,
    counters: Arc<Counters>,
) -> Result<Box<dyn RowCursor>, SqlError> {
    let b = &inner.tables[at];
    let Some(c) = b.coll.as_ref() else {
        return Err(internal("not a collection"));
    };
    let Some(sp) = space(inner, c)? else {
        return Ok(Box::new(Rows(VecDeque::new())));
    };
    if let Some(p) = path_of(c, key) {
        let rows = path_scan(inner, b, &sp, p, ranges, &counters)?;
        return Ok(Box::new(Rows(rows)));
    }
    if key == KeyId::Primary {
        let lo = sp.docs();
        let hi = doclayout::after_prefix(&lo);
        counters.add_seek();
        return Ok(Box::new(Docs {
            inner: Arc::clone(inner),
            table: at,
            lo,
            hi,
            reverse,
            buf: VecDeque::new(),
            more: true,
            batch: 8,
            counters,
            pulled: 0,
        }));
    }
    // The `_id` key: each range's rows in `_id` order, the ranges in the
    // order given (reversed already for a reverse scan).
    let mut out: VecDeque<Row> = VecDeque::new();
    let mut whole: Option<Vec<Row>> = None;
    for r in ranges {
        inner.check_time()?;
        let mut rows = if let Some(text) = point_text(r) {
            by_id(inner, b, &sp, &text, &counters)?
        } else {
            if whole.is_none() {
                whole = Some(all_rows(inner, b, &sp, &counters)?);
            }
            whole
                .as_ref()
                .map(|w| {
                    w.iter()
                        .filter(|x| in_range(row_id(x), r))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };
        rows.sort_by(|x, y| row_id(x).cmp(row_id(y)));
        if reverse {
            rows.reverse();
        }
        for x in &mut rows {
            b.mask(x);
        }
        out.extend(rows);
    }
    for _ in &out {
        counters.add_row();
    }
    Ok(Box::new(Rows(out)))
}

/// The text of a one-value range of the `_id` key.
fn point_text(r: &KeyRange) -> Option<Vec<u8>> {
    if !r.is_point(1) {
        return None;
    }
    match &r.start {
        RangeBound::Included(v) => match v.first() {
            Some(Datum::Str(s) | Datum::Bytes(s)) => Some(s.to_vec()),
            _ => None,
        },
        _ => None,
    }
}

/// The bound's text, when it is one.
fn bound_text(v: &[Datum]) -> Option<&[u8]> {
    match v.first() {
        Some(Datum::Str(s) | Datum::Bytes(s)) => Some(s),
        _ => None,
    }
}

/// Whether `id` lies in `r` (bytes, as the NO PAD binary collation
/// orders them).
fn in_range(id: &[u8], r: &KeyRange) -> bool {
    let lo = match &r.start {
        RangeBound::Unbounded => true,
        RangeBound::Included(v) => bound_text(v).is_some_and(|t| id >= t),
        RangeBound::Excluded(v) => bound_text(v).is_some_and(|t| id > t),
    };
    let hi = match &r.end {
        RangeBound::Unbounded => true,
        RangeBound::Included(v) => bound_text(v).is_some_and(|t| id <= t),
        RangeBound::Excluded(v) => bound_text(v).is_some_and(|t| id < t),
    };
    lo && hi
}

/// Every document of the collection, as rows with their `_id` (the whole
/// row when the statement reads more).
fn all_rows(
    inner: &Inner,
    b: &Bound,
    sp: &DocSpace,
    counters: &Counters,
) -> Result<Vec<Row>, SqlError> {
    let lo = sp.docs();
    let hi = doclayout::after_prefix(&lo);
    read_range(inner, b, lo, &hi, counters, |_| true)
}

/// The documents of `[lo, hi)` whose row `keep` accepts.
fn read_range(
    inner: &Inner,
    b: &Bound,
    mut lo: Vec<u8>,
    hi: &[u8],
    counters: &Counters,
    keep: impl Fn(&Row) -> bool,
) -> Result<Vec<Row>, SqlError> {
    counters.add_seek();
    let mut out = Vec::new();
    loop {
        inner.check_time()?;
        let Some(batch) = read_batch(inner, b, &lo, hi, false, SCAN_BATCH)? else {
            return Ok(out);
        };
        let full = batch.len() == SCAN_BATCH;
        for (_, v) in &batch {
            let r = id_and_rest(inner, b, v)?;
            if keep(&r) {
                out.push(r);
            }
        }
        match batch.last() {
            Some((k, _)) if full => lo = crate::engine::keys::prefix_end(k),
            _ => return Ok(out),
        }
    }
}

/// A document's row with its `_id` computed even when the statement
/// doesn't read the column (the key's checks need it); masked otherwise.
fn id_and_rest(inner: &Inner, b: &Bound, value: &[u8]) -> Result<Row, SqlError> {
    let mut read = b.read.clone();
    if let Some(x) = read.get_mut(ID) {
        *x = true;
    }
    collection::row(value, &read).map_err(|e| {
        inner.fail(sprig_common::SprigError::internal(format!(
            "engine: MongoDB collection: {e}"
        )))
    })
}

/// The documents whose `_id` prints as `text`.
fn by_id(
    inner: &Inner,
    b: &Bound,
    sp: &DocSpace,
    text: &[u8],
    counters: &Counters,
) -> Result<Vec<Row>, SqlError> {
    let Ok(s) = std::str::from_utf8(text) else {
        return Ok(Vec::new());
    };
    let (keys, classes) = collection::id_candidates(s);
    let mut found: Vec<(Vec<u8>, Row)> = Vec::new();
    for k in keys {
        counters.add_seek();
        let key = sp.doc(&k);
        let lock = b.lock;
        let got = inner.with(|tx| match lock {
            // Locked even when missing: its place, so an insert of that
            // `_id` waits (REPEATABLE READ and SERIALIZABLE).
            Some((mode, wait)) => Ok(tx
                .lock_read_blocking(&Bytes::from(key.clone()), mode, wait)?
                .flatten()),
            None => tx.get(&key),
        })?;
        let Some(Some(v)) = got else {
            continue;
        };
        let r = id_and_rest(inner, b, &v)?;
        if row_id(&r) == text {
            found.push((key, r));
        }
    }
    let docs = sp.docs();
    for cl in classes {
        let mut lo = docs.clone();
        lo.push(cl);
        let mut hi = docs.clone();
        hi.push(cl + 1);
        let rows = read_range(inner, b, lo, &hi, counters, |r| row_id(r) == text)?;
        found.extend(rows.into_iter().map(|r| (Vec::new(), r)));
    }
    // A key found twice (two candidate values with one key) is one row.
    found.sort_by(|x, y| x.0.cmp(&y.0));
    found.dedup_by(|x, y| !x.0.is_empty() && x.0 == y.0);
    Ok(found.into_iter().map(|(_, r)| r).collect())
}

/// Rows already read.
struct Rows(VecDeque<Row>);

impl RowCursor for Rows {
    fn next(&mut self) -> Result<Option<Row>, SqlError> {
        Ok(self.0.pop_front())
    }
}

/// The documents of a collection in stored order, read in batches.
struct Docs {
    inner: Arc<Inner>,
    table: usize,
    lo: Vec<u8>,
    hi: Vec<u8>,
    reverse: bool,
    buf: VecDeque<(Bytes, Bytes)>,
    more: bool,
    batch: usize,
    counters: Arc<Counters>,
    pulled: u32,
}

impl RowCursor for Docs {
    fn next(&mut self) -> Result<Option<Row>, SqlError> {
        loop {
            self.pulled = self.pulled.wrapping_add(1);
            if self.pulled % 64 == 1 {
                self.inner.check_time()?;
            }
            if let Some((_, v)) = self.buf.pop_front() {
                let b = &self.inner.tables[self.table];
                let r = doc_row(&self.inner, b, &v)?;
                self.counters.add_row();
                return Ok(Some(r));
            }
            if !self.more || self.lo >= self.hi {
                return Ok(None);
            }
            let want = self.batch;
            let b = &self.inner.tables[self.table];
            let Some(batch) = read_batch(&self.inner, b, &self.lo, &self.hi, self.reverse, want)?
            else {
                return Ok(None);
            };
            self.more = batch.len() == want;
            self.batch = (self.batch * 4).min(SCAN_BATCH);
            if let Some((k, _)) = batch.last() {
                if self.reverse {
                    self.hi = k.to_vec();
                } else {
                    self.lo = crate::engine::keys::prefix_end(k);
                }
            }
            self.buf.extend(batch);
        }
    }
}
