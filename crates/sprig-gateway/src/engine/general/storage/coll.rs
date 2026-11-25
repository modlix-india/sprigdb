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
//! A locking read of a collection is refused: collections are read-only
//! from SQL.

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
use crate::engine::not_on_engine;
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

fn refuse_lock(inner: &Inner, b: &Bound) -> Result<(), SqlError> {
    if b.lock.is_some() {
        return Err(inner.fail(not_on_engine(
            "a locking read of a MongoDB collection (collections are read-only from SQL)",
        )));
    }
    Ok(())
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

/// Entries of `key` in `range`: one per value of `_id`.
pub(super) fn rows_in_range(
    inner: &Inner,
    b: &Bound,
    c: &CollBound,
    key: KeyId,
    range: &KeyRange,
) -> u64 {
    match key {
        KeyId::Index(_) if range.is_point(1) => 1,
        KeyId::Index(_) => (table_rows(inner, b, c) / 3).max(1),
        KeyId::Primary => table_rows(inner, b, c),
    }
}

/// Rows per value of the `_id` key: one.
pub(super) fn rows_per_key(key: KeyId) -> Option<f64> {
    match key {
        KeyId::Index(_) => Some(1.0),
        KeyId::Primary => None,
    }
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
    refuse_lock(inner, b)?;
    let Some(sp) = space(inner, c)? else {
        return Ok(Box::new(Rows(VecDeque::new())));
    };
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
        let Some(batch) = inner.with(|tx| tx.scan(&lo, hi, false, SCAN_BATCH))? else {
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
        let Some(Some(v)) = inner.with(|tx| tx.get(&key))? else {
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
            let (lo, hi, rev) = (&self.lo, &self.hi, self.reverse);
            let Some(batch) = self.inner.with(|tx| tx.scan(lo, hi, rev, want))? else {
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
