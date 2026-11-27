//! The planner's storage over the session's engine transaction: point
//! gets, key-range scans of the primary key and of secondary indexes
//! (index entry, then the row by its primary key; or the entry alone when
//! it holds every column wanted), full scans, and row estimates.
//!
//! - **Reads** are plain reads at the transaction's read view, with its
//!   own writes merged in ([`Tx::get`], [`Tx::scan`]); they take no lock.
//!   A locking read (a locking clause, or any read of a SERIALIZABLE
//!   transaction) reads each row as it is now and locks it as the
//!   single-table path does: a point get locks the row (or, missing, its
//!   place at REPEATABLE READ), a range of the primary key or of an index
//!   locks every entry read and the stretch between them, and a row found
//!   through an index is locked too.
//! - **Only the bound tables, only the checked columns.** A table the
//!   statement wasn't bound to is an error; of a row, the columns the
//!   statement doesn't name are handed over as `NULL` ([`Bound`]).
//! - **Key bytes** follow [`crate::engine::keys`]: a bound of typed values
//!   becomes the bytes of those key parts under the table's row or index
//!   prefix. A unique index keeps a row's primary key in the entry's
//!   value, not its key, unless a part is `NULL`; a bound that reaches
//!   into the primary-key parts of such an index is checked entry by
//!   entry.
//! - **Estimates.** From the table's statistics when it has them
//!   (`crate::engine::stats`): its row count, a range's rows by reading its
//!   first entries and, past them, from the first part's histogram and
//!   the index's distinct counts, an index's rows per value, and the share
//!   of rows a condition on one column keeps. Without statistics a table's
//!   row count is counted up to a cap and remembered for a short while, a
//!   range's by its first entries, an index's rows per value from a
//!   sample.
//! - **Errors** of the store, and the statement time limit, are kept as
//!   they are ([`TxStorage::finish`]); the executor sees a stand-in.

use std::collections::{HashMap, VecDeque};
use std::ops::Bound as RangeBound;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use sprig_common::SprigError;
use sprig_engine::keys::typed;
use sprig_engine::txn::{LockMode, Wait};
use sprig_exec::SqlError;
use sprig_exec::ops::Row;
use sprig_plan::access::{Counters, Fetch, KeyRange, RowCursor, Storage, key_holds};
use sprig_plan::catalog::{KeyId, TableDef as PlanTable};

use super::names::TableUse;
use crate::engine::keys::{self, Space};
use crate::engine::row::{self, Datum};
use crate::engine::stats;
use crate::engine::store::Tx;
use crate::engine::table::{ColType, TableDef};

mod coll;
mod redis;

/// Entries a scan reads per store call, at most (it starts smaller: many
/// scans want one row).
const SCAN_BATCH: usize = 256;
/// Entries read to estimate a range.
const RANGE_PROBE: usize = 24;
/// Rows counted for a table's size; beyond it the table is "large".
const COUNT_CAP: usize = 20_000;
/// What a table past the cap is taken to hold.
const LARGE_TABLE: u64 = 1_000_000;
/// Entries sampled for an index's rows per value.
const SAMPLE: usize = 512;
/// How long an estimate is kept.
const STATS_TTL: Duration = Duration::from_secs(20);

/// One table of the statement as storage serves it.
pub(super) struct Bound {
    def: Arc<TableDef>,
    plan: Arc<PlanTable>,
    sp: Space,
    /// The columns handed over; the others read as `NULL`.
    read: Vec<bool>,
    /// Every column is handed over.
    all: bool,
    /// A locking read of this table (`FOR UPDATE`, `LOCK IN SHARE MODE`,
    /// any read of a SERIALIZABLE transaction): every row and index entry
    /// read is locked in this mode, waiting as the clause says, and read as
    /// it is now.
    lock: Option<(LockMode, Wait)>,
    /// A MongoDB collection read as a table (`coll`).
    coll: Option<coll::CollBound>,
    /// A stream read as a table: its storage (`engine::streamtable`).
    stream: Option<Arc<dyn Storage>>,
    /// A Redis database's table (`engine::redistable`), read in the
    /// transaction.
    redis: Option<crate::engine::redistable::RedisTable>,
}

impl Bound {
    pub(super) fn new(t: &TableUse, sp: Space, lock: Option<(LockMode, Wait)>) -> Self {
        // A table with `FULLTEXT` keys hands over its primary key too:
        // `MATCH` finds a row's score by it.
        let mut read = t.read.clone();
        if !t.def.fulltext.is_empty() {
            for &c in &t.def.pk {
                if let Some(r) = read.get_mut(c) {
                    *r = true;
                }
            }
        }
        Self {
            lock,
            def: Arc::clone(&t.def),
            plan: Arc::clone(&t.plan),
            sp,
            all: read.iter().all(|r| *r),
            read,
            coll: t.coll.clone().map(coll::CollBound::new),
            stream: t.stream.as_ref().map(|s| Arc::clone(&s.storage)),
            redis: t.stream.as_ref().and_then(|s| s.redis),
        }
    }

    fn mask(&self, row: &mut [Datum]) {
        if self.all {
            return;
        }
        for (v, keep) in row.iter_mut().zip(&self.read) {
            if !keep {
                *v = Datum::Null;
            }
        }
    }
}

/// A locking read of a stream: refused (a stream's records aren't locked).
fn stream_locked() -> SqlError {
    SqlError::new(
        1235,
        "42000",
        "This version of SprigDB doesn't yet support 'a locking read of a stream or a queue'",
    )
}

/// A keyspace for planning without reading (a `PREPARE`'s description).
pub(super) fn unread_space(def: &TableDef) -> Space {
    Space {
        tenant: 0,
        object: 0,
        ids: Arc::from(vec![0u16; def.indexes.len()]),
        first_format: false,
        layout: Arc::new(crate::engine::layout::Layout::first(def)),
    }
}

struct Inner {
    /// The transaction; `None` for storage that holds no rows.
    tx: Mutex<Option<Tx>>,
    tables: Vec<Bound>,
    deadline: Option<(Instant, fn() -> SprigError)>,
    /// The first error of the store (or the time limit) as it was.
    failed: Mutex<Option<SprigError>>,
}

/// [`Storage`] for one statement.
pub(super) struct TxStorage {
    inner: Arc<Inner>,
}

fn stand_in(e: &SprigError) -> SqlError {
    SqlError::new(e.protocol().mysql_errno, "HY000", e.message.clone())
}

fn internal(what: &str) -> SqlError {
    SqlError::new(1105, "HY000", format!("engine: {what}"))
}

impl Inner {
    fn fail(&self, e: SprigError) -> SqlError {
        let s = stand_in(&e);
        let mut f = self.failed.lock().unwrap_or_else(PoisonError::into_inner);
        if f.is_none() {
            *f = Some(e);
        }
        s
    }

    /// Runs `f` on the transaction; `None` when storage holds no rows.
    fn with<T>(
        &self,
        f: impl FnOnce(&mut Tx) -> sprig_common::Result<T>,
    ) -> Result<Option<T>, SqlError> {
        let mut g = self.tx.lock().unwrap_or_else(PoisonError::into_inner);
        match g.as_mut() {
            None => Ok(None),
            Some(tx) => f(tx).map(Some).map_err(|e| self.fail(e)),
        }
    }

    fn check_time(&self) -> Result<(), SqlError> {
        match &self.deadline {
            Some((at, e)) if Instant::now() >= *at => Err(self.fail(e())),
            _ => Ok(()),
        }
    }

    /// The position of the bound table the planner means: the definition
    /// it was handed.
    fn bound_at(&self, table: &PlanTable) -> Result<usize, SqlError> {
        self.tables
            .iter()
            .position(|b| std::ptr::eq(Arc::as_ptr(&b.plan), table))
            .or_else(|| {
                self.tables
                    .iter()
                    .position(|b| b.plan.schema == table.schema && b.plan.name == table.name)
            })
            .ok_or_else(|| internal("a table the statement wasn't bound to"))
    }

    fn bound(&self, table: &PlanTable) -> Result<&Bound, SqlError> {
        self.bound_at(table).map(|at| &self.tables[at])
    }
}

/// The key parts a bound of `key` may name, as the table's keys encode
/// them: the key's own, then (for an index) the primary key's.
fn key_parts(b: &Bound, key: KeyId) -> Vec<typed::KeyColumn> {
    let ff = b.sp.first_format;
    match key {
        KeyId::Primary => b.def.pk_key(ff).to_vec(),
        KeyId::Index(ix) => {
            let mut p = b.def.index_key(ix, ff).to_vec();
            p.extend_from_slice(b.def.pk_key(ff));
            p
        }
    }
}

fn key_prefix(b: &Bound, key: KeyId) -> Result<Vec<u8>, SqlError> {
    match key {
        KeyId::Primary => Ok(b.sp.rows()),
        KeyId::Index(ix) if ix < b.def.indexes.len() => Ok(b.sp.index(&b.def, ix)),
        KeyId::Index(_) => Err(internal("the table has no such key")),
    }
}

/// A range of one key in bytes: `[lo, hi)`. `tail`: the range reaches into
/// the primary-key parts of a unique index, whose entries keep those parts
/// in their value: `lo` and `hi` then cover the index parts alone, and
/// each entry's parts and primary key together are checked against
/// `tail`.
struct ByteRange {
    lo: Vec<u8>,
    hi: Vec<u8>,
    tail: Option<(Vec<u8>, Vec<u8>)>,
}

/// `range` of `key` in bytes; `None` when it holds nothing.
fn byte_range(b: &Bound, key: KeyId, range: &KeyRange) -> Result<Option<ByteRange>, SqlError> {
    let parts = key_parts(b, key);
    let prefix = key_prefix(b, key)?;
    // How many parts an entry's key holds for sure.
    let own = match key {
        KeyId::Index(ix) if b.def.indexes[ix].unique => b.def.indexes[ix].cols.len(),
        _ => parts.len(),
    };
    let enc = |values: &[Datum], upto: usize| -> Result<Vec<u8>, SqlError> {
        if values.len() > parts.len() {
            return Err(internal("a key bound names more parts than the key has"));
        }
        let mut out = prefix.clone();
        for (v, kc) in values.iter().zip(&parts).take(upto) {
            typed::encode_column(&mut out, kc, v)
                .map_err(|_| internal("a key bound of the wrong type"))?;
        }
        Ok(out)
    };
    let bounds = |upto: usize| -> Result<(Vec<u8>, Vec<u8>), SqlError> {
        // A bound cut to the entry's own parts widens to every entry that
        // shares them.
        let cut = |v: &Vec<Datum>| v.len() > upto;
        let lo = match &range.start {
            RangeBound::Unbounded => prefix.clone(),
            RangeBound::Included(v) => enc(v, upto)?,
            RangeBound::Excluded(v) if cut(v) => enc(v, upto)?,
            RangeBound::Excluded(v) => keys::prefix_end(&enc(v, upto)?),
        };
        let hi = match &range.end {
            RangeBound::Unbounded => keys::prefix_end(&prefix),
            RangeBound::Included(v) => keys::prefix_end(&enc(v, upto)?),
            RangeBound::Excluded(v) if cut(v) => keys::prefix_end(&enc(v, upto)?),
            RangeBound::Excluded(v) => enc(v, upto)?,
        };
        Ok((lo, hi))
    };
    let longest = match (&range.start, &range.end) {
        (RangeBound::Unbounded, RangeBound::Unbounded) => 0,
        (RangeBound::Included(a) | RangeBound::Excluded(a), RangeBound::Unbounded)
        | (RangeBound::Unbounded, RangeBound::Included(a) | RangeBound::Excluded(a)) => a.len(),
        (
            RangeBound::Included(a) | RangeBound::Excluded(a),
            RangeBound::Included(c) | RangeBound::Excluded(c),
        ) => a.len().max(c.len()),
    };
    let (lo, hi) = bounds(own)?;
    if lo >= hi {
        return Ok(None);
    }
    let tail = if longest > own {
        let (tl, th) = bounds(parts.len())?;
        if tl >= th {
            return Ok(None);
        }
        Some((tl, th))
    } else {
        None
    };
    Ok(Some(ByteRange { lo, hi, tail }))
}

/// The primary-key bytes an index entry points at, and whether the entry
/// lies in `tail` when its parts and primary key are read as one key.
fn entry_row<'a>(
    b: &Bound,
    ix: usize,
    prefix_len: usize,
    ik: &'a [u8],
    iv: &'a [u8],
    tail: Option<&(Vec<u8>, Vec<u8>)>,
) -> Result<Option<&'a [u8]>, SqlError> {
    let bad = || internal("an index entry doesn't decode");
    let pk = keys::entry_target(&b.sp, &b.def, ik, iv).ok_or_else(bad)?;
    let Some((lo, hi)) = tail else {
        return Ok(Some(pk));
    };
    // The entry's parts: all of its key when the primary key is in the
    // value; otherwise the key ends with the primary key already.
    let mut rest = ik.get(prefix_len..).ok_or_else(bad)?;
    for kc in b.def.index_key(ix, b.sp.first_format) {
        rest = typed::decode_column(rest, kc).map_err(|_| bad())?.1;
    }
    let whole: Vec<u8> = if rest.is_empty() {
        [ik, pk].concat()
    } else {
        ik.to_vec()
    };
    Ok((whole.as_slice() >= lo.as_slice() && whole.as_slice() < hi.as_slice()).then_some(pk))
}

/// Types whose key bytes give back the stored value exactly.
fn reads_back(ty: ColType) -> bool {
    matches!(
        ty,
        ColType::Int { .. }
            | ColType::Date
            | ColType::Year
            | ColType::Time { .. }
            | ColType::DateTime { .. }
            | ColType::Timestamp { .. }
    )
}

/// Whether an entry of index `ix` can stand for its row in a key-only
/// read: every column the planner takes from it reads back exactly.
fn entry_suffices(b: &Bound, ix: usize) -> bool {
    !b.sp.first_format
        && b.plan
            .ordering_parts(KeyId::Index(ix))
            .iter()
            .all(|p| !key_holds(&b.plan, p) || reads_back(b.def.columns[p.column].ty))
}

/// The row a key-only read hands over for an entry: the key's and the
/// primary key's columns, the rest `NULL`.
fn entry_values(b: &Bound, ix: usize, prefix_len: usize, ik: &[u8], pk: &[u8]) -> Option<Row> {
    let mut row = vec![Datum::Null; b.def.columns.len() + usize::from(b.def.hidden)];
    let x = &b.def.indexes[ix];
    let mut rest = ik.get(prefix_len..)?;
    for (&c, kc) in x.cols.iter().zip(&x.parts) {
        let (v, r) = typed::decode_column(rest, kc).ok()?;
        rest = r;
        if kc.prefix.is_none() && reads_back(kc.ty) {
            // `None` here is the part's NULL.
            row[c] = v.unwrap_or(Datum::Null);
        }
    }
    let mut rest = pk;
    for (&c, kc) in b.def.pk.iter().zip(&b.def.pk_parts) {
        let (v, r) = typed::decode_column(rest, kc).ok()?;
        rest = r;
        if kc.prefix.is_none() && reads_back(kc.ty) {
            row[c] = v?;
        }
    }
    Some(row)
}

/// A table: tenant, object, schema version.
type TableKey = (u64, u32, u16);

/// A key of a table: the table's parts, whether it is an index, which
/// one, and how many of its parts lead.
type KeyKey = (u64, u32, u16, bool, u16, usize);

/// Estimates kept per table for a short while.
#[derive(Default)]
struct Stats {
    rows: HashMap<TableKey, (u64, Instant)>,
    per_key: HashMap<KeyKey, (f64, Instant)>,
}

fn stats() -> &'static Mutex<Stats> {
    static STATS: OnceLock<Mutex<Stats>> = OnceLock::new();
    STATS.get_or_init(Mutex::default)
}

impl TxStorage {
    /// Storage over `tx` (`None`: no rows, for a description) serving
    /// `tables`.
    pub(super) fn new(
        tx: Option<Tx>,
        tables: Vec<Bound>,
        deadline: Option<(Instant, fn() -> SprigError)>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                tx: Mutex::new(tx),
                tables,
                deadline,
                failed: Mutex::new(None),
            }),
        }
    }

    /// The handle the planner holds.
    pub(super) fn shared(&self) -> Arc<dyn Storage> {
        Arc::new(Self {
            inner: Arc::clone(&self.inner),
        })
    }

    /// Takes the transaction back, with the first store error if any.
    pub(super) fn finish(self) -> (Option<Tx>, Option<SprigError>) {
        let tx = self
            .inner
            .tx
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let failed = self
            .inner
            .failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        (tx, failed)
    }
}

impl Inner {
    /// How many entries `[lo, hi)` holds, counted up to `cap`.
    fn count(&self, lo: &[u8], hi: &[u8], cap: usize) -> usize {
        let mut n = 0;
        let mut from = lo.to_vec();
        while n < cap {
            let want = (cap - n).min(1024);
            let Ok(Some(batch)) = self.with(|tx| tx.scan(&from, hi, false, want)) else {
                break;
            };
            n += batch.len();
            match batch.last() {
                Some((k, _)) if batch.len() == want => from = keys::prefix_end(k),
                _ => break,
            }
        }
        n
    }

    /// The table's statistics, when it has them (none for storage that
    /// holds no rows, a collection or a stream).
    fn known(&self, b: &Bound) -> Option<stats::Known> {
        if b.coll.is_some()
            || b.stream.is_some()
            || self
                .tx
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_none()
        {
            return None;
        }
        let read = |k: &[u8]| self.with(|tx| tx.get(k)).ok().flatten().flatten();
        let p = b.sp.rows();
        let probe = |n: usize| self.count(&p, &keys::prefix_end(&p), n);
        stats::known(b.sp.tenant, &b.def, b.sp.object, &read, &probe)
    }

    fn table_rows(&self, b: &Bound) -> u64 {
        if let Some(k) = self.known(b) {
            return k.rows;
        }
        let key = (b.sp.tenant, b.sp.object, b.sp.layout.row_gen);
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
        let p = b.sp.rows();
        let n = self.count(&p, &keys::prefix_end(&p), COUNT_CAP);
        let n = if n >= COUNT_CAP {
            LARGE_TABLE
        } else {
            n as u64
        };
        stats::counted(b.sp.tenant, &b.def, n);
        stats()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .rows
            .insert(key, (n, now));
        n
    }
}

/// How many values lie between two bounds of one integer key part, when
/// both are whole numbers of the same prefix: the most rows the range can
/// hold on a key of that part alone.
fn integer_span(range: &KeyRange) -> Option<u64> {
    let v = |b: &RangeBound<Vec<Datum>>| match b {
        RangeBound::Included(v) | RangeBound::Excluded(v) => match v.as_slice() {
            [Datum::Int(i)] => Some(i128::from(*i)),
            [Datum::UInt(u)] => Some(i128::from(*u)),
            _ => None,
        },
        RangeBound::Unbounded => None,
    };
    let (a, c) = (v(&range.start)?, v(&range.end)?);
    u64::try_from((c - a).abs() + 1).ok()
}

/// Rows of `range` of `key` from the table's statistics: the first part's
/// share from its histogram; each further part both bounds fix narrows the
/// first part's rows by the index's distinct counts; a further range part
/// keeps a third. `None` when the statistics can't say.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)] // estimates
fn range_rows(b: &Bound, k: &stats::Known, key: KeyId, range: &KeyRange) -> Option<u64> {
    let (cols, name, desc): (Vec<usize>, &str, bool) = match key {
        KeyId::Primary if !b.def.hidden => (
            b.def.pk.clone(),
            stats::PRIMARY,
            b.def.pk_parts.first().is_some_and(|p| p.desc),
        ),
        KeyId::Index(ix) => {
            let x = b.def.indexes.get(ix)?;
            (
                x.cols.clone(),
                x.name.as_str(),
                x.parts.first().is_some_and(|p| p.desc),
            )
        }
        KeyId::Primary => return None,
    };
    let first = b.def.columns.get(*cols.first()?)?;
    let vals = |x: &RangeBound<Vec<Datum>>| match x {
        RangeBound::Included(v) | RangeBound::Excluded(v) => v.clone(),
        RangeBound::Unbounded => Vec::new(),
    };
    let (lo, hi) = (vals(&range.start), vals(&range.end));
    let common = lo.iter().zip(&hi).take_while(|(a, c)| a == c).count();
    // The first part's own range, in ascending order.
    let one = |x: &RangeBound<Vec<Datum>>, v: &[Datum]| match (x, v.first()) {
        (_, None) => RangeBound::Unbounded,
        (RangeBound::Excluded(_), Some(d)) if v.len() == 1 => RangeBound::Excluded(vec![d.clone()]),
        (_, Some(d)) => RangeBound::Included(vec![d.clone()]),
    };
    let (start, end) = (one(&range.start, &lo), one(&range.end, &hi));
    let first_range = if desc {
        KeyRange {
            start: end,
            end: start,
        }
    } else {
        KeyRange { start, end }
    };
    let mut share = k.stats.share(&first.name, &first.ty, &[first_range])?;
    if common >= 2 {
        let ix = k.stats.index(name)?;
        let d1 = *ix.distinct.first()?;
        let dk = *ix.distinct.get(common.min(ix.distinct.len()) - 1)?;
        share *= (d1 / dk.max(1.0)).min(1.0);
    }
    if lo.len().max(hi.len()) > common.max(1) {
        share /= 3.0;
    }
    Some((share * k.rows as f64).round() as u64)
}

impl Storage for TxStorage {
    fn table_rows(&self, table: &PlanTable) -> u64 {
        self.inner
            .bound(table)
            .map_or(0, |b| match (&b.coll, &b.stream) {
                _ if b.redis.is_some() => b
                    .redis
                    .as_ref()
                    .map_or(0, |r| redis::table_rows(&self.inner, r)),
                (_, Some(s)) => s.table_rows(table),
                (Some(c), _) => coll::table_rows(&self.inner, b, c),
                _ => self.inner.table_rows(b),
            })
    }

    fn rows_in_range(&self, table: &PlanTable, key: KeyId, range: &KeyRange) -> u64 {
        let Ok(b) = self.inner.bound(table) else {
            return 0;
        };
        if let Some(r) = &b.redis {
            return redis::rows_in_range(&self.inner, r, key, range);
        }
        if let Some(s) = &b.stream {
            return s.rows_in_range(table, key, range);
        }
        if let Some(c) = &b.coll {
            return coll::rows_in_range(&self.inner, b, c, key, range);
        }
        let Ok(Some(r)) = byte_range(b, key, range) else {
            return 0;
        };
        let n = self.inner.count(&r.lo, &r.hi, RANGE_PROBE);
        if n < RANGE_PROBE {
            return n as u64;
        }
        let total = self.inner.table_rows(b);
        // More than the probe read: from the statistics when the table has
        // them.
        if let Some(k) = self.inner.known(b)
            && let Some(est) = range_rows(b, &k, key, range)
        {
            return est.clamp(RANGE_PROBE as u64, total.max(RANGE_PROBE as u64));
        }
        // Else a tenth of the table, or what an integer key's bounds allow,
        // whichever is less.
        let mut est = (total / 10).max(RANGE_PROBE as u64);
        let one_part = match key {
            KeyId::Primary => b.def.pk.len() == 1,
            KeyId::Index(_) => false,
        };
        if one_part && let Some(span) = integer_span(range) {
            est = est.min(span);
        }
        est.clamp(RANGE_PROBE as u64, total.max(RANGE_PROBE as u64))
    }

    fn rows_per_key(&self, table: &PlanTable, key: KeyId, parts: usize) -> Option<f64> {
        let b = self.inner.bound(table).ok()?;
        if let Some(r) = &b.redis {
            return redis::rows_per_key(r, key, parts);
        }
        if let Some(s) = &b.stream {
            return s.rows_per_key(table, key, parts);
        }
        if b.coll.is_some() {
            return coll::rows_per_key(key);
        }
        let ff = b.sp.first_format;
        let (cols, index_id, unique) = match key {
            KeyId::Primary => (b.def.pk_key(ff), b.sp.layout.row_gen, false),
            KeyId::Index(ix) => (
                b.def.index_key(ix, ff),
                *b.sp.ids.get(ix)?,
                b.def.indexes.get(ix)?.unique,
            ),
        };
        if parts == 0 || parts > cols.len() {
            return None;
        }
        if unique && parts == cols.len() {
            return Some(1.0);
        }
        if let Some(k) = self.inner.known(b) {
            let name = match key {
                KeyId::Primary => stats::PRIMARY,
                KeyId::Index(ix) => b.def.indexes[ix].name.as_str(),
            };
            if let Some(v) = k.stats.rows_per_key(name, parts) {
                // Rows per value grow with the table since.
                #[allow(clippy::cast_precision_loss)]
                let grown = k.rows as f64 / (k.stats.rows.max(1)) as f64;
                return Some((v * grown).max(1.0));
            }
        }
        let stat_key = (
            b.sp.tenant,
            b.sp.object,
            b.sp.layout.row_gen,
            matches!(key, KeyId::Primary),
            index_id,
            parts,
        );
        let now = Instant::now();
        if let Some((v, at)) = stats()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .per_key
            .get(&stat_key)
            && now.duration_since(*at) < STATS_TTL
        {
            return Some(*v);
        }
        let prefix = key_prefix(b, key).ok()?;
        let sample = self
            .inner
            .with(|tx| tx.scan(&prefix, &keys::prefix_end(&prefix), false, SAMPLE))
            .ok()??;
        let mut distinct = 0u64;
        let mut last: Option<&[u8]> = None;
        for (k, _) in &sample {
            let tail = k.get(prefix.len()..)?;
            let mut rest = tail;
            for kc in &cols[..parts] {
                rest = typed::decode_column(rest, kc).ok()?.1;
            }
            let head = &tail[..tail.len() - rest.len()];
            if last != Some(head) {
                distinct += 1;
                last = Some(head);
            }
        }
        let v = if distinct == 0 {
            1.0
        } else {
            #[allow(clippy::cast_precision_loss)]
            let v = sample.len() as f64 / distinct as f64;
            // A whole unique key holds one row per value whatever the
            // sample says of its first entries.
            if unique && parts == cols.len() {
                1.0
            } else {
                v
            }
        };
        stats()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .per_key
            .insert(stat_key, (v, now));
        Some(v)
    }

    fn column_share(&self, table: &PlanTable, column: usize, ranges: &[KeyRange]) -> Option<f64> {
        let b = self.inner.bound(table).ok()?;
        let col = b.def.columns.get(column)?;
        let k = self.inner.known(b)?;
        k.stats.share(&col.name, &col.ty, ranges)
    }

    fn column_distinct(&self, table: &PlanTable, column: usize) -> Option<f64> {
        let b = self.inner.bound(table).ok()?;
        let col = b.def.columns.get(column)?;
        let k = self.inner.known(b)?;
        Some(k.stats.column(&col.name)?.distinct)
    }

    fn get(
        &self,
        table: &PlanTable,
        key: &[Datum],
        counters: &Counters,
    ) -> Result<Option<Row>, SqlError> {
        let b = self.inner.bound(table)?;
        if let Some(r) = &b.redis {
            return redis::get(&self.inner, b, r, key, counters);
        }
        if let Some(s) = &b.stream {
            if b.lock.is_some() {
                return Err(stream_locked());
            }
            return s.get(table, key, counters);
        }
        if b.coll.is_some() {
            return Err(internal("a MongoDB collection has no primary key"));
        }
        let parts = b.def.pk_key(b.sp.first_format);
        if key.len() != parts.len() {
            return Err(internal("a point lookup needs every primary-key part"));
        }
        self.inner.check_time()?;
        counters.add_seek();
        let mut k = b.sp.rows();
        for (v, kc) in key.iter().zip(parts) {
            typed::encode_column(&mut k, kc, v)
                .map_err(|_| internal("a key value of the wrong type"))?;
        }
        let lock = b.lock;
        let got = self.inner.with(|tx| match lock {
            Some((mode, wait)) => Ok(tx
                .lock_read_blocking(&Bytes::from(k.clone()), mode, wait)
                .map_err(|e| crate::engine::exec::named_1020(e, &b.def.name))?
                .flatten()),
            None => tx.get(&k),
        })?;
        let Some(Some(raw)) = got else {
            return Ok(None);
        };
        let mut r = row::decode(&b.def, &b.sp, &k, &raw).map_err(|e| self.inner.fail(e))?;
        // The planner's rows are the columns, and a hidden row number (see
        // `names::plan_table`).
        r.truncate(b.def.columns.len() + usize::from(b.def.hidden));
        b.mask(&mut r);
        counters.add_row();
        Ok(Some(r))
    }

    fn scan(
        &self,
        table: &PlanTable,
        key: KeyId,
        ranges: Vec<KeyRange>,
        reverse: bool,
        fetch: Fetch,
        counters: Arc<Counters>,
    ) -> Result<Box<dyn RowCursor>, SqlError> {
        let at = self.inner.bound_at(table)?;
        let b = &self.inner.tables[at];
        if let Some(r) = &b.redis {
            return redis::scan(&self.inner, b, r, key, &ranges, reverse, &counters);
        }
        if let Some(s) = &b.stream {
            if b.lock.is_some() {
                return Err(stream_locked());
            }
            return s.scan(table, key, ranges, reverse, fetch, counters);
        }
        if b.coll.is_some() {
            return coll::scan(&self.inner, at, key, &ranges, reverse, counters);
        }
        let prefix_len = key_prefix(b, key)?.len();
        let mut bytes = Vec::with_capacity(ranges.len());
        for r in &ranges {
            if let Some(r) = byte_range(b, key, r)? {
                bytes.push(r);
            }
        }
        // A locking read locks each row too: never from the entry alone.
        let key_only = match key {
            KeyId::Index(ix) => {
                b.lock.is_none() && fetch == Fetch::KeyOnly && entry_suffices(b, ix)
            }
            KeyId::Primary => false,
        };
        Ok(Box::new(Cursor {
            inner: Arc::clone(&self.inner),
            table: at,
            key,
            prefix_len,
            ranges: bytes.into(),
            buf: VecDeque::new(),
            more: true,
            batch: 8,
            started: false,
            reverse,
            key_only,
            counters,
            pulled: 0,
            last: None,
        }))
    }
}

#[allow(clippy::struct_excessive_bools)] // independent switches of one read
struct Cursor {
    inner: Arc<Inner>,
    table: usize,
    key: KeyId,
    prefix_len: usize,
    /// The ranges still to read; the first is the one being read, its
    /// bounds moved past what was read.
    ranges: VecDeque<ByteRange>,
    /// Entries read and not yet handed over.
    buf: VecDeque<(Bytes, Bytes)>,
    /// The range being read may hold more than `buf`.
    more: bool,
    batch: usize,
    started: bool,
    reverse: bool,
    key_only: bool,
    counters: Arc<Counters>,
    pulled: u32,
    /// The key of the last row handed over, when it was read locked.
    last: Option<Bytes>,
}

impl Cursor {
    /// Reads the next batch of the current range into `buf`; `false` when
    /// every range is read.
    fn fill(&mut self) -> Result<bool, SqlError> {
        loop {
            let Some(r) = self.ranges.front_mut() else {
                return Ok(false);
            };
            if !self.more || r.lo >= r.hi {
                self.ranges.pop_front();
                self.more = true;
                self.started = false;
                continue;
            }
            if !self.started {
                self.started = true;
                self.counters.add_seek();
            }
            let want = self.batch;
            let (lo, hi, rev) = (&r.lo, &r.hi, self.reverse);
            let lock = self.inner.tables[self.table].lock;
            let rows = self.key == KeyId::Primary;
            let name = &self.inner.tables[self.table].def.name;
            let Some(batch) = self.inner.with(|tx| match lock {
                Some((mode, how)) => tx
                    .scan_lock_blocking(lo, hi, rev, want, mode, how, rows)
                    .map_err(|e| crate::engine::exec::named_1020(e, name)),
                None => tx.scan(lo, hi, rev, want),
            })?
            else {
                return Ok(false);
            };
            self.more = batch.len() == want;
            self.batch = (self.batch * 4).min(SCAN_BATCH);
            if let Some((k, _)) = batch.last() {
                if self.reverse {
                    r.hi = k.to_vec();
                } else {
                    r.lo = keys::prefix_end(k);
                }
            }
            if batch.is_empty() {
                continue;
            }
            self.buf.extend(batch);
            return Ok(true);
        }
    }
}

impl RowCursor for Cursor {
    fn next(&mut self) -> Result<Option<Row>, SqlError> {
        loop {
            self.pulled = self.pulled.wrapping_add(1);
            if self.pulled % 64 == 1 {
                self.inner.check_time()?;
            }
            let Some((k, v)) = self.buf.pop_front() else {
                if self.fill()? {
                    continue;
                }
                return Ok(None);
            };
            let inner = Arc::clone(&self.inner);
            let b = &inner.tables[self.table];
            let mut r = match self.key {
                KeyId::Primary => row::decode(&b.def, &b.sp, &k, &v).map_err(|e| inner.fail(e))?,
                KeyId::Index(ix) => {
                    self.counters.add_entry();
                    let tail = self.ranges.front().and_then(|r| r.tail.as_ref());
                    let Some(pk) = entry_row(b, ix, self.prefix_len, &k, &v, tail)? else {
                        continue;
                    };
                    let from_entry = if self.key_only {
                        entry_values(b, ix, self.prefix_len, &k, pk)
                    } else {
                        None
                    };
                    if let Some(r) = from_entry {
                        r
                    } else {
                        let mut rk = b.sp.rows();
                        rk.extend_from_slice(pk);
                        let lock = b.lock;
                        let got = inner.with(|tx| match lock {
                            Some((mode, wait)) => Ok(tx
                                .lock_read_blocking(&Bytes::from(rk.clone()), mode, wait)
                                .map_err(|e| crate::engine::exec::named_1020(e, &b.def.name))?
                                .flatten()),
                            None => tx.get(&rk),
                        })?;
                        let Some(Some(raw)) = got else {
                            // An entry without its row: not at this view.
                            continue;
                        };
                        let r = row::decode(&b.def, &b.sp, &rk, &raw).map_err(|e| inner.fail(e))?;
                        if lock.is_some() {
                            self.last = Some(Bytes::from(rk));
                        }
                        r
                    }
                }
            };
            if b.lock.is_some() && self.key == KeyId::Primary {
                self.last = Some(k);
            }
            r.truncate(b.def.columns.len() + usize::from(b.def.hidden));
            b.mask(&mut r);
            self.counters.add_row();
            return Ok(Some(r));
        }
    }

    /// READ COMMITTED: the row's own conditions refused it, so its lock
    /// goes, as on the servers, except where they keep it: a row reached
    /// through a secondary index keeps it on MariaDB, and on MySQL when
    /// the index is unique (a locking read by a unique key).
    fn refused(&mut self) {
        let Some(k) = self.last.take() else {
            return;
        };
        let b = &self.inner.tables[self.table];
        let unique = match self.key {
            KeyId::Primary => None,
            KeyId::Index(ix) => Some(b.def.indexes.get(ix).is_some_and(|x| x.unique)),
        };
        let _ = self.inner.with(|tx| {
            let keep = match unique {
                None => false,
                Some(u) => !tx.mysql() || u,
            };
            if !keep {
                tx.unlock_unmatched(&k);
            }
            Ok(())
        });
    }
}
