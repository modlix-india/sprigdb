//! What the planner asks of storage: how many rows a key range holds, and
//! the rows themselves, by point lookup or by a scan of key ranges.

use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sprig_engine::row::Value;
use sprig_exec::Result;
use sprig_exec::ops::Row;

use crate::catalog::{KeyId, TableDef};

/// A contiguous run of a key's entries, in the key's own order (a
/// descending key part runs from its high values to its low ones).
///
/// A bound is a prefix of key-part values. `Included` at the start means
/// "from the first entry whose leading parts are these values", at the end
/// "to the last entry whose leading parts are these values". `Excluded`
/// at the start means "after every entry whose leading parts are these
/// values", at the end "before the first such entry". A value may be
/// `NULL`, which a nullable key part orders before every other value.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyRange {
    /// Where the run starts.
    pub start: Bound<Vec<Value>>,
    /// Where the run ends.
    pub end: Bound<Vec<Value>>,
}

impl KeyRange {
    /// Every entry of the key.
    #[must_use]
    pub fn all() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }

    /// The entries whose leading parts are exactly `values`.
    #[must_use]
    pub fn point(values: Vec<Value>) -> Self {
        Self {
            start: Bound::Included(values.clone()),
            end: Bound::Included(values),
        }
    }

    /// Whether the range is the entries of one value prefix of `parts`
    /// key parts.
    #[must_use]
    pub fn is_point(&self, parts: usize) -> bool {
        match (&self.start, &self.end) {
            (Bound::Included(a), Bound::Included(b)) => a.len() == parts && a == b,
            _ => false,
        }
    }
}

/// What a scan gives back for each entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fetch {
    /// The whole row.
    Row,
    /// Only the columns the key's entry holds (the key's own columns and
    /// the primary key's); the other columns of the row are `NULL`. A
    /// column is held by an entry when it is a whole-column key part of a
    /// type that reads back from key bytes (see [`key_holds`]).
    KeyOnly,
}

/// Whether a key entry gives back the value of this part's column: the
/// whole column must be in the key, and a character string is stored as
/// collation weights, which don't read back as text.
#[must_use]
pub fn key_holds(table: &TableDef, part: &crate::catalog::IndexPart) -> bool {
    use sprig_engine::row::ColumnType as T;
    part.prefix.is_none()
        && matches!(
            table.columns[part.column].ty,
            T::Int { .. }
                | T::Decimal { .. }
                | T::Double
                | T::Date
                | T::Time { .. }
                | T::DateTime { .. }
                | T::Timestamp { .. }
                | T::Year
                | T::Binary { .. }
                | T::VarBinary { .. }
        )
}

/// The work one table access did.
#[derive(Debug, Default)]
pub struct Counters {
    rows: AtomicU64,
    entries: AtomicU64,
    seeks: AtomicU64,
}

impl Counters {
    /// A fresh counter set.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// One more row handed to the executor.
    pub fn add_row(&self) {
        self.rows.fetch_add(1, Ordering::Relaxed);
    }

    /// One more index entry read.
    pub fn add_entry(&self) {
        self.entries.fetch_add(1, Ordering::Relaxed);
    }

    /// One more positioning in a key (a point lookup or a range start).
    pub fn add_seek(&self) {
        self.seeks.fetch_add(1, Ordering::Relaxed);
    }

    /// Rows examined: the rows this access handed to the executor.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Secondary-index entries read.
    #[must_use]
    pub fn entries(&self) -> u64 {
        self.entries.load(Ordering::Relaxed)
    }

    /// Positionings in a key.
    #[must_use]
    pub fn seeks(&self) -> u64 {
        self.seeks.load(Ordering::Relaxed)
    }
}

/// Rows coming out of storage, one at a time. A row has one value per
/// table column.
pub trait RowCursor: Send {
    /// The next row, or `None` after the last.
    fn next(&mut self) -> Result<Option<Row>>;

    /// The conditions on the row's own table refused the row just handed
    /// over: a locking read at READ COMMITTED may give up its lock, as the
    /// servers do. Conditions spanning two tables never call it. Storage
    /// that keeps no locks does nothing.
    fn refused(&mut self) {}
}

/// The storage under the planner's row sources.
pub trait Storage: Send + Sync {
    /// The table's row count, as well as storage knows it.
    fn table_rows(&self, table: &TableDef) -> u64;

    /// How many entries of `key` lie in `range`, as well as storage knows.
    fn rows_in_range(&self, table: &TableDef, key: KeyId, range: &KeyRange) -> u64;

    /// The average number of rows sharing one value of the first `parts`
    /// parts of `key`; `None` when storage keeps no such figure.
    fn rows_per_key(&self, table: &TableDef, key: KeyId, parts: usize) -> Option<f64>;

    /// The row with this complete primary key.
    fn get(&self, table: &TableDef, key: &[Value], counters: &Counters) -> Result<Option<Row>>;

    /// The rows of `ranges` of `key`, range after range, each in key order
    /// (or, with `reverse`, every range backwards; the caller then passes
    /// the ranges last first). A full scan is one unbounded range of the
    /// primary key; a table without a primary key has only that scan.
    fn scan(
        &self,
        table: &TableDef,
        key: KeyId,
        ranges: Vec<KeyRange>,
        reverse: bool,
        fetch: Fetch,
        counters: Arc<Counters>,
    ) -> Result<Box<dyn RowCursor>>;
}
