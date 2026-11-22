//! The leaves of a plan: row sources over storage and over rows held in
//! memory.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::rc::Rc;
use std::sync::Arc;

use sprig_engine::row::{ColumnType, Value};
use sprig_exec::ops::{
    AggKind, Aggregate, Aggregation, InnerSource, Memory, Row, RowSource, Sort, SortKey, VecSource,
};
use sprig_exec::{Compiled, Ctx, ExprType, Result, Session, SqlError};

use crate::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use crate::catalog::{KeyId, TableDef};
use crate::keyval::{KeyVal, key_value, truncate};
use crate::resolve::Derived;

/// The static types of a table's row.
pub(crate) fn table_types(table: &TableDef) -> Vec<ExprType> {
    table
        .columns
        .iter()
        .map(|c| ExprType::of_column(&c.ty, c.nullable))
        .collect()
}

/// A scan of key ranges of a stored table.
pub(crate) struct ScanSource {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) table: Arc<TableDef>,
    pub(crate) key: KeyId,
    pub(crate) ranges: Vec<KeyRange>,
    pub(crate) reverse: bool,
    pub(crate) fetch: Fetch,
    pub(crate) counters: Arc<Counters>,
    pub(crate) cursor: Option<Box<dyn RowCursor>>,
}

/// The values of a range that is one whole primary key, if it is that.
fn whole_key<'r>(table: &TableDef, key: KeyId, range: &'r KeyRange) -> Option<&'r [Value]> {
    let pk = table.primary.as_ref()?;
    if key != KeyId::Primary
        || pk.parts.iter().any(|p| p.prefix.is_some())
        || !range.is_point(pk.parts.len())
    {
        return None;
    }
    match &range.start {
        std::ops::Bound::Included(v) => Some(v),
        _ => None,
    }
}

/// Point gets of a list of primary keys, one as each row is asked for.
struct Points {
    storage: Arc<dyn Storage>,
    table: Arc<TableDef>,
    counters: Arc<Counters>,
    keys: std::vec::IntoIter<Vec<Value>>,
}

impl RowCursor for Points {
    fn next(&mut self) -> Result<Option<Row>> {
        for key in self.keys.by_ref() {
            if let Some(row) = self.storage.get(&self.table, &key, &self.counters)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// A cursor over rows already fetched.
struct Fetched(std::vec::IntoIter<Row>);

impl RowCursor for Fetched {
    fn next(&mut self) -> Result<Option<Row>> {
        Ok(self.0.next())
    }
}

impl ScanSource {
    fn cursor(&mut self) -> Result<&mut Box<dyn RowCursor>> {
        // Whole primary keys are point gets, one per key, in key order.
        if self.cursor.is_none()
            && !self.ranges.is_empty()
            && self
                .ranges
                .iter()
                .all(|r| whole_key(&self.table, self.key, r).is_some())
        {
            let mut keys: Vec<Vec<Value>> = self
                .ranges
                .iter()
                .filter_map(|r| whole_key(&self.table, self.key, r).map(<[Value]>::to_vec))
                .collect();
            if self.reverse {
                keys.reverse();
            }
            self.cursor = Some(Box::new(Points {
                storage: self.storage.clone(),
                table: self.table.clone(),
                counters: self.counters.clone(),
                keys: keys.into_iter(),
            }));
        }
        if self.cursor.is_none() {
            let mut ranges = self.ranges.clone();
            if self.reverse {
                ranges.reverse();
            }
            self.cursor = Some(self.storage.scan(
                &self.table,
                self.key,
                ranges,
                self.reverse,
                self.fetch,
                self.counters.clone(),
            )?);
        }
        match self.cursor.as_mut() {
            Some(c) => Ok(c),
            None => unreachable!("the cursor was just opened"),
        }
    }
}

impl ScanSource {
    /// See [`RowCursor::refused`].
    pub(crate) fn refused(&mut self) {
        if let Some(c) = self.cursor.as_mut() {
            c.refused();
        }
    }
}

/// A stored table's scan under the conditions on that table alone: a row
/// they refuse is reported to the scan's cursor ([`RowCursor::refused`]).
pub(crate) struct TableFilter {
    pub(crate) scan: ScanSource,
    pub(crate) cond: Compiled,
}

impl RowSource for TableFilter {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        while let Some(row) = RowSource::next(&mut self.scan, ctx)? {
            if self.cond.eval_bool(ctx, &row)? {
                return Ok(Some(row));
            }
            self.scan.refused();
        }
        Ok(None)
    }

    fn types(&self) -> Vec<ExprType> {
        RowSource::types(&self.scan)
    }
}

impl RowSource for ScanSource {
    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        self.cursor()?.next()
    }

    fn types(&self) -> Vec<ExprType> {
        table_types(&self.table)
    }
}

impl InnerSource for ScanSource {
    fn open(&mut self, _: &mut Ctx<'_>, _: &[Value]) -> Result<()> {
        self.cursor = None;
        Ok(())
    }

    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        RowSource::next(self, ctx)
    }

    fn types(&self) -> Vec<ExprType> {
        table_types(&self.table)
    }
}

/// One bound key part of a lookup.
pub(crate) struct LookupPart {
    /// The value to look up, computed from the outer row.
    pub(crate) expr: Compiled,
    /// The key column's type.
    pub(crate) column: ColumnType,
    /// The key part's index prefix length.
    pub(crate) prefix: Option<u32>,
}

/// The inner side of a nested-loop join by key: for each outer row, the
/// rows whose leading key parts equal values computed from it.
pub(crate) struct LookupInner {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) table: Arc<TableDef>,
    pub(crate) key: KeyId,
    pub(crate) parts: Vec<LookupPart>,
    pub(crate) fetch: Fetch,
    pub(crate) counters: Arc<Counters>,
    pub(crate) cursor: Option<Box<dyn RowCursor>>,
}

impl InnerSource for LookupInner {
    fn open(&mut self, ctx: &mut Ctx<'_>, outer: &[Value]) -> Result<()> {
        self.cursor = None;
        let mut values = Vec::with_capacity(self.parts.len());
        let mut usable = true;
        for p in &self.parts {
            let v = p.expr.eval(ctx, outer)?;
            match key_value(&p.column, &v, p.expr.ty()) {
                KeyVal::Exact(v) => values.push(match p.prefix {
                    Some(n) => truncate(&p.column, v, n),
                    None => v,
                }),
                // No row of the table equals this value.
                KeyVal::Null | KeyVal::Between(..) => return Ok(()),
                // The value can't be turned into a key: read every row and
                // leave it to the join condition.
                KeyVal::Unusable => {
                    usable = false;
                    break;
                }
            }
        }
        if usable {
            let range = KeyRange::point(values);
            if let Some(values) = whole_key(&self.table, self.key, &range) {
                let row = self.storage.get(&self.table, values, &self.counters)?;
                self.cursor = Some(Box::new(Fetched(
                    row.into_iter().collect::<Vec<_>>().into_iter(),
                )));
                return Ok(());
            }
            values = match range.start {
                std::ops::Bound::Included(v) => v,
                _ => Vec::new(),
            };
        }
        let (key, range, fetch) = if usable {
            (self.key, KeyRange::point(values), self.fetch)
        } else if self.table.primary.is_some() || self.key == KeyId::Primary {
            (KeyId::Primary, KeyRange::all(), Fetch::Row)
        } else {
            (self.key, KeyRange::all(), Fetch::Row)
        };
        self.cursor = Some(self.storage.scan(
            &self.table,
            key,
            vec![range],
            false,
            fetch,
            self.counters.clone(),
        )?);
        Ok(())
    }

    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        match self.cursor.as_mut() {
            Some(c) => c.next(),
            None => Ok(None),
        }
    }

    fn types(&self) -> Vec<ExprType> {
        table_types(&self.table)
    }
}

/// The rows of a derived table.
pub(crate) struct DerivedSource {
    derived: Rc<Derived>,
    rows: Option<Rc<Vec<Row>>>,
    pos: usize,
}

impl DerivedSource {
    pub(crate) fn new(derived: Rc<Derived>) -> Self {
        Self {
            derived,
            rows: None,
            pos: 0,
        }
    }
}

impl RowSource for DerivedSource {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        if self.rows.is_none() {
            self.rows = Some(self.derived.rows(ctx)?);
        }
        let row = self.rows.as_ref().and_then(|r| r.get(self.pos).cloned());
        self.pos += 1;
        Ok(row)
    }

    fn types(&self) -> Vec<ExprType> {
        self.derived.types.clone()
    }
}

/// An inner side read once and replayed for every outer row.
pub(crate) struct CachedInner {
    source: Option<Box<dyn RowSource>>,
    types: Vec<ExprType>,
    rows: Vec<Row>,
    pos: usize,
}

impl CachedInner {
    pub(crate) fn new(source: Box<dyn RowSource>) -> Self {
        let types = source.types();
        Self {
            source: Some(source),
            types,
            rows: Vec::new(),
            pos: 0,
        }
    }
}

impl InnerSource for CachedInner {
    fn open(&mut self, ctx: &mut Ctx<'_>, _: &[Value]) -> Result<()> {
        if let Some(mut source) = self.source.take() {
            while let Some(row) = source.next(ctx)? {
                self.rows.push(row);
            }
        }
        self.pos = 0;
        Ok(())
    }

    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        let row = self.rows.get(self.pos).cloned();
        self.pos += 1;
        Ok(row)
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// A fixed list of rows of known types: no rows at all (a condition that
/// can't hold), or the one empty row a `SELECT` without `FROM` computes
/// its expressions on.
pub(crate) struct Fixed {
    rows: std::vec::IntoIter<Row>,
    types: Vec<ExprType>,
}

impl Fixed {
    pub(crate) fn new(rows: Vec<Row>, types: Vec<ExprType>) -> Self {
        Self {
            rows: rows.into_iter(),
            types,
        }
    }
}

impl RowSource for Fixed {
    fn next(&mut self, _: &mut Ctx<'_>) -> Result<Option<Row>> {
        Ok(self.rows.next())
    }

    fn types(&self) -> Vec<ExprType> {
        self.types.clone()
    }
}

/// One aggregate of a scalar subquery computed per row.
pub(crate) struct ApplyAgg {
    pub(crate) kind: AggKind,
    /// The argument, over the outer row followed by the inner row.
    pub(crate) arg: Option<Compiled>,
    pub(crate) distinct: bool,
    pub(crate) separator: Option<Vec<u8>>,
}

/// A scalar subquery tied to its outer rows by equalities: for each outer
/// row the matching inner rows are found (by key lookup where there is a
/// key), one value is computed from them, and the value is added to the
/// row as a last column. The value is remembered per combination of the
/// outer values the subquery reads.
pub(crate) struct ApplyOp {
    pub(crate) outer: Box<dyn RowSource>,
    pub(crate) inner: Box<dyn InnerSource>,
    /// The subquery's conditions, over the outer row followed by the
    /// inner row.
    pub(crate) on: Option<Compiled>,
    /// The outer values the subquery reads.
    pub(crate) keys: Vec<Compiled>,
    pub(crate) aggs: Vec<ApplyAgg>,
    /// Whether the subquery aggregates (it then gives one value also
    /// when no row matches).
    pub(crate) grouped: bool,
    /// The value: over the aggregates when there are any, else over the
    /// outer row followed by the inner row.
    pub(crate) item: Compiled,
    /// With `ORDER BY ... LIMIT 1`: the order (over the outer row
    /// followed by the inner row) whose first row gives the value.
    pub(crate) first_by: Vec<(Compiled, bool)>,
    pub(crate) ty: ExprType,
    pub(crate) session: Session,
    pub(crate) memory: Arc<Memory>,
    pub(crate) joined_types: Vec<ExprType>,
    pub(crate) cache: HashMap<String, Value>,
}

impl ApplyOp {
    fn value(&mut self, ctx: &mut Ctx<'_>, outer: &[Value]) -> Result<Value> {
        self.inner.open(ctx, outer)?;
        let mut matched = Vec::new();
        while let Some(inner) = self.inner.next(ctx)? {
            let mut joined = outer.to_vec();
            joined.extend(inner);
            let pass = match &self.on {
                Some(on) => on.eval_bool(ctx, &joined)?,
                None => true,
            };
            if !pass {
                continue;
            }
            if !self.grouped && self.first_by.is_empty() && !matched.is_empty() {
                return Err(SqlError::new(
                    1242,
                    "21000",
                    "Subquery returns more than 1 row",
                ));
            }
            matched.push(joined);
        }
        if !self.grouped {
            if matched.len() > 1 {
                let keys = self
                    .first_by
                    .iter()
                    .map(|(expr, desc)| SortKey {
                        expr: expr.clone(),
                        desc: *desc,
                    })
                    .collect();
                let rows = VecSource::new(matched, self.joined_types.clone());
                let mut sort = Sort::top_n(Box::new(rows), keys, 1, self.memory.clone());
                return match sort.next(ctx)? {
                    Some(row) => self.item.eval(ctx, &row),
                    None => Ok(Value::Null),
                };
            }
            return match matched.first() {
                Some(row) => self.item.eval(ctx, row),
                None => Ok(Value::Null),
            };
        }
        let mut aggs = Vec::with_capacity(self.aggs.len());
        for a in &self.aggs {
            let mut agg = Aggregate::new(a.kind, a.arg.clone(), a.distinct, &self.session)?;
            if let Some(sep) = &a.separator {
                agg = agg.separator(sep);
            }
            aggs.push(agg);
        }
        let rows = VecSource::new(matched, self.joined_types.clone());
        let mut grouping = Aggregation::hash(Box::new(rows), Vec::new(), aggs, self.memory.clone());
        let out = grouping.next(ctx)?.unwrap_or_default();
        self.item.eval(ctx, &out)
    }
}

impl RowSource for ApplyOp {
    fn next(&mut self, ctx: &mut Ctx<'_>) -> Result<Option<Row>> {
        let Some(mut row) = self.outer.next(ctx)? else {
            return Ok(None);
        };
        let mut key = String::new();
        for k in &self.keys {
            let _ = write!(key, "{:?}|", k.eval(ctx, &row)?);
        }
        let v = match self.cache.get(&key) {
            Some(v) => v.clone(),
            None => {
                let v = self.value(ctx, &row)?;
                self.cache.insert(key, v.clone());
                v
            }
        };
        row.push(v);
        Ok(Some(row))
    }

    fn types(&self) -> Vec<ExprType> {
        let mut t = self.outer.types();
        t.push(self.ty);
        t
    }
}
