//! Tables held in memory, behind the same [`Catalog`] and [`Storage`]
//! traits the gateway's engine tables will sit behind. Rows are kept in
//! primary-key order under the engine's typed key encoding, and every
//! secondary index is a sorted map from its key bytes to the row's primary
//! key, so key order, `DESC` parts, index prefixes and collations behave
//! as they do in the engine.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use sprig_engine::keys::prefix_end;
use sprig_engine::keys::typed::{decode_column, encode_column};
use sprig_engine::row::Value;
use sprig_exec::ops::Row;
use sprig_exec::{Result, SqlError};

use crate::access::{Counters, Fetch, KeyRange, RowCursor, Storage, key_holds};
use crate::catalog::{Catalog, IndexPart, KeyId, TableDef};

fn internal(e: impl std::fmt::Display) -> SqlError {
    SqlError::new(1105, "HY000", e.to_string())
}

/// One table's rows and index entries.
#[derive(Debug)]
struct MemTable {
    def: Arc<TableDef>,
    /// Primary-key bytes (or, without a primary key, the insertion
    /// number) to the row.
    rows: BTreeMap<Vec<u8>, Row>,
    /// Per secondary index: the index's key bytes followed by the
    /// primary-key bytes, to the primary-key bytes.
    indexes: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
    next_rowid: u64,
    per_key: Mutex<HashMap<(KeyId, usize), f64>>,
}

impl Clone for MemTable {
    fn clone(&self) -> Self {
        Self {
            def: self.def.clone(),
            rows: self.rows.clone(),
            indexes: self.indexes.clone(),
            next_rowid: self.next_rowid,
            per_key: Mutex::new(HashMap::new()),
        }
    }
}

fn encode_parts(def: &TableDef, parts: &[IndexPart], values: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for (part, v) in parts.iter().zip(values) {
        encode_column(&mut out, &def.key_column(part), v).map_err(internal)?;
    }
    Ok(out)
}

impl MemTable {
    fn primary_bytes(&self, row: &[Value]) -> Result<Option<Vec<u8>>> {
        let Some(pk) = &self.def.primary else {
            return Ok(None);
        };
        let values: Vec<Value> = pk.parts.iter().map(|p| row[p.column].clone()).collect();
        encode_parts(&self.def, &pk.parts, &values).map(Some)
    }

    fn insert(&mut self, row: Row) -> Result<()> {
        if row.len() != self.def.columns.len() {
            return Err(SqlError::new(
                1136,
                "21S01",
                "Column count doesn't match value count at row 1",
            ));
        }
        for (v, c) in row.iter().zip(&self.def.columns) {
            if v.is_null() {
                if !c.nullable {
                    return Err(SqlError::new(
                        1048,
                        "23000",
                        format!("Column '{}' cannot be null", c.name),
                    ));
                }
            } else {
                c.ty.check(v).map_err(internal)?;
            }
        }
        let pk = match self.primary_bytes(&row)? {
            Some(pk) => {
                if self.rows.contains_key(&pk) {
                    return Err(SqlError::new(
                        1062,
                        "23000",
                        format!("Duplicate entry for key '{}.PRIMARY'", self.def.name),
                    ));
                }
                pk
            }
            None => {
                self.next_rowid += 1;
                self.next_rowid.to_be_bytes().to_vec()
            }
        };
        for (i, index) in self.def.indexes.iter().enumerate() {
            let values: Vec<Value> = index.parts.iter().map(|p| row[p.column].clone()).collect();
            let mut key = encode_parts(&self.def, &index.parts, &values)?;
            if index.unique && values.iter().all(|v| !v.is_null()) {
                let taken = self.indexes[i]
                    .range::<[u8], _>((Bound::Included(key.as_slice()), Bound::Unbounded))
                    .next()
                    .is_some_and(|(k, _)| k.starts_with(&key));
                if taken {
                    return Err(SqlError::new(
                        1062,
                        "23000",
                        format!("Duplicate entry for key '{}.{}'", self.def.name, index.name),
                    ));
                }
            }
            key.extend_from_slice(&pk);
            self.indexes[i].insert(key, pk.clone());
        }
        self.rows.insert(pk, row);
        self.per_key.lock().map_err(internal)?.clear();
        Ok(())
    }

    /// The parts a bound of key `id` may name.
    fn bound_parts(&self, id: KeyId) -> Vec<IndexPart> {
        self.def.ordering_parts(id)
    }

    /// A range as bytes: an inclusive start and an exclusive end (`None`
    /// for "to the last entry"). `Ok(None)` when the range holds nothing.
    fn byte_range(
        &self,
        id: KeyId,
        range: &KeyRange,
    ) -> Result<Option<(Vec<u8>, Option<Vec<u8>>)>> {
        let parts = self.bound_parts(id);
        let enc = |values: &[Value]| -> Result<Vec<u8>> {
            if values.len() > parts.len() {
                return Err(internal("a key bound names more parts than the key has"));
            }
            encode_parts(&self.def, &parts, values)
        };
        let start = match &range.start {
            Bound::Unbounded => Vec::new(),
            Bound::Included(v) => enc(v)?,
            Bound::Excluded(v) => match prefix_end(&enc(v)?) {
                Some(s) => s,
                None => return Ok(None),
            },
        };
        let end = match &range.end {
            Bound::Unbounded => None,
            Bound::Included(v) => prefix_end(&enc(v)?),
            Bound::Excluded(v) => Some(enc(v)?),
        };
        if end.as_ref().is_some_and(|e| *e <= start) {
            return Ok(None);
        }
        Ok(Some((start, end)))
    }

    fn count(&self, id: KeyId, start: &[u8], end: Option<&[u8]>) -> u64 {
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        let bounds = (Bound::Included(start), upper);
        let n = match id {
            KeyId::Primary => self.rows.range::<[u8], _>(bounds).count(),
            KeyId::Index(i) => self.indexes[i].range::<[u8], _>(bounds).count(),
        };
        n as u64
    }

    /// The row a scan of `id` hands over for the row `full`.
    fn shape(&self, id: KeyId, fetch: Fetch, full: &Row) -> Row {
        if fetch == Fetch::Row || id == KeyId::Primary {
            return full.clone();
        }
        let mut out = vec![Value::Null; full.len()];
        for part in self.bound_parts(id) {
            if key_holds(&self.def, &part) {
                out[part.column] = full[part.column].clone();
            }
        }
        out
    }
}

/// A database of in-memory tables: a [`Catalog`] and its [`Storage`].
#[derive(Debug, Default, Clone)]
pub struct MemDb {
    schema: String,
    tables: HashMap<(String, String), Arc<MemTable>>,
}

impl MemDb {
    /// An empty database whose current schema is `schema`.
    #[must_use]
    pub fn new(schema: &str) -> Self {
        Self {
            schema: schema.to_owned(),
            tables: HashMap::new(),
        }
    }

    fn key(schema: &str, name: &str) -> (String, String) {
        (schema.to_ascii_lowercase(), name.to_ascii_lowercase())
    }

    /// Adds an empty table.
    pub fn create_table(&mut self, def: TableDef) {
        let key = Self::key(&def.schema, &def.name);
        let indexes = def.indexes.iter().map(|_| BTreeMap::new()).collect();
        self.tables.insert(
            key,
            Arc::new(MemTable {
                def: Arc::new(def),
                rows: BTreeMap::new(),
                indexes,
                next_rowid: 0,
                per_key: Mutex::new(HashMap::new()),
            }),
        );
    }

    /// Adds a row to table `name` of the current schema.
    ///
    /// # Errors
    /// The table is unknown, the row doesn't fit its columns, or a key
    /// value is already taken.
    pub fn insert(&mut self, name: &str, row: Row) -> Result<()> {
        let key = Self::key(&self.schema, name);
        let Some(table) = self.tables.get_mut(&key) else {
            return Err(no_table(&self.schema, name));
        };
        Arc::make_mut(table).insert(row)
    }

    fn mem(&self, def: &TableDef) -> Result<&Arc<MemTable>> {
        self.tables
            .get(&Self::key(&def.schema, &def.name))
            .ok_or_else(|| no_table(&def.schema, &def.name))
    }
}

fn no_table(schema: &str, name: &str) -> SqlError {
    SqlError::new(
        1146,
        "42S02",
        format!("Table '{schema}.{name}' doesn't exist"),
    )
}

impl Catalog for MemDb {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>> {
        self.tables
            .get(&Self::key(schema, name))
            .map(|t| t.def.clone())
    }

    fn default_schema(&self) -> Option<String> {
        Some(self.schema.clone())
    }
}

impl Storage for MemDb {
    fn table_rows(&self, table: &TableDef) -> u64 {
        self.mem(table).map_or(0, |t| t.rows.len() as u64)
    }

    fn rows_in_range(&self, table: &TableDef, key: KeyId, range: &KeyRange) -> u64 {
        let Ok(t) = self.mem(table) else { return 0 };
        match t.byte_range(key, range) {
            Ok(Some((start, end))) => t.count(key, &start, end.as_deref()),
            _ => 0,
        }
    }

    fn rows_per_key(&self, table: &TableDef, key: KeyId, parts: usize) -> Option<f64> {
        let t = self.mem(table).ok()?;
        let def = t.def.key(key)?;
        if parts == 0 || parts > def.parts.len() {
            return None;
        }
        if let Some(v) = t.per_key.lock().ok()?.get(&(key, parts)) {
            return Some(*v);
        }
        let cols: Vec<_> = def.parts[..parts]
            .iter()
            .map(|p| t.def.key_column(p))
            .collect();
        let prefix_of = |k: &[u8]| -> Option<usize> {
            let mut rest = k;
            for c in &cols {
                rest = decode_column(rest, c).ok()?.1;
            }
            Some(k.len() - rest.len())
        };
        let mut distinct = 0u64;
        let mut total = 0u64;
        let mut last: Option<Vec<u8>> = None;
        let mut visit = |k: &[u8]| -> Option<()> {
            let n = prefix_of(k)?;
            total += 1;
            if last.as_deref() != Some(&k[..n]) {
                distinct += 1;
                last = Some(k[..n].to_vec());
            }
            Some(())
        };
        match key {
            KeyId::Primary => {
                for k in t.rows.keys() {
                    visit(k)?;
                }
            }
            KeyId::Index(i) => {
                for k in t.indexes[i].keys() {
                    visit(k)?;
                }
            }
        }
        let v = if distinct == 0 {
            1.0
        } else {
            total as f64 / distinct as f64
        };
        t.per_key.lock().ok()?.insert((key, parts), v);
        Some(v)
    }

    fn get(&self, table: &TableDef, key: &[Value], counters: &Counters) -> Result<Option<Row>> {
        let t = self.mem(table)?;
        let Some(pk) = &t.def.primary else {
            return Err(internal("a point lookup needs a primary key"));
        };
        if key.len() != pk.parts.len() {
            return Err(internal("a point lookup needs every primary-key part"));
        }
        counters.add_seek();
        let bytes = encode_parts(&t.def, &pk.parts, key)?;
        let row = t.rows.get(&bytes).cloned();
        if row.is_some() {
            counters.add_row();
        }
        Ok(row)
    }

    fn scan(
        &self,
        table: &TableDef,
        key: KeyId,
        ranges: Vec<KeyRange>,
        reverse: bool,
        fetch: Fetch,
        counters: Arc<Counters>,
    ) -> Result<Box<dyn RowCursor>> {
        let t = self.mem(table)?.clone();
        if t.def.key(key).is_none() && !(key == KeyId::Primary && t.def.primary.is_none()) {
            return Err(internal("the table has no such key"));
        }
        let mut bytes = Vec::with_capacity(ranges.len());
        for r in &ranges {
            if t.def.primary.is_none() && key == KeyId::Primary {
                if *r != KeyRange::all() {
                    return Err(internal(
                        "a table without a primary key has only the full scan",
                    ));
                }
                bytes.push((Vec::new(), None));
            } else if let Some(b) = t.byte_range(key, r)? {
                bytes.push(b);
            }
        }
        Ok(Box::new(MemCursor {
            table: t,
            key,
            ranges: bytes,
            at: 0,
            last: None,
            started: false,
            reverse,
            fetch,
            counters,
        }))
    }
}

struct MemCursor {
    table: Arc<MemTable>,
    key: KeyId,
    ranges: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    at: usize,
    last: Option<Vec<u8>>,
    started: bool,
    reverse: bool,
    fetch: Fetch,
    counters: Arc<Counters>,
}

impl RowCursor for MemCursor {
    fn next(&mut self) -> Result<Option<Row>> {
        while self.at < self.ranges.len() {
            let (start, end) = &self.ranges[self.at];
            if !self.started {
                self.started = true;
                self.counters.add_seek();
            }
            let lower = match (&self.last, self.reverse) {
                (Some(k), false) => Bound::Excluded(k.as_slice()),
                _ => Bound::Included(start.as_slice()),
            };
            let upper = match (&self.last, self.reverse, end) {
                (Some(k), true, _) => Bound::Excluded(k.as_slice()),
                (_, _, Some(e)) => Bound::Excluded(e.as_slice()),
                (_, _, None) => Bound::Unbounded,
            };
            let found: Option<(Vec<u8>, Row)> = match self.key {
                KeyId::Primary => {
                    let mut it = self.table.rows.range::<[u8], _>((lower, upper));
                    let hit = if self.reverse {
                        it.next_back()
                    } else {
                        it.next()
                    };
                    hit.map(|(k, row)| (k.clone(), row.clone()))
                }
                KeyId::Index(i) => {
                    let mut it = self.table.indexes[i].range::<[u8], _>((lower, upper));
                    let hit = if self.reverse {
                        it.next_back()
                    } else {
                        it.next()
                    };
                    match hit {
                        Some((k, pk)) => {
                            self.counters.add_entry();
                            let row = self.table.rows.get(pk).ok_or_else(|| {
                                internal("an index entry points at a missing row")
                            })?;
                            Some((k.clone(), self.table.shape(self.key, self.fetch, row)))
                        }
                        None => None,
                    }
                }
            };
            match found {
                Some((k, row)) => {
                    self.last = Some(k);
                    self.counters.add_row();
                    return Ok(Some(row));
                }
                None => {
                    self.at += 1;
                    self.last = None;
                    self.started = false;
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use sprig_engine::keys::collation::Collation;
    use sprig_engine::row::ColumnType;

    use super::*;
    use crate::catalog::{ColumnDef, IndexDef};

    fn db() -> Arc<MemDb> {
        let mut db = MemDb::new("d");
        db.create_table(TableDef {
            schema: "d".into(),
            name: "t".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::int(false)).not_null(),
                ColumnDef::new("a", ColumnType::int(false)),
                ColumnDef::new(
                    "s",
                    ColumnType::VarChar {
                        chars: 10,
                        collation: Collation::Utf8mb4GeneralCi,
                    },
                ),
            ],
            primary: Some(IndexDef::new("PRIMARY", true, vec![IndexPart::asc(0)])),
            indexes: vec![
                IndexDef::new("k_a", false, vec![IndexPart::desc(1)]),
                IndexDef::new("k_s", false, vec![IndexPart::prefix(2, 2)]),
            ],
        });
        let rows: [(i64, Option<i64>, &str); 6] = [
            (1, Some(5), "apple"),
            (2, None, "APRICOT"),
            (3, Some(7), "banana"),
            (4, Some(5), "ap"),
            (5, Some(1), "cherry"),
            (6, None, "a"),
        ];
        for (id, a, s) in rows {
            db.insert(
                "t",
                vec![
                    Value::Int(id),
                    a.map_or(Value::Null, Value::Int),
                    Value::str(s),
                ],
            )
            .expect("the row is stored");
        }
        Arc::new(db)
    }

    fn ids(db: &Arc<MemDb>, key: KeyId, range: KeyRange, reverse: bool) -> (Vec<i64>, u64) {
        let table = db.table("d", "t").expect("the table");
        let counters = Counters::new();
        let mut cursor = db
            .scan(
                &table,
                key,
                vec![range],
                reverse,
                Fetch::Row,
                counters.clone(),
            )
            .expect("a cursor");
        let mut out = Vec::new();
        while let Some(row) = cursor.next().expect("a row") {
            if let Value::Int(i) = row[0] {
                out.push(i);
            }
        }
        (out, counters.rows())
    }

    #[test]
    fn a_descending_index_runs_high_to_low_with_nulls_last() {
        let db = db();
        let (all, n) = ids(&db, KeyId::Index(0), KeyRange::all(), false);
        assert_eq!(all, [3, 1, 4, 5, 2, 6]);
        assert_eq!(n, 6);
        // From 7 down to, but not including, 1.
        let range = KeyRange {
            start: Bound::Included(vec![Value::Int(7)]),
            end: Bound::Excluded(vec![Value::Int(1)]),
        };
        assert_eq!(ids(&db, KeyId::Index(0), range.clone(), false).0, [3, 1, 4]);
        assert_eq!(ids(&db, KeyId::Index(0), range, true).0, [4, 1, 3]);
        // The NULL entries are a point of their own.
        let nulls = KeyRange::point(vec![Value::Null]);
        assert_eq!(ids(&db, KeyId::Index(0), nulls, false).0, [2, 6]);
        // An index entry ends with the primary key.
        let one = KeyRange::point(vec![Value::Int(5), Value::Int(4)]);
        assert_eq!(ids(&db, KeyId::Index(0), one, false), (vec![4], 1));
    }

    #[test]
    fn a_prefix_index_holds_the_start_of_each_value() {
        let db = db();
        let table = db.table("d", "t").expect("the table");
        // 'ap', 'apple' and 'APRICOT' share the two-character prefix
        // under the case-insensitive collation; the bound is cut to it.
        let range = KeyRange::point(vec![Value::str("apple")]);
        let (mut found, _) = ids(&db, KeyId::Index(1), range.clone(), false);
        found.sort_unstable();
        assert_eq!(found, [1, 2, 4]);
        assert_eq!(db.rows_in_range(&table, KeyId::Index(1), &range), 3);
        assert_eq!(db.rows_per_key(&table, KeyId::Index(0), 1), Some(1.5));
    }

    #[test]
    fn rows_are_checked_on_the_way_in() {
        let mut db = MemDb::new("d");
        db.create_table(TableDef {
            schema: "d".into(),
            name: "u".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::int(false)).not_null(),
                ColumnDef::new("k", ColumnType::int(false)),
            ],
            primary: Some(IndexDef::new("PRIMARY", true, vec![IndexPart::asc(0)])),
            indexes: vec![IndexDef::new("uq", true, vec![IndexPart::asc(1)])],
        });
        assert!(db.insert("u", vec![Value::Int(1), Value::Int(7)]).is_ok());
        assert_eq!(
            db.insert("u", vec![Value::Int(1), Value::Int(8)])
                .map_err(|e| e.code),
            Err(1062)
        );
        assert_eq!(
            db.insert("u", vec![Value::Int(2), Value::Int(7)])
                .map_err(|e| e.code),
            Err(1062)
        );
        // Two NULLs don't collide in a unique index.
        assert!(db.insert("u", vec![Value::Int(3), Value::Null]).is_ok());
        assert!(db.insert("u", vec![Value::Int(4), Value::Null]).is_ok());
        assert_eq!(
            db.insert("u", vec![Value::Null, Value::Null])
                .map_err(|e| e.code),
            Err(1048)
        );
    }
}
