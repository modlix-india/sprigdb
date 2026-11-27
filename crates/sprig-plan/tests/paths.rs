//! Keys over values inside a JSON column (a MongoDB collection's field
//! indexes read from SQL): the planner turns conditions on the path into
//! ranges of the key, hands them to storage, and checks every row again.

#![allow(clippy::unwrap_used, clippy::type_complexity)]

use std::sync::{Arc, Mutex};

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, LobSize, Value};
use sprig_exec::ops::Row;
use sprig_exec::{Ctx, Persona, Result, Session};
use sprig_plan::Planner;
use sprig_plan::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use sprig_plan::catalog::{Catalog, ColumnDef, IndexDef, IndexPart, KeyId, PathPart, TableDef};
use sprig_plan::mem::MemDb;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// `c(id, doc)`: `doc` holds `{"n": i % 50, "s": "v<i % 7>"}`.
fn base() -> (MemDb, TableDef) {
    let def = TableDef {
        schema: "d".into(),
        name: "c".into(),
        columns: vec![
            ColumnDef::new("id", ColumnType::int(false)).not_null(),
            ColumnDef::new(
                "doc",
                ColumnType::Text {
                    size: LobSize::Long,
                    collation: Collation::Utf8mb4Bin,
                },
            ),
        ],
        primary: Some(IndexDef::new("PRIMARY", true, vec![IndexPart::asc(0)])),
        indexes: vec![],
    };
    let mut db = MemDb::new("d");
    db.create_table(def.clone());
    for i in 0..500i64 {
        let doc = format!("{{\"n\": {}, \"s\": \"v{}\"}}", i % 50, i % 7);
        db.insert("c", vec![Value::Int(i), Value::str(&doc)])
            .unwrap();
    }
    (db, def)
}

fn path(p: &str, ty: ColumnType) -> PathPart {
    PathPart {
        path: p.to_owned(),
        ty,
    }
}

/// The table as the planner sees it: two keys over `doc`'s `$.n` (as a
/// number) and `$.s` (as a string).
fn with_paths(def: &TableDef) -> Arc<TableDef> {
    let mut t = def.clone();
    t.indexes.push(
        IndexDef::new("n_num", false, vec![IndexPart::asc(1)])
            .with_paths(vec![Some(path("$.n", ColumnType::Double))]),
    );
    t.indexes.push(
        IndexDef::new("s_str", false, vec![IndexPart::asc(1)]).with_paths(vec![Some(path(
            "$.s",
            ColumnType::VarChar {
                chars: 1024,
                collation: Collation::Utf8mb4Bin,
            },
        ))]),
    );
    Arc::new(t)
}

struct Cat(Arc<TableDef>);

impl Catalog for Cat {
    fn table(&self, _schema: &str, name: &str) -> Option<Arc<TableDef>> {
        (name == "c").then(|| Arc::clone(&self.0))
    }
    fn default_schema(&self) -> Option<String> {
        Some("d".into())
    }
}

/// Storage that answers a path key with every row (a superset, as storage
/// may), and records the ranges it was handed.
struct PathStore {
    mem: MemDb,
    base: TableDef,
    seen: Mutex<Vec<(KeyId, Vec<KeyRange>)>>,
}

impl Storage for PathStore {
    fn table_rows(&self, _table: &TableDef) -> u64 {
        self.mem.table_rows(&self.base)
    }
    fn rows_in_range(&self, table: &TableDef, key: KeyId, range: &KeyRange) -> u64 {
        if table.key(key).is_some_and(IndexDef::has_paths) {
            return if range.is_point(1) { 10 } else { 100 };
        }
        self.mem.rows_in_range(&self.base, key, range)
    }
    fn rows_per_key(&self, _table: &TableDef, key: KeyId, parts: usize) -> Option<f64> {
        self.mem.rows_per_key(&self.base, key, parts)
    }
    fn get(&self, _table: &TableDef, key: &[Value], counters: &Counters) -> Result<Option<Row>> {
        self.mem.get(&self.base, key, counters)
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
        if table.key(key).is_some_and(IndexDef::has_paths) {
            assert_eq!(fetch, Fetch::Row, "a path key never covers");
            self.seen.lock().unwrap().push((key, ranges));
            return self.mem.scan(
                &self.base,
                KeyId::Primary,
                vec![KeyRange::all()],
                false,
                Fetch::Row,
                counters,
            );
        }
        self.mem
            .scan(&self.base, key, ranges, reverse, fetch, counters)
    }
}

fn run(sql: &str) -> (Vec<Vec<Value>>, Vec<String>, Vec<(KeyId, Vec<KeyRange>)>) {
    let (mem, def) = base();
    let cat = Cat(with_paths(&def));
    let store = Arc::new(PathStore {
        mem,
        base: def,
        seen: Mutex::new(Vec::new()),
    });
    let session = Session::new(Persona::MariaDb);
    let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
    let storage: Arc<dyn Storage> = store.clone();
    let mut plan = Planner::new(&cat, storage, &session)
        .plan(&stmt, &[])
        .unwrap();
    let mut ctx = Ctx::new(&session);
    let mut rows = Vec::new();
    while let Some(r) = plan.source.next(&mut ctx).unwrap() {
        rows.push(r);
    }
    let explain = plan
        .explain
        .iter()
        .map(|r| {
            format!(
                "{}:{}",
                r.access.as_deref().unwrap_or("-"),
                r.key.as_deref().unwrap_or("-")
            )
        })
        .collect();
    let seen = std::mem::take(&mut *store.seen.lock().unwrap());
    (rows, explain, seen)
}

#[test]
fn a_number_on_a_path_reads_its_key_and_rows_are_checked_again() {
    let (rows, explain, seen) = run("SELECT id FROM c WHERE doc->>'$.n' = 7 ORDER BY id");
    assert_eq!(explain, vec!["ref:n_num"], "{explain:?}");
    assert_eq!(rows.len(), 10, "every row checked again: {rows:?}");
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].1, vec![KeyRange::point(vec![Value::Double(7.0)])]);
}

#[test]
fn json_value_ranges_and_strings_use_their_keys() {
    let (rows, explain, seen) =
        run("SELECT id FROM c WHERE JSON_VALUE(doc, '$.n') BETWEEN 3 AND 4 ORDER BY id");
    assert_eq!(explain, vec!["range:n_num"], "{explain:?}");
    assert_eq!(rows.len(), 20);
    assert!(
        matches!(&seen[0].1[0].start, std::ops::Bound::Included(v) if v == &vec![Value::Double(3.0)])
    );
    let (rows, explain, seen) =
        run("SELECT id FROM c WHERE doc->>'$.s' IN ('v1', 'v3') ORDER BY id");
    assert_eq!(explain, vec!["range:s_str"], "{explain:?}");
    assert_eq!(rows.len(), 143);
    assert_eq!(seen[0].1.len(), 2);
}

#[test]
fn a_path_key_needs_a_condition_of_its_own_type_and_path() {
    // A string compared with the number key's path, another path, the
    // whole column: no path key.
    for sql in [
        "SELECT id FROM c WHERE doc->>'$.n' = '7'",
        "SELECT id FROM c WHERE doc->>'$.m' = 7",
        "SELECT id FROM c WHERE doc = '{}'",
        "SELECT id FROM c ORDER BY doc->>'$.n' LIMIT 3",
    ] {
        let (_, explain, seen) = run(sql);
        assert_eq!(explain[0], "ALL:-", "{sql}: {explain:?}");
        assert!(seen.is_empty(), "{sql}");
    }
}
