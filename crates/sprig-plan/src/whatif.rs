//! What-if planning: how a statement would be planned if a table had one
//! more secondary index, without building it.
//!
//! ```text
//! estimate(stmt)                 plan as the tables are: the keys read, rows
//!                                examined as the plan's estimates add up
//! estimate_with(stmt, hypo)      the same with `hypo.index` appended to its
//!                                table's definition (a hypothetical index):
//!   catalog  the table's definition gains the index, last, so every
//!            existing key keeps its position
//!   storage  the hypothetical key's figures come from a sample of the
//!            table's rows (the first `sample_rows` in primary-key order),
//!            loaded into an in-memory table that has the index, scaled to
//!            the table's row count; every other key asks real storage
//! ```
//!
//! Nothing is executed: the planner builds its operators, which read
//! nothing until they are pulled, and the plan is dropped. A plan that
//! would have to read the hypothetical index while being planned (an
//! uncorrelated subquery folded to a constant) fails, and the statement
//! has no estimate with the index.
//!
//! **Rows examined** of a plan: for each query block, the first table's
//! estimated rows, plus each later table's rows times the rows of the
//! tables before it (a nested loop's reads); blocks add up.

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_engine::row::Value;
use sprig_exec::Result;
use sprig_exec::Session;
use sprig_exec::ops::Row;
use sqlparser::ast;

use crate::access::{Counters, Fetch, KeyRange, RowCursor, Storage};
use crate::catalog::{Catalog, IndexDef, KeyId, TableDef};
use crate::explain::ExplainRow;
use crate::mem::MemDb;
use crate::plan::{PlanOptions, Planner};

/// Rows sampled for a hypothetical index's figures when the caller names
/// no other number.
pub const SAMPLE_ROWS: usize = 4096;

/// One more secondary index on a table.
#[derive(Debug, Clone)]
pub struct Hypothetical {
    /// The table's database.
    pub schema: String,
    /// The table.
    pub table: String,
    /// The index (its parts name the table's columns by position).
    pub index: IndexDef,
    /// Rows read from the table for the index's figures.
    pub sample_rows: usize,
}

/// A statement's plan, as estimates.
#[derive(Debug, Clone, PartialEq)]
pub struct Estimate {
    /// Rows the plan is estimated to examine.
    pub examined: f64,
    /// The keys the plan reads: `(table alias, key name)`, in plan order.
    pub keys: Vec<(String, String)>,
    /// The plan as `EXPLAIN` shows it.
    pub explain: Vec<ExplainRow>,
}

impl Estimate {
    /// Whether the plan reads the key called `name` (names compare without
    /// case, as index names do).
    #[must_use]
    pub fn reads(&self, name: &str) -> bool {
        self.keys.iter().any(|(_, k)| k.eq_ignore_ascii_case(name))
    }
}

/// Rows examined from `EXPLAIN` rows: per block, a nested loop's reads.
#[must_use]
pub fn examined_of(rows: &[ExplainRow]) -> f64 {
    let mut blocks: BTreeMap<usize, Vec<&ExplainRow>> = BTreeMap::new();
    for r in rows.iter().filter(|r| r.table.is_some()) {
        blocks.entry(r.id).or_default().push(r);
    }
    blocks
        .values()
        .map(|steps| {
            let mut total = 0.0;
            let mut loops = 1.0;
            for s in steps {
                let n = s.rows.unwrap_or(1).max(1) as f64;
                total += loops * n;
                loops *= n;
            }
            total
        })
        .sum()
}

fn estimate_of(rows: Vec<ExplainRow>) -> Estimate {
    let keys = rows
        .iter()
        .filter_map(|r| Some((r.table.clone()?, r.key.clone()?)))
        .flat_map(|(t, k)| {
            // An index merge names several keys, comma-separated.
            k.split(',')
                .map(|one| (t.clone(), one.trim().to_owned()))
                .collect::<Vec<_>>()
        })
        .collect();
    Estimate {
        examined: examined_of(&rows),
        keys,
        explain: rows,
    }
}

/// Plans `stmt` (a `SELECT`) with `params` as the tables are.
///
/// # Errors
/// The planner's.
pub fn estimate(
    catalog: &dyn Catalog,
    storage: Arc<dyn Storage>,
    session: &Session,
    options: Option<PlanOptions>,
    stmt: &ast::Statement,
    params: &[Value],
) -> Result<Estimate> {
    let mut p = Planner::new(catalog, storage, session);
    if let Some(o) = options {
        p.options = o;
    }
    let plan = p.plan(stmt, params)?;
    Ok(estimate_of(plan.explain))
}

/// Plans `stmt` with `params` as if `hypo` existed.
///
/// # Errors
/// The planner's; 1146 when `hypo` names no table of `catalog`.
pub fn estimate_with(
    catalog: &dyn Catalog,
    storage: Arc<dyn Storage>,
    session: &Session,
    options: Option<PlanOptions>,
    stmt: &ast::Statement,
    params: &[Value],
    hypo: &Hypothetical,
) -> Result<Estimate> {
    let prepared = Prepared::new(catalog, storage, hypo)?;
    prepared.estimate(session, options, stmt, params)
}

/// A hypothetical index with its sample loaded, for planning several
/// statements against it.
pub struct Prepared<'a> {
    catalog: WithIndex<'a>,
    storage: Arc<dyn Storage>,
}

impl<'a> Prepared<'a> {
    /// Loads the sample of `hypo`'s table.
    ///
    /// # Errors
    /// 1146 when `hypo` names no table of `catalog`; storage's errors.
    pub fn new(
        catalog: &'a dyn Catalog,
        storage: Arc<dyn Storage>,
        hypo: &Hypothetical,
    ) -> Result<Self> {
        let Some(base) = catalog.table(&hypo.schema, &hypo.table) else {
            return Err(sprig_exec::SqlError::new(
                1146,
                "42S02",
                format!("Table '{}.{}' doesn't exist", hypo.schema, hypo.table),
            ));
        };
        let mut def = (*base).clone();
        let hypo_key = KeyId::Index(def.indexes.len());
        def.indexes.push(hypo.index.clone());
        let def = Arc::new(def);
        let total = storage.table_rows(&base);
        let mut mem = MemDb::new(&def.schema);
        mem.create_table((*def).clone());
        let counters = Counters::new();
        let mut cursor = storage.scan(
            &base,
            KeyId::Primary,
            vec![KeyRange::all()],
            false,
            Fetch::Row,
            counters,
        )?;
        let mut sampled = 0usize;
        while sampled < hypo.sample_rows.max(1) {
            let Some(row) = cursor.next()? else { break };
            // A row the in-memory table refuses (a value its key encoding
            // can't take) is left out of the sample.
            if mem.insert(&def.name, row).is_ok() {
                sampled += 1;
            }
        }
        let complete = cursor.next()?.is_none();
        let sample = Sample {
            mem,
            sampled: sampled as u64,
            total: if complete {
                sampled as u64
            } else {
                total.max(sampled as u64)
            },
        };
        let storage: Arc<dyn Storage> = Arc::new(WhatIfStorage {
            real: storage,
            hypo_key,
            schema: def.schema.clone(),
            table: def.name.clone(),
            sample,
        });
        Ok(Self {
            catalog: WithIndex {
                inner: catalog,
                def,
            },
            storage,
        })
    }

    /// Plans `stmt` with `params` with the hypothetical index.
    ///
    /// # Errors
    /// The planner's.
    pub fn estimate(
        &self,
        session: &Session,
        options: Option<PlanOptions>,
        stmt: &ast::Statement,
        params: &[Value],
    ) -> Result<Estimate> {
        estimate(
            &self.catalog,
            Arc::clone(&self.storage),
            session,
            options,
            stmt,
            params,
        )
    }
}

/// A catalog whose one table has the hypothetical index.
struct WithIndex<'a> {
    inner: &'a dyn Catalog,
    def: Arc<TableDef>,
}

impl Catalog for WithIndex<'_> {
    fn table(&self, schema: &str, name: &str) -> Option<Arc<TableDef>> {
        let t = self.inner.table(schema, name)?;
        if t.schema.eq_ignore_ascii_case(&self.def.schema)
            && t.name.eq_ignore_ascii_case(&self.def.name)
        {
            return Some(Arc::clone(&self.def));
        }
        Some(t)
    }

    fn default_schema(&self) -> Option<String> {
        self.inner.default_schema()
    }

    fn sequence(
        &self,
        schema: &str,
        name: &str,
    ) -> std::result::Result<String, sprig_exec::SqlError> {
        self.inner.sequence(schema, name)
    }
}

/// The sampled rows of the hypothetical index's table.
struct Sample {
    mem: MemDb,
    sampled: u64,
    total: u64,
}

impl Sample {
    fn scale(&self) -> f64 {
        if self.sampled == 0 {
            1.0
        } else {
            self.total as f64 / self.sampled as f64
        }
    }

    fn complete(&self) -> bool {
        self.sampled == self.total
    }
}

/// Storage that answers the hypothetical key from the sample.
struct WhatIfStorage {
    real: Arc<dyn Storage>,
    hypo_key: KeyId,
    schema: String,
    table: String,
    sample: Sample,
}

impl WhatIfStorage {
    fn is_hypo(&self, table: &TableDef, key: KeyId) -> bool {
        key == self.hypo_key
            && table.schema.eq_ignore_ascii_case(&self.schema)
            && table.name.eq_ignore_ascii_case(&self.table)
    }

    fn not_built() -> sprig_exec::SqlError {
        sprig_exec::SqlError::new(1235, "42000", "a hypothetical index has no entries to read")
    }
}

impl Storage for WhatIfStorage {
    fn table_rows(&self, table: &TableDef) -> u64 {
        self.real.table_rows(table)
    }

    fn rows_in_range(&self, table: &TableDef, key: KeyId, range: &KeyRange) -> u64 {
        if !self.is_hypo(table, key) {
            return self.real.rows_in_range(table, key, range);
        }
        let n = self.sample.mem.rows_in_range(table, key, range);
        if self.sample.complete() {
            return n;
        }
        // Not in the sample: rarer than one sampled row stands for.
        ((n as f64 * self.sample.scale()).round() as u64).max(1)
    }

    fn rows_per_key(&self, table: &TableDef, key: KeyId, parts: usize) -> Option<f64> {
        if !self.is_hypo(table, key) {
            return self.real.rows_per_key(table, key, parts);
        }
        let per = self.sample.mem.rows_per_key(table, key, parts)?;
        if self.sample.complete() {
            return Some(per);
        }
        let n = self.sample.sampled as f64;
        let distinct = (n / per.max(1.0)).max(1.0);
        // Few values: the sample has seen them all, and each value's rows
        // grow with the table. Many: most values are yet unseen, and the
        // distinct count grows with the table too.
        let estimated_distinct = if distinct <= n / 10.0 {
            distinct
        } else {
            distinct * self.sample.scale() * (distinct / n)
        };
        Some((self.sample.total as f64 / estimated_distinct.max(1.0)).max(1.0))
    }

    fn get(&self, table: &TableDef, key: &[Value], counters: &Counters) -> Result<Option<Row>> {
        self.real.get(table, key, counters)
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
        if self.is_hypo(table, key) {
            return Err(Self::not_built());
        }
        self.real.scan(table, key, ranges, reverse, fetch, counters)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use sprig_engine::row::ColumnType;
    use sprig_exec::Persona;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    use super::*;
    use crate::catalog::{ColumnDef, IndexPart};

    /// `t(id PK, a, b)`: `a` has 100 values, `b` 10, `rows` rows; `k_b`
    /// on `b`, optionally ignored.
    fn db(rows: i64, b_ignored: bool) -> Arc<MemDb> {
        let mut db = MemDb::new("d");
        db.create_table(TableDef {
            schema: "d".into(),
            name: "t".into(),
            columns: vec![
                ColumnDef::new("id", ColumnType::int(false)).not_null(),
                ColumnDef::new("a", ColumnType::int(false)),
                ColumnDef::new("b", ColumnType::int(false)),
            ],
            primary: Some(IndexDef::new("PRIMARY", true, vec![IndexPart::asc(0)])),
            indexes: vec![IndexDef::new("k_b", false, vec![IndexPart::asc(2)]).ignored(b_ignored)],
        });
        for id in 0..rows {
            db.insert(
                "t",
                vec![Value::Int(id), Value::Int(id % 100), Value::Int(id % 10)],
            )
            .unwrap();
        }
        Arc::new(db)
    }

    fn stmt(sql: &str) -> ast::Statement {
        Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0)
    }

    fn hypo(col: usize, sample_rows: usize) -> Hypothetical {
        Hypothetical {
            schema: "d".into(),
            table: "t".into(),
            index: IndexDef::new("sprig_auto_test", false, vec![IndexPart::asc(col)]),
            sample_rows,
        }
    }

    #[test]
    fn an_index_on_the_filtered_column_cuts_rows_examined() {
        let db = db(5000, false);
        let s = Session::new(Persona::MariaDb);
        let q = stmt("SELECT id FROM t WHERE a = ?");
        let p = [Value::Int(7)];
        let before = estimate(&*db, db.clone(), &s, None, &q, &p).unwrap();
        assert!(before.examined >= 5000.0, "{before:?}");
        assert!(!before.reads("sprig_auto_test"));
        // A sample of a fifth of the table, scaled up.
        let after = estimate_with(&*db, db.clone(), &s, None, &q, &p, &hypo(1, 1000)).unwrap();
        assert!(after.reads("sprig_auto_test"), "{after:?}");
        assert!(after.examined <= 200.0, "{after:?}");
        // The whole table sampled: the exact count.
        let exact = estimate_with(&*db, db.clone(), &s, None, &q, &p, &hypo(1, 10_000)).unwrap();
        assert_eq!(exact.examined, 50.0, "{exact:?}");
    }

    #[test]
    fn an_index_the_filter_cant_use_is_not_read() {
        let db = db(2000, false);
        let s = Session::new(Persona::MariaDb);
        let q = stmt("SELECT id FROM t WHERE a = 3");
        // An index on `b` doesn't serve `a = 3`.
        let e = estimate_with(&*db, db.clone(), &s, None, &q, &[], &hypo(2, 500)).unwrap();
        assert!(!e.reads("sprig_auto_test"), "{e:?}");
        assert!(e.examined >= 2000.0);
    }

    #[test]
    fn ignored_keys_are_never_planned() {
        let s = Session::new(Persona::MariaDb);
        let q = stmt("SELECT id FROM t WHERE b = 3");
        let visible = db(2000, false);
        let e = estimate(&*visible, visible.clone(), &s, None, &q, &[]).unwrap();
        assert!(e.reads("k_b"), "{e:?}");
        let hidden = db(2000, true);
        let e = estimate(&*hidden, hidden.clone(), &s, None, &q, &[]).unwrap();
        assert!(!e.reads("k_b"), "{e:?}");
        assert!(e.examined >= 2000.0);
    }

    #[test]
    fn joins_multiply_the_inner_reads() {
        let rows = vec![
            ExplainRow {
                rows: Some(10),
                ..ExplainRow::new(1, "SIMPLE", "a")
            },
            ExplainRow {
                rows: Some(3),
                ..ExplainRow::new(1, "SIMPLE", "b")
            },
            ExplainRow {
                rows: Some(5),
                ..ExplainRow::new(2, "SUBQUERY", "c")
            },
        ];
        assert_eq!(examined_of(&rows), 10.0 + 30.0 + 5.0);
    }
}
