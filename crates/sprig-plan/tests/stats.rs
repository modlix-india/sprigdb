//! The planner with storage that keeps statistics: conditions on columns
//! without an index get their share of rows from a histogram, so the
//! join order follows the data instead of fixed guesses.

#![allow(clippy::unwrap_used, clippy::cast_possible_wrap)]

mod common;

use std::sync::Arc;

use common::world::int;
use common::{Outcome, Spec, col, col_nn, load_mem, run_ours, session};
use sprig_engine::row::ColumnType;
use sprig_exec::Persona;
use sprig_plan::mem::MemDb;

/// `a(id, kind)`: 2,000 rows, `kind` 1 on all but four. `b(id, a_id, tag)`
/// with an index on `a_id` (each `a` row has one `b` row): 2,000 rows,
/// `tag` 7 on 99% of them. Neither `kind` nor `tag` has an index.
fn db(stats: bool) -> Arc<MemDb> {
    let a = Spec {
        name: "a",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("kind", "INT", ColumnType::int(false)),
        ],
        pk: vec![("id", None, false)],
        keys: vec![],
        rows: (0..2000)
            .map(|i| vec![int(i), int(i64::from(i % 500 != 3))])
            .collect(),
    };
    let b = Spec {
        name: "b",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("a_id", "INT", ColumnType::int(false)),
            col("tag", "INT", ColumnType::int(false)),
        ],
        pk: vec![("id", None, false)],
        keys: vec![("k_a", false, vec![("a_id", None, false)])],
        rows: (0..2000)
            .map(|i| {
                vec![
                    int(i),
                    int((i * 7) % 2000),
                    int(if i % 100 == 0 { i % 5 } else { 7 }),
                ]
            })
            .collect(),
    };
    let db = load_mem("d", &[a, b]);
    Arc::new((*db).clone().with_stats(stats))
}

fn plan(stats: bool, sql: &str) -> (Vec<Vec<u8>>, String, u64) {
    let out = run_ours(&db(stats), &session(Persona::MariaDb), sql, None);
    let Outcome::Rows(rows) = out.outcome else {
        panic!("{sql}: {:?}", out.outcome)
    };
    let sig = out
        .explain
        .iter()
        .map(|r| {
            format!(
                "{}:{}:{}",
                r.table.as_deref().unwrap_or("-"),
                r.access.as_deref().unwrap_or("-"),
                r.key.as_deref().unwrap_or("-")
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    (rows, sig, out.rows_examined)
}

#[test]
fn a_rare_value_on_a_column_without_an_index_leads_the_join() {
    let sql = "SELECT a.id, b.id FROM a JOIN b ON b.a_id = a.id WHERE a.kind = 0 AND b.tag = 7 \
               ORDER BY 1, 2";
    let (guess_rows, guess_sig, guess_examined) = plan(false, sql);
    let (rows, sig, examined) = plan(true, sql);
    assert_eq!(rows, guess_rows, "same answer either way");
    assert_eq!(rows.len(), 4);
    // With the histograms `a.kind = 0` keeps 4 rows of 2,000 and `b.tag =
    // 7` nearly all: `a` is read first and `b` reached by its index (the
    // guesses, a tenth each, read `b` first and look up `a` 1,980 times).
    assert!(sig.starts_with("a:ALL"), "{sig}");
    assert!(sig.contains("b:ref:k_a"), "{sig}");
    assert!(
        examined < guess_examined,
        "statistics {examined} ({sig}) vs guesses {guess_examined} ({guess_sig})"
    );
}
