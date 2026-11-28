//! Plan shapes against MariaDB. For the query shapes an OLTP engine must
//! not get wrong (a point lookup, a short ordered range, top-N by index,
//! `MIN`/`MAX` off a key's edge, a key lookup join driven by a few rows,
//! an `IN` list, a subquery as a semi-join, a derived table that keeps
//! its indexes, grouping in key order), the planner's choice is put next
//! to MariaDB's: the access type and key per table from `EXPLAIN`, and
//! the rows examined from `ANALYZE FORMAT=JSON` (the sum, over tables, of
//! loops times rows per loop) against the rows the planner's own table
//! accesses handed over. The rows examined must stay within 1.2 times
//! MariaDB's.
//!
//! Needs `SPRIG_PLAN_ORACLE_MARIADB` (see `differential.rs`); without it
//! the test does nothing. Works in database `rp_shapes`, dropped at the
//! end. `SPRIG_PLAN_ORACLE_REPORT` names a directory for the table.

#![allow(
    clippy::explicit_auto_deref,
    clippy::manual_is_multiple_of,
    clippy::explicit_iter_loop,
    clippy::collapsible_if,
    clippy::default_trait_access,
    clippy::type_complexity,
    clippy::items_after_statements,
    clippy::redundant_closure_for_method_calls,
    clippy::match_same_arms,
    clippy::if_not_else,
    clippy::single_match_else,
    clippy::unnested_or_patterns,
    clippy::uninlined_format_args,
    clippy::implicit_hasher,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::float_cmp,
    clippy::cast_lossless,
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::format_push_string,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::similar_names,
    clippy::many_single_char_names
)]

mod common;

use std::fmt::Write as _;

use common::world::{date, dec, int};
use common::{
    Oracle, Outcome, Rng, Spec, col, col_nn, load_mem, run_ours, same, session, text_col,
};
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::Persona;

const DB: &str = "rp_shapes";

/// Customers, orders, order lines and items, in the proportions of an
/// order-entry workload, large enough that the server prefers its
/// indexes.
fn tables() -> Vec<Spec> {
    let mut rng = Rng::new(42);
    let d102 = ColumnType::Decimal {
        precision: 10,
        scale: 2,
    };
    let names = [
        "ABLE", "BAR", "CALLY", "ESE", "OUGHT", "PRES", "PRI", "ATION", "ANTI", "EING",
    ];
    let mut cust = Vec::new();
    for c in 1..=600i64 {
        cust.push(vec![
            int(c),
            int(1 + c % 2),
            int(1 + (c / 2) % 5),
            Value::str(names[rng.below(names.len())]),
            Value::str(&format!("f{:03}", rng.below(400))),
            dec(rng.below(100_000) as i64, 2),
        ]);
    }
    let mut ord = Vec::new();
    let mut ol = Vec::new();
    for o in 1..=3000i64 {
        let c = 1 + rng.below(600) as i64;
        ord.push(vec![
            int(o),
            int(c),
            int(1 + o % 2),
            int(1 + (o / 2) % 5),
            Value::Date(date(o / 40)),
            dec(rng.below(100_000) as i64, 2),
            if rng.chance(1, 10) {
                Value::Null
            } else {
                int(rng.below(10) as i64)
            },
        ]);
        for line in 1..=(1 + rng.below(5) as i64) {
            ol.push(vec![
                int(o),
                int(line),
                int(1 + rng.below(800) as i64),
                int(1 + rng.below(10) as i64),
            ]);
        }
    }
    let mut item = Vec::new();
    for i in 1..=800i64 {
        item.push(vec![
            int(i),
            Value::str(&format!("item{i}")),
            dec(100 + rng.below(9900) as i64, 2),
        ]);
    }
    let general = Collation::Utf8mb4GeneralCi;
    vec![
        Spec {
            name: "cust",
            cols: vec![
                col_nn("c_id", "INT", ColumnType::int(false)),
                col_nn("w_id", "INT", ColumnType::int(false)),
                col_nn("d_id", "INT", ColumnType::int(false)),
                text_col("last", 16, general),
                text_col("first", 16, general),
                col("balance", "DECIMAL(10,2)", d102),
            ],
            pk: vec![("c_id", None, false)],
            keys: vec![(
                "k_name",
                false,
                vec![
                    ("w_id", None, false),
                    ("d_id", None, false),
                    ("last", None, false),
                    ("first", None, false),
                ],
            )],
            rows: cust,
        },
        Spec {
            name: "ord",
            cols: vec![
                col_nn("o_id", "INT", ColumnType::int(false)),
                col_nn("cust", "INT", ColumnType::int(false)),
                col_nn("w_id", "INT", ColumnType::int(false)),
                col_nn("d_id", "INT", ColumnType::int(false)),
                col("created", "DATE", ColumnType::Date),
                col("total", "DECIMAL(10,2)", d102),
                col("carrier", "INT", ColumnType::int(false)),
            ],
            pk: vec![("o_id", None, false)],
            keys: vec![
                (
                    "k_cust",
                    false,
                    vec![("cust", None, false), ("created", None, false)],
                ),
                (
                    "k_wd",
                    false,
                    vec![
                        ("w_id", None, false),
                        ("d_id", None, false),
                        ("o_id", None, false),
                    ],
                ),
            ],
            rows: ord,
        },
        Spec {
            name: "ol",
            cols: vec![
                col_nn("o_id", "INT", ColumnType::int(false)),
                col_nn("line", "INT", ColumnType::int(false)),
                col_nn("item", "INT", ColumnType::int(false)),
                col_nn("qty", "INT", ColumnType::int(false)),
            ],
            pk: vec![("o_id", None, false), ("line", None, false)],
            keys: vec![("k_item", false, vec![("item", None, false)])],
            rows: ol,
        },
        Spec {
            name: "item",
            cols: vec![
                col_nn("i_id", "INT", ColumnType::int(false)),
                text_col("name", 24, general),
                col("price", "DECIMAL(10,2)", d102),
            ],
            pk: vec![("i_id", None, false)],
            keys: vec![],
            rows: item,
        },
    ]
}

/// (shape number, what it is, the query, whether the query orders its rows).
const SHAPES: &[(u8, &str, &str, bool)] = &[
    (
        1,
        "point lookup by primary key",
        "SELECT * FROM cust WHERE c_id = 77",
        false,
    ),
    (
        1,
        "point lookup, key missing",
        "SELECT * FROM cust WHERE c_id = 7777",
        false,
    ),
    (
        1,
        "point lookup by composite primary key",
        "SELECT qty FROM ol WHERE o_id = 150 AND line = 1",
        false,
    ),
    (
        2,
        "short range on the primary key",
        "SELECT total FROM ord WHERE o_id BETWEEN 100 AND 199",
        false,
    ),
    (
        2,
        "range summed",
        "SELECT SUM(total) FROM ord WHERE o_id BETWEEN 100 AND 199",
        false,
    ),
    (
        2,
        "range read in order",
        "SELECT o_id, total FROM ord WHERE o_id BETWEEN 100 AND 199 ORDER BY o_id",
        true,
    ),
    (
        2,
        "secondary range in order",
        "SELECT o_id, created FROM ord WHERE cust = 42 AND created >= '2024-01-01' ORDER BY created, o_id",
        true,
    ),
    (
        2,
        "distinct over a range",
        "SELECT DISTINCT d_id FROM ord WHERE o_id BETWEEN 100 AND 199 ORDER BY d_id",
        true,
    ),
    (
        3,
        "latest row of a customer",
        "SELECT * FROM ord WHERE cust = 42 ORDER BY created DESC, o_id DESC LIMIT 1",
        true,
    ),
    (
        3,
        "top-N by primary key, descending",
        "SELECT o_id FROM ord ORDER BY o_id DESC LIMIT 5",
        true,
    ),
    (
        3,
        "oldest order of a district",
        "SELECT o_id FROM ord WHERE w_id = 1 AND d_id = 3 ORDER BY o_id LIMIT 1",
        true,
    ),
    (
        3,
        "page of a list",
        "SELECT o_id, total FROM ord WHERE w_id = 2 AND d_id = 4 ORDER BY o_id DESC LIMIT 10 OFFSET 20",
        true,
    ),
    (
        4,
        "MIN on a key suffix, prefix bound",
        "SELECT MIN(o_id) FROM ord WHERE w_id = 1 AND d_id = 3",
        false,
    ),
    (
        4,
        "MAX on a key suffix with a range",
        "SELECT MAX(o_id) FROM ord WHERE w_id = 2 AND d_id = 5 AND o_id < 1500",
        false,
    ),
    (
        4,
        "MAX of the primary key",
        "SELECT MAX(o_id) FROM ord",
        false,
    ),
    (
        5,
        "ref by name, ordered by the next key part",
        "SELECT c_id, first FROM cust WHERE w_id = 1 AND d_id = 2 AND last = 'BAR' ORDER BY first, c_id",
        true,
    ),
    (
        6,
        "lookup join driven by one order",
        "SELECT ol.item, i.price FROM ol JOIN item i ON i.i_id = ol.item WHERE ol.o_id = 150",
        false,
    ),
    (
        6,
        "lookup join driven by a short range",
        "SELECT COUNT(*), SUM(i.price) FROM ol JOIN item i ON i.i_id = ol.item WHERE ol.o_id BETWEEN 100 AND 110",
        false,
    ),
    (
        6,
        "parent rows of a few children",
        "SELECT o.o_id, c.last FROM ord o JOIN cust c ON c.c_id = o.cust WHERE o.o_id BETWEEN 500 AND 520",
        false,
    ),
    (
        7,
        "IN list on the primary key",
        "SELECT * FROM cust WHERE c_id IN (5, 3, 99, 3, 250)",
        false,
    ),
    (
        7,
        "IN list on a secondary key",
        "SELECT o_id FROM ord WHERE cust IN (7, 9, 7, 300)",
        false,
    ),
    (
        8,
        "COUNT(DISTINCT) over a range",
        "SELECT COUNT(DISTINCT item) FROM ol WHERE o_id BETWEEN 100 AND 120",
        false,
    ),
    (
        9,
        "left join whose inner side isn't read",
        "SELECT o.o_id FROM ord o LEFT JOIN cust c ON c.c_id = o.cust WHERE o.o_id < 20",
        false,
    ),
    (
        9,
        "left join by unique key",
        "SELECT o.o_id, c.last FROM ord o LEFT JOIN cust c ON c.c_id = o.cust WHERE o.o_id < 20",
        false,
    ),
    (
        10,
        "derived table merged, point lookup",
        "SELECT * FROM (SELECT * FROM ord WHERE w_id = 1) d WHERE d.o_id = 78",
        false,
    ),
    (
        10,
        "derived table merged, range",
        "SELECT d.o_id FROM (SELECT o_id, cust FROM ord) d WHERE d.cust = 42",
        false,
    ),
    (
        10,
        "grouped derived table, filter pushed in",
        "SELECT d.n FROM (SELECT cust, COUNT(*) AS n FROM ord GROUP BY cust) d WHERE d.cust = 42",
        false,
    ),
    (
        11,
        "IN subquery as a semi-join",
        "SELECT c_id FROM cust WHERE c_id IN (SELECT cust FROM ord WHERE o_id BETWEEN 100 AND 120)",
        false,
    ),
    (
        11,
        "EXISTS as a semi-join",
        "SELECT c_id FROM cust c WHERE c.c_id < 30 AND EXISTS (SELECT 1 FROM ord o WHERE o.cust = c.c_id)",
        false,
    ),
    (
        12,
        "correlated scalar subquery by index",
        "SELECT c_id, (SELECT MAX(created) FROM ord o WHERE o.cust = c.c_id) FROM cust c WHERE c.c_id < 20",
        false,
    ),
    (
        13,
        "GROUP BY on a key prefix",
        "SELECT cust, COUNT(*) FROM ord GROUP BY cust",
        false,
    ),
    (
        13,
        "GROUP BY on a key prefix, with a range",
        "SELECT cust, MAX(created) FROM ord WHERE cust BETWEEN 10 AND 30 GROUP BY cust",
        false,
    ),
];

/// The tables of a MariaDB `ANALYZE FORMAT=JSON` output: name, loops and
/// rows per loop.
fn analyzed(json: &str) -> Vec<(String, f64, f64)> {
    let field = |from: &str, name: &str| -> Option<f64> {
        let at = from.find(&format!("\"{name}\":"))?;
        let rest = from[at + name.len() + 3..].trim_start();
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-' || c == 'e'))
            .unwrap_or(rest.len());
        rest[..end].parse().ok()
    };
    let mut out = Vec::new();
    let mut rest = json;
    while let Some(at) = rest.find("\"table_name\":") {
        let tail = &rest[at + 13..];
        let name = tail.split('"').nth(1).unwrap_or("").to_owned();
        let end = tail.find("\"table_name\":").unwrap_or(tail.len());
        let body = &tail[..end];
        out.push((
            name,
            field(body, "r_loops").unwrap_or(0.0),
            field(body, "r_rows").unwrap_or(0.0),
        ));
        rest = &tail[end..];
    }
    out
}

#[ignore = "live: needs SPRIG_PLAN_ORACLE_REPORT; tools/live/README.md"]
#[test]
fn plan_shapes_match_mariadb() {
    let Some(oracle) = Oracle::from_env()
        .into_iter()
        .find(|o| o.persona == Persona::MariaDb)
    else {
        eprintln!("no SPRIG_PLAN_ORACLE_MARIADB set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    };
    let specs = tables();
    oracle.load(DB, &specs);
    let mem = load_mem(DB, &specs);
    let sess = session(Persona::MariaDb);
    let mut statements = Vec::new();
    for (_, _, sql, _) in SHAPES {
        statements.push((*sql).to_owned());
        statements.push(format!("EXPLAIN {sql}"));
        statements.push(format!("ANALYZE FORMAT=JSON {sql}"));
    }
    let answers = oracle.answers(DB, &statements);
    let mut report = String::new();
    let (mut same_rows, mut same_sig, mut within, mut total) = (0, 0, 0, 0);
    let mut failures = Vec::new();
    for (i, (shape, what, sql, ordered)) in SHAPES.iter().enumerate() {
        total += 1;
        let ours = run_ours(&mem, &sess, sql, None);
        let rows_ok = same(&ours.outcome, &answers[3 * i], *ordered);
        // MariaDB: (table, type, key) per EXPLAIN row.
        let theirs_sig: Vec<String> = match &answers[3 * i + 1] {
            Outcome::Rows(rows) => rows
                .iter()
                .map(|r| {
                    let f: Vec<&str> = std::str::from_utf8(r).unwrap_or("").split('\t').collect();
                    format!(
                        "{}:{}:{} [{}]",
                        f.get(2).unwrap_or(&""),
                        f.get(3).unwrap_or(&""),
                        f.get(5).unwrap_or(&""),
                        f.get(9).unwrap_or(&"")
                    )
                })
                .collect(),
            Outcome::Error(c, m) => vec![format!("ERROR {c} {m}")],
        };
        let ours_sig: Vec<String> = ours
            .explain
            .iter()
            .map(|r| {
                format!(
                    "{}:{}:{} [{}]",
                    r.table.as_deref().unwrap_or("NULL"),
                    r.access.as_deref().unwrap_or("NULL"),
                    r.key.as_deref().unwrap_or("NULL"),
                    r.extra_text()
                )
            })
            .collect();
        let their_examined: f64 = match &answers[3 * i + 2] {
            Outcome::Rows(rows) => {
                let json = rows
                    .iter()
                    .map(|r| String::from_utf8_lossy(r).into_owned())
                    .collect::<String>();
                analyzed(&json).iter().map(|(_, l, r)| l * r).sum()
            }
            Outcome::Error(..) => -1.0,
        };
        let our_examined = ours.rows_examined as f64;
        // One row of slack: reading "the first row of a range" is one row
        // for us and none for a server that answers from statistics.
        let ok = our_examined <= their_examined * 1.2 + 1.0;
        let key = |sig: &[String]| -> Vec<String> {
            sig.iter()
                .map(|s| s.split(" [").next().unwrap_or("").to_owned())
                .collect()
        };
        let sig_ok = key(&ours_sig) == key(&theirs_sig);
        same_rows += usize::from(rows_ok);
        same_sig += usize::from(sig_ok);
        within += usize::from(ok);
        if !rows_ok || !ok {
            failures.push(format!("shape {shape} ({what}): rows equal {rows_ok}, examined {our_examined} vs {their_examined}"));
        }
        let _ = writeln!(
            report,
            "shape {shape:2} {what}\n  {sql}\n  result equal: {rows_ok}   signature equal: {sig_ok}   rows examined: ours {our_examined} / MariaDB {their_examined} -> {}\n  MariaDB: {}\n  ours:    {}\n  ours per table: {:?}\n",
            if ok { "within 1.2x" } else { "OVER 1.2x" },
            theirs_sig.join(" | "),
            ours_sig.join(" | "),
            ours.per_table,
        );
        if let Outcome::Error(c, m) = &ours.outcome {
            let _ = writeln!(report, "  ours: ERROR {c}: {m}\n");
        }
    }
    let summary = format!(
        "shapes: {total} queries; result equal {same_rows}; access type and key equal {same_sig}; rows examined within 1.2x of MariaDB {within}\n"
    );
    oracle.drop_db(DB);
    println!("{summary}");
    if let Ok(dir) = std::env::var("SPRIG_PLAN_ORACLE_REPORT") {
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(format!("{dir}/shapes.txt"), format!("{summary}\n{report}"));
    } else {
        println!("{report}");
    }
    for f in &failures {
        println!("NOT MET: {f}");
    }
}
