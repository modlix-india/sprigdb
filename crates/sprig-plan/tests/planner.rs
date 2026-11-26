//! The planner on two small hand-written tables: results that can be
//! checked by eye, the access path chosen for each statement shape, and
//! the rows each plan examines. No server is needed.

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
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

mod common;

use std::sync::Arc;

use common::world::{dec, int};
use common::{Outcome, Spec, col, col_nn, load_mem, run_ours, run_ours_params, session, text_col};
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Value};
use sprig_exec::Persona;
use sprig_plan::PlanOptions;
use sprig_plan::mem::MemDb;

/// `emp(id, dept, name, sal)` with indexes on `dept` and `name`, and
/// `dept(id, title)` with a unique index on `title`. Employee 4 has no
/// department, employee 6 a department that doesn't exist, and
/// department 40 has no employee.
fn db() -> Arc<MemDb> {
    let emp = Spec {
        name: "emp",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            col("dept", "INT", ColumnType::int(false)),
            text_col("name", 10, Collation::Utf8mb4GeneralCi),
            col(
                "sal",
                "DECIMAL(8,2)",
                ColumnType::Decimal {
                    precision: 8,
                    scale: 2,
                },
            ),
        ],
        pk: vec![("id", None, false)],
        keys: vec![
            ("k_dept", false, vec![("dept", None, false)]),
            ("k_name", false, vec![("name", None, false)]),
        ],
        rows: vec![
            vec![int(1), int(10), Value::str("Ann"), dec(10000, 2)],
            vec![int(2), int(10), Value::str("bob"), dec(20000, 2)],
            vec![int(3), int(20), Value::str("Cy"), Value::Null],
            vec![int(4), Value::Null, Value::str("dee"), dec(5000, 2)],
            vec![int(5), int(20), Value::str("ann"), dec(30000, 2)],
            vec![int(6), int(30), Value::str("Eve"), dec(15000, 2)],
        ],
    };
    let dept = Spec {
        name: "dept",
        cols: vec![
            col_nn("id", "INT", ColumnType::int(false)),
            text_col("title", 10, Collation::Utf8mb4Bin),
        ],
        pk: vec![("id", None, false)],
        keys: vec![("uq_title", true, vec![("title", None, false)])],
        rows: vec![
            vec![int(10), Value::str("eng")],
            vec![int(20), Value::str("ops")],
            vec![int(40), Value::str("hr")],
        ],
    };
    load_mem("d", &[emp, dept])
}

/// The rows of a statement, the plan's signature (`table:type:key` per
/// table access) and the rows examined.
fn run(sql: &str) -> (Vec<String>, String, u64) {
    let db = db();
    let out = run_ours(&db, &session(Persona::MariaDb), sql, None);
    let rows = match &out.outcome {
        Outcome::Rows(rows) => rows
            .iter()
            .map(|r| String::from_utf8_lossy(r).replace('\t', "|"))
            .collect(),
        Outcome::Error(code, msg) => panic!("{sql}: error {code}: {msg}"),
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

fn error(persona: Persona, sql: &str) -> (u16, String) {
    match run_ours(&db(), &session(persona), sql, None).outcome {
        Outcome::Error(code, msg) => (code, msg),
        Outcome::Rows(rows) => panic!("{sql}: {} rows, no error", rows.len()),
    }
}

#[test]
fn point_lookups_read_one_row() {
    let (rows, sig, examined) = run("SELECT name FROM emp WHERE id = 3");
    assert_eq!(rows, ["Cy"]);
    assert_eq!(sig, "emp:const:PRIMARY");
    assert_eq!(examined, 1);
    // A key that isn't there, a constant no integer equals, NULL.
    for sql in [
        "SELECT name FROM emp WHERE id = 99",
        "SELECT name FROM emp WHERE id = 2.5",
        "SELECT name FROM emp WHERE id = NULL",
    ] {
        let (rows, _, examined) = run(sql);
        assert!(rows.is_empty(), "{sql}");
        assert_eq!(examined, 0, "{sql}");
    }
    // The unique index, compared by bytes: 'OPS' is another title.
    let (rows, sig, examined) = run("SELECT id FROM dept WHERE title = 'ops'");
    assert_eq!((rows, examined), (vec!["20".to_owned()], 1));
    assert_eq!(sig, "dept:const:uq_title");
    assert!(run("SELECT id FROM dept WHERE title = 'OPS'").0.is_empty());
    // A pattern with a literal start is a range of a key whose strings
    // order by code point.
    let (rows, sig, examined) = run("SELECT id FROM dept WHERE title LIKE 'o%'");
    assert_eq!((rows, examined), (vec!["20".to_owned()], 1));
    assert_eq!(sig, "dept:range:uq_title");
    let (rows, _, examined) = run("SELECT id FROM dept WHERE title LIKE 'e_g' OR title LIKE 'h%'");
    assert_eq!(rows.len(), 2);
    assert!(examined <= 3);
}

#[test]
fn secondary_indexes_serve_equalities_lists_and_ranges() {
    let (rows, sig, examined) = run("SELECT id FROM emp WHERE dept = 10 ORDER BY id");
    assert_eq!(rows, ["1", "2"]);
    assert_eq!(sig, "emp:ref:k_dept");
    assert_eq!(examined, 2);
    // The index on `name` compares without case.
    let (rows, sig, examined) = run("SELECT id FROM emp WHERE name = 'ANN' ORDER BY id");
    assert_eq!(rows, ["1", "5"]);
    assert_eq!(sig, "emp:ref:k_name");
    assert_eq!(examined, 2);
    // A list with a duplicate: two probes, in key order.
    let (rows, sig, examined) = run("SELECT id FROM emp WHERE dept IN (20, 10, 20) AND id <> 1");
    assert_eq!(rows, ["2", "3", "5"]);
    assert_eq!(sig, "emp:range:k_dept");
    assert_eq!(examined, 4);
    // The index entries end with the primary key: a range on it narrows
    // the entries of one department.
    let (rows, sig, examined) = run("SELECT id FROM emp WHERE dept = 20 AND id > 3");
    assert_eq!(rows, ["5"]);
    assert_eq!(sig, "emp:range:k_dept");
    assert_eq!(examined, 1);
    let (rows, _, examined) = run("SELECT id FROM emp WHERE dept IS NULL");
    assert_eq!((rows, examined), (vec!["4".to_owned()], 1));
    let (rows, _, examined) = run("SELECT id FROM emp WHERE dept > 10 AND dept <= 30 ORDER BY id");
    assert_eq!(rows, ["3", "5", "6"]);
    assert_eq!(examined, 3);
    // A string that is plainly a number uses the index like the number;
    // any other string is compared row by row.
    let (rows, sig, examined) = run("SELECT id FROM emp WHERE dept = '10' ORDER BY id");
    assert_eq!(rows, ["1", "2"]);
    assert_eq!(sig, "emp:ref:k_dept");
    assert_eq!(examined, 2);
    let (rows, sig, _) = run("SELECT id FROM emp WHERE dept = '10 ' ORDER BY id");
    assert_eq!(rows, ["1", "2"]);
    assert_eq!(sig, "emp:index:k_dept");
}

#[test]
fn an_index_gives_the_order_and_a_limit_stops_the_scan() {
    let (rows, sig, examined) = run("SELECT id FROM emp ORDER BY id DESC LIMIT 2");
    assert_eq!(rows, ["6", "5"]);
    assert_eq!(sig, "emp:index:PRIMARY");
    assert_eq!(examined, 2);
    let (rows, _, examined) =
        run("SELECT id, dept FROM emp WHERE dept IS NOT NULL ORDER BY dept DESC, id DESC LIMIT 3");
    assert_eq!(rows, ["6|30", "5|20", "3|20"]);
    assert_eq!(examined, 3);
    let (rows, _, examined) = run("SELECT id FROM emp WHERE dept = 20 ORDER BY id DESC LIMIT 1");
    assert_eq!((rows, examined), (vec!["5".to_owned()], 1));
    // An order no index gives is sorted.
    let (rows, _, examined) = run("SELECT id FROM emp ORDER BY sal DESC, id LIMIT 2");
    assert_eq!(rows, ["5", "2"]);
    assert_eq!(examined, 6);
}

#[test]
fn min_and_max_come_off_the_edge_of_a_key() {
    let (rows, sig, examined) = run("SELECT MIN(dept), 1 FROM emp");
    assert_eq!(rows, ["10|1"]);
    assert_eq!(sig, "-:-:-");
    assert_eq!(examined, 1);
    let (rows, _, examined) = run("SELECT MAX(id) FROM emp WHERE dept = 20");
    assert_eq!((rows, examined), (vec!["5".to_owned()], 1));
    let (rows, _, examined) = run("SELECT MAX(dept) FROM emp WHERE dept < 30");
    assert_eq!((rows, examined), (vec!["20".to_owned()], 1));
    let (rows, _, examined) = run("SELECT MAX(dept) FROM emp WHERE dept > 99");
    assert_eq!((rows, examined), (vec!["NULL".to_owned()], 0));
    // Not off a key: every row is read.
    let (rows, _, examined) = run("SELECT MAX(sal), MIN(id), COUNT(*) FROM emp");
    assert_eq!(rows, ["300.00|1|6"]);
    assert_eq!(examined, 6);
}

#[test]
fn joins_keep_unmatched_rows_only_when_asked() {
    let (rows, _, _) =
        run("SELECT e.name, d.title FROM emp e JOIN dept d ON d.id = e.dept ORDER BY e.id");
    assert_eq!(rows, ["Ann|eng", "bob|eng", "Cy|ops", "ann|ops"]);
    let (rows, _, _) =
        run("SELECT e.id, d.title FROM emp e LEFT JOIN dept d ON d.id = e.dept ORDER BY e.id");
    assert_eq!(
        rows,
        ["1|eng", "2|eng", "3|ops", "4|NULL", "5|ops", "6|NULL"]
    );
    // The department nobody is in.
    let (rows, _, _) =
        run("SELECT d.id FROM dept d LEFT JOIN emp e ON e.dept = d.id WHERE e.id IS NULL");
    assert_eq!(rows, ["40"]);
    let (rows, _, _) = run(
        "SELECT d.id, e.id FROM emp e RIGHT JOIN dept d ON e.dept = d.id AND e.sal > 150 ORDER BY d.id, e.id",
    );
    assert_eq!(rows, ["10|2", "20|5", "40|NULL"]);
    // A WHERE condition on the inner side makes it an inner join.
    let (rows, _, _) = run(
        "SELECT e.id FROM emp e LEFT JOIN dept d ON d.id = e.dept WHERE d.title = 'ops' ORDER BY e.id",
    );
    assert_eq!(rows, ["3", "5"]);
    let (rows, _, _) = run("SELECT emp.id, title FROM emp JOIN dept USING (id) ORDER BY 1");
    assert!(rows.is_empty());
    let (rows, _, _) = run("SELECT COUNT(*) FROM emp, dept");
    assert_eq!(rows, ["18"]);
}

#[test]
fn a_left_join_nobody_reads_is_not_read() {
    let (rows, sig, examined) = run("SELECT e.id FROM emp e LEFT JOIN dept d ON d.id = e.dept");
    assert_eq!(rows.len(), 6);
    assert_eq!(sig, "e:index:k_dept");
    assert_eq!(examined, 6);
    // Read as soon as a column of it is shown.
    let (_, sig, _) = run("SELECT e.id, d.title FROM emp e LEFT JOIN dept d ON d.id = e.dept");
    assert!(sig.contains("d:"), "{sig}");
}

#[test]
fn grouping_and_distinct() {
    let (rows, _, _) =
        run("SELECT dept, COUNT(*), SUM(sal), COUNT(sal) FROM emp GROUP BY dept ORDER BY dept");
    assert_eq!(
        rows,
        [
            "NULL|1|50.00|1",
            "10|2|300.00|2",
            "20|2|300.00|1",
            "30|1|150.00|1"
        ]
    );
    let (rows, _, _) =
        run("SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept HAVING n > 1 ORDER BY dept DESC");
    assert_eq!(rows, ["20|2", "10|2"]);
    // 'Ann' and 'ann' are one name under the column's collation.
    let (rows, _, _) = run("SELECT COUNT(DISTINCT name), COUNT(DISTINCT dept) FROM emp");
    assert_eq!(rows, ["5|3"]);
    let (rows, _, _) = run("SELECT DISTINCT dept FROM emp ORDER BY dept");
    assert_eq!(rows, ["NULL", "10", "20", "30"]);
    let (rows, _, _) = run("SELECT COUNT(*) FROM emp WHERE id > 100");
    assert_eq!(rows, ["0"]);
    let (rows, _, _) = run("SELECT SUM(sal) FROM emp WHERE 1 = 0");
    assert_eq!(rows, ["NULL"]);
}

#[test]
fn subqueries_derived_tables_and_unions() {
    let (rows, _, _) = run(
        "SELECT id FROM emp WHERE dept IN (SELECT id FROM dept WHERE title = 'ops') ORDER BY id",
    );
    assert_eq!(rows, ["3", "5"]);
    // NOT IN with a NULL in the list is never true.
    let (rows, _, _) = run("SELECT id FROM dept WHERE id NOT IN (SELECT dept FROM emp)");
    assert!(rows.is_empty());
    let (rows, _, _) =
        run("SELECT id FROM dept WHERE id NOT IN (SELECT dept FROM emp WHERE dept IS NOT NULL)");
    assert_eq!(rows, ["40"]);
    let (rows, _, _) =
        run("SELECT id FROM dept d WHERE NOT EXISTS (SELECT 1 FROM emp e WHERE e.dept = d.id)");
    assert_eq!(rows, ["40"]);
    let (rows, _, _) = run(
        "SELECT id FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept = d.id AND e.sal > 250)",
    );
    assert_eq!(rows, ["20"]);
    let (rows, _, _) = run(
        "SELECT d.id, (SELECT COUNT(*) FROM emp e WHERE e.dept = d.id), (SELECT MAX(e.sal) FROM emp e WHERE e.dept = d.id) FROM dept d ORDER BY d.id",
    );
    assert_eq!(rows, ["10|2|200.00", "20|2|300.00", "40|0|NULL"]);
    let (rows, _, _) = run("SELECT name FROM emp WHERE sal = (SELECT MAX(sal) FROM emp)");
    assert_eq!(rows, ["ann"]);
    let (rows, _, _) = run(
        "SELECT x.dept, x.n FROM (SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept) x WHERE x.dept = 20",
    );
    assert_eq!(rows, ["20|2"]);
    let (rows, _, _) = run(
        "WITH top AS (SELECT id, sal FROM emp WHERE sal >= 150) SELECT COUNT(*), MIN(id) FROM top",
    );
    assert_eq!(rows, ["3|2"]);
    let (rows, _, _) = run("SELECT id FROM dept UNION SELECT dept FROM emp ORDER BY id");
    assert_eq!(rows, ["NULL", "10", "20", "30", "40"]);
    let (rows, _, _) =
        run("SELECT id FROM dept UNION ALL SELECT dept FROM emp WHERE dept = 10 ORDER BY 1");
    assert_eq!(rows, ["10", "10", "10", "20", "40"]);
}

/// `INTERSECT` and `EXCEPT`, with and without `ALL`; table value
/// constructors on their own and as a set operation's input; `ORDER BY`
/// and `LIMIT` over the whole result.
#[test]
fn intersect_except_and_values() {
    // emp.dept: 10, 10, 20, NULL, 20, 30; dept.id: 10, 20, 40.
    let q = |sql: &str| run(sql).0;
    assert_eq!(
        q("SELECT dept FROM emp INTERSECT SELECT id FROM dept ORDER BY 1"),
        ["10", "20"]
    );
    assert_eq!(
        q("SELECT dept FROM emp INTERSECT ALL SELECT id FROM dept UNION ALL SELECT 10 ORDER BY 1"),
        ["10", "10", "20"]
    );
    assert_eq!(
        q(
            "SELECT dept FROM emp INTERSECT ALL (SELECT id FROM dept UNION ALL SELECT 20) ORDER BY 1"
        ),
        ["10", "20", "20"]
    );
    assert_eq!(
        q("SELECT dept FROM emp EXCEPT SELECT id FROM dept ORDER BY 1"),
        ["NULL", "30"]
    );
    assert_eq!(
        q("SELECT dept FROM emp EXCEPT ALL SELECT id FROM dept ORDER BY 1"),
        ["NULL", "10", "20", "30"]
    );
    assert_eq!(q("SELECT id FROM dept EXCEPT SELECT dept FROM emp"), ["40"]);
    // NULL matches NULL.
    assert_eq!(q("SELECT dept FROM emp INTERSECT SELECT NULL"), ["NULL"]);
    // INTERSECT binds tighter than UNION and EXCEPT.
    assert_eq!(
        q("SELECT 40 UNION SELECT dept FROM emp INTERSECT SELECT id FROM dept ORDER BY 1"),
        ["10", "20", "40"]
    );
    assert_eq!(
        q("SELECT id FROM dept EXCEPT SELECT 10 INTERSECT SELECT 10 ORDER BY 1"),
        ["20", "40"]
    );
    // Case-insensitive names are equal under emp.name's collation.
    assert_eq!(
        q("SELECT name FROM emp WHERE id = 1 INTERSECT SELECT name FROM emp WHERE id = 5"),
        ["Ann"]
    );
    assert_eq!(
        q("SELECT id FROM dept EXCEPT SELECT dept FROM emp ORDER BY 1 LIMIT 1"),
        ["40"]
    );
    // Table value constructors: MariaDB's form and MySQL's ROW form.
    assert_eq!(q("VALUES (1, 'a'), (2, 'b')"), ["1|a", "2|b"]);
    assert_eq!(q("VALUES ROW(3), ROW(1) ORDER BY 1"), ["1", "3"]);
    assert_eq!(
        q("SELECT id, title FROM dept WHERE id = 10 UNION ALL VALUES (7, 'x'), (8, NULL)"),
        ["10|eng", "7|x", "8|NULL"]
    );
    assert_eq!(
        q(
            "SELECT e.name, g.t FROM emp e JOIN (SELECT 10 AS d, 'ten' AS t UNION ALL VALUES (20, 'twenty')) g ON g.d = e.dept ORDER BY e.id"
        ),
        ["Ann|ten", "bob|ten", "Cy|twenty", "ann|twenty"]
    );
    assert_eq!(
        error(
            Persona::MySql,
            "SELECT id FROM emp INTERSECT SELECT id, title FROM dept"
        )
        .0,
        1222
    );
}

#[test]
fn a_simple_derived_table_keeps_its_tables_keys() {
    let (rows, sig, examined) =
        run("SELECT x.id FROM (SELECT id, dept FROM emp WHERE sal > 0) x WHERE x.id = 2");
    assert_eq!(rows, ["2"]);
    assert_eq!(sig, "emp:const:PRIMARY");
    assert_eq!(examined, 1);
    // A condition on a grouping column moves into the grouped query.
    let (rows, sig, examined) = run(
        "SELECT x.n FROM (SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept) x WHERE x.dept = 10",
    );
    assert_eq!(rows, ["2"]);
    assert!(sig.contains("emp:ref:k_dept"), "{sig}");
    assert_eq!(examined, 2);
}

#[test]
fn subqueries_reach_the_other_table_by_key() {
    // The subquery's two departments are read, then the employees of
    // each by the index on `dept`.
    let (rows, sig, examined) =
        run("SELECT id FROM emp WHERE dept IN (SELECT id FROM dept WHERE id < 30) ORDER BY id");
    assert_eq!(rows, ["1", "2", "3", "5"]);
    assert!(sig.contains("dept:range:PRIMARY"), "{sig}");
    assert!(examined <= 6, "{sig} {examined}");
    // EXISTS tied to the outer row by an equality: on tables this small a
    // hash table of the six employees is cheaper than three key probes
    // (the plan-shape test against the server covers the probe).
    let (rows, sig, examined) = run(
        "SELECT id FROM dept d WHERE EXISTS (SELECT 1 FROM emp e WHERE e.dept = d.id) ORDER BY id",
    );
    assert_eq!(rows, ["10", "20"]);
    assert!(examined <= 9, "{sig} {examined}");
}

#[test]
fn parameters_are_planned_as_constants() {
    let db = db();
    let sess = session(Persona::MySql);
    let out = run_ours_params(
        &db,
        &sess,
        "SELECT name FROM emp WHERE id = ? OR name = ?",
        None,
        &[Value::Int(5), Value::str("BOB")],
    );
    let Outcome::Rows(mut rows) = out.outcome else {
        panic!("an error");
    };
    rows.sort();
    assert_eq!(rows, [b"ann".to_vec(), b"bob".to_vec()]);
    let out = run_ours_params(
        &db,
        &sess,
        "SELECT id FROM emp WHERE id > ? ORDER BY id LIMIT ?",
        None,
        &[Value::Int(2), Value::Int(2)],
    );
    assert_eq!(
        out.outcome,
        Outcome::Rows(vec![b"3".to_vec(), b"4".to_vec()])
    );
    assert_eq!(out.rows_examined, 2);
}

#[test]
fn name_errors_read_as_on_each_server() {
    let m = Persona::MariaDb;
    let y = Persona::MySql;
    assert_eq!(
        error(m, "SELECT zz FROM emp"),
        (1054, "Unknown column 'zz' in 'SELECT'".into())
    );
    assert_eq!(
        error(y, "SELECT zz FROM emp"),
        (1054, "Unknown column 'zz' in 'field list'".into())
    );
    assert_eq!(
        error(m, "SELECT * FROM emp WHERE e.id = 1"),
        (1054, "Unknown column 'e.id' in 'WHERE'".into())
    );
    assert_eq!(
        error(y, "SELECT * FROM emp ORDER BY zz"),
        (1054, "Unknown column 'zz' in 'order clause'".into())
    );
    assert_eq!(
        error(m, "SELECT id FROM emp, dept"),
        (1052, "Column 'id' in SELECT is ambiguous".into())
    );
    assert_eq!(
        error(y, "SELECT * FROM emp JOIN dept ON id = 1"),
        (1052, "Column 'id' in on clause is ambiguous".into())
    );
    assert_eq!(
        error(m, "SELECT * FROM nope"),
        (1146, "Table 'd.nope' doesn't exist".into())
    );
    assert_eq!(
        error(m, "SELECT * FROM emp, emp"),
        (1066, "Not unique table/alias: 'emp'".into())
    );
    assert_eq!(
        error(y, "SELECT id FROM emp UNION SELECT id, title FROM dept").0,
        1222
    );
    assert_eq!(error(m, "SELECT id FROM emp ORDER BY 3").0, 1054);
    assert_eq!(error(m, "SELECT name AS n FROM emp WHERE n = 'x'").0, 1054);
    assert_eq!(
        error(m, "SELECT id FROM emp WHERE id = (SELECT id FROM dept)").0,
        1242
    );
    assert_eq!(error(y, "SELECT id, COUNT(*) FROM emp").0, 1140);
    assert_eq!(
        error(y, "SELECT name, COUNT(*) FROM emp GROUP BY dept").0,
        1055
    );
    // The primary key decides every other column of its table.
    let ok = run_ours(
        &db(),
        &session(y),
        "SELECT id, name, COUNT(*) FROM emp GROUP BY id",
        None,
    );
    assert!(matches!(ok.outcome, Outcome::Rows(r) if r.len() == 6));
}

#[test]
fn explain_is_a_result_set() {
    let (rows, _, _) = run("EXPLAIN SELECT id FROM emp WHERE dept = 10");
    assert_eq!(
        rows,
        ["1|SIMPLE|emp|ref|k_dept|k_dept|5|const|2|Using index"]
    );
    let (rows, _, _) = run(
        "EXPLAIN SELECT e.name, d.title FROM emp e JOIN dept d ON d.id = e.dept WHERE e.id = 1",
    );
    assert_eq!(
        rows,
        [
            "1|SIMPLE|e|const|PRIMARY|PRIMARY|4|const|1|",
            "1|SIMPLE|d|eq_ref|PRIMARY|PRIMARY|4|d.e.dept|1|"
        ]
    );
}

#[test]
fn every_condition_kept_gives_the_same_rows() {
    let db = db();
    let sess = session(Persona::MariaDb);
    for sql in [
        "SELECT id FROM emp WHERE dept IN (10, 20) AND id BETWEEN 2 AND 5",
        "SELECT id FROM emp WHERE dept = 10 OR dept = 30",
        "SELECT e.id, d.id FROM emp e JOIN dept d ON d.id = e.dept WHERE d.title > 'f'",
    ] {
        let a = run_ours(&db, &sess, sql, None);
        let b = run_ours(
            &db,
            &sess,
            sql,
            Some(PlanOptions {
                keep_filters: true,
                ..PlanOptions::for_persona(Persona::MariaDb)
            }),
        );
        assert!(common::same(&a.outcome, &b.outcome, false), "{sql}");
    }
}

#[test]
fn parenthesized_inner_joins_are_written_out_flat() {
    let flat = run("SELECT e.name FROM emp e JOIN dept d ON d.id = e.dept JOIN emp f ON f.id = e.id ORDER BY e.id").0;
    for sql in [
        "SELECT e.name FROM (emp e JOIN dept d ON d.id = e.dept) JOIN emp f ON f.id = e.id ORDER BY e.id",
        "SELECT e.name FROM emp e JOIN (dept d CROSS JOIN emp f) ON d.id = e.dept AND f.id = e.id ORDER BY e.id",
        "SELECT e.name FROM (emp e JOIN dept d ON d.id = e.dept JOIN emp f ON f.id = e.id) ORDER BY e.id",
    ] {
        assert_eq!(run(sql).0, flat, "{sql}");
    }
    let left = "SELECT e.name, f.name FROM emp e JOIN dept d ON d.id = e.dept LEFT JOIN emp f ON f.id = e.id + 10 ORDER BY e.id";
    let nested = "SELECT e.name, f.name FROM (emp e JOIN dept d ON d.id = e.dept) LEFT JOIN emp f ON f.id = e.id + 10 ORDER BY e.id";
    assert_eq!(run(nested).0, run(left).0);
}

/// An inner join's `ON` may hold a scalar subquery that reads a row of
/// the join (hierarchy functions expand to one): it filters as the same
/// condition in `WHERE` does.
#[test]
fn a_correlated_scalar_subquery_in_an_inner_join_on() {
    let on = "SELECT e.name, f.name FROM emp e JOIN emp f ON e.sal >= (SELECT g.sal FROM emp g WHERE g.id = f.id) WHERE f.id = 2 ORDER BY e.id";
    let wh = "SELECT e.name, f.name FROM emp e JOIN emp f ON TRUE WHERE e.sal >= (SELECT g.sal FROM emp g WHERE g.id = f.id) AND f.id = 2 ORDER BY e.id";
    let (rows, _, _) = run(on);
    assert!(!rows.is_empty());
    assert_eq!(rows, run(wh).0);
    let count = "SELECT COUNT(*) FROM emp a JOIN emp b ON a.id >= (SELECT h.id FROM emp AS h WHERE h.id = b.id) AND a.id < (SELECT h.id + 3 FROM emp AS h WHERE h.id = b.id) WHERE b.id = 1";
    assert_eq!(
        run(count).0,
        run("SELECT COUNT(*) FROM emp WHERE id >= 1 AND id < 4").0
    );
}

/// A parenthesized join of any kind as the first item of `FROM` is
/// written out flat: joins associate to the left.
#[test]
fn a_leading_parenthesized_outer_join_is_written_out_flat() {
    let flat = "SELECT e.name, d.title FROM emp e LEFT JOIN dept d ON d.id = e.dept ORDER BY e.id";
    let nested =
        "SELECT e.name, d.title FROM (emp e LEFT JOIN dept d ON d.id = e.dept) ORDER BY e.id";
    assert_eq!(run(nested).0, run(flat).0);
    let more = "SELECT e.name, f.name FROM (emp e LEFT JOIN dept d ON d.id = e.dept) JOIN emp f ON f.id = e.id ORDER BY e.id";
    let more_flat = "SELECT e.name, f.name FROM emp e LEFT JOIN dept d ON d.id = e.dept JOIN emp f ON f.id = e.id ORDER BY e.id";
    assert_eq!(run(more).0, run(more_flat).0);
}

/// A condition on a derived table's column reaches the tables inside it
/// when the derived table's query starts with `WITH`, is a `UNION ALL`, or
/// reads a `LEFT JOIN`'s kept side; a common table expression named twice
/// under the same key is read once. Nothing moves past a `LIMIT` or into a
/// query with window functions.
#[test]
fn conditions_reach_with_bodies_union_parts_and_overlays() {
    // A `WITH` body that joins: the key reaches `emp` through `c`.
    let (rows, sig, examined) = run(
        "SELECT x.name FROM (WITH c AS (SELECT id, name FROM emp WHERE sal > 0 OR sal IS NULL) \
         SELECT c.id, c.name FROM c LEFT JOIN dept d ON d.id = c.id) x WHERE x.id = 3",
    );
    assert_eq!(rows, ["Cy"]);
    assert!(sig.contains("emp:const:PRIMARY"), "{sig}");
    assert!(examined <= 2, "{examined}");
    // Each `UNION ALL` part reads its table by key.
    let (rows, sig, examined) = run(
        "SELECT u.id FROM (SELECT id FROM emp UNION ALL SELECT id FROM dept) u WHERE u.id = 20",
    );
    assert_eq!(rows, ["20"]);
    assert!(
        sig.contains("emp:const:PRIMARY") && sig.contains("dept:const:PRIMARY"),
        "{sig}"
    );
    assert_eq!(examined, 1);
    // The overlay shape of a merged view: the parent's rows with a layer's
    // fields over them, and the layer's own rows the parent hasn't. The
    // parent's rows (`p`) are named twice and read once, by key.
    let overlay = "SELECT v.id, v.name FROM (WITH p AS (SELECT id, name FROM emp WHERE id > 0), \
         l AS (SELECT p.id, IF(d.id IS NULL, p.name, d.title) AS name FROM p LEFT JOIN dept d \
         ON d.id = p.id UNION ALL SELECT d.id, d.title FROM dept d WHERE NOT EXISTS \
         (SELECT 1 FROM p WHERE p.id = d.id)) SELECT id, name FROM l) v WHERE v.id";
    let (rows, sig, examined) = run(&format!("{overlay} = 20"));
    assert_eq!(rows, ["20|ops"]);
    // (`<derivedN>:ALL` reads a derived table's rows, already cut.)
    assert!(!sig.contains("emp:ALL") && !sig.contains("d:ALL"), "{sig}");
    assert!(examined <= 2, "{examined}");
    let (rows, _, examined) = run(&format!("{overlay} IN (2, 40) ORDER BY 1"));
    assert_eq!(rows, ["2|bob", "40|hr"]);
    assert!(examined <= 4, "{examined}");
    let (rows, _, _) = run(&format!("{overlay} > 0 ORDER BY 1"));
    assert_eq!(rows.len(), 9);
    // Nothing moves past a part's LIMIT.
    let (rows, _, _) = run(
        "SELECT u.id FROM ((SELECT id FROM emp ORDER BY id LIMIT 2) UNION ALL \
         (SELECT id FROM dept ORDER BY id LIMIT 1)) u WHERE u.id IN (2, 3, 10, 20) ORDER BY 1",
    );
    assert_eq!(rows, ["2", "10"]);
    // Nor past the whole query's LIMIT.
    let (rows, _, _) = run(
        "SELECT u.id FROM (SELECT id FROM emp UNION ALL SELECT id FROM dept ORDER BY id LIMIT 3) u \
         WHERE u.id = 3",
    );
    assert_eq!(rows, ["3"]);
    let (rows, _, _) = run(
        "SELECT u.id FROM (SELECT id FROM emp UNION ALL SELECT id FROM dept ORDER BY id LIMIT 2) u \
         WHERE u.id = 3",
    );
    assert!(rows.is_empty());
    // Nor into a query with a window function.
    let (rows, _, _) = run("SELECT x.id, x.rn FROM (WITH c AS (SELECT id FROM emp) \
         SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM c) x WHERE x.id = 3");
    assert_eq!(rows, ["3|3"]);
    // A window function's argument is a column the block reads: no index
    // that lacks it is chosen.
    let (rows, _, _) = run("SELECT id, SUM(sal) OVER (ORDER BY id) FROM emp");
    assert_eq!(
        rows,
        [
            "1|100.00", "2|300.00", "3|300.00", "4|350.00", "5|650.00", "6|800.00"
        ]
    );
    // A union of a string and a number column compares as strings: the
    // condition stays outside the number part.
    let (rows, _, _) = run(
        "SELECT u.v FROM (SELECT name AS v FROM emp UNION ALL SELECT id FROM dept) u \
         WHERE u.v = 'ann' ORDER BY 1",
    );
    assert_eq!(rows.len(), 2);
}
