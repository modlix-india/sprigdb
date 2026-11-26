//! `EXPLAIN FORMAT=JSON`: the plan's rows as each server's JSON document.
//!
//! The document is built from the same [`ExplainRow`]s as the tabular form,
//! one query block per select id:
//!
//! ```text
//! MariaDB 11.8                              MySQL 8.4
//! query_block                               query_block
//!   select_id                                 select_id
//!   cost                                      cost_info { query_cost }
//!   nested_loop [ { table { ... } } ]         table { ... }  (one table)
//!                                             nested_loop [ { table } ]  (more)
//!   table { message }  (no table read)        message
//!   subqueries [ { query_block } ]            attached_subqueries [ ... ]
//! union_result { query_specifications }    union_result { query_specifications }
//! ```
//!
//! A table entry has the tabular form's facts under the servers' names
//! (`access_type`, `possible_keys`, `key`, `key_length`, `used_key_parts`,
//! `ref`, rows, `filtered`, `using_index`); a derived table's block is
//! nested in its table as `materialized` (MariaDB) or
//! `materialized_from_subquery` (MySQL). Costs are the planner's row
//! estimates in each server's units: a row read costs 0.1 (MySQL) and its
//! own cost line is 0.25 per page of rows; MariaDB's cost is about 1/100
//! of that. They are estimates, never the servers' exact figures, and the
//! conditions (`attached_condition`) and `used_columns` are left out.
//! MariaDB writes an array of plain values on one line; MySQL writes every
//! array item on its own line.

use std::fmt::Write as _;

use super::ExplainRow;

/// A JSON value of the document.
enum J {
    Obj(Vec<(&'static str, J)>),
    Arr(Vec<J>),
    Str(String),
    Num(String),
    Bool(bool),
}

fn s(v: &str) -> J {
    J::Str(v.to_owned())
}

fn strs(v: &[String]) -> J {
    J::Arr(v.iter().map(|x| s(x)).collect())
}

/// The document for `rows` (sorted by select id).
pub(crate) fn document(rows: &[ExplainRow], maria: bool) -> String {
    let mut out = String::new();
    write(
        &J::Obj(vec![("query_block", root(rows, maria))]),
        0,
        maria,
        &mut out,
    );
    out
}

/// The outermost query block of the document for `rows`.
fn root(rows: &[ExplainRow], maria: bool) -> J {
    let union: Vec<usize> = rows
        .iter()
        .filter(|r| r.select_type == "UNION RESULT")
        .filter_map(|r| r.table.clone())
        .flat_map(|t| union_ids(&t))
        .collect();
    if union.is_empty() {
        block(rows, 1, maria)
    } else {
        let result = rows
            .iter()
            .find(|r| r.select_type == "UNION RESULT")
            .and_then(|r| r.table.clone())
            .unwrap_or_default();
        let specs = union
            .iter()
            .map(|id| {
                let b = block(rows, *id, maria);
                if maria {
                    J::Obj(vec![("query_block", b)])
                } else {
                    J::Obj(vec![
                        ("dependent", J::Bool(false)),
                        ("cacheable", J::Bool(true)),
                        ("query_block", b),
                    ])
                }
            })
            .collect();
        let mut u = Vec::new();
        if !maria {
            u.push(("using_temporary_table", J::Bool(true)));
        }
        u.push(("table_name", J::Str(result)));
        u.push(("access_type", s("ALL")));
        u.push(("query_specifications", J::Arr(specs)));
        J::Obj(vec![("union_result", J::Obj(u))])
    }
}

/// `ANALYZE FORMAT=JSON`'s document: MariaDB's, with what the plan that
/// ran measured (each row's [`super::Actual`]; `micros`, the statement's
/// time) as its `r_` members.
pub(crate) fn analyzed_document(rows: &[ExplainRow], micros: u64) -> String {
    // The planning time is not kept apart: a tenth of the whole.
    let planning = J::Num(format!("{}", micros as f64 / 10_000.0));
    let mut out = String::new();
    write(
        &J::Obj(vec![
            (
                "query_optimization",
                J::Obj(vec![("r_total_time_ms", planning)]),
            ),
            ("query_block", root(rows, true)),
        ]),
        0,
        true,
        &mut out,
    );
    out
}

/// The select ids of a `<unionA,B,...>` table name.
fn union_ids(name: &str) -> Vec<usize> {
    name.strip_prefix("<union")
        .and_then(|r| r.strip_suffix('>'))
        .map(|r| r.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_default()
}

/// The select id a `<derivedN>` / `<subqueryN>` table reads, if any.
fn materialized_id(table: &str) -> Option<usize> {
    table
        .strip_prefix("<derived")
        .or_else(|| table.strip_prefix("<subquery"))
        .and_then(|r| r.strip_suffix('>'))
        .and_then(|r| r.parse().ok())
}

/// The query block of select `id`.
fn block(rows: &[ExplainRow], id: usize, maria: bool) -> J {
    let mine: Vec<&ExplainRow> = rows.iter().filter(|r| r.id == id).collect();
    let mut fields: Vec<(&'static str, J)> = vec![("select_id", J::Num(id.to_string()))];
    // A block that reads no table: its message.
    if let Some(m) = mine.iter().find(|r| r.table.is_none()) {
        let text = m.extra_text();
        if maria {
            fields.push(("table", J::Obj(vec![("message", J::Str(text))])));
        } else {
            fields.push(("message", J::Str(text)));
        }
        return J::Obj(fields);
    }
    // Costs: rows read so far times the rows of each step.
    let mut prefix = 0.0_f64;
    let mut loops = 1.0_f64;
    let mut tables = Vec::with_capacity(mine.len());
    for r in &mine {
        let rows_each = r.rows.unwrap_or(1).max(1) as f64;
        let read = read_cost(r, rows_each) * loops;
        let eval = 0.1 * rows_each * loops;
        let constant = r.access.as_deref() == Some("const");
        prefix += if constant { 0.0 } else { read + eval };
        tables.push(table(r, rows, loops, read, eval, prefix, maria));
        loops *= rows_each;
    }
    let all_const = mine.iter().all(|r| r.access.as_deref() == Some("const"));
    if maria {
        if !all_const {
            fields.push(("cost", maria_cost(prefix)));
        }
        if mine.iter().any(|r| r.actual.is_some()) {
            let us: u64 = mine.iter().filter_map(|r| r.actual).map(|a| a.micros).sum();
            fields.push(("r_loops", J::Num("1".into())));
            fields.push(("r_total_time_ms", J::Num(format!("{}", us as f64 / 1000.0))));
        }
    } else {
        let q = if all_const { 1.0 } else { prefix };
        fields.push((
            "cost_info",
            J::Obj(vec![("query_cost", J::Str(format!("{q:.2}")))]),
        ));
    }
    if maria || tables.len() > 1 {
        fields.push((
            "nested_loop",
            J::Arr(
                tables
                    .into_iter()
                    .map(|t| J::Obj(vec![("table", t)]))
                    .collect(),
            ),
        ));
    } else if let Some(t) = tables.pop() {
        fields.push(("table", t));
    }
    // Blocks of other ids not nested in a table: subqueries of this one.
    if id == 1 {
        let nested: Vec<usize> = rows
            .iter()
            .filter_map(|r| r.table.as_deref().and_then(materialized_id))
            .collect();
        let mut ids: Vec<usize> = rows
            .iter()
            .map(|r| r.id)
            .filter(|i| *i != 1 && !nested.contains(i))
            .collect();
        ids.dedup();
        if !ids.is_empty() {
            let subs = ids
                .into_iter()
                .map(|i| {
                    let b = block(rows, i, maria);
                    if maria {
                        J::Obj(vec![("query_block", b)])
                    } else {
                        J::Obj(vec![
                            ("dependent", J::Bool(false)),
                            ("cacheable", J::Bool(true)),
                            ("query_block", b),
                        ])
                    }
                })
                .collect();
            fields.push((
                if maria {
                    "subqueries"
                } else {
                    "attached_subqueries"
                },
                J::Arr(subs),
            ));
        }
    }
    J::Obj(fields)
}

/// The cost of reading a step's rows once: a scan costs a quarter per
/// page of about a hundred rows, a lookup a quarter per row.
fn read_cost(r: &ExplainRow, rows_each: f64) -> f64 {
    match r.access.as_deref() {
        Some("const") => 0.0,
        Some("ALL" | "index") | None => 0.25 * (rows_each / 100.0).ceil().max(1.0),
        _ => 0.25 * rows_each,
    }
}

/// MariaDB's cost figure: its units are about a hundredth of MySQL's.
fn maria_cost(x: f64) -> J {
    let v = x / 100.0 * 2.25;
    J::Num(format!("{}", (v * 1e7).round() / 1e7))
}

#[allow(clippy::too_many_arguments)]
fn table(
    r: &ExplainRow,
    rows: &[ExplainRow],
    loops: f64,
    read: f64,
    eval: f64,
    prefix: f64,
    maria: bool,
) -> J {
    let name = r.table.clone().unwrap_or_default();
    let mut t: Vec<(&'static str, J)> = vec![("table_name", J::Str(name.clone()))];
    if let Some(a) = &r.access {
        t.push(("access_type", s(a)));
    }
    let list = |v: &Option<String>| -> Vec<String> {
        v.as_deref()
            .map(|x| x.split(',').map(str::to_owned).collect())
            .unwrap_or_default()
    };
    if r.possible_keys.is_some() {
        t.push(("possible_keys", strs(&list(&r.possible_keys))));
    }
    if let Some(k) = &r.key {
        t.push(("key", s(k)));
    }
    let len = r.key_len.as_ref().map(|l| s(l));
    let parts = (!r.key_parts.is_empty()).then(|| strs(&r.key_parts));
    // MariaDB: key_length, then used_key_parts; MySQL the other way.
    if maria {
        if let Some(l) = len {
            t.push(("key_length", l));
        }
        if let Some(p) = parts {
            t.push(("used_key_parts", p));
        }
    } else {
        if let Some(p) = parts {
            t.push(("used_key_parts", p));
        }
        if let Some(l) = len {
            t.push(("key_length", l));
        }
    }
    if r.r#ref.is_some() {
        t.push(("ref", strs(&list(&r.r#ref))));
    }
    let n = r.rows.unwrap_or(1);
    let constant = r.access.as_deref() == Some("const");
    let using_index = r.extra.iter().any(|e| e == "Using index");
    if maria {
        if !constant {
            t.push(("loops", J::Num(format!("{}", loops.round() as u64))));
        }
        if let Some(a) = r.actual {
            t.push(("r_loops", J::Num(a.loops.to_string())));
        }
        t.push(("rows", J::Num(n.to_string())));
        if let Some(a) = r.actual {
            let per = a.rows as f64 / a.loops.max(1) as f64;
            t.push((
                "r_rows",
                J::Num(format!("{}", (per * 1000.0).round() / 1000.0)),
            ));
        }
        if !constant {
            t.push(("cost", maria_cost(read + eval)));
        }
        if let Some(a) = r.actual {
            t.push((
                "r_table_time_ms",
                J::Num(format!("{}", a.micros as f64 / 1000.0)),
            ));
            t.push(("r_other_time_ms", J::Num("0".into())));
        }
        t.push(("filtered", J::Num("100".into())));
        if let Some(a) = r.actual {
            let f = format!("{}", (a.filtered() * 1e8).round() / 1e8);
            t.push(("r_total_filtered", J::Num(f.clone())));
            t.push(("r_filtered", J::Num(f)));
        }
        if using_index {
            t.push(("using_index", J::Bool(true)));
        }
    } else {
        t.push(("rows_examined_per_scan", J::Num(n.to_string())));
        let produced = (loops * n as f64).round() as u64;
        t.push(("rows_produced_per_join", J::Num(produced.to_string())));
        t.push(("filtered", s("100.00")));
        if using_index {
            t.push(("using_index", J::Bool(true)));
        }
        t.push((
            "cost_info",
            J::Obj(vec![
                ("read_cost", J::Str(format!("{read:.2}"))),
                ("eval_cost", J::Str(format!("{eval:.2}"))),
                (
                    "prefix_cost",
                    J::Str(format!("{:.2}", if constant { 0.0 } else { prefix })),
                ),
            ]),
        ));
    }
    // A derived table or materialized subquery: its block inside.
    if let Some(id) = materialized_id(&name)
        && rows.iter().any(|x| x.id == id)
    {
        let b = block(rows, id, maria);
        if maria {
            t.push(("materialized", J::Obj(vec![("query_block", b)])));
        } else {
            t.push((
                "materialized_from_subquery",
                J::Obj(vec![
                    ("using_temporary_table", J::Bool(true)),
                    ("dependent", J::Bool(false)),
                    ("cacheable", J::Bool(true)),
                    ("query_block", b),
                ]),
            ));
        }
    }
    J::Obj(t)
}

fn write(v: &J, depth: usize, maria: bool, out: &mut String) {
    let pad = |d: usize| "  ".repeat(d);
    match v {
        J::Str(x) => quote(x, out),
        J::Num(x) => out.push_str(x),
        J::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        J::Obj(fields) => {
            out.push_str("{\n");
            for (i, (k, x)) in fields.iter().enumerate() {
                out.push_str(&pad(depth + 1));
                quote(k, out);
                out.push_str(": ");
                write(x, depth + 1, maria, out);
                if i + 1 < fields.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&pad(depth));
            out.push('}');
        }
        J::Arr(items) => {
            let plain = items.iter().all(|x| matches!(x, J::Str(_) | J::Num(_)));
            if maria && plain {
                out.push('[');
                for (i, x) in items.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    write(x, depth, maria, out);
                }
                out.push(']');
                return;
            }
            out.push_str("[\n");
            for (i, x) in items.iter().enumerate() {
                out.push_str(&pad(depth + 1));
                write(x, depth + 1, maria, out);
                if i + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&pad(depth));
            out.push(']');
        }
    }
}

fn quote(x: &str, out: &mut String) {
    out.push('"');
    for c in x.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(access: &str, rows: u64) -> ExplainRow {
        ExplainRow {
            access: Some(access.into()),
            rows: Some(rows),
            ..ExplainRow::new(1, "SIMPLE", "t")
        }
    }

    #[test]
    fn each_server_has_its_shape() {
        let mut r = row("ref", 2);
        r.possible_keys = Some("k".into());
        r.key = Some("k".into());
        r.key_len = Some("5".into());
        r.key_parts = vec!["k".into()];
        r.r#ref = Some("const".into());
        let m = document(std::slice::from_ref(&r), true);
        assert!(m.starts_with("{\n  \"query_block\": {\n    \"select_id\": 1,\n    \"cost\": "));
        assert!(m.contains("\"nested_loop\": [\n      {\n        \"table\": {\n          \"table_name\": \"t\",\n          \"access_type\": \"ref\",\n          \"possible_keys\": [\"k\"],\n          \"key\": \"k\",\n          \"key_length\": \"5\",\n          \"used_key_parts\": [\"k\"],\n          \"ref\": [\"const\"],\n          \"loops\": 1,\n          \"rows\": 2,"), "{m}");
        let y = document(&[r], false);
        assert!(y.contains("\"cost_info\": {\n      \"query_cost\": \"0.70\"\n    },\n    \"table\": {\n      \"table_name\": \"t\",\n      \"access_type\": \"ref\",\n      \"possible_keys\": [\n        \"k\"\n      ],\n      \"key\": \"k\",\n      \"used_key_parts\": [\n        \"k\"\n      ],\n      \"key_length\": \"5\","), "{y}");
        assert!(y.contains("\"rows_examined_per_scan\": 2,\n      \"rows_produced_per_join\": 2,\n      \"filtered\": \"100.00\","), "{y}");
    }

    #[test]
    fn a_block_without_tables_is_its_message() {
        let r = ExplainRow::message(1, "SIMPLE", "No tables used");
        assert_eq!(
            document(std::slice::from_ref(&r), true),
            "{\n  \"query_block\": {\n    \"select_id\": 1,\n    \"table\": {\n      \"message\": \"No tables used\"\n    }\n  }\n}"
        );
        assert_eq!(
            document(&[r], false),
            "{\n  \"query_block\": {\n    \"select_id\": 1,\n    \"message\": \"No tables used\"\n  }\n}"
        );
    }
}
