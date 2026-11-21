//! A working tool, not a check: evaluates the expressions of a file (one
//! per line, `--` starts a comment) through the executor under both
//! personas and, when the reference servers are configured (see
//! `diff_common`), on the servers too, and prints the answers side by
//! side. `SPRIG_EXEC_PROBE` names the file; without it the test does
//! nothing. `SPRIG_EXEC_PROBE_DIFF=1` prints only the expressions whose
//! answers differ.
//!
//! It also checks, without any server, that the test parser reads back
//! what the expression printer writes.

#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::format_push_string
)]

mod diff_common;

use diff_common::generator::{AREAS, Generator};
use diff_common::sqlparse::parse;
use diff_common::{
    Oracle, Rng, evaluate, load_table, meta_agrees, run_meta, run_values, show_meta,
    show_oracle_meta, show_outcome, show_warnings, table, uses_columns,
};
use sprig_engine::row::Value;
use sprig_exec::{Persona, Session, Warning};

const DB: &str = "rx4_probe";

fn session_of(persona: Persona) -> Session {
    let mut s = Session::new(persona);
    s.now_seconds = diff_common::NOW_SECONDS;
    s.now_micros = diff_common::NOW_MICROS;
    s.time_zone_offset = diff_common::TZ_OFFSET;
    s
}

#[test]
fn probe_expressions() {
    let Ok(path) = std::env::var("SPRIG_EXEC_PROBE") else {
        return;
    };
    let only_diff = std::env::var("SPRIG_EXEC_PROBE_DIFF").is_ok();
    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .collect();
    let oracles = Oracle::from_env();
    let mut out = vec![String::new(); lines.len()];
    let mut differs = vec![false; lines.len()];
    for persona in [Persona::MariaDb, Persona::MySql] {
        let label = if persona == Persona::MariaDb {
            "maria"
        } else {
            "mysql"
        };
        let (cols, rows) = table(persona);
        let exprs: Vec<_> = lines
            .iter()
            .map(|l| parse(l, &cols, persona.flavor()))
            .collect();
        let with_table: Vec<bool> = lines
            .iter()
            .zip(&exprs)
            .map(|(l, e)| match e {
                Ok(e) => uses_columns(e),
                Err(_) => l.contains('`'),
            })
            .collect();
        let oracle = oracles.iter().find(|o| o.persona == persona);
        let answers = oracle.map(|o| {
            load_table(o, DB);
            let selects: Vec<String> = lines
                .iter()
                .zip(&with_table)
                .map(|(l, t)| {
                    if *t {
                        format!("SELECT {l} AS c FROM {DB}.t ORDER BY id")
                    } else {
                        format!("SELECT {l} AS c")
                    }
                })
                .collect();
            let nrows: Vec<usize> = with_table
                .iter()
                .map(|t| if *t { rows.len() } else { 1 })
                .collect();
            (run_values(o, DB, &selects, &nrows), run_meta(o, &selects))
        });
        let empty: Vec<Vec<Value>> = vec![Vec::new()];
        for (i, e) in exprs.iter().enumerate() {
            let mine = e.as_ref().ok().map(|e| {
                let mut s = session_of(persona);
                s.fold_constants = with_table[i];
                let input: &[Vec<Value>] = if with_table[i] { &rows } else { &empty };
                evaluate(e, &s, input)
            });
            let mine_text = match (&mine, e) {
                (Some(m), _) => format!(
                    "{}   [{}]{}",
                    show_outcome(&m.outcome),
                    m.meta.as_ref().map_or("-".to_owned(), show_meta),
                    warn_text(&m.warnings)
                ),
                (None, Err(err)) => format!("(not parsed: {err})"),
                (None, Ok(_)) => String::new(),
            };
            match &answers {
                None => out[i].push_str(&format!("   {label} E: {mine_text}\n")),
                Some((values, metas)) => {
                    let a = &values[i];
                    let theirs = format!(
                        "{}   [{}]{}",
                        show_outcome(&a.outcome),
                        metas[i].as_ref().map_or("-".to_owned(), show_oracle_meta),
                        warn_text(&a.warnings)
                    );
                    let same = mine.as_ref().is_some_and(|m| {
                        let meta_same = match (&m.meta, &metas[i]) {
                            (Some(x), Some(y)) => meta_agrees(x, y, persona) == (true, true, true),
                            (None, None) => true,
                            // An error at run time still has metadata on one
                            // side only.
                            _ => matches!(m.outcome, diff_common::Outcome::Error(_)),
                        };
                        m.outcome == a.outcome && m.warnings == a.warnings && meta_same
                    });
                    if same {
                        out[i].push_str(&format!("   {label} = {theirs}\n"));
                    } else {
                        differs[i] = true;
                        out[i].push_str(&format!(
                            "   {label} S: {theirs}\n   {label} E: {mine_text}\n"
                        ));
                    }
                }
            }
        }
    }
    let mut shown = 0;
    for (i, l) in lines.iter().enumerate() {
        if only_diff && !differs[i] {
            continue;
        }
        shown += 1;
        println!("> {l}\n{}", out[i]);
    }
    println!(
        "{} expressions, {} differ, {shown} shown",
        lines.len(),
        differs.iter().filter(|d| **d).count()
    );
}

fn warn_text(w: &[Warning]) -> String {
    if w.is_empty() {
        String::new()
    } else {
        format!("   W: {}", show_warnings(w))
    }
}

/// The parser reads back every expression the generator makes, from the
/// text the printer writes for it.
#[test]
fn parser_reads_what_the_printer_writes() {
    for persona in [Persona::MariaDb, Persona::MySql] {
        let (cols, _) = table(persona);
        for (ai, area) in AREAS.iter().enumerate() {
            for with_cols in [false, true] {
                let mut g = Generator::new(
                    Rng::new(77_000 + ai as u64 * 2 + u64::from(with_cols)),
                    &cols,
                    with_cols,
                    true,
                );
                for _ in 0..300 {
                    let e = g.area(area);
                    let sql = e.to_sql();
                    let back = parse(&sql, &cols, persona.flavor())
                        .unwrap_or_else(|err| panic!("{sql}: {err}"));
                    // The text decides what a literal is (as on the
                    // servers), so the texts are compared: the generator
                    // writes one whole number as a decimal literal.
                    assert_eq!(back.to_sql(), sql);
                    if !sql.contains("18446744073709551616") {
                        assert_eq!(back, e, "{sql}");
                    }
                }
            }
        }
    }
}
