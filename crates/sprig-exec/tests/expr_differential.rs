//! Expressions against the servers. Thousands of generated expressions,
//! typed and nested, over literals of every type and over the columns of a
//! small typed table, are run on MariaDB 11.8 and MySQL 8.4 and through
//! the executor under the matching persona. Compared per expression:
//!
//! 1. **value**: every row's text (or the error number); a difference
//!    that is kept on purpose (`diff_common::kept_reason`) is counted
//!    apart, under "kept";
//! 2. **warnings**: level and number of each condition, in order, and
//!    separately their texts;
//! 3. **type**: the result metadata's type, signedness, decimals and
//!    collation; separately its length and its nullability.
//!
//! The servers are reached through shell commands (see `diff_common`),
//! given in `SPRIG_EXEC_ORACLE_MARIADB` and `SPRIG_EXEC_ORACLE_MYSQL`; a
//! server whose variable is unset is skipped, and with neither the test
//! does nothing. `SPRIG_EXEC_ORACLE_CASES` sets the expressions per area
//! (default 400), `SPRIG_EXEC_ORACLE_SEED` varies them,
//! `SPRIG_EXEC_ORACLE_ONLY` runs the areas whose name contains it,
//! `SPRIG_EXEC_ORACLE_REPORT` names a directory for the counts and the
//! mismatches, and `SPRIG_EXEC_ORACLE_LOGMAX` caps the mismatches written
//! per area and facet (default 60).
//!
//! The test works in its own database, `rx4_diff`, and drops it at the
//! end.

#![allow(
    clippy::collapsible_if,
    clippy::assigning_clones,
    clippy::many_single_char_names,
    clippy::explicit_iter_loop,
    clippy::unnecessary_box_returns,
    clippy::explicit_auto_deref,
    clippy::needless_pass_by_value,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::format_push_string,
    clippy::similar_names
)]

mod diff_common;

use std::collections::BTreeMap;
use std::fmt::Write as _;

use diff_common::generator::{AREAS, Generator};
use diff_common::{
    Oracle, Rng, drop_db, evaluate, kept_reason, load_table, meta_agrees, run_meta, run_values,
    show_outcome, show_warnings, table,
};
use sprig_engine::row::Value;
use sprig_exec::{Expr, Meta, Warning};

const DB: &str = "rx4_diff";

#[derive(Default, Clone)]
struct Tally {
    total: usize,
    value: usize,
    /// Value differences that are kept on purpose (see `kept_reason`).
    kept: usize,
    warn_codes: usize,
    warn_text: usize,
    meta_seen: usize,
    ty: usize,
    len: usize,
    null: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[ignore = "live: needs SPRIG_EXEC_ORACLE_CASES, SPRIG_EXEC_ORACLE_SEED, SPRIG_EXEC_ORACLE_ONLY, ...; tools/live/README.md"]
#[test]
fn expressions_match_the_servers() {
    let oracles = Oracle::from_env();
    if oracles.is_empty() {
        eprintln!("no SPRIG_EXEC_ORACLE_* command set: skipped");
        panic!("live test run without its environment: see tools/live/README.md");
    }
    let cases = env_usize("SPRIG_EXEC_ORACLE_CASES", 400);
    let seed = env_usize("SPRIG_EXEC_ORACLE_SEED", 1) as u64;
    let only = std::env::var("SPRIG_EXEC_ORACLE_ONLY").unwrap_or_default();
    let report_dir = std::env::var("SPRIG_EXEC_ORACLE_REPORT").ok();
    let temporal = std::env::var("SPRIG_EXEC_ORACLE_TEMPORAL").is_ok();
    let log_max = env_usize("SPRIG_EXEC_ORACLE_LOGMAX", 60);
    let mut summary = String::new();
    for oracle in &oracles {
        load_table(oracle, DB);
        let session = oracle.session();
        let (cols, rows) = table(oracle.persona);
        let mut tallies: BTreeMap<String, Tally> = BTreeMap::new();
        let mut mismatches = String::new();
        let mut logged: BTreeMap<String, usize> = BTreeMap::new();
        for (ai, area) in AREAS.iter().enumerate() {
            if !only.is_empty() && !area.contains(only.as_str()) {
                continue;
            }
            for with_cols in [false, true] {
                let count = if with_cols { cases / 4 } else { cases };
                let mut generator = Generator::new(
                    Rng::new(seed * 1000 + ai as u64 * 2 + u64::from(with_cols)),
                    &cols,
                    with_cols,
                    temporal || *area == "temporal",
                );
                let exprs: Vec<Expr> = (0..count).map(|_| generator.area(area)).collect();
                let key = format!("{area}/{}", if with_cols { "col" } else { "lit" });
                let empty_row: Vec<Vec<Value>> = vec![Vec::new()];
                let input: &[Vec<Value>] = if with_cols { &rows } else { &empty_row };
                for chunk in exprs.chunks(400) {
                    let selects: Vec<String> = chunk
                        .iter()
                        .map(|e| {
                            if with_cols {
                                format!("SELECT {} AS c FROM {DB}.t ORDER BY id", e.to_sql())
                            } else {
                                format!("SELECT {} AS c", e.to_sql())
                            }
                        })
                        .collect();
                    let nrows = vec![input.len(); chunk.len()];
                    let answers = run_values(oracle, DB, &selects, &nrows);
                    let metas = run_meta(oracle, &selects);
                    for (i, e) in chunk.iter().enumerate() {
                        let mut session = session.clone();
                        session.fold_constants = with_cols;
                        let mine = evaluate(e, &session, input);
                        let theirs = &answers[i];
                        let tally = tallies.entry(key.clone()).or_default();
                        tally.total += 1;
                        let mut log = |facet: &str, theirs: String, mine: String| {
                            let n = logged.entry(format!("{key}/{facet}")).or_default();
                            *n += 1;
                            if *n <= log_max {
                                let _ = writeln!(
                                    mismatches,
                                    "[{key}] {facet}: {}\n    server: {theirs}\n    engine: {mine}",
                                    selects[i]
                                );
                            }
                        };
                        if mine.outcome == theirs.outcome {
                            tally.value += 1;
                        } else if let Some(reason) =
                            kept_reason(e, oracle.persona, &mine, &theirs.outcome)
                        {
                            tally.kept += 1;
                            log(
                                &format!("kept_{}", reason.replace('-', "_")),
                                show_outcome(&theirs.outcome),
                                show_outcome(&mine.outcome),
                            );
                        } else {
                            log(
                                "value",
                                show_outcome(&theirs.outcome),
                                show_outcome(&mine.outcome),
                            );
                        }
                        let codes = |w: &[Warning]| -> Vec<(&'static str, u16)> {
                            w.iter().map(|w| (w.level.name(), w.code)).collect()
                        };
                        if codes(&mine.warnings) == codes(&theirs.warnings) {
                            tally.warn_codes += 1;
                            if mine.warnings == theirs.warnings {
                                tally.warn_text += 1;
                            } else {
                                log(
                                    "warntext",
                                    show_warnings(&theirs.warnings),
                                    show_warnings(&mine.warnings),
                                );
                            }
                        } else {
                            log(
                                "warnings",
                                show_warnings(&theirs.warnings),
                                show_warnings(&mine.warnings),
                            );
                        }
                        if let (Some(m), Some(t)) = (&mine.meta, &metas[i]) {
                            tally.meta_seen += 1;
                            let (ty, len, null) = meta_agrees(m, t, oracle.persona);
                            tally.ty += usize::from(ty);
                            tally.len += usize::from(len);
                            tally.null += usize::from(null);
                            let show = |m: &Meta| {
                                format!(
                                    "{}({},{}){}{} coll={}",
                                    m.field.name(),
                                    m.len,
                                    m.dec,
                                    if m.unsigned { " unsigned" } else { "" },
                                    if m.not_null { " not null" } else { "" },
                                    if m.binary { "binary" } else { m.coll.name() },
                                )
                            };
                            let show_theirs = format!(
                                "{}({},{}){}{} coll={}",
                                t.field,
                                t.len,
                                t.dec,
                                if t.unsigned { " unsigned" } else { "" },
                                if t.not_null { " not null" } else { "" },
                                t.collation
                            );
                            if !ty {
                                log("type", show_theirs.clone(), show(m));
                            } else if !len {
                                log("len", show_theirs.clone(), show(m));
                            } else if !null {
                                log("null", show_theirs, show(m));
                            }
                        }
                    }
                }
            }
        }
        let _ = writeln!(
            summary,
            "{:<8} {:<14} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
            oracle.label,
            "area",
            "cases",
            "value",
            "kept",
            "wcode",
            "wtext",
            "metas",
            "type",
            "len",
            "null"
        );
        for (key, t) in &tallies {
            let _ = writeln!(
                summary,
                "{:<8} {:<14} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}",
                oracle.label,
                key,
                t.total,
                t.value,
                t.kept,
                t.warn_codes,
                t.warn_text,
                t.meta_seen,
                t.ty,
                t.len,
                t.null
            );
        }
        if let Some(dir) = &report_dir {
            let _ = std::fs::create_dir_all(dir);
            let _ = std::fs::write(format!("{dir}/mismatch-{}.txt", oracle.label), &mismatches);
        }
        drop_db(oracle, DB);
    }
    println!("{summary}");
    if let Some(dir) = &report_dir {
        let _ = std::fs::write(format!("{dir}/summary.txt"), &summary);
    }
}
