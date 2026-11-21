//! The executor against the servers' recorded answers, without the
//! servers: every case of `tests/golden/*.txt` and `*.txt.lz` is evaluated under both
//! personas and compared with what MariaDB 11.8 and MySQL 8.4 answered
//! (value, warnings, result metadata). See `diff_common::golden` for the
//! files and for how known differences are recorded.
//!
//! Two working modes, both off in an ordinary run:
//!
//! - `SPRIG_EXEC_GOLDEN_BLESS=1` rewrites the recorded differences from
//!   what the executor does now (no server needed). A value difference
//!   still needs a kept reason.
//! - `SPRIG_EXEC_GOLDEN_CAPTURE=named` asks the reference servers (see
//!   `diff_common`) again for the answers of the hand-written files
//!   (`named-*.txt`); `SPRIG_EXEC_GOLDEN_CAPTURE=gen:<cases>:<seeds>`
//!   generates `<cases>` expressions per area and seed (a quarter as many
//!   over the table), asks the servers and writes `gen-<area>.txt.lz`
//!   (compressed, see `diff_common::lz`).

#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::format_push_string
)]

mod diff_common;

use std::collections::BTreeMap;
use std::path::PathBuf;

use diff_common::generator::{AREAS, Generator};
use diff_common::golden::{Case, capture, read, replay, write};
use diff_common::{Oracle, Rng, drop_db, load_table, table};
use sprig_exec::Persona;

const DB: &str = "rx4_golden";

fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

fn files() -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir())
        .map(|d| {
            d.filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension().is_some_and(|x| x == "txt" || x == "lz"))
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

/// A golden file's text; a `.lz` file is stored compressed.
fn load(path: &std::path::Path) -> Vec<u8> {
    let bytes = std::fs::read(path).unwrap();
    if path.extension().is_some_and(|x| x == "lz") {
        diff_common::lz::decompress(&bytes)
    } else {
        bytes
    }
}

fn store(path: &std::path::Path, text: &[u8]) {
    if path.extension().is_some_and(|x| x == "lz") {
        std::fs::write(path, diff_common::lz::compress(text)).unwrap();
    } else {
        std::fs::write(path, text).unwrap();
    }
}

#[test]
fn compression_round_trips() {
    let mut text = Vec::new();
    for i in 0..5000u32 {
        text.extend_from_slice(
            format!("Q (`i{}` + {})\nM V {}\t{}\n", i % 7, i, i * 3, i % 11).as_bytes(),
        );
    }
    text.extend_from_slice(&[0, 255, 128, 10, 9, 0, 0, 0]);
    let packed = diff_common::lz::compress(&text);
    assert!(packed.len() < text.len());
    assert_eq!(diff_common::lz::decompress(&packed), text);
    assert_eq!(
        diff_common::lz::decompress(&diff_common::lz::compress(b"")),
        b""
    );
    assert_eq!(
        diff_common::lz::decompress(&diff_common::lz::compress(b"abc")),
        b"abc"
    );
}

#[test]
fn golden_answers_replay() {
    if std::env::var("SPRIG_EXEC_GOLDEN_CAPTURE").is_ok() {
        return;
    }
    let bless_mode = std::env::var("SPRIG_EXEC_GOLDEN_BLESS").ok();
    let bless = bless_mode.is_some();
    // `SPRIG_EXEC_GOLDEN_BLESS=open` also records value differences that
    // have no reason as open work; any other value never does.
    let mark_open = bless_mode.as_deref() == Some("open");
    let mut total = 0;
    let mut compared = 0;
    let mut known: BTreeMap<String, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    for path in files() {
        let mut cases = read(&load(&path));
        let r = replay(&mut cases, bless, mark_open);
        if bless {
            store(&path, &write(&cases));
        }
        total += r.cases;
        compared += r.compared;
        for (k, n) in r.known {
            *known.entry(k).or_default() += n;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        failures.extend(r.failures.into_iter().map(|f| format!("{name}: {f}")));
    }
    println!("golden: {total} cases, {compared} comparisons with a server's answer");
    for (k, n) in &known {
        println!("  recorded difference: {k}: {n}");
    }
    for f in failures.iter().take(60) {
        println!("FAIL {f}");
    }
    assert!(total > 0, "no golden files in {}", dir().display());
    assert!(
        failures.is_empty(),
        "{} golden cases do not agree with their file",
        failures.len()
    );
}

#[test]
fn capture_golden() {
    let Ok(mode) = std::env::var("SPRIG_EXEC_GOLDEN_CAPTURE") else {
        return;
    };
    let oracles = Oracle::from_env();
    assert!(!oracles.is_empty(), "no SPRIG_EXEC_ORACLE_* command set");
    for o in &oracles {
        load_table(o, DB);
    }
    if mode == "named" {
        for path in files() {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            if !name.starts_with("named") {
                continue;
            }
            let mut cases = read(&std::fs::read(&path).unwrap());
            capture(&mut cases, &oracles, DB);
            let r = replay(&mut cases, true, false);
            std::fs::write(&path, write(&cases)).unwrap();
            println!(
                "{name}: {} cases captured, {} without a kept reason",
                cases.len(),
                r.failures.len()
            );
            for f in r.failures.iter().take(40) {
                println!("FAIL {f}");
            }
        }
    } else if let Some(spec) = mode.strip_prefix("gen:") {
        let mut parts = spec.split(':');
        let per_area: usize = parts.next().and_then(|v| v.parse().ok()).unwrap_or(500);
        let seeds: u64 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(1);
        let (cols, _) = table(Persona::MariaDb);
        // `SPRIG_EXEC_GOLDEN_ONLY=a,b`: only these areas' files.
        let only = std::env::var("SPRIG_EXEC_GOLDEN_ONLY").unwrap_or_default();
        for (ai, area) in AREAS.iter().enumerate() {
            if !only.is_empty() && !only.split(',').any(|o| o == *area) {
                continue;
            }
            let mut cases: Vec<Case> = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for seed in 1..=seeds {
                for with_cols in [false, true] {
                    let count = if with_cols { per_area / 4 } else { per_area };
                    let mut g = Generator::new(
                        Rng::new(900_000 + seed * 1000 + ai as u64 * 2 + u64::from(with_cols)),
                        &cols,
                        with_cols,
                        *area == "temporal",
                    );
                    for _ in 0..count {
                        let sql = g.area(area).to_sql();
                        if seen.insert(sql.clone()) {
                            cases.push(Case {
                                comments: Vec::new(),
                                sql,
                                observed: [None, None],
                                known: [Vec::new(), Vec::new()],
                            });
                        }
                    }
                }
            }
            capture(&mut cases, &oracles, DB);
            let r = replay(&mut cases, true, false);
            let path = dir().join(format!("gen-{area}.txt.lz"));
            store(&path, &write(&cases));
            println!(
                "gen-{area}.txt.lz: {} cases, {} without a kept reason",
                cases.len(),
                r.failures.len()
            );
            for f in r.failures.iter().take(8) {
                println!("FAIL {f}");
            }
        }
    }
    for o in &oracles {
        drop_db(o, DB);
    }
}
