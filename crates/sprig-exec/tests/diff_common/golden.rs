//! The golden files: expressions with the answers MariaDB 11.8 and MySQL
//! 8.4 gave for them (value, result metadata, warnings), kept as data so
//! that the executor is checked against the servers without the servers.
//!
//! A file is lines of text (values can hold any bytes but line breaks and
//! tabs, which the client escapes):
//!
//! ```text
//! # a comment; a named case says here what it shows
//! Q <expression>
//! M V <value>[<tab><value>...]      MariaDB: one value per row
//! M X <error number>                ... or the error the statement ended with
//! M T <type> <length> <decimals> <flags> <collation id>
//! M W <N|W|E> <number> <message>    one line per condition
//! Y ...                             MySQL, the same way
//! K <M|Y> <facet>[ <facet>...]      where the executor is known to differ
//! ```
//!
//! An expression that reads a column is evaluated over the shared test
//! table, any other over one row. `K` lines are not observations: they
//! record, per server, the facets on which the executor's answer is known
//! to differ today (`v=<reason>` a value difference kept on purpose, by
//! the name `kept_reason` gives it; `w` the warnings' levels and numbers;
//! `x` only their texts; `t` the type; `l` only the length; `n` only the
//! nullability). The replay fails on a difference that is not recorded
//! and on a recorded one that is gone, so the lists only ever shrink on
//! purpose; a value difference can be recorded only with a reason, or as
//! `v=open`: a difference that is known, has no reason yet and is listed
//! as open work. `open` is never written by an ordinary bless: it takes
//! the explicit mode, so that a new unexplained difference always fails
//! first.

use std::fmt::Write as _;

use sprig_exec::{Level, Persona, Warning};

use super::sqlparse::parse;
use super::{
    Answer, Col, Mine, Oracle, OracleMeta, Outcome, evaluate, kept_reason, meta_agrees, run_meta,
    run_values, table, uses_columns,
};

/// One server's answer for a case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observed {
    pub outcome: Outcome,
    pub meta: Option<OracleMeta>,
    pub warnings: Vec<Warning>,
}

/// One case of a golden file.
#[derive(Debug, Clone)]
pub struct Case {
    /// Comment lines in front of the case, without the `#`.
    pub comments: Vec<String>,
    pub sql: String,
    /// MariaDB's and MySQL's answers.
    pub observed: [Option<Observed>; 2],
    /// The recorded differences of the executor, per server.
    pub known: [Vec<String>; 2],
}

fn tag(i: usize) -> char {
    if i == 0 { 'M' } else { 'Y' }
}

pub fn persona_of(i: usize) -> Persona {
    if i == 0 {
        Persona::MariaDb
    } else {
        Persona::MySql
    }
}

fn split_once(line: &[u8]) -> (&[u8], &[u8]) {
    match line.iter().position(|&b| b == b' ') {
        Some(p) => (&line[..p], &line[p + 1..]),
        None => (line, &[]),
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Reads a golden file.
pub fn read(bytes: &[u8]) -> Vec<Case> {
    let mut cases: Vec<Case> = Vec::new();
    let mut comments = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if line[0] == b'#' {
            comments.push(text(&line[1..]).trim().to_owned());
            continue;
        }
        let (kind, rest) = split_once(line);
        if kind == b"Q" {
            cases.push(Case {
                comments: std::mem::take(&mut comments),
                sql: text(rest),
                observed: [None, None],
                known: [Vec::new(), Vec::new()],
            });
            continue;
        }
        let Some(case) = cases.last_mut() else {
            continue;
        };
        if kind == b"K" {
            let (who, facets) = split_once(rest);
            let i = usize::from(who == b"Y");
            case.known[i] = text(facets).split(' ').map(str::to_owned).collect();
            continue;
        }
        let i = usize::from(kind == b"Y");
        let (what, body) = split_once(rest);
        let o = case.observed[i].get_or_insert(Observed {
            outcome: Outcome::Rows(Vec::new()),
            meta: None,
            warnings: Vec::new(),
        });
        match what {
            b"V" => {
                o.outcome =
                    Outcome::Rows(body.split(|&b| b == b'\t').map(<[u8]>::to_vec).collect());
            }
            b"X" => o.outcome = Outcome::Error(text(body).parse().unwrap_or(0)),
            b"T" => {
                let t = text(body);
                let p: Vec<&str> = t.split(' ').collect();
                if p.len() == 5 {
                    o.meta = Some(OracleMeta {
                        field: p[0].to_owned(),
                        len: p[1].parse().unwrap_or(0),
                        dec: p[2].parse().unwrap_or(0),
                        unsigned: p[3].contains('U'),
                        not_null: p[3].contains('N'),
                        collation: p[4].parse().unwrap_or(0),
                    });
                }
            }
            b"W" => {
                let (level, rest) = split_once(body);
                let (code, message) = split_once(rest);
                o.warnings.push(Warning {
                    level: match level {
                        b"N" => Level::Note,
                        b"W" => Level::Warning,
                        _ => Level::Error,
                    },
                    code: text(code).parse().unwrap_or(0),
                    message: text(message),
                });
            }
            _ => {}
        }
    }
    cases
}

/// Writes a golden file.
pub fn write(cases: &[Case]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in cases {
        for line in &c.comments {
            out.extend_from_slice(format!("# {line}\n").as_bytes());
        }
        out.extend_from_slice(format!("Q {}\n", c.sql).as_bytes());
        for (i, o) in c.observed.iter().enumerate() {
            let Some(o) = o else { continue };
            let t = tag(i);
            match &o.outcome {
                Outcome::Rows(rows) => {
                    out.extend_from_slice(format!("{t} V ").as_bytes());
                    for (j, r) in rows.iter().enumerate() {
                        if j > 0 {
                            out.push(b'\t');
                        }
                        out.extend_from_slice(r);
                    }
                    out.push(b'\n');
                }
                Outcome::Error(code) => out.extend_from_slice(format!("{t} X {code}\n").as_bytes()),
            }
            if let Some(m) = &o.meta {
                let mut flags = String::new();
                if m.unsigned {
                    flags.push('U');
                }
                if m.not_null {
                    flags.push('N');
                }
                if flags.is_empty() {
                    flags.push('-');
                }
                out.extend_from_slice(
                    format!(
                        "{t} T {} {} {} {flags} {}\n",
                        m.field, m.len, m.dec, m.collation
                    )
                    .as_bytes(),
                );
            }
            for w in &o.warnings {
                let level = match w.level {
                    Level::Note => 'N',
                    Level::Warning => 'W',
                    Level::Error => 'E',
                };
                out.extend_from_slice(
                    format!("{t} W {level} {} {}\n", w.code, w.message).as_bytes(),
                );
            }
        }
        for (i, k) in c.known.iter().enumerate() {
            if !k.is_empty() {
                out.extend_from_slice(format!("K {} {}\n", tag(i), k.join(" ")).as_bytes());
            }
        }
    }
    out
}

/// Asks the servers for the answers of every case (the servers that are
/// configured; the other's answers stay as they are).
pub fn capture(cases: &mut [Case], oracles: &[Oracle], db: &str) {
    for oracle in oracles {
        let i = usize::from(oracle.persona == Persona::MySql);
        let (cols, rows) = table(oracle.persona);
        for chunk in cases.chunks_mut(400) {
            let with_table: Vec<bool> = chunk
                .iter()
                .map(|c| match parse(&c.sql, &cols, oracle.persona.flavor()) {
                    Ok(e) => uses_columns(&e),
                    Err(_) => c.sql.contains('`'),
                })
                .collect();
            let selects: Vec<String> = chunk
                .iter()
                .zip(&with_table)
                .map(|(c, t)| {
                    if *t {
                        format!("SELECT {} AS c FROM {db}.t ORDER BY id", c.sql)
                    } else {
                        format!("SELECT {} AS c", c.sql)
                    }
                })
                .collect();
            let nrows: Vec<usize> = with_table
                .iter()
                .map(|t| if *t { rows.len() } else { 1 })
                .collect();
            let answers: Vec<Answer> = run_values(oracle, db, &selects, &nrows);
            let metas = run_meta(oracle, &selects);
            for ((case, a), m) in chunk.iter_mut().zip(answers).zip(metas) {
                case.observed[i] = Some(Observed {
                    outcome: a.outcome,
                    meta: m,
                    warnings: a.warnings,
                });
            }
        }
    }
}

/// The facets on which the executor's answer differs from one server's
/// (see the module text for their names). `Err` is a value difference
/// that no kept rule covers (unless the case records it as open, or
/// `mark_open` asks for that), or a case the test parser cannot read.
pub fn differences(
    case: &Case,
    i: usize,
    cols: &[Col],
    rows: &[Vec<sprig_engine::row::Value>],
    mark_open: bool,
) -> Result<Vec<String>, String> {
    let persona = persona_of(i);
    let Some(theirs) = &case.observed[i] else {
        return Ok(Vec::new());
    };
    let e = parse(&case.sql, cols, persona.flavor()).map_err(|err| format!("not parsed: {err}"))?;
    let with_table = uses_columns(&e);
    let mut session = super::session_for(persona);
    session.fold_constants = with_table;
    let empty: Vec<Vec<sprig_engine::row::Value>> = vec![Vec::new()];
    let input = if with_table { rows } else { &empty[..] };
    let mine: Mine = evaluate(&e, &session, input);
    let mut out = Vec::new();
    if mine.outcome != theirs.outcome {
        match kept_reason(&e, persona, &mine, &theirs.outcome) {
            Some(reason) => out.push(format!("v={reason}")),
            None if mark_open || case.known[i].iter().any(|f| f == "v=open") => {
                out.push("v=open".to_owned());
            }
            None => {
                return Err(format!(
                    "value: server {} / executor {}",
                    super::show_outcome(&theirs.outcome),
                    super::show_outcome(&mine.outcome)
                ));
            }
        }
    }
    let codes = |w: &[Warning]| -> Vec<(&'static str, u16)> {
        w.iter().map(|w| (w.level.name(), w.code)).collect()
    };
    if codes(&mine.warnings) != codes(&theirs.warnings) {
        out.push("w".to_owned());
    } else if mine.warnings != theirs.warnings {
        out.push("x".to_owned());
    }
    if let (Some(m), Some(t)) = (&mine.meta, &theirs.meta) {
        let (ty, len, null) = meta_agrees(m, t, persona);
        if !ty {
            out.push("t".to_owned());
        } else {
            if !len {
                out.push("l".to_owned());
            }
            if !null {
                out.push("n".to_owned());
            }
        }
    }
    Ok(out)
}

/// What a replay of a set of cases found.
#[derive(Default)]
pub struct Replay {
    pub cases: usize,
    /// Comparisons made (cases times servers with an answer).
    pub compared: usize,
    /// Per facet letter: how many comparisons differ, as recorded.
    pub known: std::collections::BTreeMap<String, usize>,
    /// What does not agree with the file.
    pub failures: Vec<String>,
}

/// Replays cases against the executor. With `bless`, the recorded
/// differences are replaced by what the executor does now (a value
/// difference without a kept reason is still a failure, unless
/// `mark_open` records it as open).
pub fn replay(cases: &mut [Case], bless: bool, mark_open: bool) -> Replay {
    let mut r = Replay {
        cases: cases.len(),
        ..Replay::default()
    };
    for i in 0..2 {
        let (cols, rows) = table(persona_of(i));
        for case in cases.iter_mut() {
            if case.observed[i].is_none() {
                continue;
            }
            r.compared += 1;
            let label = if i == 0 { "MariaDB" } else { "MySQL" };
            match differences(case, i, &cols, &rows, bless && mark_open) {
                Err(why) => {
                    let mut msg = String::new();
                    let _ = write!(msg, "{label}: {}\n    {why}", case.sql);
                    r.failures.push(msg);
                }
                Ok(now) => {
                    if bless {
                        case.known[i] = now.clone();
                    } else if now != case.known[i] {
                        r.failures.push(format!(
                            "{label}: {}\n    differs on [{}], recorded [{}]",
                            case.sql,
                            now.join(" "),
                            case.known[i].join(" ")
                        ));
                    }
                    for f in &now {
                        let key = f.split('=').next().unwrap_or(f).to_owned();
                        *r.known.entry(format!("{label} {key}")).or_default() += 1;
                        if let Some(reason) = f.strip_prefix("v=") {
                            *r.known.entry(format!("{label} kept {reason}")).or_default() += 1;
                        }
                    }
                }
            }
        }
    }
    r
}
