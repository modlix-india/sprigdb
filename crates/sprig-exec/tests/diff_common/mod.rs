//! Shared by the differential tests: the reference servers reached through
//! shell commands, the parsing of what the `mysql` client prints, a small
//! deterministic random generator, and the typed table both the servers
//! and the executor read.

#![allow(
    clippy::collapsible_if,
    clippy::assigning_clones,
    clippy::many_single_char_names,
    clippy::explicit_iter_loop,
    clippy::unnecessary_box_returns,
    clippy::explicit_auto_deref,
    clippy::needless_pass_by_value,
    dead_code,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::must_use_candidate,
    clippy::format_push_string,
    clippy::if_not_else,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::struct_excessive_bools,
    clippy::single_match_else,
    clippy::redundant_closure_for_method_calls,
    clippy::manual_let_else,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::bool_to_int_with_if,
    clippy::unnested_or_patterns,
    clippy::doc_markdown,
    clippy::uninlined_format_args,
    clippy::map_unwrap_or,
    clippy::option_if_let_else,
    clippy::cast_lossless
)]

use std::collections::HashMap;
use std::io::Write as _;
use std::process::{Command, Stdio};

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, Time, Value};
use sprig_exec::{Ctx, Expr, Level, Meta, Persona, Session, Warning, compile};

pub mod generator;
pub mod golden;
pub mod lz;
pub mod sqlparse;

/// A reference server.
pub struct Oracle {
    pub persona: Persona,
    pub label: &'static str,
    command: String,
}

impl Oracle {
    /// The servers named by `SPRIG_EXEC_ORACLE_MARIADB` and
    /// `SPRIG_EXEC_ORACLE_MYSQL`: shell commands that read SQL on stdin and
    /// print rows tab-separated without a header, accepting extra client
    /// options as arguments.
    pub fn from_env() -> Vec<Oracle> {
        let mut out = Vec::new();
        for (var, persona, label) in [
            ("SPRIG_EXEC_ORACLE_MARIADB", Persona::MariaDb, "mariadb"),
            ("SPRIG_EXEC_ORACLE_MYSQL", Persona::MySql, "mysql"),
        ] {
            if let Ok(command) = std::env::var(var) {
                if !command.trim().is_empty() {
                    out.push(Oracle {
                        persona,
                        label,
                        command,
                    });
                }
            }
        }
        out
    }

    /// Runs a script; returns what the client wrote to stdout and stderr.
    pub fn run(&self, sql: &[u8], extra: &str) -> (Vec<u8>, String) {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("{} --force {extra}", self.command))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the oracle command starts");
        let mut stdin = child.stdin.take().unwrap();
        let sql = sql.to_vec();
        let writer = std::thread::spawn(move || {
            let _ = stdin.write_all(&sql);
        });
        let out = child.wait_with_output().expect("the oracle command ends");
        let _ = writer.join();
        (
            out.stdout,
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    /// The session both sides evaluate under.
    pub fn session(&self) -> Session {
        session_for(self.persona)
    }

    /// The statements that put the server's session into that state.
    pub fn header(&self) -> String {
        let coll = match self.persona {
            Persona::MariaDb => "utf8mb4_uca1400_ai_ci",
            Persona::MySql => "utf8mb4_0900_ai_ci",
        };
        // `SPRIG_EXEC_ORACLE_TZNAME` puts both sides in a named zone.
        let zone =
            std::env::var("SPRIG_EXEC_ORACLE_TZNAME").unwrap_or_else(|_| "+05:30".to_owned());
        format!(
            "SET NAMES utf8mb4 COLLATE {coll}; SET time_zone='{zone}'; SET timestamp={NOW_SECONDS}.{NOW_MICROS:06}; SET max_error_count=64;\n"
        )
    }
}

/// The session the tests evaluate under: the persona's defaults at the
/// fixed clock and time zone the servers are put into.
pub fn session_for(persona: Persona) -> Session {
    let mut s = Session::new(persona);
    s.now_seconds = NOW_SECONDS;
    s.now_micros = NOW_MICROS;
    s.time_zone_offset = TZ_OFFSET;
    if let Ok(zone) = std::env::var("SPRIG_EXEC_ORACLE_TZNAME") {
        s.time_zone_name = Some(zone);
    }
    s
}

pub const NOW_SECONDS: i64 = 1_700_000_000;
pub const NOW_MICROS: u32 = 123_456;
pub const TZ_OFFSET: i32 = 19_800;

/// What a server answered for one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The rows' single column as the client prints it.
    Rows(Vec<Vec<u8>>),
    /// The statement failed with this error number.
    Error(u16),
}

/// One statement's answer with its warnings.
#[derive(Debug, Clone)]
pub struct Answer {
    pub outcome: Outcome,
    pub warnings: Vec<Warning>,
}

/// The result metadata a server reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleMeta {
    pub field: String,
    pub collation: u32,
    pub len: u32,
    pub dec: u8,
    pub unsigned: bool,
    pub not_null: bool,
}

fn error_lines(stderr: &str) -> HashMap<usize, u16> {
    let mut out = HashMap::new();
    for line in stderr.lines() {
        // ERROR 1690 (22003) at line 5: ...
        let Some(rest) = line.strip_prefix("ERROR ") else {
            continue;
        };
        let code: u16 = rest
            .split(' ')
            .next()
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        if let Some(pos) = rest.find(" at line ") {
            let n: usize = rest[pos + 9..]
                .split(':')
                .next()
                .and_then(|c| c.trim().parse().ok())
                .unwrap_or(0);
            out.insert(n, code);
        }
    }
    out
}

/// Runs `selects` (each a complete `SELECT` of one column, on one line)
/// and returns each one's rows or error and its warnings. `rows` is how
/// many rows each statement returns when it succeeds.
pub fn run_values(oracle: &Oracle, db: &str, selects: &[String], rows: &[usize]) -> Vec<Answer> {
    let mut sql = oracle.header().into_bytes();
    for (i, s) in selects.iter().enumerate() {
        sql.extend_from_slice(
            format!("SELECT '\u{1}#{i}' FROM {db}.one;\n{s};\nSHOW WARNINGS;\n").as_bytes(),
        );
    }
    let (out, err) = oracle.run(&sql, "");
    let errors = error_lines(&err);
    let mut answers: Vec<Answer> = (0..selects.len())
        .map(|i| Answer {
            outcome: match errors.get(&(3 + 3 * i)) {
                Some(code) => Outcome::Error(*code),
                None => Outcome::Rows(Vec::new()),
            },
            warnings: Vec::new(),
        })
        .collect();
    let mut cur: Option<usize> = None;
    for line in out.split(|&b| b == b'\n') {
        if line.starts_with(b"\x01#") {
            cur = std::str::from_utf8(&line[2..])
                .ok()
                .and_then(|t| t.parse().ok());
            continue;
        }
        let Some(i) = cur else { continue };
        let a = &mut answers[i];
        if let Outcome::Rows(r) = &mut a.outcome {
            if r.len() < rows[i] {
                r.push(line.to_vec());
                continue;
            }
        }
        if line.is_empty() {
            continue;
        }
        let text = String::from_utf8_lossy(line);
        let mut parts = text.splitn(3, '\t');
        let level = match parts.next() {
            Some("Note") => Level::Note,
            Some("Warning") => Level::Warning,
            _ => Level::Error,
        };
        let code = parts.next().and_then(|c| c.parse().ok()).unwrap_or(0);
        let message = parts.next().unwrap_or("").to_owned();
        if level != Level::Error {
            a.warnings.push(Warning {
                level,
                code,
                message,
            });
        }
    }
    answers
}

/// Runs `selects` for their result metadata only. `None` for a statement
/// that failed.
pub fn run_meta(oracle: &Oracle, selects: &[String]) -> Vec<Option<OracleMeta>> {
    let mut sql = oracle.header().into_bytes();
    for s in selects {
        sql.extend_from_slice(format!("{s} LIMIT 0;\n").as_bytes());
    }
    let (out, err) = oracle.run(&sql, "-t --column-type-info");
    let errors = error_lines(&err);
    let text = String::from_utf8_lossy(&out);
    let mut blocks = Vec::new();
    let mut cur = OracleMeta {
        field: String::new(),
        collation: 0,
        len: 0,
        dec: 0,
        unsigned: false,
        not_null: false,
    };
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("Type:") {
            cur.field = v.trim().to_owned();
        } else if let Some(v) = line.strip_prefix("Collation:") {
            cur.collation = v
                .rsplit('(')
                .next()
                .and_then(|x| x.trim_end_matches(')').parse().ok())
                .unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("Length:") {
            cur.len = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("Decimals:") {
            cur.dec = v.trim().parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("Flags:") {
            cur.unsigned = v.split(' ').any(|f| f == "UNSIGNED");
            cur.not_null = v.split(' ').any(|f| f == "NOT_NULL");
            blocks.push(cur.clone());
        }
    }
    let mut it = blocks.into_iter();
    (0..selects.len())
        .map(|i| {
            if errors.contains_key(&(2 + i)) {
                None
            } else {
                it.next()
            }
        })
        .collect()
}

/// The collation ids the servers print for the collations the tests use.
pub fn collation_id(c: Collation, persona: Persona) -> Option<u32> {
    Some(match c {
        Collation::Binary => 63,
        Collation::Utf8mb4GeneralCi => 45,
        Collation::Utf8mb4Bin => 46,
        Collation::Utf8mb4UnicodeCi => 224,
        Collation::Utf8mb4Unicode520Ci => 246,
        Collation::Utf8mb4Mysql0900AiCi => 255,
        Collation::Utf8mb4Uca1400AiCi if persona == Persona::MariaDb => 2304,
        Collation::Latin1SwedishCi => 8,
        _ => return None,
    })
}

/// Which parts of the metadata agree: (type, length, nullability).
pub fn meta_agrees(mine: &Meta, theirs: &OracleMeta, persona: Persona) -> (bool, bool, bool) {
    // The servers report the result character set's collation for every
    // character string, whatever the expression's own collation is: only
    // "binary or not" can be compared here.
    let _ = persona;
    let coll_ok = mine.binary == (theirs.collation == 63);
    let ty = mine.field.name() == theirs.field
        && mine.unsigned == theirs.unsigned
        && mine.dec == theirs.dec
        && coll_ok;
    (ty, mine.len == theirs.len, mine.not_null == theirs.not_null)
}

/// The client's batch-mode escaping of a value.
pub fn client_escape(v: Option<&[u8]>) -> Vec<u8> {
    let Some(v) = v else {
        return b"NULL".to_vec();
    };
    let mut out = Vec::with_capacity(v.len());
    for &b in v {
        match b {
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0 => out.extend_from_slice(b"\\0"),
            b'\\' => out.extend_from_slice(b"\\\\"),
            other => out.push(other),
        }
    }
    out
}

/// xorshift64*: small, deterministic, good enough to vary test cases.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number below `n`.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// True with probability `num / den`.
    pub fn chance(&mut self, num: usize, den: usize) -> bool {
        self.below(den) < num
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// One column of the shared test table.
#[derive(Clone)]
pub struct Col {
    pub name: &'static str,
    pub sql_type: String,
    pub ty: ColumnType,
}

fn dec(s: &str) -> Value {
    Value::Decimal(Decimal::parse(s).unwrap())
}

/// The shared table: its columns for a persona, and its rows (the same on
/// both servers). The first column, `id`, is the primary key.
pub fn table(persona: Persona) -> (Vec<Col>, Vec<Vec<Value>>) {
    let (default_coll, default_name) = match persona {
        Persona::MariaDb => (Collation::Utf8mb4Uca1400AiCi, "utf8mb4_uca1400_ai_ci"),
        Persona::MySql => (Collation::Utf8mb4Mysql0900AiCi, "utf8mb4_0900_ai_ci"),
    };
    let col = |name, sql: &str, ty| Col {
        name,
        sql_type: sql.to_owned(),
        ty,
    };
    let cols = vec![
        col("id", "INT NOT NULL", ColumnType::int(false)),
        col("i1", "TINYINT", ColumnType::tinyint(false)),
        col("i4", "INT", ColumnType::int(false)),
        col("i8", "BIGINT", ColumnType::bigint(false)),
        col("u4", "INT UNSIGNED", ColumnType::int(true)),
        col("u8", "BIGINT UNSIGNED", ColumnType::bigint(true)),
        col(
            "d2",
            "DECIMAL(10,2)",
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
        ),
        col(
            "d0",
            "DECIMAL(20,0)",
            ColumnType::Decimal {
                precision: 20,
                scale: 0,
            },
        ),
        col(
            "d10",
            "DECIMAL(30,10)",
            ColumnType::Decimal {
                precision: 30,
                scale: 10,
            },
        ),
        col("f8", "DOUBLE", ColumnType::Double),
        col(
            "vc",
            &format!("VARCHAR(20) CHARACTER SET utf8mb4 COLLATE {default_name}"),
            ColumnType::VarChar {
                chars: 20,
                collation: default_coll,
            },
        ),
        col(
            "vb",
            "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
            ColumnType::VarChar {
                chars: 20,
                collation: Collation::Utf8mb4Bin,
            },
        ),
        col(
            "vg",
            "VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
            ColumnType::VarChar {
                chars: 20,
                collation: Collation::Utf8mb4GeneralCi,
            },
        ),
        col("bn", "VARBINARY(20)", ColumnType::VarBinary { bytes: 20 }),
        col("dt", "DATE", ColumnType::Date),
        col("tm", "TIME(3)", ColumnType::Time { fsp: 3 }),
        col("dtm", "DATETIME(6)", ColumnType::DateTime { fsp: 6 }),
        col("dt0", "DATETIME", ColumnType::DateTime { fsp: 0 }),
        col(
            "l1",
            "VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_swedish_ci",
            ColumnType::VarChar {
                chars: 20,
                collation: Collation::Latin1SwedishCi,
            },
        ),
        col(
            "lb",
            "VARCHAR(20) CHARACTER SET latin1 COLLATE latin1_bin",
            ColumnType::VarChar {
                chars: 20,
                collation: Collation::Latin1Bin,
            },
        ),
        col(
            "a1",
            "VARCHAR(20) CHARACTER SET ascii COLLATE ascii_general_ci",
            ColumnType::VarChar {
                chars: 20,
                collation: Collation::AsciiGeneralCi,
            },
        ),
    ];
    // latin1 and ascii strings, as their bytes.
    let l1s: [&[u8]; 12] = [
        b"", b"a", b"A", b"\xE9", b"\xC9", b"e", b"\xDF", b"ss", b"\x80", b"abc ", b"1", b"x",
    ];
    let lbs: [&[u8]; 12] = [
        b"b", b"B", b"\xE9", b"e", b"abc", b"ABC", b"\x80", b"\xFF", b"1.5", b" ", b"z", b"\xE4",
    ];
    let a1s: [&[u8]; 12] = [
        b"a", b"A", b"1", b"abc", b"", b" x", b"z", b"12abc", b"1e2", b"ABC ", b"b", b"%",
    ];
    let ints: [(i64, i64, i64, u64, u64); 12] = [
        (0, 0, 0, 0, 0),
        (1, 1, 1, 1, 1),
        (-1, -1, -1, 2, 2),
        (127, 2_147_483_647, i64::MAX, 4_294_967_295, u64::MAX),
        (-128, -2_147_483_648, i64::MIN, 0, 0),
        (2, 3, 5, 7, 11),
        (-7, 100, -100, 100, 1 << 63),
        (10, -10, 9_007_199_254_740_993, 10, 9_007_199_254_740_993),
        (5, 65_536, 4_294_967_296, 65_536, 4_294_967_296),
        (
            100,
            1_000_000,
            1_000_000_000_000,
            1_000_000,
            1_000_000_000_000,
        ),
        (-50, -3, 7, 3, 7),
        (3, 12_345, -98_765_432_100, 12_345, 98_765_432_100),
    ];
    let decs: [(&str, &str, &str); 12] = [
        ("0.00", "0", "0.0000000000"),
        ("1.00", "1", "1.0000000000"),
        ("-1.50", "-1", "-1.5000000000"),
        (
            "99999999.99",
            "99999999999999999999",
            "99999999999999999999.9999999999",
        ),
        (
            "-99999999.99",
            "-99999999999999999999",
            "-99999999999999999999.9999999999",
        ),
        ("2.50", "3", "0.0000000001"),
        ("0.01", "10", "3.1415926536"),
        ("-0.01", "-10", "-0.5000000000"),
        ("123.45", "18446744073709551616", "123456789.1234567890"),
        ("0.50", "2", "0.5000000000"),
        ("1.25", "100", "2.5000000000"),
        ("3.33", "7", "0.3333333333"),
    ];
    let dbls: [f64; 12] = [
        0.0,
        1.0,
        -1.5,
        1e15,
        -1e-15,
        2.5,
        0.1,
        1e300,
        123_456.789,
        0.5,
        3.5,
        -0.0001,
    ];
    let strs: [(&str, &str, &str, &[u8]); 12] = [
        ("", "", "", b""),
        ("a", "a", "a", b"a"),
        ("A", "A", "A", b"A"),
        ("abc", "abc", "abc", b"abc"),
        ("ABC ", "ABC ", "ABC ", b"ABC "),
        ("1", "12", "1.5", b"1"),
        ("12abc", " 7 ", "-3", b"12abc"),
        ("\u{e9}t\u{e9}", "ete", "\u{c9}T\u{c9}", b"\xC3\xA9"),
        ("b", "B", "\u{df}", b"\x00\x01"),
        ("ab%", "a_c", "ss", b"ab%"),
        ("1e2", "0.5", "abd", b"~"),
        ("z", "Z", "2024-01-15", b"z"),
    ];
    let temporals: [(Date, i64, DateTime, DateTime); 12] = [
        (
            d(2024, 1, 15),
            t(0, 10, 30, 0, 0),
            dtm(2024, 1, 15, 10, 30, 0, 0),
            dtm(2024, 1, 15, 10, 30, 0, 0),
        ),
        (
            d(2024, 2, 29),
            t(0, 23, 59, 59, 999_000),
            dtm(2024, 2, 29, 23, 59, 59, 999_999),
            dtm(2024, 2, 29, 23, 59, 59, 0),
        ),
        (
            d(2023, 12, 31),
            t(0, 0, 0, 0, 0),
            dtm(2023, 12, 31, 0, 0, 0, 0),
            dtm(2023, 12, 31, 0, 0, 0, 0),
        ),
        (
            d(1970, 1, 1),
            t(1, 1, 2, 3, 500_000),
            dtm(1970, 1, 1, 0, 0, 1, 500_000),
            dtm(1970, 1, 1, 0, 0, 1, 0),
        ),
        (
            d(9999, 12, 31),
            t(0, 838, 59, 59, 0),
            dtm(9999, 12, 31, 23, 59, 59, 999_999),
            dtm(9999, 12, 31, 23, 59, 59, 0),
        ),
        (
            d(1000, 1, 1),
            t(1, 838, 59, 59, 0),
            dtm(1000, 1, 1, 0, 0, 0, 0),
            dtm(1000, 1, 1, 0, 0, 0, 0),
        ),
        (
            d(2000, 2, 29),
            t(0, 12, 0, 0, 0),
            dtm(2000, 2, 29, 12, 0, 0, 0),
            dtm(2000, 2, 29, 12, 0, 0, 0),
        ),
        (
            d(2024, 12, 31),
            t(0, 0, 0, 1, 1000),
            dtm(2024, 12, 31, 23, 59, 59, 500_000),
            dtm(2024, 12, 31, 23, 59, 59, 0),
        ),
        (
            d(2021, 3, 28),
            t(0, 100, 0, 0, 0),
            dtm(2021, 3, 28, 2, 30, 0, 0),
            dtm(2021, 3, 28, 2, 30, 0, 0),
        ),
        (
            d(2019, 7, 4),
            t(1, 0, 0, 0, 1000),
            dtm(2019, 7, 4, 6, 7, 8, 90_000),
            dtm(2019, 7, 4, 6, 7, 8, 0),
        ),
        (
            d(2038, 1, 19),
            t(0, 3, 14, 7, 0),
            dtm(2038, 1, 19, 3, 14, 7, 0),
            dtm(2038, 1, 19, 3, 14, 7, 0),
        ),
        (
            d(2001, 9, 9),
            t(0, 1, 46, 40, 0),
            dtm(2001, 9, 9, 1, 46, 40, 123_456),
            dtm(2001, 9, 9, 1, 46, 40, 0),
        ),
    ];
    let mut rows = Vec::new();
    // Twelve rows of values, then a row of NULLs, then rows that mix the
    // twelve by different strides so that each column meets the others'
    // values.
    for r in 0..24usize {
        let id = Value::Int(r as i64 + 1);
        if r == 12 {
            let mut row = vec![id];
            row.extend((1..cols.len()).map(|_| Value::Null));
            rows.push(row);
            continue;
        }
        let k = |stride: usize| (r * stride + r / 12) % 12;
        let (a, b, c, e, f) = if r < 12 {
            (r, r, r, r, r)
        } else {
            (k(1), k(5), k(7), k(11), k(5))
        };
        let nullish = |v: Value, on: usize| {
            if r > 12 && r % 7 == on {
                Value::Null
            } else {
                v
            }
        };
        rows.push(vec![
            id,
            Value::Int(ints[a].0),
            Value::Int(ints[b].1),
            Value::Int(ints[c].2),
            nullish(Value::UInt(ints[e].3), 0),
            Value::UInt(ints[a].4),
            dec(decs[b].0),
            nullish(dec(decs[c].1), 1),
            dec(decs[e].2),
            nullish(Value::Double(dbls[c]), 2),
            Value::str(strs[a].0),
            nullish(Value::str(strs[b].1), 3),
            Value::str(strs[c].2),
            Value::bytes(strs[e].3),
            nullish(Value::Date(temporals[f].0), 4),
            Value::Time(Time::from_micros(temporals[a].1).unwrap()),
            nullish(Value::DateTime(temporals[b].2), 5),
            Value::DateTime(temporals[c].3),
            Value::Str(l1s[a].to_vec().into()),
            nullish(Value::Str(lbs[b].to_vec().into()), 6),
            Value::Str(a1s[e].to_vec().into()),
        ]);
    }
    (cols, rows)
}

fn d(y: u16, m: u8, day: u8) -> Date {
    Date::new(y, m, day)
}

fn t(neg: i64, h: i64, m: i64, s: i64, micros: i64) -> i64 {
    let v = (h * 3600 + m * 60 + s) * 1_000_000 + micros;
    if neg == 1 { -v } else { v }
}

fn dtm(y: u16, mo: u8, day: u8, h: u8, mi: u8, s: u8, micros: u32) -> DateTime {
    DateTime::new(Date::new(y, mo, day), h, mi, s, micros)
}

/// A value as a SQL literal for loading the table.
pub fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Double(x) => format!("{x:e}"),
        Value::Str(s) | Value::Bytes(s) => {
            let mut out = String::from("0x");
            if s.is_empty() {
                return "''".to_owned();
            }
            for b in s.iter() {
                out.push_str(&format!("{b:02X}"));
            }
            out
        }
        Value::Date(d) => format!("'{d}'"),
        Value::Time(t) => format!("'{t}'"),
        Value::DateTime(d) => format!("'{d}'"),
        other => panic!("no literal for {other:?}"),
    }
}

/// Creates database `db` with the one-row marker table and the shared
/// table `t` on a server.
pub fn load_table(oracle: &Oracle, db: &str) {
    let (cols, rows) = table(oracle.persona);
    let mut sql = oracle.header();
    sql.push_str(&format!(
        "DROP DATABASE IF EXISTS {db}; CREATE DATABASE {db}; CREATE TABLE {db}.one (x INT PRIMARY KEY); INSERT INTO {db}.one VALUES (1);\n"
    ));
    sql.push_str(&format!("CREATE TABLE {db}.t ("));
    for c in &cols {
        sql.push_str(&format!("`{}` {}, ", c.name, c.sql_type));
    }
    sql.push_str("PRIMARY KEY (id));\n");
    for row in &rows {
        let vals: Vec<String> = row.iter().map(sql_literal).collect();
        sql.push_str(&format!(
            "INSERT INTO {db}.t VALUES ({});\n",
            vals.join(", ")
        ));
    }
    let (_, err) = oracle.run(sql.as_bytes(), "");
    assert!(
        err.trim().is_empty(),
        "loading the table on {}: {err}",
        oracle.label
    );
}

/// Drops database `db` on a server.
pub fn drop_db(oracle: &Oracle, db: &str) {
    let _ = oracle.run(format!("DROP DATABASE IF EXISTS {db};\n").as_bytes(), "");
}

/// What the executor answered for one expression.
pub struct Mine {
    pub outcome: Outcome,
    pub warnings: Vec<Warning>,
    pub meta: Option<Meta>,
    /// Decimal results MariaDB would have kept as a negative zero.
    pub negative_zeros: usize,
    /// Decimal results that needed more than 65 digits.
    pub wide_decimals: usize,
}

/// Compiles `e` for `session` and evaluates it over `rows`, as one
/// statement: the warnings of all rows are collected in order.
pub fn evaluate(e: &Expr, session: &Session, rows: &[Vec<Value>]) -> Mine {
    let compiled = match compile(e, session) {
        Ok(c) => c,
        Err(err) => {
            return Mine {
                outcome: Outcome::Error(err.code),
                warnings: Vec::new(),
                meta: None,
                negative_zeros: 0,
                wide_decimals: 0,
            };
        }
    };
    let meta = Some(compiled.meta());
    let mut ctx = Ctx::new(session);
    for w in compiled.prepare_warnings() {
        ctx.push(w.level, w.code, w.message.clone());
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match compiled.eval_text(&mut ctx, row) {
            Ok(v) => out.push(client_escape(v.as_deref())),
            Err(err) => {
                return Mine {
                    outcome: Outcome::Error(err.code),
                    negative_zeros: ctx.negative_zeros() + compiled.prepare_negative_zeros(),
                    wide_decimals: ctx.wide_decimals() + compiled.prepare_wide_decimals(),
                    warnings: ctx.take_warnings(),
                    meta,
                };
            }
        }
    }
    Mine {
        outcome: Outcome::Rows(out),
        negative_zeros: ctx.negative_zeros() + compiled.prepare_negative_zeros(),
        wide_decimals: ctx.wide_decimals() + compiled.prepare_wide_decimals(),
        warnings: ctx.take_warnings(),
        meta,
    }
}

/// The functions whose last digit comes from the platform's math library.
/// Old link names of the time zone database that the MariaDB oracle's
/// time zone tables lack.
const LINK_ZONES: &[&str] = &["Asia/Calcutta", "US/Pacific", "Europe/Kiev", "Zulu"];

const MATH_LIBRARY: &[&str] = &[
    "POW", "POWER", "EXP", "LN", "LOG", "LOG2", "LOG10", "SIN", "COS", "TAN", "ASIN", "ACOS",
    "ATAN", "ATAN2", "COT",
];

/// Why the executor's value is allowed to differ from a server's for this
/// expression, if it is: the name of a difference that is kept on
/// purpose. Each rule is narrow, so that a wrong answer of another kind
/// still counts as a mismatch.
///
/// - `negative-zero`: on the MariaDB persona, the statement produced a
///   decimal zero that MariaDB keeps negative (it shows `-0.00` and
///   compares it below zero); the executor counted it.
/// - `wide-decimal`: a decimal result needed more than the 65 digits a
///   value holds; the executor counted it.
/// - `math-library`: the expression calls a transcendental function and
///   every differing value is a number within a few units in the last
///   place of the server's.
/// - `zone-name-unknown-to-server`: MariaDB answers `NULL` for an old
///   link name of the time zone database (`Asia/Calcutta`, `US/Pacific`,
///   `posix/...`) that its time zone tables lack; the executor's bundled
///   database has them.
/// - `number-padded-as-latin1`: MariaDB's `LPAD`/`RPAD` of a number with
///   a pad beyond ASCII comes out encoded twice; the executor's does not.
pub fn kept_reason(
    e: &Expr,
    persona: Persona,
    mine: &Mine,
    theirs: &Outcome,
) -> Option<&'static str> {
    if persona == Persona::MariaDb && mine.negative_zeros > 0 {
        return Some("negative-zero");
    }
    if mine.wide_decimals > 0 {
        return Some("wide-decimal");
    }
    let mut math = false;
    let mut link_zone = false;
    walk(e, &mut |x| {
        if let Expr::Func(name, args) = x {
            math |= MATH_LIBRARY.contains(&name.as_str());
            if name == "CONVERT_TZ" {
                for z in args.iter().skip(1) {
                    if let Expr::Str(z) = z {
                        link_zone |= LINK_ZONES.iter().any(|l| z.eq_ignore_ascii_case(l))
                            || z.to_ascii_lowercase().starts_with("posix/");
                    }
                }
            }
        }
    });
    if persona == Persona::MariaDb
        && link_zone
        && matches!(theirs, Outcome::Rows(r) if r.iter().all(|v| v == b"NULL"))
    {
        return Some("zone-name-unknown-to-server");
    }
    // MariaDB pads a number as a latin1 string and sends the pad's UTF-8
    // bytes on as if they were latin1 characters: its answer is the
    // executor's with every byte above ASCII encoded once more.
    if persona == Persona::MariaDb {
        let mut pads_number = false;
        walk(e, &mut |x| {
            if let Expr::Func(name, args) = x {
                if matches!(name.as_str(), "LPAD" | "RPAD") {
                    pads_number |= !matches!(
                        args.first(),
                        Some(Expr::Str(_) | Expr::Column(_) | Expr::Func(..) | Expr::Cast(..))
                    );
                }
            }
        });
        if pads_number {
            if let (Outcome::Rows(a), Outcome::Rows(b)) = (&mine.outcome, theirs) {
                // (MariaDB's latin1 is Windows-1252: 0x80 to 0x9F are
                // its printable characters.)
                const HIGH: [u32; 32] = [
                    0x20AC, 0x81, 0x201A, 0x192, 0x201E, 0x2026, 0x2020, 0x2021, 0x2C6, 0x2030,
                    0x160, 0x2039, 0x152, 0x8D, 0x17D, 0x8F, 0x90, 0x2018, 0x2019, 0x201C, 0x201D,
                    0x2022, 0x2013, 0x2014, 0x2DC, 0x2122, 0x161, 0x203A, 0x153, 0x9D, 0x17E,
                    0x178,
                ];
                let again = |v: &[u8]| -> Vec<u8> {
                    let mut out = Vec::with_capacity(v.len() * 2);
                    for &c in v {
                        if c < 0x80 {
                            out.push(c);
                        } else {
                            let code = if c < 0xA0 {
                                HIGH[usize::from(c - 0x80)]
                            } else {
                                u32::from(c)
                            };
                            let ch = char::from_u32(code).unwrap_or('?');
                            let mut buf = [0u8; 4];
                            out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                        }
                    }
                    out
                };
                if a.len() == b.len() && a.iter().zip(b).all(|(x, y)| &again(x) == y) {
                    return Some("number-padded-as-latin1");
                }
            }
        }
    }
    if math {
        if let (Outcome::Rows(a), Outcome::Rows(b)) = (&mine.outcome, theirs) {
            let close = |x: &[u8], y: &[u8]| {
                if x == y {
                    return true;
                }
                let parse = |v: &[u8]| std::str::from_utf8(v).ok()?.parse::<f64>().ok();
                match (parse(x), parse(y)) {
                    (Some(p), Some(q)) => (p - q).abs() <= 1e-14 * p.abs().max(q.abs()),
                    _ => false,
                }
            };
            if a.len() == b.len() && a.iter().zip(b).all(|(x, y)| close(x, y)) {
                return Some("math-library");
            }
        }
    }
    None
}

pub fn show_outcome(o: &Outcome) -> String {
    match o {
        Outcome::Error(c) => format!("ERROR {c}"),
        Outcome::Rows(r) => r
            .iter()
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect::<Vec<_>>()
            .join(" | "),
    }
}

pub fn show_warnings(w: &[Warning]) -> String {
    w.iter()
        .map(|w| format!("{} {} {}", w.level.name(), w.code, w.message))
        .collect::<Vec<_>>()
        .join(" ; ")
}

pub fn show_meta(m: &Meta) -> String {
    format!(
        "{}({},{}){}{} coll={}",
        m.field.name(),
        m.len,
        m.dec,
        if m.unsigned { " unsigned" } else { "" },
        if m.not_null { " not null" } else { "" },
        if m.binary { "binary" } else { "text" },
    )
}

pub fn show_oracle_meta(t: &OracleMeta) -> String {
    format!(
        "{}({},{}){}{} coll={}",
        t.field,
        t.len,
        t.dec,
        if t.unsigned { " unsigned" } else { "" },
        if t.not_null { " not null" } else { "" },
        if t.collation == 63 { "binary" } else { "text" },
    )
}

/// Calls `f` on `e` and on every expression inside it.
pub fn walk(e: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(e);
    match e {
        Expr::Neg(a)
        | Expr::Not(a)
        | Expr::BitNot(a)
        | Expr::IsNull(a, _)
        | Expr::IsTruth(a, _, _)
        | Expr::Cast(a, _)
        | Expr::Collate(a, _) => walk(a, f),
        Expr::Binary(_, a, b) => {
            walk(a, f);
            walk(b, f);
        }
        Expr::In(a, list, _) => {
            walk(a, f);
            for x in list {
                walk(x, f);
            }
        }
        Expr::Between(a, b, c, _) => {
            walk(a, f);
            walk(b, f);
            walk(c, f);
        }
        Expr::Like(a, b, c, _) => {
            walk(a, f);
            walk(b, f);
            if let Some(c) = c {
                walk(c, f);
            }
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            if let Some(o) = operand {
                walk(o, f);
            }
            for (w, t) in whens {
                walk(w, f);
                walk(t, f);
            }
            if let Some(o) = otherwise {
                walk(o, f);
            }
        }
        Expr::Func(_, args) => {
            for a in args {
                walk(a, f);
            }
        }
        _ => {}
    }
}

/// Whether the expression reads a column.
pub fn uses_columns(e: &Expr) -> bool {
    let mut found = false;
    walk(e, &mut |x| found |= matches!(x, Expr::Column(_)));
    found
}
