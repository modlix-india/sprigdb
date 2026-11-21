//! Typed keys against the servers: for every column type (and, for strings,
//! every collation), generated values are loaded into MariaDB 11.8 and
//! MySQL 8.4, read back, and four things are checked per table:
//!
//! 1. **stored**: the server stored the value the engine's type holds (what
//!    comes back equals what was sent);
//! 2. **compare** and **equal**: the engine's key order and equality classes
//!    are those of the server's comparison operators. For each row the
//!    server counts the rows its `<` puts below it and names the rows its
//!    `=` matches; rows with one key must be exactly the rows `=` groups, and
//!    a row with more rows below it must have the greater key;
//! 3. **sort**: along the server's `ORDER BY v`, the keys never go down;
//! 4. **weights** (character strings): `Collation::weight_string` equals the
//!    server's `WEIGHT_STRING(v)` byte for byte.
//!
//! The servers are reached through shell commands that read SQL on stdin and
//! print rows tab-separated without a header (for example a `docker exec -i
//! ... mariadb -N -B` wrapper), given in `SPRIG_TYPES_ORACLE_MARIADB` and
//! `SPRIG_TYPES_ORACLE_MYSQL`. A server whose variable is unset is skipped.
//! `SPRIG_TYPES_ORACLE_ROWS` sets the values per table (default 2000),
//! `SPRIG_TYPES_ORACLE_REPORT` a file the per-table counts are written to,
//! `SPRIG_TYPES_ORACLE_ONLY` a substring tables must contain to be run,
//! `SPRIG_TYPES_ORACLE_SEED` a number that changes the generated values.
//!
//! The test works in its own database, `r_types_diff`, and drops it at the
//! end.

#![allow(
    clippy::unwrap_used,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::float_cmp,
    clippy::format_collect
)]

mod types_common;

use std::fmt::Write as _;
use std::io::Write as _;
use std::process::{Command, Stdio};

use bytes::Bytes;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use sprig_engine::keys::collation::{Charset, Collation, Flavor};
use sprig_engine::keys::typed::{KeyColumn, encode_column};
use sprig_engine::row::temporal::Time;
use sprig_engine::row::{ColumnType, Date, DateTime, Decimal, LobSize, Timestamp, Value};
use types_common::{all_types, string_for, value_of};

const DB: &str = "r_types_diff";

struct Oracle {
    flavor: Flavor,
    label: &'static str,
    command: String,
}

impl Oracle {
    fn run(&self, sql: &str) -> Result<String, String> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| e.to_string())?;
        let mut stdin = child.stdin.take().unwrap();
        let sql = sql.to_owned();
        let writer = std::thread::spawn(move || {
            let _ = stdin.write_all(sql.as_bytes());
        });
        let out = child.wait_with_output().map_err(|e| e.to_string())?;
        let _ = writer.join();
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).trim().to_owned())
        }
    }
}

/// One table of the run: a column type and how the server declares it.
struct Case {
    ty: ColumnType,
    /// The server's type text.
    sql_type: String,
    /// Strings limited to the BMP (a `utf8mb3` column).
    bmp_only: bool,
}

fn collation_name(c: Collation, flavor: Flavor) -> &'static str {
    if flavor == Flavor::MySql && c == Collation::Utf8mb4NopadBin {
        "utf8mb4_0900_bin"
    } else {
        c.name()
    }
}

fn charset_name(c: Collation) -> &'static str {
    match c.charset() {
        Charset::Binary => "binary",
        Charset::Ascii => "ascii",
        Charset::Latin1 => "latin1",
        Charset::Utf8mb4 => "utf8mb4",
    }
}

fn members(n: u32) -> String {
    (1..=n)
        .map(|i| format!("'m{i}'"))
        .collect::<Vec<_>>()
        .join(",")
}

fn sql_type(ty: ColumnType, flavor: Flavor) -> Option<String> {
    let text = |c: Collation| -> Option<String> {
        // MySQL has the NO PAD binary collation under its own name.
        let here = c.only_on().is_none_or(|f| f == flavor)
            || (flavor == Flavor::MySql && c == Collation::Utf8mb4NopadBin);
        if !here || c == Collation::Binary {
            return None;
        }
        Some(format!(
            "CHARACTER SET {} COLLATE {}",
            charset_name(c),
            collation_name(c, flavor)
        ))
    };
    Some(match ty {
        ColumnType::Int { bytes, unsigned } => format!(
            "{}{}",
            match bytes {
                1 => "TINYINT",
                2 => "SMALLINT",
                3 => "MEDIUMINT",
                4 => "INT",
                _ => "BIGINT",
            },
            if unsigned { " UNSIGNED" } else { "" }
        ),
        ColumnType::Bit { bits } => format!("BIT({bits})"),
        ColumnType::Decimal { precision, scale } => {
            // MySQL stops at 30 decimals.
            if flavor == Flavor::MySql && scale > 30 {
                return None;
            }
            format!("DECIMAL({precision},{scale})")
        }
        ColumnType::Float => "FLOAT".into(),
        ColumnType::Double => "DOUBLE".into(),
        ColumnType::Date => "DATE".into(),
        ColumnType::Time { fsp } => format!("TIME({fsp})"),
        ColumnType::DateTime { fsp } => format!("DATETIME({fsp})"),
        ColumnType::Timestamp { fsp } => format!("TIMESTAMP({fsp})"),
        ColumnType::Year => "YEAR".into(),
        ColumnType::Char { chars, collation } => format!("CHAR({chars}) {}", text(collation)?),
        ColumnType::VarChar { chars, collation } => {
            format!("VARCHAR({chars}) {}", text(collation)?)
        }
        ColumnType::Text { collation, .. } => format!("TEXT {}", text(collation)?),
        ColumnType::Binary { bytes } => format!("BINARY({bytes})"),
        ColumnType::VarBinary { bytes } => format!("VARBINARY({bytes})"),
        ColumnType::Blob { size } => match size {
            LobSize::Tiny => "TINYBLOB",
            LobSize::Normal => "BLOB",
            LobSize::Medium => "MEDIUMBLOB",
            LobSize::Long => "LONGBLOB",
        }
        .into(),
        ColumnType::Enum { members: n } => format!("ENUM({})", members(u32::from(n))),
        ColumnType::Set { members: n } => format!("SET({})", members(u32::from(n))),
        ColumnType::Json => return None,
    })
}

fn cases(flavor: Flavor) -> Vec<Case> {
    let mut out = Vec::new();
    let mut types = all_types();
    // A 65,535-member ENUM is half a megabyte of DDL; 300 members already
    // need the second byte.
    types.retain(|t| !matches!(t, ColumnType::Enum { members: 65_535 }));
    types.push(ColumnType::Enum { members: 300 });
    types.push(ColumnType::Decimal {
        precision: 30,
        scale: 30,
    });
    for ty in types {
        if let Some(sql_type) = sql_type(ty, flavor) {
            out.push(Case {
                ty,
                sql_type,
                bmp_only: false,
            });
        }
    }
    // `utf8mb3` columns under the `utf8mb4` collation of the same name.
    let mut mb3 = vec![
        ("utf8mb3_general_ci", Collation::Utf8mb4GeneralCi),
        ("utf8mb3_bin", Collation::Utf8mb4Bin),
        ("utf8mb3_unicode_ci", Collation::Utf8mb4UnicodeCi),
        ("utf8mb3_unicode_520_ci", Collation::Utf8mb4Unicode520Ci),
    ];
    if flavor == Flavor::MariaDb {
        mb3.push(("utf8mb3_uca1400_ai_ci", Collation::Utf8mb4Uca1400AiCi));
        mb3.push(("utf8mb3_uca1400_as_cs", Collation::Utf8mb4Uca1400AsCs));
        mb3.push(("utf8mb3_general_nopad_ci", Collation::Utf8mb4GeneralNopadCi));
    }
    for (name, collation) in mb3 {
        assert_eq!(Collation::resolve(name, flavor), Some(collation));
        out.push(Case {
            ty: ColumnType::VarChar {
                chars: 12,
                collation,
            },
            sql_type: format!("VARCHAR(12) CHARACTER SET utf8mb3 COLLATE {name}"),
            bmp_only: true,
        });
    }
    out
}

/// Characters that take part in contractions and expansions of the UCA
/// tables, on top of the shared alphabet.
const EXTRA: &[char] = &[
    '\u{0F71}', '\u{0F72}', '\u{0F74}', '\u{0F80}', '\u{0FB2}', '\u{0FB3}', '\u{0CC6}', '\u{0CC2}',
    '\u{0CD5}', '\u{0DD9}', '\u{0DCF}', '\u{0DCA}', '\u{1025}', '\u{102E}', '\u{0E40}', '\u{0E01}',
    '\u{0EC0}', '\u{0E81}', '\u{0627}', '\u{0653}', '\u{0654}', '\u{0655}', '\u{FDFA}', '\u{00BD}',
    '\u{2460}', '\u{33A0}', '\u{0132}', '\u{01C4}', '\u{1100}', '\u{1161}', '\u{11A8}', '\u{AC00}',
    '\u{3042}', '\u{30A2}', '\u{FF76}', '\u{309B}', '\u{00C5}', '\u{212B}', '\u{017F}', '\u{0387}',
];

fn diff_string(collation: Collation, max: usize, bmp_only: bool) -> BoxedStrategy<Vec<u8>> {
    if collation.charset() != Charset::Utf8mb4 {
        return string_for(collation, max);
    }
    let shared = string_for(collation, 1).prop_map(|b| String::from_utf8(b).unwrap());
    let ch = prop_oneof![
        4 => shared,
        1 => proptest::sample::select(EXTRA).prop_map(String::from),
        1 => (0x20u32..0x3000).prop_filter_map("char", char::from_u32).prop_map(String::from),
        1 => any::<char>().prop_map(String::from),
    ]
    .prop_filter("bmp", move |s| {
        !bmp_only || s.chars().all(|c| u32::from(c) <= 0xFFFF)
    });
    proptest::collection::vec(ch, 0..=max)
        .prop_map(|v| v.concat().into_bytes())
        .boxed()
}

fn strip_spaces(mut s: Vec<u8>) -> Vec<u8> {
    while s.last() == Some(&b' ') {
        s.pop();
    }
    s
}

fn strategy(case: &Case) -> BoxedStrategy<Value> {
    let b = case.bmp_only;
    match case.ty {
        ColumnType::Char { chars, collation } => diff_string(collation, usize::from(chars), b)
            .prop_map(|s| Value::Str(Bytes::from(strip_spaces(s))))
            .boxed(),
        ColumnType::VarChar { chars, collation } => diff_string(collation, chars as usize, b)
            .prop_map(|s| Value::Str(Bytes::from(s)))
            .boxed(),
        ColumnType::Text { collation, .. } => diff_string(collation, 20, b)
            .prop_map(|s| Value::Str(Bytes::from(s)))
            .boxed(),
        ty => value_of(ty),
    }
}

/// Adjusts a generated value to what the server of `flavor` can store.
fn adjust(v: Value, flavor: Flavor) -> Value {
    match v {
        Value::Timestamp(t) => {
            // MySQL's range ends in 2038. Second 0 is the zero timestamp,
            // which has no fraction.
            let seconds = if flavor == Flavor::MySql {
                t.seconds & 0x7FFF_FFFF
            } else {
                t.seconds
            };
            let micros = if seconds == 0 { 0 } else { t.micros };
            Value::Timestamp(Timestamp::new(seconds, micros))
        }
        // MySQL's largest TIME is 838:59:59 with no fraction; MariaDB keeps
        // the fraction up to 838:59:59.999999.
        Value::Time(t) if flavor == Flavor::MySql => {
            let whole = Time::MAX_MICROS - 999_999;
            Value::Time(Time::from_micros(t.micros().clamp(-whole, whole)).unwrap())
        }
        v => v,
    }
}

/// Days since 1970-01-01 to a civil date.
fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02X}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) | Value::Bit(u) | Value::Set(u) => u.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::Float(f) => format!("{f:e}"),
        Value::Double(f) => format!("{f:e}"),
        Value::Date(d) => format!("'{d}'"),
        Value::Time(t) => {
            let abs = t.micros().unsigned_abs();
            format!(
                "'{}{}:{:02}:{:02}.{:06}'",
                if t.micros() < 0 { "-" } else { "" },
                abs / 3_600_000_000,
                abs / 60_000_000 % 60,
                abs / 1_000_000 % 60,
                abs % 1_000_000
            )
        }
        Value::DateTime(d) => format!(
            "'{} {:02}:{:02}:{:02}.{:06}'",
            d.date, d.hour, d.minute, d.second, d.micros
        ),
        Value::Timestamp(t) => {
            if t.is_zero() {
                return "'0000-00-00 00:00:00'".into();
            }
            let s = i64::from(t.seconds);
            let (y, m, d) = civil(s / 86_400);
            let r = s % 86_400;
            format!(
                "'{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:06}'",
                r / 3600,
                r / 60 % 60,
                r % 60,
                t.micros
            )
        }
        Value::Year(y) => y.to_string(),
        Value::Enum(i) => i.to_string(),
        Value::Str(b) | Value::Bytes(b) | Value::Json(b) => format!("x'{}'", hex(b)),
    }
}

/// The expression that reads the column back without loss.
fn read_back(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Bit { .. } | ColumnType::Enum { .. } | ColumnType::Set { .. } => {
            "CAST(v AS UNSIGNED)"
        }
        ColumnType::Float => "CAST(v AS DOUBLE)",
        ColumnType::Timestamp { .. } => "UNIX_TIMESTAMP(v)",
        ColumnType::Char { .. }
        | ColumnType::VarChar { .. }
        | ColumnType::Text { .. }
        | ColumnType::Binary { .. }
        | ColumnType::VarBinary { .. }
        | ColumnType::Blob { .. } => "HEX(v)",
        _ => "v",
    }
}

fn fraction(s: &str) -> Option<u32> {
    if s.is_empty() {
        return Some(0);
    }
    let digits = s.strip_prefix('.')?;
    if digits.len() > 6 {
        return None;
    }
    let n: u32 = digits.parse().ok()?;
    Some(n * 10u32.pow(6 - u32::try_from(digits.len()).ok()?))
}

fn parse_date(s: &str) -> Option<Date> {
    let mut p = s.split('-');
    let d = Date::new(
        p.next()?.parse().ok()?,
        p.next()?.parse().ok()?,
        p.next()?.parse().ok()?,
    );
    p.next().is_none().then_some(d)
}

fn parse_clock(s: &str) -> Option<(u16, u8, u8, u32)> {
    let (hms, frac) = match s.find('.') {
        Some(i) => (&s[..i], &s[i..]),
        None => (s, ""),
    };
    let mut p = hms.split(':');
    let out = (
        p.next()?.parse().ok()?,
        p.next()?.parse().ok()?,
        p.next()?.parse().ok()?,
        fraction(frac)?,
    );
    p.next().is_none().then_some(out)
}

/// The value a server's text stands for, in the column's variant.
fn parse_back(ty: ColumnType, s: &str) -> Option<Value> {
    if s == "NULL" {
        return Some(Value::Null);
    }
    Some(match ty {
        ColumnType::Int {
            unsigned: false, ..
        } => Value::Int(s.parse().ok()?),
        ColumnType::Int { unsigned: true, .. } => Value::UInt(s.parse().ok()?),
        ColumnType::Bit { .. } => Value::Bit(s.parse().ok()?),
        ColumnType::Decimal { .. } => Value::Decimal(Decimal::parse(s)?),
        ColumnType::Float => {
            let d: f64 = s.parse().ok()?;
            let f = d as f32;
            // The double must be the float exactly, or the read lost bits.
            (f64::from(f).to_bits() == d.to_bits() || d == 0.0).then_some(Value::Float(f))?
        }
        ColumnType::Double => Value::Double(s.parse().ok()?),
        ColumnType::Date => Value::Date(parse_date(s)?),
        ColumnType::Time { .. } => {
            let (neg, body) = s.strip_prefix('-').map_or((false, s), |b| (true, b));
            let (h, m, sec, us) = parse_clock(body)?;
            Value::Time(Time::new(neg, h, m, sec, us)?)
        }
        ColumnType::DateTime { .. } => {
            let (date, clock) = s.split_once(' ')?;
            let (h, m, sec, us) = parse_clock(clock)?;
            Value::DateTime(DateTime::new(
                parse_date(date)?,
                u8::try_from(h).ok()?,
                m,
                sec,
                us,
            ))
        }
        ColumnType::Timestamp { .. } => {
            let (secs, frac) = match s.find('.') {
                Some(i) => (&s[..i], &s[i..]),
                None => (s, ""),
            };
            Value::Timestamp(Timestamp::new(secs.parse().ok()?, fraction(frac)?))
        }
        ColumnType::Year => Value::Year(s.parse().ok()?),
        ColumnType::Char { .. } | ColumnType::VarChar { .. } | ColumnType::Text { .. } => {
            Value::Str(Bytes::from(unhex(s)?))
        }
        ColumnType::Binary { .. } | ColumnType::VarBinary { .. } | ColumnType::Blob { .. } => {
            Value::Bytes(Bytes::from(unhex(s)?))
        }
        ColumnType::Enum { .. } => Value::Enum(s.parse().ok()?),
        ColumnType::Set { .. } => Value::Set(s.parse().ok()?),
        ColumnType::Json => return None,
    })
}

/// Stored-value equality: bit for bit, except that a zero's sign is the
/// server's to keep or drop.
fn same_stored(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x == y || x.to_bits() == y.to_bits(),
        (Value::Double(x), Value::Double(y)) => x == y || x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

#[derive(Default)]
struct Counts {
    rows: usize,
    stored: usize,
    compare_ok: usize,
    compare_pairs: usize,
    equal_ok: usize,
    equal_rows: usize,
    sort_ok: usize,
    sort_pairs: usize,
    weights_ok: usize,
    weights: usize,
    notes: Vec<String>,
}

impl Counts {
    fn passed(&self, sort_enforced: bool) -> bool {
        self.rows > 0
            && self.stored == self.rows
            && self.compare_ok == self.compare_pairs
            && self.equal_ok == self.equal_rows
            && self.equal_rows == self.rows
            && self.weights_ok == self.weights
            && (!sort_enforced || self.sort_ok == self.sort_pairs)
    }

    fn add(&mut self, c: &Counts) {
        self.rows += c.rows;
        self.stored += c.stored;
        self.compare_ok += c.compare_ok;
        self.compare_pairs += c.compare_pairs;
        self.equal_ok += c.equal_ok;
        self.equal_rows += c.equal_rows;
        self.sort_ok += c.sort_ok;
        self.sort_pairs += c.sort_pairs;
        self.weights_ok += c.weights_ok;
        self.weights += c.weights;
    }

    fn line(&self) -> String {
        format!(
            "rows {} | stored {} | compare {}/{} | equal {}/{} | sort {}/{} | weights {}/{}",
            self.rows,
            self.stored,
            self.compare_ok,
            self.compare_pairs,
            self.equal_ok,
            self.equal_rows,
            self.sort_ok,
            self.sort_pairs,
            self.weights_ok,
            self.weights
        )
    }
}

fn note(c: &mut Counts, text: String) {
    if c.notes.len() < 4 {
        c.notes.push(text);
    }
}

/// Whether the server's `ORDER BY` is held to the engine's key order for
/// this table. MariaDB's sort deviates from its own comparison operators
/// (and from its index order) for NO PAD collations and for collations of
/// more than one level, in corner cases the corpus contains: it sorts by
/// padded, concatenated weight strings there. The comparison operators are
/// what a key must agree with; for those tables (and for MySQL's 64-member
/// `SET`) the sort count is reported but not required.
fn sort_enforced(case: &Case, flavor: Flavor) -> bool {
    use sprig_engine::keys::collation::Pad;
    match (case.ty, case.ty.collation()) {
        (_, Some(c)) if flavor == Flavor::MariaDb => c.pad() == Pad::Space && c.levels() == 1,
        // MySQL's sort reads a 64-member SET as a signed number (a set with
        // the last member sorts first); its index, like the key, doesn't.
        (ColumnType::Set { members: 64 }, _) => flavor != Flavor::MySql,
        _ => true,
    }
}

/// The engine's weight string in the form the server prints for this
/// column: a `utf8mb3_bin` column's weights are two bytes a character where
/// `utf8mb4_bin`'s are three.
fn engine_weights(case: &Case, c: Collation, s: &[u8], flavor: Flavor) -> String {
    let w = c.weight_string(s, flavor);
    if case.bmp_only && c == Collation::Utf8mb4Bin {
        return hex(&w
            .chunks(3)
            .flat_map(|t| t[1..].to_vec())
            .collect::<Vec<u8>>());
    }
    hex(&w)
}

struct Row {
    /// How many rows the server's `<` puts below this one; -1 for `NULL`.
    below: i64,
    /// How many rows the server's `=` matches, itself included; -1 for `NULL`.
    equals: i64,
    key: Vec<u8>,
    value: Value,
}

fn run_case(oracle: &Oracle, case: &Case, rows: usize, seed: u64) -> Counts {
    let mut counts = Counts::default();
    let mut seed_bytes = [0u8; 32];
    seed_bytes[..8].copy_from_slice(&seed.to_le_bytes());
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes),
    );
    let strat = strategy(case);
    let sent: Vec<Value> = (0..rows)
        .map(|i| {
            if i % 13 == 5 {
                Value::Null
            } else {
                adjust(
                    strat.new_tree(&mut runner).unwrap().current(),
                    oracle.flavor,
                )
            }
        })
        .collect();

    let is_text = case.ty.collation().is_some();
    // ENUM and SET columns compare with each other as their text; an index
    // (and ORDER BY) orders them by number, which is what the key encodes.
    let (a, b) = match case.ty {
        ColumnType::Enum { .. } | ColumnType::Set { .. } => {
            ("CAST(a.v AS UNSIGNED)", "CAST(b.v AS UNSIGNED)")
        }
        _ => ("a.v", "b.v"),
    };
    let mut sql = format!(
        "SET NAMES utf8mb4; SET sql_mode='ALLOW_INVALID_DATES'; SET time_zone='+00:00';\n\
         CREATE DATABASE IF NOT EXISTS {DB}; USE {DB};\n\
         DROP TABLE IF EXISTS t; CREATE TABLE t (id INT PRIMARY KEY, v {} NULL);\n",
        case.sql_type
    );
    for (chunk_no, chunk) in sent.chunks(250).enumerate() {
        sql.push_str("INSERT INTO t VALUES ");
        for (i, v) in chunk.iter().enumerate() {
            let _ = write!(
                sql,
                "{}({},{})",
                if i == 0 { "" } else { "," },
                chunk_no * 250 + i,
                literal(v)
            );
        }
        sql.push_str(";\n");
    }
    let _ = writeln!(
        sql,
        "SELECT 'R', id, {}, {} FROM t ORDER BY id;\n\
         SELECT 'C', a.id, IF(a.v IS NULL, -1, SUM({b} < {a})), IF(a.v IS NULL, -1, SUM({b} = {a})) \
         FROM t a JOIN t b GROUP BY a.id ORDER BY a.id;\n\
         SELECT 'O', id FROM t ORDER BY v, id;\n\
         DROP TABLE t;",
        read_back(case.ty),
        if is_text {
            "HEX(WEIGHT_STRING(v))"
        } else {
            "''"
        }
    );
    let out = match oracle.run(&sql) {
        Ok(out) => out,
        Err(e) => {
            counts
                .notes
                .push(format!("the server refused the table: {e}"));
            return counts;
        }
    };

    let col = KeyColumn::new(case.ty).nullable();
    let mut got: Vec<Option<Row>> = Vec::with_capacity(rows);
    let mut compared = Vec::new();
    let mut sorted: Vec<usize> = Vec::new();
    for line in out.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        match f.first().copied() {
            Some("C") => compared.push((
                f.get(2).and_then(|s| s.parse::<i64>().ok()),
                f.get(3).and_then(|s| s.parse::<i64>().ok()),
            )),
            Some("O") => sorted.extend(f.get(1).and_then(|s| s.parse::<usize>().ok())),
            Some("R") => {
                let i = counts.rows;
                counts.rows += 1;
                got.push(None);
                let (Some(sent), Some(stored)) =
                    (sent.get(i), f.get(2).and_then(|s| parse_back(case.ty, s)))
                else {
                    note(&mut counts, format!("unreadable row: {line}"));
                    continue;
                };
                if same_stored(&stored, sent) {
                    counts.stored += 1;
                } else {
                    note(&mut counts, format!("sent {sent:?}, stored {stored:?}"));
                }
                let mut key = Vec::new();
                if let Err(e) = encode_column(&mut key, &col, &stored) {
                    note(&mut counts, format!("{stored:?} has no key: {e}"));
                    continue;
                }
                if let (Some(c), Value::Str(s)) = (case.ty.collation(), &stored) {
                    counts.weights += 1;
                    let ours = engine_weights(case, c, s, oracle.flavor);
                    if f.get(3) == Some(&ours.as_str()) {
                        counts.weights_ok += 1;
                    } else {
                        let theirs = f.get(3).unwrap_or(&"");
                        let text =
                            format!("weights of x'{}': server {theirs}, engine {ours}", hex(s));
                        note(&mut counts, text);
                    }
                }
                got[i] = Some(Row {
                    below: 0,
                    equals: 0,
                    key,
                    value: stored,
                });
            }
            _ => note(&mut counts, format!("unexpected line: {line}")),
        }
    }
    if counts.rows != sent.len() || compared.len() != sent.len() || sorted.len() != sent.len() {
        let text = format!(
            "{} rows, {} comparisons and {} sorted ids came back of {}",
            counts.rows,
            compared.len(),
            sorted.len(),
            sent.len()
        );
        note(&mut counts, text);
        return counts;
    }
    for (row, (below, equals)) in got.iter_mut().zip(&compared) {
        if let (Some(row), Some(below), Some(equals)) = (row.as_mut(), below, equals) {
            row.below = *below;
            row.equals = *equals;
        }
    }

    // Equality: the server's `=` matches as many rows as share the key.
    // (With the order check below, which ties equal ranks to equal keys,
    // that makes the two groupings the same.)
    let mut with_key: std::collections::HashMap<&[u8], i64> = std::collections::HashMap::new();
    for row in got.iter().flatten() {
        *with_key.entry(&row.key).or_insert(0) += 1;
    }
    for row in got.iter().flatten() {
        counts.equal_rows += 1;
        let ours = if row.value.is_null() {
            -1
        } else {
            with_key[row.key.as_slice()]
        };
        if ours == row.equals {
            counts.equal_ok += 1;
        } else {
            let text = format!(
                "equality: the server's `=` matches {} rows to {:?}; {ours} have its key",
                row.equals, row.value
            );
            note(&mut counts, text);
        }
    }

    // Order: going up the server's `<`, the keys go up.
    let mut by_rank: Vec<&Row> = got.iter().flatten().collect();
    by_rank.sort_by(|x, y| x.below.cmp(&y.below).then_with(|| x.key.cmp(&y.key)));
    for w in by_rank.windows(2) {
        counts.compare_pairs += 1;
        if w[0].below.cmp(&w[1].below) == w[0].key.cmp(&w[1].key) {
            counts.compare_ok += 1;
        } else {
            let text = format!(
                "comparison: the server puts {} rows below {:?} and {} below {:?}; keys compare {:?}",
                w[0].below,
                w[0].value,
                w[1].below,
                w[1].value,
                w[0].key.cmp(&w[1].key)
            );
            note(&mut counts, text);
        }
    }

    // Sort: along the server's ORDER BY, the keys never go down.
    for w in sorted.windows(2) {
        let (Some(Some(x)), Some(Some(y))) = (got.get(w[0]), got.get(w[1])) else {
            continue;
        };
        counts.sort_pairs += 1;
        if x.key <= y.key {
            counts.sort_ok += 1;
        } else if sort_enforced(case, oracle.flavor) {
            let text = format!(
                "sort: the server orders {:?} before {:?}; the keys say the opposite",
                x.value, y.value
            );
            note(&mut counts, text);
        }
    }
    counts
}

fn run_oracle(oracle: &Oracle, rows: usize, report: &mut String) -> usize {
    let only = std::env::var("SPRIG_TYPES_ORACLE_ONLY").ok();
    let seed = std::env::var("SPRIG_TYPES_ORACLE_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x5eed_0000);
    let _ = writeln!(report, "seed {seed}, {rows} values per table");
    let version = oracle
        .run("SELECT VERSION()")
        .unwrap_or_else(|e| panic!("{} is not reachable: {e}", oracle.label));
    let _ = writeln!(report, "## {} {}", oracle.label, version.trim());
    let mut failed = 0;
    let (mut tables, mut total) = (0, Counts::default());
    for (i, case) in cases(oracle.flavor).iter().enumerate() {
        if only
            .as_ref()
            .is_some_and(|o| !case.sql_type.contains(o.as_str()))
        {
            continue;
        }
        let c = run_case(oracle, case, rows, seed.wrapping_add(i as u64));
        let enforced = sort_enforced(case, oracle.flavor);
        let ok = c.passed(enforced);
        let _ = writeln!(
            report,
            "{} | {} | {}{} | {}",
            oracle.label,
            case.sql_type.chars().take(70).collect::<String>(),
            c.line(),
            if enforced { "" } else { " (sort not required)" },
            if ok { "ok" } else { "FAILED" }
        );
        for n in &c.notes {
            let _ = writeln!(report, "    {n}");
        }
        tables += 1;
        failed += usize::from(!ok);
        total.add(&c);
    }
    let _ = oracle.run(&format!("DROP DATABASE IF EXISTS {DB}"));
    let _ = writeln!(
        report,
        "{} total: {tables} tables, {failed} failed | {}\n",
        oracle.label,
        total.line()
    );
    failed
}

#[test]
fn key_order_matches_the_servers() {
    let rows = std::env::var("SPRIG_TYPES_ORACLE_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);
    let oracles: Vec<Oracle> = [
        (Flavor::MariaDb, "MariaDB", "SPRIG_TYPES_ORACLE_MARIADB"),
        (Flavor::MySql, "MySQL", "SPRIG_TYPES_ORACLE_MYSQL"),
    ]
    .into_iter()
    .filter_map(|(flavor, label, var)| {
        std::env::var(var).ok().map(|command| Oracle {
            flavor,
            label,
            command,
        })
    })
    .collect();
    if oracles.is_empty() {
        eprintln!("skipped: set SPRIG_TYPES_ORACLE_MARIADB and SPRIG_TYPES_ORACLE_MYSQL to run it");
        return;
    }
    let mut report = String::new();
    let mut failed = 0;
    for o in &oracles {
        failed += run_oracle(o, rows, &mut report);
    }
    if let Ok(path) = std::env::var("SPRIG_TYPES_ORACLE_REPORT") {
        std::fs::write(path, &report).unwrap();
    }
    println!("{report}");
    assert_eq!(failed, 0, "{failed} tables disagree with a server");
}

#[test]
fn helpers() {
    assert_eq!(civil(0), (1970, 1, 1));
    assert_eq!(civil(19_782), (2024, 2, 29));
    assert_eq!(civil(i64::from(u32::MAX) / 86_400), (2106, 2, 7));
    assert_eq!(
        literal(&Value::Timestamp(Timestamp::new(u32::MAX, 5))),
        "'2106-02-07 06:28:15.000005'"
    );
    assert_eq!(
        literal(&Value::Time(Time::new(true, 838, 59, 59, 999_999).unwrap())),
        "'-838:59:59.999999'"
    );
    assert_eq!(
        parse_back(ColumnType::Time { fsp: 2 }, "-00:00:01.50"),
        Some(Value::Time(Time::from_micros(-1_500_000).unwrap()))
    );
    assert_eq!(
        parse_back(ColumnType::Timestamp { fsp: 3 }, "12.500"),
        Some(Value::Timestamp(Timestamp::new(12, 500_000)))
    );
    assert_eq!(
        parse_back(ColumnType::DateTime { fsp: 0 }, "0000-00-00 00:00:00"),
        Some(Value::DateTime(DateTime::default()))
    );
    assert_eq!(unhex("0aFF"), Some(vec![10, 255]));
    assert_eq!(unhex("0"), None);
    assert!(!cases(Flavor::MariaDb).is_empty());
    assert!(cases(Flavor::MySql).len() < cases(Flavor::MariaDb).len());
}

/// Every code point (and every byte of the single-byte sets) against dumps
/// of the servers' `WEIGHT_STRING`, one `<number>\t<hex>` line each, in the
/// directory `SPRIG_TYPES_ORACLE_DUMPS` names (files `maria_<collation>.tsv`,
/// `maria_utf8mb4_uca1400_as_cs_l<level>.tsv`, `mysql_<collation>.tsv`).
/// Skipped when the variable is unset.
#[test]
fn every_code_point_weighs_as_dumped() {
    let Ok(dir) = std::env::var("SPRIG_TYPES_ORACLE_DUMPS") else {
        eprintln!("skipped: set SPRIG_TYPES_ORACLE_DUMPS to a directory of weight dumps");
        return;
    };
    let unit = |c: Collation, n: u32| -> Vec<u8> {
        if c.charset() == Charset::Utf8mb4 {
            char::from_u32(n).unwrap().to_string().into_bytes()
        } else {
            vec![n as u8]
        }
    };
    let check = |file: &str, weigh: &dyn Fn(u32) -> Vec<u8>| -> usize {
        let text = std::fs::read_to_string(format!("{dir}/{file}")).unwrap();
        let mut lines = 0;
        for line in text.lines() {
            let (n, want) = line.split_once('\t').unwrap_or((line, ""));
            let n: u32 = n.parse().unwrap();
            assert_eq!(hex(&weigh(n)), want, "{file}: {n:#X}");
            lines += 1;
        }
        lines
    };
    let mut checked = 0;
    for level in 0..3 {
        let c = Collation::Utf8mb4Uca1400AsCs;
        checked += check(
            &format!("maria_utf8mb4_uca1400_as_cs_l{}.tsv", level + 1),
            &|n| {
                let mut w = Vec::new();
                c.level_weights(&unit(c, n), level, &mut w);
                w.iter().flat_map(|x| (*x as u16).to_be_bytes()).collect()
            },
        );
    }
    for (server, flavor, list) in [
        (
            "maria",
            Flavor::MariaDb,
            &[
                Collation::Utf8mb4GeneralCi,
                Collation::Utf8mb4UnicodeCi,
                Collation::Utf8mb4Unicode520Ci,
                Collation::Latin1SwedishCi,
                Collation::Latin1GeneralCi,
                Collation::Latin1GeneralCs,
                Collation::Latin1Bin,
                Collation::AsciiGeneralCi,
            ][..],
        ),
        (
            "mysql",
            Flavor::MySql,
            &[
                Collation::Utf8mb4Mysql0900AiCi,
                Collation::Utf8mb4Mysql0900AsCi,
                Collation::Utf8mb4Mysql0900AsCs,
                Collation::Utf8mb4GeneralCi,
                Collation::Utf8mb4UnicodeCi,
                Collation::Utf8mb4Unicode520Ci,
                Collation::Latin1SwedishCi,
                Collation::Latin1GeneralCi,
                Collation::Latin1GeneralCs,
                Collation::Latin1Bin,
                Collation::AsciiGeneralCi,
            ][..],
        ),
    ] {
        for &c in list {
            checked += check(&format!("{server}_{}.tsv", c.name()), &|n| {
                c.weight_string(&unit(c, n), flavor)
            });
        }
    }
    println!("{checked} weight strings equal the servers' dumps");
    assert!(checked > 10_000_000);
}
