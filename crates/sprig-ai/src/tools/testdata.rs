//! Test rows generated from a table's schema: the generator behind the
//! DBA assistant's `generate_rows` proposal.
//!
//! ```text
//! the gateway reads, as the caller: columns (types, NOT NULL, defaults,
//!   auto_increment, generated), unique keys, foreign keys with up to
//!   MAX_PARENT_KEYS existing parent keys, CHECK clauses, and MAX of each
//!   unique integer column
//! per column, the first that applies:
//!   skipped: auto_increment and generated columns
//!   the model's hint (kind, values, min, max, domain, null_percent)
//!   a foreign key: an existing parent key (a whole tuple for composite keys)
//!   ENUM and SET: their members; a CHECK's IN list
//!   the name (email, phone, names, address parts, country, currency, url,
//!     uuid, status, ...) for text; the type (integer and decimal ranges,
//!     dates in the last two years, times, JSON, binary, IP, UUID)
//!   CHECK bounds (col > n, col BETWEEN a AND b, AND of these) narrow ranges
//! unique columns: sequences from MAX + 1, or a run tag and a sequence in
//!   strings; composite unique keys: retried until distinct
//! → INSERT batches of 500 rows; at most MAX_ROWS rows and MAX_SQL_BYTES
//! ```
//!
//! The rows go only to the user (the proposal's `generated` part); the
//! model gets [`Generated::summary`], which holds no value. Generators are
//! simple built-in word lists and a seeded PRNG: no outside data.

#![allow(
    clippy::many_single_char_names,
    clippy::similar_names,
    clippy::struct_excessive_bools,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)] // generators: short names for numbers, ranges clamped before casts

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;

use serde_json::{Value, json};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use super::sensitive::{self, Category, ColumnInfo};

/// Rows one proposal generates at most.
pub const MAX_ROWS: usize = 10_000;
/// The generated statements' size at most.
pub const MAX_SQL_BYTES: usize = 4 * 1024 * 1024;
/// Rows per `INSERT`.
pub const BATCH: usize = 500;
/// Existing parent keys a foreign key picks from, at most.
pub const MAX_PARENT_KEYS: usize = 10_000;
/// Column hints one call takes, at most.
pub const MAX_HINTS: usize = 200;

/// The kinds of value a hint may ask for.
pub const KINDS: &[&str] = &[
    "one_of",
    "sequence",
    "email",
    "phone",
    "first_name",
    "last_name",
    "full_name",
    "company",
    "street_address",
    "city",
    "postcode",
    "country_code",
    "currency",
    "url",
    "ip_address",
    "uuid",
    "word",
    "sentence",
    "integer",
    "decimal",
    "date",
    "datetime",
    "boolean",
    "null",
];

/// One column, as the gateway read it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenColumn {
    /// Its name.
    pub name: String,
    /// `DATA_TYPE`, lower case.
    pub data_type: String,
    /// `COLUMN_TYPE`, lower case (`varchar(255)`, `enum('a','b')`).
    pub column_type: String,
    /// `IS_NULLABLE`.
    pub nullable: bool,
    /// `auto_increment`: left out.
    pub auto_increment: bool,
    /// A generated column: left out.
    pub generated: bool,
    /// The primary key or a unique key on this column alone.
    pub unique: bool,
    /// For a unique integer column: its largest value now.
    pub max_existing: Option<i64>,
}

/// A foreign key with the parent keys that exist.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ForeignKey {
    /// The child's columns.
    pub columns: Vec<String>,
    /// The parent table (`db.table` or `table`).
    pub parent: String,
    /// Existing parent keys, each a tuple in `columns`' order.
    pub keys: Vec<Vec<Value>>,
}

/// A table, as the gateway read it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GenTable {
    /// The database.
    pub db: String,
    /// The table.
    pub table: String,
    /// Its columns, in order.
    pub columns: Vec<GenColumn>,
    /// Unique keys of more than one column.
    pub unique_keys: Vec<Vec<String>>,
    /// Its foreign keys.
    pub foreign_keys: Vec<ForeignKey>,
    /// Its CHECK clauses.
    pub checks: Vec<String>,
}

/// The model's hint for one column.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hint {
    /// The column.
    pub column: String,
    /// One of [`KINDS`].
    pub kind: Option<String>,
    /// Values to pick from.
    pub values: Vec<String>,
    /// The smallest value: a number or `YYYY-MM-DD`.
    pub min: Option<String>,
    /// The largest value.
    pub max: Option<String>,
    /// The domain of emails and URLs.
    pub domain: Option<String>,
    /// Rows to leave NULL, in percent.
    pub null_percent: Option<u8>,
}

/// One hint line: `<column>: <kind>[, values=a|b][, min=..][, max=..]
/// [, domain=..][, nulls=NN]`, as an object.
fn hint_line(line: &str) -> Result<Value, String> {
    let (col, rest) = line
        .split_once(':')
        .ok_or_else(|| format!("hint {line:?} is \"<column>: <kind>, ...\""))?;
    let mut h = serde_json::Map::new();
    h.insert("column".into(), json!(col.trim()));
    for (i, part) in rest.split(',').map(str::trim).enumerate() {
        if part.is_empty() {
            continue;
        }
        match part.split_once('=') {
            None if i == 0 => {
                h.insert("kind".into(), json!(part));
            }
            None => return Err(format!("hint {line:?}: {part:?} isn't key=value")),
            Some((k, v)) => {
                let v = v.trim();
                match k.trim() {
                    "values" => {
                        h.insert(
                            "values".into(),
                            json!(v.split('|').map(str::trim).collect::<Vec<_>>()),
                        );
                    }
                    "min" | "max" | "domain" => {
                        h.insert(k.trim().into(), json!(v));
                    }
                    "nulls" | "null_percent" => {
                        let n: u64 = v
                            .trim_end_matches('%')
                            .parse()
                            .map_err(|_| format!("hint {line:?}: nulls is a percent"))?;
                        h.insert("null_percent".into(), json!(n));
                    }
                    other => return Err(format!("hint {line:?}: unknown key {other}")),
                }
            }
        }
    }
    Ok(Value::Object(h))
}

/// `generate_rows`' `columns`: hint lines (as the tool takes them, which
/// keeps its schema small for providers' strict modes), or hint objects.
fn hint_array(v: Option<&Value>) -> Result<Vec<Value>, String> {
    match v {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(a)) => a
            .iter()
            .map(|x| match x {
                Value::String(line) => hint_line(line),
                Value::Object(_) => Ok(x.clone()),
                _ => Err("a column hint is a line or an object".to_owned()),
            })
            .collect(),
        Some(Value::String(t)) if t.trim().is_empty() => Ok(Vec::new()),
        Some(Value::String(t)) => match serde_json::from_str::<Value>(t) {
            Ok(Value::Array(a)) => Ok(a),
            _ => Err("columns is a list of hint lines".to_owned()),
        },
        Some(_) => Err("columns is a list of hint lines".to_owned()),
    }
}

/// The hints in a `generate_rows` input's `columns`.
#[must_use]
pub fn hints_of(v: Option<&Value>) -> Vec<Hint> {
    let arr = Value::Array(hint_array(v).unwrap_or_default());
    hints_in(Some(&arr))
}

fn hints_in(v: Option<&Value>) -> Vec<Hint> {
    let s = |h: &Value, k: &str| {
        h.get(k)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_owned)
    };
    v.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|h| Hint {
            column: s(h, "column").unwrap_or_default(),
            kind: s(h, "kind"),
            values: h
                .get("values")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect(),
            min: s(h, "min"),
            max: s(h, "max"),
            domain: s(h, "domain"),
            null_percent: h
                .get("null_percent")
                .and_then(Value::as_u64)
                .and_then(|n| u8::try_from(n.min(100)).ok()),
        })
        .collect()
}

/// The checks a schema can't say about hints.
///
/// # Errors
/// What is wrong, for the model.
pub fn check_hints(v: Option<&Value>) -> Result<(), String> {
    let arr = hint_array(v)?;
    if arr.iter().any(|h| !h.is_object()) {
        return Err("every column hint is an object".to_owned());
    }
    let v = Value::Array(arr);
    let v = Some(&v);
    let hs = hints_of(v);
    if hs.len() > MAX_HINTS {
        return Err(format!("columns holds at most {MAX_HINTS} hints"));
    }
    for h in &hs {
        if h.column.is_empty() {
            return Err("every column hint names its column".to_owned());
        }
        if h.kind.as_deref() == Some("one_of") && h.values.is_empty() {
            return Err(format!("{}: kind one_of needs values", h.column));
        }
        if h.values.len() > 1000 {
            return Err(format!("{}: at most 1000 values", h.column));
        }
        if let Some(k) = &h.kind
            && !KINDS.contains(&k.as_str())
        {
            return Err(format!("{}: kind is one of {}", h.column, KINDS.join(", ")));
        }
    }
    for h in v.and_then(Value::as_array).into_iter().flatten() {
        if let Some(n) = h.get("null_percent").and_then(Value::as_i64)
            && !(0..=100).contains(&n)
        {
            return Err("null_percent is 0 to 100".to_owned());
        }
    }
    Ok(())
}

/// What was generated.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Generated {
    /// The columns written, in order.
    pub columns: Vec<String>,
    /// The rows, as SQL literals.
    pub rows: Vec<Vec<String>>,
    /// How each column was filled, in words (no value).
    pub plan: Vec<(String, String)>,
    /// Columns left out, and why.
    pub skipped: Vec<(String, String)>,
    /// CHECK clauses used, and ones not understood.
    pub checks_used: Vec<String>,
    /// CHECK clauses not understood: rows may fail them.
    pub checks_unknown: Vec<String>,
    /// Anything else the user should know.
    pub notes: Vec<String>,
}

// ---- randomness ----

/// `SplitMix64`: small, seeded, good enough for test data.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// A generator from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// In `lo..=hi`.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        if hi <= lo {
            return lo;
        }
        let span = (i128::from(hi) - i128::from(lo) + 1) as u128;
        let r = u128::from(self.next()) % span;
        (i128::from(lo) + r as i128) as i64
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        let i = self.range(0, xs.len() as i64 - 1);
        &xs[usize::try_from(i).unwrap_or(0)]
    }

    fn word(&mut self, xs: &[&'static str]) -> &'static str {
        let i = self.range(0, xs.len() as i64 - 1);
        xs[usize::try_from(i).unwrap_or(0)]
    }

    fn percent(&mut self, p: u8) -> bool {
        self.range(0, 99) < i64::from(p)
    }
}

// ---- word lists ----

const FIRST: &[&str] = &[
    "Ada", "Alan", "Amara", "Ana", "Arjun", "Ben", "Chen", "Clara", "Dev", "Elena", "Emma",
    "Farah", "Grace", "Hana", "Ivan", "Jonas", "Kai", "Lara", "Leo", "Maya", "Mina", "Noah",
    "Omar", "Priya", "Ravi", "Rosa", "Sam", "Sara", "Tariq", "Uma", "Vera", "Yuki", "Zoe",
];
const LAST: &[&str] = &[
    "Abbott", "Banerjee", "Becker", "Costa", "Diaz", "Eriksen", "Fischer", "Garcia", "Haddad",
    "Ito", "Jensen", "Khan", "Kowalski", "Larsen", "Lopez", "Mehta", "Moreau", "Nakamura", "Novak",
    "Okafor", "Patel", "Quinn", "Rossi", "Silva", "Tanaka", "Umar", "Varga", "Weber", "Xu",
    "Yilmaz", "Zhang",
];
const COMPANY: &[&str] = &[
    "Acme",
    "Bluebird",
    "Cobalt",
    "Driftwood",
    "Evergreen",
    "Fernway",
    "Granite",
    "Harbor",
    "Ironleaf",
    "Juniper",
    "Keystone",
    "Lumen",
    "Meridian",
    "Northwind",
    "Orchid",
    "Pinecrest",
];
const COMPANY_END: &[&str] = &["Ltd", "Inc", "GmbH", "Labs", "Group", "Partners", "Co"];
const STREET: &[&str] = &[
    "Oak", "Maple", "Station", "Church", "Mill", "Park", "River", "High", "Market", "Garden",
];
const STREET_END: &[&str] = &["Street", "Road", "Lane", "Avenue", "Way", "Close"];
const CITY: &[&str] = &[
    "Amsterdam",
    "Austin",
    "Berlin",
    "Bengaluru",
    "Lisbon",
    "Lyon",
    "Manchester",
    "Melbourne",
    "Montreal",
    "Nairobi",
    "Osaka",
    "Porto",
    "Pune",
    "Seoul",
    "Toronto",
    "Valencia",
    "Zurich",
];
const COUNTRY2: &[&str] = &[
    "AU", "BR", "CA", "DE", "ES", "FR", "GB", "IE", "IN", "IT", "JP", "KE", "NL", "PT", "SE", "US",
];
const COUNTRY3: &[&str] = &[
    "AUS", "BRA", "CAN", "DEU", "ESP", "FRA", "GBR", "IRL", "IND", "ITA", "JPN", "KEN", "NLD",
    "PRT", "SWE", "USA",
];
const CURRENCY: &[&str] = &[
    "AUD", "CAD", "CHF", "EUR", "GBP", "INR", "JPY", "SEK", "USD",
];
const WORDS: &[&str] = &[
    "amber", "basil", "cedar", "delta", "ember", "fable", "garnet", "harbor", "indigo", "jasper",
    "kepler", "lotus", "meadow", "nimbus", "onyx", "pebble", "quartz", "raven", "sierra", "tundra",
    "umber", "velvet", "willow", "zephyr",
];
const STATUS: &[&str] = &["new", "active", "pending", "complete", "cancelled"];
const COLOR: &[&str] = &["red", "green", "blue", "black", "white", "grey", "yellow"];
/// Card numbers card networks publish for tests: they pass the Luhn check
/// and charge nothing.
const TEST_CARDS: &[&str] = &[
    "4111111111111111",
    "4012888888881881",
    "5555555555554444",
    "5105105105105100",
    "378282246310005",
    "6011111111111117",
];

// ---- dates ----

/// Days since 1970-01-01 of a civil date.
#[must_use]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of a day number.
#[must_use]
pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn parse_date(s: &str) -> Option<i64> {
    let mut p = s.trim().get(..10)?.split('-');
    let y = p.next()?.parse().ok()?;
    let m: i64 = p.next()?.parse().ok()?;
    let d: i64 = p.next()?.parse().ok()?;
    ((1..=12).contains(&m) && (1..=31).contains(&d)).then(|| days_from_civil(y, m, d))
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

// ---- CHECK constraints ----

/// What CHECK clauses say about one column.
#[derive(Debug, Clone, Default, PartialEq)]
struct Bounds {
    min: Option<f64>,
    max: Option<f64>,
    /// `min` and `max` exclude themselves.
    min_open: bool,
    max_open: bool,
    /// An IN list, as SQL literals.
    set: Option<Vec<String>>,
}

fn ident_of(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(i) => Some(i.value.to_lowercase()),
        Expr::CompoundIdentifier(p) => p.last().map(|i| i.value.to_lowercase()),
        Expr::Nested(x) => ident_of(x),
        _ => None,
    }
}

fn number_of(e: &Expr) -> Option<f64> {
    match e {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => n.parse().ok(),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => number_of(expr).map(|n| -n),
        Expr::Nested(x) => number_of(x),
        _ => None,
    }
}

fn literal_of(e: &Expr) -> Option<String> {
    match e {
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => Some(n.clone()),
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Some(quote(s)),
            _ => None,
        },
        _ => number_of(e).map(|n| n.to_string()),
    }
}

/// Applies `e` to `bounds`; false when it isn't understood.
fn apply_check(e: &Expr, bounds: &mut BTreeMap<String, Bounds>) -> bool {
    match e {
        Expr::Nested(x) => apply_check(x, bounds),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => apply_check(left, bounds) & apply_check(right, bounds),
        Expr::BinaryOp { left, op, right } => {
            let (col, n, op) = match (
                ident_of(left),
                number_of(right),
                ident_of(right),
                number_of(left),
            ) {
                (Some(c), Some(n), _, _) => (c, n, op.clone()),
                (_, _, Some(c), Some(n)) => (
                    c,
                    n,
                    match op {
                        BinaryOperator::Gt => BinaryOperator::Lt,
                        BinaryOperator::GtEq => BinaryOperator::LtEq,
                        BinaryOperator::Lt => BinaryOperator::Gt,
                        BinaryOperator::LtEq => BinaryOperator::GtEq,
                        o => o.clone(),
                    },
                ),
                _ => return false,
            };
            let b = bounds.entry(col).or_default();
            match op {
                BinaryOperator::Gt => {
                    b.min = Some(b.min.map_or(n, |m| m.max(n)));
                    b.min_open = true;
                }
                BinaryOperator::GtEq => b.min = Some(b.min.map_or(n, |m| m.max(n))),
                BinaryOperator::Lt => {
                    b.max = Some(b.max.map_or(n, |m| m.min(n)));
                    b.max_open = true;
                }
                BinaryOperator::LtEq => b.max = Some(b.max.map_or(n, |m| m.min(n))),
                BinaryOperator::Eq => {
                    b.min = Some(n);
                    b.max = Some(n);
                }
                // `<> 0` and the like hold for almost every generated value.
                BinaryOperator::NotEq => {}
                _ => return false,
            }
            true
        }
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } => match (ident_of(expr), number_of(low), number_of(high)) {
            (Some(c), Some(lo), Some(hi)) => {
                let b = bounds.entry(c).or_default();
                b.min = Some(b.min.map_or(lo, |m| m.max(lo)));
                b.max = Some(b.max.map_or(hi, |m| m.min(hi)));
                true
            }
            _ => false,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let Some(c) = ident_of(expr) else {
                return false;
            };
            let vals: Option<Vec<String>> = list.iter().map(literal_of).collect();
            match vals {
                Some(v) if !v.is_empty() => {
                    bounds.entry(c).or_default().set = Some(v);
                    true
                }
                _ => false,
            }
        }
        Expr::IsNotNull(x) => ident_of(x).is_some(),
        _ => false,
    }
}

fn parse_checks(
    checks: &[String],
    used: &mut Vec<String>,
    unknown: &mut Vec<String>,
) -> BTreeMap<String, Bounds> {
    let mut bounds = BTreeMap::new();
    for c in checks {
        let parsed = Parser::new(&MySqlDialect {})
            .try_with_sql(c)
            .and_then(|mut p| p.parse_expr());
        let ok = match parsed {
            Ok(e) => {
                let mut b = bounds.clone();
                let ok = apply_check(&e, &mut b);
                if ok {
                    bounds = b;
                }
                ok
            }
            Err(_) => false,
        };
        if ok {
            used.push(c.clone());
        } else {
            unknown.push(c.clone());
        }
    }
    bounds
}

// ---- literals ----

/// A string as a SQL literal.
#[must_use]
pub fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

fn json_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_owned(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote(s),
        other => quote(&other.to_string()),
    }
}

/// The text inside `name(...)`: `varchar(255)` → `255`.
fn args_of(column_type: &str) -> Option<&str> {
    let open = column_type.find('(')?;
    let close = column_type.rfind(')')?;
    (close > open).then(|| &column_type[open + 1..close])
}

/// ENUM and SET members, as SQL literals.
fn members(column_type: &str) -> Vec<String> {
    let Some(a) = args_of(column_type) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut inside = false;
    let mut chars = a.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' if inside && chars.peek() == Some(&'\'') => {
                chars.next();
                cur.push('\'');
            }
            '\'' if inside => {
                out.push(quote(&cur));
                cur.clear();
                inside = false;
            }
            '\'' => inside = true,
            c if inside => cur.push(c),
            _ => {}
        }
    }
    out
}

fn max_len(c: &GenColumn) -> Option<usize> {
    match c.data_type.as_str() {
        "char" | "varchar" | "binary" | "varbinary" => args_of(&c.column_type)?.trim().parse().ok(),
        "tinytext" | "tinyblob" => Some(255),
        _ => None,
    }
}

fn int_range(c: &GenColumn) -> (i64, i64) {
    let unsigned = c.column_type.contains("unsigned");
    match (c.data_type.as_str(), unsigned) {
        ("tinyint", false) => (-128, 127),
        ("tinyint", true) => (0, 255),
        ("smallint", false) => (-32_768, 32_767),
        ("smallint", true) => (0, 65_535),
        ("mediumint", false) => (-8_388_608, 8_388_607),
        ("mediumint", true) => (0, 16_777_215),
        ("int" | "integer", false) => (i64::from(i32::MIN), i64::from(i32::MAX)),
        ("int" | "integer", true) => (0, i64::from(u32::MAX)),
        (_, true) => (0, i64::MAX),
        _ => (i64::MIN, i64::MAX),
    }
}

/// `decimal(10,2)` → (10, 2).
fn precision(c: &GenColumn) -> (u32, u32) {
    let a = args_of(&c.column_type).unwrap_or("10,0");
    let mut p = a.split(',').map(|x| x.trim().parse::<u32>().unwrap_or(0));
    let prec = p.next().unwrap_or(10).clamp(1, 38);
    let scale = p.next().unwrap_or(0).min(prec);
    (prec, scale)
}

fn is_bool(c: &GenColumn) -> bool {
    matches!(c.data_type.as_str(), "bool" | "boolean")
        || c.column_type.starts_with("tinyint(1)")
        || c.column_type.starts_with("bit(1)")
}

fn is_textual(c: &GenColumn) -> bool {
    matches!(
        c.data_type.as_str(),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
    )
}

fn toks(name: &str) -> Vec<String> {
    name.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|x| !x.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

// ---- generators ----

/// How one column is filled.
#[derive(Debug, Clone, PartialEq)]
enum Gen {
    Null,
    OneOf(Vec<String>),
    Sequence { next: i64 },
    Int { lo: i64, hi: i64 },
    Decimal { lo: f64, hi: f64, scale: u32 },
    Date { lo: i64, hi: i64, time: bool },
    Time,
    Year { lo: i64, hi: i64 },
    Bool,
    Email { domain: String },
    Phone,
    First,
    Last,
    Full,
    Company,
    Street,
    City,
    Postcode,
    Country(usize),
    Currency,
    Url { domain: String },
    Ip { v6: bool },
    Uuid,
    Word,
    Sentence,
    Code,
    Card,
    NationalId,
    Iban,
    Secret,
    Json,
    Binary,
    Point,
    Parent(usize),
}

impl Gen {
    fn words(&self) -> String {
        match self {
            Gen::Null => "NULL".into(),
            Gen::OneOf(v) => format!("one of {} values", v.len()),
            Gen::Sequence { next } => format!("a sequence from {next}"),
            Gen::Int { lo, hi } => format!("integers from {lo} to {hi}"),
            Gen::Decimal { lo, hi, scale } => {
                format!("decimals from {lo} to {hi} with {scale} places")
            }
            Gen::Date { lo, hi, time } => format!(
                "{} from {} to {}",
                if *time { "date-times" } else { "dates" },
                date_text(*lo),
                date_text(*hi)
            ),
            Gen::Time => "times of day".into(),
            Gen::Year { lo, hi } => format!("years from {lo} to {hi}"),
            Gen::Bool => "0 or 1".into(),
            Gen::Email { domain } => format!("emails on {domain}"),
            Gen::Phone => "phone numbers in the 555 test range".into(),
            Gen::First => "first names".into(),
            Gen::Last => "last names".into(),
            Gen::Full => "full names".into(),
            Gen::Company => "company names".into(),
            Gen::Street => "street addresses".into(),
            Gen::City => "city names".into(),
            Gen::Postcode => "postcodes".into(),
            Gen::Country(n) => format!("ISO country codes ({n} letters)"),
            Gen::Currency => "ISO currency codes".into(),
            Gen::Url { domain } => format!("URLs on {domain}"),
            Gen::Ip { v6 } => format!(
                "{} documentation addresses",
                if *v6 { "IPv6" } else { "IPv4" }
            ),
            Gen::Uuid => "UUIDs".into(),
            Gen::Word => "words".into(),
            Gen::Sentence => "short sentences".into(),
            Gen::Code => "upper-case codes".into(),
            Gen::Card => "payment networks' test card numbers".into(),
            Gen::NationalId => "obviously fake ids (TEST-...)".into(),
            Gen::Iban => "the published example IBAN".into(),
            Gen::Secret => "random hex strings".into(),
            Gen::Json => "small JSON objects".into(),
            Gen::Binary => "random bytes".into(),
            Gen::Point => "points".into(),
            Gen::Parent(_) => "existing parent keys".into(),
        }
    }
}

struct Ctx<'a> {
    today: i64,
    hint: Option<&'a Hint>,
}

fn hint_num(s: Option<&String>) -> Option<f64> {
    s.and_then(|x| x.trim().parse::<f64>().ok())
}

fn hint_date(s: Option<&String>) -> Option<i64> {
    s.and_then(|x| parse_date(x))
}

#[allow(clippy::too_many_lines, clippy::cast_possible_truncation)] // one rule per kind
fn plan_column(
    c: &GenColumn,
    cx: &Ctx<'_>,
    b: Option<&Bounds>,
    table: &str,
) -> Result<Gen, String> {
    let t = toks(&c.name);
    let has = |w: &str| t.iter().any(|x| x == w);
    let domain = cx
        .hint
        .and_then(|h| h.domain.clone())
        .unwrap_or_else(|| "example.com".to_owned());
    // The hint first.
    if let Some(h) = cx.hint
        && let Some(k) = h.kind.as_deref()
    {
        let lo_n = hint_num(h.min.as_ref());
        let hi_n = hint_num(h.max.as_ref());
        return Ok(match k {
            "one_of" => Gen::OneOf(h.values.iter().map(|v| quote(v)).collect()),
            "sequence" => Gen::Sequence {
                next: lo_n.map_or(1, |x| x as i64),
            },
            "null" => Gen::Null,
            "email" => Gen::Email { domain },
            "phone" => Gen::Phone,
            "first_name" => Gen::First,
            "last_name" => Gen::Last,
            "full_name" => Gen::Full,
            "company" => Gen::Company,
            "street_address" => Gen::Street,
            "city" => Gen::City,
            "postcode" => Gen::Postcode,
            "country_code" => Gen::Country(if max_len(c) == Some(3) { 3 } else { 2 }),
            "currency" => Gen::Currency,
            "url" => Gen::Url { domain },
            "ip_address" => Gen::Ip {
                v6: c.data_type == "inet6",
            },
            "uuid" => Gen::Uuid,
            "word" => Gen::Word,
            "sentence" => Gen::Sentence,
            "boolean" => Gen::Bool,
            "integer" => Gen::Int {
                lo: lo_n.map_or(1, |x| x as i64),
                hi: hi_n.map_or(1000, |x| x as i64),
            },
            "decimal" => Gen::Decimal {
                lo: lo_n.unwrap_or(1.0),
                hi: hi_n.unwrap_or(1000.0),
                scale: precision(c).1,
            },
            "date" | "datetime" => Gen::Date {
                lo: hint_date(h.min.as_ref()).unwrap_or(cx.today - 730),
                hi: hint_date(h.max.as_ref()).unwrap_or(cx.today),
                time: k == "datetime" || c.data_type != "date",
            },
            other => return Err(format!("{}: unknown kind {other}", c.name)),
        });
    }
    if let Some(h) = cx.hint
        && !h.values.is_empty()
    {
        return Ok(Gen::OneOf(h.values.iter().map(|v| quote(v)).collect()));
    }
    if matches!(c.data_type.as_str(), "enum" | "set") {
        let m = members(&c.column_type);
        if !m.is_empty() {
            return Ok(Gen::OneOf(m));
        }
    }
    if let Some(set) = b.and_then(|b| b.set.clone()) {
        return Ok(Gen::OneOf(set));
    }
    let (hlo, hhi) = (
        cx.hint.and_then(|h| hint_num(h.min.as_ref())),
        cx.hint.and_then(|h| hint_num(h.max.as_ref())),
    );
    let dt = c.data_type.as_str();
    // Numbers.
    if is_bool(c) {
        return Ok(Gen::Bool);
    }
    if matches!(
        dt,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
    ) {
        let (tlo, thi) = int_range(c);
        let (mut lo, mut hi) = if has("age") {
            (18, 90)
        } else if has("year") {
            (1990, 2030)
        } else if has("rating") || has("stars") {
            (1, 5)
        } else if has("score") || has("percent") || has("pct") {
            (0, 100)
        } else if has("qty") || has("quantity") || has("count") || has("stock") {
            (1, 50)
        } else if has("priority") || has("level") || has("rank") {
            (1, 10)
        } else {
            (1, 100_000)
        };
        if let Some(b) = b {
            if let Some(m) = b.min {
                lo = (m.ceil() as i64) + i64::from(b.min_open && m.fract() == 0.0);
                hi = hi.max(lo);
            }
            if let Some(m) = b.max {
                hi = (m.floor() as i64) - i64::from(b.max_open && m.fract() == 0.0);
                lo = lo.min(hi);
            }
        }
        lo = hlo.map_or(lo, |x| x as i64).clamp(tlo, thi);
        hi = hhi.map_or(hi, |x| x as i64).clamp(tlo, thi);
        if c.unique {
            let start = c.max_existing.map_or(lo, |m| m.saturating_add(1).max(lo));
            return Ok(Gen::Sequence { next: start });
        }
        return Ok(Gen::Int { lo, hi: hi.max(lo) });
    }
    if matches!(dt, "decimal" | "numeric" | "float" | "double" | "real") {
        let (p, s) = if dt == "decimal" || dt == "numeric" {
            precision(c)
        } else {
            (12, 2)
        };
        let cap = 10f64.powi(i32::try_from(p - s).unwrap_or(9).min(15)) - 1.0;
        let floor = if c.column_type.contains("unsigned") {
            0.0
        } else {
            -cap
        };
        let (mut lo, mut hi): (f64, f64) = if has("lat") || has("latitude") {
            (-60.0, 60.0)
        } else if has("lng") || has("lon") || has("longitude") {
            (-150.0, 150.0)
        } else if has("rate") || has("ratio") || has("discount") {
            (0.0, 1.0)
        } else {
            (1.0, 1000.0)
        };
        if let Some(b) = b {
            if let Some(m) = b.min {
                lo = m + if b.min_open { 0.01 } else { 0.0 };
                hi = hi.max(lo);
            }
            if let Some(m) = b.max {
                hi = m - if b.max_open { 0.01 } else { 0.0 };
                lo = lo.min(hi);
            }
        }
        lo = hlo.unwrap_or(lo).clamp(floor, cap);
        hi = hhi.unwrap_or(hi).clamp(floor, cap);
        return Ok(Gen::Decimal {
            lo,
            hi: hi.max(lo),
            scale: s.min(6),
        });
    }
    if matches!(dt, "date" | "datetime" | "timestamp") {
        let birth = has("dob") || has("birth") || has("birthday") || has("birthdate");
        let (lo, hi) = if birth {
            (days_from_civil(1950, 1, 1), days_from_civil(2006, 12, 31))
        } else {
            let lo = cx.today - 730;
            // TIMESTAMP holds dates from 1970 to 2038.
            (lo.max(0), cx.today)
        };
        let lo = cx
            .hint
            .and_then(|h| hint_date(h.min.as_ref()))
            .unwrap_or(lo);
        let hi = cx
            .hint
            .and_then(|h| hint_date(h.max.as_ref()))
            .unwrap_or(hi);
        return Ok(Gen::Date {
            lo,
            hi: hi.max(lo),
            time: dt != "date",
        });
    }
    match dt {
        "time" => return Ok(Gen::Time),
        "year" => return Ok(Gen::Year { lo: 1990, hi: 2030 }),
        "json" => return Ok(Gen::Json),
        "inet4" => return Ok(Gen::Ip { v6: false }),
        "inet6" => return Ok(Gen::Ip { v6: true }),
        "uuid" => return Ok(Gen::Uuid),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
            return Ok(Gen::Binary);
        }
        "point" | "geometry" => return Ok(Gen::Point),
        _ => {}
    }
    if !is_textual(c) {
        return if c.nullable {
            Ok(Gen::Null)
        } else {
            Err(format!(
                "{}: SprigDB can't generate values of type {}; give it a default or a hint",
                c.name, c.column_type
            ))
        };
    }
    // Text, by the name.
    let info = ColumnInfo {
        table: table.to_owned(),
        column: c.name.clone(),
        data_type: c.data_type.clone(),
        column_type: c.column_type.clone(),
        ..ColumnInfo::default()
    };
    let len = max_len(c);
    let g = match sensitive::by_schema(&info).map(|f| f.category) {
        Some(Category::Email) => Gen::Email { domain },
        Some(Category::Phone) => Gen::Phone,
        Some(Category::PersonName) if has("first") || has("firstname") || has("given") => {
            Gen::First
        }
        Some(Category::PersonName) if has("last") || has("lastname") || has("surname") => Gen::Last,
        Some(Category::PersonName) => Gen::Full,
        Some(Category::Address) if has("city") || has("town") => Gen::City,
        Some(Category::Address)
            if has("zip") || has("postcode") || has("postal") || has("zipcode") =>
        {
            Gen::Postcode
        }
        Some(Category::Address) => Gen::Street,
        Some(Category::IpAddress) => Gen::Ip { v6: false },
        Some(Category::CardNumber) => Gen::Card,
        Some(Category::NationalId) => Gen::NationalId,
        Some(Category::BankAccount) => Gen::Iban,
        Some(Category::Secret) => Gen::Secret,
        Some(Category::BirthDate) => Gen::Date {
            lo: days_from_civil(1950, 1, 1),
            hi: days_from_civil(2006, 12, 31),
            time: false,
        },
        _ if has("country") => Gen::Country(if len == Some(3) { 3 } else { 2 }),
        _ if has("currency") => Gen::Currency,
        _ if has("url") || has("website") || has("link") || has("homepage") => Gen::Url { domain },
        _ if has("uuid") || has("guid") => Gen::Uuid,
        _ if has("company") || has("organisation") || has("organization") || has("employer") => {
            Gen::Company
        }
        _ if has("status") || has("state") => Gen::OneOf(STATUS.iter().map(|s| quote(s)).collect()),
        _ if has("color") || has("colour") => Gen::OneOf(COLOR.iter().map(|s| quote(s)).collect()),
        _ if has("code") || has("sku") || has("ref") || has("reference") => Gen::Code,
        _ if matches!(c.data_type.as_str(), "text" | "mediumtext" | "longtext")
            || has("title")
            || has("description")
            || has("notes")
            || has("comment") =>
        {
            Gen::Sentence
        }
        _ => Gen::Word,
    };
    Ok(g)
}

fn uuid(r: &mut Rng) -> String {
    let a = r.next();
    let b = r.next();
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        a >> 32,
        (a >> 16) & 0xffff,
        a & 0xfff,
        0x8000 | (b >> 48) & 0x3fff,
        b & 0xffff_ffff_ffff
    )
}

/// `s` cut to `len` characters.
fn cut(s: &str, len: Option<usize>) -> String {
    match len {
        Some(n) if s.chars().count() > n => s.chars().take(n).collect(),
        _ => s.to_owned(),
    }
}

/// `base` made unique with the run tag and the row number, kept within
/// `len`.
fn unique_text(base: &str, tag: &str, i: usize, len: Option<usize>) -> String {
    let suffix = format!("-{tag}{i}");
    match len {
        Some(n) if n <= suffix.len() => {
            // Too short for a word: the number alone, base 36.
            let mut x = (i as u64) + 1;
            let mut s = String::new();
            while x > 0 {
                let d = u32::try_from(x % 36).unwrap_or(0);
                s.insert(
                    0,
                    char::from_digit(d, 36).unwrap_or('0').to_ascii_uppercase(),
                );
                x /= 36;
            }
            s
        }
        Some(n) => {
            let keep = n - suffix.len();
            format!("{}{suffix}", cut(base, Some(keep)))
        }
        None => format!("{base}{suffix}"),
    }
}

#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn value(
    g: &mut Gen,
    c: &GenColumn,
    r: &mut Rng,
    i: usize,
    tag: &str,
    fks: &[ForeignKey],
    fk_pick: &BTreeMap<usize, usize>,
) -> String {
    let len = max_len(c);
    let text = |s: String| {
        quote(&if c.unique {
            unique_text(&s, tag, i, len)
        } else {
            cut(&s, len)
        })
    };
    match g {
        Gen::Null => "NULL".into(),
        Gen::OneOf(v) => {
            if c.unique && i < v.len() {
                v[i].clone()
            } else {
                r.pick(v).clone()
            }
        }
        Gen::Sequence { next } => {
            let v = *next;
            *next = next.saturating_add(1);
            if is_textual(c) {
                quote(&cut(&v.to_string(), len))
            } else {
                v.to_string()
            }
        }
        Gen::Int { lo, hi } | Gen::Year { lo, hi } => r.range(*lo, *hi).to_string(),
        Gen::Decimal { lo, hi, scale } => {
            let m = 10f64.powi(*scale as i32);
            let a = (*lo * m).ceil() as i64;
            let b = (*hi * m).floor() as i64;
            let n = r.range(a, b.max(a));
            let x = n as f64 / m;
            format!("{x:.prec$}", prec = *scale as usize)
        }
        Gen::Date { lo, hi, time } => {
            let d = date_text(r.range(*lo, *hi));
            if *time {
                quote(&format!(
                    "{d} {:02}:{:02}:{:02}",
                    r.range(0, 23),
                    r.range(0, 59),
                    r.range(0, 59)
                ))
            } else {
                quote(&d)
            }
        }
        Gen::Time => quote(&format!(
            "{:02}:{:02}:{:02}",
            r.range(0, 23),
            r.range(0, 59),
            r.range(0, 59)
        )),
        Gen::Bool => r.range(0, 1).to_string(),
        Gen::Email { domain } => {
            let local = format!(
                "{}.{}",
                r.pick(FIRST).to_lowercase(),
                r.pick(LAST).to_lowercase()
            );
            let local = if c.unique {
                format!("{local}.{tag}{i}")
            } else {
                local
            };
            quote(&cut(&format!("{local}@{domain}"), len))
        }
        Gen::Phone => {
            let s = format!(
                "+1 202 555 {:04}",
                if c.unique {
                    i % 10_000
                } else {
                    r.range(0, 9999) as usize
                }
            );
            if c.unique {
                text(s)
            } else {
                quote(&cut(&s, len))
            }
        }
        Gen::First => text((*r.pick(FIRST)).to_owned()),
        Gen::Last => text((*r.pick(LAST)).to_owned()),
        Gen::Full => text(format!("{} {}", r.pick(FIRST), r.pick(LAST))),
        Gen::Company => text(format!("{} {}", r.pick(COMPANY), r.pick(COMPANY_END))),
        Gen::Street => text(format!(
            "{} {} {}",
            r.range(1, 250),
            r.pick(STREET),
            r.pick(STREET_END)
        )),
        Gen::City => text((*r.pick(CITY)).to_owned()),
        Gen::Postcode => text(format!("{:05}", r.range(10_000, 99_999))),
        Gen::Country(n) => {
            let list = if *n == 3 { COUNTRY3 } else { COUNTRY2 };
            if c.unique && i < list.len() {
                quote(list[i])
            } else {
                quote(r.word(list))
            }
        }
        Gen::Currency => {
            if c.unique && i < CURRENCY.len() {
                quote(CURRENCY[i])
            } else {
                quote(r.word(CURRENCY))
            }
        }
        Gen::Url { domain } => text(format!("https://{domain}/{}", r.pick(WORDS))),
        Gen::Ip { v6 } => {
            if *v6 {
                quote(&format!(
                    "2001:db8::{:x}",
                    if c.unique {
                        i
                    } else {
                        r.range(1, 0xffff) as usize
                    }
                ))
            } else if c.unique {
                quote(&format!("198.51.{}.{}", (i / 254) % 256, i % 254 + 1))
            } else {
                quote(&format!("192.0.2.{}", r.range(1, 254)))
            }
        }
        Gen::Uuid => quote(&uuid(r)),
        Gen::Word => text((*r.pick(WORDS)).to_owned()),
        Gen::Sentence => text(format!(
            "{} {} {} {}.",
            capital(r.word(WORDS)),
            r.pick(WORDS),
            r.pick(WORDS),
            r.pick(WORDS)
        )),
        Gen::Code => text(format!(
            "{}-{:04}",
            r.pick(WORDS)
                .to_ascii_uppercase()
                .chars()
                .take(3)
                .collect::<String>(),
            r.range(0, 9999)
        )),
        Gen::Card => text((*r.pick(TEST_CARDS)).to_owned()),
        Gen::NationalId => text(format!("TEST-{:06}", r.range(0, 999_999))),
        Gen::Iban => text("GB82WEST12345698765432".to_owned()),
        Gen::Secret => {
            let s = format!("{:016x}{:016x}", r.next(), r.next());
            text(s)
        }
        Gen::Json => quote(&format!("{{\"n\": {i}}}")),
        Gen::Binary => {
            let n = len.unwrap_or(16).min(16);
            let mut s = String::from("X'");
            for _ in 0..n {
                let _ = write!(s, "{:02x}", r.range(0, 255));
            }
            s.push('\'');
            s
        }
        Gen::Point => format!(
            "ST_GeomFromText('POINT({} {})')",
            r.range(-150, 150),
            r.range(-60, 60)
        ),
        Gen::Parent(fk) => {
            let f = &fks[*fk];
            let pos = f
                .columns
                .iter()
                .position(|x| x.eq_ignore_ascii_case(&c.name))
                .unwrap_or(0);
            let k = fk_pick.get(fk).copied().unwrap_or(0);
            f.keys.get(k).and_then(|t| t.get(pos)).map_or_else(
                || "NULL".to_owned(),
                |v| match v {
                    // Keys read as text, written as numbers into numeric columns.
                    Value::String(s) if !is_textual(c) && s.parse::<i64>().is_ok() => s.clone(),
                    v => json_literal(v),
                },
            )
        }
    }
}

fn capital(s: &str) -> String {
    let mut c = s.chars();
    c.next()
        .map(|f| f.to_ascii_uppercase().to_string() + c.as_str())
        .unwrap_or_default()
}

/// Generates `n` rows of `t`, with `hints`, from `seed`; `today` is the
/// day number dates end at.
///
/// # Errors
/// Why the rows can't be generated (a NOT NULL foreign key whose parent
/// has no rows, a type SprigDB can't fill, too few distinct values for a
/// unique key, too much SQL), for the model to fix or tell the user.
#[allow(clippy::too_many_lines)]
pub fn generate(
    t: &GenTable,
    n: usize,
    hints: &[Hint],
    seed: u64,
    today: i64,
) -> Result<Generated, String> {
    if n == 0 || n > MAX_ROWS {
        return Err(format!("n must be 1 to {MAX_ROWS}"));
    }
    for h in hints {
        if !t
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(&h.column))
        {
            return Err(format!("{}.{} has no column {}", t.db, t.table, h.column));
        }
    }
    let mut out = Generated::default();
    let bounds = parse_checks(&t.checks, &mut out.checks_used, &mut out.checks_unknown);
    let mut r = Rng::new(seed);
    let tag: String = format!("{:04x}", seed & 0xffff);
    // Which foreign key fills each column.
    let mut fk_of: BTreeMap<String, usize> = BTreeMap::new();
    for (k, f) in t.foreign_keys.iter().enumerate() {
        for c in &f.columns {
            fk_of.entry(c.to_lowercase()).or_insert(k);
        }
    }
    let mut cols: Vec<(&GenColumn, Gen, u8)> = Vec::new();
    for c in &t.columns {
        if c.auto_increment {
            out.skipped
                .push((c.name.clone(), "auto_increment fills it".into()));
            continue;
        }
        if c.generated {
            out.skipped
                .push((c.name.clone(), "a generated column".into()));
            continue;
        }
        let hint = hints
            .iter()
            .find(|h| h.column.eq_ignore_ascii_case(&c.name));
        let nulls = if c.nullable {
            hint.and_then(|h| h.null_percent).unwrap_or(0)
        } else {
            0
        };
        let g = if let (Some(k), None) = (
            fk_of.get(&c.name.to_lowercase()),
            hint.and_then(|h| h.kind.as_ref()),
        ) {
            let f = &t.foreign_keys[*k];
            if f.keys.is_empty() {
                if c.nullable {
                    out.notes.push(format!(
                        "{} is NULL in every row: {} has no rows to point at",
                        c.name, f.parent
                    ));
                    Gen::Null
                } else {
                    return Err(format!(
                        "{}.{} needs existing rows of {} ({} is a NOT NULL foreign key), and it has none: generate rows for {} first",
                        t.db, t.table, f.parent, c.name, f.parent
                    ));
                }
            } else {
                Gen::Parent(*k)
            }
        } else {
            plan_column(
                c,
                &Ctx { today, hint },
                bounds.get(&c.name.to_lowercase()),
                &t.table,
            )?
        };
        if c.unique
            && let Gen::OneOf(v) = &g
            && v.len() < n
        {
            return Err(format!(
                "{} is unique but has only {} possible values; ask for at most {} rows",
                c.name,
                v.len(),
                v.len()
            ));
        }
        if !c.nullable && g == Gen::Null {
            return Err(format!("{} is NOT NULL; it can't be left NULL", c.name));
        }
        let mut how = g.words();
        if let Gen::Parent(k) = &g {
            let f = &t.foreign_keys[*k];
            how = format!("existing keys of {} ({} found)", f.parent, f.keys.len());
        }
        if c.unique && !matches!(g, Gen::Sequence { .. } | Gen::Parent(_)) {
            how.push_str(", unique");
        }
        if nulls > 0 {
            let _ = write!(how, ", {nulls}% NULL");
        }
        out.plan.push((c.name.clone(), how));
        out.columns.push(c.name.clone());
        cols.push((c, g, nulls));
    }
    if cols.is_empty() {
        return Err(format!(
            "every column of {}.{} fills itself; nothing to generate",
            t.db, t.table
        ));
    }
    // Composite unique keys, by column positions in `cols`.
    let keys: Vec<Vec<usize>> = t
        .unique_keys
        .iter()
        .filter_map(|k| {
            k.iter()
                .map(|name| {
                    cols.iter()
                        .position(|(c, _, _)| c.name.eq_ignore_ascii_case(name))
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect();
    let mut seen: Vec<HashSet<Vec<String>>> = vec![HashSet::new(); keys.len()];
    let mut single: Vec<HashSet<String>> = vec![HashSet::new(); cols.len()];
    let mut bytes = 0usize;
    for i in 0..n {
        let mut row = Vec::new();
        let mut tries = 0;
        loop {
            let fk_pick: BTreeMap<usize, usize> = t
                .foreign_keys
                .iter()
                .enumerate()
                .filter(|(_, f)| !f.keys.is_empty())
                .map(|(k, f)| {
                    (
                        k,
                        usize::try_from(r.range(0, f.keys.len() as i64 - 1)).unwrap_or(0),
                    )
                })
                .collect();
            row.clear();
            for (c, g, nulls) in &mut cols {
                if *nulls > 0 && r.percent(*nulls) {
                    row.push("NULL".to_owned());
                } else {
                    row.push(value(g, c, &mut r, i, &tag, &t.foreign_keys, &fk_pick));
                }
            }
            let dup_single = cols
                .iter()
                .enumerate()
                .any(|(j, (c, _, _))| c.unique && row[j] != "NULL" && single[j].contains(&row[j]));
            let dup_key = keys.iter().enumerate().any(|(k, pos)| {
                let tuple: Vec<String> = pos.iter().map(|p| row[*p].clone()).collect();
                !tuple.iter().any(|x| x == "NULL") && seen[k].contains(&tuple)
            });
            if !dup_single && !dup_key {
                break;
            }
            tries += 1;
            if tries > 50 {
                return Err(format!(
                    "after {i} rows, no more distinct values for a unique key of {}.{}; ask for fewer rows or hint more values",
                    t.db, t.table
                ));
            }
        }
        for (j, (c, _, _)) in cols.iter().enumerate() {
            if c.unique && row[j] != "NULL" {
                single[j].insert(row[j].clone());
            }
        }
        for (k, pos) in keys.iter().enumerate() {
            seen[k].insert(pos.iter().map(|p| row[*p].clone()).collect());
        }
        bytes += row.iter().map(|x| x.len() + 2).sum::<usize>() + 4;
        if bytes > MAX_SQL_BYTES {
            return Err(format!(
                "{i} rows already make {MAX_SQL_BYTES} bytes of SQL; ask for fewer rows"
            ));
        }
        out.rows.push(row);
    }
    if !keys.is_empty() || cols.iter().any(|(c, _, _)| c.unique) {
        out.notes.push(
            "unique values are distinct among the generated rows; text ones carry a run tag and integers start after the largest existing value, so they don't collide with rows already there".to_owned(),
        );
    }
    if !out.checks_unknown.is_empty() {
        out.notes.push(format!(
            "{} CHECK constraint(s) weren't understood; rows that fail them are refused when the statements run",
            out.checks_unknown.len()
        ));
    }
    Ok(out)
}

impl Generated {
    /// The `INSERT` statements, [`BATCH`] rows each.
    #[must_use]
    pub fn statements(&self, db: &str, table: &str) -> Vec<String> {
        let cols = self
            .columns
            .iter()
            .map(|c| ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        self.rows
            .chunks(BATCH)
            .map(|chunk| {
                let mut s = format!(
                    "INSERT INTO {}.{} ({cols}) VALUES\n",
                    ident(db),
                    ident(table)
                );
                for (k, row) in chunk.iter().enumerate() {
                    if k > 0 {
                        s.push_str(",\n");
                    }
                    let _ = write!(s, "({})", row.join(", "));
                }
                s
            })
            .collect()
    }

    /// What the model and the user are told: how each column was filled,
    /// counts and notes. It holds no generated value.
    #[must_use]
    pub fn summary(&self, t: &GenTable, statements: usize, bytes: usize) -> Value {
        json!({
            "table": format!("{}.{}", t.db, t.table),
            "rows": self.rows.len(),
            "statements": statements,
            "bytes": bytes,
            "columns": self.plan.iter().map(|(c, how)| json!({"column": c, "values": how})).collect::<Vec<_>>(),
            "skipped": self.skipped.iter().map(|(c, why)| json!({"column": c, "why": why})).collect::<Vec<_>>(),
            "checks_applied": self.checks_used.len(),
            "checks_not_understood": self.checks_unknown.len(),
            "notes": self.notes,
            "values": "generated by SprigDB and shown only to the user, in the proposal; not sent to the assistant",
        })
    }
}

// ---- reading a table's shape ----

/// A row of an `information_schema` answer, by column name.
pub type Row = serde_json::Map<String, Value>;

fn lit(s: &str) -> String {
    quote(s)
}

fn cell<'a>(r: &'a Row, k: &str) -> &'a str {
    r.get(k)
        .or_else(|| r.get(&k.to_ascii_lowercase()))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// The statements that read a table's shape, in the order
/// [`GenTable::from_parts`] takes their rows: columns, indexes, foreign
/// keys, CHECK clauses. They read only `information_schema`.
#[must_use]
pub fn shape_queries(db: &str, table: &str) -> [String; 4] {
    let (d, t) = (lit(db), lit(table));
    [
        format!(
            "SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, \
             IS_NULLABLE AS nullable, EXTRA AS extra FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = {d} AND TABLE_NAME = {t} ORDER BY ORDINAL_POSITION"
        ),
        format!(
            "SELECT INDEX_NAME AS index_name, COLUMN_NAME AS name, NON_UNIQUE AS non_unique \
             FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = {d} AND TABLE_NAME = {t} \
             ORDER BY INDEX_NAME, SEQ_IN_INDEX"
        ),
        format!(
            "SELECT CONSTRAINT_NAME AS fk, COLUMN_NAME AS name, \
             REFERENCED_TABLE_SCHEMA AS ref_db, REFERENCED_TABLE_NAME AS ref_table, \
             REFERENCED_COLUMN_NAME AS ref_column FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = {d} AND TABLE_NAME = {t} AND REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION"
        ),
        format!(
            "SELECT CHECK_CLAUSE AS clause FROM information_schema.CHECK_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = {d} AND TABLE_NAME = {t}"
        ),
    ]
}

/// A foreign key as read, before its parent keys: its columns and the
/// parent's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FkShape {
    /// The child's columns.
    pub columns: Vec<String>,
    /// The parent's database.
    pub ref_db: String,
    /// The parent table.
    pub ref_table: String,
    /// The parent's columns, in the same order.
    pub ref_columns: Vec<String>,
}

impl FkShape {
    /// The statement that reads up to [`MAX_PARENT_KEYS`] parent keys.
    #[must_use]
    pub fn keys_query(&self) -> String {
        format!(
            "SELECT DISTINCT {} FROM {}.{} WHERE {} LIMIT {MAX_PARENT_KEYS}",
            self.ref_columns
                .iter()
                .map(|c| ident(c))
                .collect::<Vec<_>>()
                .join(", "),
            ident(&self.ref_db),
            ident(&self.ref_table),
            self.ref_columns
                .iter()
                .map(|c| format!("{} IS NOT NULL", ident(c)))
                .collect::<Vec<_>>()
                .join(" AND ")
        )
    }
}

impl GenTable {
    /// The table from the rows of [`shape_queries`]; foreign keys come
    /// back without parent keys (read them with [`FkShape::keys_query`]
    /// into [`ForeignKey::keys`]).
    ///
    /// # Errors
    /// When the table has no columns the caller can see.
    pub fn from_parts(
        db: &str,
        table: &str,
        columns: &[Row],
        indexes: &[Row],
        fks: &[Row],
        checks: &[Row],
    ) -> Result<(Self, Vec<FkShape>), String> {
        if columns.is_empty() {
            return Err(format!(
                "there is no table {db}.{table} that the user can see"
            ));
        }
        // Indexes: unique ones, by name, in column order.
        let mut idx: Vec<(String, Vec<String>)> = Vec::new();
        for r in indexes {
            let unique = matches!(r.get("non_unique"), Some(Value::Number(n)) if n.as_i64() == Some(0))
                || cell(r, "non_unique") == "0";
            if !unique {
                continue;
            }
            let name = cell(r, "index_name").to_owned();
            match idx.iter_mut().find(|(n, _)| *n == name) {
                Some((_, cs)) => cs.push(cell(r, "name").to_owned()),
                None => idx.push((name, vec![cell(r, "name").to_owned()])),
            }
        }
        let cols: Vec<GenColumn> = columns
            .iter()
            .map(|r| {
                let name = cell(r, "name").to_owned();
                let extra = cell(r, "extra").to_ascii_lowercase();
                GenColumn {
                    unique: idx
                        .iter()
                        .any(|(_, cs)| cs.len() == 1 && cs[0].eq_ignore_ascii_case(&name)),
                    data_type: cell(r, "data_type").to_ascii_lowercase(),
                    column_type: cell(r, "column_type").to_ascii_lowercase(),
                    nullable: cell(r, "nullable").eq_ignore_ascii_case("YES"),
                    auto_increment: extra.contains("auto_increment"),
                    generated: extra.contains("generated")
                        || extra.contains("virtual")
                        || extra.contains("persistent"),
                    max_existing: None,
                    name,
                }
            })
            .collect();
        let unique_keys = idx
            .into_iter()
            .filter(|(_, cs)| cs.len() > 1)
            .map(|(_, cs)| cs)
            .collect();
        let mut shapes: Vec<(String, FkShape)> = Vec::new();
        for r in fks {
            let name = cell(r, "fk").to_owned();
            let i = if let Some(i) = shapes.iter().position(|(n, _)| *n == name) {
                i
            } else {
                shapes.push((
                    name,
                    FkShape {
                        ref_db: Some(cell(r, "ref_db"))
                            .filter(|d| !d.is_empty())
                            .unwrap_or(db)
                            .to_owned(),
                        ref_table: cell(r, "ref_table").to_owned(),
                        ..FkShape::default()
                    },
                ));
                shapes.len() - 1
            };
            shapes[i].1.columns.push(cell(r, "name").to_owned());
            shapes[i]
                .1
                .ref_columns
                .push(cell(r, "ref_column").to_owned());
        }
        let shapes: Vec<FkShape> = shapes.into_iter().map(|(_, s)| s).collect();
        let foreign_keys = shapes
            .iter()
            .map(|s| ForeignKey {
                columns: s.columns.clone(),
                parent: if s.ref_db == db {
                    s.ref_table.clone()
                } else {
                    format!("{}.{}", s.ref_db, s.ref_table)
                },
                keys: Vec::new(),
            })
            .collect();
        Ok((
            Self {
                db: db.to_owned(),
                table: table.to_owned(),
                columns: cols,
                unique_keys,
                foreign_keys,
                checks: checks
                    .iter()
                    .map(|r| cell(r, "clause").to_owned())
                    .filter(|c| !c.is_empty())
                    .collect(),
            },
            shapes,
        ))
    }

    /// The statement reading the largest value of each unique integer
    /// column that isn't `auto_increment`, so sequences start after it;
    /// `None` when there is none. Its one row is in [`Self::unique_ints`]'
    /// order.
    #[must_use]
    pub fn max_query(&self) -> Option<String> {
        let cs = self.unique_ints();
        (!cs.is_empty()).then(|| {
            format!(
                "SELECT {} FROM {}.{}",
                cs.iter()
                    .map(|c| format!("MAX({})", ident(c)))
                    .collect::<Vec<_>>()
                    .join(", "),
                ident(&self.db),
                ident(&self.table)
            )
        })
    }

    /// Unique integer columns that aren't `auto_increment`.
    #[must_use]
    pub fn unique_ints(&self) -> Vec<String> {
        self.columns
            .iter()
            .filter(|c| {
                c.unique
                    && !c.auto_increment
                    && matches!(
                        c.data_type.as_str(),
                        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
                    )
            })
            .map(|c| c.name.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests;
