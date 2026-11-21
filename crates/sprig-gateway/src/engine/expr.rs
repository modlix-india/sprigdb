//! Expressions compiled once per plan and evaluated over a row's slots.
//!
//! The engine evaluates what point lookups, sysbench's statements and
//! row-level security predicates need: columns, `?` parameters, literals
//! (integers, decimals, floating-point numbers, strings, hexadecimal and
//! bit strings), comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`),
//! `AND`, `OR`, `XOR`, `NOT`, `IS [NOT] NULL`, `[NOT] IN (...)`, `[NOT]
//! BETWEEN`, and `+`, `-`, `*`; and the current date and time (`NOW()`,
//! `CURRENT_TIMESTAMP`, `CURDATE()`, `CURTIME()`, their `UTC_` forms and
//! `UNIX_TIMESTAMP()`) as a value to store or to compare a date or time
//! column with: the statement's time, the same for every row. Anything
//! else makes the statement `ER_SPRIG_NOT_ON_ENGINE`.
//!
//! Semantics follow MySQL: three-valued logic; integer overflow is 1690.
//! How two values compare follows from what they are:
//!
//! | Values | Compared as |
//! |---|---|
//! | two character strings | the column's collation (bytes without one) |
//! | a byte string with any string | bytes |
//! | two integers | integers, signed and unsigned together |
//! | integers and decimals | exact decimals |
//! | a floating-point number or a string with a number | doubles |
//! | an `ENUM` or `SET` with a string | the member text, in the column's collation; with a number, its index or bit mask |
//! | a `BIT` with a number or a string | numbers (the string's number, not its bytes) |
//! | a `DATE`, `DATETIME` or `TIMESTAMP` with anything | dates and times: the other side read as one (a `TIMESTAMP` in the session's zone) |
//! | a `TIME` with anything | times |
//! | a `YEAR` with a number or a string | years, after the two-digit rule (`24` is 2024) |
//!
//! A statement's text is UTF-8. Compared with a `latin1`, `ascii` or
//! `utf8mb3` column it is first put into the column's character set; a
//! character the set doesn't have fails the comparison with 1267, as on
//! the servers.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::{DateTime, Decimal, Time, Timestamp};
use sprig_proto_mysql::Value;
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectNamePart, UnaryOperator, Value as SqlValue,
};

use super::assign::{self, Env};
use super::not_on_engine;
use super::row::{self, Datum};
use super::table::{ColType, Column, TableDef};
use super::temporal;

/// A comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cmp {
    Eq,
    NullSafeEq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arith {
    Add,
    Sub,
    Mul,
}

/// What a comparison takes from the column on one of its sides.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct CmpCx {
    /// A string column's collation.
    pub collation: Option<Collation>,
    /// An `ENUM` or `SET` column's members.
    pub members: Option<Arc<[Bytes]>>,
    /// The column's character set is `utf8mb3`.
    pub mb3: bool,
    /// The left side is the statement's text (not a column).
    pub text_left: bool,
    /// The right side is the statement's text.
    pub text_right: bool,
    /// The decimals of a `FLOAT(M,D)` column.
    pub float_decimals: Option<u8>,
}

/// What a statement's expressions are evaluated with.
#[derive(Debug)]
pub(crate) struct Args<'a> {
    /// The bound parameters.
    pub params: &'a [Datum],
    /// The session's rules.
    pub env: Env,
    /// The statement's time: seconds since the epoch (UTC) and
    /// microseconds, the same for every row and column of the statement.
    /// Read when a column first asks for it (most statements never do).
    pub now: std::sync::OnceLock<(i64, u32)>,
}

#[cfg(test)]
impl<'a> Args<'a> {
    /// `params` under the default rules (strict mode, UTC).
    pub(crate) fn of(params: &'a [Datum]) -> Self {
        Self {
            params,
            env: Env::STRICT,
            now: std::sync::OnceLock::new(),
        }
    }
}

/// A compiled expression.
#[derive(Debug, Clone)]
pub(crate) enum CExpr {
    /// A column of the table's row.
    Col(usize),
    /// The `n`th `?`.
    Param(usize),
    /// A constant.
    Lit(Datum),
    /// A comparison, with what its column side brings.
    Cmp(Cmp, Box<CExpr>, Box<CExpr>, CmpCx),
    /// `AND`.
    And(Box<CExpr>, Box<CExpr>),
    /// `OR`.
    Or(Box<CExpr>, Box<CExpr>),
    /// `XOR`.
    Xor(Box<CExpr>, Box<CExpr>),
    /// `NOT`.
    Not(Box<CExpr>),
    /// `IS NULL` (`true`) or `IS NOT NULL` (`false`).
    IsNull(Box<CExpr>, bool),
    /// `[NOT] IN (list)`.
    In(Box<CExpr>, Vec<CExpr>, bool, CmpCx),
    /// `[NOT] BETWEEN a AND b`.
    Between(Box<CExpr>, Box<CExpr>, Box<CExpr>, bool, CmpCx),
    /// Arithmetic.
    Arith(Arith, Box<CExpr>, Box<CExpr>),
    /// Unary minus.
    Neg(Box<CExpr>),
    /// The statement's date or time, cut to `fsp` fractional digits.
    Now {
        /// What of it.
        kind: NowKind,
        /// In UTC instead of the session's zone.
        utc: bool,
        /// Fractional digits kept.
        fsp: u8,
    },
}

/// Whether `e` reads the row, a parameter or the clock: without any of
/// them it has one value.
pub(crate) fn reads_row_or_clock(e: &CExpr) -> bool {
    match e {
        CExpr::Col(_) | CExpr::Param(_) | CExpr::Now { .. } => true,
        CExpr::Lit(_) => false,
        CExpr::Cmp(_, a, b, _)
        | CExpr::And(a, b)
        | CExpr::Or(a, b)
        | CExpr::Xor(a, b)
        | CExpr::Arith(_, a, b) => reads_row_or_clock(a) || reads_row_or_clock(b),
        CExpr::Not(a) | CExpr::IsNull(a, _) | CExpr::Neg(a) => reads_row_or_clock(a),
        CExpr::In(a, list, _, _) => reads_row_or_clock(a) || list.iter().any(reads_row_or_clock),
        CExpr::Between(a, b, c, _, _) => {
            reads_row_or_clock(a) || reads_row_or_clock(b) || reads_row_or_clock(c)
        }
    }
}

/// What a call for the current date or time gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NowKind {
    /// `NOW()`: date and time.
    DateTime,
    /// `CURDATE()`: the date.
    Date,
    /// `CURTIME()`: the time of day.
    Time,
    /// `UNIX_TIMESTAMP()`: seconds since 1970.
    Unix,
}

/// Names and parameters while compiling.
pub(crate) struct Compiler<'a> {
    /// The table.
    pub def: &'a TableDef,
    /// Names the table may be qualified with (its name, its alias).
    pub quals: Vec<String>,
    /// `?` seen so far.
    pub params: usize,
    /// Columns read (for privilege checks), by index.
    pub read: Vec<bool>,
    /// Whether `\` in literals is an escape (`NO_BACKSLASH_ESCAPES` off).
    pub backslash: bool,
    /// The type each `?` takes from its context (a column it is compared
    /// with or stored into), as MySQL reports it in `PREPARE_OK`.
    pub hints: Vec<Option<ColType>>,
}

impl Compiler<'_> {
    /// Gives the parameters of `e` the type `ty` (integer arithmetic
    /// makes its parameters integers).
    pub(crate) fn hint(&mut self, e: &CExpr, ty: ColType) {
        match e {
            CExpr::Param(i) => {
                if let Some(h) = self.hints.get_mut(*i)
                    && h.is_none()
                {
                    *h = Some(ty);
                }
            }
            CExpr::Arith(_, a, b) => {
                let int = ColType::Int {
                    bytes: 8,
                    unsigned: false,
                };
                self.hint(a, int);
                self.hint(b, int);
            }
            CExpr::Neg(a) => self.hint(a, ty),
            _ => {}
        }
    }

    /// The current date or time is compared with columns of its own
    /// kind only (and constants beside them): what it means against a
    /// number or a string differs between the servers.
    fn now_compared(&self, left: &CExpr, right: &[&CExpr]) -> Result<()> {
        let all = || std::iter::once(left).chain(right.iter().copied());
        let mut cols = 0;
        let mut nows = 0;
        for n in all() {
            let CExpr::Now { kind, .. } = n else {
                continue;
            };
            nows += 1;
            for e in all() {
                match e {
                    CExpr::Col(i) => {
                        let t = self.def.columns[*i].ty;
                        let fits = match kind {
                            NowKind::DateTime | NowKind::Date => matches!(
                                t,
                                ColType::Date
                                    | ColType::DateTime { .. }
                                    | ColType::Timestamp { .. }
                            ),
                            NowKind::Time => matches!(t, ColType::Time { .. }),
                            NowKind::Unix => matches!(t, ColType::Int { .. }),
                        };
                        if !fits {
                            return Err(not_on_engine(
                                "the current date or time compared with a column of another kind",
                            ));
                        }
                        cols += 1;
                    }
                    CExpr::Now { .. } | CExpr::Lit(_) | CExpr::Param(_) => {}
                    _ => {
                        return Err(not_on_engine(
                            "the current date or time compared with an expression",
                        ));
                    }
                }
            }
        }
        if nows > 0 && cols == 0 {
            return Err(not_on_engine(
                "the current date or time compared with a constant",
            ));
        }
        Ok(())
    }

    fn col_type(&self, e: &CExpr) -> Option<ColType> {
        match e {
            CExpr::Col(i) => Some(self.def.columns[*i].ty),
            _ => None,
        }
    }

    fn column(&mut self, parts: &[Ident]) -> Result<usize> {
        let (name, qual) = match parts {
            [c] => (&c.value, None),
            [t, c] => (&c.value, Some(&t.value)),
            [_db, t, c] => (&c.value, Some(&t.value)),
            _ => return Err(not_on_engine("a column reference")),
        };
        if let Some(q) = qual
            && !self.quals.iter().any(|x| x.eq_ignore_ascii_case(q))
        {
            return Err(unknown_column(&format!("{q}.{name}"), "WHERE"));
        }
        let i = self
            .def
            .column(name)
            .ok_or_else(|| unknown_column(name, "WHERE"))?;
        self.read[i] = true;
        Ok(i)
    }

    /// What a comparison of `left` with `right` (one expression, or the
    /// items of a list) takes from a column among them: the first
    /// column's collation and members.
    ///
    /// # Errors
    /// `ER_SPRIG_NOT_ON_ENGINE` for a list that holds a column beside a
    /// `latin1` or `ascii` column (its items are taken as statement text).
    fn cmp_cx(&self, left: &CExpr, right: &[&CExpr], list: bool) -> Result<CmpCx> {
        let is_col = |e: &CExpr| matches!(e, CExpr::Col(_));
        self.now_compared(left, right)?;
        for e in std::iter::once(left).chain(right.iter().copied()) {
            if let CExpr::Col(i) = e {
                let c = &self.def.columns[*i];
                if let Some(coll) = c.collation {
                    let narrow = matches!(coll.charset(), Charset::Latin1 | Charset::Ascii);
                    if list && narrow && right.iter().any(|x| is_col(x)) {
                        return Err(not_on_engine(
                            "a column in a list compared with a latin1 or ascii column",
                        ));
                    }
                    return Ok(CmpCx {
                        float_decimals: None,
                        collation: c.collation,
                        members: (!c.members.is_empty()).then(|| Arc::clone(&c.members)),
                        mb3: c.mb3,
                        text_left: !is_col(left),
                        text_right: list || right.iter().any(|x| !is_col(x)),
                    });
                }
            }
        }
        // A FLOAT(M,D) column compares as the number its decimals print.
        for e in std::iter::once(left).chain(right.iter().copied()) {
            if let CExpr::Col(i) = e
                && let (ColType::Float, Some((_, d))) =
                    (self.def.columns[*i].ty, self.def.columns[*i].float_md)
            {
                return Ok(CmpCx {
                    float_decimals: Some(d),
                    ..CmpCx::default()
                });
            }
        }
        Ok(CmpCx::default())
    }

    /// Compiles `e`.
    ///
    /// # Errors
    /// `ER_SPRIG_NOT_ON_ENGINE` for what the engine doesn't evaluate; 1054.
    #[allow(clippy::too_many_lines)] // one arm per expression kind
    pub(crate) fn compile(&mut self, e: &Expr) -> Result<CExpr> {
        Ok(match e {
            Expr::Identifier(i) => CExpr::Col(self.column(std::slice::from_ref(i))?),
            Expr::CompoundIdentifier(p) => CExpr::Col(self.column(p)?),
            Expr::Nested(x) => self.compile(x)?,
            Expr::Value(v) => match &v.value {
                SqlValue::Placeholder(p) if p == "?" => {
                    self.params += 1;
                    self.hints.push(None);
                    CExpr::Param(self.params - 1)
                }
                other => CExpr::Lit(literal(other, self.backslash)?),
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => CExpr::Not(Box::new(no_now(self.compile(expr)?)?)),
                UnaryOperator::Minus => match self.compile(expr)? {
                    CExpr::Lit(Datum::Int(v)) => match v.checked_neg() {
                        Some(n) => CExpr::Lit(Datum::Int(n)),
                        None => CExpr::Neg(Box::new(CExpr::Lit(Datum::Int(v)))),
                    },
                    CExpr::Lit(Datum::UInt(v)) if v <= 1u64 << 63 => {
                        CExpr::Lit(Datum::Int(0i64.wrapping_sub_unsigned(v)))
                    }
                    CExpr::Lit(Datum::Decimal(d)) => CExpr::Lit(Datum::Decimal(decimal_neg(&d))),
                    CExpr::Lit(Datum::Double(d)) => CExpr::Lit(Datum::Double(-d)),
                    x => CExpr::Neg(Box::new(no_now(x)?)),
                },
                UnaryOperator::Plus => self.compile(expr)?,
                _ => return Err(not_on_engine("an operator")),
            },
            Expr::BinaryOp { left, op, right } => {
                let a = self.compile(left)?;
                let b = self.compile(right)?;
                if let Some(t) = self.col_type(&a) {
                    self.hint(&b, t);
                }
                if let Some(t) = self.col_type(&b) {
                    self.hint(&a, t);
                }
                if matches!(
                    op,
                    BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply
                ) {
                    let int = ColType::Int {
                        bytes: 8,
                        unsigned: false,
                    };
                    self.hint(&a, int);
                    self.hint(&b, int);
                }
                let (a, b) = if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::Spaceship
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                ) {
                    (a, b)
                } else {
                    (no_now(a)?, no_now(b)?)
                };
                let cmp = |c: Cmp, s: &Self| -> Result<CExpr> {
                    let cx = s.cmp_cx(&a, &[&b], false)?;
                    Ok(CExpr::Cmp(c, Box::new(a.clone()), Box::new(b.clone()), cx))
                };
                match op {
                    BinaryOperator::Eq => cmp(Cmp::Eq, self)?,
                    BinaryOperator::Spaceship => cmp(Cmp::NullSafeEq, self)?,
                    BinaryOperator::NotEq => cmp(Cmp::Ne, self)?,
                    BinaryOperator::Lt => cmp(Cmp::Lt, self)?,
                    BinaryOperator::LtEq => cmp(Cmp::Le, self)?,
                    BinaryOperator::Gt => cmp(Cmp::Gt, self)?,
                    BinaryOperator::GtEq => cmp(Cmp::Ge, self)?,
                    BinaryOperator::And => CExpr::And(Box::new(a), Box::new(b)),
                    BinaryOperator::Or => CExpr::Or(Box::new(a), Box::new(b)),
                    BinaryOperator::Xor => CExpr::Xor(Box::new(a), Box::new(b)),
                    BinaryOperator::Plus => CExpr::Arith(Arith::Add, Box::new(a), Box::new(b)),
                    BinaryOperator::Minus => CExpr::Arith(Arith::Sub, Box::new(a), Box::new(b)),
                    BinaryOperator::Multiply => CExpr::Arith(Arith::Mul, Box::new(a), Box::new(b)),
                    _ => return Err(not_on_engine("an operator")),
                }
            }
            Expr::IsNull(x) => CExpr::IsNull(Box::new(self.compile(x)?), true),
            Expr::IsNotNull(x) => CExpr::IsNull(Box::new(self.compile(x)?), false),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let a = self.compile(expr)?;
                let l = list
                    .iter()
                    .map(|x| self.compile(x))
                    .collect::<Result<Vec<_>>>()?;
                let cx = self.cmp_cx(&a, &l.iter().collect::<Vec<_>>(), true)?;
                if let Some(t) = self.col_type(&a) {
                    for x in &l {
                        self.hint(x, t);
                    }
                }
                CExpr::In(Box::new(a), l, *negated, cx)
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let a = self.compile(expr)?;
                let lo = self.compile(low)?;
                let hi = self.compile(high)?;
                let cx = self.cmp_cx(&a, &[&lo, &hi], true)?;
                if let Some(t) = self.col_type(&a) {
                    self.hint(&lo, t);
                    self.hint(&hi, t);
                }
                CExpr::Between(Box::new(a), Box::new(lo), Box::new(hi), *negated, cx)
            }
            Expr::Function(f) => now_call(f)?,
            _ => return Err(not_on_engine("an expression")),
        })
    }
}

/// `e`, which isn't the current date or time (in arithmetic and logic it
/// is a number the engine doesn't work out).
fn no_now(e: CExpr) -> Result<CExpr> {
    if matches!(e, CExpr::Now { .. }) {
        return Err(not_on_engine(
            "the current date or time in arithmetic or logic",
        ));
    }
    Ok(e)
}

/// The current date or time a call names (`NOW()`, `CURDATE()`,
/// `CURRENT_TIMESTAMP(3)`, ...).
fn now_call(f: &Function) -> Result<CExpr> {
    let refuse = || {
        not_on_engine(
            "a function call (of the functions, the engine evaluates the current date and time)",
        )
    };
    let [ObjectNamePart::Identifier(name)] = f.name.0.as_slice() else {
        return Err(refuse());
    };
    let plain = name.quote_style.is_none()
        && f.over.is_none()
        && f.filter.is_none()
        && f.null_treatment.is_none()
        && f.within_group.is_empty()
        && !f.uses_odbc_syntax
        && matches!(f.parameters, FunctionArguments::None);
    // What it gives, in UTC or not, whether it takes a precision, and
    // whether it may stand without parentheses.
    let (kind, utc, precise, bare) = match name.value.to_ascii_uppercase().as_str() {
        "NOW" => (NowKind::DateTime, false, true, false),
        "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => {
            (NowKind::DateTime, false, true, true)
        }
        "CURDATE" => (NowKind::Date, false, false, false),
        "CURRENT_DATE" => (NowKind::Date, false, false, true),
        "CURTIME" => (NowKind::Time, false, true, false),
        "CURRENT_TIME" => (NowKind::Time, false, true, true),
        "UTC_TIMESTAMP" => (NowKind::DateTime, true, true, false),
        "UTC_DATE" => (NowKind::Date, true, false, false),
        "UTC_TIME" => (NowKind::Time, true, true, false),
        "UNIX_TIMESTAMP" => (NowKind::Unix, true, false, false),
        _ => return Err(refuse()),
    };
    let fsp = match &f.args {
        FunctionArguments::None if bare => Some(0),
        FunctionArguments::List(l) if l.duplicate_treatment.is_none() && l.clauses.is_empty() => {
            match l.args.as_slice() {
                [] => Some(0),
                [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v)))] if precise => {
                    match &v.value {
                        SqlValue::Number(n, false) => n.parse::<u8>().ok().filter(|n| *n <= 6),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        _ => None,
    };
    match fsp {
        Some(fsp) if plain => Ok(CExpr::Now { kind, utc, fsp }),
        _ => Err(refuse()),
    }
}

/// Whether the engine stores the current date or time `value` into `col`
/// as the servers do (any other value: yes).
///
/// # Errors
/// `ER_SPRIG_NOT_ON_ENGINE` for a column of another kind.
pub(crate) fn now_into(value: &CExpr, col: &Column) -> Result<()> {
    let CExpr::Now { kind, fsp, .. } = value else {
        return Ok(());
    };
    let text = matches!(
        col.ty,
        ColType::Char { .. } | ColType::VarChar { .. } | ColType::Text { .. }
    );
    let dated = matches!(
        col.ty,
        ColType::Date | ColType::DateTime { .. } | ColType::Timestamp { .. }
    );
    // A date into a `TIME` column and a time into a date column are
    // left out: the two servers answer them differently.
    let fits = match kind {
        NowKind::DateTime => dated || (text && *fsp == 0),
        NowKind::Date => dated || text,
        NowKind::Time => matches!(col.ty, ColType::Time { .. }) || (text && *fsp == 0),
        NowKind::Unix => matches!(col.ty, ColType::Int { .. }),
    };
    if fits {
        Ok(())
    } else {
        Err(not_on_engine(
            "the current date or time stored into a column of another kind",
        ))
    }
}

/// The statement's time as `kind` asks for it.
fn now_datum(kind: NowKind, utc: bool, fsp: u8, a: &Args<'_>) -> Result<Datum> {
    let (secs, micros) = *a.now.get_or_init(temporal::now);
    if kind == NowKind::Unix {
        return Ok(Datum::Int(secs));
    }
    let unit = 10u32.pow(6 - u32::from(fsp.min(6)));
    let t = Timestamp::new(
        u32::try_from(secs).unwrap_or(u32::MAX),
        micros - micros % unit,
    );
    let tz = if utc { 0 } else { assign::zone(a.env)? };
    let dt = temporal::from_timestamp(t, tz);
    Ok(match kind {
        NowKind::Date => Datum::Date(dt.date),
        NowKind::Time => {
            let secs = (i64::from(dt.hour) * 60 + i64::from(dt.minute)) * 60 + i64::from(dt.second);
            Datum::Time(
                Time::from_micros(secs * 1_000_000 + i64::from(dt.micros)).unwrap_or_default(),
            )
        }
        NowKind::DateTime | NowKind::Unix => Datum::DateTime(dt),
    })
}

/// 1054 as MariaDB words it.
pub(crate) fn unknown_column(name: &str, clause: &str) -> SprigError {
    SprigError::standard(
        1054,
        *b"42S22",
        format!("Unknown column '{name}' in '{clause}'"),
    )
}

fn decimal_neg(d: &Decimal) -> Decimal {
    Decimal::from_parts(!d.is_negative(), d.int_digits(), d.frac_digits()).unwrap_or_default()
}

fn hex_bytes(s: &str) -> Option<Bytes> {
    // An odd count has a leading zero digit, as the servers read it.
    let padded;
    let s = if s.len() % 2 == 1 {
        padded = format!("0{s}");
        padded.as_str()
    } else {
        s
    };
    let digit = |c: u8| {
        char::from(c)
            .to_digit(16)
            .and_then(|d| u8::try_from(d).ok())
    };
    s.as_bytes()
        .chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect::<Option<Vec<u8>>>()
        .map(Bytes::from)
}

/// A bit literal's bytes: its bits, big-endian, in as many bytes as they
/// need.
fn bit_bytes(s: &str) -> Option<Bytes> {
    if !s.bytes().all(|b| b == b'0' || b == b'1') {
        return None;
    }
    let mut out = vec![0u8; s.len().div_ceil(8).max(1)];
    let n = out.len() * 8;
    for (i, b) in s.bytes().rev().enumerate() {
        if b == b'1' {
            let bit = n - 1 - i;
            out[bit / 8] |= 1 << (7 - bit % 8);
        }
    }
    Some(Bytes::from(out))
}

fn literal(v: &SqlValue, backslash: bool) -> Result<Datum> {
    Ok(match v {
        SqlValue::Null => Datum::Null,
        SqlValue::Boolean(b) => Datum::Int(i64::from(*b)),
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Datum::Int(i)
            } else if let Ok(u) = n.parse::<u64>() {
                Datum::UInt(u)
            } else if n.contains(['e', 'E']) {
                // A number with an exponent is a double.
                match n.parse::<f64>() {
                    Ok(f) if f.is_finite() => Datum::Double(f),
                    _ => return Err(not_on_engine("a number beyond the DOUBLE range")),
                }
            } else {
                // A number with a point, or more digits than BIGINT has,
                // is an exact decimal.
                match Decimal::parse(n) {
                    Some(d) => Datum::Decimal(d),
                    None => return Err(not_on_engine("a decimal literal of more than 65 digits")),
                }
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            if !backslash && s.contains('\\') {
                // The parser unescapes; under NO_BACKSLASH_ESCAPES that
                // would change the value.
                return Err(not_on_engine("a backslash under NO_BACKSLASH_ESCAPES"));
            }
            Datum::Str(Bytes::copy_from_slice(s.as_bytes()))
        }
        SqlValue::HexStringLiteral(s) => match hex_bytes(s) {
            Some(b) => Datum::Bytes(b),
            None => return Err(not_on_engine("a hexadecimal literal")),
        },
        SqlValue::SingleQuotedByteStringLiteral(s) | SqlValue::DoubleQuotedByteStringLiteral(s) => {
            match bit_bytes(s) {
                Some(b) => Datum::Bytes(b),
                None => return Err(not_on_engine("a bit literal")),
            }
        }
        _ => return Err(not_on_engine("a literal")),
    })
}

/// A bound parameter as a datum.
///
/// # Errors
/// None today: every wire value has a datum (the executor's call site
/// keeps the `Result` for values that may not).
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn param_datum(v: &Value) -> Result<Datum> {
    Ok(match v {
        Value::Null => Datum::Null,
        Value::Int(i) => Datum::Int(*i),
        Value::UInt(u) => Datum::UInt(*u),
        Value::Double(d) => Datum::Double(*d),
        Value::Float(f) => Datum::Float(*f),
        Value::Bytes(b) => Datum::Str(b.clone()),
        Value::DateTime(d) => Datum::DateTime(DateTime::new(
            sprig_engine::row::Date::new(d.year, d.month, d.day),
            d.hour,
            d.minute,
            d.second,
            d.micro,
        )),
        Value::Time(t) => {
            let secs = (i64::from(t.days) * 24 + i64::from(t.hour)) * 3600
                + i64::from(t.minute) * 60
                + i64::from(t.second);
            let micros = secs * 1_000_000 + i64::from(t.micro);
            match Time::from_micros(if t.negative { -micros } else { micros }) {
                Some(x) => Datum::Time(x),
                // Beyond a TIME's range: as text, for the column's rules.
                None => Datum::Str(Bytes::from(format!(
                    "{}{}:{:02}:{:02}",
                    if t.negative { "-" } else { "" },
                    secs / 3600,
                    t.minute,
                    t.second
                ))),
            }
        }
    })
}

/// The numeric prefix of `s` as a double, and whether `s` was entirely a
/// number (surrounding spaces allowed).
pub(crate) fn str_to_f64(s: &[u8]) -> (f64, bool) {
    let t = std::str::from_utf8(s).unwrap_or("");
    let lead = t.trim_start();
    let mut end = 0;
    let b = lead.as_bytes();
    let mut seen_digit = false;
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
        seen_digit = true;
        end = i;
    }
    if i < b.len() && b[i] == b'.' {
        i += 1;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
            seen_digit = true;
            end = i;
        }
    }
    if seen_digit && i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        let ds = j;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j > ds {
            end = j;
        }
    }
    let v = lead[..end].parse::<f64>().unwrap_or(0.0);
    (v, seen_digit && lead[end..].trim().is_empty())
}

/// A `DOUBLE` as the servers print it: the shortest digits that read back
/// to the same value, without a point for a whole number, and in exponent
/// form (`1e30`, `1.5e-16`) for a whole number of 10^15 or more and for a
/// number below 10^-15. Zero prints as `0`, whatever its sign.
pub(crate) fn double_text(v: f64) -> String {
    shortest(&format!("{v:e}"))
}

/// A `FLOAT` as the servers print it: six significant digits (`16777216`
/// prints as `16777200`), laid out as [`double_text`] lays a double out.
pub(crate) fn float_text(v: f32) -> String {
    shortest(&format!("{:.5e}", f64::from(v)))
}

/// Lays out the shortest digits of a number (`sci`: Rust's `{:e}` form,
/// `d[.ddd]e[-]x`) the way the servers do: plain digits from 10^-15 up,
/// except for whole numbers of 10^15 or more, which take the exponent
/// form like the numbers below 10^-15; zeros at the end of the fraction
/// go.
fn shortest(sci: &str) -> String {
    let Some((mant, exp)) = sci.split_once('e') else {
        return sci.to_owned();
    };
    let Ok(exp) = exp.parse::<i32>() else {
        return sci.to_owned();
    };
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant),
    };
    let digits: String = mant.chars().filter(char::is_ascii_digit).collect();
    let digits = match digits.trim_end_matches('0') {
        "" => "0",
        d => d,
    };
    if digits.bytes().all(|b| b == b'0') {
        return "0".into();
    }
    let sign = if neg { "-" } else { "" };
    let n = i32::try_from(digits.len()).unwrap_or(i32::MAX);
    // Exponent form: below 10^-15, and whole numbers from 10^15.
    if exp < -15 || (exp >= 15 && exp + 1 >= n) {
        let frac = &digits[1..];
        return if frac.is_empty() {
            format!("{sign}{}e{exp}", &digits[..1])
        } else {
            format!("{sign}{}.{frac}e{exp}", &digits[..1])
        };
    }
    if exp < 0 {
        // `exp` is from -15 to -1.
        let zeros = usize::try_from(-exp - 1).unwrap_or(0);
        return format!("{sign}0.{}{digits}", "0".repeat(zeros));
    }
    if exp + 1 >= n {
        let zeros = usize::try_from(exp + 1 - n).unwrap_or(0);
        return format!("{sign}{digits}{}", "0".repeat(zeros));
    }
    let point = usize::try_from(exp + 1).unwrap_or(0);
    format!("{sign}{}.{}", &digits[..point], &digits[point..])
}

/// A value as a double, when it is a number or text with one.
// MySQL converts to DOUBLE the same way, losing the same low bits.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn as_f64(d: &Datum) -> f64 {
    match d {
        Datum::Int(v) => *v as f64,
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => *v as f64,
        Datum::Enum(v) | Datum::Year(v) => f64::from(*v),
        Datum::Decimal(v) => v.to_string().parse().unwrap_or(0.0),
        Datum::Float(v) => f64::from(*v),
        Datum::Double(v) => *v,
        Datum::Str(s) | Datum::Json(s) => str_to_f64(s).0,
        Datum::Bytes(b) => assign::bytes_number(b).map_or(f64::MAX, |n| n as f64),
        _ => 0.0,
    }
}

/// A value as an integer, when it is one (a `BIT`, `YEAR`, `ENUM` index
/// and `SET` mask included).
fn as_int(d: &Datum) -> Option<i128> {
    Some(match d {
        Datum::Int(v) => i128::from(*v),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => i128::from(*v),
        Datum::Enum(v) | Datum::Year(v) => i128::from(*v),
        Datum::Bytes(b) => i128::from(assign::bytes_number(b)?),
        _ => return None,
    })
}

/// A value as an exact decimal, when it is an integer or a decimal.
fn as_decimal(d: &Datum) -> Option<Decimal> {
    match d {
        Datum::Decimal(v) => Some(v.clone()),
        other => {
            let v = as_int(other)?;
            Decimal::parse(&v.to_string())
        }
    }
}

/// The member text of an `ENUM` index.
pub(crate) fn enum_text(members: &[Bytes], index: u16) -> Bytes {
    match index {
        0 => Bytes::new(),
        i => members.get(usize::from(i) - 1).cloned().unwrap_or_default(),
    }
}

/// The member text of a `SET` mask: its members in definition order,
/// joined by commas.
pub(crate) fn set_text(members: &[Bytes], mask: u64) -> Bytes {
    let mut out = Vec::new();
    for (i, m) in members.iter().enumerate().take(64) {
        if mask & (1 << i) != 0 {
            if !out.is_empty() {
                out.push(b',');
            }
            out.extend_from_slice(m);
        }
    }
    Bytes::from(out)
}

/// 1267 as MariaDB words it, for statement text that the column's
/// character set can't hold.
fn illegal_mix(c: Collation, cx: &CmpCx, env: &Env, op: &str) -> SprigError {
    let own = if cx.mb3 {
        c.name().replacen("utf8mb4", "utf8mb3", 1)
    } else {
        c.name().to_owned()
    };
    let session = if env.mysql {
        "utf8mb4_0900_ai_ci"
    } else {
        "utf8mb4_uca1400_ai_ci"
    };
    SprigError::standard(
        1267,
        *b"HY000",
        format!(
            "Illegal mix of collations ({own},IMPLICIT) and ({session},COERCIBLE) for operation '{op}'"
        ),
    )
}

/// The statement's text `s` (UTF-8) in the character set of the column it
/// is compared with.
fn statement_text<'s>(s: &'s [u8], cx: &CmpCx, env: &Env, op: &str) -> Result<Cow<'s, [u8]>> {
    let Some(c) = cx.collation else {
        return Ok(Cow::Borrowed(s));
    };
    if s.is_ascii() {
        return Ok(Cow::Borrowed(s));
    }
    match c.charset() {
        Charset::Binary => Ok(Cow::Borrowed(s)),
        Charset::Utf8mb4 if !cx.mb3 || !s.iter().any(|b| *b >= 0xF0) => Ok(Cow::Borrowed(s)),
        Charset::Latin1 => std::str::from_utf8(s)
            .ok()
            .and_then(|t| {
                t.chars()
                    .map(assign::latin1_byte)
                    .collect::<Option<Vec<u8>>>()
            })
            .map(Cow::Owned)
            .ok_or_else(|| illegal_mix(c, cx, env, op)),
        Charset::Utf8mb4 | Charset::Ascii => Err(illegal_mix(c, cx, env, op)),
    }
}

/// Orders two strings under the comparison's collation; `text`: whether
/// each is the statement's text.
fn strings(
    a: &[u8],
    b: &[u8],
    text: (bool, bool),
    cx: &CmpCx,
    env: &Env,
    op: &str,
) -> Result<Ordering> {
    let Some(c) = cx.collation else {
        return Ok(a.cmp(b));
    };
    let a = if text.0 {
        statement_text(a, cx, env, op)?
    } else {
        Cow::Borrowed(a)
    };
    let b = if text.1 {
        statement_text(b, cx, env, op)?
    } else {
        Cow::Borrowed(b)
    };
    Ok(c.compare(&a, &b))
}

/// Orders two non-NULL values; `None` when they can't be compared (the
/// comparison is then `NULL`).
///
/// `op`: the operator, for messages.
///
/// # Errors
/// 1267 for statement text outside the column's character set;
/// `ER_SPRIG_NOT_ON_ENGINE` for a pair the engine doesn't compare yet.
pub(crate) fn compare(
    a: &Datum,
    b: &Datum,
    cx: &CmpCx,
    env: &Env,
    op: &str,
) -> Result<Option<Ordering>> {
    use Datum as D;
    let sides = (cx.text_left, cx.text_right);
    Ok(Some(match (a, b) {
        (D::Int(x), D::Int(y)) => x.cmp(y),
        (D::UInt(x), D::UInt(y)) => x.cmp(y),
        (D::Str(x), D::Str(y)) => strings(x, y, sides, cx, env, op)?,
        (D::Bytes(x) | D::Json(x), D::Bytes(y) | D::Json(y) | D::Str(y))
        | (D::Str(x), D::Bytes(y) | D::Json(y)) => x.cmp(y),
        // An ENUM or SET with a string: the member text.
        (D::Enum(i), D::Str(s)) if cx.members.is_some() => {
            let m = cx.members.as_deref().unwrap_or(&[]);
            strings(&enum_text(m, *i), s, (false, sides.1), cx, env, op)?
        }
        (D::Str(s), D::Enum(i)) if cx.members.is_some() => {
            let m = cx.members.as_deref().unwrap_or(&[]);
            strings(s, &enum_text(m, *i), (sides.0, false), cx, env, op)?
        }
        (D::Set(v), D::Str(s)) if cx.members.is_some() => {
            let m = cx.members.as_deref().unwrap_or(&[]);
            strings(&set_text(m, *v), s, (false, sides.1), cx, env, op)?
        }
        (D::Str(s), D::Set(v)) if cx.members.is_some() => {
            let m = cx.members.as_deref().unwrap_or(&[]);
            strings(s, &set_text(m, *v), (sides.0, false), cx, env, op)?
        }
        (D::Date(x), D::Date(y)) => x.cmp(y),
        (D::DateTime(x), D::DateTime(y)) => x.cmp(y),
        (D::Timestamp(x), D::Timestamp(y)) => x.cmp(y),
        (D::Time(x), D::Time(y)) => x.cmp(y),
        (D::Time(_), _) | (_, D::Time(_)) => {
            let (Some(x), Some(y)) = (as_time(a)?, as_time(b)?) else {
                return Ok(None);
            };
            x.cmp(&y)
        }
        (D::Date(_) | D::DateTime(_) | D::Timestamp(_), _)
        | (_, D::Date(_) | D::DateTime(_) | D::Timestamp(_)) => {
            let kind = match (a, b) {
                (D::Date(_), _) | (_, D::Date(_)) => "DATE",
                _ => "DATETIME",
            };
            as_moment(a, env, kind)?.cmp(&as_moment(b, env, kind)?)
        }
        (D::Year(y), other) | (other, D::Year(y)) if assign::year_number(other).is_some() => {
            let n = assign::year_number(other).map_or(0, |(n, _)| n);
            let n = match n {
                1..=99 => temporal::year_from_int(n).map_or(n, i128::from),
                _ => n,
            };
            let o = i128::from(*y).cmp(&n);
            if matches!(a, D::Year(_)) {
                o
            } else {
                o.reverse()
            }
        }
        _ => {
            if let (Some(x), Some(y)) = (as_int(a), as_int(b)) {
                x.cmp(&y)
            } else if let (Some(x), Some(y)) = (as_decimal(a), as_decimal(b)) {
                x.cmp_numeric(&y)
            } else {
                as_f64(&shown(a, cx))
                    .partial_cmp(&as_f64(&shown(b, cx)))
                    .unwrap_or(Ordering::Equal)
            }
        }
    }))
}

/// A `FLOAT(M,D)` column's value as comparisons see it: the number its
/// `D` decimals print (a stored 0.1 is equal to 0.1 there, which a plain
/// `FLOAT`'s isn't).
fn shown<'d>(d: &'d Datum, cx: &CmpCx) -> Cow<'d, Datum> {
    match (d, cx.float_decimals) {
        (Datum::Float(f), Some(n)) => {
            let text = format!("{:.*}", usize::from(n), f64::from(*f));
            Cow::Owned(Datum::Double(text.parse().unwrap_or(f64::from(*f))))
        }
        _ => Cow::Borrowed(d),
    }
}

/// The digits of a decimal at `scale` decimals, as one number without its
/// point.
fn decimal_digits(x: &Decimal, scale: usize) -> Vec<u8> {
    let mut v = x.int_digits().to_vec();
    v.extend_from_slice(x.frac_digits());
    v.resize(v.len() + scale.saturating_sub(x.frac_digits().len()), 0);
    v
}

fn magnitude_cmp(a: &[u8], b: &[u8]) -> Ordering {
    let strip = |d: &[u8]| -> usize { d.iter().take_while(|x| **x == 0).count() };
    let (a, b) = (&a[strip(a)..], &b[strip(b)..]);
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

fn magnitude_add(a: &[u8], b: &[u8]) -> Vec<u8> {
    let n = a.len().max(b.len()) + 1;
    let mut out = vec![0u8; n];
    let mut carry = 0u8;
    for i in 0..n {
        let x = if i < a.len() { a[a.len() - 1 - i] } else { 0 };
        let y = if i < b.len() { b[b.len() - 1 - i] } else { 0 };
        let s = x + y + carry;
        out[n - 1 - i] = s % 10;
        carry = s / 10;
    }
    out
}

/// `a - b` for `a >= b`.
fn magnitude_sub(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut out = a.to_vec();
    let mut borrow = 0u8;
    for i in 0..a.len() {
        let y = if i < b.len() { b[b.len() - 1 - i] } else { 0 } + borrow;
        let x = &mut out[a.len() - 1 - i];
        if *x >= y {
            *x -= y;
            borrow = 0;
        } else {
            *x = *x + 10 - y;
            borrow = 1;
        }
    }
    out
}

fn magnitude_mul(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut acc = vec![0u32; a.len() + b.len()];
    for (i, x) in a.iter().rev().enumerate() {
        for (j, y) in b.iter().rev().enumerate() {
            acc[i + j] += u32::from(*x) * u32::from(*y);
        }
    }
    let mut carry = 0u32;
    let mut out: Vec<u8> = acc
        .iter()
        .map(|v| {
            let t = v + carry;
            carry = t / 10;
            // A remainder of 10 fits a byte.
            #[allow(clippy::cast_possible_truncation)]
            let d = (t % 10) as u8;
            d
        })
        .collect();
    out.reverse();
    out
}

/// Exact decimal arithmetic, as the servers type it: a sum or difference
/// has the larger of the two scales, a product their sum (rounded half
/// away from zero past 38 decimals). `None` past 65 digits.
fn decimal_arith(op: Arith, x: &Decimal, y: &Decimal) -> Option<Decimal> {
    let (sx, sy) = (x.frac_digits().len(), y.frac_digits().len());
    let (negative, mut digits, mut scale) = match op {
        Arith::Mul => (
            x.is_negative() != y.is_negative(),
            magnitude_mul(&decimal_digits(x, sx), &decimal_digits(y, sy)),
            sx + sy,
        ),
        Arith::Add | Arith::Sub => {
            let scale = sx.max(sy);
            let (a, b) = (decimal_digits(x, scale), decimal_digits(y, scale));
            let y_negative = y.is_negative() != (op == Arith::Sub);
            if x.is_negative() == y_negative {
                (x.is_negative(), magnitude_add(&a, &b), scale)
            } else if magnitude_cmp(&a, &b) == Ordering::Less {
                (y_negative, magnitude_sub(&b, &a), scale)
            } else {
                (x.is_negative(), magnitude_sub(&a, &b), scale)
            }
        }
    };
    if scale > Decimal::MAX_SCALE {
        let cut = scale - Decimal::MAX_SCALE;
        let keep = digits.len().saturating_sub(cut);
        let up = digits.get(keep).is_some_and(|d| *d >= 5);
        digits.truncate(keep);
        if up {
            digits = magnitude_add(&digits, &[1]);
        }
        scale = Decimal::MAX_SCALE;
    }
    if digits.len() < scale {
        let mut padded = vec![0u8; scale - digits.len()];
        padded.extend_from_slice(&digits);
        digits = padded;
    }
    let (int, frac) = digits.split_at(digits.len() - scale);
    Decimal::from_parts(negative, int, frac)
}

/// A value as a date and time (with the seventh fraction digit a text
/// may carry), for a comparison with a date column. Text that is no date
/// compares as the zero date on MariaDB's port and is error 1525 on
/// MySQL's.
fn as_moment(d: &Datum, env: &Env, kind: &str) -> Result<(DateTime, u8)> {
    let (read, text) = match d {
        Datum::Date(x) => return Ok((DateTime::new(*x, 0, 0, 0, 0), 0)),
        Datum::DateTime(x) => return Ok((*x, 0)),
        Datum::Timestamp(t) => return Ok((temporal::from_timestamp(*t, assign::zone(*env)?), 0)),
        Datum::Str(s) | Datum::Json(s) => (
            temporal::datetime_from_str(s),
            String::from_utf8_lossy(s).into_owned(),
        ),
        other => (
            assign::number_digits(other).and_then(|p| temporal::datetime_from_number(&p)),
            String::from_utf8_lossy(&assign::text_of(other)).into_owned(),
        ),
    };
    match read {
        Some(r) => Ok((r.dt, r.seventh)),
        None if env.mysql => Err(SprigError::standard(
            1525,
            *b"HY000",
            format!("Incorrect {kind} value: '{text}'"),
        )),
        None => Ok((DateTime::default(), 0)),
    }
}

/// A value as a time in microseconds (and the seventh fraction digit),
/// for a comparison with a `TIME`; `None` for what is no time.
fn as_time(d: &Datum) -> Result<Option<(i64, u8)>> {
    Ok(match d {
        Datum::Time(t) => Some((t.micros(), 0)),
        Datum::Str(s) | Datum::Json(s) => temporal::time_from_str(s).map(|r| (r.micros, r.seventh)),
        Datum::Date(_) | Datum::DateTime(_) | Datum::Timestamp(_) => {
            return Err(not_on_engine(
                "a comparison of a TIME with a date (it would take the current date)",
            ));
        }
        other => assign::number_digits(other)
            .and_then(|p| temporal::time_from_number(&p))
            .map(|r| (r.micros, r.seventh)),
    })
}

fn bool_datum(b: Option<bool>) -> Datum {
    match b {
        None => Datum::Null,
        Some(v) => Datum::Int(i64::from(v)),
    }
}

/// 1690 for a result out of range.
fn out_of_range(ty: &str, e: &str) -> SprigError {
    SprigError::standard(
        1690,
        *b"22003",
        format!("{ty} value is out of range in '{e}'"),
    )
}

/// Evaluates `e` over `row`.
///
/// # Errors
/// 1690; `ER_SPRIG_NOT_ON_ENGINE`.
#[allow(clippy::too_many_lines)] // one arm per expression kind
pub(crate) fn eval(e: &CExpr, row: &[Datum], a: &Args<'_>) -> Result<Datum> {
    Ok(match e {
        CExpr::Col(i) => row.get(*i).cloned().unwrap_or(Datum::Null),
        CExpr::Param(i) => a.params.get(*i).cloned().unwrap_or(Datum::Null),
        CExpr::Lit(d) => d.clone(),
        CExpr::Cmp(op, l, r, cx) => {
            let x = eval(l, row, a)?;
            let y = eval(r, row, a)?;
            if *op == Cmp::NullSafeEq {
                return Ok(Datum::Int(i64::from(match (x.is_null(), y.is_null()) {
                    (true, true) => true,
                    (false, false) => compare(&x, &y, cx, &a.env, "<=>")? == Some(Ordering::Equal),
                    _ => false,
                })));
            }
            if x.is_null() || y.is_null() {
                return Ok(Datum::Null);
            }
            let sym = match op {
                Cmp::Eq => "=",
                Cmp::NullSafeEq => "<=>",
                Cmp::Ne => "<>",
                Cmp::Lt => "<",
                Cmp::Le => "<=",
                Cmp::Gt => ">",
                Cmp::Ge => ">=",
            };
            let Some(o) = compare(&x, &y, cx, &a.env, sym)? else {
                return Ok(Datum::Null);
            };
            let r = match op {
                Cmp::Eq | Cmp::NullSafeEq => o == Ordering::Equal,
                Cmp::Ne => o != Ordering::Equal,
                Cmp::Lt => o == Ordering::Less,
                Cmp::Le => o != Ordering::Greater,
                Cmp::Gt => o == Ordering::Greater,
                Cmp::Ge => o != Ordering::Less,
            };
            Datum::Int(i64::from(r))
        }
        CExpr::And(l, r) => {
            let x = row::truth(&eval(l, row, a)?);
            if x == Some(false) {
                return Ok(Datum::Int(0));
            }
            let y = row::truth(&eval(r, row, a)?);
            bool_datum(match (x, y) {
                (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            })
        }
        CExpr::Or(l, r) => {
            let x = row::truth(&eval(l, row, a)?);
            if x == Some(true) {
                return Ok(Datum::Int(1));
            }
            let y = row::truth(&eval(r, row, a)?);
            bool_datum(match (x, y) {
                (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            })
        }
        CExpr::Xor(l, r) => {
            let x = row::truth(&eval(l, row, a)?);
            let y = row::truth(&eval(r, row, a)?);
            bool_datum(x.zip(y).map(|(x, y)| x != y))
        }
        CExpr::Not(x) => bool_datum(row::truth(&eval(x, row, a)?).map(|v| !v)),
        CExpr::IsNull(x, want) => Datum::Int(i64::from(eval(x, row, a)?.is_null() == *want)),
        CExpr::In(l, list, neg, cx) => {
            let x = eval(l, row, a)?;
            if x.is_null() {
                return Ok(Datum::Null);
            }
            let mut saw_null = false;
            let mut found = false;
            for y in list {
                let y = eval(y, row, a)?;
                if y.is_null() {
                    saw_null = true;
                    continue;
                }
                match compare(&x, &y, cx, &a.env, " IN ")? {
                    Some(Ordering::Equal) => {
                        found = true;
                        break;
                    }
                    Some(_) => {}
                    None => saw_null = true,
                }
            }
            bool_datum(if found {
                Some(!neg)
            } else if saw_null {
                None
            } else {
                Some(*neg)
            })
        }
        CExpr::Between(x, lo, hi, neg, cx) => {
            let x = eval(x, row, a)?;
            let l = eval(lo, row, a)?;
            let h = eval(hi, row, a)?;
            if x.is_null() {
                return Ok(Datum::Null);
            }
            // `x >= l AND x <= h` in three-valued logic: a NULL bound
            // leaves its half unknown, and the other half's false still
            // decides (`5 NOT BETWEEN NULL AND 2` is true).
            let ge = if l.is_null() {
                None
            } else {
                compare(&x, &l, cx, &a.env, "between")?.map(|o| o != Ordering::Less)
            };
            let le = if h.is_null() {
                None
            } else {
                compare(&x, &h, cx, &a.env, "between")?.map(|o| o != Ordering::Greater)
            };
            let inside = match (ge, le) {
                (Some(false), _) | (_, Some(false)) => false,
                (Some(true), Some(true)) => true,
                _ => return Ok(Datum::Null),
            };
            Datum::Int(i64::from(inside != *neg))
        }
        CExpr::Arith(op, l, r) => {
            let x = eval(l, row, a)?;
            let y = eval(r, row, a)?;
            if x.is_null() || y.is_null() {
                return Ok(Datum::Null);
            }
            arith(*op, &x, &y)?
        }
        CExpr::Now { kind, utc, fsp } => now_datum(*kind, *utc, *fsp, a)?,
        CExpr::Neg(x) => match eval(x, row, a)? {
            Datum::Null => Datum::Null,
            Datum::Int(v) => {
                Datum::Int(v.checked_neg().ok_or_else(|| out_of_range("BIGINT", "-"))?)
            }
            Datum::UInt(v) if v <= 1u64 << 63 => Datum::Int(0i64.wrapping_sub_unsigned(v)),
            Datum::UInt(_) => return Err(out_of_range("BIGINT", "-")),
            Datum::Decimal(d) => Datum::Decimal(decimal_neg(&d)),
            other => Datum::Double(-as_f64(&other)),
        },
    })
}

fn arith(op: Arith, x: &Datum, y: &Datum) -> Result<Datum> {
    let sym = match op {
        Arith::Add => "+",
        Arith::Sub => "-",
        Arith::Mul => "*",
    };
    let int = |d: &Datum| match d {
        Datum::Int(v) => Some(i128::from(*v)),
        Datum::UInt(v) | Datum::Bit(v) | Datum::Set(v) => Some(i128::from(*v)),
        Datum::Enum(v) | Datum::Year(v) => Some(i128::from(*v)),
        Datum::Bytes(b) => assign::bytes_number(b).map(i128::from),
        _ => None,
    };
    if let (Some(a), Some(b)) = (int(x), int(y)) {
        let r = match op {
            Arith::Add => a + b,
            Arith::Sub => a - b,
            Arith::Mul => a.checked_mul(b).unwrap_or(i128::MAX),
        };
        let unsigned = matches!(x, Datum::UInt(_)) || matches!(y, Datum::UInt(_));
        return if unsigned {
            u64::try_from(r)
                .map(Datum::UInt)
                .map_err(|_| out_of_range("BIGINT UNSIGNED", sym))
        } else {
            i64::try_from(r)
                .map(Datum::Int)
                .map_err(|_| out_of_range("BIGINT", sym))
        };
    }
    // A decimal with an exact number: exact.
    if (matches!(x, Datum::Decimal(_)) || matches!(y, Datum::Decimal(_)))
        && let (Some(a), Some(b)) = (as_decimal(x), as_decimal(y))
    {
        return decimal_arith(op, &a, &b)
            .map(Datum::Decimal)
            .ok_or_else(|| out_of_range("DECIMAL", sym));
    }
    if matches!(
        (x, y),
        (
            Datum::Date(_) | Datum::Time(_) | Datum::DateTime(_) | Datum::Timestamp(_),
            _
        ) | (
            _,
            Datum::Date(_) | Datum::Time(_) | Datum::DateTime(_) | Datum::Timestamp(_)
        )
    ) {
        return Err(not_on_engine("arithmetic on a date or time value"));
    }
    let (a, b) = (as_f64(x), as_f64(y));
    let r = match op {
        Arith::Add => a + b,
        Arith::Sub => a - b,
        Arith::Mul => a * b,
    };
    if !r.is_finite() {
        return Err(out_of_range("DOUBLE", sym));
    }
    Ok(Datum::Double(r))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Datum {
        Datum::Str(Bytes::copy_from_slice(v.as_bytes()))
    }

    fn eq(a: &Datum, b: &Datum, c: Option<Collation>) -> bool {
        let cx = CmpCx {
            collation: c,
            ..CmpCx::default()
        };
        compare(a, b, &cx, &Env::STRICT, "=").unwrap() == Some(Ordering::Equal)
    }

    #[test]
    fn numeric_prefixes_parse_as_mysql_reads_them() {
        assert_eq!(str_to_f64(b" 12abc"), (12.0, false));
        assert_eq!(str_to_f64(b"1.5e2"), (150.0, true));
        assert_eq!(str_to_f64(b"abc"), (0.0, false));
        assert_eq!(str_to_f64(b"-3 "), (-3.0, true));
    }

    #[test]
    fn strings_compare_under_the_collation() {
        let c = Some(Collation::Utf8mb4Uca1400AiCi);
        assert!(eq(&s("Hello"), &s("hello  "), c));
        assert!(!eq(&s("Hello"), &s("hello  "), None));
        assert!(eq(&Datum::Int(12), &s("12"), None));
        // A byte string compares by bytes whatever the collation.
        let b = Datum::Bytes(Bytes::from_static(b"hello"));
        assert!(!eq(&s("Hello"), &b, c));
        assert!(eq(&s("hello"), &b, c));
    }

    #[test]
    fn numbers_compare_exactly_when_both_are_exact() {
        let d = |t: &str| Datum::Decimal(Decimal::parse(t).unwrap());
        assert!(eq(&Datum::Int(13), &d("13.0"), None));
        assert!(!eq(&Datum::Int(13), &d("13.5"), None));
        assert!(eq(&Datum::UInt(u64::MAX), &d("18446744073709551615"), None));
        assert!(!eq(&Datum::Int(-1), &Datum::UInt(u64::MAX), None));
        // Beyond 2^53 two integers still differ.
        assert!(!eq(
            &Datum::Int(9_007_199_254_740_993),
            &Datum::Int(9_007_199_254_740_992),
            None
        ));
        assert!(eq(&Datum::Int(13), &Datum::Double(13.0), None));
        // A hexadecimal literal is a number beside a number.
        assert!(eq(
            &Datum::Int(12594),
            &Datum::Bytes(Bytes::from_static(b"12")),
            None
        ));
    }

    #[test]
    fn enums_and_sets_compare_by_text_with_strings() {
        let members: Arc<[Bytes]> = Arc::from(vec![
            Bytes::from_static(b"red"),
            Bytes::from_static(b"green"),
        ]);
        let cx = CmpCx {
            collation: Some(Collation::Utf8mb4GeneralCi),
            members: Some(members),
            ..CmpCx::default()
        };
        let e = &Env::STRICT;
        assert_eq!(
            compare(&Datum::Enum(2), &s("GREEN"), &cx, e, "=").unwrap(),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Datum::Enum(0), &s(""), &cx, e, "=").unwrap(),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Datum::Set(3), &s("red,green"), &cx, e, "=").unwrap(),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare(&Datum::Enum(2), &Datum::Int(2), &cx, e, "=").unwrap(),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn three_valued_logic() {
        let a = Args::of(&[]);
        let and = CExpr::And(
            Box::new(CExpr::Lit(Datum::Null)),
            Box::new(CExpr::Lit(Datum::Int(0))),
        );
        assert_eq!(eval(&and, &[], &a).unwrap(), Datum::Int(0));
        let or = CExpr::Or(
            Box::new(CExpr::Lit(Datum::Null)),
            Box::new(CExpr::Lit(Datum::Int(0))),
        );
        assert_eq!(eval(&or, &[], &a).unwrap(), Datum::Null);
        let inl = CExpr::In(
            Box::new(CExpr::Lit(Datum::Int(1))),
            vec![CExpr::Lit(Datum::Int(2)), CExpr::Lit(Datum::Null)],
            false,
            CmpCx::default(),
        );
        assert_eq!(eval(&inl, &[], &a).unwrap(), Datum::Null);
    }

    #[test]
    fn literals_keep_their_kind() {
        use sqlparser::dialect::MySqlDialect;
        use sqlparser::parser::Parser;
        let lit = |t: &str| {
            let e = Parser::new(&MySqlDialect {})
                .try_with_sql(t)
                .unwrap()
                .parse_expr()
                .unwrap();
            match e {
                Expr::Value(v) => literal(&v.value, true).unwrap(),
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(lit("12"), Datum::Int(12));
        assert_eq!(lit("18446744073709551615"), Datum::UInt(u64::MAX));
        assert_eq!(lit("1.50"), Datum::Decimal(Decimal::parse("1.50").unwrap()));
        assert_eq!(lit("1.5e0"), Datum::Double(1.5));
        assert_eq!(
            lit("18446744073709551616"),
            Datum::Decimal(Decimal::parse("18446744073709551616").unwrap())
        );
        assert_eq!(lit("0x3132"), Datum::Bytes(Bytes::from_static(b"12")));
        assert_eq!(lit("x'3132'"), Datum::Bytes(Bytes::from_static(b"12")));
        assert_eq!(lit("b'101'"), Datum::Bytes(Bytes::from_static(&[5])));
        assert_eq!(lit("TRUE"), Datum::Int(1));
    }

    #[test]
    fn doubles_print_as_the_servers_print_them() {
        assert_eq!(double_text(1.5), "1.5");
        assert_eq!(double_text(-22_500_000_000.0), "-22500000000");
        assert_eq!(double_text(1e30), "1e30");
        assert_eq!(double_text(0.0), "0");
        assert_eq!(double_text(-0.0), "0");
        assert_eq!(double_text(123_456.789_1), "123456.7891");
        assert_eq!(double_text(0.001), "0.001");
        assert_eq!(float_text(1.5), "1.5");
        assert_eq!(float_text(1.23456), "1.23456");
        // The corpus both servers printed.
        for (v, want) in [
            (1e-16, "1e-16"),
            (1e-15, "0.000000000000001"),
            (-9.876_543_21e-15, "-0.00000000000000987654321"),
            (1e-7, "0.0000001"),
            (150_000_000_000_000.0, "150000000000000"),
            (1e15, "1e15"),
            (-9.876_543_21e15, "-9.87654321e15"),
            (0.1 + 0.2, "0.30000000000000004"),
            (123_456_789_012_345_678.0, "1.2345678901234568e17"),
            (999_999_999_999_999.0, "999999999999999"),
            (123_456_789_012_345.6, "123456789012345.6"),
            (1_234_567_890_123_456.7, "1234567890123456.8"),
            (5e-324, "5e-324"),
            (1.797_693_134_862_315_7e308, "1.7976931348623157e308"),
            (1_234_567_890_123_456.0, "1.234567890123456e15"),
            (1_000_000_000_000_000.5, "1000000000000000.5"),
            (9_007_199_254_740_992.0, "9.007199254740992e15"),
            (
                1.234_567_890_123_456_7e-15,
                "0.0000000000000012345678901234568",
            ),
            (1.234_567_890_123_456_7e-16, "1.2345678901234568e-16"),
            (9.99e-16, "9.99e-16"),
            (123_456_789.123_456_79, "123456789.12345679"),
        ] {
            assert_eq!(double_text(v), want);
        }
        for (v, want) in [
            (-9.876_54e-25_f32, "-9.87654e-25"),
            (1.0 / 3.0, "0.333333"),
            (2.0 / 3.0, "0.666667"),
            (16_777_216.0, "16777200"),
            (123_456_789_012_345.6, "123457000000000"),
            (1_234_567_890_123_456.7, "1.23457e15"),
            (f32::MAX, "3.40282e38"),
            (1e-45, "1.4013e-45"),
            (0.1, "0.1"),
            (1_234_567.8, "1234570"),
            (1e15, "1e15"),
            (100_000_000_000_000.0, "100000000000000"),
        ] {
            assert_eq!(float_text(v), want);
        }
    }

    #[test]
    fn decimals_add_subtract_and_multiply_exactly() {
        let d = |t: &str| Datum::Decimal(Decimal::parse(t).unwrap());
        let text = |r: Datum| match r {
            Datum::Decimal(x) => x.to_string(),
            other => format!("{other:?}"),
        };
        let go = |op, a: &Datum, b: &Datum| text(arith(op, a, b).unwrap());
        assert_eq!(go(Arith::Add, &d("123.45"), &Datum::Int(1)), "124.45");
        assert_eq!(go(Arith::Mul, &d("124.45"), &Datum::Int(2)), "248.90");
        assert_eq!(go(Arith::Mul, &d("248.90"), &d("1.111")), "276.52790");
        assert_eq!(go(Arith::Sub, &d("276.53"), &d("0.005")), "276.525");
        assert_eq!(go(Arith::Sub, &Datum::Int(5), &d("9.00")), "-4.00");
        assert_eq!(go(Arith::Add, &d("-1.5"), &d("1.50")), "0.00");
        assert_eq!(go(Arith::Sub, &d("0.1"), &d("0.25")), "-0.15");
        assert_eq!(go(Arith::Add, &d("999.99"), &d("0.01")), "1000.00");
        assert_eq!(
            go(Arith::Mul, &d("1.0000000001"), &d("1.0000000001")),
            "1.00000000020000000001"
        );
        // With a string or a double the arithmetic is a double's.
        assert_eq!(
            arith(Arith::Add, &d("-12.00"), &s("1.5")).unwrap(),
            Datum::Double(-10.5)
        );
        // Past 65 digits: 1690.
        let big = d(&"9".repeat(40));
        let e = arith(Arith::Mul, &big, &big).unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 1690);
    }
}
