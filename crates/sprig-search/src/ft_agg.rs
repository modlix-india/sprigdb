//! `FT.AGGREGATE`'s pipeline over the matching keys' fields.
//!
//! ```text
//! FT.AGGREGATE idx query [VERBATIM] [LOAD n @f... | LOAD *]
//!   [GROUPBY n @f... [REDUCE fn nargs arg... [AS name]]...]
//!   [SORTBY n (@f [ASC|DESC])... [MAX m]] [APPLY expr AS name]
//!   [FILTER expr] [LIMIT offset num] [PARAMS n k v...] [DIALECT d]
//! ```
//!
//! Steps run in the order given, over rows of named values (each hash's
//! loaded fields, as Redis Search loads them: text). Reducers: `COUNT`,
//! `COUNT_DISTINCT`, `COUNT_DISTINCTISH` (exact here), `SUM`, `MIN`,
//! `MAX`, `AVG`, `STDDEV`, `QUANTILE`, `TOLIST`, `FIRST_VALUE`,
//! `RANDOM_SAMPLE` (the first values here). Expressions have Redis Search's
//! operators and the functions listed in [`call`]. Numbers print as C's
//! `%.12g`, as Redis Search prints them. A `SORTBY` without `MAX` keeps the
//! first 10 rows unless a `LIMIT` follows, as Redis does. `WITHCURSOR` pages the rows
//! through `FT.CURSOR` (the gateway keeps them).

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};

use sprig_common::{Result, SprigCode, SprigError};

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

fn text(a: &[u8]) -> String {
    String::from_utf8_lossy(a).into_owned()
}

fn upper(a: &[u8]) -> String {
    text(a).to_ascii_uppercase()
}

/// A value in a row.
#[derive(Debug, Clone, PartialEq)]
pub enum AggVal {
    /// Missing.
    Null,
    /// A number.
    Num(f64),
    /// Text.
    Str(String),
    /// `TOLIST`'s values.
    List(Vec<AggVal>),
}

impl AggVal {
    /// As a number: numbers, and text that parses as one.
    #[must_use]
    pub fn num(&self) -> Option<f64> {
        match self {
            Self::Num(n) => Some(*n),
            Self::Str(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    fn truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Num(n) => *n != 0.0,
            Self::Str(s) => !s.is_empty(),
            Self::List(l) => !l.is_empty(),
        }
    }

    /// The text Redis Search replies with (`None`: nil).
    #[must_use]
    pub fn reply_text(&self) -> Option<String> {
        match self {
            Self::Null | Self::List(_) => None,
            Self::Num(n) => Some(fmt_g(*n)),
            Self::Str(s) => Some(s.clone()),
        }
    }

    fn key_text(&self) -> String {
        match self {
            Self::Null => "\u{0}null".to_owned(),
            Self::List(l) => l
                .iter()
                .map(Self::key_text)
                .collect::<Vec<_>>()
                .join("\u{1}"),
            other => other.reply_text().unwrap_or_default(),
        }
    }
}

/// C's `%.12g`.
#[must_use]
pub fn fmt_g(n: f64) -> String {
    if n.is_nan() {
        return "nan".to_owned();
    }
    if n.is_infinite() {
        return if n > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if n == 0.0 {
        return if n.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }
    let e = format!("{n:.11e}");
    let (mant, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let trim = |s: &str| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_owned()
        } else {
            s.to_owned()
        }
    };
    if (-4..12).contains(&exp) {
        let decimals = usize::try_from(11 - exp).unwrap_or(0);
        trim(&format!("{n:.decimals$}"))
    } else {
        let m = trim(mant);
        format!("{m}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
    }
}

/// A row: named values in order.
pub type Row = Vec<(String, AggVal)>;

fn get<'a>(r: &'a Row, name: &str) -> &'a AggVal {
    r.iter()
        .find(|(n, _)| n == name)
        .map_or(&AggVal::Null, |(_, v)| v)
}

fn set(r: &mut Row, name: &str, v: AggVal) {
    if let Some(x) = r.iter_mut().find(|(n, _)| n == name) {
        x.1 = v;
    } else {
        r.push((name.to_owned(), v));
    }
}

// ---- Expressions ----

/// An expression of `APPLY` and `FILTER`.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A literal.
    Lit(AggVal),
    /// `@field`.
    Field(String),
    /// A unary operator (`-`, `!`).
    Unary(char, Box<Expr>),
    /// A binary operator.
    Bin(String, Box<Expr>, Box<Expr>),
    /// A function call.
    Call(String, Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
enum Et {
    Num(f64),
    Str(String),
    Field(String),
    Name(String),
    Op(String),
    Open,
    Close,
    Comma,
}

fn lex_expr(s: &str, params: &[(String, String)]) -> Result<Vec<Et>> {
    let c: Vec<char> = s.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
        } else if ch == '(' {
            out.push(Et::Open);
            i += 1;
        } else if ch == ')' {
            out.push(Et::Close);
            i += 1;
        } else if ch == ',' {
            out.push(Et::Comma);
            i += 1;
        } else if ch == '"' || ch == '\'' {
            let mut v = String::new();
            i += 1;
            while i < c.len() && c[i] != ch {
                if c[i] == '\\' && i + 1 < c.len() {
                    i += 1;
                }
                v.push(c[i]);
                i += 1;
            }
            if i >= c.len() {
                return Err(syntax("Syntax error: unterminated string"));
            }
            i += 1;
            out.push(Et::Str(v));
        } else if ch == '@' || ch == '$' {
            let mut v = String::new();
            i += 1;
            while i < c.len() && (c[i].is_alphanumeric() || "_.:-".contains(c[i])) {
                v.push(c[i]);
                i += 1;
            }
            if v.is_empty() {
                return Err(syntax(format!("Syntax error at '{ch}'")));
            }
            if ch == '@' {
                out.push(Et::Field(v));
            } else {
                let p = params
                    .iter()
                    .find(|(k, _)| *k == v)
                    .ok_or_else(|| syntax(format!("No such parameter `{v}`")))?;
                out.push(match p.1.parse::<f64>() {
                    Ok(n) => Et::Num(n),
                    Err(_) => Et::Str(p.1.clone()),
                });
            }
        } else if ch.is_ascii_digit()
            || (ch == '.' && c.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let mut v = String::new();
            while i < c.len()
                && (c[i].is_ascii_alphanumeric()
                    || c[i] == '.'
                    || ((c[i] == '-' || c[i] == '+')
                        && matches!(v.chars().last(), Some('e' | 'E'))))
            {
                v.push(c[i]);
                i += 1;
            }
            out.push(Et::Num(
                v.parse()
                    .map_err(|_| syntax(format!("Syntax error: bad number {v}")))?,
            ));
        } else if ch.is_alphabetic() || ch == '_' {
            let mut v = String::new();
            while i < c.len() && (c[i].is_alphanumeric() || c[i] == '_') {
                v.push(c[i]);
                i += 1;
            }
            out.push(Et::Name(v));
        } else {
            let two: String = c[i..(i + 2).min(c.len())].iter().collect();
            if ["==", "!=", "<=", ">=", "&&", "||"].contains(&two.as_str()) {
                out.push(Et::Op(two));
                i += 2;
            } else if "+-*/%^<>!".contains(ch) {
                out.push(Et::Op(ch.to_string()));
                i += 1;
            } else {
                return Err(syntax(format!("Syntax error at '{ch}'")));
            }
        }
    }
    Ok(out)
}

struct Ep {
    t: Vec<Et>,
    i: usize,
}

fn prec(op: &str) -> Option<u8> {
    Some(match op {
        "||" => 1,
        "&&" => 2,
        "==" | "!=" | "<" | "<=" | ">" | ">=" => 3,
        "+" | "-" => 4,
        "*" | "/" | "%" => 5,
        "^" => 6,
        _ => return None,
    })
}

impl Ep {
    fn expr(&mut self, min: u8) -> Result<Expr> {
        let mut lhs = self.unary()?;
        while let Some(Et::Op(op)) = self.t.get(self.i).cloned() {
            let Some(p) = prec(&op) else { break };
            if p < min {
                break;
            }
            self.i += 1;
            // `^` is right-associative.
            let rhs = self.expr(if op == "^" { p } else { p + 1 })?;
            lhs = Expr::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn unary(&mut self) -> Result<Expr> {
        match self.t.get(self.i).cloned() {
            Some(Et::Op(op)) if op == "-" || op == "!" => {
                self.i += 1;
                let e = self.expr(6)?;
                Ok(Expr::Unary(op.chars().next().unwrap_or('-'), Box::new(e)))
            }
            Some(Et::Op(op)) if op == "+" => {
                self.i += 1;
                self.expr(6)
            }
            _ => self.primary(),
        }
    }

    fn primary(&mut self) -> Result<Expr> {
        let t = self
            .t
            .get(self.i)
            .cloned()
            .ok_or_else(|| syntax("Syntax error: unexpected end of expression"))?;
        self.i += 1;
        Ok(match t {
            Et::Num(n) => Expr::Lit(AggVal::Num(n)),
            Et::Str(s) => Expr::Lit(AggVal::Str(s)),
            Et::Field(f) => Expr::Field(f),
            Et::Open => {
                let e = self.expr(0)?;
                if self.t.get(self.i) != Some(&Et::Close) {
                    return Err(syntax("Syntax error: expected ')'"));
                }
                self.i += 1;
                e
            }
            Et::Name(n) => {
                let lname = n.to_ascii_lowercase();
                if self.t.get(self.i) != Some(&Et::Open) {
                    return match lname.as_str() {
                        "inf" => Ok(Expr::Lit(AggVal::Num(f64::INFINITY))),
                        "nan" => Ok(Expr::Lit(AggVal::Num(f64::NAN))),
                        _ => Err(syntax(format!("Syntax error: unknown name {n}"))),
                    };
                }
                self.i += 1;
                let mut args = Vec::new();
                if self.t.get(self.i) == Some(&Et::Close) {
                    self.i += 1;
                } else {
                    loop {
                        args.push(self.expr(0)?);
                        match self.t.get(self.i) {
                            Some(Et::Comma) => self.i += 1,
                            Some(Et::Close) => {
                                self.i += 1;
                                break;
                            }
                            _ => return Err(syntax("Syntax error: expected ',' or ')'")),
                        }
                    }
                }
                if !FUNCTIONS.contains(&lname.as_str()) {
                    return Err(syntax(format!("Unknown function name '{n}'")));
                }
                Expr::Call(lname, args)
            }
            other => return Err(syntax(format!("Syntax error at {other:?}"))),
        })
    }
}

/// Parses an expression.
///
/// # Errors
/// 39822.
pub fn parse_expr(s: &str, params: &[(String, String)]) -> Result<Expr> {
    let mut p = Ep {
        t: lex_expr(s, params)?,
        i: 0,
    };
    let e = p.expr(0)?;
    if p.i != p.t.len() {
        return Err(syntax("Syntax error: extra text after the expression"));
    }
    Ok(e)
}

const FUNCTIONS: &[&str] = &[
    "upper",
    "lower",
    "strlen",
    "substr",
    "contains",
    "startswith",
    "exists",
    "abs",
    "ceil",
    "floor",
    "log",
    "log2",
    "sqrt",
    "exp",
    "format",
    "to_number",
    "to_str",
];

fn cmp_vals(a: &AggVal, b: &AggVal) -> Option<Ordering> {
    match (a.num(), b.num()) {
        (Some(x), Some(y)) => x.partial_cmp(&y),
        _ => match (a, b) {
            (AggVal::Null, _) | (_, AggVal::Null) => None,
            _ => Some(a.key_text().cmp(&b.key_text())),
        },
    }
}

fn bool_val(b: bool) -> AggVal {
    AggVal::Num(if b { 1.0 } else { 0.0 })
}

fn num1(args: &[AggVal], f: fn(f64) -> f64) -> AggVal {
    args.first()
        .and_then(AggVal::num)
        .map_or(AggVal::Null, |x| AggVal::Num(f(x)))
}

fn str_arg(v: &AggVal) -> Option<String> {
    match v {
        AggVal::Str(s) => Some(s.clone()),
        AggVal::Num(n) => Some(fmt_g(*n)),
        _ => None,
    }
}

/// A function's value.
fn call(name: &str, args: &[AggVal], raw: &[Expr], row: &Row) -> AggVal {
    let s0 = || args.first().and_then(str_arg);
    match name {
        "upper" => match args.first() {
            Some(AggVal::Str(s)) => AggVal::Str(s.to_uppercase()),
            _ => AggVal::Null,
        },
        "lower" => match args.first() {
            Some(AggVal::Str(s)) => AggVal::Str(s.to_lowercase()),
            _ => AggVal::Null,
        },
        "strlen" => s0().map_or(AggVal::Null, |s| {
            #[allow(clippy::cast_precision_loss)]
            AggVal::Num(s.len() as f64)
        }),
        "substr" => {
            let Some(s) = s0() else { return AggVal::Null };
            let off = args.get(1).and_then(AggVal::num).unwrap_or(0.0);
            let len = args.get(2).and_then(AggVal::num).unwrap_or(-1.0);
            let b = s.as_bytes();
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let start = (off.max(0.0) as usize).min(b.len());
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let end = if len < 0.0 {
                b.len()
            } else {
                (start + len as usize).min(b.len())
            };
            AggVal::Str(String::from_utf8_lossy(&b[start..end]).into_owned())
        }
        "contains" => match (s0(), args.get(1).and_then(str_arg)) {
            #[allow(clippy::cast_precision_loss)]
            (Some(s), Some(p)) if !p.is_empty() => AggVal::Num(s.matches(&p).count() as f64),
            (Some(_), Some(_)) => AggVal::Num(1.0),
            _ => AggVal::Null,
        },
        "startswith" => match (s0(), args.get(1).and_then(str_arg)) {
            (Some(s), Some(p)) => bool_val(s.starts_with(&p)),
            _ => AggVal::Null,
        },
        "exists" => match raw.first() {
            Some(Expr::Field(f)) => bool_val(!matches!(get(row, f), AggVal::Null)),
            _ => bool_val(args.first().is_some_and(|v| !matches!(v, AggVal::Null))),
        },
        "abs" => num1(args, f64::abs),
        "ceil" => num1(args, f64::ceil),
        "floor" => num1(args, f64::floor),
        "log" => num1(args, f64::ln),
        "log2" => num1(args, f64::log2),
        "sqrt" => num1(args, f64::sqrt),
        "exp" => num1(args, f64::exp),
        "to_number" => args
            .first()
            .and_then(AggVal::num)
            .map_or(AggVal::Null, AggVal::Num),
        "to_str" => s0().map_or(AggVal::Null, AggVal::Str),
        "format" => {
            let Some(f) = s0() else { return AggVal::Null };
            let mut out = String::new();
            let mut rest = args.iter().skip(1);
            let mut it = f.chars().peekable();
            while let Some(c) = it.next() {
                if c == '%' {
                    match it.next() {
                        Some('s') => {
                            out.push_str(
                                &rest
                                    .next()
                                    .and_then(AggVal::reply_text)
                                    .unwrap_or_else(|| "(null)".to_owned()),
                            );
                        }
                        Some('%') | None => out.push('%'),
                        Some(o) => {
                            out.push('%');
                            out.push(o);
                        }
                    }
                } else {
                    out.push(c);
                }
            }
            AggVal::Str(out)
        }
        _ => AggVal::Null,
    }
}

/// The value of `e` on `row`.
#[must_use]
pub fn eval(e: &Expr, row: &Row) -> AggVal {
    match e {
        Expr::Lit(v) => v.clone(),
        Expr::Field(f) => get(row, f).clone(),
        Expr::Unary(op, x) => {
            let v = eval(x, row);
            if *op == '!' {
                bool_val(!v.truthy())
            } else {
                v.num().map_or(AggVal::Null, |n| AggVal::Num(-n))
            }
        }
        Expr::Bin(op, a, b) => {
            let x = eval(a, row);
            match op.as_str() {
                "&&" => return bool_val(x.truthy() && eval(b, row).truthy()),
                "||" => return bool_val(x.truthy() || eval(b, row).truthy()),
                _ => {}
            }
            let y = eval(b, row);
            match op.as_str() {
                "==" => bool_val(cmp_vals(&x, &y) == Some(Ordering::Equal)),
                "!=" => bool_val(cmp_vals(&x, &y) != Some(Ordering::Equal)),
                "<" => bool_val(cmp_vals(&x, &y) == Some(Ordering::Less)),
                "<=" => bool_val(matches!(
                    cmp_vals(&x, &y),
                    Some(Ordering::Less | Ordering::Equal)
                )),
                ">" => bool_val(cmp_vals(&x, &y) == Some(Ordering::Greater)),
                ">=" => bool_val(matches!(
                    cmp_vals(&x, &y),
                    Some(Ordering::Greater | Ordering::Equal)
                )),
                _ => match (x.num(), y.num()) {
                    (Some(p), Some(q)) => AggVal::Num(match op.as_str() {
                        "+" => p + q,
                        "-" => p - q,
                        "*" => p * q,
                        "/" => p / q,
                        "%" => {
                            #[allow(clippy::cast_possible_truncation)]
                            let (pi, qi) = (p as i64, q as i64);
                            if qi == 0 {
                                f64::NAN
                            } else {
                                #[allow(clippy::cast_precision_loss)]
                                let r = (pi % qi) as f64;
                                r
                            }
                        }
                        _ => p.powf(q),
                    }),
                    _ => AggVal::Null,
                },
            }
        }
        Expr::Call(name, args) => {
            let vals: Vec<AggVal> = args.iter().map(|a| eval(a, row)).collect();
            call(name, &vals, args, row)
        }
    }
}

fn fields_of(e: &Expr, out: &mut Vec<String>) {
    match e {
        Expr::Field(f) => out.push(f.clone()),
        Expr::Unary(_, x) => fields_of(x, out),
        Expr::Bin(_, a, b) => {
            fields_of(a, out);
            fields_of(b, out);
        }
        Expr::Call(_, a) => a.iter().for_each(|x| fields_of(x, out)),
        Expr::Lit(_) => {}
    }
}

// ---- The pipeline ----

/// One reducer.
#[derive(Debug, Clone, PartialEq)]
pub struct Reducer {
    /// The function (lower case).
    pub func: String,
    /// Its arguments (`@` removed from field names).
    pub args: Vec<String>,
    /// The output name.
    pub alias: String,
}

/// One step.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    /// `GROUPBY`.
    Group {
        /// Grouping fields.
        by: Vec<String>,
        /// Reducers.
        reducers: Vec<Reducer>,
    },
    /// `SORTBY` (field, descending) with `MAX`.
    Sort(Vec<(String, bool)>, Option<usize>),
    /// `APPLY expr AS name`.
    Apply(Expr, String),
    /// `FILTER expr`.
    Filter(Expr),
    /// `LIMIT offset num`.
    Limit(usize, usize),
}

/// What to load from each hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Load {
    /// `LOAD *`.
    All,
    /// Named fields.
    Fields(Vec<String>),
}

/// A parsed `FT.AGGREGATE`.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// `LOAD`.
    pub load: Option<Load>,
    /// The steps.
    pub steps: Vec<Step>,
    /// `PARAMS`.
    pub params: Vec<(String, String)>,
    /// `DIALECT`.
    pub dialect: u32,
    /// `VERBATIM`.
    pub verbatim: bool,
    /// `WITHCURSOR [COUNT n] [MAXIDLE ms]`.
    pub cursor: Option<CursorOpts>,
}

/// `WITHCURSOR`'s options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorOpts {
    /// Rows per read (`COUNT`, 1000 by default).
    pub count: usize,
    /// How long an unread cursor lives, in milliseconds (`MAXIDLE`,
    /// at most and by default 300,000).
    pub max_idle_ms: u64,
}

/// The longest `MAXIDLE` (Redis's default `CURSOR_MAX_IDLE`).
pub const CURSOR_MAX_IDLE_MS: u64 = 300_000;

/// A positive number for `COUNT` or `MAXIDLE`, worded as Redis words it.
///
/// # Errors
/// 39822 when `a` isn't a number of at least 1.
pub fn cursor_number(a: Option<&[u8]>, what: &str) -> Result<u64> {
    let t = a.map(text).unwrap_or_default();
    match t.parse::<i64>() {
        Ok(n) if n >= 1 => Ok(n.unsigned_abs()),
        Ok(_) => Err(syntax(format!(
            "SEARCH_PARSE_ARGS Bad arguments for {what}: Value is outside acceptable bounds"
        ))),
        Err(_) => Err(syntax(format!(
            "SEARCH_PARSE_ARGS Bad arguments for {what}: Value is not an integer"
        ))),
    }
}

const REDUCERS: &[&str] = &[
    "count",
    "count_distinct",
    "count_distinctish",
    "sum",
    "min",
    "max",
    "avg",
    "stddev",
    "quantile",
    "tolist",
    "first_value",
    "random_sample",
];

fn field_name(a: &[u8]) -> String {
    let t = text(a);
    t.strip_prefix('@').map_or(t.clone(), str::to_owned)
}

/// Parses `FT.AGGREGATE`'s arguments after the index and the query.
/// Expressions are parsed after `PARAMS` is known.
///
/// # Errors
/// 39822 for malformed steps.
pub fn parse_plan(a: &[&[u8]]) -> Result<Plan> {
    let mut plan = Plan {
        load: None,
        steps: Vec::new(),
        params: Vec::new(),
        dialect: 1,
        verbatim: false,
        cursor: None,
    };
    // Expressions wait for PARAMS, which may come last.
    let mut pending: Vec<(usize, String, Option<String>)> = Vec::new();
    let num = |i: usize, what: &str| -> Result<usize> {
        a.get(i)
            .and_then(|x| text(x).parse::<usize>().ok())
            .ok_or_else(|| syntax(format!("Bad arguments for {what}: expected a number")))
    };
    let mut i = 0;
    while i < a.len() {
        let opt = upper(a[i]);
        match opt.as_str() {
            "VERBATIM" => plan.verbatim = true,
            "ADDSCORES" => {}
            "TIMEOUT" | "SCORER" => i += 1,
            "LOAD" => {
                let n = a.get(i + 1).map(|x| text(x)).unwrap_or_default();
                if n == "*" {
                    plan.load = Some(Load::All);
                    i += 1;
                } else {
                    let n = num(i + 1, "LOAD")?;
                    if i + 2 + n > a.len() {
                        return Err(syntax("Bad arguments for LOAD"));
                    }
                    let mut v: Vec<String> = Vec::new();
                    let mut j = i + 2;
                    while j < i + 2 + n {
                        let f = field_name(a[j]);
                        // `AS name` inside LOAD's count.
                        if j + 2 < i + 2 + n && upper(a[j + 1]) == "AS" {
                            v.push(f);
                            j += 3;
                        } else {
                            v.push(f);
                            j += 1;
                        }
                    }
                    match &mut plan.load {
                        Some(Load::Fields(f)) => f.extend(v),
                        Some(Load::All) => {}
                        None => plan.load = Some(Load::Fields(v)),
                    }
                    i += 1 + n;
                }
            }
            "GROUPBY" => {
                let n = num(i + 1, "GROUPBY")?;
                if i + 2 + n > a.len() {
                    return Err(syntax("Bad arguments for GROUPBY"));
                }
                let by: Vec<String> = a[i + 2..i + 2 + n].iter().map(|x| field_name(x)).collect();
                if a[i + 2..i + 2 + n].iter().any(|x| !x.starts_with(b"@")) {
                    return Err(syntax("Bad arguments for GROUPBY: Unknown property"));
                }
                i += 2 + n;
                let mut reducers = Vec::new();
                while a.get(i).is_some_and(|x| upper(x) == "REDUCE") {
                    let func = a
                        .get(i + 1)
                        .map(|x| text(x).to_ascii_lowercase())
                        .ok_or_else(|| syntax("Bad arguments for REDUCE"))?;
                    if !REDUCERS.contains(&func.as_str()) {
                        return Err(syntax(format!("reducer not found `{func}`")));
                    }
                    let nargs = num(i + 2, "REDUCE")?;
                    if i + 3 + nargs > a.len() {
                        return Err(syntax("Bad arguments for REDUCE"));
                    }
                    let args: Vec<String> = a[i + 3..i + 3 + nargs]
                        .iter()
                        .map(|x| field_name(x))
                        .collect();
                    i += 3 + nargs;
                    let alias = if a.get(i).is_some_and(|x| upper(x) == "AS") {
                        let al = a
                            .get(i + 1)
                            .map(|x| text(x))
                            .ok_or_else(|| syntax("Bad arguments for AS"))?;
                        i += 2;
                        al
                    } else {
                        format!("__generated_alias{func}{}", args.join(","))
                    };
                    reducers.push(Reducer { func, args, alias });
                }
                plan.steps.push(Step::Group { by, reducers });
                continue;
            }
            "SORTBY" => {
                let n = num(i + 1, "SORTBY")?;
                if i + 2 + n > a.len() {
                    return Err(syntax("Bad arguments for SORTBY"));
                }
                let mut keys: Vec<(String, bool)> = Vec::new();
                for x in &a[i + 2..i + 2 + n] {
                    match upper(x).as_str() {
                        "ASC" => {}
                        "DESC" => {
                            if let Some(k) = keys.last_mut() {
                                k.1 = true;
                            }
                        }
                        _ => keys.push((field_name(x), false)),
                    }
                }
                i += 2 + n;
                let mut max = None;
                if a.get(i).is_some_and(|x| upper(x) == "MAX") {
                    max = Some(num(i + 1, "MAX")?);
                    i += 2;
                }
                plan.steps.push(Step::Sort(keys, max));
                continue;
            }
            "APPLY" => {
                let e = a
                    .get(i + 1)
                    .map(|x| text(x))
                    .ok_or_else(|| syntax("Bad arguments for APPLY"))?;
                if a.get(i + 2).is_none_or(|x| upper(x) != "AS") || a.get(i + 3).is_none() {
                    return Err(syntax("Bad arguments for APPLY: expected AS name"));
                }
                pending.push((plan.steps.len(), e, Some(text(a[i + 3]))));
                plan.steps.push(Step::Limit(0, 0));
                i += 3;
            }
            "FILTER" => {
                let e = a
                    .get(i + 1)
                    .map(|x| text(x))
                    .ok_or_else(|| syntax("Bad arguments for FILTER"))?;
                pending.push((plan.steps.len(), e, None));
                plan.steps.push(Step::Limit(0, 0));
                i += 1;
            }
            "LIMIT" => {
                plan.steps
                    .push(Step::Limit(num(i + 1, "LIMIT")?, num(i + 2, "LIMIT")?));
                i += 2;
            }
            "PARAMS" => {
                let n = num(i + 1, "PARAMS")?;
                if n % 2 != 0 || i + 2 + n > a.len() {
                    return Err(syntax("Parameters must be name value pairs"));
                }
                for p in a[i + 2..i + 2 + n].chunks(2) {
                    plan.params.push((text(p[0]), text(p[1])));
                }
                i += 1 + n;
            }
            "DIALECT" => {
                let d = num(i + 1, "DIALECT")?;
                if !(1..=4).contains(&d) {
                    return Err(syntax(
                        "DIALECT requires a non negative integer >=1 and <= 4",
                    ));
                }
                plan.dialect = u32::try_from(d).unwrap_or(1);
                i += 1;
            }
            "WITHCURSOR" => {
                let mut c = CursorOpts {
                    count: 1000,
                    max_idle_ms: CURSOR_MAX_IDLE_MS,
                };
                loop {
                    match a.get(i + 1).map(|x| upper(x)).as_deref() {
                        Some("COUNT") => {
                            c.count =
                                usize::try_from(cursor_number(a.get(i + 2).copied(), "COUNT")?)
                                    .unwrap_or(usize::MAX);
                            i += 2;
                        }
                        Some("MAXIDLE") => {
                            c.max_idle_ms = cursor_number(a.get(i + 2).copied(), "MAXIDLE")?
                                .min(CURSOR_MAX_IDLE_MS);
                            i += 2;
                        }
                        _ => break,
                    }
                }
                plan.cursor = Some(c);
            }
            other => return Err(syntax(format!("Unknown argument `{other}`"))),
        }
        i += 1;
    }
    for (at, e, name) in pending {
        let expr = parse_expr(&e, &plan.params)?;
        plan.steps[at] = match name {
            Some(n) => Step::Apply(expr, n),
            None => Step::Filter(expr),
        };
    }
    Ok(plan)
}

impl Plan {
    /// The hash fields the steps read: `None` for every field (`LOAD *`).
    #[must_use]
    pub fn fields_needed(&self) -> Option<Vec<String>> {
        if self.load == Some(Load::All) {
            return None;
        }
        let mut v = match &self.load {
            Some(Load::Fields(f)) => f.clone(),
            _ => Vec::new(),
        };
        for s in &self.steps {
            match s {
                // Steps after a GROUPBY read its output, not the hashes.
                Step::Group { by, reducers } => {
                    v.extend(by.iter().cloned());
                    for r in reducers {
                        v.extend(r.args.iter().cloned());
                    }
                    break;
                }
                Step::Sort(k, _) => v.extend(k.iter().map(|(f, _)| f.clone())),
                Step::Apply(e, _) | Step::Filter(e) => fields_of(e, &mut v),
                Step::Limit(..) => {}
            }
        }
        v.sort();
        v.dedup();
        Some(v)
    }

    /// The fields the reply shows before any step (`LOAD`'s).
    #[must_use]
    pub fn loaded(&self) -> Option<Vec<String>> {
        match &self.load {
            Some(Load::All) => None,
            Some(Load::Fields(f)) => Some(f.clone()),
            None => Some(Vec::new()),
        }
    }

    /// Runs the steps over `rows`; the rows and Redis Search's count (the
    /// rows before the last `LIMIT`).
    ///
    /// # Errors
    /// 39822 for a property a step needs that isn't there.
    pub fn run(&self, mut rows: Vec<Row>) -> Result<(usize, Vec<Row>)> {
        let mut count = rows.len();
        for (at, s) in self.steps.iter().enumerate() {
            match s {
                Step::Group { by, reducers } => {
                    rows = group(&rows, by, reducers)?;
                    count = rows.len();
                }
                Step::Sort(keys, max) => {
                    rows.sort_by(|a, b| {
                        for (f, desc) in keys {
                            let (x, y) = (get(a, f), get(b, f));
                            let o = match (x, y) {
                                (AggVal::Null, AggVal::Null) => Ordering::Equal,
                                (AggVal::Null, _) => return Ordering::Greater,
                                (_, AggVal::Null) => return Ordering::Less,
                                _ => cmp_vals(x, y).unwrap_or(Ordering::Equal),
                            };
                            let o = if *desc { o.reverse() } else { o };
                            if o != Ordering::Equal {
                                return o;
                            }
                        }
                        Ordering::Equal
                    });
                    // Without MAX, as Redis does: the first 10 rows,
                    // unless a LIMIT follows.
                    let limited = self.steps[at..]
                        .iter()
                        .any(|x| matches!(x, Step::Limit(..)));
                    match max {
                        Some(m) => rows.truncate(*m),
                        None if !limited => rows.truncate(10),
                        None => {}
                    }
                }
                Step::Apply(e, name) => {
                    for r in &mut rows {
                        let v = eval(e, r);
                        set(r, name, v);
                    }
                }
                Step::Filter(e) => rows.retain(|r| eval(e, r).truthy()),
                Step::Limit(off, n) => {
                    count = rows.len();
                    rows = rows.into_iter().skip(*off).take(*n).collect();
                }
            }
        }
        Ok((count, rows))
    }
}

fn group(rows: &[Row], by: &[String], reducers: &[Reducer]) -> Result<Vec<Row>> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, (Vec<AggVal>, Vec<&Row>)> = HashMap::new();
    for r in rows {
        let vals: Vec<AggVal> = by.iter().map(|f| get(r, f).clone()).collect();
        let k = vals
            .iter()
            .map(AggVal::key_text)
            .collect::<Vec<_>>()
            .join("\u{2}");
        groups
            .entry(k.clone())
            .or_insert_with(|| {
                order.push(k.clone());
                (vals, Vec::new())
            })
            .1
            .push(r);
    }
    let mut out = Vec::new();
    for k in order {
        let Some((vals, members)) = groups.remove(&k) else {
            continue;
        };
        let mut row: Row = by.iter().cloned().zip(vals).collect();
        for red in reducers {
            row.push((red.alias.clone(), reduce(red, &members)?));
        }
        out.push(row);
    }
    Ok(out)
}

#[allow(clippy::cast_precision_loss)]
fn reduce(r: &Reducer, rows: &[&Row]) -> Result<AggVal> {
    let arg = |i: usize| -> Result<&str> {
        r.args
            .get(i)
            .map(String::as_str)
            .ok_or_else(|| syntax(format!("Bad arguments for {}", r.func.to_uppercase())))
    };
    let nums = |f: &str| -> Vec<f64> { rows.iter().filter_map(|x| get(x, f).num()).collect() };
    Ok(match r.func.as_str() {
        "count" => AggVal::Num(rows.len() as f64),
        "count_distinct" | "count_distinctish" => {
            let f = arg(0)?;
            let set: BTreeSet<String> = rows
                .iter()
                .map(|x| get(x, f))
                .filter(|v| !matches!(v, AggVal::Null))
                .map(AggVal::key_text)
                .collect();
            AggVal::Num(set.len() as f64)
        }
        "sum" => AggVal::Num(nums(arg(0)?).iter().sum()),
        "min" => nums(arg(0)?)
            .into_iter()
            .reduce(f64::min)
            .map_or(AggVal::Null, AggVal::Num),
        "max" => nums(arg(0)?)
            .into_iter()
            .reduce(f64::max)
            .map_or(AggVal::Null, AggVal::Num),
        "avg" => {
            let v = nums(arg(0)?);
            if v.is_empty() {
                AggVal::Num(f64::NAN)
            } else {
                AggVal::Num(v.iter().sum::<f64>() / v.len() as f64)
            }
        }
        "stddev" => {
            let v = nums(arg(0)?);
            if v.len() < 2 {
                AggVal::Num(0.0)
            } else {
                let m = v.iter().sum::<f64>() / v.len() as f64;
                let var = v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64;
                AggVal::Num(var.sqrt())
            }
        }
        "quantile" => {
            let mut v = nums(arg(0)?);
            let q: f64 = arg(1)?
                .parse()
                .map_err(|_| syntax("Bad arguments for QUANTILE"))?;
            if v.is_empty() {
                AggVal::Null
            } else {
                v.sort_by(f64::total_cmp);
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let i = ((v.len() - 1) as f64 * q.clamp(0.0, 1.0)).floor() as usize;
                AggVal::Num(v[i])
            }
        }
        "tolist" => {
            let f = arg(0)?;
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for x in rows {
                let v = get(x, f);
                if !matches!(v, AggVal::Null) && seen.insert(v.key_text()) {
                    out.push(v.clone());
                }
            }
            AggVal::List(out)
        }
        "first_value" => {
            let f = arg(0)?;
            let by = (r.args.len() >= 3 && r.args[1].eq_ignore_ascii_case("BY"))
                .then(|| r.args[2].as_str());
            let desc = r
                .args
                .get(3)
                .is_some_and(|d| d.eq_ignore_ascii_case("DESC"));
            match by {
                None => rows.first().map_or(AggVal::Null, |x| get(x, f).clone()),
                Some(b) => {
                    let mut best: Option<&Row> = None;
                    for x in rows {
                        let better = match best {
                            None => true,
                            Some(cur) => {
                                let o = cmp_vals(get(x, b), get(cur, b));
                                if desc {
                                    o == Some(Ordering::Greater)
                                } else {
                                    o == Some(Ordering::Less)
                                }
                            }
                        };
                        if better {
                            best = Some(x);
                        }
                    }
                    best.map_or(AggVal::Null, |x| get(x, f).clone())
                }
            }
        }
        "random_sample" => {
            let f = arg(0)?;
            let n: usize = arg(1)?.parse().unwrap_or(1);
            AggVal::List(
                rows.iter()
                    .map(|x| get(x, f).clone())
                    .filter(|v| !matches!(v, AggVal::Null))
                    .take(n)
                    .collect(),
            )
        }
        other => return Err(syntax(format!("reducer not found `{other}`"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<Vec<u8>> {
        s.split_whitespace()
            .map(|x| x.as_bytes().to_vec())
            .collect()
    }

    fn row(v: &[(&str, &str)]) -> Row {
        v.iter()
            .map(|(k, x)| ((*k).to_owned(), AggVal::Str((*x).to_owned())))
            .collect()
    }

    fn rows() -> Vec<Row> {
        vec![
            row(&[("tags", "red"), ("price", "10"), ("title", "Hello world")]),
            row(&[("tags", "blue"), ("price", "25"), ("title", "Hello there")]),
            row(&[("tags", "red"), ("price", "5"), ("title", "Goodbye world")]),
        ]
    }

    #[test]
    fn numbers_print_as_g12() {
        assert_eq!(fmt_g(25.0), "25");
        assert_eq!(fmt_g(5.0 / 3.0), "1.66666666667");
        assert_eq!(fmt_g(5f64.sqrt()), "2.2360679775");
        assert_eq!(fmt_g(40.0 / 3.0), "13.3333333333");
        assert_eq!(fmt_g(1e20), "1e+20");
        assert_eq!(fmt_g(0.0001), "0.0001");
        assert_eq!(fmt_g(0.000_012_5), "1.25e-05");
        assert_eq!(fmt_g(-2.5), "-2.5");
    }

    #[test]
    fn group_reduce_sort() {
        let a = args(
            "GROUPBY 1 @tags REDUCE COUNT 0 AS n REDUCE SUM 1 @price AS total REDUCE AVG 1 @price REDUCE TOLIST 1 @title AS titles SORTBY 2 @n DESC",
        );
        let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
        let p = parse_plan(&refs).unwrap();
        assert_eq!(
            p.fields_needed(),
            Some(vec!["price".to_owned(), "tags".into(), "title".into()])
        );
        let (n, out) = p.run(rows()).unwrap();
        assert_eq!(n, 2);
        assert_eq!(get(&out[0], "tags"), &AggVal::Str("red".into()));
        assert_eq!(get(&out[0], "n").reply_text().unwrap(), "2");
        assert_eq!(get(&out[0], "total").reply_text().unwrap(), "15");
        assert_eq!(
            get(&out[0], "__generated_aliasavgprice")
                .reply_text()
                .unwrap(),
            "7.5"
        );
        assert!(matches!(get(&out[0], "titles"), AggVal::List(l) if l.len() == 2));
    }

    #[test]
    fn apply_filter_limit() {
        let a: Vec<Vec<u8>> = vec![
            b"APPLY".to_vec(),
            b"@price * 2 + 1".to_vec(),
            b"AS".to_vec(),
            b"d".to_vec(),
            b"FILTER".to_vec(),
            b"@d > $min && startswith(@title, 'Hello')".to_vec(),
            b"SORTBY".to_vec(),
            b"2".to_vec(),
            b"@price".to_vec(),
            b"ASC".to_vec(),
            b"LIMIT".to_vec(),
            b"0".to_vec(),
            b"1".to_vec(),
            b"PARAMS".to_vec(),
            b"2".to_vec(),
            b"min".to_vec(),
            b"15".to_vec(),
        ];
        let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
        let p = parse_plan(&refs).unwrap();
        let (n, out) = p.run(rows()).unwrap();
        assert_eq!(n, 2);
        assert_eq!(out.len(), 1);
        assert_eq!(get(&out[0], "d").reply_text().unwrap(), "21");
        assert_eq!(
            eval(
                &parse_expr("upper(@t) == 'X' || 2 ^ 3 == 8", &[]).unwrap(),
                &row(&[("t", "x")])
            ),
            AggVal::Num(1.0)
        );
        assert_eq!(
            eval(&parse_expr("-(@p % 4)", &[]).unwrap(), &row(&[("p", "10")])),
            AggVal::Num(-2.0)
        );
        assert!(parse_expr("nosuch(1)", &[]).is_err());
        assert!(parse_expr("1 +", &[]).is_err());
        let plan_of = |s: &str| {
            let a = args(s);
            let refs: Vec<&[u8]> = a.iter().map(Vec::as_slice).collect();
            parse_plan(&refs)
        };
        let c = plan_of("LOAD 1 @a WITHCURSOR").unwrap().cursor.unwrap();
        assert_eq!((c.count, c.max_idle_ms), (1000, CURSOR_MAX_IDLE_MS));
        let p = plan_of("WITHCURSOR COUNT 5 MAXIDLE 900000 LIMIT 0 3").unwrap();
        let c = p.cursor.unwrap();
        assert_eq!((c.count, c.max_idle_ms), (5, CURSOR_MAX_IDLE_MS));
        assert_eq!(p.steps, vec![Step::Limit(0, 3)]);
        assert_eq!(
            plan_of("WITHCURSOR COUNT 0").unwrap_err().message,
            "SEARCH_PARSE_ARGS Bad arguments for COUNT: Value is outside acceptable bounds"
        );
        assert!(plan_of("WITHCURSOR MAXIDLE x").is_err());
    }
}
