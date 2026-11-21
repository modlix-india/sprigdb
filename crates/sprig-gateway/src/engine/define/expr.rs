//! Expressions of a definition (`CHECK`, `DEFAULT (...)`, `GENERATED
//! ALWAYS AS (...)`): read, and printed the way MariaDB 11.8 prints them
//! in `SHOW CREATE TABLE`.
//!
//! What the printing does, as observed on the server:
//! - names in back quotes, operators with one space each side, the fewest
//!   parentheses: a left operand is parenthesized when it binds looser
//!   than its operator, a right operand when it binds looser or as loose;
//!   `AND` / `OR` chains are flattened;
//! - `and`, `or`, `xor`, `like`, `regexp`, `in`, `between`, `is`, `case`
//!   in lower case; `DIV`, `MOD` (also for `%`) in upper case; `!=` as
//!   `<>`; function names in lower case with arguments joined by `,`, and
//!   `upper` / `lower` / `length` / `now` as `ucase` / `lcase` /
//!   `octet_length` / `current_timestamp`;
//! - `NOT` pushed into what it negates: a comparison flips (`not a = 2` is
//!   `a <> 2`), `AND` / `OR` follow De Morgan, `IS [NOT]`, `[NOT] IN`,
//!   `[NOT] BETWEEN` and `[NOT] LIKE` flip, `NOT REGEXP` is `!(...)`, and a
//!   plain value `x` becomes `x = 0`; a one-member `IN` is `=`;
//! - literals: strings in single quotes with `\'` for a quote, `NULL`,
//!   `TRUE` / `FALSE` as `1` / `0`, `x'..'` as `X'..'`, `b'..'` as hex
//!   bytes, unary minus on a number folded into it.

use super::lex::{T, Tok};

/// An expression.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum E {
    /// A column.
    Col(String),
    /// A number as written.
    Num(String),
    /// A string, with its character set introducer (`_utf8mb4'x'`).
    Str(String, Option<String>),
    /// `X'..'`.
    Hex(String),
    /// `0x..`.
    HexNum(String),
    /// `b'..'`.
    Bin(String),
    /// `NULL`.
    Null,
    /// A unary operator: `-`, `~`, `+`.
    Unary(&'static str, Box<E>),
    /// `NOT` / `!`.
    Not(Box<E>),
    /// `!(..)` as printed.
    Bang(Box<E>),
    /// A binary operator (printed spelling).
    Bin2(&'static str, Box<E>, Box<E>),
    /// `AND` / `OR` / `XOR` chains.
    Logic(&'static str, Vec<E>),
    /// `x IS [NOT] NULL|TRUE|FALSE|UNKNOWN`.
    Is(Box<E>, bool, &'static str),
    /// `x [NOT] LIKE p [ESCAPE e]`.
    Like(Box<E>, Box<E>, Option<Box<E>>, bool),
    /// `x [NOT] REGEXP p`.
    Regexp(Box<E>, Box<E>),
    /// `x [NOT] IN (list)`.
    In(Box<E>, Vec<E>, bool),
    /// `x [NOT] BETWEEN lo AND hi`.
    Between(Box<E>, Box<E>, Box<E>, bool),
    /// `CASE [operand] WHEN .. THEN .. [ELSE ..] END`.
    Case(Option<Box<E>>, Vec<(E, E)>, Option<Box<E>>),
    /// A function call: printed name, arguments.
    Func(String, Vec<E>),
    /// `CAST(x AS type)`, the type as printed.
    Cast(Box<E>, String),
    /// `x COLLATE name`.
    Collate(Box<E>, String),
    /// Text printed as it is (a form read but not modelled: `TRIM(LEADING
    /// ...)`, `CONVERT(... USING ...)`).
    Raw(String),
}

/// What reading an expression found wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Bad {
    /// A syntax error at token index `usize`.
    Syntax(usize),
    /// A subquery (1970 in a `CHECK` / default, 1901 in a generated column).
    Subquery,
    /// A user or system variable.
    Variable,
}

/// The table's default character set, for `CAST(x AS CHAR)`.
pub(crate) struct Reader<'a> {
    pub t: &'a [Tok],
    pub i: usize,
    pub charset: &'a str,
}

// Binding strengths, loosest first.
const P_OR: u8 = 2;
const P_XOR: u8 = 3;
const P_AND: u8 = 4;
const P_NOT: u8 = 5;
const P_BETWEEN: u8 = 6;
const P_CMP: u8 = 7;
const P_BITOR: u8 = 8;
const P_BITAND: u8 = 9;
const P_SHIFT: u8 = 10;
const P_ADD: u8 = 11;
const P_MUL: u8 = 12;
const P_BITXOR: u8 = 13;
const P_NEG: u8 = 15;
const P_BANG: u8 = 17;
const P_COLLATE: u8 = 18;
const P_TOP: u8 = 20;

impl Reader<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn word(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| t.is(w))
    }
    fn eat_word(&mut self, w: &str) -> bool {
        if self.word(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn op(&self, o: &str) -> bool {
        self.peek().is_some_and(|t| t.op(o))
    }
    fn eat_op(&mut self, o: &str) -> bool {
        if self.op(o) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn expect_op(&mut self, o: &str) -> Result<(), Bad> {
        if self.eat_op(o) { Ok(()) } else { Err(Bad::Syntax(self.i)) }
    }

    /// One expression.
    pub(crate) fn expr(&mut self) -> Result<E, Bad> {
        self.binary(0)
    }

    fn binary(&mut self, min: u8) -> Result<E, Bad> {
        let mut left = self.prefix()?;
        loop {
            let Some(t) = self.peek().cloned() else { break };
            // `NOT LIKE`, `NOT IN`, `NOT BETWEEN`, `NOT REGEXP`
            let negated_postfix = t.is("NOT")
                && self.t.get(self.i + 1).is_some_and(|n| {
                    n.is("LIKE") || n.is("IN") || n.is("BETWEEN") || n.is("REGEXP") || n.is("RLIKE")
                });
            let (prec, name): (u8, &str) = match &t.t {
                T::Word(w) => match w.to_ascii_uppercase().as_str() {
                    "OR" => (P_OR, "or"),
                    "XOR" => (P_XOR, "xor"),
                    "AND" => (P_AND, "and"),
                    "IS" | "LIKE" | "IN" | "REGEXP" | "RLIKE" => (P_CMP, "cmpword"),
                    "NOT" if negated_postfix => (P_CMP, "cmpword"),
                    "BETWEEN" => (P_BETWEEN, "between"),
                    "DIV" => (P_MUL, "DIV"),
                    "MOD" => (P_MUL, "MOD"),
                    "COLLATE" => (P_COLLATE, "collate"),
                    _ => break,
                },
                T::Op(o) => match *o {
                    "||" => (P_OR, "or"),
                    "&&" => (P_AND, "and"),
                    "=" | "<=>" | "<>" | "!=" | "<" | ">" | "<=" | ">=" => (P_CMP, o),
                    "|" => (P_BITOR, "|"),
                    "&" => (P_BITAND, "&"),
                    "<<" | ">>" => (P_SHIFT, o),
                    "+" | "-" => (P_ADD, o),
                    "*" | "/" => (P_MUL, o),
                    "%" => (P_MUL, "MOD"),
                    "^" => (P_BITXOR, "^"),
                    _ => break,
                },
                _ => break,
            };
            if prec < min {
                break;
            }
            self.i += 1;
            left = match name {
                "or" | "and" | "xor" => {
                    let right = self.binary(prec + 1)?;
                    let op: &'static str = match name {
                        "or" => "or",
                        "and" => "and",
                        _ => "xor",
                    };
                    logic(op, left, right)
                }
                "collate" => {
                    let n = self.name().ok_or(Bad::Syntax(self.i))?;
                    E::Collate(Box::new(left), n)
                }
                "between" => self.between(left, false)?,
                "cmpword" => self.cmp_word(&t, left)?,
                _ => {
                    let right = self.binary(prec + 1)?;
                    let op: &'static str = match name {
                        "!=" => "<>",
                        "=" => "=",
                        "<=>" => "<=>",
                        "<>" => "<>",
                        "<" => "<",
                        ">" => ">",
                        "<=" => "<=",
                        ">=" => ">=",
                        "|" => "|",
                        "&" => "&",
                        "<<" => "<<",
                        ">>" => ">>",
                        "+" => "+",
                        "-" => "-",
                        "*" => "*",
                        "/" => "/",
                        "DIV" => "DIV",
                        "MOD" => "MOD",
                        _ => "^",
                    };
                    E::Bin2(op, Box::new(left), Box::new(right))
                }
            };
        }
        Ok(left)
    }

    fn between(&mut self, left: E, not: bool) -> Result<E, Bad> {
        let lo = self.binary(P_CMP)?;
        if !self.eat_word("AND") {
            return Err(Bad::Syntax(self.i));
        }
        let hi = self.binary(P_CMP)?;
        Ok(E::Between(Box::new(left), Box::new(lo), Box::new(hi), not))
    }

    fn cmp_word(&mut self, t: &Tok, left: E) -> Result<E, Bad> {
        let mut not = false;
        let kw = if t.is("NOT") {
            not = true;
            let k = self.peek().cloned().ok_or(Bad::Syntax(self.i))?;
            self.i += 1;
            k
        } else {
            t.clone()
        };
        if kw.is("IS") {
            let n = self.eat_word("NOT");
            let what = if self.eat_word("NULL") {
                "null"
            } else if self.eat_word("TRUE") {
                "true"
            } else if self.eat_word("FALSE") {
                "false"
            } else if self.eat_word("UNKNOWN") {
                "unknown"
            } else {
                return Err(Bad::Syntax(self.i));
            };
            return Ok(E::Is(Box::new(left), n, what));
        }
        if kw.is("LIKE") {
            let p = self.binary(P_CMP + 1)?;
            let esc = if self.eat_word("ESCAPE") { Some(Box::new(self.prefix()?)) } else { None };
            return Ok(E::Like(Box::new(left), Box::new(p), esc, not));
        }
        if kw.is("REGEXP") || kw.is("RLIKE") {
            let p = self.binary(P_CMP + 1)?;
            let r = E::Regexp(Box::new(left), Box::new(p));
            return Ok(if not { E::Not(Box::new(r)) } else { r });
        }
        if kw.is("IN") {
            self.expect_op("(")?;
            if self.word("SELECT") {
                return Err(Bad::Subquery);
            }
            let mut list = vec![self.expr()?];
            while self.eat_op(",") {
                list.push(self.expr()?);
            }
            self.expect_op(")")?;
            return Ok(E::In(Box::new(left), list, not));
        }
        if kw.is("BETWEEN") {
            return self.between(left, not);
        }
        Err(Bad::Syntax(self.i))
    }

    fn name(&mut self) -> Option<String> {
        let t = self.peek()?.clone();
        let n = match t.t {
            T::Word(w) => w,
            T::Quoted(q) => q,
            T::Str(s) => s,
            _ => return None,
        };
        self.i += 1;
        Some(n)
    }

    fn prefix(&mut self) -> Result<E, Bad> {
        let Some(t) = self.peek().cloned() else { return Err(Bad::Syntax(self.i)) };
        self.i += 1;
        match t.t {
            T::Num(n) => Ok(E::Num(n)),
            T::Str(s) => Ok(E::Str(s, None)),
            T::Hex(h) => Ok(E::Hex(h)),
            T::HexNum(h) => Ok(E::HexNum(h)),
            T::Bin(b) => Ok(E::Bin(b)),
            T::Var(_) => Err(Bad::Variable),
            T::Quoted(q) => self.after_name(q, true),
            T::Op("(") => {
                if self.word("SELECT") {
                    return Err(Bad::Subquery);
                }
                let e = self.expr()?;
                self.expect_op(")")?;
                Ok(e)
            }
            T::Op("-") => {
                let e = self.binary(P_NEG)?;
                Ok(neg(e))
            }
            T::Op("+") => self.binary(P_NEG),
            T::Op("~") => Ok(E::Unary("~", Box::new(self.binary(P_NEG)?))),
            T::Op("!") => Ok(E::Not(Box::new(self.binary(P_BANG)?))),
            T::Word(w) => {
                let u = w.to_ascii_uppercase();
                match u.as_str() {
                    "NULL" => Ok(E::Null),
                    "TRUE" => Ok(E::Num("1".into())),
                    "FALSE" => Ok(E::Num("0".into())),
                    "NOT" => Ok(E::Not(Box::new(self.binary(P_NOT)?))),
                    "BINARY" => {
                        let e = self.binary(P_NEG)?;
                        Ok(E::Cast(Box::new(e), "char charset binary".into()))
                    }
                    "CASE" => self.case(),
                    "CAST" | "CONVERT" if self.op("(") => self.cast(&u),
                    "INTERVAL" => Err(Bad::Syntax(self.i - 1)),
                    "EXISTS" if self.op("(") => Err(Bad::Subquery),
                    _ if u.starts_with('_') && matches!(self.peek().map(|t| &t.t), Some(T::Str(_))) => {
                        let Some(T::Str(s)) = self.peek().map(|t| t.t.clone()) else { unreachable!() };
                        self.i += 1;
                        Ok(E::Str(s, Some(w[1..].to_ascii_lowercase())))
                    }
                    "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "UTC_TIMESTAMP" | "UTC_DATE" | "UTC_TIME" | "CURRENT_USER" if !self.op("(") => {
                        Ok(E::Func(func_name(&u).to_owned(), Vec::new()))
                    }
                    _ => self.after_name(w, false),
                }
            }
            _ => Err(Bad::Syntax(self.i - 1)),
        }
    }

    fn after_name(&mut self, n: String, quoted: bool) -> Result<E, Bad> {
        if !quoted && self.op("(") {
            self.i += 1;
            let u = n.to_ascii_uppercase();
            if u == "TRIM" && (self.word("LEADING") || self.word("TRAILING") || self.word("BOTH")) {
                return self.raw_until_close("trim(");
            }
            let mut args = Vec::new();
            if !self.op(")") {
                if self.word("SELECT") {
                    return Err(Bad::Subquery);
                }
                // COUNT(*) and the like aren't allowed in a definition; read them.
                if self.eat_op("*") {
                    args.push(E::Raw("*".into()));
                } else {
                    args.push(self.expr()?);
                }
                while self.eat_op(",") {
                    args.push(self.expr()?);
                }
            }
            self.expect_op(")")?;
            return Ok(E::Func(func_name(&u).to_owned(), args));
        }
        // `t.c`: the table's own name qualifies a column.
        if self.op(".") {
            self.i += 1;
            return match self.name() {
                Some(c) => Ok(E::Col(c)),
                None => Err(Bad::Syntax(self.i)),
            };
        }
        Ok(E::Col(n))
    }

    /// Text up to the matching `)`, printed as written in lower case
    /// keywords (forms not modelled).
    fn raw_until_close(&mut self, head: &str) -> Result<E, Bad> {
        let mut depth = 1;
        let mut parts = Vec::new();
        while let Some(t) = self.peek().cloned() {
            self.i += 1;
            match &t.t {
                T::Op("(") => depth += 1,
                T::Op(")") => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(E::Raw(format!("{head}{})", parts.join(" "))));
                    }
                }
                _ => {}
            }
            parts.push(match t.t {
                T::Word(w) => w.to_ascii_lowercase(),
                T::Quoted(q) => quote_name(&q),
                T::Str(s) => quote_str(&s),
                T::Num(n) => n,
                T::Op(o) => o.to_owned(),
                _ => return Err(Bad::Syntax(self.i - 1)),
            });
        }
        Err(Bad::Syntax(self.i))
    }

    fn case(&mut self) -> Result<E, Bad> {
        let operand = if self.word("WHEN") { None } else { Some(Box::new(self.expr()?)) };
        let mut whens = Vec::new();
        while self.eat_word("WHEN") {
            let w = self.expr()?;
            if !self.eat_word("THEN") {
                return Err(Bad::Syntax(self.i));
            }
            whens.push((w, self.expr()?));
        }
        if whens.is_empty() {
            return Err(Bad::Syntax(self.i));
        }
        let els = if self.eat_word("ELSE") { Some(Box::new(self.expr()?)) } else { None };
        if !self.eat_word("END") {
            return Err(Bad::Syntax(self.i));
        }
        Ok(E::Case(operand, whens, els))
    }

    fn cast(&mut self, which: &str) -> Result<E, Bad> {
        self.expect_op("(")?;
        let e = self.expr()?;
        if which == "CONVERT" && self.eat_word("USING") {
            let cs = self.name().ok_or(Bad::Syntax(self.i))?;
            self.expect_op(")")?;
            return Ok(E::Func("convert".into(), vec![e, E::Raw(format!("using {}", cs.to_ascii_lowercase()))]));
        }
        if which == "CAST" {
            if !self.eat_word("AS") {
                return Err(Bad::Syntax(self.i));
            }
        } else {
            self.expect_op(",")?;
        }
        let ty = self.cast_type()?;
        self.expect_op(")")?;
        Ok(E::Cast(Box::new(e), ty))
    }

    fn cast_type(&mut self) -> Result<String, Bad> {
        let w = self.name().ok_or(Bad::Syntax(self.i))?.to_ascii_lowercase();
        let mut args = String::new();
        if self.eat_op("(") {
            let mut v = Vec::new();
            while let Some(T::Num(n)) = self.peek().map(|t| t.t.clone()) {
                self.i += 1;
                v.push(n);
                if !self.eat_op(",") {
                    break;
                }
            }
            self.expect_op(")")?;
            args = format!("({})", v.join(","));
        }
        Ok(match w.as_str() {
            "signed" | "unsigned" => {
                self.eat_word("INTEGER");
                self.eat_word("INT");
                w
            }
            "char" | "nchar" | "varchar" => {
                let mut cs = self.charset.to_owned();
                if self.eat_word("CHARACTER") {
                    self.eat_word("SET");
                    cs = self.name().ok_or(Bad::Syntax(self.i))?.to_ascii_lowercase();
                } else if self.eat_word("CHARSET") {
                    cs = self.name().ok_or(Bad::Syntax(self.i))?.to_ascii_lowercase();
                } else if self.eat_word("ASCII") {
                    cs = "latin1".into();
                } else if self.eat_word("UNICODE") {
                    cs = "ucs2".into();
                }
                if w == "nchar" {
                    cs = "utf8mb3".into();
                }
                format!("char{args} charset {cs}")
            }
            "binary" => format!("char{args} charset binary"),
            "decimal" | "date" | "datetime" | "time" | "double" | "float" | "json" | "year" => format!("{w}{args}"),
            "integer" | "int" => "signed".into(),
            _ => return Err(Bad::Syntax(self.i - 1)),
        })
    }
}

/// The printed name of function `u` (upper case as read).
fn func_name(u: &str) -> &str {
    match u {
        "UPPER" | "UCASE" => "ucase",
        "LOWER" | "LCASE" => "lcase",
        "LENGTH" | "OCTET_LENGTH" => "octet_length",
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIME" | "LOCALTIMESTAMP" => "current_timestamp",
        "CURRENT_DATE" | "CURDATE" => "curdate",
        "CURRENT_TIME" | "CURTIME" => "curtime",
        "SUBSTRING" | "SUBSTR" | "MID" => "substr",
        "CHARACTER_LENGTH" | "CHAR_LENGTH" => "char_length",
        "CURRENT_USER" => "current_user",
        "POW" | "POWER" => "pow",
        "CEIL" | "CEILING" => "ceiling",
        _ => leak_lower(u),
    }
}

/// `u` in lower case (function names are few and short-lived; a small
/// leak per distinct name keeps the printer's names `&str`).
fn leak_lower(u: &str) -> &'static str {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static NAMES: OnceLock<Mutex<HashMap<String, &'static str>>> = OnceLock::new();
    let mut m = NAMES.get_or_init(Default::default).lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(s) = m.get(u) {
        return s;
    }
    if m.len() > 4096 {
        return "unknown_function";
    }
    let s: &'static str = Box::leak(u.to_ascii_lowercase().into_boxed_str());
    m.insert(u.to_owned(), s);
    s
}

/// `a op b`, flattening chains of the same operator.
fn logic(op: &'static str, a: E, b: E) -> E {
    let mut v = Vec::new();
    for x in [a, b] {
        match x {
            E::Logic(o, xs) if o == op => v.extend(xs),
            x => v.push(x),
        }
    }
    E::Logic(op, v)
}

/// Unary minus, folded into a number.
fn neg(e: E) -> E {
    match e {
        E::Num(n) if n.starts_with('-') => E::Num(n[1..].to_owned()),
        E::Num(n) => E::Num(format!("-{n}")),
        E::Unary("-", inner) => *inner,
        e => E::Unary("-", Box::new(e)),
    }
}

/// `NOT e`, pushed in as the server does.
pub(crate) fn negate(e: E) -> E {
    match e {
        E::Bin2(op @ ("=" | "<>" | "<" | ">" | "<=" | ">="), a, b) => {
            let flipped = match op {
                "=" => "<>",
                "<>" => "=",
                "<" => ">=",
                ">" => "<=",
                "<=" => ">",
                _ => "<",
            };
            E::Bin2(flipped, a, b)
        }
        E::Logic("and", xs) => E::Logic("or", xs.into_iter().map(negate).collect()),
        E::Logic("or", xs) => E::Logic("and", xs.into_iter().map(negate).collect()),
        E::Is(x, n, w) => E::Is(x, !n, w),
        E::Like(x, p, esc, n) => E::Like(x, p, esc, !n),
        E::In(x, l, n) => E::In(x, l, !n),
        E::Between(x, lo, hi, n) => E::Between(x, lo, hi, !n),
        E::Not(x) => *x,
        e @ (E::Regexp(..) | E::Bang(_) | E::Bin2("<=>", ..) | E::Logic("xor", _)) => E::Bang(Box::new(e)),
        e => E::Bin2("=", Box::new(e), Box::new(E::Num("0".into()))),
    }
}

/// Applies the server's rewrites: `NOT` pushed in, one-member `IN`.
pub(crate) fn normalize(e: E) -> E {
    match e {
        E::Not(x) => negate(normalize(*x)),
        E::In(x, mut l, n) if l.len() == 1 => {
            let v = normalize(l.remove(0));
            E::Bin2(if n { "<>" } else { "=" }, Box::new(normalize(*x)), Box::new(v))
        }
        E::In(x, l, n) => E::In(Box::new(normalize(*x)), l.into_iter().map(normalize).collect(), n),
        E::Unary(o, x) => match (o, normalize(*x)) {
            ("-", x) => neg(x),
            ("+", x) => x,
            (o, x) => E::Unary(o, Box::new(x)),
        },
        E::Bang(x) => E::Bang(Box::new(normalize(*x))),
        E::Bin2(o, a, b) => E::Bin2(o, Box::new(normalize(*a)), Box::new(normalize(*b))),
        E::Logic(o, xs) => {
            let mut out = Vec::new();
            for x in xs {
                match normalize(x) {
                    E::Logic(o2, ys) if o2 == o => out.extend(ys),
                    y => out.push(y),
                }
            }
            E::Logic(o, out)
        }
        E::Is(x, n, w) => E::Is(Box::new(normalize(*x)), n, w),
        E::Like(x, p, esc, n) => E::Like(Box::new(normalize(*x)), Box::new(normalize(*p)), esc.map(|e| Box::new(normalize(*e))), n),
        E::Regexp(x, p) => E::Regexp(Box::new(normalize(*x)), Box::new(normalize(*p))),
        E::Between(x, lo, hi, n) => E::Between(Box::new(normalize(*x)), Box::new(normalize(*lo)), Box::new(normalize(*hi)), n),
        E::Case(o, ws, el) => E::Case(
            o.map(|x| Box::new(normalize(*x))),
            ws.into_iter().map(|(a, b)| (normalize(a), normalize(b))).collect(),
            el.map(|x| Box::new(normalize(*x))),
        ),
        E::Func(n, args) => E::Func(n, args.into_iter().map(normalize).collect()),
        E::Cast(x, t) => E::Cast(Box::new(normalize(*x)), t),
        E::Collate(x, c) => E::Collate(Box::new(normalize(*x)), c),
        e => e,
    }
}

fn prec(e: &E) -> u8 {
    match e {
        E::Logic("or", _) => P_OR,
        E::Logic("xor", _) => P_XOR,
        E::Logic(_, _) => P_AND,
        E::Not(_) => P_NOT,
        E::Between(..) => P_BETWEEN,
        E::Bin2(o, ..) => match *o {
            "|" => P_BITOR,
            "&" => P_BITAND,
            "<<" | ">>" => P_SHIFT,
            "+" | "-" => P_ADD,
            "*" | "/" | "DIV" | "MOD" => P_MUL,
            "^" => P_BITXOR,
            _ => P_CMP,
        },
        E::Is(..) | E::Like(..) | E::Regexp(..) | E::In(..) => P_CMP,
        E::Unary(..) => P_NEG,
        E::Num(n) if n.starts_with('-') => P_NEG,
        E::Bang(_) => P_BANG,
        E::Collate(..) => P_COLLATE,
        _ => P_TOP,
    }
}

/// `name` in back quotes.
pub(crate) fn quote_name(n: &str) -> String {
    format!("`{}`", n.replace('`', "``"))
}

/// A string literal as MariaDB prints it.
pub(crate) fn quote_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => o.push_str("\\'"),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\0' => o.push_str("\\0"),
            '\u{1a}' => o.push_str("\\Z"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// A number as the server prints it: integers without leading zeros or a
/// `+`.
fn print_num(n: &str) -> String {
    if n.bytes().all(|b| b.is_ascii_digit() || b == b'-') {
        let neg = n.starts_with('-');
        let digits = n.trim_start_matches('-').trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        return if neg && digits != "0" { format!("-{digits}") } else { digits.to_owned() };
    }
    n.to_owned()
}

/// `b'..'` as hex bytes.
fn bin_hex(b: &str) -> String {
    let mut v: u128 = 0;
    for c in b.chars() {
        v = v.wrapping_mul(2).wrapping_add(u128::from(c == '1'));
    }
    let bytes = b.len().div_ceil(8).max(1);
    let mut s = String::from("0x");
    for k in (0..bytes).rev() {
        s.push_str(&format!("{:02x}", (v >> (8 * k)) & 0xff));
    }
    s
}

/// The expression as MariaDB prints it.
pub(crate) fn print(e: &E) -> String {
    let mut s = String::new();
    put(e, &mut s);
    s
}

fn put_child(e: &E, parent: u8, right: bool, s: &mut String) {
    let p = prec(e);
    if p < parent || (right && p == parent && parent != P_TOP) {
        s.push('(');
        put(e, s);
        s.push(')');
    } else {
        put(e, s);
    }
}

fn put(e: &E, s: &mut String) {
    match e {
        E::Col(c) => s.push_str(&quote_name(c)),
        E::Num(n) => s.push_str(&print_num(n)),
        E::Str(v, intro) => {
            if let Some(cs) = intro {
                s.push('_');
                s.push_str(cs);
            }
            s.push_str(&quote_str(v));
        }
        E::Hex(h) => {
            s.push_str("X'");
            s.push_str(h);
            s.push('\'');
        }
        E::HexNum(h) => {
            s.push_str("0x");
            s.push_str(h);
        }
        E::Bin(b) => s.push_str(&bin_hex(b)),
        E::Null => s.push_str("NULL"),
        E::Unary(o, x) => {
            s.push_str(o);
            put_child(x, P_NEG, false, s);
        }
        E::Not(x) => {
            s.push_str("!(");
            put(x, s);
            s.push(')');
        }
        E::Bang(x) => {
            s.push_str("!(");
            put(x, s);
            s.push(')');
        }
        E::Bin2(o, a, b) => {
            let p = prec(e);
            put_child(a, p, false, s);
            s.push(' ');
            s.push_str(o);
            s.push(' ');
            put_child(b, p, true, s);
        }
        E::Logic(o, xs) => {
            let p = prec(e);
            for (k, x) in xs.iter().enumerate() {
                if k > 0 {
                    s.push(' ');
                    s.push_str(o);
                    s.push(' ');
                }
                put_child(x, p, k > 0 && prec(x) == p, s);
            }
        }
        E::Is(x, n, w) => {
            put_child(x, P_CMP, false, s);
            s.push_str(if *n { " is not " } else { " is " });
            s.push_str(w);
        }
        E::Like(x, p, esc, n) => {
            put_child(x, P_CMP, false, s);
            // The server prints two spaces before `not like`.
            s.push_str(if *n { "  not like " } else { " like " });
            put_child(p, P_CMP, true, s);
            if let Some(esc) = esc {
                s.push_str(" escape ");
                put(esc, s);
            }
        }
        E::Regexp(x, p) => {
            put_child(x, P_CMP, false, s);
            s.push_str(" regexp ");
            put_child(p, P_CMP, true, s);
        }
        E::In(x, l, n) => {
            put_child(x, P_CMP, false, s);
            s.push_str(if *n { " not in (" } else { " in (" });
            for (k, v) in l.iter().enumerate() {
                if k > 0 {
                    s.push(',');
                }
                put(v, s);
            }
            s.push(')');
        }
        E::Between(x, lo, hi, n) => {
            put_child(x, P_BETWEEN, false, s);
            s.push_str(if *n { " not between " } else { " between " });
            put_child(lo, P_BETWEEN, true, s);
            s.push_str(" and ");
            put_child(hi, P_BETWEEN, true, s);
        }
        E::Case(o, ws, el) => {
            s.push_str("case ");
            if let Some(o) = o {
                put(o, s);
                s.push(' ');
            }
            for (w, t) in ws {
                s.push_str("when ");
                put(w, s);
                s.push_str(" then ");
                put(t, s);
                s.push(' ');
            }
            if let Some(el) = el {
                s.push_str("else ");
                put(el, s);
                s.push(' ');
            }
            s.push_str("end");
        }
        E::Func(n, args) => {
            s.push_str(n);
            s.push('(');
            for (k, a) in args.iter().enumerate() {
                if k > 0 {
                    s.push(',');
                }
                put(a, s);
            }
            s.push(')');
        }
        E::Cast(x, t) => {
            s.push_str("cast(");
            put(x, s);
            s.push_str(" as ");
            s.push_str(t);
            s.push(')');
        }
        E::Collate(x, c) => {
            put_child(x, P_COLLATE, false, s);
            s.push_str(" collate ");
            s.push_str(&c.to_ascii_lowercase());
        }
        E::Raw(r) => s.push_str(r),
    }
}

/// The columns `e` reads.
pub(crate) fn columns(e: &E, out: &mut Vec<String>) {
    match e {
        E::Col(c) => out.push(c.clone()),
        E::Unary(_, x) | E::Not(x) | E::Bang(x) | E::Cast(x, _) | E::Collate(x, _) | E::Is(x, _, _) => columns(x, out),
        E::Bin2(_, a, b) | E::Regexp(a, b) => {
            columns(a, out);
            columns(b, out);
        }
        E::Logic(_, xs) | E::Func(_, xs) => xs.iter().for_each(|x| columns(x, out)),
        E::Like(x, p, esc, _) => {
            columns(x, out);
            columns(p, out);
            if let Some(e) = esc {
                columns(e, out);
            }
        }
        E::In(x, l, _) => {
            columns(x, out);
            l.iter().for_each(|x| columns(x, out));
        }
        E::Between(x, a, b, _) => {
            columns(x, out);
            columns(a, out);
            columns(b, out);
        }
        E::Case(o, ws, el) => {
            if let Some(o) = o {
                columns(o, out);
            }
            for (a, b) in ws {
                columns(a, out);
                columns(b, out);
            }
            if let Some(el) = el {
                columns(el, out);
            }
        }
        _ => {}
    }
}

/// Renames column `from` to `to` in `e` (names compare without case).
pub(crate) fn rename_column(e: &mut E, from: &str, to: &str) {
    match e {
        E::Col(c) if c.eq_ignore_ascii_case(from) => *c = to.to_owned(),
        E::Unary(_, x) | E::Not(x) | E::Bang(x) | E::Cast(x, _) | E::Collate(x, _) | E::Is(x, _, _) => rename_column(x, from, to),
        E::Bin2(_, a, b) | E::Regexp(a, b) => {
            rename_column(a, from, to);
            rename_column(b, from, to);
        }
        E::Logic(_, xs) | E::Func(_, xs) => xs.iter_mut().for_each(|x| rename_column(x, from, to)),
        E::Like(x, p, esc, _) => {
            rename_column(x, from, to);
            rename_column(p, from, to);
            if let Some(e) = esc {
                rename_column(e, from, to);
            }
        }
        E::In(x, l, _) => {
            rename_column(x, from, to);
            l.iter_mut().for_each(|x| rename_column(x, from, to));
        }
        E::Between(x, a, b, _) => {
            rename_column(x, from, to);
            rename_column(a, from, to);
            rename_column(b, from, to);
        }
        E::Case(o, ws, el) => {
            if let Some(o) = o {
                rename_column(o, from, to);
            }
            for (a, b) in ws {
                rename_column(a, from, to);
                rename_column(b, from, to);
            }
            if let Some(el) = el {
                rename_column(el, from, to);
            }
        }
        _ => {}
    }
}

/// A literal value (a `DEFAULT (...)` that is one is stored as a value).
pub(crate) fn is_literal(e: &E) -> bool {
    matches!(e, E::Num(_) | E::Str(..) | E::Hex(_) | E::HexNum(_) | E::Bin(_) | E::Null)
}

/// Whether the server prints this `DEFAULT` expression without
/// parentheses: a plain function call (not `CAST`, not `CASE`).
pub(crate) fn bare_default(e: &E) -> bool {
    matches!(e, E::Func(..))
}
