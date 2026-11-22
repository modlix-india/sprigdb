//! A small parser from the text of a SQL value expression to an [`Expr`],
//! for the tests: the golden files keep each case as the text that was
//! sent to the servers, and the named cases are written by hand. It reads
//! what `Expr::to_sql` writes and ordinary hand-written expressions over
//! the shared test table; it is not the product's SQL parser.

use sprig_engine::keys::collation::{Collation, Flavor};
use sprig_exec::{BinOp, CastTarget, Expr, Truth};

use super::Col;

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Int(String),
    Dec(String),
    Dbl(String),
    Str(String),
    Hex(Vec<u8>),
    /// A bare word, as written.
    Word(String),
    /// A back-quoted name.
    Name(String),
    Sym(&'static str),
    End,
}

struct Lexer<'a> {
    s: &'a [u8],
    i: usize,
}

const SYMS: &[&str] = &[
    "<=>", "<=", ">=", "<>", "!=", "<<", ">>", "||", "&&", "(", ")", ",", "+", "-", "*", "/", "%",
    "^", "&", "|", "~", "=", "<", ">", "!",
];

impl Lexer<'_> {
    fn next(&mut self) -> Result<Tok, String> {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
        let s = self.s;
        let Some(&c) = s.get(self.i) else {
            return Ok(Tok::End);
        };
        let start = self.i;
        if c == b'\'' {
            return self.string();
        }
        if c == b'`' {
            let end = s[start + 1..]
                .iter()
                .position(|&b| b == b'`')
                .ok_or("unclosed back quote")?;
            self.i = start + end + 2;
            return Ok(Tok::Name(
                String::from_utf8_lossy(&s[start + 1..start + 1 + end]).into_owned(),
            ));
        }
        if c == b'0' && matches!(s.get(start + 1), Some(b'x')) {
            let mut j = start + 2;
            while j < s.len() && s[j].is_ascii_hexdigit() {
                j += 1;
            }
            self.i = j;
            return Ok(Tok::Hex(hex_bytes(&s[start + 2..j])));
        }
        if (c == b'x' || c == b'X') && s.get(start + 1) == Some(&b'\'') {
            let end = s[start + 2..]
                .iter()
                .position(|&b| b == b'\'')
                .ok_or("unclosed hex literal")?;
            self.i = start + end + 3;
            return Ok(Tok::Hex(hex_bytes(&s[start + 2..start + 2 + end])));
        }
        let digit_next = |j: usize| s.get(j).is_some_and(u8::is_ascii_digit);
        if c.is_ascii_digit() || (c == b'.' && digit_next(start + 1)) {
            let mut j = start;
            while digit_next(j) {
                j += 1;
            }
            let mut point = false;
            if s.get(j) == Some(&b'.') {
                point = true;
                j += 1;
                while digit_next(j) {
                    j += 1;
                }
            }
            let mut exp = false;
            if matches!(s.get(j), Some(b'e' | b'E')) {
                let mut k = j + 1;
                if matches!(s.get(k), Some(b'+' | b'-')) {
                    k += 1;
                }
                if digit_next(k) {
                    while digit_next(k) {
                        k += 1;
                    }
                    exp = true;
                    j = k;
                }
            }
            self.i = j;
            let text = String::from_utf8_lossy(&s[start..j]).into_owned();
            return Ok(if exp {
                Tok::Dbl(text)
            } else if point {
                Tok::Dec(text)
            } else {
                Tok::Int(text)
            });
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'@' {
            let mut j = start + 1;
            while j < s.len() && (s[j].is_ascii_alphanumeric() || s[j] == b'_' || s[j] == b'$') {
                j += 1;
            }
            self.i = j;
            return Ok(Tok::Word(
                String::from_utf8_lossy(&s[start..j]).into_owned(),
            ));
        }
        for sym in SYMS {
            if s[start..].starts_with(sym.as_bytes()) {
                self.i = start + sym.len();
                return Ok(Tok::Sym(sym));
            }
        }
        Err(format!(
            "unexpected character {:?} at {start}",
            char::from(c)
        ))
    }

    fn string(&mut self) -> Result<Tok, String> {
        let s = self.s;
        let mut out = Vec::new();
        let mut j = self.i + 1;
        loop {
            let Some(&c) = s.get(j) else {
                return Err("unclosed string".to_owned());
            };
            j += 1;
            match c {
                b'\'' if s.get(j) == Some(&b'\'') => {
                    out.push(b'\'');
                    j += 1;
                }
                b'\'' => break,
                b'\\' => {
                    let Some(&e) = s.get(j) else {
                        return Err("unclosed string".to_owned());
                    };
                    j += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'0' => out.push(0),
                        b'Z' => out.push(0x1a),
                        b'b' => out.push(8),
                        b'%' | b'_' => {
                            out.push(b'\\');
                            out.push(e);
                        }
                        other => out.push(other),
                    }
                }
                other => out.push(other),
            }
        }
        self.i = j;
        String::from_utf8(out)
            .map(Tok::Str)
            .map_err(|_| "a string literal that is not UTF-8".to_owned())
    }
}

fn hex_bytes(digits: &[u8]) -> Vec<u8> {
    let mut d: Vec<u8> = digits
        .iter()
        .map(|c| char::from(*c).to_digit(16).unwrap_or(0) as u8)
        .collect();
    if d.len() % 2 == 1 {
        d.insert(0, 0);
    }
    d.chunks(2).map(|p| p[0] << 4 | p[1]).collect()
}

/// The units an `INTERVAL` takes.
pub const UNITS: &[&str] = &[
    "MICROSECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
    "SECOND_MICROSECOND",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "HOUR_MICROSECOND",
    "HOUR_SECOND",
    "HOUR_MINUTE",
    "DAY_MICROSECOND",
    "DAY_SECOND",
    "DAY_MINUTE",
    "DAY_HOUR",
    "YEAR_MONTH",
];

/// Functions written without parentheses.
const NILADIC: &[&str] = &[
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "UTC_TIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
];

struct Parser<'a> {
    lex: Lexer<'a>,
    tok: Tok,
    cols: &'a [Col],
    flavor: Flavor,
}

/// Parses one expression over the columns `cols` (the row's slots, in
/// order). Collation names are resolved as `flavor` does.
pub fn parse(sql: &str, cols: &[Col], flavor: Flavor) -> Result<Expr, String> {
    let mut p = Parser {
        lex: Lexer {
            s: sql.as_bytes(),
            i: 0,
        },
        tok: Tok::End,
        cols,
        flavor,
    };
    p.advance()?;
    let e = p.expr()?;
    if p.tok != Tok::End {
        return Err(format!("unexpected {:?} after the expression", p.tok));
    }
    Ok(e)
}

fn b(e: Expr) -> Box<Expr> {
    Box::new(e)
}

impl Parser<'_> {
    fn advance(&mut self) -> Result<(), String> {
        self.tok = self.lex.next()?;
        Ok(())
    }

    fn word_is(&self, w: &str) -> bool {
        matches!(&self.tok, Tok::Word(x) if x.eq_ignore_ascii_case(w))
    }

    fn eat_word(&mut self, w: &str) -> Result<bool, String> {
        if self.word_is(w) {
            self.advance()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn eat_sym(&mut self, s: &str) -> Result<bool, String> {
        if matches!(&self.tok, Tok::Sym(x) if *x == s) {
            self.advance()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn expect_sym(&mut self, s: &str) -> Result<(), String> {
        if self.eat_sym(s)? {
            Ok(())
        } else {
            Err(format!("expected {s:?}, found {:?}", self.tok))
        }
    }

    fn expect_word(&mut self, w: &str) -> Result<(), String> {
        if self.eat_word(w)? {
            Ok(())
        } else {
            Err(format!("expected {w}, found {:?}", self.tok))
        }
    }

    fn take_word(&mut self) -> Result<String, String> {
        match std::mem::replace(&mut self.tok, Tok::End) {
            Tok::Word(w) => {
                self.advance()?;
                Ok(w)
            }
            other => Err(format!("expected a word, found {other:?}")),
        }
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut left = self.xor()?;
        loop {
            if self.eat_word("OR")? || self.eat_sym("||")? {
                left = Expr::binary(BinOp::Or, left, self.xor()?);
            } else {
                return Ok(left);
            }
        }
    }

    fn xor(&mut self) -> Result<Expr, String> {
        let mut left = self.and()?;
        while self.eat_word("XOR")? {
            left = Expr::binary(BinOp::Xor, left, self.and()?);
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Expr, String> {
        let mut left = self.not()?;
        loop {
            if self.eat_word("AND")? || self.eat_sym("&&")? {
                left = Expr::binary(BinOp::And, left, self.not()?);
            } else {
                return Ok(left);
            }
        }
    }

    fn not(&mut self) -> Result<Expr, String> {
        if self.eat_word("NOT")? {
            return Ok(Expr::Not(b(self.not()?)));
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Expr, String> {
        let mut left = self.predicate()?;
        loop {
            let op = match &self.tok {
                Tok::Sym("=") => BinOp::Eq,
                Tok::Sym("<=>") => BinOp::NullSafeEq,
                Tok::Sym("<>" | "!=") => BinOp::Ne,
                Tok::Sym("<") => BinOp::Lt,
                Tok::Sym("<=") => BinOp::Le,
                Tok::Sym(">") => BinOp::Gt,
                Tok::Sym(">=") => BinOp::Ge,
                Tok::Word(w) if w.eq_ignore_ascii_case("IS") => {
                    self.advance()?;
                    let not = self.eat_word("NOT")?;
                    let what = self.take_word()?.to_ascii_uppercase();
                    left = match what.as_str() {
                        "NULL" => Expr::IsNull(b(left), not),
                        "TRUE" => Expr::IsTruth(b(left), Truth::True, not),
                        "FALSE" => Expr::IsTruth(b(left), Truth::False, not),
                        "UNKNOWN" => Expr::IsTruth(b(left), Truth::Unknown, not),
                        other => return Err(format!("IS {other}?")),
                    };
                    continue;
                }
                _ => return Ok(left),
            };
            self.advance()?;
            left = Expr::binary(op, left, self.predicate()?);
        }
    }

    fn predicate(&mut self) -> Result<Expr, String> {
        let left = self.bit_or()?;
        let not = if self.word_is("NOT") {
            // Only NOT IN, NOT BETWEEN and NOT LIKE continue a predicate.
            let save = (self.lex.i, self.tok.clone());
            self.advance()?;
            if self.word_is("IN")
                || self.word_is("BETWEEN")
                || self.word_is("LIKE")
                || self.word_is("REGEXP")
                || self.word_is("RLIKE")
            {
                true
            } else {
                self.lex.i = save.0;
                self.tok = save.1;
                return Ok(left);
            }
        } else {
            false
        };
        if self.eat_word("IN")? {
            self.expect_sym("(")?;
            let list = self.list()?;
            return Ok(Expr::In(b(left), list, not));
        }
        if self.eat_word("BETWEEN")? {
            let lo = self.bit_or()?;
            self.expect_word("AND")?;
            let hi = self.predicate()?;
            return Ok(Expr::Between(b(left), b(lo), b(hi), not));
        }
        if self.eat_word("REGEXP")? || self.eat_word("RLIKE")? {
            let pattern = self.bit_or()?;
            let m = Expr::func("REGEXP", vec![left, pattern]);
            return Ok(if not { Expr::Not(b(m)) } else { m });
        }
        if self.eat_word("LIKE")? {
            let pattern = self.unary()?;
            let escape = if self.eat_word("ESCAPE")? {
                Some(b(self.unary()?))
            } else {
                None
            };
            return Ok(Expr::Like(b(left), b(pattern), escape, not));
        }
        Ok(left)
    }

    /// Expressions up to the closing parenthesis, which is consumed.
    fn list(&mut self) -> Result<Vec<Expr>, String> {
        let mut out = Vec::new();
        if self.eat_sym(")")? {
            return Ok(out);
        }
        loop {
            out.push(self.expr()?);
            if self.eat_sym(")")? {
                return Ok(out);
            }
            self.expect_sym(",")?;
        }
    }

    fn bit_or(&mut self) -> Result<Expr, String> {
        let mut left = self.bit_and()?;
        while self.eat_sym("|")? {
            left = Expr::binary(BinOp::BitOr, left, self.bit_and()?);
        }
        Ok(left)
    }

    fn bit_and(&mut self) -> Result<Expr, String> {
        let mut left = self.shift()?;
        while self.eat_sym("&")? {
            left = Expr::binary(BinOp::BitAnd, left, self.shift()?);
        }
        Ok(left)
    }

    fn shift(&mut self) -> Result<Expr, String> {
        let mut left = self.additive()?;
        loop {
            let op = match &self.tok {
                Tok::Sym("<<") => BinOp::Shl,
                Tok::Sym(">>") => BinOp::Shr,
                _ => return Ok(left),
            };
            self.advance()?;
            left = Expr::binary(op, left, self.additive()?);
        }
    }

    fn additive(&mut self) -> Result<Expr, String> {
        let mut left = self.multiplicative()?;
        loop {
            let op = match &self.tok {
                Tok::Sym("+") => BinOp::Add,
                Tok::Sym("-") => BinOp::Sub,
                _ => return Ok(left),
            };
            self.advance()?;
            left = Expr::binary(op, left, self.multiplicative()?);
        }
    }

    fn multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.bit_xor()?;
        loop {
            let op = match &self.tok {
                Tok::Sym("*") => BinOp::Mul,
                Tok::Sym("/") => BinOp::Div,
                Tok::Sym("%") => BinOp::Mod,
                Tok::Word(w) if w.eq_ignore_ascii_case("DIV") => BinOp::IntDiv,
                Tok::Word(w) if w.eq_ignore_ascii_case("MOD") => BinOp::Mod,
                _ => return Ok(left),
            };
            self.advance()?;
            left = Expr::binary(op, left, self.bit_xor()?);
        }
    }

    fn bit_xor(&mut self) -> Result<Expr, String> {
        let mut left = self.unary()?;
        while self.eat_sym("^")? {
            left = Expr::binary(BinOp::BitXor, left, self.unary()?);
        }
        Ok(left)
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.eat_sym("-")? {
            return Ok(Expr::Neg(b(self.unary()?)));
        }
        if self.eat_sym("+")? {
            return self.unary();
        }
        if self.eat_sym("~")? {
            return Ok(Expr::BitNot(b(self.unary()?)));
        }
        if self.eat_sym("!")? {
            return Ok(Expr::Not(b(self.unary()?)));
        }
        let mut e = self.primary()?;
        while self.eat_word("COLLATE")? {
            let name = self.take_word()?;
            let coll = Collation::resolve(&name.to_ascii_lowercase(), self.flavor)
                .ok_or_else(|| format!("unknown collation {name}"))?;
            e = Expr::Collate(b(e), coll);
        }
        Ok(e)
    }

    fn column(&self, name: &str) -> Option<Expr> {
        self.cols
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .map(|slot| Expr::column(slot, self.cols[slot].name, self.cols[slot].ty, true))
    }

    fn primary(&mut self) -> Result<Expr, String> {
        match std::mem::replace(&mut self.tok, Tok::End) {
            Tok::Int(t) => {
                self.advance()?;
                Ok(Expr::Int(t))
            }
            Tok::Dec(t) => {
                self.advance()?;
                Ok(Expr::Decimal(t))
            }
            Tok::Dbl(t) => {
                self.advance()?;
                Ok(Expr::Double(t))
            }
            Tok::Str(t) => {
                self.advance()?;
                Ok(Expr::Str(t))
            }
            Tok::Hex(h) => {
                self.advance()?;
                Ok(Expr::Hex(h))
            }
            Tok::Name(n) => {
                self.advance()?;
                self.column(&n).ok_or_else(|| format!("no column `{n}`"))
            }
            Tok::Sym("(") => {
                self.advance()?;
                let first = self.expr()?;
                if self.eat_sym(")")? {
                    return Ok(first);
                }
                // A row value: (a, b, ...).
                self.expect_sym(",")?;
                let mut items = vec![first];
                items.extend(self.list()?);
                Ok(Expr::Func("ROW".to_owned(), items))
            }
            Tok::Word(w) => {
                self.advance()?;
                self.word(&w)
            }
            other => Err(format!("unexpected {other:?}")),
        }
    }

    fn word(&mut self, w: &str) -> Result<Expr, String> {
        let upper = w.to_ascii_uppercase();
        // A character set introducer: _latin1'...'.
        if let Some(cs) = upper.strip_prefix('_') {
            if matches!(self.tok, Tok::Str(_) | Tok::Hex(_)) {
                let lit = self.primary()?;
                return Ok(Expr::Func(
                    "_CHARSET".to_owned(),
                    vec![Expr::Keyword(cs.to_ascii_lowercase()), lit],
                ));
            }
        }
        match upper.as_str() {
            "NULL" => return Ok(Expr::Null),
            "TRUE" => return Ok(Expr::Bool(true)),
            "FALSE" => return Ok(Expr::Bool(false)),
            "CASE" => return self.case(),
            "CAST" => {
                self.expect_sym("(")?;
                let e = self.expr()?;
                self.expect_word("AS")?;
                let t = self.cast_target()?;
                self.expect_sym(")")?;
                return Ok(Expr::Cast(b(e), t));
            }
            "CONVERT" => {
                self.expect_sym("(")?;
                let e = self.expr()?;
                if self.eat_word("USING")? {
                    let cs = self.take_word()?.to_ascii_lowercase();
                    self.expect_sym(")")?;
                    return Ok(Expr::Func(
                        "CONVERT_USING".to_owned(),
                        vec![e, Expr::Keyword(cs)],
                    ));
                }
                self.expect_sym(",")?;
                let t = self.cast_target()?;
                self.expect_sym(")")?;
                return Ok(Expr::Cast(b(e), t));
            }
            "TRIM" => return self.trim(),
            "EXTRACT" => {
                self.expect_sym("(")?;
                let unit = self.take_word()?.to_ascii_uppercase();
                self.expect_word("FROM")?;
                let e = self.expr()?;
                self.expect_sym(")")?;
                return Ok(Expr::Func(
                    "EXTRACT".to_owned(),
                    vec![Expr::Keyword(unit), e],
                ));
            }
            "TIMESTAMPDIFF" | "TIMESTAMPADD" => {
                self.expect_sym("(")?;
                let unit = self.take_word()?.to_ascii_uppercase();
                self.expect_sym(",")?;
                let mut args = vec![Expr::Keyword(unit)];
                args.extend(self.list()?);
                return Ok(Expr::Func(upper, args));
            }
            "DATE_ADD" | "DATE_SUB" | "ADDDATE" | "SUBDATE" => {
                self.expect_sym("(")?;
                let d = self.expr()?;
                self.expect_sym(",")?;
                if self.word_is("INTERVAL") {
                    self.advance()?;
                    let n = self.expr()?;
                    let unit = self.take_word()?.to_ascii_uppercase();
                    self.expect_sym(")")?;
                    return Ok(Expr::Func(upper, vec![d, n, Expr::Keyword(unit)]));
                }
                let n = self.expr()?;
                self.expect_sym(")")?;
                return Ok(Expr::Func(upper, vec![d, n]));
            }
            "CHAR" if matches!(self.tok, Tok::Sym("(")) => {
                self.advance()?;
                let mut args = Vec::new();
                loop {
                    args.push(self.expr()?);
                    if self.eat_word("USING")? {
                        let cs = self.take_word()?.to_ascii_lowercase();
                        args.push(Expr::Keyword(format!("USING {cs}")));
                        self.expect_sym(")")?;
                        break;
                    }
                    if self.eat_sym(")")? {
                        break;
                    }
                    self.expect_sym(",")?;
                }
                return Ok(Expr::Func(upper, args));
            }
            _ => {}
        }
        if self.eat_sym("(")? {
            let args = self.list()?;
            return Ok(Expr::Func(upper, args));
        }
        if let Some(c) = self.column(w) {
            return Ok(c);
        }
        if NILADIC.contains(&upper.as_str()) {
            return Ok(Expr::Func(upper, Vec::new()));
        }
        Err(format!("unknown name {w}"))
    }

    fn case(&mut self) -> Result<Expr, String> {
        let operand = if self.word_is("WHEN") {
            None
        } else {
            Some(b(self.expr()?))
        };
        let mut whens = Vec::new();
        while self.eat_word("WHEN")? {
            let w = self.expr()?;
            self.expect_word("THEN")?;
            whens.push((w, self.expr()?));
        }
        let otherwise = if self.eat_word("ELSE")? {
            Some(b(self.expr()?))
        } else {
            None
        };
        self.expect_word("END")?;
        Ok(Expr::Case {
            operand,
            whens,
            otherwise,
        })
    }

    fn trim(&mut self) -> Result<Expr, String> {
        self.expect_sym("(")?;
        let side = ["BOTH", "LEADING", "TRAILING"]
            .into_iter()
            .find(|s| self.word_is(s));
        if side.is_some() {
            self.advance()?;
        }
        if side.is_some() && self.eat_word("FROM")? {
            // TRIM(LEADING FROM s): spaces.
            let s = self.expr()?;
            self.expect_sym(")")?;
            let name = format!("TRIM_{}", side.unwrap_or("BOTH"));
            return Ok(Expr::Func(name, vec![Expr::str(" "), s]));
        }
        let first = self.expr()?;
        if self.eat_word("FROM")? {
            let s = self.expr()?;
            self.expect_sym(")")?;
            let name = format!("TRIM_{}", side.unwrap_or("BOTH"));
            return Ok(Expr::Func(name, vec![first, s]));
        }
        self.expect_sym(")")?;
        Ok(Expr::Func("TRIM".to_owned(), vec![first]))
    }

    fn opt_number(&mut self) -> Result<Option<(u32, Option<u32>)>, String> {
        if !self.eat_sym("(")? {
            return Ok(None);
        }
        let read = |p: &mut Self| -> Result<u32, String> {
            match std::mem::replace(&mut p.tok, Tok::End) {
                Tok::Int(t) => {
                    p.advance()?;
                    t.parse().map_err(|_| format!("bad number {t}"))
                }
                other => Err(format!("expected a number, found {other:?}")),
            }
        };
        let first = read(self)?;
        let second = if self.eat_sym(",")? {
            Some(read(self)?)
        } else {
            None
        };
        self.expect_sym(")")?;
        Ok(Some((first, second)))
    }

    fn cast_target(&mut self) -> Result<CastTarget, String> {
        let name = self.take_word()?.to_ascii_uppercase();
        Ok(match name.as_str() {
            "SIGNED" => {
                let _ = self.eat_word("INTEGER")? || self.eat_word("INT")?;
                CastTarget::Signed
            }
            "UNSIGNED" => {
                let _ = self.eat_word("INTEGER")? || self.eat_word("INT")?;
                CastTarget::Unsigned
            }
            "DECIMAL" => match self.opt_number()? {
                None => CastTarget::Decimal(10, 0),
                Some((p, s)) => CastTarget::Decimal(p as u8, s.unwrap_or(0) as u8),
            },
            "DOUBLE" => CastTarget::Double,
            "CHAR" => CastTarget::Char(self.opt_number()?.map(|(n, _)| n)),
            "BINARY" => CastTarget::Binary(self.opt_number()?.map(|(n, _)| n)),
            "DATE" => CastTarget::Date,
            "TIME" => CastTarget::Time(self.opt_number()?.map_or(0, |(n, _)| n as u8)),
            "DATETIME" => CastTarget::DateTime(self.opt_number()?.map_or(0, |(n, _)| n as u8)),
            other => return Err(format!("CAST AS {other}?")),
        })
    }
}
