//! Lua 5.1's grammar: a lexer and a recursive-descent parser giving the
//! syntax tree the Lua front end lowers. It follows the reference
//! implementation's `llex.c` and `lparser.c` (Lua 5.1.5): the same
//! tokens, escapes, long brackets, operator priorities, and the rule that
//! `return` and `break` end their block. Text is UTF-8: a string literal
//! whose escapes make bytes that aren't UTF-8 is refused (the engine tier
//! still runs it).

use std::rc::Rc;

/// Where something is in the source: line and column, from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pos {
    /// Line, from 1.
    pub line: u32,
    /// Column in bytes, from 1.
    pub col: u32,
}

impl Pos {
    /// A total order key (for widening and inlining keys).
    #[must_use]
    pub fn key(self) -> u32 {
        self.line
            .saturating_mul(4096)
            .saturating_add(self.col.min(4095))
    }
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^`
    Pow,
    /// `..`
    Concat,
    /// `==`
    Eq,
    /// `~=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `and`
    And,
    /// `or`
    Or,
}

impl BinOp {
    /// Left and right priorities (`lparser.c`'s `priority` table).
    const fn priority(self) -> (u8, u8) {
        match self {
            BinOp::Add | BinOp::Sub => (6, 6),
            BinOp::Mul | BinOp::Div | BinOp::Mod => (7, 7),
            BinOp::Pow => (10, 9),
            BinOp::Concat => (5, 4),
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => (3, 3),
            BinOp::And => (2, 2),
            BinOp::Or => (1, 1),
        }
    }

    /// The operator as written.
    #[must_use]
    pub const fn text(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Pow => "^",
            BinOp::Concat => "..",
            BinOp::Eq => "==",
            BinOp::Ne => "~=",
            BinOp::Lt => "<",
            BinOp::Le => "<=",
            BinOp::Gt => ">",
            BinOp::Ge => ">=",
            BinOp::And => "and",
            BinOp::Or => "or",
        }
    }
}

/// Unary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    /// `-`
    Neg,
    /// `not`
    Not,
    /// `#`
    Len,
}

/// The priority of unary operators.
const UNARY_PRIORITY: u8 = 8;

/// An expression and where it starts.
#[derive(Debug, Clone, PartialEq)]
pub struct Expr {
    /// Where.
    pub pos: Pos,
    /// What.
    pub k: ExprKind,
}

/// The kinds of expression.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprKind {
    /// `nil`
    Nil,
    /// `true`
    True,
    /// `false`
    False,
    /// A number (every Lua 5.1 number is a double).
    Num(f64),
    /// A string.
    Str(String),
    /// `...`
    Vararg,
    /// `function (...) ... end`
    Function(Rc<Func>),
    /// A name.
    Name(String),
    /// `a.b`
    Field(Box<Expr>, String),
    /// `a[b]`
    Index(Box<Expr>, Box<Expr>),
    /// `f(args)`, `f{...}`, `f'...'`
    Call(Box<Expr>, Vec<Expr>),
    /// `o:m(args)`
    Method(Box<Expr>, String, Vec<Expr>),
    /// `{ ... }`
    Table(Vec<TableField>),
    /// A binary operator.
    Bin(BinOp, Box<Expr>, Box<Expr>),
    /// A unary operator.
    Un(UnOp, Box<Expr>),
    /// `(e)`: one value of `e`.
    Paren(Box<Expr>),
}

/// A field of a table constructor.
#[derive(Debug, Clone, PartialEq)]
pub enum TableField {
    /// `name = e`
    Named(String, Expr),
    /// `[k] = e`
    Keyed(Expr, Expr),
    /// `e` (the next array element)
    Item(Expr),
}

/// A function's parameters and body.
#[derive(Debug, Clone, PartialEq)]
pub struct Func {
    /// Where `function` is.
    pub pos: Pos,
    /// Parameter names.
    pub params: Vec<String>,
    /// Whether it ends with `...`.
    pub vararg: bool,
    /// The body.
    pub body: Block,
}

/// A statement and where it starts.
#[derive(Debug, Clone, PartialEq)]
pub struct Stat {
    /// Where.
    pub pos: Pos,
    /// What.
    pub k: StatKind,
}

/// A block of statements.
pub type Block = Vec<Stat>;

/// The kinds of statement.
#[derive(Debug, Clone, PartialEq)]
pub enum StatKind {
    /// `a, b.c, d[e] = x, y`
    Assign(Vec<Expr>, Vec<Expr>),
    /// A call as a statement.
    Call(Expr),
    /// `do ... end`
    Do(Block),
    /// `while e do ... end`
    While(Expr, Block),
    /// `repeat ... until e`
    Repeat(Block, Expr),
    /// `if e then ... elseif e then ... else ... end`
    If(Vec<(Expr, Block)>, Option<Block>),
    /// `for v = a, b [, c] do ... end`
    NumFor {
        /// The loop's variable.
        var: String,
        /// Start.
        start: Expr,
        /// Limit.
        limit: Expr,
        /// Step.
        step: Option<Expr>,
        /// Body.
        body: Block,
    },
    /// `for a, b in e do ... end`
    GenFor {
        /// The loop's variables.
        names: Vec<String>,
        /// The iterator expressions.
        exprs: Vec<Expr>,
        /// Body.
        body: Block,
    },
    /// `function a.b:c (...) ... end`: a global (or field) assignment.
    Function {
        /// `a`, `b`
        path: Vec<String>,
        /// `c`
        method: Option<String>,
        /// The function.
        func: Rc<Func>,
    },
    /// `local function f (...) ... end`
    LocalFunction(String, Rc<Func>),
    /// `local a, b = x, y`
    Local(Vec<String>, Vec<Expr>),
    /// `return e, ...`
    Return(Vec<Expr>),
    /// `break`
    Break,
}

// ---------------------------------------------------------------------------
// The lexer.

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Name(String),
    Num(f64),
    Str(String),
    Kw(&'static str),
    Op(&'static str),
    Eof,
}

const KEYWORDS: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "if", "in", "local",
    "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];

const OPS: &[&str] = &[
    "...", "..", "==", "~=", "<=", ">=", "+", "-", "*", "/", "%", "^", "#", "<", ">", "=", "(",
    ")", "{", "}", "[", "]", ";", ":", ",", ".",
];

struct Lexer<'a> {
    s: &'a [u8],
    i: usize,
    line: u32,
    line_start: usize,
}

type PResult<T> = std::result::Result<T, String>;

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            s: src.as_bytes(),
            i: 0,
            line: 1,
            line_start: 0,
        }
    }

    fn pos(&self) -> Pos {
        Pos {
            line: self.line,
            col: u32::try_from(self.i - self.line_start + 1).unwrap_or(u32::MAX),
        }
    }

    fn peek(&self, k: usize) -> u8 {
        self.s.get(self.i + k).copied().unwrap_or(0)
    }

    fn at_end(&self) -> bool {
        self.i >= self.s.len()
    }

    fn err<T>(&self, msg: impl Into<String>) -> PResult<T> {
        Err(format!("line {}: {}", self.line, msg.into()))
    }

    /// Skips a newline (`\n`, `\r`, `\n\r` or `\r\n`, as `llex.c` does).
    fn newline(&mut self) {
        let c = self.peek(0);
        self.i += 1;
        let d = self.peek(0);
        if (d == b'\n' || d == b'\r') && d != c {
            self.i += 1;
        }
        self.line += 1;
        self.line_start = self.i;
    }

    /// The level of a long bracket at `[` (`[[` is 0, `[==[` is 2), or
    /// `None` when it isn't one.
    fn long_level(&self) -> Option<usize> {
        let mut k = 1;
        while self.peek(k) == b'=' {
            k += 1;
        }
        (self.peek(k) == b'[').then_some(k - 1)
    }

    fn long_text(&mut self, level: usize, what: &str) -> PResult<String> {
        // Past `[`, the `=`s and `[`.
        self.i += level + 2;
        if matches!(self.peek(0), b'\n' | b'\r') {
            self.newline();
        }
        let mut out = Vec::new();
        loop {
            if self.at_end() {
                return self.err(format!("unfinished long {what}"));
            }
            let c = self.peek(0);
            if c == b']' {
                let mut k = 1;
                while self.peek(k) == b'=' {
                    k += 1;
                }
                if k - 1 == level && self.peek(k) == b']' {
                    self.i += k + 1;
                    break;
                }
                out.push(c);
                self.i += 1;
            } else if c == b'\n' || c == b'\r' {
                out.push(b'\n');
                self.newline();
            } else {
                out.push(c);
                self.i += 1;
            }
        }
        String::from_utf8(out).or_else(|_| self.err("a long string that isn't UTF-8"))
    }

    fn skip_space(&mut self) -> PResult<()> {
        loop {
            match self.peek(0) {
                b'\n' | b'\r' => self.newline(),
                b' ' | b'\t' | 0x0b | 0x0c => self.i += 1,
                b'-' if self.peek(1) == b'-' => {
                    self.i += 2;
                    if self.peek(0) == b'['
                        && let Some(level) = self.long_level()
                    {
                        self.long_text(level, "comment")?;
                        continue;
                    }
                    while !self.at_end() && !matches!(self.peek(0), b'\n' | b'\r') {
                        self.i += 1;
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn string(&mut self, q: u8) -> PResult<String> {
        self.i += 1;
        let mut out = Vec::new();
        loop {
            if self.at_end() {
                return self.err("unfinished string");
            }
            let c = self.peek(0);
            if c == q {
                self.i += 1;
                break;
            }
            match c {
                b'\n' | b'\r' => return self.err("unfinished string"),
                b'\\' => {
                    self.i += 1;
                    let e = self.peek(0);
                    match e {
                        b'a' => out.push(7),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'v' => out.push(11),
                        b'\n' | b'\r' => {
                            out.push(b'\n');
                            self.newline();
                            continue;
                        }
                        _ if self.at_end() => return self.err("unfinished string"),
                        b'0'..=b'9' => {
                            let mut v: u32 = 0;
                            let mut n = 0;
                            while n < 3 && self.peek(0).is_ascii_digit() {
                                v = v * 10 + u32::from(self.peek(0) - b'0');
                                self.i += 1;
                                n += 1;
                            }
                            if v > 255 {
                                return self.err("escape sequence too large");
                            }
                            out.push(v as u8);
                            continue;
                        }
                        // Any other character stands for itself (Lua 5.1).
                        other => out.push(other),
                    }
                    self.i += 1;
                }
                _ => {
                    out.push(c);
                    self.i += 1;
                }
            }
        }
        String::from_utf8(out).or_else(|_| self.err("a string that isn't UTF-8"))
    }

    fn number(&mut self) -> PResult<f64> {
        let start = self.i;
        // `llex.c`: digits and dots, an exponent with its sign, then any
        // further alphanumerics; `strtod` (and `strtoul` for hex) decides.
        while self.peek(0).is_ascii_digit() || self.peek(0) == b'.' {
            self.i += 1;
        }
        if matches!(self.peek(0), b'e' | b'E') {
            self.i += 1;
            if matches!(self.peek(0), b'+' | b'-') {
                self.i += 1;
            }
        }
        while self.peek(0).is_ascii_alphanumeric() || self.peek(0) == b'_' {
            self.i += 1;
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
        if let Some(h) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
            return u64::from_str_radix(h, 16)
                .map(|v| v as f64)
                .or_else(|_| self.err(format!("malformed number near '{text}'")));
        }
        text.parse::<f64>()
            .ok()
            .filter(|x| x.is_finite() || text.contains(['e', 'E']))
            .map_or_else(|| self.err(format!("malformed number near '{text}'")), Ok)
    }

    fn next(&mut self) -> PResult<(Pos, Tok)> {
        self.skip_space()?;
        let pos = self.pos();
        if self.at_end() {
            return Ok((pos, Tok::Eof));
        }
        let c = self.peek(0);
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = self.i;
            while self.peek(0).is_ascii_alphanumeric() || self.peek(0) == b'_' {
                self.i += 1;
            }
            let w = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
            if let Some(k) = KEYWORDS.iter().find(|k| **k == w) {
                return Ok((pos, Tok::Kw(k)));
            }
            return Ok((pos, Tok::Name(w.to_owned())));
        }
        if c.is_ascii_digit() || (c == b'.' && self.peek(1).is_ascii_digit()) {
            return Ok((pos, Tok::Num(self.number()?)));
        }
        if c == b'"' || c == b'\'' {
            return Ok((pos, Tok::Str(self.string(c)?)));
        }
        if c == b'['
            && let Some(level) = self.long_level()
        {
            return Ok((pos, Tok::Str(self.long_text(level, "string")?)));
        }
        for op in OPS {
            if self.s[self.i..].starts_with(op.as_bytes()) {
                self.i += op.len();
                return Ok((pos, Tok::Op(op)));
            }
        }
        self.err(format!("unexpected symbol near '{}'", char::from(c)))
    }
}

// ---------------------------------------------------------------------------
// The parser.

struct Parser<'a> {
    lex: Lexer<'a>,
    tok: Tok,
    pos: Pos,
    ahead: Option<(Pos, Tok)>,
}

/// Parses a chunk.
///
/// # Errors
/// Lua's syntax error, with its line.
pub fn parse(src: &str) -> PResult<Block> {
    let mut lex = Lexer::new(src);
    let (pos, tok) = lex.next()?;
    let mut p = Parser {
        lex,
        tok,
        pos,
        ahead: None,
    };
    let b = p.block()?;
    if p.tok != Tok::Eof {
        return p.err("'<eof>' expected");
    }
    Ok(b)
}

impl Parser<'_> {
    fn err<T>(&self, msg: impl Into<String>) -> PResult<T> {
        Err(format!(
            "line {}: {} near {}",
            self.pos.line,
            msg.into(),
            self.near()
        ))
    }

    fn near(&self) -> String {
        match &self.tok {
            Tok::Name(n) => format!("'{n}'"),
            Tok::Num(x) => format!("'{x}'"),
            Tok::Str(s) => format!("'{s}'"),
            Tok::Kw(k) | Tok::Op(k) => format!("'{k}'"),
            Tok::Eof => "'<eof>'".to_owned(),
        }
    }

    fn advance(&mut self) -> PResult<()> {
        let (p, t) = match self.ahead.take() {
            Some(x) => x,
            None => self.lex.next()?,
        };
        self.pos = p;
        self.tok = t;
        Ok(())
    }

    /// Whether the token after the current one is `=`.
    fn next_is_assign(&mut self) -> PResult<bool> {
        if self.ahead.is_none() {
            self.ahead = Some(self.lex.next()?);
        }
        Ok(matches!(&self.ahead, Some((_, Tok::Op("=")))))
    }

    fn is_op(&self, op: &str) -> bool {
        matches!(&self.tok, Tok::Op(o) if *o == op)
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(&self.tok, Tok::Kw(k) if *k == kw)
    }

    fn eat_op(&mut self, op: &str) -> PResult<bool> {
        if self.is_op(op) {
            self.advance()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn expect_op(&mut self, op: &str) -> PResult<()> {
        if self.eat_op(op)? {
            Ok(())
        } else {
            self.err(format!("'{op}' expected"))
        }
    }

    fn expect_kw(&mut self, kw: &str) -> PResult<()> {
        if self.is_kw(kw) {
            self.advance()
        } else {
            self.err(format!("'{kw}' expected"))
        }
    }

    fn name(&mut self) -> PResult<String> {
        if let Tok::Name(n) = &self.tok {
            let n = n.clone();
            self.advance()?;
            Ok(n)
        } else {
            self.err("<name> expected")
        }
    }

    fn block_follow(&self) -> bool {
        matches!(self.tok, Tok::Eof)
            || matches!(&self.tok, Tok::Kw(k) if matches!(*k, "else" | "elseif" | "end" | "until"))
    }

    fn block(&mut self) -> PResult<Block> {
        let mut out = Vec::new();
        while !self.block_follow() {
            let last = self.is_kw("return") || self.is_kw("break");
            let s = self.statement()?;
            out.push(s);
            self.eat_op(";")?;
            if last {
                break;
            }
        }
        Ok(out)
    }

    fn statement(&mut self) -> PResult<Stat> {
        let pos = self.pos;
        let k = match &self.tok {
            Tok::Kw("if") => {
                self.advance()?;
                let mut arms = Vec::new();
                let c = self.expr()?;
                self.expect_kw("then")?;
                arms.push((c, self.block()?));
                let mut other = None;
                loop {
                    if self.is_kw("elseif") {
                        self.advance()?;
                        let c = self.expr()?;
                        self.expect_kw("then")?;
                        arms.push((c, self.block()?));
                    } else if self.is_kw("else") {
                        self.advance()?;
                        other = Some(self.block()?);
                        self.expect_kw("end")?;
                        break;
                    } else {
                        self.expect_kw("end")?;
                        break;
                    }
                }
                StatKind::If(arms, other)
            }
            Tok::Kw("while") => {
                self.advance()?;
                let c = self.expr()?;
                self.expect_kw("do")?;
                let b = self.block()?;
                self.expect_kw("end")?;
                StatKind::While(c, b)
            }
            Tok::Kw("do") => {
                self.advance()?;
                let b = self.block()?;
                self.expect_kw("end")?;
                StatKind::Do(b)
            }
            Tok::Kw("for") => {
                self.advance()?;
                let first = self.name()?;
                if self.eat_op("=")? {
                    let start = self.expr()?;
                    self.expect_op(",")?;
                    let limit = self.expr()?;
                    let step = if self.eat_op(",")? {
                        Some(self.expr()?)
                    } else {
                        None
                    };
                    self.expect_kw("do")?;
                    let body = self.block()?;
                    self.expect_kw("end")?;
                    StatKind::NumFor {
                        var: first,
                        start,
                        limit,
                        step,
                        body,
                    }
                } else if self.is_op(",") || self.is_kw("in") {
                    let mut names = vec![first];
                    while self.eat_op(",")? {
                        names.push(self.name()?);
                    }
                    self.expect_kw("in")?;
                    let exprs = self.expr_list()?;
                    self.expect_kw("do")?;
                    let body = self.block()?;
                    self.expect_kw("end")?;
                    StatKind::GenFor { names, exprs, body }
                } else {
                    return self.err("'=' or 'in' expected");
                }
            }
            Tok::Kw("repeat") => {
                self.advance()?;
                let b = self.block()?;
                self.expect_kw("until")?;
                let c = self.expr()?;
                StatKind::Repeat(b, c)
            }
            Tok::Kw("function") => {
                self.advance()?;
                let mut path = vec![self.name()?];
                while self.eat_op(".")? {
                    path.push(self.name()?);
                }
                let method = if self.eat_op(":")? {
                    Some(self.name()?)
                } else {
                    None
                };
                let func = self.func_body(pos, method.is_some())?;
                StatKind::Function { path, method, func }
            }
            Tok::Kw("local") => {
                self.advance()?;
                if self.is_kw("function") {
                    let fpos = self.pos;
                    self.advance()?;
                    let n = self.name()?;
                    let f = self.func_body(fpos, false)?;
                    StatKind::LocalFunction(n, f)
                } else {
                    let mut names = vec![self.name()?];
                    while self.eat_op(",")? {
                        names.push(self.name()?);
                    }
                    let vals = if self.eat_op("=")? {
                        self.expr_list()?
                    } else {
                        Vec::new()
                    };
                    StatKind::Local(names, vals)
                }
            }
            Tok::Kw("return") => {
                self.advance()?;
                let vals = if self.block_follow() || self.is_op(";") {
                    Vec::new()
                } else {
                    self.expr_list()?
                };
                StatKind::Return(vals)
            }
            Tok::Kw("break") => {
                self.advance()?;
                StatKind::Break
            }
            _ => {
                let e = self.suffixed()?;
                if self.is_op("=") || self.is_op(",") {
                    let mut targets = vec![e];
                    while self.eat_op(",")? {
                        targets.push(self.suffixed()?);
                    }
                    self.expect_op("=")?;
                    for t in &targets {
                        if !matches!(
                            t.k,
                            ExprKind::Name(_) | ExprKind::Field(..) | ExprKind::Index(..)
                        ) {
                            return self.err("syntax error");
                        }
                    }
                    let vals = self.expr_list()?;
                    StatKind::Assign(targets, vals)
                } else {
                    if !matches!(e.k, ExprKind::Call(..) | ExprKind::Method(..)) {
                        return self.err("syntax error");
                    }
                    StatKind::Call(e)
                }
            }
        };
        Ok(Stat { pos, k })
    }

    fn func_body(&mut self, pos: Pos, method: bool) -> PResult<Rc<Func>> {
        self.expect_op("(")?;
        let mut params = Vec::new();
        if method {
            params.push("self".to_owned());
        }
        let mut vararg = false;
        if !self.is_op(")") {
            loop {
                if self.eat_op("...")? {
                    vararg = true;
                    break;
                }
                params.push(self.name()?);
                if !self.eat_op(",")? {
                    break;
                }
            }
        }
        self.expect_op(")")?;
        let body = self.block()?;
        self.expect_kw("end")?;
        Ok(Rc::new(Func {
            pos,
            params,
            vararg,
            body,
        }))
    }

    fn expr_list(&mut self) -> PResult<Vec<Expr>> {
        let mut out = vec![self.expr()?];
        while self.eat_op(",")? {
            out.push(self.expr()?);
        }
        Ok(out)
    }

    fn expr(&mut self) -> PResult<Expr> {
        self.sub_expr(0)
    }

    fn bin_op(&self) -> Option<BinOp> {
        Some(match &self.tok {
            Tok::Op(o) => match *o {
                "+" => BinOp::Add,
                "-" => BinOp::Sub,
                "*" => BinOp::Mul,
                "/" => BinOp::Div,
                "%" => BinOp::Mod,
                "^" => BinOp::Pow,
                ".." => BinOp::Concat,
                "==" => BinOp::Eq,
                "~=" => BinOp::Ne,
                "<" => BinOp::Lt,
                "<=" => BinOp::Le,
                ">" => BinOp::Gt,
                ">=" => BinOp::Ge,
                _ => return None,
            },
            Tok::Kw("and") => BinOp::And,
            Tok::Kw("or") => BinOp::Or,
            _ => return None,
        })
    }

    fn sub_expr(&mut self, limit: u8) -> PResult<Expr> {
        let pos = self.pos;
        let un = match &self.tok {
            Tok::Kw("not") => Some(UnOp::Not),
            Tok::Op("-") => Some(UnOp::Neg),
            Tok::Op("#") => Some(UnOp::Len),
            _ => None,
        };
        let mut left = if let Some(op) = un {
            self.advance()?;
            let e = self.sub_expr(UNARY_PRIORITY)?;
            Expr {
                pos,
                k: ExprKind::Un(op, Box::new(e)),
            }
        } else {
            self.simple()?
        };
        while let Some(op) = self.bin_op() {
            let (l, r) = op.priority();
            if l <= limit {
                break;
            }
            let opos = self.pos;
            self.advance()?;
            let right = self.sub_expr(r)?;
            left = Expr {
                pos: opos,
                k: ExprKind::Bin(op, Box::new(left), Box::new(right)),
            };
        }
        Ok(left)
    }

    fn simple(&mut self) -> PResult<Expr> {
        let pos = self.pos;
        let k = match &self.tok {
            Tok::Num(x) => ExprKind::Num(*x),
            Tok::Str(s) => ExprKind::Str(s.clone()),
            Tok::Kw("nil") => ExprKind::Nil,
            Tok::Kw("true") => ExprKind::True,
            Tok::Kw("false") => ExprKind::False,
            Tok::Op("...") => ExprKind::Vararg,
            Tok::Op("{") => return self.table(),
            Tok::Kw("function") => {
                self.advance()?;
                let f = self.func_body(pos, false)?;
                return Ok(Expr {
                    pos,
                    k: ExprKind::Function(f),
                });
            }
            _ => return self.suffixed(),
        };
        self.advance()?;
        Ok(Expr { pos, k })
    }

    fn primary(&mut self) -> PResult<Expr> {
        let pos = self.pos;
        match &self.tok {
            Tok::Name(n) => {
                let n = n.clone();
                self.advance()?;
                Ok(Expr {
                    pos,
                    k: ExprKind::Name(n),
                })
            }
            Tok::Op("(") => {
                self.advance()?;
                let e = self.expr()?;
                self.expect_op(")")?;
                Ok(Expr {
                    pos,
                    k: ExprKind::Paren(Box::new(e)),
                })
            }
            _ => self.err("unexpected symbol"),
        }
    }

    fn suffixed(&mut self) -> PResult<Expr> {
        let mut e = self.primary()?;
        loop {
            let pos = self.pos;
            match &self.tok {
                Tok::Op(".") => {
                    self.advance()?;
                    let n = self.name()?;
                    e = Expr {
                        pos: e.pos,
                        k: ExprKind::Field(Box::new(e), n),
                    };
                }
                Tok::Op("[") => {
                    self.advance()?;
                    let k = self.expr()?;
                    self.expect_op("]")?;
                    e = Expr {
                        pos: e.pos,
                        k: ExprKind::Index(Box::new(e), Box::new(k)),
                    };
                }
                Tok::Op(":") => {
                    self.advance()?;
                    let n = self.name()?;
                    let args = self.args()?;
                    e = Expr {
                        pos,
                        k: ExprKind::Method(Box::new(e), n, args),
                    };
                }
                Tok::Op("(" | "{") | Tok::Str(_) => {
                    let args = self.args()?;
                    e = Expr {
                        pos: e.pos,
                        k: ExprKind::Call(Box::new(e), args),
                    };
                }
                _ => return Ok(e),
            }
        }
    }

    fn args(&mut self) -> PResult<Vec<Expr>> {
        match &self.tok {
            Tok::Str(s) => {
                let e = Expr {
                    pos: self.pos,
                    k: ExprKind::Str(s.clone()),
                };
                self.advance()?;
                Ok(vec![e])
            }
            Tok::Op("{") => Ok(vec![self.table()?]),
            Tok::Op("(") => {
                self.advance()?;
                if self.eat_op(")")? {
                    return Ok(Vec::new());
                }
                let a = self.expr_list()?;
                self.expect_op(")")?;
                Ok(a)
            }
            _ => self.err("function arguments expected"),
        }
    }

    fn table(&mut self) -> PResult<Expr> {
        let pos = self.pos;
        self.expect_op("{")?;
        let mut fields = Vec::new();
        while !self.is_op("}") {
            let named = match &self.tok {
                Tok::Name(n) => Some(n.clone()),
                _ => None,
            };
            let named = match named {
                Some(n) if self.next_is_assign()? => Some(n),
                _ => None,
            };
            let f = match &self.tok {
                Tok::Name(_) if named.is_some() => {
                    let n = named.unwrap_or_default();
                    self.advance()?;
                    self.advance()?;
                    TableField::Named(n, self.expr()?)
                }
                Tok::Op("[") => {
                    self.advance()?;
                    let k = self.expr()?;
                    self.expect_op("]")?;
                    self.expect_op("=")?;
                    TableField::Keyed(k, self.expr()?)
                }
                _ => TableField::Item(self.expr()?),
            };
            fields.push(f);
            if !(self.eat_op(",")? || self.eat_op(";")?) {
                break;
            }
        }
        self.expect_op("}")?;
        Ok(Expr {
            pos,
            k: ExprKind::Table(fields),
        })
    }
}

/// Calls `f` on every statement of `b`, nested blocks and function
/// bodies included.
pub fn each_stat(b: &[Stat], f: &mut dyn FnMut(&Stat)) {
    fn func_expr(e: &Expr, f: &mut dyn FnMut(&Stat)) {
        each_expr(e, &mut |x| {
            if let ExprKind::Function(func) = &x.k {
                each_stat(&func.body, f);
            }
        });
    }
    for s in b {
        f(s);
        match &s.k {
            StatKind::Assign(t, v) => {
                t.iter().chain(v).for_each(|e| func_expr(e, f));
            }
            StatKind::Call(e) => func_expr(e, f),
            StatKind::Do(b) => each_stat(b, f),
            StatKind::While(c, b) | StatKind::Repeat(b, c) => {
                func_expr(c, f);
                each_stat(b, f);
            }
            StatKind::If(arms, other) => {
                for (c, b) in arms {
                    func_expr(c, f);
                    each_stat(b, f);
                }
                if let Some(b) = other {
                    each_stat(b, f);
                }
            }
            StatKind::NumFor {
                start,
                limit,
                step,
                body,
                ..
            } => {
                func_expr(start, f);
                func_expr(limit, f);
                if let Some(s) = step {
                    func_expr(s, f);
                }
                each_stat(body, f);
            }
            StatKind::GenFor { exprs, body, .. } => {
                for e in exprs {
                    func_expr(e, f);
                }
                each_stat(body, f);
            }
            StatKind::Function { func, .. } | StatKind::LocalFunction(_, func) => {
                each_stat(&func.body, f);
            }
            StatKind::Local(_, v) | StatKind::Return(v) => v.iter().for_each(|e| func_expr(e, f)),
            StatKind::Break => {}
        }
    }
}

/// Calls `f` on `e` and every expression inside it (not inside nested
/// function bodies).
pub fn each_expr(e: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(e);
    match &e.k {
        ExprKind::Field(a, _) | ExprKind::Un(_, a) | ExprKind::Paren(a) => each_expr(a, f),
        ExprKind::Index(a, b) | ExprKind::Bin(_, a, b) => {
            each_expr(a, f);
            each_expr(b, f);
        }
        ExprKind::Call(a, args) | ExprKind::Method(a, _, args) => {
            each_expr(a, f);
            for x in args {
                each_expr(x, f);
            }
        }
        ExprKind::Table(fields) => {
            for fl in fields {
                match fl {
                    TableField::Named(_, v) | TableField::Item(v) => each_expr(v, f),
                    TableField::Keyed(k, v) => {
                        each_expr(k, f);
                        each_expr(v, f);
                    }
                }
            }
        }
        ExprKind::Nil
        | ExprKind::True
        | ExprKind::False
        | ExprKind::Num(_)
        | ExprKind::Str(_)
        | ExprKind::Vararg
        | ExprKind::Function(_)
        | ExprKind::Name(_) => {}
    }
}

/// The names `b` assigns with `=` anywhere (nested blocks and functions
/// included).
#[must_use]
pub fn assigned_names(b: &[Stat]) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    each_stat(b, &mut |s| {
        if let StatKind::Assign(targets, _) = &s.k {
            for t in targets {
                if let ExprKind::Name(n) = &t.k {
                    out.insert(n.clone());
                }
            }
        }
        if let StatKind::Function { path, method, .. } = &s.k
            && path.len() == 1
            && method.is_none()
        {
            out.insert(path[0].clone());
        }
    });
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn tokens_strings_and_numbers() {
        let b = parse(
            "local a = 'it''s' .. \"\\65\\n\" .. [[\nlong]] .. [==[x]]y]==] -- c\n\
             --[[ long\n comment ]] local b = 0x1F + 3e2 + .5 + 7.",
        );
        // `'it''s'` is two strings next to each other: a syntax error.
        assert!(b.is_err());
        let b = parse(
            "local a = 'it\\'s' .. \"\\65\\n\" .. [[\nlong]] .. [==[x]]y]==] -- c\n\
             --[[ long\n comment ]] local b = 0x1F + 3e2 + .5 + 7.",
        )
        .unwrap();
        assert_eq!(b.len(), 2);
        let StatKind::Local(_, v) = &b[0].k else {
            panic!()
        };
        let mut strs = Vec::new();
        each_expr(&v[0], &mut |e| {
            if let ExprKind::Str(s) = &e.k {
                strs.push(s.clone());
            }
        });
        assert_eq!(strs, ["it's", "A\n", "long", "x]]y"]);
        let StatKind::Local(_, v) = &b[1].k else {
            panic!()
        };
        let mut nums = Vec::new();
        each_expr(&v[0], &mut |e| {
            if let ExprKind::Num(x) = &e.k {
                nums.push(*x);
            }
        });
        assert_eq!(nums, [31.0, 300.0, 0.5, 7.0]);
        assert_eq!(b[1].pos, Pos { line: 4, col: 13 });
    }

    #[test]
    fn priorities() {
        let b = parse("return 1 + 2 * 3 ^ 2 ^ 2 .. 'x' .. 'y' == z and not a or -b").unwrap();
        let StatKind::Return(v) = &b[0].k else {
            panic!()
        };
        // ((... == z) and (not a)) or (-b)
        let ExprKind::Bin(BinOp::Or, l, r) = &v[0].k else {
            panic!("{:?}", v[0])
        };
        assert!(matches!(r.k, ExprKind::Un(UnOp::Neg, _)));
        let ExprKind::Bin(BinOp::And, l, _) = &l.k else {
            panic!()
        };
        let ExprKind::Bin(BinOp::Eq, l, _) = &l.k else {
            panic!()
        };
        // `..` is right associative and below `+`.
        let ExprKind::Bin(BinOp::Concat, sum, rest) = &l.k else {
            panic!()
        };
        assert!(matches!(rest.k, ExprKind::Bin(BinOp::Concat, _, _)));
        let ExprKind::Bin(BinOp::Add, _, prod) = &sum.k else {
            panic!()
        };
        let ExprKind::Bin(BinOp::Mul, _, pow) = &prod.k else {
            panic!()
        };
        // `^` is right associative: 3 ^ (2 ^ 2).
        let ExprKind::Bin(BinOp::Pow, _, inner) = &pow.k else {
            panic!()
        };
        assert!(matches!(inner.k, ExprKind::Bin(BinOp::Pow, _, _)));
        // Unary minus binds tighter than `^`'s left side only: -x^2 is -(x^2).
        let b = parse("return -x ^ 2").unwrap();
        let StatKind::Return(v) = &b[0].k else {
            panic!()
        };
        assert!(
            matches!(&v[0].k, ExprKind::Un(UnOp::Neg, e) if matches!(e.k, ExprKind::Bin(BinOp::Pow, _, _)))
        );
    }

    #[test]
    fn statements() {
        let b = parse(
            "local function f(a, b) return a end
             function g.h:m(x) end
             for i = 1, 10, 2 do break end
             for k, v in ipairs(t) do end
             repeat local x = 1 until x
             while true do end
             if a then elseif b then else end
             a, b.c, d[1] = 1, 2
             f{1, 2; x = 3, ['y'] = 4}
             f'x':upper()
             do end
             return",
        )
        .unwrap();
        assert_eq!(b.len(), 12);
        assert!(matches!(&b[1].k, StatKind::Function { func, .. } if func.params == ["self", "x"]));
        // `return` and `break` end their block.
        assert!(parse("return 1 local x = 2").is_err());
        assert!(parse("while true do break local x = 1 end").is_err());
        assert!(parse("x").is_err());
        assert!(parse("f() = 1").is_err());
        assert!(parse("local s = 'a\nb'").is_err());
        assert!(parse("local s = '\\300'").is_err());
        assert!(parse("local s = '\\255'").is_err(), "not UTF-8");
    }
}
