//! The parser for `CREATE PROCEDURE | FUNCTION` and stored-program bodies
//! (the SQL/PSM language of both servers' default mode): the header, the
//! characteristics, and the body as a tree of compound statements whose
//! plain SQL statements and expressions stay text, as written.

use sprig_common::{Result, SprigError};

use super::super::define::lex::{self, T, Tok};

/// Procedure or function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    /// `CREATE PROCEDURE`.
    Procedure,
    /// `CREATE FUNCTION`.
    Function,
    /// `CREATE TRIGGER`.
    Trigger,
}

impl Kind {
    /// The keyword.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Kind::Procedure => "PROCEDURE",
            Kind::Function => "FUNCTION",
            Kind::Trigger => "TRIGGER",
        }
    }
}

/// A parameter's direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// `IN`.
    In,
    /// `OUT`.
    Out,
    /// `INOUT`.
    InOut,
}

impl Mode {
    /// The keyword.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Mode::In => "IN",
            Mode::Out => "OUT",
            Mode::InOut => "INOUT",
        }
    }
}

/// A parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Param {
    /// Direction (`IN` when not written).
    pub mode: Mode,
    /// Whether the direction was written.
    pub mode_written: bool,
    /// Its name.
    pub name: String,
    /// Its type, as written.
    pub ty: String,
}

/// What a routine says about its data access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Access {
    /// `CONTAINS SQL` (the default).
    #[default]
    Contains,
    /// `NO SQL`.
    No,
    /// `READS SQL DATA`.
    Reads,
    /// `MODIFIES SQL DATA`.
    Modifies,
}

impl Access {
    /// The words `SHOW CREATE` and `information_schema` print.
    pub(crate) fn words(self) -> &'static str {
        match self {
            Access::Contains => "CONTAINS SQL",
            Access::No => "NO SQL",
            Access::Reads => "READS SQL DATA",
            Access::Modifies => "MODIFIES SQL DATA",
        }
    }
}

/// The characteristics (last one written wins).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Chars {
    /// `COMMENT '...'`.
    pub comment: Option<String>,
    /// `[NOT] DETERMINISTIC`, when written.
    pub deterministic: Option<bool>,
    /// The data access words, when written.
    pub access: Option<Access>,
    /// `SQL SECURITY INVOKER` (`Some(true)`) or `DEFINER` (`Some(false)`).
    pub invoker: Option<bool>,
    /// `LANGUAGE SQL` was written.
    pub language: bool,
}

/// The header and body of a `CREATE PROCEDURE | FUNCTION`.
#[derive(Debug, Clone)]
pub(crate) struct Create {
    /// Procedure or function.
    pub kind: Kind,
    /// `DEFINER = ...`: (user, host); `None` when not written. `CURRENT_USER`
    /// is `Some(("", ""))`.
    pub definer: Option<(String, String)>,
    /// The name.
    pub name: String,
    /// The parameters.
    pub params: Vec<Param>,
    /// The parameter list as written, between the parentheses.
    pub params_text: String,
    /// `RETURNS type`, for a function.
    pub returns: Option<String>,
    /// The characteristics.
    pub chars: Chars,
    /// The body's text, as written.
    pub body_text: String,
    /// The body.
    pub body: Stmt,
}

/// A piece of SQL text: an expression or a statement, as written.
pub(crate) type Frag = String;

/// A variable a statement assigns: a local name or a user variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// A local variable or parameter (resolved when compiled).
    Local(String),
    /// `@name`.
    User(String),
}

/// One assignment of a `SET`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Assign {
    /// `local = expr`.
    Local(String, Frag),
    /// Anything else (`@x = ..`, `@@x = ..`, `SESSION x = ..`, `NAMES ..`):
    /// run as a `SET` statement of its own.
    Other(Frag),
}

/// A condition a handler, `SIGNAL` or `DECLARE ... CONDITION` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cond {
    /// An error number.
    Code(u16),
    /// `SQLSTATE [VALUE] 'xxxxx'`.
    State(String),
    /// A declared condition's name.
    Name(String),
    /// `SQLWARNING`.
    Warning,
    /// `NOT FOUND`.
    NotFound,
    /// `SQLEXCEPTION`.
    Exception,
}

/// A declaration at the start of a block.
#[derive(Debug, Clone)]
pub(crate) enum Decl {
    /// `DECLARE a, b type [DEFAULT expr]`.
    Var {
        /// The names.
        names: Vec<String>,
        /// The type, as written.
        ty: String,
        /// The default.
        default: Option<Frag>,
    },
    /// `DECLARE name CONDITION FOR ...`.
    Cond {
        /// Its name.
        name: String,
        /// What it stands for (a code or a SQLSTATE).
        value: Cond,
    },
    /// `DECLARE name CURSOR FOR select`.
    Cursor {
        /// Its name.
        name: String,
        /// The query.
        query: Frag,
    },
    /// `DECLARE CONTINUE | EXIT HANDLER FOR ... statement`.
    Handler {
        /// `EXIT`.
        exit: bool,
        /// The conditions.
        conds: Vec<Cond>,
        /// The handler's statement.
        body: Box<Stmt>,
    },
}

/// A `SIGNAL` / `RESIGNAL` item: (`MESSAGE_TEXT`, ...) = value.
pub(crate) type SignalItem = (String, Frag);

/// `GET DIAGNOSTICS` target = item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiagItem {
    /// Where the value goes.
    pub target: Target,
    /// The item's name, upper case (`ROW_COUNT`, `MESSAGE_TEXT`, ...).
    pub item: String,
}

/// A statement of a body.
#[derive(Debug, Clone)]
pub(crate) enum Stmt {
    /// `[label:] BEGIN decls stmts END [label]`.
    Block {
        /// The label.
        label: Option<String>,
        /// Declarations, in order.
        decls: Vec<Decl>,
        /// Statements.
        body: Vec<Stmt>,
    },
    /// A plain SQL statement, as written (without its `INTO` clause).
    Sql {
        /// The text.
        text: Frag,
        /// `SELECT ... INTO` targets.
        into: Option<Vec<Target>>,
        /// It may send a result set (a `SELECT` without `INTO`, `SHOW`, ...).
        results: bool,
    },
    /// `CALL name[(args)]` (run by the runtime, not as SQL).
    Call {
        /// The database, when written.
        db: Option<String>,
        /// The procedure.
        name: String,
        /// The arguments, as written.
        args: Vec<Frag>,
    },
    /// `SET ...`.
    Set(Vec<Assign>),
    /// `IF ... END IF`.
    If {
        /// (condition, statements), in order.
        arms: Vec<(Frag, Vec<Stmt>)>,
        /// `ELSE`.
        other: Option<Vec<Stmt>>,
    },
    /// `CASE ... END CASE`.
    Case {
        /// The simple form's expression.
        subject: Option<Frag>,
        /// (value or condition, statements).
        arms: Vec<(Frag, Vec<Stmt>)>,
        /// `ELSE`.
        other: Option<Vec<Stmt>>,
    },
    /// `[label:] LOOP ... END LOOP`.
    Loop {
        /// The label.
        label: Option<String>,
        /// The body.
        body: Vec<Stmt>,
    },
    /// `[label:] WHILE c DO ... END WHILE`.
    While {
        /// The label.
        label: Option<String>,
        /// The condition.
        cond: Frag,
        /// The body.
        body: Vec<Stmt>,
    },
    /// `[label:] REPEAT ... UNTIL c END REPEAT`.
    Repeat {
        /// The label.
        label: Option<String>,
        /// The body.
        body: Vec<Stmt>,
        /// The condition.
        until: Frag,
    },
    /// `LEAVE label`.
    Leave(String),
    /// `ITERATE label`.
    Iterate(String),
    /// `RETURN expr`.
    Return(Frag),
    /// `OPEN c`.
    Open(String),
    /// `FETCH c INTO ...`.
    Fetch {
        /// The cursor.
        cursor: String,
        /// The targets.
        into: Vec<Target>,
    },
    /// `CLOSE c`.
    Close(String),
    /// `SIGNAL` (`resignal: false`) or `RESIGNAL`.
    Signal {
        /// `RESIGNAL`.
        resignal: bool,
        /// The condition (required for `SIGNAL`).
        cond: Option<Cond>,
        /// `SET` items.
        items: Vec<SignalItem>,
    },
    /// `GET [CURRENT | STACKED] DIAGNOSTICS`.
    Diagnostics {
        /// `STACKED`.
        stacked: bool,
        /// `CONDITION n`: the condition number's expression.
        condition: Option<Frag>,
        /// The items.
        items: Vec<DiagItem>,
    },
}

/// A syntax error, quoting the text from where the parser stopped, as both
/// servers word it.
pub(crate) fn syntax(text: &str, at: usize) -> SprigError {
    let near: String = text.get(at..).unwrap_or("").chars().take(80).collect();
    let line = text[..at.min(text.len())].matches('\n').count() + 1;
    SprigError::standard(
        1064,
        *b"42000",
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your \
             MariaDB server version for the right syntax to use near '{near}' at line {line}"
        ),
    )
}

/// The text with each lone `:` (a label's) made a `~`, which the
/// statement lexer reads as an operator: quotes and comments are left
/// alone. The text keeps its length, so offsets still match.
fn colons(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = b.to_vec();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' | b'"' | b'`' => {
                i += 1;
                while i < b.len() {
                    if b[i] == b'\\' && c != b'`' {
                        i += 2;
                        continue;
                    }
                    if b[i] == c {
                        if b.get(i + 1) == Some(&c) {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'-' if b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace) =>
            {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            b':' if b.get(i + 1) != Some(&b'=') => {
                out[i] = b'~';
                i += 1;
            }
            _ => i += 1,
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_owned())
}

/// The parser's state.
pub(crate) struct P<'a> {
    pub(crate) text: &'a str,
    pub(crate) toks: Vec<Tok>,
    pub(crate) i: usize,
}

/// Words that start a statement of a body other than a plain SQL one.
const LABELLED: [&str; 5] = ["BEGIN", "LOOP", "WHILE", "REPEAT", "FOR"];

impl<'a> P<'a> {
    /// The tokens of `text`.
    pub(crate) fn new(text: &'a str) -> Result<Self> {
        let toks = lex::lex(&colons(text)).map_err(|at| syntax(text, at))?;
        Ok(P { text, toks, i: 0 })
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.i)
    }

    fn peek_at(&self, k: usize) -> Option<&Tok> {
        self.toks.get(self.i + k)
    }

    fn at(&self) -> usize {
        self.toks.get(self.i).map_or(self.text.len(), |t| t.at)
    }

    fn err(&self) -> SprigError {
        syntax(self.text, self.at())
    }

    fn is(&self, w: &str) -> bool {
        self.peek().is_some_and(|t| t.is(w))
    }

    fn is_at(&self, k: usize, w: &str) -> bool {
        self.peek_at(k).is_some_and(|t| t.is(w))
    }

    fn op(&self, o: &str) -> bool {
        self.peek().is_some_and(|t| t.op(o))
    }

    fn eat(&mut self, w: &str) -> bool {
        if self.is(w) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn eat_op(&mut self, o: &str) -> bool {
        if self.op(o) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn want(&mut self, w: &str) -> Result<()> {
        if self.eat(w) { Ok(()) } else { Err(self.err()) }
    }

    fn want_op(&mut self, o: &str) -> Result<()> {
        if self.eat_op(o) {
            Ok(())
        } else {
            Err(self.err())
        }
    }

    /// A name: a bare word or a back-quoted one.
    fn name(&mut self) -> Result<String> {
        match self.peek().map(|t| &t.t) {
            Some(T::Word(w) | T::Quoted(w)) => {
                let w = w.clone();
                self.i += 1;
                Ok(w)
            }
            _ => Err(self.err()),
        }
    }

    /// The text from token `from` up to the current token, trimmed.
    fn slice(&self, from: usize) -> String {
        let a = self.toks.get(from).map_or(self.text.len(), |t| t.at);
        let b = self.at();
        self.text.get(a..b.max(a)).unwrap_or("").trim().to_owned()
    }

    /// Skips a balanced `( ... )` starting at the current token.
    fn skip_parens(&mut self) -> Result<()> {
        let mut depth = 0usize;
        loop {
            let Some(t) = self.peek() else {
                return Err(self.err());
            };
            if t.op("(") {
                depth += 1;
            } else if t.op(")") {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    self.i += 1;
                    return Ok(());
                }
            }
            self.i += 1;
        }
    }

    /// An expression's text up to (not including) one of `stops` at depth
    /// 0, outside `CASE ... END` expressions; `;` and the end also stop.
    fn expr_until(&mut self, stops: &[&str]) -> Result<Frag> {
        let from = self.i;
        let mut depth = 0usize;
        let mut cases = 0usize;
        while let Some(t) = self.peek() {
            if t.op("(") {
                depth += 1;
            } else if t.op(")") {
                if depth == 0 {
                    break;
                }
                depth -= 1;
            } else if depth == 0 {
                if t.op(";") {
                    break;
                }
                if t.is("CASE") {
                    cases += 1;
                } else if t.is("END") && cases > 0 {
                    cases -= 1;
                } else if cases == 0
                    && (stops.iter().any(|w| t.is(w)) || (t.op(",") && stops.contains(&",")))
                {
                    break;
                }
            }
            self.i += 1;
        }
        if self.i == from {
            return Err(self.err());
        }
        Ok(self.slice(from))
    }

    /// A data type as written: base words, `(args)`, then attributes.
    pub(crate) fn data_type(&mut self) -> Result<String> {
        let from = self.i;
        let base = self.name()?.to_ascii_uppercase();
        // Two-word types.
        match base.as_str() {
            "DOUBLE" => {
                self.eat("PRECISION");
            }
            "CHARACTER" | "CHAR" | "NATIONAL" | "NCHAR" => {
                self.eat("VARYING");
                if base == "NATIONAL" && !self.eat("CHAR") && !self.eat("CHARACTER") {
                    self.eat("VARCHAR");
                }
                self.eat("VARYING");
            }
            "LONG" => {
                if !self.eat("VARCHAR") {
                    self.eat("VARBINARY");
                }
            }
            _ => {}
        }
        if self.op("(") {
            self.skip_parens()?;
        }
        loop {
            if self.eat("UNSIGNED")
                || self.eat("SIGNED")
                || self.eat("ZEROFILL")
                || self.eat("BINARY")
                || self.eat("ASCII")
                || self.eat("UNICODE")
                || self.eat("BYTE")
            {
                continue;
            }
            if self.is("CHARACTER") && self.is_at(1, "SET") {
                self.i += 2;
                self.name()?;
                continue;
            }
            if self.eat("CHARSET") || self.eat("COLLATE") {
                self.name()?;
                continue;
            }
            break;
        }
        Ok(self.slice(from))
    }

    /// `name [(params)] ...` header after `PROCEDURE | FUNCTION`.
    fn qualified(&mut self) -> Result<(Option<String>, String)> {
        let a = self.name()?;
        if self.eat_op(".") {
            let b = self.name()?;
            return Ok((Some(a), b));
        }
        Ok((None, a))
    }
}

/// Parses a whole `CREATE [OR REPLACE] [DEFINER = ...] PROCEDURE | FUNCTION`
/// statement.
#[allow(clippy::too_many_lines)] // the header, in order
pub(crate) fn parse_create(text: &str) -> Result<Create> {
    let mut p = P::new(text)?;
    p.want("CREATE")?;
    if p.eat("OR") {
        p.want("REPLACE")?;
    }
    let mut definer = None;
    if p.eat("DEFINER") {
        p.want_op("=")?;
        let from = p.i;
        if p.eat("CURRENT_USER") {
            if p.eat_op("(") {
                p.want_op(")")?;
            }
            definer = Some((String::new(), String::new()));
        } else {
            while let Some(t) = p.peek() {
                if t.is("PROCEDURE") || t.is("FUNCTION") || t.is("AGGREGATE") {
                    break;
                }
                p.i += 1;
            }
            definer = Some(user_host(&p.slice(from)).ok_or_else(|| p.err())?);
        }
    }
    if p.eat("AGGREGATE") {
        return Err(super::not_on_engine("CREATE AGGREGATE FUNCTION"));
    }
    let kind = if p.eat("PROCEDURE") {
        Kind::Procedure
    } else if p.eat("FUNCTION") {
        Kind::Function
    } else {
        return Err(p.err());
    };
    if p.is("IF") && p.is_at(1, "NOT") && p.is_at(2, "EXISTS") {
        p.i += 3;
    }
    let (_, name) = p.qualified()?;
    p.want_op("(")?;
    let pfrom = p.i;
    let mut params = Vec::new();
    if !p.op(")") {
        loop {
            let (mode, written) = if p.eat("INOUT") {
                (Mode::InOut, true)
            } else if p.eat("OUT") {
                (Mode::Out, true)
            } else if p.eat("IN") {
                (Mode::In, true)
            } else {
                (Mode::In, false)
            };
            if written && kind == Kind::Function && mode != Mode::In {
                return Err(super::not_on_engine(
                    "OUT or INOUT parameters of a function",
                ));
            }
            let pname = p.name()?;
            let ty = p.data_type()?;
            if p.is("DEFAULT") {
                return Err(super::not_on_engine("parameter defaults"));
            }
            params.push(Param {
                mode,
                mode_written: written,
                name: pname,
                ty,
            });
            if !p.eat_op(",") {
                break;
            }
        }
    }
    let params_text = p.slice(pfrom);
    p.want_op(")")?;
    let returns = if kind == Kind::Function {
        p.want("RETURNS")?;
        Some(p.data_type()?)
    } else {
        None
    };
    let mut chars = Chars::default();
    p.chars_into(&mut chars)?;
    let body_from = p.at();
    if p.peek().is_none() {
        return Err(p.err());
    }
    let body = p.statement(true)?;
    while p.eat_op(";") {}
    if p.peek().is_some() {
        return Err(p.err());
    }
    let body_text = text[body_from..]
        .trim_end()
        .trim_end_matches(';')
        .trim_end()
        .to_owned();
    Ok(Create {
        kind,
        definer,
        name,
        params,
        params_text,
        returns,
        chars,
        body_text,
        body,
    })
}

impl P<'_> {
    /// Characteristics, any order, the last of a kind winning.
    fn chars_into(&mut self, chars: &mut Chars) -> Result<()> {
        loop {
            if self.eat("COMMENT") {
                match self.peek().map(|t| &t.t) {
                    Some(T::Str(s)) => {
                        chars.comment = Some(s.clone());
                        self.i += 1;
                    }
                    _ => return Err(self.err()),
                }
            } else if self.is("LANGUAGE") && self.is_at(1, "SQL") {
                self.i += 2;
                chars.language = true;
            } else if self.is("NOT") && self.is_at(1, "DETERMINISTIC") {
                self.i += 2;
                chars.deterministic = Some(false);
            } else if self.eat("DETERMINISTIC") {
                chars.deterministic = Some(true);
            } else if self.is("CONTAINS") && self.is_at(1, "SQL") {
                self.i += 2;
                chars.access = Some(Access::Contains);
            } else if self.is("NO") && self.is_at(1, "SQL") {
                self.i += 2;
                chars.access = Some(Access::No);
            } else if self.is("READS") && self.is_at(1, "SQL") && self.is_at(2, "DATA") {
                self.i += 3;
                chars.access = Some(Access::Reads);
            } else if self.is("MODIFIES") && self.is_at(1, "SQL") && self.is_at(2, "DATA") {
                self.i += 3;
                chars.access = Some(Access::Modifies);
            } else if self.is("SQL") && self.is_at(1, "SECURITY") {
                self.i += 2;
                if self.eat("DEFINER") {
                    chars.invoker = Some(false);
                } else if self.eat("INVOKER") {
                    chars.invoker = Some(true);
                } else {
                    return Err(self.err());
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// `ALTER PROCEDURE | FUNCTION [db.]name characteristic ...`: the
    /// characteristics (`DETERMINISTIC` can't change).
    pub(crate) fn alter_chars(&mut self) -> Result<Chars> {
        self.want("ALTER")?;
        if !self.eat("PROCEDURE") {
            self.want("FUNCTION")?;
        }
        self.qualified()?;
        let mut c = Chars::default();
        self.chars_into(&mut c)?;
        if self.peek().is_some() && !self.op(";") {
            return Err(self.err());
        }
        if c.deterministic.is_some() {
            return Err(self.err());
        }
        Ok(c)
    }
}

/// `'user'@'host'`, `user@host`, `` `u`@`h` `` as written: the parts,
/// unquoted. A missing host is `%`.
pub(crate) fn user_host(s: &str) -> Option<(String, String)> {
    fn part(s: &str) -> Option<String> {
        let s = s.trim();
        let b = s.as_bytes();
        if b.len() >= 2 && matches!(b[0], b'\'' | b'"' | b'`') && b[b.len() - 1] == b[0] {
            let q = b[0] as char;
            return Some(s[1..s.len() - 1].replace(&format!("{q}{q}"), &q.to_string()));
        }
        if s.is_empty() || s.contains(char::is_whitespace) {
            return None;
        }
        Some(s.to_owned())
    }
    let s = s.trim();
    // The `@` outside quotes.
    let mut q: Option<char> = None;
    for (i, c) in s.char_indices() {
        match (q, c) {
            (None, '\'' | '"' | '`') => q = Some(c),
            (Some(x), c) if c == x => q = None,
            (None, '@') => return Some((part(&s[..i])?, part(&s[i + 1..])?)),
            _ => {}
        }
    }
    Some((part(s)?, "%".to_owned()))
}

impl P<'_> {
    /// A label before a compound statement: `name:` (the `:` lexed as `~`).
    fn label(&mut self) -> Option<String> {
        let is_label = matches!(self.peek().map(|t| &t.t), Some(T::Word(_) | T::Quoted(_)))
            && self.peek_at(1).is_some_and(|t| t.op("~"))
            && self
                .peek_at(2)
                .is_some_and(|t| LABELLED.iter().any(|w| t.is(w)));
        if !is_label {
            return None;
        }
        let n = self.name().ok()?;
        self.i += 1;
        Some(n)
    }

    /// An end label, which must repeat the begin label.
    fn end_label(&mut self, label: Option<&String>) -> Result<()> {
        if let Some(Tok {
            t: T::Word(w) | T::Quoted(w),
            ..
        }) = self.peek()
        {
            if self.peek().is_some_and(|t| {
                t.is("END") || t.is("ELSE") || t.is("ELSEIF") || t.is("WHEN") || t.is("UNTIL")
            }) {
                return Ok(());
            }
            let w = w.clone();
            match label {
                Some(l) if l.eq_ignore_ascii_case(&w) => {
                    self.i += 1;
                    Ok(())
                }
                _ => Err(SprigError::standard(
                    1310,
                    *b"42000",
                    format!("End-label {w} without match"),
                )),
            }
        } else {
            Ok(())
        }
    }

    /// Statements up to (not including) one of `ends`, each ended by `;`.
    fn statements(&mut self, ends: &[&str]) -> Result<Vec<Stmt>> {
        let mut out = Vec::new();
        loop {
            if self.peek().is_none() {
                return Err(self.err());
            }
            if ends.iter().any(|w| self.is(w)) {
                return Ok(out);
            }
            if self.is("DECLARE") {
                return Err(self.err());
            }
            out.push(self.statement(false)?);
            if !self.eat_op(";") {
                return Err(self.err());
            }
        }
    }

    /// One statement. `top`: the routine's body (no `;` after it).
    #[allow(clippy::too_many_lines)] // one arm per statement
    pub(crate) fn statement(&mut self, top: bool) -> Result<Stmt> {
        let _ = top;
        let label = self.label();
        if self.eat("BEGIN") {
            if self.is("NOT") && self.is_at(1, "ATOMIC") {
                self.i += 2;
            }
            let mut decls = Vec::new();
            while self.is("DECLARE") {
                decls.push(self.declare()?);
                if !self.eat_op(";") {
                    return Err(self.err());
                }
            }
            let body = self.statements(&["END"])?;
            self.want("END")?;
            self.end_label(label.as_ref())?;
            return Ok(Stmt::Block { label, decls, body });
        }
        if self.eat("LOOP") {
            let body = self.statements(&["END"])?;
            self.want("END")?;
            self.want("LOOP")?;
            self.end_label(label.as_ref())?;
            return Ok(Stmt::Loop { label, body });
        }
        if self.eat("WHILE") {
            let cond = self.expr_until(&["DO"])?;
            self.want("DO")?;
            let body = self.statements(&["END"])?;
            self.want("END")?;
            self.want("WHILE")?;
            self.end_label(label.as_ref())?;
            return Ok(Stmt::While { label, cond, body });
        }
        if self.eat("REPEAT") {
            let body = self.statements(&["UNTIL"])?;
            self.want("UNTIL")?;
            let until = self.expr_until(&["END"])?;
            self.want("END")?;
            self.want("REPEAT")?;
            self.end_label(label.as_ref())?;
            return Ok(Stmt::Repeat { label, body, until });
        }
        if self.is("FOR") && label.is_some() {
            return Err(super::not_on_engine("FOR loops in stored programs"));
        }
        if label.is_some() {
            return Err(self.err());
        }
        if self.eat("IF") {
            let mut arms = Vec::new();
            let mut other = None;
            loop {
                let c = self.expr_until(&["THEN"])?;
                self.want("THEN")?;
                let body = self.statements(&["ELSEIF", "ELSE", "END"])?;
                if body.is_empty() {
                    return Err(self.err());
                }
                arms.push((c, body));
                if self.eat("ELSEIF") {
                    continue;
                }
                if self.eat("ELSE") {
                    let b = self.statements(&["END"])?;
                    if b.is_empty() {
                        return Err(self.err());
                    }
                    other = Some(b);
                }
                break;
            }
            self.want("END")?;
            self.want("IF")?;
            return Ok(Stmt::If { arms, other });
        }
        if self.eat("CASE") {
            let subject = if self.is("WHEN") {
                None
            } else {
                Some(self.expr_until(&["WHEN"])?)
            };
            let mut arms = Vec::new();
            let mut other = None;
            while self.eat("WHEN") {
                let v = self.expr_until(&["THEN"])?;
                self.want("THEN")?;
                let body = self.statements(&["WHEN", "ELSE", "END"])?;
                if body.is_empty() {
                    return Err(self.err());
                }
                arms.push((v, body));
            }
            if arms.is_empty() {
                return Err(self.err());
            }
            if self.eat("ELSE") {
                let b = self.statements(&["END"])?;
                if b.is_empty() {
                    return Err(self.err());
                }
                other = Some(b);
            }
            self.want("END")?;
            self.want("CASE")?;
            return Ok(Stmt::Case {
                subject,
                arms,
                other,
            });
        }
        if self.eat("LEAVE") {
            return Ok(Stmt::Leave(self.name()?));
        }
        if self.eat("ITERATE") {
            return Ok(Stmt::Iterate(self.name()?));
        }
        if self.eat("RETURN") {
            return Ok(Stmt::Return(self.expr_until(&[])?));
        }
        if self.eat("OPEN") {
            return Ok(Stmt::Open(self.name()?));
        }
        if self.eat("CLOSE") {
            return Ok(Stmt::Close(self.name()?));
        }
        if self.eat("FETCH") {
            if self.eat("NEXT") {
                self.want("FROM")?;
            } else {
                self.eat("FROM");
            }
            let cursor = self.name()?;
            self.want("INTO")?;
            let into = self.targets()?;
            return Ok(Stmt::Fetch { cursor, into });
        }
        if self.is("SIGNAL") || self.is("RESIGNAL") {
            let resignal = self.is("RESIGNAL");
            self.i += 1;
            let cond = if self.is("SET") || self.op(";") || self.peek().is_none() {
                if !resignal {
                    return Err(self.err());
                }
                None
            } else {
                Some(self.condition()?)
            };
            let mut items = Vec::new();
            if self.eat("SET") {
                loop {
                    let item = self.name()?.to_ascii_uppercase();
                    self.want_op("=")?;
                    let v = self.expr_until(&[","])?;
                    items.push((item, v));
                    if !self.eat_op(",") {
                        break;
                    }
                }
            }
            return Ok(Stmt::Signal {
                resignal,
                cond,
                items,
            });
        }
        if self.is("GET")
            && (self.is_at(1, "DIAGNOSTICS")
                || ((self.is_at(1, "CURRENT") || self.is_at(1, "STACKED"))
                    && self.is_at(2, "DIAGNOSTICS")))
        {
            self.i += 1;
            let stacked = self.eat("STACKED");
            self.eat("CURRENT");
            self.want("DIAGNOSTICS")?;
            // `CONDITION n`: a number, a variable or a name (one token).
            let condition = if self.eat("CONDITION") {
                let from = self.i;
                if self.peek().is_none() {
                    return Err(self.err());
                }
                self.i += 1;
                Some(self.slice(from))
            } else {
                None
            };
            let mut items = Vec::new();
            loop {
                let Some(target) = self.targets()?.into_iter().next() else {
                    return Err(self.err());
                };
                self.want_op("=")?;
                let item = self.name()?.to_ascii_uppercase();
                items.push(DiagItem { target, item });
                if !self.eat_op(",") {
                    break;
                }
            }
            return Ok(Stmt::Diagnostics {
                stacked,
                condition,
                items,
            });
        }
        if self.eat("CALL") {
            let (db, name) = self.qualified()?;
            let mut args = Vec::new();
            if self.eat_op("(") && !self.eat_op(")") {
                loop {
                    args.push(self.expr_until(&[","])?);
                    if self.eat_op(")") {
                        break;
                    }
                    self.want_op(",")?;
                }
            }
            return Ok(Stmt::Call { db, name, args });
        }
        if self.is("SET") && !self.is_at(1, "TRANSACTION") && !self.is_at(1, "PASSWORD") {
            return self.set();
        }
        if self.is("DECLARE") {
            return Err(self.err());
        }
        self.plain()
    }

    /// `SET a = 1, @b := 2, ...`.
    fn set(&mut self) -> Result<Stmt> {
        let from_set = self.i;
        self.want("SET")?;
        // `SET NAMES`, `SET CHARACTER SET`, `SET STATEMENT ... FOR`: one
        // statement of their own.
        if self.is("NAMES")
            || self.is("CHARSET")
            || self.is("STATEMENT")
            || (self.is("CHARACTER") && self.is_at(1, "SET"))
        {
            self.i = from_set;
            return self.plain();
        }
        let mut out = Vec::new();
        loop {
            let from = self.i;
            let assigns = |k: usize| self.peek_at(k).is_some_and(|t| t.op("=") || t.op(":="));
            let local = match self.peek().map(|t| &t.t) {
                Some(T::Word(w) | T::Quoted(w)) if assigns(1) => Some((w.clone(), 2)),
                // A trigger's `NEW.col` / `OLD.col`.
                Some(T::Word(w))
                    if (w.eq_ignore_ascii_case("NEW") || w.eq_ignore_ascii_case("OLD"))
                        && self.peek_at(1).is_some_and(|t| t.op("."))
                        && assigns(3) =>
                {
                    match self.peek_at(2).map(|t| &t.t) {
                        Some(T::Word(c) | T::Quoted(c)) => {
                            Some((format!("{}.{c}", w.to_ascii_uppercase()), 4))
                        }
                        _ => None,
                    }
                }
                _ => None,
            };
            if let Some((n, skip)) = local {
                self.i += skip;
                let v = self.expr_until(&[","])?;
                out.push(Assign::Local(n, v));
            } else {
                // Up to the next assignment's comma.
                let _ = self.expr_until(&[","])?;
                out.push(Assign::Other(self.slice(from)));
            }
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(Stmt::Set(out))
    }

    /// `FETCH`/`SELECT ... INTO` targets.
    fn targets(&mut self) -> Result<Vec<Target>> {
        let mut out = Vec::new();
        loop {
            match self.peek().map(|t| &t.t) {
                Some(T::Var(v)) if !v.starts_with("@@") => {
                    let v = v.clone();
                    out.push(Target::User(v[1..].to_owned()));
                    self.i += 1;
                    // `@'name'`, `@"name"`, `` @`name` ``.
                    if v.len() == 1 {
                        match self.peek().map(|t| &t.t) {
                            Some(T::Str(s) | T::Quoted(s)) => {
                                let s = s.clone();
                                self.i += 1;
                                if let Some(Target::User(n)) = out.last_mut() {
                                    *n = s;
                                }
                            }
                            _ => return Err(self.err()),
                        }
                    }
                }
                Some(T::Word(_) | T::Quoted(_)) => out.push(Target::Local(self.name()?)),
                _ => return Err(self.err()),
            }
            if !self.eat_op(",") {
                return Ok(out);
            }
        }
    }

    /// A condition value of a handler, `SIGNAL` or `DECLARE ... CONDITION`.
    fn condition(&mut self) -> Result<Cond> {
        if self.eat("SQLSTATE") {
            self.eat("VALUE");
            return match self.peek().map(|t| &t.t) {
                Some(T::Str(s)) => {
                    let s = s.clone();
                    self.i += 1;
                    if s.len() != 5
                        || !s
                            .bytes()
                            .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
                        || s.starts_with("00")
                    {
                        return Err(SprigError::standard(
                            1407,
                            *b"42000",
                            format!("Bad SQLSTATE: '{s}'"),
                        ));
                    }
                    Ok(Cond::State(s))
                }
                _ => Err(self.err()),
            };
        }
        if self.eat("SQLWARNING") {
            return Ok(Cond::Warning);
        }
        if self.eat("SQLEXCEPTION") {
            return Ok(Cond::Exception);
        }
        if self.is("NOT") && self.is_at(1, "FOUND") {
            self.i += 2;
            return Ok(Cond::NotFound);
        }
        match self.peek().map(|t| &t.t) {
            Some(T::Num(n)) => {
                let n = n.clone();
                self.i += 1;
                let v: u16 = n.parse().map_err(|_| self.err())?;
                if v == 0 {
                    return Err(SprigError::standard(
                        1525,
                        *b"HY000",
                        "Incorrect CONDITION value: '0'",
                    ));
                }
                Ok(Cond::Code(v))
            }
            Some(T::Word(_) | T::Quoted(_)) => Ok(Cond::Name(self.name()?)),
            _ => Err(self.err()),
        }
    }

    /// `DECLARE ...` (the word not yet read).
    fn declare(&mut self) -> Result<Decl> {
        self.want("DECLARE")?;
        if self.is("CONTINUE") || self.is("EXIT") || self.is("UNDO") {
            if self.is("UNDO") {
                return Err(self.err());
            }
            let exit = self.is("EXIT");
            self.i += 1;
            self.want("HANDLER")?;
            self.want("FOR")?;
            let mut conds = Vec::new();
            loop {
                conds.push(self.condition()?);
                if !self.eat_op(",") {
                    break;
                }
            }
            let body = self.statement(false)?;
            return Ok(Decl::Handler {
                exit,
                conds,
                body: Box::new(body),
            });
        }
        let first = self.name()?;
        if self.eat("CONDITION") {
            self.want("FOR")?;
            let value = self.condition()?;
            if matches!(
                value,
                Cond::Name(_) | Cond::Warning | Cond::NotFound | Cond::Exception
            ) {
                return Err(self.err());
            }
            return Ok(Decl::Cond { name: first, value });
        }
        if self.eat("CURSOR") {
            self.want("FOR")?;
            let from = self.i;
            let _ = self.plain()?;
            return Ok(Decl::Cursor {
                name: first,
                query: self.slice(from),
            });
        }
        let mut names = vec![first];
        while self.eat_op(",") {
            names.push(self.name()?);
        }
        let ty = self.data_type()?;
        let default = if self.eat("DEFAULT") {
            Some(self.expr_until(&[])?)
        } else {
            None
        };
        Ok(Decl::Var { names, ty, default })
    }

    /// A plain SQL statement up to `;` at depth 0 (compound statements
    /// can't be inside one).
    fn plain(&mut self) -> Result<Stmt> {
        let from = self.i;
        let first = self.peek().map(|t| match &t.t {
            T::Word(w) => w.to_ascii_uppercase(),
            T::Op(o) => (*o).to_owned(),
            _ => String::new(),
        });
        let mut depth = 0usize;
        // The top-level `INTO` of a SELECT: (token index, end index).
        let mut into: Option<(usize, usize)> = None;
        let selecting = matches!(first.as_deref(), Some("SELECT" | "WITH" | "("));
        let mut prev_insert = false;
        while let Some(t) = self.peek() {
            if t.op("(") {
                depth += 1;
            } else if t.op(")") {
                if depth == 0 {
                    return Err(self.err());
                }
                depth -= 1;
            } else if t.op(";") && depth == 0 {
                break;
            } else if selecting
                && depth == 0
                && into.is_none()
                && t.is("INTO")
                && !prev_insert
                && !self.is_at(1, "OUTFILE")
                && !self.is_at(1, "DUMPFILE")
            {
                let start = self.i;
                self.i += 1;
                let _ = self.targets()?;
                into = Some((start, self.i));
                continue;
            }
            prev_insert = t.is("INSERT") || t.is("REPLACE");
            self.i += 1;
        }
        if self.i == from {
            return Err(self.err());
        }
        let results_word = matches!(
            first.as_deref(),
            Some(
                "SELECT"
                    | "WITH"
                    | "("
                    | "SHOW"
                    | "EXPLAIN"
                    | "DESCRIBE"
                    | "DESC"
                    | "VALUES"
                    | "TABLE"
                    | "CHECK"
                    | "ANALYZE"
                    | "OPTIMIZE"
                    | "REPAIR"
                    | "CHECKSUM"
                    | "HELP"
            )
        );
        match into {
            Some((a, b)) => {
                let end = self.i;
                // The text without the INTO clause.
                let s = self.toks[from].at;
                let ia = self.toks[a].at;
                let ib = self.toks.get(b).map_or(self.text.len(), |t| t.at);
                let e = self.toks.get(end).map_or(self.text.len(), |t| t.at);
                let text = format!("{} {}", &self.text[s..ia], &self.text[ib..e]);
                let mut q = P {
                    text: self.text,
                    toks: self.toks.clone(),
                    i: a + 1,
                };
                let targets = q.targets()?;
                Ok(Stmt::Sql {
                    text: text.trim().to_owned(),
                    into: Some(targets),
                    results: false,
                })
            }
            None => Ok(Stmt::Sql {
                text: self.slice(from),
                into: None,
                results: results_word,
            }),
        }
    }
}

/// What a trigger fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Event {
    /// `INSERT`.
    Insert,
    /// `UPDATE`.
    Update,
    /// `DELETE`.
    Delete,
}

impl Event {
    /// The keyword.
    pub(crate) fn word(self) -> &'static str {
        match self {
            Event::Insert => "INSERT",
            Event::Update => "UPDATE",
            Event::Delete => "DELETE",
        }
    }
}

/// A `CREATE TRIGGER` statement.
#[derive(Debug, Clone)]
pub(crate) struct TrigCreate {
    /// `DEFINER = ...` as for a routine.
    pub definer: Option<(String, String)>,
    /// The name.
    pub name: String,
    /// `BEFORE` (else `AFTER`).
    pub before: bool,
    /// The event.
    pub event: Event,
    /// The table's database, when written.
    pub table_db: Option<String>,
    /// The table.
    pub table: String,
    /// `FOLLOWS t` (`true`) or `PRECEDES t`.
    pub order: Option<(bool, String)>,
    /// The text from `TRIGGER` to the end, as written.
    pub rest: String,
    /// The body's text.
    pub body_text: String,
    /// The body.
    pub body: Stmt,
}

/// Parses `CREATE [OR REPLACE] [DEFINER = ...] TRIGGER [IF NOT EXISTS]
/// [db.]name {BEFORE | AFTER} {INSERT | UPDATE | DELETE} ON [db.]table FOR
/// EACH ROW [{FOLLOWS | PRECEDES} other] body`.
pub(crate) fn parse_trigger(text: &str) -> Result<TrigCreate> {
    let mut p = P::new(text)?;
    p.want("CREATE")?;
    if p.eat("OR") {
        p.want("REPLACE")?;
    }
    let mut definer = None;
    if p.eat("DEFINER") {
        p.want_op("=")?;
        let from = p.i;
        if p.eat("CURRENT_USER") {
            if p.eat_op("(") {
                p.want_op(")")?;
            }
            definer = Some((String::new(), String::new()));
        } else {
            while p.peek().is_some() && !p.is("TRIGGER") {
                p.i += 1;
            }
            definer = Some(user_host(&p.slice(from)).ok_or_else(|| p.err())?);
        }
    }
    let rest_at = p.at();
    p.want("TRIGGER")?;
    if p.is("IF") && p.is_at(1, "NOT") && p.is_at(2, "EXISTS") {
        p.i += 3;
    }
    let (_, name) = p.qualified()?;
    let before = if p.eat("BEFORE") {
        true
    } else {
        p.want("AFTER")?;
        false
    };
    let event = if p.eat("INSERT") {
        Event::Insert
    } else if p.eat("UPDATE") {
        Event::Update
    } else {
        p.want("DELETE")?;
        Event::Delete
    };
    if event == Event::Update && p.is("OF") {
        return Err(super::not_on_engine("UPDATE OF column lists on triggers"));
    }
    p.want("ON")?;
    let (table_db, table) = p.qualified()?;
    p.want("FOR")?;
    p.want("EACH")?;
    p.want("ROW")?;
    let order = if p.eat("FOLLOWS") {
        Some((true, p.name()?))
    } else if p.eat("PRECEDES") {
        Some((false, p.name()?))
    } else {
        None
    };
    let body_from = p.at();
    if p.peek().is_none() {
        return Err(p.err());
    }
    let body = p.statement(true)?;
    while p.eat_op(";") {}
    if p.peek().is_some() {
        return Err(p.err());
    }
    let trim = |s: &str| s.trim_end().trim_end_matches(';').trim_end().to_owned();
    Ok(TrigCreate {
        definer,
        name,
        before,
        event,
        table_db,
        table,
        order,
        rest: trim(&text[rest_at..]),
        body_text: trim(&text[body_from..]),
        body,
    })
}

/// An event's schedule as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Schedule {
    /// `AT expr`.
    At(Frag),
    /// `EVERY value unit [STARTS expr] [ENDS expr]`.
    Every {
        /// The interval's value.
        value: Frag,
        /// The unit, upper case.
        unit: String,
        /// `STARTS expr`.
        starts: Option<Frag>,
        /// `ENDS expr`.
        ends: Option<Frag>,
    },
}

/// `ENABLE`, `DISABLE`, `DISABLE ON SLAVE | REPLICA`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum EvStatus {
    /// Scheduled.
    Enabled,
    /// Not scheduled.
    Disabled,
    /// Created through replication: not scheduled here.
    SlaveDisabled,
}

/// A `CREATE EVENT` or `ALTER EVENT` statement.
#[derive(Debug, Clone)]
pub(crate) struct EvStmt {
    /// `CREATE` (else `ALTER`).
    pub create: bool,
    /// `DEFINER = ...` as for a routine.
    pub definer: Option<(String, String)>,
    /// The database, when written.
    pub db: Option<String>,
    /// The name.
    pub name: String,
    /// `ON SCHEDULE ...`.
    pub schedule: Option<Schedule>,
    /// `ON COMPLETION [NOT] PRESERVE`: `Some(true)` for `PRESERVE`.
    pub preserve: Option<bool>,
    /// `ENABLE` / `DISABLE` / `DISABLE ON SLAVE`.
    pub status: Option<EvStatus>,
    /// `COMMENT '...'`.
    pub comment: Option<String>,
    /// `RENAME TO [db.]name`.
    pub rename: Option<(Option<String>, String)>,
    /// The body's text, after `DO`.
    pub body_text: Option<String>,
    /// The body.
    pub body: Option<Stmt>,
}

/// The interval units of `EVERY`.
pub(crate) const UNITS: [&str; 20] = [
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "YEAR_MONTH",
    "DAY_HOUR",
    "DAY_MINUTE",
    "DAY_SECOND",
    "HOUR_MINUTE",
    "HOUR_SECOND",
    "MINUTE_SECOND",
    "MICROSECOND",
    "DAY_MICROSECOND",
    "HOUR_MICROSECOND",
    "MINUTE_MICROSECOND",
    "SECOND_MICROSECOND",
];

/// Parses `CREATE [OR REPLACE] [DEFINER = ...] EVENT [IF NOT EXISTS]
/// [db.]name ON SCHEDULE schedule [ON COMPLETION [NOT] PRESERVE] [ENABLE |
/// DISABLE | DISABLE ON SLAVE] [COMMENT '...'] DO body`, and `ALTER
/// [DEFINER = ...] EVENT [db.]name [ON SCHEDULE ...] [ON COMPLETION ...]
/// [RENAME TO name] [ENABLE | DISABLE ...] [COMMENT ...] [DO body]`.
#[allow(clippy::too_many_lines)] // the clauses, in order
pub(crate) fn parse_event(text: &str) -> Result<EvStmt> {
    const TAIL: [&str; 6] = ["ON", "ENABLE", "DISABLE", "COMMENT", "DO", "RENAME"];
    let mut p = P::new(text)?;
    let create = if p.eat("CREATE") {
        if p.eat("OR") {
            p.want("REPLACE")?;
        }
        true
    } else {
        p.want("ALTER")?;
        false
    };
    let mut definer = None;
    if p.eat("DEFINER") {
        p.want_op("=")?;
        let from = p.i;
        if p.eat("CURRENT_USER") {
            if p.eat_op("(") {
                p.want_op(")")?;
            }
            definer = Some((String::new(), String::new()));
        } else {
            while p.peek().is_some() && !p.is("EVENT") {
                p.i += 1;
            }
            definer = Some(user_host(&p.slice(from)).ok_or_else(|| p.err())?);
        }
    }
    p.want("EVENT")?;
    if create && p.is("IF") && p.is_at(1, "NOT") && p.is_at(2, "EXISTS") {
        p.i += 3;
    }
    let (db, name) = p.qualified()?;
    let mut ev = EvStmt {
        create,
        definer,
        db,
        name,
        schedule: None,
        preserve: None,
        status: None,
        comment: None,
        rename: None,
        body_text: None,
        body: None,
    };
    if create && !(p.is("ON") && p.is_at(1, "SCHEDULE")) {
        return Err(p.err());
    }
    loop {
        if p.is("ON") && p.is_at(1, "SCHEDULE") {
            p.i += 2;
            if p.eat("AT") {
                ev.schedule = Some(Schedule::At(p.expr_until(&TAIL)?));
            } else {
                p.want("EVERY")?;
                let value = p.expr_until(&UNITS)?;
                let unit = p.name()?.to_ascii_uppercase();
                if !UNITS.contains(&unit.as_str()) {
                    return Err(p.err());
                }
                let mut starts = None;
                let mut ends = None;
                if p.eat("STARTS") {
                    let mut stops = TAIL.to_vec();
                    stops.push("ENDS");
                    starts = Some(p.expr_until(&stops)?);
                }
                if p.eat("ENDS") {
                    ends = Some(p.expr_until(&TAIL)?);
                }
                ev.schedule = Some(Schedule::Every {
                    value,
                    unit,
                    starts,
                    ends,
                });
            }
        } else if p.is("ON") && p.is_at(1, "COMPLETION") {
            p.i += 2;
            let not = p.eat("NOT");
            p.want("PRESERVE")?;
            ev.preserve = Some(!not);
        } else if p.eat("ENABLE") {
            ev.status = Some(EvStatus::Enabled);
        } else if p.eat("DISABLE") {
            if p.is("ON") && (p.is_at(1, "SLAVE") || p.is_at(1, "REPLICA")) {
                p.i += 2;
                ev.status = Some(EvStatus::SlaveDisabled);
            } else {
                ev.status = Some(EvStatus::Disabled);
            }
        } else if p.eat("COMMENT") {
            match p.peek().map(|t| &t.t) {
                Some(T::Str(s)) => {
                    ev.comment = Some(s.clone());
                    p.i += 1;
                }
                _ => return Err(p.err()),
            }
        } else if !create && p.is("RENAME") && p.is_at(1, "TO") {
            p.i += 2;
            ev.rename = Some(p.qualified()?);
        } else {
            break;
        }
    }
    if p.eat("DO") {
        let body_from = p.at();
        if p.peek().is_none() {
            return Err(p.err());
        }
        ev.body = Some(p.statement(true)?);
        ev.body_text = Some(
            text[body_from..]
                .trim_end()
                .trim_end_matches(';')
                .trim_end()
                .to_owned(),
        );
    } else if create {
        return Err(p.err());
    }
    while p.eat_op(";") {}
    if p.peek().is_some() {
        return Err(p.err());
    }
    if !create
        && ev.schedule.is_none()
        && ev.preserve.is_none()
        && ev.status.is_none()
        && ev.comment.is_none()
        && ev.rename.is_none()
        && ev.body.is_none()
        && ev.definer.is_none()
    {
        return Err(p.err());
    }
    Ok(ev)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_and_its_schedule_read() {
        let e = parse_event(
            "CREATE EVENT IF NOT EXISTS e1 ON SCHEDULE EVERY '1:30' HOUR_MINUTE STARTS NOW() + INTERVAL 1 DAY ENDS '2031-01-01' ON COMPLETION PRESERVE DISABLE ON SLAVE COMMENT 'it''s' DO BEGIN INSERT INTO t VALUES (1); END",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(e.name, "e1");
        assert_eq!(
            e.schedule,
            Some(Schedule::Every {
                value: "'1:30'".into(),
                unit: "HOUR_MINUTE".into(),
                starts: Some("NOW() + INTERVAL 1 DAY".into()),
                ends: Some("'2031-01-01'".into()),
            })
        );
        assert_eq!(e.preserve, Some(true));
        assert_eq!(e.status, Some(EvStatus::SlaveDisabled));
        assert_eq!(e.comment.as_deref(), Some("it's"));
        assert_eq!(
            e.body_text.as_deref(),
            Some("BEGIN INSERT INTO t VALUES (1); END")
        );
        let a = parse_event("ALTER EVENT d.e1 RENAME TO e2 ENABLE").unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(a.rename, Some((None, "e2".into())));
        assert_eq!(a.status, Some(EvStatus::Enabled));
        assert!(parse_event("ALTER EVENT e1").is_err());
        assert!(parse_event("CREATE EVENT e1 DO SELECT 1").is_err());
        let at = parse_event("CREATE EVENT e ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO SET @a = 1")
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            at.schedule,
            Some(Schedule::At("CURRENT_TIMESTAMP + INTERVAL 1 HOUR".into()))
        );
    }

    #[test]
    fn a_procedure_and_its_body_read() {
        let c = parse_create(
            "CREATE DEFINER=`root`@`%` PROCEDURE p1(IN a INT, OUT b INT, INOUT c VARCHAR(10) CHARSET utf8mb4) \
             COMMENT 'hi' READS SQL DATA lbl: BEGIN DECLARE x, y INT DEFAULT a*2; \
             DECLARE done INT DEFAULT 0; DECLARE c1 CURSOR FOR SELECT id FROM t WHERE k = a; \
             DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; \
             SELECT x, a INTO y, @u FROM t LIMIT 1; SET b = x + 1, @z = 2; \
             IF a > 1 THEN SELECT 1; ELSEIF a < 0 THEN SELECT CASE WHEN a THEN 2 END; ELSE SET c = 'q'; END IF; \
             w: WHILE x > 0 DO SET x = x - 1; IF x = 3 THEN LEAVE w; END IF; END WHILE w; \
             CALL q(1, b); END lbl",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(c.kind, Kind::Procedure);
        assert_eq!(c.definer, Some(("root".into(), "%".into())));
        assert_eq!(c.params.len(), 3);
        assert_eq!(c.params[2].ty, "VARCHAR(10) CHARSET utf8mb4");
        assert_eq!(c.chars.comment.as_deref(), Some("hi"));
        assert_eq!(c.chars.access, Some(Access::Reads));
        assert!(c.body_text.starts_with("lbl: BEGIN"));
        let Stmt::Block { label, decls, body } = &c.body else {
            panic!("a block")
        };
        assert_eq!(label.as_deref(), Some("lbl"));
        assert_eq!(decls.len(), 4);
        assert_eq!(body.len(), 5);
        match &body[0] {
            Stmt::Sql { text, into, .. } => {
                assert_eq!(text, "SELECT x, a  FROM t LIMIT 1");
                assert_eq!(
                    into.as_deref(),
                    Some(&[Target::Local("y".into()), Target::User("u".into())][..])
                );
            }
            s => panic!("{s:?}"),
        }
        match &body[1] {
            Stmt::Set(a) => assert_eq!(
                a,
                &vec![
                    Assign::Local("b".into(), "x + 1".into()),
                    Assign::Other("@z = 2".into())
                ]
            ),
            s => panic!("{s:?}"),
        }
        assert!(matches!(&body[2], Stmt::If { arms, other: Some(_) } if arms.len() == 2));
        assert!(matches!(&body[3], Stmt::While { label: Some(l), .. } if l == "w"));
        assert!(
            matches!(&body[4], Stmt::Call { name, args, .. } if name == "q" && args.len() == 2)
        );
    }

    #[test]
    fn a_function_with_a_one_statement_body_reads() {
        let c = parse_create("CREATE FUNCTION db1.f(s CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ', s, '!')")
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(c.kind, Kind::Function);
        assert_eq!(c.returns.as_deref(), Some("CHAR(50)"));
        assert_eq!(c.chars.deterministic, Some(true));
        assert!(matches!(c.body, Stmt::Return(ref e) if e == "CONCAT('Hello, ', s, '!')"));
        assert_eq!(c.body_text, "RETURN CONCAT('Hello, ', s, '!')");
    }

    #[test]
    fn mismatched_end_labels_and_bad_sqlstates_are_refused() {
        let e = parse_create("CREATE PROCEDURE p() a: BEGIN END b").unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 1310);
        let e = parse_create(
            "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR SQLSTATE '00000' SET @a = 1; END",
        )
        .unwrap_err();
        assert_eq!(e.protocol().mysql_errno, 1407);
        assert_eq!(
            user_host("'bob'@'local host'"),
            Some(("bob".into(), "local host".into()))
        );
    }
}
