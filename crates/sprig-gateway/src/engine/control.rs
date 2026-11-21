//! Transaction-control statements, read from their text.
//!
//! The session's transaction on the engine is the gateway's own, so these
//! statements never reach MariaDB while it is open:
//!
//! | Statement | [`Control`] |
//! |---|---|
//! | `BEGIN [WORK]`, `START TRANSACTION [READ ONLY \| READ WRITE \| WITH CONSISTENT SNAPSHOT], ...` | `Begin` |
//! | `COMMIT [WORK] [AND [NO] CHAIN] [[NO] RELEASE]` | `Commit` |
//! | `ROLLBACK [WORK] [AND [NO] CHAIN] [[NO] RELEASE]` | `Rollback` |
//! | `SAVEPOINT s`, `ROLLBACK [WORK] TO [SAVEPOINT] s`, `RELEASE SAVEPOINT s` | `Savepoint`, `RollbackTo`, `Release` |
//! | `SET [SESSION \| GLOBAL] TRANSACTION ISOLATION LEVEL ... \| READ ONLY \| READ WRITE` | `SetTransaction` |
//! | `SET ... autocommit = v`, `SET ... innodb_lock_wait_timeout = n`, `SET NAMES`, `SET character_set_results` | `Set` |
//! | `XA START \| END \| PREPARE \| COMMIT \| ROLLBACK xid`, `XA RECOVER` | `Xa` |
//! | `LOCK TABLES ...`, `UNLOCK TABLES` | `LockTables`, `UnlockTables` |
//!
//! A text that isn't exactly one of these forms is not a control
//! statement ([`parse`] gives `None`): a compound `BEGIN ... END`, a
//! malformed `START TRANSACTION`, anything inside an executable comment.

use sprig_engine::txn::Isolation;

/// The scope of `SET TRANSACTION`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    /// No scope word: the next transaction only.
    Next,
    /// `SESSION` or `LOCAL`.
    Session,
    /// `GLOBAL`.
    Global,
}

/// A transaction-control statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Control {
    /// `BEGIN`, `START TRANSACTION`.
    Begin {
        /// `READ ONLY` (`Some(true)`) or `READ WRITE` (`Some(false)`).
        read_only: Option<bool>,
        /// `WITH CONSISTENT SNAPSHOT`.
        snapshot: bool,
    },
    /// `COMMIT`.
    Commit {
        /// `AND CHAIN` (`Some(true)`), `AND NO CHAIN` (`Some(false)`).
        chain: Option<bool>,
        /// `RELEASE` (`Some(true)`), `NO RELEASE` (`Some(false)`).
        release: Option<bool>,
    },
    /// `ROLLBACK`.
    Rollback {
        /// As for `Commit`.
        chain: Option<bool>,
        /// As for `Commit`.
        release: Option<bool>,
    },
    /// `SAVEPOINT name`.
    Savepoint(String),
    /// `ROLLBACK TO [SAVEPOINT] name`.
    RollbackTo(String),
    /// `RELEASE SAVEPOINT name`.
    Release(String),
    /// `SET [scope] TRANSACTION ...`.
    SetTransaction {
        /// The scope.
        scope: Scope,
        /// `ISOLATION LEVEL ...`.
        isolation: Option<Level>,
        /// `READ ONLY` (`Some(true)`) or `READ WRITE` (`Some(false)`).
        read_only: Option<bool>,
    },
    /// A `SET` that assigns the session's `autocommit` or
    /// `innodb_lock_wait_timeout`, or its results character set (MariaDB
    /// runs it too; the engine follows these).
    Set {
        /// `autocommit`, when assigned.
        autocommit: Option<Switch>,
        /// `innodb_lock_wait_timeout`, when assigned.
        lock_wait: Option<LockWait>,
        /// The results character set is set (`SET NAMES`, `SET CHARACTER
        /// SET`, `SET character_set_results`): from here on MySQL reports
        /// that character set's collation, not the handshake's.
        results_charset: bool,
    },
    /// An `XA` statement.
    Xa(Xa),
    /// `LOCK TABLE[S] ...`, with the tables it names: each one's database
    /// qualifier, if written, and its name.
    LockTables(Vec<(Option<String>, String)>),
    /// `UNLOCK TABLE[S]`.
    UnlockTables,
}

/// A transaction branch identifier: `gtrid [, bqual [, formatID]]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Xid {
    /// The global transaction id (up to 64 bytes).
    pub gtrid: Vec<u8>,
    /// The branch qualifier (up to 64 bytes).
    pub bqual: Vec<u8>,
    /// The format id (1 when not given).
    pub format: u32,
}

impl Xid {
    /// The key the engine keeps a branch of `tenant` under: the tenant
    /// first, so one tenant's branches are listed and found apart from
    /// every other's.
    pub(crate) fn engine_key(&self, tenant: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(14 + self.gtrid.len() + self.bqual.len());
        k.extend_from_slice(&tenant.to_be_bytes());
        k.extend_from_slice(&self.format.to_be_bytes());
        k.push(u8::try_from(self.gtrid.len()).unwrap_or(u8::MAX));
        k.push(u8::try_from(self.bqual.len()).unwrap_or(u8::MAX));
        k.extend_from_slice(&self.gtrid);
        k.extend_from_slice(&self.bqual);
        k
    }

    /// The identifier an engine key of `tenant` holds; `None` for another
    /// tenant's key (or one this layer didn't write).
    pub(crate) fn from_engine_key(tenant: u64, key: &[u8]) -> Option<Self> {
        let rest = key.strip_prefix(&tenant.to_be_bytes()[..])?;
        let format = u32::from_be_bytes(rest.get(..4)?.try_into().ok()?);
        let (g, b) = (usize::from(*rest.get(4)?), usize::from(*rest.get(5)?));
        let data = rest.get(6..)?;
        (data.len() == g + b).then(|| Self {
            gtrid: data[..g].to_vec(),
            bqual: data[g..].to_vec(),
            format,
        })
    }
}

/// An `XA` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Xa {
    /// `XA {START | BEGIN} xid [JOIN | RESUME]`.
    Start {
        /// The branch.
        xid: Xid,
        /// `JOIN` or `RESUME` (neither server takes them).
        join: bool,
    },
    /// `XA END xid [SUSPEND [FOR MIGRATE]]`.
    End {
        /// The branch.
        xid: Xid,
        /// `SUSPEND`.
        suspend: bool,
    },
    /// `XA PREPARE xid`.
    Prepare(Xid),
    /// `XA COMMIT xid [ONE PHASE]`.
    Commit {
        /// The branch.
        xid: Xid,
        /// `ONE PHASE`.
        one_phase: bool,
    },
    /// `XA ROLLBACK xid`.
    Rollback(Xid),
    /// `XA RECOVER` (without options).
    Recover,
    /// `XA RECOVER` with an option (`FORMAT = 'SQL'`, `CONVERT XID`).
    RecoverWith,
    /// Any other form (an identifier too long to be one, a word out of
    /// place): not read here; the server it goes to answers it.
    Other,
}

/// The value a `SET` gives `autocommit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Switch {
    /// `1`, `ON`, `TRUE`.
    On,
    /// `0`, `OFF`, `FALSE`.
    Off,
    /// Anything else (`DEFAULT`, an expression): not read.
    Unknown,
}

/// The value a `SET` gives `innodb_lock_wait_timeout`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockWait {
    /// Seconds.
    Seconds(u64),
    /// `DEFAULT`.
    Default,
    /// An expression: not read.
    Unknown,
}

/// An isolation level as the client names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    /// `READ UNCOMMITTED`.
    ReadUncommitted,
    /// `READ COMMITTED`.
    ReadCommitted,
    /// `REPEATABLE READ`.
    RepeatableRead,
    /// `SERIALIZABLE`.
    Serializable,
}

impl Level {
    /// The engine's level (READ UNCOMMITTED runs as READ COMMITTED).
    pub(crate) fn engine(self) -> Isolation {
        match self {
            Level::ReadUncommitted | Level::ReadCommitted => Isolation::ReadCommitted,
            Level::RepeatableRead => Isolation::RepeatableRead,
            Level::Serializable => Isolation::Serializable,
        }
    }

    /// A level from a variable's value (`READ-COMMITTED`, `repeatable read`).
    pub(crate) fn from_value(v: &str) -> Option<Self> {
        let n: String = v
            .trim()
            .trim_matches(|c| c == '\'' || c == '"')
            .chars()
            .map(|c| match c {
                '-' | '_' => ' ',
                c => c.to_ascii_uppercase(),
            })
            .collect();
        match n.as_str() {
            "READ UNCOMMITTED" => Some(Level::ReadUncommitted),
            "READ COMMITTED" => Some(Level::ReadCommitted),
            "REPEATABLE READ" => Some(Level::RepeatableRead),
            "SERIALIZABLE" => Some(Level::Serializable),
            _ => None,
        }
    }
}

/// A token of a control statement.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    /// A bare word, upper case.
    Word(String),
    /// A quoted identifier, as written.
    Quoted(String),
    /// A string literal's bytes (a doubled quote and a backslash escape
    /// each stand for the one byte they name).
    Lit(Vec<u8>),
    /// Any other character.
    Punct(char),
}

/// The tokens of `text`, comments skipped; `None` when it holds something
/// a control statement never does (an executable comment, an unterminated
/// quote).
fn tokens(text: &str) -> Option<Vec<Tok>> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            // Executable comments hold statement text.
            if matches!(b.get(i + 2), Some(b'!')) || text[i + 2..].starts_with("M!") {
                return None;
            }
            let end = text[i + 2..].find("*/")?;
            i += 2 + end + 2;
        } else if c == b'#'
            || (c == b'-'
                && b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace))
        {
            i = text[i..].find('\n').map_or(b.len(), |n| i + n + 1);
        } else if c == b'`' {
            let mut name = String::new();
            let mut j = i + 1;
            loop {
                let rest = text.get(j..)?;
                let end = rest.find('`')?;
                name.push_str(&rest[..end]);
                j += end + 1;
                if b.get(j) == Some(&b'`') {
                    name.push('`');
                    j += 1;
                } else {
                    break;
                }
            }
            out.push(Tok::Quoted(name));
            i = j;
        } else if c == b'\'' || c == b'"' {
            // To the closing quote; a doubled quote or a backslash keeps
            // the next byte inside.
            let mut j = i + 1;
            let mut lit = Vec::new();
            loop {
                match b.get(j)? {
                    b'\\' => {
                        lit.push(match *b.get(j + 1)? {
                            b'n' => b'\n',
                            b't' => b'\t',
                            b'r' => b'\r',
                            b'0' => 0,
                            x => x,
                        });
                        j += 2;
                    }
                    q if *q == c => {
                        if b.get(j + 1) == Some(&c) {
                            lit.push(c);
                            j += 2;
                        } else {
                            break;
                        }
                    }
                    x => {
                        lit.push(*x);
                        j += 1;
                    }
                }
            }
            out.push(Tok::Lit(lit));
            i = j + 1;
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
            let start = i;
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            out.push(Tok::Word(text[start..i].to_ascii_uppercase()));
        } else {
            out.push(Tok::Punct(c as char));
            i += 1;
        }
    }
    // A statement's own terminator.
    while out.last() == Some(&Tok::Punct(';')) {
        out.pop();
    }
    Some(out)
}

/// A cursor over tokens.
struct Cur<'a> {
    t: &'a [Tok],
    /// The text, for names as written.
    text: &'a str,
}

impl Cur<'_> {
    fn word(&mut self, w: &str) -> bool {
        match self.t.first() {
            Some(Tok::Word(x)) if x == w => {
                self.t = &self.t[1..];
                true
            }
            _ => false,
        }
    }

    fn punct(&mut self, c: char) -> bool {
        match self.t.first() {
            Some(Tok::Punct(x)) if *x == c => {
                self.t = &self.t[1..];
                true
            }
            _ => false,
        }
    }

    fn done(&self) -> bool {
        self.t.is_empty()
    }

    /// An identifier: a bare word (as written in the text, found again
    /// without regard to case) or a quoted name.
    fn name(&mut self) -> Option<String> {
        match self.t.first()? {
            Tok::Quoted(n) => {
                let n = n.clone();
                self.t = &self.t[1..];
                Some(n)
            }
            Tok::Word(w) => {
                let up = self.text.to_ascii_uppercase();
                let n = up
                    .rfind(w.as_str())
                    .and_then(|i| self.text.get(i..i + w.len()))
                    .unwrap_or(w)
                    .to_owned();
                self.t = &self.t[1..];
                Some(n)
            }
            Tok::Punct(_) | Tok::Lit(_) => None,
        }
    }
}

/// `[AND [NO] CHAIN] [[NO] RELEASE]`, to the end.
fn completion(c: &mut Cur<'_>) -> Option<(Option<bool>, Option<bool>)> {
    let mut chain = None;
    let mut release = None;
    if c.word("AND") {
        let no = c.word("NO");
        if !c.word("CHAIN") {
            return None;
        }
        chain = Some(!no);
    }
    if c.word("NO") {
        if !c.word("RELEASE") {
            return None;
        }
        release = Some(false);
    } else if c.word("RELEASE") {
        release = Some(true);
    }
    c.done().then_some((chain, release))
}

/// `ISOLATION LEVEL x | READ ONLY | READ WRITE`, comma separated, to the
/// end; each kind at most once.
fn characteristics(c: &mut Cur<'_>) -> Option<(Option<Level>, Option<bool>)> {
    let mut level = None;
    let mut read_only = None;
    loop {
        if c.word("ISOLATION") {
            if !c.word("LEVEL") || level.is_some() {
                return None;
            }
            level = Some(if c.word("READ") {
                if c.word("UNCOMMITTED") {
                    Level::ReadUncommitted
                } else if c.word("COMMITTED") {
                    Level::ReadCommitted
                } else {
                    return None;
                }
            } else if c.word("REPEATABLE") {
                if !c.word("READ") {
                    return None;
                }
                Level::RepeatableRead
            } else if c.word("SERIALIZABLE") {
                Level::Serializable
            } else {
                return None;
            });
        } else if c.word("READ") {
            if read_only.is_some() {
                return None;
            }
            read_only = Some(if c.word("ONLY") {
                true
            } else if c.word("WRITE") {
                false
            } else {
                return None;
            });
        } else {
            return None;
        }
        if !c.punct(',') {
            break;
        }
    }
    c.done().then_some((level, read_only))
}

/// The tokens of the value a `SET` assigns the session variable `name`
/// (upper case), up to the next comma; `None` when the statement doesn't
/// assign it (a user variable of that name and the global value don't
/// count).
fn assigned<'a>(t: &'a [Tok], name: &str) -> Option<&'a [Tok]> {
    // `[@@][scope.]name {= | :=} value`, anywhere in the list.
    let at = t
        .iter()
        .position(|x| matches!(x, Tok::Word(w) if w == name))?;
    let before = |n: usize| at.checked_sub(n).and_then(|i| t.get(i));
    let is_at = |x: Option<&Tok>| x == Some(&Tok::Punct('@'));
    let is_global = |x: Option<&Tok>| matches!(x, Some(Tok::Word(w)) if w == "GLOBAL");
    if (is_at(before(1)) && !is_at(before(2)))
        || is_global(before(1))
        || (before(1) == Some(&Tok::Punct('.')) && is_global(before(2)))
    {
        return None;
    }
    let mut rest = &t[at + 1..];
    if rest.first() == Some(&Tok::Punct(':')) {
        rest = &rest[1..];
    }
    if rest.first() != Some(&Tok::Punct('=')) {
        return None;
    }
    let rest = &rest[1..];
    let end = rest
        .iter()
        .position(|x| matches!(x, Tok::Punct(',' | ';')))
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

/// When `text` is a `SET` that gives the session's `autocommit` a
/// statement parameter (`SET autocommit = ?`): which parameter, counted
/// from 0.
pub(crate) fn autocommit_param(text: &[u8]) -> Option<usize> {
    let first = text
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .map(u8::to_ascii_uppercase)?;
    if first != b'S' {
        return None;
    }
    let text = std::str::from_utf8(text).ok()?;
    let toks = tokens(text)?;
    if toks.first() != Some(&Tok::Word("SET".to_owned())) {
        return None;
    }
    let value = assigned(&toks, "AUTOCOMMIT")?;
    if value != [Tok::Punct('?')] {
        return None;
    }
    // The parameters before it: every `?` left of the variable's name.
    let at = toks
        .iter()
        .position(|x| matches!(x, Tok::Word(w) if w == "AUTOCOMMIT"))?;
    Some(toks[..at].iter().filter(|t| **t == Tok::Punct('?')).count())
}

/// What a `SET` assigns of the variables the engine's transactions
/// follow, if any.
fn set_vars(t: &[Tok]) -> Option<Control> {
    let autocommit = assigned(t, "AUTOCOMMIT").map(|v| match v {
        [Tok::Word(w)] => match w.as_str() {
            "1" | "ON" | "TRUE" => Switch::On,
            "0" | "OFF" | "FALSE" => Switch::Off,
            _ => Switch::Unknown,
        },
        // A longer expression (`1 + 0`, `@v`) isn't read.
        _ => Switch::Unknown,
    });
    let lock_wait = assigned(t, "INNODB_LOCK_WAIT_TIMEOUT").map(|v| match v {
        [Tok::Word(w)] if w == "DEFAULT" => LockWait::Default,
        [Tok::Word(w)] => w.parse().map_or(LockWait::Unknown, LockWait::Seconds),
        _ => LockWait::Unknown,
    });
    let results_charset = assigned(t, "CHARACTER_SET_RESULTS").is_some();
    if autocommit.is_none() && lock_wait.is_none() && !results_charset {
        return None;
    }
    Some(Control::Set {
        autocommit,
        lock_wait,
        results_charset,
    })
}

/// Pairs of hex digits as bytes.
fn unhex(h: &[u8]) -> Option<Vec<u8>> {
    if !h.len().is_multiple_of(2) {
        return None;
    }
    h.chunks(2)
        .map(|p| {
            let d = |c: u8| {
                char::from(c)
                    .to_digit(16)
                    .and_then(|v| u8::try_from(v).ok())
            };
            Some(d(p[0])? << 4 | d(p[1])?)
        })
        .collect()
}

/// One part of an xid: a string literal, `X'hex'` or `0xhex`; at most 64
/// bytes.
fn xid_part(c: &mut Cur<'_>) -> Option<Vec<u8>> {
    let (v, used) = match c.t {
        [Tok::Lit(v), ..] => (v.clone(), 1),
        [Tok::Word(w), Tok::Lit(h), ..] if w == "X" => (unhex(h)?, 2),
        [Tok::Word(w), ..] if w.starts_with("0X") && w.len() > 2 => (unhex(&w.as_bytes()[2..])?, 1),
        _ => return None,
    };
    c.t = &c.t[used..];
    (v.len() <= 64).then_some(v)
}

/// `gtrid [, bqual [, formatID]]`.
fn xid(c: &mut Cur<'_>) -> Option<Xid> {
    let gtrid = xid_part(c)?;
    let mut bqual = Vec::new();
    let mut format = 1;
    if c.punct(',') {
        bqual = xid_part(c)?;
        if c.punct(',') {
            let Some(Tok::Word(n)) = c.t.first() else {
                return None;
            };
            format = n.parse().ok()?;
            c.t = &c.t[1..];
        }
    }
    Some(Xid {
        gtrid,
        bqual,
        format,
    })
}

/// The `XA` statement after its first word; `None` for a form not read.
fn xa(c: &mut Cur<'_>) -> Option<Xa> {
    let x = if c.word("START") || c.word("BEGIN") {
        let xid = xid(c)?;
        let join = c.word("JOIN") || c.word("RESUME");
        Xa::Start { xid, join }
    } else if c.word("END") {
        let xid = xid(c)?;
        let suspend = c.word("SUSPEND");
        if suspend && c.word("FOR") && !c.word("MIGRATE") {
            return None;
        }
        Xa::End { xid, suspend }
    } else if c.word("PREPARE") {
        Xa::Prepare(xid(c)?)
    } else if c.word("COMMIT") {
        let xid = xid(c)?;
        let one_phase = c.word("ONE");
        if one_phase && !c.word("PHASE") {
            return None;
        }
        Xa::Commit { xid, one_phase }
    } else if c.word("ROLLBACK") {
        Xa::Rollback(xid(c)?)
    } else if c.word("RECOVER") {
        return Some(if c.done() {
            Xa::Recover
        } else {
            Xa::RecoverWith
        });
    } else {
        return None;
    };
    c.done().then_some(x)
}

/// The tables of `LOCK TABLES t [[AS] alias] READ | WRITE ..., ...`, as far
/// as they can be read (a list that doesn't read gives what was found).
fn locked_tables(c: &mut Cur<'_>) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    loop {
        let Some(first) = c.name() else {
            break;
        };
        let name = if c.punct('.') {
            match c.name() {
                Some(t) => (Some(first), t),
                None => break,
            }
        } else {
            (None, first)
        };
        out.push(name);
        // To the next comma: the alias and the lock kind.
        while let Some(t) = c.t.first() {
            let comma = *t == Tok::Punct(',');
            c.t = &c.t[1..];
            if comma {
                break;
            }
        }
        if c.done() {
            break;
        }
    }
    out
}

/// What a `SET` gives the session's `lock_wait_timeout` (how long a
/// definition change waits for the statements and transactions using its
/// table); `None` when the text doesn't assign it. A value that isn't a
/// plain number counts as `DEFAULT`.
pub(crate) fn metadata_wait(text: &[u8]) -> Option<LockWait> {
    let first = text.iter().find(|b| !b.is_ascii_whitespace())?;
    if !first.eq_ignore_ascii_case(&b'S') {
        return None;
    }
    let toks = tokens(std::str::from_utf8(text).ok()?)?;
    if !matches!(toks.first(), Some(Tok::Word(w)) if w == "SET") {
        return None;
    }
    let v = assigned(&toks, "LOCK_WAIT_TIMEOUT")?;
    Some(match v {
        [Tok::Word(w)] => w.parse().map_or(LockWait::Default, LockWait::Seconds),
        _ => LockWait::Default,
    })
}

/// The control statement `text` is, if it is one.
#[allow(clippy::too_many_lines)] // one branch per statement
pub(crate) fn parse(text: &[u8]) -> Option<Control> {
    // The first byte decides for nearly every statement (no allocation
    // for a `SELECT` or an `INSERT`).
    let first = text
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .map(u8::to_ascii_uppercase)?;
    if !matches!(
        first,
        b'B' | b'S' | b'C' | b'R' | b'X' | b'L' | b'U' | b'/' | b'#' | b'-'
    ) {
        return None;
    }
    let text = std::str::from_utf8(text).ok()?;
    let toks = tokens(text)?;
    let mut c = Cur { t: &toks, text };
    if c.word("BEGIN") {
        let _ = c.word("WORK");
        return c.done().then_some(Control::Begin {
            read_only: None,
            snapshot: false,
        });
    }
    if c.word("START") {
        if !c.word("TRANSACTION") {
            return None;
        }
        let mut read_only = None;
        let mut snapshot = false;
        if !c.done() {
            loop {
                if c.word("WITH") {
                    if !(c.word("CONSISTENT") && c.word("SNAPSHOT")) || snapshot {
                        return None;
                    }
                    snapshot = true;
                } else if c.word("READ") {
                    if read_only.is_some() {
                        return None;
                    }
                    read_only = Some(if c.word("ONLY") {
                        true
                    } else if c.word("WRITE") {
                        false
                    } else {
                        return None;
                    });
                } else {
                    return None;
                }
                if !c.punct(',') {
                    break;
                }
            }
        }
        return c.done().then_some(Control::Begin {
            read_only,
            snapshot,
        });
    }
    if c.word("COMMIT") {
        let _ = c.word("WORK");
        let (chain, release) = completion(&mut c)?;
        return Some(Control::Commit { chain, release });
    }
    if c.word("ROLLBACK") {
        let _ = c.word("WORK");
        if c.word("TO") {
            let _ = c.word("SAVEPOINT");
            let name = c.name()?;
            return c.done().then_some(Control::RollbackTo(name));
        }
        let (chain, release) = completion(&mut c)?;
        return Some(Control::Rollback { chain, release });
    }
    if c.word("SAVEPOINT") {
        let name = c.name()?;
        return c.done().then_some(Control::Savepoint(name));
    }
    if c.word("RELEASE") {
        if !c.word("SAVEPOINT") {
            return None;
        }
        let name = c.name()?;
        return c.done().then_some(Control::Release(name));
    }
    if c.word("SET") {
        let scope = if c.word("SESSION") || c.word("LOCAL") {
            Scope::Session
        } else if c.word("GLOBAL") {
            Scope::Global
        } else {
            Scope::Next
        };
        if c.word("TRANSACTION") {
            let (isolation, read_only) = characteristics(&mut c)?;
            return Some(Control::SetTransaction {
                scope,
                isolation,
                read_only,
            });
        }
        // `SET STATEMENT ... FOR ...` sets nothing for the session.
        if c.word("STATEMENT") {
            return None;
        }
        if c.word("NAMES") || c.word("CHARSET") || (c.word("CHARACTER") && c.word("SET")) {
            return Some(Control::Set {
                autocommit: None,
                lock_wait: None,
                results_charset: true,
            });
        }
        return set_vars(&toks);
    }
    if c.word("XA") {
        return Some(Control::Xa(xa(&mut c).unwrap_or(Xa::Other)));
    }
    if c.word("LOCK") {
        if !(c.word("TABLES") || c.word("TABLE")) {
            return None;
        }
        return Some(Control::LockTables(locked_tables(&mut c)));
    }
    if c.word("UNLOCK") {
        if !(c.word("TABLES") || c.word("TABLE")) {
            return None;
        }
        return c.done().then_some(Control::UnlockTables);
    }
    None
}
