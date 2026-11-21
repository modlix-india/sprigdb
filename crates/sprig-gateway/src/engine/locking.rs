//! The locking clause of a `SELECT`, read off the end of its text.
//!
//! | Clause | Lock | Port |
//! |---|---|---|
//! | `FOR UPDATE` | exclusive | both |
//! | `LOCK IN SHARE MODE` | shared | both |
//! | `FOR SHARE` | shared | MySQL |
//! | `... NOWAIT`, `... SKIP LOCKED` | don't wait | both (not after MySQL's `LOCK IN SHARE MODE`) |
//! | `... WAIT n` | wait up to `n` seconds | MariaDB |
//! | `... OF t` | names the table | MySQL |
//!
//! The statement parser reads `FOR UPDATE` and `FOR SHARE` only, so the
//! clause is split off first ([`split`]) and the rest parsed as a plain
//! `SELECT`.

use sprig_engine::txn::Wait;

/// How the clause was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Form {
    /// `FOR UPDATE`.
    ForUpdate,
    /// `FOR SHARE`.
    ForShare,
    /// `LOCK IN SHARE MODE`.
    ShareMode,
}

/// A locking clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Clause {
    /// The form.
    pub form: Form,
    /// What to do about a row another transaction holds.
    pub wait: Wait,
    /// `WAIT n` was written (MariaDB's syntax).
    pub wait_n: bool,
    /// `OF` names (MySQL's syntax), as written.
    pub of: Vec<String>,
    /// Where in the statement's text the lock word (`UPDATE`, `SHARE`),
    /// the `OF` list and the wait option start: a port that doesn't take
    /// one of them reports its syntax error from there.
    pub at: Places,
}

/// Byte offsets into the statement's text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Places {
    /// The word after `FOR`.
    pub lock_word: usize,
    /// `OF`.
    pub of: Option<usize>,
    /// `NOWAIT`, `SKIP` or `WAIT`.
    pub wait: Option<usize>,
}

impl Clause {
    /// Whether the lock is exclusive.
    pub(crate) fn exclusive(&self) -> bool {
        self.form == Form::ForUpdate
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Word,
    Punct(u8),
    Other,
}

/// A token at parenthesis depth 0, with where it starts.
struct Tok<'a> {
    kind: Kind,
    text: &'a str,
    start: usize,
}

impl Tok<'_> {
    fn is(&self, word: &str) -> bool {
        self.kind == Kind::Word && self.text.eq_ignore_ascii_case(word)
    }
}

/// The depth-0 tokens of `text`; literals, quoted names and anything in
/// parentheses are single `Other` tokens. `None` on an unterminated quote
/// or comment.
fn tokens(text: &str) -> Option<Vec<Tok<'_>>> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0u32;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        let mut kind = Kind::Other;
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        } else if c == b'/' && b.get(i + 1) == Some(&b'*') {
            i += 2 + text[i + 2..].find("*/")? + 2;
            continue;
        } else if c == b'#'
            || (c == b'-'
                && b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(u8::is_ascii_whitespace))
        {
            i = text[i..].find('\n').map_or(b.len(), |n| i + n + 1);
            continue;
        } else if c == b'\'' || c == b'"' || c == b'`' {
            let mut j = i + 1;
            loop {
                match *b.get(j)? {
                    b'\\' if c != b'`' => j += 2,
                    q if q == c => {
                        if b.get(j + 1) == Some(&c) {
                            j += 2;
                        } else {
                            break;
                        }
                    }
                    _ => j += 1,
                }
            }
            i = j + 1;
        } else if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
            while i < b.len()
                && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$' || b[i] >= 0x80)
            {
                i += 1;
            }
            kind = Kind::Word;
        } else {
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth = depth.saturating_sub(1);
            }
            kind = Kind::Punct(c);
            i += 1;
        }
        if depth == 0 && c != b')' {
            out.push(Tok {
                kind,
                text: text.get(start..i)?,
                start,
            });
        } else if out.last().is_none_or(|t: &Tok<'_>| t.kind != Kind::Other) {
            // Something in parentheses: one opaque token.
            out.push(Tok {
                kind: Kind::Other,
                text: "",
                start,
            });
        }
    }
    Some(out)
}

/// Splits a trailing locking clause off `text`: the statement without it,
/// and the clause. `None` when `text` isn't a `SELECT` ending in one.
pub(crate) fn split(text: &str) -> Option<(&str, Clause)> {
    // Cheap refusals first: nearly every statement stops here.
    let head = text.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
    if !head
        .get(..6)
        .is_some_and(|w| w.eq_ignore_ascii_case("SELECT"))
    {
        return None;
    }
    let tail = &text.as_bytes()[text.len().saturating_sub(64)..];
    let has = |w: &[u8]| tail.windows(w.len()).any(|x| x.eq_ignore_ascii_case(w));
    if !(has(b"UPDATE") || has(b"SHARE")) {
        return None;
    }
    let toks = tokens(text)?;
    let mut n = toks.len();
    while n > 0 && toks[n - 1].kind == Kind::Punct(b';') {
        n -= 1;
    }
    let mut wait = Wait::Block;
    let mut wait_n = false;
    let mut at = Places::default();
    let before = n;
    if n >= 2 && toks[n - 2].is("SKIP") && toks[n - 1].is("LOCKED") {
        wait = Wait::SkipLocked;
        n -= 2;
    } else if n >= 1 && toks[n - 1].is("NOWAIT") {
        wait = Wait::NoWait;
        n -= 1;
    } else if n >= 2
        && toks[n - 2].is("WAIT")
        && toks[n - 1].kind == Kind::Word
        && let Ok(secs) = toks[n - 1].text.parse::<u64>()
    {
        wait = Wait::Seconds(u32::try_from(secs).unwrap_or(u32::MAX));
        wait_n = true;
        n -= 2;
    }
    if n < before {
        at.wait = Some(toks[n].start);
    }
    // `OF a, b`: names and commas back to `OF`.
    let mut of = Vec::new();
    let mut m = n;
    while m >= 1
        && (matches!(toks[m - 1].kind, Kind::Punct(b',' | b'.'))
            || toks[m - 1].kind == Kind::Other
            || (toks[m - 1].kind == Kind::Word && !toks[m - 1].is("OF")))
    {
        m -= 1;
        if m + 3 < n.saturating_sub(16) {
            break;
        }
    }
    if m >= 3
        && m < n
        && toks[m - 1].is("OF")
        && toks[m - 3].is("FOR")
        && (toks[m - 2].is("UPDATE") || toks[m - 2].is("SHARE"))
    {
        of = toks[m..n]
            .iter()
            .filter(|t| !matches!(t.kind, Kind::Punct(_)))
            .map(|t| t.text.trim_matches('`').to_owned())
            .collect();
        at.of = Some(toks[m - 1].start);
        n = m - 1;
    }
    let (form, cut) = if n >= 2 && toks[n - 2].is("FOR") && toks[n - 1].is("UPDATE") {
        (Form::ForUpdate, n - 2)
    } else if n >= 2 && toks[n - 2].is("FOR") && toks[n - 1].is("SHARE") {
        (Form::ForShare, n - 2)
    } else if n >= 4
        && toks[n - 4].is("LOCK")
        && toks[n - 3].is("IN")
        && toks[n - 2].is("SHARE")
        && toks[n - 1].is("MODE")
    {
        (Form::ShareMode, n - 4)
    } else {
        return None;
    };
    // The clause follows something (`SELECT 1 FOR UPDATE` at least).
    if cut == 0 {
        return None;
    }
    at.lock_word = toks[cut + 1].start;
    Some((
        &text[..toks[cut].start],
        Clause {
            form,
            wait,
            wait_n,
            of,
            at,
        },
    ))
}
