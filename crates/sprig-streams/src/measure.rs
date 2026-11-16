//! How many bytes a value of an `INSERT INTO <stream> VALUES (...)` row
//! writes, read from the statement's text, for the publish size check
//! (`stream.max_message_bytes`, 39944).
//!
//! Every literal form MariaDB has is measured as the value it stands for:
//!
//! - strings `'..'` and `".."` with their escapes (as `sql_mode` reads
//!   them), quotes doubled, and adjacent strings joined (`'a' 'b'` is
//!   `ab`); national strings `N'..'`;
//! - hexadecimal `X'..'` and `0x..`, bit strings `B'..'` and `0b..` (whole
//!   bytes);
//! - a `_charset` introducer before any of them (`_utf8mb4 X'..'`, the
//!   form the engine tier's `db.publish` writes), for the character sets
//!   whose bytes are written as they are: `binary`, `ascii`, `utf8`,
//!   `utf8mb3`, `utf8mb4`;
//! - numbers as MariaDB writes them into a text column (`007` is `7`,
//!   `.5` is `0.5`), with a sign; `TRUE` and `FALSE` (`1`, `0`); `NULL`
//!   and `DEFAULT` (the outbox's columns default to `NULL`);
//! - `CONCAT(...)` of measured values, and parentheses.
//!
//! Anything else (a column, a function, a variable, a subquery, a
//! placeholder, a number with an exponent, another introducer) is
//! [`Size::Unknown`]: the gateway then has MariaDB evaluate the rows and
//! measures what comes back.

use sprig_sql::lex::{Token, TokenKind, string_value};

/// What a value expression writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Size {
    /// `NULL`: nothing.
    Null,
    /// Exactly this many bytes.
    Bytes(u64),
    /// Not known from the text.
    Unknown,
}

impl Size {
    /// The bytes it counts for in a record (`NULL` counts 0); `None` when
    /// unknown.
    #[must_use]
    pub const fn bytes(self) -> Option<u64> {
        match self {
            Size::Null => Some(0),
            Size::Bytes(n) => Some(n),
            Size::Unknown => None,
        }
    }
}

/// Character sets whose introduced literals are written byte for byte.
const AS_IS: [&str; 5] = ["binary", "ascii", "utf8", "utf8mb3", "utf8mb4"];

/// Measures one value expression: `toks` are its non-trivia tokens in
/// `src`.
#[must_use]
pub fn value(src: &[u8], toks: &[Token], no_backslash_escapes: bool) -> Size {
    let text = |t: &Token| &src[t.range()];
    let is_word = |t: &Token, w: &str| {
        t.kind == TokenKind::Word && text(t).eq_ignore_ascii_case(w.as_bytes())
    };
    let punct = |t: &Token, p: u8| t.kind == TokenKind::Punct && text(t) == [p];
    match toks {
        [] => Size::Unknown,
        [t] if is_word(t, "NULL") || is_word(t, "DEFAULT") => Size::Null,
        [t] if is_word(t, "TRUE") || is_word(t, "FALSE") => Size::Bytes(1),
        [t] if t.kind == TokenKind::Number => number(text(t)).map_or(Size::Unknown, Size::Bytes),
        [sign, t]
            if sign.kind == TokenKind::Op
                && matches!(text(sign), b"-" | b"+")
                && t.kind == TokenKind::Number =>
        {
            number(text(t)).map_or(Size::Unknown, |n| {
                Size::Bytes(n + u64::from(text(sign) == b"-"))
            })
        }
        [open, inner @ .., close] if punct(open, b'(') && punct(close, b')') => {
            if balanced(src, inner) {
                value(src, inner, no_backslash_escapes)
            } else {
                Size::Unknown
            }
        }
        [f, open, args @ .., close]
            if is_word(f, "CONCAT") && punct(open, b'(') && punct(close, b')') =>
        {
            concat(src, args, no_backslash_escapes)
        }
        [first, rest @ ..] if first.kind == TokenKind::Word && text(first).starts_with(b"_") => {
            let cs = String::from_utf8_lossy(&text(first)[1..]).to_ascii_lowercase();
            if rest.is_empty() || !AS_IS.contains(&cs.as_str()) {
                return Size::Unknown;
            }
            literal(src, rest, no_backslash_escapes)
        }
        _ => literal(src, toks, no_backslash_escapes),
    }
}

/// One literal (adjacent strings joined).
fn literal(src: &[u8], toks: &[Token], nbe: bool) -> Size {
    let text = |t: &Token| &src[t.range()];
    match toks {
        [t] => match t.kind {
            TokenKind::String | TokenKind::NationalString => strings(src, toks, nbe),
            TokenKind::HexString => {
                // X'..': two digits a byte.
                let digits = text(t).len().saturating_sub(3);
                Size::Bytes((digits / 2) as u64)
            }
            TokenKind::HexNumber => {
                let digits = text(t).len().saturating_sub(2);
                Size::Bytes(digits.div_ceil(2) as u64)
            }
            TokenKind::BitString => {
                let bits = text(t).len().saturating_sub(3);
                Size::Bytes(bits.div_ceil(8) as u64)
            }
            TokenKind::BitNumber => {
                let bits = text(t).len().saturating_sub(2);
                Size::Bytes(bits.div_ceil(8) as u64)
            }
            _ => Size::Unknown,
        },
        _ => strings(src, toks, nbe),
    }
}

/// Adjacent string literals, joined.
fn strings(src: &[u8], toks: &[Token], nbe: bool) -> Size {
    let mut n = 0u64;
    for (i, t) in toks.iter().enumerate() {
        let tok = &src[t.range()];
        let v = match t.kind {
            TokenKind::String => string_value(tok, nbe),
            // Only the first of a run may be national.
            TokenKind::NationalString if i == 0 => string_value(&tok[1..], nbe),
            _ => None,
        };
        match v {
            Some(v) => n += v.len() as u64,
            None => return Size::Unknown,
        }
    }
    Size::Bytes(n)
}

/// A decimal number's text as MariaDB writes it into a text column, in
/// bytes: no leading zeros (`0` before a point), the fraction as written.
/// `None` with an exponent (a double's text depends on its value).
fn number(t: &[u8]) -> Option<u64> {
    if t.iter().any(|b| !(b.is_ascii_digit() || *b == b'.')) {
        return None;
    }
    let (int, frac) = match t.iter().position(|b| *b == b'.') {
        Some(p) => (&t[..p], Some(&t[p + 1..])),
        None => (t, None),
    };
    let int = int.iter().skip_while(|b| **b == b'0').count().max(1);
    Some((int + frac.map_or(0, |f| 1 + f.len())) as u64)
}

/// Whether `toks` is one balanced run (the parentheses around it belong
/// together).
fn balanced(src: &[u8], toks: &[Token]) -> bool {
    let mut depth = 0i64;
    for t in toks {
        if t.kind == TokenKind::Punct {
            match &src[t.range()] {
                b"(" => depth += 1,
                b")" => {
                    depth -= 1;
                    if depth < 0 {
                        return false;
                    }
                }
                _ => {}
            }
        }
    }
    depth == 0
}

/// `CONCAT(args)`: the sum of its arguments, `NULL` when one is.
fn concat(src: &[u8], args: &[Token], nbe: bool) -> Size {
    if !balanced(src, args) || args.is_empty() {
        return Size::Unknown;
    }
    let mut total = Some(0u64);
    let mut null = false;
    for a in split_commas(src, args) {
        match value(src, a, nbe) {
            Size::Unknown => return Size::Unknown,
            Size::Null => null = true,
            Size::Bytes(n) => total = total.map(|t| t + n),
        }
    }
    match (null, total) {
        (true, _) => Size::Null,
        (false, Some(n)) => Size::Bytes(n),
        (false, None) => Size::Unknown,
    }
}

/// Splits `toks` at its top-level commas.
pub(crate) fn split_commas<'t>(src: &[u8], toks: &'t [Token]) -> Vec<&'t [Token]> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, t) in toks.iter().enumerate() {
        if t.kind != TokenKind::Punct {
            continue;
        }
        match &src[t.range()] {
            b"(" => depth += 1,
            b")" => depth = depth.saturating_sub(1),
            b"," if depth == 0 => {
                out.push(&toks[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&toks[start..]);
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sprig_sql::lex::{LexOptions, lex};

    fn size(expr: &str) -> Size {
        size_with(expr, false)
    }

    fn size_with(expr: &str, nbe: bool) -> Size {
        let opts = LexOptions::mariadb().no_backslash_escapes(nbe);
        let toks: Vec<Token> = lex(expr.as_bytes(), &opts)
            .unwrap()
            .into_iter()
            .filter(|t| !t.kind.is_trivia())
            .collect();
        value(expr.as_bytes(), &toks, nbe)
    }

    #[test]
    fn every_literal_form_is_measured() {
        assert_eq!(size("'abc'"), Size::Bytes(3));
        assert_eq!(size("\"abc\""), Size::Bytes(3));
        assert_eq!(size("'it''s'"), Size::Bytes(4));
        assert_eq!(size(r"'a\nb'"), Size::Bytes(3));
        assert_eq!(size_with(r"'a\nb'", true), Size::Bytes(4));
        assert_eq!(size(r"'50\%'"), Size::Bytes(4));
        assert_eq!(size("'ab' 'cd'  \"e\""), Size::Bytes(5));
        assert_eq!(size("N'héllo'"), Size::Bytes(6));
        assert_eq!(size("X'0A0B0C'"), Size::Bytes(3));
        assert_eq!(size("x''"), Size::Bytes(0));
        assert_eq!(size("0x0A0B0"), Size::Bytes(3));
        assert_eq!(size("B'101'"), Size::Bytes(1));
        assert_eq!(size("b'111111111'"), Size::Bytes(2));
        assert_eq!(size("0b1"), Size::Bytes(1));
        assert_eq!(size("_utf8mb4 X'414243'"), Size::Bytes(3));
        assert_eq!(size("_binary'abc'"), Size::Bytes(3));
        assert_eq!(size("_UTF8MB4'a' 'b'"), Size::Bytes(2));
        assert_eq!(size("_utf16'ab'"), Size::Unknown);
        assert_eq!(size("_latin1'é'"), Size::Unknown);
        assert_eq!(size("NULL"), Size::Null);
        assert_eq!(size("default"), Size::Null);
        assert_eq!(size("TRUE"), Size::Bytes(1));
    }

    #[test]
    fn numbers_as_mariadb_writes_them() {
        assert_eq!(size("12345"), Size::Bytes(5));
        assert_eq!(size("007"), Size::Bytes(1));
        assert_eq!(size("0"), Size::Bytes(1));
        assert_eq!(size("1.50"), Size::Bytes(4));
        assert_eq!(size(".5"), Size::Bytes(3));
        assert_eq!(size("-42"), Size::Bytes(3));
        assert_eq!(size("+42"), Size::Bytes(2));
        assert_eq!(size("1e3"), Size::Unknown);
    }

    #[test]
    fn concat_and_parentheses() {
        assert_eq!(size("CONCAT('ab', X'00', 12)"), Size::Bytes(5));
        assert_eq!(size("concat('a', CONCAT('b', 'c'))"), Size::Bytes(3));
        assert_eq!(size("CONCAT('a', NULL)"), Size::Null);
        assert_eq!(size("(('abc'))"), Size::Bytes(3));
        assert_eq!(size("('a') || ('b')"), Size::Unknown);
        assert_eq!(size("CONCAT('a', REPEAT('x', 9))"), Size::Unknown);
        assert_eq!(size("REPEAT('x', 1000)"), Size::Unknown);
        assert_eq!(size("@v"), Size::Unknown);
        assert_eq!(size("?"), Size::Unknown);
        assert_eq!(size("col"), Size::Unknown);
        assert_eq!(size("(SELECT v FROM t)"), Size::Unknown);
    }
}
