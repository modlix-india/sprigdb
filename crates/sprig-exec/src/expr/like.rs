//! `LIKE`: `%` and `_` wildcards with an escape character, matched one
//! character at a time under the collation.

use std::cmp::Ordering;

use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::Value;

use super::{Env, Node, string_node};
use crate::error::{Result, SqlError};
use crate::session::SqlMode;
use crate::types::{Dtc, ExprType};

/// Splits a string into its characters.
pub(crate) fn chars_of(charset: Charset, s: &[u8]) -> Vec<&[u8]> {
    match charset {
        Charset::Utf8mb4 => {
            let mut out = Vec::with_capacity(s.len());
            let mut i = 0;
            while i < s.len() {
                let mut j = i + 1;
                while j < s.len() && s[j] & 0xC0 == 0x80 {
                    j += 1;
                }
                out.push(&s[i..j]);
                i = j;
            }
            out
        }
        _ => s.chunks(1).collect(),
    }
}

enum Pat<'a> {
    Any,
    One,
    Lit(&'a [u8]),
}

/// The escape an empty `ESCAPE ''` gives on MariaDB: the NUL byte (the
/// empty string's end) and the default backslash both escape.
const BOTH_ESCAPES: &[u8] = b"\0\\";

fn parse_pattern<'a>(chars: &[&'a [u8]], escape: Option<&[u8]>) -> Vec<Pat<'a>> {
    let mut out = Vec::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // (BOTH_ESCAPES stands for an empty ESCAPE on MariaDB: the NUL
        // byte and the backslash both escape.)
        let escapes = match escape {
            Some(e) if e == BOTH_ESCAPES => c == b"\0" || c == b"\\",
            Some(e) => c == e,
            None => false,
        };
        if escapes && i + 1 < chars.len() {
            out.push(Pat::Lit(chars[i + 1]));
            i += 2;
            continue;
        }
        out.push(match c {
            b"%" => Pat::Any,
            b"_" => Pat::One,
            _ => Pat::Lit(c),
        });
        i += 1;
    }
    out
}

fn char_eq(coll: Collation, a: &[u8], b: &[u8]) -> bool {
    a == b || coll.compare(a, b) == Ordering::Equal
}

/// Whether `subject` matches `pattern` under `coll`.
pub(crate) fn like_match(
    coll: Collation,
    subject: &[u8],
    pattern: &[u8],
    escape: Option<&[u8]>,
) -> bool {
    let cs = coll.charset();
    let s = chars_of(cs, subject);
    let pchars = chars_of(cs, pattern);
    let p = parse_pattern(&pchars, escape);
    let (mut si, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while si < s.len() {
        let step = match p.get(pi) {
            Some(Pat::Any) => {
                star = Some((pi, si));
                pi += 1;
                continue;
            }
            Some(Pat::One) => true,
            Some(Pat::Lit(c)) => char_eq(coll, s[si], c),
            None => false,
        };
        if step {
            si += 1;
            pi += 1;
        } else if let Some((sp, ss)) = star {
            pi = sp + 1;
            si = ss + 1;
            star = Some((sp, ss + 1));
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|x| matches!(x, Pat::Any))
}

/// `[NOT] LIKE pattern [ESCAPE escape]`.
pub(crate) fn like(a: Node, p: Node, esc: Option<Node>, not: bool, env: Env<'_>) -> Result<Node> {
    let bad_escape = || SqlError::new(1210, "HY000", "Incorrect arguments to ESCAPE");
    let default_escape = if env.s.sql_mode.has(SqlMode::NO_BACKSLASH_ESCAPES) {
        None
    } else {
        Some(b"\\".to_vec())
    };
    let explicit_escape = esc.is_some();
    let mut empty_escape = false;
    // The escape must be a constant; both servers evaluate it while they
    // prepare the statement.
    let escape: Option<Vec<u8>> = match esc {
        None => default_escape,
        Some(e) => {
            if !e.konst {
                return Err(bad_escape());
            }
            let e = string_node(e, env);
            let charset = e.ty.coll.charset();
            let v = env.at_prepare(|ctx| e.eval(ctx, &[]))?;
            empty_escape = matches!(&v, Value::Str(s) | Value::Bytes(s) if s.is_empty());
            match v {
                Value::Str(s) | Value::Bytes(s) => match chars_of(charset, &s).len() {
                    // An empty escape is the NUL byte (the empty string's
                    // end). MariaDB keeps the default backslash as well,
                    // and only the backslash for a literal pattern.
                    0 if env.maria() && p.lit.is_some() => default_escape,
                    0 if env.maria() && default_escape.is_some() => Some(BOTH_ESCAPES.to_vec()),
                    0 => Some(vec![0]),
                    1 => Some(s.to_vec()),
                    _ => return Err(bad_escape()),
                },
                _ => return Err(bad_escape()),
            }
        }
    };
    let (a, p) = (string_node(a, env), string_node(p, env));
    let pair = [Dtc::of(&a.ty, env.conn()), Dtc::of(&p.ty, env.conn())];
    let coll = match crate::types::aggregate_types(&[&a.ty, &p.ty], env.conn(), "like", true) {
        Ok(d) => d.coll,
        // MySQL counts an ESCAPE clause among the operands it reports.
        Err(_) if explicit_escape && !env.maria() => {
            let three = [
                pair[0],
                pair[1],
                Dtc {
                    coll: env.conn(),
                    deriv: crate::types::Derivation::Coercible,
                },
            ];
            return Err(crate::types::illegal_mix(&three, "like"));
        }
        Err(e) => return Err(e),
    };
    let [a, p]: [Node; 2] = super::recode_checked(vec![a, p], coll, "like", env)?
        .try_into()
        .map_err(|_| crate::error::SqlError::param_count("like"))?;
    // (MariaDB's empty escape under a binary comparison is the NUL byte
    // alone.)
    let escape = if empty_escape && env.maria() && coll == Collation::Binary {
        Some(vec![0])
    } else {
        escape
    };
    // MariaDB evaluates a constant pattern while it prepares.
    if env.maria() && p.konst && p.lit.is_none() {
        env.at_prepare(|ctx| p.eval(ctx, &[]))?;
    }
    let ty = ExprType::boolean().with_null(a.ty.nullable || p.ty.nullable);
    let text = format!("({} like {})", a.text, p.text);
    Ok(Node::new(ty, text, move |ctx, row| {
        let x = a.eval(ctx, row)?;
        let (Value::Str(x) | Value::Bytes(x)) = x else {
            return Ok(Value::Null);
        };
        let y = p.eval(ctx, row)?;
        let (Value::Str(y) | Value::Bytes(y)) = y else {
            return Ok(Value::Null);
        };
        let hit = like_match(coll, &x, &y, escape.as_deref());
        Ok(Value::Int(i64::from(hit != not)))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(s: &str, p: &str) -> bool {
        like_match(
            Collation::Utf8mb4GeneralCi,
            s.as_bytes(),
            p.as_bytes(),
            Some(b"\\"),
        )
    }

    #[test]
    fn wildcards_and_escapes() {
        assert!(m("abc", "a%"));
        assert!(m("abc", "A_C"));
        assert!(m("abc", "%"));
        assert!(m("", "%"));
        assert!(!m("", "_"));
        assert!(m("a%c", "a\\%c"));
        assert!(!m("abc", "a\\%c"));
        assert!(m("abcabc", "%b%c"));
        assert!(!m("abc", "ab"));
        assert!(m("h\u{e9}llo", "h_llo"));
        assert!(m("h\u{e9}llo", "HELLO"));
        assert!(!m("abc ", "abc"));
        assert!(m("a_c", "a\\_c"));
        assert!(m("ab\\", "ab\\"));
        assert!(like_match(Collation::Binary, b"a|%", b"a||%", Some(b"|")));
    }
}
