//! Regular expressions: `expr REGEXP pattern` (and `RLIKE`), MySQL's
//! `REGEXP_LIKE`, and `REGEXP_INSTR`, `REGEXP_SUBSTR`, `REGEXP_REPLACE`.
//!
//! MariaDB matches with PCRE rules and MySQL with ICU's; both are close to
//! the backtracking engine used here (back references, look-around,
//! inline flags). What each port does differently is kept here: the
//! functions' argument lists (MariaDB takes only the subject, pattern and
//! replacement), the replacement syntax (`\1` on MariaDB, `$1` on MySQL),
//! how a replacement loop treats an empty match, what a missed substring
//! answers, the error codes for a bad pattern, and MySQL's refusal to mix
//! a binary string with a character string.
//!
//! Strings are matched as characters of their character set: UTF-8 as it
//! is, and a single-byte set (latin1, ascii, binary) one byte per
//! character. Matching ignores case when the collation does (a `_ci`
//! collation), unless MySQL's match type says otherwise.

use std::sync::{Arc, Mutex};

use fancy_regex::{Regex, RegexBuilder};
use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::Value;

use super::string::{dtc_of, int32, str_type, text, unknown_chars, wrap};
use crate::error::{Result, SqlError};
use crate::expr::{Env, Node, recode_checked, string_node};
use crate::session::Ctx;
use crate::types::{ExprType, Kind, mb_max_len};

/// The matching options of one call.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Flags {
    ci: bool,
    multi_line: bool,
    dot_all: bool,
}

/// A string of the call's character set as text the engine matches.
fn to_text(cs: Charset, b: &[u8]) -> String {
    match cs {
        Charset::Utf8mb4 => String::from_utf8_lossy(b).into_owned(),
        _ => b.iter().map(|&c| char::from(c)).collect(),
    }
}

/// Text back as the character set's bytes.
fn from_text(cs: Charset, s: &str) -> Vec<u8> {
    match cs {
        Charset::Utf8mb4 => s.as_bytes().to_vec(),
        _ => s
            .chars()
            .map(|c| u8::try_from(u32::from(c)).unwrap_or(b'?'))
            .collect(),
    }
}

/// The byte offset of character `n` (from 0) of `s`, `None` past the end.
fn byte_of_char(s: &str, n: usize) -> Option<usize> {
    if n == 0 {
        return Some(0);
    }
    match s.char_indices().nth(n) {
        Some((i, _)) => Some(i),
        None if s.chars().count() == n => Some(s.len()),
        None => None,
    }
}

/// The count of characters before byte offset `i`.
fn chars_before(s: &str, i: usize) -> usize {
    s[..i].chars().count()
}

/// The byte just past the character at `i`.
fn next_char(s: &str, i: usize) -> usize {
    s[i..]
        .chars()
        .next()
        .map_or(s.len() + 1, |c| i + c.len_utf8())
}

fn rx_error(code: u16, message: &str) -> SqlError {
    SqlError::new(code, "HY000", message)
}

/// MySQL's error for an index past the subject.
fn out_of_bounds() -> SqlError {
    rx_error(3686, "Index out of bounds in regular expression search.")
}

fn illegal() -> SqlError {
    rx_error(3685, "Illegal argument to a regular expression.")
}

/// The pattern errors both servers find before matching, with each
/// server's report: MariaDB's (1139, the message and offset of its regex
/// library) and MySQL's (its own code per kind).
fn precheck(pat: &str, maria: bool) -> Result<()> {
    let maria_err = |what: &str, at: usize| {
        SqlError::new(
            1139,
            "42000",
            format!("Regex error '{what} at offset {at}'"),
        )
    };
    let chars: Vec<char> = pat.chars().collect();
    let mut depth: i64 = 0;
    let mut i = 0;
    let mut prev_atom = false;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' => {
                if i + 1 >= chars.len() {
                    return Err(if maria {
                        maria_err("\\ at end of pattern", i + 1)
                    } else {
                        rx_error(3689, "Unrecognized escape sequence in regular expression.")
                    });
                }
                i += 2;
                prev_atom = true;
                continue;
            }
            '[' => {
                // A class: find its end, checking each range.
                let mut j = i + 1;
                if chars.get(j) == Some(&'^') {
                    j += 1;
                }
                if chars.get(j) == Some(&']') {
                    j += 1;
                }
                let mut last: Option<char> = None;
                while j < chars.len() && chars[j] != ']' {
                    if chars[j] == '[' && chars.get(j + 1) == Some(&':') {
                        if let Some(k) = (j + 2..chars.len().saturating_sub(1))
                            .find(|&k| chars[k] == ':' && chars[k + 1] == ']')
                        {
                            j = k + 2;
                            last = None;
                            continue;
                        }
                    }
                    let mut ch = chars[j];
                    if ch == '\\' && j + 1 < chars.len() {
                        j += 1;
                        ch = chars[j];
                        if ch.is_ascii_alphabetic() {
                            last = None;
                            j += 1;
                            continue;
                        }
                    }
                    if ch == '-'
                        && let Some(lo) = last
                        && j + 1 < chars.len()
                        && chars[j + 1] != ']'
                    {
                        let mut k = j + 1;
                        let mut hi = chars[k];
                        if hi == '\\' && k + 1 < chars.len() {
                            k += 1;
                            hi = chars[k];
                        }
                        if hi < lo {
                            return Err(if maria {
                                maria_err("range out of order in character class", k)
                            } else {
                                rx_error(
                                    3697,
                                    "The regular expression contains an [x-y] character range where x comes after y.",
                                )
                            });
                        }
                        j = k + 1;
                        last = None;
                        continue;
                    }
                    last = Some(ch);
                    j += 1;
                }
                i = j + 1;
                prev_atom = true;
                continue;
            }
            '(' => {
                depth += 1;
                prev_atom = false;
                // A group's flags or kind: `(?`
                if chars.get(i + 1) == Some(&'?') {
                    i += 2;
                    continue;
                }
            }
            ')' => {
                depth -= 1;
                if depth < 0 {
                    return Err(if maria {
                        maria_err("unmatched closing parenthesis", i)
                    } else {
                        rx_error(3691, "Mismatched parenthesis in regular expression.")
                    });
                }
                prev_atom = true;
            }
            '{' => {
                // `{m,n}` with n < m.
                let close = chars[i..].iter().position(|&x| x == '}').map(|p| i + p);
                if let Some(close) = close {
                    let body: String = chars[i + 1..close].iter().collect();
                    if let Some((m, n)) = body.split_once(',')
                        && let (Ok(m), Ok(n)) = (m.trim().parse::<u64>(), n.trim().parse::<u64>())
                        && n < m
                    {
                        return Err(if maria {
                            maria_err("numbers out of order in {} quantifier", close)
                        } else {
                            rx_error(
                                3693,
                                "The maximum is less than the minumum in a {min,max} interval.",
                            )
                        });
                    }
                }
            }
            '*' | '+' | '?' => {
                if !prev_atom {
                    return Err(if maria {
                        maria_err("quantifier does not follow a repeatable item", i)
                    } else {
                        rx_error(
                            3688,
                            &format!(
                                "Syntax error in regular expression on line 1, character {}.",
                                i + 1
                            ),
                        )
                    });
                }
                // A lazy or possessive mark may follow one quantifier.
                if matches!(chars.get(i + 1), Some('?' | '+')) {
                    i += 2;
                    prev_atom = false;
                    continue;
                }
                prev_atom = false;
                i += 1;
                continue;
            }
            '|' => prev_atom = false,
            _ => prev_atom = true,
        }
        i += 1;
    }
    if depth > 0 {
        return Err(if maria {
            maria_err("missing closing parenthesis", chars.len())
        } else {
            rx_error(3691, "Mismatched parenthesis in regular expression.")
        });
    }
    Ok(())
}

/// MariaDB's word-boundary classes, written the way the engine reads
/// them; MySQL refuses them.
fn word_classes(pat: &str, maria: bool) -> Result<String> {
    if !pat.contains("[[:<:]]") && !pat.contains("[[:>:]]") {
        return Ok(pat.to_owned());
    }
    if !maria {
        return Err(illegal());
    }
    Ok(pat
        .replace("[[:<:]]", r"\b(?=\w)")
        .replace("[[:>:]]", r"\b(?<=\w)"))
}

/// Compiles `pat` under `flags`, with each server's errors.
fn compile(pat: &str, flags: Flags, maria: bool) -> Result<Regex> {
    if pat.is_empty() && !maria {
        return Err(illegal());
    }
    precheck(pat, maria)?;
    let pat = word_classes(pat, maria)?;
    let mut b = RegexBuilder::new(&pat);
    b.case_insensitive(flags.ci)
        .multi_line(flags.multi_line)
        .dot_matches_new_line(flags.dot_all)
        .backtrack_limit(10_000_000);
    b.build().map_err(|e| {
        if maria {
            let at = match &e {
                fancy_regex::Error::ParseError(at, _) => *at,
                _ => 0,
            };
            SqlError::new(
                1139,
                "42000",
                format!("Regex error '{} at offset {at}'", plain_message(&e)),
            )
        } else {
            illegal()
        }
    })
}

/// The library's message without its own prefixes.
fn plain_message(e: &fancy_regex::Error) -> String {
    match e {
        fancy_regex::Error::ParseError(_, p) => p.to_string(),
        other => other.to_string(),
    }
}

/// MySQL's match type: `c` case-sensitive, `i` case-insensitive, `m`
/// multiple lines, `n` dot matches line ends, `u` Unix line ends (no
/// effect here); the last of `c` and `i` counts.
fn match_type(base: Flags, mt: &[u8], fname: &str) -> Result<Flags> {
    let mut f = base;
    for &c in mt {
        match c {
            b'c' => f.ci = false,
            b'i' => f.ci = true,
            b'm' => f.multi_line = true,
            b'n' => f.dot_all = true,
            b'u' => {}
            _ => {
                return Err(SqlError::new(
                    1210,
                    "HY000",
                    format!("Incorrect arguments to {fname}"),
                ));
            }
        }
    }
    Ok(f)
}

fn case_insensitive(coll: Collation) -> bool {
    coll != Collation::Binary && coll.name().ends_with("_ci")
}

/// The compiled pattern of a call: once, for a constant pattern and match
/// type; otherwise per row, keeping the last one.
struct Compiled {
    fixed: Option<Arc<Regex>>,
    last: Mutex<Option<(String, Flags, Arc<Regex>)>>,
}

impl Compiled {
    fn get(&self, pat: &str, flags: Flags, maria: bool) -> Result<Arc<Regex>> {
        if let Some(r) = &self.fixed {
            return Ok(Arc::clone(r));
        }
        if let Ok(guard) = self.last.lock()
            && let Some((p, f, r)) = guard.as_ref()
            && p == pat
            && *f == flags
        {
            return Ok(Arc::clone(r));
        }
        let r = Arc::new(compile(pat, flags, maria)?);
        if let Ok(mut guard) = self.last.lock() {
            *guard = Some((pat.to_owned(), flags, Arc::clone(&r)));
        }
        Ok(r)
    }
}

/// The matches of `re` in `s` from byte `from`, as ICU and PCRE step
/// through a string: after a match, the next search starts at its end; an
/// empty match moves the search on by one character. The search reads
/// the text from `from` on as the whole subject (MySQL's starting
/// position: `^` matches there). `maria_stop` ends the walk at the first
/// empty match (MariaDB's replacement loop). Offsets are into `s`.
fn walk(re: &Regex, s: &str, from: usize, maria_stop: bool) -> Result<Vec<(usize, usize)>> {
    let sub = &s[from..];
    let mut out = Vec::new();
    let mut pos = 0;
    while pos <= sub.len() {
        let m = re.find_from_pos(sub, pos).map_err(|_| backtrack())?;
        let Some(m) = m else { break };
        let (a, b) = (m.start(), m.end());
        if a == b {
            if maria_stop {
                break;
            }
            out.push((from + a, from + b));
            pos = next_char(sub, b);
        } else {
            out.push((from + a, from + b));
            pos = b;
        }
    }
    Ok(out)
}

fn backtrack() -> SqlError {
    rx_error(3699, "Timeout exceeded in regular expression match.")
}

/// The shared front of every call: the subject and pattern as strings of
/// one collation (MySQL refuses a binary string beside a character
/// string), and the default matching options.
struct Front {
    s: Node,
    p: Node,
    coll: Collation,
    flags: Flags,
    compiled: Arc<Compiled>,
    /// The constant match type's flags were applied when compiling.
    nullable: bool,
}

fn front(s: Node, p: Node, fname: &str, env: Env<'_>) -> Result<Front> {
    let maria = env.maria();
    let any_null = s.ty.kind == Kind::Null || p.ty.kind == Kind::Null;
    let (s, p) = (string_node(s, env), string_node(p, env));
    if !maria && !any_null {
        let bin = |n: &Node| n.ty.coll == Collation::Binary;
        let named = |n: &Node| {
            if n.ty.coll == Collation::Binary {
                "binary".to_owned()
            } else {
                n.ty.coll.name().to_owned()
            }
        };
        if bin(&s) != bin(&p) {
            return Err(SqlError::new(
                3995,
                "HY000",
                format!(
                    "Character set '{}' cannot be used in conjunction with '{}' in call to {fname}.",
                    named(&s),
                    named(&p)
                ),
            ));
        }
    }
    let dtc = dtc_of(&[&s, &p], fname, true, env)?;
    let [s, p]: [Node; 2] = recode_checked(vec![s, p], dtc.coll, fname, env)?
        .try_into()
        .map_err(|_| SqlError::param_count(fname))?;
    let flags = Flags {
        ci: case_insensitive(dtc.coll),
        multi_line: false,
        dot_all: false,
    };
    let nullable = s.ty.nullable || p.ty.nullable;
    Ok(Front {
        s,
        p,
        coll: dtc.coll,
        flags,
        compiled: Arc::new(Compiled {
            fixed: None,
            last: Mutex::new(None),
        }),
        nullable,
    })
}

/// Compiles a constant pattern while the statement is prepared (a bad
/// one fails the statement there, as on both servers).
fn fix_pattern(f: &mut Front, mt: Option<&Node>, fname: &str, env: Env<'_>) -> Result<()> {
    if !f.p.konst || mt.is_some_and(|m| !m.konst) {
        return Ok(());
    }
    let maria = env.maria();
    let v = env.at_prepare(|ctx| f.p.eval(ctx, &[]))?;
    let Some(b) = text(v) else { return Ok(()) };
    let mut flags = f.flags;
    if let Some(m) = mt {
        let v = env.at_prepare(|ctx| m.eval(ctx, &[]))?;
        let Some(mb) = text(v) else { return Ok(()) };
        flags = match_type(flags, &mb, fname)?;
    }
    let pat = to_text(f.coll.charset(), &b);
    let re = compile(&pat, flags, maria)?;
    f.compiled = Arc::new(Compiled {
        fixed: Some(Arc::new(re)),
        last: Mutex::new(None),
    });
    Ok(())
}

/// The value of an optional integer argument; `Err(None)` for `NULL`.
fn opt_int(
    n: Option<&Node>,
    ctx: &mut Ctx<'_>,
    row: &[Value],
    default: i64,
) -> Result<Option<i64>> {
    match n {
        None => Ok(Some(default)),
        Some(n) => n.int_arg(ctx, row),
    }
}

/// The subject, pattern and flags of one row; `None` when one is `NULL`.
fn row_parts(
    f: &Front,
    mt: Option<&Node>,
    fname: &str,
    ctx: &mut Ctx<'_>,
    row: &[Value],
) -> Result<Option<(String, Arc<Regex>)>> {
    let maria = ctx.maria();
    let Some(sb) = text(f.s.eval(ctx, row)?) else {
        return Ok(None);
    };
    let Some(pb) = text(f.p.eval(ctx, row)?) else {
        return Ok(None);
    };
    let mut flags = f.flags;
    if let Some(m) = mt {
        let Some(mb) = text(m.eval(ctx, row)?) else {
            return Ok(None);
        };
        flags = match_type(flags, &mb, fname)?;
    }
    let cs = f.coll.charset();
    let pat = to_text(cs, &pb);
    let re = f.compiled.get(&pat, flags, maria)?;
    Ok(Some((to_text(cs, &sb), re)))
}

/// MySQL's starting position: from 1, at most the subject's length (any
/// position fits an empty subject's first).
fn start_at(s: &str, pos: i64) -> Result<usize> {
    let n = s.chars().count();
    if pos < 1 || (pos as u64 > n as u64 && pos != 1) {
        return Err(out_of_bounds());
    }
    byte_of_char(s, usize::try_from(pos - 1).unwrap_or(usize::MAX)).ok_or_else(out_of_bounds)
}

/// `expr REGEXP pattern` and MySQL's `REGEXP_LIKE(expr, pattern [, type])`.
pub(crate) fn like(mut args: Vec<Node>, fname: &str, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    let lower = fname.to_ascii_lowercase();
    // MySQL reports REGEXP under its function's name.
    let msg_name = if maria {
        lower.clone()
    } else {
        "regexp_like".to_owned()
    };
    if args.len() < 2 || args.len() > if fname == "REGEXP" { 2 } else { 3 } {
        return Err(SqlError::param_count(fname));
    }
    let mt = if args.len() == 3 { args.pop() } else { None };
    let mt = mt.map(|m| string_node(m, env));
    let p = args.pop().ok_or_else(|| SqlError::param_count(fname))?;
    let s = args.pop().ok_or_else(|| SqlError::param_count(fname))?;
    let mut f = front(s, p, &msg_name, env)?;
    fix_pattern(&mut f, mt.as_ref(), &msg_name, env)?;
    let nullable = f.nullable || mt.as_ref().is_some_and(|m| m.ty.nullable);
    let ty = ExprType::boolean().with_null(nullable);
    let text_of = format!("({} regexp {})", f.s.text, f.p.text);
    Ok(Node::new(ty, text_of, move |ctx, row| {
        let Some((s, re)) = row_parts(&f, mt.as_ref(), &msg_name, ctx, row)? else {
            return Ok(Value::Null);
        };
        let hit = re.is_match(s.as_str()).map_err(|_| backtrack())?;
        Ok(Value::Int(i64::from(hit)))
    }))
}

/// `REGEXP_INSTR(s, pattern [, pos [, occurrence [, return_option [, type]]]])`
/// (MariaDB: the first two only).
pub(crate) fn instr(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    let max = if maria { 2 } else { 6 };
    if args.len() < 2 || args.len() > max {
        return Err(SqlError::param_count("REGEXP_INSTR"));
    }
    let mut it = args.into_iter();
    let (s, p) = (
        it.next().unwrap_or_else(null_node),
        it.next().unwrap_or_else(null_node),
    );
    let pos = it.next();
    let occ = it.next();
    let ret = it.next();
    let mt = it.next().map(|m| string_node(m, env));
    let mut f = front(s, p, "regexp_instr", env)?;
    fix_pattern(&mut f, mt.as_ref(), "regexp_instr", env)?;
    if let Some(r) = &ret
        && let Some(v) = r.const_i64(env)?
        && v != 0
        && v != 1
    {
        return Err(SqlError::new(
            1210,
            "HY000",
            "Incorrect arguments to regexp_instr: return_option must be 1 or 0.",
        ));
    }
    let nullable = f.nullable
        || [&pos, &occ, &ret]
            .iter()
            .any(|n| n.as_ref().is_some_and(|n| n.ty.nullable))
        || mt.as_ref().is_some_and(|m| m.ty.nullable);
    let ty = int32(if maria { 11 } else { 21 }, env).with_null(nullable);
    Ok(Node::new(ty, "regexp_instr", move |ctx, row| {
        let Some((s, re)) = row_parts(&f, mt.as_ref(), "regexp_instr", ctx, row)? else {
            return Ok(Value::Null);
        };
        let (Some(pos), Some(occ), Some(ret)) = (
            opt_int(pos.as_ref(), ctx, row, 1)?,
            opt_int(occ.as_ref(), ctx, row, 1)?,
            opt_int(ret.as_ref(), ctx, row, 0)?,
        ) else {
            return Ok(Value::Null);
        };
        if ret != 0 && ret != 1 {
            return Err(SqlError::new(
                1210,
                "HY000",
                "Incorrect arguments to regexp_instr: return_option must be 1 or 0.",
            ));
        }
        let from = if ctx.maria() { 0 } else { start_at(&s, pos)? };
        let n = usize::try_from(occ.max(1)).unwrap_or(usize::MAX);
        let hits = walk(&re, s.as_str(), from, false)?;
        Ok(Value::Int(match hits.get(n - 1) {
            None => 0,
            Some(&(a, b)) => {
                let at = if ret == 1 { b } else { a };
                i64::try_from(chars_before(&s, at)).unwrap_or(i64::MAX) + 1
            }
        }))
    }))
}

fn null_node() -> Node {
    Node::literal(ExprType::null(), "NULL", Value::Null)
}

/// `REGEXP_SUBSTR(s, pattern [, pos [, occurrence [, type]]])` (MariaDB:
/// the first two only; a miss is the empty string there, `NULL` on MySQL).
pub(crate) fn substr(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    let max = if maria { 2 } else { 5 };
    if args.len() < 2 || args.len() > max {
        return Err(SqlError::param_count("REGEXP_SUBSTR"));
    }
    let mut it = args.into_iter();
    let (s, p) = (
        it.next().unwrap_or_else(null_node),
        it.next().unwrap_or_else(null_node),
    );
    let pos = it.next();
    let occ = it.next();
    let mt = it.next().map(|m| string_node(m, env));
    let mut f = front(s, p, "regexp_substr", env)?;
    fix_pattern(&mut f, mt.as_ref(), "regexp_substr", env)?;
    let binary = f.coll == Collation::Binary;
    let dtc = crate::types::Dtc {
        coll: f.coll,
        deriv: f.s.ty.deriv,
    };
    let ty = str_type(u64::from(f.s.ty.len), dtc, true);
    let cs = f.coll.charset();
    Ok(Node::new(ty, "regexp_substr", move |ctx, row| {
        let Some((s, re)) = row_parts(&f, mt.as_ref(), "regexp_substr", ctx, row)? else {
            return Ok(Value::Null);
        };
        let (Some(pos), Some(occ)) = (
            opt_int(pos.as_ref(), ctx, row, 1)?,
            opt_int(occ.as_ref(), ctx, row, 1)?,
        ) else {
            return Ok(Value::Null);
        };
        let maria = ctx.maria();
        let from = if maria { 0 } else { start_at(&s, pos)? };
        let n = usize::try_from(occ.max(1)).unwrap_or(usize::MAX);
        let hits = walk(&re, s.as_str(), from, false)?;
        Ok(match hits.get(n - 1) {
            Some(&(a, b)) => wrap(from_text(cs, &s[a..b]), binary),
            None if maria => wrap(Vec::new(), binary),
            None => Value::Null,
        })
    }))
}

/// One replacement's text with its group references filled in: `\N` on
/// MariaDB, `$N` on MySQL (a backslash takes the next character as it
/// is on both).
fn expand(
    rep: &str,
    caps: &fancy_regex::Captures<'_, str>,
    maria: bool,
    out: &mut String,
) -> Result<()> {
    let mut it = rep.chars().peekable();
    while let Some(c) = it.next() {
        if c == '\\' {
            match it.next() {
                Some(d) if maria && d.is_ascii_digit() => {
                    let g = usize::from(d as u8 - b'0');
                    if let Some(m) = caps.get(g) {
                        out.push_str(m.as_str());
                    }
                }
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else if c == '$' && !maria {
            // ICU reads as many digits as still name a group.
            let Some(&d) = it.peek() else {
                return Err(rx_error(3887, "A capture group has an invalid name."));
            };
            if !d.is_ascii_digit() {
                return Err(rx_error(3887, "A capture group has an invalid name."));
            }
            it.next();
            let mut g = usize::from(d as u8 - b'0');
            while let Some(&d2) = it.peek() {
                if !d2.is_ascii_digit() {
                    break;
                }
                let wider = g * 10 + usize::from(d2 as u8 - b'0');
                if wider >= caps.len() {
                    break;
                }
                g = wider;
                it.next();
            }
            if g >= caps.len() {
                return Err(out_of_bounds());
            }
            if let Some(m) = caps.get(g) {
                out.push_str(m.as_str());
            }
        } else {
            out.push(c);
        }
    }
    Ok(())
}

/// `REGEXP_REPLACE(s, pattern, replacement [, pos [, occurrence [, type]]])`
/// (MariaDB: the first three only).
pub(crate) fn replace(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    let max = if maria { 3 } else { 6 };
    if args.len() < 3 || args.len() > max {
        return Err(SqlError::param_count("REGEXP_REPLACE"));
    }
    let mut it = args.into_iter();
    let s = it.next().unwrap_or_else(null_node);
    let p = it.next().unwrap_or_else(null_node);
    let r = string_node(it.next().unwrap_or_else(null_node), env);
    let pos = it.next();
    let occ = it.next();
    let mt = it.next().map(|m| string_node(m, env));
    if let Some(v) = pos
        .as_ref()
        .map(|n| n.const_i64(env))
        .transpose()?
        .flatten()
        && v < 1
    {
        return Err(SqlError::new(
            1583,
            "42000",
            "Incorrect parameters in the call to native function 'regexp_replace'",
        ));
    }
    let mut f = front(s, p, "regexp_replace", env)?;
    fix_pattern(&mut f, mt.as_ref(), "regexp_replace", env)?;
    let [r]: [Node; 1] = recode_checked(vec![r], f.coll, "regexp_replace", env)?
        .try_into()
        .map_err(|_| SqlError::param_count("regexp_replace"))?;
    let binary = f.coll == Collation::Binary;
    let dtc = crate::types::Dtc {
        coll: f.coll,
        deriv: f.s.ty.deriv,
    };
    // Both servers leave room for max_allowed_packet.
    let chars = unknown_chars(env, u64::from(mb_max_len(f.coll)));
    let ty = str_type(chars, dtc, true);
    let cs = f.coll.charset();
    Ok(Node::new(ty, "regexp_replace", move |ctx, row| {
        let Some((s, re)) = row_parts(&f, mt.as_ref(), "regexp_replace", ctx, row)? else {
            return Ok(Value::Null);
        };
        let Some(rb) = text(r.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let (Some(pos), Some(occ)) = (
            opt_int(pos.as_ref(), ctx, row, 1)?,
            opt_int(occ.as_ref(), ctx, row, 0)?,
        ) else {
            return Ok(Value::Null);
        };
        let maria = ctx.maria();
        let rep = to_text(cs, &rb);
        let from = if maria { 0 } else { start_at(&s, pos)? };
        let hits = walk(&re, s.as_str(), from, maria)?;
        let chosen: Vec<(usize, usize)> = if occ <= 0 {
            hits
        } else {
            let n = usize::try_from(occ).unwrap_or(usize::MAX);
            hits.get(n - 1).copied().into_iter().collect()
        };
        let mut out = String::with_capacity(s.len());
        let mut at = 0;
        for (a, b) in chosen {
            out.push_str(&s[at..a]);
            let caps = re
                .captures_from_pos(&s[from..], a - from)
                .map_err(|_| backtrack())?
                .filter(|c| {
                    c.get(0)
                        .is_some_and(|m| m.start() + from == a && m.end() + from == b)
                });
            match caps {
                Some(c) => expand(&rep, &c, maria, &mut out)?,
                None => out.push_str(&rep),
            }
            at = b;
        }
        out.push_str(&s[at..]);
        let bytes = from_text(cs, &out);
        if bytes.len() as u64 > ctx.session.max_allowed_packet {
            return Ok(super::string::too_big(ctx, "regexp_replace"));
        }
        Ok(wrap(bytes, binary))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn re(p: &str, maria: bool) -> Result<Regex> {
        compile(
            p,
            Flags {
                ci: true,
                multi_line: false,
                dot_all: false,
            },
            maria,
        )
    }

    #[test]
    fn walking_matches_like_icu_and_pcre() {
        let r = re("a*", false).unwrap();
        assert_eq!(
            walk(&r, "baaac", 0, false).unwrap(),
            vec![(0, 0), (1, 4), (4, 4), (5, 5)]
        );
        assert_eq!(walk(&r, "aaa", 0, false).unwrap(), vec![(0, 3), (3, 3)]);
        assert_eq!(walk(&r, "aaa", 0, true).unwrap(), vec![(0, 3)]);
        assert!(walk(&r, "baaac", 0, true).unwrap().is_empty());
    }

    #[test]
    fn pattern_errors_per_server() {
        let code = |p: &str, maria: bool| re(p, maria).err().map(|e| e.code);
        assert_eq!(code("(", true), Some(1139));
        assert_eq!(code("(", false), Some(3691));
        assert_eq!(code("a{2,1}", false), Some(3693));
        assert_eq!(code("[z-a]", false), Some(3697));
        assert_eq!(code("\\", false), Some(3689));
        assert_eq!(code("a**", false), Some(3688));
        assert_eq!(code("", false), Some(3685));
        assert_eq!(code("", true), None);
        assert_eq!(code("[[:<:]]b", false), Some(3685));
        assert_eq!(code("(a)\\1", false), None);
        assert_eq!(code("(?<=a)b", true), None);
        let e = re("a{2,1}", true).err().unwrap();
        assert_eq!(
            e.message,
            "Regex error 'numbers out of order in {} quantifier at offset 5'"
        );
        let e = re("[z-a]", true).err().unwrap();
        assert_eq!(
            e.message,
            "Regex error 'range out of order in character class at offset 3'"
        );
        let e = re("(", true).err().unwrap();
        assert_eq!(
            e.message,
            "Regex error 'missing closing parenthesis at offset 1'"
        );
        let e = re("a**", true).err().unwrap();
        assert_eq!(
            e.message,
            "Regex error 'quantifier does not follow a repeatable item at offset 2'"
        );
    }

    #[test]
    fn word_classes_on_mariadb() {
        let r = re("[[:<:]]b", true).unwrap();
        assert!(!r.is_match("ab").unwrap());
        assert!(r.is_match("a b").unwrap());
    }

    #[test]
    fn single_byte_text_round_trips() {
        let b: Vec<u8> = (0u8..=255).collect();
        assert_eq!(from_text(Charset::Latin1, &to_text(Charset::Latin1, &b)), b);
    }
}
