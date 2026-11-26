//! The less common string functions: `SUBSTRING_INDEX`, `INSERT`,
//! `FIELD`, `ELT`, `FIND_IN_SET`, `FORMAT`, `QUOTE`, `UNHEX`, `BIN`,
//! `OCT`, `CONV`, `CHAR`, and the digests `MD5`, `SHA1`, `SHA2`, `CRC32`.

use std::cmp::Ordering;
use std::fmt::Write as _;

use sprig_engine::keys::collation::{Charset, Collation};
use sprig_engine::row::Value;

use super::hash;
use super::string::{char_slice, dtc_of, int32, str_type, text, too_big, wrap};
use crate::conv::{self, is_space};
use crate::error::Result;
use crate::expr::{Env, Node, recode_checked, string_node};
use crate::num::{Dec, Round};
use crate::session::Ctx;
use crate::types::{Derivation, Dtc, ExprType, Kind, mb_max_len};

/// The result type of a string function whose first argument may be the
/// `NULL` literal: MySQL types such a result as binary.
fn first_null_dtc(mut dtc: Dtc, first_null: bool, env: Env<'_>) -> Dtc {
    if first_null && !env.maria() {
        dtc.coll = Collation::Binary;
        dtc.deriv = Derivation::Ignorable;
        dtc.mb3 = false;
    }
    dtc
}

/// Where `pat` occurs in `s`, left to right, without overlaps.
fn occurrences(s: &[u8], pat: &[u8]) -> Vec<usize> {
    let mut out = Vec::new();
    if pat.is_empty() || pat.len() > s.len() {
        return out;
    }
    let mut i = 0;
    while i + pat.len() <= s.len() {
        if &s[i..i + pat.len()] == pat {
            out.push(i);
            i += pat.len();
        } else {
            i += 1;
        }
    }
    out
}

/// `SUBSTRING_INDEX(s, delim, count)`: the part of `s` before the
/// `count`-th delimiter (after it from the right for a negative count).
/// The delimiter is matched byte by byte, so the match is case-sensitive
/// whatever the collation. MariaDB reads the count as a 32-bit number.
pub(crate) fn substring_index(s: Node, delim: Node, count: Node, env: Env<'_>) -> Result<Node> {
    let first_null = s.ty.kind == Kind::Null;
    let (s, delim) = (string_node(s, env), string_node(delim, env));
    let dtc = dtc_of(&[&s, &delim], "substring_index", env.maria(), env)?;
    let [s, delim]: [Node; 2] = recode_checked(vec![s, delim], dtc.coll, "substring_index", env)?
        .try_into()
        .map_err(|_| crate::error::SqlError::param_count("substring_index"))?;
    let dtc = first_null_dtc(dtc, first_null, env);
    let binary = dtc.coll == Collation::Binary;
    let ty = str_type(u64::from(s.ty.len), dtc, true);
    Ok(Node::new(ty, "substring_index", move |ctx, row| {
        let Some(b) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(d) = text(delim.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(n) = count.int_typed(ctx, row)? else {
            return Ok(Value::Null);
        };
        let n: i128 = if ctx.maria() {
            i128::from(n.v as i32)
        } else {
            n.wide()
        };
        if n == 0 || d.is_empty() {
            return Ok(wrap(Vec::new(), binary));
        }
        let occ = occurrences(&b, &d);
        let k = n.unsigned_abs();
        if k > occ.len() as u128 {
            return Ok(wrap(b, binary));
        }
        let k = k as usize;
        let out = if n > 0 {
            b.slice(..occ[k - 1])
        } else {
            b.slice(occ[occ.len() - k] + d.len()..)
        };
        Ok(wrap(out, binary))
    }))
}

/// `INSERT(s, pos, len, new)`: `len` characters of `s` from `pos` replaced
/// by `new`; `s` itself when `pos` is outside it.
pub(crate) fn insert(s: Node, pos: Node, len: Node, new: Node, env: Env<'_>) -> Result<Node> {
    let first_null = s.ty.kind == Kind::Null;
    // MariaDB sends INSERT into a number on as latin1, as it does RPAD of
    // a number (see `pad`).
    let latin1_number = env.maria() && !matches!(s.ty.kind, Kind::Str | Kind::Null);
    let (s, new) = (string_node(s, env), string_node(new, env));
    let dtc = dtc_of(&[&s, &new], "insert", false, env)?;
    let [s, new]: [Node; 2] = recode_checked(vec![s, new], dtc.coll, "insert", env)?
        .try_into()
        .map_err(|_| crate::error::SqlError::param_count("insert"))?;
    let dtc = first_null_dtc(dtc, first_null, env);
    let charset = dtc.coll.charset();
    let binary = dtc.coll == Collation::Binary;
    let mut chars = u64::from(s.ty.len) + u64::from(new.ty.len);
    if first_null && !env.maria() {
        // (The binary result counts the text's bytes.)
        chars *= u64::from(mb_max_len(new.ty.coll));
    }
    let ty = str_type(chars, dtc, true);
    let mut node = Node::new(ty, "insert", move |ctx, row| {
        let p = pos.int_typed(ctx, row)?;
        let l = len.int_typed(ctx, row)?;
        let b = text(s.eval(ctx, row)?);
        let n = text(new.eval(ctx, row)?);
        let (Some(p), Some(l), Some(b), Some(n)) = (p, l, b, n) else {
            return Ok(Value::Null);
        };
        let total = charset.char_count(&b) as i128;
        let p = p.wide();
        // MySQL checks the position against the string's bytes and then
        // against its characters, so the position just past them appends.
        let outside = if ctx.maria() {
            p > total
        } else {
            p > b.len() as i128 || p - 1 > total
        };
        if p < 1 || outside {
            return Ok(wrap(b, binary));
        }
        let start = (p - 1) as usize;
        let l = l.wide();
        let rest = total - (p - 1);
        let take = if l < 0 || l > rest { rest } else { l } as usize;
        let head = char_slice(charset, &b, 0, start);
        let mid = char_slice(charset, &b, start, take);
        let tail = &b[head.len() + mid.len()..];
        let mut out = Vec::with_capacity(head.len() + n.len() + tail.len());
        out.extend_from_slice(head);
        out.extend_from_slice(&n);
        out.extend_from_slice(tail);
        if out.len() as u64 > ctx.session.max_allowed_packet {
            return Ok(too_big(ctx, "insert"));
        }
        Ok(wrap(out, binary))
    });
    node.latin1_send = latin1_number && !binary;
    Ok(node)
}

/// How `FIELD` and `INTERVAL` compare their arguments.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Str(Collation),
    Int,
    Dec,
    Real,
}

/// A value converted for a [`Cmp`].
enum Key {
    Str(bytes::Bytes),
    Int(i128),
    Dec(Dec),
    Real(f64),
}

fn key_of(cmp: Cmp, n: &Node, ctx: &mut Ctx<'_>, row: &[Value]) -> Result<Option<Key>> {
    Ok(match cmp {
        Cmp::Str(_) => text(n.eval(ctx, row)?).map(Key::Str),
        Cmp::Int => n.int(ctx, row)?.map(|i| Key::Int(i.wide())),
        Cmp::Dec => n.dec(ctx, row)?.map(Key::Dec),
        Cmp::Real => n.f64(ctx, row)?.map(Key::Real),
    })
}

fn order(cmp: Cmp, a: &Key, b: &Key) -> Option<Ordering> {
    match (a, b) {
        (Key::Str(x), Key::Str(y)) => Some(match cmp {
            Cmp::Str(c) => c.compare(x, y),
            _ => x.cmp(y),
        }),
        (Key::Int(x), Key::Int(y)) => Some(x.cmp(y)),
        (Key::Dec(x), Key::Dec(y)) => Some(x.cmp(y)),
        (Key::Real(x), Key::Real(y)) => x.partial_cmp(y),
        _ => None,
    }
}

/// The comparison type of `FIELD`'s arguments: strings when all are
/// strings (dates and times count as their text), integers when all are
/// integers, decimals when all are exact numbers, doubles otherwise. The
/// `NULL` literal takes no part.
fn field_cmp(args: &[Node], env: Env<'_>) -> Result<Cmp> {
    let kinds: Vec<Kind> = args
        .iter()
        .map(|a| a.ty.kind)
        .filter(|k| *k != Kind::Null)
        .collect();
    let stringish = |k: &Kind| *k == Kind::Str || k.is_temporal();
    if kinds.is_empty() || kinds.iter().all(stringish) {
        let strs: Vec<Node> = args.iter().map(|a| string_node(a.clone(), env)).collect();
        let refs: Vec<&Node> = strs.iter().collect();
        return Ok(Cmp::Str(dtc_of(&refs, "field", true, env)?.coll));
    }
    let hexless = args.iter().all(|a| !a.ty.hex);
    if hexless && kinds.iter().all(|k| k.is_int()) {
        Ok(Cmp::Int)
    } else if hexless && kinds.iter().all(|k| k.is_exact()) {
        Ok(Cmp::Dec)
    } else {
        Ok(Cmp::Real)
    }
}

/// `FIELD(x, a, b, ...)`: the position of the first argument equal to
/// `x`; 0 when none is, or when `x` is `NULL`.
pub(crate) fn field(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let cmp = field_cmp(&args, env)?;
    let args: Vec<Node> = if let Cmp::Str(coll) = cmp {
        let strs = args.into_iter().map(|a| string_node(a, env)).collect();
        recode_checked(strs, coll, "field", env)?
    } else {
        args
    };
    let ty = int32(3, env).with_null(false);
    Ok(Node::new(ty, "field", move |ctx, row| {
        let Some(x) = key_of(cmp, &args[0], ctx, row)? else {
            return Ok(Value::Int(0));
        };
        for (i, a) in args.iter().enumerate().skip(1) {
            if let Some(y) = key_of(cmp, a, ctx, row)? {
                if order(cmp, &x, &y) == Some(Ordering::Equal) {
                    return Ok(Value::Int(i as i64));
                }
            }
        }
        Ok(Value::Int(0))
    }))
}

/// `INTERVAL(n, n1, n2, ...)`: how many of the list come before the
/// first one greater than `n` (`NULL` items are never greater); -1 when
/// `n` is `NULL`. The arguments compare as numbers.
pub(crate) fn interval(args: Vec<Node>, env: Env<'_>) -> Node {
    // Each item compares with the first argument in the type of the pair:
    // integers when both are, decimals when both are exact, doubles
    // otherwise.
    let pair = |a: &Node, b: &Node| {
        let int = |n: &Node| n.ty.kind.is_int() && !n.ty.hex;
        let exact = |n: &Node| n.ty.kind.is_exact() && !n.ty.hex;
        if int(a) && int(b) {
            Cmp::Int
        } else if exact(a) && exact(b) {
            Cmp::Dec
        } else {
            Cmp::Real
        }
    };
    let cmps: Vec<Cmp> = args[1..].iter().map(|b| pair(&args[0], b)).collect();
    let ty = ExprType {
        prec: 2,
        ..int32(2, env)
    }
    .with_null(false);
    Node::new(ty, "interval", move |ctx, row| {
        // Both servers read a computed first argument twice: once to see
        // whether it is NULL, once to compare.
        if args[0].lit.is_none() && args[0].f64(ctx, row)?.is_none() {
            return Ok(Value::Int(-1));
        }
        let mut seen: Vec<(Cmp, Key)> = Vec::new();
        let mut n = 0i64;
        for (a, cmp) in args[1..].iter().zip(&cmps) {
            let x = match seen.iter().find(|(c, _)| c == cmp) {
                Some((_, k)) => Some(clone_key(k)),
                None => {
                    let k = key_of(*cmp, &args[0], ctx, row)?;
                    if let Some(k) = &k {
                        seen.push((*cmp, clone_key(k)));
                    }
                    k
                }
            };
            let Some(x) = x else {
                return Ok(Value::Int(-1));
            };
            if let Some(y) = key_of(*cmp, a, ctx, row)? {
                if order(*cmp, &y, &x) == Some(Ordering::Greater) {
                    break;
                }
            }
            n += 1;
        }
        Ok(Value::Int(n))
    })
}

fn clone_key(k: &Key) -> Key {
    match k {
        Key::Str(b) => Key::Str(b.clone()),
        Key::Int(i) => Key::Int(*i),
        Key::Dec(d) => Key::Dec(d.clone()),
        Key::Real(x) => Key::Real(*x),
    }
}

/// `ELT(n, a, b, ...)`: the `n`-th string of the list, `NULL` when there
/// is none.
pub(crate) fn elt(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let mut it = args.into_iter();
    let n = it
        .next()
        .ok_or_else(|| crate::error::SqlError::param_count("elt"))?;
    let list: Vec<Node> = it.map(|a| string_node(a, env)).collect();
    let refs: Vec<&Node> = list.iter().collect();
    let dtc = dtc_of(&refs, "elt", false, env)?;
    let list = recode_checked(list, dtc.coll, "elt", env)?;
    let binary = dtc.coll == Collation::Binary;
    let chars = list.iter().map(|a| u64::from(a.ty.len)).max().unwrap_or(0);
    let ty = str_type(chars, dtc, true);
    Ok(Node::new(ty, "elt", move |ctx, row| {
        let Some(i) = n.int_typed(ctx, row)? else {
            return Ok(Value::Null);
        };
        // MariaDB reads the position as a 32-bit number.
        let i = if ctx.maria() {
            i128::from(i.v as i32)
        } else {
            i.wide()
        };
        if i < 1 || i > list.len() as i128 {
            return Ok(Value::Null);
        }
        Ok(match text(list[(i - 1) as usize].eval(ctx, row)?) {
            None => Value::Null,
            Some(b) => wrap(b, binary),
        })
    }))
}

/// `FIND_IN_SET(s, list)`: the position of `s` among the comma-separated
/// items of `list`, compared under the collation; 0 when `s` holds a
/// comma or isn't there.
pub(crate) fn find_in_set(s: Node, list: Node, env: Env<'_>) -> Result<Node> {
    let (s, list) = (string_node(s, env), string_node(list, env));
    let coll = dtc_of(&[&s, &list], "find_in_set", true, env)?.coll;
    let [s, list]: [Node; 2] = recode_checked(vec![s, list], coll, "find_in_set", env)?
        .try_into()
        .map_err(|_| crate::error::SqlError::param_count("find_in_set"))?;
    let ty = int32(3, env).with_null(s.ty.nullable || list.ty.nullable);
    Ok(Node::new(ty, "find_in_set", move |ctx, row| {
        let Some(x) = text(s.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let Some(l) = text(list.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        if x.contains(&b',') || l.is_empty() {
            return Ok(Value::Int(0));
        }
        for (i, item) in l.split(|b| *b == b',').enumerate() {
            if equal_no_pad(coll, item, &x) {
                return Ok(Value::Int(i as i64 + 1));
            }
        }
        Ok(Value::Int(0))
    }))
}

/// Equality without the space padding of a PAD SPACE collation: trailing
/// spaces count (`FIND_IN_SET` compares its items so).
fn equal_no_pad(coll: Collation, a: &[u8], b: &[u8]) -> bool {
    let spaces = |s: &[u8]| s.iter().rev().take_while(|c| **c == b' ').count();
    coll.compare(a, b) == Ordering::Equal && spaces(a) == spaces(b)
}

/// How a locale writes a number: the thousands separator (none for some),
/// the decimal point, and whether groups after the first are of two
/// digits (the Indian grouping).
#[derive(Clone, Copy)]
struct Locale {
    thousands: Option<u8>,
    point: u8,
    indian: bool,
}

const EN: Locale = Locale {
    thousands: Some(b','),
    point: b'.',
    indian: false,
};

/// The locales both servers know, by how they format numbers (as
/// observed: `FORMAT(123456789012.5, 2, locale)` on each server).
const LOCALES: &[(&str, Locale)] = &[
    (
        "ar_AE ar_BH ar_DZ ar_EG ar_IN ar_IQ ar_JO ar_KW ar_LB ar_LY ar_MA ar_OM ar_QA ar_SD \
         ar_SY ar_TN ar_YE en_AU en_CA en_GB en_NZ en_PH en_US en_ZA en_ZW es_DO es_GT es_HN \
         es_MX es_NI es_PA es_PE es_PR es_SV es_US gu_IN he_IL hi_IN ja_JP ko_KR ms_MY th_TH \
         ur_PK zh_CN zh_HK zh_TW",
        EN,
    ),
    (
        "en_IN ta_IN te_IN",
        Locale {
            thousands: Some(b','),
            point: b'.',
            indian: true,
        },
    ),
    (
        "ar_SA sr_RS",
        Locale {
            thousands: None,
            point: b'.',
            indian: false,
        },
    ),
    (
        "be_BY da_DK de_BE de_DE de_LU es_AR es_BO es_CL es_CO es_EC es_ES es_PY es_UY es_VE \
         fo_FO hu_HU id_ID is_IS lt_LT mn_MN nb_NO no_NO ro_RO ru_UA sq_AL tr_TR uk_UA vi_VN",
        Locale {
            thousands: Some(b'.'),
            point: b',',
            indian: false,
        },
    ),
    (
        "cs_CZ es_CR et_EE fi_FI lv_LV mk_MK ru_RU sk_SK sv_FI sv_SE",
        Locale {
            thousands: Some(b' '),
            point: b',',
            indian: false,
        },
    ),
    (
        "ca_ES de_AT el_GR eu_ES fr_BE fr_CA fr_CH fr_FR fr_LU gl_ES hr_HR it_IT nl_BE nl_NL \
         pl_PL pt_BR pt_PT sl_SI",
        Locale {
            thousands: None,
            point: b',',
            indian: false,
        },
    ),
    (
        "de_CH",
        Locale {
            thousands: Some(b'\''),
            point: b'.',
            indian: false,
        },
    ),
    (
        "it_CH rm_CH",
        Locale {
            thousands: Some(b'\''),
            point: b',',
            indian: false,
        },
    ),
    (
        "bg_BG",
        Locale {
            thousands: Some(0),
            point: b',',
            indian: false,
        },
    ),
];

/// Locales only MariaDB knows.
const MARIA_LOCALES: &[(&str, Locale)] = &[
    (
        "ka_GE",
        Locale {
            thousands: Some(b' '),
            point: b',',
            indian: false,
        },
    ),
    ("sw_KE", EN),
];

/// The locale of a name (case does not matter); `None` when unknown.
/// MySQL still knows `sr_YU`, which it warns is `sr_RS`'s old name.
fn locale(ctx: &mut Ctx<'_>, name: &[u8]) -> Option<Locale> {
    let name = std::str::from_utf8(name).ok()?;
    let find = |table: &[(&str, Locale)]| {
        table.iter().find_map(|(names, l)| {
            names
                .split_ascii_whitespace()
                .any(|n| n.eq_ignore_ascii_case(name))
                .then_some(*l)
        })
    };
    if let Some(l) = find(LOCALES) {
        return Some(l);
    }
    if ctx.maria() {
        return find(MARIA_LOCALES);
    }
    if name.eq_ignore_ascii_case("sr_YU") {
        ctx.warn(
            1287,
            "'sr_YU' is deprecated and will be removed in a future release. Please use sr_RS instead",
        );
        return Some(Locale {
            thousands: None,
            point: b'.',
            indian: false,
        });
    }
    None
}

/// Writes `digits` (a plain number: an optional sign, digits, an optional
/// point and fraction) in a locale.
fn localize(plain: &str, l: Locale) -> Vec<u8> {
    let (sign, rest) = match plain.strip_prefix('-') {
        Some(r) => ("-", r),
        None => ("", plain),
    };
    let (int, frac) = match rest.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (rest, None),
    };
    let mut out = Vec::with_capacity(plain.len() + plain.len() / 3 + 2);
    out.extend_from_slice(sign.as_bytes());
    let digits = int.as_bytes();
    match l.thousands {
        None => out.extend_from_slice(digits),
        Some(sep) => {
            // Group boundaries counted from the right: 3, then 3 or 2.
            let n = digits.len();
            let mut marks = Vec::new();
            let mut at = 3;
            while at < n {
                marks.push(n - at);
                at += if l.indian { 2 } else { 3 };
            }
            for (i, d) in digits.iter().enumerate() {
                if marks.contains(&i) {
                    out.push(sep);
                }
                out.push(*d);
            }
        }
    }
    if let Some(f) = frac {
        out.push(l.point);
        out.extend_from_slice(f.as_bytes());
    }
    out
}

/// `FORMAT(x, d [, locale])`: `x` rounded to `d` decimals with thousands
/// separators. An exact number rounds half away from zero; anything else
/// is read as a double and rounded the way `ROUND` rounds doubles.
pub(crate) fn format(args: Vec<Node>, env: Env<'_>) -> Node {
    let mut it = args.into_iter();
    let (Some(x), Some(d)) = (it.next(), it.next()) else {
        unreachable!("FORMAT takes two or three arguments");
    };
    let loc = it.next().map(|l| string_node(l, env));
    let exact = x.ty.kind.is_exact() && !x.ty.hex;
    let max_dec: i64 = if env.maria() { 38 } else { 30 };
    // MySQL's length: the number's text, a separator per three of its
    // characters, and 32 more; MariaDB's is left at that too.
    let sl = match x.ty.kind {
        Kind::Null => 0,
        Kind::Double if !env.maria() => 22,
        _ => x.ty.len,
    };
    let chars = sl + sl / 3 + 32;
    let ty = ExprType::string(chars, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    // The locale is resolved before the number is read; a constant one
    // once, while the statement is prepared.
    let fixed_locale = match &loc {
        Some(n) if n.konst => Some(env.at_prepare(|ctx| resolve_locale(ctx, n, &[]))),
        _ => None,
    };
    Node::new(ty, "format", move |ctx, row| {
        let l = match (&fixed_locale, &loc) {
            (Some(l), _) => match l {
                Ok(l) => *l,
                Err(e) => return Err(e.clone()),
            },
            (None, None) => EN,
            (None, Some(n)) => resolve_locale(ctx, n, row)?,
        };
        let dd = |ctx: &mut Ctx<'_>| -> Result<Option<i64>> {
            // MariaDB reads the decimals twice; both servers keep their
            // low 32 bits, as a signed number.
            if ctx.maria() {
                d.int_typed(ctx, row)?;
            }
            Ok(d.int_typed(ctx, row)?.map(|i| i64::from(i.v as i32)))
        };
        // The decimals are read first: NULL ends the call there.
        let Some(dd) = dd(ctx)? else {
            return Ok(Value::Null);
        };
        let plain = if exact {
            let Some(v) = x.dec(ctx, row)? else {
                return Ok(Value::Null);
            };
            let dd = dd.clamp(0, max_dec) as u32;
            fixed_dec(&v, dd)
        } else {
            let v = x.f64(ctx, row)?;
            let Some(v) = v else {
                return Ok(Value::Null);
            };
            let dd = dd.clamp(0, max_dec);
            let r = super::numeric::round_double(v, dd, false);
            let mut s = conv::fmt_double_fixed(r, dd as u8);
            if ctx.maria()
                && s.starts_with('-')
                && s.bytes().all(|c| matches!(c, b'-' | b'0' | b'.'))
            {
                s.remove(0);
            }
            s
        };
        Ok(wrap(localize(&plain, l), binary))
    })
}

/// The locale `FORMAT`'s third argument names; `en_US` with warning 1649
/// when the name is unknown or `NULL`.
fn resolve_locale(ctx: &mut Ctx<'_>, n: &Node, row: &[Value]) -> Result<Locale> {
    let name = text(n.eval(ctx, row)?);
    Ok(match name.as_deref().and_then(|b| locale(ctx, b)) {
        Some(l) => l,
        None => {
            let shown = match &name {
                None => "NULL".to_owned(),
                Some(b) => String::from_utf8_lossy(b).chars().take(64).collect(),
            };
            ctx.warn(1649, format!("Unknown locale: '{shown}'"));
            EN
        }
    })
}

/// A decimal rounded half away from zero to exactly `d` decimals.
fn fixed_dec(v: &Dec, d: u32) -> String {
    let r = v.round(d as i32, Round::HalfUp);
    let (int, mut frac) = r.split_digits();
    while frac.len() < d as usize {
        frac.push(0);
    }
    frac.truncate(d as usize);
    let zero = int.iter().all(|c| *c == 0) && frac.iter().all(|c| *c == 0);
    let mut s = String::new();
    if r.neg && !zero {
        s.push('-');
    }
    if int.is_empty() {
        s.push('0');
    }
    for c in &int {
        s.push(char::from(b'0' + c));
    }
    if d > 0 {
        s.push('.');
        for c in &frac {
            s.push(char::from(b'0' + c));
        }
    }
    s
}

/// `QUOTE(s)`: `s` as a quoted SQL string literal (backslash, quote, NUL
/// and Ctrl-Z escaped); the word `NULL` for `NULL`. MariaDB keeps the
/// argument's collation, MySQL answers in the connection's.
pub(crate) fn quote(a: Node, env: Env<'_>) -> Node {
    let null_arg = a.ty.kind == Kind::Null;
    let a = string_node(a, env);
    let maria = env.maria();
    let len = u64::from(a.ty.len) * 2 + 2;
    let ty = if maria {
        if null_arg {
            ExprType::string(4, Collation::Binary, Derivation::Ignorable)
        } else {
            str_type(
                len,
                Dtc {
                    coll: a.ty.coll,
                    deriv: a.ty.deriv,
                    mb3: a.ty.mb3,
                },
                true,
            )
        }
    } else {
        ExprType::string(len.max(4) as u32, env.conn(), Derivation::Coercible)
    }
    .nullable();
    let binary = ty.coll == Collation::Binary;
    Node::new(ty, "quote", move |ctx, row| {
        let Some(b) = text(a.eval(ctx, row)?) else {
            return Ok(wrap(&b"NULL"[..], binary));
        };
        let mut out = Vec::with_capacity(b.len() + 2);
        out.push(b'\'');
        for &c in b.iter() {
            match c {
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'\'' => out.extend_from_slice(b"\\'"),
                0 => out.extend_from_slice(b"\\0"),
                0x1a => out.extend_from_slice(b"\\Z"),
                c => out.push(c),
            }
        }
        out.push(b'\'');
        Ok(wrap(out, binary))
    })
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// `UNHEX(s)`: the bytes a string of hexadecimal digits spells (an odd
/// count gets a leading zero); `NULL` for any other character, which
/// MySQL also warns about.
pub(crate) fn unhex(a: Node, env: Env<'_>) -> Node {
    let shown = a.text.clone();
    let numeric = a.ty.kind != Kind::Str;
    let a = string_node(a, env);
    let mb = if numeric { 1 } else { mb_max_len(a.ty.coll) };
    let bytes = (u64::from(a.ty.len) * u64::from(mb)).div_ceil(2);
    let mut ty =
        ExprType::string(bytes as u32, Collation::Binary, Derivation::Coercible).nullable();
    if env.maria() {
        ty.dec = 0;
    }
    Node::new(ty, "unhex", move |ctx, row| {
        let Some(b) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let mut out = Vec::with_capacity(b.len() / 2 + 1);
        let mut digits: Vec<u8> = Vec::with_capacity(b.len() + 1);
        if b.len() % 2 == 1 {
            digits.push(0);
        }
        for &c in b.iter() {
            match hex_digit(c) {
                Some(d) => digits.push(d),
                None => {
                    if !ctx.maria() {
                        ctx.warn(
                            1411,
                            format!("Incorrect string value: '{shown}' for function unhex"),
                        );
                    }
                    return Ok(Value::Null);
                }
            }
        }
        for p in digits.chunks(2) {
            out.push((p[0] << 4) | p[1]);
        }
        Ok(Value::bytes(&out))
    })
}

/// The digit value of `c` in `base`: up to base 36 letters of either case
/// count from 10; MariaDB's bases 37 to 62 read upper-case letters as 10
/// to 35 and lower-case letters as 36 to 61.
fn digit_in(c: u8, base: u32) -> Option<u32> {
    let v = match c {
        b'0'..=b'9' => u32::from(c - b'0'),
        b'A'..=b'Z' => u32::from(c - b'A') + 10,
        b'a'..=b'z' if base <= 36 => u32::from(c - b'a') + 10,
        b'a'..=b'z' => u32::from(c - b'a') + 36,
        _ => return None,
    };
    (v < base).then_some(v)
}

/// Reads a number in `base` the way the C library's string-to-integer
/// conversion does: spaces, a sign, digits up to the first that isn't
/// one. Unsigned reading wraps a negative number around 2^64; an overflow
/// sticks at the limit (MySQL's unsigned reading of a negative overflow
/// gives zero). Answers the value and whether to warn (no digit, or an
/// overflow).
fn read_in_base(s: &[u8], base: u32, signed: bool, maria: bool) -> (u64, bool) {
    let mut i = 0;
    while i < s.len() && is_space(s[i]) {
        i += 1;
    }
    let mut neg = false;
    if i < s.len() && (s[i] == b'-' || s[i] == b'+') {
        neg = s[i] == b'-';
        i += 1;
    }
    let start = i;
    let mut mag: u128 = 0;
    let mut over = false;
    while i < s.len() {
        let Some(d) = digit_in(s[i], base) else {
            break;
        };
        if !over {
            mag = mag * u128::from(base) + u128::from(d);
            if mag > u128::from(u64::MAX) {
                over = true;
            }
        }
        i += 1;
    }
    if i == start {
        return (0, true);
    }
    if signed {
        let limit = if neg { 1u128 << 63 } else { (1u128 << 63) - 1 };
        if over || mag > limit {
            return (
                if neg {
                    i64::MIN as u64
                } else {
                    i64::MAX as u64
                },
                true,
            );
        }
        let v = if neg {
            (mag as i128).wrapping_neg() as i64
        } else {
            mag as i64
        };
        return (v as u64, false);
    }
    if over {
        return (if neg && !maria { 0 } else { u64::MAX }, true);
    }
    let v = mag as u64;
    (if neg { v.wrapping_neg() } else { v }, false)
}

/// `v` written in `base` (upper-case letters; MariaDB's bases above 36
/// use lower-case letters for 36 to 61); a negative base writes it as a
/// signed number.
fn write_in_base(v: u64, base: i64) -> String {
    const D: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let (neg, mut m) = if base < 0 && (v as i64) < 0 {
        (true, (v as i64).unsigned_abs())
    } else {
        (false, v)
    };
    let b = base.unsigned_abs();
    if m == 0 {
        return "0".to_owned();
    }
    let mut out = Vec::new();
    while m > 0 {
        out.push(D[(m % b) as usize]);
        m /= b;
    }
    if neg {
        out.push(b'-');
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// `CONV(n, from, to)`, and `BIN(n)` and `OCT(n)` (`to` 2 and 8, `from`
/// 10). `n` is read as text in base `from` (as a signed number when
/// `from` is negative); MySQL reads a hexadecimal literal as its number.
/// Bases run from 2 to 36 (to 62 on MariaDB); another base, or an empty
/// string, is `NULL`.
pub(crate) fn conv_fn(
    n: Node,
    from: Option<Node>,
    to: Option<Node>,
    fixed: Option<(i64, i64)>,
    env: Env<'_>,
) -> Node {
    let hex = n.ty.hex && !env.maria();
    let n = if hex { n } else { string_node(n, env) };
    let ty = ExprType::string(65, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    let max_base: i64 = if env.maria() { 62 } else { 36 };
    Node::new(ty, "conv", move |ctx, row| {
        let (f, t) = match fixed {
            Some(ft) => ft,
            None => {
                let (Some(fnode), Some(tnode)) = (&from, &to) else {
                    return Ok(Value::Null);
                };
                let f = fnode.int_arg(ctx, row)?;
                let t = tnode.int_arg(ctx, row)?;
                let (Some(f), Some(t)) = (f, t) else {
                    return Ok(Value::Null);
                };
                (f, t)
            }
        };
        let ok = |b: i64| (2..=max_base).contains(&b.saturating_abs());
        if !ok(f) || !ok(t) {
            // The argument is still read.
            let _ = n.eval(ctx, row)?;
            return Ok(Value::Null);
        }
        let v = if hex {
            match n.int(ctx, row)? {
                None => return Ok(Value::Null),
                Some(i) => i.v as u64,
            }
        } else {
            let Some(b) = text(n.eval(ctx, row)?) else {
                return Ok(Value::Null);
            };
            if b.is_empty() {
                return Ok(Value::Null);
            }
            let (v, warn) = read_in_base(&b, f.unsigned_abs() as u32, f < 0, ctx.maria());
            if warn && !ctx.maria() {
                let s = String::from_utf8_lossy(&b);
                ctx.warn(1292, format!("Truncated incorrect DECIMAL value: '{s}'"));
            }
            v
        };
        Ok(wrap(write_in_base(v, t).into_bytes(), binary))
    })
}

/// `CHAR(n, ... [USING charset])`: each number's bytes, most significant
/// first, without leading zero bytes (a number is cut to 32 bits; `NULL`
/// is skipped). Without `USING` the result is a binary string; with it,
/// a string of that character set, `NULL` with a warning when the bytes
/// aren't valid there (MySQL also warns about ASCII's replaced bytes).
pub(crate) fn char_fn(args: Vec<Node>, using: Option<Collation>, env: Env<'_>) -> Node {
    let n = args.len() as u32;
    // (MariaDB counts a UTF-8 result's four bytes per number as one
    // character.)
    let chars = match using {
        Some(c) if c.charset() == Charset::Utf8mb4 && env.maria() => n,
        _ => n * 4,
    };
    let ty = match using {
        None => ExprType::string(chars, Collation::Binary, Derivation::Coercible),
        Some(c) => ExprType::string(chars, c, Derivation::Coercible),
    }
    .nullable();
    Node::new(ty, "char", move |ctx, row| {
        let mut out = Vec::with_capacity(args.len() * 4);
        for a in &args {
            let Some(i) = a.int_typed(ctx, row)? else {
                continue;
            };
            let v = i.v as u32;
            let bytes = v.to_be_bytes();
            let skip = if v == 0 {
                3
            } else {
                (v.leading_zeros() / 8) as usize
            };
            out.extend_from_slice(&bytes[skip..]);
        }
        let Some(coll) = using else {
            return Ok(Value::bytes(&out));
        };
        match coll.charset() {
            Charset::Binary => Ok(Value::bytes(&out)),
            Charset::Latin1 => Ok(Value::Str(out.into())),
            Charset::Ascii => {
                if let Some(bad) = out.iter().position(|b| *b > 0x7f) {
                    if !ctx.maria() {
                        ctx.warn(
                            1300,
                            format!(
                                "Invalid ascii character string: '{}'",
                                hex_upper(&out[bad..])
                            ),
                        );
                    }
                    for b in &mut out {
                        if *b > 0x7f {
                            *b = b'?';
                        }
                    }
                }
                Ok(Value::Str(out.into()))
            }
            Charset::Utf8mb4 => match std::str::from_utf8(&out) {
                Ok(_) => Ok(Value::Str(out.into())),
                Err(e) => {
                    let bad = &out[e.valid_up_to()..];
                    ctx.warn(
                        1300,
                        format!(
                            "Invalid utf8mb4 character string: '{}'",
                            hex_upper(&bad[..bad.len().min(3)])
                        ),
                    );
                    Ok(Value::Null)
                }
            },
        }
    })
}

fn hex_upper(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02X}");
    }
    s
}

/// `MD5(s)`, `SHA1(s)`/`SHA(s)`: the digest of the argument's bytes as
/// lower-case hexadecimal.
pub(crate) fn digest(name: &'static str, a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let chars = if name == "md5" { 32 } else { 40 };
    let ty = ExprType::string(chars, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    Node::new(ty, name, move |ctx, row| {
        let Some(b) = text(a.eval(ctx, row)?) else {
            return Ok(Value::Null);
        };
        let h = if name == "md5" {
            hash::hex_lower(&hash::md5(&b))
        } else {
            hash::hex_lower(&hash::sha1(&b))
        };
        Ok(wrap(h.into_bytes(), binary))
    })
}

/// `SHA2(s, bits)`: SHA-224, -256, -384 or -512 (0 means 256); another
/// length is `NULL` with warning 1583. A `NULL` length is 256 on MariaDB
/// and that same `NULL` with the warning on MySQL.
pub(crate) fn sha2(a: Node, bits: Node, env: Env<'_>) -> Result<Node> {
    let a = string_node(a, env);
    let chars = match bits.const_i64(env)? {
        Some(224) => 56,
        Some(384) => 96,
        Some(512) => 128,
        Some(_) => 64,
        // A length known only per row: the longest digest.
        None if !bits.konst => 128,
        None => 64,
    };
    let ty = ExprType::string(chars, env.conn(), Derivation::Coercible).nullable();
    let binary = env.conn() == Collation::Binary;
    Ok(Node::new(ty, "sha2", move |ctx, row| {
        // The length is checked first, also for a NULL string.
        let n = match (bits.int_arg(ctx, row)?, ctx.maria()) {
            (Some(n), _) => Some(n),
            (None, true) => Some(0),
            (None, false) => None,
        };
        let n = n.map(|n| if n == 0 { 256 } else { n });
        let valid = n.filter(|n| matches!(n, 224 | 256 | 384 | 512));
        if valid.is_none() {
            ctx.warn(
                1583,
                "Incorrect parameters in the call to native function 'sha2'",
            );
        }
        let b = text(a.eval(ctx, row)?);
        let (Some(b), Some(n)) = (b, valid) else {
            return Ok(Value::Null);
        };
        match hash::sha2(&b, n as u32) {
            Some(h) => Ok(wrap(hash::hex_lower(&h).into_bytes(), binary)),
            None => Ok(Value::Null),
        }
    }))
}

/// `CRC32(s)`: the checksum of the argument's bytes, unsigned.
pub(crate) fn crc32(a: Node, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let ty = ExprType {
        prec: 10,
        ..int32(10, env)
    };
    let ty = ExprType {
        unsigned: true,
        ..ty
    }
    .with_null(a.ty.nullable);
    Node::new(ty, "crc32", move |ctx, row| {
        Ok(match text(a.eval(ctx, row)?) {
            None => Value::Null,
            Some(b) => Value::UInt(u64::from(hash::crc32(&b))),
        })
    })
}

/// `CONVERT(x USING charset)`: `x`'s text converted to the character set
/// (a binary string's bytes are read as text of it). A character the set
/// lacks becomes `?` (MariaDB warns 1977); bytes that are no UTF-8 become
/// `?` with warning 1300 on MariaDB and make the result NULL on MySQL.
/// The result holds the set's collation as a cast does (MariaDB) or as a
/// column does (MySQL).
pub(crate) fn convert_using(a: Node, coll: Collation, env: Env<'_>) -> Node {
    let a = string_node(a, env);
    let from = a.ty.coll.charset();
    let to = coll.charset();
    // (A conversion to binary is held as a binary cast is.)
    let deriv = if env.maria() && to != Charset::Binary {
        Derivation::Cast
    } else {
        Derivation::Implicit
    };
    // (Both servers report the conversion as nullable.)
    let ty = ExprType {
        ascii: to == Charset::Ascii,
        ..ExprType::string(a.ty.len, coll, deriv).nullable()
    };
    Node::new(ty, "convert", move |ctx, row| {
        let (b, src) = match a.eval(ctx, row)? {
            Value::Str(b) if from != Charset::Binary => (b, from),
            Value::Str(b) | Value::Bytes(b) | Value::Json(b) => (b, Charset::Binary),
            _ => return Ok(Value::Null),
        };
        let (out, trouble) = crate::charset::convert(&b, src, to);
        match trouble {
            Some(crate::charset::Trouble::Unmappable(ch)) if ctx.maria() => {
                ctx.warn(
                    1977,
                    format!(
                        "Cannot convert '{}' character 0x{} to '{}'",
                        crate::charset::name(src),
                        crate::charset::hex(&ch),
                        crate::charset::name(to)
                    ),
                );
            }
            Some(crate::charset::Trouble::Invalid(bad)) => {
                let bad = &bad[..bad.len().min(3)];
                if ctx.maria() {
                    ctx.warn(
                        1300,
                        format!(
                            "Invalid {} character string: '{}'",
                            crate::charset::name(to),
                            crate::charset::escaped(bad)
                        ),
                    );
                } else {
                    ctx.warn(
                        1300,
                        format!(
                            "Invalid {} character string: '{}'",
                            crate::charset::name(to),
                            crate::charset::hex(bad)
                        ),
                    );
                    return Ok(Value::Null);
                }
            }
            _ => {}
        }
        Ok(wrap(out, to == Charset::Binary))
    })
}

/// A string literal with a character-set introducer (`_latin1'...'`,
/// `_binary 0x...`): its bytes, as written, are text of that set.
pub(crate) fn introducer(bytes: &[u8], text: String, coll: Collation) -> Node {
    let cs = coll.charset();
    let ty = ExprType {
        ascii: bytes.is_ascii(),
        ..ExprType::string(cs.char_count(bytes) as u32, coll, Derivation::Coercible)
    };
    let v = if cs == Charset::Binary {
        Value::bytes(bytes)
    } else {
        Value::Str(bytes.to_vec().into())
    };
    Node::literal(ty, text, v)
}
