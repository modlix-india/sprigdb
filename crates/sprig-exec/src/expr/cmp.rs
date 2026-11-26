//! Comparison: which type two operands are compared in, the comparison
//! operators, `IN` and `BETWEEN`.

use std::cmp::Ordering;

use bytes::Bytes;
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::Value;

use super::{BinOp, Env, Node};
use crate::conv::{self, Info, IntVal};
use crate::error::Result;
use crate::num::Dec;
use crate::session::Ctx;
use crate::temporal;
use crate::types::{Dtc, ExprType, Kind};

/// The family an operand belongs to when a comparison type is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    Str,
    Int,
    Dec,
    Real,
    Date,
    DateTime,
    Time,
    Null,
}

pub(crate) fn side(ty: &ExprType) -> Side {
    match ty.kind {
        Kind::Null => Side::Null,
        Kind::Int | Kind::Year | Kind::Bit => Side::Int,
        Kind::Decimal => Side::Dec,
        Kind::Double | Kind::Float => Side::Real,
        Kind::Str => Side::Str,
        Kind::Date => Side::Date,
        Kind::DateTime | Kind::Timestamp => Side::DateTime,
        Kind::Time => Side::Time,
    }
}

/// The comparison type of two operand families. An integer against a
/// string compares as exact decimals on MariaDB and as doubles on MySQL.
pub(crate) fn pair(a: Side, b: Side, maria: bool) -> Side {
    use Side::{Date, DateTime, Dec, Int, Null, Real, Str, Time};
    match (a, b) {
        (Null, x) | (x, Null) => x,
        (Str, Str) => Str,
        (Int, Int) => Int,
        (Date, Date) => Date,
        (Time, Time) => Time,
        (Date | DateTime, Time) | (Time, Date | DateTime) | (DateTime, _) | (_, DateTime) => {
            DateTime
        }
        (Date, _) | (_, Date) => DateTime,
        (Time, _) | (_, Time) => Time,
        (Int, Str) | (Str, Int) => {
            if maria {
                Dec
            } else {
                Real
            }
        }
        (Int | Dec, Int | Dec) => Dec,
        _ => Real,
    }
}

/// How two non-`NULL` values are ordered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CmpKind {
    Str(Collation),
    Int,
    Dec,
    Real,
    Date,
    DateTime,
    Time,
    /// MySQL's comparison of JSON values (an operand of a JSON type).
    Json,
}

/// A value converted into its comparison type.
#[derive(Debug, Clone)]
pub(crate) enum Key {
    Str(Bytes),
    Int(IntVal),
    Dec(Dec),
    Real(f64),
    Json(crate::func::json::J),
}

/// Whether MySQL compares operands of these types as JSON values: one of
/// them is a JSON value. (MariaDB's JSON is text.)
fn json_compare(types: &[&ExprType], env: Env<'_>) -> bool {
    !env.maria() && types.iter().any(|t| t.json)
}

/// A comparison chosen at compile time.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Comparator {
    pub(crate) kind: CmpKind,
    /// MariaDB's comparison operators, `IN` and `NULLIF` compare a decimal
    /// operand rounded to its type's decimals: `(1/3) = 0.3333` is true
    /// though the quotient holds more digits, and a `CAST(x AS CHAR)`
    /// (no decimals) compares as a whole number. `BETWEEN`, `CASE`,
    /// `GREATEST` and `LEAST` compare the digits as they are.
    pub(crate) rounds: bool,
    /// Whether that rounding also applies to a string with fixed
    /// decimals (it does for one comparison, not for a list of items).
    pub(crate) rounds_strings: bool,
    /// MySQL's `CASE` and its `IN` over a list it cannot sort compare two
    /// integers by their 64 bits, whatever their signs: -1 equals
    /// 18446744073709551615 there.
    pub(crate) bits: bool,
    /// MySQL's comparison operators refuse a string that is no date when
    /// they compare dates (error 1525).
    pub(crate) strict_dates: bool,
    /// MariaDB's comparison operators compare two doubles whose decimals
    /// are both fixed as equal when they differ by less than half a unit
    /// of one more decimal than the larger count.
    pub(crate) real_precision: Option<f64>,
}

impl Comparator {
    /// A plain comparison of `kind`.
    pub(crate) fn of(kind: CmpKind) -> Self {
        Self {
            kind,
            rounds: false,
            rounds_strings: false,
            bits: false,
            strict_dates: false,
            real_precision: None,
        }
    }
}

fn text_of(v: &Value, ty: &ExprType, maria: bool) -> Bytes {
    match v {
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => s.clone(),
        other => conv::to_text_in_string(other, ty, maria).unwrap_or_default(),
    }
}

/// The character sets a string of type `ty` is converted between to be
/// compared under `coll`, when they differ (and neither is binary).
pub(crate) fn recode_for(
    ty: &ExprType,
    coll: Collation,
) -> Option<(
    sprig_engine::keys::collation::Charset,
    sprig_engine::keys::collation::Charset,
)> {
    use sprig_engine::keys::collation::Charset;
    let (from, to) = (ty.coll.charset(), coll.charset());
    (ty.kind == Kind::Str && from != to && from != Charset::Binary && to != Charset::Binary)
        .then_some((from, to))
}

/// The decimals a MariaDB comparison rounds an operand of type `ty` to.
fn rounding_scale(ty: &ExprType) -> Option<i32> {
    match ty.kind {
        Kind::Decimal => Some(i32::from(ty.dec)),
        Kind::Str if ty.dec < crate::types::NOT_FIXED && !ty.hex => Some(i32::from(ty.dec)),
        _ => None,
    }
}

impl Comparator {
    /// Converts one operand's value (not `NULL`) into the comparison type.
    /// `None` means the comparison is `NULL` (a value that couldn't be read
    /// as the comparison's date or time).
    pub(crate) fn key(
        &self,
        ctx: &mut Ctx<'_>,
        v: &Value,
        info: Info,
        text: &str,
        ty: &ExprType,
    ) -> Result<Option<Key>> {
        Ok(match self.kind {
            CmpKind::Str(coll) => {
                let b = text_of(v, ty, ctx.maria());
                Some(Key::Str(match recode_for(ty, coll) {
                    Some((from, to)) if matches!(v, Value::Str(_)) => {
                        crate::charset::convert(&b, from, to).0.into()
                    }
                    _ => b,
                }))
            }
            CmpKind::Int => conv::conv_int(ctx, v, info, text)?.map(Key::Int),
            CmpKind::Dec => conv::conv_dec(ctx, v, info).map(|d| self.rounded(ctx, d, ty)),
            CmpKind::Real => conv::conv_f64(ctx, v, info).map(Key::Real),
            CmpKind::Date | CmpKind::DateTime | CmpKind::Time => {
                temporal::cmp_key_strict(ctx, self.kind, v, self.strict_dates)?.map(Key::Dec)
            }
            CmpKind::Json => crate::func::json::compared(v, ty).map(Key::Json),
        })
    }

    fn rounded(&self, ctx: &mut Ctx<'_>, d: Dec, ty: &ExprType) -> Key {
        let applies = self.rounds && (ty.kind != Kind::Str || self.rounds_strings);
        match rounding_scale(ty) {
            Some(scale) if applies && d.scale as i32 > scale => {
                let r = d.round(scale, crate::num::Round::HalfUp);
                // A negative number rounded to zero stays negative on
                // MariaDB, and below zero.
                if d.neg && r.is_zero() && !d.is_zero() {
                    ctx.negative_zeros += 1;
                }
                Key::Dec(r)
            }
            _ => Key::Dec(d),
        }
    }

    /// Evaluates an operand and converts it into the comparison type.
    /// `None` when the operand is `NULL` (or can't be read as the
    /// comparison's date or time).
    pub(crate) fn node_key(
        &self,
        ctx: &mut Ctx<'_>,
        n: &Node,
        row: &[Value],
    ) -> Result<Option<Key>> {
        if n.alt.is_some() {
            match self.kind {
                CmpKind::Real => return Ok(n.f64(ctx, row)?.map(Key::Real)),
                CmpKind::Dec => {
                    let d = n.dec(ctx, row)?;
                    return Ok(d.map(|d| self.rounded(ctx, d, &n.ty)));
                }
                CmpKind::Str(_) => {
                    if let Some(t) = n.alt.as_ref().and_then(|a| a.text.as_ref()) {
                        return Ok(match t(ctx, row)? {
                            Value::Str(b) | Value::Bytes(b) => Some(Key::Str(b)),
                            _ => None,
                        });
                    }
                }
                CmpKind::Int => return Ok(n.int(ctx, row)?.map(Key::Int)),
                _ => {}
            }
        }
        let temporal = matches!(self.kind, CmpKind::Date | CmpKind::DateTime | CmpKind::Time);
        let v = match n.alt.as_ref().and_then(|a| a.date_key.as_ref()) {
            Some(f) if temporal => f(ctx, row)?,
            _ => n.eval(ctx, row)?,
        };
        if v.is_null() {
            return Ok(None);
        }
        self.key(ctx, &v, n.info(), &n.text, &n.ty)
    }

    /// Orders two converted values.
    pub(crate) fn order(&self, a: &Key, b: &Key) -> Option<Ordering> {
        match (a, b) {
            (Key::Str(x), Key::Str(y)) => match self.kind {
                CmpKind::Str(coll) => Some(coll.compare(x, y)),
                _ => Some(x.cmp(y)),
            },
            (Key::Int(x), Key::Int(y)) if self.bits && x.v == y.v => Some(Ordering::Equal),
            (Key::Int(x), Key::Int(y)) => Some(x.wide().cmp(&y.wide())),
            (Key::Dec(x), Key::Dec(y)) => Some(x.cmp(y)),
            (Key::Json(x), Key::Json(y)) => Some(crate::func::json::mysql_cmp(x, y)),
            (Key::Real(x), Key::Real(y)) => match self.real_precision {
                Some(p) if x == y || (x - y).abs() < p => Some(Ordering::Equal),
                _ => x.partial_cmp(y),
            },
            _ => None,
        }
    }
}

fn kind_of(s: Side, coll: Option<Collation>) -> CmpKind {
    match s {
        Side::Str => CmpKind::Str(coll.unwrap_or(Collation::Binary)),
        Side::Int | Side::Null => CmpKind::Int,
        Side::Dec => CmpKind::Dec,
        Side::Real => CmpKind::Real,
        Side::Date => CmpKind::Date,
        Side::DateTime => CmpKind::DateTime,
        Side::Time => CmpKind::Time,
    }
}

/// A hex literal is a number unless everything it meets is a string.
fn effective_side(ty: &ExprType, others_all_str: bool) -> Side {
    if ty.hex && !others_all_str {
        Side::Int
    } else {
        side(ty)
    }
}

/// The collation string operands compare under, merged over the string
/// ones; a mix that cannot be merged is reported over all operands, as
/// the servers count them (the error number depends on the count).
fn merged_collation(all: &[&ExprType], op: &str, extra: usize, env: Env<'_>) -> Result<Collation> {
    let string_tys: Vec<&ExprType> = all
        .iter()
        .filter(|t| t.kind == Kind::Str)
        .copied()
        .collect();
    let strings: Vec<Dtc> = string_tys.iter().map(|t| Dtc::of(t, env.conn())).collect();
    match crate::types::aggregate_types(&string_tys, env.conn(), op, true) {
        Ok(d) => Ok(d.coll),
        Err(e) => {
            let mut every: Vec<Dtc> = all.iter().map(|t| Dtc::of(t, env.conn())).collect();
            // Operands that are counted but not compared (MySQL's ESCAPE).
            for _ in 0..extra {
                every.push(Dtc::new(env.conn(), crate::types::Derivation::Coercible));
            }
            if every.len() == strings.len() {
                Err(e)
            } else {
                Err(crate::types::illegal_mix(&every, op))
            }
        }
    }
}

/// The comparison type of two operands of a comparison operator. Beyond
/// the families' own rule, both servers compare a decimal that is not a
/// constant with a constant string as decimals, not as doubles (so a
/// `DECIMAL` column against a quoted number is exact).
fn pair_nodes(a: &Node, b: &Node, all_str: bool, env: Env<'_>) -> Side {
    let (sa, sb) = (
        effective_side(&a.ty, all_str),
        effective_side(&b.ty, all_str),
    );
    if !env.maria() {
        // MySQL compares a date, datetime or time that is no column with
        // a number as numbers.
        let dated = |n: &Node, s: Side| {
            matches!(s, Side::Date | Side::DateTime | Side::Time) && !n.ty.column
        };
        let number = |s: Side| matches!(s, Side::Int | Side::Dec | Side::Real);
        if (dated(a, sa) && number(sb)) || (dated(b, sb) && number(sa)) {
            return if sa == Side::Real || sb == Side::Real {
                Side::Real
            } else {
                Side::Dec
            };
        }
    }
    let base = pair(sa, sb, env.maria());
    let dec_const_str = |d: &Node, s: &Node, sd: Side, ss: Side| {
        sd == Side::Dec && ss == Side::Str && !d.konst && s.konst
    };
    if base == Side::Real && (dec_const_str(a, b, sa, sb) || dec_const_str(b, a, sb, sa)) {
        return Side::Dec;
    }
    base
}

/// Which constants [`int_literal`] converts.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fold {
    /// Strings that are whole numbers.
    Strings,
    /// Strings and doubles that are whole numbers.
    StringsAndDoubles,
    /// MySQL's single comparison: strings, doubles and decimals whose
    /// stored integer equals their double value (see [`loose_int_literal`]).
    Loose,
}

/// Whether a node is a `BIGINT` column read as it is: the columns whose
/// values a double cannot hold, for which the servers convert a constant
/// instead (see [`int_literal`]).
pub(crate) fn int_column(n: &Node) -> bool {
    n.ty.column && n.ty.kind == Kind::Int && n.ty.width == 8
}

/// A constant that both servers convert, while preparing the statement,
/// to the type of the integer column it is compared with: the integer
/// literal it stands for, if it is exactly a whole number in the 64-bit
/// range (`'9007199254740993'`, `'1e2'`, `9.007199254740992e15`). The
/// comparison is then one of integers: exact, and without a conversion
/// (or its warning) per row. `None` leaves the constant as it is.
pub(crate) fn int_literal(col: &Node, k: &Node, fold: Fold, env: Env<'_>) -> Option<Node> {
    if !k.konst || k.ty.hex {
        return None;
    }
    let side = side(&k.ty);
    let wanted = match fold {
        Fold::Strings => side == Side::Str,
        Fold::StringsAndDoubles => matches!(side, Side::Str | Side::Real),
        Fold::Loose => matches!(side, Side::Str | Side::Real | Side::Dec),
    };
    if !wanted {
        return None;
    }
    let mut scratch = Ctx::new(env.s);
    let v = k.eval(&mut scratch, &[]).ok()?;
    if fold == Fold::Loose {
        return loose_int_literal(col, k, &v, env);
    }
    let d = match &v {
        Value::Str(s) | Value::Bytes(s) => {
            let sc = conv::scan_number(s);
            if !sc.digits || !s[sc.end..].iter().all(|&b| conv::is_space(b)) {
                return None;
            }
            conv::prefix_to_dec(&s[sc.start..sc.end])
        }
        Value::Double(x) if x.is_finite() => conv::f64_to_dec(*x),
        _ => return None,
    };
    let whole = d.round(0, crate::num::Round::Truncate);
    if whole.cmp(&d) != Ordering::Equal {
        return None;
    }
    // The literal takes the column's signedness.
    let (ty, value) = if col.ty.unsigned {
        (ExprType::bigint(true), Value::UInt(whole.to_u64().ok()?))
    } else {
        (ExprType::bigint(false), Value::Int(whole.to_i64().ok()?))
    };
    let mut lit = Node::literal(ty, k.text.clone(), value);
    lit.konst = true;
    Some(lit)
}

/// MySQL's conversion of a constant for a single comparison with a
/// `BIGINT` column: the constant is stored as the column would store it
/// (a string's leading number rounded to a whole one, a decimal or double
/// rounded) and kept in that form when the stored integer equals the
/// constant's own double value (a decimal must equal it exactly). So
/// `'abc'` and `''` become 0 (the string's
/// conversion warning is raised once, while preparing), `'1.5'` stays.
fn loose_int_literal(col: &Node, k: &Node, v: &Value, env: Env<'_>) -> Option<Node> {
    let (stored, f) = match v {
        Value::Str(s) | Value::Bytes(s) => {
            let sc = conv::scan_number(s);
            let d = if sc.digits {
                conv::prefix_to_dec(&s[sc.start..sc.end])
            } else {
                Dec::zero()
            };
            let mut scratch = Ctx::new(env.s);
            (d, conv::str_to_f64(&mut scratch, s))
        }
        Value::Double(x) if x.is_finite() => (conv::f64_to_dec(*x), *x),
        Value::Decimal(d) => {
            let d = Dec::from_decimal(d);
            let f = d.to_f64();
            (d, f)
        }
        _ => return None,
    };
    let whole = stored.round(0, crate::num::Round::HalfUp);
    // A decimal is compared with the stored integer exactly.
    if matches!(v, Value::Decimal(_)) && whole.cmp(&stored) != Ordering::Equal {
        return None;
    }
    let (ty, value) = if col.ty.unsigned {
        let u = whole.to_u64().ok()?;
        if u as f64 != f {
            return None;
        }
        (ExprType::bigint(true), Value::UInt(u))
    } else {
        let i = whole.to_i64().ok()?;
        if i as f64 != f {
            return None;
        }
        (ExprType::bigint(false), Value::Int(i))
    };
    if let Value::Str(s) | Value::Bytes(s) = v {
        env.at_prepare(|ctx| conv::str_to_f64(ctx, s));
    }
    let mut lit = Node::literal(ty, k.text.clone(), value);
    lit.konst = true;
    Some(lit)
}

/// The comparator of a two-operand comparison (`=` and its kin,
/// `NULLIF`).
pub(crate) fn comparator2(a: &Node, b: &Node, op: &str, env: Env<'_>) -> Result<Comparator> {
    if json_compare(&[&a.ty, &b.ty], env) {
        return Ok(Comparator::of(CmpKind::Json));
    }
    let all_str = [a, b]
        .iter()
        .all(|n| matches!(n.ty.kind, Kind::Str | Kind::Null));
    let side = pair_nodes(a, b, all_str, env);
    let coll = if side == Side::Str {
        let c = merged_collation(&[&a.ty, &b.ty], op, 0, env)?;
        if !super::convertible(a, c, env) || !super::convertible(b, c, env) {
            let dtcs = [Dtc::of(&a.ty, env.conn()), Dtc::of(&b.ty, env.conn())];
            return Err(crate::types::illegal_mix(&dtcs, op));
        }
        Some(c)
    } else {
        None
    };
    // (See `real_precision`.)
    let real_precision = (env.maria()
        && side == Side::Real
        && a.ty.dec < crate::types::NOT_FIXED
        && b.ty.dec < crate::types::NOT_FIXED)
        .then(|| 5.0 * 10f64.powi(-(i32::from(a.ty.dec.max(b.ty.dec)) + 1)));
    Ok(Comparator {
        kind: kind_of(side, coll),
        rounds: env.maria(),
        rounds_strings: env.maria(),
        bits: false,
        // (A constant string that is no date fails while the statement is
        // prepared; one computed per row compares as the zero date.)
        strict_dates: false,
        real_precision,
    })
}

/// The comparator for the operands of one comparison (three for
/// `BETWEEN`; any number for `GREATEST` and `LEAST`): the comparison type
/// is merged from left to right, and string operands merge their
/// collations. With `null_is_string`, a `NULL` literal takes part as the
/// string the servers type it as (which turns two exact numbers'
/// comparison into an inexact one).
pub(crate) fn comparator_n(
    types: &[&ExprType],
    op: &str,
    null_is_string: bool,
    env: Env<'_>,
) -> Result<Comparator> {
    if json_compare(types, env) {
        return Ok(Comparator::of(CmpKind::Json));
    }
    let all_str = types
        .iter()
        .all(|t| matches!(t.kind, Kind::Str | Kind::Null));
    let mut acc = Side::Null;
    for t in types {
        let mut s = effective_side(t, all_str);
        if s == Side::Null && null_is_string {
            s = Side::Str;
        }
        acc = pair(acc, s, env.maria());
    }
    let coll = if acc == Side::Str {
        Some(merged_collation(types, op, 0, env)?)
    } else {
        None
    };
    Ok(Comparator::of(kind_of(acc, coll)))
}

/// The comparator for operands given by their types alone (join keys,
/// `UNION` columns): the families' own rule, no constants involved.
pub(crate) fn comparator(types: &[&ExprType], op: &str, env: Env<'_>) -> Result<Comparator> {
    comparator_n(types, op, false, env)
}

/// MySQL's conversion of a constant compared with an integer column (see
/// [`int_literal`]); MariaDB compares the two as they are.
pub(crate) fn fold_for_column(a: Node, b: Node, env: Env<'_>) -> (Node, Node) {
    if env.maria() {
        return (a, b);
    }
    if int_column(&a) {
        if let Some(lit) = int_literal(&a, &b, Fold::Loose, env) {
            return (a, lit);
        }
    } else if int_column(&b) {
        if let Some(lit) = int_literal(&b, &a, Fold::Loose, env) {
            return (lit, b);
        }
    }
    (a, b)
}

/// `= <> < <= > >= <=>`.
pub(crate) fn compare(op: BinOp, a: Node, b: Node, env: Env<'_>) -> Result<Node> {
    let (a, b) = fold_for_column(a, b, env);
    let mut cmp = comparator2(&a, &b, op.sql(), env)?;
    if !env.maria() && cmp.kind == CmpKind::DateTime {
        // MySQL compares a DATE column with a constant number as dates
        // (the number's time of day is dropped).
        let date_column = |t: &Node| t.ty.column && t.ty.kind == Kind::Date;
        let number = |n: &Node| {
            n.konst && matches!(n.ty.kind, Kind::Int | Kind::Decimal | Kind::Double) && !n.ty.hex
        };
        if (date_column(&a) && number(&b)) || (date_column(&b) && number(&a)) {
            cmp.kind = CmpKind::Date;
        }
    }
    if !env.maria() && matches!(cmp.kind, CmpKind::Date | CmpKind::DateTime) {
        // MySQL compares a TIME column with a constant date or datetime
        // as times (the date's time of day: midnight for a DATE).
        let time_column = |t: &Node| t.ty.column && t.ty.kind == Kind::Time;
        let dated_constant =
            |n: &Node| n.konst && !n.ty.column && matches!(n.ty.kind, Kind::Date | Kind::DateTime);
        if (time_column(&a) && dated_constant(&b)) || (time_column(&b) && dated_constant(&a)) {
            cmp.kind = CmpKind::Time;
        }
    }
    if !env.maria() && cmp.kind == CmpKind::Time {
        // MySQL compares a TIME with a constant string that is not
        // cleanly a time as two strings.
        let not_a_time = |n: &Node| {
            if n.ty.kind != Kind::Str || !n.konst {
                return false;
            }
            let mut scratch = Ctx::new(env.s);
            match n.eval(&mut scratch, &[]) {
                Ok(Value::Str(s) | Value::Bytes(s)) => !matches!(
                    temporal::parse_time(&scratch, &s),
                    Some((_, temporal::Read::Clean))
                ),
                _ => false,
            }
        };
        // ... and a TIME that is no column with any constant string
        // ('10:30:00' >= CAST('20240115103000' AS TIME(4)) is false).
        let full_datetime = |t: &Node, n: &Node| {
            !t.ty.column && t.ty.kind == Kind::Time && n.ty.kind == Kind::Str && n.konst
        };
        if not_a_time(&a) || not_a_time(&b) || full_datetime(&a, &b) || full_datetime(&b, &a) {
            cmp.kind = CmpKind::Str(env.conn());
        }
    }
    if !env.maria() && matches!(cmp.kind, CmpKind::Date | CmpKind::DateTime) {
        // MySQL refuses a constant string that is no date while it
        // prepares the comparison.
        for n in [&a, &b] {
            if n.ty.kind == Kind::Str && n.konst {
                let v = env.at_prepare(|ctx| n.eval(ctx, &[]))?;
                env.at_prepare(|ctx| temporal::cmp_key(ctx, cmp.kind, &v))?;
            }
        }
    }
    let (a, b) = (super::cached(a, env), super::cached(b, env));
    let null_safe = op == BinOp::NullSafeEq;
    let ty = ExprType::boolean().with_null(!null_safe && (a.ty.nullable || b.ty.nullable));
    let text = format!("({} {} {})", a.text, op.sql(), b.text);
    let mysql = !env.maria();
    // MySQL reads `NULL <=> x` as `x IS NULL`, and evaluates an operand of
    // `<=>` that turned out NULL a second time.
    let null_literal = |n: &Node| mysql && null_safe && n.ty.kind == Kind::Null && n.lit.is_some();
    // ... and so a constant left operand that turns out NULL.
    let null_left = mysql && null_safe && a.konst && a.lit.is_none() && {
        let mut scratch = Ctx::new(env.s);
        matches!(a.eval(&mut scratch, &[]), Ok(Value::Null))
    };
    let (a_null, b_null) = (null_literal(&a) || null_left, null_literal(&b));
    if a_null || b_null {
        let null_first = a_null;
        // (A computed NULL on the left is still computed, for its
        // conditions.)
        let left = (null_left && !null_literal(&a)).then(|| a.clone());
        let other = if null_first { b } else { a };
        if let Some(left) = left {
            let nullable = other.ty.nullable;
            return Ok(Node::new(ty, text, move |ctx, row| {
                // (Twice, as for any operand of `<=>` that is NULL.)
                left.eval(ctx, row)?;
                left.eval(ctx, row)?;
                if !nullable {
                    return Ok(Value::Int(0));
                }
                Ok(Value::Int(i64::from(other.eval(ctx, row)?.is_null())))
            }));
        }
        if !other.ty.nullable {
            if null_first {
                return Ok(Node::new(ty, text, |_, _| Ok(Value::Int(0))));
            }
            // (`x <=> NULL` still evaluates x: its error fails the
            // statement.)
            return Ok(Node::new(ty, text, move |ctx, row| {
                other.eval(ctx, row)?;
                Ok(Value::Int(0))
            }));
        }
        return Ok(Node::new(ty, text, move |ctx, row| {
            Ok(Value::Int(i64::from(other.eval(ctx, row)?.is_null())))
        }));
    }
    let twice = |n: &Node| mysql && null_safe && n.ty.nullable && n.lit.is_none();
    let (twice_a, twice_b) = (twice(&a), twice(&b));
    // MariaDB converts a constant operand once per statement that reads
    // tables (its conditions come once).
    let keep = |n: &Node| {
        if env.maria() && env.s.fold_constants {
            super::row::const_id(n)
        } else {
            None
        }
    };
    let (ia, ib) = (keep(&a), keep(&b));
    Ok(Node::new(ty, text, move |ctx, row| {
        let kx = super::row::element_key(&cmp, &a, ia, ctx, row)?;
        if kx.is_none() {
            if !null_safe {
                return Ok(Value::Null);
            }
            if twice_a {
                a.eval(ctx, row)?;
            }
        }
        let ky = super::row::element_key(&cmp, &b, ib, ctx, row)?;
        if ky.is_none() && twice_b {
            b.eval(ctx, row)?;
        }
        let ord = match (&kx, &ky) {
            (Some(kx), Some(ky)) => cmp.order(kx, ky),
            (None, None) if null_safe => return Ok(Value::Int(1)),
            _ if null_safe => return Ok(Value::Int(0)),
            _ => return Ok(Value::Null),
        };
        let Some(ord) = ord else {
            return Ok(if null_safe {
                Value::Int(0)
            } else {
                Value::Null
            });
        };
        let hit = match op {
            BinOp::Eq | BinOp::NullSafeEq => ord == Ordering::Equal,
            BinOp::Ne => ord != Ordering::Equal,
            BinOp::Lt => ord == Ordering::Less,
            BinOp::Le => ord != Ordering::Greater,
            BinOp::Gt => ord == Ordering::Greater,
            _ => ord != Ordering::Less,
        };
        Ok(Value::Int(i64::from(hit)))
    }))
}

/// One comparator per list item: each item is compared in the type its
/// pair with the left side chooses; string pairs share one collation
/// merged over all string operands. `rounds` and `bits` are the
/// comparators' flags (see [`Comparator`]).
pub(crate) fn pairwise(
    a: &Node,
    items: &[&Node],
    op: &str,
    rounds: bool,
    bits: bool,
    env: Env<'_>,
) -> Result<Vec<Comparator>> {
    let all_str = a.ty.kind == Kind::Str && items.iter().all(|n| n.ty.kind == Kind::Str);
    // (A NULL left side takes part as a string.)
    let coll = if matches!(a.ty.kind, Kind::Str | Kind::Null)
        && items.iter().any(|n| n.ty.kind == Kind::Str)
    {
        let mut all: Vec<&ExprType> = vec![&a.ty];
        all.extend(items.iter().map(|n| &n.ty));
        Some(merged_collation(&all, op, 0, env)?)
    } else {
        None
    };
    let mysql_json = !env.maria();
    Ok(items
        .iter()
        .map(|n| {
            if mysql_json && (a.ty.json || n.ty.json) {
                return Comparator {
                    rounds,
                    bits,
                    ..Comparator::of(CmpKind::Json)
                };
            }
            let s = pair(
                effective_side(&a.ty, all_str),
                effective_side(&n.ty, all_str),
                env.maria(),
            );
            Comparator {
                kind: kind_of(s, coll),
                rounds,
                rounds_strings: false,
                bits,
                strict_dates: false,
                real_precision: None,
            }
        })
        .collect())
}

/// The left side of a list comparison, converted once per comparison type.
pub(crate) struct LeftKeys<'r> {
    seen: Vec<(CmpKind, Option<Key>)>,
    row: &'r [Value],
}

impl<'r> LeftKeys<'r> {
    pub(crate) fn new(row: &'r [Value]) -> Self {
        Self {
            seen: Vec::new(),
            row,
        }
    }

    pub(crate) fn get(
        &mut self,
        ctx: &mut Ctx<'_>,
        cmp: &Comparator,
        x: &Value,
        node: &Node,
    ) -> Result<Option<Key>> {
        if let Some((_, k)) = self.seen.iter().find(|(kind, _)| *kind == cmp.kind) {
            return Ok(k.clone());
        }
        let k = if node.alt.is_some() && !matches!(cmp.kind, CmpKind::Str(_)) {
            cmp.node_key(ctx, node, self.row)?
        } else {
            cmp.key(ctx, x, node.info(), &node.text, &node.ty)?
        };
        self.seen.push((cmp.kind, k.clone()));
        Ok(k)
    }
}

/// `[NOT] IN (list)`.
///
/// A list of one item compares as `=` does. For a longer list each item
/// is compared in the type its pair with the left side chooses. When all
/// items are constants that compare in one type, both servers convert
/// them once, while preparing the statement (so their conditions come
/// once and first, and an error in one fails the statement even when no
/// row reaches it); any other list is walked for each row.
pub(crate) fn in_list(a: Node, mut items: Vec<Node>, not: bool, env: Env<'_>) -> Result<Node> {
    let mysql = !env.maria();
    // String operands of other character sets must convert to the merged
    // one (1271 and its siblings otherwise).
    {
        let strs: Vec<&Node> = std::iter::once(&a)
            .chain(items.iter())
            .filter(|n| n.ty.kind == Kind::Str)
            .collect();
        let tys: Vec<&ExprType> = strs.iter().map(|n| &n.ty).collect();
        if strs.len() > 1 {
            if let Ok(d) = crate::types::aggregate_types(&tys, env.conn(), "in", true) {
                if strs.iter().any(|n| !super::convertible(n, d.coll, env)) {
                    let mut all: Vec<crate::types::Dtc> = vec![Dtc::of(&a.ty, env.conn())];
                    all.extend(items.iter().map(|n| Dtc::of(&n.ty, env.conn())));
                    return Err(crate::types::illegal_mix(&all, "in"));
                }
            }
        }
    }
    if mysql && int_column(&a) {
        let fold = if items.len() == 1 {
            Fold::StringsAndDoubles
        } else {
            Fold::Strings
        };
        for item in &mut items {
            if let Some(lit) = int_literal(&a, item, fold, env) {
                *item = lit;
            }
        }
    }
    let mut cmps = if let [only] = &items[..] {
        vec![comparator2(&a, only, " IN ", env)?]
    } else {
        let refs: Vec<&Node> = items.iter().collect();
        // (A list compares the values as they are: MariaDB's rounding to
        // the declared scale is the single comparison's.)
        pairwise(&a, &refs, " IN ", false, false, env)?
    };
    // A list of two or more constants is converted once when all its
    // items compare in one type. MariaDB also brings integer and decimal
    // comparisons together as decimals.
    let exact = |c: &Comparator| matches!(c.kind, CmpKind::Int | CmpKind::Dec);
    // (Not when the list starts with a string: MariaDB then walks it,
    // stopping at the first match.)
    if env.maria()
        && cmps.len() > 1
        && cmps.iter().all(exact)
        && !cmps.iter().all(|c| c.kind == CmpKind::Int)
        && items.first().is_none_or(|n| n.ty.kind != Kind::Str)
    {
        for c in &mut cmps {
            c.kind = CmpKind::Dec;
        }
    }
    let uniform = cmps.windows(2).all(|w| w[0].kind == w[1].kind);
    let sorted = cmps.len() > 1 && uniform && items.iter().all(|n| n.konst);
    let null_left = a.ty.kind == Kind::Null && a.lit.is_some();
    // MySQL compares integers by their bits when it walks a list (one
    // item is an equality).
    if mysql && !sorted && cmps.len() > 1 {
        for c in &mut cmps {
            c.bits = true;
        }
    }
    let nullable = a.ty.nullable || items.iter().any(|n| n.ty.nullable);
    let ty = ExprType::boolean().with_null(nullable);
    let text = format!("({} in (...))", a.text);
    let a = super::cached(a, env);
    // MySQL evaluates a list of constants while it prepares `NULL IN
    // (...)`: an error in an item fails the statement.
    if mysql && null_left && !sorted && items.iter().all(|n| n.konst) {
        for item in &items {
            env.at_prepare(|ctx| item.eval(ctx, &[]))?;
        }
    }
    if sorted {
        let mut keys = Vec::with_capacity(items.len());
        for (item, cmp) in items.iter().zip(&cmps) {
            keys.push(env.at_prepare(|ctx| cmp.node_key(ctx, item, &[]))?);
        }
        return Ok(Node::new(ty, text, move |ctx, row| {
            let x = a.eval(ctx, row)?;
            if x.is_null() {
                return Ok(Value::Null);
            }
            let mut left = LeftKeys::new(row);
            let mut unknown = false;
            for (ky, cmp) in keys.iter().zip(&cmps) {
                let kx = left.get(ctx, cmp, &x, &a)?;
                match (&kx, ky) {
                    (Some(kx), Some(ky)) => {
                        if cmp.order(kx, ky) == Some(Ordering::Equal) {
                            return Ok(Value::Int(i64::from(!not)));
                        }
                    }
                    _ => unknown = true,
                }
            }
            Ok(if unknown {
                Value::Null
            } else {
                Value::Int(i64::from(not))
            })
        }));
    }
    let items: Vec<(Node, Comparator)> = items
        .into_iter()
        .map(|n| super::cached(n, env))
        .zip(cmps)
        .collect();
    Ok(Node::new(ty, text, move |ctx, row| {
        let x = a.eval(ctx, row)?;
        if x.is_null() {
            if mysql && !null_left && items.len() > 1 {
                // MySQL still walks the list.
                for (item, cmp) in &items {
                    cmp.node_key(ctx, item, row)?;
                }
            }
            return Ok(Value::Null);
        }
        let mut left = LeftKeys::new(row);
        let mut unknown = false;
        for (item, cmp) in &items {
            let kx = left.get(ctx, cmp, &x, &a)?;
            let ky = cmp.node_key(ctx, item, row)?;
            match (&kx, &ky) {
                (Some(kx), Some(ky)) => {
                    if cmp.order(kx, ky) == Some(Ordering::Equal) {
                        return Ok(Value::Int(i64::from(!not)));
                    }
                }
                _ => unknown = true,
            }
        }
        Ok(if unknown {
            Value::Null
        } else {
            Value::Int(i64::from(not))
        })
    }))
}

/// `[NOT] BETWEEN low AND high`.
pub(crate) fn between(
    a: Node,
    mut lo: Node,
    mut hi: Node,
    not: bool,
    env: Env<'_>,
) -> Result<Node> {
    // Constant bounds of an integer column become integers where they are
    // whole numbers: on MariaDB each one by itself, on MySQL both or
    // neither.
    if int_column(&a) {
        let fold = |n: &Node| int_literal(&a, n, Fold::StringsAndDoubles, env);
        let whole = |n: &Node| n.ty.kind == Kind::Int;
        let (flo, fhi) = (fold(&lo), fold(&hi));
        if env.maria() || ((flo.is_some() || whole(&lo)) && (fhi.is_some() || whole(&hi))) {
            lo = flo.unwrap_or(lo);
            hi = fhi.unwrap_or(hi);
        }
    }
    let mut cmp = comparator_n(&[&a.ty, &lo.ty, &hi.ty], "between", true, env)?;
    // MySQL compares a decimal that is not a constant with constant
    // strings as decimals here too.
    if !env.maria()
        && cmp.kind == CmpKind::Real
        && a.ty.kind == Kind::Decimal
        && !a.konst
        && [&lo, &hi]
            .iter()
            .all(|n| n.konst && matches!(side(&n.ty), Side::Str | Side::Int | Side::Dec))
    {
        cmp.kind = CmpKind::Dec;
    }
    let nullable = a.ty.nullable || lo.ty.nullable || hi.ty.nullable;
    let ty = ExprType::boolean().with_null(nullable);
    let text = format!("({} between {} and {})", a.text, lo.text, hi.text);
    let (a, lo, hi) = (
        super::cached(a, env),
        super::cached(lo, env),
        super::cached(hi, env),
    );
    Ok(Node::new(ty, text, move |ctx, row| {
        let Some(kx) = cmp.node_key(ctx, &a, row)? else {
            return Ok(Value::Null);
        };
        let bound = |n: &Node, ctx: &mut Ctx<'_>| -> Result<Option<Ordering>> {
            Ok(match cmp.node_key(ctx, n, row)? {
                Some(k) => cmp.order(&kx, &k),
                None => None,
            })
        };
        let ge = bound(&lo, ctx)?.map(|o| o != Ordering::Less);
        let le = bound(&hi, ctx)?.map(|o| o != Ordering::Greater);
        let inside = match (ge, le) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        };
        Ok(match inside {
            None => Value::Null,
            Some(b) => Value::Int(i64::from(b != not)),
        })
    }))
}
