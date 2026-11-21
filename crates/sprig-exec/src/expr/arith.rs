//! Arithmetic: `+ - * / DIV %`, unary minus and the bit operators, with
//! the servers' result types, overflow errors and division-by-zero rule.

use sprig_engine::row::Value;

use super::{BinOp, Env, Node};
use crate::conv::{self, check_double, range_error};
use crate::error::Result;
use crate::num::Round;
use crate::session::{Ctx, SqlMode};
use crate::types::{ExprType, Kind, NOT_FIXED};

/// How an operand takes part in arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    Int,
    Dec,
    Real,
}

/// An operand's numeric shape.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NumTy {
    pub(crate) class: Class,
    pub(crate) unsigned: bool,
    /// All digits, the scale included.
    pub(crate) prec: u32,
    pub(crate) scale: u32,
    /// The printed decimals when the operand is used as a double.
    pub(crate) dec: u8,
}

impl NumTy {
    pub(crate) fn int_part(&self) -> u32 {
        self.prec.saturating_sub(self.scale)
    }
}

pub(crate) fn num_ty(ty: &ExprType) -> NumTy {
    let exact = |prec: u32, scale: u32, unsigned: bool| NumTy {
        class: if scale == 0 { Class::Int } else { Class::Dec },
        unsigned,
        prec,
        scale,
        dec: scale as u8,
    };
    let real = |dec: u8| NumTy {
        class: Class::Real,
        unsigned: false,
        prec: ty.len.clamp(1, 65),
        scale: 0,
        dec: dec.min(NOT_FIXED),
    };
    match ty.kind {
        Kind::Int | Kind::Year | Kind::Bit => NumTy {
            class: Class::Int,
            ..exact(u32::from(ty.prec), 0, ty.unsigned)
        },
        Kind::Decimal => NumTy {
            class: Class::Dec,
            ..exact(u32::from(ty.prec), u32::from(ty.dec), ty.unsigned)
        },
        // A double keeps an unsigned flag (MOD's follows its dividend).
        Kind::Double | Kind::Float => NumTy {
            unsigned: ty.unsigned,
            ..real(ty.dec)
        },
        Kind::Str if ty.hex => NumTy {
            class: Class::Int,
            ..exact(ty.len.saturating_mul(3).clamp(1, 20), 0, true)
        },
        Kind::Str => real(ty.dec),
        // The NULL literal counts as a string without decimals.
        Kind::Null => real(0),
        Kind::Date => exact(8, 0, false),
        Kind::DateTime | Kind::Timestamp => exact(14 + u32::from(ty.dec), u32::from(ty.dec), false),
        Kind::Time => exact(7 + u32::from(ty.dec), u32::from(ty.dec), false),
    }
}

/// Whether a zero quotient or remainder is a plain zero, without fraction
/// digits (a later division counts them): on MySQL always, on MariaDB
/// when the dividend was zero.
fn plain_zero(ctx: &Ctx<'_>, r: &crate::num::Dec, dividend: &crate::num::Dec) -> bool {
    r.is_zero() && (!ctx.maria() || dividend.is_zero())
}

/// The first nine-digit group of a decimal's digits that isn't zero, and
/// how many groups its integer part has (groups are counted from the
/// point).
fn lead_group(d: &crate::num::Dec) -> (usize, u64) {
    let (int, frac) = d.split_digits();
    let int: Vec<u8> = int.into_iter().skip_while(|&c| c == 0).collect();
    let words = int.len().div_ceil(9);
    let group = |g: &[u8]| g.iter().fold(0u64, |v, &c| v * 10 + u64::from(c));
    if !int.is_empty() {
        let first = int.len() - 9 * (words - 1);
        return (words, group(&int[..first]));
    }
    for chunk in frac.chunks(9) {
        let mut g: Vec<u8> = chunk.to_vec();
        g.resize(9, 0);
        let v = group(&g);
        if v != 0 {
            return (0, v);
        }
    }
    (0, 0)
}

/// Whether the servers' long division of `x` by `y` makes room for an
/// integer group in the quotient (so a quotient below one in size keeps
/// its sign as "-0"): the integer groups of the dividend less the
/// divisor's, plus one when the dividend's first group is not the
/// smaller.
fn quotient_has_int_word(x: &crate::num::Dec, y: &crate::num::Dec) -> bool {
    let ((wa, la), (wb, lb)) = (lead_group(x), lead_group(y));
    wa as i64 - wb as i64 + i64::from(la >= lb) > 0
}

fn finish_int(r: Option<i128>, unsigned: bool, text: &str) -> Result<Value> {
    if unsigned {
        match r.and_then(|v| u64::try_from(v).ok()) {
            Some(v) => Ok(Value::UInt(v)),
            None => Err(range_error("BIGINT UNSIGNED", text)),
        }
    } else {
        match r.and_then(|v| i64::try_from(v).ok()) {
            Some(v) => Ok(Value::Int(v)),
            None => Err(range_error("BIGINT", text)),
        }
    }
}

/// A decimal result as a value. The value type holds 65 digits in all;
/// the servers compute with up to 81. A result with more than 65 digits
/// before the point is refused (error 1690) though the servers would
/// show it, and one that only fits by giving up digits after the point
/// loses them. Both cases are counted in the statement's context.
fn wide_decimal(ctx: &mut Ctx<'_>, r: &crate::num::Dec, text: &str) -> Result<Value> {
    if r.int_digits() > 65 {
        ctx.wide_decimals += 1;
        return Err(range_error("DECIMAL", text));
    }
    if r.int_digits() + r.scale as usize > 65 && !r.is_zero() {
        ctx.wide_decimals += 1;
    }
    Ok(conv::dec_value(r))
}

fn div_by_zero(ctx: &mut Ctx<'_>) -> Value {
    if ctx
        .session
        .sql_mode
        .has(SqlMode::ERROR_FOR_DIVISION_BY_ZERO)
    {
        ctx.warn(1365, "Division by 0");
    }
    Value::Null
}

fn op_text(op: BinOp, a: &Node, b: &Node, env: Env<'_>) -> String {
    if env.maria() {
        format!("{} {} {}", a.text, op.sql(), b.text)
    } else {
        format!("({} {} {})", a.text, op.sql(), b.text)
    }
}

/// The type of a double result with `dec` decimals: the length follows
/// the servers' formula for fixed decimals.
pub(crate) fn double_ty(dec: u32, nullable: bool) -> ExprType {
    if dec >= u32::from(NOT_FIXED) {
        return ExprType::double().with_null(nullable);
    }
    ExprType {
        dec: dec as u8,
        len: 17 + dec,
        ..ExprType::double()
    }
    .with_null(nullable)
}

fn real_node(
    ty: ExprType,
    text: String,
    f: impl Fn(&mut Ctx<'_>, f64, f64) -> Result<Value> + Send + Sync + 'static,
    a: Node,
    b: Node,
) -> Node {
    let mut n = Node::new(ty, text, move |ctx, row| {
        // Both operands are evaluated even when the first is NULL.
        let (x, y) = (a.f64(ctx, row)?, b.f64(ctx, row)?);
        let (Some(x), Some(y)) = (x, y) else {
            return Ok(Value::Null);
        };
        f(ctx, x, y)
    });
    n.strict_real = true;
    n
}

/// `+ - * / DIV %`.
pub(crate) fn binary(op: BinOp, a: Node, b: Node, env: Env<'_>) -> Node {
    let (mut ta, mut tb) = (num_ty(&a.ty), num_ty(&b.ty));
    // MariaDB computes a date or time without a fraction together with a
    // decimal without one as integers (and so within 64 bits).
    let maria_temporal = env.maria() && (a.ty.kind.is_temporal() || b.ty.kind.is_temporal());
    if maria_temporal {
        for t in [&mut ta, &mut tb] {
            if t.class == Class::Dec && t.scale == 0 {
                t.class = Class::Int;
                t.prec = t.prec.min(20);
            }
        }
    }
    let text = op_text(op, &a, &b, env);
    let nullable = a.ty.nullable || b.ty.nullable;
    let real = ta.class == Class::Real || tb.class == Class::Real;
    let both_int = ta.class == Class::Int && tb.class == Class::Int;
    let incr = u32::from(env.s.div_precision_increment);
    let fixed = ta.dec < NOT_FIXED && tb.dec < NOT_FIXED;
    let (da, db) = (u32::from(ta.dec), u32::from(tb.dec));
    let max_scale = env.max_scale();
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul if real => {
            // A double result shows the larger of the operands' decimals,
            // for a product too.
            let dec = if fixed {
                da.max(db)
            } else {
                u32::from(NOT_FIXED)
            };
            let msg = text.clone();
            real_node(
                double_ty(dec, nullable),
                text,
                move |_, x, y| {
                    check_double(
                        match op {
                            BinOp::Add => x + y,
                            BinOp::Sub => x - y,
                            _ => x * y,
                        },
                        &msg,
                    )
                },
                a,
                b,
            )
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul if both_int => {
            let unsigned = if maria_temporal {
                // MariaDB computes with a date as a decimal: signed unless
                // both operands are unsigned, and a difference is signed.
                ta.unsigned && tb.unsigned && op != BinOp::Sub
            } else if op == BinOp::Sub && env.s.sql_mode.has(SqlMode::NO_UNSIGNED_SUBTRACTION) {
                false
            } else {
                ta.unsigned || tb.unsigned
            };
            let prec = if op == BinOp::Mul {
                ta.prec + tb.prec
            } else {
                ta.prec.max(tb.prec) + 1
            };
            let ty = ExprType::int_prec(prec, unsigned).with_null(nullable);
            let msg = text.clone();
            let maria = env.maria();
            Node::new(ty, text, move |ctx, row| {
                let (x, y) = (a.int(ctx, row)?, b.int(ctx, row)?);
                let (Some(x), Some(y)) = (x, y) else {
                    return Ok(Value::Null);
                };
                let r = match op {
                    BinOp::Add => x.wide().checked_add(y.wide()),
                    BinOp::Sub => x.wide().checked_sub(y.wide()),
                    _ => x.wide().checked_mul(y.wide()),
                };
                if !unsigned {
                    // MariaDB lets zero minus the smallest integer wrap
                    // back to it; MySQL refuses a product that is exactly
                    // the smallest integer unless a factor already was.
                    let min = i128::from(i64::MIN);
                    if op == BinOp::Sub && maria && x.wide() == 0 && y.wide() == min {
                        return Ok(Value::Int(i64::MIN));
                    }
                    if op == BinOp::Mul
                        && !maria
                        && r == Some(min)
                        && x.wide() != min
                        && y.wide() != min
                    {
                        return Err(range_error("BIGINT", &msg));
                    }
                }
                finish_int(r, unsigned, &msg)
            })
        }
        BinOp::Add | BinOp::Sub | BinOp::Mul => {
            let (scale, prec) = if op == BinOp::Mul {
                ((ta.scale + tb.scale).min(max_scale), ta.prec + tb.prec)
            } else {
                let scale = ta.scale.max(tb.scale);
                (scale, ta.int_part().max(tb.int_part()) + 1 + scale)
            };
            // MySQL keeps a difference of two unsigned decimals unsigned
            // (and refuses a negative one).
            let unsigned = ta.unsigned && tb.unsigned && (op != BinOp::Sub || !env.maria());
            let ty = env.decimal(prec, scale, unsigned).with_null(nullable);
            let keep_scale = if env.maria() { max_scale } else { 38 };
            let msg = text.clone();
            Node::new(ty, text, move |ctx, row| {
                // A NULL first operand ends the evaluation.
                let Some(x) = a.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                let Some(y) = b.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                let mut r = match op {
                    BinOp::Add => x.add(&y),
                    BinOp::Sub => x.sub(&y),
                    _ => x.mul(&y),
                };
                if op == BinOp::Mul && !r.is_zero() {
                    // The servers multiply in nine-digit groups, nine of
                    // them in all, and drop the factors' last groups when
                    // the product would need more. The product here is
                    // exact; such a statement is counted.
                    let groups = |d: &crate::num::Dec| {
                        d.int_digits().div_ceil(9) + (d.scale as usize).div_ceil(9)
                    };
                    if groups(&x) + groups(&y) > 9 {
                        ctx.wide_decimals += 1;
                    }
                }
                // A zero product is a plain zero, without fraction
                // digits (a later division counts them): always on
                // MySQL, on MariaDB when the factors' signs differ.
                if op == BinOp::Mul && r.is_zero() && (!ctx.maria() || x.neg != y.neg) {
                    r = crate::num::Dec::zero();
                }
                // MySQL keeps a product's digits beyond its 30-digit
                // scale for whatever reads it next (only the text is
                // rounded); MariaDB's scale limit is its working limit.
                if r.scale > keep_scale {
                    r = r.round(keep_scale as i32, Round::HalfUp);
                }
                if unsigned && r.neg && !r.is_zero() && !ctx.maria() {
                    return Err(range_error("DECIMAL UNSIGNED", &msg));
                }
                wide_decimal(ctx, &r, &msg)
            })
        }
        BinOp::Div if real => {
            let dec = if fixed {
                da.max(db) + incr
            } else {
                u32::from(NOT_FIXED)
            };
            let msg = text.clone();
            real_node(
                double_ty(dec, true),
                text,
                move |ctx, x, y| {
                    if y == 0.0 {
                        return Ok(div_by_zero(ctx));
                    }
                    check_double(x / y, &msg)
                },
                a,
                b,
            )
        }
        BinOp::Div => {
            let scale = (ta.scale + incr).min(max_scale);
            let prec = ta.prec + tb.scale + incr;
            let ty = env
                .decimal(prec, scale, ta.unsigned && tb.unsigned)
                .nullable();
            let msg = text.clone();
            Node::new(ty, text, move |ctx, row| {
                let Some(x) = a.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                let Some(y) = b.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                // The quotient keeps its whole 9-digit groups: only a
                // reader of the text sees it rounded to the type's scale.
                let Some(mut q) = x.div(&y, incr) else {
                    return Ok(div_by_zero(ctx));
                };
                // MariaDB rounds a quotient beyond its 38-digit scale.
                if ctx.maria() && q.scale > 38 {
                    q = q.round(38, Round::HalfUp);
                }
                if q.is_zero() && !x.is_zero() && x.neg != y.neg {
                    ctx.negative_zeros += 1;
                }
                if plain_zero(ctx, &q, &x) {
                    q = crate::num::Dec::zero();
                }
                wide_decimal(ctx, &q, &msg)
            })
        }
        BinOp::IntDiv => {
            let unsigned = ta.unsigned || tb.unsigned;
            // The length follows the dividend's integer digits; MySQL
            // adds the divisor's decimals (a string's length counts as
            // either), and a double makes it full length.
            let maria = env.maria();
            let part = |t: &NumTy, ty: &ExprType| match ty.kind {
                Kind::Double | Kind::Float if maria => ty.len,
                Kind::Double | Kind::Float => 21,
                Kind::Str if !ty.hex => ty.len,
                Kind::Null => 0,
                _ => t.int_part(),
            };
            let len = if maria {
                part(&ta, &a.ty).min(21)
            } else {
                let divisor = match b.ty.kind {
                    Kind::Str if !b.ty.hex => b.ty.len,
                    Kind::Double | Kind::Float if tb.dec >= NOT_FIXED => 31,
                    Kind::Double | Kind::Float => u32::from(tb.dec),
                    _ => tb.scale,
                };
                (part(&ta, &a.ty) + divisor).min(21) + u32::from(!unsigned)
            };
            let ty = ExprType {
                prec: ta.int_part().clamp(1, 20) as u8,
                ..ExprType::int(len, unsigned)
            }
            .nullable();
            let msg = text.clone();
            if both_int {
                Node::new(ty, text, move |ctx, row| {
                    let (x, y) = (a.int(ctx, row)?, b.int(ctx, row)?);
                    let (Some(x), Some(y)) = (x, y) else {
                        return Ok(Value::Null);
                    };
                    if y.v == 0 {
                        return Ok(div_by_zero(ctx));
                    }
                    let q = x.wide().checked_div(y.wide());
                    // MySQL refuses a signed quotient that is exactly the
                    // smallest integer.
                    if !unsigned && !ctx.maria() && q == Some(i128::from(i64::MIN)) {
                        return Err(range_error("BIGINT", &msg));
                    }
                    finish_int(q, unsigned, &msg)
                })
            } else {
                let signed_wrap = env.maria() && unsigned;
                Node::new(ty, text, move |ctx, row| {
                    let Some(x) = a.dec(ctx, row)? else {
                        return Ok(Value::Null);
                    };
                    let Some(y) = b.dec(ctx, row)? else {
                        return Ok(Value::Null);
                    };
                    let Some(q) = x.div(&y, 0) else {
                        return Ok(div_by_zero(ctx));
                    };
                    let q = q.round(0, Round::Truncate);
                    let wide = q.mag.to_u128().and_then(|m| i128::try_from(m).ok());
                    let wide = wide.map(|m| if q.neg { -m } else { m });
                    // MariaDB refuses a negative quotient of an unsigned
                    // DIV even when it truncates to zero ('-0').
                    let negative_zero = unsigned
                        && ctx.maria()
                        && q.is_zero()
                        && !x.is_zero()
                        && x.neg != y.neg
                        && x.scale == 0
                        && y.scale == 0
                        && quotient_has_int_word(&x, &y);
                    let fits = !negative_zero
                        && wide.is_some_and(|w| {
                            if unsigned {
                                u64::try_from(w).is_ok()
                            } else {
                                i64::try_from(w).is_ok()
                            }
                        });
                    if !fits {
                        let shown = if negative_zero {
                            "-0".to_owned()
                        } else {
                            q.to_string()
                        };
                        let shown = &shown[..shown.len().min(128)];
                        let to = if unsigned { "UNSIGNED INT" } else { "INT" };
                        conv::overflow_condition(ctx, shown, "DECIMAL", to);
                        if negative_zero {
                            return Err(range_error("BIGINT UNSIGNED", &msg));
                        }
                    }
                    // MariaDB answers an unsigned quotient beyond the signed
                    // range of a dividend within it as the signed number
                    // of its bits.
                    if signed_wrap && fits && x.cmp(&crate::num::Dec::from_i64(i64::MAX)).is_le() {
                        if let Some(w) = wide.filter(|w| *w > i128::from(i64::MAX)) {
                            return Ok(Value::Int(w as u64 as i64));
                        }
                    }
                    finish_int(wide, unsigned, &msg)
                })
            }
        }
        BinOp::Mod if real => {
            let dec = if fixed {
                da.max(db)
            } else {
                u32::from(NOT_FIXED)
            };
            real_node(
                ExprType {
                    unsigned: ta.unsigned,
                    ..double_ty(dec, true)
                },
                text,
                move |ctx, x, y| {
                    if y == 0.0 {
                        return Ok(div_by_zero(ctx));
                    }
                    Ok(Value::Double(x % y))
                },
                a,
                b,
            )
        }
        BinOp::Mod if both_int => {
            let unsigned = ta.unsigned;
            let ty = ExprType::int_prec(ta.prec.max(tb.prec), unsigned).nullable();
            let msg = text.clone();
            Node::new(ty, text, move |ctx, row| {
                let (x, y) = (a.int(ctx, row)?, b.int(ctx, row)?);
                let (Some(x), Some(y)) = (x, y) else {
                    return Ok(Value::Null);
                };
                if y.v == 0 {
                    return Ok(div_by_zero(ctx));
                }
                finish_int(x.wide().checked_rem(y.wide()), unsigned, &msg)
            })
        }
        _ => {
            let scale = ta.scale.max(tb.scale);
            let ty = env
                .decimal(ta.prec.max(tb.prec), scale, ta.unsigned)
                .nullable();
            Node::new(ty, text, move |ctx, row| {
                let Some(x) = a.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                let Some(y) = b.dec(ctx, row)? else {
                    return Ok(Value::Null);
                };
                match x.rem(&y) {
                    None => Ok(div_by_zero(ctx)),
                    Some(mut r) => {
                        if r.is_zero() && x.neg && !x.is_zero() {
                            ctx.negative_zeros += 1;
                        }
                        if plain_zero(ctx, &r, &x) {
                            r = crate::num::Dec::zero();
                        }
                        Ok(conv::dec_value(&r))
                    }
                }
            })
        }
    }
}

/// Unary minus of a non-literal operand.
///
/// Both servers look at a constant integer operand's value while they
/// prepare the statement: if it is negative, or beyond the signed range,
/// the negation is done as a decimal (so it cannot overflow). An error
/// in that evaluation fails the statement there.
pub(crate) fn neg(a: Node, env: Env<'_>) -> Result<Node> {
    let ta = num_ty(&a.ty);
    let text = if env.maria() {
        format!("-{}", a.text)
    } else {
        format!("-({})", a.text)
    };
    let nullable = a.ty.nullable;
    Ok(match ta.class {
        Class::Real => {
            let mut ty = double_ty(u32::from(ta.dec), nullable);
            if ta.dec < NOT_FIXED && env.maria() {
                ty.len = a.ty.len + 1;
            }
            let mut n = Node::new(ty, text, move |ctx, row| {
                Ok(match a.f64(ctx, row)? {
                    None => Value::Null,
                    Some(x) => Value::Double(-x),
                })
            });
            n.strict_real = true;
            n
        }
        Class::Int if a.konst && const_needs_decimal(&a, env)? => {
            let prec = if env.maria() {
                a.ty.len
            } else {
                a.ty.len.saturating_sub(1)
            };
            let ty = env.decimal(prec.max(1), 0, false).with_null(nullable);
            Node::new(ty, text, move |ctx, row| {
                Ok(match a.int(ctx, row)? {
                    None => Value::Null,
                    Some(x) => conv::dec_value(&x.dec().negated()),
                })
            })
        }
        Class::Int => {
            let ty = ExprType::int_prec(ta.prec, false).with_null(nullable);
            let msg = text.clone();
            Node::new(ty, text, move |ctx, row| match a.int(ctx, row)? {
                None => Ok(Value::Null),
                Some(x) => finish_int(x.wide().checked_neg(), false, &msg),
            })
        }
        Class::Dec => {
            let ty = env.decimal(ta.prec, ta.scale, false).with_null(nullable);
            Node::new(ty, text, move |ctx, row| {
                Ok(match a.dec(ctx, row)? {
                    None => Value::Null,
                    Some(x) => conv::dec_value(&x.negated()),
                })
            })
        }
    })
}

/// Whether a constant integer operand's value is negative or above the
/// signed range: its negation is then a decimal.
fn const_needs_decimal(a: &Node, env: Env<'_>) -> Result<bool> {
    let v = env.at_prepare(|ctx| a.int(ctx, &[]))?;
    Ok(match v {
        Some(i) if i.unsigned => i.v as u64 >= 1 << 63,
        Some(i) => i.v < 0,
        None => false,
    })
}

fn bit_ty(nullable: bool) -> ExprType {
    ExprType {
        prec: 20,
        ..ExprType::int(21, true)
    }
    .with_null(nullable)
}

/// A bit operator's operand as 64 bits. MySQL reads it as an integer
/// (strings by their integer prefix, doubles to the nearest even);
/// MariaDB takes everything but integers through a decimal.
fn bits(ctx: &mut Ctx<'_>, n: &Node, row: &[Value]) -> Result<Option<u64>> {
    if !ctx.maria() {
        // An unsigned decimal result is read over the unsigned range.
        if n.ty.kind == Kind::Decimal && n.ty.unsigned && n.lit.is_none() && n.alt.is_none() {
            let Some(d) = n.dec(ctx, row)? else {
                return Ok(None);
            };
            if let Ok(u) = d.to_u64() {
                return Ok(Some(u));
            }
            let v = conv::dec_value(&d);
            return Ok(conv::conv_int(ctx, &v, n.info(), &n.text)?.map(|i| i.v as u64));
        }
        return Ok(n.int(ctx, row)?.map(|i| i.v as u64));
    }
    if n.alt.as_ref().is_some_and(|a| a.dec.is_some()) {
        let Some(d) = n.dec(ctx, row)? else {
            return Ok(None);
        };
        let v = conv::dec_value(&d);
        return Ok(conv::maria_bit_int(ctx, &v, n.info()));
    }
    let v = n.eval(ctx, row)?;
    Ok(conv::maria_bit_int(ctx, &v, n.info()))
}

fn is_binary_string(ty: &ExprType) -> bool {
    ty.is_binary_string() && !ty.hex
}

fn bytes_of(v: Value) -> Option<bytes::Bytes> {
    match v {
        Value::Bytes(b) | Value::Str(b) => Some(b),
        _ => None,
    }
}

/// Shifts a byte string, read as one big-endian number, by `count` bits.
fn shift_bytes(b: &[u8], count: u64, left: bool) -> Vec<u8> {
    let n = b.len();
    let mut out = vec![0u8; n];
    if count >= 8 * n as u64 {
        return out;
    }
    let (bytes, bits) = ((count / 8) as usize, (count % 8) as u32);
    for i in 0..n {
        // Byte `i` of the result takes its bits from two source bytes.
        let (hi, lo) = if left {
            (i.checked_add(bytes), i.checked_add(bytes + 1))
        } else {
            (i.checked_sub(bytes), i.checked_sub(bytes + 1))
        };
        let get = |j: Option<usize>| j.and_then(|j| b.get(j)).copied().unwrap_or(0);
        out[i] = if bits == 0 {
            get(hi)
        } else if left {
            (get(hi) << bits) | (get(lo) >> (8 - bits))
        } else {
            (get(hi) >> bits) | (get(lo) << (8 - bits))
        };
    }
    out
}

/// `& | ^ << >>`: on unsigned 64-bit integers, or, on MySQL when the
/// operands are binary strings, byte by byte.
pub(crate) fn bit_op(op: BinOp, a: Node, b: Node, env: Env<'_>) -> Node {
    let nullable = a.ty.nullable || b.ty.nullable;
    let text = format!("({} {} {})", a.text, op.sql(), b.text);
    let shift = matches!(op, BinOp::Shl | BinOp::Shr);
    let maria = env.maria();
    if !maria && is_binary_string(&a.ty) && (shift || is_binary_string(&b.ty)) {
        let len = if shift {
            a.ty.len
        } else {
            a.ty.len.max(b.ty.len)
        };
        let ty = ExprType::string(
            len,
            sprig_engine::keys::collation::Collation::Binary,
            crate::types::Derivation::Coercible,
        )
        .with_null(nullable);
        return Node::new(ty, text, move |ctx, row| {
            let x = a.eval(ctx, row)?;
            if x.is_null() {
                return Ok(Value::Null);
            }
            if shift {
                let count = b.int(ctx, row)?;
                let (Some(x), Some(count)) = (bytes_of(x), count) else {
                    return Ok(Value::Null);
                };
                let out = shift_bytes(&x, count.v as u64, op == BinOp::Shl);
                return Ok(Value::Bytes(out.into()));
            }
            let y = b.eval(ctx, row)?;
            let (Some(x), Some(y)) = (bytes_of(x), bytes_of(y)) else {
                return Ok(Value::Null);
            };
            if x.len() != y.len() {
                return Err(crate::error::SqlError::new(
                    3513,
                    "HY000",
                    "Binary operands of bitwise operators must be of equal length",
                ));
            }
            let out: Vec<u8> = x
                .iter()
                .zip(y.iter())
                .map(|(p, q)| match op {
                    BinOp::BitAnd => p & q,
                    BinOp::BitOr => p | q,
                    _ => p ^ q,
                })
                .collect();
            Ok(Value::Bytes(out.into()))
        });
    }
    // A NULL left operand ends the evaluation, except for MariaDB's `^`,
    // `<<` and `>>`, which read both operands first.
    let both = maria && matches!(op, BinOp::BitXor | BinOp::Shl | BinOp::Shr);
    Node::new(bit_ty(nullable), text, move |ctx, row| {
        let x = bits(ctx, &a, row)?;
        if x.is_none() && !both {
            return Ok(Value::Null);
        }
        // MariaDB reads a shift count as an ordinary integer, 32 bits of
        // it.
        let y = if shift && maria && b.ty.kind == Kind::Decimal && b.ty.unsigned {
            // (An unsigned decimal is read over the unsigned range.)
            b.dec(ctx, row)?
                .map(|d| conv::dec_to_int(ctx, &d, true).v as u64)
        } else if shift && maria {
            b.int(ctx, row)?.map(|i| i.v as u64)
        } else {
            bits(ctx, &b, row)?
        };
        let (Some(x), Some(y)) = (x, y) else {
            return Ok(Value::Null);
        };
        let count = if maria { u64::from(y as u32) } else { y };
        Ok(Value::UInt(match op {
            BinOp::BitAnd => x & y,
            BinOp::BitOr => x | y,
            BinOp::BitXor => x ^ y,
            BinOp::Shl if count < 64 => x << count,
            BinOp::Shr if count < 64 => x >> count,
            _ => 0,
        }))
    })
}

/// `~`.
pub(crate) fn bit_not(a: Node, env: Env<'_>) -> Node {
    let text = format!("~{}", a.text);
    if !env.maria() && is_binary_string(&a.ty) {
        let ty = a.ty;
        return Node::new(ty, text, move |ctx, row| {
            Ok(match bytes_of(a.eval(ctx, row)?) {
                None => Value::Null,
                Some(b) => Value::Bytes(b.iter().map(|x| !x).collect::<Vec<u8>>().into()),
            })
        });
    }
    Node::new(bit_ty(a.ty.nullable), text, move |ctx, row| {
        Ok(match bits(ctx, &a, row)? {
            None => Value::Null,
            Some(x) => Value::UInt(!x),
        })
    })
}
