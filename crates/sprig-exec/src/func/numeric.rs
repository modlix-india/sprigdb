//! Numeric functions.

use sprig_engine::row::Value;

use crate::conv::{self, check_double, range_error};
use crate::error::Result;
use crate::expr::arith::{Class, double_ty, num_ty};
use crate::expr::{Env, Node};
use crate::num::Round;
use crate::session::SqlMode;
use crate::types::{ExprType, NOT_FIXED};

/// `ABS(x)`: the argument's numeric family is kept.
pub(crate) fn abs(a: Node, env: Env<'_>) -> Node {
    let t = num_ty(&a.ty);
    let nullable = a.ty.nullable;
    match t.class {
        Class::Int => {
            let ty = ExprType {
                prec: t.prec.clamp(1, 20) as u8,
                ..ExprType::int(a.ty.len.max(1), t.unsigned)
            }
            .with_null(nullable);
            Node::new(ty, "abs", move |ctx, row| match a.int(ctx, row)? {
                None => Ok(Value::Null),
                Some(i) if i.unsigned || i.v >= 0 => Ok(i.value()),
                Some(i) => match i.v.checked_neg() {
                    Some(v) => Ok(Value::Int(v)),
                    None => Err(range_error("BIGINT", &format!("abs({})", a.text))),
                },
            })
        }
        Class::Dec => {
            let ty = env.decimal(t.prec, t.scale, t.unsigned).with_null(nullable);
            Node::new(ty, "abs", move |ctx, row| {
                Ok(match a.dec(ctx, row)? {
                    None => Value::Null,
                    Some(d) => conv::dec_value(&d.abs()),
                })
            })
        }
        Class::Real => {
            let mut n = Node::new(
                double_ty(u32::from(t.dec), nullable),
                "abs",
                move |ctx, row| {
                    Ok(match a.f64(ctx, row)? {
                        None => Value::Null,
                        Some(x) => Value::Double(x.abs()),
                    })
                },
            );
            n.strict_real = true;
            n
        }
    }
}

/// `SIGN(x)`.
pub(crate) fn sign(a: Node, env: Env<'_>) -> Node {
    // (MySQL reports a BIGINT's full length.)
    let ty = ExprType {
        prec: 1,
        ..ExprType::int(if env.maria() { 2 } else { 21 }, false)
    }
    .with_null(a.ty.nullable);
    let class = num_ty(&a.ty).class;
    Node::new(ty, "sign", move |ctx, row| {
        let s = match class {
            Class::Real => a.f64(ctx, row)?.map(|x| {
                if x > 0.0 {
                    1
                } else if x < 0.0 {
                    -1
                } else {
                    0
                }
            }),
            Class::Int => a.int(ctx, row)?.map(|i| i.wide().signum() as i64),
            Class::Dec => a.dec(ctx, row)?.map(|d| {
                if d.is_zero() {
                    0
                } else if d.neg {
                    -1
                } else {
                    1
                }
            }),
        };
        Ok(s.map_or(Value::Null, Value::Int))
    })
}

/// `CEIL(x)` and `FLOOR(x)`.
pub(crate) fn ceil_floor(a: Node, ceil: bool, env: Env<'_>) -> Node {
    let name = if ceil { "ceiling" } else { "floor" };
    let t = num_ty(&a.ty);
    let nullable = a.ty.nullable;
    match t.class {
        Class::Int => {
            let mut n = a.clone();
            n.lit = None;
            n.ty.column = false;
            n.ty.width = 0;
            if !env.maria() {
                // MySQL reports an integer result at a BIGINT's length.
                n.ty.len = 21;
            }
            n
        }
        Class::Dec => {
            // One more digit than the integer part can appear when the
            // fraction rounds up.
            let digits = t.int_part() + 1;
            let mode = if ceil { Round::Ceil } else { Round::Floor };
            if digits <= 18 {
                let mut ty = ExprType::int_prec(digits, t.unsigned).with_null(nullable);
                if !env.maria() {
                    ty.len = 21;
                }
                Node::new(ty, name, move |ctx, row| {
                    Ok(match a.dec(ctx, row)? {
                        None => Value::Null,
                        Some(d) => {
                            let r = d.round(0, mode);
                            conv::dec_to_int(ctx, &r, false).value()
                        }
                    })
                })
            } else {
                let ty = env.decimal(digits, 0, t.unsigned).with_null(nullable);
                Node::new(ty, name, move |ctx, row| {
                    Ok(match a.dec(ctx, row)? {
                        None => Value::Null,
                        Some(d) => conv::dec_value(&d.round(0, mode)),
                    })
                })
            }
        }
        Class::Real => {
            let dec = if env.maria() { 0 } else { u32::from(NOT_FIXED) };
            let mut n = Node::new(double_ty(dec, nullable), name, move |ctx, row| {
                Ok(match a.f64(ctx, row)? {
                    None => Value::Null,
                    Some(x) => Value::Double(if ceil { x.ceil() } else { x.floor() }),
                })
            });
            n.strict_real = true;
            n
        }
    }
}

/// Ten to the power `d` as the nearest double; infinite beyond the range.
fn pow10(d: u64) -> f64 {
    if d > 308 {
        return f64::INFINITY;
    }
    format!("1e{d}").parse().unwrap_or(f64::INFINITY)
}

/// A double rounded to `d` decimals the way the servers compute it: the
/// number is multiplied by a power of ten (or, for a negative `d`, divided
/// by one), rounded to a whole number (ties to even; `TRUNCATE` cuts
/// toward zero) and scaled back. The scaling itself is inexact for large
/// `d`, and the servers' answers show it; when the product leaves the
/// range the number is returned as it is.
pub(super) fn round_double(x: f64, d: i64, truncate: bool) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let whole = |v: f64| {
        if truncate {
            v.trunc()
        } else {
            v.round_ties_even()
        }
    };
    let scale = pow10(d.unsigned_abs());
    if d < 0 {
        if scale.is_infinite() {
            return 0.0;
        }
        return whole(x / scale) * scale;
    }
    let scaled = x * scale;
    if !scaled.is_finite() {
        return x;
    }
    whole(scaled) / scale
}

/// `ROUND(x [, d])` and `TRUNCATE(x, d)`.
pub(crate) fn round(a: Node, d: Option<Node>, truncate: bool, env: Env<'_>) -> Result<Node> {
    let name = if truncate { "truncate" } else { "round" };
    let t = num_ty(&a.ty);
    let lit_d = match &d {
        None => Some(0),
        // MySQL looks at the constant only where it shapes the type.
        Some(n) if !env.maria() && t.class == Class::Real => {
            let mut scratch = crate::Ctx::new(env.s);
            if n.konst {
                n.int(&mut scratch, &[]).ok().flatten().map(|i| i.v)
            } else {
                None
            }
        }
        Some(n) => n.const_i64(env)?,
    };
    let maria = env.maria();
    let nullable = a.ty.nullable || d.as_ref().is_some_and(|n| n.ty.nullable);
    let digits = move |ctx: &mut crate::Ctx<'_>, row: &[Value]| -> Result<Option<i64>> {
        match &d {
            None => Ok(Some(0)),
            Some(n) => n.int_arg(ctx, row),
        }
    };
    let max_scale = i64::from(env.max_scale());
    Ok(match t.class {
        Class::Real => {
            // MariaDB prints the result with the constant number of
            // decimals; MySQL prints the double as it is.
            let dec = match lit_d {
                _ if !maria => u32::from(NOT_FIXED),
                Some(v) if (0..i64::from(NOT_FIXED)).contains(&v) => v as u32,
                Some(v) if v < 0 => 0,
                Some(_) => u32::from(NOT_FIXED),
                None => u32::from(t.dec),
            };
            let mut n = Node::new(double_ty(dec, nullable), name, move |ctx, row| {
                let x = a.f64(ctx, row)?;
                let k = digits(ctx, row)?;
                let (Some(x), Some(k)) = (x, k) else {
                    return Ok(Value::Null);
                };
                Ok(Value::Double(round_double(x, k, truncate)))
            });
            n.strict_real = true;
            n
        }
        Class::Int => {
            // Rounding to tens or beyond can add a digit. MariaDB then
            // widens the type: a 32-bit one to 64 bits, a 64-bit one to
            // a decimal. MySQL keeps a 64-bit integer and fails when the
            // result leaves it.
            let grows = !truncate && lit_d.is_none_or(|v| v < 0);
            let wide = a.ty.width == 8 || (a.ty.width == 0 && a.ty.len > 9);
            let mode = if truncate {
                Round::Truncate
            } else {
                Round::HalfUp
            };
            if maria && grows && wide {
                let len = a.ty.len.max(1) + 1;
                let prec = len - u32::from(!t.unsigned);
                let ty = env.decimal(prec, 0, t.unsigned).with_null(nullable);
                return Ok(Node::new(ty, name, move |ctx, row| {
                    let x = a.int(ctx, row)?;
                    let k = digits(ctx, row)?;
                    let (Some(x), Some(k)) = (x, k) else {
                        return Ok(Value::Null);
                    };
                    Ok(conv::dec_value(
                        &x.dec().round(k.clamp(-40, 0) as i32, mode),
                    ))
                }));
            }
            let ty = if !maria {
                ExprType {
                    prec: if t.unsigned { 20 } else { 19 },
                    ..ExprType::int(21, t.unsigned)
                }
            } else if grows {
                ExprType {
                    prec: (t.prec + 1).clamp(1, 20) as u8,
                    ..ExprType::int(a.ty.len.max(1) + 1, t.unsigned)
                }
            } else {
                ExprType {
                    prec: t.prec.clamp(1, 20) as u8,
                    width: a.ty.width,
                    ..ExprType::int(a.ty.len.max(1), t.unsigned)
                }
            }
            .with_null(nullable);
            let msg = format!("{name}({})", a.text);
            Node::new(ty, name, move |ctx, row| {
                let x = a.int(ctx, row)?;
                let k = digits(ctx, row)?;
                let (Some(x), Some(k)) = (x, k) else {
                    return Ok(Value::Null);
                };
                if k >= 0 {
                    return Ok(x.value());
                }
                let r = x.dec().round(k.max(-40) as i32, mode);
                let fits = if x.unsigned {
                    r.to_u64().is_ok()
                } else {
                    r.to_i64().is_ok()
                };
                if !fits && !ctx.maria() {
                    let what = if x.unsigned {
                        "BIGINT UNSIGNED"
                    } else {
                        "BIGINT"
                    };
                    return Err(range_error(what, &msg));
                }
                Ok(conv::dec_to_int(ctx, &r, x.unsigned).value())
            })
        }
        Class::Dec => {
            let (scale, prec) = match lit_d {
                Some(v) => {
                    let s = v.clamp(0, max_scale) as u32;
                    // MariaDB's result has the asked scale; MySQL's never
                    // more than the argument's.
                    let s = if maria { s } else { s.min(t.scale) };
                    // Rounding digits away can carry into a new digit.
                    let carry = u32::from(!truncate && s < t.scale);
                    (s, t.int_part() + s + carry)
                }
                None => (t.scale, t.prec),
            };
            let ty = env.decimal(prec, scale, t.unsigned).with_null(nullable);
            let type_scale = t.scale;
            let result_scale = ty.dec;
            Node::new(ty, name, move |ctx, row| {
                let x = a.dec(ctx, row)?;
                let k = digits(ctx, row)?;
                let (Some(x), Some(k)) = (x, k) else {
                    return Ok(Value::Null);
                };
                let mode = if truncate {
                    Round::Truncate
                } else {
                    Round::HalfUp
                };
                let mut k = k.clamp(-70, max_scale) as i32;
                if !maria {
                    // MySQL's result has no more decimals than the
                    // argument's type; digits a quotient holds beyond
                    // them are rounded or cut away there.
                    k = k.min(type_scale as i32);
                } else {
                    // MariaDB's has no more than its own type's.
                    k = k.min(result_scale as i32);
                }
                let r = x.round(k.min(x.scale as i32), mode);
                Ok(conv::dec_value(&r))
            })
        }
    })
}

/// A function of one or two doubles giving a double. `f` returns `None`
/// outside its domain: the result is then `NULL`.
fn real_fn(
    name: &'static str,
    args: Vec<Node>,
    log: bool,
    f: impl Fn(f64, f64) -> Option<f64> + Send + Sync + 'static,
) -> Node {
    let mut n = Node::new(ExprType::double().nullable(), name, move |ctx, row| {
        let x = args[0].f64(ctx, row)?;
        let y = match args.get(1) {
            Some(b) => b.f64(ctx, row)?,
            None => Some(0.0),
        };
        let (Some(x), Some(y)) = (x, y) else {
            return Ok(Value::Null);
        };
        match f(x, y) {
            Some(v) => check_double(v, name),
            None => {
                if log {
                    // A logarithm outside its domain.
                    if ctx.maria() {
                        if ctx
                            .session
                            .sql_mode
                            .has(SqlMode::ERROR_FOR_DIVISION_BY_ZERO)
                        {
                            ctx.warn(1365, "Division by 0");
                        }
                    } else {
                        ctx.warn(3020, "Invalid argument for logarithm");
                    }
                }
                Ok(Value::Null)
            }
        }
    });
    n.strict_real = true;
    n
}

/// `POW`, `SQRT`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`.
pub(crate) fn real(name: &str, args: Vec<Node>, env: Env<'_>) -> Node {
    let two = args.len() == 2;
    if name == "LOG2" && !env.maria() {
        return real_fn("log2", args, true, |x, _| (x > 0.0).then(|| x.log2()));
    }
    match name {
        "POW" | "POWER" => real_fn("pow", args, false, |x, y| Some(x.powf(y))),
        "SQRT" => real_fn("sqrt", args, false, |x, _| (x >= 0.0).then(|| x.sqrt())),
        "EXP" => real_fn("exp", args, false, |x, _| Some(x.exp())),
        "LOG2" => real_fn("log2", args, true, |x, _| {
            (x > 0.0).then(|| x.ln() / std::f64::consts::LN_2)
        }),
        "LOG10" => real_fn("log10", args, true, |x, _| (x > 0.0).then(|| x.log10())),
        "LOG" if two => log_base(args),
        _ => real_fn("ln", args, true, |x, _| (x > 0.0).then(|| x.ln())),
    }
}

/// `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN` (one or two
/// arguments) and `ATAN2`. Outside `ASIN`'s and `ACOS`'s domain the result
/// is `NULL`; an infinite result (`COT(0)`) is error 1690.
pub(crate) fn trig(name: &str, args: Vec<Node>) -> Node {
    match name {
        "SIN" => real_fn("sin", args, false, |x, _| Some(x.sin())),
        "COS" => real_fn("cos", args, false, |x, _| Some(x.cos())),
        "TAN" => real_fn("tan", args, false, |x, _| Some(x.tan())),
        "COT" => real_fn("cot", args, false, |x, _| Some(1.0 / x.tan())),
        "ASIN" => real_fn("asin", args, false, |x, _| {
            (-1.0..=1.0).contains(&x).then(|| x.asin())
        }),
        "ACOS" => real_fn("acos", args, false, |x, _| {
            (-1.0..=1.0).contains(&x).then(|| x.acos())
        }),
        _ if args.len() == 2 => atan2(args),
        _ => real_fn("atan", args, false, |x, _| Some(x.atan())),
    }
}

/// `DEGREES(x)` and `RADIANS(x)`: `NULL` only for a `NULL` argument.
pub(crate) fn degrees(name: &str, a: Node) -> Node {
    let nullable = a.ty.nullable;
    let mut n = if name == "DEGREES" {
        // (Adding zero turns a negative zero into zero, as on MySQL.)
        real_fn("degrees", vec![a], false, |x, _| {
            Some(x * (180.0 / std::f64::consts::PI) + 0.0)
        })
    } else {
        real_fn("radians", vec![a], false, |x, _| {
            Some(x * (std::f64::consts::PI / 180.0) + 0.0)
        })
    };
    n.ty.nullable = nullable;
    n
}

/// `PI()`: shown with six decimals.
pub(crate) fn pi() -> Node {
    let ty = ExprType {
        len: 8,
        dec: 6,
        ..ExprType::double()
    };
    let mut n = Node::new(ty, "pi()", |_, _| Ok(Value::Double(std::f64::consts::PI)));
    n.strict_real = true;
    n
}

static NEXT_RAND: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// The random number generator's modulus.
const RAND_MAX: u64 = 0x3FFF_FFFF;

/// The generator's two state words for a seed: the seed is cut to 32
/// bits and mixed in 32-bit arithmetic.
fn rand_init(seed: u32) -> (u64, u64) {
    let s1 = seed.wrapping_mul(0x10001).wrapping_add(55_555_555);
    let s2 = seed.wrapping_mul(0x1000_0001);
    (u64::from(s1) % RAND_MAX, u64::from(s2) % RAND_MAX)
}

/// One step of the generator: the next state and the number in [0, 1).
fn rand_step(s1: u64, s2: u64) -> (u64, u64, f64) {
    let a = (s1 * 3 + s2) % RAND_MAX;
    let b = (a + s2 + 33) % RAND_MAX;
    (a, b, a as f64 / RAND_MAX as f64)
}

/// `RAND([seed])`. With a constant seed the statement draws a sequence
/// from the seeded generator, one number per call; with a seed read from
/// the row the generator is seeded again for each row; without a seed it
/// is seeded from the clock once per statement.
pub(crate) fn rand(seed: Option<Node>) -> Node {
    let id = NEXT_RAND.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let key = usize::MAX - id;
    let ty = ExprType::double().with_null(seed.as_ref().is_some_and(|s| s.ty.nullable));
    let mut n = Node::new(ty, "rand", move |ctx, row| {
        let state = match &seed {
            Some(s) if !s.konst => {
                let v = s.int_typed(ctx, row)?.map_or(0, |i| i.v as u32);
                rand_init(v)
            }
            _ => match ctx.consts.get(&key) {
                Some(Value::UInt(p)) => (p >> 32, p & 0xFFFF_FFFF),
                _ => match &seed {
                    Some(s) => {
                        let v = s.int_typed(ctx, row)?.map_or(0, |i| i.v as u32);
                        rand_init(v)
                    }
                    None => {
                        let t = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_or(0, |d| d.subsec_nanos() ^ d.as_secs() as u32);
                        rand_init(t ^ id as u32)
                    }
                },
            },
        };
        let (a, b, x) = rand_step(state.0, state.1);
        ctx.consts.insert(key, Value::UInt((a << 32) | b));
        Ok(Value::Double(x))
    });
    n.strict_real = true;
    n
}

/// `LOG(b, x)`: the base is read first, and a base that is no positive
/// number ends the call (`x` is then not read); `x` must be positive and
/// the base not 1. Each failure is `NULL` with the logarithm's condition.
fn log_base(args: Vec<Node>) -> Node {
    let invalid = |ctx: &mut crate::session::Ctx<'_>| {
        if ctx.maria() {
            if ctx
                .session
                .sql_mode
                .has(SqlMode::ERROR_FOR_DIVISION_BY_ZERO)
            {
                ctx.warn(1365, "Division by 0");
            }
        } else {
            ctx.warn(3020, "Invalid argument for logarithm");
        }
        Ok(Value::Null)
    };
    let mut n = Node::new(ExprType::double().nullable(), "log", move |ctx, row| {
        let Some(b) = args[0].f64(ctx, row)? else {
            return Ok(Value::Null);
        };
        if b <= 0.0 {
            return invalid(ctx);
        }
        let Some(x) = args[1].f64(ctx, row)? else {
            return Ok(Value::Null);
        };
        if x <= 0.0 || b == 1.0 {
            return invalid(ctx);
        }
        check_double(x.ln() / b.ln(), "log")
    });
    n.strict_real = true;
    n
}

/// `ATAN(y, x)` and `ATAN2(y, x)`: a `NULL` first argument ends the call
/// before the second is read.
fn atan2(args: Vec<Node>) -> Node {
    let mut n = Node::new(ExprType::double().nullable(), "atan2", move |ctx, row| {
        let Some(y) = args[0].f64(ctx, row)? else {
            return Ok(Value::Null);
        };
        let Some(x) = args[1].f64(ctx, row)? else {
            return Ok(Value::Null);
        };
        check_double(y.atan2(x), "atan2")
    });
    n.strict_real = true;
    n
}
