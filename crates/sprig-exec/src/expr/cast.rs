//! `CAST`, the conversion of a value into a merged result type, and the
//! merge itself (what `CASE`, `IF`, `COALESCE` and `UNION` columns share).

use std::sync::Arc;

use bytes::Bytes;
use sprig_engine::keys::collation::Collation;
use sprig_engine::row::Value;

use super::{CastTarget, Env, Node};
use crate::conv::{
    self, Info, IntVal, dec_to_int, err_text, f64_to_i64, fmt_double, range_error, str_to_dec,
    str_to_int, to_dec, to_f64,
};
use crate::error::{Level, Result, SqlError};
use crate::num::{Dec, Round};
use crate::session::Ctx;
use crate::temporal;
use crate::types::{Derivation, ExprType, Kind, NOT_FIXED};

/// The node as a string: itself when it is one, else its text in the
/// connection collation with the `NUMERIC` derivation.
pub(crate) fn string_node(n: Node, env: Env<'_>) -> Node {
    // A JSON value read as a string on MySQL is its text in MySQL's form.
    if n.ty.json && !env.maria() {
        let ty = ExprType {
            json: false,
            ..n.ty
        };
        let text = n.text.clone();
        let konst = n.konst;
        let mut out = Node::new(ty, text, move |ctx, row| {
            Ok(match n.eval(ctx, row)? {
                Value::Json(b) => Value::Str(crate::func::json::mysql_text(&b)),
                other => other,
            })
        });
        out.konst = konst;
        return out;
    }
    if n.ty.kind == Kind::Str {
        return n;
    }
    let maria = env.maria();
    let from = n.ty;
    let chars = match from.kind {
        Kind::Null => 0,
        Kind::Double if !env.maria() => 22,
        _ => from.len,
    };
    let (coll, deriv) = if from.kind == Kind::Null {
        (Collation::Binary, Derivation::Ignorable)
    } else if !env.maria() && n.lit.is_none() {
        // MySQL holds a computed number's text as loosely as a literal's.
        (env.conn(), Derivation::Coercible)
    } else {
        (env.conn(), Derivation::Numeric)
    };
    let ty = ExprType {
        ascii: true,
        ..ExprType::string(chars, coll, deriv).with_null(from.nullable)
    };
    let text = n.text.clone();
    let binary = coll == Collation::Binary;
    let lit = n
        .lit
        .as_ref()
        .and_then(|v| conv::to_text_in_string(v, &from, maria))
        .map(|b| {
            if binary {
                Value::Bytes(b)
            } else {
                Value::Str(b)
            }
        });
    let own_text = n.alt.as_ref().and_then(|a| a.text.clone());
    let konst = n.konst;
    let mut out = Node::new(ty, text, move |ctx, row| {
        if let Some(t) = &own_text {
            return t(ctx, row);
        }
        let v = n.eval(ctx, row)?;
        Ok(match conv::to_text_in_string(&v, &from, maria) {
            None => Value::Null,
            Some(b) if binary => Value::Bytes(b),
            Some(b) => Value::Str(b),
        })
    });
    out.lit = lit;
    out.konst = konst;
    out
}

pub(crate) type CoerceFn = Arc<dyn Fn(&mut Ctx<'_>, Value) -> Result<Value> + Send + Sync>;

/// How a value of static type `from` becomes a value of merged type `to`.
pub(crate) fn coerce_fn(from: &ExprType, to: &ExprType, env: Env<'_>) -> CoerceFn {
    let (from, to) = (*from, *to);
    let offset = env.s.time_zone_offset;
    let maria = env.maria();
    match to.kind {
        Kind::Null => Arc::new(|_, v| Ok(v)),
        Kind::Int | Kind::Year | Kind::Bit => Arc::new(move |ctx, v| {
            Ok(match v {
                Value::Null => Value::Null,
                Value::Int(_) | Value::UInt(_) => v,
                other => conv::to_int(ctx, &other).map_or(Value::Null, IntVal::value),
            })
        }),
        Kind::Decimal => Arc::new(move |ctx, v| {
            Ok(match to_dec(ctx, &v) {
                None => Value::Null,
                Some(d) => conv::dec_value(&d),
            })
        }),
        Kind::Double | Kind::Float => Arc::new(move |ctx, v| {
            Ok(match to_f64(ctx, &v) {
                None => Value::Null,
                Some(x) => Value::Double(x),
            })
        }),
        Kind::Str => {
            let binary = to.coll == Collation::Binary;
            // A string of another (non-binary) character set is converted.
            let (fc, tc) = (from.coll.charset(), to.coll.charset());
            let recode = from.kind == Kind::Str
                && fc != tc
                && fc != sprig_engine::keys::collation::Charset::Binary
                && tc != sprig_engine::keys::collation::Charset::Binary;
            Arc::new(move |_, v| {
                Ok(match v {
                    Value::Null => Value::Null,
                    Value::Str(b) if recode => {
                        Value::Str(crate::charset::convert(&b, fc, tc).0.into())
                    }
                    // A JSON value made text on MySQL: its MySQL form.
                    Value::Json(b) if !maria && !to.json => {
                        let t = crate::func::json::mysql_text(&b);
                        if binary {
                            Value::Bytes(t)
                        } else {
                            Value::Str(t)
                        }
                    }
                    Value::Str(b) | Value::Bytes(b) | Value::Json(b) => {
                        if binary {
                            Value::Bytes(b)
                        } else {
                            Value::Str(b)
                        }
                    }
                    other => match conv::to_text_in_string(&other, &from, maria) {
                        None => Value::Null,
                        Some(b) if binary => Value::Bytes(b),
                        Some(b) => Value::Str(b),
                    },
                })
            })
        }
        Kind::Date | Kind::Time | Kind::DateTime | Kind::Timestamp => {
            let _ = offset;
            Arc::new(move |ctx, v| Ok(temporal::coerce(ctx, v, to.kind, to.dec)))
        }
    }
}

/// The merged type of result branches (`CASE`, `IF`, `IFNULL`, `COALESCE`,
/// `UNION` columns). `op` names the operation in a collation error.
pub(crate) fn agg_result_type(types: &[ExprType], op: &str, env: Env<'_>) -> Result<ExprType> {
    let nullable = types.iter().any(|t| t.nullable);
    let live: Vec<&ExprType> = types.iter().filter(|t| t.kind != Kind::Null).collect();
    if live.is_empty() {
        return Ok(ExprType::null());
    }
    let all = |f: fn(Kind) -> bool| live.iter().all(|t| f(t.kind));
    let any = |f: fn(Kind) -> bool| live.iter().any(|t| f(t.kind));
    let max_len = live.iter().map(|t| t.len).max().unwrap_or(0);
    let out = if all(Kind::is_int) && !live.iter().any(|t| t.hex) {
        let all_unsigned = live.iter().all(|t| t.unsigned);
        let none_unsigned = live.iter().all(|t| !t.unsigned);
        let prec = live.iter().map(|t| u32::from(t.prec)).max().unwrap_or(1);
        // Column integers of one signedness keep the widest column's
        // width. On MariaDB a computed integer next to a column counts as
        // the type it is reported as (`INT` up to nine digits, else
        // `BIGINT`): `int_col UNION SELECT 7` stays `INT`.
        let eff = |t: &&ExprType| match t.width {
            0 if t.len <= 9 => 4,
            0 => 8,
            w => w,
        };
        let columns_only = live.iter().all(|t| t.width > 0);
        let width = if (all_unsigned || none_unsigned)
            && (columns_only || (env.maria() && live.iter().any(|t| t.width > 0)))
        {
            live.iter().map(eff).max().unwrap_or(0)
        } else {
            0
        };
        // The length follows the columns-only rule either way.
        let merged_len = |signed_extra: u32| {
            if env.maria() && !((all_unsigned || none_unsigned) && columns_only) {
                prec + signed_extra
            } else {
                max_len
            }
        };
        let with_null = types.iter().any(|t| t.kind == Kind::Null);
        if all_unsigned && with_null && env.maria() && op != "if" {
            // MariaDB counts the NULL literal as signed: with an
            // unsigned integer the result is a decimal (not in IF).
            ExprType::decimal(prec, 0, false)
        } else if all_unsigned || none_unsigned {
            ExprType {
                prec: prec as u8,
                width,
                ..ExprType::int(merged_len(u32::from(none_unsigned)), all_unsigned)
            }
        } else {
            // Signed and unsigned integers together: MariaDB makes the
            // result a decimal; MySQL only when the unsigned side is as
            // wide as a 64-bit one.
            let wide_unsigned = live
                .iter()
                .filter(|t| t.unsigned)
                .map(|t| u32::from(t.prec))
                .max()
                .unwrap_or(0);
            if env.maria() {
                ExprType::decimal(prec, 0, false)
            } else if wide_unsigned >= 19 {
                ExprType::decimal(prec.max(wide_unsigned), 0, false)
            } else {
                ExprType {
                    prec: prec as u8,
                    ..ExprType::int(max_len, false)
                }
            }
        }
    } else if all(Kind::is_exact) && !live.iter().any(|t| t.hex) {
        let scale = live.iter().map(|t| t.scale()).max().unwrap_or(0);
        let intg = live.iter().map(|t| t.int_part()).max().unwrap_or(1);
        let unsigned = live.iter().all(|t| t.unsigned);
        env.decimal(intg + scale, scale, unsigned)
    } else if all(Kind::is_numeric) && !live.iter().any(|t| t.hex) {
        let fixed = live.iter().all(|t| t.dec < NOT_FIXED);
        let dec = if fixed {
            live.iter().map(|t| t.dec).max().unwrap_or(0)
        } else {
            NOT_FIXED
        };
        ExprType {
            kind: if all(|k| k == Kind::Float) {
                Kind::Float
            } else {
                Kind::Double
            },
            dec,
            // MariaDB keeps the longest branch's length; MySQL gives a
            // double its full length.
            len: if env.maria() && dec == NOT_FIXED {
                max_len
            } else if dec == NOT_FIXED {
                23
            } else {
                max_len.max(17 + u32::from(dec))
            },
            ..ExprType::double()
        }
    } else if all(Kind::is_temporal) {
        let fsp = live.iter().map(|t| t.dec).max().unwrap_or(0);
        if all(|k| k == Kind::Date) {
            ExprType::date()
        } else if all(|k| k == Kind::Time) {
            ExprType::time(fsp)
        } else if all(|k| k == Kind::Timestamp) {
            ExprType {
                kind: Kind::Timestamp,
                ..ExprType::datetime(fsp)
            }
        } else {
            ExprType::datetime(fsp)
        }
    } else {
        let _ = any;
        let tys: Vec<&ExprType> = live.clone();
        let dtc = crate::types::aggregate_types(&tys, env.conn(), op, false)?;
        let chars = live
            .iter()
            .map(|t| match t.kind {
                Kind::Double | Kind::Float if !env.maria() => 22,
                _ => t.len,
            })
            .max()
            .unwrap_or(0);
        let all_hex = live.iter().all(|t| t.hex);
        // MariaDB gives a merged string that can only be empty no
        // decimals: arithmetic on it is a double with the other
        // operand's.
        let empty = env.maria() && chars == 0 && live.iter().all(|t| t.kind == Kind::Str);
        // JSON values: on MySQL, all of them make a JSON value; JSON with
        // anything else (and any JSON on MariaDB) is the longest text.
        let all_json = live.iter().all(|t| t.json);
        let any_json = live.iter().any(|t| t.json);
        ExprType {
            hex: all_hex,
            unsigned: all_hex,
            dec: if all_hex || empty { 0 } else { NOT_FIXED },
            json: all_json,
            mb3: dtc.mb3,
            ..ExprType::string(
                if any_json && (env.maria() || !all_json) {
                    crate::types::JSON_TEXT_LEN
                } else {
                    chars
                },
                dtc.coll,
                dtc.deriv,
            )
        }
    };
    Ok(out.with_null(nullable))
}

fn note_or_warn(ctx: &mut Ctx<'_>, maria_level: Level, code: u16, msg: String) {
    ctx.push(maria_level, code, msg);
}

/// `CAST(a AS target)`.
pub(crate) fn cast(a: Node, target: CastTarget, env: Env<'_>) -> Result<Node> {
    let maria = env.maria();
    let from = a.ty;
    let nullable = from.nullable;
    let name = a.text.clone();
    Ok(match target {
        CastTarget::Signed | CastTarget::Unsigned => {
            let unsigned = target == CastTarget::Unsigned;
            let ty = if maria {
                if unsigned {
                    let non_negative = from.kind.is_int()
                        && (from.unsigned
                            || a.lit_i64().is_some_and(|v| v >= 0)
                            || matches!(a.lit, Some(Value::UInt(_))));
                    if non_negative {
                        ExprType::int_prec(u32::from(from.prec), true)
                    } else {
                        ExprType {
                            prec: 20,
                            ..ExprType::int(20, true)
                        }
                    }
                } else if from.kind.is_real() {
                    ExprType {
                        prec: 19,
                        ..ExprType::int(20, false)
                    }
                } else {
                    let len = from.len.clamp(2, 21);
                    ExprType {
                        prec: (len - 1).clamp(1, 19) as u8,
                        ..ExprType::int(len, false)
                    }
                }
            } else {
                ExprType {
                    prec: if unsigned { 21 } else { 20 },
                    ..ExprType::int(21, unsigned)
                }
            }
            .with_null(nullable);
            let text = format!(
                "cast({} as {})",
                a.text,
                if unsigned { "unsigned" } else { "signed" }
            );
            let info = a.info();
            // MySQL asks a node that reads as an integer in its own way
            // (IF and CASE: the chosen branch; a cast to DOUBLE: cut,
            // not rounded) for that integer.
            // A string-typed one is cast from its text.
            let own_int = !maria
                && matches!(a.ty.kind, Kind::Double | Kind::Int)
                && a.alt.as_ref().is_some_and(|x| x.int.is_some());
            let unsigned_decimal =
                !maria && !unsigned && from.kind == Kind::Decimal && from.unsigned;
            Node::new(ty, text, move |ctx, row| {
                if own_int {
                    return Ok(match a.int(ctx, row)? {
                        None => Value::Null,
                        Some(i) if unsigned => Value::UInt(i.v as u64),
                        Some(i) => Value::Int(i.v),
                    });
                }
                let v = a.eval(ctx, row)?;
                // MySQL casts an unsigned decimal beyond the signed range
                // as an unsigned integer read back as signed (it wraps).
                if unsigned_decimal {
                    if let Value::Decimal(d) = &v {
                        let d = Dec::from_decimal(d);
                        if !d.neg {
                            return Ok(Value::Int(conv::dec_to_int(ctx, &d, true).v));
                        }
                    }
                }
                cast_int(ctx, &v, unsigned, info, &a.text)
            })
        }
        CastTarget::Decimal(p, s) => {
            let ty = ExprType::decimal(u32::from(p), u32::from(s), false).with_null(nullable);
            let text = format!("cast({} as decimal({p},{s}))", a.text);
            let info = a.info();
            let own_dec = a.alt.as_ref().is_some_and(|x| x.dec.is_some());
            Node::new(ty, text, move |ctx, row| {
                let d = if own_dec {
                    match a.dec(ctx, row)? {
                        Some(d) => d,
                        None => return Ok(Value::Null),
                    }
                } else {
                    let v = a.eval(ctx, row)?;
                    match &v {
                        Value::Null => return Ok(Value::Null),
                        Value::Str(b) | Value::Bytes(b) if !info.hex && !info.quiet => {
                            str_to_dec(ctx, b, true)
                        }
                        other => match conv::conv_dec(ctx, other, info) {
                            Some(d) => d,
                            None => return Ok(Value::Null),
                        },
                    }
                };
                let r = d.round(i32::from(s), Round::HalfUp);
                if r.int_digits() > 65 {
                    // The number didn't even fit a decimal.
                    if ctx.maria() {
                        ctx.warn(
                            1916,
                            "Got overflow when converting '' to DECIMAL. Value truncated",
                        );
                    } else {
                        ctx.warn(1292, "Truncated incorrect DECIMAL value: '1'");
                    }
                }
                if r.int_digits() > usize::from(p - s.min(p)) {
                    ctx.warn(
                        1264,
                        format!("Out of range value for column '{name}' at row 1"),
                    );
                    return Ok(conv::dec_value(&Dec::max_of(
                        u32::from(p),
                        u32::from(s),
                        r.neg,
                    )));
                }
                Ok(conv::dec_value(&r))
            })
        }
        CastTarget::Double => {
            let ty = if maria {
                ExprType {
                    len: 22,
                    ..ExprType::double()
                }
                .nullable()
            } else {
                ExprType::double().with_null(nullable)
            };
            let text = format!("cast({} as double)", a.text);
            let a2 = a.clone();
            let name: Arc<str> = text.clone().into();
            let mut n = Node::new(ty, text, move |ctx, row| {
                Ok(a.f64(ctx, row)?.map_or(Value::Null, Value::Double))
            });
            n.strict_real = true;
            if !maria {
                // MySQL reads this cast as an integer by cutting the
                // fraction off, not by rounding, and refuses what does
                // not lie strictly inside the 64-bit range.
                n.alt = Some(Arc::new(super::Alt {
                    int: Some(Arc::new(move |ctx, row| {
                        let Some(x) = a2.f64(ctx, row)? else {
                            return Ok(None);
                        };
                        let t = x.trunc();
                        if !(t > -9_223_372_036_854_775_808.0 && t < 9_223_372_036_854_775_808.0) {
                            return Err(range_error("BIGINT", &name));
                        }
                        Ok(Some(IntVal::signed(t as i64)))
                    })),
                    ..super::Alt::default()
                }));
            }
            n
        }
        CastTarget::Char(n) => {
            let a = string_node(a, env);
            let chars = n.unwrap_or(a.ty.len);
            let coll = env.conn();
            let deriv = if maria {
                Derivation::Cast
            } else {
                Derivation::Implicit
            };
            let ty = ExprType {
                dec: if maria { 0 } else { NOT_FIXED },
                cast_char: maria,
                ..ExprType::string(chars, coll, deriv)
            }
            .nullable();
            let text = format!("cast({} as char)", a.text);
            let charset = coll.charset();
            Node::new(ty, text, move |ctx, row| {
                let v = a.eval(ctx, row)?;
                let (Value::Str(b) | Value::Bytes(b)) = v else {
                    return Ok(Value::Null);
                };
                let Some(n) = n else {
                    return Ok(Value::Str(b));
                };
                let cut = charset.char_prefix(&b, n as usize);
                if cut.len() < b.len() {
                    ctx.warn(
                        1292,
                        format!("Truncated incorrect CHAR({n}) value: '{}'", err_text(&b)),
                    );
                    return Ok(Value::Str(b.slice(..cut.len())));
                }
                Ok(Value::Str(b))
            })
        }
        CastTarget::Binary(n) => {
            let was_string = a.ty.kind == Kind::Str;
            let a = string_node(a, env);
            // The bytes a value can have: a number's characters (ASCII),
            // a string's characters at their widest.
            let mb = if was_string {
                crate::types::mb_max_len(a.ty.coll)
            } else {
                1
            };
            let bytes = n.unwrap_or(a.ty.len.saturating_mul(mb));
            let ty = ExprType {
                dec: if maria { 0 } else { NOT_FIXED },
                ..ExprType::string(bytes, Collation::Binary, Derivation::Implicit)
            }
            .nullable();
            let text = format!("cast({} as binary)", a.text);
            let from_charset = a.ty.coll.charset();
            Node::new(ty, text, move |ctx, row| {
                let v = a.eval(ctx, row)?;
                let (Value::Str(b) | Value::Bytes(b)) = v else {
                    return Ok(Value::Null);
                };
                let Some(n) = n.map(|n| n as usize) else {
                    return Ok(Value::Bytes(b));
                };
                if maria {
                    // MariaDB keeps `n` characters of the source string,
                    // then pads to `n` bytes.
                    let cut = from_charset.char_prefix(&b, n);
                    if cut.len() < b.len() {
                        ctx.warn(
                            1292,
                            format!("Truncated incorrect BINARY({n}) value: '{}'", err_text(&b)),
                        );
                        return Ok(Value::Bytes(b.slice(..cut.len())));
                    }
                    if b.len() >= n {
                        return Ok(Value::Bytes(b));
                    }
                    let mut padded = b.to_vec();
                    padded.resize(n, 0);
                    return Ok(Value::Bytes(Bytes::from(padded)));
                }
                if b.len() > n {
                    ctx.warn(
                        1292,
                        format!("Truncated incorrect BINARY({n}) value: '{}'", err_text(&b)),
                    );
                    return Ok(Value::Bytes(b.slice(..n)));
                }
                let mut padded = b.to_vec();
                padded.resize(n, 0);
                Ok(Value::Bytes(Bytes::from(padded)))
            })
        }
        CastTarget::Date | CastTarget::Time(_) | CastTarget::DateTime(_) => {
            temporal::cast(a, target, env)?
        }
        // MariaDB has no JSON cast (its parser refuses it).
        CastTarget::Json if maria => return Err(SqlError::unsupported("CAST AS JSON")),
        CastTarget::Json => crate::func::json::cast_json(a, env),
    })
}

/// `CAST(v AS SIGNED)` and `CAST(v AS UNSIGNED)`.
fn cast_int(ctx: &mut Ctx<'_>, v: &Value, unsigned: bool, info: Info, text: &str) -> Result<Value> {
    let maria = ctx.maria();
    if info.hex {
        return Ok(
            conv::conv_int(ctx, v, info, text)?.map_or(Value::Null, |i| {
                if unsigned {
                    Value::UInt(i.v as u64)
                } else {
                    Value::Int(i.v)
                }
            }),
        );
    }
    if info.strict_real && !maria {
        if let Value::Double(x) = v {
            let (i, over) = f64_to_i64(*x);
            if over {
                return Err(range_error("BIGINT", text));
            }
            return Ok(if unsigned {
                Value::UInt(i as u64)
            } else {
                Value::Int(i)
            });
        }
    }
    Ok(cast_int_plain(ctx, v, unsigned))
}

fn cast_int_plain(ctx: &mut Ctx<'_>, v: &Value, unsigned: bool) -> Value {
    let maria = ctx.maria();
    let complement = |ctx: &mut Ctx<'_>, from_string: bool| {
        if maria {
            note_or_warn(
                ctx,
                Level::Note,
                1105,
                if unsigned {
                    "Cast to unsigned converted negative integer to it's positive complement"
                } else {
                    "Cast to signed converted positive out-of-range integer to it's negative complement"
                }
                .to_owned(),
            );
        } else if from_string {
            ctx.warn(
                1105,
                if unsigned {
                    "Cast to unsigned converted negative integer to its positive complement"
                } else {
                    "Cast to signed converted positive out-of-range integer to it's negative complement"
                },
            );
        }
    };
    let finish = |ctx: &mut Ctx<'_>, i: IntVal, from_string: bool| {
        if unsigned {
            if !i.unsigned && i.v < 0 {
                complement(ctx, from_string);
            }
            Value::UInt(i.v as u64)
        } else {
            Value::Int(i.v)
        }
    };
    match v {
        Value::Null => Value::Null,
        Value::Str(s) | Value::Bytes(s) | Value::Json(s) => {
            let i = str_to_int(ctx, s);
            finish(ctx, i, true)
        }
        Value::Decimal(d) => {
            let d = Dec::from_decimal(d);
            if unsigned && d.neg {
                if maria {
                    let r = d.round(0, Round::HalfUp);
                    if r.is_zero() {
                        return Value::UInt(0);
                    }
                    ctx.warn(
                        1916,
                        format!(
                            "Got overflow when converting '{d}' to UNSIGNED INT. Value truncated"
                        ),
                    );
                    return Value::UInt(0);
                }
                let i = dec_to_int(ctx, &d, false);
                return Value::UInt(i.v as u64);
            }
            let i = dec_to_int(ctx, &d, unsigned);
            if unsigned {
                Value::UInt(i.v as u64)
            } else {
                Value::Int(i.v)
            }
        }
        Value::Double(_) | Value::Float(_) => {
            let x = match v {
                Value::Float(f) => f64::from(*f),
                Value::Double(d) => *d,
                _ => 0.0,
            };
            if unsigned && maria {
                let r = x.round_ties_even();
                if r < 0.0 || r >= 18_446_744_073_709_551_616.0 {
                    ctx.note(
                        1916,
                        format!(
                            "Got overflow when converting '{}' to UNSIGNED BIGINT. Value truncated",
                            fmt_double(x)
                        ),
                    );
                    return Value::UInt(if r < 0.0 { 0 } else { u64::MAX });
                }
                return Value::UInt(r as u64);
            }
            let (i, over) = f64_to_i64(x);
            if over && maria {
                ctx.note(
                    1916,
                    format!(
                        "Got overflow when converting '{}' to SIGNED BIGINT. Value truncated",
                        fmt_double(x)
                    ),
                );
            }
            if unsigned {
                Value::UInt(i as u64)
            } else {
                Value::Int(i)
            }
        }
        other => match conv::to_int(ctx, other) {
            None => Value::Null,
            Some(i) => finish(ctx, i, false),
        },
    }
}
