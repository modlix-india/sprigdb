//! Control flow and `NULL` handling: `CASE`, `IF`, `IFNULL`, `NULLIF`,
//! `COALESCE`, `GREATEST` and `LEAST`.

use std::cmp::Ordering;
use std::sync::Arc;

use sprig_engine::row::Value;

use super::cast::CoerceFn;
use super::cmp::{CmpKind, Comparator, Key, LeftKeys, comparator, pairwise};
use super::{Alt, Env, Node, agg_result_type, coerce_fn};
use crate::conv;
use crate::error::Result;
use crate::num::{Dec, Round};
use crate::session::Ctx;
use crate::types::{Derivation, ExprType, Kind, NOT_FIXED, bin_collation};

/// How a branch's value becomes the result of `IF` or `CASE`. MariaDB
/// converts it into the merged type. MySQL hands a decimal branch through
/// in its own shape (`IF(1, 2, 1.50)` is `2`, not `2.00`).
/// [`branch_fn`] for a CASE: on MariaDB a text branch keeps its character
/// set in a binary result (see [`keep_charset_fn`]).
fn case_branch_fn(from: &ExprType, to: &ExprType, env: Env<'_>) -> CoerceFn {
    // (A binary branch that is itself a CASE hands on what it holds.)
    let binary = sprig_engine::keys::collation::Collation::Binary;
    if env.maria()
        && from.kind == Kind::Str
        && !from.hex
        && to.kind == Kind::Str
        && to.coll == binary
    {
        return Arc::new(|_, v| Ok(v));
    }
    if env.maria() && !to.raw {
        return keep_charset_fn(from, to, env);
    }
    branch_fn(from, to, env)
}

fn branch_fn(from: &ExprType, to: &ExprType, env: Env<'_>) -> CoerceFn {
    if !to.raw {
        return coerce_fn(from, to, env);
    }
    let from = *from;
    Arc::new(move |_, v| {
        Ok(match v {
            // (A nested IF or CASE hands its own branch's shape on.)
            Value::Decimal(d)
                if from.kind == Kind::Decimal && !from.raw && d.scale() != from.dec =>
            {
                conv::dec_value(&Dec::from_decimal(&d).round(i32::from(from.dec), Round::HalfUp))
            }
            other => other,
        })
    })
}

/// Refuses (1267 and its siblings) string branches that cannot be
/// converted to the merged type's character set.
fn check_branches(nodes: &[&Node], ty: &ExprType, op: &str, env: Env<'_>) -> Result<()> {
    if ty.kind == Kind::Str && nodes.iter().any(|n| !super::convertible(n, ty.coll, env)) {
        let dtcs: Vec<crate::types::Dtc> = nodes
            .iter()
            .map(|n| crate::types::Dtc::of(&n.ty, env.conn()))
            .collect();
        return Err(crate::types::illegal_mix(&dtcs, op));
    }
    Ok(())
}

fn branch_type(types: &[ExprType], op: &str, env: Env<'_>) -> Result<ExprType> {
    let mut ty = agg_result_type(types, op, env)?;
    if !env.maria() && ty.kind == Kind::Decimal {
        ty.raw = true;
    }
    Ok(ty)
}

type PickFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<usize>> + Send + Sync>;

/// The node of `IF` or `CASE`: `pick` chooses a branch, the branch's value
/// is converted to the merged type. On MySQL a reader that wants a number
/// or a string asks the chosen branch itself: an integer branch of a
/// `DOUBLE`-typed `IF` is read exactly, and a quotient keeps the digits it
/// holds beyond its scale.
fn branching(
    ty: ExprType,
    name: &'static str,
    pick: PickFn,
    branches: Vec<Node>,
    env: Env<'_>,
) -> Node {
    let coerce: Vec<CoerceFn> = branches
        .iter()
        .map(|b| {
            if name == "case" {
                case_branch_fn(&b.ty, &ty, env)
            } else {
                branch_fn(&b.ty, &ty, env)
            }
        })
        .collect();
    let alt = if !env.maria() && ty.kind != Kind::Null && !ty.kind.is_temporal() {
        let (p1, b1) = (pick.clone(), branches.clone());
        let (p2, b2) = (pick.clone(), branches.clone());
        let (p3, b3) = (pick.clone(), branches.clone());
        let (p4, b4) = (pick.clone(), branches.clone());
        let p5 = pick.clone();
        let texts: Vec<Node> = branches
            .iter()
            .map(|b| super::string_node(b.clone(), env))
            .collect();
        let binary =
            ty.kind == Kind::Str && ty.coll == sprig_engine::keys::collation::Collation::Binary;
        Some(Arc::new(Alt {
            f64: Some(Arc::new(move |ctx, row| match p1(ctx, row)? {
                Some(i) => b1[i].f64(ctx, row),
                None => Ok(None),
            })),
            dec: Some(Arc::new(move |ctx, row| match p2(ctx, row)? {
                Some(i) => b2[i].dec(ctx, row),
                None => Ok(None),
            })),
            int: Some(Arc::new(move |ctx, row| match p3(ctx, row)? {
                Some(i) => b3[i].int(ctx, row),
                None => Ok(None),
            })),
            truth: Some(Arc::new(move |ctx, row| match p4(ctx, row)? {
                Some(i) => b4[i].truth(ctx, row),
                None => Ok(None),
            })),
            date_key: None,
            temporal: None,
            temporal_fsp: None,
            as_time: None,
            text: Some(Arc::new(move |ctx, row| match p5(ctx, row)? {
                Some(i) => Ok(match texts[i].eval(ctx, row)? {
                    Value::Str(b) | Value::Bytes(b) if binary => Value::Bytes(b),
                    Value::Str(b) | Value::Bytes(b) => Value::Str(b),
                    _ => Value::Null,
                }),
                None => Ok(Value::Null),
            })),
        }))
    } else {
        None
    };
    let string_result = ty.kind == Kind::Str;
    // MySQL's DOUBLE result is the chosen branch's own double (a quotient
    // with all the digits it holds).
    let double_result = !env.maria() && ty.kind == Kind::Double;
    let mut node = Node::new(ty, name, move |ctx, row| match pick(ctx, row)? {
        Some(i) if double_result => Ok(branches[i]
            .f64(ctx, row)?
            .map_or(Value::Null, Value::Double)),
        Some(i) => {
            // A branch that hands over its own text (a nested IF or CASE
            // on MySQL) does so here too.
            if string_result {
                if let Some(t) = branches[i].alt.as_ref().and_then(|a| a.text.as_ref()) {
                    return t(ctx, row);
                }
            }
            let v = branches[i].eval(ctx, row)?;
            coerce[i](ctx, v)
        }
        None => Ok(Value::Null),
    });
    node.alt = alt;
    node
}

/// Both forms of `CASE`.
pub(crate) fn case(
    operand: Option<Node>,
    pairs: Vec<(Node, Node)>,
    otherwise: Option<Node>,
    env: Env<'_>,
) -> Result<Node> {
    let mut types: Vec<ExprType> = pairs.iter().map(|(_, t)| t.ty).collect();
    if let Some(o) = &otherwise {
        types.push(o.ty);
    }
    let mut ty = branch_type(&types, "case", env)?;
    // MariaDB answers a CASE whose every result is NULL without looking at
    // its conditions.
    if env.maria() && ty.kind == Kind::Null && types.iter().all(|t| t.kind == Kind::Null) {
        return Ok(Node::literal(ExprType::null(), "case", Value::Null));
    }
    // MariaDB reports a CASE as nullable when any operand is.
    let any_nullable =
        operand.as_ref().is_some_and(|o| o.ty.nullable) || pairs.iter().any(|(w, _)| w.ty.nullable);
    if otherwise.is_none() || (env.maria() && any_nullable) {
        ty.nullable = true;
    }
    let cmps: Vec<Option<Comparator>> = match &operand {
        Some(op) => {
            let whens: Vec<&Node> = pairs.iter().map(|(w, _)| w).collect();
            pairwise(op, &whens, "case", false, !env.maria(), env)?
                .into_iter()
                .map(Some)
                .collect()
        }
        None => vec![None; pairs.len()],
    };
    let (whens, mut branches): (Vec<Node>, Vec<Node>) = pairs.into_iter().unzip();
    let has_else = otherwise.is_some();
    if let Some(o) = otherwise {
        branches.push(o);
    }
    let else_index = whens.len();
    let pick: PickFn = Arc::new(move |ctx, row| {
        if let Some(o) = &operand {
            let x = o.eval(ctx, row)?;
            if !x.is_null() {
                let mut left = LeftKeys::new(row);
                for (i, w) in whens.iter().enumerate() {
                    let Some(c) = &cmps[i] else { continue };
                    let kx = left.get(ctx, c, &x, o)?;
                    let ky = c.node_key(ctx, w, row)?;
                    if let (Some(kx), Some(ky)) = (&kx, &ky) {
                        if c.order(kx, ky) == Some(Ordering::Equal) {
                            return Ok(Some(i));
                        }
                    }
                }
            }
        } else {
            for (i, w) in whens.iter().enumerate() {
                if w.truth(ctx, row)? == Some(true) {
                    return Ok(Some(i));
                }
            }
        }
        Ok(has_else.then_some(else_index))
    });
    Ok(branching(ty, "case", pick, branches, env))
}

/// `IF(c, a, b)`.
pub(crate) fn if_(c: Node, a: Node, b: Node, env: Env<'_>) -> Result<Node> {
    let mut ty = branch_type(&[a.ty, b.ty], "if", env)?;
    check_branches(&[&a, &b], &ty, "if", env)?;
    if env.maria() && c.ty.nullable {
        ty.nullable = true;
    }
    let pick: PickFn =
        Arc::new(move |ctx, row| Ok(Some(usize::from(c.truth(ctx, row)? != Some(true)))));
    Ok(branching(ty, "if", pick, vec![a, b], env))
}

/// MySQL reads the string result of `COALESCE`, `IFNULL`, `GREATEST` and
/// `LEAST` as a number without the usual warning, and treats their double
/// result as a computed one.
fn hybrid(mut n: Node, env: Env<'_>) -> Node {
    if !env.maria() {
        n.quiet = n.ty.kind == Kind::Str;
        n.strict_real = n.ty.kind == Kind::Double;
    }
    n
}

/// How an argument's value becomes the result of `COALESCE` (and, on
/// MariaDB, of `CASE`). Both servers hand a text argument's string on in
/// its own character set when the result is binary: a function reading it
/// next (`CHAR_LENGTH`, `LPAD`) counts its characters, not its bytes.
/// (`IF` and `IFNULL` convert it.)
fn keep_charset_fn(from: &ExprType, to: &ExprType, env: Env<'_>) -> CoerceFn {
    let text_into_binary = from.kind == Kind::Str
        && !from.hex
        && from.coll != sprig_engine::keys::collation::Collation::Binary
        && to.kind == Kind::Str
        && to.coll == sprig_engine::keys::collation::Collation::Binary;
    if text_into_binary {
        return Arc::new(|_, v| Ok(v));
    }
    coerce_fn(from, to, env)
}

/// Whether a string result of MySQL's `COALESCE` or `IFNULL` is an
/// argument's own text: MySQL asks the argument for its string, and a
/// nested `IF` or `CASE` answers with its chosen branch's text
/// (`COALESCE(CASE ... THEN -0.10 ... ELSE 1e15 END, 'x')` is `-0.10`).
fn passes_text(ty: &ExprType, env: Env<'_>) -> bool {
    !env.maria() && ty.kind == Kind::Str
}

/// Whether a DOUBLE result of MySQL's `COALESCE` or `IFNULL` is the
/// argument's own double (a quotient with all the digits it holds).
fn passes_double(ty: &ExprType, env: Env<'_>) -> bool {
    !env.maria() && ty.kind == Kind::Double
}

/// An argument's own text, when it has one (see [`passes_text`]).
fn own_text(n: &Node) -> Option<&super::EvalFn> {
    n.alt.as_ref().and_then(|a| a.text.as_ref())
}

/// `IFNULL(a, b)`.
pub(crate) fn ifnull(a: Node, b: Node, env: Env<'_>) -> Result<Node> {
    let mut ty = agg_result_type(&[a.ty, b.ty], "ifnull", env)?;
    check_branches(&[&a, &b], &ty, "ifnull", env)?;
    ty.nullable = a.ty.nullable && b.ty.nullable;
    let (ca, cb) = (coerce_fn(&a.ty, &ty, env), coerce_fn(&b.ty, &ty, env));
    let pass = passes_text(&ty, env);
    let double = passes_double(&ty, env);
    Ok(hybrid(
        Node::new(ty, "ifnull", move |ctx, row| {
            if double {
                if let Some(x) = a.f64(ctx, row)? {
                    return Ok(Value::Double(x));
                }
                return Ok(b.f64(ctx, row)?.map_or(Value::Null, Value::Double));
            }
            if let Some(t) = pass.then(|| own_text(&a)).flatten() {
                let v = t(ctx, row)?;
                if !v.is_null() {
                    return Ok(v);
                }
            } else {
                let v = a.eval(ctx, row)?;
                if !v.is_null() {
                    return ca(ctx, v);
                }
            }
            if let Some(t) = pass.then(|| own_text(&b)).flatten() {
                return t(ctx, row);
            }
            let w = b.eval(ctx, row)?;
            cb(ctx, w)
        }),
        env,
    ))
}

/// `COALESCE(a, ...)`.
pub(crate) fn coalesce(args: Vec<Node>, env: Env<'_>) -> Result<Node> {
    let types: Vec<ExprType> = args.iter().map(|a| a.ty).collect();
    let mut ty = agg_result_type(&types, "coalesce", env)?;
    check_branches(&args.iter().collect::<Vec<_>>(), &ty, "coalesce", env)?;
    ty.nullable = types.iter().all(|t| t.nullable);
    let args: Vec<_> = args
        .into_iter()
        .map(|a| {
            let co = keep_charset_fn(&a.ty, &ty, env);
            (a, co)
        })
        .collect();
    let pass = passes_text(&ty, env);
    let double = passes_double(&ty, env);
    Ok(hybrid(
        Node::new(ty, "coalesce", move |ctx, row| {
            for (a, co) in &args {
                if double {
                    if let Some(x) = a.f64(ctx, row)? {
                        return Ok(Value::Double(x));
                    }
                    continue;
                }
                if let Some(t) = pass.then(|| own_text(a)).flatten() {
                    let v = t(ctx, row)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                    continue;
                }
                let v = a.eval(ctx, row)?;
                if !v.is_null() {
                    return co(ctx, v);
                }
            }
            Ok(Value::Null)
        }),
        env,
    ))
}

/// `NULLIF(a, b)`. As on both servers, `a` is evaluated a second time for
/// the result when the two aren't equal (a predicate's value is kept), and
/// a reader that wants a number asks `a` for it.
pub(crate) fn nullif(a: Node, b: Node, env: Env<'_>) -> Result<Node> {
    // MySQL's conversion of a constant compared with a BIGINT column
    // replaces the argument itself: `NULLIF('', bigint_col)` is the
    // integer 0, `NULLIF(1e16, bigint_col)` an integer.
    let was_int = a.ty.kind == Kind::Int;
    let (mut a, b) = super::cmp::fold_for_column(a, b, env);
    let compared = a.clone();
    if !was_int && a.ty.kind == Kind::Int {
        a.ty.len = 22;
    }
    let cmp = super::cmp::comparator2(&compared, &b, "nullif", env)?;
    let ty = a.ty.nullable();
    // Whether the two are equal; the first argument's value comes along.
    let (a0, b0) = (compared, b);
    // A first argument with its own way to a number (MariaDB's LEAST of
    // strings, MySQL's CASE) is compared by that number.
    let by_alt = a0.alt.is_some()
        && matches!(
            cmp.kind,
            super::cmp::CmpKind::Int | super::cmp::CmpKind::Dec | super::cmp::CmpKind::Real
        );
    let equal: Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<(bool, Value)> + Send + Sync> =
        Arc::new(move |ctx, row| {
            if by_alt {
                let kx = cmp.node_key(ctx, &a0, row)?;
                let ky = match &kx {
                    Some(_) => cmp.node_key(ctx, &b0, row)?,
                    None => None,
                };
                if let (Some(kx), Some(ky)) = (&kx, &ky) {
                    if cmp.order(kx, ky) == Some(Ordering::Equal) {
                        return Ok((true, Value::Null));
                    }
                }
                return Ok((false, Value::Null));
            }
            let x = a0.eval(ctx, row)?;
            if !x.is_null() {
                let kx = cmp.key(ctx, &x, a0.info(), &a0.text, &a0.ty)?;
                let ky = cmp.node_key(ctx, &b0, row)?;
                if let (Some(kx), Some(ky)) = (&kx, &ky) {
                    if cmp.order(kx, ky) == Some(Ordering::Equal) {
                        return Ok((true, x));
                    }
                }
            }
            Ok((false, x))
        });
    let (e1, e2, e3, e4) = (equal.clone(), equal.clone(), equal.clone(), equal.clone());
    // MariaDB reads the result's number from its value: the number of
    // `GREATEST`'s string, not the greatest number (the date behind a
    // DATE_ADD text excepted).
    let from_value = env.maria() && a.alt.as_ref().is_some_and(|x| x.temporal.is_none());
    if from_value {
        a.alt = None;
    }
    let (a1, a2, a3, a4) = (a.clone(), a.clone(), a.clone(), a.clone());
    let alt = Alt {
        f64: Some(Arc::new(move |ctx, row| {
            if e1(ctx, row)?.0 {
                return Ok(None);
            }
            a1.f64(ctx, row)
        })),
        dec: Some(Arc::new(move |ctx, row| {
            if e2(ctx, row)?.0 {
                return Ok(None);
            }
            a2.dec(ctx, row)
        })),
        int: Some(Arc::new(move |ctx, row| {
            if e3(ctx, row)?.0 {
                return Ok(None);
            }
            a3.int(ctx, row)
        })),
        truth: Some(Arc::new(move |ctx, row| {
            if e4(ctx, row)?.0 {
                return Ok(None);
            }
            a4.truth(ctx, row)
        })),
        // MySQL's string reader of NULLIF gets the argument's own text
        // (a nested CASE's chosen branch).
        text: (!env.maria() && a.alt.as_ref().is_some_and(|x| x.text.is_some())).then(|| {
            let (e5, a5) = (equal.clone(), a.clone());
            let f: super::EvalFn = Arc::new(move |ctx, row| {
                if e5(ctx, row)?.0 {
                    return Ok(Value::Null);
                }
                match a5.alt.as_ref().and_then(|x| x.text.as_ref()) {
                    Some(t) => t(ctx, row),
                    None => Ok(Value::Null),
                }
            });
            f
        }),
        date_key: None,
        temporal: None,
        temporal_fsp: None,
        as_time: None,
    };
    let (quiet, strict_real) = (a.quiet, a.strict_real);
    let mut node = Node::new(ty, "nullif", move |ctx, row| {
        let (same, x) = equal(ctx, row)?;
        if same {
            return Ok(Value::Null);
        }
        if a.pred && !by_alt {
            Ok(x)
        } else {
            a.eval(ctx, row)
        }
    });
    node.alt = Some(Arc::new(alt));
    node.quiet = quiet;
    node.strict_real = strict_real;
    Ok(node)
}

/// `GREATEST(...)` and `LEAST(...)`: `NULL` if any argument is.
///
/// With a string among numbers MariaDB compares doubles and MySQL compares
/// strings. Among strings equal under the collation, `LEAST` keeps the
/// first; `GREATEST` keeps the first on MariaDB and the last on MySQL.
pub(crate) fn extreme(args: Vec<Node>, greatest: bool, env: Env<'_>) -> Result<Node> {
    let name = if greatest { "greatest" } else { "least" };
    let types: Vec<ExprType> = args.iter().map(|a| a.ty).collect();
    let live: Vec<&ExprType> = types.iter().filter(|t| t.kind != Kind::Null).collect();
    let is_str = |t: &&ExprType| t.kind == Kind::Str && !t.hex;
    let all_str = !live.is_empty() && live.iter().all(is_str);
    let any_str = live.iter().any(is_str);
    let mut all_temporal = !live.is_empty() && live.iter().all(|t| t.kind.is_temporal());
    // MariaDB compares dates with strings as dates, and answers a date.
    let dates_and_strings = env.maria()
        && !all_temporal
        && live.iter().any(|t| t.kind.is_temporal())
        && live.iter().all(|t| t.kind.is_temporal() || is_str(t));
    let nullable = types.iter().any(|t| t.nullable);
    let (ty, cmp) = if dates_and_strings {
        all_temporal = true;
        let temporal: Vec<ExprType> = types
            .iter()
            .filter(|t| t.kind.is_temporal())
            .copied()
            .collect();
        let refs: Vec<&ExprType> = temporal.iter().collect();
        (
            agg_result_type(&temporal, name, env)?.with_null(true),
            comparator(&refs, name, env)?,
        )
    } else if all_str || (any_str && !env.maria()) {
        let ty = agg_result_type(&types, name, env)?;
        check_branches(&args.iter().collect::<Vec<_>>(), &ty, name, env)?;
        let tys: Vec<&ExprType> = live.clone();
        let d = crate::types::aggregate_types(&tys, env.conn(), name, false)?;
        let coll = if d.deriv == Derivation::None {
            bin_collation(d.coll)
        } else {
            d.coll
        };
        (ty, Comparator::of(CmpKind::Str(coll)))
    } else if all_temporal {
        let refs: Vec<&ExprType> = types.iter().collect();
        (
            agg_result_type(&types, name, env)?,
            comparator(&refs, name, env)?,
        )
    } else if live.is_empty() {
        // Only NULLs: the NULL type on both servers.
        (ExprType::null(), Comparator::of(CmpKind::Real))
    } else if live.iter().any(|t| t.kind.is_real() || is_str(t))
        || (env.maria() && types.iter().any(|t| t.kind == Kind::Null))
    {
        // (A NULL among numbers makes MariaDB compare doubles.)
        let decs: Vec<u8> = live.iter().map(|t| super::arith::num_ty(t).dec).collect();
        let dec = if decs.iter().all(|d| *d < NOT_FIXED) {
            u32::from(decs.iter().copied().max().unwrap_or(0))
        } else {
            u32::from(NOT_FIXED)
        };
        (
            super::arith::double_ty(dec, nullable),
            Comparator::of(CmpKind::Real),
        )
    } else {
        let numeric_types: Vec<ExprType> = types
            .iter()
            .map(|t| {
                let n = super::arith::num_ty(t);
                if t.kind == Kind::Null {
                    *t
                } else if n.scale == 0 && t.kind != Kind::Decimal {
                    ExprType::int_prec(n.prec, n.unsigned).with_null(t.nullable)
                } else {
                    env.decimal(n.prec, n.scale, n.unsigned)
                        .with_null(t.nullable)
                }
            })
            .collect();
        let ty = agg_result_type(&numeric_types, name, env)?;
        let kind = if ty.kind == Kind::Decimal {
            CmpKind::Dec
        } else {
            CmpKind::Int
        };
        (ty, Comparator::of(kind))
    };
    let string_result = matches!(cmp.kind, CmpKind::Str(_));
    // Of equal arguments MySQL's GREATEST keeps the last (visible with
    // strings equal under a collation, and with 0 and -0).
    let later_wins = greatest && !env.maria();
    let coerce: Vec<CoerceFn> = args.iter().map(|a| coerce_fn(&a.ty, &ty, env)).collect();
    let take = move |ord: Ordering| {
        if greatest {
            ord == Ordering::Greater || (later_wins && ord == Ordering::Equal)
        } else {
            ord == Ordering::Less
        }
    };
    // MariaDB answers a reader that wants a double, a decimal or an
    // integer by comparing the arguments as that: `GREATEST('10', '9') + 0`
    // compares numbers though the strings compare the other way, and an
    // integer among doubles is read exactly.
    let int_result = ty.kind.is_int();
    let alt = if env.maria() && !all_temporal {
        let (a1, a2, a3) = (args.clone(), args.clone(), args.clone());
        Some(Arc::new(Alt {
            f64: Some(Arc::new(move |ctx, row| {
                let mut best: Option<f64> = None;
                for a in &a1 {
                    let Some(x) = a.f64(ctx, row)? else {
                        return Ok(None);
                    };
                    best = Some(match best {
                        Some(b) if (greatest && x <= b) || (!greatest && x >= b) => b,
                        _ => x,
                    });
                }
                Ok(best)
            })),
            dec: Some(Arc::new(move |ctx, row| {
                let mut best: Option<Dec> = None;
                for a in &a2 {
                    let Some(x) = a.dec(ctx, row)? else {
                        return Ok(None);
                    };
                    best = Some(match best {
                        Some(b) => {
                            let ord = x.cmp(&b);
                            let better = if greatest {
                                ord == Ordering::Greater
                            } else {
                                ord == Ordering::Less
                            };
                            if better { x } else { b }
                        }
                        None => x,
                    });
                }
                Ok(best)
            })),
            int: Some(Arc::new(move |ctx, row| {
                let mut best: Option<conv::IntVal> = None;
                for a in &a3 {
                    let Some(x) = a.int(ctx, row)? else {
                        return Ok(None);
                    };
                    best = Some(match best {
                        Some(b) => {
                            // Only an integer result knows which of its
                            // arguments are unsigned; any other compares
                            // the 64 bits as signed.
                            let ord = if int_result {
                                x.wide().cmp(&b.wide())
                            } else {
                                x.v.cmp(&b.v)
                            };
                            let (x, b) = if int_result {
                                (x, b)
                            } else {
                                (conv::IntVal::signed(x.v), conv::IntVal::signed(b.v))
                            };
                            let better = if greatest {
                                ord == Ordering::Greater
                            } else {
                                ord == Ordering::Less
                            };
                            if better { x } else { b }
                        }
                        None => x,
                    });
                }
                Ok(best)
            })),
            truth: None,
            text: None,
            date_key: None,
            temporal: None,
            temporal_fsp: None,
            as_time: None,
        }))
    } else {
        None
    };
    let mut node = Node::new(ty, name, move |ctx, row| {
        let mut best: Option<(Value, Key)> = None;
        for (i, a) in args.iter().enumerate() {
            // Each argument is converted once: the converted value is the
            // result.
            let own_text = a.alt.as_ref().and_then(|x| x.text.as_ref());
            let (v, k) = if let (true, Some(t)) = (string_result, own_text) {
                // (An argument that has a text of its own, as MySQL's
                // CASE has, is compared and returned as that.)
                let v = t(ctx, row)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                let Some(k) = cmp.key(ctx, &v, a.info(), &a.text, &a.ty)? else {
                    return Ok(Value::Null);
                };
                (v, k)
            } else if string_result || all_temporal {
                let v = a.eval(ctx, row)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                let Some(k) = cmp.key(ctx, &v, a.info(), &a.text, &a.ty)? else {
                    return Ok(Value::Null);
                };
                (coerce[i](ctx, v)?, k)
            } else {
                let Some(k) = cmp.node_key(ctx, a, row)? else {
                    return Ok(Value::Null);
                };
                let v = match &k {
                    Key::Str(s) => Value::Str(s.clone()),
                    Key::Int(i) => i.value(),
                    Key::Dec(d) => conv::dec_value(d),
                    Key::Real(x) => Value::Double(*x),
                    Key::Json(j) => Value::Json(crate::func::json::key_text(j)),
                };
                (v, k)
            };
            best = Some(match best {
                None => (v, k),
                Some((bv, bk)) => {
                    if take(cmp.order(&k, &bk).unwrap_or(Ordering::Equal)) {
                        (v, k)
                    } else {
                        (bv, bk)
                    }
                }
            });
        }
        Ok(best.map_or(Value::Null, |(v, _)| v))
    });
    node.alt = alt;
    Ok(hybrid(node, env))
}
