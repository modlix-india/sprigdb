//! Helpers over the executor's expression tree: walking, rewriting column
//! slots, splitting conjunctions, and telling constants from row-dependent
//! expressions.

use std::collections::BTreeSet;

use sprig_engine::keys::collation::Collation;
use sprig_engine::row::ColumnType;
use sprig_exec::{BinOp, ColumnRef, Expr, ExprType, Kind};

/// Functions whose value differs from call to call: an expression using
/// one is never treated as a constant.
const VOLATILE: &[&str] = &["RAND", "UUID", "UUID_SHORT", "SYSDATE", "RANDOM_BYTES"];

/// Calls `f` on the direct sub-expressions of `e`.
pub(crate) fn for_children<'a>(e: &'a Expr, f: &mut dyn FnMut(&'a Expr)) {
    match e {
        Expr::Neg(a)
        | Expr::Not(a)
        | Expr::BitNot(a)
        | Expr::IsNull(a, _)
        | Expr::IsTruth(a, _, _)
        | Expr::Cast(a, _)
        | Expr::Collate(a, _) => f(a),
        Expr::Binary(_, a, b) => {
            f(a);
            f(b);
        }
        Expr::In(a, list, _) => {
            f(a);
            list.iter().for_each(f);
        }
        Expr::Between(a, b, c, _) => {
            f(a);
            f(b);
            f(c);
        }
        Expr::Like(a, b, c, _) => {
            f(a);
            f(b);
            if let Some(c) = c {
                f(c);
            }
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => {
            if let Some(o) = operand {
                f(o);
            }
            for (w, t) in whens {
                f(w);
                f(t);
            }
            if let Some(o) = otherwise {
                f(o);
            }
        }
        Expr::Func(_, args) => args.iter().for_each(f),
        _ => {}
    }
}

/// The expression with `f` applied top-down: where `f` gives a
/// replacement the sub-tree is replaced (and not looked into), elsewhere
/// the children are rewritten.
pub(crate) fn rewrite(e: &Expr, f: &mut dyn FnMut(&Expr) -> Option<Expr>) -> Expr {
    if let Some(r) = f(e) {
        return r;
    }
    let mut b = |x: &Expr| Box::new(rewrite(x, f));
    match e {
        Expr::Neg(a) => Expr::Neg(b(a)),
        Expr::Not(a) => Expr::Not(b(a)),
        Expr::BitNot(a) => Expr::BitNot(b(a)),
        Expr::IsNull(a, n) => Expr::IsNull(b(a), *n),
        Expr::IsTruth(a, t, n) => Expr::IsTruth(b(a), *t, *n),
        Expr::Cast(a, t) => Expr::Cast(b(a), *t),
        Expr::Collate(a, c) => Expr::Collate(b(a), *c),
        Expr::Binary(op, x, y) => Expr::Binary(*op, b(x), b(y)),
        Expr::In(a, list, n) => {
            let a = b(a);
            Expr::In(a, list.iter().map(|x| rewrite(x, f)).collect(), *n)
        }
        Expr::Between(a, lo, hi, n) => Expr::Between(b(a), b(lo), b(hi), *n),
        Expr::Like(a, p, esc, n) => {
            let (a, p) = (b(a), b(p));
            Expr::Like(a, p, esc.as_ref().map(|x| Box::new(rewrite(x, f))), *n)
        }
        Expr::Case {
            operand,
            whens,
            otherwise,
        } => Expr::Case {
            operand: operand.as_ref().map(|x| Box::new(rewrite(x, f))),
            whens: whens
                .iter()
                .map(|(w, t)| (rewrite(w, f), rewrite(t, f)))
                .collect(),
            otherwise: otherwise.as_ref().map(|x| Box::new(rewrite(x, f))),
        },
        Expr::Func(name, args) => {
            Expr::Func(name.clone(), args.iter().map(|x| rewrite(x, f)).collect())
        }
        other => other.clone(),
    }
}

/// Adds the slots `e` reads to `out`.
pub(crate) fn collect_slots(e: &Expr, out: &mut BTreeSet<usize>) {
    if let Expr::Column(c) = e {
        out.insert(c.slot);
    }
    for_children(e, &mut |x| collect_slots(x, out));
}

/// The slots `e` reads.
pub(crate) fn slots(e: &Expr) -> BTreeSet<usize> {
    let mut out = BTreeSet::new();
    collect_slots(e, &mut out);
    out
}

/// Whether `e` calls a function whose value changes between calls.
pub(crate) fn is_volatile(e: &Expr) -> bool {
    if let Expr::Func(name, _) = e
        && VOLATILE.contains(&name.as_str())
    {
        return true;
    }
    let mut any = false;
    for_children(e, &mut |x| any |= is_volatile(x));
    any
}

/// Whether `e` has the same value for every row of the statement.
pub(crate) fn is_const(e: &Expr) -> bool {
    slots(e).is_empty() && !is_volatile(e)
}

/// The expression with every column slot passed through `map`.
pub(crate) fn remap(e: &Expr, map: &dyn Fn(usize) -> usize) -> Expr {
    rewrite(e, &mut |x| match x {
        Expr::Column(c) => Some(Expr::Column(ColumnRef {
            slot: map(c.slot),
            ..c.clone()
        })),
        _ => None,
    })
}

/// The parts of a conjunction: `a AND (b AND c)` gives `a`, `b`, `c`.
pub(crate) fn conjuncts(e: Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::Binary(BinOp::And, a, b) => {
            conjuncts(*a, out);
            conjuncts(*b, out);
        }
        other => out.push(other),
    }
}

/// The conjunction of `parts`; `None` when there are none.
pub(crate) fn and_all(parts: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    parts
        .into_iter()
        .reduce(|a, b| Expr::binary(BinOp::And, a, b))
}

/// The column type whose values a computed expression of type `ty` reads
/// back as, for a slot of an intermediate row (an aggregate's result, a
/// derived table's column).
pub(crate) fn slot_type(ty: &ExprType) -> ColumnType {
    match ty.kind {
        Kind::Null => ColumnType::VarBinary { bytes: 0 },
        Kind::Int => ColumnType::Int {
            bytes: if ty.width == 0 { 8 } else { ty.width },
            unsigned: ty.unsigned,
        },
        Kind::Year => ColumnType::Year,
        Kind::Bit => ColumnType::Bit { bits: 64 },
        Kind::Decimal => {
            let scale = ty.dec.min(38);
            ColumnType::Decimal {
                precision: ty.prec.clamp(scale.max(1), 65),
                scale,
            }
        }
        Kind::Double => ColumnType::Double,
        Kind::Float => ColumnType::Float,
        Kind::Str => {
            if ty.coll == Collation::Binary {
                ColumnType::VarBinary { bytes: ty.len }
            } else {
                ColumnType::VarChar {
                    chars: ty.len,
                    collation: ty.coll,
                }
            }
        }
        Kind::Date => ColumnType::Date,
        Kind::Time => ColumnType::Time { fsp: ty.dec.min(6) },
        Kind::DateTime | Kind::Timestamp => ColumnType::DateTime { fsp: ty.dec.min(6) },
    }
}

/// A reference to slot `slot` of an intermediate row holding values of
/// type `ty`.
pub(crate) fn slot_ref(slot: usize, name: &str, ty: &ExprType) -> Expr {
    Expr::Column(ColumnRef {
        slot,
        ty: slot_type(ty),
        nullable: ty.nullable,
        name: name.to_owned(),
    })
}
