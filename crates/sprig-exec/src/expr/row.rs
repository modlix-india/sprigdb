//! Row values: `(a, b) <op> (c, d)` and `(a, b) [NOT] IN ((c, d), ...)`.
//! A row is the function `ROW` of its items.

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use sprig_engine::row::Value;

use super::cmp::{self, CmpKind, Comparator, Key, LeftKeys};
use super::{BinOp, Env, Expr, Node, build};
use crate::conv::IntVal;
use crate::error::{Result, SqlError};
use crate::num::Dec;
use crate::session::Ctx;
use crate::types::{ExprType, Kind};

/// Whether an expression is a row value.
pub(crate) fn is_row(e: &Expr) -> bool {
    matches!(e, Expr::Func(n, _) if n == "ROW")
}

fn items(e: &Expr) -> &[Expr] {
    match e {
        Expr::Func(n, args) if n == "ROW" => args,
        other => std::slice::from_ref(other),
    }
}

/// 1241: an operand of the wrong number of columns.
pub(crate) fn operand_columns(n: usize) -> SqlError {
    SqlError::new(
        1241,
        "21000",
        format!("Operand should contain {n} column(s)"),
    )
}

/// The error when a row meets a value of another shape: MariaDB calls a
/// row against a single value an illegal pair of types (4078) when the two
/// are the comparison's own operands; otherwise, and on MySQL, it is a
/// wrong column count (1241). The servers resolve every element first, so
/// an error in one of them (an illegal collation, a constant out of range)
/// comes before.
fn shape_error(a: &Expr, b: &Expr, op: &str, top: bool, env: Env<'_>) -> Result<SqlError> {
    if top && env.maria() && is_row(a) != is_row(b) {
        let name = |e: &Expr| if is_row(e) { "row" } else { "int" };
        return Ok(SqlError::new(
            4078,
            "HY000",
            format!(
                "Illegal parameter data types {} and {} for operation '{op}'",
                name(a),
                name(b)
            ),
        ));
    }
    Ok(operand_columns(items(a).len()))
}

/// Whether two operands have the same shape at every depth; `None` when
/// they do, else whether the first mismatch is between the operands
/// themselves (a row against a single value).
fn mismatch(a: &Expr, b: &Expr) -> Option<bool> {
    if !is_row(a) && !is_row(b) {
        return None;
    }
    if is_row(a) != is_row(b) {
        return Some(true);
    }
    if items(a).len() != items(b).len() {
        return Some(false);
    }
    items(a)
        .iter()
        .zip(items(b))
        .find_map(|(x, y)| mismatch(x, y).map(|_| false))
}

/// Before a shape error the servers resolve every element of the whole
/// comparison (MariaDB also computes its constants), so an error there
/// comes first.
fn resolve_all(sides: &[&Expr], env: Env<'_>) -> Result<()> {
    for side in sides {
        for leaf in leaves(side) {
            let n = build(leaf, env)?;
            if env.maria() && n.konst && n.lit.is_none() {
                env.at_prepare(|ctx| n.eval(ctx, &[]))?;
            }
        }
    }
    Ok(())
}

fn leaves(e: &Expr) -> Vec<&Expr> {
    if is_row(e) {
        items(e).iter().flat_map(leaves).collect()
    } else {
        vec![e]
    }
}

/// How the elements of a row decide the comparison.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// `=` and `<>`: an unequal element decides; a `NULL` one makes the
    /// answer `NULL` unless a later element is unequal.
    Eq,
    /// `<=>`: `NULL` equals `NULL`.
    NullSafe,
    /// `<`, `<=`, `>`, `>=`: the first unequal element decides; a `NULL`
    /// before it makes the answer `NULL`.
    Order,
}

type CmpFn = Arc<dyn Fn(&mut Ctx<'_>, &[Value]) -> Result<Option<Ordering>> + Send + Sync>;

/// A compiled comparison of two rows (or two values): how they are
/// ordered, `None` for unknown.
struct Compared {
    f: CmpFn,
    nullable: bool,
}

static NEXT_KEY: AtomicUsize = AtomicUsize::new(0);

/// A key kept as a value in the statement's constant store.
fn key_value(k: Option<&Key>) -> Option<Value> {
    Some(match k {
        None => Value::Null,
        Some(Key::Str(b)) => Value::Bytes(b.clone()),
        Some(Key::Int(i)) => i.value(),
        Some(Key::Dec(d)) => Value::Decimal(d.to_decimal()?),
        Some(Key::Real(x)) => Value::Double(*x),
        Some(Key::Json(j)) => Value::Json(crate::func::json::key_text(j)),
    })
}

fn value_key(v: &Value) -> Option<Key> {
    match v {
        Value::Bytes(b) => Some(Key::Str(b.clone())),
        Value::Int(i) => Some(Key::Int(IntVal::signed(*i))),
        Value::UInt(u) => Some(Key::Int(IntVal::unsigned(*u))),
        Value::Decimal(d) => Some(Key::Dec(Dec::from_decimal(d))),
        Value::Double(x) => Some(Key::Real(*x)),
        Value::Json(b) => crate::func::json::parse(b).map(Key::Json),
        _ => None,
    }
}

/// An element's comparison key. A constant element is converted once per
/// statement, as both servers convert a constant compared with a row's
/// value once.
pub(crate) fn element_key(
    c: &Comparator,
    n: &Node,
    id: Option<usize>,
    ctx: &mut Ctx<'_>,
    row: &[Value],
) -> Result<Option<Key>> {
    let Some(id) = id else {
        return c.node_key(ctx, n, row);
    };
    if let Some(v) = ctx.consts.get(&id) {
        return Ok(value_key(v));
    }
    let k = c.node_key(ctx, n, row)?;
    if let Some(v) = key_value(k.as_ref()) {
        ctx.consts.insert(id, v);
    }
    Ok(k)
}

pub(crate) fn const_id(n: &Node) -> Option<usize> {
    (n.konst && !n.sized).then(|| usize::MAX / 2 - NEXT_KEY.fetch_add(1, AtomicOrdering::Relaxed))
}

/// MariaDB computes a row's constant elements once while it prepares the
/// statement (an error there fails it, whatever the rows),
/// and again when they compare.
fn prepare_eval(n: &Node, env: Env<'_>) -> Result<()> {
    // (MySQL does so only now and then, with no rule found: see the
    // decision on constant folding.)
    if env.maria() && n.konst && n.lit.is_none() {
        env.at_prepare(|ctx| n.eval(ctx, &[]))?;
    }
    Ok(())
}

fn compared(
    a: &Expr,
    b: &Expr,
    op: BinOp,
    mode: Mode,
    top: bool,
    env: Env<'_>,
) -> Result<Compared> {
    if is_row(a) || is_row(b) {
        if !is_row(a) || !is_row(b) || items(a).len() != items(b).len() {
            return Err(shape_error(a, b, op.sql(), top, env)?);
        }
        let parts = items(a)
            .iter()
            .zip(items(b))
            .map(|(x, y)| compared(x, y, op, mode, false, env))
            .collect::<Result<Vec<_>>>()?;
        let nullable = parts.iter().any(|p| p.nullable);
        let fs: Vec<CmpFn> = parts.into_iter().map(|p| p.f).collect();
        let f: CmpFn = Arc::new(move |ctx, row| {
            let mut unknown = false;
            for f in &fs {
                match f(ctx, row)? {
                    Some(Ordering::Equal) => {}
                    Some(o) => return Ok(Some(o)),
                    None if mode == Mode::Order => return Ok(None),
                    None => unknown = true,
                }
            }
            Ok(if unknown { None } else { Some(Ordering::Equal) })
        });
        return Ok(Compared { f, nullable });
    }
    let (x, y) = cmp::fold_for_column(build(a, env)?, build(b, env)?, env);
    prepare_eval(&x, env)?;
    prepare_eval(&y, env)?;
    let c = cmp::comparator2(&x, &y, op.sql(), env)?;
    let nullable = x.ty.nullable || y.ty.nullable;
    let (ix, iy) = (const_id(&x), const_id(&y));
    let f: CmpFn = Arc::new(move |ctx, row| {
        let kx = element_key(&c, &x, ix, ctx, row)?;
        let ky = element_key(&c, &y, iy, ctx, row)?;
        Ok(match (kx, ky) {
            (Some(kx), Some(ky)) => c.order(&kx, &ky),
            (None, None) if mode == Mode::NullSafe => Some(Ordering::Equal),
            (None, _) if mode == Mode::NullSafe => Some(Ordering::Less),
            (_, None) if mode == Mode::NullSafe => Some(Ordering::Greater),
            _ => None,
        })
    });
    Ok(Compared { f, nullable })
}

/// `row <op> row` for the comparison operators.
pub(crate) fn compare(op: BinOp, a: &Expr, b: &Expr, env: Env<'_>) -> Result<Node> {
    let mode = match op {
        BinOp::Eq | BinOp::Ne => Mode::Eq,
        BinOp::NullSafeEq => Mode::NullSafe,
        _ => Mode::Order,
    };
    if mismatch(a, b).is_some() {
        resolve_all(&[a, b], env)?;
    }
    let c = compared(a, b, op, mode, true, env)?;
    let nullable = c.nullable && mode != Mode::NullSafe;
    let f = c.f;
    let ty = ExprType::boolean().with_null(nullable);
    Ok(Node::new(ty, "row", move |ctx, row| {
        let o = f(ctx, row)?;
        let hit = match (op, o) {
            (_, None) => return Ok(Value::Null),
            (BinOp::Eq | BinOp::NullSafeEq, Some(o)) => o == Ordering::Equal,
            (BinOp::Ne, Some(o)) => o != Ordering::Equal,
            (BinOp::Lt, Some(o)) => o == Ordering::Less,
            (BinOp::Le, Some(o)) => o != Ordering::Greater,
            (BinOp::Gt, Some(o)) => o == Ordering::Greater,
            (_, Some(o)) => o != Ordering::Less,
        };
        Ok(Value::Int(i64::from(hit)))
    }))
}

/// MariaDB's comparator for one position of a list of rows.
fn position_comparator(tys: &[&ExprType], env: Env<'_>) -> Result<Comparator> {
    let kinds: Vec<Kind> = tys
        .iter()
        .map(|t| t.kind)
        .filter(|k| *k != Kind::Null)
        .collect();
    let numberish = |k: &Kind| k.is_numeric();
    if kinds.contains(&Kind::Str) && kinds.iter().any(numberish) {
        return Ok(Comparator::of(CmpKind::Real));
    }
    // A single string among NULLs keeps its own collation.
    let strings: Vec<&&ExprType> = tys.iter().filter(|t| t.kind == Kind::Str).collect();
    if strings.len() == 1 && kinds.len() == 1 {
        return Ok(Comparator::of(CmpKind::Str(strings[0].coll)));
    }
    cmp::comparator_n(tys, "in", false, env)
}

/// The answer of `IN` from its items' outcomes.
fn in_answer(found: bool, unknown: bool, not: bool) -> Value {
    if found {
        Value::Int(i64::from(!not))
    } else if unknown {
        Value::Null
    } else {
        Value::Int(i64::from(not))
    }
}

/// `row [NOT] IN (row, ...)`: true when an item equals the row; `NULL`
/// when none does but one could (a `NULL` element); false otherwise.
///
/// The row on the left is evaluated once and each of its elements is
/// converted once per comparison type. MariaDB compares each element with
/// an item's in the type the pair chooses; MySQL, for a list of more than
/// one item, in the left element's own type (a list of one is `=`).
pub(crate) fn in_list(a: &Expr, list: &[Expr], not: bool, env: Env<'_>) -> Result<Node> {
    // Shapes: the items against the row first, then inside them.
    let shapes: Vec<Option<bool>> = list.iter().map(|item| mismatch(a, item)).collect();
    if shapes.iter().any(Option::is_some) {
        let mut sides = vec![a];
        sides.extend(list.iter());
        resolve_all(&sides, env)?;
        // The first item that is no row, or a row of another size.
        if let Some(item) = list
            .iter()
            .find(|item| !is_row(item) || items(item).len() != items(a).len())
        {
            return Err(shape_error(a, item, "in", true, env)?);
        }
    }
    let flat = is_row(a)
        && !items(a).iter().any(is_row)
        && list
            .iter()
            .all(|r| is_row(r) && items(r).len() == items(a).len() && !items(r).iter().any(is_row));
    if !flat || (!env.maria() && list.len() == 1) {
        let parts = list
            .iter()
            .map(|item| compared(a, item, BinOp::Eq, Mode::Eq, true, env))
            .collect::<Result<Vec<_>>>()?;
        let nullable = parts.iter().any(|p| p.nullable);
        let fs: Vec<CmpFn> = parts.into_iter().map(|p| p.f).collect();
        let ty = ExprType::boolean().with_null(nullable);
        return Ok(Node::new(ty, "row in", move |ctx, row| {
            let mut unknown = false;
            for f in &fs {
                match f(ctx, row)? {
                    Some(Ordering::Equal) => return Ok(in_answer(true, false, not)),
                    Some(_) => {}
                    None => unknown = true,
                }
            }
            Ok(in_answer(false, unknown, not))
        }));
    }
    let left = items(a)
        .iter()
        .map(|e| build(e, env))
        .collect::<Result<Vec<_>>>()?;
    for l in &left {
        prepare_eval(l, env)?;
    }
    let mut rows: Vec<Vec<(Node, Comparator, Option<usize>)>> = Vec::with_capacity(list.len());
    let mut nullable = left.iter().any(|n| n.ty.nullable);
    for item in list {
        let mut cols = Vec::with_capacity(left.len());
        for (l, e) in left.iter().zip(items(item)) {
            let r = build(e, env)?;
            prepare_eval(&r, env)?;
            nullable |= r.ty.nullable;
            let c = if env.maria() {
                // (Chosen per position below.)
                Comparator::of(CmpKind::Real)
            } else {
                // The left element's own type.
                let mut c = cmp::comparator2(l, l, "in", env)?;
                if l.ty.kind == Kind::Null {
                    c.kind = CmpKind::Str(env.conn());
                }
                c
            };
            let id = const_id(&r);
            cols.push((r, c, id));
        }
        rows.push(cols);
    }
    if env.maria() {
        // MariaDB compares each position in one type for the whole list:
        // strings when all are strings, integers or decimals when all are
        // exact numbers, doubles when strings meet numbers.
        for (i, l) in left.iter().enumerate() {
            let mut tys: Vec<&ExprType> = vec![&l.ty];
            tys.extend(rows.iter().map(|r| &r[i].0.ty));
            let c = position_comparator(&tys, env)?;
            for r in &mut rows {
                r[i].1 = c;
            }
        }
    }
    let ty = ExprType::boolean().with_null(nullable);
    Ok(Node::new(ty, "row in", move |ctx, row| {
        let mut vals = Vec::with_capacity(left.len());
        for l in &left {
            vals.push(l.eval(ctx, row)?);
        }
        let mut keys: Vec<LeftKeys<'_>> = left.iter().map(|_| LeftKeys::new(row)).collect();
        // The left row is converted at once, position by position.
        if let Some(first) = rows.first() {
            for (i, (_, c, _)) in first.iter().enumerate() {
                if !vals[i].is_null() {
                    keys[i].get(ctx, c, &vals[i], &left[i])?;
                }
            }
        }
        let mut unknown = false;
        'items: for cols in &rows {
            let mut item_unknown = false;
            for (i, (r, c, id)) in cols.iter().enumerate() {
                let kl = if vals[i].is_null() {
                    None
                } else {
                    keys[i].get(ctx, c, &vals[i], &left[i])?
                };
                let kr = element_key(c, r, *id, ctx, row)?;
                match (kl, kr) {
                    (Some(kl), Some(kr)) => match c.order(&kl, &kr) {
                        Some(Ordering::Equal) => {}
                        Some(_) => continue 'items,
                        None => item_unknown = true,
                    },
                    _ => item_unknown = true,
                }
            }
            if item_unknown {
                unknown = true;
            } else {
                return Ok(in_answer(true, false, not));
            }
        }
        Ok(in_answer(false, unknown, not))
    }))
}
