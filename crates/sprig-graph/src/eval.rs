//! The gateway evaluator: Cypher expressions
//! over one binding row, with three-valued logic, `null` propagation and
//! the function library (scalar, list, string and math functions; the
//! aggregates are in [`crate::exec`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use sprig_common::{Result, SprigCode, SprigError};

use crate::ast::{BinOp, Expr, MapItem, UnOp};
use crate::value::{Value, fmt_float, type_error};

/// One binding row: variable names and their values, as slots in the
/// order they were bound. Names are shared between the copies of a row, so
/// copying a row (once per relationship a hop finds) copies its values and
/// bumps its names' counts with one allocation, where a hash map copied
/// every name; lookups compare a few short names.
#[derive(Clone, Default)]
pub struct Row {
    slots: Vec<(Arc<String>, Value)>,
}

impl Row {
    /// An empty row with room for `n` variables.
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            slots: Vec::with_capacity(n),
        }
    }

    fn find(&self, k: &str) -> Option<usize> {
        self.slots.iter().position(|(n, _)| n.as_str() == k)
    }

    /// The value of `k`.
    #[must_use]
    pub fn get(&self, k: &str) -> Option<&Value> {
        self.slots
            .iter()
            .find(|(n, _)| n.as_str() == k)
            .map(|(_, v)| v)
    }

    /// The value of `k`, to change.
    pub fn get_mut(&mut self, k: &str) -> Option<&mut Value> {
        self.slots
            .iter_mut()
            .find(|(n, _)| n.as_str() == k)
            .map(|(_, v)| v)
    }

    /// Whether `k` is bound.
    #[must_use]
    pub fn contains_key(&self, k: &str) -> bool {
        self.find(k).is_some()
    }

    /// Binds `k`, giving back its former value.
    pub fn insert(&mut self, k: String, v: Value) -> Option<Value> {
        if let Some(i) = self.find(&k) {
            return Some(std::mem::replace(&mut self.slots[i].1, v));
        }
        self.slots.push((Arc::new(k), v));
        None
    }

    /// Binds `k` by a shared name (no allocation for the name).
    pub fn insert_name(&mut self, k: &Arc<String>, v: Value) -> Option<Value> {
        if let Some(i) = self.find(k) {
            return Some(std::mem::replace(&mut self.slots[i].1, v));
        }
        self.slots.push((Arc::clone(k), v));
        None
    }

    /// Unbinds `k`, giving back its value.
    pub fn remove(&mut self, k: &str) -> Option<Value> {
        self.find(k).map(|i| self.slots.remove(i).1)
    }

    /// The bound names, in binding order.
    pub fn keys(&self) -> impl Iterator<Item = &String> + '_ {
        self.slots.iter().map(|(n, _)| &**n)
    }

    /// The values, in binding order.
    pub fn values(&self) -> impl Iterator<Item = &Value> + '_ {
        self.slots.iter().map(|(_, v)| v)
    }

    /// The values, to change.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Value> + '_ {
        self.slots.iter_mut().map(|(_, v)| v)
    }

    /// Names and values.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> + '_ {
        self.slots.iter().map(|(n, v)| (&**n, v))
    }

    /// Names and values, to change.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Value)> + '_ {
        self.slots.iter_mut().map(|(n, v)| (&**n, v))
    }

    /// Keeps the bindings `f` keeps.
    pub fn retain(&mut self, mut f: impl FnMut(&String, &mut Value) -> bool) {
        self.slots.retain_mut(|(n, v)| f(n, v));
    }

    /// Variables bound.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether nothing is bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

impl PartialEq for Row {
    /// The same bindings, in any order.
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().all(|(k, v)| other.get(k) == Some(v))
    }
}

impl std::fmt::Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl FromIterator<(String, Value)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, Value)>>(it: I) -> Self {
        let mut r = Row::default();
        r.extend(it);
        r
    }
}

impl Extend<(String, Value)> for Row {
    fn extend<I: IntoIterator<Item = (String, Value)>>(&mut self, it: I) {
        for (k, v) in it {
            self.insert(k, v);
        }
    }
}

impl IntoIterator for Row {
    type Item = (String, Value);
    type IntoIter = std::iter::Map<
        std::vec::IntoIter<(Arc<String>, Value)>,
        fn((Arc<String>, Value)) -> (String, Value),
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.slots
            .into_iter()
            .map(|(n, v)| (Arc::try_unwrap(n).unwrap_or_else(|a| (*a).clone()), v))
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (Arc<String>, Value)>,
        fn(&'a (Arc<String>, Value)) -> (&'a String, &'a Value),
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.slots.iter().map(|(n, v)| (&**n, v))
    }
}

/// The hasher of [`Row`]s' keys: variable names of the statement, short
/// and the statement's own, so a multiply-rotate hash (as rustc's `FxHash`)
/// instead of `SipHash`, which the gateway spent a tenth of a large read's
/// time in.
#[derive(Debug, Default, Clone, Copy)]
pub struct RowHasher(u64);

impl std::hash::Hasher for RowHasher {
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x51_7c_c1_b7_27_22_0a_95;
        let mut h = self.0;
        let mut it = bytes.chunks_exact(8);
        for c in &mut it {
            let mut w = [0u8; 8];
            w.copy_from_slice(c);
            h = (h.rotate_left(5) ^ u64::from_le_bytes(w)).wrapping_mul(K);
        }
        for &b in it.remainder() {
            h = (h.rotate_left(5) ^ u64::from(b)).wrapping_mul(K);
        }
        self.0 = h;
    }

    fn write_u8(&mut self, i: u8) {
        self.write(&[i]);
    }

    fn write_usize(&mut self, i: usize) {
        self.write(&i.to_le_bytes());
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// [`RowHasher`]'s builder.
pub type RowState = std::hash::BuildHasherDefault<RowHasher>;

/// Statement parameters.
pub type Params = BTreeMap<String, Value>;

fn unsupported(what: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::GraphUnsupported,
        format!("{what} is not supported"),
    )
}

fn arith_err(msg: &str) -> SprigError {
    // Neo4j's code for arithmetic errors (39909 in SQL).
    SprigError::new(
        SprigCode::GraphPropertyType,
        format!("[Neo.ClientError.Statement.ArithmeticError] {msg}"),
    )
}

/// Evaluates `e` over `row`.
///
/// # Errors
/// 39909 for type errors, 39901 for unknown functions, 39900 for unknown
/// variables and parameters.
pub fn eval(e: &Expr, row: &Row, params: &Params) -> Result<Value> {
    eval_in(e, row, params)
}

/// What an expression's variables are bound to: a [`Row`], or another
/// row form (the operator planner's slot rows).
pub trait Vars {
    /// The value of variable `name`.
    fn var(&self, name: &str) -> Option<&Value>;
}

impl Vars for Row {
    fn var(&self, name: &str) -> Option<&Value> {
        self.get(name)
    }
}

/// One local variable over an outer scope (a comprehension's).
struct Scope<'a> {
    outer: &'a dyn Vars,
    name: &'a str,
    value: Value,
}

impl<'a> Scope<'a> {
    fn new(outer: &'a dyn Vars, name: &'a str, value: Value) -> Self {
        Self { outer, name, value }
    }

    fn into_value(self) -> Value {
        self.value
    }
}

impl Vars for Scope<'_> {
    fn var(&self, name: &str) -> Option<&Value> {
        if name == self.name {
            Some(&self.value)
        } else {
            self.outer.var(name)
        }
    }
}

/// Evaluates `e` over the variables `row` holds ([`eval`] over any
/// [`Vars`], such as the operator planner's slot rows).
///
/// # Errors
/// 39909 for type errors, 39901 for unknown functions, 39900 for unknown
/// variables and parameters.
pub fn eval_in(e: &Expr, row: &dyn Vars, params: &Params) -> Result<Value> {
    Ok(match e {
        Expr::Lit(v) => v.clone(),
        Expr::Param(p) => params.get(p).cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::GraphSyntax,
                format!("Expected parameter(s): {p}"),
            )
            .with_neo4j("Neo.ClientError.Statement.ParameterMissing")
        })?,
        Expr::Var(v) | Expr::Slot(v) => row.var(v).cloned().ok_or_else(|| {
            SprigError::new(
                SprigCode::GraphSyntax,
                format!("Variable `{v}` not defined"),
            )
        })?,
        Expr::Prop(b, k) => prop(&eval_in(b, row, params)?, k)?,
        Expr::Index(b, i) => index(&eval_in(b, row, params)?, &eval_in(i, row, params)?)?,
        Expr::Slice(b, lo, hi) => {
            let v = eval_in(b, row, params)?;
            let lo = lo.as_ref().map(|x| eval_in(x, row, params)).transpose()?;
            let hi = hi.as_ref().map(|x| eval_in(x, row, params)).transpose()?;
            slice(&v, lo.as_ref(), hi.as_ref())?
        }
        Expr::List(l) => Value::List(
            l.iter()
                .map(|x| eval_in(x, row, params))
                .collect::<Result<_>>()?,
        ),
        Expr::Map(m) => Value::Map(
            m.iter()
                .map(|(k, x)| Ok((k.clone(), eval_in(x, row, params)?)))
                .collect::<Result<_>>()?,
        ),
        Expr::Unary(op, x) => unary(*op, eval_in(x, row, params)?)?,
        Expr::Binary(BinOp::And, a, b) => {
            let l = eval_in(a, row, params)?.truth()?;
            if l == Some(false) {
                return Ok(Value::Bool(false));
            }
            let r = eval_in(b, row, params)?.truth()?;
            tri(match (l, r) {
                (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            })
        }
        Expr::Binary(BinOp::Or, a, b) => {
            let l = eval_in(a, row, params)?.truth()?;
            if l == Some(true) {
                return Ok(Value::Bool(true));
            }
            let r = eval_in(b, row, params)?.truth()?;
            tri(match (l, r) {
                (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            })
        }
        Expr::Binary(op, a, b) => binary(*op, eval_in(a, row, params)?, eval_in(b, row, params)?)?,
        Expr::IsNull(x, neg) => Value::Bool(eval_in(x, row, params)?.is_null() != *neg),
        Expr::HasLabels(x, l) => match eval_in(x, row, params)? {
            Value::Null => Value::Null,
            Value::Node(n) => Value::Bool(l.matches(&n.labels)),
            other => {
                return Err(type_error(format!(
                    "label check on a {}",
                    other.type_name()
                )));
            }
        },
        Expr::Case {
            subject,
            whens,
            else_,
        } => {
            let s = subject
                .as_ref()
                .map(|x| eval_in(x, row, params))
                .transpose()?;
            for (w, t) in whens {
                let hit = match &s {
                    Some(sv) => sv.cypher_eq(&eval_in(w, row, params)?) == Some(true),
                    None => eval_in(w, row, params)?.truth()? == Some(true),
                };
                if hit {
                    return eval_in(t, row, params);
                }
            }
            match else_ {
                Some(x) => eval_in(x, row, params)?,
                None => Value::Null,
            }
        }
        Expr::ListComp {
            var,
            list,
            pred,
            map,
        } => {
            let l = match eval_in(list, row, params)? {
                Value::Null => return Ok(Value::Null),
                Value::List(l) => l,
                other => return Err(type_error(format!("IN over a {}", other.type_name()))),
            };
            let mut out = Vec::new();
            for x in l {
                let inner = Scope::new(row, var, x);
                if let Some(p) = pred
                    && eval_in(p, &inner, params)?.truth()? != Some(true)
                {
                    continue;
                }
                out.push(match map {
                    Some(m) => eval_in(m, &inner, params)?,
                    None => inner.into_value(),
                });
            }
            Value::List(out)
        }
        Expr::Reduce {
            acc,
            init,
            var,
            list,
            expr,
        } => {
            let start = eval_in(init, row, params)?;
            let l = match eval_in(list, row, params)? {
                Value::Null => return Ok(Value::Null),
                Value::List(l) => l,
                other => return Err(type_error(format!("IN over a {}", other.type_name()))),
            };
            let mut a = start;
            for x in l {
                let outer = Scope::new(row, acc, a);
                let inner = Scope::new(&outer, var, x);
                a = eval_in(expr, &inner, params)?;
            }
            a
        }
        Expr::Quantifier {
            kind,
            var,
            list,
            pred,
        } => {
            let l = match eval_in(list, row, params)? {
                Value::Null => return Ok(Value::Null),
                Value::List(l) => l,
                other => return Err(type_error(format!("IN over a {}", other.type_name()))),
            };
            let (mut t, mut f, mut n) = (0usize, 0usize, 0usize);
            for x in l {
                let inner = Scope::new(row, var, x);
                match eval_in(pred, &inner, params)?.truth()? {
                    Some(true) => t += 1,
                    Some(false) => f += 1,
                    None => n += 1,
                }
            }
            tri(match kind.as_str() {
                "all" => {
                    if f > 0 {
                        Some(false)
                    } else if n > 0 {
                        None
                    } else {
                        Some(true)
                    }
                }
                "any" => {
                    if t > 0 {
                        Some(true)
                    } else if n > 0 {
                        None
                    } else {
                        Some(false)
                    }
                }
                "none" => {
                    if t > 0 {
                        Some(false)
                    } else if n > 0 {
                        None
                    } else {
                        Some(true)
                    }
                }
                _ => {
                    if n > 0 {
                        None
                    } else {
                        Some(t == 1)
                    }
                }
            })
        }
        Expr::Func {
            name,
            distinct: _,
            args,
        } => {
            if crate::ast::is_aggregate(name) {
                return Err(SprigError::new(
                    SprigCode::GraphSyntax,
                    format!("aggregate {name}() is not allowed here"),
                ));
            }
            let vals = args
                .iter()
                .map(|x| eval_in(x, row, params))
                .collect::<Result<Vec<_>>>()?;
            call(name, vals)?
        }
        Expr::CountStar => {
            return Err(SprigError::new(
                SprigCode::GraphSyntax,
                "count(*) is not allowed here",
            ));
        }
        Expr::Pattern(_) | Expr::Subquery(..) => {
            return Err(SprigError::new(
                SprigCode::GraphUnsupported,
                "a pattern or subquery in this position is not supported yet",
            ));
        }
        Expr::MapProjection(b, items) => {
            let base = eval_in(b, row, params)?;
            match &base {
                Value::Null => return Ok(Value::Null),
                Value::Node(_) | Value::Rel(_) | Value::Map(_) => {}
                other => {
                    return Err(type_error(format!(
                        "a map projection of a {}",
                        other.type_name()
                    )));
                }
            }
            // A late-read element gives only the keys asked for.
            let one = |k: &str| -> Result<Value> {
                match &base {
                    Value::Node(n) => n.prop(k),
                    Value::Rel(r) => r.prop(k),
                    Value::Map(m) => Ok(m.get(k).cloned().unwrap_or(Value::Null)),
                    _ => Ok(Value::Null),
                }
            };
            let mut out = BTreeMap::new();
            for it in items {
                match it {
                    MapItem::All => out.extend(match &base {
                        Value::Node(n) => n.all_props()?.clone(),
                        Value::Rel(r) => r.all_props()?.clone(),
                        Value::Map(m) => m.clone(),
                        _ => BTreeMap::new(),
                    }),
                    MapItem::Prop(k) => {
                        out.insert(k.clone(), one(k)?);
                    }
                    MapItem::Lit(k, e) => {
                        out.insert(k.clone(), eval_in(e, row, params)?);
                    }
                    MapItem::Var(v) => {
                        out.insert(v.clone(), eval_in(&Expr::Var(v.clone()), row, params)?);
                    }
                }
            }
            Value::Map(out)
        }
    })
}

/// Extension: attach a Neo4j status code to an error's message, for codes
/// the SprigDB code doesn't decide (parameter missing and the like).
trait WithNeo4j {
    fn with_neo4j(self, code: &str) -> Self;
}

impl WithNeo4j for SprigError {
    fn with_neo4j(mut self, code: &str) -> Self {
        self.message = format!("[{code}] {}", self.message);
        self
    }
}

fn tri(b: Option<bool>) -> Value {
    b.map_or(Value::Null, Value::Bool)
}

fn prop(v: &Value, k: &str) -> Result<Value> {
    Ok(match v {
        Value::Null => Value::Null,
        Value::Node(n) => n.prop(k)?,
        Value::Rel(r) => r.prop(k)?,
        Value::Map(m) => m.get(k).cloned().unwrap_or(Value::Null),
        other => match crate::temporal::property(other, k)? {
            Some(v) => v,
            None => {
                return Err(type_error(format!(
                    "Type mismatch: expected a map, node or relationship but was {} (property '{k}')",
                    other.type_name()
                )));
            }
        },
    })
}

fn as_index(i: &Value, len: usize) -> Result<Option<usize>> {
    let Value::Int(i) = i else {
        return Err(type_error("a list index must be an INTEGER"));
    };
    let len = i64::try_from(len).unwrap_or(i64::MAX);
    let j = if *i < 0 { len + i } else { *i };
    Ok(if (0..len).contains(&j) {
        usize::try_from(j).ok()
    } else {
        None
    })
}

fn index(v: &Value, i: &Value) -> Result<Value> {
    Ok(match (v, i) {
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::List(l), i) => as_index(i, l.len())?.map_or(Value::Null, |j| l[j].clone()),
        (Value::Map(_) | Value::Node(_) | Value::Rel(_), Value::String(k)) => prop(v, k)?,
        (other, _) => return Err(type_error(format!("can't index a {}", other.type_name()))),
    })
}

fn clamp(i: Option<&Value>, len: usize, default: usize) -> Result<Option<usize>> {
    let len_i = i64::try_from(len).unwrap_or(i64::MAX);
    match i {
        None => Ok(Some(default)),
        Some(Value::Null) => Ok(None),
        Some(Value::Int(i)) => {
            let j = if *i < 0 { len_i + i } else { *i };
            Ok(Some(usize::try_from(j.clamp(0, len_i)).unwrap_or(0)))
        }
        Some(_) => Err(type_error("a slice bound must be an INTEGER")),
    }
}

fn slice(v: &Value, lo: Option<&Value>, hi: Option<&Value>) -> Result<Value> {
    let Value::List(l) = v else {
        if v.is_null() {
            return Ok(Value::Null);
        }
        return Err(type_error(format!("can't slice a {}", v.type_name())));
    };
    let (Some(a), Some(b)) = (clamp(lo, l.len(), 0)?, clamp(hi, l.len(), l.len())?) else {
        return Ok(Value::Null);
    };
    Ok(Value::List(if a < b { l[a..b].to_vec() } else { vec![] }))
}

fn unary(op: UnOp, v: Value) -> Result<Value> {
    Ok(match (op, v) {
        (_, Value::Null) => Value::Null,
        (UnOp::Not, v) => tri(v.truth()?.map(|b| !b)),
        (UnOp::Neg, Value::Int(i)) => Value::Int(
            i.checked_neg()
                .ok_or_else(|| arith_err("integer overflow"))?,
        ),
        (UnOp::Neg, Value::Float(f)) => Value::Float(-f),
        (UnOp::Plus, v @ (Value::Int(_) | Value::Float(_))) => v,
        (_, v) => return Err(type_error(format!("unary minus on a {}", v.type_name()))),
    })
}

#[allow(clippy::cast_precision_loss)]
fn num(a: &Value, b: &Value) -> Option<(f64, f64)> {
    let f = |v: &Value| match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        _ => None,
    };
    Some((f(a)?, f(b)?))
}

/// Applies a binary operator other than `AND`/`OR` (which short-circuit).
///
/// # Errors
/// 39909 for operand types that don't fit.
#[allow(clippy::too_many_lines)] // one arm per operator
pub fn binary(op: BinOp, a: Value, b: Value) -> Result<Value> {
    use std::cmp::Ordering as O;
    if matches!(op, BinOp::Add | BinOp::Sub)
        && !a.is_null()
        && !b.is_null()
        && let Some(v) = crate::temporal::add(&a, &b, if op == BinOp::Add { 1 } else { -1 })?
    {
        return Ok(v);
    }
    Ok(match op {
        BinOp::Eq => tri(a.cypher_eq(&b)),
        BinOp::Ne => tri(a.cypher_eq(&b).map(|x| !x)),
        BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            if a.is_null() || b.is_null() {
                return Ok(Value::Null);
            }
            tri(a.cypher_cmp(&b).map(|o| match op {
                BinOp::Lt => o == O::Less,
                BinOp::Le => o != O::Greater,
                BinOp::Gt => o == O::Greater,
                _ => o != O::Less,
            }))
        }
        BinOp::Xor => match (a.truth()?, b.truth()?) {
            (Some(x), Some(y)) => Value::Bool(x != y),
            _ => Value::Null,
        },
        BinOp::In => match b {
            Value::Null => Value::Null,
            Value::List(l) => {
                let mut unknown = false;
                for x in &l {
                    match a.cypher_eq(x) {
                        Some(true) => return Ok(Value::Bool(true)),
                        None => unknown = true,
                        Some(false) => {}
                    }
                }
                if unknown {
                    Value::Null
                } else {
                    Value::Bool(false)
                }
            }
            other => return Err(type_error(format!("IN over a {}", other.type_name()))),
        },
        BinOp::StartsWith | BinOp::EndsWith | BinOp::Contains => match (&a, &b) {
            (Value::String(x), Value::String(y)) => Value::Bool(match op {
                BinOp::StartsWith => x.starts_with(y.as_str()),
                BinOp::EndsWith => x.ends_with(y.as_str()),
                _ => x.contains(y.as_str()),
            }),
            _ => Value::Null,
        },
        BinOp::Regex => match (&a, &b) {
            (Value::String(x), Value::String(p)) => {
                let re = regex::Regex::new(&format!("^(?:{p})$")).map_err(|e| {
                    SprigError::new(SprigCode::GraphSyntax, format!("Invalid regex: {e}"))
                })?;
                Value::Bool(re.is_match(x))
            }
            _ => Value::Null,
        },
        BinOp::Mul | BinOp::Div
            if matches!(
                (&a, &b),
                (Value::Duration(_), Value::Int(_) | Value::Float(_))
            ) || (op == BinOp::Mul
                && matches!(
                    (&a, &b),
                    (Value::Int(_) | Value::Float(_), Value::Duration(_))
                )) =>
        {
            #[allow(clippy::cast_precision_loss)]
            let (d, n) = match (&a, &b) {
                (Value::Duration(d), Value::Int(i)) | (Value::Int(i), Value::Duration(d)) => {
                    (*d, *i as f64)
                }
                (Value::Duration(d), Value::Float(f)) | (Value::Float(f), Value::Duration(d)) => {
                    (*d, *f)
                }
                _ => return Err(SprigError::internal("duration scaling")),
            };
            if op == BinOp::Div && n == 0.0 {
                return Err(arith_err("/ by zero"));
            }
            Value::Duration(crate::temporal::scale(
                &d,
                if op == BinOp::Div { 1.0 / n } else { n },
            ))
        }
        BinOp::Add => match (a, b) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Int(x), Value::Int(y)) => Value::Int(
                x.checked_add(y)
                    .ok_or_else(|| arith_err("integer overflow"))?,
            ),
            (Value::String(x), Value::String(y)) => Value::String(x + &y),
            (Value::String(x), y @ (Value::Int(_) | Value::Float(_) | Value::Bool(_))) => {
                Value::String(x + &y.display())
            }
            (x @ (Value::Int(_) | Value::Float(_) | Value::Bool(_)), Value::String(y)) => {
                Value::String(x.display() + &y)
            }
            (Value::List(mut x), Value::List(y)) => {
                x.extend(y);
                Value::List(x)
            }
            (Value::List(mut x), y) => {
                x.push(y);
                Value::List(x)
            }
            (x, Value::List(y)) => {
                let mut l = vec![x];
                l.extend(y);
                Value::List(l)
            }
            (x, y) => match num(&x, &y) {
                Some((p, q)) => Value::Float(p + q),
                None => {
                    return Err(type_error(format!(
                        "Cannot add {} and {}",
                        x.type_name(),
                        y.type_name()
                    )));
                }
            },
        },
        BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod | BinOp::Pow => {
            if a.is_null() || b.is_null() {
                return Ok(Value::Null);
            }
            if let (Value::Int(x), Value::Int(y)) = (&a, &b) {
                let (x, y) = (*x, *y);
                return Ok(match op {
                    BinOp::Sub => Value::Int(
                        x.checked_sub(y)
                            .ok_or_else(|| arith_err("integer overflow"))?,
                    ),
                    BinOp::Mul => Value::Int(
                        x.checked_mul(y)
                            .ok_or_else(|| arith_err("integer overflow"))?,
                    ),
                    BinOp::Div => {
                        if y == 0 {
                            return Err(arith_err("/ by zero"));
                        }
                        Value::Int(x.wrapping_div(y))
                    }
                    BinOp::Mod => {
                        if y == 0 {
                            return Err(arith_err("/ by zero"));
                        }
                        Value::Int(x.wrapping_rem(y))
                    }
                    #[allow(clippy::cast_precision_loss)]
                    _ => Value::Float((x as f64).powf(y as f64)),
                });
            }
            let Some((x, y)) = num(&a, &b) else {
                return Err(type_error(format!(
                    "arithmetic on {} and {}",
                    a.type_name(),
                    b.type_name()
                )));
            };
            Value::Float(match op {
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div => x / y,
                BinOp::Mod => x % y,
                _ => x.powf(y),
            })
        }
        BinOp::And | BinOp::Or => return Err(SprigError::internal("AND/OR reached binary()")),
    })
}

fn arg(args: &[Value], i: usize) -> &Value {
    args.get(i).unwrap_or(&Value::Null)
}

fn want(name: &str, args: &[Value], n: usize) -> Result<()> {
    if args.len() == n {
        Ok(())
    } else {
        Err(SprigError::new(
            SprigCode::GraphSyntax,
            format!("{name}() takes {n} argument(s), got {}", args.len()),
        ))
    }
}

fn int_arg(v: &Value, name: &str) -> Result<Option<i64>> {
    match v {
        Value::Null => Ok(None),
        Value::Int(i) => Ok(Some(*i)),
        #[allow(clippy::cast_possible_truncation)]
        Value::Float(f) => Ok(Some(*f as i64)),
        other => Err(type_error(format!(
            "{name}() expects a number, got {}",
            other.type_name()
        ))),
    }
}

fn str_fn(args: &[Value], f: impl Fn(&str) -> String) -> Result<Value> {
    Ok(match arg(args, 0) {
        Value::Null => Value::Null,
        Value::String(s) => Value::String(f(s)),
        other => {
            return Err(type_error(format!(
                "expected a STRING, got {}",
                other.type_name()
            )));
        }
    })
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn math(args: &[Value], f: impl Fn(f64) -> f64) -> Result<Value> {
    Ok(match arg(args, 0) {
        Value::Null => Value::Null,
        Value::Int(i) => Value::Float(f(*i as f64)),
        Value::Float(x) => Value::Float(f(*x)),
        other => {
            return Err(type_error(format!(
                "expected a number, got {}",
                other.type_name()
            )));
        }
    })
}

/// Calls a scalar function.
///
/// # Errors
/// 39901 for unknown functions; 39909 for argument types.
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]
pub fn call(name: &str, args: Vec<Value>) -> Result<Value> {
    let a0 = arg(&args, 0).clone();
    Ok(match name {
        "id" => match a0 {
            Value::Node(n) => Value::Int(i64::try_from(n.id).unwrap_or(i64::MAX)),
            Value::Rel(r) => Value::Int(i64::try_from(r.id).unwrap_or(i64::MAX)),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("id() of a {}", o.type_name()))),
        },
        "elementid" => match a0 {
            Value::Node(n) => Value::String(n.element_id()),
            Value::Rel(r) => Value::String(r.element_id()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("elementId() of a {}", o.type_name()))),
        },
        "labels" => match a0 {
            Value::Node(n) => Value::List(n.labels.iter().cloned().map(Value::String).collect()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("labels() of a {}", o.type_name()))),
        },
        "type" => match a0 {
            Value::Rel(r) => Value::String(r.rtype.clone()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("type() of a {}", o.type_name()))),
        },
        "properties" => match a0 {
            Value::Node(n) => Value::Map(n.all_props()?.clone()),
            Value::Rel(r) => Value::Map(r.all_props()?.clone()),
            m @ Value::Map(_) => m,
            Value::Null => Value::Null,
            o => return Err(type_error(format!("properties() of a {}", o.type_name()))),
        },
        "keys" => match a0 {
            Value::Node(n) => {
                Value::List(n.all_props()?.keys().cloned().map(Value::String).collect())
            }
            Value::Rel(r) => {
                Value::List(r.all_props()?.keys().cloned().map(Value::String).collect())
            }
            Value::Map(m) => Value::List(m.keys().cloned().map(Value::String).collect()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("keys() of a {}", o.type_name()))),
        },
        "size" => match a0 {
            Value::List(l) => Value::Int(i64::try_from(l.len()).unwrap_or(i64::MAX)),
            Value::Bytes(b) => Value::Int(i64::try_from(b.len()).unwrap_or(i64::MAX)),
            Value::String(s) => Value::Int(i64::try_from(s.chars().count()).unwrap_or(i64::MAX)),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("size() of a {}", o.type_name()))),
        },
        "length" => match a0 {
            Value::Path(p) => Value::Int(i64::try_from(p.rels.len()).unwrap_or(i64::MAX)),
            Value::String(s) => Value::Int(i64::try_from(s.chars().count()).unwrap_or(i64::MAX)),
            Value::List(l) => Value::Int(i64::try_from(l.len()).unwrap_or(i64::MAX)),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("length() of a {}", o.type_name()))),
        },
        "nodes" => match a0 {
            Value::Path(p) => Value::List(p.nodes.iter().cloned().map(Value::Node).collect()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("nodes() of a {}", o.type_name()))),
        },
        "relationships" => match a0 {
            Value::Path(p) => Value::List(p.rels.iter().cloned().map(Value::Rel).collect()),
            Value::Null => Value::Null,
            o => {
                return Err(type_error(format!(
                    "relationships() of a {}",
                    o.type_name()
                )));
            }
        },
        "startnode" | "endnode" => match a0 {
            Value::Null => Value::Null,
            _ => return Err(unsupported(format!("{name}()"))),
        },
        "coalesce" => args
            .into_iter()
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null),
        "head" => match a0 {
            Value::List(l) => l.into_iter().next().unwrap_or(Value::Null),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("head() of a {}", o.type_name()))),
        },
        "last" => match a0 {
            Value::List(l) => l.into_iter().last().unwrap_or(Value::Null),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("last() of a {}", o.type_name()))),
        },
        "tail" => match a0 {
            Value::List(l) => Value::List(l.into_iter().skip(1).collect()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("tail() of a {}", o.type_name()))),
        },
        "reverse" => match a0 {
            Value::List(mut l) => {
                l.reverse();
                Value::List(l)
            }
            Value::String(s) => Value::String(s.chars().rev().collect()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("reverse() of a {}", o.type_name()))),
        },
        "range" => {
            let lo = int_arg(arg(&args, 0), name)?.unwrap_or(0);
            let hi = int_arg(arg(&args, 1), name)?.unwrap_or(0);
            let step = if args.len() > 2 {
                int_arg(arg(&args, 2), name)?.unwrap_or(1)
            } else {
                1
            };
            if step == 0 {
                return Err(type_error("range() step must not be 0"));
            }
            let mut out = Vec::new();
            let mut i = lo;
            while (step > 0 && i <= hi) || (step < 0 && i >= hi) {
                out.push(Value::Int(i));
                if out.len() > 10_000_000 {
                    return Err(SprigError::new(
                        SprigCode::GraphBudgetExceeded,
                        "range() is too long (rows)",
                    ));
                }
                i = match i.checked_add(step) {
                    Some(n) => n,
                    None => break,
                };
            }
            Value::List(out)
        }
        "isempty" => match a0 {
            Value::List(l) => Value::Bool(l.is_empty()),
            Value::String(s) => Value::Bool(s.is_empty()),
            Value::Map(m) => Value::Bool(m.is_empty()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("isEmpty() of a {}", o.type_name()))),
        },
        "tostring" => match a0 {
            Value::Null => Value::Null,
            Value::Float(f) => Value::String(fmt_float(f)),
            v @ (Value::Int(_)
            | Value::Bool(_)
            | Value::String(_)
            | Value::Date(_)
            | Value::LocalTime(_)
            | Value::Time(..)
            | Value::LocalDateTime(_)
            | Value::DateTime(_)
            | Value::Duration(_)
            | Value::Point(_)) => Value::String(v.display()),
            o => return Err(type_error(format!("toString() of a {}", o.type_name()))),
        },
        "tointeger" => match a0 {
            Value::Int(i) => Value::Int(i),
            Value::Float(f) => Value::Int(f.trunc() as i64),
            Value::Bool(b) => Value::Int(i64::from(b)),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .or_else(|_| {
                    s.trim()
                        .parse::<f64>()
                        .map(|f| Value::Int(f.trunc() as i64))
                })
                .unwrap_or(Value::Null),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("toInteger() of a {}", o.type_name()))),
        },
        "tofloat" => match a0 {
            Value::Int(i) => Value::Float(i as f64),
            Value::Float(f) => Value::Float(f),
            Value::String(s) => s.trim().parse::<f64>().map_or(Value::Null, Value::Float),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("toFloat() of a {}", o.type_name()))),
        },
        "toboolean" => match a0 {
            Value::Bool(b) => Value::Bool(b),
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                _ => Value::Null,
            },
            Value::Int(i) => Value::Bool(i != 0),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("toBoolean() of a {}", o.type_name()))),
        },
        "abs" => match a0 {
            Value::Int(i) => Value::Int(
                i.checked_abs()
                    .ok_or_else(|| arith_err("integer overflow"))?,
            ),
            Value::Float(f) => Value::Float(f.abs()),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("abs() of a {}", o.type_name()))),
        },
        "sign" => match a0 {
            Value::Int(i) => Value::Int(i.signum()),
            Value::Float(f) => Value::Int(if f > 0.0 {
                1
            } else if f < 0.0 {
                -1
            } else {
                0
            }),
            Value::Null => Value::Null,
            o => return Err(type_error(format!("sign() of a {}", o.type_name()))),
        },
        "ceil" => math(&args, f64::ceil)?,
        "floor" => math(&args, f64::floor)?,
        "round" => {
            if args.len() > 1 {
                let p = int_arg(arg(&args, 1), name)?.unwrap_or(0);
                let m = 10f64.powi(i32::try_from(p).unwrap_or(0));
                math(&args, |x| (x * m).round() / m)?
            } else {
                math(&args, f64::round)?
            }
        }
        "sqrt" => math(&args, f64::sqrt)?,
        "exp" => math(&args, f64::exp)?,
        "log" => math(&args, f64::ln)?,
        "log10" => math(&args, f64::log10)?,
        "sin" => math(&args, f64::sin)?,
        "cos" => math(&args, f64::cos)?,
        "tan" => math(&args, f64::tan)?,
        "pi" => Value::Float(std::f64::consts::PI),
        "e" => Value::Float(std::f64::consts::E),
        "rand" => {
            let n = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos());
            Value::Float(f64::from(n % 1_000_000) / 1_000_000.0)
        }
        "timestamp" => Value::Int(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0)),
        ),
        "toupper" | "upper" => str_fn(&args, str::to_uppercase)?,
        "tolower" | "lower" => str_fn(&args, str::to_lowercase)?,
        "trim" => str_fn(&args, |s| s.trim().to_owned())?,
        "ltrim" => str_fn(&args, |s| s.trim_start().to_owned())?,
        "rtrim" => str_fn(&args, |s| s.trim_end().to_owned())?,
        "replace" => {
            want(name, &args, 3)?;
            match (&args[0], &args[1], &args[2]) {
                (Value::String(s), Value::String(f), Value::String(t)) => {
                    Value::String(s.replace(f.as_str(), t))
                }
                _ => Value::Null,
            }
        }
        "split" => match (&a0, arg(&args, 1)) {
            (Value::String(s), Value::String(d)) => {
                Value::List(s.split(d.as_str()).map(Value::str).collect())
            }
            _ => Value::Null,
        },
        "substring" => {
            let Value::String(s) = &a0 else {
                return Ok(Value::Null);
            };
            let chars: Vec<char> = s.chars().collect();
            let start = usize::try_from(int_arg(arg(&args, 1), name)?.unwrap_or(0).max(0))
                .unwrap_or(0)
                .min(chars.len());
            let len = if args.len() > 2 {
                usize::try_from(int_arg(arg(&args, 2), name)?.unwrap_or(0).max(0)).unwrap_or(0)
            } else {
                chars.len()
            };
            Value::String(
                chars[start..(start + len).min(chars.len())]
                    .iter()
                    .collect(),
            )
        }
        "left" | "right" => {
            let Value::String(s) = &a0 else {
                return Ok(Value::Null);
            };
            let chars: Vec<char> = s.chars().collect();
            let n = usize::try_from(int_arg(arg(&args, 1), name)?.unwrap_or(0).max(0))
                .unwrap_or(0)
                .min(chars.len());
            Value::String(if name == "left" {
                chars[..n].iter().collect()
            } else {
                chars[chars.len() - n..].iter().collect()
            })
        }
        "exists" => Value::Bool(!a0.is_null()),
        // Neo4j types a byte array as a list of integers.
        "valuetype" if matches!(a0, Value::Bytes(_)) => {
            Value::str("LIST<INTEGER NOT NULL> NOT NULL")
        }
        "valuetype" => Value::str(a0.type_name()),
        other => match crate::temporal::call(other, &args)? {
            Some(v) => v,
            None => return Err(unsupported(format!("function {other}()"))),
        },
    })
}

/// A path value from its parts.
#[must_use]
pub fn path_value(nodes: Vec<Arc<crate::value::Node>>, rels: Vec<Arc<crate::value::Rel>>) -> Value {
    Value::Path(Arc::new(crate::value::Path { nodes, rels }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_expr;

    fn ev(s: &str) -> Value {
        eval(
            &parse_expr(s).expect("parse"),
            &Row::default(),
            &Params::new(),
        )
        .unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    #[test]
    fn three_valued_logic_and_nulls() {
        assert_eq!(ev("null AND false"), Value::Bool(false));
        assert_eq!(ev("null AND true"), Value::Null);
        assert_eq!(ev("null OR true"), Value::Bool(true));
        assert_eq!(ev("NOT null"), Value::Null);
        assert_eq!(ev("1 = '1'"), Value::Bool(false));
        assert_eq!(ev("1 = 1.0"), Value::Bool(true));
        assert_eq!(ev("1 < 'a'"), Value::Null);
        assert_eq!(ev("2 IN [1, null]"), Value::Null);
        assert_eq!(ev("1 IN [1, null]"), Value::Bool(true));
        assert_eq!(ev("null IS NULL"), Value::Bool(true));
        assert_eq!(ev("true XOR false"), Value::Bool(true));
    }

    #[test]
    fn arithmetic_strings_and_lists() {
        assert_eq!(ev("7 / 2"), Value::Int(3));
        assert_eq!(ev("7 / 2.0"), Value::Float(3.5));
        assert_eq!(ev("-7 % 3"), Value::Int(-1));
        assert_eq!(ev("2 ^ 3"), Value::Float(8.0));
        assert_eq!(ev("'a' + 1"), Value::str("a1"));
        assert_eq!(ev("[1] + 2 + [3]"), ev("[1, 2, 3]"));
        assert_eq!(ev("[1,2,3,4][1..3]"), ev("[2, 3]"));
        assert_eq!(ev("[1,2,3][-1]"), Value::Int(3));
        assert_eq!(ev("[1,2,3][5]"), Value::Null);
        assert_eq!(ev("'abc' STARTS WITH 'a'"), Value::Bool(true));
        assert_eq!(ev("'abc' =~ 'a.c'"), Value::Bool(true));
        assert_eq!(ev("'abc' =~ 'b'"), Value::Bool(false));
        assert_eq!(
            ev("[x IN range(1, 6) WHERE x % 2 = 0 | x * 10]"),
            ev("[20, 40, 60]")
        );
        assert_eq!(ev("all(x IN [1, 2] WHERE x > 0)"), Value::Bool(true));
        assert_eq!(ev("single(x IN [1, 2] WHERE x > 1)"), Value::Bool(true));
        assert_eq!(
            ev("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' END"),
            Value::str("b")
        );
        assert_eq!(ev("{a: 1, b: [1, 2]}.b[0]"), Value::Int(1));
        let e = eval(
            &parse_expr("1 / 0").expect("p"),
            &Row::default(),
            &Params::new(),
        )
        .expect_err("div");
        assert_eq!(e.code(), Some(SprigCode::GraphPropertyType));
        let e = eval(
            &parse_expr("9223372036854775807 + 1").expect("p"),
            &Row::default(),
            &Params::new(),
        )
        .expect_err("overflow");
        assert_eq!(e.code(), Some(SprigCode::GraphPropertyType));
    }

    #[test]
    fn functions() {
        assert_eq!(ev("toUpper('ab')"), Value::str("AB"));
        assert_eq!(ev("substring('hello', 1, 3)"), Value::str("ell"));
        assert_eq!(ev("size([1,2])"), Value::Int(2));
        assert_eq!(ev("coalesce(null, 2)"), Value::Int(2));
        assert_eq!(ev("toInteger('42')"), Value::Int(42));
        assert_eq!(ev("toInteger('x')"), Value::Null);
        assert_eq!(ev("toString(1.0)"), Value::str("1.0"));
        assert_eq!(ev("round(2.567, 2)"), Value::Float(2.57));
        assert_eq!(ev("split('a,b', ',')"), ev("['a', 'b']"));
        assert_eq!(ev("reverse('abc')"), Value::str("cba"));
        assert_eq!(ev("range(0, 10, 5)"), ev("[0, 5, 10]"));
        let e = eval(
            &parse_expr("nope(1)").expect("p"),
            &Row::default(),
            &Params::new(),
        )
        .expect_err("unknown");
        assert_eq!(e.code(), Some(SprigCode::GraphUnsupported));
    }
}
